//! KV client: finds the leader and retries under one session sequence number.
//!
//! One `KvClient` is one session. Every retry of an operation reuses that
//! operation's `seq_num`, so the server's dedup table turns a retried write into
//! a replay instead of a second apply.
//!
//! The seq advances only on a definitive answer: a reply, or a rejection. A
//! timeout is not definitive, because the operation may still have committed.
//! It stays pending under its seq, and the next call resends it until it
//! settles before starting anything new. Advancing past it would let a caller's
//! retry apply twice; reusing its seq for a different operation would let the
//! dedup table answer that operation with this one's cached result.

// `attempt` returns `tonic::Status`, which is large; it never leaves this module.
#![allow(clippy::result_large_err)]

use std::net::SocketAddr;
use std::time::Duration;

use prost::Message;
use tokio::time::Instant;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};

use crate::proto::kv as pb;
use crate::proto::kv::kv_client::KvClient as KvStub;

const BACKOFF_MIN: Duration = Duration::from_millis(20);
const BACKOFF_MAX: Duration = Duration::from_millis(500);

pub struct ClientConfig {
    pub endpoints: Vec<SocketAddr>,
    /// Whole call, every retry included.
    pub request_timeout: Duration,
    /// One RPC to one node.
    pub attempt_timeout: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("no endpoints configured")]
    NoEndpoints,

    #[error("invalid endpoint {addr}: {source}")]
    InvalidEndpoint {
        addr: SocketAddr,
        #[source]
        source: tonic::transport::Error,
    },

    /// The outcome is unknown: the operation may or may not have committed.
    /// It stays pending, and the next call on this client resends it first.
    #[error("{op} unconfirmed after {attempts} attempts in {elapsed:?}, last tried {node}: {last}")]
    Timeout {
        op: &'static str,
        node: SocketAddr,
        attempts: u32,
        elapsed: Duration,
        last: String,
    },

    /// This call's operation was never sent: an earlier one is still unsettled
    /// and was retried until this call's deadline.
    #[error("{op} not sent: an earlier {pending} is still unconfirmed")]
    Unsettled {
        op: &'static str,
        pending: &'static str,
        #[source]
        cause: Box<ClientError>,
    },

    /// A client bug (bad meta, stale seq). Retrying cannot fix it.
    #[error("{op} rejected by {node} ({code:?}): {message}")]
    Rejected {
        op: &'static str,
        node: SocketAddr,
        code: Code,
        message: String,
    },

    #[error("{op}: reply did not match the request")]
    Mismatch { op: &'static str },
}

pub struct KvClient {
    session: Session,
    leader: LeaderCache,
    config: ClientConfig,
    /// An operation that timed out. Its seq is still the current one.
    pending: Option<Op>,
}

impl KvClient {
    /// Does not connect; channels open on first use. Must be called inside a
    /// Tokio runtime, because a lazy channel spawns its connection task.
    pub fn new(config: ClientConfig) -> Result<Self, ClientError> {
        Ok(Self {
            session: Session::new(rand::random()),
            leader: LeaderCache::new(&config.endpoints)?,
            config,
            pending: None,
        })
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<pb::GetResponse, ClientError> {
        match self.execute(Op::Get(key.to_vec())).await? {
            Reply::Get(reply) => Ok(reply),
            _ => Err(ClientError::Mismatch { op: "get" }),
        }
    }

    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<pb::PutResponse, ClientError> {
        match self.execute(Op::Put(key.to_vec(), value.to_vec())).await? {
            Reply::Put(reply) => Ok(reply),
            _ => Err(ClientError::Mismatch { op: "put" }),
        }
    }

    pub async fn delete(&mut self, key: &[u8]) -> Result<pb::DeleteResponse, ClientError> {
        match self.execute(Op::Delete(key.to_vec())).await? {
            Reply::Delete(reply) => Ok(reply),
            _ => Err(ClientError::Mismatch { op: "delete" }),
        }
    }

    pub async fn cas(
        &mut self,
        key: &[u8],
        expected_version: Option<u64>,
        value: Option<&[u8]>,
    ) -> Result<pb::CasResponse, ClientError> {
        let op = Op::Cas {
            key: key.to_vec(),
            expected_version,
            value: value.map(<[u8]>::to_vec),
        };
        match self.execute(op).await? {
            Reply::Cas(reply) => Ok(reply),
            _ => Err(ClientError::Mismatch { op: "cas" }),
        }
    }

    /// Settles any pending operation, then runs `op`, all within one
    /// `request_timeout`.
    async fn execute(&mut self, op: Op) -> Result<Reply, ClientError> {
        let deadline = Instant::now() + self.config.request_timeout;

        if let Some(pending) = self.pending.take() {
            // The caller is retrying the unconfirmed operation itself: same seq,
            // so if the first attempt committed, this returns its cached result.
            if pending == op {
                return self.settle(op, deadline).await;
            }
            // Something new. The unconfirmed one goes first under its own seq;
            // its caller already saw a timeout, so its result is dropped.
            let pending_name = pending.name();
            match self.settle(pending, deadline).await {
                // Settled; the earlier caller already timed out, so drop the reply.
                Ok(_) => {}
                // Still unknown: re-parked inside `settle`. Do not start `op`.
                Err(cause @ ClientError::Timeout { .. }) => {
                    return Err(ClientError::Unsettled {
                        op: op.name(),
                        pending: pending_name,
                        cause: Box::new(cause),
                    });
                }
                // Definitive failure (`Rejected`, `Mismatch`, …). Seq advanced and
                // the slot is clear; proceed with the caller's new operation.
                Err(_) => {}
            }
        }

        self.settle(op, deadline).await
    }

    /// Retries `op` under the current seq until a definitive answer or
    /// `deadline`. A definitive answer advances the seq; a timeout parks `op` in
    /// `pending` and leaves the seq where it is.
    async fn settle(&mut self, op: Op, deadline: Instant) -> Result<Reply, ClientError> {
        let started = Instant::now();
        let mut attempts = 0_u32;
        let mut node = self.leader.current();
        let mut last = String::from("no attempt completed");
        let attempt_timeout = self.config.attempt_timeout;
        let leader = &mut self.leader;
        let session = &self.session;
        let op_ref = &op;

        // Cancelled mid-RPC or mid-sleep at the deadline. Safe: the locals it
        // writes are only read after it is gone, and nothing else changes inside.
        let outcome = tokio::time::timeout_at(deadline, async {
            let mut backoff = BACKOFF_MIN;
            loop {
                attempts += 1;
                node = leader.current();
                let status =
                    match attempt(leader.stub(), session.meta(), op_ref, attempt_timeout).await {
                        Ok(reply) => return Ok(reply),
                        Err(status) => status,
                    };
                last = format!("{:?} {}", status.code(), status.message());

                match status.code() {
                    Code::FailedPrecondition => match leader_hint(&status) {
                        // A new name is worth trying at once.
                        Some(hint) if hint != node => {
                            leader.redirect(hint);
                            continue;
                        }
                        // The refusing node names itself: it has just become
                        // leader, so try it again after the backoff.
                        Some(_) => {}
                        None => leader.rotate(),
                    },
                    // Retryable: another node may still complete the request.
                    Code::Unavailable
                    | Code::DeadlineExceeded
                    | Code::Cancelled
                    | Code::Unknown => {
                        leader.rotate();
                    }
                    // Some Internals are permanent (durability, wrong result shape).
                    // Retrying them only burns the deadline.
                    Code::Internal if !definitive_internal(&status) => leader.rotate(),
                    code => {
                        return Err(ClientError::Rejected {
                            op: op_ref.name(),
                            node,
                            code,
                            message: status.message().to_owned(),
                        });
                    }
                }

                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(BACKOFF_MAX);
            }
        })
        .await;

        match outcome {
            Ok(definitive) => {
                self.session.advance();
                // A definitive answer clears any prior park of this op.
                self.pending = None;
                definitive
            }
            Err(_) => {
                let name = op.name();
                self.pending = Some(op);
                Err(ClientError::Timeout {
                    op: name,
                    node,
                    attempts,
                    elapsed: started.elapsed(),
                    last,
                })
            }
        }
    }
}

/// Internals that will not succeed on another node or a later attempt.
fn definitive_internal(status: &Status) -> bool {
    let message = status.message();
    message.contains("durability failed") || message.contains("result does not match")
}

struct Session {
    client_id: u64,
    next_seq: u64,
}

impl Session {
    fn new(client_id: u64) -> Self {
        Self {
            client_id,
            next_seq: 1,
        }
    }

    /// The current seq. Does not advance.
    fn meta(&self) -> pb::ClientMeta {
        pb::ClientMeta {
            client_id: self.client_id,
            seq_num: self.next_seq,
        }
    }

    /// Only on a definitive answer; see the module docs.
    fn advance(&mut self) {
        self.next_seq += 1;
    }
}

struct LeaderCache {
    /// Configured endpoints first, then any address learned from a hint.
    members: Vec<(SocketAddr, Channel)>,
    /// Always a valid index: `members` is never empty and only grows.
    current: usize,
}

impl LeaderCache {
    fn new(endpoints: &[SocketAddr]) -> Result<Self, ClientError> {
        if endpoints.is_empty() {
            return Err(ClientError::NoEndpoints);
        }
        let members = endpoints
            .iter()
            .map(|&addr| Ok((addr, lazy_channel(addr)?)))
            .collect::<Result<Vec<_>, ClientError>>()?;
        Ok(Self {
            members,
            current: 0,
        })
    }

    fn current(&self) -> SocketAddr {
        self.members[self.current].0
    }

    fn stub(&self) -> KvStub<Channel> {
        // A `Channel` is a cheap handle to one shared connection, and the
        // generated client takes it by value.
        KvStub::new(self.members[self.current].1.clone())
    }

    fn redirect(&mut self, to: SocketAddr) {
        if let Some(index) = self.members.iter().position(|(addr, _)| *addr == to) {
            self.current = index;
            return;
        }
        match lazy_channel(to) {
            Ok(channel) => {
                self.members.push((to, channel));
                self.current = self.members.len() - 1;
            }
            Err(_) => self.rotate(),
        }
    }

    fn rotate(&mut self) {
        self.current = (self.current + 1) % self.members.len();
    }
}

fn lazy_channel(addr: SocketAddr) -> Result<Channel, ClientError> {
    Endpoint::from_shared(format!("http://{addr}"))
        .map(|endpoint| endpoint.connect_lazy())
        .map_err(|source| ClientError::InvalidEndpoint { addr, source })
}

#[derive(PartialEq, Eq)]
enum Op {
    Get(Vec<u8>),
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    Cas {
        key: Vec<u8>,
        expected_version: Option<u64>,
        value: Option<Vec<u8>>,
    },
}

impl Op {
    fn name(&self) -> &'static str {
        match self {
            Op::Get(_) => "get",
            Op::Put(..) => "put",
            Op::Delete(_) => "delete",
            Op::Cas { .. } => "cas",
        }
    }
}

enum Reply {
    Get(pb::GetResponse),
    Put(pb::PutResponse),
    Delete(pb::DeleteResponse),
    Cas(pb::CasResponse),
}

/// One RPC to one node.
async fn attempt(
    mut stub: KvStub<Channel>,
    meta: pb::ClientMeta,
    op: &Op,
    timeout: Duration,
) -> Result<Reply, Status> {
    let meta = Some(meta);
    // tonic takes the request by value, so each attempt copies key and value.
    // Retries are the rare path; the first attempt pays one copy.
    let call = async move {
        match op {
            Op::Get(key) => stub
                .get(request(
                    pb::GetRequest {
                        meta,
                        key: key.clone(),
                    },
                    timeout,
                ))
                .await
                .map(|r| Reply::Get(r.into_inner())),
            Op::Put(key, value) => stub
                .put(request(
                    pb::PutRequest {
                        meta,
                        key: key.clone(),
                        value: value.clone(),
                    },
                    timeout,
                ))
                .await
                .map(|r| Reply::Put(r.into_inner())),
            Op::Delete(key) => stub
                .delete(request(
                    pb::DeleteRequest {
                        meta,
                        key: key.clone(),
                    },
                    timeout,
                ))
                .await
                .map(|r| Reply::Delete(r.into_inner())),
            Op::Cas {
                key,
                expected_version,
                value,
            } => stub
                .cas(request(
                    pb::CasRequest {
                        meta,
                        key: key.clone(),
                        expected_version: *expected_version,
                        value: value.clone(),
                    },
                    timeout,
                ))
                .await
                .map(|r| Reply::Cas(r.into_inner())),
        }
    };
    // `set_timeout` only informs the server. A partitioned node never answers,
    // so the client needs its own clock too.
    tokio::time::timeout(timeout, call)
        .await
        .unwrap_or_else(|_| Err(Status::deadline_exceeded("attempt timed out")))
}

fn request<T>(message: T, timeout: Duration) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request.set_timeout(timeout);
    request
}

/// Reads the leader's KV address from a [`pb::NotLeader`] status detail. An
/// empty hint, or no detail at all, means no leader is known.
fn leader_hint(status: &Status) -> Option<SocketAddr> {
    pb::NotLeader::decode(status.details())
        .ok()?
        .leader_hint
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    #[test]
    fn a_hint_is_read_from_the_not_leader_detail() {
        let not_leader = |hint: &str| {
            let detail = pb::NotLeader {
                leader_hint: hint.to_owned(),
            };
            Status::with_details(
                Code::FailedPrecondition,
                "wording the client must not depend on",
                detail.encode_to_vec().into(),
            )
        };
        assert_eq!(leader_hint(&not_leader("127.0.0.1:9002")), Some(addr(9002)));
        assert_eq!(
            leader_hint(&not_leader("[::1]:9002")),
            Some("[::1]:9002".parse().expect("v6"))
        );
        assert_eq!(leader_hint(&not_leader("")), None);
        // A hint in the message text alone is not a hint.
        assert_eq!(
            leader_hint(&Status::failed_precondition(
                "not leader; try 127.0.0.1:9002"
            )),
            None
        );
    }

    #[test]
    fn the_session_reuses_its_seq_until_advanced() {
        let mut session = Session::new(7);
        assert_eq!(session.meta().seq_num, 1);
        assert_eq!(session.meta().seq_num, 1, "reading must not advance");
        session.advance();
        assert_eq!(session.meta().seq_num, 2);
    }

    #[test]
    fn an_empty_endpoint_list_is_rejected() {
        assert!(matches!(
            LeaderCache::new(&[]),
            Err(ClientError::NoEndpoints)
        ));
    }

    #[tokio::test]
    async fn rotate_visits_every_endpoint_before_repeating() {
        let mut cache = LeaderCache::new(&[addr(1), addr(2), addr(3)]).expect("cache");
        let seen: Vec<_> = (0..4)
            .map(|_| {
                let current = cache.current();
                cache.rotate();
                current
            })
            .collect();
        assert_eq!(seen, vec![addr(1), addr(2), addr(3), addr(1)]);
    }

    #[tokio::test]
    async fn a_hint_outside_the_configured_list_is_learned() {
        let mut cache = LeaderCache::new(&[addr(1)]).expect("cache");
        cache.redirect(addr(9));
        assert_eq!(cache.current(), addr(9));
        cache.redirect(addr(1));
        assert_eq!(cache.current(), addr(1));
    }

    /// A KV server that records the `(op, seq)` of every request it receives,
    /// and answers nothing while `stalled` is set.
    mod fake {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::{Arc, Mutex};

        use tokio::task::JoinHandle;
        use tonic::{Code, Request, Response, Status};

        use crate::proto::kv as pb;
        use crate::proto::kv::kv_server::{Kv, KvServer};

        #[derive(Clone, Default)]
        pub struct FakeKv {
            pub seen: Arc<Mutex<Vec<(&'static str, u64)>>>,
            pub stalled: Arc<AtomicBool>,
            /// When set, every RPC fails with this status instead of succeeding.
            pub fail_with: Arc<Mutex<Option<(Code, String)>>>,
        }

        impl FakeKv {
            async fn record(
                &self,
                op: &'static str,
                meta: Option<pb::ClientMeta>,
            ) -> Result<(), Status> {
                let seq = meta.map_or(0, |m| m.seq_num);
                self.seen.lock().expect("seen").push((op, seq));
                while self.stalled.load(Ordering::SeqCst) {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                if let Some((code, message)) = self.fail_with.lock().expect("fail").clone() {
                    return Err(Status::new(code, message));
                }
                Ok(())
            }

            pub fn seen(&self) -> Vec<(&'static str, u64)> {
                self.seen.lock().expect("seen").clone()
            }

            pub fn set_fail(&self, code: Code, message: impl Into<String>) {
                *self.fail_with.lock().expect("fail") = Some((code, message.into()));
            }

            pub fn clear_fail(&self) {
                *self.fail_with.lock().expect("fail") = None;
            }

            pub async fn serve(self) -> (std::net::SocketAddr, JoinHandle<()>) {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("bind");
                let addr = listener.local_addr().expect("addr");
                let server = tokio::spawn(async move {
                    let _ = tonic::transport::Server::builder()
                        .add_service(KvServer::new(self))
                        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                            listener,
                        ))
                        .await;
                });
                (addr, server)
            }
        }

        #[tonic::async_trait]
        impl Kv for FakeKv {
            async fn get(
                &self,
                r: Request<pb::GetRequest>,
            ) -> Result<Response<pb::GetResponse>, Status> {
                self.record("get", r.into_inner().meta).await?;
                Ok(Response::new(pb::GetResponse::default()))
            }

            async fn put(
                &self,
                r: Request<pb::PutRequest>,
            ) -> Result<Response<pb::PutResponse>, Status> {
                self.record("put", r.into_inner().meta).await?;
                Ok(Response::new(pb::PutResponse {
                    previous: None,
                    version: 1,
                }))
            }

            async fn delete(
                &self,
                r: Request<pb::DeleteRequest>,
            ) -> Result<Response<pb::DeleteResponse>, Status> {
                self.record("delete", r.into_inner().meta).await?;
                Ok(Response::new(pb::DeleteResponse::default()))
            }

            async fn cas(
                &self,
                r: Request<pb::CasRequest>,
            ) -> Result<Response<pb::CasResponse>, Status> {
                self.record("cas", r.into_inner().meta).await?;
                Ok(Response::new(pb::CasResponse::default()))
            }
        }
    }

    /// A client against `fake` whose calls time out while it is stalled.
    async fn stalled_client() -> (KvClient, fake::FakeKv, tokio::task::JoinHandle<()>) {
        let fake = fake::FakeKv::default();
        fake.stalled
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let (addr, server) = fake.clone().serve().await;
        let client = KvClient::new(ClientConfig {
            endpoints: vec![addr],
            request_timeout: Duration::from_millis(300),
            attempt_timeout: Duration::from_millis(100),
        })
        .expect("client");
        (client, fake, server)
    }

    async fn live_client() -> (KvClient, fake::FakeKv, tokio::task::JoinHandle<()>) {
        let fake = fake::FakeKv::default();
        let (addr, server) = fake.clone().serve().await;
        let client = KvClient::new(ClientConfig {
            endpoints: vec![addr],
            request_timeout: Duration::from_millis(500),
            attempt_timeout: Duration::from_millis(200),
        })
        .expect("client");
        (client, fake, server)
    }

    #[tokio::test]
    async fn retrying_a_timed_out_op_reuses_its_seq() {
        let (mut client, fake, server) = stalled_client().await;

        let error = client.put(b"k", b"v").await.expect_err("stalled");
        assert!(
            matches!(error, ClientError::Timeout { op: "put", .. }),
            "{error}"
        );

        fake.stalled
            .store(false, std::sync::atomic::Ordering::SeqCst);
        client.put(b"k", b"v").await.expect("retry");
        client.get(b"k").await.expect("next op");

        let seen = fake.seen();
        let (puts, gets): (Vec<_>, Vec<_>) = seen.iter().copied().partition(|(op, _)| *op == "put");
        assert!(
            puts.iter().all(|(_, seq)| *seq == 1),
            "every put attempt, before and after the timeout, must carry seq 1: {seen:?}"
        );
        assert_eq!(
            gets,
            vec![("get", 2)],
            "the next op gets the next seq: {seen:?}"
        );
        server.abort();
    }

    #[tokio::test]
    async fn a_timed_out_op_is_resent_before_a_different_one() {
        let (mut client, fake, server) = stalled_client().await;

        client.put(b"k", b"v").await.expect_err("stalled");
        let before = fake.seen().len();

        fake.stalled
            .store(false, std::sync::atomic::Ordering::SeqCst);
        client.get(b"k").await.expect("get");

        let after: Vec<_> = fake.seen().split_off(before);
        assert_eq!(
            after,
            vec![("put", 1), ("get", 2)],
            "the unconfirmed put settles first, under its own seq"
        );
        server.abort();
    }

    #[tokio::test]
    async fn a_still_unconfirmed_op_blocks_the_next_one() {
        let (mut client, fake, server) = stalled_client().await;

        client.put(b"k", b"v").await.expect_err("stalled");
        let error = client.get(b"k").await.expect_err("put still unconfirmed");
        assert!(
            matches!(
                error,
                ClientError::Unsettled {
                    op: "get",
                    pending: "put",
                    ..
                }
            ),
            "{error}"
        );
        assert!(
            fake.seen().iter().all(|(op, _)| *op == "put"),
            "the get must never be sent while the put is unsettled"
        );
        server.abort();
    }

    #[tokio::test]
    async fn a_rejected_pending_op_clears_the_slot_for_the_next() {
        let (mut client, fake, server) = stalled_client().await;

        client.put(b"k", b"v").await.expect_err("stalled");
        fake.stalled
            .store(false, std::sync::atomic::Ordering::SeqCst);
        // Pending put settles as a hard rejection (e.g. stale seq).
        fake.set_fail(
            Code::InvalidArgument,
            "stale seq_num: client already moved past it",
        );

        // Settling the put rejects and advances; the get then hits the same fail
        // mode once. Clear fail before a third call so the slot is proven empty.
        let error = client
            .get(b"k")
            .await
            .expect_err("put rejected while settling");
        assert!(
            matches!(error, ClientError::Rejected { .. }),
            "expected Rejected after pending settled, got {error}"
        );
        assert!(
            !matches!(error, ClientError::Unsettled { .. }),
            "rejection must clear pending, not leave Unsettled"
        );

        fake.clear_fail();
        client.get(b"k").await.expect("slot clear");

        let seen = fake.seen();
        let puts: Vec<_> = seen
            .iter()
            .copied()
            .filter(|(op, _)| *op == "put")
            .collect();
        let gets: Vec<_> = seen
            .iter()
            .copied()
            .filter(|(op, _)| *op == "get")
            .collect();
        assert!(
            puts.iter().all(|(_, seq)| *seq == 1),
            "pending put stays on seq 1 until rejected: {seen:?}"
        );
        assert!(
            gets.iter().any(|(_, seq)| *seq >= 2),
            "later ops must advance past the rejected put: {seen:?}"
        );
        server.abort();
    }

    #[tokio::test]
    async fn durability_internal_is_definitive_not_retried() {
        let (mut client, fake, server) = live_client().await;
        fake.set_fail(Code::Internal, "durability failed: disk full");

        let error = client.put(b"k", b"v").await.expect_err("durability");
        assert!(
            matches!(
                error,
                ClientError::Rejected {
                    op: "put",
                    code: Code::Internal,
                    ..
                }
            ),
            "{error}"
        );
        assert_eq!(
            fake.seen().iter().filter(|(op, _)| *op == "put").count(),
            1,
            "a definitive Internal must not be rotated/retried: {:?}",
            fake.seen()
        );
        server.abort();
    }

    #[test]
    fn definitive_internal_detects_permanent_server_faults() {
        assert!(definitive_internal(&Status::internal(
            "durability failed: x"
        )));
        assert!(definitive_internal(&Status::internal(
            "result does not match request: Noop"
        )));
        assert!(!definitive_internal(&Status::internal("transient glitch")));
    }
}
