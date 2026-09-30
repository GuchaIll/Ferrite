//! gRPC key-value service: the client-facing edge of a node.
//!
//! Handlers never touch the state machine. They propose through the node actor
//! and wait for their own entry to apply, so every response is the result of a
//! committed command.

// `tonic::Status` is large; every handler returns it, and boxing it here would
// only be unboxed again at the tonic boundary.
#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use prost::Message;
use tokio::sync::broadcast::error::RecvError;
use tonic::{Code, Request, Response, Status};

use crate::config::NodeId;
use crate::kv::{CasOutcome, ClientMeta, ClientRequest, Command, CommandResult};
use crate::proto::kv as pb;
use crate::proto::kv::kv_server::Kv;
use crate::server::{Applied, NodeError, NodeHandle};

/// Server-side cap on waiting for an apply. The client's deadline normally fires first.
const APPLY_WAIT: Duration = Duration::from_secs(5);

pub struct KvServiceImpl {
    handle: NodeHandle,
    kv_advertise: BTreeMap<NodeId, SocketAddr>,
}

impl KvServiceImpl {
    pub fn new(handle: NodeHandle, kv_advertise: BTreeMap<NodeId, SocketAddr>) -> Self {
        Self {
            handle,
            kv_advertise,
        }
    }

    /// Proposes `command` and waits for the entry at its own (index, term) to apply.
    async fn submit(
        &self,
        meta: Option<pb::ClientMeta>,
        command: Command,
    ) -> Result<CommandResult, Status> {
        let meta =
            meta.ok_or_else(|| Status::invalid_argument("request is missing client meta"))?;
        let expected = ClientMeta {
            client_id: meta.client_id,
            seq_num: meta.seq_num,
        };
        let bytes = ClientRequest::new(meta.client_id, meta.seq_num, command)
            .encode()
            .map_err(|e| Status::internal(e.to_string()))?;

        // Before propose: a fast commit could otherwise apply before we listen.
        let mut applied = self.handle.subscribe_apply();
        let proposed = self
            .handle
            .propose(bytes)
            .await
            .map_err(|e| node_error(e, &self.kv_advertise))?;

        let deadline = tokio::time::Instant::now() + APPLY_WAIT;
        loop {
            let next = tokio::time::timeout_at(deadline, applied.recv())
                .await
                .map_err(|_| Status::deadline_exceeded("timed out waiting for commit; retry"))?;

            match next {
                Ok(entry) => {
                    match classify_apply(proposed.index, proposed.term, expected, &entry) {
                        ApplyOutcome::Earlier => continue,
                        ApplyOutcome::Ours(result) => return Ok(result),
                        // A later leader wrote a different entry at our index. That
                        // entry just applied here, so this node already knows who it is.
                        ApplyOutcome::Overwritten => {
                            let hint = self.handle.role().borrow().leader_hint;
                            return Err(not_leader(hint, &self.kv_advertise));
                        }
                        // Our index was covered by an installed snapshot, which does
                        // not broadcast. Outcome unknown; a same-seq retry hits dedup.
                        ApplyOutcome::Past => {
                            return Err(Status::unavailable("commit outcome unknown; retry"));
                        }
                        // Same index/term but wrong client: driver bug, fail closed.
                        ApplyOutcome::IdentityMismatch => {
                            return Err(Status::internal("applied entry identity mismatch"));
                        }
                    }
                }
                Err(RecvError::Lagged(_)) => {
                    return Err(Status::unavailable("apply stream fell behind; retry"));
                }
                Err(RecvError::Closed) => {
                    return Err(Status::unavailable("node is shutting down"));
                }
            }
        }
    }
}

/// How an applied entry relates to the proposal this RPC is waiting on.
#[derive(Debug, PartialEq, Eq)]
enum ApplyOutcome {
    /// Still catching up; keep waiting.
    Earlier,
    /// This is our committed command.
    Ours(CommandResult),
    /// Same index, different term: the log was overwritten.
    Overwritten,
    /// Applied past our index without seeing it (e.g. snapshot).
    Past,
    /// Same index and term, but not our `(client_id, seq_num)`.
    IdentityMismatch,
}

/// Decides whether an applied entry finishes, fails, or continues the wait.
///
/// Never returns another command's result: identity is `(index, term)` plus the
/// client meta stamped into the log entry.
fn classify_apply(
    proposed_index: u64,
    proposed_term: u64,
    expected: ClientMeta,
    entry: &Applied,
) -> ApplyOutcome {
    if entry.index < proposed_index {
        return ApplyOutcome::Earlier;
    }
    if entry.index > proposed_index {
        return ApplyOutcome::Past;
    }
    // entry.index == proposed_index
    if entry.term != proposed_term {
        return ApplyOutcome::Overwritten;
    }
    match entry.client {
        Some(client) if client == expected => ApplyOutcome::Ours(entry.result.clone()),
        _ => ApplyOutcome::IdentityMismatch,
    }
}

#[tonic::async_trait]
impl Kv for KvServiceImpl {
    async fn get(
        &self,
        request: Request<pb::GetRequest>,
    ) -> Result<Response<pb::GetResponse>, Status> {
        let pb::GetRequest { meta, key } = request.into_inner();
        let result = self.submit(meta, Command::Get { key }).await?;
        get_response(result).map(Response::new)
    }

    async fn put(
        &self,
        request: Request<pb::PutRequest>,
    ) -> Result<Response<pb::PutResponse>, Status> {
        let pb::PutRequest { meta, key, value } = request.into_inner();
        let result = self.submit(meta, Command::Set { key, value }).await?;
        put_response(result).map(Response::new)
    }

    async fn delete(
        &self,
        request: Request<pb::DeleteRequest>,
    ) -> Result<Response<pb::DeleteResponse>, Status> {
        let pb::DeleteRequest { meta, key } = request.into_inner();
        let result = self.submit(meta, Command::Delete { key }).await?;
        delete_response(result).map(Response::new)
    }

    async fn cas(
        &self,
        request: Request<pb::CasRequest>,
    ) -> Result<Response<pb::CasResponse>, Status> {
        let pb::CasRequest {
            meta,
            key,
            expected_version,
            value,
        } = request.into_inner();
        let result = self
            .submit(
                meta,
                Command::Cas {
                    key,
                    expected_version,
                    value,
                },
            )
            .await?;
        cas_response(result).map(Response::new)
    }
}

/// Maps a proposal failure to a status.
fn node_error(error: NodeError, kv_advertise: &BTreeMap<NodeId, SocketAddr>) -> Status {
    match error {
        NodeError::NotLeader { leader_hint } => not_leader(leader_hint, kv_advertise),
        NodeError::Shutdown => Status::unavailable("node is shutting down"),
        NodeError::Durability(reason) => Status::internal(format!("durability failed: {reason}")),
    }
}

/// `FAILED_PRECONDITION` carrying a [`pb::NotLeader`] detail. The hint is the
/// leader's KV address: its Raft address is useless to a client. The message
/// repeats it for humans; clients read only the detail.
fn not_leader(leader_hint: Option<NodeId>, kv_advertise: &BTreeMap<NodeId, SocketAddr>) -> Status {
    let addr = leader_hint.and_then(|id| kv_advertise.get(&id));
    let message = match addr {
        Some(addr) => format!("not leader; try {addr}"),
        None => "not leader; leader unknown".to_owned(),
    };
    let detail = pb::NotLeader {
        leader_hint: addr.map(ToString::to_string).unwrap_or_default(),
    };
    Status::with_details(
        Code::FailedPrecondition,
        message,
        detail.encode_to_vec().into(),
    )
}

fn get_response(result: CommandResult) -> Result<pb::GetResponse, Status> {
    match result {
        CommandResult::Get { value, version } => Ok(pb::GetResponse { value, version }),
        other => Err(unexpected(other)),
    }
}

fn put_response(result: CommandResult) -> Result<pb::PutResponse, Status> {
    match result {
        CommandResult::Set { previous, version } => Ok(pb::PutResponse { previous, version }),
        other => Err(unexpected(other)),
    }
}

fn delete_response(result: CommandResult) -> Result<pb::DeleteResponse, Status> {
    match result {
        CommandResult::Delete { previous } => Ok(pb::DeleteResponse { previous }),
        other => Err(unexpected(other)),
    }
}

fn cas_response(result: CommandResult) -> Result<pb::CasResponse, Status> {
    match result {
        CommandResult::Cas {
            outcome: CasOutcome::Applied { previous, version },
        } => Ok(pb::CasResponse {
            applied: true,
            previous,
            version,
            actual_version: None,
            actual_value: None,
        }),
        CommandResult::Cas {
            outcome:
                CasOutcome::Failed {
                    actual_version,
                    actual_value,
                },
        } => Ok(pb::CasResponse {
            applied: false,
            previous: None,
            version: None,
            actual_version,
            actual_value,
        }),
        other => Err(unexpected(other)),
    }
}

/// `Noop` for a client entry means the dedup table judged the seq stale: the
/// client already moved past it, so there is no result to return.
fn unexpected(result: CommandResult) -> Status {
    match result {
        CommandResult::Noop => {
            Status::invalid_argument("stale seq_num: client already moved past it")
        }
        other => Status::internal(format!("result does not match request: {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn advertise() -> BTreeMap<NodeId, SocketAddr> {
        BTreeMap::from([(2, "127.0.0.1:9002".parse().expect("addr"))])
    }

    fn detail(status: &Status) -> pb::NotLeader {
        pb::NotLeader::decode(status.details()).expect("NotLeader detail")
    }

    fn applied(
        index: u64,
        term: u64,
        client: Option<ClientMeta>,
        result: CommandResult,
    ) -> Applied {
        Applied {
            index,
            term,
            client,
            result,
        }
    }

    fn us() -> ClientMeta {
        ClientMeta {
            client_id: 7,
            seq_num: 1,
        }
    }

    fn them() -> ClientMeta {
        ClientMeta {
            client_id: 9,
            seq_num: 3,
        }
    }

    #[test]
    fn not_leader_carries_the_leaders_kv_address_as_a_detail() {
        let status = node_error(
            NodeError::NotLeader {
                leader_hint: Some(2),
            },
            &advertise(),
        );
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert_eq!(detail(&status).leader_hint, "127.0.0.1:9002");
    }

    #[test]
    fn an_unadvertised_hint_is_reported_as_unknown() {
        let status = not_leader(Some(9), &advertise());
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert_eq!(detail(&status).leader_hint, "");
    }

    #[test]
    fn failed_cas_carries_what_blocked_it() {
        let response = cas_response(CommandResult::Cas {
            outcome: CasOutcome::Failed {
                actual_version: Some(3),
                actual_value: Some(b"v".to_vec()),
            },
        })
        .expect("cas result");
        assert!(!response.applied);
        assert_eq!(response.actual_version, Some(3));
        assert_eq!(response.actual_value.as_deref(), Some(b"v".as_slice()));
    }

    #[test]
    fn a_stale_seq_is_a_client_error_not_an_empty_success() {
        let status = put_response(CommandResult::Noop).expect_err("noop is not a put result");
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[test]
    fn a_mismatched_result_variant_is_internal() {
        let status =
            get_response(CommandResult::Delete { previous: None }).expect_err("wrong variant");
        assert_eq!(status.code(), Code::Internal);
    }

    #[test]
    fn classify_skips_earlier_applies() {
        let entry = applied(4, 1, Some(us()), CommandResult::Noop);
        assert_eq!(classify_apply(5, 2, us(), &entry), ApplyOutcome::Earlier);
    }

    #[test]
    fn classify_accepts_our_index_term_and_client() {
        let result = CommandResult::Set {
            previous: None,
            version: 1,
        };
        let entry = applied(5, 2, Some(us()), result.clone());
        assert_eq!(
            classify_apply(5, 2, us(), &entry),
            ApplyOutcome::Ours(result)
        );
    }

    #[test]
    fn classify_treats_same_index_different_term_as_overwrite() {
        // Foreign payload must never be returned.
        let foreign = CommandResult::Set {
            previous: None,
            version: 99,
        };
        let entry = applied(5, 3, Some(them()), foreign);
        assert_eq!(
            classify_apply(5, 2, us(), &entry),
            ApplyOutcome::Overwritten
        );
    }

    #[test]
    fn classify_rejects_same_slot_with_wrong_client() {
        let entry = applied(
            5,
            2,
            Some(them()),
            CommandResult::Set {
                previous: None,
                version: 1,
            },
        );
        assert_eq!(
            classify_apply(5, 2, us(), &entry),
            ApplyOutcome::IdentityMismatch
        );
    }

    #[test]
    fn classify_rejects_same_slot_with_missing_client() {
        let entry = applied(5, 2, None, CommandResult::Noop);
        assert_eq!(
            classify_apply(5, 2, us(), &entry),
            ApplyOutcome::IdentityMismatch
        );
    }

    #[test]
    fn classify_marks_past_index_unknown() {
        let entry = applied(6, 2, Some(us()), CommandResult::Noop);
        assert_eq!(classify_apply(5, 2, us(), &entry), ApplyOutcome::Past);
    }
}
