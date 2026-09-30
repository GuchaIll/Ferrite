//! gRPC peer transport.
//!
//! One bounded queue and one task per peer. That shape is the whole point: a
//! leader talks to every follower on every heartbeat, and a follower that has
//! crashed, hung, or fallen off the network must not slow the ones that are
//! healthy. Per-peer isolation means a stuck peer fills its own queue and
//! nothing else.
//!
//! When a queue is full the message is **dropped**. Raft is built for a lossy
//! network: a dropped `AppendEntries` is retried on the next heartbeat, and a
//! dropped vote costs one election timeout. Blocking the consensus actor to
//! avoid a drop would trade a cheap, expected failure for an expensive,
//! unexpected one.

use std::collections::BTreeMap;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tonic::transport::{Channel, Endpoint};

use crate::{
    config::{NodeId, RaftPeer},
    proto::raft as pb,
    proto::raft::raft_service_client::RaftServiceClient,
    raft::RaftRpc,
    transport::{Transport, convert},
};

/// Outbound queue depth per peer.
///
/// Deep enough to absorb a burst of catch-up `AppendEntries` while a peer
/// reconnects, shallow enough that a dead peer's backlog stays stale-bounded:
/// there is no value in delivering a heartbeat from ten seconds ago.
const PEER_QUEUE_DEPTH: usize = 256;

/// Reconnect backoff bounds.
const BACKOFF_MIN: Duration = Duration::from_millis(50);
const BACKOFF_MAX: Duration = Duration::from_secs(2);

/// Sends Raft RPCs to peers over gRPC.
pub struct GrpcTransport {
    peers: BTreeMap<NodeId, mpsc::Sender<pb::RaftMessage>>,
}

impl GrpcTransport {
    /// Starts one outbound task per peer and returns the transport handle.
    ///
    /// Tasks are registered in `tasks` rather than detached, so a shutdown can
    /// join them and a panic surfaces instead of vanishing.
    pub fn spawn(
        peers: &BTreeMap<NodeId, RaftPeer>,
        me: NodeId,
        rpc_timeout: Duration,
        tasks: &mut JoinSet<()>,
    ) -> Self {
        let mut senders = BTreeMap::new();

        for (&peer_id, peer) in peers {
            if peer_id == me {
                continue;
            }

            let (tx, rx) = mpsc::channel(PEER_QUEUE_DEPTH);
            senders.insert(peer_id, tx);

            // Lazily connected: a peer that is not up yet must not stop this
            // node from starting. `Endpoint` only parses the address here.
            let endpoint = Endpoint::from_shared(format!("http://{}", peer.addr))
                .map(|e| e.timeout(rpc_timeout).connect_timeout(rpc_timeout));

            match endpoint {
                Ok(endpoint) => {
                    tasks.spawn(peer_loop(peer_id, endpoint, rx));
                }
                Err(error) => {
                    // A malformed peer address is a config bug, not a runtime
                    // condition. Drop the sender so sends to it are no-ops.
                    tracing::error!(peer = peer_id, addr = %peer.addr, %error, "invalid peer endpoint");
                    senders.remove(&peer_id);
                }
            }
        }

        Self { peers: senders }
    }
}

impl Transport for GrpcTransport {
    fn send(&mut self, from: NodeId, to: NodeId, rpc: RaftRpc) {
        let Some(queue) = self.peers.get(&to) else {
            tracing::debug!(peer = to, "send to unknown peer");
            return;
        };

        // Encoding is cheap and synchronous, so the peer task stays a pure
        // shuttle and this stays a non-blocking handoff.
        let message = convert::to_wire(from, rpc);

        // try_send, never send().await: this runs inside the consensus actor's
        // drain, where suspending would stall every other output behind it.
        if let Err(mpsc::error::TrySendError::Full(_)) = queue.try_send(message) {
            tracing::warn!(peer = to, "outbound queue full, dropping RPC");
        }
    }
}

/// Shuttles queued messages to one peer, reconnecting as needed.
///
/// Exits when the queue's sender is dropped, which is how shutdown reaches it.
async fn peer_loop(peer: NodeId, endpoint: Endpoint, mut rx: mpsc::Receiver<pb::RaftMessage>) {
    let mut client: Option<RaftServiceClient<Channel>> = None;
    let mut backoff = BACKOFF_MIN;

    while let Some(message) = rx.recv().await {
        if client.is_none() {
            match endpoint.connect().await {
                Ok(channel) => {
                    tracing::info!(peer, "peer connected");
                    client = Some(RaftServiceClient::new(channel));
                    backoff = BACKOFF_MIN;
                }
                Err(error) => {
                    tracing::debug!(peer, %error, "peer connect failed");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                    // Drop this message rather than hold it: by the time the
                    // peer is reachable, a heartbeat or vote from the past is
                    // worse than nothing.
                    continue;
                }
            }
        }

        if let Some(connected) = client.as_mut()
            && let Err(status) = connected.deliver(message).await
        {
            // Force a reconnect on the next message; the channel may be dead.
            tracing::debug!(peer, %status, "deliver failed");
            client = None;
        }
    }

    tracing::debug!(peer, "peer task stopping");
}
