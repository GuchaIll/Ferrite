//! Raft transport abstraction.

use crate::config::NodeId;
use crate::error::TransportError;
use crate::raft::log::LogEntry;

pub mod grpc;
pub mod simulated;

/// `AppendEntries` RPC arguments, sent by a leader to a follower.
#[derive(Debug, Clone)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

/// `AppendEntries` RPC result, returned by a follower to the leader.
#[derive(Debug, Clone)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
}

/// Sends Raft RPCs to peer nodes. Implementations must not block the async
/// executor; network I/O belongs behind `tokio::*`, never `std::net`.
pub trait Transport {
    async fn append_entries(
        &self,
        to: NodeId,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, TransportError>;
}
