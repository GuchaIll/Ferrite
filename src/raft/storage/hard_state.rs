//! Durable Raft election metadata.

use crate::config::NodeId;

/// Persistent state required to preserve Raft election safety across restarts.
///
/// Storage must write this state before the node sends a response that relies
/// on its term or recorded vote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardState {
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
}
