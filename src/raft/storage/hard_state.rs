//! Durable Raft election metadata.

use crate::config::NodeId;
use serde::{Deserialize, Serialize};

/// Persistent state required to preserve Raft election safety across restarts.
///
/// Storage must write this state before the node sends a response that relies
/// on its term or recorded vote.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardState {
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
}
