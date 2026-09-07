//! Public Raft node API.
use std::{collections::HashMap, time::Duration};

use crate::config::NodeId;

pub mod election;
pub mod log;
pub mod replication;
pub mod snapshot;
pub mod state;
pub mod storage;

pub use log::LogEntry;
use state::RaftState;
use crate::sim::Input;
use crate::sim::Output;
use crate::sim::SimNode;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteRequest {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftRpc {
    RequestVote(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntries(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardState {
    pub current_term: u64,
    pub voted_for: Option<NodeId>
}
// TODO: `Storage`, `Transport`, and `KVStateMachine` are minimal
// placeholders so `RaftNode` compiles. Replace them with real designs
// before this module leaves phase 2 — see the architecture rules in
// AGENTS.md (type-state node lifecycle, storage/transport decoupled from
// consensus logic via an async channel in the driver, never in `src/raft/`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Storage;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Transport;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KVStateMachine;



// /// Work requested by [`RaftNode::step`], to be drained by its driver in order.
// ///
// /// The ordering is a contract, not an implementation detail: `Persist` must
// /// be drained before any `Send` that depends on it, since Raft requires a
// /// node to persist `currentTerm`/`votedFor` before responding to an RPC. A
// /// driver that reorders this for throughput introduces a data-loss bug.
// #[derive(Debug, Clone, PartialEq, Eq)]
// pub enum Output {
//     Send { to: NodeId, rpc: RaftRpc },
//     Apply(LogEntry),
//     Persist(HardState),
// }

#[derive(Debug, PartialEq, Clone)]
pub struct RaftNode {
    // Node identifier and cluster membership.
    id: NodeId,
    peers: Vec<NodeId>,

    // Persistent state on all servers, updated before responding to RPCs.
    log: Vec<LogEntry>,
    current_term: u64,
    voted_for: Option<NodeId>,

    // Volatile state on all servers.
    state: RaftState,
    leader_id: Option<NodeId>,

    commit_index: u64,
    last_applied: u64,

    // Volatile state on leaders only, reinitialized after election.
    next_index: HashMap<NodeId, u64>,
    match_index: HashMap<NodeId, u64>,

    // Components per Raft node.
    storage: Storage,
    transport: Transport,
    state_machine: KVStateMachine,

    // Timing parameters.
    election_timeout: Duration,
    heartbeat_interval: Duration,
}

impl SimNode for RaftNode {
    fn id(&self) -> NodeId {
        self.id
    }

    /// Handles one driver input. Only `Tick` is implemented — election,
    /// `AppendEntries`, and client-command handling land in later issues.
    fn step(&mut self, input: Input) -> Vec<Output> {
        match input {
            Input::Tick => Vec::new(),
            Input::Message { .. } | Input::ClientCommand(_) => Vec::new(),
        }
    }
}

impl RaftNode {
    pub fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        storage: Storage,
        transport: Transport,
        state_machine: KVStateMachine,
    ) -> RaftNode {
        RaftNode {
            id,
            peers,
            log: Vec::new(),
            current_term: 0,
            voted_for: None,
            state: RaftState::Follower,
            leader_id: None,
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            storage,
            transport,
            state_machine,
            election_timeout: Duration::from_millis(500),
            heartbeat_interval: Duration::from_millis(200),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: NodeId) -> RaftNode {
        RaftNode::new(id, vec![2, 3], Storage, Transport, KVStateMachine)
    }

    #[test]
    fn id_returns_the_configured_node_id() {
        assert_eq!(node(1).id(), 1);
    }

    #[test]
    fn tick_does_not_panic_and_returns_no_output() {
        let mut node = node(1);

        assert_eq!(node.step(Input::Tick), Vec::new());
    }

    #[test]
    fn message_and_client_command_are_unimplemented_and_return_no_output() {
        let mut node = node(1);

        assert_eq!(
            node.step(Input::Message {
                from: 2,
                rpc: RaftRpc::RequestVote(RequestVoteRequest {
                    term: 1,
                    candidate_id: 2,
                    last_log_index: 0,
                    last_log_term: 0,
                }),
            }),
            Vec::new()
        );
        assert_eq!(node.step(Input::ClientCommand(vec![1])), Vec::new());
    }
}
