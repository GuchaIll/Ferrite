//! Public Raft node API.
use std::collections::HashMap;

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

use crate::config::NodeId;

pub mod election;
pub mod log;
pub mod replication;
pub mod snapshot;
pub mod state;
pub mod storage;

pub use election::ElectionState;
pub use log::{LogEntry, RaftLog};
use state::RaftState;
pub use storage::HardState;

const DEFAULT_ELECTION_TIMEOUT_TICKS: u64 = 500;
const DEFAULT_HEARTBEAT_TICKS: u64 = 50;
/// Default jitter window used by [`RaftNode::with_rng`].
const DEFAULT_ELECTION_TIMEOUT_MIN_TICKS: u64 = 150;
const DEFAULT_ELECTION_TIMEOUT_MAX_TICKS: u64 = 300;

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

#[derive(Debug, Clone)]
pub struct RaftNode {
    // Node identifier and cluster membership.
    pub(crate) id: NodeId,
    pub(crate) peers: Vec<NodeId>,

    // Persistent state on all servers, updated before responding to RPCs.
    pub(crate) log: RaftLog,
    pub(crate) current_term: u64,
    pub(crate) voted_for: Option<NodeId>,

    // Volatile state on all servers.
    pub(crate) state: RaftState,
    pub(crate) leader_id: Option<NodeId>,

    pub(crate) commit_index: u64,
    pub(crate) last_applied: u64,

    // Volatile state on leaders only, reinitialized after election.
    pub(crate) next_index: HashMap<NodeId, u64>,
    pub(crate) match_index: HashMap<NodeId, u64>,

    // Timing parameters (logical ticks).
    pub(crate) heartbeat_interval_ticks: u64,
    pub(crate) election_timeout_min_ticks: u64,
    pub(crate) election_timeout_max_ticks: u64,

    pub(crate) election: ElectionState,
    pub(crate) heartbeat_ticks: u64,

    /// Seeded PRNG for election timeout jitter (deterministic under sim seeds).
    pub(crate) rng: ChaCha8Rng,
}

impl PartialEq for RaftNode {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.peers == other.peers
            && self.log == other.log
            && self.current_term == other.current_term
            && self.voted_for == other.voted_for
            && self.state == other.state
            && self.leader_id == other.leader_id
            && self.commit_index == other.commit_index
            && self.last_applied == other.last_applied
            && self.next_index == other.next_index
            && self.match_index == other.match_index
            && self.election == other.election
            && self.heartbeat_ticks == other.heartbeat_ticks
            && self.election_timeout_min_ticks == other.election_timeout_min_ticks
            && self.election_timeout_max_ticks == other.election_timeout_max_ticks
            && self.heartbeat_interval_ticks == other.heartbeat_interval_ticks
    }
}

impl RaftNode {
    /// Creates a Raft state machine without binding it to a runtime.
    ///
    /// Uses a fixed election timeout (no jitter) so unit tests stay simple.
    /// Persistence, transport, and application effects are driven by the
    /// runtime boundary after Raft returns them; they do not live in the core.
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> RaftNode {
        Self::new_with_timeouts(
            id,
            peers,
            ChaCha8Rng::seed_from_u64(id),
            DEFAULT_ELECTION_TIMEOUT_TICKS,
            DEFAULT_ELECTION_TIMEOUT_TICKS,
            DEFAULT_HEARTBEAT_TICKS,
        )
    }

    /// Creates a node with a seeded PRNG and randomized election timeouts.
    ///
    /// Used by the deterministic simulator so the same root seed replays the
    /// same timeout sequence.
    pub fn with_rng(id: NodeId, peers: Vec<NodeId>, rng: ChaCha8Rng) -> RaftNode {
        Self::new_with_timeouts(
            id,
            peers,
            rng,
            DEFAULT_ELECTION_TIMEOUT_MIN_TICKS,
            DEFAULT_ELECTION_TIMEOUT_MAX_TICKS,
            DEFAULT_HEARTBEAT_TICKS,
        )
    }

    fn new_with_timeouts(
        id: NodeId,
        peers: Vec<NodeId>,
        mut rng: ChaCha8Rng,
        timeout_min: u64,
        timeout_max: u64,
        heartbeat_ticks: u64,
    ) -> RaftNode {
        let mut election = ElectionState::new(timeout_min.max(1));
        election::reset_timeout(&mut election, timeout_min..=timeout_max, &mut rng);

        RaftNode {
            id,
            peers,
            log: RaftLog::new(),
            current_term: 0,
            voted_for: None,
            state: RaftState::Follower,
            leader_id: None,
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            heartbeat_interval_ticks: heartbeat_ticks,
            election_timeout_min_ticks: timeout_min,
            election_timeout_max_ticks: timeout_max,
            election,
            heartbeat_ticks: 0,
            rng,
        }
    }

    /// Returns this node's stable Raft identity.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Returns the current Raft role.
    pub fn state(&self) -> RaftState {
        self.state
    }

    /// Returns the current term.
    pub fn current_term(&self) -> u64 {
        self.current_term
    }

    /// Advances the protocol's logical clock by one driver tick.
    /// Returns ordered effects for the runtime.
    pub(crate) fn on_tick(&mut self) -> Vec<election::ElectionAction> {
        if self.state == RaftState::Leader {
            self.heartbeat_ticks = self.heartbeat_ticks.saturating_sub(1);
            if self.heartbeat_ticks == 0 {
                let actions = self.heartbeat_actions();
                self.heartbeat_ticks = self.heartbeat_interval_ticks;
                return actions;
            }
            return Vec::new();
        }
        election::on_tick(self)
    }

    fn heartbeat_actions(&self) -> Vec<election::ElectionAction> {
        let prev = self.log.last_index();
        let prev_term = self.log.last_term();

        self.peers
            .iter()
            .filter(|&&peer| peer != self.id)
            .map(|&to| election::ElectionAction::Send {
                to,
                rpc: RaftRpc::AppendEntries(AppendEntriesRequest {
                    term: self.current_term,
                    leader_id: self.id,
                    prev_log_index: prev,
                    prev_log_term: prev_term,
                    entries: Vec::new(),
                    leader_commit: self.commit_index,
                }),
            })
            .collect()
    }

    /// Handles an RPC delivered by a runtime adapter.
    ///
    /// Returns ordered effects for the runtime (`Persist` before dependent `Send`).
    pub(crate) fn handle_rpc(
        &mut self,
        from: NodeId,
        rpc: RaftRpc,
    ) -> Vec<election::ElectionAction> {
        match rpc {
            RaftRpc::RequestVote(req) => election::handle_request_vote_request(self, from, req),
            RaftRpc::RequestVoteResponse(res) => {
                election::handle_request_vote_response(self, from, res)
            }
            RaftRpc::AppendEntries(req) => {
                election::handle_append_entries_request(self, from, req)
            }
            RaftRpc::AppendEntriesResponse(res) => election::observe_term(self, res.term),
        }
    }

    /// Handles a serialized client command delivered by a runtime adapter.
    pub(crate) fn handle_client_command(&mut self, _command: Vec<u8>) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: NodeId) -> RaftNode {
        RaftNode::new(id, vec![2, 3])
    }

    #[test]
    fn id_returns_the_configured_node_id() {
        assert_eq!(node(1).id(), 1);
    }

    #[test]
    fn tick_does_not_panic() {
        let mut node = node(1);
        node.on_tick();
    }

    #[test]
    fn rpc_and_client_command_are_unimplemented_but_safe() {
        let mut node = node(1);

        let _ = node.handle_rpc(
            2,
            RaftRpc::RequestVote(RequestVoteRequest {
                term: 1,
                candidate_id: 2,
                last_log_index: 0,
                last_log_term: 0,
            }),
        );
        node.handle_client_command(vec![1]);
    }
}
