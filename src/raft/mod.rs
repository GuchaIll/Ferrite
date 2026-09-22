//! Public Raft node API.
use std::collections::HashMap;

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

use crate::config::NodeId;

pub mod election;
pub mod invariant;
pub mod log;
pub mod replication;
pub mod snapshot;
pub mod state;
pub mod storage;

pub use election::ElectionState;
pub use log::{LogEntry, RaftLog};
use state::RaftState;
pub use snapshot::{Snapshot, SnapshotMeta};
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
    /// Last log index the responder actually acknowledged (0 on rejection).
    pub match_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallSnapshotRequest {
    pub term: u64,
    pub leader_id: NodeId,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub offset: u64,
    pub data: Vec<u8>,
    pub done: bool,
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallSnapshotResponse {
    pub term: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftRpc {
    RequestVote(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntries(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
    InstallSnapshot(InstallSnapshotRequest),
    InstallSnapshotResponse(InstallSnapshotResponse),
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

    pub(crate) compaction_threshold: u64,
    pub(crate) snapshot: Option<snapshot::Snapshot>,
    pub(crate) pending_snapshot: Option<snapshot::PendingSnapshot>,
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

    /// Rebuilds a node after a restart from its durable state alone.
    ///
    /// Only `currentTerm`, `votedFor`, the log, and the snapshot come back.
    /// Role, leader hint, and leader bookkeeping reset; `commit_index` and
    /// `last_applied` restart at the snapshot boundary. `rng` should be a
    /// fresh per-node stream: a reboot does not resume the old jitter sequence.
    pub fn recover(
        id: NodeId,
        peers: Vec<NodeId>,
        rng: ChaCha8Rng,
        recovered: storage::Recovered,
    ) -> RaftNode {
        let mut node = Self::with_rng(id, peers, rng);
        let applied = recovered.log.last_included_index();
        node.current_term = recovered.hard_state.current_term;
        node.voted_for = recovered.hard_state.voted_for;
        node.log = recovered.log;
        node.snapshot = recovered.snapshot;
        node.commit_index = applied;
        node.last_applied = applied;
        node
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
            // 0 disables automatic snapshot requests in unit tests without a driver SM.
            compaction_threshold: 0,
            snapshot: None,
            pending_snapshot: None,
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
            let mut actions = Vec::new();
            if snapshot::should_snapshot(self) {
                actions.push(snapshot::request_snapshot_action(self));
            }

            self.heartbeat_ticks = self.heartbeat_ticks.saturating_sub(1);
            if self.heartbeat_ticks == 0 {
                self.heartbeat_ticks = self.heartbeat_interval_ticks;
                actions.extend(replication::heartbeat_actions(self));
                return actions;
            }

            // Opportunistic catch-up when a follower is behind between heartbeats.
            // Prefer InstallSnapshot when nextIndex cannot be served from the log.
            for peer in self.peers.iter().copied().filter(|&peer| peer != self.id) {
                let next = self.next_index.get(&peer).copied().unwrap_or(1);
                if next <= self.log.last_included_index() {
                    actions.extend(snapshot::send_install_snapshot(self, peer));
                } else if self.log.last_index() >= next {
                    actions.push(replication::send_append_entries(self, peer));
                }
            }
            return actions;
        }
        election::on_tick(self)
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
                replication::handle_append_entries_request(self, from, req)
            }
            RaftRpc::AppendEntriesResponse(res) => {
                replication::handle_append_entries_response(self, from, res)
            }
            RaftRpc::InstallSnapshot(req) => {
                snapshot::handle_install_snapshot_request(self, from, req)
            }
            RaftRpc::InstallSnapshotResponse(res) => {
                snapshot::handle_install_snapshot_response(self, from, res)
            }
        }
    }

    /// Handles a serialized client command delivered by a runtime adapter.
    pub(crate) fn handle_client_command(
        &mut self,
        command: Vec<u8>,
    ) -> Vec<election::ElectionAction> {
        replication::append_and_replicate(self, command)
    }

    /// Handles snapshot bytes produced by the driver after [`ElectionAction::RequestSnapshot`].
    pub(crate) fn handle_snapshot_taken(
        &mut self,
        snapshot: snapshot::Snapshot,
    ) -> Vec<election::ElectionAction> {
        snapshot::on_snapshot_taken(self, snapshot)
    }

    /// Handles driver confirmation that a snapshot is durable (then trims the log).
    pub(crate) fn handle_snapshot_persisted(
        &mut self,
        meta: snapshot::SnapshotMeta,
    ) -> Vec<election::ElectionAction> {
        snapshot::on_snapshot_persisted(self, meta)
    }

    /// Returns a shared view of the in-memory Raft log (sim/tests).
    pub fn log(&self) -> &RaftLog {
        &self.log
    }
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
    fn recover_restores_durable_state_and_resets_volatile_state() {
        let mut log = RaftLog::new();
        log.install_snapshot(2, 1);
        log.append(LogEntry::new(3, 2, b"c".to_vec())).unwrap();
        let snapshot = Snapshot::new(SnapshotMeta::new(2, 1), b"sm".to_vec());

        let node = RaftNode::recover(
            1,
            vec![2, 3],
            ChaCha8Rng::seed_from_u64(1),
            storage::Recovered {
                hard_state: HardState {
                    current_term: 4,
                    voted_for: Some(3),
                },
                log: log.clone(),
                snapshot: Some(snapshot.clone()),
            },
        );

        assert_eq!(node.current_term, 4);
        assert_eq!(node.voted_for, Some(3));
        assert_eq!(node.log, log);
        assert_eq!(node.snapshot, Some(snapshot));
        assert_eq!(node.state, RaftState::Follower);
        assert_eq!(node.leader_id, None);
        // Everything up to the snapshot is applied; nothing past it is known committed.
        assert_eq!(node.commit_index, 2);
        assert_eq!(node.last_applied, 2);
        assert!(node.next_index.is_empty() && node.match_index.is_empty());
        assert!(node.pending_snapshot.is_none());
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
