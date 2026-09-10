//! Raft election state transitions.
//!
//! This module owns volatile election state. Durable term and vote metadata
//! remain in [`crate::raft::storage::HardState`].

use std::collections::BTreeSet;
use std::ops::RangeInclusive;

use rand::{Rng, RngCore};

use crate::{
    config::NodeId,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, RaftNode, RaftRpc, RequestVoteRequest,
        RequestVoteResponse, state::RaftState, storage::HardState,
    },
};

/// Effects that election handling will request from the Raft node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElectionAction {
    Persist(HardState),
    Send { to: NodeId, rpc: RaftRpc },
    PromoteLeader,
    DemoteFollower,
}

/// Volatile election state for one Raft node.
///
/// `ticks_remaining` always resets to `election_timeout_ticks`; selection of
/// a new timeout belongs to `RaftNode`, which owns the configured policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectionState {
    ticks_remaining: u64,
    votes_granted: BTreeSet<NodeId>,
    election_timeout_ticks: u64,
}

impl ElectionState {
    pub fn new(election_timeout_ticks: u64) -> Self {
        Self {
            ticks_remaining: election_timeout_ticks,
            votes_granted: BTreeSet::new(),
            election_timeout_ticks,
        }
    }

    /// Replaces the node's selected election timeout and restarts its timer.
    pub fn set_election_timeout(&mut self, election_timeout_ticks: u64) {
        self.election_timeout_ticks = election_timeout_ticks;
        self.ticks_remaining = election_timeout_ticks;
    }

    /// Restarts the countdown from the node's currently selected timeout.
    pub fn reset(&mut self) {
        self.ticks_remaining = self.election_timeout_ticks;
    }

    /// Decrements the countdown and reports whether an election is due.
    pub(crate) fn tick(&mut self) -> bool {
        self.ticks_remaining = self.ticks_remaining.saturating_sub(1);
        self.ticks_remaining == 0
    }
}

/// Selects a timeout from the configured range and restarts the countdown.
///
/// `RaftNode` decides when a new timeout is selected. Once selected, normal
/// timer resets use [`ElectionState::reset`] and do not sample again.
pub(crate) fn reset_timeout(
    state: &mut ElectionState,
    range: RangeInclusive<u64>,
    rng: &mut impl RngCore,
) {
    let lo = *range.start();
    let hi = *range.end();
    // Inclusive range; gen_range needs lo < hi or lo..=lo style.
    let timeout = if lo >= hi {
        lo
    } else {
        rng.gen_range(lo..=hi)
    };
    state.set_election_timeout(timeout);
}

/// Advances election handling after a driver tick.
pub(crate) fn on_tick(node: &mut RaftNode) -> Vec<ElectionAction> {
    if node.state == RaftState::Leader {
        return Vec::new();
    }
    if node.election.tick() {
        return start_election(node);
    }
    Vec::new()
}

/// Raft §5.4.1 — candidate is at least as up-to-date as the voter.
fn is_log_up_to_date(
    candidate_last_term: u64,
    candidate_last_index: u64,
    voter_last_term: u64,
    voter_last_index: u64,
) -> bool {
    candidate_last_term > voter_last_term
        || (candidate_last_term == voter_last_term && candidate_last_index >= voter_last_index)
}

/// Handles an incoming RequestVote RPC.
///
/// Effects are ordered so `Persist` precedes the dependent `Send` reply.
pub(crate) fn handle_request_vote_request(
    node: &mut RaftNode,
    from: NodeId,
    request: RequestVoteRequest,
) -> Vec<ElectionAction> {
    let mut actions = Vec::new();

    if request.term > node.current_term {
        become_follower(node, request.term);
        actions.push(persist_hard_state(node));
    }

    if request.term < node.current_term {
        actions.push(ElectionAction::Send {
            to: from,
            rpc: RaftRpc::RequestVoteResponse(RequestVoteResponse {
                term: node.current_term,
                vote_granted: false,
            }),
        });
        return actions;
    }

    let can_vote = match node.voted_for {
        None => true,
        Some(id) => id == request.candidate_id,
    };
    let log_ok = is_log_up_to_date(
        request.last_log_term,
        request.last_log_index,
        node.log.last_term(),
        node.log.last_index(),
    );

    let vote_granted = can_vote && log_ok;
    if vote_granted {
        node.voted_for = Some(request.candidate_id);
        node.election.reset();
        actions.retain(|a| !matches!(a, ElectionAction::Persist(_)));
        actions.push(persist_hard_state(node));
    }

    actions.push(ElectionAction::Send {
        to: from,
        rpc: RaftRpc::RequestVoteResponse(RequestVoteResponse {
            term: node.current_term,
            vote_granted,
        }),
    });

    actions
}

fn persist_hard_state(node: &RaftNode) -> ElectionAction {
    ElectionAction::Persist(HardState {
        current_term: node.current_term,
        voted_for: node.voted_for,
    })
}

/// Handles a vote response received while this node is a candidate.
pub(crate) fn handle_request_vote_response(
    node: &mut RaftNode,
    from: NodeId,
    response: RequestVoteResponse,
) -> Vec<ElectionAction> {
    if response.term > node.current_term {
        become_follower(node, response.term);
        return vec![
            ElectionAction::DemoteFollower,
            persist_hard_state(node),
        ];
    }

    if node.state == RaftState::Candidate
        && response.term == node.current_term
        && response.vote_granted
    {
        return tally_granted_vote(node, from);
    }

    Vec::new()
}

/// Minimal AppendEntries handling sufficient for election stability.
///
/// Heartbeats reset the election timer and establish leader identity. Log
/// matching / entry append remain out of scope for issue #9.
pub(crate) fn handle_append_entries_request(
    node: &mut RaftNode,
    from: NodeId,
    request: AppendEntriesRequest,
) -> Vec<ElectionAction> {
    let mut actions = Vec::new();

    if request.term > node.current_term {
        become_follower(node, request.term);
        actions.push(persist_hard_state(node));
    }

    if request.term < node.current_term {
        actions.push(ElectionAction::Send {
            to: from,
            rpc: RaftRpc::AppendEntriesResponse(AppendEntriesResponse {
                term: node.current_term,
                success: false,
            }),
        });
        return actions;
    }

    // Same term: accept leader authority and suppress elections.
    if node.state != RaftState::Follower {
        node.state = RaftState::Follower;
        actions.push(ElectionAction::DemoteFollower);
    }
    node.leader_id = Some(request.leader_id);
    node.election.reset();

    actions.push(ElectionAction::Send {
        to: from,
        rpc: RaftRpc::AppendEntriesResponse(AppendEntriesResponse {
            term: node.current_term,
            success: true,
        }),
    });

    actions
}

pub(crate) fn become_follower(node: &mut RaftNode, term: u64) {
    node.current_term = term;
    node.voted_for = None;
    node.state = RaftState::Follower;
    node.leader_id = None;
    node.election.reset();
}

fn become_leader(node: &mut RaftNode) -> Vec<ElectionAction> {
    debug_assert_eq!(node.state, RaftState::Candidate);

    node.state = RaftState::Leader;
    node.leader_id = Some(node.id);

    // Copy membership once to end the immutable peer-list borrow.
    let peers = node.peers.clone();
    let mut actions = vec![ElectionAction::PromoteLeader];

    for peer in peers {
        let next = node.log.next_index();
        node.next_index.insert(peer, next);
        node.match_index.insert(peer, 0);

        let prev = next.saturating_sub(1);
        actions.push(ElectionAction::Send {
            to: peer,
            rpc: RaftRpc::AppendEntries(AppendEntriesRequest {
                term: node.current_term,
                leader_id: node.id,
                prev_log_index: prev,
                prev_log_term: node.log.term_at(prev).unwrap_or(0),
                entries: Vec::new(),
                leader_commit: node.commit_index,
            }),
        });
    }

    node.heartbeat_ticks = node.heartbeat_interval_ticks;
    actions
}

pub(crate) fn observe_term(node: &mut RaftNode, observed_term: u64) -> Vec<ElectionAction> {
    if observed_term > node.current_term {
        become_follower(node, observed_term);
        vec![
            ElectionAction::DemoteFollower,
            persist_hard_state(node),
        ]
    } else {
        Vec::new()
    }
}

fn start_election(node: &mut RaftNode) -> Vec<ElectionAction> {
    node.current_term = node.current_term.saturating_add(1);
    node.state = RaftState::Candidate;
    node.leader_id = None;
    node.voted_for = Some(node.id);
    node.election.votes_granted.clear();
    node.election.votes_granted.insert(node.id);

    // Fresh randomized timeout for split-vote retries (seeded PRNG).
    let range = node.election_timeout_min_ticks..=node.election_timeout_max_ticks;
    reset_timeout(&mut node.election, range, &mut node.rng);

    let last_log_index = node.log.last_index();
    let last_log_term = node.log.last_term();
    let mut actions = vec![persist_hard_state(node)];

    for &peer in &node.peers {
        if peer == node.id {
            continue;
        }
        actions.push(ElectionAction::Send {
            to: peer,
            rpc: RaftRpc::RequestVote(RequestVoteRequest {
                term: node.current_term,
                candidate_id: node.id,
                last_log_index,
                last_log_term,
            }),
        });
    }

    let voters = node.peers.len() + 1;
    let quorum = voters / 2 + 1;
    if node.election.votes_granted.len() >= quorum {
        actions.extend(become_leader(node));
    }

    actions
}

fn tally_granted_vote(node: &mut RaftNode, from: NodeId) -> Vec<ElectionAction> {
    node.election.votes_granted.insert(from);

    let voters = node.peers.len() + 1;
    let quorum = voters / 2 + 1;
    if node.election.votes_granted.len() >= quorum {
        return become_leader(node);
    }

    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::{ElectionState, is_log_up_to_date, reset_timeout};
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    #[test]
    fn reset_uses_the_nodes_selected_timeout() {
        let mut state = ElectionState::new(5);

        assert!(!state.tick());
        state.set_election_timeout(3);

        assert!(!state.tick());
        assert!(!state.tick());
        assert!(state.tick());
    }

    #[test]
    fn log_up_to_date_prefers_higher_term() {
        assert!(is_log_up_to_date(2, 1, 1, 100));
        assert!(!is_log_up_to_date(1, 100, 2, 1));
    }

    #[test]
    fn log_up_to_date_uses_index_when_terms_equal() {
        assert!(is_log_up_to_date(1, 5, 1, 5));
        assert!(is_log_up_to_date(1, 6, 1, 5));
        assert!(!is_log_up_to_date(1, 4, 1, 5));
    }

    #[test]
    fn reset_timeout_is_deterministic_for_same_seed() {
        let mut a = ChaCha8Rng::seed_from_u64(42);
        let mut b = ChaCha8Rng::seed_from_u64(42);
        let mut sa = ElectionState::new(1);
        let mut sb = ElectionState::new(1);

        reset_timeout(&mut sa, 10..=20, &mut a);
        reset_timeout(&mut sb, 10..=20, &mut b);
        assert_eq!(sa, sb);

        reset_timeout(&mut sa, 10..=20, &mut a);
        reset_timeout(&mut sb, 10..=20, &mut b);
        assert_eq!(sa, sb);
    }
}
