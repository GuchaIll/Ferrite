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
        LogEntry, RaftNode, RaftRpc, RequestVoteRequest, RequestVoteResponse, replication,
        state::RaftState, storage::HardState,
    },
};

/// Effects that election handling will request from the Raft node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElectionAction {
    Persist(HardState),
    /// Make a log mutation durable: drop entries at or above `truncate_from`
    /// (if set), then append `entries`. Precedes any `Send` that depends on it.
    PersistLog {
        truncate_from: Option<u64>,
        entries: Vec<LogEntry>,
    },
    Send {
        to: NodeId,
        rpc: RaftRpc,
    },
    PromoteLeader,
    DemoteFollower,
    RedirectLeader {
        leader_hint: Option<NodeId>,
    },
    /// One committed log entry to apply to the state machine (index order).
    ApplyCommittedEntries {
        entry: LogEntry,
    },
    /// Ask the driver for state-machine snapshot bytes at the given index/term.
    RequestSnapshot {
        last_included_index: u64,
        last_included_term: u64,
    },
    /// Persist a snapshot before the log prefix it replaces is discarded.
    PersistSnapshot(crate::raft::snapshot::Snapshot),
    /// Install snapshot bytes into the application state machine (incl. dedup table).
    ApplySnapshot(crate::raft::snapshot::Snapshot),
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
    let timeout = if lo >= hi { lo } else { rng.gen_range(lo..=hi) };
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

pub(crate) fn persist_hard_state(node: &RaftNode) -> ElectionAction {
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
        return vec![ElectionAction::DemoteFollower, persist_hard_state(node)];
    }

    if node.state == RaftState::Candidate
        && response.term == node.current_term
        && response.vote_granted
    {
        return tally_granted_vote(node, from);
    }

    Vec::new()
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

    // §5.4.2: a fresh leader cannot count replicas for previous-term entries, so it
    // appends a no-op in its own term. Committing the no-op transitively commits every
    // entry before it, which is how the leader learns its real commit point.
    //
    // The no-op lands at the end of the log, so each peer's optimistic `next_index`
    // points at it; `match_index` stays pessimistic at 0 until a peer acknowledges.
    let noop_index = node.log.next_index();
    for &peer in &node.peers {
        node.next_index.insert(peer, noop_index);
        node.match_index.insert(peer, 0);
    }

    // `next_index()` is `last_index() + 1`, so this append is contiguous by construction.
    let noop = LogEntry::new(noop_index, node.current_term, Vec::new());
    let appended = node.log.append(noop.clone());
    debug_assert!(
        appended.is_ok(),
        "no-op at leader's next_index must be contiguous: {appended:?}"
    );

    node.heartbeat_ticks = node.heartbeat_interval_ticks;
    let mut actions = vec![ElectionAction::PromoteLeader];
    // The no-op is durable before any AppendEntries carrying it leaves.
    if appended.is_ok() {
        actions.push(ElectionAction::PersistLog {
            truncate_from: None,
            entries: vec![noop],
        });
    }
    actions.extend(replication::broadcast_append_entries(node));
    actions
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

    if node.election.votes_granted.len() >= quorum(node) {
        actions.extend(become_leader(node));
    }

    actions
}

fn tally_granted_vote(node: &mut RaftNode, from: NodeId) -> Vec<ElectionAction> {
    node.election.votes_granted.insert(from);

    if node.election.votes_granted.len() >= quorum(node) {
        return become_leader(node);
    }

    Vec::new()
}

/// Votes needed to win an election.
///
/// `peers` is filtered for this node's own id, matching how
/// [`crate::raft::replication`] sizes the cluster for commit. Both must agree:
/// if one counts a self-entry in `peers` as an extra voter and the other does
/// not, a three-node cluster needs three votes to elect but only two to commit,
/// and it simply stops electing leaders the moment any node is down. Config is
/// the likely source of a self-entry, so tolerating it beats trusting it.
fn quorum(node: &RaftNode) -> usize {
    let voters = 1 + node.peers.iter().filter(|&&peer| peer != node.id).count();
    voters / 2 + 1
}

#[cfg(test)]
mod tests {
    use super::{
        ElectionAction, ElectionState, handle_request_vote_response, is_log_up_to_date, quorum,
        reset_timeout,
    };
    use crate::raft::{
        AppendEntriesRequest, LogEntry, RaftNode, RaftRpc, RequestVoteResponse, state::RaftState,
    };
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    fn candidate_with_self_vote(id: u64, peers: Vec<u64>, term: u64) -> RaftNode {
        let mut node = RaftNode::new(id, peers);
        node.current_term = term;
        node.state = RaftState::Candidate;
        node.voted_for = Some(id);
        node.election.votes_granted.clear();
        node.election.votes_granted.insert(id);
        node
    }

    fn vote_ok(term: u64) -> RequestVoteResponse {
        RequestVoteResponse {
            term,
            vote_granted: true,
        }
    }

    fn ae_sends(actions: &[ElectionAction]) -> Vec<(u64, &AppendEntriesRequest)> {
        actions
            .iter()
            .filter_map(|a| match a {
                ElectionAction::Send {
                    to,
                    rpc: RaftRpc::AppendEntries(req),
                } => Some((*to, req)),
                _ => None,
            })
            .collect()
    }

    /// Piece 2: winning an election appends a blank current-term entry and
    /// replicates it immediately via AppendEntries.
    #[test]
    fn become_leader_appends_current_term_noop_and_replicates() {
        let mut node = candidate_with_self_vote(1, vec![2, 3], 4);
        // Prior-term residue that cannot commit by majority alone (Figure 8).
        node.log
            .append(LogEntry::new(1, 2, b"old".to_vec()))
            .unwrap();
        node.log
            .append(LogEntry::new(2, 3, b"old2".to_vec()))
            .unwrap();
        node.commit_index = 0;

        let actions = handle_request_vote_response(&mut node, 2, vote_ok(4));

        assert_eq!(node.state, RaftState::Leader);
        assert_eq!(node.leader_id, Some(1));
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, ElectionAction::PromoteLeader))
        );

        // No-op sits at the tail: empty command, leader's term, index 3.
        assert_eq!(node.log.last_index(), 3);
        let noop = node.log.entry(3).unwrap();
        assert_eq!(noop.term, 4);
        assert!(noop.command.is_empty());
        // Prior entries untouched.
        assert_eq!(node.log.entry(1).unwrap().command, b"old");
        assert_eq!(node.log.entry(2).unwrap().command, b"old2");

        // next_index points at the no-op; match_index starts at 0.
        assert_eq!(node.next_index[&2], 3);
        assert_eq!(node.next_index[&3], 3);
        assert_eq!(node.match_index[&2], 0);
        assert_eq!(node.match_index[&3], 0);

        let sends = ae_sends(&actions);
        assert_eq!(sends.len(), 2, "one AE per peer, got {sends:?}");
        for (to, req) in sends {
            assert!(to == 2 || to == 3);
            assert_eq!(req.term, 4);
            assert_eq!(req.leader_id, 1);
            assert_eq!(req.prev_log_index, 2);
            assert_eq!(req.prev_log_term, 3);
            assert_eq!(req.leader_commit, 0);
            assert_eq!(req.entries.len(), 1);
            assert_eq!(req.entries[0].index, 3);
            assert_eq!(req.entries[0].term, 4);
            assert!(req.entries[0].command.is_empty());
        }
    }

    #[test]
    fn become_leader_noop_on_empty_log_starts_at_index_one() {
        let mut node = candidate_with_self_vote(1, vec![2, 3], 1);
        let actions = handle_request_vote_response(&mut node, 2, vote_ok(1));

        assert_eq!(node.log.last_index(), 1);
        let noop = node.log.entry(1).unwrap();
        assert_eq!(noop.term, 1);
        assert!(noop.command.is_empty());
        assert_eq!(node.next_index[&2], 1);
        assert_eq!(node.match_index[&2], 0);

        let sends = ae_sends(&actions);
        assert_eq!(sends.len(), 2);
        for (_to, req) in sends {
            assert_eq!(req.prev_log_index, 0);
            assert_eq!(req.prev_log_term, 0);
            assert_eq!(req.entries, vec![LogEntry::new(1, 1, vec![])]);
        }

        // The no-op is persisted before the first AppendEntries carrying it.
        let persist = actions.iter().position(|a| {
            *a == ElectionAction::PersistLog {
                truncate_from: None,
                entries: vec![LogEntry::new(1, 1, vec![])],
            }
        });
        let first_send = actions
            .iter()
            .position(|a| matches!(a, ElectionAction::Send { .. }));
        assert!(
            matches!((persist, first_send), (Some(p), Some(s)) if p < s),
            "no-op PersistLog must precede the first Send: {actions:?}"
        );
    }

    #[test]
    fn single_node_cluster_becomes_leader_with_noop_on_election_start() {
        // peers empty → self-vote alone is a quorum; start_election → become_leader.
        let mut node = RaftNode::new(1, vec![]);
        node.current_term = 0;
        node.election.set_election_timeout(1);
        // First tick expires the timer.
        let actions = super::on_tick(&mut node);

        assert_eq!(node.state, RaftState::Leader);
        assert_eq!(node.current_term, 1);
        assert_eq!(node.log.last_index(), 1);
        assert!(node.log.entry(1).unwrap().command.is_empty());
        assert_eq!(node.log.entry(1).unwrap().term, 1);
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, ElectionAction::PromoteLeader))
        );
        // No peers → no AppendEntries Sends.
        assert!(ae_sends(&actions).is_empty());
    }

    #[test]
    fn quorum_ignores_this_node_appearing_in_its_own_peer_list() {
        // A config that lists every member, this node included, is the obvious
        // way to write one. Counting the self-entry as a fourth voter in a
        // three-node cluster would demand unanimity to elect while commit still
        // needed two — the cluster would stop electing as soon as one node died.
        let with_self = RaftNode::new(1, vec![1, 2, 3]);
        let without_self = RaftNode::new(1, vec![2, 3]);

        assert_eq!(quorum(&with_self), 2);
        assert_eq!(quorum(&without_self), 2);
    }

    #[test]
    fn vote_below_quorum_does_not_append_noop() {
        let mut node = candidate_with_self_vote(1, vec![2, 3, 4, 5], 2);
        // 5-node cluster needs 3 votes; self + one grant is not enough.
        let actions = handle_request_vote_response(&mut node, 2, vote_ok(2));

        assert_eq!(node.state, RaftState::Candidate);
        assert_eq!(node.log.last_index(), 0);
        assert!(actions.is_empty());
        assert!(node.next_index.is_empty());
    }

    /// Piece 2 + 1: after the no-op is majority-acked, commit can advance and
    /// previous-term entries become committed indirectly.
    #[test]
    fn noop_majority_enables_commit_of_prior_term_entries() {
        use crate::raft::AppendEntriesResponse;
        use crate::raft::replication::handle_append_entries_response;

        let mut node = candidate_with_self_vote(1, vec![2, 3], 5);
        node.log.append(LogEntry::new(1, 2, b"a".to_vec())).unwrap();
        node.log.append(LogEntry::new(2, 4, b"b".to_vec())).unwrap();
        node.commit_index = 0;
        node.last_applied = 0;

        handle_request_vote_response(&mut node, 2, vote_ok(5));
        assert_eq!(node.log.last_index(), 3); // no-op at 3
        assert_eq!(node.commit_index, 0); // not yet replicated

        // One follower acks through the no-op → majority (leader + peer 2).
        let actions = handle_append_entries_response(
            &mut node,
            2,
            AppendEntriesResponse {
                term: 5,
                success: true,
                match_index: 3,
            },
        );

        assert_eq!(node.commit_index, 3);
        assert_eq!(node.last_applied, 3);
        let applied: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                ElectionAction::ApplyCommittedEntries { entry } => {
                    Some((entry.index, entry.term, entry.command.clone()))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            applied,
            vec![
                (1, 2, b"a".to_vec()),
                (2, 4, b"b".to_vec()),
                (3, 5, vec![]), // no-op
            ]
        );
    }

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
