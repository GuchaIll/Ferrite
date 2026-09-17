//! Raft log replication state transitions.
//!
//! Transport delivery belongs in [`crate::transport`]; this module owns only
//! the protocol decisions for AppendEntries requests and responses.

use crate::{
    config::NodeId,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, LogEntry, RaftNode, RaftRpc,
        election::{ElectionAction, become_follower, persist_hard_state},
        state::RaftState,
    },
};

/// Handles an incoming AppendEntries RPC (including empty heartbeats).
///
/// Enforces the log consistency check (§5.3): accept only when the follower
/// holds `prev_log_index`/`prev_log_term`. Conflicting suffixes are replaced;
/// already-matching (stale/duplicate) batches are a no-op.
pub(crate) fn handle_append_entries_request(
    node: &mut RaftNode,
    from: NodeId,
    req: AppendEntriesRequest,
) -> Vec<ElectionAction> {
    let mut actions = Vec::new();

    if req.term > node.current_term {
        become_follower(node, req.term);
        actions.push(ElectionAction::DemoteFollower);
        actions.push(persist_hard_state(node));
    }

    if req.term < node.current_term {
        actions.push(append_entries_reply(from, node.current_term, false, 0));
        return actions;
    }

    // Same (or newly adopted) term: accept leader authority.
    if node.state != RaftState::Follower {
        node.state = RaftState::Follower;
        actions.push(ElectionAction::DemoteFollower);
    }
    node.leader_id = Some(req.leader_id);
    node.election.reset();

    // Consistency check: missing or mismatched prev entry → reject, no truncate.
    if !node.log.contains(req.prev_log_index, req.prev_log_term) {
        actions.push(append_entries_reply(from, node.current_term, false, 0));
        return actions;
    }

    // append_from_leader validates contiguity first, then truncates only at the
    // first conflicting index (exact-match prefix is retained / no-op).
    if node.log.append_from_leader(&req.entries).is_err() {
        actions.push(append_entries_reply(from, node.current_term, false, 0));
        return actions;
    }

    // Paper step 5: advance commitIndex from leaderCommit (apply is separate).
    if req.leader_commit > node.commit_index {
        node.commit_index = req.leader_commit.min(node.log.last_index());
    }

    let ack = req.prev_log_index + req.entries.len() as u64;
    actions.push(append_entries_reply(from, node.current_term, true, ack));
    actions
}

/// Handles an AppendEntries response on the leader.
///
/// `match_index` is pessimistic evidence: advanced only on an acknowledged
/// match, never speculatively. `next_index` is optimistic: initialised to the
/// leader's log tail and walked back on rejection until the follower's common
/// prefix is found, then streamed forward entry-by-entry.
pub(crate) fn handle_append_entries_response(
    node: &mut RaftNode,
    from: NodeId,
    resp: AppendEntriesResponse,
) -> Vec<ElectionAction> {
    if resp.term > node.current_term {
        become_follower(node, resp.term);
        return vec![ElectionAction::DemoteFollower, persist_hard_state(node)];
    }

    if node.state != RaftState::Leader || resp.term < node.current_term {
        return Vec::new();
    }

    if resp.success {
        // Advance match_index/next_index using the acknowledged index the
        // follower reported — never assume our own log.last_index() matches.
        let matched = resp.match_index;
        debug_assert!(
            matched <= node.log.last_index(),
            "follower {from} acked match_index {matched} beyond leader last_index {}",
            node.log.last_index()
        );
        node.match_index.insert(from, matched);
        node.next_index.insert(from, matched.saturating_add(1));
        return Vec::new();
    }

    // Rejected: step next_index back one and retry immediately.
    // Floor at start_index so a compacted log never sends an impossible prev.
    let current_next = node
        .next_index
        .get(&from)
        .copied()
        .unwrap_or_else(|| node.log.next_index());
    let new_next = current_next
        .saturating_sub(1)
        .max(node.log.start_index());
    debug_assert!(
        new_next >= node.log.start_index(),
        "next_index for {from} would fall below log start_index {}",
        node.log.start_index()
    );
    node.next_index.insert(from, new_next);

    vec![send_append_entries(node, from)]
}

/// Appends a client command on the leader and replicates to all peers.
pub(crate) fn append_and_replicate(node: &mut RaftNode, command: Vec<u8>) -> Vec<ElectionAction> {
    if node.state != RaftState::Leader {
        return vec![ElectionAction::RedirectLeader {
            leader_hint: node.leader_id,
        }];
    }

    let entry = LogEntry {
        index: node.log.next_index(),
        term: node.current_term,
        command,
    };

    if node.log.append(entry).is_err() {
        // Unreachable for a contiguous leader log; refuse rather than panic.
        return Vec::new();
    }

    broadcast_append_entries(node)
}

/// Builds one AppendEntries Send for every peer from the leader's `next_index`.
pub(crate) fn broadcast_append_entries(node: &RaftNode) -> Vec<ElectionAction> {
    node.peers
        .iter()
        .filter(|&&peer| peer != node.id)
        .map(|&peer| send_append_entries(node, peer))
        .collect()
}

/// Periodic leader probes. Uses the same per-follower `next_index` path as
/// replication so a "heartbeat" to a lagging follower still carries entries.
/// Caught-up followers receive the empty-entry form.
pub(crate) fn heartbeat_actions(node: &RaftNode) -> Vec<ElectionAction> {
    broadcast_append_entries(node)
}

pub(crate) fn send_append_entries(node: &RaftNode, to: NodeId) -> ElectionAction {
    let next = next_index_for(node, to);
    let prev = next.saturating_sub(1);
    ElectionAction::Send {
        to,
        rpc: RaftRpc::AppendEntries(AppendEntriesRequest {
            term: node.current_term,
            leader_id: node.id,
            prev_log_index: prev,
            prev_log_term: node.log.term_at(prev).unwrap_or(0),
            entries: node.log.entries_from(next),
            leader_commit: node.commit_index,
        }),
    }
}

fn next_index_for(node: &RaftNode, peer: NodeId) -> u64 {
    node.next_index
        .get(&peer)
        .copied()
        .unwrap_or_else(|| node.log.next_index())
}

fn append_entries_reply(to: NodeId, term: u64, success: bool, match_index: u64) -> ElectionAction {
    ElectionAction::Send {
        to,
        rpc: RaftRpc::AppendEntriesResponse(AppendEntriesResponse {
            term,
            success,
            match_index,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{LogEntry, RaftNode, state::RaftState};

    fn leader_with_log(entries: &[(u64, u64, &[u8])]) -> RaftNode {
        let mut node = RaftNode::new(1, vec![2, 3]);
        node.state = RaftState::Leader;
        node.current_term = entries.last().map(|e| e.1).unwrap_or(1).max(1);
        node.leader_id = Some(1);
        node.next_index.insert(2, 1);
        node.next_index.insert(3, 1);
        node.match_index.insert(2, 0);
        node.match_index.insert(3, 0);
        for &(index, term, cmd) in entries {
            node.log
                .append(LogEntry::new(index, term, cmd.to_vec()))
                .unwrap();
        }
        let next = node.log.next_index();
        node.next_index.insert(2, next);
        node.next_index.insert(3, next);
        node
    }

    fn follower() -> RaftNode {
        RaftNode::new(2, vec![1, 3])
    }

    fn ae(
        term: u64,
        prev_idx: u64,
        prev_term: u64,
        entries: Vec<LogEntry>,
    ) -> AppendEntriesRequest {
        AppendEntriesRequest {
            term,
            leader_id: 1,
            prev_log_index: prev_idx,
            prev_log_term: prev_term,
            entries,
            leader_commit: 0,
        }
    }

    #[test]
    fn empty_follower_log_accepts_first_entry() {
        let mut node = follower();
        let actions = handle_append_entries_request(
            &mut node,
            1,
            ae(1, 0, 0, vec![LogEntry::new(1, 1, b"x".to_vec())]),
        );
        assert!(actions.iter().any(|a| matches!(
            a,
            ElectionAction::Send {
                rpc: RaftRpc::AppendEntriesResponse(AppendEntriesResponse { success: true, .. }),
                ..
            }
        )));
        assert_eq!(node.log.last_index(), 1);
        assert_eq!(node.log.term_at(1), Ok(1));
    }

    #[test]
    fn missing_prev_index_rejects_without_mutation() {
        let mut node = follower();
        let actions = handle_append_entries_request(
            &mut node,
            1,
            ae(1, 5, 1, vec![LogEntry::new(6, 1, b"x".to_vec())]),
        );
        assert!(matches!(
            actions.last(),
            Some(ElectionAction::Send {
                rpc: RaftRpc::AppendEntriesResponse(AppendEntriesResponse { success: false, .. }),
                ..
            })
        ));
        assert_eq!(node.log.last_index(), 0);
    }

    #[test]
    fn mismatched_prev_term_rejects() {
        let mut node = follower();
        node.log.append(LogEntry::new(1, 1, b"a".to_vec())).unwrap();

        let actions = handle_append_entries_request(
            &mut node,
            1,
            ae(2, 1, 9, vec![LogEntry::new(2, 2, b"b".to_vec())]),
        );
        assert!(matches!(
            actions.last(),
            Some(ElectionAction::Send {
                rpc: RaftRpc::AppendEntriesResponse(AppendEntriesResponse { success: false, .. }),
                ..
            })
        ));
        assert_eq!(node.log.last_index(), 1);
    }

    #[test]
    fn divergent_suffix_is_replaced_prefix_preserved() {
        let mut node = follower();
        for e in [
            LogEntry::new(1, 1, b"a".to_vec()),
            LogEntry::new(2, 1, b"b".to_vec()),
            LogEntry::new(3, 2, b"old1".to_vec()),
            LogEntry::new(4, 2, b"old2".to_vec()),
            LogEntry::new(5, 2, b"old3".to_vec()),
        ] {
            node.log.append(e).unwrap();
        }

        let actions = handle_append_entries_request(
            &mut node,
            1,
            ae(
                3,
                2,
                1,
                vec![
                    LogEntry::new(3, 3, b"new1".to_vec()),
                    LogEntry::new(4, 3, b"new2".to_vec()),
                    LogEntry::new(5, 3, b"new3".to_vec()),
                ],
            ),
        );
        assert!(matches!(
            actions.last(),
            Some(ElectionAction::Send {
                rpc: RaftRpc::AppendEntriesResponse(AppendEntriesResponse { success: true, .. }),
                ..
            })
        ));
        assert_eq!(
            node.log.entries_from(1),
            vec![
                LogEntry::new(1, 1, b"a".to_vec()),
                LogEntry::new(2, 1, b"b".to_vec()),
                LogEntry::new(3, 3, b"new1".to_vec()),
                LogEntry::new(4, 3, b"new2".to_vec()),
                LogEntry::new(5, 3, b"new3".to_vec()),
            ]
        );
    }

    #[test]
    fn exact_match_duplicate_is_noop() {
        let mut node = follower();
        for e in [
            LogEntry::new(1, 1, b"a".to_vec()),
            LogEntry::new(2, 1, b"b".to_vec()),
            LogEntry::new(3, 1, b"c".to_vec()),
        ] {
            node.log.append(e).unwrap();
        }
        let before = node.log.clone();

        let actions = handle_append_entries_request(
            &mut node,
            1,
            ae(
                1,
                1,
                1,
                vec![
                    LogEntry::new(2, 1, b"b".to_vec()),
                    LogEntry::new(3, 1, b"c".to_vec()),
                ],
            ),
        );
        assert!(matches!(
            actions.last(),
            Some(ElectionAction::Send {
                rpc: RaftRpc::AppendEntriesResponse(AppendEntriesResponse { success: true, .. }),
                ..
            })
        ));
        assert_eq!(node.log, before);
    }

    #[test]
    fn follower_ahead_keeps_extra_suffix_when_prefix_matches() {
        let mut node = follower();
        for e in [
            LogEntry::new(1, 1, b"a".to_vec()),
            LogEntry::new(2, 1, b"b".to_vec()),
            LogEntry::new(3, 1, b"c".to_vec()),
            LogEntry::new(4, 1, b"d".to_vec()),
        ] {
            node.log.append(e).unwrap();
        }
        let before = node.log.clone();

        let actions = handle_append_entries_request(
            &mut node,
            1,
            ae(
                1,
                0,
                0,
                vec![
                    LogEntry::new(1, 1, b"a".to_vec()),
                    LogEntry::new(2, 1, b"b".to_vec()),
                ],
            ),
        );
        assert!(matches!(
            actions.last(),
            Some(ElectionAction::Send {
                rpc: RaftRpc::AppendEntriesResponse(AppendEntriesResponse { success: true, .. }),
                ..
            })
        ));
        assert_eq!(node.log, before);
    }

    #[test]
    fn stale_term_rejects_with_append_entries_response() {
        let mut node = follower();
        node.current_term = 5;
        let actions = handle_append_entries_request(
            &mut node,
            1,
            ae(3, 0, 0, vec![LogEntry::new(1, 3, b"x".to_vec())]),
        );
        assert!(matches!(
            actions.last(),
            Some(ElectionAction::Send {
                rpc: RaftRpc::AppendEntriesResponse(AppendEntriesResponse {
                    term: 5,
                    success: false,
                    ..
                }),
                ..
            })
        ));
        assert_eq!(node.log.last_index(), 0);
    }

    #[test]
    fn leader_appends_locally_before_replicate_actions() {
        let mut node = leader_with_log(&[]);
        node.current_term = 1;
        let actions = append_and_replicate(&mut node, b"cmd".to_vec());
        assert_eq!(node.log.last_index(), 1);
        assert_eq!(node.log.entry(1).unwrap().command, b"cmd");
        let sends: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                ElectionAction::Send {
                    to,
                    rpc: RaftRpc::AppendEntries(req),
                } => Some((
                    *to,
                    req.entries.clone(),
                    req.prev_log_index,
                    req.prev_log_term,
                )),
                _ => None,
            })
            .collect();
        assert_eq!(sends.len(), 2);
        for (_to, entries, prev_idx, prev_term) in sends {
            assert_eq!(prev_idx, 0);
            assert_eq!(prev_term, 0);
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].command, b"cmd");
        }
    }

    // ── handle_append_entries_response: backoff walk ──────────────────────

    fn ae_response(term: u64, success: bool, match_index: u64) -> AppendEntriesResponse {
        AppendEntriesResponse {
            term,
            success,
            match_index,
        }
    }

    #[test]
    fn success_response_advances_match_and_next_index() {
        let mut node = leader_with_log(&[(1, 1, b"a"), (2, 1, b"b"), (3, 1, b"c")]);
        node.next_index.insert(2, 4);
        node.match_index.insert(2, 0);

        let actions = handle_append_entries_response(&mut node, 2, ae_response(1, true, 3));

        assert!(actions.is_empty());
        assert_eq!(node.match_index[&2], 3);
        assert_eq!(node.next_index[&2], 4);
    }

    #[test]
    fn rejection_decrements_next_index_and_sends_retry() {
        let mut node = leader_with_log(&[(1, 1, b"a"), (2, 1, b"b"), (3, 1, b"c")]);
        node.next_index.insert(2, 4);
        node.match_index.insert(2, 0);

        let actions = handle_append_entries_response(&mut node, 2, ae_response(1, false, 0));

        assert_eq!(node.next_index[&2], 3);
        assert!(actions.iter().any(|a| matches!(
            a,
            ElectionAction::Send {
                to: 2,
                rpc: RaftRpc::AppendEntries(_),
            }
        )));
    }

    #[test]
    fn next_index_floors_at_log_start_on_repeated_rejection() {
        let mut node = leader_with_log(&[(1, 1, b"a")]);
        node.next_index.insert(2, 1);
        node.match_index.insert(2, 0);

        // next_index is already at 1; rejection must not push it below 1.
        handle_append_entries_response(&mut node, 2, ae_response(1, false, 0));
        assert_eq!(node.next_index[&2], 1);
    }

    #[test]
    fn backoff_walk_decrements_step_by_step_until_floored() {
        let mut node = leader_with_log(&[
            (1, 1, b"a"),
            (2, 1, b"b"),
            (3, 1, b"c"),
            (4, 1, b"d"),
            (5, 1, b"e"),
        ]);
        // next_index[2] is already 6 after leader_with_log; override to make explicit.
        node.next_index.insert(2, 6);

        // Five rejections walk next_index: 6 → 5 → 4 → 3 → 2 → 1.
        for expected_next in (1u64..=5).rev() {
            handle_append_entries_response(&mut node, 2, ae_response(1, false, 0));
            assert_eq!(node.next_index[&2], expected_next, "after rejection");
        }
        // Floored at log start (1): further rejections must not push it below 1.
        handle_append_entries_response(&mut node, 2, ae_response(1, false, 0));
        assert_eq!(node.next_index[&2], 1);
    }

    #[test]
    fn higher_term_in_response_demotes_leader() {
        let mut node = leader_with_log(&[(1, 1, b"a")]);

        let actions = handle_append_entries_response(&mut node, 2, ae_response(5, false, 0));

        assert_eq!(node.current_term, 5);
        assert_eq!(node.state, RaftState::Follower);
        assert!(actions
            .iter()
            .any(|a| matches!(a, ElectionAction::DemoteFollower)));
    }

    #[test]
    fn stale_term_response_is_ignored() {
        let mut node = leader_with_log(&[(1, 3, b"a")]);
        node.next_index.insert(2, 2);
        let before_match = node.match_index[&2];
        let before_next = node.next_index[&2];

        // Response from an old term — must be a no-op.
        let actions = handle_append_entries_response(&mut node, 2, ae_response(1, true, 1));

        assert!(actions.is_empty());
        assert_eq!(node.match_index[&2], before_match);
        assert_eq!(node.next_index[&2], before_next);
    }

    #[test]
    fn heartbeat_is_empty_entries_same_path() {
        let mut node = follower();
        node.log.append(LogEntry::new(1, 1, b"a".to_vec())).unwrap();
        let before = node.log.clone();
        let actions = handle_append_entries_request(&mut node, 1, ae(1, 1, 1, vec![]));
        assert!(matches!(
            actions.last(),
            Some(ElectionAction::Send {
                rpc: RaftRpc::AppendEntriesResponse(AppendEntriesResponse { success: true, .. }),
                ..
            })
        ));
        assert_eq!(node.log, before);
    }
}
