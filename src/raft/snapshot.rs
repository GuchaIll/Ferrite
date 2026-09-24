//! Raft snapshot protocol state transitions.
//!
//! Durable snapshot records belong under [`crate::raft::storage`]. The Raft
//! core never opens files: it emits ordered effects
//! (`RequestSnapshot` → driver fills state-machine bytes → `SnapshotTaken` →
//! `PersistSnapshot` → driver confirms → log trim) and `InstallSnapshot` RPCs
//! for lagging followers.

use crate::{
    config::NodeId,
    raft::{
        InstallSnapshotRequest, InstallSnapshotResponse, RaftNode, RaftRpc,
        election::{ElectionAction, become_follower, persist_hard_state},
        state::RaftState,
    },
};

use serde::{Deserialize, Serialize};

/// Metadata describing the last log entry covered by a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub last_included_index: u64,
    pub last_included_term: u64,
}

/// Full snapshot handed across the driver boundary (persist / apply / install).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub meta: SnapshotMeta,
    pub data: Vec<u8>,
}

/// In-flight InstallSnapshot assembly on a follower (chunked by `offset`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingSnapshot {
    pub meta: SnapshotMeta,
    /// Expected next byte offset; rejects gaps, reorders, and duplicates.
    pub next_offset: u64,
    pub data: Vec<u8>,
}

impl SnapshotMeta {
    pub fn new(last_included_index: u64, last_included_term: u64) -> Self {
        Self {
            last_included_index,
            last_included_term,
        }
    }
}

impl Snapshot {
    pub fn new(meta: SnapshotMeta, data: Vec<u8>) -> Self {
        Self { meta, data }
    }
}

impl PendingSnapshot {
    pub fn new(meta: SnapshotMeta) -> Self {
        Self {
            meta,
            next_offset: 0,
            data: Vec::new(),
        }
    }
}

/// Applied entries since the last snapshot that warrant taking a new one.
///
/// Snapshotting is capped at `last_applied` so compaction never outruns
/// application (issue 06).
pub(crate) fn should_snapshot(node: &RaftNode) -> bool {
    if node.compaction_threshold == 0 {
        return false;
    }
    let applied_since = node
        .last_applied
        .saturating_sub(node.log.last_included_index());
    applied_since >= node.compaction_threshold
}

/// Asks the driver for state-machine bytes at `last_applied`.
pub(crate) fn request_snapshot_action(node: &RaftNode) -> ElectionAction {
    debug_assert!(
        node.last_applied >= node.log.last_included_index(),
        "snapshot request must not target below the existing snapshot boundary"
    );
    let last_included_index = node.last_applied;
    let last_included_term = node.log.term_at(last_included_index).unwrap_or(0);
    ElectionAction::RequestSnapshot {
        last_included_index,
        last_included_term,
    }
}

/// Driver returned snapshot bytes: retain them and demand durable persist first.
///
/// Log trim happens only in [`on_snapshot_persisted`] after the driver drains
/// `PersistSnapshot` (snapshot-before-trim durability order).
pub(crate) fn on_snapshot_taken(node: &mut RaftNode, snapshot: Snapshot) -> Vec<ElectionAction> {
    let last_included_index = snapshot.meta.last_included_index;

    debug_assert!(
        last_included_index <= node.last_applied,
        "compaction must not trim past last_applied ({} > {})",
        last_included_index,
        node.last_applied
    );
    if last_included_index <= node.log.last_included_index() {
        return Vec::new();
    }
    if last_included_index > node.last_applied {
        return Vec::new();
    }

    // Clone justified: PersistSnapshot effect owns one copy; node retains another
    // for InstallSnapshot catch-up until replaced by a newer snapshot.
    node.snapshot = Some(snapshot.clone());
    vec![ElectionAction::PersistSnapshot(snapshot)]
}

/// Driver finished persisting the snapshot; now it is safe to discard the prefix.
pub(crate) fn on_snapshot_persisted(
    node: &mut RaftNode,
    meta: SnapshotMeta,
) -> Vec<ElectionAction> {
    debug_assert!(
        meta.last_included_index <= node.last_applied,
        "compaction must not trim past last_applied"
    );
    if meta.last_included_index <= node.log.last_included_index() {
        return Vec::new();
    }
    if meta.last_included_index > node.last_applied {
        return Vec::new();
    }

    node.log
        .compact(meta.last_included_index, meta.last_included_term);

    if node.commit_index < meta.last_included_index {
        node.commit_index = meta.last_included_index;
    }
    if node.last_applied < meta.last_included_index {
        node.last_applied = meta.last_included_index;
    }
    Vec::new()
}

/// Leader path: send InstallSnapshot when the follower's nextIndex is at or
/// below the compacted boundary (AppendEntries cannot form a valid prev).
pub(crate) fn send_install_snapshot(node: &RaftNode, to: NodeId) -> Vec<ElectionAction> {
    let Some(snapshot) = node.snapshot.as_ref() else {
        return Vec::new();
    };

    // Single-chunk install for now (streaming is out of scope). offset=0, done=true.
    // Clone justified: RPC payload crosses the driver boundary.
    vec![ElectionAction::Send {
        to,
        rpc: RaftRpc::InstallSnapshot(InstallSnapshotRequest {
            term: node.current_term,
            leader_id: node.id,
            last_included_index: snapshot.meta.last_included_index,
            last_included_term: snapshot.meta.last_included_term,
            offset: 0,
            data: snapshot.data.clone(),
            done: true,
        }),
    }]
}

/// Follower InstallSnapshot handling with offset-checked chunk assembly.
pub(crate) fn handle_install_snapshot_request(
    node: &mut RaftNode,
    from: NodeId,
    request: InstallSnapshotRequest,
) -> Vec<ElectionAction> {
    let mut actions = Vec::new();

    if request.term > node.current_term {
        become_follower(node, request.term);
        actions.push(ElectionAction::DemoteFollower);
        actions.push(persist_hard_state(node));
    }

    if request.term < node.current_term {
        actions.push(install_snapshot_reply(from, node.current_term));
        return actions;
    }

    if node.state != RaftState::Follower {
        node.state = RaftState::Follower;
        actions.push(ElectionAction::DemoteFollower);
    }
    node.leader_id = Some(request.leader_id);
    node.election.reset();

    // Already covered by an equal-or-newer local snapshot boundary.
    if request.last_included_index <= node.log.last_included_index() && request.done {
        actions.push(install_snapshot_reply(from, node.current_term));
        return actions;
    }

    let meta = SnapshotMeta::new(request.last_included_index, request.last_included_term);

    // Start or continue chunk assembly. Duplicate / reordered offsets are rejected.
    let accept = match node.pending_snapshot.as_mut() {
        Some(pending) if pending.meta == meta && request.offset == pending.next_offset => {
            pending.data.extend_from_slice(&request.data);
            pending.next_offset = pending
                .next_offset
                .saturating_add(request.data.len() as u64);
            true
        }
        Some(pending) if pending.meta == meta => false,
        _ if request.offset == 0 => {
            let mut pending = PendingSnapshot::new(meta);
            pending.data.extend_from_slice(&request.data);
            pending.next_offset = request.data.len() as u64;
            node.pending_snapshot = Some(pending);
            true
        }
        _ => false,
    };

    if !accept {
        return actions;
    }

    if !request.done {
        return actions;
    }

    let Some(pending) = node.pending_snapshot.take() else {
        return actions;
    };

    let snapshot = Snapshot::new(pending.meta, pending.data);

    // A local entry at the boundary with a different term means the whole
    // local suffix is discarded. Storage must record that discard, or replay
    // would resurrect the stale entries. A log shorter than the boundary holds
    // nothing replay would keep, so it needs no record.
    let discards_local_log = node.log.last_index() >= snapshot.meta.last_included_index
        && !node.log.contains(
            snapshot.meta.last_included_index,
            snapshot.meta.last_included_term,
        );

    // Log install: keep suffix on matching (index, term), else discard.
    node.log.install_snapshot(
        snapshot.meta.last_included_index,
        snapshot.meta.last_included_term,
    );

    node.commit_index = node.commit_index.max(snapshot.meta.last_included_index);
    node.last_applied = node.last_applied.max(snapshot.meta.last_included_index);
    // Clone justified: node keeps snapshot for future catch-up; effects own copies.
    node.snapshot = Some(snapshot.clone());

    // Durability before reply: persist snapshot, apply to state machine, then ack.
    // The truncate follows the snapshot so a crash between them recovers the
    // snapshot, whose boundary check discards the stale entries anyway.
    let last_included_index = snapshot.meta.last_included_index;
    actions.push(ElectionAction::PersistSnapshot(snapshot.clone()));
    if discards_local_log {
        actions.push(ElectionAction::PersistLog {
            truncate_from: Some(last_included_index),
            entries: Vec::new(),
        });
    }
    actions.push(ElectionAction::ApplySnapshot(snapshot));
    actions.push(install_snapshot_reply(from, node.current_term));
    actions
}

/// Leader InstallSnapshot response: advance next/match like a successful catch-up.
pub(crate) fn handle_install_snapshot_response(
    node: &mut RaftNode,
    from: NodeId,
    response: InstallSnapshotResponse,
) -> Vec<ElectionAction> {
    if response.term > node.current_term {
        become_follower(node, response.term);
        return vec![ElectionAction::DemoteFollower, persist_hard_state(node)];
    }

    if node.state != RaftState::Leader || response.term < node.current_term {
        return Vec::new();
    }

    let Some(snapshot) = node.snapshot.as_ref() else {
        return Vec::new();
    };

    let matched = snapshot.meta.last_included_index;
    let prev_match = node.match_index.get(&from).copied().unwrap_or(0);
    if matched > prev_match {
        node.match_index.insert(from, matched);
        node.next_index.insert(from, matched.saturating_add(1));
    }

    Vec::new()
}

fn install_snapshot_reply(to: NodeId, term: u64) -> ElectionAction {
    ElectionAction::Send {
        to,
        rpc: RaftRpc::InstallSnapshotResponse(InstallSnapshotResponse { term }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::LogEntry;

    fn leader_with_entries(n: u64, threshold: u64) -> RaftNode {
        let mut node = RaftNode::new(1, vec![2, 3]);
        node.state = RaftState::Leader;
        node.current_term = 1;
        node.leader_id = Some(1);
        node.compaction_threshold = threshold;
        for i in 1..=n {
            node.log
                .append(LogEntry::new(i, 1, format!("c{i}").into_bytes()))
                .unwrap();
        }
        node.commit_index = n;
        node.last_applied = n;
        node
    }

    #[test]
    fn should_snapshot_uses_applied_since_last_included() {
        let mut node = leader_with_entries(5, 3);
        assert!(should_snapshot(&node));
        node.log.compact(3, 1);
        node.snapshot = Some(Snapshot::new(SnapshotMeta::new(3, 1), vec![1]));
        assert!(!should_snapshot(&node));
        node.last_applied = 6;
        assert!(should_snapshot(&node));
    }

    #[test]
    fn on_snapshot_taken_persists_before_trim() {
        let mut node = leader_with_entries(5, 3);
        let snap = Snapshot::new(SnapshotMeta::new(4, 1), b"sm".to_vec());
        let actions = on_snapshot_taken(&mut node, snap.clone());
        assert_eq!(actions, vec![ElectionAction::PersistSnapshot(snap.clone())]);
        // Prefix still present until persist is confirmed.
        assert_eq!(node.log.last_included_index(), 0);
        assert_eq!(node.log.start_index(), 1);

        let follow = on_snapshot_persisted(&mut node, snap.meta);
        assert!(follow.is_empty());
        assert_eq!(node.log.last_included_index(), 4);
        assert_eq!(node.log.start_index(), 5);
        assert_eq!(node.log.last_index(), 5);
    }

    #[test]
    fn install_snapshot_rejects_offset_gap() {
        let mut node = RaftNode::new(2, vec![1, 3]);
        node.current_term = 1;
        let req = InstallSnapshotRequest {
            term: 1,
            leader_id: 1,
            last_included_index: 2,
            last_included_term: 1,
            offset: 5,
            data: b"nope".to_vec(),
            done: true,
        };
        let actions = handle_install_snapshot_request(&mut node, 1, req);
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, ElectionAction::ApplySnapshot(_))),
            "gap must not apply: {actions:?}"
        );
        assert!(node.pending_snapshot.is_none());
    }

    #[test]
    fn install_snapshot_done_applies_and_persists() {
        let mut node = RaftNode::new(2, vec![1, 3]);
        node.current_term = 1;
        node.log.append(LogEntry::new(1, 1, b"a".to_vec())).unwrap();
        let req = InstallSnapshotRequest {
            term: 1,
            leader_id: 1,
            last_included_index: 2,
            last_included_term: 1,
            offset: 0,
            data: b"full".to_vec(),
            done: true,
        };
        let actions = handle_install_snapshot_request(&mut node, 1, req);
        assert!(matches!(
            actions.as_slice(),
            [
                ElectionAction::PersistSnapshot(_),
                ElectionAction::ApplySnapshot(_),
                ElectionAction::Send { .. },
            ]
        ));
        assert_eq!(node.log.last_included_index(), 2);
        assert_eq!(node.last_applied, 2);
    }

    #[test]
    fn install_over_conflicting_log_persists_truncate_after_snapshot() {
        let mut node = RaftNode::new(2, vec![1, 3]);
        node.current_term = 3;
        for entry in [
            LogEntry::new(1, 1, b"a".to_vec()),
            LogEntry::new(2, 2, b"stale".to_vec()),
            LogEntry::new(3, 2, b"stale".to_vec()),
        ] {
            node.log.append(entry).unwrap();
        }
        let req = InstallSnapshotRequest {
            term: 3,
            leader_id: 1,
            last_included_index: 2,
            last_included_term: 3,
            offset: 0,
            data: b"full".to_vec(),
            done: true,
        };

        let actions = handle_install_snapshot_request(&mut node, 1, req);

        // Snapshot first, then the durable discard, then apply and ack.
        assert!(
            matches!(
                actions.as_slice(),
                [
                    ElectionAction::PersistSnapshot(_),
                    ElectionAction::PersistLog {
                        truncate_from: Some(2),
                        entries,
                    },
                    ElectionAction::ApplySnapshot(_),
                    ElectionAction::Send { .. },
                ] if entries.is_empty()
            ),
            "got {actions:?}"
        );
        assert_eq!(node.log.last_index(), 2);
    }
}
