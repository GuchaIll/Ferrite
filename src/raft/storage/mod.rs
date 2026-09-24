//! Raft-specific persistence types and storage boundary.
//!
//! This module owns data that must survive a node restart. Backend mechanics
//! remain behind the storage implementation rather than in the Raft core.
//!
//! - [`memory`] — in-memory backend for the sim and unit tests
//! - [`disk`]   — segment-file backend (issue 03)

pub mod disk;
mod hard_state;
pub mod memory;
pub mod record;

pub use disk::DiskStorage;
pub use hard_state::HardState;
pub use memory::MemoryStorage;
pub use record::LogOp;

use std::collections::BTreeMap;

use crate::{
    config::StorageBackend,
    error::{Error, StorageError},
    raft::{LogEntry, RaftLog, Snapshot, SnapshotMeta},
};

/// Opens the configured storage backend.
///
/// `Memory` is for the simulator and unit tests. `Disk` opens the segment-file
/// store under `data_dir` (blocking IO — call via `spawn_blocking` from async).
pub fn open(backend: &StorageBackend) -> Result<Box<dyn Storage>, Error> {
    match backend {
        StorageBackend::Memory => Ok(Box::new(MemoryStorage::default())),
        StorageBackend::Disk { data_dir } => Ok(Box::new(DiskStorage::open(data_dir)?)),
    }
}

// ── public types ──────────────────────────────────────────────────────────────

/// Durable state a node restarts from. Everything else resets.
///
/// `log` carries the snapshot boundary and must be built by [`replay`], never
/// assembled by hand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recovered {
    pub hard_state: HardState,
    pub log: RaftLog,
    pub snapshot: Option<Snapshot>,
}

/// All mutations produced by one driver turn.
///
/// `commit` makes the whole batch durable in a single group-commit call.
/// Durable order is: snapshot → hard state → log (see [`DiskStorage::commit`]
/// and [`MemoryStorage::commit`]).
#[derive(Default)]
pub struct WriteBatch {
    /// Last `Persist` effect in the batch wins.
    pub hard_state: Option<HardState>,
    /// Log ops in output order (appends and truncations).
    pub log: Vec<LogOp>,
    pub snapshot: Option<Snapshot>,
}

impl WriteBatch {
    pub fn is_empty(&self) -> bool {
        self.hard_state.is_none() && self.log.is_empty() && self.snapshot.is_none()
    }
}

// ── Storage trait ─────────────────────────────────────────────────────────────

pub trait Storage {
    /// Makes the whole batch durable; one sync per touched file.
    ///
    /// Blocking. Callers in an async context must run this via
    /// `tokio::task::spawn_blocking` (issue 04).
    fn commit(&mut self, batch: &WriteBatch) -> Result<(), Error>;

    /// Rebuilds durable state. Torn tail tolerated; corruption is an error.
    ///
    /// Blocking. Same async caveat as `commit`.
    fn recover(&self) -> Result<Recovered, Error>;

    /// Number of fsyncs performed so far. Used in group-commit tests.
    fn sync_count(&self) -> u64;
}

// ── replay ────────────────────────────────────────────────────────────────────

/// Rebuilds a [`RaftLog`] from a sequence of log ops and an optional snapshot.
///
/// This is the only way a recovered log gets constructed; both the memory and
/// disk backends call it from their `recover` implementations.
///
/// ## Algorithm
///
/// 1. Fold ops into a sparse `BTreeMap<index, entry>`:
///    - `Append(e)` drops all keys ≥ `e.index`, then inserts `e`.
///    - `TruncateFrom(i)` drops all keys ≥ `i`.
///
/// 2. Reconcile with the snapshot boundary `(idx, term)`:
///    - Entry at `idx` has the same term → keep entries above `idx`.
///    - Entry at `idx` has a different term → drop everything (stale branch).
///    - No entry at `idx` (already compacted) → keep entries above `idx`.
///
/// 3. Build the log: `install_snapshot` sets the boundary; then `append` each
///    kept entry. A gap returns `Corruption`.
pub fn replay(ops: &[LogOp], snap_meta: Option<&SnapshotMeta>) -> Result<RaftLog, StorageError> {
    let mut map: BTreeMap<u64, LogEntry> = BTreeMap::new();

    for op in ops {
        match op {
            LogOp::Append(e) => {
                // Clone justified: ops is borrowed; map takes ownership of each entry.
                let _ = map.split_off(&e.index); // drop keys ≥ e.index
                map.insert(e.index, e.clone());
            }
            LogOp::TruncateFrom(i) => {
                let _ = map.split_off(i); // drop keys ≥ i
            }
        }
    }

    let kept: BTreeMap<u64, LogEntry> = match snap_meta {
        None => map,
        Some(snap) => {
            let idx = snap.last_included_index;
            let keep_tail = match map.get(&idx) {
                Some(e) => e.term == snap.last_included_term,
                None => true, // already compacted past this boundary
            };
            if keep_tail {
                map.split_off(&idx.saturating_add(1)) // keep keys > idx
            } else {
                BTreeMap::new()
            }
        }
    };

    let mut log = RaftLog::new();
    if let Some(snap) = snap_meta {
        log.install_snapshot(snap.last_included_index, snap.last_included_term);
    }

    for (_, entry) in kept {
        log.append(entry).map_err(|e| StorageError::Corruption {
            segment: None,
            offset: e.actual, // the first out-of-order index
        })?;
    }

    Ok(log)
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{LogEntry, snapshot::SnapshotMeta};

    fn entry(index: u64, term: u64) -> LogEntry {
        LogEntry::new(index, term, format!("c{index}").into_bytes())
    }

    #[test]
    fn replay_plain_appends() {
        let ops = vec![
            LogOp::Append(entry(1, 1)),
            LogOp::Append(entry(2, 1)),
            LogOp::Append(entry(3, 1)),
        ];
        let log = replay(&ops, None).unwrap();
        assert_eq!(log.last_index(), 3);
    }

    #[test]
    fn replay_conflict_truncate_then_new_entries() {
        let ops = vec![
            LogOp::Append(entry(1, 1)),
            LogOp::Append(entry(2, 1)),
            LogOp::Append(entry(3, 1)),
            LogOp::TruncateFrom(2),
            LogOp::Append(entry(2, 2)),
            LogOp::Append(entry(3, 2)),
        ];
        let log = replay(&ops, None).unwrap();
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.term_at(2), Ok(2));
        assert_eq!(log.term_at(3), Ok(2));
    }

    #[test]
    fn replay_snapshot_boundary_match_keeps_tail() {
        let snap = SnapshotMeta::new(3, 1);
        let ops = vec![
            LogOp::Append(entry(1, 1)),
            LogOp::Append(entry(2, 1)),
            LogOp::Append(entry(3, 1)),
            LogOp::Append(entry(4, 1)),
        ];
        let log = replay(&ops, Some(&snap)).unwrap();
        assert_eq!(log.last_included_index(), 3);
        assert_eq!(log.last_index(), 4);
    }

    #[test]
    fn replay_snapshot_boundary_conflict_drops_tail() {
        let snap = SnapshotMeta::new(3, 2); // different term
        let ops = vec![
            LogOp::Append(entry(1, 1)),
            LogOp::Append(entry(2, 1)),
            LogOp::Append(entry(3, 1)), // term 1, but snap says term 2
            LogOp::Append(entry(4, 1)),
        ];
        let log = replay(&ops, Some(&snap)).unwrap();
        assert_eq!(log.last_included_index(), 3);
        assert_eq!(log.last_index(), 3); // stale tail dropped
    }

    #[test]
    fn replay_gap_is_corruption() {
        let ops = vec![
            LogOp::Append(entry(1, 1)),
            LogOp::Append(entry(3, 1)), // gap: missing index 2
        ];
        let result = replay(&ops, None);
        assert!(matches!(result, Err(StorageError::Corruption { .. })));
    }
}
