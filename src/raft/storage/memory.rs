//! In-memory storage backend for the simulator and unit tests.
//!
//! Holds the same byte stream that a disk segment file would hold, so
//! `record::decode` + `replay` work identically on both backends.

use crate::{error::Error, raft::Snapshot};

use super::{HardState, Recovered, Storage, WriteBatch, record, replay};

#[derive(Debug, Default, Clone)]
pub struct MemoryStorage {
    /// Encoded log record stream — same byte layout as a disk segment file.
    log_bytes: Vec<u8>,
    hard_state: Option<HardState>,
    snapshot: Option<Snapshot>,
    syncs: u64,
    /// When set (tests only), the next `commit` returns this many failures first.
    #[cfg(test)]
    fail_commits: u32,
}

impl MemoryStorage {
    /// Exposes the raw log bytes for byte-level corruption tests (step 5).
    #[cfg(test)]
    pub fn log_bytes_mut(&mut self) -> &mut Vec<u8> {
        &mut self.log_bytes
    }

    /// Next `n` commits fail with a synthetic IO error (fail-closed driver tests).
    #[cfg(test)]
    pub fn fail_next_commits(&mut self, n: u32) {
        self.fail_commits = n;
    }
}

impl Storage for MemoryStorage {
    fn commit(&mut self, batch: &WriteBatch) -> Result<(), Error> {
        if batch.is_empty() {
            return Ok(());
        }

        #[cfg(test)]
        if self.fail_commits > 0 {
            self.fail_commits -= 1;
            return Err(Error::Storage(crate::error::StorageError::Io(
                std::io::Error::other("injected commit failure"),
            )));
        }

        // Drop any torn tail left by a prior partial write before extending.
        let decoded = record::decode(&self.log_bytes)?;
        if decoded.valid_len < self.log_bytes.len() {
            self.log_bytes.truncate(decoded.valid_len);
        }

        // Same durable order as DiskStorage: snapshot → hard state → log.
        if let Some(snap) = &batch.snapshot {
            // Clone justified: batch borrows snap; MemoryStorage needs ownership.
            self.snapshot = Some(snap.clone());
        }

        if let Some(hs) = &batch.hard_state {
            // Clone justified: batch borrows hs; MemoryStorage needs ownership.
            self.hard_state = Some(hs.clone());
        }

        for op in &batch.log {
            record::encode(op, &mut self.log_bytes)?;
        }

        // One "sync" per non-empty batch models the fsync cost in group-commit tests.
        self.syncs += 1;
        Ok(())
    }

    fn recover(&self) -> Result<Recovered, Error> {
        let decoded = record::decode(&self.log_bytes)?;
        let snap_meta = self.snapshot.as_ref().map(|s| &s.meta);
        let log = replay(&decoded.ops, snap_meta)?;
        let hard_state = self.hard_state.clone().unwrap_or_default();
        Ok(Recovered {
            hard_state,
            log,
            // Clone justified: MemoryStorage retains snapshot; Recovered needs owned copy.
            snapshot: self.snapshot.clone(),
        })
    }

    fn sync_count(&self) -> u64 {
        self.syncs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::storage::LogOp;
    use crate::raft::{LogEntry, Snapshot, snapshot::SnapshotMeta};

    fn entry(index: u64) -> LogEntry {
        LogEntry::new(index, 1, format!("c{index}").into_bytes())
    }

    fn batch_append(entries: impl IntoIterator<Item = LogEntry>) -> WriteBatch {
        WriteBatch {
            hard_state: None,
            log: entries.into_iter().map(LogOp::Append).collect(),
            snapshot: None,
        }
    }

    #[test]
    fn empty_batch_is_noop_and_no_sync() {
        let mut s = MemoryStorage::default();
        s.commit(&WriteBatch::default()).unwrap();
        assert_eq!(s.sync_count(), 0);
    }

    #[test]
    fn commit_then_recover_returns_same_entries() {
        let mut s = MemoryStorage::default();
        s.commit(&batch_append([entry(1), entry(2), entry(3)]))
            .unwrap();
        let r = s.recover().unwrap();
        assert_eq!(r.log.last_index(), 3);
        assert_eq!(r.hard_state.current_term, 0);
        assert!(r.snapshot.is_none());
    }

    #[test]
    fn hard_state_survives_recover() {
        let mut s = MemoryStorage::default();
        let hs = HardState {
            current_term: 5,
            voted_for: Some(2),
        };
        s.commit(&WriteBatch {
            hard_state: Some(hs.clone()),
            log: vec![],
            snapshot: None,
        })
        .unwrap();
        let r = s.recover().unwrap();
        assert_eq!(r.hard_state, hs);
    }

    #[test]
    fn snapshot_survives_recover() {
        let mut s = MemoryStorage::default();
        let snap = Snapshot::new(SnapshotMeta::new(3, 1), b"sm".to_vec());
        s.commit(&WriteBatch {
            hard_state: None,
            log: vec![],
            snapshot: Some(snap.clone()),
        })
        .unwrap();
        let r = s.recover().unwrap();
        assert_eq!(r.snapshot, Some(snap));
        assert_eq!(r.log.last_included_index(), 3);
    }

    #[test]
    fn sync_count_increments_once_per_non_empty_batch() {
        let mut s = MemoryStorage::default();
        s.commit(&batch_append([entry(1)])).unwrap();
        s.commit(&batch_append([entry(2)])).unwrap();
        assert_eq!(s.sync_count(), 2);
    }

    #[test]
    fn byte_corruption_in_non_final_record_returns_corruption_error() {
        let mut s = MemoryStorage::default();
        s.commit(&batch_append([entry(1), entry(2)])).unwrap();
        // Flip a byte inside the first record's payload (offset 9 = kind + first byte).
        s.log_bytes_mut()[9] ^= 0xFF;
        let result = s.recover();
        assert!(result.is_err());
    }

    #[test]
    fn commit_failure_injected() {
        let mut s = MemoryStorage::default();
        s.fail_next_commits(1);
        s.commit(&batch_append([entry(1), entry(2), entry(3)]))
            .unwrap_err();
        assert_eq!(s.sync_count(), 0);
        // Subsequent commit succeeds after the injected failure is consumed.
        s.commit(&batch_append([entry(1)])).unwrap();
        assert_eq!(s.sync_count(), 1);
    }
}
