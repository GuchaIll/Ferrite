//! Segment-file durable storage backend (issue 03).
//!
//! Stubbed while issue 06 lands the snapshot/compaction protocol path.
//! The Raft core never calls this module directly; only the driver does.

use std::path::PathBuf;

use crate::error::{Error, StorageError};
use crate::raft::{LogEntry, Snapshot};

use super::HardState;

/// Append-only segment-file storage. Implementation lands in issue 03.
#[derive(Debug)]
pub struct DiskStorage {
    data_dir: PathBuf,
}

impl DiskStorage {
    /// Opens (or creates) a segment-file backend under `data_dir`.
    pub fn open(data_dir: impl Into<PathBuf>) -> Result<Self, Error> {
        let data_dir = data_dir.into();
        Ok(Self { data_dir })
    }

    /// Directory holding segment files and hard-state.
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    /// Persists election metadata. Issue 03: atomic temp/fsync/rename.
    pub fn persist_hard_state(&mut self, _hard_state: &HardState) -> Result<(), Error> {
        Err(StorageError::Internal(
            "DiskStorage::persist_hard_state not implemented (issue 03)".into(),
        )
        .into())
    }

    /// Appends log entries that must be durable before an ack. Issue 03.
    pub fn append_entries(&mut self, _entries: &[LogEntry]) -> Result<(), Error> {
        Err(StorageError::Internal(
            "DiskStorage::append_entries not implemented (issue 03)".into(),
        )
        .into())
    }

    /// Records a durable suffix truncation. Issue 03.
    pub fn truncate_from(&mut self, _index: u64) -> Result<(), Error> {
        Err(StorageError::Internal(
            "DiskStorage::truncate_from not implemented (issue 03)".into(),
        )
        .into())
    }

    /// Atomically persists a snapshot before the log prefix is discarded. Issue 03 / 06.
    pub fn persist_snapshot(&mut self, _snapshot: &Snapshot) -> Result<(), Error> {
        Err(StorageError::Internal(
            "DiskStorage::persist_snapshot not implemented (issue 03)".into(),
        )
        .into())
    }

    /// Loads durable state after restart. Issue 03.
    pub fn recover(&self) -> Result<(HardState, Vec<LogEntry>, Option<Snapshot>), Error> {
        Err(StorageError::Internal(
            "DiskStorage::recover not implemented (issue 03)".into(),
        )
        .into())
    }
}
