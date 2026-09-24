//! Segment-file durable storage backend.
//!
//! Layout under `data_dir`:
//!   hard_state        — single-record atomic file (write .tmp → fsync → rename → fsync dir)
//!   snapshot          — same atomic pattern
//!   log/{seq:020}.seg — append-only record stream; only the highest-seq segment
//!                       may have a torn tail (truncated on open)
//!
//! `commit` and `recover` are blocking. Callers in an async context must run
//! them via `tokio::task::spawn_blocking` (issue 04).

use std::{
    collections::BTreeMap,
    fmt,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use crate::error::{Error, StorageError};

use super::{
    HardState, Recovered, Storage, WriteBatch,
    record::{self, LogOp},
    replay,
};

const DEFAULT_ROTATE_THRESHOLD: u64 = 16 * 1024 * 1024; // 16 MiB

// ── ActiveSegment ──────────────────────────────────────────────────────────────

struct ActiveSegment {
    seq: u64,
    file: File,
    written: u64,
}

// `File` does not impl `Debug`; provide a manual impl.
impl fmt::Debug for ActiveSegment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActiveSegment")
            .field("seq", &self.seq)
            .field("written", &self.written)
            .finish_non_exhaustive()
    }
}

// ── DiskStorage ────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct DiskStorage {
    data_dir: PathBuf,
    active_seg: Option<ActiveSegment>,
    /// seq → highest index (Append index or TruncateFrom value) the segment touched.
    /// Kept up to date on every write; used for GC.
    seg_max_index: BTreeMap<u64, u64>,
    syncs: u64,
    rotate_threshold: u64,
}

impl DiskStorage {
    /// Opens (or creates) a segment-file backend under `data_dir`.
    ///
    /// Scans existing segments, truncates any torn tail on the last one, and
    /// leaves the backend ready to accept `commit` calls.
    pub fn open(data_dir: impl Into<PathBuf>) -> Result<Self, Error> {
        let data_dir = data_dir.into();
        fs::create_dir_all(&data_dir).map_err(StorageError::Io)?;
        fs::create_dir_all(data_dir.join("log")).map_err(StorageError::Io)?;

        // Remove leftover .tmp files from a previous crash before reading anything.
        for entry in fs::read_dir(&data_dir).map_err(StorageError::Io)? {
            let path = entry.map_err(StorageError::Io)?.path();
            if path.extension().is_some_and(|e| e == "tmp") {
                fs::remove_file(&path).map_err(StorageError::Io)?;
            }
        }

        let mut s = Self {
            data_dir,
            active_seg: None,
            seg_max_index: BTreeMap::new(),
            syncs: 0,
            rotate_threshold: DEFAULT_ROTATE_THRESHOLD,
        };
        s.load_segments()?;
        Ok(s)
    }

    /// Path to the directory holding segment files and atomic state files.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Lowers the segment rotation threshold. Tests set this to a small value
    /// (e.g. 512 bytes) to exercise rotation without writing megabytes.
    #[cfg(test)]
    pub fn set_rotate_threshold(&mut self, bytes: u64) {
        self.rotate_threshold = bytes;
    }

    // ── path helpers ───────────────────────────────────────────────────────────

    fn log_dir(&self) -> PathBuf {
        self.data_dir.join("log")
    }

    fn seg_path(&self, seq: u64) -> PathBuf {
        self.log_dir().join(format!("{seq:020}.seg"))
    }

    fn hard_state_path(&self) -> PathBuf {
        self.data_dir.join("hard_state")
    }

    fn snapshot_path(&self) -> PathBuf {
        self.data_dir.join("snapshot")
    }

    // ── startup ────────────────────────────────────────────────────────────────

    /// Reads every segment in seq order.
    ///
    /// Non-last segments: any CRC error is `Corruption { segment, offset }`.
    /// Last segment: torn tail on the final record is silently truncated; a CRC
    /// error on an earlier record in that segment is still `Corruption`.
    fn load_segments(&mut self) -> Result<(), Error> {
        let mut seqs: Vec<u64> = fs::read_dir(self.log_dir())
            .map_err(StorageError::Io)?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name();
                let s = name.to_str()?.strip_suffix(".seg")?.parse::<u64>().ok()?;
                Some(s)
            })
            .collect();
        seqs.sort_unstable();

        let last_seq = seqs.last().copied();

        for seq in seqs {
            let path = self.seg_path(seq);
            let bytes = fs::read(&path).map_err(StorageError::Io)?;
            let is_last = Some(seq) == last_seq;

            let decoded = record::decode(&bytes).map_err(|e| match e {
                StorageError::Corruption { offset, .. } if !is_last => {
                    Error::Storage(StorageError::Corruption {
                        segment: Some(seq),
                        offset,
                    })
                }
                other => Error::Storage(other),
            })?;

            if let Some(max) = max_index_of(&decoded.ops) {
                self.seg_max_index.insert(seq, max);
            }

            if is_last {
                if decoded.valid_len < bytes.len() {
                    let f = OpenOptions::new()
                        .write(true)
                        .open(&path)
                        .map_err(StorageError::Io)?;
                    f.set_len(decoded.valid_len as u64)
                        .map_err(StorageError::Io)?;
                    f.sync_all().map_err(StorageError::Io)?;
                }
                let file = OpenOptions::new()
                    .append(true)
                    .open(&path)
                    .map_err(StorageError::Io)?;
                self.active_seg = Some(ActiveSegment {
                    seq,
                    file,
                    written: decoded.valid_len as u64,
                });
            }
        }
        Ok(())
    }

    // ── write helpers ──────────────────────────────────────────────────────────

    fn open_new_segment(&mut self) -> Result<(), Error> {
        // Derive seq from the highest closed segment so that re-use is impossible
        // after a rotation that sets active_seg = None.
        let seq = self.seg_max_index.keys().max().copied().unwrap_or(0) + 1;
        let path = self.seg_path(seq);
        let file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)
            .map_err(StorageError::Io)?;
        self.active_seg = Some(ActiveSegment {
            seq,
            file,
            written: 0,
        });
        Ok(())
    }

    /// Encodes `op` and appends it to the active segment.
    ///
    /// If the write would push the segment past `rotate_threshold`, the active
    /// segment is fsynced first (`syncs += 1`), then a new segment opens.
    fn write_log_op(&mut self, op: &LogOp) -> Result<(), Error> {
        let mut buf = Vec::new();
        record::encode(op, &mut buf)?;

        let would_overflow = self
            .active_seg
            .as_ref()
            .is_some_and(|s| s.written + buf.len() as u64 > self.rotate_threshold);

        if would_overflow {
            if let Some(seg) = self.active_seg.as_mut() {
                seg.file.sync_all().map_err(StorageError::Io)?;
                self.syncs += 1;
            }
            self.active_seg = None;
        }

        if self.active_seg.is_none() {
            self.open_new_segment()?;
        }

        if let Some(seg) = self.active_seg.as_mut() {
            seg.file.write_all(&buf).map_err(StorageError::Io)?;
            seg.written += buf.len() as u64;

            let touched = match op {
                LogOp::Append(e) => e.index,
                LogOp::TruncateFrom(i) => *i,
            };
            let seq = seg.seq;
            let max = self.seg_max_index.entry(seq).or_insert(0);
            if touched > *max {
                *max = touched;
            }
        }

        Ok(())
    }

    /// Writes `bytes` to `path` atomically: `.tmp` → fsync (`syncs += 1`) →
    /// rename → fsync dir (`syncs += 1`).
    fn atomic_write(&mut self, path: &Path, bytes: &[u8]) -> Result<(), Error> {
        let tmp = path.with_extension("tmp");
        {
            let mut f = File::create(&tmp).map_err(StorageError::Io)?;
            f.write_all(bytes).map_err(StorageError::Io)?;
            f.sync_all().map_err(StorageError::Io)?;
            self.syncs += 1;
        }
        fs::rename(&tmp, path).map_err(StorageError::Io)?;
        // fsync the directory so the rename survives a power loss.
        let dir = File::open(path.parent().unwrap_or(Path::new("."))).map_err(StorageError::Io)?;
        dir.sync_all().map_err(StorageError::Io)?;
        self.syncs += 1;
        Ok(())
    }

    /// Deletes a *prefix* of segments fully below `snapshot_index`.
    ///
    /// Two constraints, both load-bearing:
    ///
    /// 1. **Prefix only.** Deleting a segment removes its records from the replay
    ///    stream. A later `TruncateFrom` can depend on appends in an earlier
    ///    segment, so removing one from the middle changes what replay rebuilds.
    ///    Iteration stops at the first segment that is not fully covered.
    ///
    /// 2. **Strictly below the boundary** (`max_idx < snapshot_index`, not `<=`).
    ///    [`replay`] decides whether to keep entries above the snapshot by
    ///    comparing the term of the entry *at* `snapshot_index`. Discarding that
    ///    entry turns a detected conflict into "already compacted, keep the tail",
    ///    which resurrects entries from a branch the snapshot supersedes.
    fn gc_segments(&mut self, snapshot_index: u64) -> Result<(), Error> {
        let active_seq = self.active_seg.as_ref().map(|s| s.seq);
        let mut to_delete: Vec<u64> = Vec::new();
        for (&seq, &max_idx) in &self.seg_max_index {
            if max_idx >= snapshot_index || Some(seq) == active_seq {
                break;
            }
            to_delete.push(seq);
        }
        for seq in to_delete {
            fs::remove_file(self.seg_path(seq)).map_err(StorageError::Io)?;
            self.seg_max_index.remove(&seq);
        }
        Ok(())
    }
}

// ── Storage impl ───────────────────────────────────────────────────────────────

impl Storage for DiskStorage {
    /// Durably commits a batch in fixed order: snapshot → hard state → log.
    ///
    /// Hard state precedes log so a crash mid-batch never leaves higher-term
    /// log entries under a stale `currentTerm`/`votedFor` (Raft election safety).
    /// Snapshot still comes first: the boundary must be durable before any
    /// truncate that depends on it.
    ///
    /// Sync budget per batch: 1 per touched log segment + 2 per atomic file
    /// written (file fsync + directory rename fsync). An empty batch is a no-op.
    fn commit(&mut self, batch: &WriteBatch) -> Result<(), Error> {
        if batch.snapshot.is_none() && batch.log.is_empty() && batch.hard_state.is_none() {
            return Ok(());
        }

        // 1. Snapshot — atomic write, then GC segments fully below its boundary.
        if let Some(snap) = &batch.snapshot {
            let snap_idx = snap.meta.last_included_index;
            let mut buf = Vec::new();
            record::encode_snapshot(snap, &mut buf)?;
            let path = self.snapshot_path();
            self.atomic_write(&path, &buf)?;
            self.gc_segments(snap_idx)?;
        }

        // 2. Hard state — atomic write before log mutations in the same batch.
        if let Some(hs) = &batch.hard_state {
            let mut buf = Vec::new();
            record::encode_hard_state(hs, &mut buf)?;
            let path = self.hard_state_path();
            self.atomic_write(&path, &buf)?;
        }

        // 3. Log ops — write all, then one fsync on the active segment.
        //    write_log_op already fsyncs the old segment when rotating.
        if !batch.log.is_empty() {
            for op in &batch.log {
                self.write_log_op(op)?;
            }
            if let Some(seg) = self.active_seg.as_mut() {
                seg.file.sync_all().map_err(StorageError::Io)?;
                self.syncs += 1;
            }
        }

        Ok(())
    }

    /// Rebuilds durable state from disk.
    ///
    /// Segments were already validated and torn tails truncated by `open`.
    /// This re-reads them and passes all ops through `replay`.
    fn recover(&self) -> Result<Recovered, Error> {
        let hard_state = if self.hard_state_path().exists() {
            let bytes = fs::read(self.hard_state_path()).map_err(StorageError::Io)?;
            record::decode_hard_state(&bytes)?
        } else {
            HardState::default()
        };

        let snapshot = if self.snapshot_path().exists() {
            let bytes = fs::read(self.snapshot_path()).map_err(StorageError::Io)?;
            Some(record::decode_snapshot(&bytes)?)
        } else {
            None
        };

        // Include an empty active segment that has no entry in seg_max_index yet.
        let mut seqs: Vec<u64> = self.seg_max_index.keys().copied().collect();
        if let Some(seg) = &self.active_seg
            && !self.seg_max_index.contains_key(&seg.seq)
        {
            seqs.push(seg.seq);
        }
        seqs.sort_unstable();

        let mut all_ops: Vec<LogOp> = Vec::new();
        for seq in seqs {
            // Segments were validated at open; no torn-tail concern here.
            let bytes = fs::read(self.seg_path(seq)).map_err(StorageError::Io)?;
            let decoded = record::decode(&bytes)?;
            all_ops.extend(decoded.ops);
        }

        let snap_meta = snapshot.as_ref().map(|s| &s.meta);
        let log = replay(&all_ops, snap_meta)?;

        Ok(Recovered {
            hard_state,
            log,
            snapshot,
        })
    }

    fn sync_count(&self) -> u64 {
        self.syncs
    }
}

// ── helpers ────────────────────────────────────────────────────────────────────

fn max_index_of(ops: &[LogOp]) -> Option<u64> {
    ops.iter()
        .map(|op| match op {
            LogOp::Append(e) => e.index,
            LogOp::TruncateFrom(i) => *i,
        })
        .max()
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{LogEntry, Snapshot, snapshot::SnapshotMeta};

    fn entry(index: u64) -> LogEntry {
        LogEntry::new(index, 1, format!("cmd{index}").into_bytes())
    }

    fn append_batch(entries: impl IntoIterator<Item = LogEntry>) -> WriteBatch {
        WriteBatch {
            hard_state: None,
            log: entries.into_iter().map(LogOp::Append).collect(),
            snapshot: None,
        }
    }

    fn open(dir: &std::path::Path) -> DiskStorage {
        DiskStorage::open(dir).expect("open DiskStorage")
    }

    // ── torn tail ─────────────────────────────────────────────────────────────

    #[test]
    #[cfg_attr(miri, ignore)]
    fn torn_tail_at_every_byte_returns_clean_prefix() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = open(tmp.path());
        // Write entry 1 (will be complete), then entry 2 (will be partially written).
        s.commit(&append_batch([entry(1), entry(2)])).unwrap();

        // Find where the first record ends inside the active segment.
        let seg_files: Vec<_> = fs::read_dir(tmp.path().join("log"))
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .collect();
        assert_eq!(seg_files.len(), 1);
        let seg_path = &seg_files[0];

        let full_bytes = fs::read(seg_path).unwrap();
        // Encode just entry(1) to find where it ends.
        let mut first_only = Vec::new();
        record::encode(&LogOp::Append(entry(1)), &mut first_only).unwrap();
        let first_end = first_only.len();

        // Cutting anywhere inside the second record yields only entry 1 on reopen.
        for cut in first_end..full_bytes.len() {
            fs::write(seg_path, &full_bytes[..cut]).unwrap();
            let s2 = open(tmp.path());
            let r = s2.recover().unwrap();
            assert_eq!(r.log.last_index(), 1, "cut at {cut}: expected last_index=1");
        }
    }

    // ── corruption ────────────────────────────────────────────────────────────

    #[test]
    #[cfg_attr(miri, ignore)]
    fn corruption_in_non_final_record_of_last_segment_errors_on_open() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = open(tmp.path());
        s.commit(&append_batch([entry(1), entry(2)])).unwrap();

        let seg_path = fs::read_dir(tmp.path().join("log"))
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .next()
            .unwrap();

        let mut bytes = fs::read(&seg_path).unwrap();
        // Flip a byte inside the first record's payload (offset 9 = kind + first byte).
        bytes[9] ^= 0xFF;
        fs::write(&seg_path, &bytes).unwrap();

        let result = DiskStorage::open(tmp.path());
        assert!(
            matches!(
                result,
                Err(crate::error::Error::Storage(
                    StorageError::Corruption { .. }
                ))
            ),
            "expected Corruption on open, got {result:?}"
        );
    }

    // ── leftover .tmp ─────────────────────────────────────────────────────────

    #[test]
    #[cfg_attr(miri, ignore)]
    fn leftover_tmp_file_is_removed_on_open() {
        let tmp = tempfile::tempdir().unwrap();
        // Create a leftover .tmp as if a crash happened mid-rename.
        fs::write(tmp.path().join("hard_state.tmp"), b"garbage").unwrap();

        let result = DiskStorage::open(tmp.path());
        assert!(
            result.is_ok(),
            "open must succeed even with a leftover .tmp"
        );
        assert!(
            !tmp.path().join("hard_state.tmp").exists(),
            ".tmp must be deleted on open"
        );
    }

    // ── snapshot + GC + reopen ────────────────────────────────────────────────

    #[test]
    #[cfg_attr(miri, ignore)]
    fn snapshot_gc_and_reopen_returns_same_state() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = open(tmp.path());
        s.set_rotate_threshold(256); // small threshold to force rotation

        // Write entries 1-5 across multiple segments.
        for i in 1u64..=5 {
            s.commit(&append_batch([entry(i)])).unwrap();
        }

        // Snapshot covering up to index 3; this should GC the segments for 1-3.
        let snap = Snapshot::new(SnapshotMeta::new(3, 1), b"sm-state".to_vec());
        s.commit(&WriteBatch {
            snapshot: Some(snap.clone()),
            log: vec![],
            hard_state: None,
        })
        .unwrap();

        // Reopen to simulate a restart.
        let s2 = open(tmp.path());
        let r = s2.recover().unwrap();

        assert_eq!(
            r.log.last_included_index(),
            3,
            "snapshot boundary must be 3"
        );
        // Entries above the snapshot must still be present.
        assert!(r.log.last_index() >= 4, "entries 4 and 5 must survive GC");
        assert_eq!(r.snapshot, Some(snap), "recovered snapshot must match");
    }

    // ── segment rotation ──────────────────────────────────────────────────────

    #[test]
    #[cfg_attr(miri, ignore)]
    fn segment_rotation_and_full_recovery() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = open(tmp.path());
        s.set_rotate_threshold(128); // force frequent rotation

        // Write 15 entries; each ~50+ bytes, so we get multiple segments.
        for i in 1u64..=15 {
            s.commit(&append_batch([entry(i)])).unwrap();
        }

        let seg_count = fs::read_dir(tmp.path().join("log"))
            .unwrap()
            .filter_map(|e| e.ok())
            .count();
        assert!(seg_count > 1, "expected multiple segments, got {seg_count}");

        let s2 = open(tmp.path());
        let r = s2.recover().unwrap();
        assert_eq!(r.log.last_index(), 15, "all entries must be recovered");
    }

    // ── GC differential vs the no-GC memory backend ───────────────────────────

    /// `MemoryStorage` never discards log records, so its `recover()` is the
    /// oracle for what segment GC must preserve. Any divergence is a GC bug.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn disk_gc_never_diverges_from_memory_backend() {
        use crate::raft::storage::MemoryStorage;

        let mut checked = 0u32;
        for old_len in 2u64..=6 {
            for trunc in 1..=old_len {
                for new_len in 0u64..=3 {
                    let final_last = trunc - 1 + new_len;
                    if final_last == 0 {
                        continue;
                    }
                    // Old branch (term 1), then a conflict truncate, then a new
                    // branch at term 2 — the shape the review's counterexample uses.
                    let mut ops: Vec<LogOp> = (1..=old_len)
                        .map(|i| LogOp::Append(LogEntry::new(i, 1, vec![b'o'])))
                        .collect();
                    ops.push(LogOp::TruncateFrom(trunc));
                    for k in 0..new_len {
                        ops.push(LogOp::Append(LogEntry::new(trunc + k, 2, vec![b'n'])));
                    }

                    for snap_idx in 1..=final_last {
                        let snap_term = if snap_idx >= trunc { 2 } else { 1 };
                        let snap =
                            Snapshot::new(SnapshotMeta::new(snap_idx, snap_term), b"sm".to_vec());

                        let tmp = tempfile::tempdir().unwrap();
                        let mut disk = open(tmp.path());
                        disk.set_rotate_threshold(1); // one op per segment
                        let mut mem = MemoryStorage::default();

                        // Same batch sequence into both backends.
                        for op in &ops {
                            let b = WriteBatch {
                                hard_state: None,
                                log: vec![op.clone()],
                                snapshot: None,
                            };
                            disk.commit(&b).unwrap();
                            mem.commit(&b).unwrap();
                        }
                        let snap_batch = WriteBatch {
                            hard_state: None,
                            log: vec![],
                            snapshot: Some(snap.clone()),
                        };
                        disk.commit(&snap_batch).unwrap();
                        mem.commit(&snap_batch).unwrap();

                        // Reopen disk to force a full segment rescan (a real restart).
                        let disk2 = open(tmp.path());
                        let got = disk2.recover().unwrap();
                        let want = mem.recover().unwrap();

                        assert_eq!(
                            got, want,
                            "GC diverged: old_len={old_len} trunc={trunc} \
                             new_len={new_len} snap_idx={snap_idx}"
                        );
                        checked += 1;
                    }
                }
            }
        }
        assert!(checked > 100, "expected a meaningful number of cases");
    }

    /// A snapshot whose term conflicts with the local entry at that index (the
    /// follower InstallSnapshot case) must not resurrect the superseded branch,
    /// even with no accompanying `TruncateFrom` in the stream.
    ///
    /// Regression: GC used to delete the segment holding the entry *at* the
    /// boundary, so `replay` saw `None` there, concluded "already compacted",
    /// and kept a tail belonging to the branch the snapshot replaced.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn conflicting_snapshot_does_not_resurrect_superseded_branch() {
        use crate::raft::storage::MemoryStorage;

        let ops: Vec<LogOp> = (1..=5)
            .map(|i| LogOp::Append(LogEntry::new(i, 1, vec![b'o'])))
            .collect();
        // Snapshot at index 4 but term 9 — conflicts with the local 4@term1.
        let snap = Snapshot::new(SnapshotMeta::new(4, 9), b"sm".to_vec());

        let tmp = tempfile::tempdir().unwrap();
        let mut disk = open(tmp.path());
        disk.set_rotate_threshold(1);
        let mut mem = MemoryStorage::default();

        for op in &ops {
            let b = WriteBatch {
                hard_state: None,
                log: vec![op.clone()],
                snapshot: None,
            };
            disk.commit(&b).unwrap();
            mem.commit(&b).unwrap();
        }
        let sb = WriteBatch {
            hard_state: None,
            log: vec![],
            snapshot: Some(snap),
        };
        disk.commit(&sb).unwrap();
        mem.commit(&sb).unwrap();

        let disk2 = open(tmp.path());
        let got = disk2.recover().unwrap();
        let want = mem.recover().unwrap();
        assert_eq!(
            got.log.last_index(),
            want.log.last_index(),
            "disk last_index={} but memory oracle says {}",
            got.log.last_index(),
            want.log.last_index()
        );
    }

    // ── hard state atomicity ──────────────────────────────────────────────────

    #[test]
    #[cfg_attr(miri, ignore)]
    fn hard_state_persists_and_recovers() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = open(tmp.path());
        let hs = HardState {
            current_term: 7,
            voted_for: Some(2),
        };
        s.commit(&WriteBatch {
            hard_state: Some(hs.clone()),
            log: vec![],
            snapshot: None,
        })
        .unwrap();

        let s2 = open(tmp.path());
        let r = s2.recover().unwrap();
        assert_eq!(r.hard_state, hs);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn leftover_tmp_keeps_previous_hard_state() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = open(tmp.path());
        let hs = HardState {
            current_term: 3,
            voted_for: Some(1),
        };
        s.commit(&WriteBatch {
            hard_state: Some(hs.clone()),
            log: vec![],
            snapshot: None,
        })
        .unwrap();

        // Crash mid-replace: new temp present, rename never happened.
        fs::write(tmp.path().join("hard_state.tmp"), b"partial-garbage").unwrap();

        let s2 = open(tmp.path());
        let r = s2.recover().unwrap();
        assert_eq!(r.hard_state, hs, "previous hard state must win over leftover tmp");
        assert!(
            !tmp.path().join("hard_state.tmp").exists(),
            "tmp cleaned on open"
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn truncate_then_snapshot_gc_does_not_resurrect_suffix() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = open(tmp.path());
        s.set_rotate_threshold(64);

        // Branch A: entries 1..4 term 1 across segments.
        for i in 1u64..=4 {
            s.commit(&WriteBatch {
                hard_state: None,
                log: vec![LogOp::Append(LogEntry::new(
                    i,
                    1,
                    format!("a{i}").into_bytes(),
                ))],
                snapshot: None,
            })
            .unwrap();
        }
        // Conflict truncate + new branch at index 3 term 2.
        s.commit(&WriteBatch {
            hard_state: None,
            log: vec![
                LogOp::TruncateFrom(3),
                LogOp::Append(LogEntry::new(3, 2, b"b3".to_vec())),
                LogOp::Append(LogEntry::new(4, 2, b"b4".to_vec())),
            ],
            snapshot: None,
        })
        .unwrap();

        // Snapshot covering up to 4 on the new branch.
        let snap = Snapshot::new(SnapshotMeta::new(4, 2), b"sm".to_vec());
        s.commit(&WriteBatch {
            hard_state: None,
            log: vec![],
            snapshot: Some(snap.clone()),
        })
        .unwrap();

        let r = open(tmp.path()).recover().unwrap();
        assert_eq!(r.snapshot.as_ref().map(|s| s.meta.clone()), Some(snap.meta));
        assert_eq!(r.log.last_included_index(), 4);
        assert_eq!(r.log.last_included_term(), 2);
        // Must not resurrect pre-truncate term-1 entries above the boundary.
        assert_eq!(r.log.last_index(), 4);
        assert!(
            r.log.entry(5).is_err(),
            "no resurrected suffix after truncate+snapshot GC"
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn truncate_from_is_durable_across_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = open(tmp.path());
        s.commit(&append_batch([entry(1), entry(2), entry(3)]))
            .unwrap();
        s.commit(&WriteBatch {
            hard_state: None,
            log: vec![
                LogOp::TruncateFrom(2),
                LogOp::Append(LogEntry::new(2, 2, b"new".to_vec())),
            ],
            snapshot: None,
        })
        .unwrap();

        let r = open(tmp.path()).recover().unwrap();
        assert_eq!(r.log.last_index(), 2);
        assert_eq!(r.log.term_at(2).unwrap(), 2);
    }
}
