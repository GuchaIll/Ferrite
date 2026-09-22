//! Raft log and log entry definitions.

use std::fmt;

use serde::{Deserialize, Serialize};

/// One replicated state-machine operation.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    /// One-based, contiguous position in the Raft log.
    pub index: u64,
    /// Term of the leader that created the entry.
    pub term: u64,
    /// Serialized state-machine command, such as Put, Delete, or CAS.
    pub command: Vec<u8>,
}

impl LogEntry {
    /// Creates a log entry. `RaftLog` validates its index when appending it.
    pub fn new(index: u64, term: u64, command: Vec<u8>) -> Self {
        Self {
            index,
            term,
            command,
        }
    }
}
/// An operation that would violate Raft's contiguous-log invariant.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LogIndexError {
    /// Index the log needed next.
    pub expected: u64,
    /// Index supplied by the caller.
    pub actual: u64,
}

impl fmt::Display for LogIndexError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "expected contiguous log index {}, received {}",
            self.expected, self.actual
        )
    }
}

impl std::error::Error for LogIndexError {}

/// The mutation [`RaftLog::append_from_leader`] applied to the local log.
///
/// Empty when every incoming entry already matched. Otherwise the driver must
/// make `truncated_from` (if any) and then `appended` durable before the
/// success reply that depends on them.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct AppendOutcome {
    /// First index removed from the local suffix, if a conflict removed any.
    pub truncated_from: Option<u64>,
    /// Entries appended, in index order.
    pub appended: Vec<LogEntry>,
}

impl AppendOutcome {
    /// Returns whether the log was left unchanged.
    pub fn is_empty(&self) -> bool {
        self.truncated_from.is_none() && self.appended.is_empty()
    }
}

/// A log lookup that cannot be satisfied without inventing a wrong answer.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum LogLookupError {
    /// `requested` is past the log's tail; it has never been appended.
    OutOfRange { requested: u64, last_index: u64 },
    /// `requested` was discarded by [`RaftLog::compact`]; a snapshot covers it.
    CompactedAway { requested: u64, start_index: u64 },
}

impl fmt::Display for LogLookupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutOfRange {
                requested,
                last_index,
            } => write!(
                formatter,
                "log index {requested} is out of range (last index is {last_index})"
            ),
            Self::CompactedAway {
                requested,
                start_index,
            } => write!(
                formatter,
                "log index {requested} was compacted away (log starts at {start_index})"
            ),
        }
    }
}

impl std::error::Error for LogLookupError {}

/// Ordered, contiguous entries held by one Raft node.
///
/// Index zero is not an entry. It is the empty-prefix sentinel used by
/// `AppendEntries` when a follower must receive the first entry.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RaftLog {
    entries: Vec<LogEntry>,
    /// Index of the oldest entry still held. Entries below this were removed
    /// by [`RaftLog::compact`]; nothing has been compacted while this is 1.
    start_index: u64,
    last_included_term: u64, //term at start_index - 1, after compaction
}

impl Default for RaftLog {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            start_index: 1,
            last_included_term: 0,
        }
    }
}

impl RaftLog {
    /// Creates an empty log.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the index at which the next entry must be appended.
    pub fn next_index(&self) -> u64 {
        self.last_index().saturating_add(1)
    }

    /// Returns the last entry index, or the last compacted index for an
    /// otherwise-empty log, or zero if nothing has ever been appended.
    pub fn last_index(&self) -> u64 {
        self.entries
            .last()
            .map_or(self.start_index.saturating_sub(1), |entry| entry.index)
    }

    /// Returns the last entry term.
    ///
    /// After compaction leaves an empty suffix, this is the snapshot's
    /// `last_included_term` rather than zero.
    pub fn last_term(&self) -> u64 {
        self.entries
            .last()
            .map_or(self.last_included_term, |entry| entry.term)
    }

    /// Returns an entry at a one-based Raft index.
    pub fn entry(&self, index: u64) -> Result<&LogEntry, LogLookupError> {
        if index != 0 && index < self.start_index {
            return Err(LogLookupError::CompactedAway {
                requested: index,
                start_index: self.start_index,
            });
        }

        let out_of_range = || LogLookupError::OutOfRange {
            requested: index,
            last_index: self.last_index(),
        };
        let offset = index
            .checked_sub(self.start_index)
            .ok_or_else(out_of_range)?;
        let offset = usize::try_from(offset).map_err(|_| out_of_range())?;
        self.entries.get(offset).ok_or_else(out_of_range)
    }

    /// Returns the term at `index`; the empty prefix at index zero has term zero.
    ///
    /// `term_at(last_included_index)` returns the snapshot's included term so the
    /// first `AppendEntries` after compaction can still anchor on the boundary.
    pub fn term_at(&self, index: u64) -> Result<u64, LogLookupError> {
        if index == 0 {
            return Ok(0);
        }
        // Snapshot boundary: index == start_index - 1 == last_included_index.
        if index + 1 == self.start_index {
            return Ok(self.last_included_term);
        }
        if index < self.start_index {
            return Err(LogLookupError::CompactedAway {
                requested: index,
                start_index: self.start_index,
            });
        }
        self.entry(index).map(|entry| entry.term)
    }

    /// Checks the `prev_log_index` / `prev_log_term` AppendEntries condition.
    pub fn contains(&self, index: u64, term: u64) -> bool {
        self.term_at(index) == Ok(term)
    }

    /// Returns copies of entries beginning at `index`, or no entries past the tail.
    ///
    /// An `index` before the oldest held entry still returns every entry the
    /// log holds, since all of them satisfy "at or after `index`".
    pub fn entries_from(&self, index: u64) -> Vec<LogEntry> {
        if index == 0 || index < self.start_index {
            return self.entries.clone();
        }

        let Some(offset) = index
            .checked_sub(self.start_index)
            .and_then(|value| usize::try_from(value).ok())
        else {
            return Vec::new();
        };
        self.entries.get(offset..).unwrap_or_default().to_vec()
    }

    /// Index of the last entry covered by a snapshot (0 before any compaction).
    pub fn last_included_index(&self) -> u64 {
        self.start_index.saturating_sub(1)
    }

    pub fn last_included_term(&self) -> u64 {
        self.last_included_term
    }

    /// Discards entries at or before `last_included_index`, recording that a
    /// snapshot now covers them. A no-op if nothing new is being compacted.
    pub fn compact(&mut self, last_included_index: u64, last_included_term: u64) {
        let new_start = last_included_index.saturating_add(1);
        if new_start <= self.start_index {
            return;
        }

        let discard = new_start.saturating_sub(self.start_index);
        let discard = usize::try_from(discard)
            .unwrap_or(self.entries.len())
            .min(self.entries.len());
        self.entries.drain(..discard);
        self.start_index = new_start;
        self.last_included_term = last_included_term;
    }

    ///Follower Install: keeping the suffix if (idx, term) matches locally else clear
    pub fn install_snapshot(&mut self, last_included_index: u64, last_included_term: u64) {
        if self.contains(last_included_index, last_included_term) {
            //Boundary matches with local entry; the suffix after is still valid
            self.compact(last_included_index, last_included_term);
        }
        else{
            //Snapshot superceeds or conflicts with local entry, discard all
            self.entries.clear();
            self.start_index = last_included_index.saturating_add(1);
            self.last_included_term = last_included_term;
        }
    }

    /// Returns the index of the oldest entry still held.
    pub fn start_index(&self) -> u64 {
        self.start_index
    }

    /// Appends an entry only when it directly follows the current log tail.
    pub fn append(&mut self, entry: LogEntry) -> Result<(), LogIndexError> {
        self.ensure_next_index(entry.index)?;
        self.entries.push(entry);
        Ok(())
    }

    /// Merges entries sent by a leader after the caller has verified the common prefix.
    ///
    /// Entries with equal index and term are retained. At the first term conflict,
    /// the local suffix is replaced by the leader's suffix. An invalid incoming batch
    /// is rejected before changing the local log.
    ///
    /// Returns the exact mutation performed so the driver can make it durable.
    pub fn append_from_leader(
        &mut self,
        incoming: &[LogEntry],
    ) -> Result<AppendOutcome, LogIndexError> {
        Self::validate_contiguous(incoming)?;

        let first_to_append = incoming
            .iter()
            .position(|entry| match self.entry(entry.index) {
                Ok(local) => local.term != entry.term,
                Err(_) => true,
            });

        let Some(position) = first_to_append else {
            return Ok(AppendOutcome::default());
        };

        let first_index = incoming[position].index;
        let truncated_from = (first_index <= self.last_index()).then_some(first_index);
        self.truncate_from(first_index)?;

        let appended = incoming[position..].to_vec();
        for entry in &appended {
            self.append(entry.clone())?;
        }
        Ok(AppendOutcome {
            truncated_from,
            appended,
        })
    }

    /// Removes every entry whose index is at least `index`.
    ///
    /// An `index` below [`RaftLog::start_index`] is rejected rather than
    /// clamped: those entries are covered by a snapshot, and truncating there
    /// would silently discard the entire log. `expected` carries the lowest
    /// index this log can legally truncate at.
    pub fn truncate_from(&mut self, index: u64) -> Result<(), LogIndexError> {
        if index < self.start_index {
            return Err(LogIndexError {
                expected: self.start_index,
                actual: index,
            });
        }

        let length = index.saturating_sub(self.start_index);
        let length = usize::try_from(length).unwrap_or(usize::MAX);
        self.entries.truncate(length);
        Ok(())
    }

    fn validate_contiguous(entries: &[LogEntry]) -> Result<(), LogIndexError> {
        for pair in entries.windows(2) {
            let expected = pair[0].index.saturating_add(1);
            if pair[1].index != expected {
                return Err(LogIndexError {
                    expected,
                    actual: pair[1].index,
                });
            }
        }
        Ok(())
    }

    fn ensure_next_index(&self, index: u64) -> Result<(), LogIndexError> {
        let expected = self.next_index();
        if index == expected {
            Ok(())
        } else {
            Err(LogIndexError {
                expected,
                actual: index,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AppendOutcome, LogEntry, LogIndexError, LogLookupError, RaftLog};

    fn entry(index: u64, term: u64, command: &[u8]) -> LogEntry {
        LogEntry::new(index, term, command.to_vec())
    }

    #[test]
    fn empty_log_has_a_valid_empty_prefix() {
        let log = RaftLog::new();

        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);
        assert_eq!(log.next_index(), 1);
        assert!(log.contains(0, 0));
        assert_eq!(
            log.entry(0),
            Err(LogLookupError::OutOfRange {
                requested: 0,
                last_index: 0,
            })
        );
    }

    #[test]
    fn out_of_range_lookup_reports_the_last_index() {
        let mut log = RaftLog::new();
        log.append(entry(1, 1, b"one")).unwrap();

        assert_eq!(
            log.entry(5),
            Err(LogLookupError::OutOfRange {
                requested: 5,
                last_index: 1,
            })
        );
        assert_eq!(
            log.term_at(5),
            Err(LogLookupError::OutOfRange {
                requested: 5,
                last_index: 1,
            })
        );
    }

    #[test]
    fn compacted_lookup_reports_the_start_index() {
        let mut log = RaftLog::new();
        for item in [
            entry(1, 1, b"one"),
            entry(2, 1, b"two"),
            entry(3, 2, b"three"),
        ] {
            log.append(item).unwrap();
        }

        log.compact(2, 1);

        assert_eq!(log.start_index(), 3);
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.entries_from(1), vec![entry(3, 2, b"three")]);
        assert_eq!(
            log.entry(2),
            Err(LogLookupError::CompactedAway {
                requested: 2,
                start_index: 3,
            })
        );
        assert_eq!(
            log.term_at(1),
            Err(LogLookupError::CompactedAway {
                requested: 1,
                start_index: 3,
            })
        );
        assert_eq!(log.entry(3).unwrap().command, b"three");
    }

    #[test]
    fn append_rejects_gaps_and_duplicate_indexes() {
        let mut log = RaftLog::new();

        assert_eq!(
            log.append(entry(2, 1, b"gap")),
            Err(LogIndexError {
                expected: 1,
                actual: 2,
            })
        );
        log.append(entry(1, 1, b"one")).unwrap();
        assert_eq!(
            log.append(entry(1, 1, b"duplicate")),
            Err(LogIndexError {
                expected: 2,
                actual: 1,
            })
        );
    }

    #[test]
    fn shorter_follower_appends_missing_leader_suffix() {
        let mut log = RaftLog::new();
        log.append(entry(1, 1, b"one")).unwrap();

        let outcome = log
            .append_from_leader(&[entry(2, 1, b"two"), entry(3, 2, b"three")])
            .unwrap();

        // Pure append: nothing local was removed.
        assert_eq!(
            outcome,
            AppendOutcome {
                truncated_from: None,
                appended: vec![entry(2, 1, b"two"), entry(3, 2, b"three")],
            }
        );
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
        assert_eq!(
            log.entries_from(1),
            vec![
                entry(1, 1, b"one"),
                entry(2, 1, b"two"),
                entry(3, 2, b"three")
            ]
        );
    }

    #[test]
    fn term_conflict_replaces_the_local_suffix() {
        let mut log = RaftLog::new();
        for item in [
            entry(1, 1, b"one"),
            entry(2, 1, b"two"),
            entry(3, 2, b"old"),
        ] {
            log.append(item).unwrap();
        }

        let outcome = log
            .append_from_leader(&[
                entry(2, 1, b"two"),
                entry(3, 3, b"new"),
                entry(4, 3, b"four"),
            ])
            .unwrap();

        // The matching entry 2 is skipped; the conflict at 3 truncates.
        assert_eq!(
            outcome,
            AppendOutcome {
                truncated_from: Some(3),
                appended: vec![entry(3, 3, b"new"), entry(4, 3, b"four")],
            }
        );

        assert_eq!(
            log.entries_from(1),
            vec![
                entry(1, 1, b"one"),
                entry(2, 1, b"two"),
                entry(3, 3, b"new"),
                entry(4, 3, b"four")
            ]
        );
    }

    #[test]
    fn invalid_leader_batch_does_not_mutate_the_log() {
        let mut log = RaftLog::new();
        log.append(entry(1, 1, b"one")).unwrap();

        assert_eq!(
            log.append_from_leader(&[entry(2, 2, b"two"), entry(4, 2, b"gap")]),
            Err(LogIndexError {
                expected: 3,
                actual: 4,
            })
        );
        assert_eq!(log.entries_from(1), vec![entry(1, 1, b"one")]);
    }

    #[test]
    fn truncate_below_the_start_index_is_rejected() {
        let mut log = RaftLog::new();
        for item in [
            entry(1, 1, b"one"),
            entry(2, 1, b"two"),
            entry(3, 2, b"three"),
        ] {
            log.append(item).unwrap();
        }
        log.compact(2, 1);

        assert_eq!(
            log.truncate_from(1),
            Err(LogIndexError {
                expected: 3,
                actual: 1,
            })
        );
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.entry(3).unwrap().command, b"three");
    }

    #[test]
    fn truncate_from_the_zero_sentinel_is_rejected() {
        let mut log = RaftLog::new();
        log.append(entry(1, 1, b"one")).unwrap();

        assert_eq!(
            log.truncate_from(0),
            Err(LogIndexError {
                expected: 1,
                actual: 0,
            })
        );
        assert_eq!(log.entries_from(1), vec![entry(1, 1, b"one")]);
    }

    #[test]
    fn leader_batch_below_the_start_index_does_not_wipe_the_log() {
        let mut log = RaftLog::new();
        for item in [
            entry(1, 1, b"one"),
            entry(2, 1, b"two"),
            entry(3, 2, b"three"),
        ] {
            log.append(item).unwrap();
        }
        log.compact(2, 1);

        assert_eq!(
            log.append_from_leader(&[entry(1, 9, b"stale")]),
            Err(LogIndexError {
                expected: 3,
                actual: 1,
            })
        );
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.entry(3).unwrap().command, b"three");
    }

    #[test]
    fn compacted_boundary_exposes_last_included_term() {
        let mut log = RaftLog::new();
        for item in [
            entry(1, 1, b"one"),
            entry(2, 2, b"two"),
            entry(3, 2, b"three"),
        ] {
            log.append(item).unwrap();
        }
        log.compact(2, 2);

        assert_eq!(log.last_included_index(), 2);
        assert_eq!(log.last_included_term(), 2);
        assert_eq!(log.start_index(), 3);
        assert_eq!(log.term_at(2), Ok(2));
        assert_eq!(log.last_term(), 2);
        assert!(log.contains(2, 2));
    }

    #[test]
    fn fully_compacted_log_reports_included_term_as_last_term() {
        let mut log = RaftLog::new();
        log.append(entry(1, 4, b"one")).unwrap();
        log.append(entry(2, 5, b"two")).unwrap();
        log.compact(2, 5);

        assert_eq!(log.start_index(), 3);
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.last_term(), 5);
        assert_eq!(log.term_at(2), Ok(5));
    }

    #[test]
    fn install_snapshot_keeps_matching_suffix() {
        let mut log = RaftLog::new();
        for item in [
            entry(1, 1, b"one"),
            entry(2, 1, b"two"),
            entry(3, 2, b"three"),
            entry(4, 2, b"four"),
        ] {
            log.append(item).unwrap();
        }

        log.install_snapshot(2, 1);
        assert_eq!(log.start_index(), 3);
        assert_eq!(
            log.entries_from(1),
            vec![entry(3, 2, b"three"), entry(4, 2, b"four")]
        );
    }

    #[test]
    fn install_snapshot_discards_conflicting_log() {
        let mut log = RaftLog::new();
        for item in [
            entry(1, 1, b"one"),
            entry(2, 9, b"conflict"),
            entry(3, 9, b"tail"),
        ] {
            log.append(item).unwrap();
        }

        log.install_snapshot(2, 1);
        assert_eq!(log.start_index(), 3);
        assert_eq!(log.last_included_term(), 1);
        assert_eq!(log.last_index(), 2);
        assert!(log.entry(3).is_err());
    }
}

#[cfg(test)]
mod property_tests {
    use proptest::prelude::*;

    use super::{LogEntry, RaftLog};

    fn arb_entries() -> impl Strategy<Value = Vec<(u64, Vec<u8>)>> {
        prop::collection::vec(
            (0u64..1000, prop::collection::vec(any::<u8>(), 0..8)),
            0..30,
        )
    }

    fn build_log(entries: &[(u64, Vec<u8>)]) -> RaftLog {
        let mut log = RaftLog::new();
        for (position, (term, command)) in entries.iter().enumerate() {
            let index = position as u64 + 1;
            log.append(LogEntry::new(index, *term, command.clone()))
                .expect("contiguous one-based indexes always append");
        }
        log
    }

    proptest! {
        /// Every entry appended can be read back unchanged at its index.
        #[test]
        fn append_then_read_round_trips(entries in arb_entries()) {
            let log = build_log(&entries);

            for (position, (term, command)) in entries.iter().enumerate() {
                let index = position as u64 + 1;
                let fetched = log.entry(index).expect("appended index must resolve");
                prop_assert_eq!(fetched.term, *term);
                prop_assert_eq!(&fetched.command, command);
            }
        }

        /// `term_at` reports exactly the term the entry was appended with.
        #[test]
        fn term_at_agrees_with_what_was_appended(entries in arb_entries()) {
            let log = build_log(&entries);

            for (position, (term, _)) in entries.iter().enumerate() {
                let index = position as u64 + 1;
                prop_assert_eq!(log.term_at(index), Ok(*term));
            }
        }

        /// `truncate_from(cut)` keeps exactly the entries below `cut` and
        /// makes every index at or above it unresolvable.
        #[test]
        fn truncate_from_removes_exactly_the_suffix(
            entries in arb_entries(),
            cut_index in 1u64..40,
        ) {
            let mut log = build_log(&entries);
            let len = entries.len() as u64;

            log.truncate_from(cut_index)
                .expect("an uncompacted log truncates at any index from 1");

            let kept = cut_index.saturating_sub(1).min(len);
            prop_assert_eq!(log.last_index(), kept);

            for (position, (term, command)) in entries.iter().enumerate().take(kept as usize) {
                let index = position as u64 + 1;
                let fetched = log.entry(index).expect("kept index must still resolve");
                prop_assert_eq!(fetched.term, *term);
                prop_assert_eq!(&fetched.command, command);
            }
            for index in (kept + 1)..=len {
                prop_assert!(log.entry(index).is_err());
            }
        }
    }
}
