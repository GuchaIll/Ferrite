//! Raft log and log entry definitions.

use std::fmt;

/// One replicated state-machine operation.
#[derive(Debug, Clone, Eq, PartialEq)]
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
}

impl Default for RaftLog {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            start_index: 1,
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

    /// Returns the last entry term, or zero if the log holds no entries.
    ///
    /// A fully compacted, entry-less log has no way to recover the term of
    /// its last-included index without snapshot metadata (out of scope
    /// here), so this returns zero in that case too.
    pub fn last_term(&self) -> u64 {
        self.entries.last().map_or(0, |entry| entry.term)
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
    pub fn term_at(&self, index: u64) -> Result<u64, LogLookupError> {
        if index == 0 {
            Ok(0)
        } else {
            self.entry(index).map(|entry| entry.term)
        }
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

    /// Discards entries at or before `last_included_index`, recording that a
    /// snapshot now covers them. A no-op if nothing new is being compacted.
    pub fn compact(&mut self, last_included_index: u64) {
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
    pub fn append_from_leader(&mut self, incoming: &[LogEntry]) -> Result<(), LogIndexError> {
        Self::validate_contiguous(incoming)?;

        let first_to_append = incoming
            .iter()
            .position(|entry| match self.entry(entry.index) {
                Ok(local) => local.term != entry.term,
                Err(_) => true,
            });

        if let Some(position) = first_to_append {
            self.truncate_from(incoming[position].index);

            for entry in &incoming[position..] {
                self.append(entry.clone())?;
            }
        }
        Ok(())
    }

    /// Removes every entry whose index is at least `index`.
    pub fn truncate_from(&mut self, index: u64) {
        let length = index.saturating_sub(self.start_index);
        let length = usize::try_from(length).unwrap_or(usize::MAX);
        self.entries.truncate(length);
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
    use super::{LogEntry, LogIndexError, LogLookupError, RaftLog};

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

        log.compact(2);

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

        log.append_from_leader(&[entry(2, 1, b"two"), entry(3, 2, b"three")])
            .unwrap();

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

        log.append_from_leader(&[
            entry(2, 1, b"two"),
            entry(3, 3, b"new"),
            entry(4, 3, b"four"),
        ])
        .unwrap();

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
            cut_index in 0u64..40,
        ) {
            let mut log = build_log(&entries);
            let len = entries.len() as u64;

            log.truncate_from(cut_index);

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
