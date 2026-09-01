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

/// Ordered, contiguous entries held by one Raft node.
///
/// Index zero is not an entry. It is the empty-prefix sentinel used by
/// `AppendEntries` when a follower must receive the first entry.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct RaftLog {
    entries: Vec<LogEntry>,
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

    /// Returns the last entry index, or zero for an empty log.
    pub fn last_index(&self) -> u64 {
        self.entries.last().map_or(0, |entry| entry.index)
    }

    /// Returns the last entry term, or zero for an empty log.
    pub fn last_term(&self) -> u64 {
        self.entries.last().map_or(0, |entry| entry.term)
    }

    /// Returns an entry at a one-based Raft index.
    pub fn entry(&self, index: u64) -> Option<&LogEntry> {
        let offset = index.checked_sub(1)?;
        let offset = usize::try_from(offset).ok()?;
        self.entries.get(offset)
    }

    /// Returns the term at `index`; the empty prefix at index zero has term zero.
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            Some(0)
        } else {
            self.entry(index).map(|entry| entry.term)
        }
    }

    /// Checks the `prev_log_index` / `prev_log_term` AppendEntries condition.
    pub fn contains(&self, index: u64, term: u64) -> bool {
        self.term_at(index) == Some(term)
    }

    /// Returns copies of entries beginning at `index`, or no entries past the tail.
    pub fn entries_from(&self, index: u64) -> Vec<LogEntry> {
        if index == 0 {
            return self.entries.clone();
        }

        let Some(offset) = index
            .checked_sub(1)
            .and_then(|value| usize::try_from(value).ok())
        else {
            return Vec::new();
        };
        self.entries.get(offset..).unwrap_or_default().to_vec()
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
                Some(local) => local.term != entry.term,
                None => true,
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
        let length = index.saturating_sub(1);
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
    use super::{LogEntry, LogIndexError, RaftLog};

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
        assert_eq!(log.entry(0), None);
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
