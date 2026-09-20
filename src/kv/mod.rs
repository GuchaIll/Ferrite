//! Key-value state machine.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::KvError;
use crate::raft::LogEntry;

pub mod command;
pub mod idempotency;

pub use command::{CasOutcome, Command, CommandResult};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ValueEntry {
    value: Vec<u8>,
    version: u64,
}

pub type ClientId = u64;
pub type SequenceNumber = u64;
pub type LastSequenceNumber = u64;

/// Deterministic key-value state machine backed by an ordered map.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KvStateMachine {
    data: BTreeMap<Vec<u8>, ValueEntry>,
    // Reserved for issue #12 (idempotency table); kept ordered for future snapshots.
    idempotency: BTreeMap<ClientId, ClientState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ClientState {
    last_sequence_number: LastSequenceNumber,
    last_seen_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SnapshotData {
    entries: BTreeMap<Vec<u8>, ValueEntry>,
}

impl KvStateMachine {
    /// Creates an empty state machine.
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
            idempotency: BTreeMap::new(),
        }
    }

    /// Returns the current value for `key`, if present.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.data.get(key).map(|entry| entry.value.as_slice())
    }

    /// Returns the current version for `key`, if present.
    pub fn version(&self, key: &[u8]) -> Option<u64> {
        self.data.get(key).map(|entry| entry.version)
    }

    /// Applies a replicated log entry.
    ///
    /// Empty commands are Raft leader no-ops and leave the store unchanged.
    pub fn apply(&mut self, entry: &LogEntry) -> Result<CommandResult, KvError> {
        if entry.command.is_empty() {
            return Ok(CommandResult::Noop);
        }
        let command = Command::decode(&entry.command)?;
        self.apply_command(command)
    }

    /// Applies a decoded command as one atomic state transition.
    pub fn apply_command(&mut self, command: Command) -> Result<CommandResult, KvError> {
        match command {
            Command::Get { key } => {
                let entry = self.data.get(&key);
                Ok(CommandResult::Get {
                    value: entry.map(|e| e.value.clone()),
                    version: entry.map(|e| e.version),
                })
            }
            Command::Set { key, value } => {
                let previous_entry = self.data.remove(&key);
                let version = match previous_entry.as_ref() {
                    Some(entry) => entry
                        .version
                        .checked_add(1)
                        .ok_or_else(|| KvError::Internal("key version overflow".to_owned()))?,
                    None => 1,
                };
                let previous = previous_entry.map(|entry| entry.value);
                self.data.insert(key, ValueEntry { value, version });
                Ok(CommandResult::Set { previous, version })
            }
            Command::Delete { key } => {
                let previous = self.data.remove(&key).map(|entry| entry.value);
                Ok(CommandResult::Delete { previous })
            }
            Command::Cas {
                key,
                expected_version,
                value,
            } => {
                let actual_version = self.data.get(&key).map(|entry| entry.version);
                if actual_version != expected_version {
                    let actual_value = self.data.get(&key).map(|entry| entry.value.clone());
                    return Ok(CommandResult::Cas {
                        outcome: CasOutcome::Failed {
                            actual_version,
                            actual_value,
                        },
                    });
                }

                match value {
                    Some(new_value) => {
                        let version = match actual_version {
                            Some(v) => v.checked_add(1).ok_or_else(|| {
                                KvError::Internal("key version overflow".to_owned())
                            })?,
                            None => 1,
                        };
                        let previous = self
                            .data
                            .insert(
                                key,
                                ValueEntry {
                                    value: new_value,
                                    version,
                                },
                            )
                            .map(|entry| entry.value);
                        Ok(CommandResult::Cas {
                            outcome: CasOutcome::Applied {
                                previous,
                                version: Some(version),
                            },
                        })
                    }
                    None => {
                        let previous = self.data.remove(&key).map(|entry| entry.value);
                        Ok(CommandResult::Cas {
                            outcome: CasOutcome::Applied {
                                previous,
                                version: None,
                            },
                        })
                    }
                }
            }
        }
    }

    /// Serializes the full store into a deterministic byte snapshot.
    pub fn snapshot(&self) -> Result<Vec<u8>, KvError> {
        let snapshot = SnapshotData {
            // Clone once at the snapshot boundary so callers own an immutable byte blob.
            entries: self.data.clone(),
        };
        bincode::serialize(&snapshot)
            .map_err(|err| KvError::Internal(format!("snapshot encode failed: {err}")))
    }

    /// Restores store contents from a snapshot previously produced by [`Self::snapshot`].
    pub fn restore(&mut self, snapshot: &[u8]) -> Result<(), KvError> {
        let snapshot: SnapshotData = bincode::deserialize(snapshot)
            .map_err(|err| KvError::Internal(format!("snapshot decode failed: {err}")))?;
        self.data = snapshot.entries;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{CasOutcome, Command, CommandResult, KvStateMachine};
    use crate::raft::LogEntry;

    fn entry(index: u64, command: Command) -> LogEntry {
        LogEntry::new(index, 1, command.encode().expect("encode command"))
    }

    #[test]
    fn set_is_readable_after_apply() {
        let mut sm = KvStateMachine::new();
        let result = sm
            .apply(&entry(
                1,
                Command::Set {
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                },
            ))
            .expect("apply");

        assert_eq!(
            result,
            CommandResult::Set {
                previous: None,
                version: 1,
            }
        );
        assert_eq!(sm.get(b"k"), Some(b"v".as_slice()));
        assert_eq!(sm.version(b"k"), Some(1));
    }

    #[test]
    fn delete_on_absent_key_is_noop() {
        let mut sm = KvStateMachine::new();
        let result = sm
            .apply(&entry(
                1,
                Command::Delete {
                    key: b"missing".to_vec(),
                },
            ))
            .expect("apply");

        assert_eq!(result, CommandResult::Delete { previous: None });
        assert!(sm.get(b"missing").is_none());
    }

    #[test]
    fn cas_compares_versions_and_has_one_winner() {
        let mut sm = KvStateMachine::new();
        sm.apply_command(Command::Set {
            key: b"k".to_vec(),
            value: b"v1".to_vec(),
        })
        .expect("set");
        assert_eq!(sm.version(b"k"), Some(1));

        let first = sm
            .apply_command(Command::Cas {
                key: b"k".to_vec(),
                expected_version: Some(1),
                value: Some(b"v2".to_vec()),
            })
            .expect("cas 1");
        let second = sm
            .apply_command(Command::Cas {
                key: b"k".to_vec(),
                expected_version: Some(1),
                value: Some(b"v3".to_vec()),
            })
            .expect("cas 2");

        assert_eq!(
            first,
            CommandResult::Cas {
                outcome: CasOutcome::Applied {
                    previous: Some(b"v1".to_vec()),
                    version: Some(2),
                },
            }
        );
        assert_eq!(
            second,
            CommandResult::Cas {
                outcome: CasOutcome::Failed {
                    actual_version: Some(2),
                    actual_value: Some(b"v2".to_vec()),
                },
            }
        );
        assert_eq!(sm.get(b"k"), Some(b"v2".as_slice()));
        assert_eq!(sm.version(b"k"), Some(2));
    }

    #[test]
    fn cas_create_if_absent_and_delete_if_matches() {
        let mut sm = KvStateMachine::new();

        let create = sm
            .apply_command(Command::Cas {
                key: b"k".to_vec(),
                expected_version: None,
                value: Some(b"created".to_vec()),
            })
            .expect("create");
        assert_eq!(
            create,
            CommandResult::Cas {
                outcome: CasOutcome::Applied {
                    previous: None,
                    version: Some(1),
                },
            }
        );
        assert_eq!(sm.get(b"k"), Some(b"created".as_slice()));
        assert_eq!(sm.version(b"k"), Some(1));

        let create_conflict = sm
            .apply_command(Command::Cas {
                key: b"k".to_vec(),
                expected_version: None,
                value: Some(b"other".to_vec()),
            })
            .expect("create conflict");
        assert_eq!(
            create_conflict,
            CommandResult::Cas {
                outcome: CasOutcome::Failed {
                    actual_version: Some(1),
                    actual_value: Some(b"created".to_vec()),
                },
            }
        );

        let delete = sm
            .apply_command(Command::Cas {
                key: b"k".to_vec(),
                expected_version: Some(1),
                value: None,
            })
            .expect("delete");
        assert_eq!(
            delete,
            CommandResult::Cas {
                outcome: CasOutcome::Applied {
                    previous: Some(b"created".to_vec()),
                    version: None,
                },
            }
        );
        assert!(sm.get(b"k").is_none());
        assert!(sm.version(b"k").is_none());
    }

    #[test]
    fn snapshot_is_deterministic_across_machines() {
        let commands = [
            Command::Set {
                key: b"b".to_vec(),
                value: b"2".to_vec(),
            },
            Command::Set {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            },
            Command::Delete { key: b"b".to_vec() },
            Command::Cas {
                key: b"c".to_vec(),
                expected_version: None,
                value: Some(b"3".to_vec()),
            },
        ];

        let mut snapshots = Vec::new();
        for _ in 0..3 {
            let mut sm = KvStateMachine::new();
            for (i, command) in commands.iter().cloned().enumerate() {
                sm.apply(&entry((i as u64) + 1, command)).expect("apply");
            }
            snapshots.push(sm.snapshot().expect("snapshot"));
        }

        assert_eq!(snapshots[0], snapshots[1]);
        assert_eq!(snapshots[1], snapshots[2]);
    }

    #[test]
    fn snapshot_restore_round_trips() {
        let mut sm = KvStateMachine::new();
        sm.apply_command(Command::Set {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        })
        .expect("set");
        sm.apply_command(Command::Set {
            key: b"x".to_vec(),
            value: b"y".to_vec(),
        })
        .expect("set");

        let bytes = sm.snapshot().expect("snapshot");
        let mut restored = KvStateMachine::new();
        restored.restore(&bytes).expect("restore");

        assert_eq!(sm, restored);
        assert_eq!(restored.get(b"k"), Some(b"v".as_slice()));
        assert_eq!(restored.version(b"k"), Some(1));
        assert_eq!(restored.get(b"x"), Some(b"y".as_slice()));
    }

    #[test]
    fn empty_log_entry_is_noop() {
        let mut sm = KvStateMachine::new();
        sm.apply_command(Command::Set {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        })
        .expect("set");

        let result = sm
            .apply(&LogEntry::new(2, 1, Vec::new()))
            .expect("apply empty");
        assert_eq!(result, CommandResult::Noop);
        assert_eq!(sm.get(b"k"), Some(b"v".as_slice()));
    }
}
