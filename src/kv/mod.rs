//! Key-value state machine.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::KvError;
use crate::raft::LogEntry;

pub mod command;
pub mod idempotency;

pub use command::{CasOutcome, Command, CommandResult};
pub use idempotency::{ClientMeta, ClientRequest, ClientState, IdempotencyTable};

use idempotency::Dedup;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ValueEntry {
    value: Vec<u8>,
    version: u64,
}

pub type ClientId = u64;
pub type SequenceNumber = u64;

/// Deterministic key-value state machine backed by an ordered map.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KvStateMachine {
    data: BTreeMap<Vec<u8>, ValueEntry>,
    // Ordered so that snapshot bytes are identical on every node.
    idempotency: IdempotencyTable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SnapshotData {
    entries: BTreeMap<Vec<u8>, ValueEntry>,
    clients: IdempotencyTable,
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

    /// Applies a replicated log entry, deduplicating client retries.
    ///
    /// The deduplication check runs before the command takes effect, and the
    /// session is recorded in the same call, so the table update is atomic with
    /// the state change.
    ///
    /// Empty commands are Raft leader no-ops and leave the store unchanged.
    pub fn apply(&mut self, entry: &LogEntry) -> Result<CommandResult, KvError> {
        if entry.command.is_empty() {
            return Ok(CommandResult::Noop);
        }
        let request = ClientRequest::decode(&entry.command)?;

        let Some(meta) = request.client else {
            let result = self.apply_command(request.command)?;
            idempotency::gc(&mut self.idempotency, entry.index);
            return Ok(result);
        };

        match idempotency::check(&mut self.idempotency, meta, entry.index) {
            Dedup::Replay(cached) => {
                idempotency::gc(&mut self.idempotency, entry.index);
                return Ok(cached);
            }
            Dedup::Stale => {
                idempotency::gc(&mut self.idempotency, entry.index);
                return Ok(CommandResult::Noop);
            }
            Dedup::Fresh => {}
        }

        let result = self.apply_command(request.command)?;
        idempotency::record(&mut self.idempotency, meta, result.clone(), entry.index);
        idempotency::gc(&mut self.idempotency, entry.index);
        Ok(result)
    }

    /// Applies a decoded command as one atomic state transition.
    ///
    /// This is the raw transition and does **not** deduplicate; [`Self::apply`]
    /// is the deduplicating entry point for replicated entries.
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
            clients: self.idempotency.clone(),
        };
        bincode::serialize(&snapshot)
            .map_err(|err| KvError::Internal(format!("snapshot encode failed: {err}")))
    }

    /// Restores store contents from a snapshot previously produced by [`Self::snapshot`].
    pub fn restore(&mut self, snapshot: &[u8]) -> Result<(), KvError> {
        let snapshot: SnapshotData = bincode::deserialize(snapshot)
            .map_err(|err| KvError::Internal(format!("snapshot decode failed: {err}")))?;
        self.data = snapshot.entries;
        self.idempotency = snapshot.clients;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{CasOutcome, ClientRequest, Command, CommandResult, KvStateMachine, idempotency};
    use crate::raft::LogEntry;

    /// An entry with no originating client, so it is never deduplicated.
    fn entry(index: u64, command: Command) -> LogEntry {
        LogEntry::new(
            index,
            1,
            ClientRequest::internal(command).encode().expect("encode"),
        )
    }

    /// An entry issued by `client_id` as its `seq_num`-th request.
    fn client_entry(index: u64, client_id: u64, seq_num: u64, command: Command) -> LogEntry {
        client_entry_in_term(index, 1, client_id, seq_num, command)
    }

    /// As [`client_entry`], but committed under an explicit leader term.
    fn client_entry_in_term(
        index: u64,
        term: u64,
        client_id: u64,
        seq_num: u64,
        command: Command,
    ) -> LogEntry {
        LogEntry::new(
            index,
            term,
            ClientRequest::new(client_id, seq_num, command)
                .encode()
                .expect("encode"),
        )
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

    #[test]
    fn retried_request_replays_cached_result_without_reapplying() {
        let mut sm = KvStateMachine::new();
        let set = || Command::Set {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };

        let first = sm.apply(&client_entry(1, 7, 1, set())).expect("first");
        // Same (client_id, seq_num) arriving as a fresh log entry: a retry whose
        // response was lost, not a new request.
        let retry = sm.apply(&client_entry(2, 7, 1, set())).expect("retry");

        assert_eq!(first, retry);
        // Re-applying would have bumped the version to 2.
        assert_eq!(sm.version(b"k"), Some(1));
    }

    #[test]
    fn retried_cas_returns_its_original_outcome() {
        let mut sm = KvStateMachine::new();
        sm.apply(&entry(
            1,
            Command::Set {
                key: b"k".to_vec(),
                value: b"v1".to_vec(),
            },
        ))
        .expect("seed");

        let cas = || Command::Cas {
            key: b"k".to_vec(),
            expected_version: Some(1),
            value: Some(b"v2".to_vec()),
        };
        let original = sm.apply(&client_entry(2, 7, 1, cas())).expect("cas");
        assert_eq!(
            original,
            CommandResult::Cas {
                outcome: CasOutcome::Applied {
                    previous: Some(b"v1".to_vec()),
                    version: Some(2),
                },
            }
        );

        // Re-evaluating would now compare Some(1) against the stored 2 and fail.
        let retry = sm.apply(&client_entry(3, 7, 1, cas())).expect("retry");
        assert_eq!(original, retry);
        assert_eq!(sm.version(b"k"), Some(2));
    }

    #[test]
    fn deduplication_survives_a_leader_change() {
        let mut sm = KvStateMachine::new();
        let set = || Command::Set {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };

        let under_first_leader = sm
            .apply(&client_entry_in_term(1, 1, 7, 1, set()))
            .expect("term 1");
        let retried_under_next_leader = sm
            .apply(&client_entry_in_term(2, 5, 7, 1, set()))
            .expect("term 5");

        assert_eq!(under_first_leader, retried_under_next_leader);
        assert_eq!(sm.version(b"k"), Some(1));
    }

    #[test]
    fn restored_node_deduplicates_a_request_it_never_saw_live() {
        let mut live = KvStateMachine::new();
        let set = || Command::Set {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };
        let original = live.apply(&client_entry(1, 7, 1, set())).expect("apply");

        let mut restored = KvStateMachine::new();
        restored
            .restore(&live.snapshot().expect("snapshot"))
            .expect("restore");
        assert_eq!(live, restored);

        // The restored node never applied entry 1 itself; the table came in with
        // the snapshot.
        let retry = restored
            .apply(&client_entry(2, 7, 1, set()))
            .expect("retry");
        assert_eq!(original, retry);
        assert_eq!(restored.version(b"k"), Some(1));
    }

    #[test]
    fn stale_sequence_number_is_not_reapplied() {
        let mut sm = KvStateMachine::new();
        sm.apply(&client_entry(
            1,
            7,
            1,
            Command::Set {
                key: b"k".to_vec(),
                value: b"v1".to_vec(),
            },
        ))
        .expect("seq 1");
        sm.apply(&client_entry(
            2,
            7,
            2,
            Command::Set {
                key: b"k".to_vec(),
                value: b"v2".to_vec(),
            },
        ))
        .expect("seq 2");

        // Only the newest result is cached, so seq 1 cannot be answered — but it
        // must not take effect either.
        let stale = sm
            .apply(&client_entry(
                3,
                7,
                1,
                Command::Set {
                    key: b"k".to_vec(),
                    value: b"v1".to_vec(),
                },
            ))
            .expect("stale");

        assert_eq!(stale, CommandResult::Noop);
        assert_eq!(sm.get(b"k"), Some(b"v2".as_slice()));
        assert_eq!(sm.version(b"k"), Some(2));
    }

    #[test]
    fn dedup_tables_stay_identical_across_nodes_through_gc() {
        // Enough entries to cross SESSION_TTL_ENTRIES and sweep many times.
        let total = idempotency::SESSION_TTL_ENTRIES + 3 * idempotency::GC_INTERVAL_ENTRIES;

        let mut snapshots = Vec::new();
        for _ in 0..3 {
            let mut sm = KvStateMachine::new();
            for index in 1..=total {
                sm.apply(&client_entry(
                    index,
                    index,
                    1,
                    Command::Set {
                        key: index.to_be_bytes().to_vec(),
                        value: b"v".to_vec(),
                    },
                ))
                .expect("apply");
            }
            assert!(
                sm.idempotency.len() < total as usize,
                "gc never evicted anything"
            );
            snapshots.push(sm.snapshot().expect("snapshot"));
        }

        assert_eq!(snapshots[0], snapshots[1]);
        assert_eq!(snapshots[1], snapshots[2]);
    }

    #[test]
    fn gc_bounds_the_table_without_evicting_a_retrying_client() {
        const CLIENTS: u64 = 10_000;
        const RETRIER: u64 = 0;

        let mut sm = KvStateMachine::new();
        let retried = sm
            .apply(&client_entry(
                1,
                RETRIER,
                1,
                Command::Set {
                    key: b"retried".to_vec(),
                    value: b"v".to_vec(),
                },
            ))
            .expect("retrier first attempt");

        let mut index = 1;
        for client in 1..=CLIENTS {
            index += 1;
            sm.apply(&client_entry(
                index,
                client,
                1,
                Command::Set {
                    key: client.to_be_bytes().to_vec(),
                    value: b"v".to_vec(),
                },
            ))
            .expect("one-shot client");

            // The retrier keeps resending, well past SESSION_TTL_ENTRIES.
            if client % 64 == 0 {
                index += 1;
                let replay = sm
                    .apply(&client_entry(
                        index,
                        RETRIER,
                        1,
                        Command::Set {
                            key: b"retried".to_vec(),
                            value: b"v".to_vec(),
                        },
                    ))
                    .expect("retry");
                assert_eq!(replay, retried, "retrier was evicted mid-retry");
            }
        }

        assert!(
            sm.idempotency.len() < CLIENTS as usize,
            "table grew to every client seen: {}",
            sm.idempotency.len()
        );
        assert!(
            sm.idempotency.contains_key(&RETRIER),
            "a client still retrying was evicted"
        );
        assert_eq!(sm.version(b"retried"), Some(1));
    }
}
