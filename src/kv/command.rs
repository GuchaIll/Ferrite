//! State-machine commands and serialization.

use serde::{Deserialize, Serialize};

use crate::error::KvError;

/// State-machine commands replicated through the Raft log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    Get {
        key: Vec<u8>,
    },
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    /// Compare-and-swap against the stored key version as one atomic log entry.
    ///
    /// - `expected_version: None` succeeds only when the key is absent.
    /// - `value: None` deletes the key when the compare succeeds.
    Cas {
        key: Vec<u8>,
        expected_version: Option<u64>,
        value: Option<Vec<u8>>,
    },
}

impl Command {
    /// Serializes this command with bincode for log storage / transport.
    pub fn encode(&self) -> Result<Vec<u8>, KvError> {
        bincode::serialize(self)
            .map_err(|err| KvError::Internal(format!("command encode failed: {err}")))
    }

    /// Deserializes a command previously produced by [`Command::encode`].
    pub fn decode(bytes: &[u8]) -> Result<Self, KvError> {
        bincode::deserialize(bytes)
            .map_err(|err| KvError::Internal(format!("command decode failed: {err}")))
    }
}

/// Result of applying a command to the state machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandResult {
    /// A Raft leader no-op entry. It advances Raft's commit index but does not
    /// change the key-value state machine.
    Noop,
    Get {
        value: Option<Vec<u8>>,
        version: Option<u64>,
    },
    Set {
        previous: Option<Vec<u8>>,
        version: u64,
    },
    Delete {
        previous: Option<Vec<u8>>,
    },
    Cas {
        outcome: CasOutcome,
    },
}

/// Outcome of a versioned compare-and-swap attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CasOutcome {
    Applied {
        previous: Option<Vec<u8>>,
        version: Option<u64>,
    },
    Failed {
        /// Actual stored version, or `None` if the key is absent.
        actual_version: Option<u64>,
        actual_value: Option<Vec<u8>>,
    },
}

#[cfg(test)]
mod tests {
    use super::Command;

    fn round_trip(command: &Command) {
        let encoded = command.encode().expect("encode");
        let decoded = Command::decode(&encoded).expect("decode");
        assert_eq!(command, &decoded);
    }

    #[test]
    fn command_round_trips_every_variant() {
        round_trip(&Command::Get { key: b"k".to_vec() });
        round_trip(&Command::Set {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        });
        round_trip(&Command::Delete { key: b"k".to_vec() });
        round_trip(&Command::Cas {
            key: b"k".to_vec(),
            expected_version: Some(1),
            value: Some(b"new".to_vec()),
        });
        round_trip(&Command::Cas {
            key: b"k".to_vec(),
            expected_version: None,
            value: None,
        });
    }

    #[test]
    fn command_round_trips_empty_and_large_values() {
        round_trip(&Command::Set {
            key: Vec::new(),
            value: Vec::new(),
        });
        round_trip(&Command::Set {
            key: vec![0_u8; 64],
            value: vec![0xAB; 64 * 1024],
        });
        round_trip(&Command::Cas {
            key: b"large".to_vec(),
            expected_version: Some(7),
            value: Some(vec![2_u8; 8 * 1024]),
        });
    }
}
