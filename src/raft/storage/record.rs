//! Record framing for log segments and single-record atomic files.
//!
//! Wire format per record:
//!   [len: u32 LE][crc32c: u32 LE][kind: u8][payload: len-1 bytes]
//!
//! `len` = 1 (kind byte) + payload length.
//! CRC covers the kind byte and the payload.
//!
//! Kind bytes:
//!   1 — LogOp::Append(LogEntry)    — log stream
//!   2 — LogOp::TruncateFrom(u64)   — log stream
//!   3 — HardState                  — single-record atomic file
//!   4 — Snapshot                   — single-record atomic file

use serde::{Deserialize, Serialize};

use super::HardState;
use crate::{
    error::StorageError,
    raft::{LogEntry, Snapshot},
};

const KIND_APPEND: u8 = 1;
const KIND_TRUNCATE: u8 = 2;
const KIND_HARD_STATE: u8 = 3;
const KIND_SNAPSHOT: u8 = 4;

// ── public types ──────────────────────────────────────────────────────────────

/// A single log mutation, used both as a `WriteBatch` element and a record kind.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogOp {
    Append(LogEntry),
    TruncateFrom(u64),
}

/// Result of decoding a log byte stream.
#[derive(Debug)]
pub struct Decoded {
    /// All valid ops decoded from the stream, in order.
    pub ops: Vec<LogOp>,
    /// Byte length of the clean prefix (everything before the torn tail, if any).
    /// Truncate the segment file to this length to remove the torn tail.
    pub valid_len: usize,
}

// ── encoding ──────────────────────────────────────────────────────────────────

fn write_framed(kind: u8, payload: &[u8], out: &mut Vec<u8>) {
    let len = (1u32 + payload.len() as u32).to_le_bytes();
    let crc = crc32c::crc32c_append(crc32c::crc32c(&[kind]), payload);
    out.extend_from_slice(&len);
    out.extend_from_slice(&crc.to_le_bytes());
    out.push(kind);
    out.extend_from_slice(payload);
}

fn to_bincode<T: Serialize>(val: &T) -> Result<Vec<u8>, StorageError> {
    bincode::serialize(val).map_err(|e| StorageError::Internal(e.to_string()))
}

/// Encodes one log op into `out`.
pub fn encode(op: &LogOp, out: &mut Vec<u8>) -> Result<(), StorageError> {
    let (kind, payload) = match op {
        LogOp::Append(e) => (KIND_APPEND, to_bincode(e)?),
        LogOp::TruncateFrom(i) => (KIND_TRUNCATE, to_bincode(i)?),
    };
    write_framed(kind, &payload, out);
    Ok(())
}

/// Encodes a `HardState` as a single record into `out`.
pub fn encode_hard_state(hs: &HardState, out: &mut Vec<u8>) -> Result<(), StorageError> {
    write_framed(KIND_HARD_STATE, &to_bincode(hs)?, out);
    Ok(())
}

/// Encodes a `Snapshot` as a single record into `out`.
pub fn encode_snapshot(snap: &Snapshot, out: &mut Vec<u8>) -> Result<(), StorageError> {
    write_framed(KIND_SNAPSHOT, &to_bincode(snap)?, out);
    Ok(())
}

// ── decoding ──────────────────────────────────────────────────────────────────

fn read_u32_le(bytes: &[u8], offset: usize) -> Result<u32, StorageError> {
    bytes
        .get(offset..offset + 4)
        .and_then(|s| <[u8; 4]>::try_from(s).ok())
        .map(u32::from_le_bytes)
        .ok_or(StorageError::Corruption {
            segment: None,
            offset: offset as u64,
        })
}

fn from_bincode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, StorageError> {
    bincode::deserialize(bytes).map_err(|_| StorageError::Corruption {
        segment: None,
        // Caller has the stream offset; single-record files use 0.
        offset: 0,
    })
}

/// Decodes a log stream (multiple records).
///
/// **Torn tail**: a record that runs past `bytes`, or a bad CRC on the *final*
/// record, is an expected artifact of a crash mid-write. Decoding stops there
/// and returns the clean prefix in `valid_len` without error.
///
/// **Corruption**: a complete record earlier in the stream with a bad CRC or
/// an unknown kind is returned as `StorageError::Corruption { offset }`.
pub fn decode(bytes: &[u8]) -> Result<Decoded, StorageError> {
    let mut ops = Vec::new();
    let mut pos = 0usize;

    loop {
        // Minimum header is 9 bytes: 4 (len) + 4 (crc) + 1 (kind).
        if pos + 9 > bytes.len() {
            break; // not enough bytes for a header → torn tail
        }

        let len = read_u32_le(bytes, pos)? as usize;
        let crc_stored = read_u32_le(bytes, pos + 4)?;

        if len == 0 {
            // len=0 means no kind byte — structurally malformed.
            let record_end = pos + 8;
            if record_end >= bytes.len() {
                break; // at end of buffer → torn tail
            }
            return Err(StorageError::Corruption {
                segment: None,
                offset: pos as u64,
            });
        }

        // Total record: 4 (len field) + 4 (crc field) + len bytes.
        let record_end = pos + 8 + len;
        if record_end > bytes.len() {
            break; // record extends past buffer → torn tail
        }

        let kind = bytes[pos + 8];
        let payload = &bytes[pos + 9..record_end];

        let crc_computed = crc32c::crc32c_append(crc32c::crc32c(&[kind]), payload);
        let is_last_record = record_end >= bytes.len();

        if crc_computed != crc_stored {
            if is_last_record {
                break; // bad CRC on the final record → torn tail
            }
            return Err(StorageError::Corruption {
                segment: None,
                offset: pos as u64,
            });
        }

        let op = match kind {
            KIND_APPEND => {
                LogOp::Append(from_bincode::<LogEntry>(payload).map_err(|e| match e {
                    StorageError::Corruption { .. } => StorageError::Corruption {
                        segment: None,
                        offset: pos as u64,
                    },
                    other => other,
                })?)
            }
            KIND_TRUNCATE => {
                LogOp::TruncateFrom(from_bincode::<u64>(payload).map_err(|e| match e {
                    StorageError::Corruption { .. } => StorageError::Corruption {
                        segment: None,
                        offset: pos as u64,
                    },
                    other => other,
                })?)
            }
            _ => {
                if is_last_record {
                    break; // unknown kind on final record → torn tail
                }
                return Err(StorageError::Corruption {
                    segment: None,
                    offset: pos as u64,
                });
            }
        };

        ops.push(op);
        pos = record_end;
    }

    Ok(Decoded {
        ops,
        valid_len: pos,
    })
}

/// Decodes a single-record file (hard_state or snapshot).
///
/// The whole file is one record with `expected_kind`. Any structural or CRC
/// error is `Corruption`.
fn decode_single<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
    expected_kind: u8,
) -> Result<T, StorageError> {
    if bytes.len() < 9 {
        return Err(StorageError::Corruption {
            segment: None,
            offset: 0,
        });
    }
    let len = read_u32_le(bytes, 0)? as usize;
    let crc_stored = read_u32_le(bytes, 4)?;

    if bytes.len() < 8 + len {
        return Err(StorageError::Corruption {
            segment: None,
            offset: 0,
        });
    }

    let kind = bytes[8];
    let payload = &bytes[9..8 + len];

    let crc_computed = crc32c::crc32c_append(crc32c::crc32c(&[kind]), payload);
    if crc_computed != crc_stored || kind != expected_kind {
        return Err(StorageError::Corruption {
            segment: None,
            offset: 0,
        });
    }

    from_bincode(payload)
}

/// Decodes a `HardState` from a single-record atomic file.
pub fn decode_hard_state(bytes: &[u8]) -> Result<HardState, StorageError> {
    decode_single(bytes, KIND_HARD_STATE)
}

/// Decodes a `Snapshot` from a single-record atomic file.
pub fn decode_snapshot(bytes: &[u8]) -> Result<Snapshot, StorageError> {
    decode_single(bytes, KIND_SNAPSHOT)
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeId;
    use crate::raft::{LogEntry, snapshot::SnapshotMeta};

    fn sample_entry(index: u64) -> LogEntry {
        LogEntry::new(index, 1, format!("cmd{index}").into_bytes())
    }

    fn encode_ops(ops: &[LogOp]) -> Vec<u8> {
        let mut buf = Vec::new();
        for op in ops {
            encode(op, &mut buf).unwrap();
        }
        buf
    }

    #[test]
    fn round_trip_append_and_truncate() {
        let ops = vec![
            LogOp::Append(sample_entry(1)),
            LogOp::Append(sample_entry(2)),
            LogOp::TruncateFrom(2),
            LogOp::Append(sample_entry(2)),
        ];
        let buf = encode_ops(&ops);
        let decoded = decode(&buf).unwrap();
        assert_eq!(decoded.valid_len, buf.len());
        assert_eq!(decoded.ops.len(), 4);
    }

    #[test]
    fn torn_tail_at_every_offset_in_last_record() {
        let ops = vec![
            LogOp::Append(sample_entry(1)),
            LogOp::Append(sample_entry(2)),
        ];
        let buf = encode_ops(&ops);

        // Find where the first record ends.
        let first_record_len = {
            let _d = decode(&buf).unwrap();
            // Encode just the first op to find its size.
            let mut first = Vec::new();
            encode(&ops[0], &mut first).unwrap();
            first.len()
        };

        // Cutting anywhere inside the second record must return only the first op.
        for cut in first_record_len..buf.len() {
            let partial = &buf[..cut];
            let d = decode(partial).unwrap();
            assert_eq!(d.ops.len(), 1, "cut at {cut}");
            assert_eq!(d.valid_len, first_record_len, "cut at {cut}");
        }
    }

    #[test]
    fn corruption_in_non_final_record_returns_error() {
        let ops = vec![
            LogOp::Append(sample_entry(1)),
            LogOp::Append(sample_entry(2)),
        ];
        let mut buf = encode_ops(&ops);

        // Flip a payload byte inside the first record (offset 9 is kind + first payload byte).
        buf[9] ^= 0xFF;

        let result = decode(&buf);
        assert!(
            matches!(result, Err(StorageError::Corruption { .. })),
            "expected Corruption, got {result:?}"
        );
    }

    #[test]
    fn hard_state_round_trip() {
        let hs = HardState {
            current_term: 7,
            voted_for: Some(3 as NodeId),
        };
        let mut buf = Vec::new();
        encode_hard_state(&hs, &mut buf).unwrap();
        let recovered = decode_hard_state(&buf).unwrap();
        assert_eq!(hs, recovered);
    }

    #[test]
    fn snapshot_round_trip() {
        let snap = Snapshot::new(SnapshotMeta::new(5, 2), b"state".to_vec());
        let mut buf = Vec::new();
        encode_snapshot(&snap, &mut buf).unwrap();
        let recovered = decode_snapshot(&buf).unwrap();
        assert_eq!(snap, recovered);
    }
}
