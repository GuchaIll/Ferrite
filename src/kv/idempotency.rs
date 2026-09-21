//! Client request deduplication.
//!
//! Raft replicates each entry exactly once, but a client whose response was lost
//! retries, and that retry becomes a *new* log entry. Exactly-once application is
//! this module's job: the state machine records the newest sequence number and
//! result per client, and replays the cached result instead of applying twice.
//!
//! The check lives at the state machine rather than the leader so that a leader
//! change between the original request and the retry cannot lose it.
use std::cmp::Ordering;
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::KvError;
use crate::kv::{ClientId, Command, CommandResult, SequenceNumber};

/// Identifies which client sent a request, and where it sits in that client's
/// own sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientMeta {
    pub client_id: ClientId,
    pub seq_num: SequenceNumber,
}

/// A command plus the client identity used to deduplicate retries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientRequest {
    /// `None` for internally generated commands, which are never deduplicated
    /// and never occupy a session slot.
    pub client: Option<ClientMeta>,
    pub command: Command,
}

impl ClientRequest {
    /// A client-issued request, subject to deduplication.
    pub fn new(client_id: ClientId, seq_num: SequenceNumber, command: Command) -> Self {
        Self {
            client: Some(ClientMeta { client_id, seq_num }),
            command,
        }
    }

    /// A command with no originating client. Applied every time it is seen.
    pub fn internal(command: Command) -> Self {
        Self {
            client: None,
            command,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, KvError> {
        bincode::serialize(self)
            .map_err(|err| KvError::Internal(format!("command encode failed: {err}")))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, KvError> {
        bincode::deserialize(bytes)
            .map_err(|err| KvError::Internal(format!("command decode failed: {err}")))
    }
}

/// Sessions are evicted once this many committed entries have been applied since
/// the client was last seen.
///
/// The TTL is counted in committed log positions and never in wall-clock time.
/// Every node applies the same entries in the same order, so an index-driven
/// eviction fires at the same entry on every node and the tables stay identical.
/// A wall-clock TTL is read independently on each node, crosses its threshold at
/// a different entry on each, and diverges the dedup tables — and therefore the
/// snapshots. That is the exact failure class this project exists to catch,
/// arriving through the mechanism meant to prevent it.
pub const SESSION_TTL_ENTRIES: u64 = 1_024;

/// How often [`gc`] sweeps, in committed entries. Index-driven for the same
/// reason as the TTL: a timer-driven sweep fires at a different entry per node.
pub const GC_INTERVAL_ENTRIES: u64 = 128;

/// One client's session: the newest sequence number applied, the result that
/// produced, and the committed index at which the client was last seen.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientState {
    pub last_sequence_number: SequenceNumber,
    pub cached_result: CommandResult,
    pub last_seen_index: u64,
}

/// Client sessions, ordered because the table is serialized into the snapshot.
///
/// A `HashMap` iterates in an order that varies per process, so two nodes
/// holding identical sessions would emit different snapshot bytes.
pub type IdempotencyTable = BTreeMap<ClientId, ClientState>;

/// What the table says about an incoming request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Dedup {
    /// Never applied. Apply it, then record the result.
    Fresh,
    /// An exact retry of this client's newest request. Return this result and
    /// apply nothing.
    Replay(CommandResult),
    /// Older than this client's newest request, so the client already saw that
    /// response and moved on. Only the newest result is cached, so there is
    /// nothing to return — but it must not be applied a second time.
    Stale,
}

/// Classifies a request against the sending client's session, and refreshes that
/// session's last-seen position.
///
/// A duplicate proves the client is alive and still retrying, so it renews the
/// session against [`gc`]. Without that renewal a client retrying across the TTL
/// would be evicted mid-retry and its next attempt re-applied — the exact
/// exactly-once break that eviction is supposed to avoid.
///
/// Clones the cached result on the replay path only; duplicates are rare.
pub fn check(table: &mut IdempotencyTable, meta: ClientMeta, applied_index: u64) -> Dedup {
    let Some(state) = table.get_mut(&meta.client_id) else {
        return Dedup::Fresh;
    };
    match meta.seq_num.cmp(&state.last_sequence_number) {
        Ordering::Equal => {
            state.last_seen_index = applied_index;
            Dedup::Replay(state.cached_result.clone())
        }
        Ordering::Less => {
            state.last_seen_index = applied_index;
            Dedup::Stale
        }
        Ordering::Greater => Dedup::Fresh,
    }
}

/// Records the result of a freshly applied request.
pub fn record(
    table: &mut IdempotencyTable,
    meta: ClientMeta,
    result: CommandResult,
    applied_index: u64,
) {
    table.insert(
        meta.client_id,
        ClientState {
            last_sequence_number: meta.seq_num,
            cached_result: result,
            last_seen_index: applied_index,
        },
    );
}

/// Evicts sessions untouched for [`SESSION_TTL_ENTRIES`] committed entries.
///
/// Sweeps only on a [`GC_INTERVAL_ENTRIES`] boundary so the cost is amortized
/// rather than paid on every apply. Both the trigger and the threshold are
/// functions of the committed index, so every node evicts the same sessions at
/// the same entry and the tables stay byte-identical across the sweep.
///
/// The age arithmetic cannot overflow: the remainder is bounded by a non-zero
/// constant, and the subtraction saturates. A `last_seen_index` ahead of
/// `applied_index` means a broken invariant upstream — Raft snapshots only
/// advance — and saturates to an age of 0, which retains the session. Eviction
/// is the destructive direction, so an unreadable age fails toward keeping.
pub fn gc(table: &mut IdempotencyTable, applied_index: u64) {
    if applied_index == 0 || !applied_index.is_multiple_of(GC_INTERVAL_ENTRIES) {
        return;
    }
    table.retain(|_, state| {
        applied_index.saturating_sub(state.last_seen_index) <= SESSION_TTL_ENTRIES
    });
}
