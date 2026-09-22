//! Driver-facing inputs and outputs for deterministic simulation.

use crate::{
    config::NodeId,
    raft::{HardState, LogEntry, RaftRpc, Snapshot, SnapshotMeta},
};

/// Input delivered by the deterministic driver to a simulated node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Input {
    /// One global logical tick has begun.
    Tick,
    /// An RPC received from another simulated node.
    Message { from: NodeId, rpc: RaftRpc },
    /// A serialized command submitted by a simulated client.
    ClientCommand(Vec<u8>),
    /// State-machine snapshot bytes produced after [`Output::RequestSnapshot`].
    SnapshotTaken(Snapshot),
    /// Driver finished draining [`Output::PersistSnapshot`]; log may now trim.
    SnapshotPersisted(SnapshotMeta),
}

/// Ordered work returned from a simulated node for its driver to drain.
///
/// Ordering contract:
/// - `Persist` (hard state) before a dependent `Send`
/// - `PersistLog` before a dependent `Send`: a follower's success reply, and
///   any AppendEntries carrying a leader's newly appended entry
/// - Durability boundary is the drained batch: every write output in a batch
///   is durable before the first `Send`, `Apply`, or `ApplySnapshot` after it
/// - One batch in flight per node: the driver finishes draining (and making
///   durable) a node's batch before stepping that node again. The leader
///   counts its own `last_index` toward a majority, which is safe only
///   because of this rule
/// - `PersistSnapshot` before the log prefix it replaces is discarded
/// - `PersistSnapshot` before a dependent InstallSnapshot reply `Send`
/// - `ApplySnapshot` installs state-machine bytes (including the dedup table)
///
/// Snapshot flow: `RequestSnapshot` → driver SM encode → `SnapshotTaken` →
/// `PersistSnapshot` → (issue 03 durable write) → `SnapshotPersisted` → trim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output {
    /// Test-only evidence emitted by the simulator echo fixture.
    #[cfg(test)]
    Echo { payload: Vec<u8> },
    /// Deliver an RPC through the simulated network.
    Send { to: NodeId, rpc: RaftRpc },
    /// Apply a committed log entry to the application state machine.
    Apply(LogEntry),
    /// Make Raft election metadata durable.
    Persist(HardState),
    /// Make a log mutation durable: drop entries at or above `truncate_from`
    /// (if set), then append `entries`.
    PersistLog {
        truncate_from: Option<u64>,
        entries: Vec<LogEntry>,
    },
    /// Ask the driver to snapshot the state machine at this log boundary.
    RequestSnapshot {
        last_included_index: u64,
        last_included_term: u64,
    },
    /// Make a snapshot durable before the replaced log prefix is discarded.
    PersistSnapshot(Snapshot),
    /// Install snapshot bytes into the application state machine.
    ApplySnapshot(Snapshot),
}
