//! Driver-facing inputs and outputs for deterministic simulation.

use crate::{
    config::NodeId,
    raft::{HardState, LogEntry, RaftRpc},
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
}

/// Ordered work returned from a simulated node for its driver to drain.
///
/// `Persist` must be drained before a dependent `Send`, since Raft requires a
/// node to persist `current_term` and `voted_for` before responding to an RPC.
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
}
