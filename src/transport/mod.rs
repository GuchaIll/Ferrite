//! The transport boundary: how an [`Output::Send`] leaves a node.
//!
//! [`Output::Send`]: crate::raft::Output::Send
//!
//! One synchronous method, implemented twice — by the simulator's network and
//! by the gRPC peer transport. Synchronous is the load-bearing part:
//!
//! - The core never waits for a reply. Responses re-enter as
//!   [`Input::Message`], so `send` has nothing to return and no reason to
//!   suspend.
//! - A driver cannot accidentally make delivery a back-pressure point on
//!   consensus. Raft tolerates lost messages; it does not tolerate a leader
//!   stalling on one unreachable follower.
//! - The simulator stays free of futures, which is what its determinism gate
//!   enforces.
//!
//! [`Input::Message`]: crate::raft::Input::Message
//!
//! Implementations must not block. Dropping a message is always a legal
//! response to a full queue or a dead peer.

use crate::{config::NodeId, raft::RaftRpc};

pub mod convert;
pub mod grpc;

/// Carries Raft RPCs between nodes.
pub trait Transport {
    /// Hands `rpc` off for delivery to `to`, without blocking and without
    /// waiting for a reply.
    ///
    /// Delivery is best-effort by design: an implementation that cannot take
    /// the message right now must drop it and return, never stall the caller.
    fn send(&mut self, from: NodeId, to: NodeId, rpc: RaftRpc);
}
