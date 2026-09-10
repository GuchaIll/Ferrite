//! Simulated network behavior.
//!
//! Same-tick, reliable, ordered delivery. Partition/loss/delay can layer on
//! later without changing the driver contract.

use std::collections::VecDeque;

use crate::{config::NodeId, raft::RaftRpc};

/// One in-flight RPC waiting for delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InFlight {
    pub from: NodeId,
    pub to: NodeId,
    pub rpc: RaftRpc,
}

/// Deterministic mailbox for RPCs between simulated nodes.
#[derive(Debug, Default)]
pub struct Network {
    queue: VecDeque<InFlight>,
}

impl Network {
    /// Creates an empty network.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enqueues an RPC for later delivery (FIFO).
    pub fn enqueue(&mut self, from: NodeId, to: NodeId, rpc: RaftRpc) {
        self.queue.push_back(InFlight { from, to, rpc });
    }

    /// Pops the next in-flight RPC, if any.
    pub fn pop_front(&mut self) -> Option<InFlight> {
        self.queue.pop_front()
    }

    /// Returns whether the mailbox is empty.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Returns the number of queued RPCs.
    pub fn len(&self) -> usize {
        self.queue.len()
    }
}
