//! Simulated network behavior.
//!
//! Same-tick, reliable, ordered delivery. Partition/loss/delay can layer on
//! later without changing the driver contract.

use std::collections::{BTreeSet, VecDeque};

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
    /// Nodes whose inbound and outbound messages are silently dropped.
    isolated: BTreeSet<NodeId>,
}

impl Network {
    /// Creates an empty network.
    pub fn new() -> Self {
        Self::default()
    }

    /// Drops all messages to or from `node_id` until [`Network::connect`] is called.
    pub fn isolate(&mut self, node_id: NodeId) {
        self.isolated.insert(node_id);
    }

    /// Restores delivery for `node_id`.
    pub fn connect(&mut self, node_id: NodeId) {
        self.isolated.remove(&node_id);
    }

    /// Enqueues an RPC for later delivery (FIFO).
    ///
    /// Silently drops the message if either endpoint is currently isolated.
    pub fn enqueue(&mut self, from: NodeId, to: NodeId, rpc: RaftRpc) {
        if self.isolated.contains(&from) || self.isolated.contains(&to) {
            return;
        }
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
