//! Test fixtures for the deterministic simulation harness.

use rand::RngCore;
use rand_chacha::ChaCha8Rng;

use super::{Input, Output, SimNode, node_rng};
use crate::config::NodeId;

/// Minimal node used to prove the simulator tick and RNG plumbing.
pub struct EchoNode {
    id: NodeId,
    rng: ChaCha8Rng,
}

impl EchoNode {
    /// Constructs an echo node with an independent deterministic RNG stream.
    pub fn new(root_seed: u64, id: NodeId) -> Self {
        Self {
            id,
            rng: node_rng(root_seed, id),
        }
    }
}

impl SimNode for EchoNode {
    fn id(&self) -> NodeId {
        self.id
    }

    fn step(&mut self, input: Input) -> Vec<Output> {
        match input {
            Input::Tick => vec![Output::Echo {
                payload: self.rng.next_u64().to_le_bytes().to_vec(),
            }],
            Input::Message { .. } | Input::ClientCommand(_) => Vec::new(),
        }
    }
}
