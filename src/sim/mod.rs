//! Deterministic simulator harness.

pub mod clock;
pub mod network;
pub mod trace;

pub type NodeId = u64;

pub enum SimInput {
    Tick,
    //Message(NodeId, Vec<u8>),
}

pub enum SimOutput {
    //Send, Apply, Persist
}

pub trait SimNode {
    fn id(&self) -> NodeId;
    fn step(&mut self, input: SimInput) -> SimOutput;
}
