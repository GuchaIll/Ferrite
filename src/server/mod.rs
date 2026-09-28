//! The running node: the consensus actor and the gRPC services around it.

pub mod kv_service;
pub mod node;
pub mod raft_service;
pub mod run;

pub use node::{Applied, NodeError, NodeHandle, NodeRuntime, Proposed, TickSchedule};
pub use run::{RunError, run_node};
