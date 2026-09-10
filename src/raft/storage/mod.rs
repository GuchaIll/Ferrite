//! Raft-specific persistence types and storage boundary.
//!
//! This module owns data that must survive a node restart. Backend mechanics
//! remain behind the storage implementation rather than in the Raft core.

mod hard_state;
pub mod memory;

pub use hard_state::HardState;
