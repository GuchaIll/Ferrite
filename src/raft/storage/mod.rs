//! Raft-specific persistence types and storage boundary.
//!
//! This module owns data that must survive a node restart. Backend mechanics
//! remain behind the storage implementation rather than in the Raft core.
//!
//! - [`memory`] — in-memory backend for the sim and unit tests
//! - [`disk`] — segment-file backend (issue 03; currently stubbed)

mod hard_state;
pub mod disk;
pub mod memory;

pub use disk::DiskStorage;
pub use hard_state::HardState;
