//! Driver-facing inputs and outputs, as the simulator sees them.
//!
//! The contract itself lives in [`crate::raft::driver`], because the runtime
//! node drives the core through exactly these types. Keeping one definition is
//! what makes "the simulator sits behind the same boundary as production" a
//! fact about the code rather than a claim about it.

pub use crate::raft::driver::{Input, Output};
