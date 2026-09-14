//! Named simulator scenarios used by `ferrite-sim` and integration tests.

pub mod election;
pub mod replicate;

pub use election::{ElectionScenarioResult, elect_within, run_election_scenario};
pub use replicate::{ReplicateScenarioResult, run_replicate_scenario};
