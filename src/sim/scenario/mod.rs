//! Named simulator scenarios used by `ferrite-sim` and integration tests.

pub mod election;

pub use election::{ElectionScenarioResult, elect_within, run_election_scenario};
