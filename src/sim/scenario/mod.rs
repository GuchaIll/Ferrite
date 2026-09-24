//! Named simulator scenarios used by `ferrite-sim` and integration tests.

pub mod crash_restart;
pub mod election;
pub mod replicate;

pub use crash_restart::{CrashRestartResult, run_crash_restart_scenario};
pub use election::{ElectionScenarioResult, elect_within, run_election_scenario};
pub use replicate::{
    LaggingFollowerResult, ReplicateScenarioResult, run_lagging_follower_scenario,
    run_replicate_scenario,
};
