//! Deterministic simulation driver CLI.
//!
//! ```text
//! ferrite-sim run --scenario election --seed N [--ticks 600]
//! ```

use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use ferrite::sim::scenario::{elect_within, run_election_scenario};

#[derive(Debug, Parser)]
#[command(name = "ferrite-sim", about = "Deterministic Ferrite simulator")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Run a named scenario.
    Run {
        /// Scenario name (currently: `election`).
        #[arg(long)]
        scenario: String,

        /// Root RNG seed.
        #[arg(long, default_value_t = 1)]
        seed: u64,

        /// Maximum logical ticks to run.
        #[arg(long, default_value_t = 600)]
        ticks: u64,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Run {
            scenario,
            seed,
            ticks,
        } => match scenario.as_str() {
            "election" => {
                let result = elect_within(seed, ticks);
                println!(
                    "scenario=election seed={seed} ticks_run={} leaders={:?} leader_term={:?}",
                    result.ticks_run, result.leaders, result.leader_term
                );
                if result.leaders.len() != 1 {
                    bail!(
                        "election failed: expected exactly one leader, got {:?}",
                        result.leaders
                    );
                }
                // Also expose the full-run helper so both paths stay linked.
                let _ = run_election_scenario(seed, result.ticks_run);
                Ok(())
            }
            other => bail!("unknown scenario '{other}' (supported: election)"),
        },
    }
}
