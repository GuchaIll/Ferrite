//! `ferrite` CLI — `validate`, `init`, and `run` subcommands.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use ferrite::{
    cli::{InitArgs, ValidateArgs, run_init, run_validate},
    config::StorageBackend,
    server::run_node,
};

#[derive(Parser)]
#[command(name = "ferrite", about = "Raft-backed distributed key-value store")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Load and validate a node config file, printing the resolved config.
    ///
    /// Exits 0 on success, 1 on any error. Error messages name the offending
    /// keys; no panic backtrace is printed.
    Validate {
        /// Path to the TOML config file.
        #[arg(long, value_name = "PATH")]
        config: Option<PathBuf>,

        /// Override cluster.peers entirely. Repeatable. Format: `<id>@<host:port>`.
        /// If any --peer flag is present, the file's peers list is discarded.
        #[arg(long = "peer", value_name = "ID@HOST:PORT")]
        peers: Vec<String>,

        /// Override cluster.client_endpoints entirely. Repeatable.
        /// Format: `<id>@<host:port>`. Replaces file endpoints if any flag present.
        #[arg(long = "client-endpoint", value_name = "ID@HOST:PORT")]
        client_endpoints: Vec<String>,

        /// Override raft.heartbeat_interval_ms.
        #[arg(long, value_name = "MS")]
        heartbeat_interval_ms: Option<u64>,

        /// Override raft.rpc_timeout_ms.
        #[arg(long, value_name = "MS")]
        rpc_timeout_ms: Option<u64>,

        /// Override raft.election_timeout_min_ms.
        #[arg(long, value_name = "MS")]
        election_timeout_min_ms: Option<u64>,

        /// Override raft.election_timeout_max_ms.
        #[arg(long, value_name = "MS")]
        election_timeout_max_ms: Option<u64>,
    },

    /// Generate initial TOML config files for a local cluster.
    ///
    /// Writes node1.toml … nodeN.toml to --dir. Every file passes
    /// `ferrite validate` before being written. Exits 0 on success, 1 on error.
    Init {
        /// Number of nodes to generate. Must be >= 1.
        #[arg(long, value_name = "N")]
        nodes: usize,

        /// Output directory. Created if it does not exist.
        #[arg(long, value_name = "PATH")]
        dir: PathBuf,

        /// First Raft port. Node i gets base_port + (i - 1). Default: 7001.
        #[arg(long, default_value = "7001", value_name = "PORT")]
        base_port: u16,

        /// First KV/client port. Node i gets client_port + (i - 1). Default: 8001.
        #[arg(long = "client-port", default_value = "8001", value_name = "PORT")]
        client_port: u16,

        /// Overwrite existing node*.toml files (leaves other files in --dir intact).
        #[arg(long)]
        force: bool,
    },

    /// Run the Raft node until SIGTERM or Ctrl-C.
    ///
    /// Serves the Raft peer service on `cluster.listen_addr` and joins the
    /// cluster. Takes the same config and override flags as `validate`, and
    /// refuses to start on anything `validate` would reject.
    Run {
        /// Path to the TOML config file.
        #[arg(long, value_name = "PATH")]
        config: Option<PathBuf>,

        /// Override cluster.peers entirely. Repeatable. Format: `<id>@<host:port>`.
        #[arg(long = "peer", value_name = "ID@HOST:PORT")]
        peers: Vec<String>,

        /// Override cluster.client_endpoints entirely. Repeatable.
        #[arg(long = "client-endpoint", value_name = "ID@HOST:PORT")]
        client_endpoints: Vec<String>,

        /// Override raft.heartbeat_interval_ms.
        #[arg(long, value_name = "MS")]
        heartbeat_interval_ms: Option<u64>,

        /// Override raft.rpc_timeout_ms.
        #[arg(long, value_name = "MS")]
        rpc_timeout_ms: Option<u64>,

        /// Override raft.election_timeout_min_ms.
        #[arg(long, value_name = "MS")]
        election_timeout_min_ms: Option<u64>,

        /// Override raft.election_timeout_max_ms.
        #[arg(long, value_name = "MS")]
        election_timeout_max_ms: Option<u64>,
    },
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}

async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Validate {
            config,
            peers,
            client_endpoints,
            heartbeat_interval_ms,
            rpc_timeout_ms,
            election_timeout_min_ms,
            election_timeout_max_ms,
        } => {
            let cfg = run_validate(ValidateArgs {
                config,
                peers,
                client_endpoints,
                heartbeat_interval_ms,
                rpc_timeout_ms,
                election_timeout_min_ms,
                election_timeout_max_ms,
            })?;

            println!("Config validated successfully.");
            println!("  node_id:   {}", cfg.cluster.node_id);
            println!("  raft_bind: {}", cfg.cluster.raft_bind);
            println!("  kv_bind:   {}", cfg.cluster.kv_bind);
            println!(
                "  peers:     {} (quorum = {})",
                cfg.cluster.peers.len(),
                cfg.cluster.quorum()
            );
            println!(
                "  timing:    heartbeat={}ms  rpc={}ms  election={}..{}ms",
                cfg.raft.heartbeat_interval.as_millis(),
                cfg.raft.rpc_timeout.as_millis(),
                cfg.raft.election_timeout_min.as_millis(),
                cfg.raft.election_timeout_max.as_millis(),
            );
            let storage_desc = match &cfg.storage.backend {
                StorageBackend::Memory => "memory".to_owned(),
                StorageBackend::Disk { data_dir } => {
                    format!("disk ({})", data_dir.display())
                }
            };
            println!("  storage:   {storage_desc}");
        }

        Commands::Init {
            nodes,
            dir,
            base_port,
            client_port,
            force,
        } => {
            let written = run_init(InitArgs {
                nodes,
                dir,
                base_port,
                client_port,
                force,
            })?;
            for path in &written {
                println!("wrote {}", path.display());
            }
        }

        Commands::Run {
            config,
            peers,
            client_endpoints,
            heartbeat_interval_ms,
            rpc_timeout_ms,
            election_timeout_min_ms,
            election_timeout_max_ms,
        } => {
            // Same validation path as `ferrite validate`: a node that would fail
            // validation must not start half-configured.
            let cfg = run_validate(ValidateArgs {
                config,
                peers,
                client_endpoints,
                heartbeat_interval_ms,
                rpc_timeout_ms,
                election_timeout_min_ms,
                election_timeout_max_ms,
            })?;

            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
                )
                .init();

            run_node(cfg).await?;
        }
    }

    Ok(())
}
