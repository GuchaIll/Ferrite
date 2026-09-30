//! `ferrite-ctl`: one KV operation against a running cluster.
//!
//! Exit codes: 0 success, 2 CAS did not apply, 1 error.
//!
//! # Timeouts and retries
//!
//! Each invocation creates a fresh [`ferrite::client::KvClient`] session
//! (`client_id`). A timed-out write may still have committed under that session;
//! re-running the same CLI command starts a **new** session and can apply twice.
//! Prefer a longer `--timeout-ms`, or a long-lived process that keeps one
//! `KvClient` so unconfirmed ops stay under the same `(client_id, seq_num)`.

use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use ferrite::client::{ClientConfig, KvClient};

#[derive(Parser)]
#[command(name = "ferrite-ctl", about = "Talk to a ferrite cluster's KV API")]
struct Cli {
    /// Comma-separated KV addresses (`client_addr` in each node's config).
    #[arg(long, value_delimiter = ',', required = true)]
    endpoints: Vec<SocketAddr>,

    /// Deadline for the whole operation, retries included.
    #[arg(long, default_value_t = 5000)]
    timeout_ms: u64,

    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Get {
        key: String,
    },
    Put {
        key: String,
        value: String,
    },
    Delete {
        key: String,
    },
    /// Compare-and-swap on the key's version.
    Cas {
        key: String,
        /// Expected version, or `absent` for create-if-missing.
        #[arg(long)]
        expect: String,
        #[arg(long, conflicts_with = "delete", required_unless_present = "delete")]
        value: Option<String>,
        /// Delete the key if the version matches.
        #[arg(long)]
        delete: bool,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(code) => code,
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::from(1)
        }
    }
}

async fn run(cli: Cli) -> Result<ExitCode> {
    let request_timeout = Duration::from_millis(cli.timeout_ms);
    let mut client = KvClient::new(ClientConfig {
        endpoints: cli.endpoints,
        request_timeout,
        // Several attempts fit in one deadline, so a dead first node is skipped.
        attempt_timeout: (request_timeout / 5).max(Duration::from_millis(100)),
    })?;

    match cli.command {
        Cmd::Get { key } => {
            let reply = client.get(key.as_bytes()).await?;
            match (reply.value, reply.version) {
                (Some(value), Some(version)) => {
                    println!(
                        "value={} version={version}",
                        String::from_utf8_lossy(&value)
                    );
                }
                _ => println!("(nil)"),
            }
        }
        Cmd::Put { key, value } => {
            let reply = client.put(key.as_bytes(), value.as_bytes()).await?;
            println!("version={}", reply.version);
        }
        Cmd::Delete { key } => {
            let reply = client.delete(key.as_bytes()).await?;
            println!(
                "{}",
                if reply.previous.is_some() {
                    "deleted"
                } else {
                    "(nil)"
                }
            );
        }
        Cmd::Cas {
            key, expect, value, ..
        } => {
            let expected = if expect == "absent" {
                None
            } else {
                Some(expect.parse().with_context(|| {
                    format!("--expect must be a version or `absent`, got {expect:?}")
                })?)
            };
            let reply = client
                .cas(
                    key.as_bytes(),
                    expected,
                    value.as_deref().map(str::as_bytes),
                )
                .await?;
            if !reply.applied {
                println!("cas: failed actual_version={:?}", reply.actual_version);
                return Ok(ExitCode::from(2));
            }
            println!("cas: applied version={:?}", reply.version);
        }
    }
    Ok(ExitCode::SUCCESS)
}
