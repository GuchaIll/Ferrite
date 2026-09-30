//! `ferrite run`: assembles a node from config and runs it until shut down.
//!
//! Startup order matters. Storage is recovered *before* the core is built,
//! because a recovered node must come back with its durable term, vote, and log
//! rather than as a fresh follower that could vote twice in one term. Peer
//! connections are lazy, so a node starts even when it is the first one up.

use std::collections::BTreeMap;

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tonic::transport::Server;

use crate::{
    config::{Config, NodeId},
    kv::KvStateMachine,
    proto::kv::kv_server::KvServer,
    proto::raft::raft_service_server::RaftServiceServer,
    raft::{RaftNode, storage},
    server::{
        kv_service::KvServiceImpl,
        node::{self, NodeParts, NodeRuntime, TickSchedule},
        raft_service::RaftServiceImpl,
    },
    transport::grpc::GrpcTransport,
};

/// Errors that stop a node from starting or running.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error("storage: {0}")]
    Storage(#[from] crate::error::Error),

    #[error("could not serve on {addr}: {source}")]
    Serve {
        addr: std::net::SocketAddr,
        #[source]
        source: tonic::transport::Error,
    },

    #[error("startup task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

/// Recovers durable state and starts a node's actor and peer transport.
///
/// Serving is left to the caller, which is how the cluster tests bind their own
/// listeners while running exactly the assembly production runs.
///
/// # What is recovered vs rebuilt
///
/// Hard state and the Raft log come back from disk. The key-value state machine
/// does **not**: it is always [`KvStateMachine::new`]. After a crash or stop,
/// applied state is rebuilt only as the leader drives `commitIndex` via
/// AppendEntries (or later via snapshot install). Callers must not assume the
/// SM is warm immediately after [`spawn_node`] returns.
///
/// # Shutdown
///
/// The `shutdown` watch is cooperative. Dropping the process with `SIGKILL`
/// mid-`spawn_blocking` is a different failure mode and is covered by storage
/// crash/restart tests, not by this path alone.
pub async fn spawn_node(
    config: &Config,
    tasks: &mut JoinSet<()>,
    shutdown: watch::Receiver<bool>,
) -> Result<NodeRuntime, RunError> {
    let id = config.cluster.node_id;
    let node = recover_node(config).await?;
    let schedule = TickSchedule::from_config(&config.raft);

    // Lazy connections: peers that are not up yet cost nothing here.
    let transport = GrpcTransport::spawn(&config.cluster.peers, id, config.raft.rpc_timeout, tasks);

    Ok(node::spawn(
        NodeParts {
            node,
            storage: storage::open(&config.storage.backend)?,
            // Intentionally empty: durable SM snapshot/replay is not wired here.
            // Catch-up is AE-driven after restart (see module docs above).
            state_machine: KvStateMachine::new(),
            transport,
            schedule,
            compaction_threshold: config.raft.compaction_threshold,
        },
        tasks,
        shutdown,
    ))
}

/// Runs a node until `SIGTERM`/Ctrl-C, then shuts it down and joins its tasks.
pub async fn run_node(config: Config) -> Result<(), RunError> {
    let id = config.cluster.node_id;
    let mut tasks = JoinSet::new();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let runtime = spawn_node(&config, &mut tasks, shutdown_rx.clone()).await?;

    let raft_bind = config.cluster.raft_bind;
    let kv_bind = config.cluster.kv_bind;

    tracing::info!(
        node_id = id,
        %raft_bind,
        %kv_bind,
        peers = config.cluster.peers.len(),
        quorum = config.cluster.quorum(),
        "serving"
    );

    let mut raft_shutdown = shutdown_rx.clone();
    let raft_server = Server::builder()
        .add_service(RaftServiceServer::new(RaftServiceImpl::new(
            runtime.inbound,
        )))
        .serve_with_shutdown(raft_bind, async move {
            // `changed()` only errs when every sender is gone, which is also a
            // shutdown; either way stop serving.
            let _ = raft_shutdown.changed().await;
        });

    let mut kv_shutdown = shutdown_rx.clone();
    // Moved, not cloned: `run_node` owns the config and nothing reads this field again.
    let kv_server = Server::builder()
        .add_service(KvServer::new(KvServiceImpl::new(
            runtime.handle,
            config.cluster.kv_advertise,
        )))
        .serve_with_shutdown(kv_bind, async move {
            let _ = kv_shutdown.changed().await;
        });

    let result = tokio::select! {
        served = raft_server => served.map_err(|source| RunError::Serve { addr: raft_bind, source }),
        served = kv_server => served.map_err(|source| RunError::Serve { addr: kv_bind, source }),
        () = wait_for_shutdown_signal() => Ok(()),
    };

    tracing::info!(node_id = id, "shutting down");
    // Stops the actor, which drops the per-peer queues, which ends the peer
    // tasks. No task is left detached.
    let _ = shutdown_tx.send(true);

    while let Some(joined) = tasks.join_next().await {
        if let Err(error) = joined {
            tracing::error!(node_id = id, %error, "task did not shut down cleanly");
        }
    }

    result
}

/// Recovers durable state and rebuilds the core from it.
async fn recover_node(config: &Config) -> Result<RaftNode, RunError> {
    let backend = config.storage.backend.clone();
    // `open` and `recover` both do blocking file IO.
    let recovered =
        tokio::task::spawn_blocking(move || storage::open(&backend)?.recover()).await??;

    let id = config.cluster.node_id;
    let peers = peer_ids(&config.cluster.peers, id);

    tracing::info!(
        node_id = id,
        term = recovered.hard_state.current_term,
        voted_for = ?recovered.hard_state.voted_for,
        last_index = recovered.log.last_index(),
        "recovered durable state"
    );

    // A fresh entropy-seeded stream per process: election jitter must not repeat
    // across a restart, or a node that keeps crashing keeps colliding with the
    // same peer's timeout.
    let rng = ChaCha8Rng::from_entropy();
    Ok(RaftNode::recover(id, peers, rng, recovered))
}

/// Peer ids excluding this node.
fn peer_ids(peers: &BTreeMap<NodeId, crate::config::RaftPeer>, me: NodeId) -> Vec<NodeId> {
    peers.keys().copied().filter(|&id| id != me).collect()
}

/// Resolves on `SIGTERM` or Ctrl-C.
async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        // If the handler cannot be installed, fall back to Ctrl-C alone rather
        // than refusing to run.
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = term.recv() => tracing::info!("received SIGTERM"),
                    result = tokio::signal::ctrl_c() => {
                        if let Err(error) = result {
                            tracing::error!(%error, "ctrl-c handler failed");
                        }
                    }
                }
            }
            Err(error) => {
                tracing::error!(%error, "could not install SIGTERM handler");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Startup timing guard: the tick period must be short enough that an election
/// timeout is observable, and long enough not to spin.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RaftConfig;
    use std::time::Duration;

    #[test]
    fn peer_ids_exclude_this_node() {
        let mut peers = BTreeMap::new();
        for id in 1..=3 {
            peers.insert(
                id,
                crate::config::RaftPeer {
                    addr: format!("127.0.0.1:700{id}").parse().expect("addr"),
                },
            );
        }

        assert_eq!(peer_ids(&peers, 2), vec![1, 3]);
    }

    #[test]
    fn default_timing_yields_a_usable_tick_period() {
        let schedule = TickSchedule::from_config(&RaftConfig::default());
        assert_eq!(schedule.period, Duration::from_millis(5));
        assert_eq!(schedule.election_min_ticks, 60);
        assert_eq!(schedule.election_max_ticks, 120);
    }
}
