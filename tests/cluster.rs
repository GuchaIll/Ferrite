//! In-process cluster tests: real gRPC, real actors, real disk.
//!
//! Each node is the same assembly `ferrite run` builds — `spawn_node` plus a
//! tonic server — so these tests exercise the production path rather than a
//! stand-in for it. Ports come from listeners the harness already bound, so
//! nothing is guessed and parallel test binaries cannot collide.
//!
//! # Stop vs process kill
//!
//! [`TestCluster::stop`] is a cooperative shutdown (`watch` → actor exit →
//! server stop). That is enough to exercise failover and restart-from-disk.
//! It is **not** `SIGKILL` mid-`spawn_blocking`: process death while a commit
//! is in flight is covered by the durability crash/restart scenarios and by
//! manual multi-process runs, not by these in-process tests.
//!
//! # Restart and the state machine
//!
//! `spawn_node` recovers the Raft log and hard state, then builds a **fresh**
//! [`ferrite::kv::KvStateMachine`]. Catch-up after restart is therefore driven
//! by AppendEntries (log identity + apply stream), not by an instantly warm KV
//! map. Assert durable log / applied indices here; full SM-on-disk is later work.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use ferrite::{
    config::{
        ClusterConfig, Config, LoggingConfig, NodeId, RaftConfig, RaftPeer, StorageBackend,
        StorageConfig,
    },
    kv::{ClientRequest, Command, CommandResult},
    proto::raft::raft_service_server::RaftServiceServer,
    raft::{
        LogEntry,
        storage::{self, Storage},
    },
    server::{
        Applied, NodeError, NodeHandle, raft_service::RaftServiceImpl, run::spawn_node,
    },
};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_stream::wrappers::TcpListenerStream;

/// Timing tuned for a loopback cluster: fast enough that a test finishes in
/// seconds, slow enough that a debug-build fsync does not trip an election.
fn test_timing() -> RaftConfig {
    RaftConfig {
        heartbeat_interval: Duration::from_millis(50),
        rpc_timeout: Duration::from_millis(100),
        election_timeout_min: Duration::from_millis(300),
        election_timeout_max: Duration::from_millis(600),
        compaction_threshold: 0,
    }
}

struct TestCluster {
    nodes: BTreeMap<NodeId, NodeHandle>,
    /// One per running node, so a test can stop a single node without the others.
    shutdowns: BTreeMap<NodeId, watch::Sender<bool>>,
    tasks: JoinSet<()>,
    /// Durable directories keyed by node id so a stopped node can restart from
    /// the same segment files.
    data_dirs: BTreeMap<NodeId, tempfile::TempDir>,
    /// Raft listen addresses fixed for the life of the cluster (including
    /// black-holed and restarted nodes).
    raft_addrs: BTreeMap<NodeId, SocketAddr>,
    peers: BTreeMap<NodeId, RaftPeer>,
    kv_advertise: BTreeMap<NodeId, SocketAddr>,
}

impl TestCluster {
    /// Starts `total` configured nodes, running only those in `run`.
    ///
    /// A configured-but-not-started node is a black hole: its address is in
    /// everyone's peer list and nothing answers there. Data directories still
    /// exist for every id so a later [`Self::restart`] can bring a black hole
    /// or a stopped node back on the same disk.
    async fn start(total: u64, run: &[NodeId]) -> Self {
        // Bind every listener first so all addresses are known before any node
        // starts; a node needs its peers' addresses at construction.
        let mut listeners = BTreeMap::new();
        let mut raft_addrs = BTreeMap::new();
        for id in 1..=total {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind raft");
            raft_addrs.insert(id, listener.local_addr().expect("addr"));
            listeners.insert(id, listener);
        }

        let mut peers = BTreeMap::new();
        let mut kv_advertise = BTreeMap::new();
        for (&id, &addr) in &raft_addrs {
            peers.insert(id, RaftPeer { addr });
            // Distinct from the Raft port on purpose: the client-facing map is
            // not Raft membership. Issue 05 serves here.
            kv_advertise.insert(id, SocketAddr::new(addr.ip(), addr.port() + 1000));
        }

        let mut data_dirs = BTreeMap::new();
        for id in 1..=total {
            data_dirs.insert(id, tempfile::tempdir().expect("tempdir"));
        }

        let mut cluster = Self {
            nodes: BTreeMap::new(),
            shutdowns: BTreeMap::new(),
            tasks: JoinSet::new(),
            data_dirs,
            raft_addrs,
            peers,
            kv_advertise,
        };

        for id in run.iter().copied() {
            let listener = listeners.remove(&id).expect("listener");
            let _ = cluster.spawn_running(id, listener).await;
        }

        cluster
    }

    fn config_for(&self, id: NodeId) -> Config {
        let data_dir = self.data_dirs[&id].path().to_path_buf();
        Config {
            cluster: ClusterConfig {
                node_id: id,
                raft_bind: self.raft_addrs[&id],
                kv_bind: self.kv_advertise[&id],
                peers: self.peers.clone(),
                kv_advertise: self.kv_advertise.clone(),
            },
            raft: test_timing(),
            storage: StorageConfig {
                // The real segment-file backend, fsync included: a test that
                // proves ordering against an in-memory stub proves nothing
                // about the durability boundary.
                backend: StorageBackend::Disk { data_dir },
            },
            logging: LoggingConfig::default(),
        }
    }

    /// Spawns actor + tonic server for `id` on `listener`, using the durable
    /// directory already registered for that id.
    ///
    /// Returns an apply subscription taken before this future yields again.
    async fn spawn_running(
        &mut self,
        id: NodeId,
        listener: TcpListener,
    ) -> tokio::sync::broadcast::Receiver<Applied> {
        assert_eq!(
            listener.local_addr().expect("addr"),
            self.raft_addrs[&id],
            "restart must rebind the same Raft address peers already know"
        );

        let config = self.config_for(id);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let runtime = spawn_node(&config, &mut self.tasks, shutdown_rx.clone())
            .await
            .expect("spawn node");
        // Subscribe before any await so catch-up applies are less likely to be
        // missed between actor start and the caller's first poll.
        let apply_rx = runtime.handle.subscribe_apply();

        let mut serve_shutdown = shutdown_rx;
        self.tasks.spawn(async move {
            let served = tonic::transport::Server::builder()
                .add_service(RaftServiceServer::new(RaftServiceImpl::new(
                    runtime.inbound,
                )))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let _ = serve_shutdown.changed().await;
                })
                .await;
            if let Err(error) = served {
                eprintln!("node {id} server stopped: {error}");
            }
        });

        self.nodes.insert(id, runtime.handle);
        self.shutdowns.insert(id, shutdown_tx);
        apply_rx
    }

    /// Restarts a previously stopped (or never-started) node from its disk dir
    /// on the same Raft address.
    ///
    /// Returns an apply subscription taken immediately after the actor starts so
    /// catch-up tests can observe re-application without missing the first AE.
    async fn restart(&mut self, id: NodeId) -> tokio::sync::broadcast::Receiver<Applied> {
        assert!(
            !self.nodes.contains_key(&id),
            "node {id} is still running; stop it first"
        );
        assert!(
            self.data_dirs.contains_key(&id),
            "no data dir registered for node {id}"
        );

        let addr = self.raft_addrs[&id];
        // Port release after cooperative stop can lag slightly on loopback.
        let listener = {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            loop {
                match TcpListener::bind(addr).await {
                    Ok(listener) => break listener,
                    Err(error) => {
                        assert!(
                            tokio::time::Instant::now() < deadline,
                            "could not rebind {addr} for node {id}: {error}"
                        );
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
            }
        };

        self.spawn_running(id, listener).await
    }

    /// Waits until some running node reports itself leader.
    async fn await_leader(&self, within: Duration) -> NodeId {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            for (&id, handle) in &self.nodes {
                if handle.role().borrow().is_leader {
                    return id;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no leader within {within:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Every node that currently believes it is leader.
    fn leaders(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|(_, handle)| handle.role().borrow().is_leader)
            .map(|(&id, _)| id)
            .collect()
    }

    /// Cooperative stop: actor and gRPC server exit; durable files stay.
    ///
    /// Not a process kill — see the module docs.
    async fn stop(&mut self, id: NodeId) {
        if let Some(shutdown) = self.shutdowns.remove(&id) {
            let _ = shutdown.send(true);
        }
        self.nodes.remove(&id);
        // Give the actor and server a turn to release the listen port before a
        // restart rebinds it, and before survivors are expected to notice silence.
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    /// Reads the durable Raft log from a **stopped** node's data directory.
    ///
    /// Must not be used while that node is still running: `DiskStorage::open`
    /// may truncate a torn tail on the live segment file.
    fn durable_log_entries(&self, id: NodeId) -> Vec<LogEntry> {
        assert!(
            !self.nodes.contains_key(&id),
            "durable_log_entries requires node {id} stopped"
        );
        let path = self.data_dirs[&id].path().to_path_buf();
        recover_log_entries(path)
    }

    async fn shutdown(mut self) {
        for (_, shutdown) in std::mem::take(&mut self.shutdowns) {
            let _ = shutdown.send(true);
        }
        self.tasks.shutdown().await;
    }
}

fn recover_log_entries(data_dir: PathBuf) -> Vec<LogEntry> {
    let backend = StorageBackend::Disk { data_dir };
    let store = storage::open(&backend).expect("open durable store");
    let recovered = store.recover().expect("recover durable store");
    recovered.log.entries_from(recovered.log.start_index())
}

fn set(key: &str, value: &str) -> Vec<u8> {
    ClientRequest::internal(Command::Set {
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
    })
    .encode()
    .expect("encode")
}

/// Collects applies up to `count` non-noop entries, or panics on timeout.
async fn collect_applies(
    handle: &NodeHandle,
    mut stream: tokio::sync::broadcast::Receiver<Applied>,
    count: usize,
    within: Duration,
) -> Vec<Applied> {
    let mut collected = Vec::with_capacity(count);
    let deadline = tokio::time::Instant::now() + within;

    while collected.len() < count {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "node {} applied only {}/{count} in {within:?}",
            handle.id(),
            collected.len()
        );

        match tokio::time::timeout(remaining, stream.recv()).await {
            // Leader no-ops carry no client command and are not what a client
            // proposed; skip them without counting.
            Ok(Ok(applied)) if applied.result == CommandResult::Noop => {}
            Ok(Ok(applied)) => collected.push(applied),
            Ok(Err(error)) => panic!("apply stream broke on node {}: {error}", handle.id()),
            Err(_) => {}
        }
    }

    collected
}

/// Waits until a running node's apply stream has seen `index` (any entry type).
async fn await_applied_index(
    handle: &NodeHandle,
    mut stream: tokio::sync::broadcast::Receiver<Applied>,
    index: u64,
    within: Duration,
) {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "node {} never applied through index {index}",
            handle.id()
        );
        match tokio::time::timeout(remaining, stream.recv()).await {
            Ok(Ok(applied)) if applied.index >= index => return,
            Ok(Ok(_)) => {}
            Ok(Err(error)) => panic!("apply stream broke on node {}: {error}", handle.id()),
            Err(_) => {}
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_elect_exactly_one_leader() {
    let cluster = TestCluster::start(3, &[1, 2, 3]).await;

    let leader = cluster.await_leader(Duration::from_secs(2)).await;
    let leaders = cluster.leaders();

    assert_eq!(
        leaders,
        vec![leader],
        "expected exactly one leader, got {leaders:?}"
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hundred_commands_apply_in_the_same_order_on_every_node() {
    const COMMANDS: usize = 100;

    let cluster = TestCluster::start(3, &[1, 2, 3]).await;
    let leader_id = cluster.await_leader(Duration::from_secs(2)).await;

    // Subscribe before proposing: a subscription taken afterwards can miss the
    // applies it is waiting for.
    let mut streams = BTreeMap::new();
    for (&id, handle) in &cluster.nodes {
        streams.insert(id, handle.subscribe_apply());
    }

    let leader = &cluster.nodes[&leader_id];
    for i in 0..COMMANDS {
        leader
            .propose(set(&format!("k{i}"), &format!("v{i}")))
            .await
            .expect("leader accepts proposal");
    }

    let mut per_node = BTreeMap::new();
    for (id, stream) in streams {
        let applied = collect_applies(
            &cluster.nodes[&id],
            stream,
            COMMANDS,
            Duration::from_secs(20),
        )
        .await;
        per_node.insert(id, applied);
    }

    // Same commands, same order, same positions on all three.
    let reference = &per_node[&leader_id];
    let indices: Vec<u64> = reference.iter().map(|a| a.index).collect();
    assert!(
        indices.windows(2).all(|w| w[0] < w[1]),
        "applies were not in index order: {indices:?}"
    );

    for (id, applied) in &per_node {
        let their_indices: Vec<u64> = applied.iter().map(|a| a.index).collect();
        assert_eq!(
            their_indices, indices,
            "node {id} applied a different order"
        );
        let their_results: Vec<&CommandResult> = applied.iter().map(|a| &a.result).collect();
        let reference_results: Vec<&CommandResult> = reference.iter().map(|a| &a.result).collect();
        assert_eq!(
            their_results, reference_results,
            "node {id} applied different results"
        );
    }

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_black_holed_peer_does_not_stop_the_others() {
    // Node 3 is configured and addressed but never started: connections to it
    // are refused forever. Two of three is still a majority, so replication must
    // proceed at full speed rather than waiting on the dead peer.
    let cluster = TestCluster::start(3, &[1, 2]).await;
    let leader_id = cluster.await_leader(Duration::from_secs(3)).await;

    let mut stream = cluster.nodes[&leader_id].subscribe_apply();
    let leader = &cluster.nodes[&leader_id];

    let started = tokio::time::Instant::now();
    for i in 0..20 {
        leader
            .propose(set(&format!("k{i}"), "v"))
            .await
            .expect("leader accepts proposal");
    }
    let applied = collect_applies(
        leader,
        std::mem::replace(&mut stream, leader.subscribe_apply()),
        20,
        Duration::from_secs(10),
    )
    .await;
    let elapsed = started.elapsed();

    assert_eq!(applied.len(), 20);
    // The dead peer's rpc timeout is 100ms. If replication were serialized
    // behind it, 20 commands could not finish in anything near this budget.
    assert!(
        elapsed < Duration::from_secs(5),
        "20 commands took {elapsed:?} with one black-holed peer"
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn survivors_elect_a_new_leader_and_keep_committing() {
    let mut cluster = TestCluster::start(3, &[1, 2, 3]).await;
    let first_leader = cluster.await_leader(Duration::from_secs(2)).await;

    cluster.nodes[&first_leader]
        .propose(set("before", "1"))
        .await
        .expect("first leader accepts");

    cluster.stop(first_leader).await;

    // Two of three remain: a majority, so a new term must be won.
    let second_leader = cluster.await_leader(Duration::from_secs(5)).await;
    assert_ne!(second_leader, first_leader);

    let handle = cluster.nodes[&second_leader].clone();
    let mut stream = handle.subscribe_apply();
    handle
        .propose(set("after", "2"))
        .await
        .expect("new leader accepts");

    let applied = collect_applies(
        &handle,
        std::mem::replace(&mut stream, handle.subscribe_apply()),
        1,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(applied.len(), 1, "new leader did not commit after failover");

    cluster.shutdown().await;
}

/// Issue 04 acceptance: after the leader is stopped, survivors keep committing,
/// and the restarted node catches up to the new leader's log.
///
/// Catch-up is asserted on applied indices and on the **durable Raft log** after
/// both sides are stopped — not on an instantly warm KV store. Restart rebuilds
/// the SM empty; the leader then drives AE / `leaderCommit` until the follower
/// re-applies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopped_leader_restarts_and_catches_up_to_new_leader_log() {
    let mut cluster = TestCluster::start(3, &[1, 2, 3]).await;
    let first_leader = cluster.await_leader(Duration::from_secs(2)).await;

    let survivor = *cluster
        .nodes
        .keys()
        .find(|&&id| id != first_leader)
        .expect("survivor");
    let survivor_stream = cluster.nodes[&survivor].subscribe_apply();

    let before = cluster.nodes[&first_leader]
        .propose(set("before", "1"))
        .await
        .expect("first leader accepts");

    // propose only means durable on the leader; wait until a survivor applies.
    await_applied_index(
        &cluster.nodes[&survivor],
        survivor_stream,
        before.index,
        Duration::from_secs(10),
    )
    .await;

    cluster.stop(first_leader).await;

    let second_leader = cluster.await_leader(Duration::from_secs(5)).await;
    assert_ne!(second_leader, first_leader);

    let leader_handle = cluster.nodes[&second_leader].clone();
    let mut leader_applies = leader_handle.subscribe_apply();

    const AFTER: usize = 5;
    let mut last_after = before;
    for i in 0..AFTER {
        last_after = leader_handle
            .propose(set(&format!("after{i}"), "x"))
            .await
            .expect("new leader accepts");
    }
    let applied = collect_applies(
        &leader_handle,
        std::mem::replace(&mut leader_applies, leader_handle.subscribe_apply()),
        AFTER,
        Duration::from_secs(15),
    )
    .await;
    assert_eq!(applied.len(), AFTER);
    let target_index = last_after.index;
    assert!(
        applied.iter().any(|a| a.index == target_index),
        "leader never applied the last post-failover entry"
    );

    // Restart the old leader from the same data dir / Raft address.
    let restarted_applies = cluster.restart(first_leader).await;
    let restarted = cluster.nodes[&first_leader].clone();
    await_applied_index(
        &restarted,
        restarted_applies,
        target_index,
        Duration::from_secs(15),
    )
    .await;

    // Strong check: stop both and compare full durable entry lists (index, term,
    // command). Never open DiskStorage while the actor still owns the dir.
    cluster.stop(first_leader).await;
    cluster.stop(second_leader).await;

    let restarted_log = cluster.durable_log_entries(first_leader);
    let leader_log = cluster.durable_log_entries(second_leader);
    assert_eq!(
        restarted_log, leader_log,
        "restarted node durable log diverged from the leader that advanced while it was down"
    );
    assert!(
        restarted_log.iter().any(|e| e.index == target_index),
        "caught-up log missing post-failover index {target_index}"
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_refuses_a_proposal_and_names_the_leader() {
    let cluster = TestCluster::start(3, &[1, 2, 3]).await;
    let leader_id = cluster.await_leader(Duration::from_secs(2)).await;

    let follower_id = *cluster
        .nodes
        .keys()
        .find(|&&id| id != leader_id)
        .expect("a follower exists");

    let error = cluster.nodes[&follower_id]
        .propose(set("k", "v"))
        .await
        .expect_err("a follower must not accept a proposal");

    // The hint is what lets a client retry against the right node without the
    // user doing anything. Issue 05 turns it into a NotLeader status.
    match error {
        NodeError::NotLeader { leader_hint } => {
            assert_eq!(
                leader_hint,
                Some(leader_id),
                "follower did not name the current leader"
            );
        }
        other => panic!("expected NotLeader, got {other:?}"),
    }

    cluster.shutdown().await;
}
