//! Shared in-process cluster harness for `tests/cluster.rs` and `tests/kv.rs`.
//!
//! Each node is the same assembly `ferrite run` builds: `spawn_node` plus a Raft
//! server and a KV server, each on a listener the harness already bound.

// Each test binary compiles this module and uses a different subset of it.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use ferrite::{
    client::{ClientConfig, KvClient},
    config::{
        ClusterConfig, Config, LoggingConfig, NodeId, RaftConfig, RaftPeer, StorageBackend,
        StorageConfig,
    },
    proto::kv::kv_client::KvClient as KvStub,
    proto::kv::kv_server::KvServer,
    proto::raft::raft_service_server::RaftServiceServer,
    raft::{
        LogEntry,
        storage::{self, Storage},
    },
    server::{Applied, KvServiceImpl, NodeHandle, raft_service::RaftServiceImpl, run::spawn_node},
};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint};

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

pub struct TestCluster {
    pub nodes: BTreeMap<NodeId, NodeHandle>,
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
    pub async fn start(total: u64, run: &[NodeId]) -> Self {
        // Bind every listener first so all addresses are known before any node
        // starts; a node needs its peers' addresses at construction.
        let mut listeners = BTreeMap::new();
        let mut raft_addrs = BTreeMap::new();
        let mut kv_advertise = BTreeMap::new();
        for id in 1..=total {
            let raft = TcpListener::bind("127.0.0.1:0").await.expect("bind raft");
            // Its own ephemeral port, not raft + offset: a computed port can
            // already be taken by another test binary.
            let kv = TcpListener::bind("127.0.0.1:0").await.expect("bind kv");
            raft_addrs.insert(id, raft.local_addr().expect("addr"));
            kv_advertise.insert(id, kv.local_addr().expect("addr"));
            listeners.insert(id, (raft, kv));
        }

        let mut peers = BTreeMap::new();
        for (&id, &addr) in &raft_addrs {
            peers.insert(id, RaftPeer { addr });
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
            let (raft, kv) = listeners.remove(&id).expect("listeners");
            let _ = cluster.spawn_running(id, raft, kv).await;
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

    /// Spawns actor + Raft and KV servers for `id`, using the durable directory
    /// already registered for that id.
    ///
    /// Returns an apply subscription taken before this future yields again.
    async fn spawn_running(
        &mut self,
        id: NodeId,
        listener: TcpListener,
        kv_listener: TcpListener,
    ) -> tokio::sync::broadcast::Receiver<Applied> {
        assert_eq!(
            listener.local_addr().expect("addr"),
            self.raft_addrs[&id],
            "restart must rebind the same Raft address peers already know"
        );
        assert_eq!(
            kv_listener.local_addr().expect("addr"),
            self.kv_advertise[&id],
            "restart must rebind the same KV address clients were given"
        );

        let config = self.config_for(id);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let runtime = spawn_node(&config, &mut self.tasks, shutdown_rx.clone())
            .await
            .expect("spawn node");
        // Subscribe before any await so catch-up applies are less likely to be
        // missed between actor start and the caller's first poll.
        let apply_rx = runtime.handle.subscribe_apply();

        // Cloned: the harness keeps `runtime.handle` in `nodes`, and a handle is
        // a bundle of channel senders. The map is cloned once per node start.
        let kv_service = KvServiceImpl::new(runtime.handle.clone(), self.kv_advertise.clone());
        let mut kv_shutdown = shutdown_rx.clone();
        self.tasks.spawn(async move {
            let served = tonic::transport::Server::builder()
                .add_service(KvServer::new(kv_service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(kv_listener), async move {
                    let _ = kv_shutdown.changed().await;
                })
                .await;
            if let Err(error) = served {
                eprintln!("node {id} kv server stopped: {error}");
            }
        });

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
    pub async fn restart(&mut self, id: NodeId) -> tokio::sync::broadcast::Receiver<Applied> {
        assert!(
            !self.nodes.contains_key(&id),
            "node {id} is still running; stop it first"
        );
        assert!(
            self.data_dirs.contains_key(&id),
            "no data dir registered for node {id}"
        );

        // Raft address first, then KV. Port release after cooperative stop can
        // lag slightly on loopback.
        let mut rebound = Vec::with_capacity(2);
        for addr in [self.raft_addrs[&id], self.kv_advertise[&id]] {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            let listener = loop {
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
            };
            rebound.push(listener);
        }
        let kv_listener = rebound.pop().expect("kv listener");
        let listener = rebound.pop().expect("raft listener");

        self.spawn_running(id, listener, kv_listener).await
    }

    /// Waits until some running node reports itself leader.
    pub async fn await_leader(&self, within: Duration) -> NodeId {
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
    pub fn leaders(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|(_, handle)| handle.role().borrow().is_leader)
            .map(|(&id, _)| id)
            .collect()
    }

    /// Cooperative stop: actor and gRPC server exit; durable files stay.
    ///
    /// Not a process kill — see the module docs.
    pub async fn stop(&mut self, id: NodeId) {
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
    pub fn durable_log_entries(&self, id: NodeId) -> Vec<LogEntry> {
        assert!(
            !self.nodes.contains_key(&id),
            "durable_log_entries requires node {id} stopped"
        );
        let path = self.data_dirs[&id].path().to_path_buf();
        recover_log_entries(path)
    }

    pub async fn shutdown(mut self) {
        for (_, shutdown) in std::mem::take(&mut self.shutdowns) {
            let _ = shutdown.send(true);
        }
        self.tasks.shutdown().await;
    }

    /// A client whose endpoint list starts at `first`, so a test controls which
    /// node it tries before any redirect.
    pub fn client(&self, first: NodeId, request_timeout: Duration) -> KvClient {
        let first_addr = self.kv_advertise[&first];
        let mut endpoints: Vec<SocketAddr> = self.kv_advertise.values().copied().collect();
        // Stable sort on `false < true`: `first` moves to the front, the rest keep order.
        endpoints.sort_by_key(|&addr| addr != first_addr);
        KvClient::new(ClientConfig {
            endpoints,
            request_timeout,
            attempt_timeout: Duration::from_millis(500),
        })
        .expect("client")
    }

    /// A raw stub to one node's KV port, for tests that must control `seq_num`.
    pub fn kv_stub(&self, id: NodeId) -> KvStub<Channel> {
        let channel = Endpoint::from_shared(format!("http://{}", self.kv_advertise[&id]))
            .expect("kv uri")
            .connect_lazy();
        KvStub::new(channel)
    }
}

fn recover_log_entries(data_dir: PathBuf) -> Vec<LogEntry> {
    let backend = StorageBackend::Disk { data_dir };
    let store = storage::open(&backend).expect("open durable store");
    let recovered = store.recover().expect("recover durable store");
    recovered.log.entries_from(recovered.log.start_index())
}
