//! Node and cluster configuration.
//!
//! # Call sequence
//!
//! ```text
//! TOML file  ──┐
//! CLI flags  ──┤──▶  Config::load  ──▶  validate  ──▶  Config
//! defaults   ──┘
//! ```
//!
//! Priority: CLI flag > file value > compiled default.
//! Entry point: [`Config::load`]. Never construct [`Config`] directly.

use std::{collections::BTreeMap, net::SocketAddr, path::Path, time::Duration};

// ── Type alias ────────────────────────────────────────────────────────────────

/// Stable integer identity for a Raft voter. `0` is reserved / invalid.
pub type NodeId = u64;

// ── Peer (Raft membership) ────────────────────────────────────────────────────

/// One Raft voter: identity + consensus endpoint.
///
/// Contains only what the Raft protocol needs. Client routing lives in
/// [`ClusterConfig::kv_advertise`], not here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftPeer {
    /// Address this peer binds for Raft RPCs (`RequestVote`, `AppendEntries`,
    /// `InstallSnapshot`). Must equal [`ClusterConfig::raft_bind`] for self.
    pub addr: SocketAddr,
}

// ── Cluster config ────────────────────────────────────────────────────────────

/// Cluster identity and addressing for one node.
///
/// # Voter set
/// [`peers`](ClusterConfig::peers) is the **bootstrap voter set**, including this node.
/// Every quorum decision uses `peers.len() / 2 + 1`. Never exclude self.
///
/// # Client advertise
/// [`kv_advertise`](ClusterConfig::kv_advertise) is **not** part of Raft membership.
/// Followers use it to populate `leader_hint` so clients know which KV address to dial.
/// Epic 6 (dynamic membership) will move this into the replicated log; until then every
/// voter must have an entry here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterConfig {
    /// This node's stable identity within the cluster.
    pub node_id: NodeId,

    /// Local socket this node binds for **Raft RPC** traffic.
    ///
    /// Must match `peers[node_id].addr` exactly; validation rejects any mismatch.
    /// TOML key: `cluster.listen_addr`.
    pub raft_bind: SocketAddr,

    /// Local socket this node binds for **KV API** (client-facing) traffic.
    ///
    /// Must match `kv_advertise[node_id]` exactly. Must not overlap any Raft addr.
    /// TOML key: `cluster.client_addr`.
    pub kv_bind: SocketAddr,

    /// Bootstrap Raft voter set. Keyed by [`NodeId`]; values hold the consensus endpoint.
    ///
    /// - Includes self (`node_id` must be present).
    /// - Quorum = `peers.len() / 2 + 1`.
    /// - `BTreeMap` keeps iteration order deterministic (never `HashMap`).
    pub peers: BTreeMap<NodeId, RaftPeer>,

    /// Per-node KV addresses advertised to clients for `leader_hint` redirection.
    ///
    /// Keys must exactly match `peers` keys (bijection enforced at load time).
    /// Not used for quorum — Raft only reads `peers`.
    /// TOML key: `cluster.client_endpoints`.
    pub kv_advertise: BTreeMap<NodeId, SocketAddr>,
}

impl ClusterConfig {
    /// Quorum size for this voter set: `peers.len() / 2 + 1`.
    pub fn quorum(&self) -> usize {
        self.peers.len() / 2 + 1
    }

    /// KV address to advertise for the given node id (used in `leader_hint`).
    pub fn kv_addr_for(&self, id: NodeId) -> Option<SocketAddr> {
        self.kv_advertise.get(&id).copied()
    }
}

// ── Raft timing config ────────────────────────────────────────────────────────

/// Timing parameters for the Raft protocol.
///
/// # Invariant (enforced at load)
/// `heartbeat_interval < rpc_timeout < election_timeout_min < election_timeout_max`
///
/// Defaults satisfy `broadcastTime ≪ electionTimeout` per the Raft paper.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftConfig {
    /// How often the leader sends heartbeats. Default: 50 ms.
    pub heartbeat_interval: Duration,

    /// Per-RPC deadline. Default: 100 ms.
    pub rpc_timeout: Duration,

    /// Lower bound of the random election timeout window. Default: 300 ms.
    pub election_timeout_min: Duration,

    /// Upper bound of the random election timeout window. Default: 600 ms.
    pub election_timeout_max: Duration,

    /// Log entries before a snapshot compaction is triggered. Default: 1000.
    pub compaction_threshold: u64,
}

/// Default heartbeat interval in milliseconds.
pub const DEFAULT_HEARTBEAT_MS: u64 = 50;
/// Default per-RPC deadline in milliseconds.
pub const DEFAULT_RPC_TIMEOUT_MS: u64 = 100;
/// Default election timeout lower bound in milliseconds.
pub const DEFAULT_ELECTION_TIMEOUT_MIN_MS: u64 = 300;
/// Default election timeout upper bound in milliseconds.
pub const DEFAULT_ELECTION_TIMEOUT_MAX_MS: u64 = 600;
/// Default log compaction threshold (entry count).
pub const DEFAULT_COMPACTION_THRESHOLD: u64 = 1000;
/// Reject any single timing field above this (10 minutes). Guards absurd TOML values.
pub const MAX_TIMING_MS: u64 = 600_000;

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval:   Duration::from_millis(DEFAULT_HEARTBEAT_MS),
            rpc_timeout:          Duration::from_millis(DEFAULT_RPC_TIMEOUT_MS),
            election_timeout_min: Duration::from_millis(DEFAULT_ELECTION_TIMEOUT_MIN_MS),
            election_timeout_max: Duration::from_millis(DEFAULT_ELECTION_TIMEOUT_MAX_MS),
            compaction_threshold: DEFAULT_COMPACTION_THRESHOLD,
        }
    }
}

// ── Storage config ────────────────────────────────────────────────────────────

/// Persistence backend selection.
///
/// `Disk` is the durable path (sled or a segment file behind `Storage` — **not** RocksDB).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum StorageBackend {
    #[default]
    Memory,
    /// On-disk store rooted at `data_dir` (implementation lands with durable storage).
    Disk { data_dir: std::path::PathBuf },
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StorageConfig {
    pub backend: StorageBackend,
}

// ── Logging config ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum LogFormat {
    #[default]
    Pretty,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LoggingConfig {
    pub format: LogFormat,
}

// ── Root config ───────────────────────────────────────────────────────────────

/// Complete node configuration. Built by [`Config::load`]; never constructed directly.
#[derive(Debug, Clone)]
pub struct Config {
    pub cluster: ClusterConfig,
    pub raft:    RaftConfig,
    pub storage: StorageConfig,
    pub logging: LoggingConfig,
}

// ── CLI overrides ─────────────────────────────────────────────────────────────

/// Values supplied via CLI flags that override file or defaults.
///
/// A non-empty `peers` or `kv_advertise` **replaces** the file's list entirely — no union.
/// Partial merge of two cluster definitions is an error, not a feature.
#[derive(Debug, Default)]
pub struct Overrides {
    /// `--peer 1@127.0.0.1:7001`. Replaces file `cluster.peers` entirely if non-empty.
    pub peers: Vec<(NodeId, SocketAddr)>,

    /// `--client-endpoint 1@127.0.0.1:8001`. Replaces file `cluster.client_endpoints` if non-empty.
    pub kv_advertise: Vec<(NodeId, SocketAddr)>,

    /// `--heartbeat-interval-ms <n>`.
    pub heartbeat_interval_ms: Option<u64>,

    /// `--rpc-timeout-ms <n>`.
    pub rpc_timeout_ms: Option<u64>,

    /// `--election-timeout-min-ms <n>`.
    pub election_timeout_min_ms: Option<u64>,

    /// `--election-timeout-max-ms <n>`.
    pub election_timeout_max_ms: Option<u64>,
}

// ── Error ─────────────────────────────────────────────────────────────────────

/// Errors produced during config load or validation.
///
/// CLI-specific parse errors (malformed `--peer` / `--client-endpoint` flags) live in
/// `cli::CliError`, not here.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("could not read config file: {0}")]
    Io(#[from] std::io::Error),

    #[error("could not parse TOML: {0}")]
    Toml(String),

    /// One or more named fields failed validation. All errors are collected before returning.
    #[error("config validation failed:\n{}", errors.join("\n"))]
    Validation { errors: Vec<String> },
}

// ── Load entrypoint ───────────────────────────────────────────────────────────

impl Config {
    /// Load, merge overrides, and validate configuration.
    ///
    /// `path = None` is reserved for a future full-flag cluster identity. Today
    /// [`Overrides`] cannot supply `node_id` / bind addresses, so a missing path
    /// returns a single clear validation error instead of a pile of "required" fields.
    pub fn load(path: Option<&Path>, overrides: Overrides) -> Result<Config, ConfigError> {
        let mut raw = match path {
            Some(p) => {
                let text = std::fs::read_to_string(p)?;
                toml::from_str::<RawConfig>(&text)
                    .map_err(|e| ConfigError::Toml(e.to_string()))?
            }
            None => {
                // Overrides currently cover peers/endpoints/timings only — not node_id or binds.
                if overrides.peers.is_empty()
                    && overrides.kv_advertise.is_empty()
                    && overrides.heartbeat_interval_ms.is_none()
                    && overrides.rpc_timeout_ms.is_none()
                    && overrides.election_timeout_min_ms.is_none()
                    && overrides.election_timeout_max_ms.is_none()
                {
                    return Err(ConfigError::Validation {
                        errors: vec![
                            "no configuration source: pass --config <path> (cluster identity \
                             cannot be built from flags alone yet)"
                                .into(),
                        ],
                    });
                }
                // Flags present but still incomplete without a file: fall through to validate
                // after merge so the user sees every missing field, led by a source hint.
                RawConfig::default()
            }
        };

        let missing_file = path.is_none();
        apply_overrides(&mut raw, overrides);
        let mut result = validate(raw);
        if missing_file
            && let Err(ConfigError::Validation { ref mut errors }) = result
        {
            errors.insert(
                0,
                "no --config path: Overrides cannot supply node_id/listen_addr/client_addr yet; \
                 provide a TOML file (flag-only cluster load is not supported)"
                    .into(),
            );
        }
        result
    }

    /// Parse and validate a TOML string directly. Used by `ferrite init` to validate
    /// generated node files before writing them to disk.
    pub fn from_toml_str(toml: &str, overrides: Overrides) -> Result<Config, ConfigError> {
        let mut raw = toml::from_str::<RawConfig>(toml)
            .map_err(|e| ConfigError::Toml(e.to_string()))?;
        apply_overrides(&mut raw, overrides);
        validate(raw)
    }
}

// ── Raw TOML types (serde layer, private) ─────────────────────────────────────
//
// These mirror the TOML structure exactly. All cluster fields are `Option` so
// defaults and overrides can fill in what the file omits. Converted to domain
// types in `validate()`.

#[derive(Debug, serde::Deserialize)]
struct RawPeer {
    id: NodeId,
    addr: String,
}

#[derive(Debug, serde::Deserialize)]
struct RawEndpoint {
    id: NodeId,
    addr: String,
}

#[derive(Debug, Default, serde::Deserialize)]
struct RawClusterConfig {
    node_id: Option<NodeId>,
    /// Maps to [`ClusterConfig::raft_bind`].
    listen_addr: Option<String>,
    /// Maps to [`ClusterConfig::kv_bind`].
    client_addr: Option<String>,
    peers: Option<Vec<RawPeer>>,
    /// Maps to [`ClusterConfig::kv_advertise`].
    client_endpoints: Option<Vec<RawEndpoint>>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct RawRaftConfig {
    heartbeat_interval_ms:   Option<u64>,
    rpc_timeout_ms:          Option<u64>,
    election_timeout_min_ms: Option<u64>,
    election_timeout_max_ms: Option<u64>,
    compaction_threshold:    Option<u64>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct RawStorageConfig {
    backend:  Option<String>, // "memory" | "disk"
    data_dir: Option<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct RawLoggingConfig {
    format: Option<String>, // "pretty" | "json"
}

#[derive(Debug, Default, serde::Deserialize)]
struct RawConfig {
    #[serde(default)]
    cluster: RawClusterConfig,
    #[serde(default)]
    raft: RawRaftConfig,
    #[serde(default)]
    storage: RawStorageConfig,
    #[serde(default)]
    logging: RawLoggingConfig,
}

// ── Override merge ────────────────────────────────────────────────────────────

fn apply_overrides(raw: &mut RawConfig, overrides: Overrides) {
    if !overrides.peers.is_empty() {
        raw.cluster.peers = Some(
            overrides.peers.into_iter()
                .map(|(id, addr)| RawPeer { id, addr: addr.to_string() })
                .collect(),
        );
    }
    if !overrides.kv_advertise.is_empty() {
        raw.cluster.client_endpoints = Some(
            overrides.kv_advertise.into_iter()
                .map(|(id, addr)| RawEndpoint { id, addr: addr.to_string() })
                .collect(),
        );
    }
    if let Some(v) = overrides.heartbeat_interval_ms   { raw.raft.heartbeat_interval_ms   = Some(v); }
    if let Some(v) = overrides.rpc_timeout_ms           { raw.raft.rpc_timeout_ms           = Some(v); }
    if let Some(v) = overrides.election_timeout_min_ms  { raw.raft.election_timeout_min_ms  = Some(v); }
    if let Some(v) = overrides.election_timeout_max_ms  { raw.raft.election_timeout_max_ms  = Some(v); }
}

// ── Validation: Raw → Config ──────────────────────────────────────────────────
//
// Collects ALL errors before returning so the user sees the full picture at once.

fn validate(raw: RawConfig) -> Result<Config, ConfigError> {
    let mut errors: Vec<String> = Vec::new();

    // ── Timing ────────────────────────────────────────────────────────────
    let hb_ms   = raw.raft.heartbeat_interval_ms  .unwrap_or(DEFAULT_HEARTBEAT_MS);
    let rpc_ms  = raw.raft.rpc_timeout_ms          .unwrap_or(DEFAULT_RPC_TIMEOUT_MS);
    let emin    = raw.raft.election_timeout_min_ms .unwrap_or(DEFAULT_ELECTION_TIMEOUT_MIN_MS);
    let emax    = raw.raft.election_timeout_max_ms .unwrap_or(DEFAULT_ELECTION_TIMEOUT_MAX_MS);
    let cthresh = raw.raft.compaction_threshold    .unwrap_or(DEFAULT_COMPACTION_THRESHOLD);

    if hb_ms == 0 {
        errors.push("raft.heartbeat_interval_ms: must not be 0".into());
    }
    if rpc_ms == 0 {
        errors.push("raft.rpc_timeout_ms: must not be 0".into());
    }
    if emin == 0 {
        errors.push("raft.election_timeout_min_ms: must not be 0".into());
    }
    if emax == 0 {
        errors.push("raft.election_timeout_max_ms: must not be 0".into());
    }
    if cthresh == 0 {
        errors.push("raft.compaction_threshold: must not be 0".into());
    }

    for (name, ms) in [
        ("raft.heartbeat_interval_ms", hb_ms),
        ("raft.rpc_timeout_ms", rpc_ms),
        ("raft.election_timeout_min_ms", emin),
        ("raft.election_timeout_max_ms", emax),
    ] {
        if ms > MAX_TIMING_MS {
            errors.push(format!(
                "{name} ({ms}) exceeds maximum allowed {MAX_TIMING_MS} ms (10 minutes)",
            ));
        }
    }

    if hb_ms > 0 && rpc_ms > 0 && hb_ms >= rpc_ms {
        errors.push(format!(
            "raft.heartbeat_interval_ms ({hb_ms}) must be < rpc_timeout_ms ({rpc_ms})"
        ));
    }
    if rpc_ms > 0 && emin > 0 && rpc_ms >= emin {
        errors.push(format!(
            "raft.rpc_timeout_ms ({rpc_ms}) must be < election_timeout_min_ms ({emin})"
        ));
    }
    if emin > 0 && emax > 0 && emin >= emax {
        errors.push(format!(
            "raft.election_timeout_min_ms ({emin}) must be < election_timeout_max_ms ({emax})"
        ));
    }
    // Soft warning only — does not fail validation. Library stays free of eprintln!.
    if hb_ms > 0 && emin > 0 && emin < 5 * hb_ms {
        tracing::warn!(
            election_timeout_min_ms = emin,
            heartbeat_interval_ms = hb_ms,
            "raft.election_timeout_min_ms < 5 × heartbeat_interval_ms; \
             spurious elections likely under scheduler jitter"
        );
    }

    // ── node_id ───────────────────────────────────────────────────────────
    let node_id = match raw.cluster.node_id {
        None    => { errors.push("cluster.node_id: required".into()); 0 }
        Some(0) => { errors.push("cluster.node_id: must not be 0".into()); 0 }
        Some(id) => id,
    };

    // ── raft_bind ─────────────────────────────────────────────────────────
    let raft_bind = parse_addr_field(
        raw.cluster.listen_addr.as_deref(),
        "cluster.listen_addr",
        &mut errors,
    );

    // ── kv_bind ───────────────────────────────────────────────────────────
    let kv_bind = parse_addr_field(
        raw.cluster.client_addr.as_deref(),
        "cluster.client_addr",
        &mut errors,
    );

    // ── peers ─────────────────────────────────────────────────────────────
    let raw_peers = raw.cluster.peers.unwrap_or_default();
    let raw_peers_len = raw_peers.len();
    if raw_peers.is_empty() {
        errors.push("cluster.peers: must not be empty".into());
    }

    let mut peers: BTreeMap<NodeId, RaftPeer> = BTreeMap::new();
    let mut seen_peer_addrs: BTreeMap<SocketAddr, NodeId> = BTreeMap::new();

    for rp in raw_peers {
        if rp.id == 0 {
            errors.push("cluster.peers: id 0 is reserved".into());
            continue;
        }
        if peers.contains_key(&rp.id) {
            errors.push(format!("cluster.peers: duplicate peer id {}", rp.id));
            continue;
        }
        let addr = match rp.addr.parse::<SocketAddr>() {
            Ok(a) => a,
            Err(e) => {
                errors.push(format!(
                    "cluster.peers[{}].addr: invalid address {:?}: {e}", rp.id, rp.addr
                ));
                continue;
            }
        };
        if let Some(existing) = seen_peer_addrs.get(&addr) {
            errors.push(format!(
                "cluster.peers: duplicate addr {addr} (ids {existing} and {})", rp.id
            ));
            continue;
        }
        seen_peer_addrs.insert(addr, rp.id);
        peers.insert(rp.id, RaftPeer { addr });
    }

    // Rows present but every entry rejected (bad addrs / reserved ids) — avoid a silent empty set.
    if peers.is_empty() && raw_peers_len > 0 {
        errors.push(
            "cluster.peers: no valid peer entries after parsing (check id/addr errors above)"
                .into(),
        );
    }

    // node_id must appear in peers
    if node_id != 0 && !peers.is_empty() && !peers.contains_key(&node_id) {
        errors.push(format!("cluster.node_id ({node_id}) is absent from cluster.peers"));
    }

    // raft_bind must match peers[node_id].addr
    if let (Some(bind), Some(self_peer)) = (raft_bind, peers.get(&node_id))
        && bind != self_peer.addr
    {
        errors.push(format!(
            "cluster.listen_addr ({bind}) does not match peers[{node_id}].addr ({})",
            self_peer.addr
        ));
    }

    // ── kv_advertise (client_endpoints) ───────────────────────────────────
    let raw_endpoints = raw.cluster.client_endpoints.unwrap_or_default();
    let mut kv_advertise: BTreeMap<NodeId, SocketAddr> = BTreeMap::new();
    let mut seen_ep_addrs: BTreeMap<SocketAddr, NodeId> = BTreeMap::new();

    for re in raw_endpoints {
        if re.id == 0 {
            errors.push("cluster.client_endpoints: id 0 is reserved".into());
            continue;
        }
        if kv_advertise.contains_key(&re.id) {
            errors.push(format!("cluster.client_endpoints: duplicate id {}", re.id));
            continue;
        }
        let addr = match re.addr.parse::<SocketAddr>() {
            Ok(a) => a,
            Err(e) => {
                errors.push(format!(
                    "cluster.client_endpoints[{}].addr: invalid address {:?}: {e}", re.id, re.addr
                ));
                continue;
            }
        };
        if let Some(existing) = seen_ep_addrs.get(&addr) {
            errors.push(format!(
                "cluster.client_endpoints: duplicate addr {addr} (ids {existing} and {})", re.id
            ));
            continue;
        }
        seen_ep_addrs.insert(addr, re.id);
        kv_advertise.insert(re.id, addr);
    }

    // peers and kv_advertise must be a bijection (same key sets)
    for id in peers.keys() {
        if !kv_advertise.contains_key(id) {
            errors.push(format!(
                "cluster.client_endpoints: missing entry for peer id {id} \
                 (if peers were replaced via --peer, also pass matching --client-endpoint flags)"
            ));
        }
    }
    for id in kv_advertise.keys() {
        if !peers.contains_key(id) {
            errors.push(format!(
                "cluster.client_endpoints: id {id} has no matching peer \
                 (if endpoints were replaced via --client-endpoint, also pass matching --peer flags)"
            ));
        }
    }

    // kv_bind must match kv_advertise[node_id]
    if let (Some(bind), Some(&advertised)) = (kv_bind, kv_advertise.get(&node_id))
        && bind != advertised
    {
        errors.push(format!(
            "cluster.client_addr ({bind}) does not match client_endpoints[{node_id}].addr ({advertised})"
        ));
    }

    // No Raft addr may equal any KV addr
    for (pid, peer) in &peers {
        if kv_advertise.values().any(|&kv| kv == peer.addr) {
            errors.push(format!(
                "cluster.peers[{pid}].addr ({}) collides with a client_endpoints addr; \
                 Raft and KV must not share a port",
                peer.addr
            ));
        }
    }

    // ── Storage ───────────────────────────────────────────────────────────
    // "disk" = durable backend (sled / segment file). "rocksdb" is rejected by name so
    // the deprecated MVP label cannot re-enter configs.
    let storage = match raw.storage.backend.as_deref() {
        None | Some("memory") => StorageConfig {
            backend: StorageBackend::Memory,
        },
        Some("disk") => match raw.storage.data_dir {
            Some(dir) => StorageConfig {
                backend: StorageBackend::Disk {
                    data_dir: dir.into(),
                },
            },
            None => {
                errors.push(
                    "storage.data_dir: required when storage.backend = \"disk\"".into(),
                );
                StorageConfig::default()
            }
        },
        Some("rocksdb") => {
            errors.push(
                "storage.backend: \"rocksdb\" is not supported; use \"disk\" (sled/segment) or \"memory\""
                    .into(),
            );
            StorageConfig::default()
        }
        Some(other) => {
            errors.push(format!(
                "storage.backend: unknown value {other:?}; expected \"memory\" or \"disk\""
            ));
            StorageConfig::default()
        }
    };

    // ── Logging ───────────────────────────────────────────────────────────
    let logging = match raw.logging.format.as_deref() {
        None | Some("pretty") => LoggingConfig { format: LogFormat::Pretty },
        Some("json")          => LoggingConfig { format: LogFormat::Json },
        Some(other) => {
            errors.push(format!(
                "logging.format: unknown value {other:?}; expected \"pretty\" or \"json\""
            ));
            LoggingConfig::default()
        }
    };

    // ── Return ────────────────────────────────────────────────────────────
    if !errors.is_empty() {
        return Err(ConfigError::Validation { errors });
    }

    // Both are Some: a None addr always pushes to `errors`, and we just checked
    // errors is empty. Use let-else to satisfy the borrow checker without unwrap().
    let (Some(raft_bind), Some(kv_bind)) = (raft_bind, kv_bind) else {
        return Err(ConfigError::Validation {
            errors: vec![
                "internal: required address field was None with no error recorded (bug)".into(),
            ],
        });
    };

    Ok(Config {
        cluster: ClusterConfig {
            node_id,
            raft_bind,
            kv_bind,
            peers,
            kv_advertise,
        },
        raft: RaftConfig {
            heartbeat_interval:   Duration::from_millis(hb_ms),
            rpc_timeout:          Duration::from_millis(rpc_ms),
            election_timeout_min: Duration::from_millis(emin),
            election_timeout_max: Duration::from_millis(emax),
            compaction_threshold: cthresh,
        },
        storage,
        logging,
    })
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn parse_addr_field(
    value: Option<&str>,
    field: &str,
    errors: &mut Vec<String>,
) -> Option<SocketAddr> {
    match value {
        None | Some("") => {
            errors.push(format!("{field}: required"));
            None
        }
        Some(s) => match s.parse::<SocketAddr>() {
            Ok(addr) => Some(addr),
            Err(e) => {
                errors.push(format!("{field}: invalid socket address {s:?}: {e}"));
                None
            }
        },
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn three_node_raw() -> RawConfig {
        RawConfig {
            cluster: RawClusterConfig {
                node_id: Some(1),
                listen_addr: Some("127.0.0.1:7001".into()),
                client_addr: Some("127.0.0.1:8001".into()),
                peers: Some(vec![
                    RawPeer { id: 1, addr: "127.0.0.1:7001".into() },
                    RawPeer { id: 2, addr: "127.0.0.1:7002".into() },
                    RawPeer { id: 3, addr: "127.0.0.1:7003".into() },
                ]),
                client_endpoints: Some(vec![
                    RawEndpoint { id: 1, addr: "127.0.0.1:8001".into() },
                    RawEndpoint { id: 2, addr: "127.0.0.1:8002".into() },
                    RawEndpoint { id: 3, addr: "127.0.0.1:8003".into() },
                ]),
            },
            raft:    RawRaftConfig::default(),
            storage: RawStorageConfig::default(),
            logging: RawLoggingConfig::default(),
        }
    }

    #[test]
    fn valid_three_node_config() {
        let cfg = validate(three_node_raw()).expect("should be valid");
        assert_eq!(cfg.cluster.node_id, 1);
        assert_eq!(cfg.cluster.quorum(), 2);
        assert_eq!(cfg.cluster.peers.len(), 3);
        assert_eq!(cfg.cluster.kv_advertise.len(), 3);
    }

    #[test]
    fn default_timing_applied() {
        let cfg = validate(three_node_raw()).unwrap();
        assert_eq!(cfg.raft, RaftConfig::default());
    }

    #[test]
    fn btreemap_peers_keys_sorted() {
        let mut raw = three_node_raw();
        raw.cluster.peers = Some(vec![
            RawPeer { id: 3, addr: "127.0.0.1:7003".into() },
            RawPeer { id: 1, addr: "127.0.0.1:7001".into() },
            RawPeer { id: 2, addr: "127.0.0.1:7002".into() },
        ]);
        let cfg = validate(raw).unwrap();
        let keys: Vec<NodeId> = cfg.cluster.peers.keys().copied().collect();
        assert_eq!(keys, vec![1, 2, 3]);
    }

    #[test]
    fn btreemap_kv_advertise_keys_sorted() {
        let mut raw = three_node_raw();
        raw.cluster.client_endpoints = Some(vec![
            RawEndpoint { id: 3, addr: "127.0.0.1:8003".into() },
            RawEndpoint { id: 1, addr: "127.0.0.1:8001".into() },
            RawEndpoint { id: 2, addr: "127.0.0.1:8002".into() },
        ]);
        let cfg = validate(raw).unwrap();
        let keys: Vec<NodeId> = cfg.cluster.kv_advertise.keys().copied().collect();
        assert_eq!(keys, vec![1, 2, 3]);
    }

    #[test]
    fn timing_inversion_rpc_ge_election_min() {
        let mut raw = three_node_raw();
        raw.raft.rpc_timeout_ms = Some(400);
        raw.raft.election_timeout_min_ms = Some(300);
        let err = validate(raw).unwrap_err().to_string();
        assert!(err.contains("rpc_timeout_ms"), "error should name rpc_timeout_ms: {err}");
        assert!(err.contains("election_timeout_min_ms"), "error should name election_timeout_min_ms: {err}");
    }

    #[test]
    fn listen_addr_mismatch_rejected() {
        let mut raw = three_node_raw();
        raw.cluster.listen_addr = Some("127.0.0.1:9999".into());
        let err = validate(raw).unwrap_err().to_string();
        assert!(err.contains("listen_addr"), "{err}");
    }

    #[test]
    fn node_id_absent_from_peers_rejected() {
        let mut raw = three_node_raw();
        raw.cluster.node_id = Some(99);
        // Also fix listen_addr to avoid a secondary error
        raw.cluster.listen_addr = Some("127.0.0.1:7099".into());
        let err = validate(raw).unwrap_err().to_string();
        assert!(err.contains("absent from cluster.peers"), "{err}");
    }

    #[test]
    fn raft_and_kv_addr_collision_rejected() {
        let mut raw = three_node_raw();
        raw.cluster.client_endpoints = Some(vec![
            RawEndpoint { id: 1, addr: "127.0.0.1:8001".into() },
            RawEndpoint { id: 2, addr: "127.0.0.1:7002".into() }, // ← same as Raft peer 2
            RawEndpoint { id: 3, addr: "127.0.0.1:8003".into() },
        ]);
        let err = validate(raw).unwrap_err().to_string();
        assert!(err.contains("collides"), "{err}");
    }

    #[test]
    fn missing_client_endpoint_for_peer_rejected() {
        let mut raw = three_node_raw();
        raw.cluster.client_endpoints = Some(vec![
            RawEndpoint { id: 1, addr: "127.0.0.1:8001".into() },
            RawEndpoint { id: 2, addr: "127.0.0.1:8002".into() },
            // id 3 missing
        ]);
        let err = validate(raw).unwrap_err().to_string();
        assert!(err.contains("missing entry for peer id 3"), "{err}");
    }

    #[test]
    fn override_peers_replaces_file_entirely() {
        let mut raw = three_node_raw();
        let overrides = Overrides {
            peers: vec![(1, "127.0.0.1:7001".parse().unwrap())],
            kv_advertise: vec![(1, "127.0.0.1:8001".parse().unwrap())],
            ..Default::default()
        };
        apply_overrides(&mut raw, overrides);
        assert_eq!(raw.cluster.peers.as_ref().unwrap().len(), 1, "override should replace, not union");
    }

    #[test]
    fn override_kv_advertise_replaces_file_entirely() {
        let mut raw = three_node_raw(); // has 3 client_endpoints
        let overrides = Overrides {
            kv_advertise: vec![(1, "127.0.0.1:8001".parse().unwrap())],
            ..Default::default()
        };
        apply_overrides(&mut raw, overrides);
        assert_eq!(
            raw.cluster.client_endpoints.as_ref().unwrap().len(),
            1,
            "--client-endpoint override should replace file client_endpoints, not union"
        );
    }

    #[test]
    fn toml_round_trip() {
        let toml_str = r#"
[cluster]
node_id     = 1
listen_addr = "127.0.0.1:7001"
client_addr = "127.0.0.1:8001"

[[cluster.peers]]
id   = 1
addr = "127.0.0.1:7001"

[[cluster.peers]]
id   = 2
addr = "127.0.0.1:7002"

[[cluster.peers]]
id   = 3
addr = "127.0.0.1:7003"

[[cluster.client_endpoints]]
id   = 1
addr = "127.0.0.1:8001"

[[cluster.client_endpoints]]
id   = 2
addr = "127.0.0.1:8002"

[[cluster.client_endpoints]]
id   = 3
addr = "127.0.0.1:8003"
"#;
        let raw: RawConfig = toml::from_str(toml_str).expect("TOML parse failed");
        let cfg = validate(raw).expect("validation failed");
        assert_eq!(cfg.cluster.node_id, 1);
        assert_eq!(cfg.cluster.peers.len(), 3);
    }

    // ── Gap coverage: timing zeros & bounds ───────────────────────────────

    #[test]
    fn election_timeout_min_zero_rejected() {
        let mut raw = three_node_raw();
        raw.raft.election_timeout_min_ms = Some(0);
        let err = validate(raw).unwrap_err().to_string();
        assert!(err.contains("election_timeout_min_ms"), "{err}");
        assert!(err.contains("must not be 0"), "{err}");
    }

    #[test]
    fn election_timeout_max_zero_rejected() {
        let mut raw = three_node_raw();
        raw.raft.election_timeout_max_ms = Some(0);
        let err = validate(raw).unwrap_err().to_string();
        assert!(err.contains("election_timeout_max_ms"), "{err}");
        assert!(err.contains("must not be 0"), "{err}");
    }

    #[test]
    fn heartbeat_zero_rejected() {
        let mut raw = three_node_raw();
        raw.raft.heartbeat_interval_ms = Some(0);
        let err = validate(raw).unwrap_err().to_string();
        assert!(err.contains("heartbeat_interval_ms"), "{err}");
        assert!(err.contains("must not be 0"), "{err}");
    }

    #[test]
    fn rpc_timeout_zero_rejected() {
        let mut raw = three_node_raw();
        raw.raft.rpc_timeout_ms = Some(0);
        let err = validate(raw).unwrap_err().to_string();
        assert!(err.contains("rpc_timeout_ms"), "{err}");
        assert!(err.contains("must not be 0"), "{err}");
    }

    #[test]
    fn compaction_threshold_zero_rejected() {
        let mut raw = three_node_raw();
        raw.raft.compaction_threshold = Some(0);
        let err = validate(raw).unwrap_err().to_string();
        assert!(err.contains("compaction_threshold"), "{err}");
    }

    #[test]
    fn heartbeat_ge_rpc_rejected() {
        let mut raw = three_node_raw();
        raw.raft.heartbeat_interval_ms = Some(100);
        raw.raft.rpc_timeout_ms = Some(100);
        let err = validate(raw).unwrap_err().to_string();
        assert!(err.contains("heartbeat_interval_ms"), "{err}");
        assert!(err.contains("rpc_timeout_ms"), "{err}");
    }

    #[test]
    fn election_min_ge_max_rejected() {
        let mut raw = three_node_raw();
        raw.raft.election_timeout_min_ms = Some(600);
        raw.raft.election_timeout_max_ms = Some(600);
        let err = validate(raw).unwrap_err().to_string();
        assert!(err.contains("election_timeout_min_ms"), "{err}");
        assert!(err.contains("election_timeout_max_ms"), "{err}");
    }

    #[test]
    fn timing_above_max_rejected() {
        let mut raw = three_node_raw();
        raw.raft.election_timeout_max_ms = Some(MAX_TIMING_MS + 1);
        // Keep chain order so only the upper-bound check fires for emax.
        raw.raft.election_timeout_min_ms = Some(300);
        let err = validate(raw).unwrap_err().to_string();
        assert!(err.contains("exceeds maximum"), "{err}");
        assert!(err.contains("election_timeout_max_ms"), "{err}");
    }

    #[test]
    fn soft_election_ratio_does_not_fail_validation() {
        // emin < 5 * hb but still satisfies hb < rpc < emin < emax
        let mut raw = three_node_raw();
        raw.raft.heartbeat_interval_ms = Some(50);
        raw.raft.rpc_timeout_ms = Some(100);
        raw.raft.election_timeout_min_ms = Some(200); // 200 < 5*50=250
        raw.raft.election_timeout_max_ms = Some(600);
        let cfg = validate(raw).expect("soft warn must not fail validation");
        assert_eq!(cfg.raft.election_timeout_min.as_millis(), 200);
    }

    // ── Gap coverage: storage disk / rocksdb ──────────────────────────────

    #[test]
    fn storage_disk_requires_data_dir() {
        let mut raw = three_node_raw();
        raw.storage.backend = Some("disk".into());
        raw.storage.data_dir = None;
        let err = validate(raw).unwrap_err().to_string();
        assert!(err.contains("data_dir"), "{err}");
        assert!(err.contains("disk"), "{err}");
    }

    #[test]
    fn storage_disk_with_data_dir_ok() {
        let mut raw = three_node_raw();
        raw.storage.backend = Some("disk".into());
        raw.storage.data_dir = Some("./data/n1".into());
        let cfg = validate(raw).expect("disk + data_dir should validate");
        match cfg.storage.backend {
            StorageBackend::Disk { data_dir } => {
                assert_eq!(data_dir, std::path::PathBuf::from("./data/n1"));
            }
            other => panic!("expected Disk, got {other:?}"),
        }
    }

    #[test]
    fn storage_rocksdb_name_rejected() {
        let mut raw = three_node_raw();
        raw.storage.backend = Some("rocksdb".into());
        raw.storage.data_dir = Some("./data".into());
        let err = validate(raw).unwrap_err().to_string();
        assert!(err.contains("rocksdb"), "{err}");
        assert!(err.contains("disk"), "{err}");
    }

    #[test]
    fn storage_unknown_backend_rejected() {
        let mut raw = three_node_raw();
        raw.storage.backend = Some("sqlite".into());
        let err = validate(raw).unwrap_err().to_string();
        assert!(err.contains("unknown value"), "{err}");
        assert!(err.contains("memory") && err.contains("disk"), "{err}");
    }

    #[test]
    fn logging_unknown_format_rejected() {
        let mut raw = three_node_raw();
        raw.logging.format = Some("yaml".into());
        let err = validate(raw).unwrap_err().to_string();
        assert!(err.contains("logging.format"), "{err}");
    }

    // ── Gap coverage: cluster identity / bijection ────────────────────────

    #[test]
    fn client_addr_mismatch_rejected() {
        let mut raw = three_node_raw();
        raw.cluster.client_addr = Some("127.0.0.1:8999".into());
        let err = validate(raw).unwrap_err().to_string();
        assert!(err.contains("client_addr"), "{err}");
    }

    #[test]
    fn extra_advertise_id_without_peer_rejected() {
        let mut raw = three_node_raw();
        raw.cluster.client_endpoints = Some(vec![
            RawEndpoint {
                id: 1,
                addr: "127.0.0.1:8001".into(),
            },
            RawEndpoint {
                id: 2,
                addr: "127.0.0.1:8002".into(),
            },
            RawEndpoint {
                id: 3,
                addr: "127.0.0.1:8003".into(),
            },
            RawEndpoint {
                id: 99,
                addr: "127.0.0.1:8099".into(),
            },
        ]);
        let err = validate(raw).unwrap_err().to_string();
        assert!(err.contains("id 99"), "{err}");
        assert!(err.contains("no matching peer"), "{err}");
    }

    #[test]
    fn duplicate_peer_ids_rejected() {
        let mut raw = three_node_raw();
        raw.cluster.peers = Some(vec![
            RawPeer {
                id: 1,
                addr: "127.0.0.1:7001".into(),
            },
            RawPeer {
                id: 1,
                addr: "127.0.0.1:7002".into(),
            },
            RawPeer {
                id: 2,
                addr: "127.0.0.1:7003".into(),
            },
        ]);
        // endpoints must still be provided for whatever keys survive — expect dup id error
        let err = validate(raw).unwrap_err().to_string();
        assert!(err.contains("duplicate peer id"), "{err}");
    }

    #[test]
    fn duplicate_peer_addrs_rejected() {
        let mut raw = three_node_raw();
        raw.cluster.peers = Some(vec![
            RawPeer {
                id: 1,
                addr: "127.0.0.1:7001".into(),
            },
            RawPeer {
                id: 2,
                addr: "127.0.0.1:7001".into(),
            },
            RawPeer {
                id: 3,
                addr: "127.0.0.1:7003".into(),
            },
        ]);
        let err = validate(raw).unwrap_err().to_string();
        assert!(err.contains("duplicate address") || err.contains("7001"), "{err}");
    }

    #[test]
    fn node_id_zero_rejected() {
        let mut raw = three_node_raw();
        raw.cluster.node_id = Some(0);
        let err = validate(raw).unwrap_err().to_string();
        assert!(err.contains("node_id"), "{err}");
    }

    #[test]
    fn peer_id_zero_rejected() {
        let mut raw = three_node_raw();
        raw.cluster.peers = Some(vec![
            RawPeer {
                id: 0,
                addr: "127.0.0.1:7000".into(),
            },
            RawPeer {
                id: 1,
                addr: "127.0.0.1:7001".into(),
            },
        ]);
        let err = validate(raw).unwrap_err().to_string();
        assert!(err.contains("id 0 is reserved") || err.contains("id: 0"), "{err}");
    }

    #[test]
    fn all_peers_unparseable_surfaces_clear_error() {
        let mut raw = three_node_raw();
        raw.cluster.peers = Some(vec![
            RawPeer {
                id: 1,
                addr: "not-a-socket".into(),
            },
            RawPeer {
                id: 2,
                addr: "also-bad".into(),
            },
        ]);
        let err = validate(raw).unwrap_err().to_string();
        assert!(
            err.contains("no valid peer entries") || err.contains("invalid socket"),
            "{err}"
        );
    }

    #[test]
    fn multi_error_validation_collects_several() {
        let mut raw = three_node_raw();
        raw.raft.heartbeat_interval_ms = Some(0);
        raw.raft.rpc_timeout_ms = Some(0);
        raw.logging.format = Some("bad".into());
        match validate(raw).unwrap_err() {
            ConfigError::Validation { errors } => {
                assert!(
                    errors.len() >= 3,
                    "expected multiple errors, got {errors:?}"
                );
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn ipv6_addrs_accepted() {
        let mut raw = three_node_raw();
        raw.cluster.listen_addr = Some("[::1]:7001".into());
        raw.cluster.client_addr = Some("[::1]:8001".into());
        raw.cluster.peers = Some(vec![
            RawPeer {
                id: 1,
                addr: "[::1]:7001".into(),
            },
            RawPeer {
                id: 2,
                addr: "[::1]:7002".into(),
            },
        ]);
        raw.cluster.client_endpoints = Some(vec![
            RawEndpoint {
                id: 1,
                addr: "[::1]:8001".into(),
            },
            RawEndpoint {
                id: 2,
                addr: "[::1]:8002".into(),
            },
        ]);
        let cfg = validate(raw).expect("IPv6 SocketAddr should be accepted");
        assert!(cfg.cluster.raft_bind.is_ipv6());
        assert_eq!(cfg.cluster.quorum(), 2);
    }

    // ── Gap coverage: load path / no config source ────────────────────────

    #[test]
    fn load_path_none_no_flags_clear_error() {
        let err = Config::load(None, Overrides::default()).unwrap_err().to_string();
        assert!(
            err.contains("no configuration source") || err.contains("--config"),
            "{err}"
        );
    }

    #[test]
    fn load_path_none_with_flags_still_requires_file() {
        let overrides = Overrides {
            peers: vec![(1, "127.0.0.1:7001".parse().unwrap())],
            kv_advertise: vec![(1, "127.0.0.1:8001".parse().unwrap())],
            ..Default::default()
        };
        let err = Config::load(None, overrides).unwrap_err().to_string();
        assert!(
            err.contains("no --config path") || err.contains("node_id"),
            "{err}"
        );
    }

    #[test]
    fn one_sided_peer_override_error_mentions_client_endpoint() {
        let mut raw = three_node_raw();
        // Simulate --peer only: shrink peers without touching endpoints
        raw.cluster.peers = Some(vec![RawPeer {
            id: 1,
            addr: "127.0.0.1:7001".into(),
        }]);
        let err = validate(raw).unwrap_err().to_string();
        assert!(
            err.contains("--client-endpoint") || err.contains("no matching peer"),
            "{err}"
        );
    }

    #[test]
    fn one_sided_endpoint_override_error_mentions_peer() {
        let mut raw = three_node_raw();
        raw.cluster.client_endpoints = Some(vec![RawEndpoint {
            id: 1,
            addr: "127.0.0.1:8001".into(),
        }]);
        let err = validate(raw).unwrap_err().to_string();
        assert!(
            err.contains("--peer") || err.contains("missing entry for peer"),
            "{err}"
        );
    }
}
