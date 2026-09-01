//! CLI command implementations: `ferrite validate` and `ferrite init`.
//!
//! This module owns everything user-facing: argument structs, flag parsing, and the
//! `run_*` functions that execute each subcommand. Clap wiring and `main` dispatch
//! live in the binary (future #20); these functions take plain structs so they can
//! be tested without touching argv.

use std::{net::SocketAddr, path::PathBuf};

use crate::config::{Config, ConfigError, NodeId, Overrides};

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error("invalid --peer value {input:?}: expected `<id>@<host:port>`, e.g. `1@127.0.0.1:7001`")]
    MalformedPeerFlag { input: String },

    #[error(
        "invalid --client-endpoint value {input:?}: expected `<id>@<host:port>`, \
         e.g. `1@127.0.0.1:8001`"
    )]
    MalformedEndpointFlag { input: String },

    #[error(transparent)]
    Config(#[from] ConfigError),

    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Init(String),
}

// ── Flag parsing ──────────────────────────────────────────────────────────────

/// Parse `<id>@<host:port>` as used by `--peer` and `--client-endpoint` flags.
/// Returns `None` on any format error (caller wraps in the appropriate error variant).
fn parse_id_addr(s: &str) -> Option<(NodeId, SocketAddr)> {
    let (id_str, addr_str) = s.split_once('@')?;
    let id: NodeId = id_str.trim().parse().ok()?;
    if id == 0 {
        return None; // 0 is reserved
    }
    let addr: SocketAddr = addr_str.trim().parse().ok()?;
    Some((id, addr))
}

/// Parse a slice of `--peer` flag values into `(NodeId, SocketAddr)` pairs.
pub fn parse_peer_flags(flags: &[String]) -> Result<Vec<(NodeId, SocketAddr)>, CliError> {
    flags
        .iter()
        .map(|s| {
            parse_id_addr(s).ok_or_else(|| CliError::MalformedPeerFlag { input: s.clone() })
        })
        .collect()
}

/// Parse a slice of `--client-endpoint` flag values into `(NodeId, SocketAddr)` pairs.
pub fn parse_endpoint_flags(flags: &[String]) -> Result<Vec<(NodeId, SocketAddr)>, CliError> {
    flags
        .iter()
        .map(|s| {
            parse_id_addr(s)
                .ok_or_else(|| CliError::MalformedEndpointFlag { input: s.clone() })
        })
        .collect()
}

// ── `ferrite validate` ────────────────────────────────────────────────────────

/// Arguments for `ferrite validate`. Populated from argv by the binary; passed here
/// directly in tests.
#[derive(Debug, Default)]
pub struct ValidateArgs {
    /// Path to the TOML config file. Required unless flags provide a complete config.
    pub config: Option<PathBuf>,

    /// `--peer <id>@<host:port>` flags. Replaces `cluster.peers` entirely if non-empty.
    pub peers: Vec<String>,

    /// `--client-endpoint <id>@<host:port>` flags. Replaces `cluster.client_endpoints` if non-empty.
    pub client_endpoints: Vec<String>,

    /// `--heartbeat-interval-ms <n>` override.
    pub heartbeat_interval_ms: Option<u64>,

    /// `--rpc-timeout-ms <n>` override.
    pub rpc_timeout_ms: Option<u64>,

    /// `--election-timeout-min-ms <n>` override.
    pub election_timeout_min_ms: Option<u64>,

    /// `--election-timeout-max-ms <n>` override.
    pub election_timeout_max_ms: Option<u64>,
}

/// Run `ferrite validate`: load, merge overrides, validate, return resolved config.
/// The binary prints the config on success or the error message on failure.
pub fn run_validate(args: ValidateArgs) -> Result<Config, CliError> {
    let overrides = Overrides {
        peers:                    parse_peer_flags(&args.peers)?,
        kv_advertise:             parse_endpoint_flags(&args.client_endpoints)?,
        heartbeat_interval_ms:    args.heartbeat_interval_ms,
        rpc_timeout_ms:           args.rpc_timeout_ms,
        election_timeout_min_ms:  args.election_timeout_min_ms,
        election_timeout_max_ms:  args.election_timeout_max_ms,
    };
    Config::load(args.config.as_deref(), overrides).map_err(CliError::Config)
}

// ── `ferrite init` ────────────────────────────────────────────────────────────

/// Arguments for `ferrite init`.
#[derive(Debug)]
pub struct InitArgs {
    /// Number of nodes to generate. Must be >= 1.
    pub nodes: usize,

    /// Output directory. Created if it does not exist.
    pub dir: PathBuf,

    /// First Raft port. Node `i` gets `base_port + (i - 1)`. Default: 7001.
    pub base_port: u16,

    /// First KV/client port. Node `i` gets `client_port + (i - 1)`. Default: 8001.
    pub client_port: u16,

    /// Overwrite existing `node*.toml` files. Does not remove other files.
    pub force: bool,
}

impl Default for InitArgs {
    fn default() -> Self {
        Self {
            nodes: 1,
            dir: PathBuf::from("."),
            base_port: 7001,
            client_port: 8001,
            force: false,
        }
    }
}

/// Run `ferrite init`. Returns the list of written file paths on success.
///
/// Validation order:
/// 1. Reject bad inputs (nodes, port overflow, port range overlap) before touching disk.
/// 2. Reject non-empty `--dir` without `--force`.
/// 3. Generate all TOML strings and validate each with `Config::from_toml_str`.
/// 4. Write all files (only after all pass validation).
pub fn run_init(args: InitArgs) -> Result<Vec<PathBuf>, CliError> {
    // ── Input validation ──────────────────────────────────────────────────
    if args.nodes < 1 {
        return Err(CliError::Init("--nodes must be >= 1".into()));
    }

    let n = args.nodes as u64;
    let base = args.base_port as u64;
    let client = args.client_port as u64;
    let raft_last = base + n - 1;
    let client_last = client + n - 1;

    if raft_last > 65535 {
        return Err(CliError::Init(format!(
            "--base-port {base} with --nodes {n} reaches port {raft_last} > 65535"
        )));
    }
    if client_last > 65535 {
        return Err(CliError::Init(format!(
            "--client-port {client} with --nodes {n} reaches port {client_last} > 65535"
        )));
    }

    // Ranges overlap when max(base, client) <= min(raft_last, client_last)
    if base.max(client) <= raft_last.min(client_last) {
        return Err(CliError::Init(format!(
            "raft port range {base}..={raft_last} overlaps with client port range \
             {client}..={client_last}; choose non-overlapping --base-port and --client-port"
        )));
    }

    // ── Directory ─────────────────────────────────────────────────────────
    let dir = &args.dir;
    if dir.exists() {
        let non_empty = std::fs::read_dir(dir)?.next().is_some();
        if non_empty && !args.force {
            return Err(CliError::Init(format!(
                "directory {} is not empty; use --force to overwrite node*.toml files",
                dir.display()
            )));
        }
    } else {
        std::fs::create_dir_all(dir)?;
    }

    // ── Build shared peer / endpoint tables ───────────────────────────────
    // All node files share identical peers and client_endpoints.
    let peers: Vec<(u64, String)> = (1..=n)
        .map(|i| (i, format!("127.0.0.1:{}", base + i - 1)))
        .collect();

    let endpoints: Vec<(u64, String)> = (1..=n)
        .map(|i| (i, format!("127.0.0.1:{}", client + i - 1)))
        .collect();

    // ── Generate, validate, then write ────────────────────────────────────
    // Generate all strings first, validate all, then write — so partial writes
    // cannot happen if validation fails mid-way.
    let mut node_files: Vec<(PathBuf, String)> = Vec::with_capacity(n as usize);

    for (idx, (node_id, raft_bind)) in peers.iter().enumerate() {
        let kv_bind = &endpoints[idx].1;
        let data_dir = format!("./data/node{node_id}");

        let toml_str = render_node_toml(*node_id, raft_bind, kv_bind, &peers, &endpoints, &data_dir);

        // Validate before touching disk
        Config::from_toml_str(&toml_str, Overrides::default()).map_err(|e| {
            CliError::Init(format!("node{node_id}.toml would fail validation: {e}"))
        })?;

        node_files.push((dir.join(format!("node{node_id}.toml")), toml_str));
    }

    // All passed — write
    let mut written: Vec<PathBuf> = Vec::with_capacity(node_files.len());
    for (path, content) in node_files {
        std::fs::write(&path, &content)?;
        written.push(path);
    }

    Ok(written)
}

// ── TOML renderer ─────────────────────────────────────────────────────────────

/// Produce the TOML file content for one node. Peers and endpoints are identical
/// across all files; only `node_id`, `raft_bind`, `kv_bind`, and `data_dir` vary.
fn render_node_toml(
    node_id: u64,
    raft_bind: &str,
    kv_bind: &str,
    peers: &[(u64, String)],
    endpoints: &[(u64, String)],
    data_dir: &str,
) -> String {
    let mut out = String::new();

    out.push_str("[cluster]\n");
    out.push_str(&format!("node_id     = {node_id}\n"));
    out.push_str(&format!("listen_addr = \"{raft_bind}\"\n"));
    out.push_str(&format!("client_addr = \"{kv_bind}\"\n"));

    for (id, addr) in peers {
        out.push_str("\n[[cluster.peers]]\n");
        out.push_str(&format!("id   = {id}\n"));
        out.push_str(&format!("addr = \"{addr}\"\n"));
    }

    for (id, addr) in endpoints {
        out.push_str("\n[[cluster.client_endpoints]]\n");
        out.push_str(&format!("id   = {id}\n"));
        out.push_str(&format!("addr = \"{addr}\"\n"));
    }

    out.push_str("\n[raft]\n");
    out.push_str("heartbeat_interval_ms   = 50\n");
    out.push_str("rpc_timeout_ms          = 100\n");
    out.push_str("election_timeout_min_ms = 300\n");
    out.push_str("election_timeout_max_ms = 600\n");
    out.push_str("compaction_threshold    = 1000\n");

    out.push_str("\n[storage]\n");
    // Durable backend name is "disk" (sled/segment) — not RocksDB.
    out.push_str("backend  = \"disk\"\n");
    out.push_str(&format!("data_dir = \"{data_dir}\"\n"));

    out.push_str("\n[logging]\n");
    out.push_str("format = \"pretty\"\n");

    out
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Flag parsing ──────────────────────────────────────────────────────

    #[test]
    fn parse_peer_flag_valid() {
        let (id, addr) = parse_id_addr("2@127.0.0.1:7002").unwrap();
        assert_eq!(id, 2);
        assert_eq!(addr.to_string(), "127.0.0.1:7002");
    }

    #[test]
    fn parse_peer_flag_rejects_id_zero() {
        assert!(parse_id_addr("0@127.0.0.1:7001").is_none());
    }

    #[test]
    fn parse_peer_flag_rejects_non_numeric_id() {
        assert!(parse_id_addr("abc@127.0.0.1:7001").is_none());
    }

    #[test]
    fn parse_peer_flag_rejects_missing_at() {
        assert!(parse_id_addr("127.0.0.1:7001").is_none());
    }

    #[test]
    fn parse_peer_flag_rejects_bad_addr() {
        assert!(parse_id_addr("1@not-an-addr").is_none());
    }

    // ── `ferrite validate` ────────────────────────────────────────────────

    #[test]
    fn validate_override_replaces_timing() {
        // Write a minimal config to a temp file, then override heartbeat via args
        let dir = std::env::temp_dir().join("ferrite_cli_validate");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // First init a cluster so we have a valid file to read
        run_init(InitArgs { nodes: 1, dir: dir.clone(), ..Default::default() }).unwrap();

        let cfg = run_validate(ValidateArgs {
            config: Some(dir.join("node1.toml")),
            heartbeat_interval_ms: Some(25),
            ..Default::default()
        })
        .unwrap();

        assert_eq!(cfg.raft.heartbeat_interval.as_millis(), 25);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── `ferrite init` ────────────────────────────────────────────────────

    #[test]
    fn init_port_overlap_rejected_before_write() {
        let dir = std::env::temp_dir().join("ferrite_init_overlap");
        let _ = std::fs::remove_dir_all(&dir);

        // base 8000..8004, client 8001..8005 — overlap
        let err = run_init(InitArgs {
            nodes: 5,
            dir: dir.clone(),
            base_port: 8000,
            client_port: 8001,
            ..Default::default()
        })
        .unwrap_err()
        .to_string();

        assert!(err.contains("overlaps"), "{err}");
        assert!(!dir.join("node1.toml").exists(), "no files should be written");
    }

    #[test]
    fn init_three_nodes_all_valid() {
        let dir = std::env::temp_dir().join("ferrite_init_3node");
        let _ = std::fs::remove_dir_all(&dir);

        let written = run_init(InitArgs {
            nodes: 3,
            dir: dir.clone(),
            ..Default::default()
        })
        .unwrap();

        assert_eq!(written.len(), 3);
        for path in &written {
            assert!(path.exists(), "{} missing", path.display());
            // Each file must pass Config::load (round-trip)
            Config::load(Some(path), Overrides::default())
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn init_port_math_node3() {
        let dir = std::env::temp_dir().join("ferrite_init_portmath");
        let _ = std::fs::remove_dir_all(&dir);

        run_init(InitArgs { nodes: 3, dir: dir.clone(), ..Default::default() }).unwrap();

        let node3 = std::fs::read_to_string(dir.join("node3.toml")).unwrap();
        assert!(node3.contains("7003"), "node3 raft port should be 7003:\n{node3}");
        assert!(node3.contains("8003"), "node3 kv port should be 8003:\n{node3}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn init_nonempty_dir_fails_without_force() {
        let dir = std::env::temp_dir().join("ferrite_init_nonempty");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("other.txt"), "existing content").unwrap();

        let err = run_init(InitArgs { nodes: 1, dir: dir.clone(), ..Default::default() })
            .unwrap_err()
            .to_string();
        assert!(err.contains("not empty"), "{err}");

        // --force should succeed and leave other.txt intact
        run_init(InitArgs { nodes: 1, dir: dir.clone(), force: true, ..Default::default() })
            .unwrap();
        assert!(dir.join("other.txt").exists(), "--force must not remove unrelated files");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Override replacement — no union ──────────────────────────────────

    #[test]
    fn override_client_endpoints_replaces_file_entirely() {
        // Generate a 3-node cluster, then call run_validate with a single
        // --client-endpoint. If union happened we'd have 4 endpoints and
        // validation would accept them; if replacement happened we'd have 1
        // endpoint and validation would fail with "missing entry for peer id 2",
        // proving the file's 3 endpoints were discarded.
        let dir = std::env::temp_dir().join("ferrite_cli_ep_replace");
        let _ = std::fs::remove_dir_all(&dir);
        run_init(InitArgs { nodes: 3, dir: dir.clone(), ..Default::default() }).unwrap();

        let err = run_validate(ValidateArgs {
            config: Some(dir.join("node1.toml")),
            // Only provide 1 endpoint — must replace the file's 3
            client_endpoints: vec!["1@127.0.0.1:8001".into()],
            ..Default::default()
        })
        .unwrap_err()
        .to_string();

        // Validation fails because peer ids 2 and 3 have no endpoint — proves
        // the override replaced (not unioned with) the file's client_endpoints.
        assert!(
            err.contains("missing entry for peer id 2") || err.contains("missing entry for peer id 3"),
            "expected 'missing entry' error proving replacement, got: {err}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn init_emits_disk_backend_not_rocksdb() {
        let dir = std::env::temp_dir().join("ferrite_init_disk_backend");
        let _ = std::fs::remove_dir_all(&dir);

        run_init(InitArgs {
            nodes: 1,
            dir: dir.clone(),
            ..Default::default()
        })
        .unwrap();

        let toml_str = std::fs::read_to_string(dir.join("node1.toml")).unwrap();
        assert!(
            toml_str.contains("backend  = \"disk\""),
            "init must emit disk backend:\n{toml_str}"
        );
        assert!(
            !toml_str.contains("rocksdb"),
            "init must not emit rocksdb:\n{toml_str}"
        );

        let cfg = Config::load(Some(&dir.join("node1.toml")), Overrides::default()).unwrap();
        match cfg.storage.backend {
            crate::config::StorageBackend::Disk { .. } => {}
            other => panic!("expected Disk backend from init, got {other:?}"),
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn validate_missing_config_path_clear_error() {
        let err = run_validate(ValidateArgs::default()).unwrap_err().to_string();
        assert!(
            err.contains("no configuration source") || err.contains("--config"),
            "{err}"
        );
    }

    // ── Error path coverage ───────────────────────────────────────────────

    #[test]
    fn malformed_toml_returns_toml_error() {
        use crate::config::{Config, ConfigError, Overrides};
        let err = Config::from_toml_str("[[[ not valid toml", Overrides::default())
            .unwrap_err();
        assert!(
            matches!(err, ConfigError::Toml(_)),
            "expected ConfigError::Toml, got: {err:?}"
        );
    }

    #[test]
    fn malformed_peer_flag_returns_cli_error() {
        let err = run_validate(ValidateArgs {
            peers: vec!["no-at-sign".into()],
            ..Default::default()
        })
        .unwrap_err();
        assert!(
            matches!(err, CliError::MalformedPeerFlag { .. }),
            "expected MalformedPeerFlag, got: {err:?}"
        );
    }

    #[test]
    fn malformed_client_endpoint_flag_returns_cli_error() {
        let err = run_validate(ValidateArgs {
            client_endpoints: vec!["0@127.0.0.1:8001".into()], // id 0 is reserved
            ..Default::default()
        })
        .unwrap_err();
        assert!(
            matches!(err, CliError::MalformedEndpointFlag { .. }),
            "expected MalformedEndpointFlag, got: {err:?}"
        );
    }

    #[test]
    fn init_peers_identical_across_files() {
        let dir = std::env::temp_dir().join("ferrite_init_identical_peers");
        let _ = std::fs::remove_dir_all(&dir);

        run_init(InitArgs { nodes: 3, dir: dir.clone(), ..Default::default() }).unwrap();

        // All three files must have all three peers
        for i in 1..=3u64 {
            let content = std::fs::read_to_string(dir.join(format!("node{i}.toml"))).unwrap();
            assert!(content.contains("7001"), "node{i}.toml missing peer 1");
            assert!(content.contains("7002"), "node{i}.toml missing peer 2");
            assert!(content.contains("7003"), "node{i}.toml missing peer 3");
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
