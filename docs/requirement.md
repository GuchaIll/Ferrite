# Raft KV Store — Project Requirements Specification

**Stack:** Rust · Tokio · gRPC (tonic) · RocksDB (optional persistence layer)  
**Scope:** Solo side project, learn-by-building distributed systems + concurrency debugging  
**Reference:** Raft paper (Ongaro & Ousterhout, 2014) + MIT 6.5840 Labs 2–4 scope

---

## 1. Project Goals

| Goal | Description |
|------|-------------|
| **Consensus correctness** | Implement Raft such that it never returns incorrect data under any failure scenario |
| **Concurrency debugging** | Surface and reason about real async races, deadlocks, and message-ordering bugs |
| **Deterministic testing** | Build a simulated network+clock layer so failures are reproducible, not flaky |
| **Production patterns** | Apply patterns from TiKV and etcd — not just a textbook implementation |

---

## 2. System Overview

```
┌─────────────────────────────────────────────────────┐
│                    Client Layer                      │
│   CLI tool / gRPC client (Get, Put, Delete, CAS)    │
└────────────────────┬────────────────────────────────┘
                     │ gRPC (linearizable requests)
┌────────────────────▼────────────────────────────────┐
│                 KV Service Layer                     │
│   Request routing · Leader redirect · Read index    │
└────────────────────┬────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────┐
│              Raft Consensus Module                   │
│  Leader election · Log replication · Snapshotting   │
└────────────────────┬────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────┐
│              State Machine (KV Store)                │
│   In-memory HashMap  OR  RocksDB-backed             │
└─────────────────────────────────────────────────────┘
```

**Cluster topology:** 3 or 5 nodes (configurable). All nodes are peers; leadership is
elected dynamically. Clients may connect to any node — followers redirect to the leader.

---

## 3. Functional Requirements

### 3.1 Raft Consensus Module

#### 3.1.1 Leader Election

- **REQ-E1** Each node starts as a Follower with a randomly seeded election timeout in
  `[150ms, 300ms]`.
- **REQ-E2** If a Follower receives no `AppendEntries` RPC (heartbeat or log) within its
  timeout, it transitions to Candidate and increments `currentTerm`.
- **REQ-E3** A Candidate sends `RequestVote` RPCs to all peers concurrently.
- **REQ-E4** A node grants a vote only if:
  - It has not voted for another candidate in this term, AND
  - The candidate's log is at least as up-to-date as the voter's (`lastLogTerm` then
    `lastLogIndex` comparison).
- **REQ-E5** A Candidate winning a majority (⌊N/2⌋ + 1) becomes Leader and immediately
  sends heartbeats to suppress new elections.
- **REQ-E6** If a node receives any RPC with a higher term, it immediately reverts to
  Follower and updates `currentTerm`.
- **REQ-E7** Split votes result in timeout and a new election with a fresh random timeout.

#### 3.1.2 Log Replication

- **REQ-L1** The Leader appends client commands to its local log before replicating.
- **REQ-L2** The Leader sends `AppendEntries` RPCs to all followers in parallel.
  Each RPC includes `prevLogIndex`, `prevLogTerm`, and a batch of entries.
- **REQ-L3** A Follower rejects an `AppendEntries` if its log doesn't contain an entry
  at `prevLogIndex` with term `prevLogTerm` (the consistency check).
- **REQ-L4** On rejection, the Leader decrements `nextIndex` for that follower and
  retries. Implement the **fast backup optimization** (skip back by term, not one-by-one).
- **REQ-L5** An entry is **committed** once the Leader has received acknowledgement from
  a majority. The Leader advances `commitIndex` and applies committed entries to the
  state machine in order.
- **REQ-L6** `commitIndex` is piggybacked on subsequent `AppendEntries` RPCs so followers
  can advance their own `commitIndex` and apply entries.
- **REQ-L7** Applied entries are never re-applied. The module tracks `lastApplied`
  separately from `commitIndex`.
- **REQ-L8** Log entries include: `{ index: u64, term: u64, command: Vec<u8> }`.

#### 3.1.3 Persistence

- **REQ-P1** The following state MUST be persisted to durable storage before responding
  to any RPC: `currentTerm`, `votedFor`, `log[]`.
- **REQ-P2** On restart, a node reads persisted state before rejoining the cluster.
- **REQ-P3** Persistence is abstracted behind a `Storage` trait so both an in-memory
  mock (for testing) and a disk-backed implementation (RocksDB or `sled`) can be
  swapped in without changing Raft logic.

```rust
pub trait Storage: Send + Sync {
    fn save_hard_state(&self, term: u64, voted_for: Option<u64>) -> Result<()>;
    fn save_entries(&self, entries: &[LogEntry]) -> Result<()>;
    fn load_hard_state(&self) -> Result<(u64, Option<u64>)>;
    fn load_entries(&self, from: u64) -> Result<Vec<LogEntry>>;
    fn save_snapshot(&self, snapshot: &Snapshot) -> Result<()>;
    fn load_snapshot(&self) -> Result<Option<Snapshot>>;
}
```

#### 3.1.4 Log Compaction (Snapshotting)

- **REQ-S1** When the log exceeds a configurable threshold (e.g., 1000 entries), the
  state machine takes a snapshot of its current state and the corresponding log index.
- **REQ-S2** Log entries up to (and including) the snapshot index are discarded.
- **REQ-S3** The Leader sends `InstallSnapshot` RPC to lagging followers whose `nextIndex`
  has fallen behind the snapshot's `lastIncludedIndex`.
- **REQ-S4** A Follower receiving `InstallSnapshot` replaces its state machine with the
  snapshot and discards its log up to `lastIncludedIndex`.
- **REQ-S5** Snapshots are serialized with `bincode` or `serde_json`; format is opaque to
  the Raft module (the state machine owns it).

---

### 3.2 Key-Value State Machine

#### 3.2.1 Operations

| Op | Arguments | Returns | Semantics |
|----|-----------|---------|-----------|
| `Get` | `key: String` | `Option<String>` | Point read |
| `Put` | `key: String, value: String` | `()` | Upsert |
| `Delete` | `key: String` | `()` | Remove key (no-op if absent) |
| `CAS` | `key, expected: Option<String>, new: Option<String>` | `bool` | Atomic compare-and-swap |

- **REQ-KV1** The state machine is deterministic: given the same sequence of log entries,
  it must produce the same state on every node.
- **REQ-KV2** `CAS` is a first-class log entry (not built from Get + Put), ensuring
  atomicity across replicas.
- **REQ-KV3** The state machine exposes `apply(entry: &LogEntry) -> CommandResult` and
  `snapshot() -> Vec<u8>` and `restore(snapshot: &[u8])`.

#### 3.2.2 Linearizability

- **REQ-LIN1** Every operation must appear to take effect instantaneously at some point
  between its invocation and its response.
- **REQ-LIN2** Two reads of the same key with no intervening write must return the same
  value or a strictly newer value (no stale reads).
- **REQ-LIN3** To prevent stale leader reads: the Leader confirms its leadership by
  broadcasting a heartbeat and waiting for majority acknowledgement before serving a
  read (read index protocol). Lease-based reads are a stretch goal.
- **REQ-LIN4** Each client request carries a `(client_id: u64, seq_num: u64)` pair.
  The Leader deduplicates requests with the same pair to ensure exactly-once semantics
  at the state machine level (idempotency table).

---

### 3.3 Client Protocol (gRPC)

#### 3.3.1 API Surface

```protobuf
service KvStore {
  rpc Get(GetRequest) returns (GetResponse);
  rpc Put(PutRequest) returns (PutResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
  rpc Cas(CasRequest) returns (CasResponse);
}

message GetRequest  { string key = 1; uint64 client_id = 2; uint64 seq_num = 3; }
message GetResponse { optional string value = 1; bool not_found = 2; string leader_hint = 3; }

message PutRequest  { string key = 1; string value = 2; uint64 client_id = 3; uint64 seq_num = 4; }
message PutResponse { bool ok = 1; string leader_hint = 2; }

// ... (Delete and Cas follow the same pattern)
```

- **REQ-C1** If a node is not the leader, it returns `leader_hint` pointing to the
  current known leader; the client retries there.
- **REQ-C2** If leadership is unknown (during election), the server returns
  `ErrorCode::LeaderUnknown` and the client retries with exponential backoff.
- **REQ-C3** Clients auto-retry on network errors, backoff starting at 50ms, capped at
  2s, with jitter.
- **REQ-C4** The CLI client supports: `kv get <key>`, `kv put <key> <value>`,
  `kv del <key>`, `kv cas <key> <expected> <new>`.

---

### 3.4 Intra-Cluster RPC

```protobuf
service Raft {
  rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
  rpc RequestVote(RequestVoteRequest)     returns (RequestVoteResponse);
  rpc InstallSnapshot(SnapshotRequest)   returns (SnapshotResponse);
}
```

- **REQ-R1** All intra-cluster RPCs are issued over persistent gRPC connections (connection
  pool per peer).
- **REQ-R2** Each RPC has a 200ms deadline. Timeouts are treated as failures, not hangs.
- **REQ-R3** The Raft module is decoupled from the transport via a `Transport` trait,
  enabling the deterministic simulator to substitute a fake transport.

```rust
#[async_trait]
pub trait Transport: Send + Sync {
    async fn append_entries(&self, to: NodeId, req: AppendEntriesRequest)
        -> Result<AppendEntriesResponse>;
    async fn request_vote(&self, to: NodeId, req: RequestVoteRequest)
        -> Result<RequestVoteResponse>;
    async fn install_snapshot(&self, to: NodeId, req: SnapshotRequest)
        -> Result<SnapshotResponse>;
}
```

---

## 4. Non-Functional Requirements

| ID | Requirement |
|----|-------------|
| **NFR-1** | A cluster of 5 nodes tolerates 2 simultaneous failures with no data loss |
| **NFR-2** | Leader election completes within 500ms of a leader failure under normal network conditions |
| **NFR-3** | Throughput of ≥500 Put ops/sec on a 3-node localhost cluster (single client, no persistence) |
| **NFR-4** | P99 latency for Put < 20ms on localhost |
| **NFR-5** | No unsafe Rust in core Raft/KV modules (unsafe permitted only in storage backends) |
| **NFR-6** | All public APIs and state transitions documented with inline rustdoc |
| **NFR-7** | Zero `unwrap()` / `expect()` in non-test code; all errors propagated with `thiserror` |

---

## 5. Architecture & Module Layout

```
raft-kv/
├── proto/
│   ├── raft.proto
│   └── kv.proto
├── src/
│   ├── main.rs                  # Node entrypoint, CLI arg parsing
│   ├── config.rs                # NodeConfig, ClusterConfig
│   ├── raft/
│   │   ├── mod.rs               # RaftNode, public API
│   │   ├── state.rs             # RaftState enum (Follower/Candidate/Leader)
│   │   ├── log.rs               # RaftLog, LogEntry
│   │   ├── election.rs          # Election timer logic
│   │   ├── replication.rs       # Leader replication loop
│   │   ├── snapshot.rs          # Snapshot trigger + InstallSnapshot handler
│   │   └── storage/
│   │       ├── mod.rs           # Storage trait
│   │       ├── memory.rs        # In-memory (test) implementation
│   │       └── rocksdb.rs       # RocksDB implementation (Phase 4)
│   ├── kv/
│   │   ├── mod.rs               # KvStateMachine
│   │   ├── command.rs           # Command enum + serialization
│   │   └── idempotency.rs       # ClientId + SeqNum dedup table
│   ├── transport/
│   │   ├── mod.rs               # Transport trait
│   │   ├── grpc.rs              # Real gRPC transport
│   │   └── simulated.rs        # Deterministic sim transport (Phase 3)
│   ├── server/
│   │   ├── kv_service.rs        # gRPC KvStore service impl
│   │   └── raft_service.rs      # gRPC Raft service impl
│   └── sim/
│       ├── mod.rs               # Simulator harness
│       ├── network.rs           # Simulated network (delay, drop, partition)
│       └── clock.rs             # Deterministic clock
├── tests/
│   ├── election_tests.rs
│   ├── replication_tests.rs
│   ├── fault_injection_tests.rs
│   └── linearizability_tests.rs
├── benches/
│   └── throughput.rs
└── client/
    └── main.rs                  # CLI client binary
```

---

## 6. Implementation Phases

### Phase 1 — Core Raft (No Persistence, In-Memory)
**Goal:** Get consensus working; pass basic election + replication tests.

- [ ] Define `LogEntry`, `RaftState`, `RaftNode` structs
- [ ] Implement `RequestVote` RPC handler and vote-granting logic
- [ ] Implement election timer (Tokio `sleep` loop with reset on heartbeat)
- [ ] Implement `AppendEntries` RPC handler (heartbeat + log consistency check)
- [ ] Implement Leader replication loop: send entries, track `nextIndex`/`matchIndex`
- [ ] Advance `commitIndex` on majority acknowledgement; notify state machine via channel
- [ ] In-memory `Storage` implementation
- [ ] Basic `KvStateMachine`: `HashMap<String, String>` + apply/snapshot stubs

**Deliverable:** 3-node cluster running in separate Tokio tasks; manual test via `println!` logs showing elections and replication.

---

### Phase 2 — Client Protocol + gRPC
**Goal:** External clients can Get/Put/Delete against the cluster.

- [ ] Define `raft.proto` and `kv.proto`; generate with `tonic-build`
- [ ] Implement `KvService` gRPC handler: route to state machine if leader, else redirect
- [ ] Implement `RaftService` gRPC handler: delegate to `RaftNode`
- [ ] Read-index protocol for linearizable reads
- [ ] Client ID + sequence number deduplication (idempotency table in state machine)
- [ ] CLI client binary with retry + backoff logic
- [ ] Integration test: 3-node cluster, client writes 100 keys, restarts one follower, verifies reads

**Deliverable:** End-to-end: `./kv put foo bar` and `./kv get foo` work against a running cluster.

---

### Phase 3 — Deterministic Simulator + Fault Injection
**Goal:** Make concurrency bugs reproducible; build confidence in correctness.

- [ ] Implement `SimulatedTransport`: in-process message queue with controllable delay/drop
- [ ] Implement deterministic clock: manual tick advancement (no real `sleep`)
- [ ] Build `Simulator` harness: spawns N Raft nodes sharing the simulated network + clock
- [ ] Fault injection primitives:
  - Network partition (isolate node(s) from subset of peers)
  - Message delay (add latency to specific node/direction)
  - Message drop (drop percentage of messages)
  - Node crash + restart (wipe volatile state, reload from `Storage`)
- [ ] Scenario tests (all deterministic, seed-reproducible):
  - Basic election: 3 nodes start → one becomes leader within T ticks
  - Leader failure: kill leader → new leader elected → writes continue
  - Network partition: isolate leader → new leader on majority side → old leader rejected
  - Split brain attempt: simultaneous candidates in same term → exactly one wins
  - Follower restart: node crashes, restarts from in-memory storage, catches up via `AppendEntries`

**Deliverable:** All scenarios pass 100% deterministically. Any failure can be replayed from its seed.

---

### Phase 4 — Log Compaction + Persistence
**Goal:** Nodes survive restarts with durable state; unbounded log growth prevented.

- [ ] Snapshot trigger: when `log.len() > COMPACTION_THRESHOLD`
- [ ] `KvStateMachine::snapshot()` → serialize current map to `Vec<u8>`
- [ ] `KvStateMachine::restore(bytes)` → deserialize snapshot
- [ ] Trim log to `lastIncludedIndex` after snapshot
- [ ] `InstallSnapshot` RPC: leader sends full snapshot to lagging follower
- [ ] Follower applies snapshot: replace state machine, reset log
- [ ] RocksDB `Storage` implementation: persist `currentTerm`, `votedFor`, `log[]`, snapshots
- [ ] Crash recovery test: write 1000 keys → crash all nodes → restart all → verify all keys readable

**Deliverable:** Cluster survives full shutdown + restart. Log stays bounded.

---

### Phase 5 — Hardening + Stretch Goals (pick any)

#### 5a. Fast Log Backup
- Replace naive `nextIndex--` with term-skipping optimization:
  Follower returns `conflictTerm` and `conflictIndex`; Leader jumps `nextIndex` back to
  the first entry in `conflictTerm` (or just before `conflictIndex` if term not found).
- Reduces round trips for heavily lagged followers from O(entries) to O(terms).

#### 5b. Lease-Based Reads
- Leader tracks when its lease (based on heartbeat round-trip) expires.
- During a valid lease, reads bypass the read-index broadcast — just check lease clock.
- **Challenge:** requires careful reasoning about clock skew; document the safety argument.

#### 5c. Pre-Vote Extension (Raftscope §4.2.3)
- Before incrementing term and starting an election, send pre-vote RPCs.
- Prevents disruption from partitioned nodes with stale terms rejoining the cluster.

#### 5d. Sharded KV (Multi-Raft)
- Add a `ShardController` service (itself Raft-replicated) that owns shard → replica-group mapping.
- Each shard is an independent Raft group with its own log.
- Clients hash keys to shards; client library routes accordingly.
- Implement shard migration: transfer ownership from one Raft group to another without losing writes.

---

## 7. Testing Requirements

### 7.1 Unit Tests

| Module | What to Test |
|--------|-------------|
| `raft::log` | Append, truncate, term-index lookup, out-of-order rejection |
| `raft::election` | Vote grant conditions (stale term, log comparison) |
| `raft::replication` | `commitIndex` advancement, `matchIndex` quorum math |
| `kv::command` | Serialization round-trip for all command variants |
| `kv::idempotency` | Duplicate sequence number deduplication |

### 7.2 Integration Tests (Real Tokio, Simulated Transport)

| Scenario | Pass Condition |
|----------|---------------|
| Basic leader election | Exactly one leader elected in <500ms wall clock |
| Log replication (no failure) | All nodes have identical logs after 100 writes |
| Follower rejoin | Node restarted after 50 writes catches up to correct state |
| Leader failure + recovery | New leader elected; no committed entry lost |
| Network partition (3-node) | Majority partition makes progress; minority does not |
| Partition heal | After heal, minority rejoins and catches up |
| Concurrent clients | 5 clients × 200 ops/each → linearizability checker passes |

### 7.3 Linearizability Checker
Integrate [**porcupine**](https://github.com/anishathalye/porcupine) (or port its algorithm to Rust):
- Record every client operation as `(invocation_time, response_time, op, result)`
- After a test scenario, run the linearizability checker over the history
- Any non-linearizable history is a bug — fail the test with the offending history printed

### 7.4 Fault Injection Scenarios (Deterministic Sim)

```
Scenario: repeated leader churn
  - 5 nodes
  - Every 200 ticks: kill current leader
  - Run for 5000 ticks with 50 concurrent write clients
  - Assert: no committed entry is lost, linearizability holds

Scenario: message drop storm
  - 3 nodes
  - 30% of all messages dropped randomly (seeded)
  - Run 2000 ticks
  - Assert: eventually consistent, no data loss

Scenario: simultaneous followers restart
  - 5 nodes, write 200 entries
  - Simultaneously restart 2 followers (persist-then-reload)
  - Assert: all nodes end up with identical logs
```

### 7.5 Benchmark

Using `criterion`:
- **Throughput:** saturate the Leader with sequential `Put` ops, report ops/sec
- **Latency distribution:** measure P50/P95/P99 round-trip for Put across 10k ops
- **Snapshot cost:** measure time to take + restore a 100k-entry snapshot

---

## 8. Correctness Invariants

These must hold at every state transition. Add debug assertions in dev builds.

| Invariant | Description |
|-----------|-------------|
| **Election Safety** | At most one leader per term |
| **Log Matching** | If two logs have an entry with the same index and term, all preceding entries are identical |
| **Leader Completeness** | A leader has all committed entries from previous terms |
| **State Machine Safety** | All nodes apply the same sequence of entries in the same order |
| **Commit Monotonicity** | `commitIndex` never decreases |
| **Term Monotonicity** | `currentTerm` never decreases |
| **Vote Once** | A node grants at most one vote per term |

---

## 9. Concurrency Design in Tokio

Raft is inherently state-machine-like; in async Rust, the safest pattern is a **single actor task** owning all mutable Raft state, communicating via channels.

```
┌──────────────────────────────────────────────────────────┐
│                    RaftActor (single task)                │
│  Owns: RaftState, RaftLog, Storage, Transport             │
│  Receives: RaftMsg enum via mpsc channel                  │
│  Sends: ApplyMsg to state machine via mpsc channel        │
└─────────────┬────────────────────────────┬───────────────┘
              │ RaftMsg::AppendEntries      │ ApplyMsg
  gRPC inbound│ RaftMsg::RequestVote        │
  handlers ───┘ RaftMsg::ClientCommand      │
                RaftMsg::ElectionTimeout    ▼
                RaftMsg::HeartbeatTick ┌──────────────────┐
                                       │ KvStateMachine   │
                                       │ (single task)    │
                                       └──────────────────┘
```

**Why:** Avoids `Arc<Mutex<RaftState>>` contention and eliminates lock-ordering bugs. All
state is owned by one task; other tasks communicate via typed messages. This also makes the
deterministic simulator trivial — just control what goes into the channel and when.

**Key message types:**

```rust
enum RaftMsg {
    AppendEntries { from: NodeId, req: AppendEntriesRequest, reply_tx: oneshot::Sender<AppendEntriesResponse> },
    RequestVote   { from: NodeId, req: RequestVoteRequest,   reply_tx: oneshot::Sender<RequestVoteResponse>   },
    InstallSnapshot { req: SnapshotRequest, reply_tx: oneshot::Sender<SnapshotResponse> },
    ClientCommand { cmd: Vec<u8>, reply_tx: oneshot::Sender<Result<CommandResult>> },
    ElectionTimeout,
    HeartbeatTick,
    Shutdown,
}
```

---

## 10. Configuration Reference

```toml
[cluster]
node_id          = 1
peers            = ["127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"]
listen_addr      = "127.0.0.1:7001"
client_addr      = "127.0.0.1:8001"

[raft]
election_timeout_min_ms  = 150
election_timeout_max_ms  = 300
heartbeat_interval_ms    = 50
rpc_timeout_ms           = 200
compaction_threshold     = 1000    # log entries before snapshot

[storage]
backend          = "memory"        # "memory" | "rocksdb"
data_dir         = "./data/node1"

[logging]
level            = "info"          # "trace" | "debug" | "info"
format           = "json"
```

---

## 11. Key Dependencies

| Crate | Purpose |
|-------|---------|
| `tokio` (full features) | Async runtime, timers, channels |
| `tonic` + `prost` | gRPC transport + protobuf codegen |
| `tonic-build` | Proto compile in `build.rs` |
| `serde` + `bincode` | Log entry + snapshot serialization |
| `thiserror` | Structured error types |
| `tracing` + `tracing-subscriber` | Structured logging, span-based debugging |
| `rocksdb` | Persistent storage backend (Phase 4) |
| `criterion` | Benchmarks |
| `proptest` | Property-based tests for edge cases |

---

## 12. Learning Checkpoints

After each phase, you should be able to answer these without looking at the code:

**Phase 1:** Why does Raft require a majority for both elections and commits? What happens if you only require majority for one?

**Phase 2:** What is the read-index protocol and why can't the leader just read its local state?

**Phase 3:** What class of bugs does the deterministic simulator find that integration tests against a real network would miss?

**Phase 4:** What is the `lastIncludedIndex` / `lastIncludedTerm` in a snapshot, and why do you need both?

**Phase 5 (if sharding):** How do you prevent a key from being lost during shard migration if the receiving group crashes mid-transfer?