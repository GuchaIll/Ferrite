# AGENTS.md, my-rust-systems-api

This file tells AI coding agents how to work on this project.
Read by Codex, Jules, Gemini, Cursor, Zed, and the AGENTS.md ecosystem.

## Stack

- Rust 2024 edition, MSRV 1.83
- Async runtime: Tokio 1.41
- Web & Telemetry: Axum 0.8 with tower middleware, tracing 0.1
- DB & Storage: SQLx 0.8 with Postgres (compile-time-checked queries), redb/sled for local raft logs
- Errors: `thiserror` in lib crates, `anyhow` in bin crates
- Verification & Tests: `cargo test` + `cargo nextest` for parallelism, `cargo miri`, `proptest`

## Base Style & Conventions

- Borrow first: `.clone()` is a last resort, not a default fix for the borrow checker. If you reach for `.clone()`, justify it in a comment.
- Errors: `?` propagation everywhere. Wrap external errors with `thiserror` variants. Log at the boundary (route handler), return below.
- Async: Every IO is non-blocking. No `std::fs`, `std::net`, or `std::thread::sleep` inside `async fn`. Use `tokio::*` or `spawn_blocking` explicitly.
- Naming: `snake_case` for fns/vars, `CamelCase` for types, `SCREAMING_SNAKE` for consts. Modules are singular nouns (`handler`, not `handlers`).

## Concurrency, Lifetimes, & State Machine Rules

- Lock Guards Across Await: NEVER hold a `std::sync::MutexGuard` or `tokio::sync::MutexGuard` across an `.await` boundary.
  - Keep lock scope minimal using explicit block scopes: `{ let guard = lock.lock(); ... }`
  - Prefer actor-based channel topologies (`tokio::sync::mpsc`, `oneshot`, `watch`) over shared mutable state (`Arc<Mutex<T>>`).
- Lock Hierarchy: Define a strict acquisition order for all locks to eliminate deadlocks (e.g., Lock A must always be acquired before Lock B).
- Cancellation Safety: Every branch in a `tokio::select!` block MUST be cancellation-safe. State changes must be atomic or committed via RAII guard types.
- Task Supervision: Do NOT write unmonitored `tokio::spawn` calls. Retain the `JoinHandle` or manage tasks through a `tokio::task::JoinSet`.
- Type-State Transitions: Encode valid lifecycle state transitions in the type system (e.g., `Node<Follower>`, `Node<Candidate>`, `Node<Leader>`) to prevent runtime state errors.

## Formal Raft Protocol Rules (Section 5 Invariants)

All consensus logic MUST enforce the invariant rules strictly as specified in the Raft consensus protocol:

1. Election Safety & Persistence:
   - At most one leader can be elected per term.
   - Nodes MUST increment `currentTerm` and reset `votedFor` before starting an election.
   - `currentTerm` and `votedFor` MUST be persisted to storage prior to responding to an RPC request.

2. Term Step-Down Rule:
   - If an RPC request or response contains term $T > \text{currentTerm}$, the server MUST immediately set $\text{currentTerm} = T$, convert to `Follower` state, and reset `votedFor`.

3. Leader Commit Constraint (Section 5.4.2):
   - A leader MUST NEVER commit a log entry from a *previous* term by counting replicas alone.
   - Only log entries from the leader's *current* term can be committed by majority counting. Previous-term entries are committed indirectly by committing a current-term entry.

4. Up-to-Date Candidate Voting Rule (Section 5.4.1):
   - A follower MUST deny its vote if the candidate's last log entry is less up-to-date than the follower's own log:
     - Compare last log terms: Higher term is more up-to-date.
     - Equal terms: Longer log is more up-to-date.

5. Log Matching & Overwrite Invariant:
   - If two logs contain an entry with the same index and term, they are identical in all entries up through that index.
   - If an `AppendEntries` RPC arrives with an entry conflicting in term with a local entry at the same index, delete the local entry AND all subsequent entries.

6. State Machine Application Ordering:
   - Entries MUST be applied to the state machine strictly in index order ($\text{lastApplied} \le \text{commitIndex}$).

## Banned Patterns

- No `unwrap()` or `expect()` outside `tests/` and `src/main.rs`.
- No raw SQL strings; use `sqlx::query!` macros for compile-time checks.
- No `.clone()` in hot loops or state machine transitions without an explicit comment explaining the memory trade-off.
- No `unsafe` blocks without a `// SAFETY:` comment outlining the specific memory invariants and lifetime guarantees.
- No ignoring task handles; panics inside spawned tasks must be caught and logged.

## Lints & Quality Gates

- `clippy` set to `deny` level for all warnings (`#![deny(clippy::all)]`).
- `rustfmt` enforced in CI pipelines.
- `cargo deny` for license checking and advisory security audits.