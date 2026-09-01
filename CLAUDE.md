# CLAUDE.md

This file provides execution commands and operational guidelines for Claude agents operating on this Rust systems repository.

## Execution & Verification Commands

- Build: `cargo build --all-targets`
- Test: `cargo nextest run --all-targets`
- Concurrency & Undefined Behavior Checks: `MIRIFLAGS="-Zmiri-disable-isolation" cargo miri test`
- Lint: `cargo clippy --all-targets -- -D warnings`
- Format Check: `cargo fmt --check`
- Single Test Execution: `cargo test --test <test_name> -- <filter> --exact`

## Agent Operational Rules

- Context Verification First:
  - Parse existing traits, state structs, and RPC definitions using `cargo check` or `rg` before adding code.
  - Do NOT invent hypothetical traits, wrapper types, or custom interfaces when idiomatic standard library or existing crate types suffice.
- Architectural Isolation:
  - Keep consensus core logic (`src/raft/`) completely decoupled from network I/O.
  - Pass consensus events via asynchronous channels (`tokio::sync::mpsc`) rather than placing `tokio::net` calls directly inside state machine routines.