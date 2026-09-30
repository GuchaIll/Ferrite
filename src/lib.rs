//! Raft-backed distributed key-value store.
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]
#![cfg_attr(test, allow(clippy::unnecessary_literal_unwrap))]

pub mod cli;
pub mod client;
pub mod config;
pub mod error;
pub mod kv;
pub mod proto;
pub mod raft;
pub mod server;
pub mod sim;
pub mod transport;
