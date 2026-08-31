//! Raft-backed distributed key-value store.
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]
#![cfg_attr(test, allow(clippy::unnecessary_literal_unwrap))]

pub mod error;
pub mod kv;
pub mod server;
pub mod sim;
