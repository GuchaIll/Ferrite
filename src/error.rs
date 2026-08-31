//! Errors shared across crate module boundaries.

use thiserror::Error;

/// Errors produced by the Raft consensus module.
#[derive(Debug, Error)]
pub enum RaftError {
    #[error("raft error: {0}")]
    Internal(String),
}

/// Errors produced by the key-value state machine.
#[derive(Debug, Error)]
pub enum KvError {
    #[error("key-value error: {0}")]
    Internal(String),
}

/// Errors produced by Raft transports.
#[derive(Debug, Error)]
pub enum TransportError {
    #[error("transport error: {0}")]
    Internal(String),
}

/// Crate-level error, flattening errors from each module.
#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Raft(#[from] RaftError),

    #[error(transparent)]
    Kv(#[from] KvError),

    #[error(transparent)]
    Transport(#[from] TransportError),
}
