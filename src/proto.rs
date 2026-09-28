//! Generated protobuf and gRPC types.
//!
//! One home for the generated code, because both sides of the wire need it: the
//! transport builds outbound messages and the service decodes inbound ones. A
//! second `include_proto!` elsewhere would compile a second, incompatible set of
//! types with the same names.

/// Types generated from `proto/raft.proto`.
#[allow(
    clippy::result_large_err,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::doc_overindented_list_items
)]
pub mod raft {
    tonic::include_proto!("raft");
}
