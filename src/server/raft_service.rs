//! gRPC Raft service implementation.
use tonic::{Request, Response, Status};

#[allow(clippy::result_large_err, clippy::unwrap_used, clippy::expect_used)]
pub mod raft_proto {
    tonic::include_proto!("raft");
}

use raft_proto::raft_service_server::RaftService;
use raft_proto::*;
#[derive(Default, Debug)]
pub struct RaftServiceImpl;

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
    async fn request_vote(
        &self,
        _request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        Ok(Response::new(RequestVoteResponse::default()))
    }

    async fn append_entries(
        &self,
        _request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        Ok(Response::new(AppendEntriesResponse::default()))
    }

    async fn snapshot(
        &self,
        _request: Request<SnapshotRequest>,
    ) -> Result<Response<SnapshotResponse>, Status> {
        Ok(Response::new(SnapshotResponse::default()))
    }
}
