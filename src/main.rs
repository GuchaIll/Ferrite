#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]
#![cfg_attr(test, allow(clippy::unnecessary_literal_unwrap))]

use ferrite::server::raft_service::RaftServiceImpl;
use ferrite::server::raft_service::raft_proto::raft_service_server::RaftServiceServer;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let raft_service = RaftServiceImpl;
    Server::builder()
        .add_service(RaftServiceServer::new(raft_service))
        .serve(addr)
        .await?;
    Ok(())
}
