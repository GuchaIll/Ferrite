//! gRPC Raft service: the inbound half of the peer transport.
//!
//! The handler does no consensus work. It decodes the envelope and hands the
//! message to the node actor, which owns every mutation. Whatever answer the
//! core produces leaves as its own outbound message, so the `Ack` returned here
//! says only "received", never "handled".

use tonic::{Request, Response, Status};

use crate::{
    config::NodeId, proto::raft as pb, proto::raft::raft_service_server::RaftService,
    raft::RaftRpc, transport::convert,
};

/// Forwards decoded Raft messages into one node actor.
#[derive(Debug, Clone)]
pub struct RaftServiceImpl {
    inbound: tokio::sync::mpsc::Sender<(NodeId, RaftRpc)>,
}

impl RaftServiceImpl {
    /// Builds a service that feeds `inbound`.
    pub fn new(inbound: tokio::sync::mpsc::Sender<(NodeId, RaftRpc)>) -> Self {
        Self { inbound }
    }
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
    async fn deliver(
        &self,
        request: Request<pb::RaftMessage>,
    ) -> Result<Response<pb::Ack>, Status> {
        let message = request.into_inner();

        let Some((from, rpc)) = convert::from_wire(message) else {
            // A malformed envelope is the sender's bug. Saying so beats
            // defaulting it into a term-0 message the core would have to reason
            // about.
            return Err(Status::invalid_argument(
                "raft message is missing a sender or a payload",
            ));
        };

        // `send`, not `try_send`: waiting here backpressures this one peer's
        // outbound task, whose own queue is bounded and drops. Dropping inbound
        // messages instead would discard heartbeats under load and cause
        // spurious elections precisely when the cluster is busiest.
        self.inbound
            .send((from, rpc))
            .await
            .map_err(|_| Status::unavailable("node is shutting down"))?;

        Ok(Response::new(pb::Ack {}))
    }
}
