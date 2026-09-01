//! Raft transport abstraction.

pub mod grpc;
pub mod simulated;


pub struct ApppenEntriesRequest {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<raft::log::LogEntry>,
    pub leader_commit: u64
}


pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
}


pub trait Transport {
    async fn append_entries(
        &self,
        to: NodeId,
        request: ApppenEntriesRequest
    ) -> Result<AppendEntriesResponse, failure::Error>;
    

}


