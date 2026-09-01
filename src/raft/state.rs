//! Raft role state definitions.
pub enum RaftState {
    Follower,
    Candidate,
    Leader
}