//! Raft role state definitions.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftState {
    Follower,
    Candidate,
    Leader,
}
