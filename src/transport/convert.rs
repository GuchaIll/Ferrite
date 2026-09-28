//! Wire conversion between [`RaftRpc`] and the generated protobuf envelope.
//!
//! Kept apart from both the transport and the service so exactly one place
//! decides what a Raft message looks like on the wire. Decoding is fallible —
//! an envelope with no payload, or a `from` of zero, is not a Raft message and
//! is rejected rather than defaulted into one.

use crate::{
    config::NodeId,
    proto::raft as pb,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, LogEntry, RaftRpc, RequestVoteRequest, RequestVoteResponse,
    },
};

/// Builds the wire envelope for one outbound RPC.
pub fn to_wire(from: NodeId, rpc: RaftRpc) -> pb::RaftMessage {
    use pb::raft_message::Payload;

    let payload = match rpc {
        RaftRpc::RequestVote(r) => Payload::RequestVote(pb::RequestVoteRequest {
            term: r.term,
            candidate_id: r.candidate_id,
            last_log_index: r.last_log_index,
            last_log_term: r.last_log_term,
        }),
        RaftRpc::RequestVoteResponse(r) => Payload::RequestVoteResponse(pb::RequestVoteResponse {
            term: r.term,
            vote_granted: r.vote_granted,
        }),
        RaftRpc::AppendEntries(r) => Payload::AppendEntries(pb::AppendEntriesRequest {
            term: r.term,
            leader_id: r.leader_id,
            prev_log_index: r.prev_log_index,
            prev_log_term: r.prev_log_term,
            entries: r.entries.into_iter().map(entry_to_wire).collect(),
            leader_commit: r.leader_commit,
        }),
        RaftRpc::AppendEntriesResponse(r) => {
            Payload::AppendEntriesResponse(pb::AppendEntriesResponse {
                term: r.term,
                success: r.success,
                match_index: r.match_index,
            })
        }
        RaftRpc::InstallSnapshot(r) => Payload::InstallSnapshot(pb::InstallSnapshotRequest {
            term: r.term,
            leader_id: r.leader_id,
            last_included_index: r.last_included_index,
            last_included_term: r.last_included_term,
            offset: r.offset,
            data: r.data,
            done: r.done,
        }),
        RaftRpc::InstallSnapshotResponse(r) => {
            Payload::InstallSnapshotResponse(pb::InstallSnapshotResponse { term: r.term })
        }
    };

    pb::RaftMessage {
        from,
        payload: Some(payload),
    }
}

/// Decodes a wire envelope, or `None` if it is not a well-formed Raft message.
pub fn from_wire(message: pb::RaftMessage) -> Option<(NodeId, RaftRpc)> {
    use pb::raft_message::Payload;

    // Node id zero is reserved, so it cannot be a legitimate sender and must not
    // be accepted as one: a reply addressed to it would go nowhere.
    if message.from == 0 {
        return None;
    }

    let rpc = match message.payload? {
        Payload::RequestVote(r) => RaftRpc::RequestVote(RequestVoteRequest {
            term: r.term,
            candidate_id: r.candidate_id,
            last_log_index: r.last_log_index,
            last_log_term: r.last_log_term,
        }),
        Payload::RequestVoteResponse(r) => RaftRpc::RequestVoteResponse(RequestVoteResponse {
            term: r.term,
            vote_granted: r.vote_granted,
        }),
        Payload::AppendEntries(r) => RaftRpc::AppendEntries(AppendEntriesRequest {
            term: r.term,
            leader_id: r.leader_id,
            prev_log_index: r.prev_log_index,
            prev_log_term: r.prev_log_term,
            entries: r.entries.into_iter().map(entry_from_wire).collect(),
            leader_commit: r.leader_commit,
        }),
        Payload::AppendEntriesResponse(r) => {
            RaftRpc::AppendEntriesResponse(AppendEntriesResponse {
                term: r.term,
                success: r.success,
                match_index: r.match_index,
            })
        }
        Payload::InstallSnapshot(r) => RaftRpc::InstallSnapshot(InstallSnapshotRequest {
            term: r.term,
            leader_id: r.leader_id,
            last_included_index: r.last_included_index,
            last_included_term: r.last_included_term,
            offset: r.offset,
            data: r.data,
            done: r.done,
        }),
        Payload::InstallSnapshotResponse(r) => {
            RaftRpc::InstallSnapshotResponse(InstallSnapshotResponse { term: r.term })
        }
    };

    Some((message.from, rpc))
}

fn entry_to_wire(entry: LogEntry) -> pb::LogEntry {
    pb::LogEntry {
        term: entry.term,
        index: entry.index,
        command: entry.command,
    }
}

fn entry_from_wire(entry: pb::LogEntry) -> LogEntry {
    LogEntry {
        index: entry.index,
        term: entry.term,
        command: entry.command,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every variant, including the responses, because a response that cannot
    /// round-trip silently stalls replication instead of failing loudly.
    fn all_variants() -> Vec<RaftRpc> {
        vec![
            RaftRpc::RequestVote(RequestVoteRequest {
                term: 7,
                candidate_id: 3,
                last_log_index: 11,
                last_log_term: 6,
            }),
            RaftRpc::RequestVoteResponse(RequestVoteResponse {
                term: 7,
                vote_granted: true,
            }),
            RaftRpc::AppendEntries(AppendEntriesRequest {
                term: 7,
                leader_id: 1,
                prev_log_index: 4,
                prev_log_term: 5,
                entries: vec![
                    LogEntry::new(5, 7, b"a".to_vec()),
                    LogEntry::new(6, 7, Vec::new()),
                ],
                leader_commit: 4,
            }),
            RaftRpc::AppendEntriesResponse(AppendEntriesResponse {
                term: 7,
                success: true,
                match_index: 6,
            }),
            RaftRpc::InstallSnapshot(InstallSnapshotRequest {
                term: 7,
                leader_id: 1,
                last_included_index: 9,
                last_included_term: 6,
                offset: 128,
                data: b"snapshot bytes".to_vec(),
                done: true,
            }),
            RaftRpc::InstallSnapshotResponse(InstallSnapshotResponse { term: 7 }),
        ]
    }

    #[test]
    fn every_rpc_round_trips_through_the_wire() {
        for rpc in all_variants() {
            let decoded = from_wire(to_wire(2, rpc.clone()));
            assert_eq!(decoded, Some((2, rpc)), "round trip lost information");
        }
    }

    #[test]
    fn match_index_survives_the_round_trip() {
        // It was missing from the proto until issue 04. A zero here turns every
        // rejection into a full nextIndex backoff walk.
        let rpc = RaftRpc::AppendEntriesResponse(AppendEntriesResponse {
            term: 3,
            success: false,
            match_index: 42,
        });
        let Some((_, RaftRpc::AppendEntriesResponse(res))) = from_wire(to_wire(2, rpc)) else {
            panic!("expected an AppendEntriesResponse back");
        };
        assert_eq!(res.match_index, 42);
    }

    #[test]
    fn envelope_without_payload_is_rejected() {
        let empty = pb::RaftMessage {
            from: 2,
            payload: None,
        };
        assert_eq!(from_wire(empty), None);
    }

    #[test]
    fn envelope_from_reserved_node_zero_is_rejected() {
        let mut message = to_wire(
            2,
            RaftRpc::InstallSnapshotResponse(InstallSnapshotResponse { term: 1 }),
        );
        message.from = 0;
        assert_eq!(from_wire(message), None);
    }
}
