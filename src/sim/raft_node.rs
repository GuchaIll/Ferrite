//! Deterministic-simulation adapter for the Raft state machine.

use crate::{
    config::NodeId, raft::{RaftNode, election::ElectionAction},
};


use super::{Input, Output, SimNode};

/// Lets the simulation driver deliver its orchestration inputs to a Raft node.
///
/// This adapter keeps simulation-specific `Input` and `Output` types out of
/// the Raft core. As protocol handling is implemented, it will translate Raft
/// effects into ordered driver outputs.
impl SimNode for RaftNode {
    fn id(&self) -> NodeId {
        RaftNode::id(self)
    }

    fn step(&mut self, input: Input) -> Vec<Output> {
        let actions = match input {
            Input::Tick => self.on_tick(),
            Input::Message { from, rpc } => self.handle_rpc(from, rpc),
            Input::ClientCommand(command) => {
                self.handle_client_command(command);
                Vec::new()
            }
        };
        actions_to_outputs(actions)
    }
}

//Core effects -> driver outputs
fn actions_to_outputs(actions: Vec<ElectionAction>) -> Vec<Output> {
    actions
        .into_iter()
        .filter_map(|action| match action {
            ElectionAction::Persist(hard_state) => Some(Output::Persist(hard_state)),
            ElectionAction::Send { to, rpc } => Some(Output::Send { to, rpc }),
            //State already mutated in Raft core
            ElectionAction::PromoteLeader => None,    
            ElectionAction::DemoteFollower => None
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{RaftRpc, RequestVoteRequest, RequestVoteResponse, HardState};

    #[test]
    fn tick_with_expired_timeout_persists_and_sends_request_votes() {
        let mut node = RaftNode::new(1, vec![2, 3]);
        // DEFAULT_ELECTION_TIMEOUT_TICKS is 500: the 500th tick expires the timer
        // and starts the election. Capture that tick's outputs (do not discard them).
        let mut outs = Vec::new();
        for _ in 0..500 {
            outs = node.step(Input::Tick);
        }
        assert!(
            outs.iter().any(|o| matches!(o, Output::Persist(_))),
            "expected Persist hard-state on election start, got {outs:?}"
        );
        assert!(
            outs.iter().any(|o| matches!(o, Output::Send { .. })),
            "expected RequestVote Send on election start, got {outs:?}"
        );
    }

    #[test]
    fn request_vote_grants_when_log_empty_and_no_prior_vote() {
        let mut node = RaftNode::new(1, vec![2, 3]);
        let outs = node.step(Input::Message {
            from: 2,
            rpc: RaftRpc::RequestVote(RequestVoteRequest {
                term: 1,
                candidate_id: 2,
                last_log_index: 0,
                last_log_term: 0,
            }),
        });

        // Persist before reply (term/vote), then Send response.
        assert!(matches!(outs.first(), Some(Output::Persist(HardState { current_term: 1, voted_for: Some(2) }))));
        assert!(matches!(
            outs.get(1),
            Some(Output::Send {
                to: 2,
                rpc: RaftRpc::RequestVoteResponse(RequestVoteResponse {
                    term: 1,
                    vote_granted: true,
                }),
            })
        ));
    }

    #[test]
    fn request_vote_denies_stale_term() {
        let mut node = RaftNode::new(1, vec![2, 3]);
        // Bump term via a first successful vote.
        let _ = node.step(Input::Message {
            from: 2,
            rpc: RaftRpc::RequestVote(RequestVoteRequest {
                term: 5,
                candidate_id: 2,
                last_log_index: 0,
                last_log_term: 0,
            }),
        });
        let outs = node.step(Input::Message {
            from: 3,
            rpc: RaftRpc::RequestVote(RequestVoteRequest {
                term: 4,
                candidate_id: 3,
                last_log_index: 0,
                last_log_term: 0,
            }),
        });
        assert!(matches!(
            outs.last(),
            Some(Output::Send {
                rpc: RaftRpc::RequestVoteResponse(RequestVoteResponse {
                    term: 5,
                    vote_granted: false,
                    ..
                }),
                ..
            })
        ));
    }
}