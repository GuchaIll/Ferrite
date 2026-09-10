//! Leader-election scenario (issue #9 acceptance harness).

use crate::{
    config::NodeId,
    raft::{RaftNode, state::RaftState},
    sim::{Simulator, node_rng},
};

/// Outcome of one election scenario run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectionScenarioResult {
    pub seed: u64,
    pub ticks_run: u64,
    pub leader_id: Option<NodeId>,
    pub leader_term: Option<u64>,
    /// Nodes observed in [`RaftState::Leader`] at the end of the run.
    pub leaders: Vec<NodeId>,
}

/// Builds a three-node cluster with independent seeded RNGs and runs `ticks`.
///
/// Returns metadata describing how many leaders were elected and when.
pub fn run_election_scenario(seed: u64, ticks: u64) -> ElectionScenarioResult {
    let ids: [NodeId; 3] = [1, 2, 3];
    let nodes: Vec<RaftNode> = ids
        .iter()
        .map(|&id| {
            let peers: Vec<NodeId> = ids.iter().copied().filter(|&peer| peer != id).collect();
            RaftNode::with_rng(id, peers, node_rng(seed, id))
        })
        .collect();

    let mut simulator = Simulator::new(seed, nodes);
    simulator.run(ticks);

    let mut leaders = Vec::new();
    let mut leader_term = None;
    for (&id, node) in simulator.nodes() {
        if node.state() == RaftState::Leader {
            leaders.push(id);
            leader_term = Some(node.current_term());
        }
    }
    leaders.sort_unstable();

    ElectionScenarioResult {
        seed,
        ticks_run: ticks,
        leader_id: if leaders.len() == 1 {
            leaders.first().copied()
        } else {
            None
        },
        leader_term,
        leaders,
    }
}

/// Scans tick-by-tick until exactly one leader exists or `max_ticks` is hit.
pub fn elect_within(seed: u64, max_ticks: u64) -> ElectionScenarioResult {
    let ids: [NodeId; 3] = [1, 2, 3];
    let nodes: Vec<RaftNode> = ids
        .iter()
        .map(|&id| {
            let peers: Vec<NodeId> = ids.iter().copied().filter(|&peer| peer != id).collect();
            RaftNode::with_rng(id, peers, node_rng(seed, id))
        })
        .collect();

    let mut simulator = Simulator::new(seed, nodes);
    let mut leaders: Vec<NodeId> = Vec::new();
    let mut leader_term = None;
    let mut ticks_run = 0;

    for tick in 0..max_ticks {
        simulator.run(1);
        ticks_run = tick + 1;

        leaders.clear();
        leader_term = None;
        for (&id, node) in simulator.nodes() {
            if node.state() == RaftState::Leader {
                leaders.push(id);
                leader_term = Some(node.current_term());
            }
        }
        leaders.sort_unstable();
        if leaders.len() == 1 {
            break;
        }
    }

    ElectionScenarioResult {
        seed,
        ticks_run,
        leader_id: if leaders.len() == 1 {
            leaders.first().copied()
        } else {
            None
        },
        leader_term,
        leaders,
    }
}

#[cfg(test)]
mod tests {
    use super::{elect_within, run_election_scenario};
    use crate::raft::state::RaftState;
    use crate::sim::{Simulator, node_rng};
    use crate::raft::RaftNode;

    #[test]
    fn elects_exactly_one_leader_within_600_ticks_for_100_seeds() {
        for seed in 0..100 {
            let result = elect_within(seed, 600);
            assert_eq!(
                result.leaders.len(),
                1,
                "seed {seed}: expected one leader, got {:?} after {} ticks",
                result.leaders,
                result.ticks_run
            );
            assert!(
                result.ticks_run <= 600,
                "seed {seed}: election took {} ticks",
                result.ticks_run
            );
        }
    }

    #[test]
    #[ignore = "expensive issue #9 acceptance: 1000 seeds"]
    fn elects_exactly_one_leader_within_600_ticks_for_1000_seeds() {
        for seed in 0..1_000 {
            let result = elect_within(seed, 600);
            assert_eq!(
                result.leaders.len(),
                1,
                "seed {seed}: expected one leader, got {:?}",
                result.leaders
            );
        }
    }

    #[test]
    fn same_seed_is_deterministic_for_election() {
        let a = elect_within(7, 600);
        let b = elect_within(7, 600);
        assert_eq!(a, b);
    }

    #[test]
    fn leader_remains_alone_after_extra_ticks() {
        let result = elect_within(11, 600);
        assert_eq!(result.leaders.len(), 1);

        // Continue the same cluster far past election and ensure no second leader.
        let ids = [1_u64, 2, 3];
        let nodes: Vec<RaftNode> = ids
            .iter()
            .map(|&id| {
                let peers: Vec<u64> = ids.iter().copied().filter(|&peer| peer != id).collect();
                RaftNode::with_rng(id, peers, node_rng(11, id))
            })
            .collect();
        let mut sim = Simulator::new(11, nodes);
        sim.run(result.ticks_run + 200);

        let leaders: Vec<_> = sim
            .nodes()
            .iter()
            .filter(|(_, n)| n.state() == RaftState::Leader)
            .map(|(&id, _)| id)
            .collect();
        assert_eq!(leaders.len(), 1, "expected stable single leader, got {leaders:?}");
    }

    #[test]
    fn run_election_scenario_reports_leaders() {
        let result = run_election_scenario(3, 600);
        assert_eq!(result.seed, 3);
        assert!(result.leaders.len() <= 1);
    }
}
