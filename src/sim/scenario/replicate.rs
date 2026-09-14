//! Log replication scenario (issue #10 acceptance harness).

use crate::{
    config::NodeId,
    raft::{RaftNode, invariant::assert_log_matching, state::RaftState},
    sim::{Input, Simulator, node_rng, scenario::election::elect_within},
};

/// Outcome of one replicate scenario run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicateScenarioResult {
    pub seed: u64,
    pub leader_id: Option<NodeId>,
    pub writes: u64,
    pub log_len: u64,
    pub logs_identical: bool,
}

/// Elects a leader, drives `writes` client commands, and checks log equality.
pub fn run_replicate_scenario(seed: u64, writes: u64) -> ReplicateScenarioResult {
    let election = elect_within(seed, 600);
    assert_eq!(
        election.leaders.len(),
        1,
        "seed {seed}: election failed before replicate: {:?}",
        election.leaders
    );
    let Some(leader_id) = election.leader_id else {
        return ReplicateScenarioResult {
            seed,
            leader_id: None,
            writes,
            log_len: 0,
            logs_identical: false,
        };
    };

    let ids: [NodeId; 3] = [1, 2, 3];
    let nodes: Vec<RaftNode> = ids
        .iter()
        .map(|&id| {
            let peers: Vec<NodeId> = ids.iter().copied().filter(|&peer| peer != id).collect();
            RaftNode::with_rng(id, peers, node_rng(seed, id))
        })
        .collect();

    let mut sim = Simulator::new(seed, nodes);
    // Reach the same elected state.
    sim.run(election.ticks_run);

    let leader_id = sim
        .nodes()
        .iter()
        .find(|(_, n)| n.state() == RaftState::Leader)
        .map(|(&id, _)| id)
        .unwrap_or(leader_id);

    for i in 0..writes {
        let cmd = format!("w{i}").into_bytes();
        sim.step_node(leader_id, Input::ClientCommand(cmd));
        // Allow heartbeats / retries to converge after each write batch.
        sim.run(5);

        let logs: Vec<_> = sim.nodes().values().map(|n| n.log()).collect();
        assert_log_matching(&logs);
    }

    // Extra ticks for any remaining AE retries.
    sim.run(200);

    let logs: Vec<_> = sim.nodes().values().map(|n| n.log().clone()).collect();
    let log_refs: Vec<_> = logs.iter().collect();
    assert_log_matching(&log_refs);

    let first = &logs[0];
    let logs_identical = logs.iter().all(|log| {
        log.last_index() == first.last_index()
            && (1..=first.last_index()).all(|idx| log.entry(idx) == first.entry(idx))
    });

    ReplicateScenarioResult {
        seed,
        leader_id: Some(leader_id),
        writes,
        log_len: first.last_index(),
        logs_identical,
    }
}

#[cfg(test)]
mod tests {
    use super::run_replicate_scenario;

    #[test]
    fn replicate_100_writes_identical_logs_for_20_seeds() {
        for seed in 0..20 {
            let result = run_replicate_scenario(seed, 100);
            assert!(
                result.logs_identical,
                "seed {seed}: logs diverged (len={})",
                result.log_len
            );
            assert_eq!(result.log_len, 100, "seed {seed}: expected 100 entries");
        }
    }

    #[test]
    #[ignore = "expensive issue #10 acceptance: 1000 seeds × 100 writes"]
    fn replicate_100_writes_identical_logs_for_1000_seeds() {
        for seed in 0..1_000 {
            let result = run_replicate_scenario(seed, 100);
            assert!(
                result.logs_identical,
                "seed {seed}: logs diverged (len={})",
                result.log_len
            );
            assert_eq!(result.log_len, 100, "seed {seed}: expected 100 entries");
        }
    }

    #[test]
    fn same_seed_is_deterministic_for_replicate() {
        let a = run_replicate_scenario(7, 10);
        let b = run_replicate_scenario(7, 10);
        assert_eq!(a, b);
    }
}
