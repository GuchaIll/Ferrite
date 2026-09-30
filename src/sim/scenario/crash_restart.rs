//! Crash/restart scenario (issue 03 acceptance harness).
//!
//! Models drained-only durability: a crash keeps only writes the driver already
//! committed to per-node storage. Vote-once and log matching must hold across
//! random crash schedules.

use std::collections::BTreeMap;

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::{
    config::NodeId,
    raft::{RaftNode, invariant::assert_log_matching, state::RaftState},
    sim::{Input, Simulator, node_rng, scenario::election::elect_within},
};

/// Outcome of one crash/restart scenario run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrashRestartResult {
    pub seed: u64,
    pub leader_id: Option<NodeId>,
    pub writes: u64,
    pub crashes: u64,
    pub logs_identical: bool,
    /// True when no node granted two different votes in the same term across restarts.
    pub vote_once: bool,
}

/// Five-node cluster: elect, write, crash/restart up to two followers at a time.
///
/// Crash targets are chosen from a dedicated ChaCha stream so the schedule is
/// reproducible from `seed` alone. Crashed nodes are restarted after a short
/// pause so the remaining majority can keep committing.
pub fn run_crash_restart_scenario(seed: u64, writes: u64) -> CrashRestartResult {
    let election = elect_within_n(seed, 5, 800);
    assert_eq!(
        election.leaders.len(),
        1,
        "seed {seed}: election failed before crash_restart: {:?}",
        election.leaders
    );
    let Some(initial_leader) = election.leader_id else {
        return CrashRestartResult {
            seed,
            leader_id: None,
            writes,
            crashes: 0,
            logs_identical: false,
            vote_once: true,
        };
    };

    let ids: [NodeId; 5] = [1, 2, 3, 4, 5];
    let nodes: Vec<RaftNode> = ids
        .iter()
        .map(|&id| {
            let peers: Vec<NodeId> = ids.iter().copied().filter(|&p| p != id).collect();
            RaftNode::with_rng(id, peers, node_rng(seed, id))
        })
        .collect();

    let mut sim = Simulator::new(seed, nodes);
    sim.run(election.ticks_run);

    let mut leader_id = sim
        .nodes()
        .iter()
        .find(|(_, n)| n.state() == RaftState::Leader)
        .map(|(&id, _)| id)
        .unwrap_or(initial_leader);

    let mut crash_rng = ChaCha8Rng::seed_from_u64(seed ^ 0xc2a5_17a5_c2a5_17a5);
    let mut crashes = 0u64;
    let mut vote_once = true;
    // (term, voter) → granted candidate; detects double votes across restarts.
    let mut votes: BTreeMap<(u64, NodeId), NodeId> = BTreeMap::new();

    for i in 0..writes {
        // Re-resolve leader if the previous one crashed or stepped down.
        if !sim.nodes().contains_key(&leader_id)
            || sim.nodes()[&leader_id].state() != RaftState::Leader
        {
            if let Some((&id, _)) = sim
                .nodes()
                .iter()
                .find(|(_, n)| n.state() == RaftState::Leader)
            {
                leader_id = id;
            } else {
                sim.run(300);
                if let Some((&id, _)) = sim
                    .nodes()
                    .iter()
                    .find(|(_, n)| n.state() == RaftState::Leader)
                {
                    leader_id = id;
                } else {
                    break;
                }
            }
        }

        let cmd = format!("cr{i}").into_bytes();
        sim.step_node(leader_id, Input::ClientCommand(cmd));
        sim.run(8);

        // Crash up to two non-leader live followers.
        let live_followers: Vec<NodeId> = sim
            .nodes()
            .keys()
            .copied()
            .filter(|&id| id != leader_id)
            .collect();
        let max_crash = 2.min(live_followers.len());
        let n_crash = if max_crash == 0 {
            0
        } else {
            crash_rng.gen_range(0..=max_crash)
        };
        if n_crash > 0 {
            let mut pick = live_followers;
            for j in 0..n_crash {
                let k = crash_rng.gen_range(j..pick.len());
                pick.swap(j, k);
            }
            for &id in pick.iter().take(n_crash) {
                record_votes(&sim, &ids, &mut votes, &mut vote_once);
                sim.crash_node(id);
                crashes += 1;
            }
            sim.run(15);
            for &id in pick.iter().take(n_crash) {
                if !sim.restart_node(id) {
                    return CrashRestartResult {
                        seed,
                        leader_id: Some(leader_id),
                        writes,
                        crashes,
                        logs_identical: false,
                        vote_once: false,
                    };
                }
            }
            sim.run(40);
        }

        record_votes(&sim, &ids, &mut votes, &mut vote_once);

        let live_logs: Vec<_> = sim.nodes().values().map(|n| n.log()).collect();
        if live_logs.len() >= 2 {
            assert_log_matching(&live_logs);
        }
    }

    // Restart anyone still down and converge.
    let down: Vec<NodeId> = ids
        .into_iter()
        .filter(|id| !sim.nodes().contains_key(id))
        .collect();
    for id in down {
        let _ = sim.restart_node(id);
    }
    sim.run(500);

    record_votes(&sim, &ids, &mut votes, &mut vote_once);

    let logs: Vec<_> = sim.nodes().values().map(|n| n.log().clone()).collect();
    let log_refs: Vec<_> = logs.iter().collect();
    if log_refs.len() >= 2 {
        assert_log_matching(&log_refs);
    }

    let logs_identical = logs.first().is_some_and(|first| {
        logs.iter().all(|log| {
            log.last_index() == first.last_index()
                && (1..=first.last_index()).all(|idx| log.entry(idx) == first.entry(idx))
        })
    });

    CrashRestartResult {
        seed,
        leader_id: Some(leader_id),
        writes,
        crashes,
        logs_identical,
        vote_once,
    }
}

fn elect_within_n(
    seed: u64,
    n: usize,
    max_ticks: u64,
) -> crate::sim::scenario::ElectionScenarioResult {
    if n == 3 {
        return elect_within(seed, max_ticks);
    }
    let ids: Vec<NodeId> = (1..=n as NodeId).collect();
    let nodes: Vec<RaftNode> = ids
        .iter()
        .map(|&id| {
            let peers: Vec<NodeId> = ids.iter().copied().filter(|&p| p != id).collect();
            RaftNode::with_rng(id, peers, node_rng(seed, id))
        })
        .collect();
    let mut simulator = Simulator::new(seed, nodes);
    let mut leaders = Vec::new();
    let mut leader_term = None;
    let mut ticks_run = 0u64;
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
    crate::sim::scenario::ElectionScenarioResult {
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

fn record_votes(
    sim: &Simulator<RaftNode>,
    ids: &[NodeId],
    votes: &mut BTreeMap<(u64, NodeId), NodeId>,
    vote_once: &mut bool,
) {
    for &id in ids {
        let Some(recovered) = sim.recovered(id) else {
            continue;
        };
        let term = recovered.hard_state.current_term;
        let Some(voted_for) = recovered.hard_state.voted_for else {
            continue;
        };
        match votes.insert((term, id), voted_for) {
            Some(prev) if prev != voted_for => *vote_once = false,
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::run_crash_restart_scenario;

    #[test]
    fn crash_restart_converges_for_20_seeds() {
        for seed in 0..20 {
            let result = run_crash_restart_scenario(seed, 6);
            assert!(
                result.vote_once,
                "seed {seed}: vote-once violated across restarts"
            );
            assert!(
                result.logs_identical,
                "seed {seed}: logs diverged after crash/restart (crashes={})",
                result.crashes
            );
        }
    }

    #[test]
    #[ignore = "expensive issue 03 acceptance: 1000 seeds"]
    fn crash_restart_converges_for_1000_seeds() {
        for seed in 0..1_000 {
            let result = run_crash_restart_scenario(seed, 10);
            assert!(
                result.vote_once,
                "seed {seed}: vote-once violated across restarts"
            );
            assert!(
                result.logs_identical,
                "seed {seed}: logs diverged after crashes={}",
                result.crashes
            );
        }
    }

    #[test]
    fn same_seed_is_deterministic_for_crash_restart() {
        let a = run_crash_restart_scenario(19, 5);
        let b = run_crash_restart_scenario(19, 5);
        assert_eq!(a, b);
    }
}
