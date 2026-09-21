//! Runtime assertions for Raft safety properties.

use std::collections::BTreeMap;

use crate::{
    config::NodeId,
    raft::{RaftNode, log::RaftLog, state::RaftState},
};

/// Log Matching (§5.3): equal (index, term) implies identical prefixes.
///
/// Checks every pair of logs pairwise through their common length.
pub fn assert_log_matching(logs: &[&RaftLog]) {
    for (i, a) in logs.iter().enumerate() {
        for b in logs.iter().skip(i + 1) {
            assert_log_matching_pair(a, b);
        }
    }
}

fn assert_log_matching_pair(a: &RaftLog, b: &RaftLog) {
    // The highest index whose terms agree witnesses the longest prefix that
    // must match; every lower witness is covered by it, so one scan suffices.
    let last = a.last_index().min(b.last_index());
    let Some(witness) = (1..=last)
        .rev()
        .find(|&index| matches!((a.term_at(index), b.term_at(index)), (Ok(x), Ok(y)) if x == y))
    else {
        return;
    };
    for prefix in 1..=witness {
        let (Ok(ea), Ok(eb)) = (a.entry(prefix), b.entry(prefix)) else {
            continue;
        };
        assert_eq!(
            (ea.index, ea.term, ea.command.as_slice()),
            (eb.index, eb.term, eb.command.as_slice()),
            "log matching violated at index {prefix} (witness index {witness})"
        );
    }
}

/// Cluster-wide safety checker that remembers history across observations.
///
/// Election Safety, Term Monotonicity, and Vote Once are properties of a
/// node's history, not of a single snapshot, so the checker records the
/// leader of each term, each node's last term, and each node's vote per term.
#[derive(Debug, Default)]
pub struct InvariantChecker {
    leader_by_term: BTreeMap<u64, NodeId>,
    last_term: BTreeMap<NodeId, u64>,
    vote_by_term: BTreeMap<(NodeId, u64), NodeId>,
}

impl InvariantChecker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Asserts every safety invariant over the current state of `nodes`.
    pub fn observe<'a>(&mut self, nodes: impl IntoIterator<Item = &'a RaftNode>) {
        let nodes: Vec<&RaftNode> = nodes.into_iter().collect();
        for node in &nodes {
            self.observe_node(node);
        }
        let logs: Vec<&RaftLog> = nodes.iter().map(|node| &node.log).collect();
        assert_log_matching(&logs);
    }

    fn observe_node(&mut self, node: &RaftNode) {
        let (id, term) = (node.id, node.current_term);

        let previous = self.last_term.insert(id, term).unwrap_or(0);
        assert!(
            term >= previous,
            "term monotonicity violated: node {id} went from term {previous} to {term}"
        );

        if node.state == RaftState::Leader {
            let leader = *self.leader_by_term.entry(term).or_insert(id);
            assert_eq!(
                leader, id,
                "election safety violated: nodes {leader} and {id} both led term {term}"
            );
        }

        if let Some(candidate) = node.voted_for {
            let vote = *self.vote_by_term.entry((id, term)).or_insert(candidate);
            assert_eq!(
                vote, candidate,
                "vote once violated: node {id} voted for {vote} and then {candidate} in term {term}"
            );
        }

        let last_index = node.log.last_index();
        assert!(
            node.commit_index <= last_index,
            "commit index {} exceeds last log index {last_index} on node {id}",
            node.commit_index
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::LogEntry;

    #[test]
    fn matching_index_and_term_implies_equal_prefix() {
        let mut a = RaftLog::new();
        let mut b = RaftLog::new();
        for e in [
            LogEntry::new(1, 1, b"x".to_vec()),
            LogEntry::new(2, 1, b"y".to_vec()),
        ] {
            a.append(e.clone()).unwrap();
            b.append(e).unwrap();
        }
        assert_log_matching(&[&a, &b]);
    }

    #[test]
    #[should_panic(expected = "log matching violated at index 1")]
    fn matching_witness_with_divergent_prefix_is_rejected() {
        let mut a = RaftLog::new();
        let mut b = RaftLog::new();
        a.append(LogEntry::new(1, 1, b"x".to_vec())).unwrap();
        b.append(LogEntry::new(1, 1, b"other".to_vec())).unwrap();
        for log in [&mut a, &mut b] {
            log.append(LogEntry::new(2, 2, b"y".to_vec())).unwrap();
        }
        assert_log_matching(&[&a, &b]);
    }

    fn node(id: NodeId, term: u64) -> RaftNode {
        let mut node = RaftNode::new(id, vec![1, 2, 3].into_iter().filter(|&p| p != id).collect());
        node.current_term = term;
        node
    }

    #[test]
    fn healthy_history_passes_every_check() {
        let mut checker = InvariantChecker::new();
        let mut leader = node(1, 1);
        leader.voted_for = Some(1);
        leader.state = RaftState::Leader;
        let mut follower = node(2, 1);
        follower.voted_for = Some(1);
        checker.observe([&leader, &follower]);

        // A later term may have a different leader and different votes.
        leader.state = RaftState::Follower;
        leader.current_term = 2;
        leader.voted_for = Some(2);
        follower.current_term = 2;
        follower.voted_for = Some(2);
        follower.state = RaftState::Leader;
        checker.observe([&leader, &follower]);
    }

    #[test]
    #[should_panic(expected = "election safety violated")]
    fn two_leaders_in_one_term_are_rejected() {
        let mut checker = InvariantChecker::new();
        let mut first = node(1, 3);
        first.state = RaftState::Leader;
        checker.observe([&first]);

        // Node 1 steps down, but node 2 must still not lead the same term.
        first.state = RaftState::Follower;
        let mut second = node(2, 3);
        second.state = RaftState::Leader;
        checker.observe([&first, &second]);
    }

    #[test]
    #[should_panic(expected = "term monotonicity violated: node 1 went from term 4 to 3")]
    fn decreasing_term_is_rejected() {
        let mut checker = InvariantChecker::new();
        let mut n = node(1, 4);
        checker.observe([&n]);
        n.current_term = 3;
        checker.observe([&n]);
    }

    #[test]
    #[should_panic(expected = "vote once violated: node 1 voted for 2 and then 3 in term 1")]
    fn second_vote_in_one_term_is_rejected() {
        let mut checker = InvariantChecker::new();
        let mut n = node(1, 1);
        n.voted_for = Some(2);
        checker.observe([&n]);
        n.voted_for = Some(3);
        checker.observe([&n]);
    }

    #[test]
    #[should_panic(expected = "commit index 1 exceeds last log index 0 on node 1")]
    fn commit_index_past_log_end_is_rejected() {
        let mut n = node(1, 1);
        n.commit_index = 1;
        InvariantChecker::new().observe([&n]);
    }
}
