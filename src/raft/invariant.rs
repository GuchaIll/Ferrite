//! Runtime assertions for Raft safety properties.

use crate::raft::log::RaftLog;

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
    let last = a.last_index().min(b.last_index());
    for index in 1..=last {
        let (Ok(term_a), Ok(term_b)) = (a.term_at(index), b.term_at(index)) else {
            continue;
        };
        if term_a != term_b {
            continue;
        }
        // Matching index+term ⇒ every preceding entry is identical.
        for prefix in 1..=index {
            let ea = a.entry(prefix);
            let eb = b.entry(prefix);
            assert_eq!(
                ea.ok().map(|e| (e.index, e.term, e.command.as_slice())),
                eb.ok().map(|e| (e.index, e.term, e.command.as_slice())),
                "log matching violated at index {prefix} (witness index {index})"
            );
        }
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
}
