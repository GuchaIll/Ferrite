//! The driver contract: what a runtime feeds the Raft core, and what it owes
//! the outside world in return.
//!
//! [`RaftNode::step`] is the only way to advance consensus, and it is pure: it
//! mutates volatile state and *describes* the durable writes, messages, and
//! state-machine applies its caller must perform. Performing them is the
//! driver's job, and the two drivers — the deterministic simulator and the
//! runtime node — differ only in how they carry the work out, never in what
//! order they carry it out.
//!
//! These types live here rather than in `src/sim` because both drivers depend
//! on them. They are plain data: no clock, no I/O, no runtime.

use crate::{
    config::NodeId,
    raft::{
        HardState, LogEntry, RaftNode, RaftRpc, Snapshot, SnapshotMeta, election::ElectionAction,
    },
};

/// Input delivered by a driver to a Raft node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Input {
    /// One tick of the driver's clock has elapsed.
    ///
    /// The core counts ticks and never reads a clock, so a logical tick in the
    /// simulator and a heartbeat-interval tick in the runtime are the same
    /// input.
    Tick,
    /// An RPC received from another node.
    ///
    /// Responses arrive this way too: the core never blocks waiting for a
    /// reply, so a driver that speaks request/response converts each reply
    /// back into one of these.
    Message { from: NodeId, rpc: RaftRpc },
    /// A serialized command submitted by a client.
    ClientCommand(Vec<u8>),
    /// Several client commands delivered in one step (opportunistic batch).
    ///
    /// The leader appends all entries, emits one `PersistLog`, and one
    /// AppendEntries broadcast — so the batch shares a single storage sync.
    /// Issue 04's actor drains the proposal `mpsc` into this variant; an empty
    /// list is a no-op. Single commands still use [`Input::ClientCommand`].
    ClientCommands(Vec<Vec<u8>>),
    /// State-machine snapshot bytes produced after [`Output::RequestSnapshot`].
    SnapshotTaken(Snapshot),
    /// Driver finished draining [`Output::PersistSnapshot`]; log may now trim.
    SnapshotPersisted(SnapshotMeta),
}

/// Ordered work returned from a Raft node for its driver to drain.
///
/// Ordering contract:
/// - `Persist` (hard state) before a dependent `Send`
/// - `PersistLog` before a dependent `Send`: a follower's success reply, and
///   any AppendEntries carrying a leader's newly appended entry
/// - Durability boundary is the drained batch: every write output in a batch
///   is durable before the first `Send`, `Apply`, or `ApplySnapshot` after it
/// - One batch in flight per node: the driver finishes draining (and making
///   durable) a node's batch before stepping that node again. The leader
///   counts its own `last_index` toward a majority, which is safe only
///   because of this rule
/// - `PersistSnapshot` before the log prefix it replaces is discarded
/// - `PersistSnapshot` before a dependent InstallSnapshot reply `Send`
/// - `ApplySnapshot` installs state-machine bytes (including the dedup table)
///
/// Snapshot flow: `RequestSnapshot` → driver SM encode → `SnapshotTaken` →
/// `PersistSnapshot` → durable write → `SnapshotPersisted` → trim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output {
    /// Test-only evidence emitted by the simulator echo fixture.
    #[cfg(test)]
    Echo { payload: Vec<u8> },
    /// Deliver an RPC to `to`.
    Send { to: NodeId, rpc: RaftRpc },
    /// Apply a committed log entry to the application state machine.
    Apply(LogEntry),
    /// Make Raft election metadata durable.
    Persist(HardState),
    /// Make a log mutation durable: drop entries at or above `truncate_from`
    /// (if set), then append `entries`.
    PersistLog {
        truncate_from: Option<u64>,
        entries: Vec<LogEntry>,
    },
    /// Ask the driver to snapshot the state machine at this log boundary.
    RequestSnapshot {
        last_included_index: u64,
        last_included_term: u64,
    },
    /// Make a snapshot durable before the replaced log prefix is discarded.
    PersistSnapshot(Snapshot),
    /// Install snapshot bytes into the application state machine.
    ApplySnapshot(Snapshot),
    /// This node cannot accept the command it was just given; `leader_hint` is
    /// the node it believes is leader, if any.
    ///
    /// Redirection is application-level routing, not Raft: the core names a
    /// [`NodeId`], and resolving that to a client-facing address belongs to the
    /// driver. A driver with no clients to answer — the simulator — ignores it.
    Redirect { leader_hint: Option<NodeId> },
}

impl RaftNode {
    /// Advances consensus by one input and returns the work its driver owes.
    ///
    /// Outputs must be drained in the returned order; see [`Output`] for the
    /// durability rules that order encodes. Nothing here performs I/O.
    pub fn step(&mut self, input: Input) -> Vec<Output> {
        let actions = match input {
            Input::Tick => self.on_tick(),
            Input::Message { from, rpc } => self.handle_rpc(from, rpc),
            Input::ClientCommand(command) => self.handle_client_command(command),
            Input::ClientCommands(commands) => self.handle_client_commands(commands),
            Input::SnapshotTaken(snapshot) => self.handle_snapshot_taken(snapshot),
            Input::SnapshotPersisted(meta) => self.handle_snapshot_persisted(meta),
        };
        actions_to_outputs(actions)
    }
}

/// Translates core effects into ordered driver outputs, preserving order.
fn actions_to_outputs(actions: Vec<ElectionAction>) -> Vec<Output> {
    actions
        .into_iter()
        .filter_map(|action| match action {
            ElectionAction::Persist(hard_state) => Some(Output::Persist(hard_state)),
            ElectionAction::PersistLog {
                truncate_from,
                entries,
            } => Some(Output::PersistLog {
                truncate_from,
                entries,
            }),
            ElectionAction::Send { to, rpc } => Some(Output::Send { to, rpc }),
            // Role changes are already reflected in Raft core state; a driver
            // reads them back through the node rather than being told.
            ElectionAction::PromoteLeader | ElectionAction::DemoteFollower => None,
            ElectionAction::RedirectLeader { leader_hint } => {
                Some(Output::Redirect { leader_hint })
            }
            ElectionAction::ApplyCommittedEntries { entry } => Some(Output::Apply(entry)),
            ElectionAction::RequestSnapshot {
                last_included_index,
                last_included_term,
            } => Some(Output::RequestSnapshot {
                last_included_index,
                last_included_term,
            }),
            ElectionAction::PersistSnapshot(snapshot) => Some(Output::PersistSnapshot(snapshot)),
            ElectionAction::ApplySnapshot(snapshot) => Some(Output::ApplySnapshot(snapshot)),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{Input, Output};
    use crate::raft::RaftNode;

    #[test]
    fn client_command_to_a_follower_redirects_instead_of_appending() {
        let mut node = RaftNode::new(1, vec![2, 3]);

        let outs = node.step(Input::ClientCommand(b"w".to_vec()));

        // A follower must not append, and the driver needs the hint in order to
        // answer the client. Dropping this output is how a KV service ends up
        // silently timing out instead of redirecting.
        assert_eq!(outs, vec![Output::Redirect { leader_hint: None }]);
    }

    #[test]
    fn client_commands_batch_one_persist_log_then_sends() {
        use crate::raft::{LogEntry, RaftRpc, state::RaftState};

        let mut node = RaftNode::new(1, vec![2, 3]);
        node.state = RaftState::Leader;
        node.current_term = 1;
        node.leader_id = Some(1);
        for &p in &[2u64, 3] {
            node.next_index.insert(p, 1);
            node.match_index.insert(p, 0);
        }

        let outs = node.step(Input::ClientCommands(vec![b"a".to_vec(), b"b".to_vec()]));

        let persist = outs
            .iter()
            .find_map(|o| match o {
                Output::PersistLog {
                    truncate_from,
                    entries,
                } => Some((truncate_from, entries)),
                _ => None,
            })
            .expect("batch must emit PersistLog");
        assert_eq!(*persist.0, None);
        assert_eq!(
            persist.1,
            &vec![
                LogEntry::new(1, 1, b"a".to_vec()),
                LogEntry::new(2, 1, b"b".to_vec()),
            ]
        );
        assert_eq!(
            outs.iter()
                .filter(|o| matches!(o, Output::PersistLog { .. }))
                .count(),
            1
        );
        let ae_sends = outs
            .iter()
            .filter(|o| {
                matches!(
                    o,
                    Output::Send {
                        rpc: RaftRpc::AppendEntries(_),
                        ..
                    }
                )
            })
            .count();
        assert_eq!(ae_sends, 2);
        // PersistLog must precede every Send.
        let persist_pos = outs
            .iter()
            .position(|o| matches!(o, Output::PersistLog { .. }))
            .unwrap();
        let first_send = outs
            .iter()
            .position(|o| matches!(o, Output::Send { .. }))
            .unwrap();
        assert!(persist_pos < first_send);
    }
}
