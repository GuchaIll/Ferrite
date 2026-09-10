//! Single-threaded deterministic simulator harness.
//!
//! A global step enters logical tick `t`, steps nodes in ascending [`NodeId`]
//! order, drains each node's outputs in returned order, delivers queued
//! messages to completion within the same tick, records `TickEnd`, and then
//! advances the clock to `t + 1`. Nodes never receive the clock.

use std::collections::BTreeMap;

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

use self::{
    clock::{Clock, SimClock},
    network::Network,
    trace::{Trace, TraceEvent, TraceEventKind, TracePayload},
};

pub mod clock;
pub mod io;
pub mod network;
pub mod raft_node;
pub mod scenario;
#[cfg(test)]
pub mod test_node;
pub mod trace;

use crate::config::NodeId;
pub use io::{Input, Output};

/// Synchronous simulated node driven by [`Simulator`].
pub trait SimNode {
    /// Returns this node's non-zero stable identifier.
    fn id(&self) -> NodeId;

    /// Handles one driver input and returns ordered outputs to be drained.
    fn step(&mut self, input: Input) -> Vec<Output>;
}

/// Derives a stable, independent ChaCha8 stream for one node.
///
/// The 32-byte seed consists of four little-endian [`splitmix64`] values over
/// the root seed, node ID, and fixed domain constants. This exact derivation is
/// a replay protocol: changing it changes the result for recorded seeds.
pub fn node_rng(root_seed: u64, node_id: NodeId) -> ChaCha8Rng {
    assert_ne!(node_id, 0, "node ID zero is reserved");

    let inputs = [
        root_seed ^ node_id.rotate_left(17),
        root_seed.rotate_left(23) ^ node_id,
        root_seed ^ node_id.rotate_right(11) ^ 0x9e37_79b9_7f4a_7c15,
        root_seed.rotate_right(29) ^ node_id ^ 0xd1b5_4a32_d192_ed03,
    ];
    let mut seed = [0_u8; 32];

    for (chunk, input) in seed.chunks_exact_mut(8).zip(inputs) {
        chunk.copy_from_slice(&splitmix64(input).to_le_bytes());
    }

    ChaCha8Rng::from_seed(seed)
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// Driver-owned deterministic simulation state.
pub struct Simulator<N: SimNode> {
    clock: SimClock,
    root_seed: u64,
    nodes: BTreeMap<NodeId, N>,
    network: Network,
    /// Durable hard-state last observed per node (sim-side bookkeeping).
    persisted: BTreeMap<NodeId, crate::raft::HardState>,
    trace: Trace,
}

impl<N: SimNode> Simulator<N> {
    /// Registers nodes and records simulation metadata at logical tick zero.
    ///
    /// Node IDs must be non-zero and unique. `BTreeMap` fixes execution and
    /// metadata order regardless of registration order.
    pub fn new(seed: u64, nodes: Vec<N>) -> Self {
        let mut registered = BTreeMap::new();

        for node in nodes {
            let node_id = node.id();
            assert_ne!(node_id, 0, "node ID zero is reserved");
            assert!(
                registered.insert(node_id, node).is_none(),
                "duplicate node ID {node_id}"
            );
        }

        let node_ids = registered.keys().copied().collect();
        let mut trace = Trace::new();
        trace.record(TraceEvent::new(
            0,
            TraceEventKind::SimStart,
            None,
            Some(TracePayload::SimulationStart {
                seed,
                nodes: node_ids,
            }),
        ));

        Self {
            clock: SimClock::new(),
            root_seed: seed,
            nodes: registered,
            network: Network::new(),
            persisted: BTreeMap::new(),
            trace,
        }
    }

    /// Runs `ticks` global simulation steps and returns the cumulative trace.
    pub fn run(&mut self, ticks: u64) -> &Trace {
        for _ in 0..ticks {
            let tick = self.clock.now();
            self.trace
                .record(TraceEvent::new(tick, TraceEventKind::TickStart, None, None));

            // Phase 1: step every node on Tick, draining outputs immediately.
            // Collect node IDs first so we can mutably re-borrow per node while
            // delivering cascading message effects.
            let node_ids: Vec<NodeId> = self.nodes.keys().copied().collect();
            for node_id in node_ids {
                self.trace.record(TraceEvent::new(
                    tick,
                    TraceEventKind::NodeStep,
                    Some(node_id),
                    None,
                ));

                let Some(node) = self.nodes.get_mut(&node_id) else {
                    continue;
                };
                let outputs = node.step(Input::Tick);
                self.drain_outputs(tick, node_id, outputs);
            }

            // Phase 2: deliver all queued messages (and any cascading replies)
            // within the same logical tick, FIFO.
            self.deliver_pending(tick);

            self.trace
                .record(TraceEvent::new(tick, TraceEventKind::TickEnd, None, None));
            self.clock.advance(1);
        }

        &self.trace
    }

    /// Drains ordered outputs from `from`, enqueuing sends onto the network.
    fn drain_outputs(&mut self, tick: u64, from: NodeId, outputs: Vec<Output>) {
        for output in outputs {
            match output {
                #[cfg(test)]
                Output::Echo { payload } => self.trace.record(TraceEvent::new(
                    tick,
                    TraceEventKind::Echo,
                    Some(from),
                    Some(TracePayload::Bytes(payload)),
                )),
                Output::Persist(hard_state) => {
                    self.persisted.insert(from, hard_state);
                    // Persistence is durable bookkeeping; no trace kind yet beyond
                    // future extension. Effects ordering is still enforced by
                    // draining Persist before subsequent Sends in this list.
                }
                Output::Apply(_entry) => {
                    // Application is out of scope for election delivery.
                }
                Output::Send { to, rpc } => {
                    self.trace.record(TraceEvent::new(
                        tick,
                        TraceEventKind::Send,
                        Some(from),
                        None,
                    ));
                    self.network.enqueue(from, to, rpc);
                }
            }
        }
    }

    /// Delivers every queued message, draining any effects produced by delivery.
    fn deliver_pending(&mut self, tick: u64) {
        // Bound cascades to avoid infinite loops on buggy handlers.
        let mut steps = 0_u64;
        const MAX_DELIVERIES_PER_TICK: u64 = 100_000;

        while let Some(msg) = self.network.pop_front() {
            steps = steps.saturating_add(1);
            assert!(
                steps <= MAX_DELIVERIES_PER_TICK,
                "message cascade exceeded {MAX_DELIVERIES_PER_TICK} deliveries in one tick"
            );

            if !self.nodes.contains_key(&msg.to) {
                self.trace.record(TraceEvent::new(
                    tick,
                    TraceEventKind::Drop,
                    Some(msg.from),
                    None,
                ));
                continue;
            }

            self.trace.record(TraceEvent::new(
                tick,
                TraceEventKind::Deliver,
                Some(msg.to),
                None,
            ));

            let Some(node) = self.nodes.get_mut(&msg.to) else {
                continue;
            };
            let outputs = node.step(Input::Message {
                from: msg.from,
                rpc: msg.rpc,
            });
            self.drain_outputs(tick, msg.to, outputs);
        }
    }

    /// Returns the cumulative simulation trace.
    pub fn trace(&self) -> &Trace {
        &self.trace
    }

    /// Returns the root seed supplied at construction.
    pub fn seed(&self) -> u64 {
        self.root_seed
    }

    /// Returns the driver-owned logical clock.
    pub fn clock(&self) -> &SimClock {
        &self.clock
    }

    /// Returns registered nodes in ascending ID order.
    pub fn nodes(&self) -> &BTreeMap<NodeId, N> {
        &self.nodes
    }

    /// Returns last-persisted hard state per node (sim bookkeeping).
    pub fn persisted(&self) -> &BTreeMap<NodeId, crate::raft::HardState> {
        &self.persisted
    }
}

#[cfg(test)]
mod tests {
    use super::test_node::EchoNode;
    use super::{Input, Output, SimNode, Simulator, node_rng};
    use crate::{
        raft::{RaftNode, RaftRpc, RequestVoteRequest, RequestVoteResponse},
        sim::clock::Clock,
        sim::trace::TraceEventKind,
    };
    use rand::RngCore;

    fn echo_nodes(seed: u64, ids: &[u64]) -> Vec<EchoNode> {
        ids.iter().map(|&id| EchoNode::new(seed, id)).collect()
    }

    struct OrderedOutputNode {
        id: u64,
    }

    impl SimNode for OrderedOutputNode {
        fn id(&self) -> u64 {
            self.id
        }

        fn step(&mut self, input: Input) -> Vec<Output> {
            match input {
                Input::Tick => vec![
                    Output::Echo { payload: vec![1] },
                    Output::Echo { payload: vec![2] },
                ],
                Input::Message { .. } | Input::ClientCommand(_) => Vec::new(),
            }
        }
    }

    /// Node that replies to any RequestVote with a fixed response, for delivery tests.
    struct BounceNode {
        id: u64,
    }

    impl SimNode for BounceNode {
        fn id(&self) -> u64 {
            self.id
        }

        fn step(&mut self, input: Input) -> Vec<Output> {
            match input {
                Input::Tick if self.id == 1 => vec![Output::Send {
                    to: 2,
                    rpc: RaftRpc::RequestVote(RequestVoteRequest {
                        term: 1,
                        candidate_id: 1,
                        last_log_index: 0,
                        last_log_term: 0,
                    }),
                }],
                Input::Message {
                    from,
                    rpc: RaftRpc::RequestVote(_),
                } => vec![Output::Send {
                    to: from,
                    rpc: RaftRpc::RequestVoteResponse(RequestVoteResponse {
                        term: 1,
                        vote_granted: true,
                    }),
                }],
                _ => Vec::new(),
            }
        }
    }

    #[test]
    fn run_uses_zero_based_ticks_and_steps_nodes_in_ascending_order() {
        let mut simulator = Simulator::new(9, echo_nodes(9, &[3, 1, 2]));
        let (stepped_ids, final_event_tick) = {
            let trace = simulator.run(1);
            let stepped_ids: Vec<_> = trace
                .events()
                .iter()
                .filter(|event| event.kind() == TraceEventKind::NodeStep)
                .map(|event| event.node())
                .collect();
            let final_event_tick = trace.events().last().map(|event| event.tick());
            (stepped_ids, final_event_tick)
        };

        assert_eq!(simulator.clock().now(), 1);
        assert_eq!(stepped_ids, vec![Some(1), Some(2), Some(3)]);
        assert_eq!(final_event_tick, Some(0));
    }

    #[test]
    fn registration_order_does_not_change_trace() {
        let mut first = Simulator::new(12, echo_nodes(12, &[3, 1, 2]));
        let mut second = Simulator::new(12, echo_nodes(12, &[1, 2, 3]));

        assert_eq!(first.run(3).canonical(), second.run(3).canonical());
    }

    #[test]
    fn drains_each_nodes_outputs_before_stepping_the_next_node() {
        let mut simulator = Simulator::new(
            1,
            vec![OrderedOutputNode { id: 2 }, OrderedOutputNode { id: 1 }],
        );

        assert_eq!(
            simulator.run(1).canonical(),
            "tick=0 kind=sim_start node=- payload=seed:1,nodes:1,2\n\
             tick=0 kind=tick_start node=- payload=-\n\
             tick=0 kind=node_step node=1 payload=-\n\
             tick=0 kind=echo node=1 payload=hex:01\n\
             tick=0 kind=echo node=1 payload=hex:02\n\
             tick=0 kind=node_step node=2 payload=-\n\
             tick=0 kind=echo node=2 payload=hex:01\n\
             tick=0 kind=echo node=2 payload=hex:02\n\
             tick=0 kind=tick_end node=- payload=-\n"
        );
    }

    #[test]
    fn delivers_send_outputs_as_messages_same_tick() {
        let mut simulator = Simulator::new(
            1,
            vec![
                BounceNode { id: 1 },
                BounceNode { id: 2 },
            ],
        );

        let trace = simulator.run(1);
        let kinds: Vec<_> = trace
            .events()
            .iter()
            .filter(|e| {
                matches!(
                    e.kind(),
                    TraceEventKind::Send | TraceEventKind::Deliver
                )
            })
            .map(|e| (e.kind(), e.node()))
            .collect();

        // Node 1 sends RequestVote → deliver to 2 → 2 replies → deliver to 1.
        assert!(
            kinds.iter().any(|(k, n)| *k == TraceEventKind::Send && *n == Some(1)),
            "expected send from 1, got {kinds:?}"
        );
        assert!(
            kinds
                .iter()
                .any(|(k, n)| *k == TraceEventKind::Deliver && *n == Some(2)),
            "expected deliver to 2, got {kinds:?}"
        );
        assert!(
            kinds.iter().any(|(k, n)| *k == TraceEventKind::Send && *n == Some(2)),
            "expected reply send from 2, got {kinds:?}"
        );
        assert!(
            kinds
                .iter()
                .any(|(k, n)| *k == TraceEventKind::Deliver && *n == Some(1)),
            "expected deliver reply to 1, got {kinds:?}"
        );
    }

    #[test]
    fn same_seed_is_deterministic_for_one_hundred_seeds() {
        for seed in 0..100 {
            let mut first = Simulator::new(seed, echo_nodes(seed, &[1, 2, 3]));
            let mut second = Simulator::new(seed, echo_nodes(seed, &[1, 2, 3]));

            assert_eq!(first.run(4).canonical(), second.run(4).canonical());
        }
    }

    #[test]
    #[ignore = "expensive deterministic replay CI coverage"]
    fn same_seed_is_deterministic_for_one_thousand_seeds() {
        for seed in 0..1_000 {
            let mut first = Simulator::new(seed, echo_nodes(seed, &[1, 2, 3]));
            let mut second = Simulator::new(seed, echo_nodes(seed, &[1, 2, 3]));

            assert_eq!(first.run(4).canonical(), second.run(4).canonical());
        }
    }

    #[test]
    fn different_seeds_produce_different_canonical_traces() {
        let mut first = Simulator::new(1, echo_nodes(1, &[1, 2, 3]));
        let mut second = Simulator::new(2, echo_nodes(2, &[1, 2, 3]));

        assert_ne!(first.run(1).canonical(), second.run(1).canonical());
    }

    #[test]
    fn adding_a_node_does_not_change_existing_rng_streams() {
        let mut two_nodes = Simulator::new(42, echo_nodes(42, &[1, 2]));
        let mut three_nodes = Simulator::new(42, echo_nodes(42, &[1, 2, 3]));

        two_nodes.run(3);
        three_nodes.run(3);

        let two_node_echoes: Vec<_> = two_nodes
            .trace()
            .events()
            .iter()
            .filter(|event| event.kind() == TraceEventKind::Echo)
            .map(|event| (event.node(), event.payload().cloned()))
            .collect();
        let three_node_echoes: Vec<_> = three_nodes
            .trace()
            .events()
            .iter()
            .filter(|event| {
                event.kind() == TraceEventKind::Echo && matches!(event.node(), Some(1 | 2))
            })
            .map(|event| (event.node(), event.payload().cloned()))
            .collect();

        assert_eq!(two_node_echoes, three_node_echoes);
    }

    #[test]
    fn node_rng_is_repeatable_and_independent_by_node_id() {
        let mut first = node_rng(44, 1);
        let mut second = node_rng(44, 1);
        let mut other_node = node_rng(44, 2);

        assert_eq!(first.next_u64(), second.next_u64());
        assert_ne!(first.next_u64(), other_node.next_u64());
    }

    #[test]
    fn display_trace_is_legible_for_a_short_echo_run() {
        let mut simulator = Simulator::new(3, echo_nodes(3, &[3, 1, 2]));
        let pretty = simulator.run(1).to_string();

        assert!(pretty.contains("tick 0"));
        assert!(pretty.contains("node 1"));
        assert!(pretty.contains("node 2"));
        assert!(pretty.contains("node 3"));
    }

    #[test]
    fn canonical_trace_matches_golden_fixture() {
        let mut simulator = Simulator::new(99, echo_nodes(99, &[3, 1, 2]));

        assert_eq!(
            simulator.run(2).canonical(),
            include_str!("golden_trace.txt")
        );
    }

    #[test]
    fn large_run_completes_with_only_logical_time() {
        let mut simulator = Simulator::new(5, echo_nodes(5, &[1, 2, 3]));

        simulator.run(10_000);

        assert_eq!(simulator.clock().now(), 10_000);
        assert_eq!(simulator.trace().len(), 80_001);
    }

    #[test]
    fn raft_cluster_elects_leader_with_message_delivery() {
        let seed = 1_u64;
        let ids = [1_u64, 2, 3];
        let nodes: Vec<RaftNode> = ids
            .iter()
            .map(|&id| {
                let peers: Vec<_> = ids.iter().copied().filter(|&p| p != id).collect();
                RaftNode::with_rng(id, peers, node_rng(seed, id))
            })
            .collect();

        let mut sim = Simulator::new(seed, nodes);
        sim.run(600);

        let leaders: Vec<_> = sim
            .nodes()
            .iter()
            .filter(|(_, n)| n.state() == crate::raft::state::RaftState::Leader)
            .map(|(&id, _)| id)
            .collect();
        assert_eq!(leaders.len(), 1, "expected one leader, got {leaders:?}");
    }
}
