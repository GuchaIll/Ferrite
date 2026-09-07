//! Single-threaded deterministic simulator harness.
//!
//! A global step enters logical tick `t`, steps nodes in ascending [`NodeId`]
//! order, drains each node's outputs in returned order, records `TickEnd`, and
//! then advances the clock to `t + 1`. Nodes never receive the clock.

use std::collections::BTreeMap;

use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;

use self::{
    clock::{Clock, SimClock},
    trace::{Trace, TraceEvent, TraceEventKind, TracePayload},
};
use crate::raft::{RaftRpc, LogEntry, HardState};

pub mod clock;
pub mod network;
pub mod trace;

/// Identifier of a simulated node. Zero is reserved and invalid.
pub type NodeId = u64;

/// Input delivered by the deterministic driver to a simulated node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Input {
    /// One global logical tick has begun.
    Tick,
    Message {  from: NodeId, rpc: RaftRpc }, ClientCommand(Vec<u8>)
}

/// An output returned from a simulated node.
///
/// The driver drains this vector in order before it steps the next node.
///
/// The ordering is a contract, not an implementation detail: `Persist` must
/// be drained before any `Send` that depends on it, since Raft requires a
/// node to persist `currentTerm`/`votedFor` before responding to an RPC. A
/// driver that reorders this for throughput introduces a data-loss bug.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output {
    /// Test-only evidence that [`EchoNode`] processed a tick.
    Echo { payload: Vec<u8> }, Send { to: NodeId, rpc: RaftRpc }, Apply(LogEntry), Persist(HardState)
}

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

/// Minimal node used to prove the deterministic step-loop and RNG plumbing.
pub struct EchoNode {
    id: NodeId,
    rng: ChaCha8Rng,
}

impl EchoNode {
    /// Constructs an echo node with its independent deterministic RNG stream.
    pub fn new(root_seed: u64, id: NodeId) -> Self {
        Self {
            id,
            rng: node_rng(root_seed, id),
        }
    }
}

impl SimNode for EchoNode {
    fn id(&self) -> NodeId {
        self.id
    }

    fn step(&mut self, input: Input) -> Vec<Output> {
        match input {
            Input::Tick => vec![Output::Echo {
                payload: self.rng.next_u64().to_le_bytes().to_vec(),
            }],
            // TODO: EchoNode only proves the tick/RNG plumbing; message and
            // client-command handling lands with the real Raft node.
            Input::Message { .. } | Input::ClientCommand(_) => Vec::new(),
        }
    }
}

/// Driver-owned deterministic simulation state.
pub struct Simulator<N: SimNode> {
    clock: SimClock,
    root_seed: u64,
    nodes: BTreeMap<NodeId, N>,
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
            trace,
        }
    }

    /// Runs `ticks` global simulation steps and returns the cumulative trace.
    pub fn run(&mut self, ticks: u64) -> &Trace {
        for _ in 0..ticks {
            let tick = self.clock.now();
            self.trace
                .record(TraceEvent::new(tick, TraceEventKind::TickStart, None, None));

            for (&node_id, node) in &mut self.nodes {
                self.trace.record(TraceEvent::new(
                    tick,
                    TraceEventKind::NodeStep,
                    Some(node_id),
                    None,
                ));

                for output in node.step(Input::Tick) {
                    match output {
                        Output::Echo { payload } => self.trace.record(TraceEvent::new(
                            tick,
                            TraceEventKind::Echo,
                            Some(node_id),
                            Some(TracePayload::Bytes(payload)),
                        )),
                        // TODO: wire real trace recording once message
                        // delivery, log application, and persistence are
                        // implemented in the driver.
                        Output::Send { .. } | Output::Apply(_) | Output::Persist(_) => {}
                    }
                }
            }

            self.trace
                .record(TraceEvent::new(tick, TraceEventKind::TickEnd, None, None));
            self.clock.advance(1);
        }

        &self.trace
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
}

#[cfg(test)]
mod tests {
    use super::{node_rng, EchoNode, Input, Output, SimNode, Simulator};
    use crate::{sim::clock::Clock, sim::trace::TraceEventKind};
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
}
