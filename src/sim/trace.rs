//! Deterministic event tracing for simulator runs.

use std::fmt;

use super::NodeId;

/// The stable category of a simulator trace event.
///
/// New variants must be append-only: canonical traces are used to replay and
/// compare deterministic simulator runs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraceEventKind {
    SimStart,
    TickStart,
    TickEnd,
    NodeStep,
    Echo,
    Send,
    Deliver,
    Drop,
}

impl TraceEventKind {
    fn canonical_name(self) -> &'static str {
        match self {
            Self::SimStart => "sim_start",
            Self::TickStart => "tick_start",
            Self::TickEnd => "tick_end",
            Self::NodeStep => "node_step",
            Self::Echo => "echo",
            Self::Send => "send",
            Self::Deliver => "deliver",
            Self::Drop => "drop",
        }
    }
}
/// Optional event data, deliberately limited to deterministic values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TracePayload {
    /// Metadata recorded once at the start of a simulator run.
    SimulationStart { seed: u64, nodes: Vec<NodeId> },
    /// Opaque deterministic bytes, serialized as lower-case hexadecimal.
    Bytes(Vec<u8>),
}

/// One structured simulator event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TraceEvent {
    tick: u64,
    kind: TraceEventKind,
    node: Option<NodeId>,
    payload: Option<TracePayload>,
}

impl TraceEvent {
    /// Creates an event at `tick` with optional node and payload fields.
    pub fn new(
        tick: u64,
        kind: TraceEventKind,
        node: Option<NodeId>,
        payload: Option<TracePayload>,
    ) -> Self {
        Self {
            tick,
            kind,
            node,
            payload,
        }
    }

    /// Returns the event's logical tick.
    pub fn tick(&self) -> u64 {
        self.tick
    }

    /// Returns the event's category.
    pub fn kind(&self) -> TraceEventKind {
        self.kind
    }

    /// Returns the node associated with this event, when applicable.
    pub fn node(&self) -> Option<NodeId> {
        self.node
    }

    /// Returns the optional event payload.
    pub fn payload(&self) -> Option<&TracePayload> {
        self.payload.as_ref()
    }
}

/// Append-only buffer of simulator events.
#[derive(Debug, Default)]
pub struct Trace {
    events: Vec<TraceEvent>,
}

impl Trace {
    /// Creates an empty trace.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends an event in the order it occurred.
    pub fn record(&mut self, event: TraceEvent) {
        self.events.push(event);
    }

    /// Returns the number of recorded events.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Returns whether no events have been recorded.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Returns the events in their recorded order.
    pub fn events(&self) -> &[TraceEvent] {
        &self.events
    }

    /// Serializes events as stable, newline-separated UTF-8 records.
    ///
    /// This is the equality and replay oracle. It has fixed field order and
    /// contains only logical ticks, node IDs, supplied payloads, and seed
    /// metadata; it never includes wall time, host paths, or map iteration.
    pub fn canonical(&self) -> String {
        let mut canonical = String::new();

        for event in &self.events {
            canonical.push_str("tick=");
            canonical.push_str(&event.tick.to_string());
            canonical.push_str(" kind=");
            canonical.push_str(event.kind.canonical_name());
            canonical.push_str(" node=");
            match event.node {
                Some(node) => canonical.push_str(&node.to_string()),
                None => canonical.push('-'),
            }
            canonical.push_str(" payload=");
            write_payload(&mut canonical, event.payload.as_ref());
            canonical.push('\n');
        }

        canonical
    }
}

impl fmt::Display for Trace {
    /// Renders the trace for humans. Use [`Trace::canonical`] for comparison.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for event in &self.events {
            writeln!(
                formatter,
                "tick {}: {}{}",
                event.tick,
                event.kind.canonical_name(),
                event
                    .node
                    .map(|node| format!(" (node {node})"))
                    .unwrap_or_default(),
            )?;
        }

        Ok(())
    }
}

fn write_payload(output: &mut String, payload: Option<&TracePayload>) {
    match payload {
        None => output.push('-'),
        Some(TracePayload::SimulationStart { seed, nodes }) => {
            output.push_str("seed:");
            output.push_str(&seed.to_string());
            output.push_str(",nodes:");
            for (index, node) in nodes.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                output.push_str(&node.to_string());
            }
        }
        Some(TracePayload::Bytes(bytes)) => {
            output.push_str("hex:");
            for byte in bytes {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                output.push(HEX[(byte >> 4) as usize] as char);
                output.push(HEX[(byte & 0x0f) as usize] as char);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Trace, TraceEvent, TraceEventKind, TracePayload};

    fn sample_run() -> Trace {
        let mut trace = Trace::new();
        trace.record(TraceEvent::new(
            0,
            TraceEventKind::SimStart,
            None,
            Some(TracePayload::SimulationStart {
                seed: 7,
                nodes: vec![1, 2, 3],
            }),
        ));
        trace.record(TraceEvent::new(0, TraceEventKind::TickStart, None, None));
        trace.record(TraceEvent::new(0, TraceEventKind::NodeStep, Some(1), None));
        trace.record(TraceEvent::new(0, TraceEventKind::Echo, Some(1), None));
        trace.record(TraceEvent::new(0, TraceEventKind::NodeStep, Some(2), None));
        trace.record(TraceEvent::new(0, TraceEventKind::Echo, Some(2), None));
        trace.record(TraceEvent::new(0, TraceEventKind::TickEnd, None, None));
        trace.record(TraceEvent::new(
            1,
            TraceEventKind::Send,
            Some(2),
            Some(TracePayload::Bytes(vec![0, 15, 255])),
        ));
        trace.record(TraceEvent::new(1, TraceEventKind::Deliver, Some(1), None));
        trace.record(TraceEvent::new(1, TraceEventKind::Drop, Some(3), None));
        trace
    }

    #[test]
    fn empty_trace_is_empty_and_has_empty_canonical_form() {
        let trace = Trace::new();

        assert_eq!(trace.len(), 0);
        assert!(trace.is_empty());
        assert!(trace.events().is_empty());
        assert_eq!(trace.canonical(), "");
        assert_eq!(trace.to_string(), "");
    }

    #[test]
    fn record_preserves_order_and_exposes_event_accessors() {
        let mut trace = Trace::new();
        let event = TraceEvent::new(
            9,
            TraceEventKind::Echo,
            Some(4),
            Some(TracePayload::Bytes(vec![0xab])),
        );
        trace.record(event.clone());
        trace.record(TraceEvent::new(10, TraceEventKind::TickEnd, None, None));

        assert_eq!(trace.len(), 2);
        assert!(!trace.is_empty());
        assert_eq!(trace.events()[0].tick(), 9);
        assert_eq!(trace.events()[0].kind(), TraceEventKind::Echo);
        assert_eq!(trace.events()[0].node(), Some(4));
        assert_eq!(
            trace.events()[0].payload(),
            Some(&TracePayload::Bytes(vec![0xab]))
        );
        assert_eq!(trace.events()[1].kind(), TraceEventKind::TickEnd);
        assert_eq!(trace.events()[1].node(), None);
        assert_eq!(trace.events()[1].payload(), None);
    }

    #[test]
    fn canonical_trace_uses_stable_field_order_and_newlines() {
        let trace = sample_run();

        assert_eq!(
            trace.canonical(),
            "tick=0 kind=sim_start node=- payload=seed:7,nodes:1,2,3\n\
             tick=0 kind=tick_start node=- payload=-\n\
             tick=0 kind=node_step node=1 payload=-\n\
             tick=0 kind=echo node=1 payload=-\n\
             tick=0 kind=node_step node=2 payload=-\n\
             tick=0 kind=echo node=2 payload=-\n\
             tick=0 kind=tick_end node=- payload=-\n\
             tick=1 kind=send node=2 payload=hex:000fff\n\
             tick=1 kind=deliver node=1 payload=-\n\
             tick=1 kind=drop node=3 payload=-\n"
        );
    }

    #[test]
    fn canonical_form_is_deterministic_across_identical_record_sequences() {
        assert_eq!(sample_run().canonical(), sample_run().canonical());
    }

    #[test]
    fn empty_payload_variants_serialize_stably() {
        let mut trace = Trace::new();
        trace.record(TraceEvent::new(
            0,
            TraceEventKind::SimStart,
            None,
            Some(TracePayload::SimulationStart {
                seed: 0,
                nodes: Vec::new(),
            }),
        ));
        trace.record(TraceEvent::new(
            0,
            TraceEventKind::Send,
            Some(1),
            Some(TracePayload::Bytes(Vec::new())),
        ));

        assert_eq!(
            trace.canonical(),
            "tick=0 kind=sim_start node=- payload=seed:0,nodes:\n\
             tick=0 kind=send node=1 payload=hex:\n"
        );
    }

    #[test]
    fn simulation_start_preserves_caller_provided_node_order() {
        let mut ascending = Trace::new();
        ascending.record(TraceEvent::new(
            0,
            TraceEventKind::SimStart,
            None,
            Some(TracePayload::SimulationStart {
                seed: 1,
                nodes: vec![1, 2, 3],
            }),
        ));

        let mut reverse = Trace::new();
        reverse.record(TraceEvent::new(
            0,
            TraceEventKind::SimStart,
            None,
            Some(TracePayload::SimulationStart {
                seed: 1,
                nodes: vec![3, 2, 1],
            }),
        ));

        assert_ne!(ascending.canonical(), reverse.canonical());
        assert!(ascending.canonical().contains("nodes:1,2,3"));
        assert!(reverse.canonical().contains("nodes:3,2,1"));
    }

    #[test]
    fn display_is_legible_but_not_the_canonical_oracle() {
        let mut with_node = Trace::new();
        with_node.record(TraceEvent::new(4, TraceEventKind::NodeStep, Some(9), None));

        let mut without_node = Trace::new();
        without_node.record(TraceEvent::new(0, TraceEventKind::TickStart, None, None));

        assert_eq!(with_node.to_string(), "tick 4: node_step (node 9)\n");
        assert_eq!(without_node.to_string(), "tick 0: tick_start\n");
        assert_ne!(with_node.to_string(), with_node.canonical());
        assert!(with_node.to_string().contains("node 9"));
        assert!(with_node.to_string().contains("4"));
        assert!(!with_node.to_string().contains("kind="));
    }

    #[test]
    fn all_event_kinds_have_stable_canonical_names() {
        let cases = [
            (TraceEventKind::SimStart, "sim_start"),
            (TraceEventKind::TickStart, "tick_start"),
            (TraceEventKind::TickEnd, "tick_end"),
            (TraceEventKind::NodeStep, "node_step"),
            (TraceEventKind::Echo, "echo"),
            (TraceEventKind::Send, "send"),
            (TraceEventKind::Deliver, "deliver"),
            (TraceEventKind::Drop, "drop"),
        ];

        for (kind, name) in cases {
            let mut trace = Trace::new();
            trace.record(TraceEvent::new(0, kind, None, None));
            assert_eq!(
                trace.canonical(),
                format!("tick=0 kind={name} node=- payload=-\n")
            );
        }
    }
}
