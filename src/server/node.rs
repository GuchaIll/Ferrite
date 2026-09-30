//! The node actor: one task owning the Raft state machine and its storage.
//!
//! # Why an actor and not a lock
//!
//! Everything that mutates consensus lives in one task. gRPC handlers, the tick
//! timer, and proposers reach it only by channel. There is no
//! `Arc<Mutex<RaftNode>>`, for two reasons that are not style preferences:
//!
//! - A lock invites holding a guard across an `.await`, and the guard would have
//!   to be held across an fsync to be useful.
//! - Two handlers holding the lock in turn interleave `step` calls in whatever
//!   order the runtime happens to poll them. The core's output ordering contract
//!   is defined per batch, and a second `step` landing mid-drain violates the
//!   one-batch-in-flight rule that makes a leader counting its own `last_index`
//!   toward a majority safe.
//!
//! # Cancellation safety
//!
//! The `select!` races **receives only**. Every branch resolves to a `Work`
//! value and nothing else; all state mutation happens after the `select!` has
//! completed, in straight-line code that cannot be cancelled. So the question
//! "is this branch cancellation-safe?" reduces to "is this receive
//! cancellation-safe?", and `mpsc::Receiver::recv`, `Interval::tick`, and
//! `watch::Receiver::changed` all are.
//!
//! # Durability
//!
//! Outputs drain in returned order with persists group-committed: every write in
//! a batch is durable before the first `Send` or `Apply` that follows it. A
//! failed commit fails closed — the node stops rather than emitting a success
//! reply or an apply for something that is not on disk.

use std::collections::VecDeque;
use std::time::Duration;

use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval};

use crate::{
    config::{NodeId, RaftConfig},
    kv::{ClientMeta, ClientRequest, CommandResult, KvStateMachine},
    raft::{
        Input, Output, RaftNode, Snapshot, SnapshotMeta,
        state::RaftState,
        storage::{LogOp, Storage, WriteBatch},
    },
    transport::Transport,
};

/// Inbound queue depth for RPCs arriving from peers.
const INBOUND_QUEUE_DEPTH: usize = 1024;
/// Queue depth for client proposals awaiting a turn in the actor.
const PROPOSAL_QUEUE_DEPTH: usize = 1024;
/// Backlog retained for apply subscribers that fall behind.
const APPLY_BROADCAST_DEPTH: usize = 1024;
/// Upper bound on how many proposals join one batch.
///
/// Batching is opportunistic, so this is a ceiling on work per step, not a
/// target: whatever arrived while the previous fsync was in flight goes in one
/// batch, which costs one fsync instead of one per proposal.
const MAX_PROPOSAL_BATCH: usize = 512;

/// How many ticks make up one heartbeat interval.
///
/// Sets the driver's tick period at `heartbeat_interval / this`. The core's
/// timers are counted in ticks, so this is the resolution at which an election
/// timeout can be observed; ten gives a tick well under the rpc timeout without
/// waking the actor pointlessly often.
const TICKS_PER_HEARTBEAT: u32 = 10;

/// A log position assigned to an accepted proposal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Proposed {
    pub index: u64,
    pub term: u64,
}

/// A committed entry that has been applied to the state machine.
///
/// `client` and `term` are what let a caller prove the entry that applied at its
/// index is the one it proposed, rather than a different command written there
/// by a later leader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Applied {
    pub index: u64,
    pub term: u64,
    pub client: Option<ClientMeta>,
    pub result: CommandResult,
}

/// Why a proposal did not become a log entry.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NodeError {
    #[error("not leader (leader hint: {leader_hint:?})")]
    NotLeader { leader_hint: Option<NodeId> },

    #[error("durability failed: {0}")]
    Durability(String),

    #[error("node is shutting down")]
    Shutdown,
}

struct Proposal {
    command: Vec<u8>,
    reply: oneshot::Sender<Result<Proposed, NodeError>>,
}

/// What the node currently believes about leadership.
///
/// Published after every turn so observers never have to infer a role by
/// proposing and seeing what happens. Issue 05's redirect hint and the cluster
/// tests both read it here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Role {
    pub is_leader: bool,
    pub term: u64,
    pub leader_hint: Option<NodeId>,
}

/// In-process handle to a running node.
#[derive(Clone)]
pub struct NodeHandle {
    id: NodeId,
    proposals: mpsc::Sender<Proposal>,
    applied: broadcast::Sender<Applied>,
    role: watch::Receiver<Role>,
}

impl NodeHandle {
    /// Returns this node's Raft identity.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Submits a serialized command and resolves once it is a durable log entry.
    ///
    /// Returning `Ok` means only that the entry is on this node's disk at that
    /// index — not that it committed. A caller that needs the command's *result*
    /// waits for that index on [`Self::subscribe_apply`], because a leader can
    /// lose its term after appending and before committing.
    pub async fn propose(&self, command: Vec<u8>) -> Result<Proposed, NodeError> {
        let (reply, response) = oneshot::channel();
        self.proposals
            .send(Proposal { command, reply })
            .await
            .map_err(|_| NodeError::Shutdown)?;
        response.await.map_err(|_| NodeError::Shutdown)?
    }

    /// Subscribes to entries as they apply, in index order.
    ///
    /// Subscribe **before** proposing. A subscription taken afterwards can miss
    /// the apply it is waiting for, and the wait then hangs until its deadline.
    pub fn subscribe_apply(&self) -> broadcast::Receiver<Applied> {
        self.applied.subscribe()
    }

    /// Watches this node's view of leadership.
    pub fn role(&self) -> watch::Receiver<Role> {
        self.role.clone()
    }
}

/// Everything a caller needs to talk to a spawned node.
pub struct NodeRuntime {
    /// For proposals and apply notifications.
    pub handle: NodeHandle,
    /// For RPCs arriving from peers; handed to the gRPC service.
    pub inbound: mpsc::Sender<(NodeId, crate::raft::RaftRpc)>,
}

/// Tick period and budgets derived from configured wall-clock timing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TickSchedule {
    pub period: Duration,
    pub heartbeat_ticks: u64,
    pub election_min_ticks: u64,
    pub election_max_ticks: u64,
}

impl TickSchedule {
    /// Converts configured durations into tick counts.
    ///
    /// Config validation already guarantees
    /// `heartbeat < rpc_timeout < election_min < election_max`, so the derived
    /// tick counts preserve that ordering as long as the period divides the
    /// heartbeat, which is how it is chosen.
    pub fn from_config(raft: &RaftConfig) -> Self {
        let period = (raft.heartbeat_interval / TICKS_PER_HEARTBEAT).max(Duration::from_millis(1));
        let ticks = |d: Duration| -> u64 {
            let period_us = period.as_micros().max(1);
            (d.as_micros() / period_us).max(1) as u64
        };

        Self {
            period,
            heartbeat_ticks: ticks(raft.heartbeat_interval),
            election_min_ticks: ticks(raft.election_timeout_min),
            election_max_ticks: ticks(raft.election_timeout_max),
        }
    }
}

/// Everything one node is assembled from.
///
/// Grouped rather than passed loose so the backend, the transport, and the
/// timing arrive together — a node built with a disk backend and a simulated
/// transport, or with timing from a different config than its peers, is a
/// mistake worth making hard to express.
pub struct NodeParts<S, T> {
    pub node: RaftNode,
    pub storage: S,
    pub state_machine: KvStateMachine,
    pub transport: T,
    pub schedule: TickSchedule,
    pub compaction_threshold: u64,
}

/// Starts a node actor and returns handles to it.
///
/// The actor task is registered in `tasks`; dropping `shutdown`'s sender or
/// setting it to `true` stops the actor and, in turn, every peer task whose
/// queue it owned.
pub fn spawn<S, T>(
    parts: NodeParts<S, T>,
    tasks: &mut JoinSet<()>,
    shutdown: watch::Receiver<bool>,
) -> NodeRuntime
where
    S: Storage + Send + 'static,
    T: Transport + Send + 'static,
{
    let NodeParts {
        mut node,
        storage,
        state_machine,
        transport,
        schedule,
        compaction_threshold,
    } = parts;

    node.set_tick_budget(
        schedule.heartbeat_ticks,
        schedule.election_min_ticks,
        schedule.election_max_ticks,
    );
    node.set_compaction_threshold(compaction_threshold);

    let id = node.id();
    let (inbound_tx, inbound_rx) = mpsc::channel(INBOUND_QUEUE_DEPTH);
    let (proposal_tx, proposal_rx) = mpsc::channel(PROPOSAL_QUEUE_DEPTH);
    let (applied_tx, _) = broadcast::channel(APPLY_BROADCAST_DEPTH);
    let (role_tx, role_rx) = watch::channel(Role::default());

    let actor = NodeActor {
        node,
        storage: Some(storage),
        state_machine,
        transport,
        applied: applied_tx.clone(),
        role: role_tx,
        inbound: inbound_rx,
        proposals: proposal_rx,
        tick_period: schedule.period,
    };

    tasks.spawn(actor.run(shutdown));

    NodeRuntime {
        handle: NodeHandle {
            id,
            proposals: proposal_tx,
            applied: applied_tx,
            role: role_rx,
        },
        inbound: inbound_tx,
    }
}

/// What one turn of the actor loop decided to do.
enum Work {
    Tick,
    Message(NodeId, crate::raft::RaftRpc),
    Propose(Proposal),
}

struct NodeActor<S, T> {
    node: RaftNode,
    /// `Option` because a `commit` moves the backend to a blocking thread and
    /// takes it back; it is never absent across an await the actor observes.
    storage: Option<S>,
    state_machine: KvStateMachine,
    transport: T,
    applied: broadcast::Sender<Applied>,
    role: watch::Sender<Role>,
    inbound: mpsc::Receiver<(NodeId, crate::raft::RaftRpc)>,
    proposals: mpsc::Receiver<Proposal>,
    tick_period: Duration,
}

impl<S, T> NodeActor<S, T>
where
    S: Storage + Send + 'static,
    T: Transport + Send + 'static,
{
    async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        let id = self.node.id();
        let mut ticker = interval(self.tick_period);
        // Delay, not Burst: under load a burst of missed ticks would fire
        // several election timeouts in one turn and unseat a live leader.
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        tracing::info!(
            node_id = id,
            period_ms = self.tick_period.as_millis(),
            "node started"
        );

        loop {
            let work = tokio::select! {
                _ = ticker.tick() => Work::Tick,
                Some((from, rpc)) = self.inbound.recv() => Work::Message(from, rpc),
                Some(proposal) = self.proposals.recv() => Work::Propose(proposal),
                _ = shutdown.changed() => break,
                else => break,
            };

            // Past this point nothing is cancellable: no `.await` below is
            // inside a `select!`, so a drain always runs to completion.
            let durable = match work {
                Work::Tick => self.drive(Input::Tick).await,
                Work::Message(from, rpc) => self.drive(Input::Message { from, rpc }).await,
                Work::Propose(first) => self.drive_proposals(first).await,
            };

            self.publish_role();

            if !durable {
                tracing::error!(node_id = id, "storage commit failed; stopping node");
                break;
            }
        }

        tracing::info!(node_id = id, "node stopped");
    }

    /// Publishes the current leadership view, if it changed.
    ///
    /// `send_if_modified` so watchers only wake on a real transition, not on
    /// every tick.
    fn publish_role(&self) {
        let next = Role {
            is_leader: self.node.state() == RaftState::Leader,
            term: self.node.current_term(),
            leader_hint: self.node.leader_id(),
        };
        self.role.send_if_modified(|current| {
            if *current == next {
                return false;
            }
            // Logged on transition only. An operator's first question after a
            // restart is who the leader is, and a line per tick would bury it.
            if current.is_leader != next.is_leader {
                if next.is_leader {
                    tracing::info!(node_id = self.node.id(), term = next.term, "became leader");
                } else {
                    tracing::info!(
                        node_id = self.node.id(),
                        term = next.term,
                        leader = ?next.leader_hint,
                        "stepped down"
                    );
                }
            }
            *current = next;
            true
        });
    }

    /// Steps the core once and drains the result.
    async fn drive(&mut self, input: Input) -> bool {
        let outputs = self.node.step(input);
        self.drain(outputs).await
    }

    /// Collects every queued proposal into one batch, then answers each one.
    async fn drive_proposals(&mut self, first: Proposal) -> bool {
        let mut batch = Vec::with_capacity(MAX_PROPOSAL_BATCH);
        batch.push(first);
        // Opportunistic: whatever arrived while the previous turn was fsyncing
        // rides along for free. At low load this is a batch of one and adds no
        // latency; a linger timer would add latency exactly when there is no
        // throughput to gain.
        while batch.len() < MAX_PROPOSAL_BATCH {
            match self.proposals.try_recv() {
                Ok(proposal) => batch.push(proposal),
                Err(_) => break,
            }
        }

        let mut commands = Vec::with_capacity(batch.len());
        let mut repliers = Vec::with_capacity(batch.len());
        for proposal in batch {
            commands.push(proposal.command);
            repliers.push(proposal.reply);
        }

        let outputs = self.node.step(Input::ClientCommands(commands));

        // Read the outcome off the outputs before draining consumes them. The
        // assigned positions are exactly the entries of the single `PersistLog`
        // a leader emits for the batch, in submission order.
        let assigned: Option<Vec<Proposed>> = outputs.iter().find_map(|output| match output {
            Output::PersistLog { entries, .. } => Some(
                entries
                    .iter()
                    .map(|e| Proposed {
                        index: e.index,
                        term: e.term,
                    })
                    .collect(),
            ),
            _ => None,
        });
        let redirect = outputs.iter().find_map(|output| match output {
            Output::Redirect { leader_hint } => Some(*leader_hint),
            _ => None,
        });

        let durable = self.drain(outputs).await;

        let outcome = match (durable, redirect, assigned) {
            // Not the leader: the hint is the client's route, not an error to
            // swallow.
            (_, Some(leader_hint), _) => Err(NodeError::NotLeader { leader_hint }),
            (false, _, _) => Err(NodeError::Durability("commit failed".to_owned())),
            (true, None, Some(positions)) if positions.len() == repliers.len() => {
                for (reply, position) in repliers.into_iter().zip(positions) {
                    let _ = reply.send(Ok(position));
                }
                return durable;
            }
            // A leader that appended nothing, or a count that does not line up,
            // is a core bug. Fail the proposals rather than hand back a
            // position that might belong to another command.
            (true, None, other) => {
                tracing::error!(
                    node_id = self.node.id(),
                    assigned = other.map(|p| p.len()).unwrap_or(0),
                    expected = repliers.len(),
                    "proposal batch produced no matching log positions"
                );
                Err(NodeError::Durability(
                    "proposal was not appended".to_owned(),
                ))
            }
        };

        for reply in repliers {
            let _ = reply.send(outcome.clone());
        }
        durable
    }

    /// Drains outputs in order, group-committing persists.
    ///
    /// Returns `false` if durability failed, in which case no further output
    /// from the batch was dispatched.
    ///
    /// Follow-up outputs (a snapshot taken, a snapshot made durable) are pushed
    /// to the **front**, so they are handled before the remainder of the current
    /// batch — the same nesting the simulator's recursive drain produces.
    async fn drain(&mut self, outputs: Vec<Output>) -> bool {
        let mut work: VecDeque<Output> = outputs.into();
        let mut batch = WriteBatch::default();

        loop {
            while let Some(output) = work.pop_front() {
                match output {
                    Output::Persist(hard_state) => batch.hard_state = Some(hard_state),
                    Output::PersistLog {
                        truncate_from,
                        entries,
                    } => {
                        if let Some(from) = truncate_from {
                            batch.log.push(LogOp::TruncateFrom(from));
                        }
                        for entry in entries {
                            batch.log.push(LogOp::Append(entry));
                        }
                    }
                    Output::PersistSnapshot(snapshot) => batch.snapshot = Some(snapshot),
                    other => {
                        // Durability boundary: everything accumulated so far is
                        // on disk before this output leaves the node.
                        if !self.flush(&mut batch, &mut work).await {
                            return false;
                        }
                        self.dispatch(other, &mut work);
                    }
                }
            }

            if batch.is_empty() {
                return true;
            }
            if !self.flush(&mut batch, &mut work).await {
                return false;
            }
            if work.is_empty() {
                return true;
            }
        }
    }

    /// Group-commits `batch`, queueing any follow-up the core produces.
    async fn flush(&mut self, batch: &mut WriteBatch, work: &mut VecDeque<Output>) -> bool {
        if batch.is_empty() {
            return true;
        }

        let snapshot_meta = batch.snapshot.as_ref().map(|s| s.meta.clone());
        let pending = std::mem::take(batch);

        let Some(storage) = self.storage.take() else {
            tracing::error!(node_id = self.node.id(), "storage missing");
            return false;
        };

        // `commit` fsyncs, which would park an executor worker for milliseconds.
        // The backend is single-owner, so it moves onto the blocking thread and
        // comes back rather than being shared behind a lock.
        let joined = tokio::task::spawn_blocking(move || {
            let mut storage = storage;
            let result = storage.commit(&pending);
            (storage, result)
        })
        .await;

        match joined {
            Ok((storage, result)) => {
                self.storage = Some(storage);
                if let Err(error) = result {
                    tracing::error!(node_id = self.node.id(), %error, "storage commit failed");
                    return false;
                }
            }
            Err(error) => {
                // A panic inside commit leaves durability unknown, which is the
                // one thing that must never be assumed. Fail closed.
                tracing::error!(node_id = self.node.id(), %error, "storage commit task panicked");
                return false;
            }
        }

        if let Some(meta) = snapshot_meta {
            queue_front(self.node.step(Input::SnapshotPersisted(meta)), work);
        }
        true
    }

    /// Routes one non-persist output.
    fn dispatch(&mut self, output: Output, work: &mut VecDeque<Output>) {
        match output {
            Output::Send { to, rpc } => self.transport.send(self.node.id(), to, rpc),

            Output::Apply(entry) => match self.state_machine.apply(&entry) {
                Ok(result) => {
                    // Decoded again here so subscribers can prove identity
                    // without re-parsing the command themselves. A second decode
                    // of a small payload is free next to the fsync that preceded
                    // it. An empty command is a leader no-op and has no client.
                    let client = ClientRequest::decode(&entry.command)
                        .ok()
                        .and_then(|request| request.client);
                    // Err means nobody is subscribed, which is normal.
                    let _ = self.applied.send(Applied {
                        index: entry.index,
                        term: entry.term,
                        client,
                        result,
                    });
                }
                Err(error) => {
                    tracing::error!(
                        node_id = self.node.id(),
                        index = entry.index,
                        %error,
                        "could not apply committed entry"
                    );
                }
            },

            Output::RequestSnapshot {
                last_included_index,
                last_included_term,
            } => match self.state_machine.snapshot() {
                Ok(data) => {
                    let snapshot = Snapshot::new(
                        SnapshotMeta::new(last_included_index, last_included_term),
                        data,
                    );
                    queue_front(self.node.step(Input::SnapshotTaken(snapshot)), work);
                }
                Err(error) => {
                    tracing::error!(node_id = self.node.id(), %error, "could not snapshot state machine");
                }
            },

            Output::ApplySnapshot(snapshot) => {
                if let Err(error) = self.state_machine.restore(&snapshot.data) {
                    tracing::error!(node_id = self.node.id(), %error, "could not restore snapshot");
                }
            }

            // Answered by the proposal path, which reads it off the batch before
            // the drain starts; there is no client here to redirect.
            Output::Redirect { .. } => {}

            #[cfg(test)]
            Output::Echo { .. } => {}

            // Accumulated by `drain` before dispatch is ever called.
            Output::Persist(_) | Output::PersistLog { .. } | Output::PersistSnapshot(_) => {}
        }
    }
}

/// Pushes follow-up outputs ahead of the remaining work, preserving their order.
///
/// A free function, not a method: the caller is already holding a mutable borrow
/// of the node in order to produce `outputs`.
fn queue_front(outputs: Vec<Output>, work: &mut VecDeque<Output>) {
    for output in outputs.into_iter().rev() {
        work.push_front(output);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::Command;
    use crate::raft::storage::{MemoryStorage, Recovered};
    use crate::raft::{RaftRpc, RequestVoteResponse};
    use std::sync::{Arc, Mutex};

    /// What the driver did, in the order it did it.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Event {
        Commit { entries: usize, sync: u64 },
        Send(NodeId),
    }

    /// Storage and transport that record into one shared log, so the ordering
    /// between a commit and the send that follows it is directly observable.
    #[derive(Clone, Default)]
    struct Recorder(Arc<Mutex<Vec<Event>>>);

    impl Recorder {
        fn events(&self) -> Vec<Event> {
            self.0.lock().expect("recorder poisoned").clone()
        }

        fn push(&self, event: Event) {
            self.0.lock().expect("recorder poisoned").push(event);
        }
    }

    struct RecordingStorage {
        inner: MemoryStorage,
        recorder: Recorder,
    }

    /// Stands in for the cost of an fsync.
    ///
    /// `MemoryStorage::commit` returns instantly, which makes batching
    /// unobservable: the actor finishes a proposal before the next one is even
    /// submitted, so every batch is a batch of one. Real durability takes
    /// milliseconds, and that gap is exactly what proposals accumulate in. This
    /// runs on a blocking thread, so sleeping here is legitimate.
    const COMMIT_COST: Duration = Duration::from_millis(2);

    impl Storage for RecordingStorage {
        fn commit(&mut self, batch: &WriteBatch) -> Result<(), crate::error::Error> {
            let entries = batch.log.len();
            std::thread::sleep(COMMIT_COST);
            self.inner.commit(batch)?;
            self.recorder.push(Event::Commit {
                entries,
                sync: self.inner.sync_count(),
            });
            Ok(())
        }

        fn recover(&self) -> Result<Recovered, crate::error::Error> {
            self.inner.recover()
        }

        fn sync_count(&self) -> u64 {
            self.inner.sync_count()
        }
    }

    struct RecordingTransport(Recorder);

    impl Transport for RecordingTransport {
        fn send(&mut self, _from: NodeId, to: NodeId, _rpc: RaftRpc) {
            self.0.push(Event::Send(to));
        }
    }

    fn command(key: &str) -> Vec<u8> {
        ClientRequest::internal(Command::Set {
            key: key.as_bytes().to_vec(),
            value: b"v".to_vec(),
        })
        .encode()
        .expect("encode")
    }

    /// Spawns a node that is already leader, so proposals are accepted.
    async fn leader(recorder: Recorder) -> (NodeRuntime, JoinSet<()>, watch::Sender<bool>) {
        let schedule = TickSchedule::from_config(&RaftConfig::default());
        let mut node = RaftNode::new(1, vec![2, 3]);
        node.set_tick_budget(
            schedule.heartbeat_ticks,
            schedule.election_min_ticks,
            schedule.election_max_ticks,
        );

        let mut tasks = JoinSet::new();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let runtime = spawn(
            NodeParts {
                node,
                storage: RecordingStorage {
                    inner: MemoryStorage::default(),
                    recorder: recorder.clone(),
                },
                state_machine: KvStateMachine::new(),
                transport: RecordingTransport(recorder),
                schedule,
                compaction_threshold: 0,
            },
            &mut tasks,
            shutdown_rx,
        );

        // Drive an election by hand. The vote must name the term the node is
        // actually campaigning in: a vote for some other term is either ignored
        // or, if it is higher, steps the node down and bumps its term — which
        // means guessing the term makes the node *less* likely to win the longer
        // you try. So read the term it published and answer that one.
        let mut role = runtime.handle.role();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

        loop {
            // Copied out, not borrowed: a `watch::Ref` must not be held across
            // an await.
            let current = *role.borrow_and_update();
            if current.is_leader {
                break;
            }

            if current.term > 0 {
                // One peer vote plus its own is a majority of three.
                runtime
                    .inbound
                    .send((
                        2,
                        RaftRpc::RequestVoteResponse(RequestVoteResponse {
                            term: current.term,
                            vote_granted: true,
                        }),
                    ))
                    .await
                    .expect("inbound open");
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "node never became leader (term {}, leader {:?})",
                current.term,
                current.leader_hint
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        (runtime, tasks, shutdown_tx)
    }

    #[tokio::test]
    async fn every_persist_completes_before_the_send_that_follows_it() {
        let recorder = Recorder::default();
        let (runtime, mut tasks, shutdown) = leader(recorder.clone()).await;

        runtime
            .handle
            .propose(command("k"))
            .await
            .expect("leader accepts");

        let events = recorder.events();
        // Walk the log: a Send must never be the first event, and every Send must
        // have a Commit before it. Sending an AppendEntries carrying an entry
        // that is not yet on the leader's disk is the data-loss bug the ordering
        // contract exists to prevent.
        let mut committed = false;
        for event in &events {
            match event {
                Event::Commit { .. } => committed = true,
                Event::Send(_) => assert!(
                    committed,
                    "a Send left the node before any Persist completed: {events:?}"
                ),
            }
        }
        assert!(
            events.iter().any(|e| matches!(e, Event::Send(_))),
            "expected replication sends, got {events:?}"
        );

        let _ = shutdown.send(true);
        tasks.shutdown().await;
    }

    // Multi-threaded so the 32 proposer tasks can actually run concurrently with
    // the actor; on a single thread they would queue up in submission order and
    // prove nothing about batching under load.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_batch_of_proposals_costs_one_fsync() {
        let recorder = Recorder::default();
        let (runtime, mut tasks, shutdown) = leader(recorder.clone()).await;

        let before = recorder
            .events()
            .iter()
            .filter(|e| matches!(e, Event::Commit { .. }))
            .count();

        // Collected, not left lazy: a `map` that spawns is only spawned as it is
        // consumed, so awaiting inside the consuming loop would start proposal
        // i+1 after i had already completed and batch nothing.
        let handle = runtime.handle.clone();
        let proposals: Vec<_> = (0..32)
            .map(|i| {
                let handle = handle.clone();
                tokio::spawn(async move { handle.propose(command(&format!("k{i}"))).await })
            })
            .collect();

        let mut indices = Vec::new();
        for proposal in proposals {
            indices.push(proposal.await.expect("task").expect("leader accepts").index);
        }

        let commits: Vec<_> = recorder
            .events()
            .into_iter()
            .filter(|e| matches!(e, Event::Commit { .. }))
            .skip(before)
            .collect();

        // Every proposal got a distinct position.
        indices.sort_unstable();
        indices.dedup();
        assert_eq!(indices.len(), 32, "positions were reused");

        // Batching must collapse concurrent proposals into multi-entry commits.
        // Exact counts depend on arrival timing; require a clear win vs 1:1 and
        // at least one commit that carried more than a single entry.
        let multi_entry = commits
            .iter()
            .filter(|e| matches!(e, Event::Commit { entries, .. } if *entries > 1))
            .count();
        assert!(
            commits.len() <= 16,
            "32 proposals produced {} commits; expected at most 16 with batching",
            commits.len()
        );
        assert!(
            multi_entry >= 1,
            "expected at least one Commit with entries > 1, got {commits:?}"
        );

        let _ = shutdown.send(true);
        tasks.shutdown().await;
    }

    #[tokio::test]
    async fn a_follower_redirects_a_proposal_instead_of_accepting_it() {
        let recorder = Recorder::default();
        let schedule = TickSchedule::from_config(&RaftConfig::default());
        let mut tasks = JoinSet::new();
        let (shutdown, shutdown_rx) = watch::channel(false);

        // Never elected, so still a follower with no leader known.
        let runtime = spawn(
            NodeParts {
                node: RaftNode::new(1, vec![2, 3]),
                storage: RecordingStorage {
                    inner: MemoryStorage::default(),
                    recorder: recorder.clone(),
                },
                state_machine: KvStateMachine::new(),
                transport: RecordingTransport(recorder.clone()),
                schedule,
                compaction_threshold: 0,
            },
            &mut tasks,
            shutdown_rx,
        );

        let error = runtime
            .handle
            .propose(command("k"))
            .await
            .expect_err("a follower must not accept a proposal");
        assert_eq!(error, NodeError::NotLeader { leader_hint: None });

        // Nothing was written: a redirect is not a log entry.
        assert!(
            !recorder
                .events()
                .iter()
                .any(|e| matches!(e, Event::Commit { .. })),
            "follower committed something for a redirected proposal"
        );

        let _ = shutdown.send(true);
        tasks.shutdown().await;
    }

    #[test]
    fn tick_schedule_preserves_the_configured_timing_order() {
        let schedule = TickSchedule::from_config(&RaftConfig::default());

        assert!(schedule.period.as_millis() >= 1);
        assert!(
            schedule.heartbeat_ticks < schedule.election_min_ticks,
            "a heartbeat at or above the election timeout cannot keep a follower quiet"
        );
        assert!(schedule.election_min_ticks <= schedule.election_max_ticks);
        // 50ms heartbeat over ten ticks: a 5ms period, and 300..600ms becomes
        // 60..120 ticks.
        assert_eq!(schedule.heartbeat_ticks, u64::from(TICKS_PER_HEARTBEAT));
    }
}
