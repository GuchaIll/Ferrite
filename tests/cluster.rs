//! In-process cluster tests: real gRPC, real actors, real disk.
//!
//! Each node is the same assembly `ferrite run` builds — `spawn_node` plus a
//! tonic server — so these tests exercise the production path rather than a
//! stand-in for it. Ports come from listeners the harness already bound, so
//! nothing is guessed and parallel test binaries cannot collide.
//!
//! # Stop vs process kill
//!
//! [`TestCluster::stop`] is a cooperative shutdown (`watch` → actor exit →
//! server stop). That is enough to exercise failover and restart-from-disk.
//! It is **not** `SIGKILL` mid-`spawn_blocking`: process death while a commit
//! is in flight is covered by the durability crash/restart scenarios and by
//! manual multi-process runs, not by these in-process tests.
//!
//! # Restart and the state machine
//!
//! `spawn_node` recovers the Raft log and hard state, then builds a **fresh**
//! [`ferrite::kv::KvStateMachine`]. Catch-up after restart is therefore driven
//! by AppendEntries (log identity + apply stream), not by an instantly warm KV
//! map. Assert durable log / applied indices here; full SM-on-disk is later work.

mod common;

use std::collections::BTreeMap;
use std::time::Duration;

use common::TestCluster;
use ferrite::{
    kv::{ClientRequest, Command, CommandResult},
    server::{Applied, NodeError, NodeHandle},
};

fn set(key: &str, value: &str) -> Vec<u8> {
    ClientRequest::internal(Command::Set {
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
    })
    .encode()
    .expect("encode")
}

/// Collects applies up to `count` non-noop entries, or panics on timeout.
async fn collect_applies(
    handle: &NodeHandle,
    mut stream: tokio::sync::broadcast::Receiver<Applied>,
    count: usize,
    within: Duration,
) -> Vec<Applied> {
    let mut collected = Vec::with_capacity(count);
    let deadline = tokio::time::Instant::now() + within;

    while collected.len() < count {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "node {} applied only {}/{count} in {within:?}",
            handle.id(),
            collected.len()
        );

        match tokio::time::timeout(remaining, stream.recv()).await {
            // Leader no-ops carry no client command and are not what a client
            // proposed; skip them without counting.
            Ok(Ok(applied)) if applied.result == CommandResult::Noop => {}
            Ok(Ok(applied)) => collected.push(applied),
            Ok(Err(error)) => panic!("apply stream broke on node {}: {error}", handle.id()),
            Err(_) => {}
        }
    }

    collected
}

/// Waits until a running node's apply stream has seen `index` (any entry type).
async fn await_applied_index(
    handle: &NodeHandle,
    mut stream: tokio::sync::broadcast::Receiver<Applied>,
    index: u64,
    within: Duration,
) {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "node {} never applied through index {index}",
            handle.id()
        );
        match tokio::time::timeout(remaining, stream.recv()).await {
            Ok(Ok(applied)) if applied.index >= index => return,
            Ok(Ok(_)) => {}
            Ok(Err(error)) => panic!("apply stream broke on node {}: {error}", handle.id()),
            Err(_) => {}
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_elect_exactly_one_leader() {
    let cluster = TestCluster::start(3, &[1, 2, 3]).await;

    let leader = cluster.await_leader(Duration::from_secs(2)).await;
    let leaders = cluster.leaders();

    assert_eq!(
        leaders,
        vec![leader],
        "expected exactly one leader, got {leaders:?}"
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hundred_commands_apply_in_the_same_order_on_every_node() {
    const COMMANDS: usize = 100;

    let cluster = TestCluster::start(3, &[1, 2, 3]).await;
    let leader_id = cluster.await_leader(Duration::from_secs(2)).await;

    // Subscribe before proposing: a subscription taken afterwards can miss the
    // applies it is waiting for.
    let mut streams = BTreeMap::new();
    for (&id, handle) in &cluster.nodes {
        streams.insert(id, handle.subscribe_apply());
    }

    let leader = &cluster.nodes[&leader_id];
    for i in 0..COMMANDS {
        leader
            .propose(set(&format!("k{i}"), &format!("v{i}")))
            .await
            .expect("leader accepts proposal");
    }

    let mut per_node = BTreeMap::new();
    for (id, stream) in streams {
        let applied = collect_applies(
            &cluster.nodes[&id],
            stream,
            COMMANDS,
            Duration::from_secs(20),
        )
        .await;
        per_node.insert(id, applied);
    }

    // Same commands, same order, same positions on all three.
    let reference = &per_node[&leader_id];
    let indices: Vec<u64> = reference.iter().map(|a| a.index).collect();
    assert!(
        indices.windows(2).all(|w| w[0] < w[1]),
        "applies were not in index order: {indices:?}"
    );

    for (id, applied) in &per_node {
        let their_indices: Vec<u64> = applied.iter().map(|a| a.index).collect();
        assert_eq!(
            their_indices, indices,
            "node {id} applied a different order"
        );
        let their_results: Vec<&CommandResult> = applied.iter().map(|a| &a.result).collect();
        let reference_results: Vec<&CommandResult> = reference.iter().map(|a| &a.result).collect();
        assert_eq!(
            their_results, reference_results,
            "node {id} applied different results"
        );
    }

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_black_holed_peer_does_not_stop_the_others() {
    // Node 3 is configured and addressed but never started: connections to it
    // are refused forever. Two of three is still a majority, so replication must
    // proceed at full speed rather than waiting on the dead peer.
    let cluster = TestCluster::start(3, &[1, 2]).await;
    let leader_id = cluster.await_leader(Duration::from_secs(3)).await;

    let stream = cluster.nodes[&leader_id].subscribe_apply();
    let leader = &cluster.nodes[&leader_id];

    let started = tokio::time::Instant::now();
    for i in 0..20 {
        leader
            .propose(set(&format!("k{i}"), "v"))
            .await
            .expect("leader accepts proposal");
    }
    let applied = collect_applies(leader, stream, 20, Duration::from_secs(10)).await;
    let elapsed = started.elapsed();

    assert_eq!(applied.len(), 20);
    // The dead peer's rpc timeout is 100ms. If replication were serialized
    // behind it, 20 commands could not finish in anything near this budget.
    assert!(
        elapsed < Duration::from_secs(5),
        "20 commands took {elapsed:?} with one black-holed peer"
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn survivors_elect_a_new_leader_and_keep_committing() {
    let mut cluster = TestCluster::start(3, &[1, 2, 3]).await;
    let first_leader = cluster.await_leader(Duration::from_secs(2)).await;

    cluster.nodes[&first_leader]
        .propose(set("before", "1"))
        .await
        .expect("first leader accepts");

    cluster.stop(first_leader).await;

    // Two of three remain: a majority, so a new term must be won.
    let second_leader = cluster.await_leader(Duration::from_secs(5)).await;
    assert_ne!(second_leader, first_leader);

    let handle = cluster.nodes[&second_leader].clone();
    let stream = handle.subscribe_apply();
    handle
        .propose(set("after", "2"))
        .await
        .expect("new leader accepts");

    let applied = collect_applies(&handle, stream, 1, Duration::from_secs(10)).await;
    assert_eq!(applied.len(), 1, "new leader did not commit after failover");

    cluster.shutdown().await;
}

/// Issue 04 acceptance: after the leader is stopped, survivors keep committing,
/// and the restarted node catches up to the new leader's log.
///
/// Catch-up is asserted on applied indices and on the **durable Raft log** after
/// both sides are stopped — not on an instantly warm KV store. Restart rebuilds
/// the SM empty; the leader then drives AE / `leaderCommit` until the follower
/// re-applies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopped_leader_restarts_and_catches_up_to_new_leader_log() {
    let mut cluster = TestCluster::start(3, &[1, 2, 3]).await;
    let first_leader = cluster.await_leader(Duration::from_secs(2)).await;

    let survivor = *cluster
        .nodes
        .keys()
        .find(|&&id| id != first_leader)
        .expect("survivor");
    let survivor_stream = cluster.nodes[&survivor].subscribe_apply();

    let before = cluster.nodes[&first_leader]
        .propose(set("before", "1"))
        .await
        .expect("first leader accepts");

    // propose only means durable on the leader; wait until a survivor applies.
    await_applied_index(
        &cluster.nodes[&survivor],
        survivor_stream,
        before.index,
        Duration::from_secs(10),
    )
    .await;

    cluster.stop(first_leader).await;

    let second_leader = cluster.await_leader(Duration::from_secs(5)).await;
    assert_ne!(second_leader, first_leader);

    let leader_handle = cluster.nodes[&second_leader].clone();
    // Keep this receiver: a subscription taken after proposal can miss a fast
    // commit and then wait forever for an entry that has already applied.
    let leader_applies = leader_handle.subscribe_apply();

    const AFTER: usize = 5;
    let mut last_after = before;
    for i in 0..AFTER {
        last_after = leader_handle
            .propose(set(&format!("after{i}"), "x"))
            .await
            .expect("new leader accepts");
    }
    let applied = collect_applies(
        &leader_handle,
        leader_applies,
        AFTER,
        Duration::from_secs(15),
    )
    .await;
    assert_eq!(applied.len(), AFTER);
    let target_index = last_after.index;
    assert!(
        applied.iter().any(|a| a.index == target_index),
        "leader never applied the last post-failover entry"
    );

    // Restart the old leader from the same data dir / Raft address.
    let restarted_applies = cluster.restart(first_leader).await;
    let restarted = cluster.nodes[&first_leader].clone();
    await_applied_index(
        &restarted,
        restarted_applies,
        target_index,
        Duration::from_secs(15),
    )
    .await;

    // Strong check: stop both and compare full durable entry lists (index, term,
    // command). Never open DiskStorage while the actor still owns the dir.
    cluster.stop(first_leader).await;
    cluster.stop(second_leader).await;

    let restarted_log = cluster.durable_log_entries(first_leader);
    let leader_log = cluster.durable_log_entries(second_leader);
    assert_eq!(
        restarted_log, leader_log,
        "restarted node durable log diverged from the leader that advanced while it was down"
    );
    assert!(
        restarted_log.iter().any(|e| e.index == target_index),
        "caught-up log missing post-failover index {target_index}"
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_refuses_a_proposal_and_names_the_leader() {
    let cluster = TestCluster::start(3, &[1, 2, 3]).await;
    let leader_id = cluster.await_leader(Duration::from_secs(2)).await;

    let follower_id = *cluster
        .nodes
        .keys()
        .find(|&&id| id != leader_id)
        .expect("a follower exists");

    let error = cluster.nodes[&follower_id]
        .propose(set("k", "v"))
        .await
        .expect_err("a follower must not accept a proposal");

    // The hint is what lets a client retry against the right node without the
    // user doing anything. Issue 05 turns it into a NotLeader status.
    match error {
        NodeError::NotLeader { leader_hint } => {
            assert_eq!(
                leader_hint,
                Some(leader_id),
                "follower did not name the current leader"
            );
        }
        other => panic!("expected NotLeader, got {other:?}"),
    }

    cluster.shutdown().await;
}
