//! KV API end to end: `KvClient` → gRPC → `KvServiceImpl` → Raft → apply.
//!
//! Same in-process harness as `tests/cluster.rs`, with the KV server running on
//! every node.

mod common;

use std::time::Duration;

use common::TestCluster;
use ferrite::client::ClientError;
use ferrite::proto::kv as pb;

const ELECTION: Duration = Duration::from_secs(5);
/// Long enough to ride out one election at the harness's 300–600ms timeouts.
const REQUEST: Duration = Duration::from_secs(5);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_redirects_and_the_write_is_readable() {
    let cluster = TestCluster::start(3, &[1, 2, 3]).await;
    let leader = cluster.await_leader(ELECTION).await;
    let follower = (1..=3).find(|&id| id != leader).expect("a follower");

    let mut client = cluster.client(follower, REQUEST);
    let put = client
        .put(b"k", b"v")
        .await
        .expect("put through a follower");
    assert_eq!(put.version, 1);
    assert_eq!(put.previous, None);

    let got = client.get(b"k").await.expect("get");
    assert_eq!(got.value.as_deref(), Some(b"v".as_slice()));
    assert_eq!(got.version, Some(1));

    cluster.shutdown().await;
}

/// The exactly-once test. A raw stub controls `seq_num`, which `KvClient`
/// deliberately hides.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retried_put_across_a_leader_change_applies_once() {
    let mut cluster = TestCluster::start(3, &[1, 2, 3]).await;
    let first_leader = cluster.await_leader(ELECTION).await;

    let put = || pb::PutRequest {
        meta: Some(pb::ClientMeta {
            client_id: 7,
            seq_num: 1,
        }),
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    };

    let first = cluster
        .kv_stub(first_leader)
        .put(put())
        .await
        .expect("first attempt")
        .into_inner();

    // As if the response were lost: the client resends the same seq, now to a
    // different leader.
    cluster.stop(first_leader).await;
    let second_leader = cluster.await_leader(ELECTION).await;
    assert_ne!(second_leader, first_leader);

    let retry = cluster
        .kv_stub(second_leader)
        .put(put())
        .await
        .expect("retry")
        .into_inner();
    assert_eq!(first, retry, "a retry must replay the original result");

    let mut client = cluster.client(second_leader, REQUEST);
    let got = client.get(b"k").await.expect("get");
    assert_eq!(got.version, Some(1), "the retried put was applied twice");

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_cas_on_one_version_has_one_winner() {
    let cluster = TestCluster::start(3, &[1, 2, 3]).await;
    let leader = cluster.await_leader(ELECTION).await;

    let mut seed = cluster.client(leader, REQUEST);
    seed.put(b"k", b"v0").await.expect("seed");

    let racers: Vec<_> = (0..5)
        .map(|i| {
            let mut client = cluster.client(leader, REQUEST);
            let value = format!("v{i}").into_bytes();
            tokio::spawn(async move { client.cas(b"k", Some(1), Some(&value)).await })
        })
        .collect();

    let mut winners = 0;
    for racer in racers {
        if racer.await.expect("task").expect("cas").applied {
            winners += 1;
        }
    }
    assert_eq!(winners, 1, "every racer expected version 1");

    let got = seed.get(b"k").await.expect("get");
    assert_eq!(got.version, Some(2));

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_client_rides_through_a_leader_failure() {
    let mut cluster = TestCluster::start(3, &[1, 2, 3]).await;
    let leader = cluster.await_leader(ELECTION).await;

    let mut client = cluster.client(leader, REQUEST);
    client.put(b"a", b"1").await.expect("before failover");

    cluster.stop(leader).await;
    // Same client, same cached leader: it has to discover the stop on its own.
    client.put(b"b", b"2").await.expect("after failover");

    let a = client.get(b"a").await.expect("get a");
    let b = client.get(b"b").await.expect("get b");
    assert_eq!(a.value.as_deref(), Some(b"1".as_slice()));
    assert_eq!(b.value.as_deref(), Some(b"2".as_slice()));

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_quorum_times_out_instead_of_hanging() {
    // One of three running: no majority, so no leader and no commit, ever.
    let cluster = TestCluster::start(3, &[1]).await;
    let deadline = Duration::from_secs(1);

    let mut client = cluster.client(1, deadline);
    let started = tokio::time::Instant::now();
    let error = client.put(b"k", b"v").await.expect_err("no quorum");

    assert!(matches!(error, ClientError::Timeout { .. }), "{error}");
    assert!(
        started.elapsed() < deadline + Duration::from_secs(1),
        "took {:?} against a {deadline:?} deadline",
        started.elapsed()
    );

    cluster.shutdown().await;
}

/// Acceptance: `cas` with `expected` absent succeeds only if the key is absent,
/// and two concurrent attempts produce one success and one mismatch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cas_with_expected_absent_creates_the_key_once() {
    let cluster = TestCluster::start(3, &[1, 2, 3]).await;
    let leader = cluster.await_leader(ELECTION).await;

    let racers: Vec<_> = (0..2)
        .map(|i| {
            let mut client = cluster.client(leader, REQUEST);
            let value = format!("v{i}").into_bytes();
            tokio::spawn(async move { client.cas(b"k", None, Some(&value)).await })
        })
        .collect();

    let mut outcomes = Vec::new();
    for racer in racers {
        outcomes.push(racer.await.expect("task").expect("cas"));
    }
    let winners = outcomes.iter().filter(|r| r.applied).count();
    assert_eq!(winners, 1, "exactly one create-if-absent may win");

    let loser = outcomes.iter().find(|r| !r.applied).expect("a mismatch");
    assert_eq!(
        loser.actual_version,
        Some(1),
        "the mismatch reports the winner's version"
    );

    cluster.shutdown().await;
}

/// Acceptance: with no leader known, the client backs off and succeeds once a
/// leader is elected, within its deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_client_waits_out_a_missing_leader_within_its_deadline() {
    let mut cluster = TestCluster::start(3, &[1]).await;
    let mut client = cluster.client(1, REQUEST);

    let started = tokio::time::Instant::now();
    let put = tokio::spawn(async move { client.put(b"k", b"v").await });

    // No quorum yet: the put can only be backing off.
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(!put.is_finished(), "put finished with no leader possible");
    cluster.restart(2).await;

    let reply = put.await.expect("task").expect("put once a leader exists");
    assert_eq!(reply.version, 1);
    assert!(started.elapsed() < REQUEST);

    cluster.shutdown().await;
}

/// Review fix: a timed-out operation is not abandoned. The next call resends it
/// under its own seq before starting anything new.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unconfirmed_put_is_resent_before_the_next_operation() {
    let mut cluster = TestCluster::start(3, &[1]).await;
    let mut client = cluster.client(1, Duration::from_millis(1500));

    let error = client.put(b"k", b"v").await.expect_err("no quorum");
    assert!(
        matches!(error, ClientError::Timeout { op: "put", .. }),
        "{error}"
    );

    cluster.restart(2).await;
    cluster.await_leader(ELECTION).await;

    // The put never reached a leader. If the client had advanced past it, this
    // get would find nothing.
    let got = client.get(b"k").await.expect("get");
    assert_eq!(got.value.as_deref(), Some(b"v".as_slice()));
    assert_eq!(got.version, Some(1));

    cluster.shutdown().await;
}

/// Acceptance: a put in flight across leader loss never surfaces another
/// command's result. Overwrite classification itself is unit-tested on
/// `classify_apply` (same index / different term → Overwritten).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_put_in_flight_across_leader_loss_does_not_return_a_foreign_result() {
    let mut cluster = TestCluster::start(3, &[1, 2, 3]).await;
    let leader = cluster.await_leader(ELECTION).await;

    let mut writer = cluster.client(leader, REQUEST);
    writer.put(b"other", b"seed").await.expect("seed");

    let mut racing = cluster.client(leader, Duration::from_millis(2500));
    let put = tokio::spawn(async move { racing.put(b"k", b"v").await });

    tokio::time::sleep(Duration::from_millis(20)).await;
    cluster.stop(leader).await;
    let new_leader = cluster.await_leader(ELECTION).await;

    match put.await.expect("task") {
        Ok(reply) => {
            // Success must be this put, not a mis-attributed apply of `other`.
            assert_eq!(reply.version, 1);
            assert_eq!(reply.previous, None);
            let mut check = cluster.client(new_leader, REQUEST);
            let got = check.get(b"k").await.expect("get k");
            assert_eq!(got.value.as_deref(), Some(b"v".as_slice()));
            let other = check.get(b"other").await.expect("get other");
            assert_eq!(other.value.as_deref(), Some(b"seed".as_slice()));
        }
        // Timeout carries no result payload; retry/idempotency is covered elsewhere.
        Err(ClientError::Timeout { op: "put", .. }) => {}
        Err(other) => panic!("unexpected client error: {other}"),
    }

    cluster.shutdown().await;
}
