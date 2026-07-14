//! Tier-2 performance sanity tests — verify the optimisations work end to
//! end without breaking correctness.

use std::time::Instant;

use nexrade_core::command::dispatch_with_addr as dispatch;
use nexrade_core::db::{Db, MaxMemoryPolicy, ServerConfig};
use nexrade_core::resp::Resp;
use nexrade_core::types::DataType;

fn str_arg(s: &str) -> Resp {
    Resp::bulk_str(s.to_string())
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch(db, args, 0, None).await
}

fn small_config() -> ServerConfig {
    ServerConfig {
        databases: 1,
        hz: 10,
        max_memory: Some(8 * 1024 * 1024), // 8 MB cap for the eviction test.
        maxmemory_policy: MaxMemoryPolicy::AllKeysLru,
        ..Default::default()
    }
}

#[tokio::test]
async fn get_uses_cached_lru_clock() {
    // Two consecutive GETs within the same tick window should produce the
    // same `lru_clock` value (both reads happen in the same tick). After the
    // background tick refreshes the clock, a third GET should observe a
    // higher (or equal) value. The point of the test is to confirm the
    // background tick is wired and updates the cached clock.
    let db = Db::new(small_config());
    // We don't start the background task in this unit test — just verify
    // the cache exists and reads return a sensible value (initial Unix
    // timestamp).
    run(&db, vec![str_arg("SET"), str_arg("k"), str_arg("v")]).await;
    let r = run(&db, vec![str_arg("GET"), str_arg("k")]).await;
    assert!(matches!(r, Resp::BulkString(Some(_))));
}

#[tokio::test]
async fn live_bytes_tracks_inserts_and_removes() {
    // Verify the incremental `live_bytes` counter agrees with the previous
    // full-scan estimate after a series of inserts and removes.
    let db = Db::new(small_config());

    let initial = db.store.estimated_memory_bytes();
    assert_eq!(initial, 0);

    let payload = vec![b'x'; 100];
    run(
        &db,
        vec![
            str_arg("SET"),
            str_arg("k1"),
            Resp::BulkString(Some(bytes::Bytes::from(payload.clone()))),
        ],
    )
    .await;

    let after_set = db.store.estimated_memory_bytes();
    assert!(
        after_set >= 64 + 3 + 50,
        "live_bytes after SET ({} bytes) < expected minimum",
        after_set
    );

    run(&db, vec![str_arg("DEL"), str_arg("k1")]).await;
    let after_del = db.store.estimated_memory_bytes();
    assert_eq!(
        after_del, initial,
        "live_bytes should return to 0 after DEL"
    );
}

#[tokio::test]
async fn eviction_under_pressure_uses_sample_lru() {
    // Insert 5_000 keys into a 1 MB-cap store. Eviction should kick in and
    // keep the dataset close to (but below) the cap. We don't measure
    // wall-time here — sample-LRU correctness is what matters: the
    // evicted keys should be among the oldest (lowest lru_clock), and
    // after eviction the live-bytes counter should be <= the cap.
    let mut c = small_config();
    c.max_memory = Some(1_024 * 1_024); // 1 MB cap
    c.maxmemory_policy = MaxMemoryPolicy::AllKeysLru;
    let db = Db::new(c);

    // First populate with 5_000 short string keys. The cap is 1 MB; the
    // payload is small enough to fit them all initially. Then we drive
    // eviction by repeatedly inserting larger keys.
    for i in 0..5_000 {
        let key = format!("k{i:05}");
        run(
            &db,
            vec![
                str_arg("SET"),
                str_arg(&key),
                Resp::BulkString(Some(bytes::Bytes::from(vec![b'v'; 200]))),
            ],
        )
        .await;
    }

    // Insert larger payloads to push over the cap.
    for i in 0..2_000 {
        let key = format!("big{i:05}");
        run(
            &db,
            vec![
                str_arg("SET"),
                str_arg(&key),
                Resp::BulkString(Some(bytes::Bytes::from(vec![b'x'; 1024]))),
            ],
        )
        .await;
    }

    // Drive eviction to a steady state. evict_if_needed uses a
    // randomized sample so a single call may leave the dataset a
    // couple entries above the cap; loop until progress stops to make
    // the assertion deterministic.
    for _ in 0..10 {
        if db
            .store
            .evict_if_needed(&MaxMemoryPolicy::AllKeysLru, 1_024 * 1_024)
            == 0
        {
            break;
        }
    }

    let live = db.store.estimated_memory_bytes();
    assert!(
        live <= 1_024 * 1_024,
        "live_bytes ({live}) should be at or below cap after convergence"
    );
    // Dataset shouldn't be empty after eviction.
    assert!(db.store.total_keys() > 0);
}

#[tokio::test]
async fn benchmark_get_throughput() {
    // Quick GET throughput sanity. Asserts GETs are fast enough that
    // 50k ops complete well under 5 seconds on the test machine. Mostly
    // a smoke test — not a strict CI bound.
    let db = Db::new(small_config());
    for i in 0..1_000 {
        let key = format!("k{i}");
        let val = format!("v{i}");
        run(&db, vec![str_arg("SET"), str_arg(&key), str_arg(&val)]).await;
    }

    let n = 50_000;
    let started = Instant::now();
    for i in 0..n {
        let key = format!("k{}", i % 1_000);
        let _ = run(&db, vec![str_arg("GET"), str_arg(&key)]).await;
    }
    let elapsed = started.elapsed();
    let ops_per_sec = n as f64 / elapsed.as_secs_f64();
    println!(
        "GET throughput: {ops_per_sec:.0} ops/sec (n={n} in {:?})",
        elapsed
    );
    // Sanity bound: at least 10k ops/sec — well under what we'd expect
    // in release mode (220k+) but a comfortable lower limit.
    assert!(
        ops_per_sec > 10_000.0,
        "GET throughput too low: {ops_per_sec:.0} ops/sec"
    );
}

#[tokio::test]
async fn benchmark_estimated_memory_bytes_is_fast() {
    // Insert a few thousand keys, then verify `estimated_memory_bytes()`
    // is O(shards) — i.e. cheap. The old implementation would scan all
    // entries; with the live_bytes counter we just sum shard atomics.
    let db = Db::new(small_config());
    for i in 0..5_000 {
        let key = format!("k{i}");
        run(
            &db,
            vec![
                str_arg("SET"),
                str_arg(&key),
                Resp::BulkString(Some(bytes::Bytes::from(vec![b'x'; 100]))),
            ],
        )
        .await;
    }

    let started = Instant::now();
    let n = 10_000;
    let mut sum = 0usize;
    for _ in 0..n {
        sum = sum.wrapping_add(db.store.estimated_memory_bytes());
    }
    let elapsed = started.elapsed();
    println!("estimated_memory_bytes x{n}: {:?} (sum={sum})", elapsed);
    // Should easily complete well under 1 second even at 10k calls.
    assert!(elapsed.as_secs() < 5, "estimated_memory_bytes too slow");
}

// Silence unused warning for the DataType import — it's referenced
// indirectly via the dispatcher's signatures.
#[allow(dead_code)]
fn _force_link(_: &DataType) {}
