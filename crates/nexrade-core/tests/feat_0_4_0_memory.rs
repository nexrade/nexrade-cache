//! 0.4.0 — live_bytes tracks dual-encoded collection mutations.
//!
//! Before 0.4.0, LPUSH/HSET/SADD/ZADD only paid for the empty key shell
//! (insert/get_or_insert_with). Payload growth was invisible to maxmemory.

use nexrade_core::command::dispatch;
use nexrade_core::db::{Db, MaxMemoryPolicy, ServerConfig};
use nexrade_core::resp::Resp;

fn small_config() -> ServerConfig {
    ServerConfig {
        databases: 1,
        max_memory: None,
        maxmemory_policy: MaxMemoryPolicy::NoEviction,
        ..ServerConfig::default()
    }
}

fn str_arg(s: &str) -> Resp {
    Resp::BulkString(Some(bytes::Bytes::from(s.to_string())))
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch(db, args, 0).await
}

#[tokio::test]
async fn live_bytes_tracks_lpush_and_lpop() {
    let db = Db::new(small_config());
    let initial = db.store.estimated_memory_bytes();
    assert_eq!(initial, 0);

    // 100 × 10-byte payloads
    for i in 0..100 {
        let _ = run(
            &db,
            vec![
                str_arg("LPUSH"),
                str_arg("mylist"),
                str_arg(&format!("{:010}", i)),
            ],
        )
        .await;
    }
    let after_push = db.store.estimated_memory_bytes();
    assert!(
        after_push >= 1000,
        "live_bytes after 100×10B LPUSH ({after_push}) < 1000 payload bytes"
    );

    for _ in 0..100 {
        let _ = run(&db, vec![str_arg("LPOP"), str_arg("mylist")]).await;
    }
    let after_pop = db.store.estimated_memory_bytes();
    // 0.4.2: empty list key is deleted (Redis-style), so live_bytes → 0.
    assert_eq!(
        after_pop, 0,
        "live_bytes after LPOP all ({after_pop}) should be 0 after empty-key GC"
    );
}

#[tokio::test]
async fn live_bytes_tracks_hset_hdel() {
    let db = Db::new(small_config());

    for i in 0..50 {
        let _ = run(
            &db,
            vec![
                str_arg("HSET"),
                str_arg("h"),
                str_arg(&format!("f{i:03}")),
                str_arg(&format!("{:020}", i)),
            ],
        )
        .await;
    }
    let after = db.store.estimated_memory_bytes();
    // 50 × (field ~4 + value 20) ≥ 1200
    assert!(
        after >= 1000,
        "live_bytes after HSET ({after}) undercounts hash payload"
    );

    for i in 0..50 {
        let _ = run(
            &db,
            vec![str_arg("HDEL"), str_arg("h"), str_arg(&format!("f{i:03}"))],
        )
        .await;
    }
    let after_del = db.store.estimated_memory_bytes();
    assert!(
        after_del < after / 2,
        "live_bytes after HDEL all ({after_del}) did not drop enough from {after}"
    );
}

#[tokio::test]
async fn live_bytes_tracks_sadd_srem() {
    let db = Db::new(small_config());

    for i in 0..80 {
        let _ = run(
            &db,
            vec![
                str_arg("SADD"),
                str_arg("s"),
                str_arg(&format!("member-{:04}", i)),
            ],
        )
        .await;
    }
    let after = db.store.estimated_memory_bytes();
    assert!(
        after >= 80 * 10,
        "live_bytes after SADD ({after}) undercounts set payload"
    );

    for i in 0..80 {
        let _ = run(
            &db,
            vec![
                str_arg("SREM"),
                str_arg("s"),
                str_arg(&format!("member-{:04}", i)),
            ],
        )
        .await;
    }
    let after_rem = db.store.estimated_memory_bytes();
    assert!(
        after_rem < after / 2,
        "live_bytes after SREM all ({after_rem}) did not drop enough from {after}"
    );
}

#[tokio::test]
async fn live_bytes_tracks_zadd_zrem() {
    let db = Db::new(small_config());

    for i in 0..60 {
        let _ = run(
            &db,
            vec![
                str_arg("ZADD"),
                str_arg("z"),
                str_arg(&format!("{i}")),
                str_arg(&format!("m{:04}", i)),
            ],
        )
        .await;
    }
    let after = db.store.estimated_memory_bytes();
    assert!(
        after >= 60 * 5,
        "live_bytes after ZADD ({after}) undercounts zset payload"
    );

    for i in 0..60 {
        let _ = run(
            &db,
            vec![
                str_arg("ZREM"),
                str_arg("z"),
                str_arg(&format!("m{:04}", i)),
            ],
        )
        .await;
    }
    let after_rem = db.store.estimated_memory_bytes();
    assert!(
        after_rem < after / 2,
        "live_bytes after ZREM all ({after_rem}) did not drop enough from {after}"
    );
}

#[tokio::test]
async fn eviction_bounds_list_payload() {
    let mut c = small_config();
    c.max_memory = Some(64 * 1024); // 64 KiB
    c.maxmemory_policy = MaxMemoryPolicy::AllKeysLru;
    let db = Db::new(c);

    // Flood with large list elements — without 0.4.0 accounting this never
    // triggers eviction because live_bytes stays near empty-key size.
    for i in 0..2000 {
        let _ = run(
            &db,
            vec![
                str_arg("LPUSH"),
                str_arg(&format!("list{i}")),
                Resp::BulkString(Some(bytes::Bytes::from(vec![b'x'; 256]))),
            ],
        )
        .await;
    }

    for _ in 0..20 {
        if db
            .store
            .evict_if_needed(&MaxMemoryPolicy::AllKeysLru, 64 * 1024)
            .is_empty()
        {
            break;
        }
    }

    let live = db.store.estimated_memory_bytes();
    assert!(
        live <= 64 * 1024 + 4096, // small slack for last write before next check
        "live_bytes ({live}) far above 64KiB cap after list-heavy load"
    );
}

#[tokio::test]
async fn lmove_tracks_payload_across_keys() {
    let db = Db::new(small_config());
    let _ = run(
        &db,
        vec![
            str_arg("LPUSH"),
            str_arg("src"),
            str_arg("abcdefghij"), // 10 bytes
        ],
    )
    .await;
    let after_push = db.store.estimated_memory_bytes();
    assert!(after_push >= 10, "src payload missing: {after_push}");

    let resp = run(
        &db,
        vec![
            str_arg("LMOVE"),
            str_arg("src"),
            str_arg("dst"),
            str_arg("LEFT"),
            str_arg("LEFT"),
        ],
    )
    .await;
    match resp {
        Resp::BulkString(Some(b)) => assert_eq!(b.as_ref(), b"abcdefghij"),
        other => panic!("expected bulk, got {other:?}"),
    }
    let after_move = db.store.estimated_memory_bytes();
    // Payload still present (now on dst); must not collapse to near-zero.
    assert!(
        after_move >= 10,
        "live_bytes after LMOVE ({after_move}) lost payload"
    );
    // Moving to empty dst creates a shell; total should stay in the same ballpark.
    assert!(
        after_move < after_push + 512,
        "live_bytes ballooned after LMOVE: before={after_push} after={after_move}"
    );
}

#[tokio::test]
async fn lmove_wrongtype_dst_preserves_src() {
    let db = Db::new(small_config());
    let _ = run(&db, vec![str_arg("LPUSH"), str_arg("src"), str_arg("elem")]).await;
    let _ = run(&db, vec![str_arg("SET"), str_arg("dst"), str_arg("str")]).await;
    let before = db.store.estimated_memory_bytes();

    let resp = run(
        &db,
        vec![
            str_arg("LMOVE"),
            str_arg("src"),
            str_arg("dst"),
            str_arg("LEFT"),
            str_arg("LEFT"),
        ],
    )
    .await;
    match resp {
        Resp::Error(e) => assert!(
            e.to_ascii_lowercase().contains("wrong") || e.to_ascii_lowercase().contains("type"),
            "unexpected error: {e}"
        ),
        other => panic!("expected WRONGTYPE error, got {other:?}"),
    }

    // Source element must still be present.
    let llen = run(&db, vec![str_arg("LLEN"), str_arg("src")]).await;
    assert_eq!(llen, Resp::int(1));
    let after = db.store.estimated_memory_bytes();
    assert_eq!(
        after, before,
        "wrong-type LMOVE must not change live_bytes (before={before} after={after})"
    );
}

#[tokio::test]
async fn smove_tracks_payload() {
    let db = Db::new(small_config());
    let _ = run(
        &db,
        vec![str_arg("SADD"), str_arg("s1"), str_arg("member-xyz")],
    )
    .await;
    let after_add = db.store.estimated_memory_bytes();
    let resp = run(
        &db,
        vec![
            str_arg("SMOVE"),
            str_arg("s1"),
            str_arg("s2"),
            str_arg("member-xyz"),
        ],
    )
    .await;
    assert_eq!(resp, Resp::int(1));
    let after_move = db.store.estimated_memory_bytes();
    assert!(
        after_move >= 10,
        "SMOVE lost payload accounting: {after_move}"
    );
    // Shell for new set + member should not be wildly larger than source alone.
    assert!(
        after_move < after_add + 512,
        "SMOVE ballooned: before={after_add} after={after_move}"
    );
}

#[tokio::test]
async fn live_bytes_tracks_lrem_and_ltrim() {
    let db = Db::new(small_config());
    for s in ["aa", "bb", "aa", "cc", "aa"] {
        let _ = run(&db, vec![str_arg("RPUSH"), str_arg("lst"), str_arg(s)]).await;
    }
    let after_push = db.store.estimated_memory_bytes();
    // 2+2+2+2+2 = 10 payload
    assert!(after_push >= 10, "RPUSH undercount: {after_push}");

    let rem = run(
        &db,
        vec![str_arg("LREM"), str_arg("lst"), str_arg("2"), str_arg("aa")],
    )
    .await;
    assert_eq!(rem, Resp::int(2));
    let after_rem = db.store.estimated_memory_bytes();
    // Removed 4 bytes; must drop by at least that much.
    assert!(
        after_rem + 4 <= after_push,
        "LREM did not shrink live_bytes: before={after_push} after={after_rem}"
    );

    // Keep only middle element ("bb" or whatever remains) via LTRIM 0 0 → one elem left
    // After LREM 2×"aa": list is bb, cc, aa (3 elems, 2+2+2=6)
    let trim = run(
        &db,
        vec![str_arg("LTRIM"), str_arg("lst"), str_arg("1"), str_arg("1")],
    )
    .await;
    assert_eq!(trim, Resp::ok());
    let after_trim = db.store.estimated_memory_bytes();
    // Dropped two 2-byte elems; one 2-byte remains.
    assert!(
        after_trim + 4 <= after_rem,
        "LTRIM did not shrink live_bytes: before={after_rem} after={after_trim}"
    );

    // Empty the list — empty-key GC → live_bytes → 0 for this key.
    let _ = run(
        &db,
        vec![str_arg("LTRIM"), str_arg("lst"), str_arg("1"), str_arg("0")],
    )
    .await;
    let after_empty = db.store.estimated_memory_bytes();
    assert_eq!(
        after_empty, 0,
        "live_bytes after empty LTRIM ({after_empty}) should be 0"
    );
}

#[tokio::test]
async fn promote_list_does_not_inflate_live_bytes() {
    // Compact → Linked must not create a false live_bytes delta (payload is
    // content-only; capacity is not counted).
    let db = Db::new(small_config());
    // Force promote with an oversize element after a small seed.
    let _ = run(&db, vec![str_arg("LPUSH"), str_arg("p"), str_arg("tiny")]).await;
    let before = db.store.estimated_memory_bytes();
    let big = "x".repeat(512); // > LIST_COMPACT_MAX_ELEM (256)
    let _ = run(&db, vec![str_arg("LPUSH"), str_arg("p"), str_arg(&big)]).await;
    let after = db.store.estimated_memory_bytes();
    // Growth should be ~512 payload bytes, not a multi-KB phantom jump.
    let delta = after.saturating_sub(before);
    assert!(
        (512..512 + 256).contains(&delta),
        "promote inflated live_bytes by {delta} (before={before} after={after})"
    );
}

#[tokio::test]
async fn live_bytes_tracks_large_hash_after_promote() {
    let db = Db::new(small_config());
    // Oversized value forces Hashtable encoding; subsequent HSETs must still
    // grow live_bytes (O(1) cache, not a re-scan that could be skipped).
    let big = "v".repeat(128); // > HASH_COMPACT_MAX_VALUE (64)
    for i in 0..40 {
        let _ = run(
            &db,
            vec![
                str_arg("HSET"),
                str_arg("fat"),
                str_arg(&format!("f{i:03}")),
                str_arg(&big),
            ],
        )
        .await;
    }
    let after = db.store.estimated_memory_bytes();
    // 40 × (~4 field + 128 value) ≥ 5000
    assert!(
        after >= 5000,
        "live_bytes after fat HSET ({after}) undercounts Hashtable payload"
    );

    for i in 0..40 {
        let _ = run(
            &db,
            vec![
                str_arg("HDEL"),
                str_arg("fat"),
                str_arg(&format!("f{i:03}")),
            ],
        )
        .await;
    }
    let after_del = db.store.estimated_memory_bytes();
    assert!(
        after_del < after / 2,
        "live_bytes after HDEL all ({after_del}) did not drop enough from {after}"
    );
}
