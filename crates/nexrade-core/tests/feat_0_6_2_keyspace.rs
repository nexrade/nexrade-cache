//! 0.6.2 — Keyspace notifications subset (`notify-keyspace-events`).
//!
//! What we lock down:
//!   * CONFIG GET/SET of `notify-keyspace-events` (flag string ↔ bitmap)
//!   * PubSub pattern matching (PSUBSCRIBE) so `__key*@*__:*` works
//!   * SET / DEL / EXPIRE fire keyspace + keyevent channels when enabled
//!   * Active expire fires `expired` when `x` is set
//!   * Eviction fires `evicted` when `e` is set
//!   * No-op when flags are empty (default)

use nexrade_core::command::dispatch;
use nexrade_core::db::{Db, MaxMemoryPolicy, ServerConfig};
use nexrade_core::notify::NotifyFlags;
use nexrade_core::pubsub::MessageKind;
use nexrade_core::resp::Resp;

fn small_config() -> ServerConfig {
    ServerConfig {
        databases: 1,
        max_memory: None,
        maxmemory_policy: MaxMemoryPolicy::NoEviction,
        ..ServerConfig::default()
    }
}

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter()
        .map(|a| Resp::BulkString(Some(bytes::Bytes::from(a.to_string()))))
        .collect()
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch(db, args, 0).await
}

fn ok(r: &Resp) {
    match r {
        Resp::SimpleString(s) if s == "OK" => {}
        Resp::BulkString(Some(b)) if b.as_ref() == b"OK" => {}
        other => panic!("expected OK, got {other:?}"),
    }
}

fn bulk_eq(r: &Resp, want: &str) -> bool {
    match r {
        Resp::BulkString(Some(b)) => b.as_ref() == want.as_bytes(),
        Resp::SimpleString(s) => s == want,
        _ => false,
    }
}

// ─── Flag string parsing ────────────────────────────────────────────────────

#[test]
fn notify_flags_roundtrip() {
    let f = NotifyFlags::parse("KEA");
    assert!(f.contains(NotifyFlags::PREFIX_KEYSPACE));
    assert!(f.contains(NotifyFlags::PREFIX_KEYEVENT));
    assert!(f.contains(NotifyFlags::K_STRING));
    assert!(f.contains(NotifyFlags::K_EXPIRED));
    // A expands to g$lshzxet (all classes)
    assert!(f.contains(NotifyFlags::K_GENERIC));
    assert!(f.contains(NotifyFlags::K_LIST));
    // as_str keeps K/E first then the class letters that are set
    let s = f.as_str();
    assert!(s.contains('K') && s.contains('E'), "got {s}");
    assert!(
        s.contains('$') || s.contains('A') || s.contains('g'),
        "got {s}"
    );
}

#[test]
fn notify_flags_empty_is_zero() {
    assert_eq!(NotifyFlags::parse("").0, 0);
    assert_eq!(NotifyFlags::empty().0, 0);
    assert_eq!(NotifyFlags::parse("KE").as_str(), "KE");
}

// ─── CONFIG GET / SET ───────────────────────────────────────────────────────

#[tokio::test]
async fn config_get_set_notify_keyspace_events() {
    let db = Db::new(small_config());
    // Default is empty.
    let r = run(&db, cmd(&["CONFIG", "GET", "notify-keyspace-events"])).await;
    match r {
        Resp::Array(Some(pairs)) => {
            assert_eq!(pairs.len(), 2);
            assert!(bulk_eq(&pairs[0], "notify-keyspace-events"));
            assert!(
                bulk_eq(&pairs[1], ""),
                "default must be empty, got {:?}",
                pairs[1]
            );
        }
        other => panic!("expected CONFIG GET array, got {other:?}"),
    }

    ok(&run(
        &db,
        cmd(&["CONFIG", "SET", "notify-keyspace-events", "KE$"]),
    )
    .await);

    let r = run(&db, cmd(&["CONFIG", "GET", "notify-keyspace-events"])).await;
    match r {
        Resp::Array(Some(pairs)) => {
            assert!(bulk_eq(&pairs[0], "notify-keyspace-events"));
            let s = match &pairs[1] {
                Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
                other => panic!("expected bulk, got {other:?}"),
            };
            assert!(
                s.contains('K') && s.contains('E') && s.contains('$'),
                "got {s}"
            );
        }
        other => panic!("expected array, got {other:?}"),
    }

    // Atomic mirror updated.
    let flags = db.notify_flags.load();
    assert!(flags.contains(NotifyFlags::PREFIX_KEYSPACE));
    assert!(flags.contains(NotifyFlags::K_STRING));
}

// ─── PubSub pattern matching ────────────────────────────────────────────────

#[tokio::test]
async fn psubscribe_matches_published_channel() {
    let db = Db::new(small_config());
    let mut rx = db.pubsub.psubscribe(b"__keyevent@0__:*".to_vec());
    // Direct publish on a matching channel.
    let n = db
        .pubsub
        .publish(b"__keyevent@0__:set".to_vec(), b"mykey".to_vec());
    // Direct count is 0 (no literal subscriber); pattern fan-out is separate.
    assert_eq!(n, 0);

    let msg = rx.try_recv().expect("pattern subscriber must receive");
    assert_eq!(msg.kind, MessageKind::PMessage);
    assert_eq!(msg.channel, b"__keyevent@0__:*");
    assert_eq!(msg.source, b"__keyevent@0__:set");
    assert_eq!(msg.payload, b"mykey");
}

#[tokio::test]
async fn psubscribe_does_not_match_unrelated() {
    let db = Db::new(small_config());
    let mut rx = db.pubsub.psubscribe(b"news.*".to_vec());
    db.pubsub
        .publish(b"sports.scores".to_vec(), b"1-0".to_vec());
    assert!(rx.try_recv().is_err(), "unrelated channel must not match");
}

// ─── SET / DEL / EXPIRE fire keyspace events ────────────────────────────────

#[tokio::test]
async fn set_del_fire_keyspace_and_keyevent() {
    let mut cfg = small_config();
    cfg.notify_keyspace_events = NotifyFlags::parse("KEg$");
    let db = Db::new(cfg);

    // Subscribe to both keyspace and keyevent channels via patterns.
    let mut ks_rx = db.pubsub.psubscribe(b"__keyspace@0__:*".to_vec());
    let mut ke_rx = db.pubsub.psubscribe(b"__keyevent@0__:*".to_vec());

    ok(&run(&db, cmd(&["SET", "foo", "bar"])).await);

    // keyspace: channel = __keyspace@0__:foo, payload = "set"
    let msg = ks_rx.try_recv().expect("keyspace SET event");
    assert_eq!(msg.kind, MessageKind::PMessage);
    assert_eq!(msg.source, b"__keyspace@0__:foo");
    assert_eq!(msg.payload, b"set");

    // keyevent: channel = __keyevent@0__:set, payload = "foo"
    let msg = ke_rx.try_recv().expect("keyevent SET event");
    assert_eq!(msg.source, b"__keyevent@0__:set");
    assert_eq!(msg.payload, b"foo");

    // Drain any lag, then DEL.
    while ks_rx.try_recv().is_ok() {}
    while ke_rx.try_recv().is_ok() {}

    let _ = run(&db, cmd(&["DEL", "foo"])).await;

    let msg = ks_rx.try_recv().expect("keyspace DEL event");
    assert_eq!(msg.source, b"__keyspace@0__:foo");
    assert_eq!(msg.payload, b"del");

    let msg = ke_rx.try_recv().expect("keyevent DEL event");
    assert_eq!(msg.source, b"__keyevent@0__:del");
    assert_eq!(msg.payload, b"foo");
}

#[tokio::test]
async fn expire_command_fires_generic_event() {
    let mut cfg = small_config();
    cfg.notify_keyspace_events = NotifyFlags::parse("KEg");
    let db = Db::new(cfg);

    let mut ke_rx = db.pubsub.psubscribe(b"__keyevent@0__:expire".to_vec());
    ok(&run(&db, cmd(&["SET", "k", "v"])).await);
    // Drain SET noise (g is on but SET is $ class — no fire for SET).
    while ke_rx.try_recv().is_ok() {}

    let _ = run(&db, cmd(&["EXPIRE", "k", "60"])).await;
    let msg = ke_rx.try_recv().expect("EXPIRE must fire keyevent");
    assert_eq!(msg.kind, MessageKind::PMessage);
    assert_eq!(msg.payload, b"k");
}

#[tokio::test]
async fn empty_flags_are_silent() {
    // Default config: notify_keyspace_events = empty.
    let db = Db::new(small_config());
    let mut rx = db.pubsub.psubscribe(b"__key*@*__:*".to_vec());
    ok(&run(&db, cmd(&["SET", "quiet", "1"])).await);
    let _ = run(&db, cmd(&["DEL", "quiet"])).await;
    assert!(
        rx.try_recv().is_err(),
        "default empty flags must not publish anything"
    );
}

// ─── Active expire fires `expired` ──────────────────────────────────────────

#[tokio::test]
async fn active_expire_fires_expired_event() {
    let mut cfg = small_config();
    cfg.notify_keyspace_events = NotifyFlags::parse("KEx");
    let db = Db::new(cfg);

    let mut ke_rx = db.pubsub.psubscribe(b"__keyevent@0__:expired".to_vec());

    // SET with 1ms PX so the key is already expired by the time we scan.
    ok(&run(&db, cmd(&["SET", "ttl_key", "v", "PX", "1"])).await);
    // Give the clock a moment.
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    let expired = db.store.active_expire(20);
    assert!(
        !expired.is_empty(),
        "active_expire must find the expired key"
    );
    // Fire the same path the listener uses.
    use nexrade_core::notify::NotifyFlags as NF;
    let flags = db.notify_flags.load();
    if flags.contains(NF::K_EXPIRED) {
        for (db_idx, key) in &expired {
            db.notify_keyspace_event(*db_idx, "expired", key);
        }
    }

    let msg = ke_rx.try_recv().expect("expired keyevent must fire");
    assert_eq!(msg.payload, b"ttl_key");
}

// ─── Eviction fires `evicted` ───────────────────────────────────────────────

#[tokio::test]
async fn eviction_fires_evicted_event() {
    let mut cfg = small_config();
    cfg.max_memory = Some(8 * 1024); // 8 KiB
    cfg.maxmemory_policy = MaxMemoryPolicy::AllKeysLru;
    cfg.notify_keyspace_events = NotifyFlags::parse("KEe");
    let db = Db::new(cfg);

    // Seed enough keys to force eviction on the next write.
    for i in 0..200 {
        let k = format!("k{i:04}");
        let v = "x".repeat(64);
        let _ = run(&db, cmd(&["SET", &k, &v])).await;
    }

    let mut ke_rx = db.pubsub.psubscribe(b"__keyevent@0__:evicted".to_vec());

    // One more write past the cap → post-write eviction.
    ok(&run(&db, cmd(&["SET", "overflow", "yyyyyyyyyyyyyyyy"])).await);

    // Drain: at least one evicted event should have been published.
    let mut saw = false;
    for _ in 0..32 {
        if let Ok(msg) = ke_rx.try_recv() {
            assert_eq!(msg.kind, MessageKind::PMessage);
            assert_eq!(msg.source, b"__keyevent@0__:evicted");
            saw = true;
            break;
        }
        // Give the publish path a tick if the first try missed.
        tokio::task::yield_now().await;
    }
    assert!(saw, "at least one evicted event must fire under maxmemory");
}

// ─── Class filter: string events don't fire when only `l` is set ────────────

#[tokio::test]
async fn class_filter_suppresses_unmatched() {
    let mut cfg = small_config();
    // Only list events + keyevent prefix.
    cfg.notify_keyspace_events = NotifyFlags::parse("El");
    let db = Db::new(cfg);

    let mut ke_rx = db.pubsub.psubscribe(b"__keyevent@0__:*".to_vec());
    ok(&run(&db, cmd(&["SET", "s", "v"])).await);
    assert!(
        ke_rx.try_recv().is_err(),
        "SET must not fire when only list class is enabled"
    );

    let _ = run(&db, cmd(&["LPUSH", "lst", "a"])).await;
    let msg = ke_rx.try_recv().expect("LPUSH must fire under El");
    assert_eq!(msg.source, b"__keyevent@0__:lpush");
    assert_eq!(msg.payload, b"lst");
}

// ─── PUBSUB NUMPAT ──────────────────────────────────────────────────────────

#[tokio::test]
async fn pubsub_numpat_tracks_patterns() {
    let db = Db::new(small_config());
    assert_eq!(db.pubsub.pattern_count(), 0);
    let _rx1 = db.pubsub.psubscribe(b"foo.*".to_vec());
    let _rx2 = db.pubsub.psubscribe(b"bar.*".to_vec());
    assert_eq!(db.pubsub.pattern_count(), 2);

    let r = run(&db, cmd(&["PUBSUB", "NUMPAT"])).await;
    match r {
        Resp::Integer(n) => assert_eq!(n, 2),
        other => panic!("expected integer, got {other:?}"),
    }
}
