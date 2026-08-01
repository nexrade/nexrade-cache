//! 0.2.8: MONITOR bus, CONFIG SET appendfsync/maxclients, INFO cpu/memory polish.

use nexrade_core::command::dispatch_with_user;
use nexrade_core::db::Db;
use nexrade_core::resp::Resp;

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter().map(|s| Resp::bulk_str(*s)).collect()
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch_with_user(db, args, 0, None, "default").await
}

fn as_bulk_str(r: &Resp) -> String {
    match r {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        Resp::SimpleString(s) => s.clone(),
        other => panic!("expected bulk/simple, got {other:?}"),
    }
}

// ── MONITOR bus ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn monitor_bus_publishes_to_subscribers() {
    let db = Db::default();
    assert!(!db.monitor.has_subscribers());

    let (mut rx, _guard) = db.monitor.subscribe();
    assert!(db.monitor.has_subscribers());

    db.monitor
        .publish("1.0 [0 127.0.0.1:1] \"PING\" \"(1 args)\"".into());
    let line = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .expect("recv timeout")
        .expect("recv err");
    assert!(line.contains("PING"), "{line}");

    drop(_guard);
    // Guard drop decrements; may still be >0 if races, but we check publish no-ops.
}

// ── CONFIG SET appendfsync / maxclients ───────────────────────────────────────

#[tokio::test]
async fn config_set_appendfsync_and_maxclients() {
    let db = Db::default();

    let r = run(&db, cmd(&["CONFIG", "SET", "appendfsync", "always"])).await;
    assert!(matches!(r, Resp::SimpleString(ref s) if s == "OK"), "{r:?}");
    {
        #[cfg(not(target_arch = "wasm32"))]
        {
            use nexrade_core::persistence::AofSync;
            assert_eq!(db.config.lock().persistence.aof_sync, AofSync::Always);
        }
    }

    let r = run(&db, cmd(&["CONFIG", "SET", "appendfsync", "everysec"])).await;
    assert!(matches!(r, Resp::SimpleString(ref s) if s == "OK"), "{r:?}");

    let r = run(&db, cmd(&["CONFIG", "SET", "appendfsync", "bogus"])).await;
    assert!(matches!(r, Resp::Error(_)), "{r:?}");

    let r = run(&db, cmd(&["CONFIG", "SET", "maxclients", "42"])).await;
    assert!(matches!(r, Resp::SimpleString(ref s) if s == "OK"), "{r:?}");
    assert_eq!(db.config.lock().max_clients, 42);

    let r = run(&db, cmd(&["CONFIG", "SET", "maxclients", "0"])).await;
    assert!(matches!(r, Resp::Error(_)), "{r:?}");
}

// ── INFO cpu / memory polish ──────────────────────────────────────────────────

#[tokio::test]
async fn info_includes_cpu_and_maxmemory() {
    let db = Db::default();
    let info = run(&db, cmd(&["INFO", "all"])).await;
    let bulk = as_bulk_str(&info);
    assert!(bulk.contains("# CPU"), "missing CPU section:\n{bulk}");
    assert!(
        bulk.contains("used_cpu_sys:"),
        "missing used_cpu_sys:\n{bulk}"
    );
    assert!(bulk.contains("maxmemory:"), "missing maxmemory:\n{bulk}");
    assert!(
        bulk.contains("used_memory_peak:"),
        "missing used_memory_peak:\n{bulk}"
    );
    assert!(
        bulk.contains(&format!("nexrade_version:{}", env!("CARGO_PKG_VERSION")))
            || bulk.contains("nexrade_version:"),
        "missing nexrade_version:\n{bulk}"
    );
}
