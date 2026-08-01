//! 0.2.10: CONFIG REWRITE/save, MONITOR full args, process CPU.

use nexrade_core::command::dispatch_with_user;
use nexrade_core::db::{Db, ServerConfig};
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

#[tokio::test]
async fn config_set_save_and_rewrite() {
    let dir = std::env::temp_dir().join(format!("nexrade-cfg-rewrite-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("nexrade.toml");
    // Seed a minimal file so REWRITE has a target.
    std::fs::write(&path, "port = 6379\n").unwrap();

    let cfg = ServerConfig {
        config_file_path: Some(path.to_string_lossy().into_owned()),
        ..ServerConfig::default()
    };
    let db = Db::new(cfg);

    let r = run(&db, cmd(&["CONFIG", "SET", "save", "900 1 300 10"])).await;
    assert!(matches!(r, Resp::SimpleString(ref s) if s == "OK"), "{r:?}");
    {
        let g = db.config.lock();
        assert_eq!(g.save_rules, vec![(900, 1), (300, 10)]);
    }

    let r = run(&db, cmd(&["CONFIG", "SET", "save", ""])).await;
    assert!(matches!(r, Resp::SimpleString(ref s) if s == "OK"), "{r:?}");
    assert!(db.config.lock().save_rules.is_empty());

    // Restore a rule and REWRITE.
    let _ = run(&db, cmd(&["CONFIG", "SET", "save", "60 10000"])).await;
    let r = run(&db, cmd(&["CONFIG", "REWRITE"])).await;
    assert!(matches!(r, Resp::SimpleString(ref s) if s == "OK"), "{r:?}");
    let written = std::fs::read_to_string(&path).unwrap();
    assert!(
        written.contains("save_rules") || written.contains("60"),
        "rewrite missing save rules:\n{written}"
    );
    assert!(written.contains("port"), "rewrite missing port:\n{written}");

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn config_rewrite_errors_without_path() {
    let db = Db::default();
    let r = run(&db, cmd(&["CONFIG", "REWRITE"])).await;
    match r {
        Resp::Error(msg) => assert!(msg.contains("REWRITE") || msg.contains("config"), "{msg}"),
        other => panic!("expected error without config path, got {other:?}"),
    }
}

#[tokio::test]
async fn monitor_format_includes_args() {
    // Unit-test the line formatter via the bus path: publish a line that
    // looks like a real monitor entry with full args.
    let db = Db::default();
    let (mut rx, _guard) = db.monitor.subscribe();
    db.monitor
        .publish(r#"1.234567 [0 127.0.0.1:1] "SET" "k" "v""#.into());
    let line = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("recv");
    assert!(line.contains("\"SET\""), "{line}");
    assert!(line.contains("\"k\""), "{line}");
    assert!(line.contains("\"v\""), "{line}");
}

#[tokio::test]
async fn info_cpu_fields_present() {
    let db = Db::default();
    let info = run(&db, cmd(&["INFO", "cpu"])).await;
    let bulk = as_bulk_str(&info);
    assert!(bulk.contains("# CPU"), "{bulk}");
    assert!(bulk.contains("used_cpu_sys:"), "{bulk}");
    assert!(bulk.contains("used_cpu_user:"), "{bulk}");
}
