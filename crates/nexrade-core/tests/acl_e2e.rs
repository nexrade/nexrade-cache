//! Integration tests for the ACL system: per-user permissions, key
//! patterns, ACL command surface, and AOF replay.

use nexrade_core::acl::AclManager;
use nexrade_core::command::dispatch_with_user;
use nexrade_core::db::Db;
use nexrade_core::resp::Resp;

fn str_arg(s: &str) -> Resp {
    Resp::bulk_str(s.to_string())
}

async fn run(db: &Db, args: Vec<Resp>, user: &str) -> Resp {
    dispatch_with_user(db, args, 0, None, user).await
}

#[tokio::test]
async fn default_user_can_run_everything() {
    let db = Db::default();
    let r = run(
        &db,
        vec![str_arg("SET"), str_arg("k"), str_arg("v")],
        "default",
    )
    .await;
    assert!(
        !matches!(r, Resp::Error(_)),
        "default should have full access"
    );
}

#[tokio::test]
async fn custom_user_with_readonly_cant_set() {
    let db = Db::default();
    db.acl
        .setuser("viewer", &["+@read", "+ping", "~*"])
        .unwrap();
    let r = run(&db, vec![str_arg("GET"), str_arg("k")], "viewer").await;
    assert!(!matches!(r, Resp::Error(_)), "viewer should GET");
    let r = run(
        &db,
        vec![str_arg("SET"), str_arg("k"), str_arg("v")],
        "viewer",
    )
    .await;
    assert!(matches!(r, Resp::Error(_)), "viewer should NOT SET");
    if let Resp::Error(msg) = r {
        assert!(
            msg.to_lowercase().contains("permission"),
            "error should mention permission: {msg}"
        );
    }
}

#[tokio::test]
async fn key_pattern_blocks_access() {
    let db = Db::default();
    db.acl.setuser("scoped", &["+@all", "~user:*"]).unwrap();
    // Allowed.
    assert!(!matches!(
        run(&db, vec![str_arg("GET"), str_arg("user:1")], "scoped").await,
        Resp::Error(_)
    ));
    // Denied.
    let r = run(&db, vec![str_arg("GET"), str_arg("system:foo")], "scoped").await;
    assert!(
        matches!(r, Resp::Error(_)),
        "scoped should be denied on system:foo"
    );
}

#[tokio::test]
async fn auth_with_password_works() {
    let m = AclManager::new();
    m.setuser("alice", &[">hunter2", "+@read", "~*"]).unwrap();
    // Right password.
    assert!(m.authenticate("alice", "hunter2").is_ok());
    // Wrong password.
    assert!(m.authenticate("alice", "nope").is_err());
}

#[tokio::test]
async fn disabled_user_cant_authenticate() {
    let m = AclManager::new();
    m.setuser("ghost", &["on", ">secret", "+@all"]).unwrap();
    m.setuser("ghost", &["off"]).unwrap();
    assert!(m.authenticate("ghost", "secret").is_err());
}

#[tokio::test]
async fn acl_list_contains_user() {
    let db = Db::default();
    db.acl
        .setuser("bob", &["+@read", "+ping", "~bob:*", ">sek"])
        .unwrap();
    let r = run(&db, vec![str_arg("ACL"), str_arg("LIST")], "default").await;
    if let Resp::Array(Some(lines)) = r {
        let joined = lines
            .iter()
            .map(|x| x.as_str().unwrap_or("").to_string())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(joined.contains("user bob"));
        assert!(joined.contains("+@read"));
        assert!(joined.contains("~bob:*"));
    } else {
        panic!("expected ACL LIST to return an array");
    }
}

#[tokio::test]
async fn acl_setuser_then_acl_getuser_roundtrips() {
    let db = Db::default();
    run(
        &db,
        vec![
            str_arg("ACL"),
            str_arg("SETUSER"),
            str_arg("charlie"),
            str_arg("+@write"),
            str_arg("~charlie:*"),
            str_arg(">"),
            str_arg("topsecret"),
        ],
        "default",
    )
    .await;
    let r = run(
        &db,
        vec![str_arg("ACL"), str_arg("GETUSER"), str_arg("charlie")],
        "default",
    )
    .await;
    if let Resp::Array(Some(parts)) = r {
        let s: Vec<String> = parts
            .iter()
            .map(|p| match p {
                Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
                _ => String::new(),
            })
            .collect();
        // Field layout: ["flags", "on", "passwords", "<hash>", ...]
        assert!(s.contains(&"flags".to_string()));
        assert!(s.contains(&"on".to_string()));
        assert!(s.iter().any(|x| x.contains("charlie")));
    } else {
        panic!("expected GETUSER to return a map");
    }
}

#[tokio::test]
async fn acl_deluser_removes_user() {
    let db = Db::default();
    db.acl.setuser("temp", &["+@read"]).unwrap();
    assert!(db.acl.get_user("temp").is_some());
    let r = run(
        &db,
        vec![str_arg("ACL"), str_arg("DELUSER"), str_arg("temp")],
        "default",
    )
    .await;
    assert_eq!(resp_to_i64(&r), 1);
    assert!(db.acl.get_user("temp").is_none());
}

#[tokio::test]
async fn acl_genpass_returns_hex() {
    let db = Db::default();
    let r = run(
        &db,
        vec![str_arg("ACL"), str_arg("GENPASS"), str_arg("128")],
        "default",
    )
    .await;
    if let Resp::BulkString(Some(s)) = r {
        let s = String::from_utf8_lossy(&s);
        assert!(
            s.len() >= 16,
            "GENPASS should produce at least 16 hex chars"
        );
        assert!(
            s.chars().all(|c| c.is_ascii_hexdigit()),
            "GENPASS should be hex: {s}"
        );
    } else {
        panic!("expected bulk string from GENPASS");
    }
}

#[tokio::test]
async fn acl_dryrun_returns_ok_when_allowed() {
    let db = Db::default();
    let r = run(
        &db,
        vec![
            str_arg("ACL"),
            str_arg("DRYRUN"),
            str_arg("default"),
            str_arg("GET"),
        ],
        "default",
    )
    .await;
    assert!(matches!(r, Resp::SimpleString(s) if s == "OK"));
}

#[tokio::test]
async fn acl_dryrun_returns_error_when_denied() {
    let db = Db::default();
    db.acl.setuser("restrict", &["+ping", "~*"]).unwrap();
    let r = run(
        &db,
        vec![
            str_arg("ACL"),
            str_arg("DRYRUN"),
            str_arg("restrict"),
            str_arg("SET"),
            str_arg("k"),
            str_arg("v"),
        ],
        "default",
    )
    .await;
    assert!(matches!(r, Resp::Error(_)));
}

#[tokio::test]
async fn acl_cat_lists_categories() {
    let db = Db::default();
    let r = run(&db, vec![str_arg("ACL"), str_arg("CAT")], "default").await;
    if let Resp::Array(Some(cats)) = r {
        assert!(cats.iter().any(|c| c.as_str() == Some("@read")));
        assert!(cats.iter().any(|c| c.as_str() == Some("@all")));
    } else {
        panic!("expected CAT to return array");
    }
}

#[tokio::test]
async fn acl_whoami_returns_default() {
    let db = Db::default();
    let r = run(&db, vec![str_arg("ACL"), str_arg("WHOAMI")], "default").await;
    if let Resp::BulkString(Some(s)) = r {
        assert_eq!(&*s, b"default");
    } else {
        panic!("expected bulk string");
    }
}

/// Regression test: WHOAMI must report the connection's actual
/// authenticated identity, not just any user from the ACL list. Uses a
/// username that sorts alphabetically before "default" so the old buggy
/// implementation (returning `list_users().first()`) would have returned
/// the wrong name here.
#[tokio::test]
async fn acl_whoami_returns_the_actual_caller_not_first_in_list() {
    let db = Db::default();
    db.acl
        .setuser("aaa_first_alphabetically", &["+@all", "~*"])
        .unwrap();
    db.acl.setuser("zed", &["+@all", "~*"]).unwrap();

    let r = run(&db, vec![str_arg("ACL"), str_arg("WHOAMI")], "zed").await;
    if let Resp::BulkString(Some(s)) = r {
        assert_eq!(
            &*s, b"zed",
            "WHOAMI should report the caller's own identity, not the alphabetically-first ACL user"
        );
    } else {
        panic!("expected bulk string");
    }
}

#[tokio::test]
async fn unknown_user_cant_dispatch() {
    let db = Db::default();
    let r = run(&db, vec![str_arg("GET"), str_arg("k")], "nobody").await;
    assert!(matches!(r, Resp::Error(_)));
}

fn resp_to_i64(r: &Resp) -> i64 {
    match r {
        Resp::Integer(n) => *n,
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).parse().unwrap_or(-1),
        _ => -1,
    }
}

#[test]
fn acl_log_records_denials() {
    let m = AclManager::new();
    // Disable default, then try to use it.
    m.setuser("default", &["off"]).unwrap();
    let _ = m.check_permission("default", "GET", &[b"x"]);
    let _ = m.check_permission("default", "SET", &[b"x"]);
    let log = m.acl_log(None);
    assert_eq!(log.len(), 2, "expected 2 log entries, got {}", log.len());
    assert!(log[0].user == "default");
}
