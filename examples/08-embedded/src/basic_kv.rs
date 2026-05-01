//! Basic key-value operations using the embedded nexrade-core library.

use anyhow::Result;
use nexrade_core::command::dispatch;
use nexrade_core::db::{Db, ServerConfig};
use nexrade_core::resp::Resp;

/// Helper: run a command and return the response as a String.
async fn cmd(db: &Db, args: &[&str]) -> String {
    let resp_args = args
        .iter()
        .map(|s| Resp::bulk_str(*s))
        .collect::<Vec<_>>();
    dispatch(db, resp_args, 0).await.to_string()
}

pub async fn run() -> Result<()> {
    let db = Db::new(ServerConfig::default());

    // SET / GET
    println!("SET hello world  → {}", cmd(&db, &["SET", "hello", "world"]).await);
    println!("GET hello        → {}", cmd(&db, &["GET", "hello"]).await);
    println!("GET missing      → {}", cmd(&db, &["GET", "missing"]).await);

    // Numeric
    cmd(&db, &["SET", "counter", "0"]).await;
    println!("INCR counter     → {}", cmd(&db, &["INCR", "counter"]).await);
    println!("INCRBY counter 9 → {}", cmd(&db, &["INCRBY", "counter", "9"]).await);
    println!("GET counter      → {}", cmd(&db, &["GET", "counter"]).await);

    // Expiry
    cmd(&db, &["SET", "token", "abc123", "EX", "60"]).await;
    println!("TTL token        → {}", cmd(&db, &["TTL", "token"]).await);

    // Hash
    cmd(&db, &["HSET", "user:1", "name", "Alice", "age", "30"]).await;
    println!("HGET user:1 name → {}", cmd(&db, &["HGET", "user:1", "name"]).await);
    println!("HGETALL user:1   → {}", cmd(&db, &["HGETALL", "user:1"]).await);

    // List
    cmd(&db, &["RPUSH", "queue", "job1", "job2", "job3"]).await;
    println!("LPOP queue       → {}", cmd(&db, &["LPOP", "queue"]).await);
    println!("LLEN queue       → {}", cmd(&db, &["LLEN", "queue"]).await);

    // Sorted set
    cmd(&db, &["ZADD", "scores", "100", "alice", "200", "bob", "150", "carol"]).await;
    println!("ZREVRANGE scores → {}", cmd(&db, &["ZREVRANGE", "scores", "0", "-1", "WITHSCORES"]).await);

    Ok(())
}
