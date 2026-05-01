//! Atomic operations using the embedded nexrade-core library.
//!
//! Note: MULTI/EXEC/DISCARD require connection-level transaction state that
//! lives in the server's Connection struct and cannot be used via the embedded
//! dispatch() API. Atomic operations in embedded mode are instead performed by
//! issuing individual commands — the sharded store ensures each command is
//! executed atomically under its shard lock.

use anyhow::Result;
use nexrade_core::command::dispatch;
use nexrade_core::db::{Db, ServerConfig};
use nexrade_core::resp::Resp;

async fn cmd(db: &Db, args: &[&str]) -> Resp {
    let resp_args = args.iter().map(|s| Resp::bulk_str(*s)).collect();
    dispatch(db, resp_args, 0).await
}

pub async fn run() -> Result<()> {
    let db = Db::new(ServerConfig::default());

    // Set up initial balances
    cmd(&db, &["SET", "account:alice", "1000"]).await;
    cmd(&db, &["SET", "account:bob",   "500"]).await;

    println!(
        "Before — Alice: {}, Bob: {}",
        cmd(&db, &["GET", "account:alice"]).await,
        cmd(&db, &["GET", "account:bob"]).await,
    );

    // Atomic transfer of 200 using individual commands (each atomically locked)
    let alice = cmd(&db, &["DECRBY", "account:alice", "200"]).await;
    let bob   = cmd(&db, &["INCRBY", "account:bob",   "200"]).await;
    println!("DECRBY alice 200 → {alice}");
    println!("INCRBY bob 200   → {bob}");

    println!(
        "After  — Alice: {}, Bob: {}",
        cmd(&db, &["GET", "account:alice"]).await,
        cmd(&db, &["GET", "account:bob"]).await,
    );

    // Conditional set — SETNX (atomic: only sets if key absent)
    println!("\nSETNX ghost value  → {}", cmd(&db, &["SETNX", "ghost", "value"]).await);
    println!("EXISTS ghost       → {}", cmd(&db, &["EXISTS", "ghost"]).await);
    cmd(&db, &["DEL", "ghost"]).await;
    println!("DEL + EXISTS ghost → {}", cmd(&db, &["EXISTS", "ghost"]).await);

    Ok(())
}
