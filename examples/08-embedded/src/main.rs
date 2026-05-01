//! 08-embedded — Use nexrade-core as an in-process library.
//!
//! No TCP server, no redis-cli. The cache lives inside your Rust process.
//!
//! Run:
//!   cargo run --manifest-path examples/08-embedded/Cargo.toml

mod basic_kv;
mod pubsub;
mod transactions;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .init();

    println!("╔══════════════════════════════════════╗");
    println!("║  nexrade-cache — Embedded Examples   ║");
    println!("╚══════════════════════════════════════╝\n");

    println!("── 1. Basic key-value ──────────────────");
    basic_kv::run().await?;

    println!("\n── 2. Transactions (MULTI/EXEC) ────────");
    transactions::run().await?;

    println!("\n── 3. Pub/Sub ──────────────────────────");
    pubsub::run().await?;

    println!("\nAll examples completed.");
    Ok(())
}
