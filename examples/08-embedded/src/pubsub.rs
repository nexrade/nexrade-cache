//! Pub/Sub using the embedded nexrade-core pubsub broker directly.

use anyhow::Result;
use nexrade_core::db::{Db, ServerConfig};
use nexrade_core::pubsub::MessageKind;

pub async fn run() -> Result<()> {
    let db = Db::new(ServerConfig::default());

    let channel = b"news".to_vec();

    // Subscribe
    let mut rx = db.pubsub.subscribe(channel.clone());

    // Publish a few messages
    let messages = ["Hello!", "World!", "nexrade-cache rocks"];
    for msg in &messages {
        let sent = db.pubsub.publish(channel.clone(), msg.as_bytes().to_vec());
        println!("PUBLISH news {:?}  → {} receiver(s)", msg, sent);
    }

    // Drain the receiver
    let mut count = 0;
    while let Ok(msg) = rx.try_recv() {
        if msg.kind == MessageKind::Message {
            let payload = String::from_utf8_lossy(&msg.payload);
            println!("Received on '{}': {}", String::from_utf8_lossy(&msg.channel), payload);
            count += 1;
        }
    }
    println!("Total messages received: {count}");

    Ok(())
}
