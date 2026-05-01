//! Publish/Subscribe message bus.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::broadcast;

const CHANNEL_CAPACITY: usize = 1024;

/// A message broadcast on a channel.
#[derive(Debug, Clone)]
pub struct Message {
    pub kind: MessageKind,
    pub channel: Vec<u8>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MessageKind {
    Message,
    PMessage, // pattern-matched
    Subscribe,
    Unsubscribe,
    PSubscribe,
    PUnsubscribe,
}

/// The central pub/sub broker.
#[derive(Clone)]
pub struct PubSub {
    inner: Arc<Mutex<PubSubInner>>,
}

struct PubSubInner {
    /// channel name → broadcast sender
    channels: HashMap<Vec<u8>, broadcast::Sender<Message>>,
    /// Number of subscriptions per channel
    counts: HashMap<Vec<u8>, usize>,
}

impl PubSub {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(PubSubInner {
                channels: HashMap::new(),
                counts: HashMap::new(),
            })),
        }
    }

    /// Subscribe to a channel, returning a receiver.
    pub fn subscribe(&self, channel: Vec<u8>) -> broadcast::Receiver<Message> {
        let mut inner = self.inner.lock();

        // Ensure the sender exists
        if !inner.channels.contains_key(&channel) {
            let (tx, _) = broadcast::channel(CHANNEL_CAPACITY);
            inner.channels.insert(channel.clone(), tx);
        }
        *inner.counts.entry(channel.clone()).or_insert(0) += 1;
        inner.channels[&channel].subscribe()
    }

    /// Unsubscribe from a channel. Returns remaining subscription count.
    pub fn unsubscribe(&self, channel: &[u8]) -> usize {
        let mut inner = self.inner.lock();
        let count = inner.counts.get_mut(channel);
        if let Some(c) = count {
            if *c > 0 {
                *c -= 1;
            }
            let remaining = *c;
            if remaining == 0 {
                inner.channels.remove(channel);
                inner.counts.remove(channel);
            }
            remaining
        } else {
            0
        }
    }

    /// Publish a message to a channel. Returns number of receivers.
    pub fn publish(&self, channel: Vec<u8>, payload: Vec<u8>) -> usize {
        let inner = self.inner.lock();
        if let Some(sender) = inner.channels.get(&channel) {
            let msg = Message {
                kind: MessageKind::Message,
                channel,
                payload,
            };
            sender.send(msg).unwrap_or(0)
        } else {
            0
        }
    }

    /// Number of active channels.
    pub fn channel_count(&self) -> usize {
        self.inner.lock().channels.len()
    }

    /// List all active channel names.
    pub fn channel_names(&self) -> Vec<Vec<u8>> {
        self.inner.lock().channels.keys().cloned().collect()
    }

    /// Subscription count for a channel.
    pub fn subscriber_count(&self, channel: &[u8]) -> usize {
        *self.inner.lock().counts.get(channel).unwrap_or(&0)
    }
}

impl Default for PubSub {
    fn default() -> Self {
        Self::new()
    }
}
