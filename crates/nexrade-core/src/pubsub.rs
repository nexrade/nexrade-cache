//! Publish/Subscribe message bus.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::broadcast;

use crate::store::glob_match;

const CHANNEL_CAPACITY: usize = 1024;

/// A message broadcast on a channel.
#[derive(Debug, Clone)]
pub struct Message {
    pub kind: MessageKind,
    /// Channel / pattern the subscriber listens on (e.g. `__keyspace@0__:foo`
    /// for a keyspace event) — used by connection framing.
    pub channel: Vec<u8>,
    /// For PMessage: the original published channel; for Message: same as
    /// `channel` (kept separate so a PMessage frame can be reconstructed
    /// without the subscriber having to dig it out).
    pub source: Vec<u8>,
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
    /// Pattern subscriptions (`PSUBSCRIBE`) — list of glob patterns with one
    /// broadcast sender each. Independent from the literal channel map so a
    /// pattern subscription that doesn't yet match anything still gets a
    /// deliverable receiver.
    patterns: HashMap<Vec<u8>, broadcast::Sender<Message>>,
    /// Number of subscribers per pattern (so PUNSUBSCRIBE can be accurate).
    pattern_counts: HashMap<Vec<u8>, usize>,
}

impl PubSub {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(PubSubInner {
                channels: HashMap::new(),
                counts: HashMap::new(),
                patterns: HashMap::new(),
                pattern_counts: HashMap::new(),
            })),
        }
    }

    /// Subscribe to a literal channel, returning a receiver.
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

    /// Subscribe to a glob pattern (PSUBSCRIBE). One broadcast sender per
    /// pattern keeps fan-out simple and avoids N linear scans per publish.
    pub fn psubscribe(&self, pattern: Vec<u8>) -> broadcast::Receiver<Message> {
        let mut inner = self.inner.lock();
        if !inner.patterns.contains_key(&pattern) {
            let (tx, _) = broadcast::channel(CHANNEL_CAPACITY);
            inner.patterns.insert(pattern.clone(), tx);
        }
        *inner.pattern_counts.entry(pattern.clone()).or_insert(0) += 1;
        inner.patterns[&pattern].subscribe()
    }

    /// Unsubscribe from a literal channel. Returns remaining subscription count.
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

    /// Unsubscribe from a pattern. Returns remaining subscription count.
    pub fn punsubscribe(&self, pattern: &[u8]) -> usize {
        let mut inner = self.inner.lock();
        let count = inner.pattern_counts.get_mut(pattern);
        if let Some(c) = count {
            if *c > 0 {
                *c -= 1;
            }
            let remaining = *c;
            if remaining == 0 {
                inner.patterns.remove(pattern);
                inner.pattern_counts.remove(pattern);
            }
            remaining
        } else {
            0
        }
    }

    /// Publish a message. Literal matches get `Message`; every pattern that
    /// matches the channel name gets a `PMessage` fan-out. Returns the number
    /// of literal receivers that received the message (pattern matches are
    /// best-effort and not counted — Redis returns just the direct count).
    pub fn publish(&self, channel: Vec<u8>, payload: Vec<u8>) -> usize {
        let inner = self.inner.lock();
        let direct = if let Some(sender) = inner.channels.get(&channel) {
            let msg = Message {
                kind: MessageKind::Message,
                channel: channel.clone(),
                source: channel.clone(),
                payload: payload.clone(),
            };
            sender.send(msg).unwrap_or(0)
        } else {
            0
        };
        // Pattern fan-out: one Message per matching pattern so a PMessage
        // subscriber sees the original published channel as the source.
        for (pattern, sender) in inner.patterns.iter() {
            if glob_match(pattern, &channel) {
                let msg = Message {
                    kind: MessageKind::PMessage,
                    channel: pattern.clone(),
                    source: channel.clone(),
                    payload: payload.clone(),
                };
                let _ = sender.send(msg);
            }
        }
        direct
    }

    /// Number of active literal channels (patterns excluded).
    pub fn channel_count(&self) -> usize {
        self.inner.lock().channels.len()
    }

    /// List all active literal channel names.
    pub fn channel_names(&self) -> Vec<Vec<u8>> {
        self.inner.lock().channels.keys().cloned().collect()
    }

    /// Subscription count for a literal channel.
    pub fn subscriber_count(&self, channel: &[u8]) -> usize {
        *self.inner.lock().counts.get(channel).unwrap_or(&0)
    }

    /// Number of active pattern subscriptions.
    pub fn pattern_count(&self) -> usize {
        self.inner.lock().patterns.len()
    }

    /// List all active pattern strings (for diagnostics / `PUBSUB NUMPAT`).
    pub fn pattern_names(&self) -> Vec<Vec<u8>> {
        self.inner.lock().patterns.keys().cloned().collect()
    }
}

impl Default for PubSub {
    fn default() -> Self {
        Self::new()
    }
}
