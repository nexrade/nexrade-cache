#!/usr/bin/env python3
"""
03-pubsub/publisher.py — Publish messages to channels.

Usage:
    python publisher.py                         # publish demo messages to 'news'
    python publisher.py sports "Goal scored!"   # publish to a specific channel

Requires: pip install redis
"""

import sys
import time
import redis

HOST = "127.0.0.1"
PORT = 6379

r = redis.Redis(host=HOST, port=PORT, decode_responses=True)

if len(sys.argv) == 3:
    channel, message = sys.argv[1], sys.argv[2]
    receivers = r.publish(channel, message)
    print(f"Published to '{channel}': {message!r}  ({receivers} receiver(s))")
else:
    # Demo: publish a stream of messages
    channel = "news"
    headlines = [
        "Breaking: nexrade-cache released!",
        "Performance: 1M ops/sec on commodity hardware",
        "Feature: built-in Prometheus metrics, no sidecar needed",
        "Feature: Lua scripting with EVAL",
        "Feature: pure-Rust TLS via rustls",
    ]
    print(f"Publishing {len(headlines)} messages to '{channel}'...\n")
    for i, headline in enumerate(headlines, 1):
        receivers = r.publish(channel, headline)
        print(f"[{i}/{len(headlines)}] {headline!r}  → {receivers} receiver(s)")
        time.sleep(0.5)
    print("\nDone.")
