#!/usr/bin/env python3
"""
03-pubsub/subscriber.py — Subscribe to channels and receive messages.

Usage:
    python subscriber.py                # subscribe to 'news'
    python subscriber.py sports tech    # subscribe to multiple channels

Run publisher.py in another terminal to send messages.
Requires: pip install redis
"""

import sys
import redis

CHANNELS = sys.argv[1:] or ["news"]
HOST = "127.0.0.1"
PORT = 6379

r = redis.Redis(host=HOST, port=PORT, decode_responses=True)
ps = r.pubsub()

ps.subscribe(*CHANNELS)
print(f"Subscribed to: {CHANNELS}")
print("Waiting for messages... (Ctrl+C to quit)\n")

for message in ps.listen():
    if message["type"] == "message":
        channel = message["channel"]
        data = message["data"]
        print(f"[{channel}] {data}")
    elif message["type"] == "subscribe":
        print(f"[system] Subscribed to '{message['channel']}' (total: {message['data']})")
