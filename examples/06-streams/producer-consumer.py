#!/usr/bin/env python3
"""
06-streams/producer-consumer.py — Redis Streams example.

Streams are an append-only log — ideal for event sourcing, message queues,
and audit trails.

Run producer and consumer from separate terminals, or run this script which
spawns both in threads.

Requires: pip install redis
"""

import threading
import time
import redis

STREAM = "events:orders"
HOST = "127.0.0.1"
PORT = 6379


# ── Producer ──────────────────────────────────────────────────────────────────
def producer():
    r = redis.Redis(host=HOST, port=PORT, decode_responses=True)
    orders = [
        {"order_id": "1001", "item": "keyboard", "qty": "1", "price": "79.99"},
        {"order_id": "1002", "item": "mouse",    "qty": "2", "price": "29.99"},
        {"order_id": "1003", "item": "monitor",  "qty": "1", "price": "399.00"},
        {"order_id": "1004", "item": "desk",     "qty": "1", "price": "249.00"},
    ]
    print("[producer] Starting...")
    for order in orders:
        msg_id = r.xadd(STREAM, order)
        print(f"[producer] Appended order {order['order_id']} → id={msg_id}")
        time.sleep(0.3)
    print("[producer] Done.\n")


# ── Consumer ──────────────────────────────────────────────────────────────────
def consumer():
    r = redis.Redis(host=HOST, port=PORT, decode_responses=True)
    last_id = "0-0"  # start from the beginning
    print("[consumer] Listening for events...\n")
    received = 0
    while received < 4:
        # Block up to 2 seconds waiting for new entries
        results = r.xread({STREAM: last_id}, count=10, block=2000)
        if not results:
            continue
        for stream_name, messages in results:
            for msg_id, fields in messages:
                print(f"[consumer] Received id={msg_id}")
                for k, v in fields.items():
                    print(f"           {k}: {v}")
                print()
                last_id = msg_id
                received += 1
    print("[consumer] Processed all events.")


# ── Stream inspection ─────────────────────────────────────────────────────────
def inspect():
    r = redis.Redis(host=HOST, port=PORT, decode_responses=True)
    time.sleep(1.5)  # wait for some messages to be produced
    print(f"\n[inspect] Stream length: {r.xlen(STREAM)}")
    print(f"[inspect] First 2 entries:")
    for msg_id, fields in r.xrange(STREAM, count=2):
        print(f"  {msg_id}: {fields}")


if __name__ == "__main__":
    r = redis.Redis(host=HOST, port=PORT)
    r.delete(STREAM)  # clean up from previous runs

    threads = [
        threading.Thread(target=producer),
        threading.Thread(target=consumer),
        threading.Thread(target=inspect),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
