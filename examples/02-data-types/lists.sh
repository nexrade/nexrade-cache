#!/usr/bin/env bash
# 02-data-types/lists.sh — List commands (queues, stacks)
set -euo pipefail
CLI="redis-cli -h ${NEXRADE_HOST:-127.0.0.1} -p ${NEXRADE_PORT:-6379}"

$CLI DEL tasks

echo "=== Push to queue (RPUSH = enqueue) ==="
$CLI RPUSH tasks "job:1" "job:2" "job:3"
$CLI LLEN tasks

echo ""
echo "=== Peek at queue ==="
$CLI LRANGE tasks 0 -1   # all elements

echo ""
echo "=== LPOP = dequeue ==="
$CLI LPOP tasks
$CLI LRANGE tasks 0 -1

echo ""
echo "=== Stack (LPUSH + LPOP) ==="
$CLI DEL stack
$CLI LPUSH stack "a" "b" "c"  # c is on top
$CLI LPOP stack               # pops c

echo ""
echo "=== LINSERT / LSET / LINDEX ==="
$CLI DEL nums
$CLI RPUSH nums 1 2 4 5
$CLI LINSERT nums BEFORE 4 3  # insert 3 before 4
$CLI LRANGE nums 0 -1
$CLI LSET nums 0 0             # replace index 0
$CLI LINDEX nums 0

echo ""
echo "=== LMOVE (non-blocking RPOPLPUSH) ==="
$CLI DEL src dst
$CLI RPUSH src "a" "b" "c"
$CLI LMOVE src dst LEFT RIGHT  # move left from src to right of dst
$CLI LRANGE src 0 -1
$CLI LRANGE dst 0 -1
