#!/usr/bin/env bash
# 02-data-types/hashes.sh — Hash commands (objects / maps)
set -euo pipefail
CLI="redis-cli -h ${NEXRADE_HOST:-127.0.0.1} -p ${NEXRADE_PORT:-6379}"

$CLI DEL user:1

echo "=== Store a user object ==="
$CLI HSET user:1 name "Alice" email "alice@example.com" age 30 role "admin"

echo ""
echo "=== Read fields ==="
$CLI HGET user:1 name
$CLI HMGET user:1 name email role

echo ""
echo "=== All fields ==="
$CLI HGETALL user:1
$CLI HKEYS user:1
$CLI HVALS user:1
$CLI HLEN user:1

echo ""
echo "=== Update / increment ==="
$CLI HINCRBY user:1 age 1
$CLI HGET user:1 age

echo ""
echo "=== HSETNX (only if field absent) ==="
$CLI HSETNX user:1 name "Bob"   # no-op, name already exists
$CLI HSETNX user:1 score 100    # sets score
$CLI HGET user:1 name
$CLI HGET user:1 score

echo ""
echo "=== HDEL ==="
$CLI HDEL user:1 score
$CLI HEXISTS user:1 score
