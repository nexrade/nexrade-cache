#!/usr/bin/env bash
# 02-data-types/sorted-sets.sh — Sorted set commands (leaderboards, rankings)
set -euo pipefail
CLI="redis-cli -h ${NEXRADE_HOST:-127.0.0.1} -p ${NEXRADE_PORT:-6379}"

$CLI DEL leaderboard

echo "=== Add scores ==="
$CLI ZADD leaderboard 1500 "alice"
$CLI ZADD leaderboard 2200 "bob"
$CLI ZADD leaderboard 1800 "carol"
$CLI ZADD leaderboard 900  "dave"
$CLI ZADD leaderboard 2200 "eve"   # tie with bob

echo ""
echo "=== Top 3 (highest score first) ==="
$CLI ZREVRANGE leaderboard 0 2 WITHSCORES

echo ""
echo "=== Alice's rank (0-based, highest = rank 0) ==="
$CLI ZREVRANK leaderboard "alice"
$CLI ZSCORE leaderboard "alice"

echo ""
echo "=== Players with score between 1000 and 2000 ==="
$CLI ZRANGEBYSCORE leaderboard 1000 2000 WITHSCORES

echo ""
echo "=== Increment score ==="
$CLI ZINCRBY leaderboard 300 "alice"
$CLI ZSCORE leaderboard "alice"

echo ""
echo "=== Full leaderboard ==="
$CLI ZREVRANGE leaderboard 0 -1 WITHSCORES

echo ""
echo "=== Remove lowest scorer ==="
$CLI ZPOPMIN leaderboard
