#!/usr/bin/env bash
# 02-data-types/strings.sh — String commands
set -euo pipefail
CLI="redis-cli -h ${NEXRADE_HOST:-127.0.0.1} -p ${NEXRADE_PORT:-6379}"

echo "=== String basics ==="
$CLI SET name "nexrade"
$CLI APPEND name "-cache"
$CLI GET name
$CLI STRLEN name

echo ""
echo "=== Numeric ==="
$CLI SET score 100
$CLI INCRBY score 50
$CLI INCRBYFLOAT score 0.5
$CLI GET score

echo ""
echo "=== Bulk SET / GET ==="
$CLI MSET k1 "alpha" k2 "beta" k3 "gamma"
$CLI MGET k1 k2 k3

echo ""
echo "=== SET with options ==="
$CLI SET token "abc123" EX 60 NX      # set only if Not eXists, expire 60s
$CLI SET token "replaced"  XX          # set only if eXists
$CLI GET token

echo ""
echo "=== GETEX / GETDEL ==="
$CLI GETEX token EXAT 9999999999       # update expiry
$CLI TTL token
$CLI GETDEL token
$CLI EXISTS token
