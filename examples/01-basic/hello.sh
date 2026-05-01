#!/usr/bin/env bash
# 01-basic/hello.sh — Connect and run basic commands
# Requires: redis-cli  or  ./target/release/nexrade-cli

set -euo pipefail
HOST=${NEXRADE_HOST:-127.0.0.1}
PORT=${NEXRADE_PORT:-6379}
CLI="redis-cli -h $HOST -p $PORT"

echo "=== Ping ==="
$CLI PING

echo ""
echo "=== SET / GET ==="
$CLI SET greeting "Hello, nexrade!"
$CLI GET greeting

echo ""
echo "=== TTL (expire in 10s) ==="
$CLI SET temp_key "I will vanish" EX 10
$CLI TTL temp_key

echo ""
echo "=== INCR counter ==="
$CLI DEL counter
$CLI INCR counter
$CLI INCR counter
$CLI INCR counter
$CLI GET counter

echo ""
echo "=== EXISTS / DEL ==="
$CLI EXISTS greeting
$CLI DEL greeting
$CLI EXISTS greeting

echo ""
echo "Done."
