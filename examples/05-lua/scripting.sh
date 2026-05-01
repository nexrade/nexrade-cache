#!/usr/bin/env bash
# 05-lua/scripting.sh — Lua EVAL examples
# Lua scripts run atomically — no other commands execute between steps.
set -euo pipefail
CLI="redis-cli -h ${NEXRADE_HOST:-127.0.0.1} -p ${NEXRADE_PORT:-6379}"

echo "=== Hello from Lua ==="
$CLI EVAL "return 'Hello from Lua 5.4!'" 0

echo ""
echo "=== Atomic GET-or-SET (if absent, set default) ==="
$CLI DEL mykey
$CLI EVAL "
  local val = redis.call('GET', KEYS[1])
  if val == false then
    redis.call('SET', KEYS[1], ARGV[1])
    return ARGV[1]
  end
  return val
" 1 mykey "default_value"

# Second call returns existing value
$CLI EVAL "
  local val = redis.call('GET', KEYS[1])
  if val == false then
    redis.call('SET', KEYS[1], ARGV[1])
    return ARGV[1]
  end
  return val
" 1 mykey "ignored"

echo ""
echo "=== Atomic rate limiter ==="
# Increment a counter, set TTL on first hit, return current count
$CLI DEL rate:user:42
$CLI EVAL "
  local key = KEYS[1]
  local limit = tonumber(ARGV[1])
  local window = tonumber(ARGV[2])
  local count = redis.call('INCR', key)
  if count == 1 then
    redis.call('EXPIRE', key, window)
  end
  if count > limit then
    return redis.error_reply('RATE_EXCEEDED')
  end
  return count
" 1 rate:user:42 5 60

echo ""
echo "=== Cache script with SCRIPT LOAD / EVALSHA ==="
SHA=$($CLI SCRIPT LOAD "
  local key = KEYS[1]
  local ttl = tonumber(ARGV[1])
  local val = redis.call('GET', key)
  if val then
    return {1, val}          -- cache hit
  end
  return {0, false}          -- cache miss
")
echo "Script SHA: $SHA"
$CLI SET cached_item "hot data"
$CLI EVALSHA "$SHA" 1 cached_item 300   # hit
$CLI EVALSHA "$SHA" 1 missing_item 300  # miss

echo ""
echo "=== Lua table → RESP array ==="
$CLI EVAL "return {1, 2, 3, 'four', 5}" 0
