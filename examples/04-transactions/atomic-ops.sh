#!/usr/bin/env bash
# 04-transactions/atomic-ops.sh — MULTI/EXEC transactions
# NOTE: MULTI/EXEC requires a single persistent connection.
#       We pipe all commands at once using redis-cli --pipe or a heredoc.
set -euo pipefail
H=${NEXRADE_HOST:-127.0.0.1}
P=${NEXRADE_PORT:-6379}
CLI="redis-cli -h $H -p $P"

echo "=== Simple atomic transfer ==="
$CLI SET account:alice 1000
$CLI SET account:bob   500

# Send MULTI/EXEC as a single persistent session via heredoc
$CLI <<'EOF'
MULTI
DECRBY account:alice 200
INCRBY account:bob 200
EXEC
EOF

echo "Alice balance: $($CLI GET account:alice)"
echo "Bob balance:   $($CLI GET account:bob)"

echo ""
echo "=== Inline MULTI/EXEC via single redis-cli call ==="
# Use -e flag with a series of commands in one session
$CLI <<'EOF'
SET account:charlie 300
MULTI
INCRBY account:charlie 100
INCRBY account:charlie 100
EXEC
GET account:charlie
EOF

echo ""
echo "=== DISCARD — cancel a queued transaction ==="
$CLI <<'EOF'
MULTI
SET will_not_be_set oops
DISCARD
EXISTS will_not_be_set
EOF

echo ""
echo "=== Transaction with error — EXECABORT ==="
$CLI <<'EOF'
MULTI
SET good_key ok
HSET
EXEC
EXISTS good_key
EOF

echo ""
echo "Done."
