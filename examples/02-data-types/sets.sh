#!/usr/bin/env bash
# 02-data-types/sets.sh — Set commands (unique tags, memberships)
set -euo pipefail
CLI="redis-cli -h ${NEXRADE_HOST:-127.0.0.1} -p ${NEXRADE_PORT:-6379}"

$CLI DEL tags:article:1 tags:article:2

echo "=== Add tags ==="
$CLI SADD tags:article:1 rust cache database performance
$CLI SADD tags:article:2 rust web async performance

echo ""
echo "=== Membership ==="
$CLI SISMEMBER tags:article:1 rust   # 1 = yes
$CLI SISMEMBER tags:article:1 python # 0 = no
$CLI SMEMBERS tags:article:1

echo ""
echo "=== Set operations ==="
echo "--- Union (all unique tags across both articles) ---"
$CLI SUNION tags:article:1 tags:article:2

echo "--- Intersection (shared tags) ---"
$CLI SINTER tags:article:1 tags:article:2

echo "--- Difference (tags only in article:1) ---"
$CLI SDIFF tags:article:1 tags:article:2

echo ""
echo "=== Store results ==="
$CLI SINTERSTORE common_tags tags:article:1 tags:article:2
$CLI SMEMBERS common_tags

echo ""
echo "=== Random sample ==="
$CLI SRANDMEMBER tags:article:1 2   # pick 2 random tags
$CLI SPOP tags:article:1            # remove and return 1 random
