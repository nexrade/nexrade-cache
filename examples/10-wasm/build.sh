#!/usr/bin/env bash
# 10-wasm/build.sh — Build the WASM package and serve the demo.
set -euo pipefail

CRATE="../../crates/nexrade-wasm"
OUT="./pkg"

echo "=== Building WASM package ==="
wasm-pack build "$CRATE" --target web --out-dir "$(pwd)/$OUT" --release

echo ""
echo "=== Build complete ==="
echo "Files in $OUT:"
ls "$OUT"

echo ""
echo "=== Serving demo on http://localhost:8080 ==="
echo "(Press Ctrl+C to stop)"
# Any static file server works; python3 is usually available
python3 -m http.server 8080
