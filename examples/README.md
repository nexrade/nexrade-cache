# nexrade-cache — Examples

Each folder is a self-contained example. Start the server first, then run any example.

```sh
# Start the server (from repo root)
./target/release/nexrade-cache --metrics
```

---

## 01-basic — Hello World
Connect and run the most common commands.

```sh
bash examples/01-basic/hello.sh
```

---

## 02-data-types — All Data Structures
Shell scripts demonstrating every major data type.

```sh
bash examples/02-data-types/strings.sh
bash examples/02-data-types/lists.sh
bash examples/02-data-types/hashes.sh
bash examples/02-data-types/sets.sh
bash examples/02-data-types/sorted-sets.sh
```

---

## 03-pubsub — Publish / Subscribe
Run subscriber and publisher in two separate terminals.

```sh
# Terminal 1
python3 examples/03-pubsub/subscriber.py

# Terminal 2
python3 examples/03-pubsub/publisher.py
```

**Requires:** `pip install redis`

---

## 04-transactions — MULTI / EXEC
Atomic operations and optimistic locking with WATCH.

```sh
# Shell version
bash examples/04-transactions/atomic-ops.sh

# Python version (includes WATCH + retry loop)
python3 examples/04-transactions/atomic-ops.py
```

**Requires:** `pip install redis`

---

## 05-lua — Lua Scripting
EVAL, EVALSHA, SCRIPT LOAD, and atomic Lua patterns.

```sh
bash examples/05-lua/scripting.sh
```

---

## 06-streams — Streams
Append-only log with producer/consumer using XADD / XREAD.

```sh
python3 examples/06-streams/producer-consumer.py
```

**Requires:** `pip install redis`

---

## 07-pipeline — Pipelining
Batch commands in a single round-trip for maximum throughput.

```sh
python3 examples/07-pipeline/benchmark.py
```

**Requires:** `pip install redis`

---

## 08-embedded — Embedded Rust Library
Use nexrade-core directly inside your Rust process — no TCP, no server.

```sh
cargo run --manifest-path examples/08-embedded/Cargo.toml
```

Source files:
- `src/basic_kv.rs` — SET, GET, INCR, HSET, ZADD, …
- `src/transactions.rs` — MULTI/EXEC/DISCARD
- `src/pubsub.rs` — in-process pub/sub broker

---

## 09-plugin — Custom Plugin
Extend nexrade-cache with your own commands.

Commands added by this example plugin:
| Command | Description |
|---------|-------------|
| `MYAPP.ECHO <msg>` | Echo with `[myapp]` prefix |
| `MYAPP.SETJSON <key> <json>` | Store only if valid JSON |
| `MYAPP.STATS` | Quick keyspace summary |

```sh
cargo build --manifest-path examples/09-plugin/Cargo.toml
```

Register in your server:
```rust
registry.register(Box::new(MyAppPlugin), &ctx).await?;
```

---

## 10-wasm — Browser / Edge (WebAssembly)
Run the full cache in your browser tab.

```sh
bash examples/10-wasm/build.sh
# Open http://localhost:8080
```

**Requires:** `wasm-pack` (`cargo install wasm-pack`)

---

## 11-python-patterns — Common redis-py Patterns
Practical Python patterns using `redis-py` tested against nexrade-cache.

Covers: `scan()`, `scan_iter()`, `keys()`, `pipeline()`, `ttl()`,
`hset()` / `hgetall()` / `hdel()`, and `delete()`.

```sh
python3 examples/11-python-patterns/patterns.py
```

**Requires:** `pip install redis`

---

## Environment Variables

All shell/Python examples respect these variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `NEXRADE_HOST` | `127.0.0.1` | Server hostname |
| `NEXRADE_PORT` | `6379` | Server port |
| `NEXRADE_PASS` | _(none)_ | Auth password |

Example:
```sh
NEXRADE_HOST=10.0.0.1 NEXRADE_PORT=6380 bash examples/01-basic/hello.sh
```
