<p align="center">
  <img src="assets/logo.svg" alt="nexrade-cache" width="320" />
</p>

# nexrade-cache

**v0.2.1**

nexrade-cache is **a Redis-protocol-compatible cache server built in Rust**. It speaks the
RESP2 / RESP3 wire format, ships with TLS, Prometheus metrics, Lua scripting, a plugin API,
and a WebAssembly target — without OpenSSL or other C dependencies. **It is not a 1:1
implementation of Redis.** It implements the commands and behaviours most commonly used by
applications and proxies that talk to Redis, with intentional gaps in niche features. Check
the compatibility matrix to verify your workload before adopting it.

```sh
nexrade-cache --port 6379 --metrics
```

```sh
redis-cli ping
# PONG
```

---

## Why nexrade-cache?

Redis is great. But it ships without built-in observability, requires OpenSSL for TLS, and can't run in the browser or at the edge. nexrade-cache fixes all of that **for workloads whose command surface fits within its compatibility matrix**.

| | nexrade-cache | Redis OSS |
|---|:---:|:---:|
| RESP2 + RESP3 protocol (`HELLO`) | ✅ | ✅ |
| Most major data types + Streams + Geo + Bitmaps | ✅ | ✅ |
| Consumer groups (XGROUP / XREADGROUP / XACK) | ✅ | ✅ |
| RDB-style snapshots + AOF persistence | ⚠️ custom binary | ✅ |
| Lua 5.4 scripting (EVAL / EVALSHA) | ✅ | ✅ |
| Pub/Sub (with RESP3 push) | ✅ | ✅ |
| Primary / replica replication (REPLICAOF / PSYNC) | ✅ | ✅ |
| **Built-in Prometheus metrics** | ✅ | ❌ |
| **Structured JSON logging** | ✅ | ❌ |
| **TLS without OpenSSL (rustls)** | ✅ | ⚠️ requires OpenSSL + compile flag |
| **Plugin API (Rust, no C required)** | ✅ | ⚠️ C module API only |
| **WebAssembly / edge target** | ✅ | ❌ |
| **Embeddable Rust library** | ✅ | ❌ |
| **Multi-core parallel writes (sharded store)** | ✅ | ❌ |
| Memory-safe (written in Rust) | ✅ | ❌ |

---

## Installation

**Pre-built binary (Linux / macOS / WSL):**
```sh
curl -fsSL https://raw.githubusercontent.com/nexrade/nexrade-cache/main/install.sh | bash
```

**Build from source:**
```sh
cargo build --release
./target/release/nexrade-cache
```

**Docker:**
```sh
docker run -p 6379:6379 nexrade/nexrade-cache
```

---

## Quick Start

```sh
# Start the server (default port 6379)
nexrade-cache

# With Prometheus metrics on :9091
nexrade-cache --metrics

# With a config file
nexrade-cache --config nexrade.toml

# Connect with the built-in CLI (redis-cli compatible)
nexrade-cli
nexrade-cli -h 10.0.0.1 -p 6380

# Or use redis-cli — fully compatible
redis-cli set foo bar
redis-cli get foo
```

---

## Configuration

All settings can be set via config file, CLI flags, or environment
variables. Precedence is **config file < explicit CLI flag < built-in
default fallback** — a value set in the file is only overridden when you
actually pass the matching flag or env var.

```toml
# nexrade.toml
bind = "0.0.0.0"
port = 6379
databases = 16
requirepass = "secret"

[persistence]
rdb_path = "dump.rdb"
aof_path = "appendonly.aof"
aof_sync  = "everysec"      # always | everysec | no
rdb_save_rules = [
    [3600, 1],   # save after 1 change in 1 hour
    [300,  100], # save after 100 changes in 5 minutes
    [60,   10000],
]

[tls]
enabled = true
port    = 6380
cert    = "cert.pem"
key     = "key.pem"

[metrics]
enabled = true
port    = 9091
```

```sh
# Preview the fully-resolved config without starting the server
nexrade-cache --config nexrade.toml --print-config

# CLI flags override the config file
nexrade-cache --config nexrade.toml --port 6380 --requirepass secret

# Environment variables
NEXRADE_PORT=6380 NEXRADE_REQUIREPASS=secret nexrade-cache
```

See [`nexrade.example.toml`](nexrade.example.toml) for the full reference.

---

## Persistence

nexrade-cache persists data the same way Redis does — no migration needed.

**RDB snapshots** — point-in-time binary snapshots, triggered automatically by save rules or manually:

```sh
redis-cli SAVE      # blocking save
redis-cli BGSAVE    # background save
redis-cli LASTSAVE  # timestamp of last successful save
```

**AOF (Append-Only File)** — every write command is logged in RESP format and replayed on startup. Set `aof_sync = "always"` for maximum durability or `"everysec"` for a good durability/performance trade-off.

Both formats are automatically loaded on startup. RDB is loaded first, then AOF is replayed on top.

---

## Observability

### Prometheus

Metrics are exposed at `http://localhost:9091/metrics`:

```
nexrade_commands_total{cmd="SET"}               12345
nexrade_command_duration_seconds{cmd="GET"}     0.000042
nexrade_connected_clients                       42
nexrade_keyspace_hits_total{db="0"}             99999
nexrade_keyspace_misses_total{db="0"}           1
nexrade_db_keys{db="0"}                         10000
```

### Structured logs

```sh
NEXRADE_LOG_JSON=1 nexrade-cache
```

---

## TLS

No stunnel, no proxy, no OpenSSL. TLS is built in via [rustls](https://github.com/rustls/rustls).

```toml
[tls]
enabled = true
port    = 6380
cert    = "cert.pem"
key     = "key.pem"
```

Generate a self-signed certificate:

```sh
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes
```

Connect with redis-cli:

```sh
redis-cli --tls -p 6380 ping
```

---

## Lua Scripting

`EVAL`, `EVALSHA`, and `SCRIPT` work the same as Redis:

```sh
# Atomic compare-and-set
redis-cli EVAL "
  if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('SET', KEYS[1], ARGV[2])
  else
    return 0
  end
" 1 mykey expected newvalue

# Cache a script and call it by SHA
SHA=$(redis-cli SCRIPT LOAD "return redis.call('GET', KEYS[1])")
redis-cli EVALSHA $SHA 1 mykey
```

---

## RESP3

Negotiate RESP3 with the `HELLO` command for richer response types (maps, sets, doubles, booleans):

```sh
redis-cli HELLO 3
```

RESP3 clients receive typed responses — no more parsing heuristics. RESP2 is the default for full backwards compatibility.

---

## Stream Consumer Groups

Full consumer group support for reliable message processing:

```sh
# Create a consumer group
redis-cli XGROUP CREATE mystream grp1 $ MKSTREAM

# Read new messages as a consumer
redis-cli XREADGROUP GROUP grp1 consumer1 COUNT 10 STREAMS mystream >

# Acknowledge processed messages
redis-cli XACK mystream grp1 1700000000000-0

# Inspect pending messages
redis-cli XPENDING mystream grp1 - + 10
```

---

## Replication

nexrade-cache supports primary/replica replication compatible with the Redis replication protocol. A replica stays in sync via a full RDB resync on first connect, then streams every write command in real time.

### Starting a replica

**At runtime (redis-cli compatible):**
```sh
# Demote this instance to a replica of the given primary
redis-cli REPLICAOF 127.0.0.1 6379

# Promote back to primary (stop replicating)
redis-cli REPLICAOF NO ONE
```

### Checking replication status

```sh
redis-cli INFO replication
# role:master
# connected_slaves:1
# slave0:ip=127.0.0.1,port=6380,state=online,offset=1024,lag=0
# master_replid:a1b2c3d4e5f6...
# master_repl_offset:1024
# master_failover_state:no-failover

# On the replica:
# role:slave
# master_host:127.0.0.1
# master_port:6379
# master_link_status:up
# master_replid:a1b2c3d4e5f6...
# master_repl_offset:1024
# connected_slaves:0
```

### Behavior

- Replicas are **read-only** — write commands return `READONLY` error.
- On disconnect, the replica retries connecting to the primary with a 5-second back-off.
- Replication uses a **broadcast channel** internally; the primary serializes each write command as a RESP array and fans it out to all connected replicas.
- `REPLCONF ACK` keeps per-replica offsets up to date so the primary knows how far behind each replica is.

---

## Plugin API

Add custom commands without modifying core:

```rust
use nexrade_plugin::{Plugin, CommandHandler};
use nexrade_core::resp::Resp;

pub struct MyPlugin;

impl Plugin for MyPlugin {
    fn name(&self)        -> &str { "my-plugin" }
    fn version(&self)     -> &str { "1.0.0" }
    fn description(&self) -> &str { "Adds MY.PING" }

    fn commands(&self) -> Vec<CommandHandler> {
        vec![CommandHandler::new("MY.PING", |_db, _args, _| {
            Box::pin(async { Ok(Resp::bulk_str("MY.PONG")) })
        })]
    }
}
```

---

## Embedded Mode

Use nexrade-core as a library in your Rust application — no server process needed:

```rust
use nexrade_core::db::Db;
use nexrade_core::command::dispatch;
use nexrade_core::resp::Resp;

#[tokio::main]
async fn main() {
    let db = Db::default();

    dispatch(&db, vec![
        Resp::bulk_str("SET"),
        Resp::bulk_str("hello"),
        Resp::bulk_str("world"),
    ], 0).await;

    let result = dispatch(&db, vec![
        Resp::bulk_str("GET"),
        Resp::bulk_str("hello"),
    ], 0).await;

    println!("{}", result); // world
}
```

---

## WebAssembly / Edge Functions

Build for the browser or edge runtimes (Cloudflare Workers, Deno Deploy):

```sh
wasm-pack build crates/nexrade-wasm --target web --features wasm
```

```javascript
import init, { NexradeWasm } from './pkg/nexrade_wasm.js';

await init();
const store = new NexradeWasm();

await store.execute('SET session:123 active');
await store.execute('EXPIRE session:123 3600');
const val = await store.execute('GET session:123');
console.log(val); // active
```

---

## Supported Commands

<details>
<summary>Strings</summary>

`SET` `GET` `GETSET` `GETDEL` `GETEX` `MSET` `MSETNX` `MGET`
`SETNX` `SETEX` `PSETEX` `INCR` `INCRBY` `INCRBYFLOAT`
`DECR` `DECRBY` `APPEND` `STRLEN` `GETRANGE` `SETRANGE`
</details>

<details>
<summary>Lists</summary>

`LPUSH` `RPUSH` `LPUSHX` `RPUSHX` `LPOP` `RPOP`
`LLEN` `LRANGE` `LINDEX` `LSET` `LINSERT` `LREM` `LTRIM`
`LMOVE` `RPOPLPUSH` `LPOS` `BLPOP` `BRPOP` `LMPOP` `BLMPOP`
</details>

<details>
<summary>Hashes</summary>

`HSET` `HMSET` `HGET` `HMGET` `HDEL` `HEXISTS`
`HGETALL` `HKEYS` `HVALS` `HLEN` `HSETNX`
`HINCRBY` `HINCRBYFLOAT` `HSCAN` `HRANDFIELD`
</details>

<details>
<summary>Sets</summary>

`SADD` `SREM` `SISMEMBER` `SMISMEMBER` `SMEMBERS` `SCARD`
`SUNION` `SUNIONSTORE` `SINTER` `SINTERSTORE` `SDIFF` `SDIFFSTORE`
`SMOVE` `SRANDMEMBER` `SPOP` `SSCAN`
</details>

<details>
<summary>Sorted Sets</summary>

`ZADD` `ZCARD` `ZSCORE` `ZMSCORE` `ZINCRBY` `ZRANK` `ZREVRANK`
`ZRANGE` `ZREVRANGE` `ZRANGEBYSCORE` `ZREVRANGEBYSCORE` `ZRANGEBYLEX`
`ZCOUNT` `ZLEXCOUNT` `ZREM` `ZREMRANGEBYRANK` `ZREMRANGEBYSCORE`
`ZPOPMIN` `ZPOPMAX` `ZMPOP` `BZMPOP` `ZRANDMEMBER`
`ZUNIONSTORE` `ZINTERSTORE` `ZUNION` `ZINTER` `ZDIFF` `ZDIFFSTORE`
`ZINTERCARD` `ZSCAN`
</details>

<details>
<summary>Streams</summary>

`XADD` `XLEN` `XRANGE` `XREVRANGE` `XREAD` `XTRIM` `XDEL`
`XGROUP` `XREADGROUP` `XACK` `XPENDING`
</details>

<details>
<summary>Bitmaps</summary>

`SETBIT` `GETBIT` `BITCOUNT` `BITPOS` `BITOP` `BITFIELD`
</details>

<details>
<summary>Geo</summary>

`GEOADD` `GEOPOS` `GEODIST` `GEOHASH` `GEOSEARCH`
`GEORADIUS` `GEORADIUSBYMEMBER`
</details>

<details>
<summary>Generic</summary>

`DEL` `UNLINK` `EXISTS` `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT`
`EXPIRETIME` `PEXPIRETIME` `TTL` `PTTL` `PERSIST` `TYPE`
`RENAME` `RENAMENX` `KEYS` `SCAN` `RANDOMKEY` `COPY` `MOVE`
`OBJECT` `DUMP` `RESTORE` `SORT` `TOUCH`
</details>

<details>
<summary>Server</summary>

`PING` `ECHO` `QUIT` `SELECT` `DBSIZE` `FLUSHDB` `FLUSHALL`
`INFO` `CONFIG` `COMMAND` `SAVE` `BGSAVE` `BGREWRITEAOF`
`LASTSAVE` `DEBUG` `SHUTDOWN` `SLOWLOG` `MEMORY` `LATENCY`
`ACL` `RESET` `HELLO`
`CLIENT` (`LIST` `INFO` `PAUSE` `UNPAUSE` `SETNAME` `GETNAME` `ID` `NO-EVICT` `REPLY` `KILL`)
`CLUSTER` (`KEYSLOT` `NODES` `INFO` `MYID` `COUNTKEYSINSLOT` `GETKEYSINSLOT` `SLOTS`)
`MULTI` `EXEC` `DISCARD` `WATCH` `UNWATCH`
`EVAL` `EVALSHA` `SCRIPT`
`SUBSCRIBE` `UNSUBSCRIBE` `PSUBSCRIBE` `PUNSUBSCRIBE` `PUBLISH` `PUBSUB`
`REPLICAOF` `SLAVEOF` `REPLCONF` `PSYNC`
</details>

---

## Multi-core Scaling

Redis processes all commands on a single thread. nexrade-cache uses a **sharded store** that scales write throughput with the number of CPU cores.

### How it works

On startup, nexrade-cache creates `N` independent shards where `N = next_power_of_two(num_cpus)` (clamped between 16 and 64). Each shard has its own `RwLock<Database>`. Keys are routed to shards via a fast FNV-1a hash of the key bytes.

```
key "user:1"  → shard 3  (RwLock<Database>)
key "user:2"  → shard 11 (RwLock<Database>)
key "session" → shard 7  (RwLock<Database>)
```

Concurrent writes to different keys acquire **different locks** — no contention. On an 8-core machine, independent key workloads can achieve up to 8× the write throughput compared to a single-threaded design.

### Atomic cross-key operations

Operations that touch multiple keys acquire shard locks in a **deterministic sorted order** to prevent deadlocks:

| Operation | Strategy |
|-----------|-----------|
| `RENAME` / `RENAMENX` / `COPY` | Lock src shard + dst shard in index order |
| `LMOVE` / `RPOPLPUSH` | Atomic cross-shard list move |
| `SMOVE` | Atomic cross-shard set move |
| `MSET` / `MSETNX` | Try-lock all affected shards; back off and retry the whole sweep on contention instead of blocking while holding earlier shards |
| `DEL` / `EXISTS` / `MGET` | One shard per key, independent |

### Whole-database operations

`KEYS`, `SCAN`, `DBSIZE`, `FLUSHDB`, `FLUSHALL`, and `RANDOMKEY` iterate or aggregate all shards. RDB persistence uses `snapshot()` which merges all shards for serialization, and distributes entries back to their correct shards on load.

---

## Performance

Measured with `redis-benchmark` against Redis 7.4.1 on the same machine (loopback, no TLS).

**No pipelining** (`-c 50 -n 100000 -q` — the shape most real client traffic takes):
nexrade-cache **beats Redis on every commonly-used command**, typically 5-13% faster with
lower p99 latency:

| Command | nexrade-cache | Redis 7.4.1 | Delta |
|---------|:---:|:---:|:---:|
| PING | 240K rps | 220K rps | **+9%** |
| SET | 243K rps | 229K rps | **+6%** |
| GET | 241K rps | 223K rps | **+8%** |
| INCR | 245K rps | 226K rps | **+8%** |
| HSET | 243K rps | 232K rps | **+5%** |
| ZADD | 246K rps | 230K rps | **+7%** |
| SADD | 245K rps | 230K rps | **+7%** |
| MSET (10 keys) | 250K rps | 275K rps | -9% |
| LRANGE_600 | 40K rps | 41K rps | -2% |

**Pipelined** (`-P 50 -c 50` — many in-flight commands per connection): the gap against
Redis has been closed from 6.5× down to **~1.0-1.1×** on the common write commands,
with several now at parity or ahead:

| Command | nexrade-cache | Redis 7.4.1 | Gap |
|---------|:---:|:---:|:---:|
| GET | 3.9-4.0M rps | 4.2M rps | 1.0-1.1× |
| HSET | 3.0M rps | 3.0M rps | ~parity |
| SET | ~3.1-3.2M rps | 3.4M rps | ~1.1× |
| LPUSH | 2.2-3.1M rps | 3.0-3.1M rps | 1.0-1.4× |
| ZADD | 2.1-3.5M rps | 2.9M rps | 0.8-1.4× (nexrade ahead at the high end) |
| INCR (single-key) | ~3.2-4.2M rps | 4.17M rps | ~1.0-1.3× |
| MSET (fixed-key) | edges ahead of Redis | — | nexrade wins |

The single-key `INCR` contention gap (previously ~4.5× under heavy same-key
concurrency) is now closed via an atomic CAS fast path that skips the shard's
exclusive write lock entirely once a key is promoted to an integer
representation. The one gap still open going into a future release is
pipelined `MSET` against a *randomized* keyspace (each call's keys land on
different, disjoint shard sets) — nexrade-cache still trails Redis there by
roughly 1.7-1.8×, since acquiring several shard locks atomically is a
structural cost real Redis's single-threaded event loop doesn't pay. Closing
that further needs a full per-shard deferred-queue design, not a smaller
patch. The numbers above come from atomic mirrors for the replica-role,
maxmemory, `CLIENT TRACKING`, and
per-command metrics-handle checks, a single-lookup entry API for
string/list/hash/set/zset writes, a try-lock-all-with-backoff rewrite for
MSET/MSETNX, a multi-threaded Tokio runtime, and per-batch (not per-command)
connection metadata refresh.

### Hot-path optimisations

Beyond single-thread throughput, the storage layer avoids the big constant-factor sources of overhead:

| Path | Before | After |
|------|--------|-------|
| `GET` LRU-clock update | `SystemTime::now()` syscall (~25-50ns) per access | Single relaxed atomic load (~1ns) refreshed by the background tick |
| LRU eviction selection (`allkeys-lru`) | Scan all entries to find min | Reservoir sample of 5 random entries (Redis default) |
| Memory check in `evict_if_needed` | Recompute total bytes from every entry | Sum of per-shard atomics (O(shards)) |
| `SET` / `HSET` / `SADD` / `ZADD` / `LPUSH`/`RPUSH` on existing key | Up to 3 `HashMap` lookups (`contains_key` → `insert` → `get_mut`) | 1 lookup via `entries.entry()` / `Database::get_or_insert_with` |
| Replica-role / replica-count / `CLIENT TRACKING`-enabled check per command | `parking_lot::RwLock` / broadcast-channel `Mutex` per call | Atomic mirror, single relaxed/acquire load — skipped entirely when nobody's using the feature |
| Per-command Prometheus metric handle resolution | `with_label_values` (hash + `RwLock::read()` + lookup) × 3 per command | Cached `(cmd_name, handles)` pair, reused across runs of the same command in a pipeline batch |
| `INCR`/`DECR`/`INCRBY`/`DECRBY` on a promoted key | Exclusive shard write-lock every call | `AtomicIntCell` read-lock CAS fast path; write-lock only for creation/promotion/expiry |
| `INCR`/`INCRBY`/`DECR`/`DECRBY` integer formatting | `i64::to_string()` (heap-allocating) | `itoa::Buffer` (stack, no allocation) |
| `MSET`/`MSETNX` shard acquisition | Sequential blocking `write()` per shard (convoy stalls under pipelining) | `try_write` sweep with backoff-and-retry; never holds a shard hostage while waiting on another |

See `crates/nexrade-core/tests/perf_tier2.rs` for the benchmark suite; the `estimated_memory_bytes()` × 10k call cost went from O(10M) entries to ~9 ms total.

---

## Memory Management

Set a memory limit and eviction policy to prevent unbounded growth:

```sh
nexrade-cache --maxmemory 256000000 --maxmemory-policy allkeys-lru
```

Or at runtime:
```sh
redis-cli CONFIG SET maxmemory 256000000
redis-cli CONFIG SET maxmemory-policy allkeys-lru
```

Supported eviction policies:

| Policy | Behavior |
|--------|----------|
| `noeviction` (default) | Return error on writes when limit reached |
| `allkeys-random` | Evict any random key |
| `allkeys-lru` | Evict least recently used key |
| `volatile-random` | Evict a random key with a TTL |
| `volatile-lru` | Evict least recently used key with a TTL |
| `volatile-ttl` | Evict the key with the soonest expiry |

Monitor memory usage:
```sh
redis-cli INFO memory
# used_memory:1048576
# used_memory_human:1.00M
```

---

## Architecture

```
nexrade-core     Storage engine (sharded store), RESP parser, all data types,
                 command handlers, replication state
nexrade-server   Async TCP listener, connection handling, slow log, RDB/AOF,
                 primary/replica replication (PSYNC streaming, replica task)
nexrade-tls      TLS via rustls (no OpenSSL dependency)
nexrade-metrics  Prometheus counters, JSON structured logs
nexrade-lua      Lua 5.4 scripting (EVAL, EVALSHA, SCRIPT)
nexrade-plugin   Plugin API for custom commands
nexrade-wasm     WebAssembly bindings for browser and edge runtimes
nexrade-cli      Server binary + interactive CLI client
```

All crates are independent. Use `nexrade-core` as a pure library, add `nexrade-server` for networking, and opt into the rest as needed.

### Internals at a glance

| Component | Detail |
|-----------|--------|
| Store | `ShardedDatabase` — N shards (FNV-1a key routing), each `RwLock<Database>` |
| Replication | `ReplicationState` (Arc-shared) — broadcast channel, offset, replica list |
| Connection | Tokio task per client; detects PSYNC and enters streaming mode for replicas |
| Replica task | Background task connecting to primary: PING → REPLCONF → PSYNC → stream |

---

## Windows Service

```powershell
# Install and start as a Windows Service (run as Administrator)
nexrade-cache.exe --install-service
Start-Service nexrade-cache

# Uninstall
nexrade-cache.exe --uninstall-service
```

### Windows ANSI Color Support

nexrade-cache and nexrade-cli automatically enable ANSI escape code support on Windows 10+, providing colored output in PowerShell, Command Prompt, and Windows Terminal.

---

## License

Licensed under either of [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE) at your option.
