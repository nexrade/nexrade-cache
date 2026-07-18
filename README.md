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

Measured with `redis-benchmark` against **Redis 7.0.15** on the same machine
(loopback, no TLS, no persistence). nexrade-cache **0.2.3** release build with
jemalloc (Linux/macOS). Methodology matches the tables below:
`-c 50 -n 100000 -q` (no pipeline) and `-c 50 -n 1000000 -P 50 -q` (pipelined).

**No pipelining** (`-c 50 -n 100000 -q` — the shape most real client traffic
takes). nexrade-cache **beats Redis on every common single-key command**,
typically **+7–13%** with lower p50 latency:

| Command | nexrade-cache | Redis 7.0.15 | Delta |
|---------|:---:|:---:|:---:|
| PING | 236K rps | 211K rps | **+12%** |
| SET | 239K rps | 215K rps | **+11%** |
| GET | 234K rps | 209K rps | **+12%** |
| INCR | 239K rps | 212K rps | **+13%** |
| LPUSH | 236K rps | 214K rps | **+11%** |
| SADD | 235K rps | 213K rps | **+10%** |
| HSET | 239K rps | 218K rps | **+10%** |
| ZADD | 238K rps | 221K rps | **+7%** |
| MSET (10 keys) | 244K rps | 276K rps | −12% |
| LRANGE_100 | 134K rps | 176K rps | −24% |
| LRANGE_600 | 37K rps | 49K rps | −23% |

**Pipelined** (`-P 50 -c 50 -n 1000000 -q` — many in-flight commands per
connection). Most hot commands are **at parity or ahead**; several read-side
commands pull well ahead of Redis:

| Command | nexrade-cache | Redis 7.0.15 | Delta |
|---------|:---:|:---:|:---:|
| PING | 8.8M rps | 4.1M rps | **+111%** |
| SET | 3.29M rps | 3.30M rps | ~parity |
| GET | 5.9M rps | 4.2M rps | **+39%** |
| INCR | 5.6M rps | 4.0M rps | **+39%** |
| LPUSH | 2.7M rps | 3.1M rps | −12% |
| SADD | 4.6M rps | 3.9M rps | **+18%** |
| HSET | 3.9M rps | 3.1M rps | **+26%** |
| ZADD | 3.2M rps | 2.8M rps | **+13%** |
| LRANGE_100 | 371K rps | 322K rps | **+15%** |
| LRANGE_600 | 53K rps | 54K rps | ~parity |
| MSET (10 keys) | 1.14M rps | 0.81M rps | **+41%** |

### Remaining gaps (structural, not “one more micro-opt”)

| Gap | Why |
|-----|-----|
| Non-pipe `LRANGE` (~−20%) | Redis short lists are often a contiguous **listpack**; we store `VecDeque<Bytes>` and frame each element as its own bulk string. Closing this needs a list-encoding change, not more serialize polish. Pipelined LRANGE is already at/above Redis. |
| Pipelined `LPUSH` (~−12%) | Multi-threaded shard lock vs Redis’s single-threaded push loop. |
| Non-pipe `MSET` (~−12%) | Multi-shard try-lock cost that Redis’s single-thread loop doesn’t pay. Pipelined fixed-key MSET is **ahead**. |

The old single-key `INCR` contention gap (previously ~4.5× under heavy same-key
concurrency) is closed via an atomic CAS fast path that skips the shard’s
exclusive write lock once a key is promoted to an integer representation.

### Hot-path optimisations

Beyond single-thread throughput, the storage and connection layers avoid the
big constant-factor sources of overhead:

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
| `LPUSH` / `XADD` / `ZADD` waiter wake | Always `Notify::notify_waiters` | Atomic waiter count; notify only when someone is parked |
| ACL check on unrestricted `default` | Users `RwLock` + HashMap every command | Single atomic “open ACL” load |
| Key extraction for tracking/ACL | Always allocate `Vec<Vec<u8>>` of touched keys | Skipped when ACL is open and no client has TRACKING enabled |
| `DataType::String` storage | `Vec<u8>` copy on every `SET` | `Bytes` with compact `copy_from_slice` (no parser-buffer pin) |
| `LRANGE` reply buffer | Allocate+free multi-KB `BytesMut` per call | Thread-local buffer reused via `split().freeze()`; static empty `*0\r\n`; owned `Resp::Raw` write |
| Global allocator (Linux/macOS) | System allocator | `tikv-jemallocator` (matches Redis) |

See `crates/nexrade-core/tests/perf_tier2.rs` for the micro-benchmark suite; the
`estimated_memory_bytes()` × 10k call cost went from O(10M) entries to ~9 ms total.

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

# Install with a config file — the path is resolved to absolute and baked
# into the service's launch command, so every SCM-triggered start (including
# after a reboot) loads the same config instead of falling back to defaults
nexrade-cache.exe --install-service --config C:\nexrade\nexrade.toml
Start-Service nexrade-cache

# Uninstall
nexrade-cache.exe --uninstall-service
```

### Windows ANSI Color Support

nexrade-cache and nexrade-cli automatically enable ANSI escape code support on Windows 10+, providing colored output in PowerShell, Command Prompt, and Windows Terminal.

---

## License

Licensed under either of [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE) at your option.
