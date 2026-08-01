<p align="center">
  <img src="assets/logo.svg" alt="nexrade-cache" width="320" />
</p>

# nexrade-cache

**v1.2.1**

nexrade-cache is **a Redis-protocol-compatible cache server built in Rust**. It speaks the
RESP2 / RESP3 wire format, ships with TLS, Prometheus metrics, Lua scripting, a plugin API,
and a WebAssembly target — without OpenSSL or other C dependencies. **It is not a 1:1
implementation of Redis.** It implements the commands and behaviours most commonly used by
applications and proxies that talk to Redis, with intentional gaps in niche features —
see [Supported Commands](#supported-commands) below for the full surface.

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

See [`nexrade.example.toml`](nexrade.example.toml) for the full reference,
or [`nexrade.cache.toml`](nexrade.cache.toml) for a **production cache
profile** (`maxmemory` + `allkeys-lru`, persistence off) — see
[`examples/12-cache-patterns/`](examples/12-cache-patterns) for matching
redis-py connection-pool, cache-aside, and eviction-smoke examples.

---

## Persistence

nexrade-cache persists its own dataset with RDB snapshots and AOF — the same
*ops model* as Redis, but the on-disk formats are **nexrade-native** (not Redis
RDB/AOF). To move data **from Redis into nexrade**, use
[`scripts/migrate_from_redis.py`](scripts/migrate_from_redis.py). For
nexrade→nexrade opaque blobs, see `DUMP` / `RESTORE`
(`NEXD` payload — not Redis RDB).

**RDB snapshots** — point-in-time binary snapshots, triggered automatically by save rules or manually:

```sh
redis-cli SAVE      # blocking save
redis-cli BGSAVE    # background save
redis-cli LASTSAVE  # timestamp of last successful save
```

**AOF (Append-Only File)** — every write command is logged in RESP format and replayed on startup. Set `aof_sync = "always"` for durable acknowledgements, `"everysec"` to accept up to roughly one second of power-loss exposure, or `"no"` only when durability is not required.

### Recovery-source rule

Current RDB and AOF files are each complete recovery sources. Configure and retain
**one authoritative source at a time**: RDB-only, AOF-only, or neither for a
pure cache. nexrade-cache deliberately rejects startup when both an RDB and a
full legacy AOF exist, because replaying an uncheckpointed full AOF after its
snapshot would duplicate non-idempotent writes such as `INCR`, `LPUSH`, and
`XADD`. Choose the artifact you trust, preserve the other until recovery is
verified, then disable its path before restarting.

Existing configured persistence artifacts are validated before the server binds:
a corrupt RDB, truncated/malformed AOF, semantic replay error, or unavailable
AOF writer aborts startup rather than serving an empty or partially replayed
dataset. `INFO persistence` exposes an AOF I/O failure and disables later
writes until the operator resolves it.

`BGREWRITEAOF` takes an exclusive persistence window while publishing and
reopening the log. It favors write safety over rewrite availability: writers
wait until the handoff is complete, so no acknowledged write is lost in the
old-file-to-new-file transition.

---

## Observability

### Prometheus

Metrics are exposed at `http://localhost:9091/metrics` (bind address and
port configurable under `[metrics]`; bound to a separate address from
the Redis data-plane listener so an operator can scope Prometheus to a
sidecar interface):

```
nexrade_commands_total{cmd="SET"}               12345
nexrade_command_duration_seconds{cmd="GET"}     0.000042
nexrade_connected_clients                       42
nexrade_keyspace_hits_total{db="0"}             99999
nexrade_keyspace_misses_total{db="0"}           1
nexrade_db_keys{db="0"}                         10000
```

### Operations HTTP (1.2.0)

`/healthz` (liveness), `/readyz` (readiness), and `/metrics` are the
operator's surface. Bind separately under `[health]` (default disabled)
and `[metrics]`. The same JSON `HealthReport` is reachable via
`INFO health`. Readiness reason codes (`aof_failed`, `snapshot_too_old`,
`replica_link_down`, `replication_lag_exceeded`, …) appear in the JSON
body and in `INFO health` when `/readyz` returns 503.

### Preflight (1.2.0)

Validate a config without binding listeners, opening files, or starting
tasks — safe as a deploy-pipeline gate:

```sh
nexrade-cache --config /etc/nexrade/nexrade.toml --preflight
```

Exit code 0 means "would start cleanly"; non-zero with per-error
diagnostics means a startup-relevant problem was found.

### Operator drill (1.2.0)

End-to-end verification of the production profile against the release
binary:

```sh
python3 scripts/operator_drill.py --binary ./target/release/nexrade-cache
```

Runs 12 checks: `--print-config`, `--preflight` (valid / RDB+AOF /
bad-path), clean start with `/healthz` + `/readyz` + `/metrics`
returning 200, SIGTERM exit 0, RDB restart preserves keys, corrupt AOF
rejected, truncated RDB rejected, unwritable persistence path rejected.
Run locally before any release tag push; CI runs the same script in
the `ops-drill` job.

### Production profile (1.2.0)

[`nexrade.production.toml`](nexrade.production.toml) is the documented
standalone profile: loopback operations HTTP, absolute persistence
paths, RDB-only default, conservative save rules, auth/TLS guidance,
graceful-shutdown expectations.

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
`DECR` `DECRBY` `APPEND` `STRLEN` `GETRANGE` `SETRANGE` `LCS`
</details>

<details>
<summary>Lists</summary>

`LPUSH` `RPUSH` `LPUSHX` `RPUSHX` `LPOP` `RPOP`
`LLEN` `LRANGE` `LINDEX` `LSET` `LINSERT` `LREM` `LTRIM`
`LMOVE` `BLMOVE` `RPOPLPUSH` `LPOS` `BLPOP` `BRPOP` `LMPOP` `BLMPOP`
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
`SUNION` `SUNIONSTORE` `SINTER` `SINTERSTORE` `SINTERCARD` `SDIFF` `SDIFFSTORE`
`SMOVE` `SRANDMEMBER` `SPOP` `SSCAN`
</details>

<details>
<summary>Sorted Sets</summary>

`ZADD` `ZCARD` `ZSCORE` `ZMSCORE` `ZINCRBY` `ZRANK` `ZREVRANK`
`ZRANGE` `ZREVRANGE` `ZRANGEBYSCORE` `ZREVRANGEBYSCORE` `ZRANGEBYLEX`
`ZCOUNT` `ZLEXCOUNT` `ZREM` `ZREMRANGEBYRANK` `ZREMRANGEBYSCORE`
`ZPOPMIN` `ZPOPMAX` `BZPOPMIN` `BZPOPMAX` `ZMPOP` `BZMPOP` `ZRANDMEMBER`
`ZUNIONSTORE` `ZINTERSTORE` `ZUNION` `ZINTER` `ZDIFF` `ZDIFFSTORE`
`ZINTERCARD` `ZSCAN`
</details>

<details>
<summary>Streams</summary>

`XADD` `XLEN` `XRANGE` `XREVRANGE` `XREAD` `XTRIM` `XDEL`
`XGROUP` `XREADGROUP` `XACK` `XPENDING` `XINFO` `XCLAIM` `XAUTOCLAIM`
</details>

<details>
<summary>Bitmaps</summary>

`SETBIT` `GETBIT` `BITCOUNT` `BITPOS` `BITOP` `BITFIELD` `BITFIELD_RO`
</details>

<details>
<summary>Geo</summary>

`GEOADD` `GEOPOS` `GEODIST` `GEOHASH` `GEOSEARCH` `GEOSEARCHSTORE`
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

## Performance

Tables below are the stable **single-key / pipe grid** first published around
**0.5.1** (2-run averages vs **Redis 7.0.15**, loopback, no TLS, no persistence,
jemalloc on Linux/macOS). Methodology: `-c 50 -n 100000 -q` (no pipeline) and
`-c 50 -n 1000000 -P 50 -q` (pipelined). **Residual rows were re-checked through
1.0.0** (no regressions; random pipe MSET improved to **1.06× Redis** at 0.7.1).

**No pipelining** (`-c 50 -n 100000 -q` — the shape most real client traffic
takes). nexrade-cache **beats Redis on every common single-key command**,
typically **+4–10%** with lower p50 latency:

| Command | nexrade-cache | Redis 7.0.15 | Delta |
|---------|:---:|:---:|:---:|
| SET | 234K rps | 214K rps | **+9%** |
| GET | 233K rps | 214K rps | **+9%** |
| LPUSH | 235K rps | 220K rps | **+7%** |
| RPUSH | 234K rps | 220K rps | **+7%** |
| LPOP | 232K rps | 220K rps | **+6%** |
| RPOP | 233K rps | 219K rps | **+6%** |
| SADD | 232K rps | 211K rps | **+10%** |
| HSET | 233K rps | 217K rps | **+7%** |
| SPOP | 234K rps | 214K rps | **+9%** |
| ZADD | 236K rps | 225K rps | **+4%** |
| MSET (10 keys) | 240K rps | 267K rps | −10% |
| LRANGE_100 | 140K rps | 178K rps | −21%† |
| LRANGE_600 | 40K rps | 50K rps | −20%† |

† Multi-client (`-c 50`) non-pipe LRANGE still trails Redis’s single-threaded
event loop when 50 clients hammer one shared list. **Single-client**
(`-c 1`) is at parity; pipelined LRANGE_100 is **ahead**. Compact buffers
are Arc-shared so concurrent LRANGE snaps with a refcount bump (no
payload memcpy) under a brief lock, then frames outside into a TLS
buffer — the remaining multi-client gap is concurrency shape, not layout.

**Pipelined** (`-P 50 -c 50 -n 1000000 -q` — many in-flight commands per
connection). Dual-encoding write paths stay competitive with accurate
`maxmemory` accounting (collections + string/bitmap/stream/geo mutators):

| Command | nexrade-cache | Redis 7.0.15 | Delta |
|---------|:---:|:---:|:---:|
| SET | 3.58M rps | 3.42M rps | **+4.7%** |
| GET | 6.85M rps | 4.31M rps | **+59%** |
| LPUSH | 3.28M rps | 2.94M rps | **+12%** |
| RPUSH | 3.46M rps | 3.32M rps | **+4%** |
| LPOP | 4.32M rps | 2.77M rps | **+56%** |
| RPOP | 4.33M rps | 2.93M rps | **+48%** |
| SADD | 4.95M rps | 3.92M rps | **+26%** |
| HSET | 4.18M rps | 3.10M rps | **+35%** |
| SPOP | 5.99M rps | 4.47M rps | **+34%** |
| ZADD | 4.49M rps | 2.72M rps | **+65%** |
| LRANGE_100 | 392K rps | 323K rps | **+21%** |
| LRANGE_600 | 54K rps | 53K rps | **+3%** |
| MSET (10 keys) | 1.29M rps | 887K rps | **+45%** |

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

## Verification

Client wire smokes and the operations drill, all CI-gated:

```sh
# Three independent RESP parsers against one release binary
python3 scripts/redis_py_smoke.py            # 31 PASS + 5 SKIP
python3 scripts/redis_py_cluster_smoke.py    # 13 PASS
python3 scripts/raw_resp_smoke.py            # 10 PASS (no client lib)

# End-to-end operations drill (print-config, preflight, health/ready/
# metrics, clean shutdown, RDB restore, damaged-input rejection)
cargo build --release -p nexrade-cache
python3 scripts/operator_drill.py --binary ./target/release/nexrade-cache
```

---

## License

Licensed under either of [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE) at your option.
