# Redis CLUSTER compatibility

nexrade-cache is a **standalone** server. It does not implement Redis Cluster:
there is no gossip, no slot migration, no `MOVED` or `ASK` redirection, and no
second node to talk to.

It does implement the `CLUSTER` command's **read-only probe** subcommands, and
the slot-hash function behind `CLUSTER KEYSLOT` is byte-for-byte Redis's. That
is deliberate: cluster-aware clients open a connection and introspect the
topology before issuing ordinary commands. If those probes fail, a client
refuses to connect at all. Making them answer correctly costs nothing and keeps
those tools usable against a single node.

This document is the target of the error `CLUSTER MEET` and its siblings
return:

```
ERR CLUSTER MEET is not supported by nexrade-cache (standalone server).
See docs/cluster-compat.md for guidance.
```

## What works

Verified against `nexrade-cache` 1.4.2 on a single node.

| Subcommand | Reply | Notes |
|---|---|---|
| `CLUSTER INFO` | bulk string | Redis 7.x field set. Always reports `cluster_enabled:0`, `cluster_known_nodes:1`, `cluster_size:1`. |
| `CLUSTER MYID` | 40-char hex | Stable for the process lifetime, from a UUIDv4. |
| `CLUSTER KEYSLOT <key>` | integer | Standard Redis CRC16 slot. See below. |
| `CLUSTER NODES` | single self-line | One node covering slots `0-16383`, role `myself,master`. |
| `CLUSTER SLOTS` | one range | `[[0, 16383, [node]]]`. |
| `CLUSTER COUNTKEYSINSLOT <slot>` | integer | Counts across every logical DB. |
| `CLUSTER GETKEYSINSLOT <slot> <count>` | array | Same scope; short-circuits at `count`. |
| `CLUSTER LINKS` | empty array | Standalone emits no gossip. |
| `CLUSTER COUNT-FAILURE-REPORTS <id>` | `0` | No failure tracking. |
| `CLUSTER HELP` | array | |

`CLUSTER KEYSLOT` matches Redis exactly, including hash tags. `{user1000}.x`
and `bar{user1000}` both report the slot Redis reports; so does the empty-tag
quirk, where `{}foo` hashes as the whole key because the tag is empty. The
CRC-16/CCITT-XMODEM table is identical to Redis's `crc16tab[256]`.

## What is rejected

These return an error naming the subcommand:

```
MEET  FORGET  REPLICATE  RESET  SETSLOT  SAVECONFIG  FAILOVER
ADDSLOTS  ADDSLOTSRANGE  DELSLOTS  DELSLOTSRANGE  BUMPEPOCH
```

`CLUSTER FAILOVER-CHECK-RAYS` is rejected too, with its own message — it has no
quorum to check against, and this server is not part of one.

Two shape notes worth knowing before you point tooling at this:

- **Unknown subcommands return `+OK`, not an error.** This is Redis's own
  behaviour and is kept deliberately, so probes for newer protocol features do
  not fail. It means `CLUSTER READONLY`, `READWRITE`, `FLUSH`, `SHARDS`, and
  `MYEPOCH` all silently succeed without doing anything. Do not read that `+OK`
  as confirmation that a slot-affecting command ran.
- **`CLUSTER SLOTS` puts `host:port` in the first element of the node triple.**
  Redis puts a bare host there, with the port as the second element. A client
  that treats element 0 as a hostname gets `127.0.0.1:6399` and may fail to
  parse it.

## Multi-key commands and CROSSSLOT

nexrade-cache **does not** return `CROSSSLOT`, and this is correct.

Redis only enforces CROSSSLOT when `cluster-enabled yes`. With
`cluster-enabled no`, standalone Redis also accepts `MSET foo 1 bar 2` across
any slots. nexrade-cache is always in that mode, so:

```
MSET foo v1 somekey v2   -> OK
MGET foo somekey         -> ["v1", "v2"]
```

A `check_same_slot` function exists in `crates/nexrade-core/src/cluster.rs` and
is correct, but nothing calls it. Enforcing it would break ordinary standalone
use with no compatibility gain.

## Hash slots do not describe where data lives

This is the part most likely to mislead. There are two independent hash layers,
and only one of them is a Redis cluster slot:

**Slot number** — `crc16(extract_hash_tag(key)) % 16384`, computed on demand in
`cluster::keyslot`. Honours `{...}` tags. Used only to answer
`CLUSTER KEYSLOT`, `COUNTKEYSINSLOT`, and `GETKEYSINSLOT`.

**Shard selection** — `fnv1a(key) & (num_shards - 1)` in `store::shard_idx`,
over the **raw key bytes**. The hash tag is never consulted. Shard count is
`cpus.max(64).next_power_of_two().clamp(64, 256)` per logical database, each
shard an `Arc<RwLock<Database>>`.

So `{a}` and `{b}` co-locate in slot space and land in unrelated shards. Hash
tags change the number `CLUSTER KEYSLOT` reports and nothing else. They exist
here for compatibility and probing, not for co-location.

Two consequences:

- `COUNTKEYSINSLOT` and `GETKEYSINSLOT` **scan every key in every database** and
  hash each one. They are O(keyspace) per call. They are fine for the
  introspection `redis-cli --cluster check` performs and unsuitable for a hot
  path or a large dataset.
- Sharding is for lock concurrency within one process. It is not a partition
  scheme, and it is not reachable from the network.

## What to do instead

Pick based on what you are actually solving.

**For read scaling**, run N independent nexrade-cache processes and shard in
your own application — consistent hashing on your key, a routing layer, or N
separate client pools. There is no in-repo example of this, because there is
no server-side support for it: the instances share nothing.

**For replicas and read-your-writes**, use real replication. It is
implemented, not stubbed:

```sh
redis-cli REPLICAOF 10.0.0.10 6380     # become a replica
redis-cli REPLICAOF NO ONE             # promote back
```

The primary serializes each write as a RESP array and broadcasts it; replicas
are read-only and reject writes with `READONLY You can't write against a read
only replica`. `WAIT <n> <timeout_ms>` blocks until `n` replicas acknowledge.
See the **Replication** section of the main README, and
`examples/12-cache-patterns/multi_az_replicas.py` for a working redis-py client
with pooling, retry, and `WAIT`.

Replication is **nexrade-to-nexrade only**. The PSYNC payload is a bincode
snapshot in a RESP bulk string, not Redis RDB, so pointing a real Redis at a
nexrade primary (or the reverse) will not interoperate. Partial resync does not
exist — every PSYNC is a full resync.

**For failover**, see [`redis-sentinel-compat.md`](redis-sentinel-compat.md).
Sentinel is not implemented either; that document covers what to run instead.

## Client configuration

**Do not point `redis.RedisCluster` at nexrade-cache.** It will probe the
topology, see one node owning every slot, and then behave as a single-node
routing layer — which works but obscures what is actually happening. Use plain
`redis.Redis(host, port)`.

`scripts/redis_py_cluster_smoke.py` exercises the probe surface a cluster
client touches and is the right starting point if you are debugging one.

## Verifying against a running server

```sh
redis-cli -p 6379 CLUSTER INFO
redis-cli -p 6379 CLUSTER NODES
redis-cli -p 6379 CLUSTER KEYSLOT 'user{1000}:profile'
redis-cli -p 6379 CLUSTER MEET 10.0.0.1 6380   # the error above
redis-cli -p 6379 MSET foo 1 bar 2             # OK, no CROSSSLOT
```