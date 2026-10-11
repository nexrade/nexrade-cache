# Redis Sentinel compatibility

nexrade-cache does **not** implement Sentinel. Every `SENTINEL` subcommand —
known or unknown, with or without arguments — returns:

```
ERR SENTINEL is not supported by nexrade-cache (standalone server).
See docs/redis-sentinel-compat.md for failover guidance.
```

There is no Sentinel mode, no `SENTINEL MONITOR`, no pub/sub-based failover,
and no discovery quorum. This document covers what to run instead.

Verified against `nexrade-cache` 1.4.2.

## Why you get a clear error instead of "unknown command"

The rejection is explicit rather than a fall-through. Sentinel clients (redis-py
`Sentinel`, Jedis, and others) classify an endpoint as a Sentinel or as not a
Sentinel based on the reply. A generic "unknown command" error is ambiguous and
some clients retry it; a message that names the reason lets them give up and
fall back immediately.

Note that this rejection is on the whole command name. `SENTINEL` with zero
arguments returns the same error, and so does a subcommand that does not exist
in Redis at all.

## What is actually available

Sentinel is the part that is missing. The rest of the high-availability picture
is implemented:

**Replication.** `REPLICAOF <host> <port>` makes this instance a replica;
`REPLICAOF NO ONE` promotes it back. The primary serializes each write command
as a RESP array and broadcasts it to connected replicas; `REPLCONF ACK` carries
per-replica offsets. Replicas reject writes with
`READONLY You can't write against a read only replica`. See the **Replication**
section of the main README.

**`WAIT <numreplicas> <timeout_ms>`.** Blocks until the given number of
replicas have acknowledged the preceding writes, or the timeout expires. This
is a real implementation, not a stub. With no replicas attached it returns `0`
immediately.

**Graceful shutdown on `SIGTERM` and `SIGINT`.** Both are handled on Unix. The
listener drains, quiesces persistence, writes a final RDB snapshot, flushes and
fsyncs the AOF, then exits. Persistence failure exits non-zero rather than
silently. This is what makes a process-manager restart lossless.

**Health and readiness probes.** `/healthz` (liveness), `/readyz`
(readiness), and `/metrics` on a separately-bound operations listener. Readiness
returns 503 while starting, recovering, or draining, and 200 only when ready.
Reason codes come from a closed enum — `aof_failed`, `snapshot_too_old`,
`replica_link_down`, `replication_lag_exceeded`, and so on — so an orchestrator
learns *why* an instance is unready without the response leaking file paths or
internal error text.

The operations listener has **no authentication**. Since 1.3.0 the probe body
is minimal by default for that reason. Bind it to loopback, or set
`health.expose_details = true` only where trusted collectors are the sole
reachable client.

## What is missing

- **No automatic failover.** Nothing monitors a primary and promotes a
  replica when it dies. `REPLICAOF NO ONE` is a manual operator step.
- **No quorum, consensus, or leader election.** No Raft, no majority logic, no
  split-brain protection.
- **No client-side discovery.** nexrade-cache does not tell clients where the
  primary currently is. Your application must be pointed at a concrete
  `host:port`.
- **No partial resync.** Every `PSYNC` is a full resync, so a replica that
  reconnects receives the whole dataset.
- **Replication is nexrade-to-nexrade only.** The PSYNC payload is a bincode
  snapshot in a RESP bulk string, not Redis RDB. A real Redis cannot be a
  replica of nexrade-cache, or the reverse.

## Recommended setup

Run nexrade-cache under a **process manager or orchestrator** and let it do
failover at the process level.

**systemd, Docker, or Kubernetes** handle restart-on-exit. Because `SIGTERM`
triggers a clean drain, stop the old primary before starting the new one and no
acknowledged write is lost.

For a primary/replica pair:

```sh
# primary
nexrade-cache --port 6379 --rdb-path /var/lib/nexrade/dump.rdb

# replica
nexrade-cache --port 6380 --rdb-path /var/lib/nexrade-replica/dump.rdb
redis-cli -p 6380 REPLICAOF 10.0.0.10 6379

# confirm
redis-cli -p 6379 INFO replication
```

Failover, when the primary at `10.0.0.10` is gone:

```sh
redis-cli -p 6380 REPLICAOF NO ONE        # promote; manual, by an operator
redis-cli -p 6380 INFO replication        # role:master
# repoint your application / load balancer at 10.0.0.11:6380
```

Replication is configured **only** at runtime through `REPLICAOF`. There is no
`--replicaof` flag and no config-file key for it.

Point your load balancer or client pool at `/readyz` so a draining instance
stops receiving traffic before it exits. Run a pre-stop `SIGTERM`, then wait
for `/readyz` to report `draining`, then stop routing new connections to it.

**Multiple availability zones** work as ordinary replication: run one primary
and N replicas across zones and use `WAIT` for write durability.
`examples/12-cache-patterns/multi_az_replicas.py` is a complete redis-py client
showing pooling, retry with exponential backoff, and a write-then-`WAIT` path.
Be aware that `WAIT` counts replicas that have acknowledged to the primary — it
is not a durability barrier against losing a replica, so size the replica count
against how many you can afford to lose.

## Client configuration

**redis-py** — use a plain client, not `Sentinel`:

```python
# Wrong: expects a Sentinel endpoint
r = redis.Sentinel([("10.0.0.10", 26379)], socket_timeout=0.2)
r.master_for("mymaster")

# Right: point at the primary you have determined
r = redis.Redis(host="10.0.0.10", port=6379)
```

The same applies to Jedis, Lettuce, and any other client — configure the
primary address directly and let your process manager own the failover.

## Verifying against a running server

```sh
redis-cli -p 6379 SENTINEL MASTER mymaster   # the error above
redis-cli -p 6379 PING                      # PONG — plain PING is unaffected
redis-cli -p 6379 INFO replication          # role, master_*, offsets
redis-cli -p 6379 WAIT 1 1000               # real; returns 0 with no replicas
```