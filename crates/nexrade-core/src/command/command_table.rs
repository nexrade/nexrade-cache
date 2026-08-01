//! Static Redis-compatible COMMAND metadata table.
//!
//! Shape of each entry (Redis 6+):
//!   [name, arity, flags[], first_key, last_key, step, categories[]]
//!
//! Enough for redis-py / jedis / go-redis / lettuce connect-time probing.

use crate::command::get_str;
use crate::error::{NexradeError, Result};
use crate::resp::Resp;

/// Compact command metadata.
pub struct CmdInfo {
    pub name: &'static str,
    /// Positive = exact arity; negative = minimum (|arity|), variable.
    pub arity: i64,
    pub flags: &'static [&'static str],
    pub first_key: i64,
    pub last_key: i64,
    pub step: i64,
    pub group: &'static str,
}

impl CmdInfo {
    pub fn to_resp(&self) -> Resp {
        Resp::array(vec![
            Resp::bulk_str(self.name.to_string()),
            Resp::int(self.arity),
            Resp::array(
                self.flags
                    .iter()
                    .map(|f| Resp::bulk_str((*f).to_string()))
                    .collect(),
            ),
            Resp::int(self.first_key),
            Resp::int(self.last_key),
            Resp::int(self.step),
            Resp::array(vec![Resp::bulk_str(format!("@{}", self.group))]),
        ])
    }
}

macro_rules! cmd {
    ($name:expr, $arity:expr, $flags:expr, $fk:expr, $lk:expr, $step:expr, $group:expr) => {
        CmdInfo {
            name: $name,
            arity: $arity,
            flags: $flags,
            first_key: $fk,
            last_key: $lk,
            step: $step,
            group: $group,
        }
    };
}

/// Static table of implemented commands. Names are UPPERCASE.
pub const COMMAND_TABLE: &[CmdInfo] = &[
    // Strings
    cmd!("GET", 2, &["readonly", "fast"], 1, 1, 1, "string"),
    cmd!("SET", -3, &["write", "denyoom"], 1, 1, 1, "string"),
    cmd!("MGET", -2, &["readonly", "fast"], 1, -1, 1, "string"),
    cmd!("MSET", -3, &["write", "denyoom"], 1, -1, 2, "string"),
    cmd!("MSETNX", -3, &["write", "denyoom"], 1, -1, 2, "string"),
    cmd!(
        "GETSET",
        3,
        &["write", "denyoom", "fast"],
        1,
        1,
        1,
        "string"
    ),
    cmd!("GETDEL", 2, &["write", "fast"], 1, 1, 1, "string"),
    cmd!("GETEX", -2, &["write", "fast"], 1, 1, 1, "string"),
    cmd!("SETNX", 3, &["write", "denyoom", "fast"], 1, 1, 1, "string"),
    cmd!("SETEX", 4, &["write", "denyoom"], 1, 1, 1, "string"),
    cmd!("PSETEX", 4, &["write", "denyoom"], 1, 1, 1, "string"),
    cmd!(
        "APPEND",
        3,
        &["write", "denyoom", "fast"],
        1,
        1,
        1,
        "string"
    ),
    cmd!("STRLEN", 2, &["readonly", "fast"], 1, 1, 1, "string"),
    cmd!("GETRANGE", 4, &["readonly"], 1, 1, 1, "string"),
    cmd!("SETRANGE", 4, &["write", "denyoom"], 1, 1, 1, "string"),
    cmd!("INCR", 2, &["write", "denyoom", "fast"], 1, 1, 1, "string"),
    cmd!(
        "INCRBY",
        3,
        &["write", "denyoom", "fast"],
        1,
        1,
        1,
        "string"
    ),
    cmd!(
        "INCRBYFLOAT",
        3,
        &["write", "denyoom", "fast"],
        1,
        1,
        1,
        "string"
    ),
    cmd!("DECR", 2, &["write", "denyoom", "fast"], 1, 1, 1, "string"),
    cmd!(
        "DECRBY",
        3,
        &["write", "denyoom", "fast"],
        1,
        1,
        1,
        "string"
    ),
    cmd!("LCS", -3, &["readonly"], 1, 2, 1, "string"),
    // Lists
    cmd!("LPUSH", -3, &["write", "denyoom", "fast"], 1, 1, 1, "list"),
    cmd!("RPUSH", -3, &["write", "denyoom", "fast"], 1, 1, 1, "list"),
    cmd!("LPUSHX", -3, &["write", "denyoom", "fast"], 1, 1, 1, "list"),
    cmd!("RPUSHX", -3, &["write", "denyoom", "fast"], 1, 1, 1, "list"),
    cmd!("LPOP", -2, &["write", "fast"], 1, 1, 1, "list"),
    cmd!("RPOP", -2, &["write", "fast"], 1, 1, 1, "list"),
    cmd!("LLEN", 2, &["readonly", "fast"], 1, 1, 1, "list"),
    cmd!("LRANGE", 4, &["readonly"], 1, 1, 1, "list"),
    cmd!("LINDEX", 3, &["readonly"], 1, 1, 1, "list"),
    cmd!("LSET", 4, &["write", "denyoom"], 1, 1, 1, "list"),
    cmd!("LINSERT", 5, &["write", "denyoom"], 1, 1, 1, "list"),
    cmd!("LREM", 4, &["write"], 1, 1, 1, "list"),
    cmd!("LTRIM", 4, &["write"], 1, 1, 1, "list"),
    cmd!("LMOVE", 5, &["write"], 1, 2, 1, "list"),
    cmd!("BLMOVE", 6, &["write", "noscript"], 1, 2, 1, "list"),
    cmd!("RPOPLPUSH", 3, &["write"], 1, 2, 1, "list"),
    cmd!("BLPOP", -3, &["write", "noscript"], 1, -2, 1, "list"),
    cmd!("BRPOP", -3, &["write", "noscript"], 1, -2, 1, "list"),
    cmd!("LMPOP", -4, &["write"], 2, -1, 1, "list"),
    cmd!("BLMPOP", -5, &["write", "noscript"], 3, -1, 1, "list"),
    cmd!("LPOS", -3, &["readonly"], 1, 1, 1, "list"),
    // Hashes
    cmd!("HSET", -4, &["write", "denyoom", "fast"], 1, 1, 1, "hash"),
    cmd!("HMSET", -4, &["write", "denyoom", "fast"], 1, 1, 1, "hash"),
    cmd!("HGET", 3, &["readonly", "fast"], 1, 1, 1, "hash"),
    cmd!("HMGET", -3, &["readonly", "fast"], 1, 1, 1, "hash"),
    cmd!("HDEL", -3, &["write", "fast"], 1, 1, 1, "hash"),
    cmd!("HEXISTS", 3, &["readonly", "fast"], 1, 1, 1, "hash"),
    cmd!("HGETALL", 2, &["readonly"], 1, 1, 1, "hash"),
    cmd!("HKEYS", 2, &["readonly"], 1, 1, 1, "hash"),
    cmd!("HVALS", 2, &["readonly"], 1, 1, 1, "hash"),
    cmd!("HLEN", 2, &["readonly", "fast"], 1, 1, 1, "hash"),
    cmd!("HSETNX", 4, &["write", "denyoom", "fast"], 1, 1, 1, "hash"),
    cmd!("HINCRBY", 4, &["write", "denyoom", "fast"], 1, 1, 1, "hash"),
    cmd!(
        "HINCRBYFLOAT",
        4,
        &["write", "denyoom", "fast"],
        1,
        1,
        1,
        "hash"
    ),
    cmd!("HSCAN", -3, &["readonly", "random"], 1, 1, 1, "hash"),
    cmd!("HRANDFIELD", -2, &["readonly", "random"], 1, 1, 1, "hash"),
    // Sets
    cmd!("SADD", -3, &["write", "denyoom", "fast"], 1, 1, 1, "set"),
    cmd!("SREM", -3, &["write", "fast"], 1, 1, 1, "set"),
    cmd!("SISMEMBER", 3, &["readonly", "fast"], 1, 1, 1, "set"),
    cmd!("SMISMEMBER", -3, &["readonly", "fast"], 1, 1, 1, "set"),
    cmd!("SMEMBERS", 2, &["readonly"], 1, 1, 1, "set"),
    cmd!("SCARD", 2, &["readonly", "fast"], 1, 1, 1, "set"),
    cmd!("SUNION", -2, &["readonly"], 1, -1, 1, "set"),
    cmd!("SUNIONSTORE", -3, &["write", "denyoom"], 1, -1, 1, "set"),
    cmd!("SINTER", -2, &["readonly"], 1, -1, 1, "set"),
    cmd!("SINTERSTORE", -3, &["write", "denyoom"], 1, -1, 1, "set"),
    cmd!("SINTERCARD", -3, &["readonly"], 2, -1, 1, "set"),
    cmd!("SDIFF", -2, &["readonly"], 1, -1, 1, "set"),
    cmd!("SDIFFSTORE", -3, &["write", "denyoom"], 1, -1, 1, "set"),
    cmd!("SMOVE", 4, &["write", "fast"], 1, 2, 1, "set"),
    cmd!("SRANDMEMBER", -2, &["readonly", "random"], 1, 1, 1, "set"),
    cmd!("SPOP", -2, &["write", "random", "fast"], 1, 1, 1, "set"),
    cmd!("SSCAN", -3, &["readonly", "random"], 1, 1, 1, "set"),
    // Sorted sets
    cmd!(
        "ZADD",
        -4,
        &["write", "denyoom", "fast"],
        1,
        1,
        1,
        "sortedset"
    ),
    cmd!("ZCARD", 2, &["readonly", "fast"], 1, 1, 1, "sortedset"),
    cmd!("ZSCORE", 3, &["readonly", "fast"], 1, 1, 1, "sortedset"),
    cmd!("ZMSCORE", -3, &["readonly", "fast"], 1, 1, 1, "sortedset"),
    cmd!(
        "ZINCRBY",
        4,
        &["write", "denyoom", "fast"],
        1,
        1,
        1,
        "sortedset"
    ),
    cmd!("ZRANK", -3, &["readonly", "fast"], 1, 1, 1, "sortedset"),
    cmd!("ZREVRANK", -3, &["readonly", "fast"], 1, 1, 1, "sortedset"),
    cmd!("ZRANGE", -4, &["readonly"], 1, 1, 1, "sortedset"),
    cmd!("ZREVRANGE", -4, &["readonly"], 1, 1, 1, "sortedset"),
    cmd!("ZRANGEBYSCORE", -4, &["readonly"], 1, 1, 1, "sortedset"),
    cmd!("ZREVRANGEBYSCORE", -4, &["readonly"], 1, 1, 1, "sortedset"),
    cmd!("ZRANGEBYLEX", -4, &["readonly"], 1, 1, 1, "sortedset"),
    cmd!("ZCOUNT", 4, &["readonly", "fast"], 1, 1, 1, "sortedset"),
    cmd!("ZLEXCOUNT", 4, &["readonly", "fast"], 1, 1, 1, "sortedset"),
    cmd!("ZREM", -3, &["write", "fast"], 1, 1, 1, "sortedset"),
    cmd!("ZREMRANGEBYRANK", 4, &["write"], 1, 1, 1, "sortedset"),
    cmd!("ZREMRANGEBYSCORE", 4, &["write"], 1, 1, 1, "sortedset"),
    cmd!("ZPOPMIN", -2, &["write", "fast"], 1, 1, 1, "sortedset"),
    cmd!("ZPOPMAX", -2, &["write", "fast"], 1, 1, 1, "sortedset"),
    cmd!(
        "BZPOPMIN",
        -3,
        &["write", "noscript", "fast"],
        1,
        -2,
        1,
        "sortedset"
    ),
    cmd!(
        "BZPOPMAX",
        -3,
        &["write", "noscript", "fast"],
        1,
        -2,
        1,
        "sortedset"
    ),
    cmd!(
        "ZRANDMEMBER",
        -2,
        &["readonly", "random"],
        1,
        1,
        1,
        "sortedset"
    ),
    cmd!(
        "ZUNIONSTORE",
        -4,
        &["write", "denyoom"],
        1,
        1,
        1,
        "sortedset"
    ),
    cmd!(
        "ZINTERSTORE",
        -4,
        &["write", "denyoom"],
        1,
        1,
        1,
        "sortedset"
    ),
    cmd!(
        "ZDIFFSTORE",
        -4,
        &["write", "denyoom"],
        1,
        1,
        1,
        "sortedset"
    ),
    cmd!(
        "ZRANGESTORE",
        -5,
        &["write", "denyoom"],
        1,
        2,
        1,
        "sortedset"
    ),
    cmd!("ZUNION", -3, &["readonly"], 0, 0, 0, "sortedset"),
    cmd!("ZINTER", -3, &["readonly"], 0, 0, 0, "sortedset"),
    cmd!("ZDIFF", -3, &["readonly"], 0, 0, 0, "sortedset"),
    cmd!("ZINTERCARD", -3, &["readonly"], 0, 0, 0, "sortedset"),
    cmd!("ZMPOP", -4, &["write"], 0, 0, 0, "sortedset"),
    cmd!("BZMPOP", -5, &["write", "noscript"], 0, 0, 0, "sortedset"),
    cmd!("ZSCAN", -3, &["readonly", "random"], 1, 1, 1, "sortedset"),
    // Generic / keyspace
    cmd!("DEL", -2, &["write"], 1, -1, 1, "keyspace"),
    cmd!("UNLINK", -2, &["write", "fast"], 1, -1, 1, "keyspace"),
    cmd!("EXISTS", -2, &["readonly", "fast"], 1, -1, 1, "keyspace"),
    cmd!("EXPIRE", -3, &["write", "fast"], 1, 1, 1, "keyspace"),
    cmd!("PEXPIRE", -3, &["write", "fast"], 1, 1, 1, "keyspace"),
    cmd!("EXPIREAT", -3, &["write", "fast"], 1, 1, 1, "keyspace"),
    cmd!("PEXPIREAT", -3, &["write", "fast"], 1, 1, 1, "keyspace"),
    cmd!("TTL", 2, &["readonly", "fast"], 1, 1, 1, "keyspace"),
    cmd!("PTTL", 2, &["readonly", "fast"], 1, 1, 1, "keyspace"),
    cmd!("PERSIST", 2, &["write", "fast"], 1, 1, 1, "keyspace"),
    cmd!("TYPE", 2, &["readonly", "fast"], 1, 1, 1, "keyspace"),
    cmd!("RENAME", 3, &["write"], 1, 2, 1, "keyspace"),
    cmd!("RENAMENX", 3, &["write", "fast"], 1, 2, 1, "keyspace"),
    cmd!("KEYS", 2, &["readonly"], 0, 0, 0, "keyspace"),
    cmd!("SCAN", -2, &["readonly", "random"], 0, 0, 0, "keyspace"),
    cmd!("RANDOMKEY", 1, &["readonly", "random"], 0, 0, 0, "keyspace"),
    cmd!("COPY", -3, &["write", "denyoom"], 1, 2, 1, "keyspace"),
    cmd!("MOVE", 3, &["write", "fast"], 1, 1, 1, "keyspace"),
    cmd!("DUMP", 2, &["readonly"], 1, 1, 1, "keyspace"),
    cmd!("RESTORE", -4, &["write", "denyoom"], 1, 1, 1, "keyspace"),
    cmd!("TOUCH", -2, &["readonly", "fast"], 1, -1, 1, "keyspace"),
    cmd!("OBJECT", -2, &["readonly"], 2, 2, 1, "keyspace"),
    cmd!("SORT", -2, &["write", "denyoom"], 1, 1, 1, "keyspace"),
    cmd!("SORT_RO", -2, &["readonly"], 1, 1, 1, "keyspace"),
    // Connection / server
    cmd!("PING", -1, &["stale", "fast"], 0, 0, 0, "connection"),
    cmd!("ECHO", 2, &["fast"], 0, 0, 0, "connection"),
    cmd!("QUIT", -1, &["loading", "stale"], 0, 0, 0, "connection"),
    cmd!("SELECT", 2, &["loading", "fast"], 0, 0, 0, "connection"),
    cmd!(
        "AUTH",
        -2,
        &["noscript", "loading", "stale", "fast", "no_auth"],
        0,
        0,
        0,
        "connection"
    ),
    cmd!(
        "HELLO",
        -1,
        &["noscript", "loading", "stale", "fast", "no_auth"],
        0,
        0,
        0,
        "connection"
    ),
    cmd!(
        "RESET",
        1,
        &["noscript", "loading", "stale", "fast"],
        0,
        0,
        0,
        "connection"
    ),
    cmd!("DBSIZE", 1, &["readonly", "fast"], 0, 0, 0, "server"),
    cmd!("FLUSHDB", -1, &["write"], 0, 0, 0, "server"),
    cmd!("FLUSHALL", -1, &["write"], 0, 0, 0, "server"),
    cmd!(
        "INFO",
        -1,
        &["random", "loading", "stale"],
        0,
        0,
        0,
        "server"
    ),
    cmd!(
        "CONFIG",
        -2,
        &["admin", "noscript", "loading", "stale"],
        0,
        0,
        0,
        "server"
    ),
    cmd!(
        "COMMAND",
        -1,
        &["random", "loading", "stale"],
        0,
        0,
        0,
        "server"
    ),
    cmd!(
        "TIME",
        1,
        &["random", "loading", "stale", "fast"],
        0,
        0,
        0,
        "server"
    ),
    cmd!(
        "ROLE",
        1,
        &["noscript", "loading", "stale"],
        0,
        0,
        0,
        "server"
    ),
    cmd!(
        "CLIENT",
        -2,
        &["admin", "noscript", "loading", "stale"],
        0,
        0,
        0,
        "connection"
    ),
    cmd!(
        "ACL",
        -2,
        &["admin", "noscript", "loading", "stale"],
        0,
        0,
        0,
        "server"
    ),
    cmd!("MEMORY", -2, &["readonly"], 0, 0, 0, "server"),
    cmd!(
        "LATENCY",
        -2,
        &["admin", "noscript", "loading", "stale"],
        0,
        0,
        0,
        "server"
    ),
    cmd!(
        "SLOWLOG",
        -2,
        &["admin", "random", "loading", "stale"],
        0,
        0,
        0,
        "server"
    ),
    cmd!("SAVE", 1, &["admin", "noscript"], 0, 0, 0, "server"),
    cmd!("BGSAVE", -1, &["admin", "noscript"], 0, 0, 0, "server"),
    cmd!("BGREWRITEAOF", 1, &["admin", "noscript"], 0, 0, 0, "server"),
    cmd!("LASTSAVE", 1, &["random", "fast"], 0, 0, 0, "server"),
    cmd!(
        "SHUTDOWN",
        -1,
        &["admin", "noscript", "loading", "stale"],
        0,
        0,
        0,
        "server"
    ),
    cmd!("WAIT", 3, &["noscript"], 0, 0, 0, "server"),
    cmd!(
        "MONITOR",
        1,
        &["admin", "noscript", "loading", "stale"],
        0,
        0,
        0,
        "server"
    ),
    // Transactions
    cmd!(
        "MULTI",
        1,
        &["noscript", "loading", "stale", "fast"],
        0,
        0,
        0,
        "transaction"
    ),
    cmd!(
        "EXEC",
        1,
        &["noscript", "loading", "stale"],
        0,
        0,
        0,
        "transaction"
    ),
    cmd!(
        "DISCARD",
        1,
        &["noscript", "loading", "stale", "fast"],
        0,
        0,
        0,
        "transaction"
    ),
    cmd!(
        "WATCH",
        -2,
        &["noscript", "loading", "stale", "fast"],
        1,
        -1,
        1,
        "transaction"
    ),
    cmd!(
        "UNWATCH",
        1,
        &["noscript", "loading", "stale", "fast"],
        0,
        0,
        0,
        "transaction"
    ),
    // Pub/Sub
    cmd!(
        "PUBLISH",
        3,
        &["pubsub", "loading", "stale", "fast"],
        0,
        0,
        0,
        "pubsub"
    ),
    cmd!(
        "SUBSCRIBE",
        -2,
        &["pubsub", "noscript", "loading", "stale"],
        0,
        0,
        0,
        "pubsub"
    ),
    cmd!(
        "UNSUBSCRIBE",
        -1,
        &["pubsub", "noscript", "loading", "stale"],
        0,
        0,
        0,
        "pubsub"
    ),
    cmd!(
        "PSUBSCRIBE",
        -2,
        &["pubsub", "noscript", "loading", "stale"],
        0,
        0,
        0,
        "pubsub"
    ),
    cmd!(
        "PUNSUBSCRIBE",
        -1,
        &["pubsub", "noscript", "loading", "stale"],
        0,
        0,
        0,
        "pubsub"
    ),
    cmd!(
        "PUBSUB",
        -2,
        &["pubsub", "random", "loading", "stale"],
        0,
        0,
        0,
        "pubsub"
    ),
    cmd!(
        "SPUBLISH",
        3,
        &["pubsub", "loading", "stale", "fast"],
        0,
        0,
        0,
        "pubsub"
    ),
    cmd!(
        "SSUBSCRIBE",
        -2,
        &["pubsub", "noscript", "loading", "stale"],
        0,
        0,
        0,
        "pubsub"
    ),
    cmd!(
        "SUNSUBSCRIBE",
        -1,
        &["pubsub", "noscript", "loading", "stale"],
        0,
        0,
        0,
        "pubsub"
    ),
    // Scripting
    cmd!(
        "EVAL",
        -3,
        &["noscript", "may_replicate", "stale"],
        0,
        0,
        0,
        "scripting"
    ),
    cmd!(
        "EVALSHA",
        -3,
        &["noscript", "may_replicate", "stale"],
        0,
        0,
        0,
        "scripting"
    ),
    cmd!("SCRIPT", -2, &["noscript"], 0, 0, 0, "scripting"),
    cmd!("FUNCTION", -2, &["noscript"], 0, 0, 0, "scripting"),
    cmd!(
        "FCALL",
        -3,
        &["noscript", "may_replicate", "stale"],
        0,
        0,
        0,
        "scripting"
    ),
    // Streams
    cmd!("XADD", -5, &["write", "denyoom", "fast"], 1, 1, 1, "stream"),
    cmd!("XLEN", 2, &["readonly", "fast"], 1, 1, 1, "stream"),
    cmd!("XRANGE", -4, &["readonly"], 1, 1, 1, "stream"),
    cmd!("XREVRANGE", -4, &["readonly"], 1, 1, 1, "stream"),
    cmd!("XREAD", -4, &["readonly"], 0, 0, 0, "stream"),
    cmd!("XTRIM", -4, &["write"], 1, 1, 1, "stream"),
    cmd!("XDEL", -3, &["write", "fast"], 1, 1, 1, "stream"),
    cmd!("XGROUP", -2, &["write"], 2, 2, 1, "stream"),
    cmd!("XREADGROUP", -7, &["write"], 0, 0, 0, "stream"),
    cmd!("XACK", -4, &["write", "fast"], 1, 1, 1, "stream"),
    cmd!("XPENDING", -3, &["readonly"], 1, 1, 1, "stream"),
    cmd!("XINFO", -2, &["readonly"], 2, 2, 1, "stream"),
    cmd!("XCLAIM", -6, &["write", "fast"], 1, 1, 1, "stream"),
    cmd!("XAUTOCLAIM", -6, &["write", "fast"], 1, 1, 1, "stream"),
    cmd!("XSETID", -3, &["write", "denyoom"], 1, 1, 1, "stream"),
    // Bitmap
    cmd!("SETBIT", 4, &["write", "denyoom"], 1, 1, 1, "bitmap"),
    cmd!("GETBIT", 3, &["readonly", "fast"], 1, 1, 1, "bitmap"),
    cmd!("BITCOUNT", -2, &["readonly"], 1, 1, 1, "bitmap"),
    cmd!("BITPOS", -3, &["readonly"], 1, 1, 1, "bitmap"),
    cmd!("BITOP", -4, &["write", "denyoom"], 2, -1, 1, "bitmap"),
    cmd!("BITFIELD", -2, &["write", "denyoom"], 1, 1, 1, "bitmap"),
    cmd!("BITFIELD_RO", -2, &["readonly", "fast"], 1, 1, 1, "bitmap"),
    // Geo
    cmd!("GEOADD", -5, &["write", "denyoom"], 1, 1, 1, "geo"),
    cmd!("GEOPOS", -2, &["readonly"], 1, 1, 1, "geo"),
    cmd!("GEODIST", -4, &["readonly"], 1, 1, 1, "geo"),
    cmd!("GEOHASH", -2, &["readonly"], 1, 1, 1, "geo"),
    cmd!("GEORADIUS", -6, &["write", "denyoom"], 1, 1, 1, "geo"),
    cmd!(
        "GEORADIUSBYMEMBER",
        -5,
        &["write", "denyoom"],
        1,
        1,
        1,
        "geo"
    ),
    cmd!("GEOSEARCH", -7, &["readonly"], 1, 1, 1, "geo"),
    cmd!("GEOSEARCHSTORE", -8, &["write", "denyoom"], 1, 2, 1, "geo"),
    // HyperLogLog
    cmd!(
        "PFADD",
        -2,
        &["write", "denyoom", "fast"],
        1,
        1,
        1,
        "hyperloglog"
    ),
    cmd!("PFCOUNT", -2, &["readonly"], 1, -1, 1, "hyperloglog"),
    cmd!(
        "PFMERGE",
        -2,
        &["write", "denyoom"],
        1,
        -1,
        1,
        "hyperloglog"
    ),
];

/// Best-effort key extraction for `COMMAND GETKEYS`.
fn command_getkeys<'a>(cmd: &str, args: &'a [Resp]) -> Vec<&'a [u8]> {
    let get = |idx: usize| -> Option<&'a [u8]> {
        args.get(idx).and_then(|r| r.as_bytes().map(|b| b.as_ref()))
    };
    match cmd {
        "GET" | "SET" | "DEL" | "EXISTS" | "TYPE" | "TTL" | "PTTL" | "EXPIRE" | "PEXPIRE"
        | "PERSIST" | "DUMP" | "RESTORE" | "STRLEN" | "APPEND" | "INCR" | "DECR" | "GETSET"
        | "GETDEL" | "GETEX" | "LPUSH" | "RPUSH" | "LPOP" | "RPOP" | "LLEN" | "LRANGE" | "HSET"
        | "HGET" | "HGETALL" | "HDEL" | "HLEN" | "SADD" | "SREM" | "SMEMBERS" | "SCARD"
        | "ZADD" | "ZREM" | "ZRANGE" | "ZCARD" | "ZSCORE" | "ZPOPMIN" | "ZPOPMAX" | "BZPOPMIN"
        | "BZPOPMAX" | "GEOADD" | "GEOPOS" | "GEODIST" | "GEOHASH" | "GEOSEARCH" | "XADD"
        | "XLEN" | "XRANGE" | "BITCOUNT" | "BITFIELD" | "BITFIELD_RO" | "SETBIT" | "GETBIT"
        | "PFADD" | "PFCOUNT" => get(1).into_iter().collect(),
        "LMOVE" | "BLMOVE" | "RENAME" | "COPY" | "LCS" | "GEOSEARCHSTORE" => {
            get(1).into_iter().chain(get(2)).collect()
        }
        "MGET" => (1..args.len()).filter_map(get).collect(),
        "MSET" | "MSETNX" => (1..args.len()).step_by(2).filter_map(get).collect(),
        "BITOP" => (2..args.len()).filter_map(get).collect(),
        _ => Vec::new(),
    }
}

/// `COMMAND [COUNT | INFO [cmd ...] | GETKEYS ... | DOCS [cmd ...] | HELP | LIST]`
pub async fn cmd_command(args: &[Resp]) -> Result<Resp> {
    if args.len() == 1 {
        return Ok(Resp::array(
            COMMAND_TABLE.iter().map(|c| c.to_resp()).collect(),
        ));
    }
    let sub = get_str(args, 1, "COMMAND")?.to_uppercase();
    match sub.as_str() {
        "COUNT" => Ok(Resp::int(COMMAND_TABLE.len() as i64)),
        "LIST" => Ok(Resp::array(
            COMMAND_TABLE
                .iter()
                .map(|c| Resp::bulk_str(c.name.to_string()))
                .collect(),
        )),
        "INFO" => {
            if args.len() == 2 {
                return Ok(Resp::array(
                    COMMAND_TABLE.iter().map(|c| c.to_resp()).collect(),
                ));
            }
            let mut out = Vec::with_capacity(args.len() - 2);
            for i in 2..args.len() {
                let name = get_str(args, i, "COMMAND")?.to_uppercase();
                match COMMAND_TABLE.iter().find(|c| c.name == name) {
                    Some(c) => out.push(c.to_resp()),
                    None => out.push(Resp::null_array()),
                }
            }
            Ok(Resp::array(out))
        }
        "GETKEYS" => {
            if args.len() < 3 {
                return Err(NexradeError::WrongArity("command|getkeys".to_string()));
            }
            let cmdline = &args[2..];
            let cmd_name = get_str(cmdline, 0, "COMMAND")?.to_uppercase();
            let keys = command_getkeys(&cmd_name, cmdline);
            Ok(Resp::array(
                keys.into_iter()
                    .map(|k| Resp::bulk(bytes::Bytes::copy_from_slice(k)))
                    .collect(),
            ))
        }
        "DOCS" => {
            let names: Vec<String> = if args.len() == 2 {
                COMMAND_TABLE.iter().map(|c| c.name.to_string()).collect()
            } else {
                (2..args.len())
                    .map(|i| get_str(args, i, "COMMAND").map(|s| s.to_uppercase()))
                    .collect::<Result<Vec<_>>>()?
            };
            let mut out = Vec::new();
            for name in names {
                if let Some(c) = COMMAND_TABLE.iter().find(|e| e.name == name) {
                    out.push(Resp::bulk_str(c.name.to_string()));
                    out.push(Resp::array(vec![
                        Resp::bulk_str("summary"),
                        Resp::bulk_str(format!("{} command", c.name)),
                        Resp::bulk_str("group"),
                        Resp::bulk_str(c.group.to_string()),
                        Resp::bulk_str("arity"),
                        Resp::int(c.arity),
                    ]));
                }
            }
            Ok(Resp::array(out))
        }
        "HELP" => Ok(Resp::array(vec![
            Resp::bulk_str("COMMAND <subcommand> [<arg> [value] [opt] ...]"),
            Resp::bulk_str("COMMAND — return details about all commands"),
            Resp::bulk_str("COMMAND COUNT — return the total number of commands"),
            Resp::bulk_str("COMMAND LIST — return a list of command names"),
            Resp::bulk_str("COMMAND INFO [command-name ...] — details about specific commands"),
            Resp::bulk_str("COMMAND GETKEYS <full command> — extract keys from a full command"),
            Resp::bulk_str("COMMAND DOCS [command-name ...] — return doc-ish metadata"),
            Resp::bulk_str("COMMAND HELP — show this help"),
        ])),
        _ => Err(NexradeError::Generic(format!(
            "ERR unknown subcommand or wrong number of arguments for '{sub}'. Try COMMAND HELP."
        ))),
    }
}
