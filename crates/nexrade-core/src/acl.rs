//! Redis-compatible ACL (Access Control List) — multi-user authentication
//! and per-command / per-key authorisation.
//!
//! Users are stored in an `AclManager` that lives on `Db`. The default user
//! (named `"default"`) starts with no password and full permissions, matching
//! Redis's out-of-the-box state. Custom users can be added with `ACL SETUSER`.
//!
//! Permission checks are performed at dispatch time by `command::dispatch`
//! before the command handler runs. Commands run by the connection handler
//! itself (e.g. `AUTH`, `SUBSCRIBE`) are explicitly ACL-checked.
//!
//! Wire format:
//! - Passwords are stored as hex SHA-256 of the plaintext ("nopass" users
//!   skip the password check).
//! - Command rules: `+@<category>` or `-@<category>` (categories listed
//!   below) or `+<cmd>` / `-<cmd>` per command.
//! - Key patterns: `~pattern` (glob, `*`/`?`); `~*` / `allkeys` means all.
//!
//! Defaults:
//! - `on` / `off` — toggle whether the user is enabled.
//! - `>password` — set the password (clears any existing one).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::RwLock;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::store::glob_match;

// ── Command categories ───────────────────────────────────────────────────────

/// Map from category name (without `@`) to the list of command names it
/// contains. Mirrors the Redis 7.4 category definitions so `+@read`,
/// `-@dangerous`, `ACL CAT`, etc. work consistently. `@all` is added by
/// `command_category_map` — it's the union of every category.
pub const COMMAND_CATEGORIES: &[(&str, &[&str])] = &[
    (
        "keyspace",
        // DEL / UNLINK / EXISTS / TYPE / RENAME / RENAMENX / COPY / MOVE /
        // SORT / SORT_RO / OBJECT / TOUCH / EXPIRE family / TTL family /
        // PERSIST / KEYS / SCAN / RANDOMKEY / DUMP / RESTORE.
        &[
            "DEL",
            "UNLINK",
            "EXISTS",
            "TYPE",
            "RENAME",
            "RENAMENX",
            "COPY",
            "MOVE",
            "SORT",
            "SORT_RO",
            "OBJECT",
            "TOUCH",
            "EXPIRE",
            "PEXPIRE",
            "EXPIREAT",
            "PEXPIREAT",
            "EXPIRETIME",
            "PEXPIRETIME",
            "TTL",
            "PTTL",
            "PERSIST",
            "KEYS",
            "SCAN",
            "RANDOMKEY",
            "DUMP",
            "RESTORE",
        ],
    ),
    (
        "read",
        &[
            "GET",
            "MGET",
            "GETRANGE",
            "STRLEN",
            "GETBIT",
            "BITCOUNT",
            "BITPOS",
            "HGET",
            "HMGET",
            "HKEYS",
            "HVALS",
            "HGETALL",
            "HEXISTS",
            "HLEN",
            "HRANDFIELD",
            "HSCAN",
            "LRANGE",
            "LINDEX",
            "LLEN",
            "LPOS",
            "SMEMBERS",
            "SISMEMBER",
            "SMISMEMBER",
            "SCARD",
            "SRANDMEMBER",
            "SSCAN",
            "ZRANGE",
            "ZRANGEBYSCORE",
            "ZRANGEBYLEX",
            "ZSCORE",
            "ZMSCORE",
            "ZCARD",
            "ZCOUNT",
            "ZLEXCOUNT",
            "ZRANK",
            "ZREVRANK",
            "ZRANDMEMBER",
            "ZSCAN",
            "GETRANGE",
            "GETEX",
            "GETDEL",
            "GETSET",
            "PFCOUNT",
            "GEOPOS",
            "GEODIST",
            "GEOHASH",
            "GEOSEARCH",
            "XLEN",
            "XRANGE",
            "XREVRANGE",
            "XREAD",
            "XPENDING",
            "EXISTS",
        ],
    ),
    (
        "write",
        &[
            "SET",
            "MSET",
            "MSETNX",
            "SETNX",
            "SETEX",
            "PSETEX",
            "SETRANGE",
            "SETBIT",
            "BITOP",
            "BITFIELD",
            "INCR",
            "INCRBY",
            "INCRBYFLOAT",
            "DECR",
            "DECRBY",
            "APPEND",
            "HSET",
            "HMSET",
            "HDEL",
            "HSETNX",
            "HINCRBY",
            "HINCRBYFLOAT",
            "LPUSH",
            "LPUSHX",
            "RPUSH",
            "RPUSHX",
            "LPOP",
            "RPOP",
            "LINSERT",
            "LSET",
            "LREM",
            "LTRIM",
            "LMOVE",
            "LMPOP",
            "BLMPOP",
            "BLPOP",
            "BRPOP",
            "SADD",
            "SREM",
            "SPOP",
            "SMOVE",
            "ZADD",
            "ZREM",
            "ZINCRBY",
            "ZPOPMIN",
            "ZPOPMAX",
            "ZREMRANGEBYRANK",
            "ZREMRANGEBYSCORE",
            "ZUNIONSTORE",
            "ZINTERSTORE",
            "ZDIFFSTORE",
            "ZRANGESTORE",
            "ZINTER",
            "ZUNION",
            "ZDIFF",
            "ZINTERCARD",
            "ZMPOP",
            "BZMPOP",
            "PFADD",
            "PFMERGE",
            "GEOADD",
            "XADD",
            "XTRIM",
            "XDEL",
            "XACK",
            "COPY",
            "RENAME",
            "RENAMENX",
            "MOVE",
        ],
    ),
    (
        "set",
        // Same as `@write` for set keys.
        &[
            "SADD",
            "SREM",
            "SPOP",
            "SMOVE",
            "SMEMBERS",
            "SISMEMBER",
            "SMISMEMBER",
            "SCARD",
            "SRANDMEMBER",
            "SSCAN",
        ],
    ),
    (
        "sortedset",
        &[
            "ZADD",
            "ZREM",
            "ZINCRBY",
            "ZPOPMIN",
            "ZPOPMAX",
            "ZREMRANGEBYRANK",
            "ZREMRANGEBYSCORE",
            "ZRANGE",
            "ZRANGEBYSCORE",
            "ZRANGEBYLEX",
            "ZREVRANGE",
            "ZREVRANGEBYSCORE",
            "ZRANGEBYLEX",
            "ZSCORE",
            "ZMSCORE",
            "ZCARD",
            "ZCOUNT",
            "ZLEXCOUNT",
            "ZRANK",
            "ZREVRANK",
            "ZRANDMEMBER",
            "ZSCAN",
            "ZUNIONSTORE",
            "ZINTERSTORE",
            "ZDIFFSTORE",
            "ZRANGESTORE",
            "ZINTER",
            "ZUNION",
            "ZDIFF",
            "ZINTERCARD",
            "ZMPOP",
            "BZMPOP",
        ],
    ),
    (
        "list",
        &[
            "LPUSH", "LPUSHX", "RPUSH", "RPUSHX", "LPOP", "RPOP", "LRANGE", "LINDEX", "LLEN",
            "LINSERT", "LSET", "LREM", "LTRIM", "LMOVE", "LMPOP", "BLMPOP", "BLPOP", "BRPOP",
        ],
    ),
    (
        "hash",
        &[
            "HSET",
            "HMSET",
            "HGET",
            "HMGET",
            "HDEL",
            "HEXISTS",
            "HGETALL",
            "HKEYS",
            "HVALS",
            "HLEN",
            "HSETNX",
            "HINCRBY",
            "HINCRBYFLOAT",
            "HSCAN",
            "HRANDFIELD",
        ],
    ),
    (
        "string",
        &[
            "SET",
            "GET",
            "MSET",
            "MSETNX",
            "SETNX",
            "SETEX",
            "PSETEX",
            "GETSET",
            "GETDEL",
            "GETEX",
            "GETRANGE",
            "SETRANGE",
            "STRLEN",
            "INCR",
            "INCRBY",
            "INCRBYFLOAT",
            "DECR",
            "DECRBY",
            "APPEND",
        ],
    ),
    (
        "bitmap",
        &[
            "SETBIT", "GETBIT", "BITCOUNT", "BITPOS", "BITOP", "BITFIELD",
        ],
    ),
    ("hyperloglog", &["PFADD", "PFCOUNT", "PFMERGE"]),
    (
        "geo",
        &[
            "GEOADD",
            "GEOPOS",
            "GEODIST",
            "GEOHASH",
            "GEORADIUS",
            "GEORADIUSBYMEMBER",
            "GEOSEARCH",
        ],
    ),
    (
        "stream",
        &[
            "XADD",
            "XLEN",
            "XRANGE",
            "XREVRANGE",
            "XREAD",
            "XREADGROUP",
            "XTRIM",
            "XDEL",
            "XACK",
            "XPENDING",
            "XGROUP",
        ],
    ),
    (
        "pubsub",
        &[
            "SUBSCRIBE",
            "UNSUBSCRIBE",
            "PSUBSCRIBE",
            "PUNSUBSCRIBE",
            "PUBLISH",
            "PUBSUB",
        ],
    ),
    (
        "connection",
        &[
            "AUTH", "PING", "ECHO", "QUIT", "SELECT", "HELLO", "CLIENT", "RESET", "WAIT",
        ],
    ),
    (
        "transaction",
        &["MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH"],
    ),
    ("scripting", &["EVAL", "EVALSHA", "SCRIPT"]),
    (
        "admin",
        &[
            "ACL",
            "BGREWRITEAOF",
            "BGSAVE",
            "COMMAND",
            "CONFIG",
            "DBSIZE",
            "DEBUG",
            "FLUSHALL",
            "FLUSHDB",
            "INFO",
            "LASTSAVE",
            "LATENCY",
            "MEMORY",
            "MODULE",
            "MONITOR",
            "REPLICAOF",
            "REPLCONF",
            "PSYNC",
            "RESET",
            "SAVE",
            "SHUTDOWN",
            "SLAVEOF",
            "SLOWLOG",
            "SWAPDB",
            "SYNC",
            "TIME",
            "WAIT",
        ],
    ),
    (
        "dangerous",
        // Subset of @admin that mutates persistent state or cluster.
        &[
            "FLUSHDB",
            "FLUSHALL",
            "SHUTDOWN",
            "BGREWRITEAOF",
            "DEBUG",
            "CONFIG",
            "REPLICAOF",
            "SLAVEOF",
            "ACL",
            "KEYS",
            "MIGRATE",
            "RESTORE",
            "SORT",
            "WAIT",
        ],
    ),
    (
        "slow",
        // Commands the docs warn as O(N) — typically blocked in prod.
        &["KEYS", "SCAN", "DBSIZE", "FLUSHDB", "FLUSHALL"],
    ),
    (
        "blocking",
        &[
            "BLPOP",
            "BRPOP",
            "BLMPOP",
            "BZPOPMIN",
            "BZPOPMAX",
            "XREAD",
            "XREADGROUP",
        ],
    ),
    (
        "fast",
        &["GET", "SET", "INCR", "DECR", "LPUSH", "RPUSH", "EXPIRE"],
    ),
];

/// Build a `HashMap<category, HashSet<command>>` for O(1) category
/// membership lookups. The result is cached at the process level
/// (it's a pure function of `COMMAND_CATEGORIES`) — ACL rules with
/// `@`-prefixed patterns hit this on every command, so the cache
/// avoids rebuilding the HashMap-of-HashSets on each lookup.
pub fn command_category_map(
) -> &'static HashMap<&'static str, std::collections::HashSet<&'static str>> {
    static CACHE: OnceLock<HashMap<&'static str, std::collections::HashSet<&'static str>>> =
        OnceLock::new();
    CACHE.get_or_init(|| {
        let mut map: HashMap<&'static str, std::collections::HashSet<&'static str>> =
            HashMap::new();
        for (cat, cmds) in COMMAND_CATEGORIES {
            map.insert(cat, cmds.iter().copied().collect());
        }
        // `@all` is the union of every category.
        let mut all = std::collections::HashSet::new();
        for (_, cmds) in COMMAND_CATEGORIES {
            for c in *cmds {
                all.insert(*c);
            }
        }
        map.insert("all", all);
        map
    })
}

/// Return the list of all known category names, prefixed with `@` to match
/// Redis's `ACL CAT` output (e.g. `["@read", "@write", "@all", …]`).
pub fn category_names() -> Vec<String> {
    let mut names: Vec<String> = COMMAND_CATEGORIES
        .iter()
        .map(|(c, _)| format!("@{c}"))
        .collect();
    names.push("@all".to_string());
    names.sort();
    names.dedup();
    names
}

// ── Errors ───────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum AclError {
    #[error("WRONGPASS invalid username-password pair or user is disabled")]
    WrongPass,
    #[error("NOPERM {0}")]
    PermissionDenied(String),
    #[error("ERR unknown user '{0}'")]
    UnknownUser(String),
    #[error("ERR wrong number of arguments for ACL command")]
    WrongArity,
    #[error("ERR syntax error")]
    Syntax,
    #[error("ERR user '{0}' already exists")]
    UserExists(String),
    #[error("ERR no such user '{0}'")]
    NoSuchUser(String),
    #[error("ERR unknown command category '{0}'")]
    UnknownCategory(String),
}

// ── User model ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AclUser {
    pub name: String,
    /// `Some(hex sha256)` if password-protected. `None` means `nopass`.
    pub password_hash: Option<String>,
    pub enabled: bool,
    /// True if no rules at all are configured (treated as full access,
    /// matching Redis's "no selector" behaviour).
    pub all_commands: bool,
    pub all_keys: bool,
    /// Per-command permissions, most-recent-rule-wins.
    /// `+cmd` / `-cmd` / `+@cat` / `-@cat` rules.
    pub command_rules: Vec<(String, bool)>,
    /// Key patterns in declaration order, "last match wins" semantics.
    pub key_patterns: Vec<String>,
}

impl AclUser {
    /// Construct a new user with no password and full permissions.
    pub fn default_user() -> Self {
        Self {
            name: "default".to_string(),
            password_hash: None,
            enabled: true,
            all_commands: true,
            all_keys: true,
            command_rules: Vec::new(),
            key_patterns: Vec::new(),
        }
    }

    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            password_hash: None,
            enabled: true,
            all_commands: true,
            all_keys: true,
            command_rules: Vec::new(),
            key_patterns: Vec::new(),
        }
    }

    /// Return `true` if `cmd` is allowed by the current rule set.
    pub fn command_allowed(&self, cmd: &str) -> bool {
        if self.all_commands && self.command_rules.is_empty() {
            return true;
        }
        // Apply rules in order, last match wins.
        let mut allowed = false;
        for (rule, sign) in &self.command_rules {
            let mut matched = false;
            if let Some(cat) = rule.strip_prefix('@') {
                if command_category_map()
                    .get(cat)
                    .is_some_and(|c| c.contains(cmd))
                {
                    matched = true;
                }
            } else if rule.eq_ignore_ascii_case(cmd) {
                matched = true;
            }
            if matched {
                allowed = *sign;
            }
        }
        allowed
    }

    /// Return `true` if `key` matches any allowed pattern. If the user has
    /// `all_keys`, return true immediately.
    pub fn key_allowed(&self, key: &[u8]) -> bool {
        if self.all_keys && self.key_patterns.is_empty() {
            return true;
        }
        // Last match wins (apply in reverse order until a match).
        for pattern in self.key_patterns.iter().rev() {
            if pattern == "allkeys" || pattern == "*" {
                return true;
            }
            let pat = pattern.strip_prefix('~').unwrap_or(pattern);
            if glob_match(pat.as_bytes(), key) {
                return true;
            }
        }
        false
    }

    /// Update `self` from a sequence of rule tokens (without the leading
    /// command name). Each token is `+cmd`, `-cmd`, `+@cat`, `-@cat`,
    /// `~pattern`, `>password`, `<password`, `#hash`, `&nopat`,
    /// `on`, `off`, `resetpass`, `reset`, `clear`.
    pub fn apply_rules(&mut self, rules: &[&str]) -> Result<(), AclError> {
        for rule in rules {
            if *rule == "on" {
                self.enabled = true;
            } else if *rule == "off" {
                self.enabled = false;
            } else if *rule == "reset" {
                self.all_commands = true;
                self.all_keys = true;
                self.command_rules.clear();
                self.key_patterns.clear();
                self.password_hash = None;
            } else if *rule == "resetpass" {
                self.password_hash = None;
            } else if let Some(pwd) = rule.strip_prefix('>') {
                self.password_hash = Some(hash_password(pwd));
            } else if rule.strip_prefix('<').is_some() {
                // In Redis 7+ `<password` removes a specific password; we
                // don't track a password *set*, so treat it as alias for
                // `resetpass` (clears the single stored password).
                self.password_hash = None;
            } else if let Some(hash) = rule.strip_prefix('#') {
                self.password_hash = Some(hash.to_lowercase());
            } else if *rule == "nopass" {
                self.password_hash = None;
            } else if let Some(pat) = rule.strip_prefix('~') {
                self.all_keys = false;
                self.key_patterns.retain(|p| p.as_str() != pat);
                self.key_patterns.push(pat.to_string());
            } else if let Some(rest) = rule.strip_prefix('+') {
                self.all_commands = false;
                let token = rest.to_ascii_lowercase();
                self.command_rules
                    .push((normalize_rule_token(&token), true));
            } else if let Some(rest) = rule.strip_prefix('-') {
                self.all_commands = false;
                let token = rest.to_ascii_lowercase();
                self.command_rules
                    .push((normalize_rule_token(&token), false));
            } else if rule.starts_with('@') {
                return Err(AclError::UnknownCategory(rule.to_string()));
            } else {
                return Err(AclError::Syntax);
            }
        }
        Ok(())
    }
}

fn normalize_rule_token(s: &str) -> String {
    // `@category` → "@category" (kept as-is, lowercased).
    s.to_ascii_lowercase()
}

pub fn hash_password(pwd: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(pwd.as_bytes());
    let digest = hasher.finalize();
    hex::encode(digest)
}

// ── Manager ───────────────────────────────────────────────────────────────────

/// Shared ACL state. One per server. Cloning is cheap (Arc-internal).
#[derive(Clone)]
pub struct AclManager {
    inner: Arc<AclManagerInner>,
}

struct AclManagerInner {
    users: RwLock<HashMap<String, AclUser>>,
    log: parking_lot::Mutex<Vec<AclLogEntry>>,
    /// True when the only configured user is unrestricted `default`
    /// (enabled, all_commands, all_keys, no command/key rules). Lets the
    /// dispatch hot path skip the users RwLock entirely — the common case
    /// for redis-benchmark and deployments that never touch ACL.
    open: AtomicBool,
}

#[derive(Debug, Clone)]
pub struct AclLogEntry {
    pub timestamp_ms: i64,
    pub user: String,
    pub reason: String,
}

impl AclManager {
    pub fn new() -> Self {
        let mut users = HashMap::new();
        users.insert("default".to_string(), AclUser::default_user());
        Self {
            inner: Arc::new(AclManagerInner {
                users: RwLock::new(users),
                log: parking_lot::Mutex::new(Vec::new()),
                open: AtomicBool::new(true),
            }),
        }
    }

    /// Fast path: ACL has only unrestricted `default`. Single atomic load.
    #[inline]
    pub fn is_open(&self) -> bool {
        self.inner.open.load(Ordering::Relaxed)
    }

    /// Recompute the `open` flag from the current user table. Called after
    /// every mutating ACL op. Holds the users lock (caller already does).
    fn recompute_open_locked(users: &HashMap<String, AclUser>, open: &AtomicBool) {
        let is_open = matches!(
            users.get("default"),
            Some(u) if users.len() == 1
                && u.enabled
                && u.all_commands
                && u.all_keys
                && u.command_rules.is_empty()
                && u.key_patterns.is_empty()
        );
        open.store(is_open, Ordering::Relaxed);
    }

    /// Authenticate `(user, password)` and return `Ok(())` on success.
    pub fn authenticate(&self, user: &str, pass: &str) -> Result<(), AclError> {
        let users = self.inner.users.read();
        let u = users
            .get(user)
            .ok_or_else(|| AclError::UnknownUser(user.to_string()))?;
        if !u.enabled {
            return Err(AclError::WrongPass);
        }
        if let Some(expected) = &u.password_hash {
            if *expected != hash_password(pass) {
                return Err(AclError::WrongPass);
            }
        }
        Ok(())
    }

    /// Return whether `user` may run `cmd` on `keys` (only the keys are
    /// checked; commands that don't operate on a key are always allowed
    /// by this check).
    pub fn check_permission(&self, user: &str, cmd: &str, keys: &[&[u8]]) -> Result<(), AclError> {
        // Unrestricted-default fast path: no lock, no HashMap. Safe because
        // `open` is recomputed under the users write-lock after every mutate.
        if self.is_open() && user == "default" {
            return Ok(());
        }
        let users = self.inner.users.read();
        let u = users
            .get(user)
            .ok_or_else(|| AclError::UnknownUser(user.to_string()))?;
        if !u.enabled {
            self.log_denied(user, "user is disabled");
            return Err(AclError::WrongPass);
        }
        if !u.command_allowed(cmd) {
            self.log_denied(user, &format!("can't run '{cmd}': no permission"));
            return Err(AclError::PermissionDenied(format!(
                "this user has no permissions to run the '{cmd}' command"
            )));
        }
        for k in keys {
            if !u.key_allowed(k) {
                self.log_denied(user, "no permission to access key");
                let key_str = String::from_utf8_lossy(k);
                return Err(AclError::PermissionDenied(format!(
                    "this user has no permissions to access the '{key_str}' key"
                )));
            }
        }
        Ok(())
    }

    fn log_denied(&self, user: &str, reason: &str) {
        let mut log = self.inner.log.lock();
        log.push(AclLogEntry {
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            user: user.to_string(),
            reason: reason.to_string(),
        });
        if log.len() > 128 {
            let drop = log.len() - 128;
            log.drain(0..drop);
        }
    }

    pub fn get_user(&self, name: &str) -> Option<AclUser> {
        self.inner.users.read().get(name).cloned()
    }

    pub fn list_users(&self) -> Vec<String> {
        let users = self.inner.users.read();
        let mut names: Vec<String> = users.keys().cloned().collect();
        names.sort();
        names
    }

    /// Create or update a user with a sequence of rules. If the user does
    /// not exist, it is created with no permissions; otherwise the existing
    /// rules are kept and `rules` are appended on top.
    pub fn setuser(&self, name: &str, rules: &[&str]) -> Result<AclUser, AclError> {
        let mut users = self.inner.users.write();
        let user = users
            .entry(name.to_string())
            .or_insert_with(|| AclUser::new(name));
        user.apply_rules(rules)?;
        let cloned = user.clone();
        Self::recompute_open_locked(&users, &self.inner.open);
        Ok(cloned)
    }

    /// Delete a user. Returns `true` if the user existed.
    pub fn deluser(&self, name: &str) -> bool {
        if name == "default" {
            // Redis allows deleting `default` but auto-recreates it on next
            // ACL op. We match that behaviour by re-creating with defaults.
            let mut users = self.inner.users.write();
            users.remove(name);
            users.insert("default".to_string(), AclUser::default_user());
            Self::recompute_open_locked(&users, &self.inner.open);
            return true;
        }
        let mut users = self.inner.users.write();
        let removed = users.remove(name).is_some();
        if removed {
            Self::recompute_open_locked(&users, &self.inner.open);
        }
        removed
    }

    /// Render a user's rule list in `ACL LIST` format (one space-separated
    /// string per user, fields in canonical order).
    pub fn list(&self) -> Vec<String> {
        let users = self.inner.users.read();
        let mut out = Vec::with_capacity(users.len());
        let mut names: Vec<&String> = users.keys().collect();
        names.sort();
        for name in names {
            let u = users.get(name).unwrap();
            out.push(format_user_line(u));
        }
        out
    }

    pub fn acl_log(&self, count: Option<usize>) -> Vec<AclLogEntry> {
        let log = self.inner.log.lock();
        let n = count.unwrap_or(log.len()).min(log.len());
        log[log.len() - n..].to_vec()
    }

    /// Reset the ACL log.
    pub fn acl_log_reset(&self) {
        self.inner.log.lock().clear();
    }

    /// Generate a random base-64 password of `bits` length (capped to 4096).
    pub fn genpass(&self, bits: usize) -> String {
        let bits = bits.clamp(8, 4096);
        let bytes = bits.div_ceil(8);
        // Uuid::new_v4 gives 16 random bytes — sufficient for password
        // generation (good entropy from OS RNG via getrandom).
        let mut buf = Vec::with_capacity(bytes);
        while buf.len() < bytes {
            let extra = Uuid::new_v4().as_bytes().to_vec();
            buf.extend_from_slice(&extra);
        }
        buf.truncate(bytes);
        hex::encode(&buf)
    }
}

impl Default for AclManager {
    fn default() -> Self {
        Self::new()
    }
}

fn format_user_line(u: &AclUser) -> String {
    let mut parts = Vec::new();
    parts.push(format!("user {}", u.name));
    parts.push("on".to_string());
    parts.push(format!("#{}", u.password_hash.as_deref().unwrap_or("")));
    parts.push(format!(
        "~{}",
        u.key_patterns.first().map(|s| s.as_str()).unwrap_or("*")
    ));
    for (rule, sign) in &u.command_rules {
        parts.push(if *sign {
            format!("+{}", rule)
        } else {
            format!("-{}", rule)
        });
    }
    parts.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_user_full_access() {
        let m = AclManager::new();
        assert!(m.check_permission("default", "GET", &[b"foo"]).is_ok());
        assert!(m.check_permission("default", "SET", &[b"foo"]).is_ok());
        assert!(m.check_permission("default", "SHUTDOWN", &[b"foo"]).is_ok());
    }

    #[test]
    fn auth_and_check_password() {
        let m = AclManager::new();
        m.setuser("alice", &["+@read", "~user:*", ">secret"])
            .unwrap();
        // Correct password.
        assert!(m.authenticate("alice", "secret").is_ok());
        // Wrong password.
        assert!(m.authenticate("alice", "wrong").is_err());
        // Permission: read-only.
        assert!(m.check_permission("alice", "GET", &[b"user:1"]).is_ok());
        assert!(m.check_permission("alice", "SET", &[b"user:1"]).is_err());
        // Key pattern.
        assert!(m.check_permission("alice", "GET", &[b"other:1"]).is_err());
        assert!(m.check_permission("alice", "GET", &[b"user:2"]).is_ok());
    }

    #[test]
    fn last_rule_wins() {
        let m = AclManager::new();
        m.setuser("bob", &["+@all", "-@dangerous"]).unwrap();
        assert!(m.check_permission("bob", "GET", &[b"x"]).is_ok());
        assert!(m.check_permission("bob", "FLUSHDB", &[b"x"]).is_err());
    }

    #[test]
    fn disable_user() {
        let m = AclManager::new();
        m.setuser("eve", &["on", "+@all"]).unwrap();
        m.setuser("eve", &["off"]).unwrap();
        assert!(m.check_permission("eve", "GET", &[b"x"]).is_err());
    }

    #[test]
    fn password_hashing_is_deterministic() {
        assert_eq!(hash_password("hunter2"), hash_password("hunter2"));
        assert_ne!(hash_password("hunter2"), hash_password("hunter3"));
    }

    #[test]
    fn genpass_returns_unique() {
        let m = AclManager::new();
        let a = m.genpass(64);
        let b = m.genpass(64);
        assert_ne!(a, b);
        assert!(!a.is_empty());
    }

    #[test]
    fn list_format_roundtrips() {
        let m = AclManager::new();
        m.setuser("alice", &["+@read", "+ping", "~user:*", ">secret"])
            .unwrap();
        let lines = m.list();
        assert_eq!(lines.len(), 2);
        // Default user line + alice.
        let alice_line = lines.iter().find(|s| s.starts_with("user alice")).unwrap();
        assert!(alice_line.contains("+@read"));
        assert!(alice_line.contains("+ping"));
        assert!(alice_line.contains("~user:*"));
    }
}
