//! Lua evaluation engine.

use std::time::Duration;

use mlua::{Lua, LuaOptions, StdLib, Value as LuaValue};

use nexrade_core::command::dispatch_with_user;
use nexrade_core::db::Db;
use nexrade_core::error::{NexradeError, Result};
use nexrade_core::resp::Resp;

use crate::script_cache::ScriptCache;
use crate::value::{lua_to_resp, resp_to_lua};

fn mlua_err(e: mlua::Error) -> NexradeError {
    NexradeError::Generic(e.to_string())
}

/// The Lua scripting engine.
#[derive(Clone)]
pub struct LuaEngine {
    pub cache: ScriptCache,
    pub time_limit: Duration,
}

impl LuaEngine {
    pub fn new(time_limit: Duration) -> Result<Self> {
        // Load only safe stdlib modules
        let safe_libs = StdLib::TABLE | StdLib::STRING | StdLib::MATH;

        let lua = Lua::new_with(safe_libs, LuaOptions::default()).map_err(mlua_err)?;

        setup_redis_table(&lua).map_err(mlua_err)?;

        Ok(Self {
            cache: ScriptCache::new(),
            time_limit,
        })
    }

    /// Execute a Lua script (EVAL).
    ///
    /// `user` is the ACL identity of the connection that issued EVAL —
    /// `redis.call`/`redis.pcall` inside the script dispatch as that same
    /// user, so a script can't use ACL-restricted commands the caller
    /// itself couldn't run directly.
    pub async fn eval(
        &self,
        script: &str,
        keys: Vec<Vec<u8>>,
        argv: Vec<Vec<u8>>,
        db: Db,
        db_index: usize,
        user: &str,
    ) -> Result<Resp> {
        // We use a blocking call since mlua's async mode requires Send bounds
        // that are complex to satisfy. For most scripts this is acceptable.
        let script = script.to_string();
        let user = user.to_string();
        let _time_limit = self.time_limit;

        tokio::task::spawn_blocking(move || {
            let lua = Lua::new_with(
                StdLib::TABLE | StdLib::STRING | StdLib::MATH,
                LuaOptions::default(),
            )
            .map_err(mlua_err)?;

            setup_redis_table(&lua).map_err(mlua_err)?;

            // Inject KEYS and ARGV
            {
                let keys_table = lua.create_table().map_err(mlua_err)?;
                for (i, k) in keys.iter().enumerate() {
                    keys_table
                        .set(i + 1, lua.create_string(k).map_err(mlua_err)?)
                        .map_err(mlua_err)?;
                }
                lua.globals().set("KEYS", keys_table).map_err(mlua_err)?;

                let argv_table = lua.create_table().map_err(mlua_err)?;
                for (i, a) in argv.iter().enumerate() {
                    argv_table
                        .set(i + 1, lua.create_string(a).map_err(mlua_err)?)
                        .map_err(mlua_err)?;
                }
                lua.globals().set("ARGV", argv_table).map_err(mlua_err)?;
            }

            // Set up redis.call to use a synchronous dispatch shim
            // (In blocking context we can use tokio::runtime::Handle::current())
            // Dispatches as `user` — the ACL identity of the connection that
            // issued EVAL — not as "default", so scripts can't use
            // ACL-restricted commands the caller itself couldn't run.
            let db_call = db.clone();
            let user_call = user.clone();
            let call_fn = lua
                .create_function(move |lua, args: mlua::MultiValue| {
                    let resp_args = multivalue_to_resp_args(args)?;
                    let result = tokio::runtime::Handle::current().block_on(dispatch_with_user(
                        &db_call, resp_args, db_index, None, &user_call,
                    ));
                    resp_to_lua(lua, result)
                })
                .map_err(mlua_err)?;

            let db_pcall = db.clone();
            let user_pcall = user.clone();
            let pcall_fn = lua
                .create_function(move |lua, args: mlua::MultiValue| {
                    let resp_args = multivalue_to_resp_args(args)?;
                    let result = tokio::runtime::Handle::current().block_on(dispatch_with_user(
                        &db_pcall,
                        resp_args,
                        db_index,
                        None,
                        &user_pcall,
                    ));
                    resp_to_lua(lua, result)
                })
                .map_err(mlua_err)?;

            let redis_table: mlua::Table = lua.globals().get("redis").map_err(mlua_err)?;
            redis_table.set("call", call_fn).map_err(mlua_err)?;
            redis_table.set("pcall", pcall_fn).map_err(mlua_err)?;

            let result: LuaValue = lua.load(&script).eval().map_err(mlua_err)?;
            Ok::<Resp, NexradeError>(lua_to_resp(result))
        })
        .await
        .map_err(|e| NexradeError::Generic(e.to_string()))?
    }

    /// Load a script into the cache (SCRIPT LOAD).
    pub fn script_load(&self, script: String) -> String {
        self.cache.store(script)
    }

    /// Execute a cached script (EVALSHA).
    pub async fn evalsha(
        &self,
        sha: &str,
        keys: Vec<Vec<u8>>,
        argv: Vec<Vec<u8>>,
        db: Db,
        db_index: usize,
        user: &str,
    ) -> Result<Resp> {
        match self.cache.get(sha) {
            None => Err(NexradeError::Prefixed(
                "NOSCRIPT No matching script. Please use EVAL.".to_string(),
            )),
            Some(script) => self.eval(&script, keys, argv, db, db_index, user).await,
        }
    }

    /// Check if scripts exist (SCRIPT EXISTS).
    pub fn script_exists(&self, shas: &[&str]) -> Vec<bool> {
        shas.iter().map(|sha| self.cache.exists(sha)).collect()
    }

    /// Flush the script cache (SCRIPT FLUSH).
    pub fn script_flush(&self) {
        self.cache.flush();
    }
}

fn setup_redis_table(lua: &Lua) -> mlua::Result<()> {
    let redis = lua.create_table()?;

    // Placeholder call/pcall — replaced per-eval with real db handle
    let noop = lua.create_function(|_, _: LuaValue| Ok(LuaValue::Nil))?;
    redis.set("call", noop.clone())?;
    redis.set("pcall", noop)?;

    // redis.status_reply(str)
    let status = lua.create_function(|lua, s: String| {
        let t = lua.create_table()?;
        t.set("ok", s)?;
        Ok(t)
    })?;
    redis.set("status_reply", status)?;

    // redis.error_reply(str)
    let error = lua.create_function(|lua, s: String| {
        let t = lua.create_table()?;
        t.set("err", s)?;
        Ok(t)
    })?;
    redis.set("error_reply", error)?;

    // redis.log(level, msg)
    let log_fn = lua.create_function(|_, (level, msg): (i64, String)| {
        match level {
            1 => tracing::debug!("[lua] {}", msg),
            2 => tracing::info!("[lua] {}", msg),
            3 => tracing::warn!("[lua] {}", msg),
            _ => tracing::error!("[lua] {}", msg),
        }
        Ok(())
    })?;
    redis.set("log", log_fn)?;
    redis.set("LOG_DEBUG", 1i64)?;
    redis.set("LOG_VERBOSE", 2i64)?;
    redis.set("LOG_NOTICE", 3i64)?;
    redis.set("LOG_WARNING", 4i64)?;

    lua.globals().set("redis", redis)?;

    // cjson stub
    let cjson = lua.create_table()?;
    let encode = lua.create_function(|_, val: LuaValue| Ok(simple_lua_to_json(&val)))?;
    let decode = lua.create_function(|_, _s: String| Ok(LuaValue::Nil))?;
    cjson.set("encode", encode)?;
    cjson.set("decode", decode)?;
    lua.globals().set("cjson", cjson)?;

    Ok(())
}

fn multivalue_to_resp_args(args: mlua::MultiValue) -> mlua::Result<Vec<Resp>> {
    let mut result = Vec::new();
    for val in args {
        let resp = match val {
            LuaValue::String(s) => Resp::bulk(bytes::Bytes::copy_from_slice(s.as_bytes())),
            LuaValue::Integer(n) => Resp::bulk_str(n.to_string()),
            LuaValue::Number(f) => Resp::bulk_str(f.to_string()),
            LuaValue::Boolean(b) => Resp::bulk_str(if b { "1" } else { "0" }),
            LuaValue::Nil => {
                return Err(mlua::Error::RuntimeError(
                    "nil argument not allowed".to_string(),
                ))
            }
            _ => {
                return Err(mlua::Error::RuntimeError(
                    "unsupported argument type".to_string(),
                ))
            }
        };
        result.push(resp);
    }
    Ok(result)
}

fn simple_lua_to_json(val: &LuaValue) -> String {
    match val {
        LuaValue::Nil => "null".to_string(),
        LuaValue::Boolean(b) => b.to_string(),
        LuaValue::Integer(n) => n.to_string(),
        LuaValue::Number(f) => f.to_string(),
        LuaValue::String(s) => format!("\"{}\"", s.to_str().unwrap_or("").replace('"', "\\\"")),
        LuaValue::Table(t) => {
            let mut items = Vec::new();
            let mut i = 1usize;
            loop {
                match t.get::<_, LuaValue>(i) {
                    Ok(LuaValue::Nil) => break,
                    Ok(v) => items.push(simple_lua_to_json(&v)),
                    Err(_) => break,
                }
                i += 1;
            }
            format!("[{}]", items.join(","))
        }
        _ => "null".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexrade_core::db::Db;

    fn engine() -> LuaEngine {
        LuaEngine::new(Duration::from_secs(5)).unwrap()
    }

    #[tokio::test]
    async fn eval_dispatches_redis_call_as_default_when_unrestricted() {
        let e = engine();
        let db = Db::default();
        let r = e
            .eval(
                "return redis.call('SET', KEYS[1], ARGV[1])",
                vec![b"k".to_vec()],
                vec![b"v".to_vec()],
                db.clone(),
                0,
                "default",
            )
            .await
            .unwrap();
        assert!(matches!(r, Resp::SimpleString(s) if s == "OK"));
    }

    /// Regression test: EVAL must run `redis.call` under the caller's own
    /// ACL identity, not a hardcoded "default" full-access user — otherwise
    /// a restricted user could bypass a command/key restriction just by
    /// wrapping the forbidden command in a script.
    #[tokio::test]
    async fn eval_enforces_callers_acl_restrictions() {
        let e = engine();
        let db = Db::default();
        // "restricted" may only PING — no SET.
        db.acl.setuser("restricted", &["+ping", "~*"]).unwrap();

        let r = e
            .eval(
                "return redis.call('SET', KEYS[1], ARGV[1])",
                vec![b"k".to_vec()],
                vec![b"v".to_vec()],
                db.clone(),
                0,
                "restricted",
            )
            .await
            .unwrap();
        // redis.call surfaces the dispatch error as a Lua table {err=...},
        // which the script returns verbatim — so the outer eval() call
        // succeeds (Ok) but its Resp payload is the permission error.
        match r {
            Resp::Error(msg) => {
                assert!(
                    msg.to_lowercase().contains("permission"),
                    "expected a permission error, got: {msg}"
                );
            }
            other => panic!("expected redis.call to surface the ACL denial, got: {other:?}"),
        }
        // Confirm the SET never actually took effect.
        assert!(
            db.store.db(0).read_for(b"k").get_ro(b"k").is_none(),
            "SET should not have applied — restricted user has no SET permission"
        );
    }

    #[tokio::test]
    async fn eval_allows_commands_the_caller_is_permitted_to_run() {
        let e = engine();
        let db = Db::default();
        // "reader" may GET but not SET; confirm GET still works via EVAL.
        db.acl.setuser("reader", &["+get", "~*"]).unwrap();
        db.store.db(0).write_for(b"k").insert(
            b"k".to_vec(),
            nexrade_core::store::Entry::new(nexrade_core::types::DataType::String(b"v".to_vec())),
        );

        let r = e
            .eval(
                "return redis.call('GET', KEYS[1])",
                vec![b"k".to_vec()],
                vec![],
                db.clone(),
                0,
                "reader",
            )
            .await
            .unwrap();
        assert!(matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == b"v"));
    }
}
