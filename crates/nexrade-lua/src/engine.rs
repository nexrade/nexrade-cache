//! Lua evaluation engine.

use std::time::Duration;

use mlua::{Lua, LuaOptions, StdLib, Value as LuaValue};

use nexrade_core::command::dispatch;
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
    pub async fn eval(
        &self,
        script: &str,
        keys: Vec<Vec<u8>>,
        argv: Vec<Vec<u8>>,
        db: Db,
        db_index: usize,
    ) -> Result<Resp> {
        // We use a blocking call since mlua's async mode requires Send bounds
        // that are complex to satisfy. For most scripts this is acceptable.
        let script = script.to_string();
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
            let db_call = db.clone();
            let call_fn = lua
                .create_function(move |lua, args: mlua::MultiValue| {
                    let resp_args = multivalue_to_resp_args(args)?;
                    let result = tokio::runtime::Handle::current()
                        .block_on(dispatch(&db_call, resp_args, db_index));
                    resp_to_lua(lua, result)
                })
                .map_err(mlua_err)?;

            let db_pcall = db.clone();
            let pcall_fn = lua
                .create_function(move |lua, args: mlua::MultiValue| {
                    let resp_args = multivalue_to_resp_args(args)?;
                    let result = tokio::runtime::Handle::current()
                        .block_on(dispatch(&db_pcall, resp_args, db_index));
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
    ) -> Result<Resp> {
        match self.cache.get(sha) {
            None => Err(NexradeError::Generic(
                "NOSCRIPT No matching script. Please use EVAL.".to_string(),
            )),
            Some(script) => self.eval(&script, keys, argv, db, db_index).await,
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
