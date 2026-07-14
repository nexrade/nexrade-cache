//! FUNCTION library — persistent named Lua functions.
//!
//! Mirrors Redis 7.0+ semantics. Each library:
//! - Has a unique name (`#!lua name=...` in the source).
//! - Contains one or more functions registered via `redis.register_function()`.
//! - Can be loaded, deleted, listed, dumped, restored, and flushed.
//!
//! Function source format (Redis 7 spec):
//! ```lua
//! #!lua name=mylib
//!
//! local function greet(name)
//!   return "hello, " .. name
//! end
//!
//! redis.register_function('greet', greet)
//! ```
//!
//! On `FUNCTION LOAD`, we compile the source once to (a) extract the
//! library name, and (b) validate it defines at least one function. On
//! each `FCALL`, we re-execute the source in a fresh Lua state (matching
//! Redis's per-call isolation) and then invoke the requested function,
//! which was captured into a registry table by `redis.register_function`.

use std::collections::HashMap;
use std::sync::Arc;

use mlua::{Function, Lua, StdLib, Value as LuaValue};
use parking_lot::RwLock;

use nexrade_core::command::dispatch_with_user;
use nexrade_core::db::Db;
use nexrade_core::error::{NexradeError, Result};
use nexrade_core::resp::Resp;

/// A loaded library: the original source plus the list of functions that
/// were registered via `redis.register_function`. The source is what we
/// re-execute on each call so user state declared at load time (e.g.
/// `local n = 0`) works the way it does in Redis.
#[derive(Debug, Clone)]
pub struct FunctionLibrary {
    pub name: String,
    pub source: String,
    /// Functions declared in the source, in registration order.
    pub functions: Vec<String>,
}

impl FunctionLibrary {
    pub fn new(name: String, source: String, functions: Vec<String>) -> Self {
        Self {
            name,
            source,
            functions,
        }
    }
}

/// Per-server registry of FUNCTION libraries. Cloning is cheap (Arc).
#[derive(Clone)]
pub struct FunctionRegistry {
    inner: Arc<RwLock<FunctionRegistryInner>>,
}

struct FunctionRegistryInner {
    libs: HashMap<String, FunctionLibrary>,
    total_runs: u64,
}

#[derive(Debug, Clone, Default)]
pub struct FunctionStats {
    pub total_libraries: u64,
    pub total_functions: u64,
    pub total_runs: u64,
}

#[derive(Debug, Clone, Copy)]
pub enum FunctionRestoreMode {
    Flush,
    Append,
    Replace,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(FunctionRegistryInner {
                libs: HashMap::new(),
                total_runs: 0,
            })),
        }
    }

    /// `FUNCTION LOAD <source>` (Redis passes the name inside the source
    /// header; some clients also pass `REPLACE` before the source — the
    /// caller strips that before calling `load`).
    pub fn load(&self, source: &str, allow_replace: bool) -> Result<Resp> {
        let (lib_name, funcs) = parse_library(source)
            .ok_or_else(|| NexradeError::Generic("ERR Missing library meta data".to_string()))?;
        if !is_valid_library_name(&lib_name) {
            return Err(NexradeError::Generic(format!(
                "ERR Library names can only contain letters, numbers, and underscores(_) and must have at least one alphabetic character. Got '{lib_name}'"
            )));
        }
        if funcs.is_empty() {
            return Err(NexradeError::Generic(
                "ERR No functions registered".to_string(),
            ));
        }

        // Actually run the source once (isolated Lua state) to validate it
        // is syntactically/semantically loadable and to confirm the
        // functions it claims to register really call
        // `redis.register_function`.
        validate_library(source, &funcs)?;

        let mut g = self.inner.write();
        if g.libs.contains_key(&lib_name) && !allow_replace {
            return Err(NexradeError::Generic(format!(
                "ERR Library '{lib_name}' already exists"
            )));
        }
        g.libs.insert(
            lib_name.clone(),
            FunctionLibrary::new(lib_name.clone(), source.to_string(), funcs),
        );
        Ok(Resp::bulk_str(lib_name))
    }

    /// `FUNCTION DELETE <name>`.
    pub fn delete(&self, name: &str) -> bool {
        self.inner.write().libs.remove(name).is_some()
    }

    /// `FUNCTION LIST` — `[[name, [func1, func2, ...]], ...]`.
    pub fn list(&self) -> Vec<Resp> {
        let g = self.inner.read();
        let mut names: Vec<&String> = g.libs.keys().collect();
        names.sort();
        names
            .into_iter()
            .map(|name| {
                let lib = &g.libs[name];
                Resp::array(vec![
                    Resp::bulk_str("library_name"),
                    Resp::bulk_str(lib.name.clone()),
                    Resp::bulk_str("engine"),
                    Resp::bulk_str("LUA"),
                    Resp::bulk_str("functions"),
                    Resp::array(
                        lib.functions
                            .iter()
                            .map(|f| {
                                Resp::array(vec![
                                    Resp::bulk_str("name"),
                                    Resp::bulk_str(f.clone()),
                                    Resp::bulk_str("description"),
                                    Resp::null(),
                                    Resp::bulk_str("flags"),
                                    Resp::array(vec![]),
                                ])
                            })
                            .collect(),
                    ),
                ])
            })
            .collect()
    }

    pub fn get(&self, name: &str) -> Option<FunctionLibrary> {
        self.inner.read().libs.get(name).cloned()
    }

    pub fn library_names(&self) -> Vec<String> {
        let g = self.inner.read();
        let mut names: Vec<String> = g.libs.keys().cloned().collect();
        names.sort();
        names
    }

    /// `FUNCTION FLUSH` — drop all libraries.
    pub fn flush(&self) {
        self.inner.write().libs.clear();
    }

    /// `FUNCTION DUMP` — produce a serialised payload using a simple
    /// line-based text format: `@name|func1,func2,...|<source, \n escaped>`.
    pub fn dump(&self) -> Vec<u8> {
        let g = self.inner.read();
        let mut out = String::new();
        let mut names: Vec<&String> = g.libs.keys().collect();
        names.sort();
        for name in names {
            let lib = &g.libs[name];
            out.push('@');
            out.push_str(&lib.name);
            out.push('|');
            out.push_str(&lib.functions.join(","));
            out.push('|');
            out.push_str(&lib.source.replace('\n', "\x1e"));
            out.push('\n');
        }
        out.into_bytes()
    }

    /// `FUNCTION RESTORE <payload> [FLUSH|APPEND|REPLACE]`.
    pub fn restore(&self, payload: &[u8], mode: FunctionRestoreMode) -> Result<Resp> {
        let text = std::str::from_utf8(payload).map_err(|_| {
            NexradeError::Generic("payload version or checksum are wrong".to_string())
        })?;
        let parsed = parse_dump(text)?;
        let mut g = self.inner.write();
        if matches!(mode, FunctionRestoreMode::Flush) {
            g.libs.clear();
        }
        for lib in parsed {
            match mode {
                FunctionRestoreMode::Append => {
                    if g.libs.contains_key(&lib.name) {
                        return Err(NexradeError::Generic(format!(
                            "ERR Library '{}' already exists",
                            lib.name
                        )));
                    }
                    g.libs.insert(lib.name.clone(), lib);
                }
                FunctionRestoreMode::Flush | FunctionRestoreMode::Replace => {
                    g.libs.insert(lib.name.clone(), lib);
                }
            }
        }
        Ok(Resp::ok())
    }

    pub fn stats(&self) -> FunctionStats {
        let g = self.inner.read();
        FunctionStats {
            total_libraries: g.libs.len() as u64,
            total_functions: g.libs.values().map(|l| l.functions.len() as u64).sum(),
            total_runs: g.total_runs,
        }
    }

    /// `FCALL <function> <numkeys> [key ...] [arg ...]` — find the library
    /// that registers `func` and invoke it.
    pub async fn call(
        &self,
        func: &str,
        keys: Vec<Vec<u8>>,
        argv: Vec<Vec<u8>>,
        db: Db,
        db_index: usize,
        user: &str,
    ) -> Result<Resp> {
        let source = {
            let g = self.inner.read();
            g.libs
                .values()
                .find(|l| l.functions.iter().any(|f| f == func))
                .map(|l| l.source.clone())
        };
        let Some(source) = source else {
            return Err(NexradeError::Generic("ERR Function not found".to_string()));
        };
        self.inner.write().total_runs += 1;
        run_function(&source, func, keys, argv, db, db_index, user).await
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Strip the `#!lua name=...` shebang header (not valid Lua syntax) before
/// handing the source to the interpreter — matches Redis's loader, which
/// treats that line as metadata, not code.
fn strip_shebang(source: &str) -> String {
    match source.split_once('\n') {
        Some((first, rest)) if first.trim_start().starts_with("#!") => rest.to_string(),
        _ => source.to_string(),
    }
}

fn is_valid_library_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 200
        && name.chars().any(|c| c.is_ascii_alphabetic())
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Run the library source in a fresh Lua state and execute `func`,
/// dispatching `redis.call` back through the real store.
async fn run_function(
    source: &str,
    func: &str,
    keys: Vec<Vec<u8>>,
    argv: Vec<Vec<u8>>,
    db: Db,
    db_index: usize,
    user: &str,
) -> Result<Resp> {
    let source = source.to_string();
    let func = func.to_string();
    let user = user.to_string();
    tokio::task::spawn_blocking(move || {
        let lua = Lua::new_with(
            StdLib::TABLE | StdLib::STRING | StdLib::MATH,
            mlua::LuaOptions::default(),
        )
        .map_err(mlua_err)?;
        setup_function_env(&lua, &db, db_index, &user).map_err(mlua_err)?;

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

        lua.load(strip_shebang(&source)).exec().map_err(mlua_err)?;

        let registered: mlua::Table = lua
            .globals()
            .get("__nexrade_registered")
            .map_err(mlua_err)?;
        let func_value: LuaValue = registered.get(func.as_str()).map_err(mlua_err)?;
        let func_fn: Function = match func_value {
            LuaValue::Function(f) => f,
            _ => {
                return Err(NexradeError::Generic(format!(
                    "ERR function '{func}' not registered by its library"
                )))
            }
        };
        // Redis calls registered functions with (keys, args) tables.
        let keys_arg: mlua::Table = lua.globals().get("KEYS").map_err(mlua_err)?;
        let argv_arg: mlua::Table = lua.globals().get("ARGV").map_err(mlua_err)?;
        let result: LuaValue = func_fn.call((keys_arg, argv_arg)).map_err(mlua_err)?;
        Ok::<Resp, NexradeError>(lua_value_to_resp(result))
    })
    .await
    .map_err(|e| NexradeError::Generic(e.to_string()))?
}

/// Validate a library source by loading it (but not calling any
/// functions) in a throwaway Lua state, confirming it registers the
/// functions it claims to.
fn validate_library(source: &str, expected_funcs: &[String]) -> Result<()> {
    let lua = Lua::new_with(
        StdLib::TABLE | StdLib::STRING | StdLib::MATH,
        mlua::LuaOptions::default(),
    )
    .map_err(mlua_err)?;
    setup_validate_env(&lua).map_err(mlua_err)?;
    lua.load(strip_shebang(source)).exec().map_err(mlua_err)?;
    let registered: mlua::Table = lua
        .globals()
        .get("__nexrade_registered")
        .map_err(mlua_err)?;
    for f in expected_funcs {
        let v: LuaValue = registered.get(f.as_str()).map_err(mlua_err)?;
        if !matches!(v, LuaValue::Function(_)) {
            return Err(NexradeError::Generic(format!(
                "ERR Error compiling function: function '{f}' not registered"
            )));
        }
    }
    Ok(())
}

/// Set up a minimal `redis` global for library validation only — no real
/// `redis.call` (validation shouldn't touch the store).
fn setup_validate_env(lua: &Lua) -> mlua::Result<()> {
    let redis = lua.create_table()?;
    let noop = lua.create_function(|_, _: mlua::MultiValue| Ok(LuaValue::Nil))?;
    redis.set("call", noop.clone())?;
    redis.set("pcall", noop)?;
    lua.globals()
        .set("__nexrade_registered", lua.create_table()?)?;
    // Look up the registry table fresh inside the closure so it doesn't
    // capture a `Table` tied to `lua`'s borrow lifetime.
    let register_fn = lua.create_function(|lua, (name, f): (String, Function)| {
        let registered: mlua::Table = lua.globals().get("__nexrade_registered")?;
        registered.set(name, f)?;
        Ok(())
    })?;
    redis.set("register_function", register_fn)?;
    lua.globals().set("redis", redis)?;
    Ok(())
}

/// Set up the real `redis` global for function execution — `redis.call`
/// dispatches to the live store as `user`, the ACL identity of the
/// connection that issued FCALL (so a function can't use ACL-restricted
/// commands the caller itself couldn't run directly).
fn setup_function_env(lua: &Lua, db: &Db, db_index: usize, user: &str) -> mlua::Result<()> {
    let redis = lua.create_table()?;

    let db_call = db.clone();
    let call_user = user.to_string();
    let call_fn = lua.create_function(move |lua, args: mlua::MultiValue| {
        let resp_args = multivalue_to_resp_args(args)?;
        let result = tokio::runtime::Handle::current().block_on(dispatch_with_user(
            &db_call, resp_args, db_index, None, &call_user,
        ));
        resp_to_lua_local(lua, result)
    })?;
    redis.set("call", call_fn.clone())?;
    redis.set("pcall", call_fn)?;

    lua.globals()
        .set("__nexrade_registered", lua.create_table()?)?;
    let register_fn = lua.create_function(|lua, (name, f): (String, Function)| {
        let registered: mlua::Table = lua.globals().get("__nexrade_registered")?;
        registered.set(name, f)?;
        Ok(())
    })?;
    redis.set("register_function", register_fn)?;

    lua.globals().set("redis", redis)?;
    Ok(())
}

fn mlua_err(e: mlua::Error) -> NexradeError {
    NexradeError::Generic(e.to_string())
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

fn resp_to_lua_local<'lua>(lua: &'lua Lua, resp: Resp) -> mlua::Result<LuaValue<'lua>> {
    Ok(match resp {
        Resp::SimpleString(s) => {
            let t = lua.create_table()?;
            t.set("ok", s)?;
            LuaValue::Table(t)
        }
        Resp::Error(e) => {
            let t = lua.create_table()?;
            t.set("err", e)?;
            LuaValue::Table(t)
        }
        Resp::Integer(n) => LuaValue::Integer(n),
        Resp::BulkString(Some(b)) => LuaValue::String(lua.create_string(&b)?),
        Resp::BulkString(None) => LuaValue::Boolean(false),
        Resp::Array(Some(items)) => {
            let t = lua.create_table()?;
            for (i, item) in items.into_iter().enumerate() {
                t.set(i + 1, resp_to_lua_local(lua, item)?)?;
            }
            LuaValue::Table(t)
        }
        Resp::Array(None) => LuaValue::Boolean(false),
        _ => LuaValue::Nil,
    })
}

fn lua_value_to_resp(val: LuaValue) -> Resp {
    match val {
        LuaValue::Nil => Resp::BulkString(None),
        LuaValue::Boolean(b) => {
            if b {
                Resp::Integer(1)
            } else {
                Resp::BulkString(None)
            }
        }
        LuaValue::Integer(n) => Resp::Integer(n),
        LuaValue::Number(f) => Resp::Integer(f as i64),
        LuaValue::String(s) => Resp::bulk(bytes::Bytes::copy_from_slice(s.as_bytes())),
        LuaValue::Table(t) => {
            // `{ok=...}` / `{err=...}` special tables, else array.
            if let Ok(ok) = t.get::<_, String>("ok") {
                return Resp::SimpleString(ok);
            }
            if let Ok(err) = t.get::<_, String>("err") {
                return Resp::Error(err);
            }
            let mut items = Vec::new();
            let mut i = 1usize;
            loop {
                match t.get::<_, LuaValue>(i) {
                    Ok(LuaValue::Nil) => break,
                    Ok(v) => items.push(lua_value_to_resp(v)),
                    Err(_) => break,
                }
                i += 1;
            }
            Resp::Array(Some(items))
        }
        _ => Resp::BulkString(None),
    }
}

// ── Parsing helpers ──────────────────────────────────────────────────────────

/// Parse a library source: extract the library name (from `#!lua name=...`)
/// and the list of functions registered via `redis.register_function(...)`.
fn parse_library(source: &str) -> Option<(String, Vec<String>)> {
    let mut name: Option<String> = None;
    let mut funcs: Vec<String> = Vec::new();
    for line in source.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("#!lua") {
            let rest = rest.trim();
            if let Some(eq) = rest.find('=') {
                let (k, v) = rest.split_at(eq);
                if k.trim() == "name" {
                    name = Some(v[1..].trim().to_string());
                }
            }
        } else if let Some(idx) = trimmed.find("register_function") {
            let rest = &trimmed[idx + "register_function".len()..];
            let rest = rest.trim_start_matches(|c: char| c != '\'' && c != '"');
            if let Some(quote) = rest.chars().next() {
                let rest = &rest[1..];
                if let Some(end) = rest.find(quote) {
                    funcs.push(rest[..end].to_string());
                }
            }
        }
    }
    name.map(|n| (n, funcs))
}

fn parse_dump(text: &str) -> Result<Vec<FunctionLibrary>> {
    let mut out = Vec::new();
    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        if !line.starts_with('@') {
            return Err(NexradeError::Generic(
                "ERR payload version or checksum are wrong".to_string(),
            ));
        }
        let parts: Vec<&str> = line.splitn(3, '|').collect();
        if parts.len() != 3 {
            return Err(NexradeError::Generic(
                "ERR payload version or checksum are wrong".to_string(),
            ));
        }
        let name = parts[0].trim_start_matches('@').to_string();
        let funcs: Vec<String> = if parts[1].is_empty() {
            Vec::new()
        } else {
            parts[1].split(',').map(|s| s.to_string()).collect()
        };
        let source = parts[2].replace('\x1e', "\n");
        out.push(FunctionLibrary::new(name, source, funcs));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIB_SRC: &str = r#"#!lua name=mylib
redis.register_function('myfunc', function(keys, args)
  return redis.call('SET', keys[1], args[1])
end)
"#;

    #[test]
    fn parse_extracts_name_and_functions() {
        let (name, funcs) = parse_library(LIB_SRC).unwrap();
        assert_eq!(name, "mylib");
        assert_eq!(funcs, vec!["myfunc".to_string()]);
    }

    #[test]
    fn load_and_list() {
        let reg = FunctionRegistry::new();
        reg.load(LIB_SRC, false).unwrap();
        let libs = reg.library_names();
        assert_eq!(libs, vec!["mylib".to_string()]);
    }

    #[test]
    fn load_twice_without_replace_fails() {
        let reg = FunctionRegistry::new();
        reg.load(LIB_SRC, false).unwrap();
        assert!(reg.load(LIB_SRC, false).is_err());
        assert!(reg.load(LIB_SRC, true).is_ok());
    }

    #[test]
    fn dump_restore_roundtrip() {
        let reg = FunctionRegistry::new();
        reg.load(LIB_SRC, false).unwrap();
        let dump = reg.dump();

        let reg2 = FunctionRegistry::new();
        reg2.restore(&dump, FunctionRestoreMode::Flush).unwrap();
        assert_eq!(reg2.library_names(), vec!["mylib".to_string()]);
        let lib = reg2.get("mylib").unwrap();
        assert_eq!(lib.functions, vec!["myfunc".to_string()]);
    }

    #[test]
    fn delete_removes_library() {
        let reg = FunctionRegistry::new();
        reg.load(LIB_SRC, false).unwrap();
        assert!(reg.delete("mylib"));
        assert!(reg.library_names().is_empty());
    }

    #[tokio::test]
    async fn fcall_executes_function() {
        let reg = FunctionRegistry::new();
        reg.load(LIB_SRC, false).unwrap();
        let db = Db::default();
        let result = reg
            .call(
                "myfunc",
                vec![b"k".to_vec()],
                vec![b"v".to_vec()],
                db.clone(),
                0,
                "default",
            )
            .await
            .unwrap();
        assert!(matches!(result, Resp::SimpleString(s) if s == "OK"));
        // Verify the SET actually took effect.
        let get = nexrade_core::command::dispatch(
            &db,
            vec![Resp::bulk_str("GET"), Resp::bulk_str("k")],
            0,
        )
        .await;
        assert!(matches!(get, Resp::BulkString(Some(_))));
    }

    /// Regression test: FCALL must run `redis.call` under the caller's own
    /// ACL identity, not a hardcoded "default" full-access user — otherwise
    /// a restricted user could bypass a command/key restriction by calling
    /// a library function that wraps the forbidden command.
    #[tokio::test]
    async fn fcall_enforces_callers_acl_restrictions() {
        let reg = FunctionRegistry::new();
        reg.load(LIB_SRC, false).unwrap();
        let db = Db::default();
        db.acl.setuser("restricted", &["+ping", "~*"]).unwrap();

        let result = reg
            .call(
                "myfunc",
                vec![b"k".to_vec()],
                vec![b"v".to_vec()],
                db.clone(),
                0,
                "restricted",
            )
            .await
            .unwrap();
        match result {
            Resp::Error(msg) => {
                assert!(
                    msg.to_lowercase().contains("permission"),
                    "expected a permission error, got: {msg}"
                );
            }
            other => panic!("expected redis.call to surface the ACL denial, got: {other:?}"),
        }
        assert!(
            db.store.db(0).read_for(b"k").get_ro(b"k").is_none(),
            "SET should not have applied — restricted user has no SET permission"
        );
    }
}
