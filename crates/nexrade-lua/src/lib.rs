//! Lua scripting for nexrade-cache.
//!
//! Implements `EVAL`, `EVALSHA`, and `SCRIPT` commands using the `mlua` crate
//! with embedded Lua 5.4.
//!
//! # Security
//!
//! - Scripts run in a sandboxed Lua environment (no `os`, `io`, `debug` modules).
//! - A configurable time limit terminates runaway scripts.
//! - Scripts can call `redis.call()` / `redis.pcall()` to execute nexrade commands.
//!
//! # Example
//!
//! ```lua
//! -- EVAL "return redis.call('SET', KEYS[1], ARGV[1])" 1 mykey myvalue
//! redis.call('SET', KEYS[1], ARGV[1])
//! return 'OK'
//! ```

pub mod engine;
pub mod script_cache;
pub mod value;

pub use engine::LuaEngine;
pub use script_cache::ScriptCache;
