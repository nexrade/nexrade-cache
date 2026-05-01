//! WebAssembly bindings for nexrade-cache.
//!
//! This crate allows nexrade-cache to run in the browser or edge functions
//! (Cloudflare Workers, Deno Deploy, etc.) via WebAssembly.
//!
//! # Building for WASM
//!
//! ```sh
//! wasm-pack build crates/nexrade-wasm --target web --features wasm
//! ```
//!
//! # Usage in JavaScript/TypeScript
//!
//! ```javascript
//! import init, { NexradeWasm } from './pkg/nexrade_wasm.js';
//!
//! await init();
//!
//! const store = new NexradeWasm();
//! await store.execute('SET foo bar');
//! const result = await store.execute('GET foo');
//! console.log(result); // "bar"
//! ```
//!
//! # Architecture
//!
//! In WASM mode, nexrade runs as a fully in-process store without networking.
//! The RESP protocol is used internally but connections are simulated via
//! JavaScript promises.

use nexrade_core::command::dispatch;
use nexrade_core::db::Db;
use nexrade_core::resp::{Resp, RespParser};

/// The core WASM-accessible store.
pub struct NexradeStore {
    db: Db,
}

impl NexradeStore {
    pub fn new() -> Self {
        Self { db: Db::default() }
    }

    /// Execute a RESP command string, return RESP response bytes.
    pub async fn execute_resp(&self, input: &[u8]) -> Vec<u8> {
        let mut parser = RespParser::new();
        parser.feed(input);
        match parser.parse_one() {
            Ok(Some(Resp::Array(Some(args)))) => {
                let result = dispatch(&self.db, args, 0).await;
                result.serialize().to_vec()
            }
            _ => Resp::error("protocol error").serialize().to_vec(),
        }
    }

    /// Execute a command given as an inline string (e.g. "SET foo bar").
    pub async fn execute_inline(&self, cmd: &str) -> String {
        let mut input = cmd.as_bytes().to_vec();
        input.extend_from_slice(b"\r\n");

        let resp_bytes = self.execute_resp(&input).await;
        let mut parser = RespParser::new();
        parser.feed(&resp_bytes);
        match parser.parse_one() {
            Ok(Some(r)) => r.to_string(),
            _ => "(error)".to_string(),
        }
    }
}

impl Default for NexradeStore {
    fn default() -> Self {
        Self::new()
    }
}

/// WASM bindings — only compiled for wasm32 targets.
#[cfg(target_arch = "wasm32")]
#[cfg(feature = "wasm")]
pub mod wasm_bindings {
    use super::*;
    use js_sys::Promise;
    use wasm_bindgen::prelude::*;
    use wasm_bindgen_futures::future_to_promise;

    /// Initialize panic hook for better error messages in browser console.
    #[wasm_bindgen(start)]
    pub fn init_panic_hook() {
        console_error_panic_hook::set_once();
    }

    /// WASM-accessible nexrade store.
    #[wasm_bindgen]
    pub struct NexradeWasm {
        store: NexradeStore,
    }

    #[wasm_bindgen]
    impl NexradeWasm {
        /// Create a new in-memory store.
        #[wasm_bindgen(constructor)]
        pub fn new() -> Self {
            Self {
                store: NexradeStore::new(),
            }
        }

        /// Execute a command (inline format like "SET key value").
        /// Returns a Promise<string>.
        pub fn execute(&self, cmd: &str) -> Promise {
            // We can't easily pass self across async boundary in WASM,
            // so we parse and dispatch synchronously here.
            let cmd = cmd.to_string();
            let db = self.store.db.clone();

            future_to_promise(async move {
                let mut parser = RespParser::new();
                let mut input = cmd.as_bytes().to_vec();
                input.extend_from_slice(b"\r\n");
                parser.feed(&input);

                let result = match parser.parse_one() {
                    Ok(Some(Resp::Array(Some(args)))) => dispatch(&db, args, 0).await,
                    _ => Resp::error("protocol error"),
                };

                Ok(JsValue::from_str(&result.to_string()))
            })
        }

        /// Ping the store.
        pub fn ping(&self) -> String {
            "PONG".to_string()
        }

        /// Get the number of keys in db 0.
        pub fn dbsize(&self) -> u32 {
            self.store.db.store.db(0).len() as u32
        }

        /// Flush all data.
        pub fn flushall(&self) {
            self.store.db.store.flush_all();
        }
    }
}

/// Native (non-WASM) async API — for embedding nexrade in Rust applications.
///
/// # Example
///
/// ```rust
/// use nexrade_wasm::NexradeStore;
///
/// #[tokio::main]
/// async fn main() {
///     let store = NexradeStore::new();
///     let result = store.execute_inline("SET hello world").await;
///     println!("{}", result); // OK
///     let result = store.execute_inline("GET hello").await;
///     println!("{}", result); // world
/// }
/// ```
#[cfg(not(target_arch = "wasm32"))]
pub use NexradeStore as EmbeddedStore;
