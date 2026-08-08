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
    use js_sys::{Array, Promise, Uint8Array};
    use wasm_bindgen::prelude::*;
    use wasm_bindgen::JsCast;
    use wasm_bindgen_futures::future_to_promise;

    /// Initialize panic hook for better error messages in browser console.
    #[wasm_bindgen(start)]
    pub fn init_panic_hook() {
        console_error_panic_hook::set_once();
    }

    fn js_args_to_resp(value: &JsValue) -> Result<Vec<Resp>, JsValue> {
        if !Array::is_array(value) {
            return Err(JsValue::from_str("command arguments must be an array"));
        }
        let array = Array::from(value);
        let mut args = Vec::with_capacity(array.length() as usize);

        for value in array.iter() {
            if let Some(text) = value.as_string() {
                args.push(Resp::bulk_str(text));
            } else if value.is_null() || value.is_undefined() {
                return Err(JsValue::from_str("command arguments cannot contain null"));
            } else if let Some(number) = value.as_f64() {
                if !number.is_finite() {
                    return Err(JsValue::from_str("command arguments must be finite"));
                }
                let text = if number.fract() == 0.0 {
                    format!("{number:.0}")
                } else {
                    number.to_string()
                };
                args.push(Resp::bulk_str(text));
            } else if let Some(bytes) = value.dyn_ref::<Uint8Array>() {
                args.push(Resp::bulk(bytes.to_vec()));
            } else {
                return Err(JsValue::from_str(
                    "command arguments must be strings, numbers, or Uint8Array values",
                ));
            }
        }

        if args.is_empty() {
            return Err(JsValue::from_str(
                "a command requires at least one argument",
            ));
        }
        Ok(args)
    }

    fn resp_to_js(value: Resp) -> Result<JsValue, JsValue> {
        match value {
            Resp::SimpleString(text) => Ok(JsValue::from_str(&text)),
            Resp::Error(message) => Err(JsValue::from_str(&message)),
            Resp::Integer(number) => Ok(JsValue::from_f64(number as f64)),
            Resp::Double(number) => Ok(JsValue::from_f64(number)),
            Resp::Bool(value) => Ok(JsValue::from_bool(value)),
            Resp::BulkString(None) | Resp::Null => Ok(JsValue::NULL),
            Resp::BulkString(Some(bytes)) => Ok(Uint8Array::from(bytes.as_ref()).into()),
            Resp::Array(None) => Ok(JsValue::NULL),
            Resp::Array(Some(values)) | Resp::Set(values) | Resp::Push(values) => {
                let output = Array::new();
                for value in values {
                    output.push(&resp_to_js(value)?);
                }
                Ok(output.into())
            }
            Resp::Map(pairs) => {
                let output = Array::new();
                for (key, value) in pairs {
                    let pair = Array::new();
                    pair.push(&resp_to_js(key)?);
                    pair.push(&resp_to_js(value)?);
                    output.push(&pair);
                }
                Ok(output.into())
            }
            Resp::Raw(bytes) => {
                let mut parser = RespParser::new();
                parser.feed(&bytes);
                match parser.parse_one() {
                    Ok(Some(value)) => resp_to_js(value),
                    Ok(None) => Err(JsValue::from_str("incomplete command response")),
                    Err(error) => Err(JsValue::from_str(&error.to_string())),
                }
            }
        }
    }

    /// WASM-accessible nexrade store.
    #[wasm_bindgen]
    pub struct NexradeWasm {
        store: NexradeStore,
    }

    // Not inside the `#[wasm_bindgen]` block: the JS side reaches the
    // constructor through `new()`, and `wasm_bindgen` does not export trait
    // impls. This exists so the Rust-side type satisfies `Default` like any
    // other no-argument constructor.
    impl Default for NexradeWasm {
        fn default() -> Self {
            Self::new()
        }
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

        /// Execute a command from structured JavaScript arguments.
        /// Strings and numbers are encoded as Redis bulk strings; Uint8Array
        /// values are sent as binary-safe bulk strings. The Promise resolves
        /// to native JavaScript values and rejects Redis errors.
        pub fn command(&self, args: JsValue) -> Promise {
            let db = self.store.db.clone();

            future_to_promise(async move {
                let args = js_args_to_resp(&args)?;
                let result = dispatch(&db, args, 0).await;
                resp_to_js(result)
            })
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
