//! 09-plugin — Example custom plugin for nexrade-cache.
//!
//! This plugin adds three custom commands:
//!   MYAPP.ECHO <message>         — echo with a prefix
//!   MYAPP.SETJSON <key> <json>   — store JSON, validate it first
//!   MYAPP.STATS                  — return a summary of the keyspace
//!
//! How to register in your server setup:
//! ```rust,no_run
//! use nexrade_plugin::{PluginRegistry, PluginContext};
//! use nexrade_embedded_examples::MyAppPlugin;
//!
//! let registry = PluginRegistry::new();
//! registry.register(Box::new(MyAppPlugin), &ctx).await.unwrap();
//! ```

use async_trait::async_trait;
use nexrade_core::command::dispatch;
use nexrade_core::db::Db;
use nexrade_core::resp::Resp;
use nexrade_plugin::{CommandHandler, Plugin, PluginContext};

pub struct MyAppPlugin;

#[async_trait]
impl Plugin for MyAppPlugin {
    fn name(&self) -> &str {
        "myapp"
    }

    fn version(&self) -> &str {
        "1.0.0"
    }

    fn description(&self) -> &str {
        "Example plugin: ECHO, SETJSON, STATS"
    }

    fn commands(&self) -> Vec<CommandHandler> {
        vec![
            // ── MYAPP.ECHO ────────────────────────────────────────────────────
            CommandHandler::new("MYAPP.ECHO", |_db, args, _| {
                Box::pin(async move {
                    let msg = args
                        .get(1)
                        .and_then(|a| a.as_str())
                        .unwrap_or("(no message)");
                    Ok(Resp::bulk_str(&format!("[myapp] {}", msg)))
                })
            })
            .with_description("Echo a message with [myapp] prefix")
            .with_arity(2)
            .with_flags(vec!["readonly", "fast"]),

            // ── MYAPP.SETJSON ─────────────────────────────────────────────────
            CommandHandler::new("MYAPP.SETJSON", |db, args, db_index| {
                Box::pin(async move {
                    let key = args
                        .get(1)
                        .and_then(|a| a.as_str())
                        .ok_or_else(|| nexrade_core::error::NexradeError::WrongArity("MYAPP.SETJSON".into()))?;
                    let json_str = args
                        .get(2)
                        .and_then(|a| a.as_str())
                        .ok_or_else(|| nexrade_core::error::NexradeError::WrongArity("MYAPP.SETJSON".into()))?;

                    // Validate JSON before storing
                    if let Err(e) = serde_json::from_str::<serde_json::Value>(json_str) {
                        return Ok(Resp::error(&format!("ERR invalid JSON: {e}")));
                    }

                    dispatch(
                        &db,
                        vec![Resp::bulk_str("SET"), Resp::bulk_str(key), Resp::bulk_str(json_str)],
                        db_index,
                    )
                    .await;
                    Ok(Resp::ok())
                })
            })
            .with_description("Store a value only if it is valid JSON")
            .with_arity(3),

            // ── MYAPP.STATS ───────────────────────────────────────────────────
            CommandHandler::new("MYAPP.STATS", |db, _args, db_index| {
                Box::pin(async move {
                    let dbsize = dispatch(
                        &db,
                        vec![Resp::bulk_str("DBSIZE")],
                        db_index,
                    )
                    .await;
                    let info = format!("keys={}", dbsize);
                    Ok(Resp::bulk_str(&info))
                })
            })
            .with_description("Return a quick stats summary")
            .with_arity(1)
            .with_flags(vec!["readonly", "fast"]),
        ]
    }

    async fn on_load(&self, _ctx: &PluginContext) -> anyhow::Result<()> {
        println!("[myapp] Plugin loaded. Commands: MYAPP.ECHO, MYAPP.SETJSON, MYAPP.STATS");
        Ok(())
    }

    async fn on_unload(&self) {
        println!("[myapp] Plugin unloaded.");
    }
}
