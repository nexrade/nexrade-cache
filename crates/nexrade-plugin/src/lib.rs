//! Plugin system for nexrade-cache.
//!
//! Plugins can register custom commands, intercept existing commands,
//! and hook into server lifecycle events.
//!
//! # Writing a plugin
//!
//! ```rust,no_run
//! use nexrade_plugin::{Plugin, PluginContext, CommandHandler};
//! use nexrade_core::resp::Resp;
//! use nexrade_core::db::Db;
//! use async_trait::async_trait;
//!
//! pub struct HelloPlugin;
//!
//! #[async_trait]
//! impl Plugin for HelloPlugin {
//!     fn name(&self) -> &str { "hello" }
//!     fn version(&self) -> &str { "0.1.0" }
//!     fn description(&self) -> &str { "Adds a HELLO command" }
//!
//!     fn commands(&self) -> Vec<CommandHandler> {
//!         vec![CommandHandler::new("HELLO", |_db, _args, _db_idx| Box::pin(async {
//!             Ok(Resp::bulk_str("Hello from plugin!"))
//!         }))]
//!     }
//!
//!     async fn on_load(&self, ctx: &PluginContext) -> anyhow::Result<()> {
//!         tracing::info!("HelloPlugin loaded");
//!         Ok(())
//!     }
//! }
//! ```
//!
//! # Registering plugins
//!
//! ```rust,no_run
//! # use nexrade_plugin::{PluginRegistry, PluginContext};
//! # use nexrade_core::db::Db;
//! # async fn example() {
//! let registry = PluginRegistry::new();
//! // registry.register(Box::new(MyPlugin), &ctx).await.unwrap();
//! # }
//! ```

pub mod handler;
pub mod registry;

pub use handler::{CommandHandler, CommandResult};
pub use registry::{Plugin, PluginContext, PluginRegistry};
