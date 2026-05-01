//! Plugin registry — manages loaded plugins and dispatches to them.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;
use tracing::{info, warn};

use nexrade_core::db::Db;
use nexrade_core::error::Result;
use nexrade_core::resp::Resp;

use crate::handler::CommandHandler;

/// Context passed to plugins during lifecycle events.
#[derive(Clone)]
pub struct PluginContext {
    pub db: Db,
}

/// Trait that all plugins must implement.
#[async_trait]
pub trait Plugin: Send + Sync {
    /// Plugin name.
    fn name(&self) -> &str;
    /// Plugin version.
    fn version(&self) -> &str;
    /// Plugin description.
    fn description(&self) -> &str;

    /// Commands provided by this plugin.
    fn commands(&self) -> Vec<CommandHandler>;

    /// Called when the plugin is loaded.
    async fn on_load(&self, _ctx: &PluginContext) -> anyhow::Result<()> {
        Ok(())
    }

    /// Called before each command execution. Return Ok(None) to pass through,
    /// Ok(Some(resp)) to short-circuit with a custom response.
    async fn before_command(
        &self,
        _cmd: &str,
        _args: &[Resp],
        _db: &Db,
        _db_index: usize,
    ) -> Option<Result<Resp>> {
        None
    }

    /// Called after each command execution.
    async fn after_command(
        &self,
        _cmd: &str,
        _args: &[Resp],
        _result: &Resp,
        _db: &Db,
        _db_index: usize,
    ) {
    }

    /// Called on server shutdown.
    async fn on_unload(&self) {}
}

/// The central plugin registry.
pub struct PluginRegistry {
    plugins: Arc<RwLock<Vec<Box<dyn Plugin>>>>,
    commands: Arc<RwLock<HashMap<String, CommandHandler>>>,
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self {
            plugins: Arc::new(RwLock::new(Vec::new())),
            commands: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a plugin and its commands.
    pub async fn register(
        &self,
        plugin: Box<dyn Plugin>,
        ctx: &PluginContext,
    ) -> anyhow::Result<()> {
        let name = plugin.name().to_string();
        let version = plugin.version().to_string();

        let cmds = plugin.commands();
        plugin.on_load(ctx).await?;

        info!(
            "loaded plugin '{}' v{} ({} commands)",
            name,
            version,
            cmds.len()
        );

        let mut cmd_map = self.commands.write();
        for cmd in cmds {
            let cmd_name = cmd.name.clone();
            if cmd_map.contains_key(&cmd_name) {
                warn!(
                    "plugin '{}' overrides existing command '{}'",
                    name, cmd_name
                );
            }
            cmd_map.insert(cmd_name, cmd);
        }

        self.plugins.write().push(plugin);
        Ok(())
    }

    /// Check if a command name is registered by any plugin.
    pub fn has_command(&self, cmd: &str) -> bool {
        self.commands.read().contains_key(cmd)
    }

    /// Execute a plugin command.
    #[allow(clippy::await_holding_lock)]
    pub async fn execute(
        &self,
        cmd: &str,
        args: Vec<Resp>,
        db: Db,
        db_index: usize,
    ) -> Option<Result<Resp>> {
        // Run before_command hooks
        {
            let plugins = self.plugins.read();
            for plugin in plugins.iter() {
                let p: &dyn Plugin = plugin.as_ref();
                if let Some(result) = p.before_command(cmd, &args, &db, db_index).await {
                    return Some(result);
                }
            }
        }

        // Execute the command
        let result = {
            let cmds = self.commands.read();
            if let Some(handler) = cmds.get(cmd) {
                let func = handler.func.clone();
                drop(cmds);
                Some(func(db.clone(), args.clone(), db_index).await)
            } else {
                None
            }
        };

        // Run after_command hooks
        if let Some(Ok(ref r)) = result {
            let plugins = self.plugins.read();
            for plugin in plugins.iter() {
                let p: &dyn Plugin = plugin.as_ref();
                p.after_command(cmd, &args, r, &db, db_index).await;
            }
        }

        result
    }

    /// List all registered plugin command names.
    pub fn command_names(&self) -> Vec<String> {
        self.commands.read().keys().cloned().collect()
    }

    /// Number of loaded plugins.
    pub fn plugin_count(&self) -> usize {
        self.plugins.read().len()
    }

    /// Unload all plugins.
    #[allow(clippy::await_holding_lock)]
    pub async fn unload_all(&self) {
        let plugins = self.plugins.read();
        for plugin in plugins.iter() {
            let p: &dyn Plugin = plugin.as_ref();
            p.on_unload().await;
        }
    }
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// A simple example plugin that adds a NEXRADE.HELLO command.
pub struct PingExamplePlugin;

#[async_trait]
impl Plugin for PingExamplePlugin {
    fn name(&self) -> &str {
        "ping-example"
    }
    fn version(&self) -> &str {
        "0.1.0"
    }
    fn description(&self) -> &str {
        "Example plugin — adds NEXRADE.PING command"
    }

    fn commands(&self) -> Vec<CommandHandler> {
        vec![CommandHandler::new("NEXRADE.PING", |_db, _args, _| {
            Box::pin(async move { Ok(Resp::bulk_str("NEXRADE PONG from plugin!")) })
        })
        .with_description("Extended PING from nexrade plugin")
        .with_arity(1)
        .with_flags(vec!["readonly", "fast"])]
    }
}
