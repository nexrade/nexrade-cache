//! Command handler definition for plugins.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use nexrade_core::db::Db;
use nexrade_core::error::Result;
use nexrade_core::resp::Resp;

/// A boxed async command handler function.
pub type CommandResult = Pin<Box<dyn Future<Output = Result<Resp>> + Send>>;
pub type CommandFn = Arc<dyn Fn(Db, Vec<Resp>, usize) -> CommandResult + Send + Sync>;

/// A registered plugin command handler.
pub struct CommandHandler {
    /// Command name (uppercase, e.g. "HELLO")
    pub name: String,
    /// Handler function
    pub func: CommandFn,
    /// Description
    pub description: String,
    /// Minimum number of arguments (including command name)
    pub arity: i64,
    /// Command flags (e.g. "write", "readonly", "admin")
    pub flags: Vec<String>,
}

impl CommandHandler {
    /// Create a new command handler.
    pub fn new(
        name: impl Into<String>,
        func: impl Fn(Db, Vec<Resp>, usize) -> CommandResult + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into().to_uppercase(),
            func: Arc::new(func),
            description: String::new(),
            arity: -1,
            flags: vec!["readonly".to_string()],
        }
    }

    /// Set the description.
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = desc.into();
        self
    }

    /// Set the minimum arity (positive = exact, negative = minimum).
    pub fn with_arity(mut self, arity: i64) -> Self {
        self.arity = arity;
        self
    }

    /// Set command flags.
    pub fn with_flags(mut self, flags: Vec<impl Into<String>>) -> Self {
        self.flags = flags.into_iter().map(Into::into).collect();
        self
    }

    /// Execute this command.
    pub async fn execute(&self, db: Db, args: Vec<Resp>, db_index: usize) -> Result<Resp> {
        (self.func)(db, args, db_index).await
    }
}
