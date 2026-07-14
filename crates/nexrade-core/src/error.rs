use thiserror::Error;

pub type Result<T> = std::result::Result<T, NexradeError>;

#[derive(Debug, Error, Clone)]
pub enum NexradeError {
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    #[error("no such key")]
    NoKey,

    #[error("value is not an integer or out of range")]
    NotInteger,

    #[error("value is not a valid float")]
    NotFloat,

    #[error("out of range value for incr")]
    Overflow,

    #[error("syntax error")]
    SyntaxError,

    #[error("ERR unknown command '{0}'{1}")]
    UnknownCommand(String, String),

    #[error("ERR wrong number of arguments for '{0}' command")]
    WrongArity(String),

    #[error("index out of range")]
    IndexOutOfRange,

    #[error("bit offset is not an integer or out of range")]
    BitError,

    #[error("ERR {0}")]
    Generic(String),

    /// Wraps an error whose `Display` already includes its own reply-code
    /// prefix (e.g. `WRONGPASS ...`, `NOPERM ...`) — unlike `Generic`,
    /// this does NOT prepend `ERR `.
    #[error("{0}")]
    Prefixed(String),

    #[error("EXECABORT Transaction discarded because of previous errors")]
    ExecAbort,

    #[error("ERR EXEC without MULTI")]
    ExecWithoutMulti,

    #[error("ERR MULTI calls can not be nested")]
    NestedMulti,

    #[error("ERR DISCARD without MULTI")]
    DiscardWithoutMulti,

    #[error("LOADING Redis is loading the dataset in memory")]
    Loading,

    #[error("READONLY You can't write against a read only replica")]
    ReadOnly,

    #[error("ERR Protocol error: {0}")]
    ProtocolError(String),

    #[error("ERR {0}")]
    Io(String),
}

impl From<std::io::Error> for NexradeError {
    fn from(e: std::io::Error) -> Self {
        NexradeError::Io(e.to_string())
    }
}

impl From<std::num::ParseIntError> for NexradeError {
    fn from(_: std::num::ParseIntError) -> Self {
        NexradeError::NotInteger
    }
}

impl From<std::num::ParseFloatError> for NexradeError {
    fn from(_: std::num::ParseFloatError) -> Self {
        NexradeError::NotFloat
    }
}
