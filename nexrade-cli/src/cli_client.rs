//! nexrade-cli — an interactive Redis-compatible CLI client.
//!
//! # Usage
//!
//! ```sh
//! nexrade-cli                         # Connect to 127.0.0.1:6379
//! nexrade-cli -h 10.0.0.1 -p 6380    # Custom host/port
//! nexrade-cli -a mypassword           # With auth
//! nexrade-cli SET foo bar             # One-shot command
//! nexrade-cli --pipe < commands.txt   # Pipe mode
//! ```

use std::io::{self, BufRead, IsTerminal, Write};

use anyhow::Result;
use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use nexrade_core::resp::{Resp, RespParser};

#[derive(Parser, Debug)]
#[command(
    name = "nexrade-cli",
    version = env!("CARGO_PKG_VERSION"),
    about = "Interactive CLI client for nexrade-cache",
    disable_help_flag = true
)]
struct Cli {
    /// Hostname
    #[arg(short = 'h', long, default_value = "127.0.0.1", env = "NEXRADE_HOST")]
    host: String,

    /// Port
    #[arg(short = 'p', long, default_value = "6379", env = "NEXRADE_PORT")]
    port: u16,

    /// Password
    #[arg(short = 'a', long, env = "NEXRADE_PASS")]
    auth: Option<String>,

    /// Database number
    #[arg(short = 'n', long, default_value = "0")]
    db: usize,

    /// Execute a single command and exit
    #[arg(trailing_var_arg = true)]
    command: Vec<String>,

    /// Pipe mode — read commands from stdin
    #[arg(long)]
    pipe: bool,

    /// Repeat command N times
    #[arg(long, default_value = "1")]
    repeat: u64,

    /// Interval between repeats (ms)
    #[arg(long, default_value = "0")]
    interval: u64,

    /// Print help
    #[arg(long, action = clap::ArgAction::Help)]
    help: Option<bool>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Enable ANSI escape codes on Windows
    #[cfg(windows)]
    {
        let _ = nexrade_cache::windows_ansi::enable_ansi_support();
    }

    let cli = Cli::parse();
    let raw = !io::stdout().is_terminal();

    let addr = format!("{}:{}", cli.host, cli.port);
    let stream = TcpStream::connect(&addr).await?;
    let (mut reader, mut writer) = tokio::io::split(stream);

    // Authenticate if password provided
    if let Some(ref pass) = cli.auth {
        let cmd = encode_command(&["AUTH", pass]);
        writer.write_all(&cmd).await?;
        let resp = read_response(&mut reader).await?;
        if !matches!(resp, Resp::SimpleString(_)) {
            eprintln!("Authentication failed: {}", resp);
            return Ok(());
        }
    }

    // Select database
    if cli.db != 0 {
        let cmd = encode_command(&["SELECT", &cli.db.to_string()]);
        writer.write_all(&cmd).await?;
        let _ = read_response(&mut reader).await?;
    }

    // One-shot command mode
    if !cli.command.is_empty() {
        let args: Vec<&str> = cli.command.iter().map(|s| s.as_str()).collect();
        for _ in 0..cli.repeat {
            let cmd = encode_command(&args);
            writer.write_all(&cmd).await?;
            let resp = read_response(&mut reader).await?;
            println!("{}", format_resp(&resp, 0, raw));

            if cli.interval > 0 {
                tokio::time::sleep(tokio::time::Duration::from_millis(cli.interval)).await;
            }
        }
        return Ok(());
    }

    // Pipe mode — read from stdin
    if cli.pipe {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            let line = line?;
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let args = match parse_args(trimmed) {
                Ok(a) => a,
                Err(e) => {
                    eprintln!("(error) {}", e);
                    continue;
                }
            };
            let args_ref: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
            let cmd = encode_command(&args_ref);
            writer.write_all(&cmd).await?;
            let resp = read_response(&mut reader).await?;
            println!("{}", format_resp(&resp, 0, raw));
        }
        return Ok(());
    }

    // Interactive REPL mode
    println!("\x1b[1;36mConnected to {}:{}\x1b[0m", cli.host, cli.port);
    println!("\x1b[90mType 'quit' or Ctrl+C to exit.\x1b[0m");

    let mut input = String::new();
    loop {
        print!("\x1b[1;34m{}:{}\x1b[0m> ", cli.host, cli.port);
        io::stdout().flush()?;

        input.clear();
        if io::stdin().read_line(&mut input)? == 0 {
            break; // EOF
        }

        let trimmed = input.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.eq_ignore_ascii_case("quit") || trimmed.eq_ignore_ascii_case("exit") {
            break;
        }

        let args = match parse_args(trimmed) {
            Ok(a) => a,
            Err(e) => {
                eprintln!("(error) {}", e);
                continue;
            }
        };
        if args.is_empty() {
            continue;
        }

        let args_ref: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let cmd = encode_command(&args_ref);
        if let Err(e) = writer.write_all(&cmd).await {
            eprintln!("Write error: {}", e);
            break;
        }

        match read_response(&mut reader).await {
            Ok(resp) => println!("{}", format_resp(&resp, 0, raw)),
            Err(e) => {
                eprintln!("Error: {}", e);
                break;
            }
        }
    }

    Ok(())
}

/// Shell-style argument tokenizer — respects single and double quoted strings.
///
/// Examples:
///   `SET key "hello world"`  → ["SET", "key", "hello world"]
///   `SET key 'hello world'`  → ["SET", "key", "hello world"]
///   `SET key hello`          → ["SET", "key", "hello"]
fn parse_args(input: &str) -> Result<Vec<String>> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            // Skip unquoted whitespace between tokens
            ' ' | '\t' => {
                if !current.is_empty() {
                    args.push(current.clone());
                    current.clear();
                }
            }
            // Double-quoted string: collect until closing `"`, handle `\"` escape
            '"' => {
                loop {
                    match chars.next() {
                        None => anyhow::bail!("unterminated double quote"),
                        Some('\\') => match chars.next() {
                            Some(c) => current.push(c),
                            None => anyhow::bail!("unterminated escape in double quote"),
                        },
                        Some('"') => break,
                        Some(c) => current.push(c),
                    }
                }
                // Keep going — don't push yet; allows `"foo""bar"` → `foobar`
            }
            // Single-quoted string: no escape processing (like POSIX sh)
            '\'' => loop {
                match chars.next() {
                    None => anyhow::bail!("unterminated single quote"),
                    Some('\'') => break,
                    Some(c) => current.push(c),
                }
            },
            c => current.push(c),
        }
    }

    if !current.is_empty() {
        args.push(current);
    }

    Ok(args)
}

/// Encode a command as RESP array.
fn encode_command(args: &[&str]) -> Vec<u8> {
    let cmd = Resp::array(args.iter().map(|s| Resp::bulk_str(*s)).collect());
    cmd.serialize().to_vec()
}

/// Read a single RESP response from the reader.
async fn read_response(reader: &mut (impl AsyncReadExt + Unpin)) -> Result<Resp> {
    let mut buf = vec![0u8; 4096];
    let mut parser = RespParser::new();

    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            anyhow::bail!("connection closed");
        }
        parser.feed(&buf[..n]);
        if let Some(resp) = parser.parse_one()? {
            return Ok(resp);
        }
    }
}

/// Format a RESP value for display.
/// `raw` mirrors redis-cli's raw mode (stdout not a TTY): no decorators around values.
fn format_resp(resp: &Resp, depth: usize, raw: bool) -> String {
    let indent = if raw {
        String::new()
    } else {
        "  ".repeat(depth)
    };
    match resp {
        Resp::SimpleString(s) => {
            if raw {
                s.clone()
            } else {
                format!("\x1b[32m{}\x1b[0m", s) // Green for simple strings
            }
        }
        Resp::Error(e) => {
            if raw {
                e.clone()
            } else {
                format!("{}\x1b[31m(error)\x1b[0m {}", indent, e) // Red for errors
            }
        }
        Resp::Integer(n) => {
            if raw {
                format!("{}", n)
            } else {
                format!("{}\x1b[36m(integer)\x1b[0m {}", indent, n) // Cyan for integers
            }
        }
        Resp::BulkString(None) => {
            if raw {
                String::new()
            } else {
                format!("{}\x1b[90m(nil)\x1b[0m", indent) // Gray for nil
            }
        }
        Resp::BulkString(Some(b)) => {
            if raw {
                String::from_utf8_lossy(b).into_owned()
            } else {
                format!("\x1b[33m\"{}\"\x1b[0m", String::from_utf8_lossy(b)) // Yellow for bulk strings
            }
        }
        Resp::Array(None) => {
            if raw {
                String::new()
            } else {
                format!("{}\x1b[90m(nil)\x1b[0m", indent)
            }
        }
        Resp::Array(Some(items)) => {
            if items.is_empty() {
                if raw {
                    return String::new();
                }
                return format!("{}\x1b[90m(empty array)\x1b[0m", indent);
            }
            if raw {
                items
                    .iter()
                    .map(|item| format_resp(item, 0, true))
                    .collect::<Vec<_>>()
                    .join("\n")
            } else {
                items
                    .iter()
                    .enumerate()
                    .map(|(i, item)| {
                        format!(
                            "{}\x1b[90m{})\x1b[0m {}",
                            indent,
                            i + 1,
                            format_resp(item, depth + 1, false).trim_start()
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        }
        // RESP3 types
        Resp::Null => {
            if raw {
                String::new()
            } else {
                format!("{}\x1b[90m(nil)\x1b[0m", indent)
            }
        }
        Resp::Bool(b) => {
            if raw {
                format!("{}", b)
            } else {
                format!("{}\x1b[35m(bool)\x1b[0m {}", indent, b) // Magenta for bools
            }
        }
        Resp::Double(f) => {
            if raw {
                format!("{}", f)
            } else {
                format!("{}\x1b[36m(double)\x1b[0m {}", indent, f) // Cyan for doubles
            }
        }
        Resp::Map(pairs) => pairs
            .iter()
            .enumerate()
            .map(|(i, (k, v))| {
                format!(
                    "{}\x1b[90m{})\x1b[0m {} \x1b[90m=>\x1b[0m {}",
                    indent,
                    i + 1,
                    format_resp(k, depth + 1, raw).trim_start(),
                    format_resp(v, depth + 1, raw).trim_start()
                )
            })
            .collect::<Vec<_>>()
            .join("\n"),
        Resp::Set(items) | Resp::Push(items) => items
            .iter()
            .enumerate()
            .map(|(i, item)| {
                format!(
                    "{}\x1b[90m{})\x1b[0m {}",
                    indent,
                    i + 1,
                    format_resp(item, depth + 1, raw).trim_start()
                )
            })
            .collect::<Vec<_>>()
            .join("\n"),
        // Raw is a server-internal type; the CLI never receives it.
        Resp::Raw(b) => String::from_utf8_lossy(b).into_owned(),
    }
}
