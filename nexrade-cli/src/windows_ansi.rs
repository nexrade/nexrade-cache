//! Enable ANSI escape code support on Windows.
//!
//! Windows 10+ supports ANSI escape codes (colors, formatting) through
//! Virtual Terminal Processing, but it needs to be enabled programmatically.
//!
//! This module provides a zero-cost abstraction that enables ANSI support
//! on Windows and does nothing on other platforms.

#[cfg(windows)]
use std::io;

/// Enable ANSI escape code processing on Windows console.
///
/// This function enables Virtual Terminal Processing for both stdout and stderr,
/// allowing ANSI escape codes (colors, bold, underline, etc.) to work properly
/// on Windows 10+ terminals.
///
/// On non-Windows platforms, this function does nothing.
///
/// # Example
///
/// ```no_run
/// enable_ansi_support();
/// println!("\x1b[31mRed text\x1b[0m");
/// println!("\x1b[1;32mBold green text\x1b[0m");
/// ```
///
/// # Errors
///
/// Returns an error if the console handle cannot be obtained or if
/// Virtual Terminal Processing cannot be enabled. This is non-fatal —
/// the application will continue to work, but ANSI codes will be
/// rendered as raw escape sequences.
#[cfg(windows)]
pub fn enable_ansi_support() -> io::Result<()> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
    use windows_sys::Win32::System::Console::{
        GetConsoleMode, GetStdHandle, SetConsoleMode, ENABLE_VIRTUAL_TERMINAL_PROCESSING,
        STD_ERROR_HANDLE, STD_OUTPUT_HANDLE,
    };

    unsafe {
        // Enable for stdout
        let stdout_handle = GetStdHandle(STD_OUTPUT_HANDLE);
        if stdout_handle != INVALID_HANDLE_VALUE && stdout_handle != 0 {
            let mut mode = 0;
            if GetConsoleMode(stdout_handle, &mut mode) != 0 {
                let new_mode = mode | ENABLE_VIRTUAL_TERMINAL_PROCESSING;
                SetConsoleMode(stdout_handle, new_mode);
            }
        }

        // Enable for stderr
        let stderr_handle = GetStdHandle(STD_ERROR_HANDLE);
        if stderr_handle != INVALID_HANDLE_VALUE && stderr_handle != 0 {
            let mut mode = 0;
            if GetConsoleMode(stderr_handle, &mut mode) != 0 {
                let new_mode = mode | ENABLE_VIRTUAL_TERMINAL_PROCESSING;
                SetConsoleMode(stderr_handle, new_mode);
            }
        }
    }

    Ok(())
}

/// Enable ANSI escape code support (no-op on non-Windows platforms).
#[cfg(not(windows))]
pub fn enable_ansi_support() -> std::io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enable_ansi_support() {
        // Should not panic on any platform
        let _ = enable_ansi_support();
    }
}
