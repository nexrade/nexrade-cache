//! Cross-platform process resource-usage helpers.
//!
//! Used by `INFO memory` (`used_memory_rss`) and `MEMORY STATS`
//! (`allocator.resident` / fragmentation ratio) to report real numbers
//! instead of hardcoded stand-ins. Every platform-specific path returns
//! `0` on failure — callers should treat `0` as "unknown", not "no
//! memory in use".

/// Return the process's current resident set size (RSS) in bytes.
pub fn resident_set_size() -> usize {
    imp::resident_set_size()
}

#[cfg(target_os = "linux")]
mod imp {
    /// `/proc/self/status` has a `VmRSS:  1234 kB` line — pure std, no
    /// extra dependency needed on Linux.
    pub fn resident_set_size() -> usize {
        let status = match std::fs::read_to_string("/proc/self/status") {
            Ok(s) => s,
            Err(_) => return 0,
        };
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                let kb: usize = rest
                    .split_whitespace()
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                return kb.saturating_mul(1024);
            }
        }
        0
    }
}

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
mod imp {
    /// `getrusage(RUSAGE_SELF).ru_maxrss` is already in bytes on macOS/BSD
    /// (unlike Linux, where it's kilobytes — hence the separate
    /// `/proc/self/status` path above).
    pub fn resident_set_size() -> usize {
        unsafe {
            let mut usage: libc::rusage = std::mem::zeroed();
            if libc::getrusage(libc::RUSAGE_SELF, &mut usage) == 0 {
                usage.ru_maxrss.max(0) as usize
            } else {
                0
            }
        }
    }
}

#[cfg(windows)]
mod imp {
    use windows_sys::Win32::System::ProcessStatus::{
        GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS,
    };
    use windows_sys::Win32::System::Threading::GetCurrentProcess;

    pub fn resident_set_size() -> usize {
        unsafe {
            let mut counters: PROCESS_MEMORY_COUNTERS = std::mem::zeroed();
            let handle = GetCurrentProcess();
            let ok = GetProcessMemoryInfo(
                handle,
                &mut counters,
                std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
            );
            if ok != 0 {
                counters.WorkingSetSize
            } else {
                0
            }
        }
    }
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    windows
)))]
mod imp {
    pub fn resident_set_size() -> usize {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resident_set_size_is_nonzero_on_supported_platforms() {
        // We can't assert an exact value (depends on the test runner's
        // memory footprint), but on Linux/macOS/Windows a live process
        // always has *some* resident memory. Platforms without a reader
        // (the catch-all `imp`) legitimately return 0, so this is
        // best-effort rather than a hard assertion everywhere.
        let rss = resident_set_size();
        #[cfg(any(target_os = "linux", target_os = "macos", windows))]
        assert!(rss > 0, "expected nonzero RSS on this platform");
        #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
        let _ = rss;
    }
}
