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

/// Process CPU time (user, system) in seconds.
/// Returns `(0.0, 0.0)` when the platform reader is unavailable.
pub fn process_cpu_seconds() -> (f64, f64) {
    imp::process_cpu_seconds()
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

    /// `/proc/self/stat` fields 14/15 are utime/stime in clock ticks.
    pub fn process_cpu_seconds() -> (f64, f64) {
        let stat = match std::fs::read_to_string("/proc/self/stat") {
            Ok(s) => s,
            Err(_) => return (0.0, 0.0),
        };
        // comm may contain spaces/parens — split after last ')' then fields.
        let after_comm = match stat.rfind(')') {
            Some(i) => &stat[i + 1..],
            None => return (0.0, 0.0),
        };
        let fields: Vec<&str> = after_comm.split_whitespace().collect();
        // After comm: state(0) ppid(1) ... utime is index 11, stime 12
        // (man proc: field 14 utime, 15 stime — 1-based from start of line;
        // after dropping pid+comm we have 0-based offset 11/12).
        if fields.len() < 13 {
            return (0.0, 0.0);
        }
        let utime: u64 = fields[11].parse().unwrap_or(0);
        let stime: u64 = fields[12].parse().unwrap_or(0);
        // CLK_TCK is commonly 100 on Linux.
        let ticks = 100.0_f64;
        (utime as f64 / ticks, stime as f64 / ticks)
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

    pub fn process_cpu_seconds() -> (f64, f64) {
        unsafe {
            let mut usage: libc::rusage = std::mem::zeroed();
            if libc::getrusage(libc::RUSAGE_SELF, &mut usage) != 0 {
                return (0.0, 0.0);
            }
            let user = timeval_to_secs(usage.ru_utime);
            let sys = timeval_to_secs(usage.ru_stime);
            (user, sys)
        }
    }

    unsafe fn timeval_to_secs(tv: libc::timeval) -> f64 {
        tv.tv_sec as f64 + (tv.tv_usec as f64) / 1_000_000.0
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

    pub fn process_cpu_seconds() -> (f64, f64) {
        // Leave as zero on Windows without extra winapi surface; INFO still
        // exposes the fields.
        (0.0, 0.0)
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
    pub fn process_cpu_seconds() -> (f64, f64) {
        (0.0, 0.0)
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
