//! Shared helpers of `tests/functional/basics/runtime/unit_tests` -- the
//! Rust twin of `_runtime_helpers.py` (rule P18c: a sibling helper module
//! with a hand-written twin; its mixin class is copied into the test cases
//! by py2rs, P12e).
//!
//! Provides:
//! - the optional-backend flags (`_HAS_PSUTIL`, `_HAS_REDIS`, ..) the
//!   `skip_unless_*` decorators test,
//! - baseline-versus-current snapshots for threads, listening TCP ports and
//!   child processes, used by every cleanup-asserting test in the suite.
//!
//! Python reads the process through `psutil`; the twin reads `/proc`
//! directly, so `_HAS_PSUTIL` is "this is Linux". All probes are
//! best-effort and never raise; they fall back to safe defaults (empty
//! list / 0) when `/proc` cannot be inspected.

use std::collections::BTreeSet;
use std::sync::LazyLock;

use laila::prelude::*;

/// `_has_module(name)`: a Python module's Rust twin exists -- the probes
/// of this file (`psutil`) and the storage backends laila-tests enables.
pub fn _has_module(name: impl IntoArg) -> bool {
    match name.into_arg().__str__().as_str() {
        "psutil" => cfg!(target_os = "linux"),
        "h5py" | "duckdb" => true,
        // Cloud object stores: credential-gated, never in this suite.
        "boto3" | "google.cloud.storage" | "azure.storage.blob" => false,
        "redis" => true,
        _ => false,
    }
}

/// `_has_binary(name)`: *name* is on `PATH`.
pub fn _has_binary(name: impl IntoArg) -> bool {
    !laila::python::stdlib::shutil.which(name).is_none()
}

pub static _HAS_PSUTIL: bool = cfg!(target_os = "linux");
pub static _HAS_HDF5: bool = true;
pub static _HAS_DUCKDB: bool = true;
pub static _HAS_BOTO3: bool = false;
pub static _HAS_GCS: bool = false;
pub static _HAS_AZURE: bool = false;
pub static _HAS_REDIS: LazyLock<bool> =
    LazyLock::new(|| _has_module("redis") && _has_binary("redis-server"));
pub static _HAS_POSTGRES: LazyLock<bool> = LazyLock::new(|| _has_binary("postgres"));
pub static _HAS_MONGO: LazyLock<bool> = LazyLock::new(|| _has_binary("mongod"));

/// `threading.active_count()`: the live threads of this process that
/// laila knows about (`threading.Thread`s plus the taskforce loop,
/// dispatcher and executor threads) -- not the OS thread count, which the
/// test harness's own waiting test threads would drift.
pub fn _live_thread_count(_args: Args) -> Result<Arg> {
    Ok(Arg::Int(live_thread_count()))
}

fn live_thread_count() -> i64 {
    laila::python::stdlib::threading.active_count()
}

/// The socket inodes this process holds (`/proc/self/fd/* -> socket:[N]`).
fn socket_inodes() -> BTreeSet<u64> {
    let mut out = BTreeSet::new();
    if let Ok(rd) = std::fs::read_dir("/proc/self/fd") {
        for e in rd.flatten() {
            if let Ok(t) = std::fs::read_link(e.path()) {
                let t = t.to_string_lossy();
                if let Some(n) = t.strip_prefix("socket:[").and_then(|s| s.strip_suffix(']')) {
                    if let Ok(n) = n.parse() {
                        out.insert(n);
                    }
                }
            }
        }
    }
    out
}

/// `_listening_ports()`: the sorted TCP ports this process listens on
/// (loopback / wildcard). Empty when `/proc` is unavailable.
pub fn _listening_ports(_args: Args) -> Result<Arg> {
    Ok(listening_ports().into_arg())
}

fn listening_ports() -> Vec<i64> {
    if !_HAS_PSUTIL {
        return Vec::new();
    }
    let inodes = socket_inodes();
    let mut out = Vec::new();
    for table in ["/proc/net/tcp", "/proc/net/tcp6"] {
        let Ok(text) = std::fs::read_to_string(table) else {
            continue;
        };
        for line in text.lines().skip(1) {
            let cols: Vec<&str> = line.split_whitespace().collect();
            if cols.len() < 10 || cols[3] != "0A" {
                continue;
            }
            let Some((ip, port)) = cols[1].rsplit_once(':') else {
                continue;
            };
            let Ok(inode) = cols[9].parse::<u64>() else {
                continue;
            };
            if !inodes.contains(&inode) {
                continue;
            }
            let loopback_or_any = match ip.len() {
                8 => ip == "0100007F" || ip == "00000000",
                32 => {
                    ip == "00000000000000000000000001000000"
                        || ip == "00000000000000000000000000000000"
                }
                _ => false,
            };
            if loopback_or_any {
                if let Ok(p) = i64::from_str_radix(port, 16) {
                    out.push(p);
                }
            }
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

/// `(pid, ppid)` of every process of the system.
fn all_processes() -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir("/proc") else {
        return out;
    };
    for e in rd.flatten() {
        let name = e.file_name();
        let Ok(pid) = name.to_string_lossy().parse::<i64>() else {
            continue;
        };
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
            continue;
        };
        // `pid (comm) state ppid ...` -- comm may contain spaces/parens.
        let Some(rest) = stat.rsplit_once(") ").map(|(_, r)| r) else {
            continue;
        };
        let mut fields = rest.split_whitespace();
        let _state = fields.next();
        if let Some(ppid) = fields.next().and_then(|p| p.parse::<i64>().ok()) {
            out.push((pid, ppid));
        }
    }
    out
}

fn cmdline_of(pid: i64) -> String {
    std::fs::read(format!("/proc/{pid}/cmdline"))
        .map(|b| String::from_utf8_lossy(&b).replace('\0', " "))
        .unwrap_or_default()
}

/// `_child_pids()`: the sorted (recursive) PIDs of every child this
/// process owns, excluding the `multiprocessing.resource_tracker` helper
/// (not laila's to reap).
pub fn _child_pids(_args: Args) -> Result<Arg> {
    Ok(child_pids().into_arg())
}

fn child_pids() -> Vec<i64> {
    if !_HAS_PSUTIL {
        return Vec::new();
    }
    let me = laila::python::stdlib::os.getpid();
    let procs = all_processes();
    let mut out = Vec::new();
    let mut frontier = vec![me];
    let mut seen = BTreeSet::new();
    while let Some(parent) = frontier.pop() {
        for (pid, ppid) in &procs {
            if *ppid == parent && seen.insert(*pid) {
                frontier.push(*pid);
                if !cmdline_of(*pid).contains("resource_tracker") {
                    out.push(*pid);
                }
            }
        }
    }
    out.sort_unstable();
    out
}

/// `_open_fd_count()`: the current process's FD count, `None` if not
/// retrievable.
pub fn _open_fd_count(_args: Args) -> Result<Arg> {
    if !_HAS_PSUTIL {
        return Ok(Arg::None);
    }
    Ok(match std::fs::read_dir("/proc/self/fd") {
        Ok(d) => Arg::Int(d.count() as i64),
        Err(_) => Arg::None,
    })
}

/// `_wait_until(predicate, timeout=5.0, interval=0.05)`: poll *predicate*
/// until it is truthy or *timeout* expires; the final truthiness.
#[pysig("_wait_until(predicate, timeout=5.0, interval=0.05)")]
pub fn _wait_until(args: impl Into<Args>) -> Result<Arg> {
    let timeout = timeout.as_f64().unwrap_or(5.0);
    let interval = interval.as_f64().unwrap_or(0.05);
    let check = || -> bool {
        predicate
            .__call__(args!())
            .map(|v| v.__bool__())
            .unwrap_or(false)
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs_f64(timeout);
    while std::time::Instant::now() < deadline {
        if check() {
            return Ok(Arg::Bool(true));
        }
        std::thread::sleep(std::time::Duration::from_secs_f64(interval));
    }
    Ok(Arg::Bool(check()))
}
