//! The Python half of the Rust <-> CPython interop suite: spawn
//! `laila-js/tests/interop/py_peer.py` (the same peer the JS port is
//! checked against) in `server` or `client` mode.
//!
//! `$LAILA_PYTHON` (interpreter, default `python3`) and `$LAILA_PYTHONPATH`
//! (default: the directory containing the `laila` package, i.e. the
//! repository's parent) select the CPython `laila`; the suite skips when
//! `import laila, websockets, numpy` fails there.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use laila::prelude::*;
use laila::python::stdlib::json;
use laila::python::{PyDictBuilder, PyObject};

fn repo_root() -> PathBuf {
    // <repo>/laila-rs/tests -> <repo>
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(PathBuf::from)
        .expect("laila-rs/tests lives two levels below the repository root")
}

fn py_peer() -> PathBuf {
    repo_root().join("laila-js/tests/interop/py_peer.py")
}

fn python() -> String {
    std::env::var("LAILA_PYTHON").unwrap_or_else(|_| String::from("python3"))
}

fn pythonpath() -> String {
    let base = std::env::var("LAILA_PYTHONPATH").unwrap_or_else(|_| {
        repo_root()
            .parent()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default()
    });
    match std::env::var("PYTHONPATH") {
        Ok(extra) if !extra.is_empty() => format!("{base}:{extra}"),
        _ => base,
    }
}

fn command(args: &[&str]) -> Command {
    let mut c = Command::new(python());
    c.args(args)
        .env("PYTHONPATH", pythonpath())
        .env("PYTHONUNBUFFERED", "1")
        .stdin(Stdio::null());
    c
}

/// `import laila, websockets, numpy` succeeds under the selected interpreter.
pub static HAVE_PYTHON: LazyLock<bool> = LazyLock::new(|| {
    command(&["-c", "import laila, websockets, numpy"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
});

/// `have_python()` for the `#[skip_unless(..)]` gate of the DSL tests.
pub fn have_python() -> bool {
    *HAVE_PYTHON
}

/// Why the suite is skipped (the JS harness's wording).
pub fn skip_reason() -> String {
    format!(
        "no CPython laila ({}, PYTHONPATH={})",
        python(),
        pythonpath()
    )
}

// ----------------------------------------------------------------------
// server mode
// ----------------------------------------------------------------------

struct ServerState {
    child: Mutex<Option<Child>>,
    info: BTreeMap<String, String>,
}

impl Drop for ServerState {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.lock().unwrap().take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

/// A CPython policy hosted by `py_peer.py server <transport>`: `info` is the
/// `KEY=value` block it printed before `READY` (`PORT`, `SECRET`,
/// `ENTRY_ID`, `ARRAY_ENTRY_ID`, `POLICY_ID`); dropping it (or `kill()`)
/// terminates the process.
#[derive(Clone)]
pub struct PyServer(Arc<ServerState>);

impl PyServer {
    /// Spawn the server and wait (up to 60 s) for `READY`.
    pub fn spawn(transport: &str) -> Result<PyServer> {
        let mut child = command(&[&py_peer().to_string_lossy(), "server", transport])
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| LailaError::runtime_error(format!("cannot spawn {}: {e}", python())))?;
        let stdout = child.stdout.take().expect("piped stdout");
        let (tx, rx) = std::sync::mpsc::channel::<String>();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(|l| l.ok()) {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        let mut info = BTreeMap::new();
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                let _ = child.kill();
                return Err(LailaError::runtime_error(
                    "python peer did not become ready",
                ));
            }
            match rx.recv_timeout(left.min(Duration::from_millis(200))) {
                Ok(line) if line == "READY" => break,
                Ok(line) => {
                    if let Some((k, v)) = line.split_once('=') {
                        info.insert(k.to_string(), v.to_string());
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    if let Ok(Some(status)) = child.try_wait() {
                        return Err(LailaError::runtime_error(format!(
                            "python peer exited early ({status})"
                        )));
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    let status = child.wait().ok();
                    return Err(LailaError::runtime_error(format!(
                        "python peer exited early ({status:?})"
                    )));
                }
            }
        }
        Ok(PyServer(Arc::new(ServerState {
            child: Mutex::new(Some(child)),
            info,
        })))
    }

    pub fn info(&self, key: &str) -> Arg {
        match self.0.info.get(key) {
            Some(v) => Arg::Str(v.as_str().into()),
            None => Arg::None,
        }
    }

    pub fn port(&self) -> i64 {
        self.0
            .info
            .get("PORT")
            .and_then(|p| p.parse().ok())
            .unwrap_or(0)
    }

    /// `ws://127.0.0.1:PORT` or `tcp://127.0.0.1:PORT`.
    pub fn uri(&self, transport: &str) -> String {
        let scheme = if transport == "tcpip" { "ws" } else { "tcp" };
        format!("{scheme}://127.0.0.1:{}", self.port())
    }

    pub fn kill(&self) {
        if let Some(mut c) = self.0.child.lock().unwrap().take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

impl PyObject for PyServer {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "PyServer"
    }
    fn __repr__(&self) -> String {
        format!("<PyServer {:?}>", self.0.info)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<PyServer>()
            .is_some_and(|o| Arc::ptr_eq(&self.0, &o.0))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "PORT" | "SECRET" | "ENTRY_ID" | "ARRAY_ENTRY_ID" | "POLICY_ID" => Ok(self.info(name)),
            other => Err(LailaError::attribute_error(format!(
                "'PyServer' object has no attribute '{other}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "kill" => {
                self.kill();
                Ok(Arg::None)
            }
            "uri" => {
                let t = args
                    .positional
                    .first()
                    .cloned()
                    .unwrap_or(Arg::None)
                    .__str__();
                Ok(Arg::Str(self.uri(&t).into()))
            }
            other => self.__getattr__(other),
        }
    }
}

impl IntoArg for PyServer {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

/// `spawn_py_server(transport)` from the DSL.
pub fn spawn_py_server(args: Args) -> Result<Arg> {
    let t = args
        .positional
        .first()
        .cloned()
        .unwrap_or(Arg::None)
        .__str__();
    PyServer::spawn(&t).map(IntoArg::into_arg)
}

// ----------------------------------------------------------------------
// client mode
// ----------------------------------------------------------------------

/// `run_py_client(uri, secret, remote_policy_id, entry_id)`: run the
/// CPython client against a Rust-hosted policy; the checks it printed,
/// keyed by `check` (`{"peered": {...}, "echo": {...}, ...}`). Fails when
/// the client exits non-zero or never prints `DONE` (120 s).
pub fn run_py_client(args: Args) -> Result<Arg> {
    let s = |i: usize| {
        args.positional
            .get(i)
            .cloned()
            .unwrap_or(Arg::None)
            .__str__()
    };
    let (uri, secret, remote_id, entry_id) = (s(0), s(1), s(2), s(3));
    let mut child = command(&[
        &py_peer().to_string_lossy(),
        "client",
        &uri,
        &secret,
        &remote_id,
        &entry_id,
    ])
    .stdout(Stdio::piped())
    .stderr(Stdio::inherit())
    .spawn()
    .map_err(|e| LailaError::runtime_error(format!("cannot spawn {}: {e}", python())))?;
    let stdout = child.stdout.take().expect("piped stdout");
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(|l| l.ok()) {
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    let checks = PyDictBuilder::new();
    let mut checks = checks.finish();
    let mut done = false;
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            let _ = child.kill();
            return Err(LailaError::runtime_error(format!(
                "python client timed out; checks so far: {}",
                checks.__repr__()
            )));
        }
        match rx.recv_timeout(left.min(Duration::from_millis(200))) {
            Ok(line) if line == "DONE" => {
                done = true;
                break;
            }
            Ok(line) if line.starts_with('{') => {
                let obj = json.loads(Args::from(Arg::Str(line.as_str().into())))?;
                let key = obj.__getitem__("check")?;
                checks.__setitem__(key, obj)?;
            }
            Ok(_) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    let status = child.wait().ok();
    let ok = status.map(|s| s.success()).unwrap_or(false);
    if !(done && ok) {
        return Err(LailaError::runtime_error(format!(
            "python client failed ({status:?}); checks so far: {}",
            checks.__repr__()
        )));
    }
    Ok(checks)
}
