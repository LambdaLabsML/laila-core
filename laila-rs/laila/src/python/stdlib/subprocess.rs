//! `import subprocess` / `import atexit` -- the slice the managed-server
//! pools use (`RedisPool`, `PostgresPool`, `MongoPool` spawn their own
//! `redis-server` / `postgres` / `mongod` and shut it down at exit).

use alloc::string::{String, ToString};
use alloc::vec::Vec;
use std::io::Read;
use std::process::{Child, ChildStderr, ChildStdout, Command, Stdio};

use parking_lot::Mutex;

use crate::python::args::Args;
use crate::python::errors::ExcKind;
use crate::python::object::{Arg, IntoArg, PyObject};
use crate::python::{LailaError, Result};

/// `subprocess.Popen(cmd, stdout=PIPE, stderr=PIPE, text=True)`.
pub struct Popen {
    child: Mutex<Option<Child>>,
    returncode: Mutex<Option<i32>>,
    stdout: Mutex<Option<ChildStdout>>,
    stderr: Mutex<Option<ChildStderr>>,
    pid: u32,
}

impl core::fmt::Debug for Popen {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "<Popen: returncode: {:?} args: ...>", self.poll())
    }
}

fn exit_code(status: std::process::ExitStatus) -> i32 {
    use std::os::unix::process::ExitStatusExt;
    status
        .code()
        .or_else(|| status.signal().map(|s| -s))
        .unwrap_or(-1)
}

impl Popen {
    /// Spawn *cmd* with both output streams piped. A missing executable
    /// is `FileNotFoundError: [Errno 2] No such file or directory: '<cmd[0]>'`.
    pub fn new(cmd: &[String]) -> Result<Popen> {
        let (program, rest) = cmd
            .split_first()
            .ok_or_else(|| LailaError::new(ExcKind::IndexError, "list index out of range"))?;
        let mut child = Command::new(program)
            .args(rest)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| {
                let kind = match e.kind() {
                    std::io::ErrorKind::NotFound => ExcKind::FileNotFoundError,
                    std::io::ErrorKind::PermissionDenied => ExcKind::PermissionError,
                    _ => ExcKind::OSError,
                };
                LailaError::new(
                    kind,
                    alloc::format!(
                        "[Errno {}] {}: '{program}'",
                        e.raw_os_error().unwrap_or(0),
                        match e.kind() {
                            std::io::ErrorKind::NotFound => "No such file or directory".to_string(),
                            std::io::ErrorKind::PermissionDenied => "Permission denied".to_string(),
                            _ => e.to_string(),
                        }
                    ),
                )
            })?;
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let pid = child.id();
        Ok(Popen {
            child: Mutex::new(Some(child)),
            returncode: Mutex::new(None),
            stdout: Mutex::new(stdout),
            stderr: Mutex::new(stderr),
            pid,
        })
    }

    /// `proc.pid`
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// `proc.poll()`: the exit code once the process has exited, else `None`.
    pub fn poll(&self) -> Option<i32> {
        if let Some(code) = *self.returncode.lock() {
            return Some(code);
        }
        let mut child = self.child.lock();
        let status = child.as_mut().and_then(|c| c.try_wait().ok().flatten());
        if let Some(status) = status {
            let code = exit_code(status);
            *self.returncode.lock() = Some(code);
            return Some(code);
        }
        None
    }

    /// `proc.wait(timeout=..)`: `subprocess.TimeoutExpired` (a
    /// `TimeoutError` here) when the process outlives *timeout* seconds.
    pub fn wait(&self, timeout: Option<f64>) -> Result<i32> {
        let deadline =
            timeout.map(|t| std::time::Instant::now() + std::time::Duration::from_secs_f64(t));
        loop {
            if let Some(code) = self.poll() {
                return Ok(code);
            }
            if let Some(d) = deadline {
                if std::time::Instant::now() >= d {
                    return Err(LailaError::new(
                        ExcKind::TimeoutError,
                        alloc::format!(
                            "subprocess.TimeoutExpired: Command timed out after {} seconds",
                            timeout.unwrap_or(0.0)
                        ),
                    ));
                }
            }
            std::thread::sleep(core::time::Duration::from_millis(10));
        }
    }

    /// `proc.terminate()` -- `SIGTERM`.
    pub fn terminate(&self) {
        if self.poll().is_none() {
            // SAFETY: plain syscall on our own child's pid.
            unsafe {
                libc::kill(self.pid as libc::pid_t, libc::SIGTERM);
            }
        }
    }

    /// `proc.kill()` -- `SIGKILL`.
    pub fn kill(&self) {
        if self.poll().is_none() {
            if let Some(c) = self.child.lock().as_mut() {
                let _ = c.kill();
            }
        }
    }

    /// `proc.stdout.read()` -- the captured standard output (read to EOF;
    /// empty while the process is still running, so a wait never blocks
    /// on a live pipe).
    pub fn read_stdout(&self) -> String {
        if self.poll().is_none() {
            return String::new();
        }
        let mut s = String::new();
        if let Some(mut out) = self.stdout.lock().take() {
            let _ = out.read_to_string(&mut s);
        }
        s
    }

    /// `proc.stderr.read()`
    pub fn read_stderr(&self) -> String {
        if self.poll().is_none() {
            return String::new();
        }
        let mut s = String::new();
        if let Some(mut err) = self.stderr.lock().take() {
            let _ = err.read_to_string(&mut s);
        }
        s
    }
}

impl Drop for Popen {
    fn drop(&mut self) {
        // Reap an exited child; never kill a live one (Python's `Popen`
        // does not either).
        if let Some(mut c) = self.child.lock().take() {
            let _ = c.try_wait();
        }
    }
}

/// `subprocess.CompletedProcess`
#[derive(Debug, Clone)]
pub struct CompletedProcess {
    pub returncode: i32,
    pub stdout: String,
    pub stderr: String,
}

/// `subprocess.run(command, capture_output=True, text=True)`:
/// `FileNotFoundError` when the executable is missing; a non-zero exit is
/// reported in `returncode` (callers raise their `CalledProcessError`
/// from the captured output, as `check=True` would).
pub fn run(command: &[String]) -> Result<CompletedProcess> {
    let (program, rest) = command
        .split_first()
        .ok_or_else(|| LailaError::new(ExcKind::IndexError, "list index out of range"))?;
    let out = Command::new(program)
        .args(rest)
        .stdin(Stdio::null())
        .output()
        .map_err(|e| {
            let kind = match e.kind() {
                std::io::ErrorKind::NotFound => ExcKind::FileNotFoundError,
                _ => ExcKind::OSError,
            };
            LailaError::new(
                kind,
                alloc::format!(
                    "[Errno {}] No such file or directory: '{program}'",
                    e.raw_os_error().unwrap_or(2)
                ),
            )
        })?;
    Ok(CompletedProcess {
        returncode: exit_code(out.status),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    })
}

/// `import atexit`
pub mod atexit {
    use super::*;

    type Handler = Box<dyn FnOnce() + Send>;
    static HANDLERS: Mutex<Vec<Handler>> = Mutex::new(Vec::new());
    static INSTALLED: std::sync::Once = std::sync::Once::new();

    extern "C" fn run_handlers() {
        // LIFO, like Python.
        let handlers: Vec<Handler> = core::mem::take(&mut *HANDLERS.lock());
        for h in handlers.into_iter().rev() {
            h();
        }
    }

    /// `atexit.register(func)`: *func* runs at interpreter exit (normal
    /// return from `main` or `sys.exit` / `std::process::exit`), after
    /// every handler registered later.
    pub fn register(f: impl FnOnce() + Send + 'static) {
        INSTALLED.call_once(|| {
            // SAFETY: a plain C-ABI function with no captures.
            unsafe {
                libc::atexit(run_handlers);
            }
        });
        HANDLERS.lock().push(Box::new(f));
    }
}

/// `import subprocess` as a value: `subprocess.run(..)`, `subprocess.PIPE`.
#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)]
pub struct subprocess_mod;
#[allow(non_upper_case_globals)]
pub static subprocess: subprocess_mod = subprocess_mod;
impl subprocess_mod {
    /// `subprocess.run(args, capture_output=True, text=True, timeout=None,
    /// check=False)`.
    ///
    /// `[sys.executable, "-c", script]` where *script* is a
    /// [`ChildScript`] (a Python-source template the override mapped to a
    /// Rust function) re-executes the current test binary on that
    /// function's child entry, so a Python test that spawns an interpreter
    /// on inline source spawns the same program in Rust.
    pub fn run(&self, args: impl Into<Args>) -> Result<CompletedProcess> {
        let a: Args = args.into();
        let argv = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("args"));
        let argv: Vec<Arg> = argv.__iter__()?;
        let timeout = match a.get("timeout") {
            Arg::None => None,
            t => t.as_number(),
        };
        let check = a.get("check").__bool__();
        let capture = a.get("capture_output").__bool__();
        let child_script = argv
            .get(2)
            .and_then(|v| match v {
                Arg::Object(o) => o.as_any().downcast_ref::<ChildScript>().cloned(),
                _ => None,
            })
            .filter(|_| argv.get(1).map(|v| v.__str__()) == Some(String::from("-c")));
        let mut command = match child_script {
            Some(script) => {
                let mut c = Command::new(argv[0].__str__());
                c.arg(script.entry_test_name())
                    .arg("--nocapture")
                    .arg("--test-threads=1")
                    .env(CHILD_ENV_NAME, &script.name)
                    .env(CHILD_ENV_KWARGS, script.kwargs_json()?);
                c
            }
            None => {
                let mut it = argv.iter().map(|v| v.__str__());
                let program = it.next().ok_or_else(|| {
                    LailaError::new(ExcKind::IndexError, "list index out of range")
                })?;
                let mut c = Command::new(program);
                c.args(it);
                c
            }
        };
        let program = argv.first().map(|v| v.__str__()).unwrap_or_default();
        command.stdin(Stdio::null());
        if capture {
            command.stdout(Stdio::piped()).stderr(Stdio::piped());
        }
        let child = command.spawn().map_err(|e| {
            let kind = match e.kind() {
                std::io::ErrorKind::NotFound => ExcKind::FileNotFoundError,
                _ => ExcKind::OSError,
            };
            LailaError::new(
                kind,
                alloc::format!(
                    "[Errno {}] No such file or directory: '{program}'",
                    e.raw_os_error().unwrap_or(2)
                ),
            )
        })?;
        let deadline =
            timeout.map(|t| std::time::Instant::now() + std::time::Duration::from_secs_f64(t));
        let mut child = child;
        // Drain the pipes on helper threads so a chatty child cannot block.
        let out_reader = child.stdout.take().map(|mut s| {
            std::thread::spawn(move || {
                let mut buf = Vec::new();
                let _ = s.read_to_end(&mut buf);
                buf
            })
        });
        let err_reader = child.stderr.take().map(|mut s| {
            std::thread::spawn(move || {
                let mut buf = Vec::new();
                let _ = s.read_to_end(&mut buf);
                buf
            })
        });
        let status = loop {
            if let Some(st) = child.try_wait().ok().flatten() {
                break st;
            }
            if let Some(d) = deadline {
                if std::time::Instant::now() >= d {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(LailaError::new(
                        ExcKind::TimeoutError,
                        alloc::format!(
                            "subprocess.TimeoutExpired: Command '{}' timed out after {} seconds",
                            argv_repr(&argv),
                            timeout.unwrap_or(0.0)
                        ),
                    ));
                }
            }
            std::thread::sleep(core::time::Duration::from_millis(10));
        };
        let stdout = out_reader
            .map(|h| h.join().unwrap_or_default())
            .unwrap_or_default();
        let stderr = err_reader
            .map(|h| h.join().unwrap_or_default())
            .unwrap_or_default();
        let done = CompletedProcess {
            returncode: exit_code(status),
            stdout: String::from_utf8_lossy(&stdout).into_owned(),
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
        };
        if check && done.returncode != 0 {
            return Err(LailaError::new(
                ExcKind::Exception,
                alloc::format!(
                    "subprocess.CalledProcessError: Command '{}' returned non-zero exit status {}.",
                    argv_repr(&argv),
                    done.returncode
                ),
            ));
        }
        Ok(done)
    }
    pub fn PIPE(&self) -> i64 {
        -1
    }
    pub fn DEVNULL(&self) -> i64 {
        -3
    }
}

fn argv_repr(argv: &[Arg]) -> String {
    Arg::List(alloc::sync::Arc::new(crate::python::sync::RwLock::new(
        argv.to_vec(),
    )))
    .__repr__()
}

impl CompletedProcess {
    pub fn returncode(&self) -> i64 {
        self.returncode as i64
    }
    pub fn stdout(&self) -> Arg {
        Arg::Str(self.stdout.as_str().into())
    }
    pub fn stderr(&self) -> Arg {
        Arg::Str(self.stderr.as_str().into())
    }
    /// `res.check_returncode()`
    pub fn check_returncode(&self) -> Result<()> {
        if self.returncode != 0 {
            return Err(LailaError::new(
                ExcKind::Exception,
                alloc::format!(
                    "subprocess.CalledProcessError: Command returned non-zero exit status {}.",
                    self.returncode
                ),
            ));
        }
        Ok(())
    }
}
impl PyObject for CompletedProcess {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "CompletedProcess"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "CompletedProcess(returncode={}, stdout={}, stderr={})",
            self.returncode,
            Arg::Str(self.stdout.as_str().into()).__repr__(),
            Arg::Str(self.stderr.as_str().into()).__repr__()
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        core::ptr::eq(
            self as *const _ as *const (),
            other as *const _ as *const (),
        )
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(self as *const _ as *const () as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "returncode" => Ok(Arg::Int(self.returncode())),
            "stdout" => Ok(self.stdout()),
            "stderr" => Ok(self.stderr()),
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'CompletedProcess' object has no attribute '{name}'"
            ))),
        }
    }
}
impl IntoArg for CompletedProcess {
    fn into_arg(self) -> Arg {
        Arg::Object(alloc::sync::Arc::new(self))
    }
}

/// The environment the parent sets for a child-script run.
pub const CHILD_ENV_NAME: &str = "LAILA_CHILD_SCRIPT";
pub const CHILD_ENV_KWARGS: &str = "LAILA_CHILD_KWARGS";

/// A Python-source template a test formats and hands to
/// `subprocess.run([sys.executable, "-c", ..])`. Rust cannot run Python
/// source, so the override maps the module constant to a Rust function
/// (`child_scripts:`, P32): the template keeps its text (`str(script)`),
/// `.format(**kw)` binds the keyword arguments, and `subprocess.run`
/// re-executes the current test binary on the function's child entry
/// (`__laila_child_<CONSTANT>`) with the bound arguments in the
/// environment.
#[derive(Clone, Debug)]
pub struct ChildScript {
    pub name: String,
    pub text: String,
    pub kwargs: Vec<(String, Arg)>,
}
impl ChildScript {
    /// The module constant's value: a `str`-like object.
    pub fn value(name: &str, text: &str) -> Arg {
        Arg::Object(alloc::sync::Arc::new(ChildScript {
            name: name.to_string(),
            text: text.to_string(),
            kwargs: Vec::new(),
        }))
    }
    /// The libtest filter that selects the child entry of this script.
    pub fn entry_test_name(&self) -> String {
        alloc::format!("__laila_child_{}", self.name)
    }
    fn kwargs_json(&self) -> Result<String> {
        let mut map = crate::python::object::PyMap::default();
        for (k, v) in &self.kwargs {
            map.insert(Arg::Str(k.as_str().into()), v.clone());
        }
        let dict = Arg::Dict(alloc::sync::Arc::new(crate::python::sync::RwLock::new(map)));
        crate::python::stdlib::json.dumps(dict)
    }
}
impl PyObject for ChildScript {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "str"
    }
    fn __repr__(&self) -> String {
        Arg::Str(self.text.as_str().into()).__repr__()
    }
    fn __str__(&self) -> String {
        self.text.clone()
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<ChildScript>()
            .is_some_and(|o| o.name == self.name && o.kwargs.len() == self.kwargs.len())
    }
    fn __hash__(&self) -> Result<i64> {
        Arg::Str(self.text.as_str().into()).__hash__()
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "format" => {
                let mut bound = self.clone();
                bound.kwargs.extend(args.keywords.iter().cloned());
                for (i, v) in args.positional.iter().enumerate() {
                    bound.kwargs.push((i.to_string(), v.clone()));
                }
                Ok(Arg::Object(alloc::sync::Arc::new(bound)))
            }
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'str' object has no attribute '{name}'"
            ))),
        }
    }
}

/// The child half of a [`ChildScript`]: the body of the generated
/// `#[test] fn __laila_child_<CONSTANT>()`. Outside a child run (the
/// environment is not set, or names another script) it is a no-op; in
/// one it calls *body* with the bound keyword arguments and exits with
/// its status -- the Python child's `os._exit(..)` / unhandled error.
pub fn child_main(name: &str, body: fn(Args) -> Result<()>) {
    if std::env::var(CHILD_ENV_NAME).ok().as_deref() != Some(name) {
        return;
    }
    let kwargs = std::env::var(CHILD_ENV_KWARGS).unwrap_or_else(|_| String::from("{}"));
    let run = || -> Result<()> {
        let dict = crate::python::stdlib::json.loads(kwargs.as_str())?;
        let mut args = Args::new();
        for item in dict.items()?.__iter__()? {
            let pair = item.__iter__()?;
            args.push_keyword(&pair[0].__str__(), pair[1].clone());
        }
        body(args)
    };
    match run() {
        Ok(()) => std::process::exit(0),
        Err(e) => {
            eprintln!("Traceback (most recent call last):\n{e}");
            std::process::exit(1)
        }
    }
}
