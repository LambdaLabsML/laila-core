//! `import os`, `os.path`, `sys`, `shutil`, `tempfile` -- the surface the
//! laila test-suite uses, on `std` (the embedded core has no filesystem).

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::python::args::Args;
use crate::python::errors::{ExcKind, LailaError, Result};
use crate::python::object::{Arg, IntoArg, PyArg, PyObject};
use crate::python::pysig;

/// `os.fspath(a)`: the path text of a `str` / `bytes` / `pathlib.Path`.
pub(crate) fn path_arg(a: &Arg) -> Result<String> {
    match a {
        Arg::Str(s) => Ok(s.to_string()),
        Arg::Bytes(b) => Ok(String::from_utf8_lossy(b).into_owned()),
        Arg::Object(o) if o.as_any().is::<super::pathlib::Path>() => Ok(o.__str__()),
        Arg::Object(o) => Ok(o.__repr__()),
        other => Err(LailaError::type_error(alloc::format!(
            "expected str, bytes or os.PathLike object, not {}",
            other.type_name()
        ))),
    }
}

fn first_path(a: &Args, what: &str) -> Result<String> {
    a.positional
        .first()
        .ok_or_else(|| {
            LailaError::type_error(alloc::format!(
                "{what}() missing required argument 'path' (pos 1)"
            ))
        })
        .and_then(path_arg)
}

fn io_error(e: &std::io::Error, path: &str) -> LailaError {
    let kind = match e.kind() {
        std::io::ErrorKind::NotFound => ExcKind::FileNotFoundError,
        std::io::ErrorKind::PermissionDenied => ExcKind::PermissionError,
        _ => ExcKind::OSError,
    };
    let errno = e.raw_os_error().unwrap_or(0);
    let text = match e.kind() {
        std::io::ErrorKind::NotFound => "No such file or directory".to_string(),
        std::io::ErrorKind::PermissionDenied => "Permission denied".to_string(),
        _ => e.to_string(),
    };
    LailaError::new(kind, alloc::format!("[Errno {errno}] {text}: '{path}'"))
}

/// `os.path`
#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)]
pub struct os_path_mod;
impl os_path_mod {
    /// `os.path.join(a, *p)`
    pub fn join(&self, args: impl Into<Args>) -> Result<String> {
        let a: Args = args.into();
        let mut out = String::new();
        for p in &a.positional {
            let p = path_arg(p)?;
            if p.starts_with('/') {
                out = p;
            } else if out.is_empty() || out.ends_with('/') {
                out.push_str(&p);
            } else {
                out.push('/');
                out.push_str(&p);
            }
        }
        Ok(out)
    }
    /// `os.path.exists(path)` (honours `patch("os.path.exists", ..)`).
    pub fn exists(&self, args: impl Into<Args>) -> Result<bool> {
        let a: Args = args.into();
        if let Some(r) = crate::python::mock::call_patched_module("os.path", "exists", a.clone()) {
            return r.map(|v| v.__bool__());
        }
        Ok(std::path::Path::new(&first_path(&a, "exists")?).exists())
    }
    pub fn isdir(&self, p: impl IntoArg) -> Result<bool> {
        Ok(std::path::Path::new(&path_arg(&p.into_arg())?).is_dir())
    }
    pub fn isfile(&self, p: impl IntoArg) -> Result<bool> {
        Ok(std::path::Path::new(&path_arg(&p.into_arg())?).is_file())
    }
    pub fn basename(&self, p: impl IntoArg) -> Result<String> {
        let p = path_arg(&p.into_arg())?;
        Ok(p.rsplit('/').next().unwrap_or("").to_string())
    }
    pub fn dirname(&self, p: impl IntoArg) -> Result<String> {
        let p = path_arg(&p.into_arg())?;
        Ok(match p.rfind('/') {
            Some(0) => "/".into(),
            Some(i) => p[..i].to_string(),
            None => String::new(),
        })
    }
    pub fn abspath(&self, p: impl IntoArg) -> Result<String> {
        let p = path_arg(&p.into_arg())?;
        let path = std::path::Path::new(&p);
        let abs = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|e| io_error(&e, &p))?
                .join(path)
        };
        Ok(normalize(&abs.to_string_lossy()))
    }
    pub fn expanduser(&self, p: impl IntoArg) -> Result<String> {
        let p = path_arg(&p.into_arg())?;
        if let Some(rest) = p.strip_prefix('~') {
            if let Ok(home) = std::env::var("HOME") {
                return Ok(alloc::format!("{home}{rest}"));
            }
        }
        Ok(p)
    }
    pub fn getsize(&self, p: impl IntoArg) -> Result<i64> {
        let p = path_arg(&p.into_arg())?;
        std::fs::metadata(&p)
            .map(|m| m.len() as i64)
            .map_err(|e| io_error(&e, &p))
    }
    /// `os.path.realpath(path)`: symlinks resolved (a path that does not
    /// exist is normalised only).
    pub fn realpath(&self, p: impl IntoArg) -> Result<String> {
        let p = path_arg(&p.into_arg())?;
        match std::fs::canonicalize(&p) {
            Ok(c) => Ok(c.to_string_lossy().into_owned()),
            Err(_) => self.abspath(p),
        }
    }
    /// `os.path.ismount(path)`: *path* is a mount point (its device differs
    /// from its parent's, or it is the root of a bind mount).
    pub fn ismount(&self, p: impl IntoArg) -> Result<bool> {
        use std::os::unix::fs::MetadataExt;
        let p = path_arg(&p.into_arg())?;
        let Ok(meta) = std::fs::symlink_metadata(&p) else {
            return Ok(false);
        };
        if meta.file_type().is_symlink() {
            return Ok(false);
        }
        let parent = std::path::Path::new(&p).join("..");
        let Ok(parent_meta) = std::fs::metadata(&parent) else {
            return Ok(false);
        };
        if meta.dev() != parent_meta.dev() {
            return Ok(true);
        }
        Ok(meta.ino() == parent_meta.ino())
    }
    pub fn splitext(&self, p: impl IntoArg) -> Result<Arg> {
        let p = path_arg(&p.into_arg())?;
        let base = p.rsplit('/').next().unwrap_or("");
        let (root, ext) = match base.rfind('.') {
            Some(i) if i > 0 => (
                p[..p.len() - base.len() + i].to_string(),
                base[i..].to_string(),
            ),
            _ => (p.clone(), String::new()),
        };
        Ok((root, ext).into_arg())
    }
}

fn normalize(p: &str) -> String {
    let mut parts: Vec<&str> = Vec::new();
    for seg in p.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            s => parts.push(s),
        }
    }
    let joined = parts.join("/");
    if p.starts_with('/') {
        alloc::format!("/{joined}")
    } else {
        joined
    }
}

/// `os.environ` (a live view of the process environment).
#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)]
pub struct os_environ;
impl os_environ {
    pub fn get(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let key = a
            .positional
            .first()
            .map(|k| k.__str__())
            .unwrap_or_default();
        let default = a.positional.get(1).cloned().unwrap_or(Arg::None);
        Ok(std::env::var(&key)
            .map(IntoArg::into_arg)
            .unwrap_or(default))
    }
    pub fn __getitem__(&self, key: impl IntoArg) -> Result<Arg> {
        let k = key.into_arg();
        std::env::var(k.__str__())
            .map(IntoArg::into_arg)
            .map_err(|_| LailaError::key_error(k.__repr__()))
    }
    pub fn __setitem__(&self, key: impl IntoArg, value: impl IntoArg) -> Result<()> {
        // SAFETY-free: std marks set_var unsafe only on edition 2024; we are on 2021.
        std::env::set_var(key.into_arg().__str__(), value.into_arg().__str__());
        Ok(())
    }
    pub fn __delitem__(&self, key: impl IntoArg) -> Result<()> {
        let k = key.into_arg().__str__();
        if std::env::var(&k).is_err() {
            return Err(LailaError::key_error(crate::python::pyrepr::str_repr(&k)));
        }
        std::env::remove_var(k);
        Ok(())
    }
    pub fn __contains__(&self, key: impl IntoArg) -> Result<bool> {
        Ok(std::env::var(key.into_arg().__str__()).is_ok())
    }
    pub fn pop(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let key = a
            .positional
            .first()
            .map(|k| k.__str__())
            .unwrap_or_default();
        match std::env::var(&key) {
            Ok(v) => {
                std::env::remove_var(&key);
                Ok(v.into_arg())
            }
            Err(_) => a
                .positional
                .get(1)
                .cloned()
                .ok_or_else(|| LailaError::key_error(crate::python::pyrepr::str_repr(&key))),
        }
    }
    pub fn copy(&self) -> Arg {
        let d = crate::python::builtins::dict(Args::new()).expect("dict");
        for (k, v) in std::env::vars() {
            let _ = d.__setitem__(k, v);
        }
        d
    }
    pub fn keys(&self) -> Arg {
        std::env::vars()
            .map(|(k, _)| k)
            .collect::<Vec<_>>()
            .into_arg()
    }
    pub fn values(&self) -> Arg {
        std::env::vars()
            .map(|(_, v)| v)
            .collect::<Vec<_>>()
            .into_arg()
    }
    pub fn items(&self) -> Arg {
        std::env::vars()
            .map(|(k, v)| Arg::Tuple(alloc::vec![k.into_arg(), v.into_arg()].into()))
            .collect::<Vec<_>>()
            .into_arg()
    }
    pub fn __len__(&self) -> usize {
        std::env::vars().count()
    }
}

/// `os.environ` as a value (`x in os.environ` through `Arg`, `f(os.environ)`,
/// `dict(os.environ)`): the mapping protocol over the live environment.
impl PyObject for os_environ {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "_Environ"
    }
    fn __repr__(&self) -> String {
        alloc::format!("environ({})", self.copy().__repr__())
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other.as_any().downcast_ref::<os_environ>().is_some()
    }
    fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error("unhashable type: '_Environ'"))
    }
    fn __len__(&self) -> Result<usize> {
        Ok(os_environ::__len__(self))
    }
    fn __bool__(&self) -> bool {
        os_environ::__len__(self) > 0
    }
    fn __contains__(&self, item: &Arg) -> Result<bool> {
        os_environ::__contains__(self, item.clone())
    }
    fn __iter__(&self) -> Result<Vec<Arg>> {
        self.keys().__iter__()
    }
    fn __getitem__(&self, key: &Arg) -> Result<Arg> {
        os_environ::__getitem__(self, key.clone())
    }
    fn __setitem__(&self, key: &Arg, value: Arg) -> Result<()> {
        os_environ::__setitem__(self, key.clone(), value)
    }
    fn __delitem__(&self, key: &Arg) -> Result<()> {
        os_environ::__delitem__(self, key.clone())
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "get" => self.get(args),
            "pop" => self.pop(args),
            "copy" => Ok(self.copy()),
            "keys" => Ok(self.keys()),
            "values" => Ok(self.values()),
            "items" => Ok(self.items()),
            "setdefault" => {
                let key = args.positional.first().cloned().unwrap_or(Arg::None);
                let default = args.positional.get(1).cloned().unwrap_or(Arg::None);
                match std::env::var(key.__str__()) {
                    Ok(v) => Ok(v.into_arg()),
                    Err(_) => {
                        os_environ::__setitem__(self, key, default.clone())?;
                        Ok(default)
                    }
                }
            }
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'_Environ' object has no attribute '{name}'"
            ))),
        }
    }
}
impl IntoArg for os_environ {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for os_environ {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(*self))
    }
}

/// `import os`
#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)]
pub struct os_mod;
pub static os: os_mod = os_mod;
impl os_mod {
    /// `os.path` (attribute access lowers to a call in Tier 2).
    pub fn path(&self) -> os_path_mod {
        os_path_mod
    }
    /// `os.environ`
    pub fn environ(&self) -> os_environ {
        os_environ
    }
    pub fn sep(&self) -> &'static str {
        "/"
    }
    pub fn linesep(&self) -> &'static str {
        "\n"
    }
    pub fn name(&self) -> &'static str {
        "posix"
    }
    /// `os._exit(status)` -- leave at once, no handlers, no flushing.
    #[allow(unreachable_code)]
    pub fn _exit(&self, status: impl IntoArg) -> Result<()> {
        std::process::exit(status.into_arg().as_i64().unwrap_or(0) as i32);
        Ok(())
    }
    pub fn getpid(&self) -> i64 {
        std::process::id() as i64
    }
    pub fn cpu_count(&self) -> Arg {
        std::thread::available_parallelism()
            .map(|n| Arg::Int(n.get() as i64))
            .unwrap_or(Arg::None)
    }
    pub fn urandom(&self, n: impl IntoArg) -> Result<Arg> {
        let n = n.into_arg().as_i64().ok_or_else(|| {
            LailaError::type_error("'float' object cannot be interpreted as an integer")
        })?;
        if n < 0 {
            return Err(LailaError::value_error("negative argument not allowed"));
        }
        Ok(crate::python::object::bytes(
            crate::_compat::random::urandom(n as usize),
        ))
    }
    pub fn getcwd(&self) -> Result<String> {
        std::env::current_dir()
            .map(|p| p.to_string_lossy().into_owned())
            .map_err(|e| io_error(&e, "."))
    }
    pub fn getenv(&self, args: impl Into<Args>) -> Result<Arg> {
        os_environ.get(args)
    }
    /// `os.close(fd)`: the descriptors `tempfile.mkstemp` hands out are
    /// handle ids with no open file behind them, so closing is a no-op.
    pub fn close(&self, _args: impl Into<Args>) -> Result<()> {
        Ok(())
    }
    /// `os.fstat(fd)`: the descriptor's `stat_result` (a dict-shaped
    /// object: `st_mode`, `st_size`, `st_ino`, `st_dev`, `st_nlink`,
    /// `st_uid`, `st_gid`); `OSError` (`[Errno 9] Bad file descriptor`)
    /// once *fd* is closed.
    pub fn fstat(&self, fd: impl IntoArg) -> Result<Arg> {
        let fd = fd.into_arg();
        let fd = fd.as_i64().ok_or_else(|| {
            LailaError::type_error(alloc::format!(
                "'{}' object cannot be interpreted as an integer",
                fd.type_name()
            ))
        })?;
        #[cfg(unix)]
        {
            let mut st: libc::stat = unsafe { core::mem::zeroed() };
            let rc = unsafe { libc::fstat(fd as i32, &mut st) };
            if rc != 0 {
                let e = std::io::Error::last_os_error();
                return Err(LailaError::new(
                    ExcKind::OSError,
                    alloc::format!("[Errno {}] {}", e.raw_os_error().unwrap_or(0), e),
                ));
            }
            let mut d = crate::python::PyDictBuilder::new();
            d.insert("st_mode", st.st_mode as i64);
            d.insert("st_ino", st.st_ino as i64);
            d.insert("st_dev", st.st_dev as i64);
            d.insert("st_nlink", st.st_nlink as i64);
            d.insert("st_uid", st.st_uid as i64);
            d.insert("st_gid", st.st_gid as i64);
            d.insert("st_size", st.st_size as i64);
            Ok(d.finish())
        }
        #[cfg(not(unix))]
        {
            Err(LailaError::new(
                ExcKind::OSError,
                alloc::format!("[Errno 9] Bad file descriptor: {fd}"),
            ))
        }
    }
    /// `os.unlink(path)`
    pub fn unlink(&self, args: impl Into<Args>) -> Result<()> {
        let p = first_path(&args.into(), "unlink")?;
        std::fs::remove_file(&p).map_err(|e| io_error(&e, &p))
    }
    /// `os.remove(path)`
    pub fn remove(&self, args: impl Into<Args>) -> Result<()> {
        let p = first_path(&args.into(), "remove")?;
        std::fs::remove_file(&p).map_err(|e| io_error(&e, &p))
    }
    /// `os.rmdir(path)`
    pub fn rmdir(&self, args: impl Into<Args>) -> Result<()> {
        let p = first_path(&args.into(), "rmdir")?;
        std::fs::remove_dir(&p).map_err(|e| io_error(&e, &p))
    }
    /// `os.mkdir(path, mode=0o777)`
    pub fn mkdir(&self, args: impl Into<Args>) -> Result<()> {
        let p = first_path(&args.into(), "mkdir")?;
        std::fs::create_dir(&p).map_err(|e| io_error(&e, &p))
    }
    /// `os.makedirs(name, exist_ok=False)`
    pub fn makedirs(&self, args: impl Into<Args>) -> Result<()> {
        let a: Args = args.into();
        let p = first_path(&a, "makedirs")?;
        let exist_ok = a.get("exist_ok").__bool__();
        if std::path::Path::new(&p).is_dir() && !exist_ok {
            return Err(LailaError::new(
                ExcKind::OSError,
                alloc::format!("[Errno 17] File exists: '{p}'"),
            ));
        }
        std::fs::create_dir_all(&p).map_err(|e| io_error(&e, &p))
    }
    pub fn listdir(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let p = a
            .positional
            .first()
            .map(path_arg)
            .transpose()?
            .unwrap_or_else(|| ".".into());
        let rd = std::fs::read_dir(&p).map_err(|e| io_error(&e, &p))?;
        let names: Vec<String> = rd
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        Ok(names.into_arg())
    }
    pub fn rename(&self, src: impl IntoArg, dst: impl IntoArg) -> Result<()> {
        let s = path_arg(&src.into_arg())?;
        let d = path_arg(&dst.into_arg())?;
        std::fs::rename(&s, &d).map_err(|e| io_error(&e, &s))
    }
}

/// `import sys`
#[derive(Clone, Debug)]
#[allow(non_camel_case_types)]
pub struct sys_mod;
pub static sys: sys_mod = sys_mod;
impl sys_mod {
    pub fn platform(&self) -> &'static str {
        if cfg!(target_os = "linux") {
            "linux"
        } else if cfg!(target_os = "macos") {
            "darwin"
        } else if cfg!(target_os = "windows") {
            "win32"
        } else {
            "unknown"
        }
    }
    pub fn maxsize(&self) -> i64 {
        i64::MAX
    }
    pub fn version(&self) -> &'static str {
        "3.14 (laila-rs)"
    }
    /// `sys.path` -- a list; `sys.path.insert(..)` / `append` are accepted and ignored.
    pub fn path(&self) -> Arg {
        static P: std::sync::OnceLock<Arg> = std::sync::OnceLock::new();
        P.get_or_init(|| Vec::<Arg>::new().into_arg()).clone()
    }
    pub fn getrecursionlimit(&self) -> i64 {
        1000
    }
    pub fn exit(&self, args: impl Into<Args>) -> ! {
        let a: Args = args.into();
        let code = a.positional.first().and_then(|c| c.as_i64()).unwrap_or(0);
        std::process::exit(code as i32)
    }
    /// `sys.executable` -- the running program (a test binary re-executes
    /// itself for `ChildScript`s).
    pub fn executable(&self) -> Arg {
        Arg::Str(
            std::env::current_exe()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default()
                .as_str()
                .into(),
        )
    }
    /// `sys.stdout`
    pub fn stdout(&self) -> StdStream {
        StdStream(false)
    }
    /// `sys.stderr`
    pub fn stderr(&self) -> StdStream {
        StdStream(true)
    }
    /// `sys.argv` (honours `monkeypatch.setattr("sys.argv", ...)`).
    pub fn argv(&self) -> Arg {
        sys_argv().into_arg()
    }
}

/// `sys.argv` override installed by `pytest.monkeypatch().setattr("sys.argv", ...)`.
static SYS_ARGV_OVERRIDE: parking_lot::RwLock<Option<Vec<String>>> = parking_lot::RwLock::new(None);

/// The current `sys.argv` (the override when one is installed, else the
/// process arguments).
pub fn sys_argv() -> Vec<String> {
    if let Some(v) = SYS_ARGV_OVERRIDE.read().as_ref() {
        return v.clone();
    }
    std::env::args().collect()
}

/// Installs (`Some`) or clears (`None`) the `sys.argv` override; returns
/// the previous override so a monkeypatch can restore it.
pub fn set_sys_argv(argv: Option<Vec<String>>) -> Option<Vec<String>> {
    core::mem::replace(&mut *SYS_ARGV_OVERRIDE.write(), argv)
}

/// `sys.stdout` / `sys.stderr`: `write(text)` returns the character
/// count, `flush()`.
#[derive(Clone, Copy, Debug)]
pub struct StdStream(bool);
impl StdStream {
    pub fn write(&self, args: impl Into<Args>) -> Result<i64> {
        use std::io::Write;
        let a: Args = args.into();
        let t = a.positional.first().cloned().unwrap_or(Arg::None).__str__();
        if self.0 {
            let _ = std::io::stderr().write_all(t.as_bytes());
        } else {
            let _ = std::io::stdout().write_all(t.as_bytes());
        }
        Ok(t.chars().count() as i64)
    }
    pub fn flush(&self, _args: impl Into<Args>) -> Result<()> {
        use std::io::Write;
        if self.0 {
            let _ = std::io::stderr().flush();
        } else {
            let _ = std::io::stdout().flush();
        }
        Ok(())
    }
}

/// `import shutil`
#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)]
pub struct shutil_mod;
pub static shutil: shutil_mod = shutil_mod;
impl shutil_mod {
    pub fn which(&self, cmd: impl IntoArg) -> Arg {
        let c = cmd.into_arg().__str__();
        if c.contains('/') {
            return if std::path::Path::new(&c).is_file() {
                c.into_arg()
            } else {
                Arg::None
            };
        }
        for dir in std::env::var("PATH").unwrap_or_default().split(':') {
            let p = std::path::Path::new(dir).join(&c);
            if p.is_file() {
                return p.to_string_lossy().into_owned().into_arg();
            }
        }
        Arg::None
    }
    /// `shutil.rmtree(path, ignore_errors=False)`
    pub fn rmtree(&self, args: impl Into<Args>) -> Result<()> {
        let a: Args = args.into();
        let p = first_path(&a, "rmtree")?;
        match std::fs::remove_dir_all(&p) {
            Ok(()) => Ok(()),
            Err(_) if a.get("ignore_errors").__bool__() => Ok(()),
            Err(e) => Err(io_error(&e, &p)),
        }
    }
    pub fn copyfile(&self, src: impl IntoArg, dst: impl IntoArg) -> Result<String> {
        let s = path_arg(&src.into_arg())?;
        let d = path_arg(&dst.into_arg())?;
        std::fs::copy(&s, &d)
            .map(|_| d)
            .map_err(|e| io_error(&e, &s))
    }
}

/// `import tempfile`
#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)]
pub struct tempfile_mod;
pub static tempfile: tempfile_mod = tempfile_mod;

fn unique_name(prefix: &str, suffix: &str) -> String {
    let r = crate::_compat::random::urandom(6);
    let tag: String = r.iter().map(|b| alloc::format!("{b:02x}")).collect();
    alloc::format!("{prefix}{tag}{suffix}")
}

impl tempfile_mod {
    pub fn gettempdir(&self) -> String {
        std::env::temp_dir()
            .to_string_lossy()
            .trim_end_matches('/')
            .to_string()
    }
    /// `tempfile.mkdtemp(suffix=None, prefix=None, dir=None)`
    pub fn mkdtemp(&self, args: impl Into<Args>) -> Result<String> {
        let a: Args = args.into();
        let prefix = a.get("prefix");
        let prefix = if prefix.is_none() {
            "tmp".to_string()
        } else {
            prefix.__str__()
        };
        let suffix = a.get("suffix");
        let suffix = if suffix.is_none() {
            String::new()
        } else {
            suffix.__str__()
        };
        let dir = a.get("dir");
        let dir = if dir.is_none() {
            self.gettempdir()
        } else {
            dir.__str__()
        };
        let p = alloc::format!("{dir}/{}", unique_name(&prefix, &suffix));
        std::fs::create_dir(&p).map_err(|e| io_error(&e, &p))?;
        Ok(p)
    }
    /// `tempfile.mkstemp(suffix=None, prefix=None, dir=None)` -> `(fd, path)`.
    ///
    /// The file is created (empty); the "descriptor" is a handle id that
    /// `os.close` accepts (the port keeps no open file object behind it).
    pub fn mkstemp(&self, args: impl Into<Args>) -> Result<(i64, String)> {
        let a: Args = args.into();
        let prefix = a.get("prefix");
        let prefix = if prefix.is_none() {
            "tmp".to_string()
        } else {
            prefix.__str__()
        };
        let suffix = a.get("suffix");
        let suffix = if suffix.is_none() {
            String::new()
        } else {
            suffix.__str__()
        };
        let dir = a.get("dir");
        let dir = if dir.is_none() {
            self.gettempdir()
        } else {
            dir.__str__()
        };
        let p = alloc::format!("{dir}/{}", unique_name(&prefix, &suffix));
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&p)
            .map_err(|e| io_error(&e, &p))?;
        static NEXT_FD: core::sync::atomic::AtomicI64 = core::sync::atomic::AtomicI64::new(1000);
        let fd = NEXT_FD.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        Ok((fd, p))
    }
    /// `tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)`
    pub fn TemporaryDirectory(&self, args: impl Into<Args>) -> Result<TemporaryDirectory> {
        Ok(TemporaryDirectory {
            name: Arc::from(self.mkdtemp(args)?.as_str()),
        })
    }
    /// `tempfile.NamedTemporaryFile(mode='w+b', suffix=None, prefix=None, dir=None, delete=True)`
    pub fn NamedTemporaryFile(&self, args: impl Into<Args>) -> Result<NamedTemporaryFile> {
        let a: Args = args.into();
        let prefix = a.get("prefix");
        let prefix = if prefix.is_none() {
            "tmp".to_string()
        } else {
            prefix.__str__()
        };
        let suffix = a.get("suffix");
        let suffix = if suffix.is_none() {
            String::new()
        } else {
            suffix.__str__()
        };
        let dir = a.get("dir");
        let dir = if dir.is_none() {
            self.gettempdir()
        } else {
            dir.__str__()
        };
        let delete = if a.__contains__("delete")? {
            a.get("delete").__bool__()
        } else {
            true
        };
        let name = alloc::format!("{dir}/{}", unique_name(&prefix, &suffix));
        std::fs::File::create(&name).map_err(|e| io_error(&e, &name))?;
        Ok(NamedTemporaryFile {
            name: Arc::from(name.as_str()),
            delete,
            closed: Arc::new(crate::python::sync::Mutex::new(false)),
        })
    }
}

/// `tempfile.TemporaryDirectory()`: `with tempfile.TemporaryDirectory() as d:`
/// binds `d` to the path; the directory is removed when the block ends.
#[derive(Clone, Debug)]
pub struct TemporaryDirectory {
    pub name: Arc<str>,
}
impl TemporaryDirectory {
    pub fn __enter__(&self) -> Result<TempDirGuard> {
        Ok(TempDirGuard {
            name: self.name.clone(),
        })
    }
    pub fn cleanup(&self, _args: impl Into<Args>) -> Result<()> {
        let _ = std::fs::remove_dir_all(&*self.name);
        Ok(())
    }
}

/// The value bound by `with tempfile.TemporaryDirectory() as d`: usable as
/// the path string; removes the directory on drop (`__exit__`).
#[derive(Debug)]
pub struct TempDirGuard {
    name: Arc<str>,
}
impl core::ops::Deref for TempDirGuard {
    type Target = str;
    fn deref(&self) -> &str {
        &self.name
    }
}
impl core::fmt::Display for TempDirGuard {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.name)
    }
}
impl PyArg for TempDirGuard {
    fn to_arg(&self) -> Arg {
        Arg::Str(self.name.clone())
    }
}
impl IntoArg for TempDirGuard {
    fn into_arg(self) -> Arg {
        Arg::Str(self.name.clone())
    }
}
impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&*self.name);
    }
}

/// `tempfile.NamedTemporaryFile(...)` (write / close / `.name`).
#[derive(Clone, Debug)]
pub struct NamedTemporaryFile {
    pub name: Arc<str>,
    pub delete: bool,
    closed: Arc<crate::python::sync::Mutex<bool>>,
}
impl NamedTemporaryFile {
    /// `f.name` -- the path of the temporary file.
    pub fn name(&self) -> String {
        String::from(&*self.name)
    }
    pub fn write(&self, data: impl IntoArg) -> Result<i64> {
        use std::io::Write;
        let d = data.into_arg();
        let bytes: Vec<u8> = match &d {
            Arg::Str(s) => s.as_bytes().to_vec(),
            other => other.as_bytes_like().ok_or_else(|| {
                LailaError::type_error(alloc::format!(
                    "a bytes-like object is required, not '{}'",
                    other.type_name()
                ))
            })?,
        };
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&*self.name)
            .map_err(|e| io_error(&e, &self.name))?;
        f.write_all(&bytes).map_err(|e| io_error(&e, &self.name))?;
        Ok(bytes.len() as i64)
    }
    pub fn flush(&self, _args: impl Into<Args>) -> Result<()> {
        Ok(())
    }
    pub fn close(&self, _args: impl Into<Args>) -> Result<()> {
        let mut c = self.closed.lock();
        if !*c {
            *c = true;
            if self.delete {
                let _ = std::fs::remove_file(&*self.name);
            }
        }
        Ok(())
    }
    pub fn __enter__(&self) -> Result<NamedTempFileGuard> {
        Ok(NamedTempFileGuard(self.clone()))
    }
}
impl PyObject for NamedTemporaryFile {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "_TemporaryFileWrapper"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<tempfile._TemporaryFileWrapper name={}>",
            crate::python::pyrepr::str_repr(&self.name)
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<NamedTemporaryFile>()
            .is_some_and(|o| Arc::ptr_eq(&o.closed, &self.closed))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.closed) as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "name" => Ok(Arg::Str(self.name.clone())),
            "delete" => Ok(Arg::Bool(self.delete)),
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'_TemporaryFileWrapper' object has no attribute '{name}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        Ok(match name {
            "write" => Arg::Int(self.write(args.positional.first().cloned().unwrap_or(Arg::None))?),
            "flush" => {
                self.flush(args)?;
                Arg::None
            }
            "close" => {
                self.close(args)?;
                Arg::None
            }
            _ => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'_TemporaryFileWrapper' object has no attribute '{name}'"
                )))
            }
        })
    }
}
impl IntoArg for NamedTemporaryFile {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
/// `with tempfile.NamedTemporaryFile() as f:` -- closes (and deletes) on drop.
#[derive(Debug)]
pub struct NamedTempFileGuard(NamedTemporaryFile);
impl core::ops::Deref for NamedTempFileGuard {
    type Target = NamedTemporaryFile;
    fn deref(&self) -> &NamedTemporaryFile {
        &self.0
    }
}
impl Drop for NamedTempFileGuard {
    fn drop(&mut self) {
        let _ = self.0.close(Args::new());
    }
}
impl IntoArg for NamedTempFileGuard {
    fn into_arg(self) -> Arg {
        self.0.clone().into_arg()
    }
}
impl IntoArg for &NamedTempFileGuard {
    fn into_arg(self) -> Arg {
        self.0.clone().into_arg()
    }
}

/// `import urllib` -- `urllib.parse.quote` / `unquote`.
#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)]
pub struct urllib_mod;
#[allow(non_upper_case_globals)]
pub static urllib: urllib_mod = urllib_mod;
impl urllib_mod {
    pub fn parse(&self) -> urllib_parse_mod {
        urllib_parse_mod
    }
}

/// `urllib.parse`
#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)]
pub struct urllib_parse_mod;
impl urllib_parse_mod {
    /// `urllib.parse.quote(string, safe='/')`: percent-encode every byte of
    /// the UTF-8 text except the unreserved `A-Za-z0-9_.-~` and *safe*.
    pub fn quote(&self, args: impl Into<Args>) -> Result<String> {
        let a: Args = args.into();
        let string = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("string"));
        let safe = a
            .positional
            .get(1)
            .cloned()
            .unwrap_or_else(|| match a.get("safe") {
                Arg::None => Arg::Str("/".into()),
                s => s,
            });
        let bytes: Vec<u8> = match &string {
            Arg::Str(s) => s.as_bytes().to_vec(),
            Arg::Bytes(b) => b.to_vec(),
            other => {
                return Err(LailaError::type_error(alloc::format!(
                    "quote() doesn't support '{}' objects",
                    other.type_name()
                )))
            }
        };
        let safe = safe.__str__();
        let mut out = String::with_capacity(bytes.len());
        for b in bytes {
            let c = b as char;
            if b.is_ascii_alphanumeric()
                || matches!(c, '_' | '.' | '-' | '~')
                || (b.is_ascii() && safe.contains(c))
            {
                out.push(c);
            } else {
                out.push_str(&alloc::format!("%{:02X}", b));
            }
        }
        Ok(out)
    }
    /// `urllib.parse.unquote(string)`: `%XX` sequences decoded as UTF-8
    /// (invalid sequences become U+FFFD, `errors='replace'`).
    pub fn unquote(&self, string: impl IntoArg) -> Result<String> {
        let string = string.into_arg().__str__();
        let bytes = string.as_bytes();
        let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%' && i + 2 < bytes.len() {
                let hex = &string[i + 1..i + 3];
                if let Ok(v) = u8::from_str_radix(hex, 16) {
                    out.push(v);
                    i += 3;
                    continue;
                }
            }
            out.push(bytes[i]);
            i += 1;
        }
        Ok(String::from_utf8_lossy(&out).into_owned())
    }
}

/// `open(file, mode='r', buffering=-1, encoding=None, errors=None, newline=None)`
/// -- the builtin, for the modes the port needs (`r`, `w`, `a`, `+`, `b`).
#[pysig("open(file, mode='r', buffering=-1, encoding=None, errors=None, newline=None)")]
pub fn open(args: impl Into<Args>) -> Result<PyFile> {
    let name = path_arg(&file)?;
    let mode = match mode {
        Arg::None => String::from("r"),
        m => m.__str__(),
    };
    let binary = mode.contains('b');
    let plus = mode.contains('+');
    let mut opts = std::fs::OpenOptions::new();
    if mode.contains('r') {
        opts.read(true).write(plus);
    } else if mode.contains('w') {
        opts.write(true).create(true).truncate(true).read(plus);
    } else if mode.contains('a') {
        opts.append(true).create(true).read(plus);
    } else if mode.contains('x') {
        opts.write(true).create_new(true).read(plus);
    } else {
        return Err(LailaError::value_error(alloc::format!(
            "invalid mode: '{mode}'"
        )));
    }
    let file = opts.open(&name).map_err(|e| io_error(&e, &name))?;
    Ok(PyFile {
        name: Arc::from(name.as_str()),
        mode: Arc::from(mode.as_str()),
        binary,
        inner: Arc::new(crate::python::sync::Mutex::new(Some(file))),
    })
}

/// The object `open()` returns (`io.TextIOWrapper` / `io.BufferedWriter`).
#[derive(Clone, Debug)]
pub struct PyFile {
    pub name: Arc<str>,
    pub mode: Arc<str>,
    binary: bool,
    inner: Arc<crate::python::sync::Mutex<Option<std::fs::File>>>,
}

impl PyFile {
    fn with_file<T>(&self, f: impl FnOnce(&mut std::fs::File) -> std::io::Result<T>) -> Result<T> {
        let mut g = self.inner.lock();
        match g.as_mut() {
            None => Err(LailaError::value_error("I/O operation on closed file.")),
            Some(file) => f(file).map_err(|e| io_error(&e, &self.name)),
        }
    }
    /// `f.write(data)` -- the number of characters / bytes written.
    pub fn write(&self, args: impl Into<Args>) -> Result<i64> {
        use std::io::Write;
        let a: Args = args.into();
        let d = a.positional.first().cloned().ok_or_else(|| {
            LailaError::type_error("write() takes exactly one argument (0 given)")
        })?;
        let (bytes, n): (Vec<u8>, i64) = match (&d, self.binary) {
            (Arg::Str(s), false) => (s.as_bytes().to_vec(), s.chars().count() as i64),
            (Arg::Str(_), true) => {
                return Err(LailaError::type_error(
                    "a bytes-like object is required, not 'str'",
                ))
            }
            (other, true) => {
                let b = other.as_bytes_like().ok_or_else(|| {
                    LailaError::type_error(alloc::format!(
                        "a bytes-like object is required, not '{}'",
                        other.type_name()
                    ))
                })?;
                let n = b.len() as i64;
                (b, n)
            }
            (other, false) => {
                return Err(LailaError::type_error(alloc::format!(
                    "write() argument must be str, not {}",
                    other.type_name()
                )))
            }
        };
        self.with_file(|f| f.write_all(&bytes))?;
        Ok(n)
    }
    /// `f.read()` -- the rest of the file (`str` in text mode, `bytes` in binary).
    pub fn read(&self, _args: impl Into<Args>) -> Result<Arg> {
        use std::io::Read;
        let mut buf = Vec::new();
        self.with_file(|f| f.read_to_end(&mut buf))?;
        Ok(if self.binary {
            Arg::Bytes(Arc::from(buf))
        } else {
            Arg::Str(Arc::from(String::from_utf8_lossy(&buf).as_ref()))
        })
    }
    /// `f.seek(offset, whence=0)`
    pub fn seek(&self, args: impl Into<Args>) -> Result<i64> {
        use std::io::Seek;
        let a: Args = args.into();
        let offset = a.positional.first().and_then(|v| v.as_i64()).unwrap_or(0);
        let whence = a.positional.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
        let pos = match whence {
            0 => std::io::SeekFrom::Start(offset.max(0) as u64),
            1 => std::io::SeekFrom::Current(offset),
            _ => std::io::SeekFrom::End(offset),
        };
        Ok(self.with_file(|f| f.seek(pos))? as i64)
    }
    /// `f.truncate(size=None)`
    pub fn truncate(&self, args: impl Into<Args>) -> Result<i64> {
        use std::io::Seek;
        let a: Args = args.into();
        let size = match a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("size"))
        {
            Arg::None => self.with_file(|f| f.stream_position())? as i64,
            v => v.as_i64().unwrap_or(0),
        };
        self.with_file(|f| f.set_len(size.max(0) as u64))?;
        Ok(size)
    }
    pub fn flush(&self, _args: impl Into<Args>) -> Result<()> {
        use std::io::Write;
        self.with_file(|f| f.flush())
    }
    pub fn close(&self, _args: impl Into<Args>) -> Result<()> {
        self.inner.lock().take();
        Ok(())
    }
    /// `f.closed`
    pub fn closed(&self) -> bool {
        self.inner.lock().is_none()
    }
    pub fn __enter__(&self) -> Result<PyFileGuard> {
        Ok(PyFileGuard(self.clone()))
    }
}

impl PyObject for PyFile {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        if self.binary {
            "BufferedWriter"
        } else {
            "TextIOWrapper"
        }
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<_io.{} name={} mode={}>",
            self.type_name(),
            crate::python::pyrepr::str_repr(&self.name),
            crate::python::pyrepr::str_repr(&self.mode)
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<PyFile>()
            .is_some_and(|o| Arc::ptr_eq(&o.inner, &self.inner))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.inner) as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "name" => Ok(Arg::Str(self.name.clone())),
            "mode" => Ok(Arg::Str(self.mode.clone())),
            "closed" => Ok(Arg::Bool(self.closed())),
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'_io.{}' object has no attribute '{name}'",
                self.type_name()
            ))),
        }
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        Ok(match name {
            "write" => Arg::Int(self.write(args)?),
            "read" => self.read(args)?,
            "seek" => Arg::Int(self.seek(args)?),
            "truncate" => Arg::Int(self.truncate(args)?),
            "flush" => {
                self.flush(args)?;
                Arg::None
            }
            "close" => {
                self.close(args)?;
                Arg::None
            }
            "__enter__" => self.clone().into_arg(),
            "__exit__" => {
                self.close(args)?;
                Arg::Bool(false)
            }
            _ => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'_io.{}' object has no attribute '{name}'",
                    self.type_name()
                )))
            }
        })
    }
}
impl IntoArg for PyFile {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for PyFile {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
/// `with open(..) as f:` -- closes on drop.
#[derive(Debug)]
pub struct PyFileGuard(PyFile);
impl core::ops::Deref for PyFileGuard {
    type Target = PyFile;
    fn deref(&self) -> &PyFile {
        &self.0
    }
}
impl Drop for PyFileGuard {
    fn drop(&mut self) {
        let _ = self.0.close(Args::new());
    }
}
impl IntoArg for PyFileGuard {
    fn into_arg(self) -> Arg {
        self.0.to_arg()
    }
}
impl IntoArg for &PyFileGuard {
    fn into_arg(self) -> Arg {
        self.0.to_arg()
    }
}
