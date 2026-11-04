//! `import pathlib` / `from pathlib import Path` -- the POSIX flavour of
//! `pathlib.Path`: a pure path value (`/` joins, `.name` / `.parent` /
//! `.suffix` / `.stem` / `.parts`) with the concrete queries and I/O the
//! tests use (`exists`, `is_file`, `is_dir`, `mkdir`, `unlink`, `rmdir`,
//! `read_text`, `write_text`, `read_bytes`, `write_bytes`, `touch`,
//! `iterdir`, `resolve`, `absolute`, `expanduser`, `home`, `cwd`).
//! `str(p)` is the path text, `repr(p)` is `PosixPath('..')`, equality
//! and hashing follow the text, and `os.*` / `open()` accept a `Path`
//! wherever Python accepts an `os.PathLike`.

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use super::super::args::Args;
use super::super::errors::{ExcKind, LailaError, Result};
use super::super::object::{Arg, IntoArg, PyObject};

/// `import pathlib`
#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)]
pub struct pathlib_mod;
pub static pathlib: pathlib_mod = pathlib_mod;
impl pathlib_mod {
    /// `pathlib.Path(*segments)`
    #[allow(non_snake_case)]
    pub fn Path(&self, args: impl Into<Args>) -> Result<Path> {
        Path::new(args)
    }
    /// `pathlib.PurePath(*segments)` / `pathlib.PosixPath(*segments)`
    #[allow(non_snake_case)]
    pub fn PurePath(&self, args: impl Into<Args>) -> Result<Path> {
        Path::new(args)
    }
    #[allow(non_snake_case)]
    pub fn PosixPath(&self, args: impl Into<Args>) -> Result<Path> {
        Path::new(args)
    }
}

/// `pathlib.Path`: an immutable POSIX path.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Path {
    text: String,
}

fn io_error(e: &std::io::Error, path: &str) -> LailaError {
    let kind = match e.kind() {
        std::io::ErrorKind::NotFound => ExcKind::FileNotFoundError,
        std::io::ErrorKind::PermissionDenied => ExcKind::PermissionError,
        std::io::ErrorKind::AlreadyExists => ExcKind::FileExistsError,
        _ => ExcKind::OSError,
    };
    let errno = e.raw_os_error().unwrap_or(0);
    let text = match e.kind() {
        std::io::ErrorKind::NotFound => "No such file or directory".to_string(),
        std::io::ErrorKind::PermissionDenied => "Permission denied".to_string(),
        std::io::ErrorKind::AlreadyExists => "File exists".to_string(),
        _ => e.to_string(),
    };
    LailaError::new(kind, alloc::format!("[Errno {errno}] {text}: '{path}'"))
}

/// One path segment as text (`str`, `bytes` or a `Path`).
fn segment(a: &Arg) -> Result<String> {
    match a {
        Arg::Str(s) => Ok(s.to_string()),
        Arg::Object(o) if o.as_any().is::<Path>() => Ok(o.__str__()),
        other => Err(LailaError::type_error(alloc::format!(
            "argument should be a str or an os.PathLike object where __fspath__ returns a str, not '{}'",
            other.type_name()
        ))),
    }
}

/// `PurePosixPath._parse_path`: collapse `//` and `.`, keep `..`, drop a
/// trailing slash (an empty path is `.`).
fn normalize(text: &str) -> String {
    let absolute = text.starts_with('/');
    let parts: Vec<&str> = text
        .split('/')
        .filter(|p| !p.is_empty() && *p != ".")
        .collect();
    let mut out = String::new();
    if absolute {
        out.push('/');
    }
    out.push_str(&parts.join("/"));
    if out.is_empty() {
        out.push('.');
    }
    out
}

impl Path {
    /// `Path(*segments)`: the segments joined (an absolute segment resets
    /// the path, as in Python).
    pub fn new(args: impl Into<Args>) -> Result<Path> {
        let a: Args = args.into();
        let mut p = Path {
            text: String::from("."),
        };
        for seg in &a.positional {
            p = p.joinpath_str(&segment(seg)?);
        }
        Ok(p)
    }

    /// A `Path` from text.
    pub fn from_text(text: &str) -> Path {
        Path {
            text: normalize(text),
        }
    }

    /// `Path.home()`
    pub fn home() -> Result<Path> {
        let home = std::env::var("HOME").map_err(|_| {
            LailaError::new(ExcKind::RuntimeError, "Could not determine home directory.")
        })?;
        Ok(Path::from_text(&home))
    }

    /// `Path.cwd()`
    pub fn cwd() -> Result<Path> {
        let cwd = std::env::current_dir().map_err(|e| io_error(&e, "."))?;
        Ok(Path::from_text(&cwd.to_string_lossy()))
    }

    fn joinpath_str(&self, other: &str) -> Path {
        if other.starts_with('/') {
            return Path::from_text(other);
        }
        if self.text == "." {
            return Path::from_text(other);
        }
        Path::from_text(&alloc::format!("{}/{other}", self.text))
    }

    /// `p / other`
    pub fn __truediv__(&self, other: &Arg) -> Result<Path> {
        Ok(self.joinpath_str(&segment(other)?))
    }

    /// `other / p` (a `str` left operand)
    pub fn __rtruediv__(&self, other: &Arg) -> Result<Path> {
        Ok(Path::from_text(&segment(other)?).joinpath_str(&self.text))
    }

    /// `p.joinpath(*segments)`
    pub fn joinpath(&self, args: impl Into<Args>) -> Result<Path> {
        let a: Args = args.into();
        let mut p = self.clone();
        for seg in &a.positional {
            p = p.joinpath_str(&segment(seg)?);
        }
        Ok(p)
    }

    /// `str(p)` / `os.fspath(p)`
    pub fn __str__(&self) -> String {
        self.text.clone()
    }

    /// `p.__fspath__()`
    pub fn __fspath__(&self) -> String {
        self.text.clone()
    }

    /// `p.parts`
    pub fn parts(&self) -> Vec<String> {
        let mut out = Vec::new();
        if self.text.starts_with('/') {
            out.push(String::from("/"));
        }
        if self.text != "." {
            out.extend(
                self.text
                    .split('/')
                    .filter(|s| !s.is_empty())
                    .map(String::from),
            );
        }
        out
    }

    /// `p.name`
    pub fn name(&self) -> String {
        if self.text == "/" || self.text == "." {
            return String::new();
        }
        self.text.rsplit('/').next().unwrap_or("").to_string()
    }

    /// `p.suffix`
    pub fn suffix(&self) -> String {
        let name = self.name();
        match name.rfind('.') {
            Some(i) if i > 0 && i + 1 < name.len() => name[i..].to_string(),
            _ => String::new(),
        }
    }

    /// `p.suffixes`
    pub fn suffixes(&self) -> Vec<String> {
        let name = self.name();
        let name = name.trim_start_matches('.');
        let mut out: Vec<String> = name
            .match_indices('.')
            .map(|(i, _)| {
                name[i..]
                    .split('.')
                    .nth(1)
                    .map(|s| alloc::format!(".{s}"))
                    .unwrap_or_default()
            })
            .filter(|s| s.len() > 1)
            .collect();
        if name.ends_with('.') {
            out.pop();
        }
        out
    }

    /// `p.stem`
    pub fn stem(&self) -> String {
        let name = self.name();
        let suffix = self.suffix();
        name[..name.len() - suffix.len()].to_string()
    }

    /// `p.parent`
    pub fn parent(&self) -> Path {
        if self.text == "/" || self.text == "." {
            return self.clone();
        }
        match self.text.rfind('/') {
            Some(0) => Path::from_text("/"),
            Some(i) => Path::from_text(&self.text[..i]),
            None => Path::from_text("."),
        }
    }

    /// `p.parents`
    pub fn parents(&self) -> Vec<Path> {
        let mut out = Vec::new();
        let mut cur = self.clone();
        loop {
            let parent = cur.parent();
            if parent == cur {
                break;
            }
            out.push(parent.clone());
            cur = parent;
        }
        out
    }

    /// `p.is_absolute()`
    pub fn is_absolute(&self) -> bool {
        self.text.starts_with('/')
    }

    /// `p.with_name(name)`
    pub fn with_name(&self, name: impl IntoArg) -> Result<Path> {
        let name = segment(&name.into_arg())?;
        if self.name().is_empty() {
            return Err(LailaError::value_error(alloc::format!(
                "{} has an empty name",
                self.__repr__()
            )));
        }
        if name.is_empty() || name.contains('/') || name == "." {
            return Err(LailaError::value_error(alloc::format!(
                "Invalid name {name:?}"
            )));
        }
        Ok(self.parent().joinpath_str(&name))
    }

    /// `p.with_suffix(suffix)`
    pub fn with_suffix(&self, suffix: impl IntoArg) -> Result<Path> {
        let suffix = segment(&suffix.into_arg())?;
        if !suffix.is_empty() && (!suffix.starts_with('.') || suffix == ".") {
            return Err(LailaError::value_error(alloc::format!(
                "Invalid suffix '{suffix}'"
            )));
        }
        let name = self.name();
        if name.is_empty() {
            return Err(LailaError::value_error(alloc::format!(
                "{} has an empty name",
                self.__repr__()
            )));
        }
        let stem = self.stem();
        Ok(self
            .parent()
            .joinpath_str(&alloc::format!("{stem}{suffix}")))
    }

    /// `p.with_stem(stem)`
    pub fn with_stem(&self, stem: impl IntoArg) -> Result<Path> {
        let stem = segment(&stem.into_arg())?;
        self.with_name(alloc::format!("{stem}{}", self.suffix()))
    }

    /// `p.expanduser()`
    pub fn expanduser(&self) -> Result<Path> {
        if self.text == "~" || self.text.starts_with("~/") {
            let home = Path::home()?;
            return Ok(home.joinpath_str(self.text.trim_start_matches('~').trim_start_matches('/')));
        }
        Ok(self.clone())
    }

    /// `p.absolute()`
    pub fn absolute(&self) -> Result<Path> {
        if self.is_absolute() {
            return Ok(self.clone());
        }
        Ok(Path::cwd()?.joinpath_str(&self.text))
    }

    /// `p.resolve(strict=False)`
    pub fn resolve(&self, args: impl Into<Args>) -> Result<Path> {
        let a: Args = args.into();
        let strict = a.get("strict").__bool__();
        match std::fs::canonicalize(&self.text) {
            Ok(p) => Ok(Path::from_text(&p.to_string_lossy())),
            Err(e) if strict => Err(io_error(&e, &self.text)),
            Err(_) => {
                // Non-strict: resolve the longest existing prefix, keep the rest.
                let abs = self.absolute()?;
                let mut out = String::new();
                for part in abs.text.split('/').filter(|s| !s.is_empty()) {
                    let next = alloc::format!("{out}/{part}");
                    out = match std::fs::canonicalize(&next) {
                        Ok(p) => p.to_string_lossy().into_owned(),
                        Err(_) => next,
                    };
                }
                Ok(Path::from_text(if out.is_empty() { "/" } else { &out }))
            }
        }
    }

    /// `p.exists()`
    pub fn exists(&self, _args: impl Into<Args>) -> Result<bool> {
        Ok(std::path::Path::new(&self.text).exists())
    }

    /// `p.is_file()`
    pub fn is_file(&self, _args: impl Into<Args>) -> Result<bool> {
        Ok(std::path::Path::new(&self.text).is_file())
    }

    /// `p.is_dir()`
    pub fn is_dir(&self, _args: impl Into<Args>) -> Result<bool> {
        Ok(std::path::Path::new(&self.text).is_dir())
    }

    /// `p.is_symlink()`
    pub fn is_symlink(&self) -> bool {
        std::fs::symlink_metadata(&self.text)
            .map(|m| m.file_type().is_symlink())
            .unwrap_or(false)
    }

    /// `p.mkdir(mode=0o777, parents=False, exist_ok=False)`
    pub fn mkdir(&self, args: impl Into<Args>) -> Result<()> {
        let a: Args = args.into();
        let parents = a.get("parents").__bool__();
        let exist_ok = a.get("exist_ok").__bool__();
        let r = if parents {
            std::fs::create_dir_all(&self.text)
        } else {
            std::fs::create_dir(&self.text)
        };
        match r {
            Ok(()) => Ok(()),
            Err(e)
                if e.kind() == std::io::ErrorKind::AlreadyExists
                    && exist_ok
                    && self.is_dir(())? =>
            {
                Ok(())
            }
            Err(e) => Err(io_error(&e, &self.text)),
        }
    }

    /// `p.rmdir()`
    pub fn rmdir(&self, _args: impl Into<Args>) -> Result<()> {
        std::fs::remove_dir(&self.text).map_err(|e| io_error(&e, &self.text))
    }

    /// `p.unlink(missing_ok=False)`
    pub fn unlink(&self, args: impl Into<Args>) -> Result<()> {
        let a: Args = args.into();
        let missing_ok = a.get("missing_ok").__bool__();
        match std::fs::remove_file(&self.text) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && missing_ok => Ok(()),
            Err(e) => Err(io_error(&e, &self.text)),
        }
    }

    /// `p.touch(mode=0o666, exist_ok=True)`
    pub fn touch(&self, args: impl Into<Args>) -> Result<()> {
        let a: Args = args.into();
        let exist_ok = match a.get("exist_ok") {
            Arg::None => true,
            v => v.__bool__(),
        };
        if std::path::Path::new(&self.text).exists() {
            if !exist_ok {
                return Err(io_error(
                    &std::io::Error::from(std::io::ErrorKind::AlreadyExists),
                    &self.text,
                ));
            }
            return Ok(());
        }
        std::fs::File::create(&self.text)
            .map(|_| ())
            .map_err(|e| io_error(&e, &self.text))
    }

    /// `p.read_text(encoding=None)`
    pub fn read_text(&self, _args: impl Into<Args>) -> Result<String> {
        std::fs::read_to_string(&self.text).map_err(|e| io_error(&e, &self.text))
    }

    /// `p.read_bytes()`
    pub fn read_bytes(&self, _args: impl Into<Args>) -> Result<Arg> {
        std::fs::read(&self.text)
            .map(|b| Arg::Bytes(b.into()))
            .map_err(|e| io_error(&e, &self.text))
    }

    /// `p.write_text(data, encoding=None)`: the number of characters written.
    pub fn write_text(&self, args: impl Into<Args>) -> Result<i64> {
        let a: Args = args.into();
        let data = match a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("data"))
        {
            Arg::Str(s) => s.to_string(),
            other => {
                return Err(LailaError::type_error(alloc::format!(
                    "data must be str, not {}",
                    other.type_name()
                )))
            }
        };
        std::fs::write(&self.text, data.as_bytes()).map_err(|e| io_error(&e, &self.text))?;
        Ok(data.chars().count() as i64)
    }

    /// `p.write_bytes(data)`: the number of bytes written.
    pub fn write_bytes(&self, args: impl Into<Args>) -> Result<i64> {
        let a: Args = args.into();
        let data = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("data"));
        let bytes = data.as_bytes_like().ok_or_else(|| {
            LailaError::type_error(alloc::format!(
                "a bytes-like object is required, not '{}'",
                data.type_name()
            ))
        })?;
        std::fs::write(&self.text, &bytes).map_err(|e| io_error(&e, &self.text))?;
        Ok(bytes.len() as i64)
    }

    /// `p.iterdir()`: the directory's entries (sorted, for determinism).
    pub fn iterdir(&self, _args: impl Into<Args>) -> Result<Vec<Path>> {
        let rd = std::fs::read_dir(&self.text).map_err(|e| io_error(&e, &self.text))?;
        let mut out: Vec<Path> = rd
            .flatten()
            .map(|e| self.joinpath_str(&e.file_name().to_string_lossy()))
            .collect();
        out.sort_by(|a, b| a.text.cmp(&b.text));
        Ok(out)
    }

    /// `p.glob(pattern)`: `*`, `?` and `**` matching under `p`.
    pub fn glob(&self, pattern: impl IntoArg) -> Result<Vec<Path>> {
        let pattern = segment(&pattern.into_arg())?;
        let mut out = Vec::new();
        self.glob_into(&pattern, &mut out)?;
        out.sort_by(|a, b| a.text.cmp(&b.text));
        Ok(out)
    }

    /// `p.rglob(pattern)` == `p.glob("**/" + pattern)`
    pub fn rglob(&self, pattern: impl IntoArg) -> Result<Vec<Path>> {
        let pattern = segment(&pattern.into_arg())?;
        self.glob(alloc::format!("**/{pattern}"))
    }

    fn glob_into(&self, pattern: &str, out: &mut Vec<Path>) -> Result<()> {
        let (head, tail) = match pattern.split_once('/') {
            Some((h, t)) => (h, Some(t)),
            None => (pattern, None),
        };
        if head == "**" {
            // Zero or more directories.
            match tail {
                Some(t) => self.glob_into(t, out)?,
                None => out.push(self.clone()),
            }
            for child in self.iterdir(())? {
                if child.is_dir(())? {
                    child.glob_into(pattern, out)?;
                }
            }
            return Ok(());
        }
        for child in self.iterdir(())? {
            if fnmatch(&child.name(), head) {
                match tail {
                    None => out.push(child),
                    Some(t) => {
                        if child.is_dir(())? {
                            child.glob_into(t, out)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// `repr(p)`
    pub fn __repr__(&self) -> String {
        alloc::format!("PosixPath('{}')", self.text)
    }
}

/// `fnmatch.fnmatchcase` for one path segment (`*`, `?`, `[..]`).
fn fnmatch(name: &str, pat: &str) -> bool {
    fn go(n: &[char], p: &[char]) -> bool {
        match p.first() {
            None => n.is_empty(),
            Some('*') => (0..=n.len()).any(|i| go(&n[i..], &p[1..])),
            Some('?') => !n.is_empty() && go(&n[1..], &p[1..]),
            Some('[') => {
                let Some(close) = p.iter().position(|c| *c == ']') else {
                    return !n.is_empty() && n[0] == '[' && go(&n[1..], &p[1..]);
                };
                let set = &p[1..close];
                let (negate, set) = match set.first() {
                    Some('!') => (true, &set[1..]),
                    _ => (false, set),
                };
                let Some(c) = n.first() else { return false };
                let mut hit = false;
                let mut i = 0;
                while i < set.len() {
                    if i + 2 < set.len() && set[i + 1] == '-' {
                        hit |= set[i] <= *c && *c <= set[i + 2];
                        i += 3;
                    } else {
                        hit |= set[i] == *c;
                        i += 1;
                    }
                }
                hit != negate && go(&n[1..], &p[close + 1..])
            }
            Some(c) => !n.is_empty() && n[0] == *c && go(&n[1..], &p[1..]),
        }
    }
    let n: Vec<char> = name.chars().collect();
    let p: Vec<char> = pat.chars().collect();
    go(&n, &p)
}

impl core::fmt::Debug for Path {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__repr__())
    }
}
impl core::fmt::Display for Path {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.text)
    }
}

fn paths(v: Vec<Path>) -> Arg {
    v.into_iter()
        .map(IntoArg::into_arg)
        .collect::<Vec<_>>()
        .into_arg()
}

impl PyObject for Path {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "PosixPath"
    }
    fn __repr__(&self) -> String {
        Path::__repr__(self)
    }
    fn __str__(&self) -> String {
        self.text.clone()
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Path>()
            .is_some_and(|o| o.text == self.text)
    }
    fn __hash__(&self) -> Result<i64> {
        Arg::Str(self.text.as_str().into()).__hash__()
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "name" => Ok(self.name().into_arg()),
            "suffix" => Ok(self.suffix().into_arg()),
            "suffixes" => Ok(self.suffixes().into_arg()),
            "stem" => Ok(self.stem().into_arg()),
            "parent" => Ok(self.parent().into_arg()),
            "parents" => Ok(paths(self.parents())),
            "parts" => Ok(Arg::Tuple(
                self.parts()
                    .into_iter()
                    .map(IntoArg::into_arg)
                    .collect::<Vec<_>>()
                    .into(),
            )),
            "anchor" | "root" => Ok(if self.is_absolute() { "/" } else { "" }.into_arg()),
            "drive" => Ok("".into_arg()),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'PosixPath' object has no attribute '{other}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let first = || -> Arg { args.positional.first().cloned().unwrap_or(Arg::None) };
        Ok(match name {
            "exists" => self.exists(args)?.into_arg(),
            "is_file" => self.is_file(args)?.into_arg(),
            "is_dir" => self.is_dir(args)?.into_arg(),
            "is_symlink" => self.is_symlink().into_arg(),
            "is_absolute" => self.is_absolute().into_arg(),
            "mkdir" => {
                self.mkdir(args)?;
                Arg::None
            }
            "rmdir" => {
                self.rmdir(args)?;
                Arg::None
            }
            "unlink" => {
                self.unlink(args)?;
                Arg::None
            }
            "touch" => {
                self.touch(args)?;
                Arg::None
            }
            "read_text" => self.read_text(args)?.into_arg(),
            "read_bytes" => self.read_bytes(args)?,
            "write_text" => self.write_text(args)?.into_arg(),
            "write_bytes" => self.write_bytes(args)?.into_arg(),
            "iterdir" => paths(self.iterdir(args)?),
            "glob" => paths(self.glob(first())?),
            "rglob" => paths(self.rglob(first())?),
            "joinpath" => self.joinpath(args)?.into_arg(),
            "with_name" => self.with_name(first())?.into_arg(),
            "with_suffix" => self.with_suffix(first())?.into_arg(),
            "with_stem" => self.with_stem(first())?.into_arg(),
            "expanduser" => self.expanduser()?.into_arg(),
            "absolute" => self.absolute()?.into_arg(),
            "resolve" => self.resolve(args)?.into_arg(),
            "__fspath__" | "__str__" | "as_posix" => self.__fspath__().into_arg(),
            "__truediv__" => self.__truediv__(&first())?.into_arg(),
            "__rtruediv__" => self.__rtruediv__(&first())?.into_arg(),
            other => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'PosixPath' object has no attribute '{other}'"
                )))
            }
        })
    }
}

impl IntoArg for Path {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl super::super::object::PyArg for Path {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl super::super::builtins::IsInstance for Path {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<Path>())
    }
}
