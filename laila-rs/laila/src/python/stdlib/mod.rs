//! Python stdlib modules as statics so `uuid.uuid4()`, `json.dumps(x)`,
//! `time.sleep(0.1)` keep their spelling. Phase 0 carries the handful the
//! spike needs; `_compat` grows per phase.

use alloc::string::String;

pub mod asyncio;
mod codecs;
pub mod collections;
mod compat;
#[cfg(feature = "std")]
pub mod concurrent;
pub mod contextvars;
#[cfg(any(feature = "sqlite", feature = "hdf5"))]
pub mod dbfiles;
pub mod extras;
pub mod inspect;
pub mod logging;
mod np;
#[cfg(feature = "std")]
mod os;
#[cfg(feature = "std")]
pub mod pathlib;
pub mod queue;
pub mod re;
#[cfg(feature = "std")]
pub mod socket;
#[cfg(feature = "std")]
pub mod subprocess;
pub mod threading;

/// Exception classes spelled through their module (`json.JSONDecodeError`,
/// `sqlite3.ProgrammingError`): the rewriter resolves `module.Name` here.
pub use super::errors::{
    JSONDecodeError, OperationalError, ParseError, ProgrammingError, TOMLDecodeError,
};
pub use asyncio::{asyncio, Coroutine, EventLoop};
pub use codecs::*;
pub use collections::{collections, OrderedDict};
pub use compat::*;
#[cfg(feature = "std")]
pub use concurrent::{concurrent, NativeFuture};
pub use contextvars::{copy_context, ContextVar};
#[cfg(any(feature = "sqlite", feature = "hdf5"))]
pub use dbfiles::*;
pub use extras::{contextlib, hashlib, math, r#struct, reduce, statistics, textwrap, Struct};
pub use inspect::inspect;
pub use logging::{logging, CapturingHandler, Formatter, LogRecord, NullHandler, StreamHandler};
pub use np::*;
#[cfg(feature = "std")]
pub use os::*;
#[cfg(feature = "std")]
pub use pathlib::{pathlib, Path};
pub use queue::{queue, PriorityQueue};
pub use re::{re, Match as ReMatch, Pattern as RePattern};
#[cfg(feature = "std")]
pub use socket::{socket, Socket};
#[cfg(feature = "std")]
pub use subprocess::{subprocess, ChildScript, CompletedProcess};
pub use threading::*;

use super::args::Args;
use super::errors::{LailaError, Result};
use super::object::{Arg, IntoArg, PyArg};

/// `import builtins` -- the module as a value (see `builtins::builtins_mod`).
pub use super::builtins::builtins;
/// `import pytest` -- the rewriter resolves bare module names here.
pub use super::unittest::pytest;
/// `pytest.<name>(..)` as a value (`pytest.fail`, `pytest.approx`, fixtures).
#[allow(non_upper_case_globals)]
pub static pytest: super::unittest::pytest::pytest_mod = super::unittest::pytest::pytest_mod;

/// `import uuid`
#[derive(Clone, Copy, Debug, Default)]
pub struct uuid_mod;
pub static uuid: uuid_mod = uuid_mod;
impl uuid_mod {
    /// `uuid.uuid4()` -> a random `UUID` (`str()` / `{}` is the hyphenated form).
    pub fn uuid4(&self) -> UUID {
        UUID {
            bytes: crate::_compat::uuid::parse(&crate::_compat::uuid::uuid4()).unwrap_or([0; 16]),
        }
    }
    /// `uuid.uuid5(namespace, name)` -> a `UUID` object (`str()` of it is the
    /// hyphenated form). `namespace` is a `UUID` or its string form.
    pub fn uuid5(&self, namespace: impl IntoArg, name: impl IntoArg) -> Result<UUID> {
        let ns = UUID::from_arg(&namespace.into_arg())?;
        let name = match name.into_arg() {
            Arg::Str(s) => s,
            Arg::Bytes(b) => String::from_utf8_lossy(&b).into(),
            other => {
                return Err(LailaError::type_error(alloc::format!(
                    "a bytes-like object is required, not '{}'",
                    other.type_name()
                )))
            }
        };
        let text = crate::_compat::uuid::uuid5(&ns.bytes, &name);
        Ok(UUID {
            bytes: crate::_compat::uuid::parse(&text).unwrap_or([0; 16]),
        })
    }
    /// `uuid.NAMESPACE_DNS`
    #[allow(non_snake_case)]
    pub fn NAMESPACE_DNS(&self) -> UUID {
        UUID {
            bytes: crate::_compat::uuid::NAMESPACE_DNS,
        }
    }
    /// `uuid.UUID(hex)` -- the class is also reachable as the type `uuid.UUID`.
    #[allow(non_snake_case)]
    pub fn UUID(&self, args: impl Into<Args>) -> Result<UUID> {
        UUID::new(args)
    }
}

/// `uuid.UUID`: an immutable 128-bit identifier. `str()` is the hyphenated
/// form, `repr()` is `UUID('...')`, equality and hashing follow the bytes.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[allow(clippy::upper_case_acronyms)]
pub struct UUID {
    pub bytes: [u8; 16],
}

impl UUID {
    /// `UUID(hex)` / `UUID(hex=..)` / `UUID(bytes=..)`.
    pub fn new(args: impl Into<Args>) -> Result<UUID> {
        let a: Args = args.into();
        let hex = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("hex"));
        if !hex.is_none() {
            let text = hex.__str__();
            let cleaned: String = text
                .trim_start_matches("urn:")
                .trim_start_matches("uuid:")
                .chars()
                .filter(|c| *c != '{' && *c != '}' && *c != '-')
                .collect();
            if cleaned.len() != 32 || !cleaned.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(LailaError::value_error(
                    "badly formed hexadecimal UUID string",
                ));
            }
            let mut bytes = [0u8; 16];
            for (i, b) in bytes.iter_mut().enumerate() {
                *b = u8::from_str_radix(&cleaned[2 * i..2 * i + 2], 16).unwrap_or(0);
            }
            return Ok(UUID { bytes });
        }
        let raw = a.get("bytes");
        if let Some(b) = raw.as_bytes_like() {
            if b.len() != 16 {
                return Err(LailaError::value_error("bytes is not a 16-char string"));
            }
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&b);
            return Ok(UUID { bytes });
        }
        Err(LailaError::type_error(
            "one of the hex, bytes, bytes_le, fields, or int arguments must be given",
        ))
    }

    /// A `UUID` from a `UUID` value or its hyphenated string form.
    pub fn from_arg(a: &Arg) -> Result<UUID> {
        if let Some(u) = a.downcast_ref::<UUID>() {
            return Ok(*u);
        }
        match a {
            Arg::Str(s) => match crate::_compat::uuid::parse(s) {
                Some(bytes) => Ok(UUID { bytes }),
                None => Err(LailaError::value_error(
                    "badly formed hexadecimal UUID string",
                )),
            },
            other => Err(LailaError::type_error(alloc::format!(
                "'{}' object has no attribute 'bytes'",
                other.type_name()
            ))),
        }
    }

    /// `str(u)`: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`.
    pub fn __str__(&self) -> String {
        let b = &self.bytes;
        alloc::format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]
        )
    }
    /// `u.hex`
    pub fn hex(&self) -> String {
        self.__str__().chars().filter(|c| *c != '-').collect()
    }
    /// `u.bytes`
    pub fn bytes(&self) -> Arg {
        Arg::Bytes(self.bytes.as_slice().into())
    }
    /// `u.version`
    pub fn version(&self) -> Arg {
        if self.bytes[8] & 0xc0 == 0x80 {
            Arg::Int((self.bytes[6] >> 4) as i64)
        } else {
            Arg::None
        }
    }
}

impl core::fmt::Debug for UUID {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "UUID('{}')", self.__str__())
    }
}
impl core::fmt::Display for UUID {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__str__())
    }
}

impl super::object::PyObject for UUID {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "UUID"
    }
    fn __repr__(&self) -> String {
        alloc::format!("UUID('{}')", self.__str__())
    }
    fn __str__(&self) -> String {
        UUID::__str__(self)
    }
    fn __eq__(&self, other: &dyn super::object::PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<UUID>()
            .is_some_and(|o| o.bytes == self.bytes)
    }
    fn __hash__(&self) -> Result<i64> {
        Arg::Bytes(self.bytes.as_slice().into()).__hash__()
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "hex" => Ok(Arg::Str(self.hex().into())),
            "bytes" => Ok(self.bytes()),
            "version" => Ok(self.version()),
            "urn" => Ok(Arg::Str(
                alloc::format!("urn:uuid:{}", self.__str__()).into(),
            )),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'UUID' object has no attribute '{other}'"
            ))),
        }
    }
}
impl IntoArg for UUID {
    fn into_arg(self) -> Arg {
        Arg::Object(alloc::sync::Arc::new(self))
    }
}
impl super::object::PyArg for UUID {
    fn to_arg(&self) -> Arg {
        Arg::Object(alloc::sync::Arc::new(*self))
    }
}
impl super::builtins::IsInstance for UUID {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<UUID>())
    }
}

/// `collections.Counter(iterable)`: the element counts as an
/// insertion-ordered dict (`Counter` is a `dict` subclass in Python).
///
/// Only the counting constructor is ported; the `__missing__ -> 0` lookup of
/// absent keys is not (a plain dict raises `KeyError`).
#[allow(non_camel_case_types)]
pub struct Counter {}
impl Counter {
    /// `Counter()` / `Counter(iterable)` / `Counter(mapping)` / `Counter(**kw)`.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let out = super::builtins::dict(Args::new())?;
        if let Some(src) = a.positional.first() {
            match src {
                Arg::Dict(_) => {
                    for k in src.__iter__()? {
                        let v = src.__getitem__(k.clone())?;
                        out.__setitem__(k, v)?;
                    }
                }
                other => {
                    for item in other.__iter__()? {
                        item.__hash__()?;
                        let n = match out.__contains__(item.clone())? {
                            true => out.__getitem__(item.clone())?.as_i64().unwrap_or(0) + 1,
                            false => 1,
                        };
                        out.__setitem__(item, Arg::Int(n))?;
                    }
                }
            }
        }
        for (k, v) in a.keywords {
            out.__setitem__(Arg::Str(k.into()), v)?;
        }
        Ok(out)
    }
}

/// `import json` -- `_compat::pyjson` (byte-identical to CPython's `json`).
#[derive(Clone, Copy, Debug, Default)]
pub struct json_mod;
pub static json: json_mod = json_mod;
impl json_mod {
    /// `json.dumps(obj, *, skipkeys=False, ensure_ascii=True, check_circular=True, allow_nan=True, cls=None, indent=None, separators=None, default=None, sort_keys=False, **kw)`
    #[laila_macros::pysig("dumps(obj, *, skipkeys=False, ensure_ascii=True, check_circular=True, allow_nan=True, cls=None, indent=None, separators=None, default=None, sort_keys=False, **kw)")]
    pub fn dumps(&self, args: impl Into<Args>) -> Result<String> {
        let _ = (skipkeys, check_circular, cls, kw);
        let mut opts = crate::_compat::pyjson::DumpsOptions {
            ensure_ascii: ensure_ascii.__bool__(),
            allow_nan: allow_nan.__bool__(),
            sort_keys: sort_keys.__bool__(),
            ..Default::default()
        };
        if !indent.is_none() {
            opts.indent = Some(indent);
        }
        if !separators.is_none() {
            let parts = separators.__iter__()?;
            if parts.len() != 2 {
                return Err(LailaError::value_error(
                    "too many values to unpack (expected 2)",
                ));
            }
            opts.separators = Some((parts[0].__str__(), parts[1].__str__()));
        }
        opts.default = match default {
            Arg::None => None,
            Arg::Callable(c) => Some(c),
            // `default=str`: a class object (or any callable object) is
            // called like a function.
            other if super::builtins::is_callable(&other) => {
                Some(alloc::sync::Arc::new(move |a: Args| other.__call__(a)))
            }
            other => {
                return Err(LailaError::type_error(alloc::format!(
                    "'{}' object is not callable",
                    other.type_name()
                )))
            }
        };
        crate::_compat::pyjson::dumps(&obj, &opts)
    }
    /// `json.dump(obj, fp, **kw)`: `fp.write(json.dumps(obj, **kw))`.
    pub fn dump(&self, args: impl Into<Args>) -> Result<()> {
        let mut a: Args = args.into();
        if a.positional.len() < 2 {
            return Err(LailaError::type_error(alloc::format!(
                "dump() missing {} required positional argument{}: {}",
                2 - a.positional.len(),
                if a.positional.len() == 1 { "" } else { "s" },
                if a.positional.is_empty() {
                    "'obj' and 'fp'"
                } else {
                    "'fp'"
                }
            )));
        }
        let fp = a.positional.remove(1);
        let text = self.dumps(a)?;
        fp.__call_method__("write", Args::from(Arg::Str(text.into())))?;
        Ok(())
    }
    /// `json.load(fp, **kw)`: `json.loads(fp.read(), **kw)`.
    pub fn load(&self, args: impl Into<Args>) -> Result<Arg> {
        let mut a: Args = args.into();
        if a.positional.is_empty() {
            return Err(LailaError::type_error(
                "load() missing 1 required positional argument: 'fp'",
            ));
        }
        let fp = a.positional.remove(0);
        let text = fp.__call_method__("read", Args::new())?;
        a.positional.insert(0, text);
        self.loads(a)
    }
    /// `json.loads(s, *, cls=None, object_hook=None, parse_float=None, parse_int=None, parse_constant=None, object_pairs_hook=None, **kw)`
    #[laila_macros::pysig("loads(s, *, cls=None, object_hook=None, parse_float=None, parse_int=None, parse_constant=None, object_pairs_hook=None, **kw)")]
    pub fn loads(&self, args: impl Into<Args>) -> Result<Arg> {
        let _ = (
            cls,
            object_hook,
            parse_float,
            parse_int,
            parse_constant,
            object_pairs_hook,
            kw,
        );
        match &s {
            Arg::Str(t) => crate::_compat::pyjson::loads(t),
            Arg::Bytes(_) | Arg::ByteArray(_) => {
                crate::_compat::pyjson::loads_bytes(&s.as_bytes_like().unwrap_or_default())
            }
            other => Err(LailaError::type_error(alloc::format!(
                "the JSON object must be str, bytes or bytearray, not {}",
                other.type_name()
            ))),
        }
    }
}

/// `import copy`
#[derive(Clone, Copy, Debug, Default)]
pub struct copy_mod;
pub static copy: copy_mod = copy_mod;
impl copy_mod {
    /// `copy.copy(x)`: a shallow copy of a container; everything else is
    /// shared (immutable or identity-bearing).
    pub fn copy(&self, x: impl IntoArg) -> Arg {
        let x = x.into_arg();
        match &x {
            Arg::List(_) | Arg::Dict(_) | Arg::Set(_) | Arg::ByteArray(_) => {
                x.__call_method__("copy", Args::new()).unwrap_or(x)
            }
            Arg::Object(o) => o.__copy__().unwrap_or(x),
            _ => x,
        }
    }
    /// `copy.deepcopy(x)`: containers are copied recursively (Python-shaped
    /// like the free [`deepcopy`], so both spellings lower the same way).
    pub fn deepcopy(&self, args: impl Into<Args>) -> Result<Arg> {
        deepcopy(args)
    }
}
/// `from copy import deepcopy` -- the function imported by name.
pub fn deepcopy(args: impl Into<Args>) -> Result<Arg> {
    let a: Args = args.into();
    let x = a.positional.first().cloned().ok_or_else(|| {
        LailaError::type_error("deepcopy() missing 1 required positional argument: 'x'")
    })?;
    Ok(x.deepcopy())
}

/// `import gc` -- Rust frees on the last drop, so `gc.collect()` has
/// nothing to collect: the `del x` before it already ran the finalizers.
#[derive(Clone, Copy, Debug, Default)]
pub struct gc_mod;
pub static gc: gc_mod = gc_mod;
impl gc_mod {
    /// `gc.collect()` -> the number of unreachable objects found (`0`).
    pub fn collect(&self) -> i64 {
        0
    }
}

/// `import functools`
#[derive(Clone, Copy, Debug, Default)]
pub struct functools_mod;
pub static functools: functools_mod = functools_mod;
impl functools_mod {
    /// `functools.reduce(function, iterable[, initial])`
    pub fn reduce(&self, args: impl Into<Args>) -> Result<Arg> {
        extras::reduce(args)
    }
    /// `functools.partial(fn, *args, **kwargs)`: a callable that calls
    /// *fn* with the bound positionals first and the bound keywords
    /// (overridable at call time).
    pub fn partial(&self, args: impl Into<Args>) -> Result<Arg> {
        let mut a: Args = args.into();
        if a.positional.is_empty() {
            return Err(LailaError::type_error(
                "type 'partial' takes at least one argument",
            ));
        }
        let f = a.positional.remove(0);
        if !crate::python::builtins::is_callable(&f) {
            return Err(LailaError::type_error(
                "the first argument must be callable",
            ));
        }
        let bound = a;
        Ok(Arg::Callable(alloc::sync::Arc::new(
            move |call: Args| -> Result<Arg> {
                let mut full = Args::new();
                for p in bound.positional.iter().chain(call.positional.iter()) {
                    full.push_positional(p.clone());
                }
                for (k, v) in bound.keywords.iter() {
                    full.push_keyword(k, v.clone());
                }
                for (k, v) in call.keywords.iter() {
                    full.push_keyword(k, v.clone());
                }
                f.__call__(full)
            },
        )))
    }
}

/// `import time`
#[derive(Clone, Copy, Debug, Default)]
pub struct time_mod;
pub static time: time_mod = time_mod;
impl time_mod {
    #[cfg(feature = "std")]
    pub fn time(&self) -> f64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0)
    }
    #[cfg(feature = "std")]
    pub fn sleep(&self, secs: impl IntoArg) -> Result<()> {
        let s = secs.into_arg().as_f64().ok_or_else(|| {
            LailaError::type_error("'str' object cannot be interpreted as an integer")
        })?;
        std::thread::sleep(core::time::Duration::from_secs_f64(s.max(0.0)));
        Ok(())
    }
    #[cfg(feature = "std")]
    pub fn monotonic(&self) -> f64 {
        static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
        START
            .get_or_init(std::time::Instant::now)
            .elapsed()
            .as_secs_f64()
    }
    #[cfg(feature = "std")]
    pub fn perf_counter(&self) -> f64 {
        self.monotonic()
    }
    #[cfg(feature = "std")]
    pub fn time_ns(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0)
    }
    #[cfg(feature = "std")]
    pub fn perf_counter_ns(&self) -> i64 {
        (self.monotonic() * 1e9) as i64
    }
    #[cfg(feature = "std")]
    pub fn monotonic_ns(&self) -> i64 {
        self.perf_counter_ns()
    }
}

/// Python `str` methods on Rust strings (`s.startswith(..)`, `s.split(..)`, ...).
pub trait PyStrExt {
    fn as_pystr(&self) -> &str;
    fn startswith(&self, prefix: impl IntoArg) -> bool {
        let p = prefix.into_arg();
        match &p {
            Arg::Tuple(t) => t.iter().any(|x| self.as_pystr().starts_with(&*x.__str__())),
            other => self.as_pystr().starts_with(&*other.__str__()),
        }
    }
    fn endswith(&self, suffix: impl IntoArg) -> bool {
        let p = suffix.into_arg();
        match &p {
            Arg::Tuple(t) => t.iter().any(|x| self.as_pystr().ends_with(&*x.__str__())),
            other => self.as_pystr().ends_with(&*other.__str__()),
        }
    }
    fn upper(&self) -> String {
        self.as_pystr().to_uppercase()
    }
    fn lower(&self) -> String {
        self.as_pystr().to_lowercase()
    }
    fn strip(&self) -> String {
        self.as_pystr().trim().into()
    }
    fn split(&self, sep: impl Into<Args>) -> Arg {
        let a: Args = sep.into();
        let sep = a.positional.first().cloned().unwrap_or(Arg::None);
        let parts: alloc::vec::Vec<Arg> = match &sep {
            Arg::None => self
                .as_pystr()
                .split_whitespace()
                .map(|p| Arg::Str(p.into()))
                .collect(),
            s => self
                .as_pystr()
                .split(&*s.__str__())
                .map(|p| Arg::Str(p.into()))
                .collect(),
        };
        parts.into_arg()
    }
    fn join(&self, items: impl Into<Args>) -> Result<String> {
        let a: Args = items.into();
        let parts: alloc::vec::Vec<String> = a
            .positional
            .first()
            .cloned()
            .unwrap_or(Arg::None)
            .__iter__()?
            .iter()
            .map(|x| x.__str__())
            .collect();
        Ok(parts.join(self.as_pystr()))
    }
    fn replace(&self, old: impl IntoArg, new: impl IntoArg) -> String {
        self.as_pystr()
            .replace(&*old.into_arg().__str__(), &new.into_arg().__str__())
    }
    fn count(&self, sub: impl IntoArg) -> usize {
        let s = sub.into_arg().__str__();
        if s.is_empty() {
            self.as_pystr().chars().count() + 1
        } else {
            self.as_pystr().matches(&*s).count()
        }
    }
    fn find(&self, sub: impl IntoArg) -> i64 {
        let s = sub.into_arg().__str__();
        self.as_pystr()
            .find(&*s)
            .map(|b| self.as_pystr()[..b].chars().count() as i64)
            .unwrap_or(-1)
    }
    fn encode(&self) -> Arg {
        Arg::Bytes(self.as_pystr().as_bytes().into())
    }
    /// `s.index(sub[, start[, end]])`: like `find` but `ValueError` when absent.
    fn index(&self, args: impl Into<Args>) -> Result<i64> {
        let a: Args = args.into();
        match Arg::Str(self.as_pystr().into()).index(a)? {
            Arg::Int(i) => Ok(i),
            _ => Err(LailaError::value_error("substring not found")),
        }
    }
    fn isdigit(&self) -> bool {
        !self.as_pystr().is_empty() && self.as_pystr().chars().all(|c| c.is_ascii_digit())
    }
    /// `s.zfill(width)`: left-pad with `0` to `width` (a sign stays first).
    fn zfill(&self, width: impl IntoArg) -> String {
        let s = self.as_pystr();
        let width = match width.into_arg() {
            Arg::Int(i) => i.max(0) as usize,
            Arg::Bool(b) => b as usize,
            _ => 0,
        };
        let n = s.chars().count();
        if n >= width {
            return s.into();
        }
        let pad: String = core::iter::repeat_n('0', width - n).collect();
        match s.chars().next() {
            Some(c @ ('+' | '-')) => alloc::format!("{c}{pad}{}", &s[1..]),
            _ => alloc::format!("{pad}{s}"),
        }
    }
    /// `s[i]` / `s[a:b]` on a Rust string (CPython `IndexError` text).
    fn __getitem__(&self, key: impl IntoArg) -> Result<Arg> {
        Arg::Str(self.as_pystr().into()).__getitem__(key)
    }
    /// `s.partition(sep)` -> `(head, sep, tail)` (`(s, "", "")` when absent).
    fn partition(&self, sep: impl IntoArg) -> Arg {
        let sep = sep.into_arg().__str__();
        let s = self.as_pystr();
        let (head, mid, tail) = match s.find(&*sep) {
            Some(i) => (&s[..i], sep.as_str(), &s[i + sep.len()..]),
            None => (s, "", ""),
        };
        Arg::Tuple(
            alloc::vec![
                Arg::Str(head.into()),
                Arg::Str(mid.into()),
                Arg::Str(tail.into())
            ]
            .into(),
        )
    }
}
impl PyStrExt for str {
    fn as_pystr(&self) -> &str {
        self
    }
}
impl PyStrExt for String {
    fn as_pystr(&self) -> &str {
        self
    }
}
impl PyStrExt for alloc::sync::Arc<str> {
    fn as_pystr(&self) -> &str {
        self
    }
}

/// Python `bytes` methods on Rust byte literals / slices (`b"".join(..)`,
/// `b"\x00".startswith(..)`).
pub trait PyBytesExt {
    fn as_pybytes(&self) -> &[u8];
    /// `sep.join(iterable_of_bytes)`.
    fn join(&self, items: impl Into<Args>) -> Result<Arg> {
        let a: Args = items.into();
        let parts = a
            .positional
            .first()
            .cloned()
            .unwrap_or(Arg::None)
            .__iter__()?;
        let sep = self.as_pybytes();
        let mut out: alloc::vec::Vec<u8> = alloc::vec::Vec::new();
        for (i, p) in parts.iter().enumerate() {
            let b = p.as_bytes_like().ok_or_else(|| {
                LailaError::type_error(alloc::format!(
                    "sequence item {i}: expected a bytes-like object, {} found",
                    p.type_name()
                ))
            })?;
            if i > 0 {
                out.extend_from_slice(sep);
            }
            out.extend_from_slice(&b);
        }
        Ok(Arg::Bytes(out.into()))
    }
    fn startswith(&self, prefix: impl IntoArg) -> bool {
        let p = prefix.into_arg();
        let one = |x: &Arg| {
            x.as_bytes_like()
                .is_some_and(|b| self.as_pybytes().starts_with(&b))
        };
        match &p {
            Arg::Tuple(t) => t.iter().any(one),
            other => one(other),
        }
    }
    fn endswith(&self, suffix: impl IntoArg) -> bool {
        let p = suffix.into_arg();
        let one = |x: &Arg| {
            x.as_bytes_like()
                .is_some_and(|b| self.as_pybytes().ends_with(&b))
        };
        match &p {
            Arg::Tuple(t) => t.iter().any(one),
            other => one(other),
        }
    }
    fn hex(&self) -> String {
        self.as_pybytes()
            .iter()
            .map(|b| alloc::format!("{b:02x}"))
            .collect()
    }
    fn decode(&self) -> Result<Arg> {
        Arg::Bytes(self.as_pybytes().into()).decode()
    }
}
impl PyBytesExt for [u8] {
    fn as_pybytes(&self) -> &[u8] {
        self
    }
}
impl<const N: usize> PyBytesExt for [u8; N] {
    fn as_pybytes(&self) -> &[u8] {
        self
    }
}
impl PyBytesExt for alloc::vec::Vec<u8> {
    fn as_pybytes(&self) -> &[u8] {
        self
    }
}

/// `xs[i]` / `xs[a:b]` on a typed Rust sequence of Python values (a
/// `Vec<Arg>` a hand-written twin returned): Python list indexing
/// (negative indices, slices, `IndexError`).
pub trait PyVecExt {
    fn as_pylist(&self) -> Arg;
    fn __getitem__(&self, key: impl IntoArg) -> Result<Arg> {
        self.as_pylist().__getitem__(key)
    }
}
impl<T: PyArg> PyVecExt for alloc::vec::Vec<T> {
    fn as_pylist(&self) -> Arg {
        self.iter()
            .map(PyArg::to_arg)
            .collect::<alloc::vec::Vec<Arg>>()
            .into_arg()
    }
}
impl<T: PyArg> PyVecExt for [T] {
    fn as_pylist(&self) -> Arg {
        self.iter()
            .map(PyArg::to_arg)
            .collect::<alloc::vec::Vec<Arg>>()
            .into_arg()
    }
}
/// `d.pop_next()[1]`: a typed `(key, value)` pair indexed like the tuple
/// Python returns.
impl<A: PyArg, B: PyArg> PyVecExt for (A, B) {
    fn as_pylist(&self) -> Arg {
        Arg::Tuple(alloc::vec![self.0.to_arg(), self.1.to_arg()].into())
    }
}
