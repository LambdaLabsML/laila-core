//! `import logging` -- the slice of the standard library logging hierarchy
//! that `laila.logger` and the tests use.
//!
//! - [`getLogger`](logging_mod::getLogger) returns one shared [`Logger`]
//!   per dotted name (a process-wide registry, like CPython's);
//!   `logger.log(level, msg, *args)` builds a [`LogRecord`] and hands it
//!   to every handler of the logger and of its ancestors whose level
//!   admits it (`propagate` is always on).
//! - Handlers are [`NullHandler`], [`StreamHandler`] (stderr) and
//!   [`CapturingHandler`] (keeps every record it receives -- the Rust
//!   twin of the `class _CapturingHandler(logging.Handler)` test helper).
//!   Any other `PyObject` exposing `level` and `handle(record)` works too.
//! - [`Formatter`] substitutes the `%(asctime)s` / `%(levelname)s` /
//!   `%(name)s` / `%(message)s` fields of a format string.

use alloc::collections::BTreeMap;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::python::args::Args;
use crate::python::errors::{LailaError, Result};
use crate::python::object::{Arg, IntoArg, PyArg, PyObject};
use crate::python::pysig;
use crate::python::sync::RwLock;

pub const NOTSET: i64 = 0;
pub const DEBUG: i64 = 10;
pub const INFO: i64 = 20;
pub const WARNING: i64 = 30;
pub const ERROR: i64 = 40;
pub const CRITICAL: i64 = 50;

/// `logging.getLevelName(level)` for the standard levels.
pub fn level_name(level: i64) -> String {
    match level {
        NOTSET => "NOTSET".into(),
        DEBUG => "DEBUG".into(),
        INFO => "INFO".into(),
        WARNING => "WARNING".into(),
        ERROR => "ERROR".into(),
        CRITICAL => "CRITICAL".into(),
        other => alloc::format!("Level {other}"),
    }
}

/// A level given as an int or as a name (`"DEBUG"`), `TypeError` otherwise.
pub fn level_of(v: &Arg) -> Result<i64> {
    match v {
        Arg::Int(i) => Ok(*i),
        Arg::Bool(b) => Ok(*b as i64),
        Arg::Float(f) => Ok(*f as i64),
        Arg::Str(s) => match s.to_uppercase().as_str() {
            "NOTSET" => Ok(NOTSET),
            "DEBUG" => Ok(DEBUG),
            "INFO" => Ok(INFO),
            "WARNING" | "WARN" => Ok(WARNING),
            "ERROR" => Ok(ERROR),
            "CRITICAL" | "FATAL" => Ok(CRITICAL),
            _ => Err(LailaError::value_error(alloc::format!(
                "Unknown level: '{s}'"
            ))),
        },
        other => Err(LailaError::type_error(alloc::format!(
            "Level not an integer or a valid string: {}",
            other.__repr__()
        ))),
    }
}

fn now_secs() -> f64 {
    #[cfg(feature = "std")]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0)
    }
    #[cfg(not(feature = "std"))]
    {
        0.0
    }
}

// ---------------------------------------------------------------------------
// LogRecord

/// `logging.LogRecord` -- what a handler receives.
#[derive(Clone)]
pub struct LogRecord {
    pub name: String,
    pub levelno: i64,
    pub msg: Arg,
    pub args: Vec<Arg>,
    pub created: f64,
}

impl LogRecord {
    /// `record.getMessage()`: `msg % args` (the message itself when there
    /// are no arguments).
    pub fn getMessage(&self) -> Result<String> {
        let msg = self.msg.__str__();
        if self.args.is_empty() {
            return Ok(msg);
        }
        let values = Arg::Tuple(Arc::from(self.args.clone()));
        crate::python::strfmt::percent_format(&msg, &values)
    }
    /// `record.levelname`
    pub fn levelname(&self) -> String {
        level_name(self.levelno)
    }
}

impl PyObject for LogRecord {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "LogRecord"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<LogRecord: {}, {}, {}>",
            self.name,
            self.levelno,
            self.msg.__repr__()
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        core::ptr::eq(
            self as *const _ as *const (),
            other.as_any() as *const _ as *const (),
        )
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(self as *const _ as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "name" => Ok(Arg::Str(self.name.as_str().into())),
            "levelno" => Ok(Arg::Int(self.levelno)),
            "levelname" => Ok(Arg::Str(self.levelname().into())),
            "msg" => Ok(self.msg.clone()),
            "args" => Ok(Arg::Tuple(Arc::from(self.args.clone()))),
            "created" => Ok(Arg::Float(self.created)),
            "message" => Ok(Arg::Str(self.getMessage()?.into())),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'LogRecord' object has no attribute '{other}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, _args: Args) -> Result<Arg> {
        match name {
            "getMessage" => Ok(Arg::Str(self.getMessage()?.into())),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'LogRecord' object has no attribute '{other}'"
            ))),
        }
    }
    fn __bool__(&self) -> bool {
        true
    }
}
impl IntoArg for Formatter {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for Formatter {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl crate::python::builtins::IsInstance for Formatter {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<Formatter>())
    }
}
impl IntoArg for LogRecord {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for LogRecord {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl crate::python::builtins::IsInstance for LogRecord {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<LogRecord>())
    }
}

// ---------------------------------------------------------------------------
// Formatter

/// `logging.Formatter(fmt=None)`.
#[derive(Clone)]
pub struct Formatter {
    pub fmt: String,
}

impl Formatter {
    /// `Formatter(fmt=None)`; the default is `%(message)s`.
    #[pysig("Formatter(fmt=None, datefmt=None, style='%')")]
    pub fn new(args: impl Into<Args>) -> Result<Formatter> {
        let _ = (datefmt, style);
        let fmt = if fmt.is_none() {
            String::from("%(message)s")
        } else {
            fmt.__str__()
        };
        Ok(Formatter { fmt })
    }
    /// `formatter.format(record)`
    pub fn format(&self, record: &LogRecord) -> Result<String> {
        let asctime = crate::_compat::datetime::DateTime::now_iso_ms_from(record.created)
            .unwrap_or_default()
            .replace('T', " ");
        Ok(self
            .fmt
            .replace("%(asctime)s", &asctime)
            .replace("%(levelname)s", &record.levelname())
            .replace("%(levelno)s", &record.levelno.to_string())
            .replace("%(name)s", &record.name)
            .replace("%(message)s", &record.getMessage()?))
    }
}

impl PyObject for Formatter {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "Formatter"
    }
    fn __repr__(&self) -> String {
        alloc::format!("<logging.Formatter {:?}>", self.fmt)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Formatter>()
            .is_some_and(|o| o.fmt == self.fmt)
    }
    fn __hash__(&self) -> Result<i64> {
        crate::python::builtins::hash(Arg::Str(self.fmt.as_str().into()))
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "format" => {
                let rec = args.positional.first().cloned().unwrap_or(Arg::None);
                let rec = record_of(&rec)?;
                Ok(Arg::Str(self.format(&rec)?.into()))
            }
            other => Err(LailaError::attribute_error(alloc::format!(
                "'Formatter' object has no attribute '{other}'"
            ))),
        }
    }
    fn __bool__(&self) -> bool {
        true
    }
}

fn record_of(a: &Arg) -> Result<LogRecord> {
    match a {
        Arg::Object(o) => o
            .as_any()
            .downcast_ref::<LogRecord>()
            .cloned()
            .ok_or_else(|| LailaError::type_error("expected a LogRecord")),
        _ => Err(LailaError::type_error("expected a LogRecord")),
    }
}

// ---------------------------------------------------------------------------
// Handlers

/// The state every handler carries (`Handler.__init__`): a level and an
/// optional formatter.
#[derive(Default)]
pub struct HandlerState {
    level: RwLock<i64>,
    formatter: RwLock<Option<Formatter>>,
}

/// What a handler does with an admitted record.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Sink {
    Null,
    Stream,
    Capture,
}

/// One handler object; [`NullHandler`], [`StreamHandler`] and
/// [`CapturingHandler`] are the three constructors (and `isinstance`
/// markers).
pub struct HandlerObject {
    sink: Sink,
    state: HandlerState,
    records: RwLock<Vec<Arg>>,
    /// `logging.StreamHandler(stream)`: a text stream with a `write`
    /// method (`io.StringIO()`); `None` writes to stderr.
    stream: Option<Arg>,
}

impl HandlerObject {
    fn with_stream(sink: Sink, stream: Option<Arg>) -> Arg {
        Arg::Object(Arc::new(HandlerObject {
            sink,
            state: HandlerState::default(),
            records: RwLock::new(Vec::new()),
            stream,
        }))
    }
    fn downcast(a: &Arg) -> Option<&HandlerObject> {
        match a {
            Arg::Object(o) => o.as_any().downcast_ref::<HandlerObject>(),
            _ => None,
        }
    }
    /// `handler.level`
    pub fn level(&self) -> i64 {
        *self.state.level.read()
    }
    /// `handler.setLevel(level)`
    pub fn setLevel(&self, level: &Arg) -> Result<()> {
        *self.state.level.write() = level_of(level)?;
        Ok(())
    }
    /// `handler.setFormatter(fmt)`
    pub fn setFormatter(&self, f: &Arg) -> Result<()> {
        let f = match f {
            Arg::None => None,
            Arg::Object(o) => o.as_any().downcast_ref::<Formatter>().cloned(),
            _ => None,
        };
        *self.state.formatter.write() = f;
        Ok(())
    }
    /// `handler.format(record)`
    pub fn format(&self, record: &LogRecord) -> Result<String> {
        match &*self.state.formatter.read() {
            Some(f) => f.format(record),
            None => record.getMessage(),
        }
    }
    /// `handler.emit(record)`
    pub fn emit(&self, record: &LogRecord) -> Result<()> {
        match self.sink {
            Sink::Null => {}
            Sink::Stream => {
                let line = self.format(record)?;
                match &self.stream {
                    Some(stream) => {
                        stream.__call_method__(
                            "write",
                            Args::from(Arg::Str(alloc::format!("{line}\n").into())),
                        )?;
                    }
                    None => crate::python::io::write_stderr(&alloc::format!("{line}\n")),
                }
            }
            Sink::Capture => self.records.write().push(record.clone().into_arg()),
        }
        Ok(())
    }
    /// `handler.handle(record)`: the level filter, then `emit`.
    pub fn handle(&self, record: &LogRecord) -> Result<bool> {
        if record.levelno < self.level() {
            return Ok(false);
        }
        self.emit(record)?;
        Ok(true)
    }
    /// `handler.records` (capturing handler): every record received.
    pub fn records(&self) -> Arg {
        self.records.read().clone().into_arg()
    }
}

impl PyObject for HandlerObject {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        match self.sink {
            Sink::Null => "NullHandler",
            Sink::Stream => "StreamHandler",
            Sink::Capture => "CapturingHandler",
        }
    }
    fn __repr__(&self) -> String {
        alloc::format!("<{} ({})>", self.type_name(), level_name(self.level()))
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        core::ptr::eq(
            self as *const HandlerObject as *const (),
            other.as_any() as *const dyn core::any::Any as *const (),
        )
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(self as *const HandlerObject as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "level" => Ok(Arg::Int(self.level())),
            "formatter" => Ok(match &*self.state.formatter.read() {
                Some(f) => f.clone().into_arg(),
                None => Arg::None,
            }),
            "records" => Ok(self.records()),
            "name" => Ok(Arg::None),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'{}' object has no attribute '{other}'",
                self.type_name()
            ))),
        }
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let first = || args.positional.first().cloned().unwrap_or(Arg::None);
        match name {
            "setLevel" => self.setLevel(&first()).map(|()| Arg::None),
            "setFormatter" => self.setFormatter(&first()).map(|()| Arg::None),
            "format" => Ok(Arg::Str(self.format(&record_of(&first())?)?.into())),
            "emit" => self.emit(&record_of(&first())?).map(|()| Arg::None),
            "handle" => self.handle(&record_of(&first())?).map(Arg::Bool),
            "close" | "flush" | "acquire" | "release" => Ok(Arg::None),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'{}' object has no attribute '{other}'",
                self.type_name()
            ))),
        }
    }
    fn __bool__(&self) -> bool {
        true
    }
}

macro_rules! handler_ctor {
    ($(#[$m:meta])* $name:ident, $sink:expr) => {
        $(#[$m])*
        #[derive(Clone, Copy, Debug, Default)]
        pub struct $name;
        impl $name {
            /// The handler object; `StreamHandler(stream)` / `StreamHandler(stream=..)`
            /// writes to that stream (anything with a `write` method).
            #[allow(clippy::new_ret_no_self)]
            pub fn new(args: impl Into<Args>) -> Result<Arg> {
                let a: Args = args.into();
                let stream = match $sink {
                    Sink::Stream => a
                        .positional
                        .first()
                        .cloned()
                        .or_else(|| a.keywords.iter().find(|(k, _)| k == "stream").map(|(_, v)| v.clone()))
                        .filter(|s| !s.is_none()),
                    _ => None,
                };
                Ok(HandlerObject::with_stream($sink, stream))
            }
        }
        impl crate::python::builtins::IsInstance for $name {
            fn isinstance(x: &Arg) -> bool {
                HandlerObject::downcast(x).is_some_and(|h| h.sink == $sink)
            }
        }
        impl crate::python::builtins::CallValue for $name {
            fn call_value(&self, args: Args) -> Result<Arg> {
                Self::new(args)
            }
        }
        impl PyArg for $name {
            fn to_arg(&self) -> Arg {
                crate::python::IntoArg::into_arg(crate::python::PyClass::with_ctor(
                    stringify!($name),
                    "logging",
                    Arc::new(|a: Args| Self::new(a)),
                ))
            }
        }
        impl IntoArg for $name {
            fn into_arg(self) -> Arg {
                self.to_arg()
            }
        }
    };
}

handler_ctor!(
    /// `logging.NullHandler()`: drops every record.
    NullHandler,
    Sink::Null
);
handler_ctor!(
    /// `logging.StreamHandler()`: writes formatted records to stderr.
    StreamHandler,
    Sink::Stream
);
handler_ctor!(
    /// A handler that stores every record it receives under `.records`
    /// (the stdlib has none; tests subclass `logging.Handler` for it).
    CapturingHandler,
    Sink::Capture
);

// ---------------------------------------------------------------------------
// Logger

struct LoggerInner {
    name: String,
    level: RwLock<i64>,
    handlers: RwLock<Vec<Arg>>,
    propagate: RwLock<bool>,
}

/// `logging.Logger` -- a handle; `getLogger(name)` always returns the same one.
#[derive(Clone)]
pub struct Logger(Arc<LoggerInner>);

static REGISTRY: RwLock<Option<BTreeMap<String, Logger>>> = RwLock::new(None);

impl Logger {
    fn fresh(name: &str) -> Logger {
        Logger(Arc::new(LoggerInner {
            name: name.into(),
            level: RwLock::new(if name.is_empty() { WARNING } else { NOTSET }),
            handlers: RwLock::new(Vec::new()),
            propagate: RwLock::new(true),
        }))
    }
    /// `logging.getLogger(name)`: the registered logger, created on first use.
    pub fn get(name: &str) -> Logger {
        let mut reg = REGISTRY.write();
        let map = reg.get_or_insert_with(BTreeMap::new);
        map.entry(name.into())
            .or_insert_with(|| Logger::fresh(name))
            .clone()
    }
    /// `logger.name`
    pub fn name(&self) -> &str {
        &self.0.name
    }
    /// `logger.level`
    pub fn level(&self) -> i64 {
        *self.0.level.read()
    }
    /// `logger.setLevel(level)`
    pub fn setLevel(&self, level: impl IntoArg) -> Result<()> {
        *self.0.level.write() = level_of(&level.into_arg())?;
        Ok(())
    }
    /// `logger.getEffectiveLevel()`: the first explicit level up the chain.
    pub fn getEffectiveLevel(&self) -> i64 {
        let mut name = self.0.name.clone();
        loop {
            let l = Logger::get(&name);
            let lv = l.level();
            if lv != NOTSET || name.is_empty() {
                return lv;
            }
            name = match name.rfind('.') {
                Some(i) => name[..i].to_string(),
                None => String::new(),
            };
        }
    }
    /// `logger.isEnabledFor(level)`
    pub fn isEnabledFor(&self, level: i64) -> bool {
        level >= self.getEffectiveLevel()
    }
    /// `logger.handlers` -- a list snapshot.
    pub fn handlers(&self) -> Arg {
        self.0.handlers.read().clone().into_arg()
    }
    /// `logger.hasHandlers()`
    pub fn hasHandlers(&self) -> bool {
        !self.0.handlers.read().is_empty()
    }
    /// `logger.addHandler(h)` (no duplicates, like CPython).
    pub fn addHandler(&self, h: impl IntoArg) -> Result<()> {
        let h = h.into_arg();
        let mut hs = self.0.handlers.write();
        if !hs.iter().any(|x| x.is(&h)) {
            hs.push(h);
        }
        Ok(())
    }
    /// `logger.removeHandler(h)` (a missing handler is silently ignored).
    pub fn removeHandler(&self, h: impl IntoArg) -> Result<()> {
        let h = h.into_arg();
        self.0.handlers.write().retain(|x| !x.is(&h));
        Ok(())
    }
    /// `logger.log(level, msg, *args)`: build the record and dispatch it
    /// through this logger's and its ancestors' handlers.
    pub fn log(&self, level: i64, msg: impl IntoArg, args: Vec<Arg>) -> Result<()> {
        if !self.isEnabledFor(level) {
            return Ok(());
        }
        let record = LogRecord {
            name: self.0.name.clone(),
            levelno: level,
            msg: msg.into_arg(),
            args,
            created: now_secs(),
        };
        self.call_handlers(&record)
    }
    fn call_handlers(&self, record: &LogRecord) -> Result<()> {
        let mut name = self.0.name.clone();
        loop {
            let l = Logger::get(&name);
            let hs: Vec<Arg> = l.0.handlers.read().clone();
            for h in hs {
                match HandlerObject::downcast(&h) {
                    Some(h) => {
                        let _ = h.handle(record);
                    }
                    None => {
                        // A foreign handler object: `level` + `handle(record)`.
                        let lv = h
                            .__getattr__("level")
                            .ok()
                            .and_then(|v| v.as_i64())
                            .unwrap_or(NOTSET);
                        if record.levelno >= lv {
                            let _ =
                                h.__call_method__("handle", Args::from(record.clone().into_arg()));
                        }
                    }
                }
            }
            if !*l.0.propagate.read() || name.is_empty() {
                return Ok(());
            }
            name = match name.rfind('.') {
                Some(i) => name[..i].to_string(),
                None => String::new(),
            };
        }
    }
    /// `logger.debug(msg, *args)`
    pub fn debug(&self, msg: impl IntoArg, args: Vec<Arg>) -> Result<()> {
        self.log(DEBUG, msg, args)
    }
    /// `logger.info(msg, *args)`
    pub fn info(&self, msg: impl IntoArg, args: Vec<Arg>) -> Result<()> {
        self.log(INFO, msg, args)
    }
    /// `logger.warning(msg, *args)`
    pub fn warning(&self, msg: impl IntoArg, args: Vec<Arg>) -> Result<()> {
        self.log(WARNING, msg, args)
    }
    /// `logger.error(msg, *args)`
    pub fn error(&self, msg: impl IntoArg, args: Vec<Arg>) -> Result<()> {
        self.log(ERROR, msg, args)
    }
    /// `logger.critical(msg, *args)`
    pub fn critical(&self, msg: impl IntoArg, args: Vec<Arg>) -> Result<()> {
        self.log(CRITICAL, msg, args)
    }
    /// Python `is`.
    pub fn is(&self, other: &Logger) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    fn level_call(&self, level: i64, args: Args) -> Result<Arg> {
        let mut it = args.positional.into_iter();
        let msg = it.next().unwrap_or(Arg::None);
        self.log(level, msg, it.collect()).map(|()| Arg::None)
    }
}

impl PyObject for Logger {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    /// `a is b`: identity of the shared handle, not of the boxing.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Logger>()
            .is_some_and(|o| self.is(o))
    }
    fn type_name(&self) -> &'static str {
        "Logger"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<Logger {} ({})>",
            self.0.name,
            level_name(self.getEffectiveLevel())
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Logger>()
            .is_some_and(|o| self.is(o))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "name" => Ok(Arg::Str(self.0.name.as_str().into())),
            "level" => Ok(Arg::Int(self.level())),
            "handlers" => Ok(self.handlers()),
            "propagate" => Ok(Arg::Bool(*self.0.propagate.read())),
            "disabled" => Ok(Arg::Bool(false)),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'Logger' object has no attribute '{other}'"
            ))),
        }
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        match name {
            "level" => self.setLevel(value),
            "propagate" => {
                *self.0.propagate.write() = crate::python::builtins::truthy(&value);
                Ok(())
            }
            other => Err(LailaError::attribute_error(alloc::format!(
                "'Logger' object has no attribute '{other}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let first = || args.positional.first().cloned().unwrap_or(Arg::None);
        match name {
            "setLevel" => self.setLevel(first()).map(|()| Arg::None),
            "getEffectiveLevel" => Ok(Arg::Int(self.getEffectiveLevel())),
            "isEnabledFor" => Ok(Arg::Bool(self.isEnabledFor(level_of(&first())?))),
            "hasHandlers" => Ok(Arg::Bool(self.hasHandlers())),
            "addHandler" => self.addHandler(first()).map(|()| Arg::None),
            "removeHandler" => self.removeHandler(first()).map(|()| Arg::None),
            "log" => {
                let mut it = args.positional.into_iter();
                let level = level_of(&it.next().unwrap_or(Arg::None))?;
                let msg = it.next().unwrap_or(Arg::None);
                self.log(level, msg, it.collect()).map(|()| Arg::None)
            }
            "debug" => self.level_call(DEBUG, args),
            "info" => self.level_call(INFO, args),
            "warning" | "warn" => self.level_call(WARNING, args),
            "error" | "exception" => self.level_call(ERROR, args),
            "critical" | "fatal" => self.level_call(CRITICAL, args),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'Logger' object has no attribute '{other}'"
            ))),
        }
    }
    fn __bool__(&self) -> bool {
        true
    }
}
impl IntoArg for Logger {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for Logger {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl crate::python::builtins::IsInstance for Logger {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<Logger>())
    }
}

// ---------------------------------------------------------------------------
// The module value

/// `import logging`
#[derive(Clone, Copy, Debug, Default)]
pub struct logging_mod;
pub static logging: logging_mod = logging_mod;

impl logging_mod {
    pub fn NOTSET(&self) -> i64 {
        NOTSET
    }
    pub fn DEBUG(&self) -> i64 {
        DEBUG
    }
    pub fn INFO(&self) -> i64 {
        INFO
    }
    pub fn WARNING(&self) -> i64 {
        WARNING
    }
    pub fn WARN(&self) -> i64 {
        WARNING
    }
    pub fn ERROR(&self) -> i64 {
        ERROR
    }
    pub fn CRITICAL(&self) -> i64 {
        CRITICAL
    }
    pub fn FATAL(&self) -> i64 {
        CRITICAL
    }
    /// `logging.getLogger(name=None)`: `None` / `""` is the root logger.
    #[pysig("getLogger(name=None)")]
    pub fn getLogger(&self, args: impl Into<Args>) -> Result<Logger> {
        let name = if name.is_none() {
            String::new()
        } else {
            name.__str__()
        };
        Ok(Logger::get(&name))
    }
    /// `logging.getLevelName(level)`
    pub fn getLevelName(&self, level: impl IntoArg) -> Result<Arg> {
        match level.into_arg() {
            Arg::Str(s) => Ok(Arg::Int(level_of(&Arg::Str(s))?)),
            other => Ok(Arg::Str(level_name(level_of(&other)?).into())),
        }
    }
    /// `logging.NullHandler()`
    pub fn NullHandler(&self, args: impl Into<Args>) -> Result<Arg> {
        NullHandler::new(args)
    }
    /// `logging.StreamHandler(stream=None)`
    pub fn StreamHandler(&self, args: impl Into<Args>) -> Result<Arg> {
        StreamHandler::new(args)
    }
    /// `logging.Formatter(fmt)`
    pub fn Formatter(&self, args: impl Into<Args>) -> Result<Formatter> {
        Formatter::new(args)
    }
    /// `logging.basicConfig(..)`: a stderr handler on the root (idempotent).
    #[pysig("basicConfig(level=None, format=None, **kwargs)")]
    pub fn basicConfig(&self, args: impl Into<Args>) -> Result<()> {
        let _ = kwargs;
        let root = Logger::get("");
        if !root.hasHandlers() {
            let h = StreamHandler::new(Args::new())?;
            if !format.is_none() {
                let f = Formatter::new(Args::from(format))?;
                HandlerObject::downcast(&h)
                    .map(|x| x.setFormatter(&f.into_arg()))
                    .transpose()?;
            }
            root.addHandler(h)?;
        }
        if !level.is_none() {
            root.setLevel(level)?;
        }
        Ok(())
    }
}
