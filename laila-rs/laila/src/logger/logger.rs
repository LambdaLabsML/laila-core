//! LAILA [`Logger`] singleton.
//!
//! The Logger is a top-level subsystem (sibling to `laila.policy`,
//! `laila.data`, `laila.entry`) that emits structured log records from
//! anywhere in the package. It has two sinks:
//!
//! 1. The standard library `logging` hierarchy rooted at the `"laila"`
//!    logger. A `StreamHandler` is attached on [`Logger::start`];
//!    downstream code can route the `"laila"` tree anywhere with stdlib
//!    idioms.
//! 2. An optional **pool sink**: when `pool_nickname` (or `pool_id`) is
//!    set, every record is wrapped in an `Entry` and written into the
//!    named pool. A thread-local recursion guard prevents the sink's own
//!    write from generating another record.
//!
//! The class enforces a **process-wide singleton**: `Logger()` always
//! returns the same instance, mirroring how `laila` exposes
//! `laila.logger` as a single global handle.
//!
//! Rust shape: [`Logger`] is the class (a pydantic model descriptor whose
//! bases are `_LAILA_CLI_CAPABLE_CLASS` and `_LAILA_IDENTIFIABLE_OBJECT`);
//! `Logger(..)` / [`Logger::new`] yields the singleton instance (`Arg`),
//! whose methods are served by the class hooks and mirrored as typed
//! associated functions taking the instance.

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::_compat::pydantic::{
    FieldInfo, LazyClass, ModelBase, ModelClass, ModelHooks, ModelInstance, PrivateAttrInfo,
};
use crate::basics::definitions::cli_capable::_LAILA_CLI_CAPABLE_CLASS;
use crate::basics::definitions::identifiable_object::_LAILA_IDENTIFIABLE_OBJECT;
use crate::macros::strings::_LOGGER_SCOPE;
use crate::python::builtins::IsInstance;
use crate::python::object::PyObject;
use crate::python::stdlib::logging as pylogging;
use crate::python::sync::RwLock;
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, PyArg, Result};

use super::record::{build_record, level_name, level_number};

/// The stdlib logger channel every laila record goes through.
pub const _LAILA_LOGGER_NAME: &str = "laila";

/// Attach a `logging.NullHandler` to the `"laila"` root once.
///
/// Mirrors the standard library-author idiom: keep the package quiet by
/// default so downstream code doesn't see "no handlers could be found"
/// warnings, and let the user opt in via [`Logger::start`].
#[pysig("_install_null_handler()")]
pub fn _install_null_handler(args: impl Into<Args>) -> Result<Arg> {
    Ok(install_null_handler().into_arg())
}

/// Typed twin of [`_install_null_handler`].
pub fn install_null_handler() -> pylogging::Logger {
    let root = pylogging::Logger::get(_LAILA_LOGGER_NAME);
    let has_null = root
        .handlers()
        .__iter__()
        .unwrap_or_default()
        .iter()
        .any(pylogging::NullHandler::isinstance);
    if !has_null {
        let _ = root.addHandler(pylogging::NullHandler::new(Args::new()).unwrap_or(Arg::None));
    }
    root
}

// ---------------------------------------------------------------------------
// Singleton slot and the recursion guard

static SINGLETON: RwLock<Option<Arg>> = RwLock::new(None);

/// `cls.__dict__.get("_singleton")`.
pub fn _singleton() -> Option<Arg> {
    SINGLETON.read().clone()
}

#[cfg(feature = "std")]
std::thread_local! {
    static IN_SINK: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };
}
#[cfg(not(feature = "std"))]
static IN_SINK_FLAG: core::sync::atomic::AtomicBool = core::sync::atomic::AtomicBool::new(false);

fn in_sink() -> bool {
    #[cfg(feature = "std")]
    {
        IN_SINK.with(|c| c.get())
    }
    #[cfg(not(feature = "std"))]
    {
        IN_SINK_FLAG.load(core::sync::atomic::Ordering::Relaxed)
    }
}
fn set_in_sink(v: bool) {
    #[cfg(feature = "std")]
    {
        IN_SINK.with(|c| c.set(v));
    }
    #[cfg(not(feature = "std"))]
    {
        IN_SINK_FLAG.store(v, core::sync::atomic::Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// The class

fn field(default: Arg, annotation: &str) -> FieldInfo {
    FieldInfo {
        default: Some(default),
        default_factory: None,
        json_schema_extra: Arg::None,
        annotation: annotation.to_string(),
        extra: Vec::new(),
    }
}

fn private(default: Arg) -> PrivateAttrInfo {
    PrivateAttrInfo {
        default: Some(default),
        default_factory: None,
    }
}

/// `"%(asctime)s [%(levelname)s] %(name)s: %(message)s"`
pub const DEFAULT_FORMAT: &str = "%(asctime)s [%(levelname)s] %(name)s: %(message)s";

static CLASS: LazyClass = LazyClass::new(|| ModelClass {
    name: "Logger",
    module: "laila.logger.logger",
    bases: alloc::vec![
        _LAILA_CLI_CAPABLE_CLASS::__class__(),
        _LAILA_IDENTIFIABLE_OBJECT::__class__(),
    ],
    fields: alloc::vec![
        (String::from("enabled"), field(Arg::Bool(false), "bool")),
        (
            String::from("level"),
            field(Arg::Str("DEBUG".into()), "str")
        ),
        (
            String::from("format"),
            field(Arg::Str(DEFAULT_FORMAT.into()), "str")
        ),
        (String::from("display"), field(Arg::Bool(false), "bool")),
        (
            String::from("pool_nickname"),
            field(Arg::None, "str | None")
        ),
        (String::from("pool_id"), field(Arg::None, "str | None")),
        (
            String::from("capture_traceback"),
            field(Arg::Bool(false), "bool")
        ),
    ],
    private_attrs: alloc::vec![
        (
            String::from("_scopes"),
            PrivateAttrInfo {
                default: None,
                default_factory: Some(Arc::new(|_a: Args| {
                    Ok(alloc::vec![Arg::Str(_LOGGER_SCOPE.into())].into_arg())
                })),
            }
        ),
        (String::from("_stdlib_root"), private(Arg::None)),
        (
            String::from("_installed_handlers"),
            PrivateAttrInfo {
                default: None,
                default_factory: Some(Arc::new(|_a: Args| Ok(Vec::<Arg>::new().into_arg()))),
            }
        ),
        (String::from("_in_sink"), private(Arg::None)),
        (String::from("_initialized"), private(Arg::Bool(false))),
        (String::from("_last_pool_sink_error"), private(Arg::None)),
    ],
    class_vars: Vec::new(),
    hooks: Some(&HOOKS),
});

/// `logger._in_sink`: Python's `threading.local()` recursion guard as a
/// value -- `.active` reads / writes the calling thread's flag.
struct InSinkLocal;

impl PyObject for InSinkLocal {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "_thread._local"
    }
    fn __repr__(&self) -> String {
        String::from("<_thread._local object>")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other.as_any().is::<InSinkLocal>()
    }
    fn __is__(&self, other: &dyn PyObject) -> bool {
        self.__eq__(other)
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(0x1a5)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "active" => Ok(Arg::Bool(in_sink())),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'_thread._local' object has no attribute '{other}'"
            ))),
        }
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        match name {
            "active" => {
                set_in_sink(value.__bool__());
                Ok(())
            }
            other => Err(LailaError::attribute_error(alloc::format!(
                "'_thread._local' object has no attribute '{other}'"
            ))),
        }
    }
}

struct Hooks;

impl ModelHooks for Hooks {
    fn getattr(&self, _inst: &ModelInstance, name: &str) -> Option<Result<Arg>> {
        match name {
            "_in_sink" => Some(Ok(Arg::Object(Arc::new(InSinkLocal)))),
            _ => None,
        }
    }
    fn call_method(&self, inst: &ModelInstance, name: &str, args: Args) -> Option<Result<Arg>> {
        let r = match name {
            "start" => Logger::start(inst).map(|()| Arg::None),
            "stop" => Logger::stop(inst).map(|()| Arg::None),
            "_remove_installed_handlers" => {
                Logger::_remove_installed_handlers(inst).map(|()| Arg::None)
            }
            "set_level" => Logger::set_level(inst, args).map(|()| Arg::None),
            "emit" => Logger::emit(inst, args).map(|()| Arg::None),
            "_memorize_record" => Logger::_memorize_record(inst, args).map(|()| Arg::None),
            "debug" => Logger::debug(inst, args).map(|()| Arg::None),
            "info" => Logger::info(inst, args).map(|()| Arg::None),
            "warning" => Logger::warning(inst, args).map(|()| Arg::None),
            "error" => Logger::error(inst, args).map(|()| Arg::None),
            "critical" => Logger::critical(inst, args).map(|()| Arg::None),
            "record_memorize" => Logger::record_memorize(inst, args).map(|()| Arg::None),
            "record_remember" => Logger::record_remember(inst, args).map(|()| Arg::None),
            "record_forget" => Logger::record_forget(inst, args).map(|()| Arg::None),
            "record_future_created" => {
                Logger::record_future_created(inst, args).map(|()| Arg::None)
            }
            "record_future_transition" => {
                Logger::record_future_transition(inst, args).map(|()| Arg::None)
            }
            "record_group_future_created" => {
                Logger::record_group_future_created(inst, args).map(|()| Arg::None)
            }
            "_pool_nickname_of" => Logger::_pool_nickname_of(inst, args),
            "reset_singleton" => Logger::reset_singleton().map(|()| Arg::None),
            _ => return None,
        };
        Some(r)
    }
    fn method_names(&self) -> &'static [&'static str] {
        &[
            "start",
            "stop",
            "set_level",
            "emit",
            "debug",
            "info",
            "warning",
            "error",
            "critical",
            "record_memorize",
            "record_remember",
            "record_forget",
            "record_future_created",
            "record_future_transition",
            "record_group_future_created",
            "reset_singleton",
        ]
    }
}

static HOOKS: Hooks = Hooks;

/// Process-wide LAILA logger singleton.
///
/// The logger has two independent sinks gated by simple flags:
///
/// * **stdout** -- a `logging.StreamHandler` writing to stderr, attached
///   on `start` when `display` is `True`.
/// * **pool** -- when `pool_nickname` (or `pool_id`) is set, every record
///   is wrapped in an `Entry` and written into that pool.
///
/// The two sinks are not mutually exclusive: with `display=True` and a
/// pool configured, records flow into both. When no pool is set,
/// `display` is forced to `True` on `start` so logs are not silently
/// lost.
///
/// Fields: `enabled` (master switch, default `False`), `level` (stdlib
/// level name, default `"DEBUG"`), `format` (stdlib formatter pattern),
/// `display` (attach the stderr handler), `pool_nickname` / `pool_id`
/// (the pool sink), `capture_traceback` (include the exception text of
/// errored futures in `extra["traceback"]`).
#[derive(Clone, Copy, Debug, Default)]
pub struct Logger;

impl ModelBase for Logger {
    fn __class__() -> &'static ModelClass {
        &CLASS
    }
}

fn inst_of(a: &Arg) -> Result<&ModelInstance> {
    ModelInstance::downcast(a).ok_or_else(|| LailaError::type_error("expected a Logger instance"))
}

fn stdlib_root(inst: &ModelInstance) -> pylogging::Logger {
    let current = inst.private("_stdlib_root").unwrap_or(Arg::None);
    if let Arg::Object(o) = &current {
        if let Some(l) = o.as_any().downcast_ref::<pylogging::Logger>() {
            return l.clone();
        }
    }
    let root = pylogging::Logger::get(_LAILA_LOGGER_NAME);
    inst.set_private("_stdlib_root", root.clone().into_arg());
    root
}

fn is_listish(a: &Arg) -> bool {
    matches!(
        a,
        Arg::List(_) | Arg::Tuple(_) | Arg::Set(_) | Arg::FrozenSet(_)
    )
}

fn getattr_or_none(obj: &Arg, name: &str) -> Arg {
    if obj.is_none() {
        return Arg::None;
    }
    obj.__getattr__(name).unwrap_or(Arg::None)
}

impl Logger {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        &CLASS
    }
    /// `Logger.model_fields`
    pub fn model_fields() -> Arg {
        Self::__class__().model_fields()
    }

    // ------------------------------------------------------------------
    // Singleton enforcement

    /// `Logger(**data)`: the singleton -- created on the first call (then
    /// `enabled=True` starts it at once), *updated* by the keyword
    /// arguments on every later call.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<Arg> {
        let data: Args = args.into();
        if let Some(existing) = _singleton() {
            let inst = inst_of(&existing)?;
            let initialized = inst
                .private("_initialized")
                .map(|v| v.__bool__())
                .unwrap_or(false);
            if initialized {
                let fields: Vec<String> = CLASS.all_fields().into_iter().map(|(n, _)| n).collect();
                for (key, value) in data.keywords {
                    if fields.contains(&key) {
                        PyObject::__setattr__(inst, &key, value)?;
                    }
                }
                return Ok(existing);
            }
        }
        let instance = ModelInstance::construct(&CLASS, data)?;
        *SINGLETON.write() = Some(instance.clone());
        let inst = inst_of(&instance)?;
        inst.set_private("_initialized", Arg::Bool(true));
        inst.set_private(
            "_stdlib_root",
            pylogging::Logger::get(_LAILA_LOGGER_NAME).into_arg(),
        );
        install_null_handler();
        if inst.field("enabled").map(|v| v.__bool__()).unwrap_or(false) {
            Self::start(inst)?;
        }
        Ok(instance)
    }

    /// `Logger.reset_singleton()`: tear down handlers and clear the
    /// singleton slot. Used by `laila.terminate` and the test suite.
    pub fn reset_singleton() -> Result<()> {
        let existing = SINGLETON.write().take();
        if let Some(existing) = existing {
            if let Some(inst) = ModelInstance::downcast(&existing) {
                let _ = Self::stop(inst);
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Lifecycle

    /// `logger.start()`: install the stdout handler on the `"laila"` root,
    /// if needed. The console handler is attached when `display` is
    /// `True`; with no pool sink configured, `display` is force-set to
    /// `True` first so records always have at least one sink. Idempotent:
    /// previously installed handlers are removed first. Sets `enabled`.
    pub fn start(inst: &ModelInstance) -> Result<()> {
        Self::_remove_installed_handlers(inst)?;
        let root = stdlib_root(inst);
        let level = inst.field("level").unwrap_or(Arg::None);
        root.setLevel(level_number(level.clone()))?;

        let pool_nickname = inst.field("pool_nickname").unwrap_or(Arg::None);
        let pool_id = inst.field("pool_id").unwrap_or(Arg::None);
        if pool_nickname.is_none() && pool_id.is_none() {
            inst.set_field("display", Arg::Bool(true));
        }

        if inst.field("display").map(|v| v.__bool__()).unwrap_or(false) {
            let format = inst.field("format").unwrap_or(Arg::None);
            let formatter = pylogging::Formatter::new(Args::from(format))?;
            let stream = pylogging::StreamHandler::new(Args::new())?;
            stream.__call_method__("setFormatter", Args::from(formatter.into_arg()))?;
            stream.__call_method__("setLevel", Args::from(Arg::Int(level_number(level))))?;
            root.addHandler(stream.clone())?;
            let installed = inst.private("_installed_handlers").unwrap_or(Arg::None);
            if is_listish(&installed) {
                installed.append(stream)?;
            } else {
                inst.set_private("_installed_handlers", alloc::vec![stream].into_arg());
            }
        }
        inst.set_field("enabled", Arg::Bool(true));
        Ok(())
    }

    /// `logger.stop()`: remove installed handlers and disable emission.
    pub fn stop(inst: &ModelInstance) -> Result<()> {
        Self::_remove_installed_handlers(inst)?;
        inst.set_field("enabled", Arg::Bool(false));
        Ok(())
    }

    /// Detach every handler this Logger added to the root.
    pub fn _remove_installed_handlers(inst: &ModelInstance) -> Result<()> {
        let root = stdlib_root(inst);
        let installed = inst.private("_installed_handlers").unwrap_or(Arg::None);
        for handler in installed.__iter__().unwrap_or_default() {
            let _ = root.removeHandler(handler.clone());
            let _ = handler.__call_method__("close", Args::new());
        }
        if is_listish(&installed) {
            let _ = installed.clear(Args::new());
        } else {
            inst.set_private("_installed_handlers", Vec::<Arg>::new().into_arg());
        }
        Ok(())
    }

    /// `logger.set_level(level)`: update both this Logger's stored level
    /// and the root level (and every installed handler).
    #[pysig("set_level(level)")]
    pub fn set_level(inst: &ModelInstance, args: impl Into<Args>) -> Result<()> {
        let canonical = level_name(level);
        inst.set_field("level", Arg::Str(canonical.as_str().into()));
        let n = level_number(Arg::Str(canonical.into()));
        let root = stdlib_root(inst);
        root.setLevel(n)?;
        let installed = inst.private("_installed_handlers").unwrap_or(Arg::None);
        for handler in installed.__iter__().unwrap_or_default() {
            let _ = handler.__call_method__("setLevel", Args::from(Arg::Int(n)));
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Emission

    fn enabled(inst: &ModelInstance) -> bool {
        inst.field("enabled").map(|v| v.__bool__()).unwrap_or(false)
    }

    fn global_id(inst: &ModelInstance) -> Arg {
        PyObject::__getattr__(inst, "global_id").unwrap_or(Arg::None)
    }

    /// `logger.emit(record)`: send *record* (a dict from
    /// [`build_record`]) to both sinks, honoring the recursion guard.
    #[pysig("emit(record)")]
    pub fn emit(inst: &ModelInstance, args: impl Into<Args>) -> Result<()> {
        if !Self::enabled(inst) {
            return Ok(());
        }
        if !record.__contains__("logger_id")? {
            record.__setitem__("logger_id", Self::global_id(inst))?;
        }
        let record_level = level_number(
            record
                .get(Args::from("level"))
                .unwrap_or_else(|_| Arg::Str("INFO".into())),
        );
        let threshold = level_number(inst.field("level").unwrap_or(Arg::None));
        if record_level < threshold {
            return Ok(());
        }
        if in_sink() {
            return Ok(());
        }
        let root = stdlib_root(inst);
        let message = {
            let m = record.get(Args::from("message")).unwrap_or(Arg::None);
            if m.is_none() || !m.__bool__() {
                record
                    .get(Args::from("event"))
                    .unwrap_or_else(|_| Arg::Str("".into()))
            } else {
                m
            }
        };
        let _ = root.log(
            record_level,
            "%s | %s",
            alloc::vec![message, record.clone()],
        );

        let pool_nickname = inst.field("pool_nickname").unwrap_or(Arg::None);
        let pool_id = inst.field("pool_id").unwrap_or(Arg::None);
        if pool_nickname.is_none() && pool_id.is_none() {
            return Ok(());
        }

        set_in_sink(true);
        let r = Self::_memorize_record(inst, Args::from(record));
        set_in_sink(false);
        if let Err(exc) = r {
            let exc = Arg::Exception(Arc::new(exc));
            inst.set_private("_last_pool_sink_error", Arg::Str(exc.__repr__().into()));
            let _ = root.warning("laila.logger pool sink failed: %r", alloc::vec![exc]);
        }
        Ok(())
    }

    /// Wrap *record* in an Entry and persist it into the configured pool.
    ///
    /// The entry is serialized through the same `Record` /
    /// `transformations` pipeline that `laila.memorize` uses, so it
    /// round-trips through `laila.remember` exactly like any other entry.
    /// The actual write bypasses `laila.memorize` and the taskforce
    /// machinery so that the worker futures and routing events spawned by
    /// a normal memorize call do not themselves generate additional log
    /// records (which would recurse forever).
    #[pysig("_memorize_record(record)")]
    pub fn _memorize_record(inst: &ModelInstance, args: impl Into<Args>) -> Result<()> {
        let nickname = alloc::format!(
            "log-{}-{}-{}",
            record
                .get(Args::from("ts_unix"))
                .unwrap_or_else(|_| Arg::Str("".into()))
                .__str__(),
            record
                .get(Args::from("event"))
                .unwrap_or_else(|_| Arg::Str("".into()))
                .__str__(),
            crate::python::builtins::id(&record)
        );
        let mut a = Args::new();
        a.push_keyword("data", record);
        a.push_keyword("nickname", nickname);
        let entry = crate::entry::Entry::constant(a)?;

        let policy = crate::policy::get_active_policy();
        let router = policy.pool_router();
        let pools = router.pools();
        let pool_id = inst.field("pool_id").unwrap_or(Arg::None);
        let pool_nickname = inst.field("pool_nickname").unwrap_or(Arg::None);
        let mut pool = Arg::None;
        if !pool_id.is_none() && pools.__contains__(pool_id.clone())? {
            pool = pools.__getitem__(pool_id.clone())?;
        } else if !pool_nickname.is_none() {
            let gid = router
                .pools_nicknames()
                .get(Args::from(pool_nickname.clone()))
                .unwrap_or(Arg::None);
            if !gid.is_none() {
                pool = pools.get(Args::from(gid)).unwrap_or(Arg::None);
            }
        }
        if pool.is_none() {
            return Err(LailaError::runtime_error(alloc::format!(
                "logger pool sink not found (pool_id={}, pool_nickname={})",
                pool_id.__repr__(),
                pool_nickname.__repr__()
            )));
        }

        let mut ra = Args::new();
        ra.push_keyword("entry", entry.clone());
        ra.push_keyword("recorder", policy.global_id());
        ra.push_keyword("borrower", policy.global_id());
        let record_wrapper = crate::policy::central::memory::record::record::Record::new(ra)?;
        let transformations = pool.__getattr__("transformations")?;
        let mut sa = Args::new();
        sa.push_keyword("transformations", transformations);
        let serialized = record_wrapper.__call_method__("serialize", sa)?;
        // Log records are append-only and never looked up by evolution or
        // creation timestamp, so bypass the pool's index-maintaining
        // ``write`` wrapper.
        pool._write(entry.global_id(), serialized)?;
        Ok(())
    }

    // ------------------------------------------------------------------
    // Convenience emitters (free-form)

    fn free_form(inst: &ModelInstance, level: &str, message: Arg, kwargs: Args) -> Result<()> {
        let mut a = Args::from(Arg::Str("log".into()));
        a.push_keyword("level", level);
        a.push_keyword("message", message);
        for (k, v) in kwargs.keywords {
            a.push_keyword(&k, v);
        }
        Self::emit(inst, Args::from(build_record(a)?))
    }

    /// Emit a free-form `DEBUG` record.
    #[pysig("debug(message, **kwargs)")]
    pub fn debug(inst: &ModelInstance, args: impl Into<Args>) -> Result<()> {
        Self::free_form(inst, "DEBUG", message, kwargs)
    }
    /// Emit a free-form `INFO` record.
    #[pysig("info(message, **kwargs)")]
    pub fn info(inst: &ModelInstance, args: impl Into<Args>) -> Result<()> {
        Self::free_form(inst, "INFO", message, kwargs)
    }
    /// Emit a free-form `WARNING` record.
    #[pysig("warning(message, **kwargs)")]
    pub fn warning(inst: &ModelInstance, args: impl Into<Args>) -> Result<()> {
        Self::free_form(inst, "WARNING", message, kwargs)
    }
    /// Emit a free-form `ERROR` record.
    #[pysig("error(message, **kwargs)")]
    pub fn error(inst: &ModelInstance, args: impl Into<Args>) -> Result<()> {
        Self::free_form(inst, "ERROR", message, kwargs)
    }
    /// Emit a free-form `CRITICAL` record.
    #[pysig("critical(message, **kwargs)")]
    pub fn critical(inst: &ModelInstance, args: impl Into<Args>) -> Result<()> {
        Self::free_form(inst, "CRITICAL", message, kwargs)
    }

    // ------------------------------------------------------------------
    // Structured emitters used by the rest of the codebase

    fn as_list(x: Arg) -> Vec<Arg> {
        if is_listish(&x) {
            x.__iter__().unwrap_or_default()
        } else {
            alloc::vec![x]
        }
    }

    /// Emit one `memory.memorize` record per entry being memorized.
    #[pysig("record_memorize(*, entries, pool, policy)")]
    pub fn record_memorize(inst: &ModelInstance, args: impl Into<Args>) -> Result<()> {
        if !Self::enabled(inst) {
            return Ok(());
        }
        let pool_nick = Self::nickname_of(&pool, &policy);
        let policy_id = getattr_or_none(&policy, "global_id");
        let pool_id = getattr_or_none(&pool, "global_id");
        for entry in Self::as_list(entries) {
            let entry_id = getattr_or_none(&entry, "global_id");
            let entry_nick = getattr_or_none(&getattr_or_none(&entry, "_constitution"), "nickname");
            let mut a = Args::from(Arg::Str("memory.memorize".into()));
            a.push_keyword("level", "INFO");
            a.push_keyword("policy_id", policy_id.clone());
            a.push_keyword("pool_id", pool_id.clone());
            a.push_keyword("pool_nickname", pool_nick.clone());
            a.push_keyword("entry_id", entry_id.clone());
            a.push_keyword("entry_nickname", entry_nick);
            a.push_keyword(
                "message",
                alloc::format!("memorize {} -> {}", entry_id.__str__(), pool_id.__str__()),
            );
            Self::emit(inst, Args::from(build_record(a)?))?;
        }
        Ok(())
    }

    fn record_ids(
        inst: &ModelInstance,
        event: &str,
        verb: &str,
        arrow: &str,
        entry_ids: Arg,
        pool: Arg,
        policy: Arg,
    ) -> Result<()> {
        if !Self::enabled(inst) {
            return Ok(());
        }
        let pool_nick = Self::nickname_of(&pool, &policy);
        let policy_id = getattr_or_none(&policy, "global_id");
        let pool_id = getattr_or_none(&pool, "global_id");
        for entry_id in Self::as_list(entry_ids) {
            let eid = match entry_id.__getattr__("global_id") {
                Ok(g) => g,
                Err(_) => entry_id,
            };
            let mut a = Args::from(Arg::Str(event.into()));
            a.push_keyword("level", "INFO");
            a.push_keyword("policy_id", policy_id.clone());
            a.push_keyword("pool_id", pool_id.clone());
            a.push_keyword("pool_nickname", pool_nick.clone());
            a.push_keyword("entry_id", eid.clone());
            a.push_keyword(
                "message",
                alloc::format!("{verb} {} {arrow} {}", eid.__str__(), pool_id.__str__()),
            );
            Self::emit(inst, Args::from(build_record(a)?))?;
        }
        Ok(())
    }

    /// Emit one `memory.remember` record per entry id being remembered.
    #[pysig("record_remember(*, entry_ids, pool, policy)")]
    pub fn record_remember(inst: &ModelInstance, args: impl Into<Args>) -> Result<()> {
        Self::record_ids(
            inst,
            "memory.remember",
            "remember",
            "<-",
            entry_ids,
            pool,
            policy,
        )
    }

    /// Emit one `memory.forget` record per entry id being forgotten.
    #[pysig("record_forget(*, entry_ids, pool, policy)")]
    pub fn record_forget(inst: &ModelInstance, args: impl Into<Args>) -> Result<()> {
        Self::record_ids(
            inst,
            "memory.forget",
            "forget",
            "--",
            entry_ids,
            pool,
            policy,
        )
    }

    /// Emit one `future.created` record on future construction.
    #[pysig("record_future_created(future)")]
    pub fn record_future_created(inst: &ModelInstance, args: impl Into<Args>) -> Result<()> {
        if !Self::enabled(inst) {
            return Ok(());
        }
        let purpose = getattr_or_none(&future, "purpose");
        let status = future
            .__getattr__("status")
            .map(|s| s.__str__())
            .unwrap_or_else(|_| String::from("not_started"));
        let mut a = Args::from(Arg::Str("future.created".into()));
        a.push_keyword("level", "INFO");
        a.push_keyword("future_id", getattr_or_none(&future, "global_id"));
        a.push_keyword("policy_id", getattr_or_none(&future, "policy_id"));
        a.push_keyword("taskforce_id", getattr_or_none(&future, "taskforce_id"));
        a.push_keyword(
            "future_group_id",
            getattr_or_none(&future, "future_group_id"),
        );
        a.push_keyword("precedence", getattr_or_none(&future, "precedence"));
        a.push_keyword("purpose", purpose.clone());
        a.push_keyword("status", status);
        a.push_keyword(
            "message",
            alloc::format!("future created (purpose={})", purpose.__str__()),
        );
        Self::emit(inst, Args::from(build_record(a)?))
    }

    /// Emit one `future.status` record on a state transition.
    #[pysig("record_future_transition(future, new_status, prev_status=None)")]
    pub fn record_future_transition(inst: &ModelInstance, args: impl Into<Args>) -> Result<()> {
        if !Self::enabled(inst) {
            return Ok(());
        }
        let value_of = |s: &Arg| -> Arg {
            match s.__getattr__("value") {
                Ok(v) if !v.is_none() => v,
                _ => Arg::Str(s.__str__().into()),
            }
        };
        let new_value = value_of(&new_status);
        let prev_value = if prev_status.is_none() {
            Arg::None
        } else {
            value_of(&prev_status)
        };
        let new_s = new_value.__str__();
        let level = if new_s == "error" || new_s == "cancelled" {
            "ERROR"
        } else {
            "INFO"
        };

        let extra = crate::python::PyDictBuilder::new().finish();
        let exc = getattr_or_none(&future, "_exception");
        if !exc.is_none() {
            extra.__setitem__("exc_type", Arg::Str(exc.type_name().into()))?;
            extra.__setitem__("exc_repr", Arg::Str(exc.__repr__().into()))?;
            if inst
                .field("capture_traceback")
                .map(|v| v.__bool__())
                .unwrap_or(false)
            {
                extra.__setitem__(
                    "traceback",
                    Arg::Str(
                        alloc::format!(
                            "Traceback (most recent call last):\n{}: {}\n",
                            exc.type_name(),
                            exc.__str__()
                        )
                        .into(),
                    ),
                )?;
            }
        }

        // ``result_global_id`` forces the lazy Entry wrap of a raw result;
        // acceptable here because we only get this far when logging is on.
        let result_id = future.__getattr__("result_global_id").unwrap_or(Arg::None);

        let future_id = getattr_or_none(&future, "global_id");
        let mut a = Args::from(Arg::Str("future.status".into()));
        a.push_keyword("level", level);
        a.push_keyword("future_id", future_id.clone());
        a.push_keyword("policy_id", getattr_or_none(&future, "policy_id"));
        a.push_keyword("taskforce_id", getattr_or_none(&future, "taskforce_id"));
        a.push_keyword(
            "future_group_id",
            getattr_or_none(&future, "future_group_id"),
        );
        a.push_keyword("precedence", getattr_or_none(&future, "precedence"));
        a.push_keyword("purpose", getattr_or_none(&future, "purpose"));
        a.push_keyword("status", new_value.clone());
        a.push_keyword("prev_status", prev_value);
        a.push_keyword("result_id", result_id);
        a.push_keyword(
            "message",
            alloc::format!("future {} -> {}", future_id.__str__(), new_s),
        );
        a.push_keyword("extra", extra);
        Self::emit(inst, Args::from(build_record(a)?))
    }

    /// Emit one `future.group_created` record.
    #[pysig("record_group_future_created(group)")]
    pub fn record_group_future_created(inst: &ModelInstance, args: impl Into<Args>) -> Result<()> {
        if !Self::enabled(inst) {
            return Ok(());
        }
        let children: Vec<Arg> = getattr_or_none(&group, "future_ids")
            .__iter__()
            .unwrap_or_default();
        let n = children.len();
        let mut a = Args::from(Arg::Str("future.group_created".into()));
        a.push_keyword("level", "INFO");
        a.push_keyword("future_id", getattr_or_none(&group, "global_id"));
        a.push_keyword("policy_id", getattr_or_none(&group, "policy_id"));
        a.push_keyword("taskforce_id", getattr_or_none(&group, "taskforce_id"));
        a.push_keyword("child_future_ids", children);
        a.push_keyword(
            "message",
            alloc::format!("group future created with {n} children"),
        );
        Self::emit(inst, Args::from(build_record(a)?))
    }

    // ------------------------------------------------------------------
    // Helpers

    /// Reverse-lookup the nickname registered for *pool* under *policy*.
    fn nickname_of(pool: &Arg, policy: &Arg) -> Arg {
        if pool.is_none() || policy.is_none() {
            return Arg::None;
        }
        let gid = getattr_or_none(pool, "global_id");
        let nicknames = (|| -> Result<Arg> {
            policy
                .__getattr__("central")?
                .__getattr__("memory")?
                .__getattr__("pool_router")?
                .__getattr__("pools_nicknames")
        })();
        let Ok(nicknames) = nicknames else {
            return Arg::None;
        };
        let items = match nicknames.items() {
            Ok(i) => i.__iter__().unwrap_or_default(),
            Err(_) => return Arg::None,
        };
        for kv in items {
            let pair = kv.__iter__().unwrap_or_default();
            if let [nick, g] = pair.as_slice() {
                if *g == gid {
                    return nick.clone();
                }
            }
        }
        Arg::None
    }

    /// `logger._pool_nickname_of(pool, policy)`
    #[pysig("_pool_nickname_of(pool, policy)")]
    pub fn _pool_nickname_of(_inst: &ModelInstance, args: impl Into<Args>) -> Result<Arg> {
        Ok(Self::nickname_of(&pool, &policy))
    }
}

impl crate::python::builtins::CallValue for Logger {
    fn call_value(&self, args: Args) -> Result<Arg> {
        Logger::new(args)
    }
}
impl PyArg for Logger {
    fn to_arg(&self) -> Arg {
        CLASS.as_pyclass().into_arg()
    }
}
impl IntoArg for Logger {
    fn into_arg(self) -> Arg {
        CLASS.as_pyclass().into_arg()
    }
}
impl IsInstance for Logger {
    fn isinstance(x: &Arg) -> bool {
        crate::_compat::pydantic::isinstance_model(x, "Logger")
    }
}
