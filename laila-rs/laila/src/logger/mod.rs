//! LAILA Logger subsystem.
//!
//! A top-level singleton (sibling to `laila.policy`, `laila.data`,
//! `laila.entry`) that fans structured log records out to two
//! independent sinks:
//!
//! - The standard-library `logging` hierarchy under the
//!   [`_LAILA_LOGGER_NAME`] channel, with optional stderr streaming via
//!   the `display=True` flag.
//! - A laila pool, by `memorize`-ing each record as a JSON-shaped entry.
//!   The pool can be selected by nickname or global id; missing pool
//!   configuration triggers an automatic fall-back to `display=True` so
//!   records are never silently dropped.
//!
//! The two sinks are independent and can both be active at once -- a
//! typical setup is "always display, also persist into Postgres" so an
//! operator can watch the live stream while history accumulates in a
//! pool.
//!
//! This module re-exports:
//!
//! - [`Logger`] -- the singleton class itself, with all the configurable
//!   fields and lifecycle methods (`start`, `stop`, `set_level`, ...).
//! - The `record` helpers ([`build_record`], [`normalize_level`],
//!   [`numeric_level`]).
//! - The convenience top-level functions [`get_logger`],
//!   [`enable_logging`], [`disable_logging`], [`set_log_level`] -- the
//!   same names re-exported on the `laila` facade.

#[allow(clippy::module_inception)]
pub mod logger;
pub mod record;

pub use logger::{_install_null_handler, Logger, _LAILA_LOGGER_NAME};
pub use record::{build_record, normalize_level, numeric_level};

use crate::_compat::pydantic::ModelInstance;
use crate::python::{pysig, Arg, Args, Result};

/// Return the process-wide [`Logger`] singleton, creating it lazily.
///
/// The first call constructs the singleton with default settings
/// (silent: no display, no pool). Subsequent calls return the same
/// instance. Use [`enable_logging`] to actually start emitting records.
#[pysig("get_logger()")]
pub fn get_logger(args: impl Into<Args>) -> Result<Arg> {
    if let Some(existing) = logger::_singleton() {
        return Ok(existing);
    }
    Logger::new(Args::new())
}

/// Configure and start the singleton logger.
///
/// The logger has two independent sinks: a stdout `StreamHandler` enabled
/// by `display=True`, and a pool sink enabled by passing `pool_nickname`
/// (or `pool_id`). They are not mutually exclusive. When no pool is
/// configured, `display` is forced to `True` so records are not silently
/// dropped. Returns the (now-running) singleton.
#[pysig("enable_logging(level='DEBUG', *, pool_nickname=None, pool_id=None, display=False, capture_traceback=False)")]
pub fn enable_logging(args: impl Into<Args>) -> Result<Arg> {
    let logger = get_logger(Args::new())?;
    logger.__setattr__("level", level)?;
    logger.__setattr__("display", display)?;
    if !pool_nickname.is_none() {
        logger.__setattr__("pool_nickname", pool_nickname)?;
    }
    if !pool_id.is_none() {
        logger.__setattr__("pool_id", pool_id)?;
    }
    logger.__setattr__("capture_traceback", capture_traceback)?;
    let inst = ModelInstance::downcast(&logger)
        .ok_or_else(|| crate::python::LailaError::type_error("expected a Logger instance"))?;
    Logger::start(inst)?;
    Ok(logger)
}

/// Stop the singleton logger if one is alive.
///
/// Idempotent: if no logger has been constructed yet (no [`get_logger`]
/// / [`enable_logging`] calls), this is a no-op.
#[pysig("disable_logging()")]
pub fn disable_logging(args: impl Into<Args>) -> Result<Arg> {
    if let Some(existing) = logger::_singleton() {
        if let Some(inst) = ModelInstance::downcast(&existing) {
            Logger::stop(inst)?;
        }
    }
    Ok(Arg::None)
}

/// Set the singleton logger's minimum level (creating it lazily).
///
/// Accepts any of the standard string level names recognised by
/// `logging` (`"DEBUG"`, `"INFO"`, `"WARNING"`, `"ERROR"`, `"CRITICAL"`).
/// Records below the configured level are discarded *before* being
/// shipped to either sink.
#[pysig("set_log_level(level)")]
pub fn set_log_level(args: impl Into<Args>) -> Result<Arg> {
    let logger = get_logger(Args::new())?;
    let inst = ModelInstance::downcast(&logger)
        .ok_or_else(|| crate::python::LailaError::type_error("expected a Logger instance"))?;
    Logger::set_level(inst, Args::from(level))?;
    Ok(Arg::None)
}

// ---------------------------------------------------------------------------
// Hooks the rest of the crate calls (best-effort: never raise, and free
// when no logger exists or it is disabled).

/// The singleton (constructed on first use, exactly as Python's
/// `get_logger()` does -- the construction mirrors the logger into
/// `laila.args.environment.logger`) when it is enabled.
fn active() -> Option<Arg> {
    let existing = match logger::_singleton() {
        Some(l) => l,
        None => get_logger(Args::new()).ok()?,
    };
    let inst = ModelInstance::downcast(&existing)?;
    if inst.field("enabled").map(|v| v.__bool__()).unwrap_or(false) {
        Some(existing)
    } else {
        None
    }
}

fn call(name: &str, args: Args) {
    if let Some(l) = active() {
        let _ = l.__call_method__(name, args);
    }
}

/// `get_logger().record_memorize(entries=.., pool=.., policy=..)` from
/// `Memory.memorize`.
pub fn hook_memorize(entries: Arg, pool: Arg, policy: Arg) {
    let mut a = Args::new();
    a.push_keyword("entries", entries);
    a.push_keyword("pool", pool);
    a.push_keyword("policy", policy);
    call("record_memorize", a);
}

/// `get_logger().record_remember(entry_ids=.., pool=.., policy=..)`.
pub fn hook_remember(entry_ids: Arg, pool: Arg, policy: Arg) {
    let mut a = Args::new();
    a.push_keyword("entry_ids", entry_ids);
    a.push_keyword("pool", pool);
    a.push_keyword("policy", policy);
    call("record_remember", a);
}

/// `get_logger().record_forget(entry_ids=.., pool=.., policy=..)`.
pub fn hook_forget(entry_ids: Arg, pool: Arg, policy: Arg) {
    let mut a = Args::new();
    a.push_keyword("entry_ids", entry_ids);
    a.push_keyword("pool", pool);
    a.push_keyword("policy", policy);
    call("record_forget", a);
}

/// `get_logger().record_future_created(future)` from `model_post_init`.
pub fn hook_future_created(future: Arg) {
    call("record_future_created", Args::from(future));
}

/// `get_logger().record_future_transition(future, status, prev)` from
/// the `status` setter.
pub fn hook_future_transition(future: Arg, new_status: Arg, prev_status: Arg) {
    let mut a = Args::from(future);
    a.push_positional(new_status);
    a.push_positional(prev_status);
    call("record_future_transition", a);
}

/// `get_logger().record_group_future_created(group)`.
pub fn hook_group_future_created(group: Arg) {
    call("record_group_future_created", Args::from(group));
}

/// `Logger.reset_singleton()` from `laila.terminate`.
pub fn reset_singleton() -> Result<()> {
    Logger::reset_singleton()
}
