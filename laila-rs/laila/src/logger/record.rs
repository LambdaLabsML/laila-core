//! Structured log record builder for the LAILA Logger singleton.
//!
//! A laila log record is a JSON-trivial dict with a *fixed* schema of
//! optional well-known fields (`policy_id`, `pool_id`, `future_id`,
//! `status`, ...) plus a free-form `extra` bag for anything not covered
//! by the schema. Records are always:
//!
//! - Composed of strings, numbers, lists, and dicts only -- so they
//!   serialise cleanly with `json.dumps` and round-trip safely through
//!   any laila pool.
//! - Time-stamped with both an ISO-8601 string (`ts`, UTC, with `Z`
//!   suffix) and a Unix epoch float (`ts_unix`) -- the former for humans,
//!   the latter for time-series analytics.
//! - Equipped with a normalised string `level` (one of `DEBUG`, `INFO`,
//!   `WARNING`, `ERROR`, `CRITICAL`).
//!
//! This module exports two utilities:
//!
//! - [`normalize_level`] / [`numeric_level`] -- bidirectional conversion
//!   between numeric `logging` levels and the canonical string names used
//!   in records.
//! - [`build_record`] -- the structured-record factory called by the
//!   `Logger` itself for every event. Optional fields are omitted from
//!   the result rather than written as `None` so records stay compact on
//!   the wire.

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use crate::python::{pysig, Arg, Args, IntoArg, PyDictBuilder, Result};

/// `_LEVEL_NUMERIC`: canonical level name -> `logging` level.
pub const _LEVEL_NUMERIC: &[(&str, i64)] = &[
    ("DEBUG", 10),
    ("INFO", 20),
    ("WARNING", 30),
    ("ERROR", 40),
    ("CRITICAL", 50),
];

/// Coerce a level (str or int) into a canonical upper-case name.
///
/// Returns one of `"DEBUG"`, `"INFO"`, `"WARNING"`, `"ERROR"`,
/// `"CRITICAL"`. Unknown values fall back to `"INFO"`.
#[pysig("normalize_level(level)")]
pub fn normalize_level(args: impl Into<Args>) -> Result<Arg> {
    Ok(Arg::Str(level_name(level).into()))
}

/// Return the integer `logging` level for a name or numeric value.
#[pysig("numeric_level(level)")]
pub fn numeric_level(args: impl Into<Args>) -> Result<Arg> {
    Ok(Arg::Int(level_number(level)))
}

/// Typed twin of [`normalize_level`]: the canonical level name.
pub fn level_name(level: impl IntoArg) -> String {
    match level.into_arg() {
        Arg::Bool(_) => String::from("INFO"),
        Arg::Int(i) => _LEVEL_NUMERIC
            .iter()
            .find(|(_, v)| *v == i)
            .map(|(n, _)| (*n).to_string())
            .unwrap_or_else(|| String::from("INFO")),
        Arg::Str(s) => {
            let upper = s.to_uppercase();
            if _LEVEL_NUMERIC.iter().any(|(n, _)| *n == upper) {
                upper
            } else {
                String::from("INFO")
            }
        }
        _ => String::from("INFO"),
    }
}

/// Typed twin of [`numeric_level`]: the integer `logging` level.
pub fn level_number(level: impl IntoArg) -> i64 {
    let name = level_name(level);
    _LEVEL_NUMERIC
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, v)| *v)
        .unwrap_or(20)
}

/// Convert any identifiable-like object to a string global_id.
#[pysig("_coerce_id(value)")]
pub fn _coerce_id(args: impl Into<Args>) -> Result<Arg> {
    Ok(coerce_id(&value))
}

/// Typed twin of [`_coerce_id`].
pub fn coerce_id(value: &Arg) -> Arg {
    match value {
        Arg::None => Arg::None,
        Arg::Str(_) => value.clone(),
        other => match other.__getattr__("global_id") {
            Ok(gid) if !gid.is_none() => Arg::Str(gid.__str__().into()),
            _ => Arg::Str(other.__str__().into()),
        },
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

/// Build a structured LAILA log record dict.
///
/// All `*_id` fields accept either a string global_id or any
/// `_LAILA_IDENTIFIABLE_OBJECT` instance and are normalized to strings.
/// Unspecified optional fields are omitted (kept out of the dict) so
/// records stay compact. The result always contains `ts`, `ts_unix`,
/// `level`, `event`, and `extra`.
#[pysig("build_record(event, *, level='INFO', message=None, policy_id=None, pool_id=None, pool_nickname=None, entry_id=None, entry_nickname=None, future_id=None, future_group_id=None, precedence=None, purpose=None, taskforce_id=None, logger_id=None, status=None, prev_status=None, result_id=None, child_future_ids=None, child_results=None, peer_id=None, extra=None)")]
pub fn build_record(args: impl Into<Args>) -> Result<Arg> {
    let now = now_secs();
    let mut record = PyDictBuilder::new();
    record.insert(
        "ts",
        Arg::Str(
            crate::_compat::datetime::DateTime::ts_to_iso_z(now)
                .unwrap_or_default()
                .into(),
        ),
    );
    record.insert("ts_unix", Arg::Float(now));
    record.insert("level", Arg::Str(level_name(level).into()));
    record.insert("event", Arg::Str(event.__str__().into()));
    let extra_bag = if extra.is_none() || !extra.__bool__() {
        PyDictBuilder::new().finish()
    } else {
        crate::python::builtins::dict(Args::from(extra))?
    };
    record.insert("extra", extra_bag);
    let record = record.finish();
    if !message.is_none() {
        record.__setitem__("message", message)?;
    }

    let optional_ids: [(&str, Arg); 13] = [
        ("policy_id", policy_id),
        ("pool_id", pool_id),
        ("pool_nickname", pool_nickname),
        ("entry_id", entry_id),
        ("entry_nickname", entry_nickname),
        ("future_id", future_id),
        ("future_group_id", future_group_id),
        ("precedence", precedence),
        ("purpose", purpose),
        ("taskforce_id", taskforce_id),
        ("logger_id", logger_id),
        ("peer_id", peer_id),
        ("result_id", result_id),
    ];
    for (key, value) in optional_ids {
        if value.is_none() {
            continue;
        }
        if matches!(key, "pool_nickname" | "entry_nickname" | "purpose") {
            record.__setitem__(key, Arg::Str(value.__str__().into()))?;
        } else {
            record.__setitem__(key, coerce_id(&value))?;
        }
    }

    if !status.is_none() {
        record.__setitem__("status", Arg::Str(status.__str__().into()))?;
    }
    if !prev_status.is_none() {
        record.__setitem__("prev_status", Arg::Str(prev_status.__str__().into()))?;
    }
    if !child_future_ids.is_none() {
        let ids: Vec<Arg> = child_future_ids.__iter__()?.iter().map(coerce_id).collect();
        record.__setitem__("child_future_ids", ids)?;
    }
    if !child_results.is_none() {
        let ids: Vec<Arg> = child_results.__iter__()?.iter().map(coerce_id).collect();
        record.__setitem__("child_results", ids)?;
    }
    Ok(record)
}
