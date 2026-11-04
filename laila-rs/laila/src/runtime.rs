//! `laila.runtime` -- futures introspection: `status`, `result`,
//! `exception`, `wait`, each accepting any shape that *refers* to a future
//! (`Future`, `GroupFuture`, `_LAILA_IDENTIFIABLE_FUTURE`, or a raw
//! `global_id` string looked up in every local policy's `future_bank`).

use crate::policy::central::command::{Future, FutureKind};
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, Result};

/// Resolve *future_ref* to a concrete future object.
///
/// Concrete futures are returned unchanged; identities and strings are
/// looked up in every local policy's `future_bank` (first hit wins).
/// `KeyError` if nothing matches; `TypeError` for any other type.
pub fn _resolve_future(future_ref: impl IntoArg) -> Result<Future> {
    let r = future_ref.into_arg();
    let gid = match &r {
        Arg::Future(f) if f.kind() != FutureKind::Identity => return Ok(f.clone()),
        Arg::Future(f) => f.global_id(),
        Arg::Str(s) => alloc::string::String::from(&**s),
        other => {
            return Err(LailaError::type_error(alloc::format!(
                "Cannot resolve future for <class '{}'>",
                other.type_name()
            )))
        }
    };
    for policy in crate::policy::_future_bank_policies() {
        let bank = policy.future_bank();
        if bank.__contains__(gid.as_str())? {
            if let Arg::Future(f) = bank.__getitem__(gid.as_str())? {
                return Ok(f);
            }
        }
    }
    Err(LailaError::key_error(crate::python::pyrepr::str_repr(
        &alloc::format!("Future {gid} not found in any local policy bank"),
    )))
}

/// `laila.runtime` as a value.
#[derive(Clone, Copy, Debug, Default)]
pub struct runtime_mod;
/// `laila.runtime`
pub static runtime: runtime_mod = runtime_mod;

impl runtime_mod {
    /// `runtime._resolve_future(future_ref)`
    pub fn _resolve_future(&self, future_ref: impl IntoArg) -> Result<Future> {
        _resolve_future(future_ref)
    }
    /// `runtime.status(future_ref)`: the current status without blocking
    /// (a `GroupFuture` reports its aggregate payload).
    pub fn status(&self, future_ref: impl IntoArg) -> Result<Arg> {
        Ok(_resolve_future(future_ref)?.status())
    }
    /// `runtime.result(future_ref)`: the result, blocking until available.
    pub fn result(&self, future_ref: impl IntoArg) -> Result<Arg> {
        _resolve_future(future_ref)?.result()
    }
    /// `runtime.exception(future_ref)`: the captured exception or `None`.
    pub fn exception(&self, future_ref: impl IntoArg) -> Result<Arg> {
        Ok(_resolve_future(future_ref)?.exception())
    }
    /// `runtime.wait(future_ref, timeout=None)`: block until the future
    /// terminates and return its result.
    #[pysig("wait(future_ref, timeout=None)")]
    pub fn wait(&self, args: impl Into<Args>) -> Result<Arg> {
        let mut a = Args::new();
        a.push_keyword("timeout", timeout);
        _resolve_future(future_ref)?.wait(a)
    }
}

/// `laila.runtime.status(future_ref)` as a module path.
pub fn status(future_ref: impl IntoArg) -> Result<Arg> {
    runtime.status(future_ref)
}
/// `laila.runtime.result(future_ref)` as a module path.
pub fn result(future_ref: impl IntoArg) -> Result<Arg> {
    runtime.result(future_ref)
}
/// `laila.runtime.exception(future_ref)` as a module path.
pub fn exception(future_ref: impl IntoArg) -> Result<Arg> {
    runtime.exception(future_ref)
}
/// `laila.runtime.wait(future_ref, timeout=None)` as a module path.
pub fn wait(args: impl Into<Args>) -> Result<Arg> {
    runtime.wait(args)
}
