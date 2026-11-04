//! `laila.policy.schema.base._LAILA_IDENTIFIABLE_POLICY` -- the policy class.
//!
//! // MERGE: Phase 3 ships the constructor only (identity + central memory
//! with its pool router), enough for `_LAILA_IDENTIFIABLE_POLICY()` /
//! `laila.activate_policy(..)` in the pool suites. The pydantic model
//! (CLI-capable fields, `central.command` / `central.communication`,
//! `future_bank`, `extend`, `terminate`) is Phase 4.

use crate::python::builtins::IsInstance;
use crate::python::{Arg, Args, IntoArg, PyArg, PyClass, Result};

use super::super::Policy;

/// The policy class (`DefaultPolicy = _LAILA_IDENTIFIABLE_POLICY`).
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct _LAILA_IDENTIFIABLE_POLICY;

impl _LAILA_IDENTIFIABLE_POLICY {
    /// `_LAILA_IDENTIFIABLE_POLICY(**kwargs)` -- a fresh policy with its
    /// own central memory (`uuid=` / `nickname=` pick the identity).
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<Policy> {
        Policy::new(args.into())
    }
    /// The class as a Python value.
    pub fn class_object() -> PyClass {
        PyClass::with_ctor(
            "_LAILA_IDENTIFIABLE_POLICY",
            "laila.policy.schema.base",
            alloc::sync::Arc::new(|a: Args| Policy::new(a).map(IntoArg::into_arg)),
        )
    }
}

impl PyArg for _LAILA_IDENTIFIABLE_POLICY {
    fn to_arg(&self) -> Arg {
        Self::class_object().into_arg()
    }
}
impl IntoArg for _LAILA_IDENTIFIABLE_POLICY {
    fn into_arg(self) -> Arg {
        Self::class_object().into_arg()
    }
}
impl crate::python::builtins::CallValue for _LAILA_IDENTIFIABLE_POLICY {
    fn call_value(&self, args: Args) -> Result<Arg> {
        Policy::new(args).map(IntoArg::into_arg)
    }
}
impl IsInstance for _LAILA_IDENTIFIABLE_POLICY {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<Policy>())
    }
}
