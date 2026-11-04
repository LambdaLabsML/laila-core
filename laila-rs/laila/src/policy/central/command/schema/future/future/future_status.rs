//! Enumeration of future lifecycle status codes.
//!
//! The string values are the canonical wire representation -- they are
//! sent verbatim through RPC frames and rendered in log records, so
//! renaming a member is a breaking change for any peer or log consumer
//! that hard-codes the value.

use alloc::string::String;
use alloc::sync::Arc;
use core::any::Any;

use crate::python::builtins::IsInstance;
use crate::python::object::PyObject;
use crate::python::{Arg, Args, IntoArg, LailaError, PyArg, Result};

/// `FutureStatus(str, Enum)` -- lifecycle states for every laila future.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[allow(clippy::upper_case_acronyms)]
pub enum FutureStatus {
    UNKNOWN,
    NOT_STARTED,
    RUNNING,
    POLL_TIMEOUT,
    FINISHED,
    ERROR,
    CANCELLED,
}

impl FutureStatus {
    pub const MEMBERS: [FutureStatus; 7] = [
        FutureStatus::UNKNOWN,
        FutureStatus::NOT_STARTED,
        FutureStatus::RUNNING,
        FutureStatus::POLL_TIMEOUT,
        FutureStatus::FINISHED,
        FutureStatus::ERROR,
        FutureStatus::CANCELLED,
    ];
    /// `FutureStatus.X.name`
    pub fn name(&self) -> &'static str {
        match self {
            FutureStatus::UNKNOWN => "UNKNOWN",
            FutureStatus::NOT_STARTED => "NOT_STARTED",
            FutureStatus::RUNNING => "RUNNING",
            FutureStatus::POLL_TIMEOUT => "POLL_TIMEOUT",
            FutureStatus::FINISHED => "FINISHED",
            FutureStatus::ERROR => "ERROR",
            FutureStatus::CANCELLED => "CANCELLED",
        }
    }
    /// `FutureStatus.X.value` (the wire string).
    pub fn value(&self) -> &'static str {
        match self {
            FutureStatus::UNKNOWN => "unknown",
            FutureStatus::NOT_STARTED => "not_started",
            FutureStatus::RUNNING => "running",
            FutureStatus::POLL_TIMEOUT => "poll_timeout",
            FutureStatus::FINISHED => "finished",
            FutureStatus::ERROR => "error",
            FutureStatus::CANCELLED => "cancelled",
        }
    }
    pub fn from_name(name: &str) -> Option<FutureStatus> {
        Self::MEMBERS.iter().copied().find(|m| m.name() == name)
    }
    /// `FutureStatus(value)` as a call with a Python value (`FutureStatus("finished")`).
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<FutureStatus> {
        let args: Args = args.into();
        let value = args.positional.first().cloned().unwrap_or(Arg::None);
        if let Some(s) = value.as_str() {
            return Self::from_value(s);
        }
        if let Some(m) = value.downcast_ref::<FutureStatus>() {
            return Ok(*m);
        }
        Err(LailaError::value_error(alloc::format!(
            "{} is not a valid FutureStatus",
            value.__repr__()
        )))
    }
    /// `FutureStatus(value)`: `ValueError` for an unknown value.
    pub fn from_value(value: &str) -> Result<FutureStatus> {
        Self::MEMBERS
            .iter()
            .copied()
            .find(|m| m.value() == value)
            .ok_or_else(|| {
                LailaError::value_error(alloc::format!(
                    "{} is not a valid FutureStatus",
                    crate::python::pyrepr::str_repr(value)
                ))
            })
    }
    /// A member from a Python value: the member itself, or (as a `str`
    /// enum) its wire string.
    pub fn from_arg(x: &Arg) -> Option<FutureStatus> {
        if let Some(s) = x.downcast_ref::<FutureStatus>() {
            return Some(*s);
        }
        x.as_str().and_then(|s| Self::from_value(s).ok())
    }
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            FutureStatus::FINISHED | FutureStatus::ERROR | FutureStatus::CANCELLED
        )
    }
    /// `str(FutureStatus.RUNNING)` -> `"FutureStatus.RUNNING"`.
    pub fn __str__(&self) -> String {
        alloc::format!("FutureStatus.{}", self.name())
    }
    /// `repr(FutureStatus.RUNNING)` -> `"<FutureStatus.RUNNING: 'running'>"`.
    pub fn __repr__(&self) -> String {
        alloc::format!("<FutureStatus.{}: '{}'>", self.name(), self.value())
    }
}

impl core::fmt::Display for FutureStatus {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__str__())
    }
}

impl PyObject for FutureStatus {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "FutureStatus"
    }
    fn __repr__(&self) -> String {
        FutureStatus::__repr__(self)
    }
    fn __str__(&self) -> String {
        FutureStatus::__str__(self)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<FutureStatus>()
            .is_some_and(|o| o == self)
    }
    /// Enum members are singletons: `FutureStatus.X is FutureStatus.X` holds across
    /// distinct boxings.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        self.__eq__(other)
    }
    /// `class FutureStatus(str, Enum)`.
    fn __bases__(&self) -> &'static [&'static str] {
        &["str"]
    }
    /// A `str` enum compares equal to its value (`FutureStatus.RUNNING == "running"`).
    fn __eq_arg__(&self, other: &Arg) -> bool {
        match other {
            Arg::Object(o) => self.__eq__(&**o),
            Arg::Str(s) => &**s == self.value(),
            _ => false,
        }
    }
    fn __hash__(&self) -> Result<i64> {
        Arg::Str(self.value().into()).__hash__()
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "name" => Ok(Arg::Str(self.name().into())),
            "value" => Ok(Arg::Str(self.value().into())),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'FutureStatus' object has no attribute '{other}'"
            ))),
        }
    }
}

impl IntoArg for FutureStatus {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for FutureStatus {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(*self))
    }
}
impl IsInstance for FutureStatus {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<FutureStatus>())
    }
}
impl PartialEq<Arg> for FutureStatus {
    fn eq(&self, other: &Arg) -> bool {
        FutureStatus::from_arg(other) == Some(*self)
    }
}
impl PartialEq<FutureStatus> for Arg {
    fn eq(&self, other: &FutureStatus) -> bool {
        other == self
    }
}
