//! Lifecycle status enumeration for task-forces.
//!
//! The members are `str`-valued so they serialize cleanly and are stable
//! across releases (rename them and you break wire compatibility).

use alloc::string::String;
use alloc::sync::Arc;
use core::any::Any;

use crate::python::builtins::IsInstance;
use crate::python::object::PyObject;
use crate::python::{Arg, IntoArg, LailaError, PyArg, Result};

/// `TaskForceStatus(str, Enum)`:
///
/// ```text
/// NOT_STARTED --start()--> RUNNING
/// RUNNING     --pause()--> PAUSED
/// PAUSED      --start()--> RUNNING
/// RUNNING     --shutdown()--> STOPPED
/// PAUSED      --shutdown()--> STOPPED
/// *           --(crash)--> CRASHED
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[allow(clippy::upper_case_acronyms)]
pub enum TaskForceStatus {
    NOT_STARTED,
    RUNNING,
    PAUSED,
    STOPPED,
    CRASHED,
}

impl TaskForceStatus {
    pub const MEMBERS: [TaskForceStatus; 5] = [
        TaskForceStatus::NOT_STARTED,
        TaskForceStatus::RUNNING,
        TaskForceStatus::PAUSED,
        TaskForceStatus::STOPPED,
        TaskForceStatus::CRASHED,
    ];
    pub fn name(&self) -> &'static str {
        match self {
            TaskForceStatus::NOT_STARTED => "NOT_STARTED",
            TaskForceStatus::RUNNING => "RUNNING",
            TaskForceStatus::PAUSED => "PAUSED",
            TaskForceStatus::STOPPED => "STOPPED",
            TaskForceStatus::CRASHED => "CRASHED",
        }
    }
    pub fn value(&self) -> &'static str {
        match self {
            TaskForceStatus::NOT_STARTED => "not_started",
            TaskForceStatus::RUNNING => "running",
            TaskForceStatus::PAUSED => "paused",
            TaskForceStatus::STOPPED => "stopped",
            TaskForceStatus::CRASHED => "crashed",
        }
    }
    /// `TaskForceStatus(value)`: `ValueError` for an unknown value.
    pub fn from_value(value: &str) -> Result<TaskForceStatus> {
        Self::MEMBERS
            .iter()
            .copied()
            .find(|m| m.value() == value)
            .ok_or_else(|| {
                LailaError::value_error(alloc::format!(
                    "{} is not a valid TaskForceStatus",
                    crate::python::pyrepr::str_repr(value)
                ))
            })
    }
    pub fn from_arg(x: &Arg) -> Option<TaskForceStatus> {
        if let Some(s) = x.downcast_ref::<TaskForceStatus>() {
            return Some(*s);
        }
        x.as_str().and_then(|s| Self::from_value(s).ok())
    }
    pub fn __str__(&self) -> String {
        alloc::format!("TaskForceStatus.{}", self.name())
    }
    pub fn __repr__(&self) -> String {
        alloc::format!("<TaskForceStatus.{}: '{}'>", self.name(), self.value())
    }
}

impl core::fmt::Display for TaskForceStatus {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__str__())
    }
}

impl PyObject for TaskForceStatus {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "TaskForceStatus"
    }
    fn __repr__(&self) -> String {
        TaskForceStatus::__repr__(self)
    }
    fn __str__(&self) -> String {
        TaskForceStatus::__str__(self)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<TaskForceStatus>()
            .is_some_and(|o| o == self)
    }
    /// Enum members are singletons: `TaskForceStatus.X is TaskForceStatus.X` holds across
    /// distinct boxings.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        self.__eq__(other)
    }
    /// `class TaskForceStatus(str, Enum)`.
    fn __bases__(&self) -> &'static [&'static str] {
        &["str"]
    }
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
                "'TaskForceStatus' object has no attribute '{other}'"
            ))),
        }
    }
}

impl IntoArg for TaskForceStatus {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for TaskForceStatus {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(*self))
    }
}
impl IsInstance for TaskForceStatus {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<TaskForceStatus>())
    }
}
impl PartialEq<Arg> for TaskForceStatus {
    fn eq(&self, other: &Arg) -> bool {
        TaskForceStatus::from_arg(other) == Some(*self)
    }
}
impl PartialEq<TaskForceStatus> for Arg {
    fn eq(&self, other: &TaskForceStatus) -> bool {
        other == self
    }
}
