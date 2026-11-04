//! Enumeration of lifecycle states for an Entry.

use alloc::string::String;
use alloc::sync::Arc;
use core::any::Any;

use crate::python::builtins::IsInstance;
use crate::python::object::PyObject;
use crate::python::{Arg, IntoArg, LailaError, PyArg, Result};

/// Lifecycle state of an `Entry`.
///
/// Members
/// -------
/// READY
///     Data is available and fully materialised.
/// POOLED
///     Entry is stored in a pool.
/// POOLING
///     Entry is being transferred to a pool.
/// STAGED
///     Entry is staged but data may not yet be materialised.
/// STALE
///     Entry data is out of date.
/// NA
///     Not applicable. Used by Entry subclasses whose lifecycle states are
///     meaningless (e.g. `Manifest`). Only `Entry` may hold non-`NA`
///     states.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[allow(clippy::upper_case_acronyms)]
pub enum EntryState {
    READY,
    POOLED,
    POOLING,
    STAGED,
    STALE,
    NA,
}

impl EntryState {
    /// Every member, in definition order (`list(EntryState)`).
    pub const MEMBERS: [EntryState; 6] = [
        EntryState::READY,
        EntryState::POOLED,
        EntryState::POOLING,
        EntryState::STAGED,
        EntryState::STALE,
        EntryState::NA,
    ];

    /// `EntryState.X.name`
    pub fn name(&self) -> &'static str {
        match self {
            EntryState::READY => "READY",
            EntryState::POOLED => "POOLED",
            EntryState::POOLING => "POOLING",
            EntryState::STAGED => "STAGED",
            EntryState::STALE => "STALE",
            EntryState::NA => "NA",
        }
    }

    /// `EntryState.X.value` (`enum.auto()`: `1..6` in definition order).
    pub fn value(&self) -> i64 {
        match self {
            EntryState::READY => 1,
            EntryState::POOLED => 2,
            EntryState::POOLING => 3,
            EntryState::STAGED => 4,
            EntryState::STALE => 5,
            EntryState::NA => 6,
        }
    }

    /// `EntryState[name]` as an `Option`.
    pub fn from_name(name: &str) -> Option<EntryState> {
        Self::MEMBERS.iter().copied().find(|m| m.name() == name)
    }

    /// `EntryState[name]`: `KeyError(name)` for an unknown member.
    pub fn __getitem__(name: impl IntoArg) -> Result<EntryState> {
        let name = name.into_arg();
        name.as_str()
            .and_then(Self::from_name)
            .ok_or_else(|| LailaError::key_error(name.__repr__()))
    }

    /// `EntryState(value)`: `ValueError` for an unknown value.
    pub fn from_value(value: i64) -> Result<EntryState> {
        Self::MEMBERS
            .iter()
            .copied()
            .find(|m| m.value() == value)
            .ok_or_else(|| {
                LailaError::value_error(alloc::format!("{value} is not a valid EntryState"))
            })
    }

    /// `isinstance(x, EntryState)` -> the member.
    pub fn from_arg(x: &Arg) -> Option<EntryState> {
        x.downcast_ref::<EntryState>().copied()
    }

    /// `str(EntryState.READY)` -> `"EntryState.READY"`.
    pub fn __str__(&self) -> String {
        alloc::format!("EntryState.{}", self.name())
    }

    /// `repr(EntryState.READY)` -> `"<EntryState.READY: 1>"`.
    pub fn __repr__(&self) -> String {
        alloc::format!("<EntryState.{}: {}>", self.name(), self.value())
    }
}

impl core::fmt::Display for EntryState {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__str__())
    }
}

impl PyObject for EntryState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "EntryState"
    }
    fn __repr__(&self) -> String {
        EntryState::__repr__(self)
    }
    fn __str__(&self) -> String {
        EntryState::__str__(self)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<EntryState>()
            .is_some_and(|o| o == self)
    }
    /// Enum members are singletons: `EntryState.X is EntryState.X` holds across
    /// distinct boxings.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        self.__eq__(other)
    }
    fn __hash__(&self) -> Result<i64> {
        // ``hash(EntryState.X)`` is ``hash(X.name)`` for ``Enum`` members.
        Arg::Str(self.name().into()).__hash__()
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "name" => Ok(Arg::Str(self.name().into())),
            "value" => Ok(Arg::Int(self.value())),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'EntryState' object has no attribute '{other}'"
            ))),
        }
    }
}

impl IntoArg for EntryState {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for EntryState {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(*self))
    }
}
impl IsInstance for EntryState {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<EntryState>())
    }
}
impl PartialEq<Arg> for EntryState {
    fn eq(&self, other: &Arg) -> bool {
        EntryState::from_arg(other) == Some(*self)
    }
}
impl PartialEq<EntryState> for Arg {
    fn eq(&self, other: &EntryState) -> bool {
        other == self
    }
}
