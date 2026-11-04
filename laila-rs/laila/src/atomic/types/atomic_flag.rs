//! Thread-safe boolean flag.
//!
//! [`AtomicFlag`] is the simplest of the atomic types: a single boolean
//! guarded by an `RLock` so the usual `set` / `clear` / `toggle` / `is_set`
//! operations are safe to call from multiple threads. Use it for "has this
//! happened yet?" signals -- shutdown sentinels, init flags, one-shot guards
//! -- where a full `threading.Event` is overkill because no thread ever
//! needs to *wait* on the flag.
//!
//! For coordinated wait-for-state semantics, prefer `threading.Event`. For
//! atomically-incrementable counters, see [`AtomicInt`](super::AtomicInt).
//!
//! The flag is a handle: `Clone` aliases the same flag (Python reference
//! semantics); `IntoArg` wraps it in `Arg::Object` with `repr`, `bool()`,
//! `.value` and the methods reachable from `laila::python!` blocks.

use alloc::string::String;
use alloc::sync::Arc;

use crate::atomic::definitions::locally_atomic_object::_now_creation_timestamp;
use crate::atomic::definitions::_LAILA_LOCALLY_ATOMIC_OBJECT;
use crate::python::args::Args;
use crate::python::builtins::IsInstance;
use crate::python::object::{Arg, HasAttrs, IntoArg, PyArg, PyAttrs, PyObject};
use crate::python::sync::{RLock, RwLock};
use crate::python::{pysig, LailaError, Result};

use super::{reject_positional, validation_error, Held};

struct FlagInner {
    value: RwLock<bool>,
    lock: RLock,
    attrs: PyAttrs,
}

/// Thread-safe boolean flag with atomic set/clear/toggle/get operations.
#[derive(Clone)]
pub struct AtomicFlag(Arc<FlagInner>);

/// pydantic's `bool` field coercion (lax mode).
fn coerce_bool(class: &str, v: &Arg) -> Result<bool> {
    match v {
        Arg::Bool(b) => Ok(*b),
        Arg::Int(0) => Ok(false),
        Arg::Int(1) => Ok(true),
        Arg::Float(f) if *f == 0.0 => Ok(false),
        Arg::Float(f) if *f == 1.0 => Ok(true),
        Arg::Str(s) => match s.to_ascii_lowercase().as_str() {
            "0" | "off" | "f" | "false" | "n" | "no" => Ok(false),
            "1" | "on" | "t" | "true" | "y" | "yes" => Ok(true),
            _ => Err(validation_error(class, "value", "boolean", v)),
        },
        _ => Err(validation_error(class, "value", "boolean", v)),
    }
}

impl AtomicFlag {
    /// `AtomicFlag(value=False)`
    pub fn new(args: impl Into<Args>) -> Result<AtomicFlag> {
        let mut a: Args = args.into();
        reject_positional("AtomicFlag", &a)?;
        let value = a.pop_or("value", false);
        let value = coerce_bool("AtomicFlag", &value)?;
        Ok(Self::from_value(value))
    }

    fn from_value(value: bool) -> AtomicFlag {
        let attrs = PyAttrs::new();
        let _ = attrs.__setattr__("_creation_timestamp", _now_creation_timestamp());
        AtomicFlag(Arc::new(FlagInner {
            value: RwLock::new(value),
            lock: RLock::new(),
            attrs,
        }))
    }

    /// Python `is`.
    pub fn is(&self, other: &AtomicFlag) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// `self._lock`
    pub fn _lock(&self) -> RLock {
        self.0.lock.clone()
    }

    fn held(&self) -> Held {
        Held::acquire(&self.0.lock)
    }

    /// `self.value` (the raw field; the lock-guarded reader is [`is_set`](Self::is_set)).
    pub fn value(&self) -> bool {
        *self.0.value.read()
    }

    /// `self.value = state` (`locked.value = True` inside `with flag.atomic()`).
    pub fn set_value(&self, state: impl IntoArg) -> Result<()> {
        let v = coerce_bool("AtomicFlag", &state.into_arg())?;
        *self.0.value.write() = v;
        Ok(())
    }

    /// Set the flag to `True`.
    #[pysig("set(self)")]
    pub fn set(&self, args: impl Into<Args>) -> Result<()> {
        let _g = self.held();
        *self.0.value.write() = true;
        Ok(())
    }

    /// Set the flag to `False`.
    #[pysig("clear(self)")]
    pub fn clear(&self, args: impl Into<Args>) -> Result<()> {
        let _g = self.held();
        *self.0.value.write() = false;
        Ok(())
    }

    /// Invert the flag value.
    pub fn toggle(&self) {
        let _g = self.held();
        let mut v = self.0.value.write();
        *v = !*v;
    }

    /// Return the current flag value.
    #[pysig("is_set(self)")]
    pub fn is_set(&self, args: impl Into<Args>) -> Result<bool> {
        let _g = self.held();
        Ok(*self.0.value.read())
    }

    /// `if flag:` reads the flag value (same as [`is_set`](Self::is_set)).
    pub fn __bool__(&self) -> bool {
        let _g = self.held();
        *self.0.value.read()
    }

    /// Set the flag to `bool(state)`.
    pub fn set_to(&self, state: impl IntoArg) {
        let _g = self.held();
        *self.0.value.write() = state.into_arg().__bool__();
    }

    /// `with flag.atomic() as locked:` -- a context manager holding the
    /// flag's lock; `__enter__` yields the flag itself.
    #[pysig("atomic(self)")]
    pub fn atomic(&self, args: impl Into<Args>) -> Result<FlagAtomic> {
        Ok(FlagAtomic(self.clone()))
    }

    /// `repr(flag)` -> `AtomicFlag(True)`
    pub fn __repr__(&self) -> String {
        let _g = self.held();
        alloc::format!(
            "AtomicFlag({})",
            if *self.0.value.read() {
                "True"
            } else {
                "False"
            }
        )
    }

    /// `str(flag)` -> `value=True` (pydantic's default `__str__` lists the fields).
    pub fn __str__(&self) -> String {
        let _g = self.held();
        alloc::format!(
            "value={}",
            if *self.0.value.read() {
                "True"
            } else {
                "False"
            }
        )
    }

    /// `flag.name`
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "value" => Ok(Arg::Bool(self.value())),
            "_lock" => Ok(self._lock().into_arg()),
            "creation_timestamp" | "_creation_timestamp" => {
                Ok(_LAILA_LOCALLY_ATOMIC_OBJECT::creation_timestamp(self))
            }
            _ => self.0.attrs.__getattr__(name).map_err(|_| {
                LailaError::attribute_error(alloc::format!(
                    "'AtomicFlag' object has no attribute '{name}'"
                ))
            }),
        }
    }

    /// `flag.name = value`
    pub fn __setattr__(&self, name: &str, value: impl IntoArg) -> Result<()> {
        match name {
            "value" => self.set_value(value),
            _ => self.0.attrs.__setattr__(name, value),
        }
    }

    /// `copy.deepcopy(flag)`: same value, fresh lock.
    pub fn deepcopy(&self) -> AtomicFlag {
        let _g = self.held();
        let out = Self::from_value(*self.0.value.read());
        let _ = crate::atomic::definitions::locally_atomic_object::deepcopy_attrs(
            &self.0.attrs,
            &out.0.attrs,
        );
        out
    }
}

impl Default for AtomicFlag {
    fn default() -> Self {
        Self::from_value(false)
    }
}

impl core::fmt::Debug for AtomicFlag {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__repr__())
    }
}

impl HasAttrs for AtomicFlag {
    fn __attrs__(&self) -> &PyAttrs {
        &self.0.attrs
    }
    fn __class_name__(&self) -> &'static str {
        "AtomicFlag"
    }
}
impl _LAILA_LOCALLY_ATOMIC_OBJECT for AtomicFlag {}

/// Context manager that holds the flag's lock (`AtomicFlag._Atomic`).
pub struct FlagAtomic(AtomicFlag);

/// The locked flag yielded by `with flag.atomic() as locked:`; releases on drop.
pub struct FlagGuard {
    flag: AtomicFlag,
    _held: Held,
}

impl FlagAtomic {
    pub fn __enter__(&self) -> Result<FlagGuard> {
        Ok(FlagGuard {
            flag: self.0.clone(),
            _held: Held::acquire(&self.0 .0.lock),
        })
    }
}
impl FlagGuard {
    /// `__exit__`: release now (also happens on drop).
    pub fn __exit__(self) -> Result<()> {
        drop(self);
        Ok(())
    }
}
/// `with f.atomic() as inner: assert inner is f` -- the guard is the
/// parent's handle.
impl PyArg for FlagGuard {
    fn to_arg(&self) -> Arg {
        self.flag.to_arg()
    }
}
impl core::ops::Deref for FlagGuard {
    type Target = AtomicFlag;
    fn deref(&self) -> &AtomicFlag {
        &self.flag
    }
}

impl PyObject for AtomicFlag {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    /// `a is b`: identity of the shared handle, not of the boxing.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<AtomicFlag>()
            .is_some_and(|o| self.is(o))
    }
    /// `copy.deepcopy(x)`: a new object with fresh locks.
    fn __deepcopy__(&self) -> Option<Arg> {
        Some(AtomicFlag::deepcopy(self).into_arg())
    }
    /// `isinstance(x, _LAILA_OBJECT)`: the atomic types are laila objects.
    fn __bases__(&self) -> &'static [&'static str] {
        &["_LAILA_LOCALLY_ATOMIC_OBJECT", "_LAILA_OBJECT"]
    }
    fn type_name(&self) -> &'static str {
        "AtomicFlag"
    }
    fn __repr__(&self) -> String {
        AtomicFlag::__repr__(self)
    }
    fn __str__(&self) -> String {
        AtomicFlag::__str__(self)
    }
    /// pydantic compares fields *and* private attributes; the private
    /// `_lock` differs between any two instances, so equality is identity.
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<AtomicFlag>()
            .is_some_and(|o| self.is(o))
    }
    fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error("unhashable type: 'AtomicFlag'"))
    }
    fn __bool__(&self) -> bool {
        AtomicFlag::__bool__(self)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        AtomicFlag::__getattr__(self, name)
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        AtomicFlag::__setattr__(self, name, value)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        Ok(match name {
            "atomic" => IntoArg::into_arg(self.atomic(args)?),
            "set" => {
                self.set(args)?;
                Arg::None
            }
            "clear" => {
                self.clear(args)?;
                Arg::None
            }
            "toggle" => {
                self.toggle();
                Arg::None
            }
            "is_set" => Arg::Bool(self.is_set(args)?),
            "set_to" => {
                let v = args.positional.first().cloned().ok_or_else(|| {
                    LailaError::type_error(
                        "AtomicFlag.set_to() missing 1 required positional argument: 'state'",
                    )
                })?;
                self.set_to(v);
                Arg::None
            }
            "lock" => Arg::Bool(_LAILA_LOCALLY_ATOMIC_OBJECT::lock(self, args)?),
            "unlock" => {
                _LAILA_LOCALLY_ATOMIC_OBJECT::unlock(self)?;
                Arg::None
            }
            "locked" => Arg::Bool(_LAILA_LOCALLY_ATOMIC_OBJECT::locked(self, args)),
            _ => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'AtomicFlag' object has no attribute '{name}'"
                )))
            }
        })
    }
}

impl IntoArg for AtomicFlag {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for AtomicFlag {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl IsInstance for AtomicFlag {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<AtomicFlag>())
    }
}
impl crate::python::builtins::Truthy for AtomicFlag {
    fn __bool__(&self) -> bool {
        AtomicFlag::__bool__(self)
    }
}
impl PartialEq for AtomicFlag {
    fn eq(&self, other: &AtomicFlag) -> bool {
        self.is(other)
    }
}
impl PartialEq<Arg> for AtomicFlag {
    fn eq(&self, other: &Arg) -> bool {
        matches!(other, Arg::Object(o) if o.as_any().downcast_ref::<AtomicFlag>().is_some_and(|f| self.is(f)))
    }
}

super::context_manager_py_object!(FlagAtomic, "AtomicFlag._Atomic");
super::guard_py_object!(FlagGuard, "AtomicFlag");
