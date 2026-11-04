//! Thread-safe integer counter.
//!
//! [`AtomicInt`] wraps an `int` with an `RLock` so that
//! increment/decrement/add/set/get are race-free even under heavy
//! contention. Use it for shared counters (active workers, in-flight jobs,
//! total processed entries) where Python's atomicity of single-byte
//! operations isn't enough because the operation reads, modifies, then
//! writes (e.g. `i += 1` is *not* atomic on CPython).
//!
//! For a single-bit signal, use [`AtomicFlag`](super::AtomicFlag). For a
//! more elaborate compute-and-set pattern, use `AtomicDict.compute` on a
//! one-key dict (rare, but possible).
//!
//! The value is kept as a Python number (`Arg`): `add(0.5)` turns the
//! counter into a float exactly as `self.value += delta` does in Python
//! (pydantic does not re-validate on assignment).

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

struct IntInner {
    value: RwLock<Arg>,
    lock: RLock,
    attrs: PyAttrs,
}

/// Thread-safe integer with atomic add, increment, decrement, set, and get operations.
#[derive(Clone)]
pub struct AtomicInt(Arc<IntInner>);

/// pydantic's `int` field coercion (lax mode): ints, integral floats,
/// numeric strings; not bools.
fn coerce_int(class: &str, v: &Arg) -> Result<Arg> {
    match v {
        Arg::Int(_) | Arg::Long(_) => Ok(v.clone()),
        Arg::Float(f) if crate::_compat::fmath::fract(*f) == 0.0 => Ok(Arg::Int(*f as i64)),
        Arg::Str(s) => s
            .trim()
            .parse::<i64>()
            .map(Arg::Int)
            .map_err(|_| validation_error(class, "value", "integer", v)),
        _ => Err(validation_error(class, "value", "integer", v)),
    }
}

impl AtomicInt {
    /// `AtomicInt(value=0)`
    pub fn new(args: impl Into<Args>) -> Result<AtomicInt> {
        let mut a: Args = args.into();
        reject_positional("AtomicInt", &a)?;
        let value = a.pop_or("value", 0i64);
        Ok(Self::from_value(coerce_int("AtomicInt", &value)?))
    }

    fn from_value(value: Arg) -> AtomicInt {
        let attrs = PyAttrs::new();
        let _ = attrs.__setattr__("_creation_timestamp", _now_creation_timestamp());
        AtomicInt(Arc::new(IntInner {
            value: RwLock::new(value),
            lock: RLock::new(),
            attrs,
        }))
    }

    /// Python `is`.
    pub fn is(&self, other: &AtomicInt) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// `self._lock`
    pub fn _lock(&self) -> RLock {
        self.0.lock.clone()
    }

    fn held(&self) -> Held {
        Held::acquire(&self.0.lock)
    }

    /// `self.value` (raw field read; the lock-guarded reader is [`get`](Self::get)).
    pub fn value(&self) -> Arg {
        self.0.value.read().clone()
    }

    /// `self.value = v` (`locked.value += 10` inside `with i.atomic()`).
    pub fn set_value(&self, v: impl IntoArg) -> Result<()> {
        *self.0.value.write() = v.into_arg();
        Ok(())
    }

    /// Set the integer to `int(new_value)`.
    pub fn set_to(&self, new_value: impl IntoArg) -> Result<()> {
        let _g = self.held();
        let v = crate::python::builtins::int(new_value)?;
        *self.0.value.write() = Arg::Int(v);
        Ok(())
    }

    /// Return the current integer value.
    #[pysig("get(self)")]
    pub fn get(&self, args: impl Into<Args>) -> Result<Arg> {
        let _g = self.held();
        Ok(self.0.value.read().clone())
    }

    /// Add *delta* to the value and return the result.
    pub fn add(&self, delta: impl IntoArg) -> Result<Arg> {
        let _g = self.held();
        let mut v = self.0.value.write();
        let new = (v.clone() + delta.into_arg())?;
        *v = new.clone();
        Ok(new)
    }

    /// Increment by 1 and return the new value.
    #[pysig("increment(self)")]
    pub fn increment(&self, args: impl Into<Args>) -> Result<Arg> {
        self.add(1i64)
    }

    /// Decrement by 1 and return the new value.
    pub fn decrement(&self) -> Result<Arg> {
        self.add(-1i64)
    }

    /// Reset the value to zero.
    pub fn reset(&self) {
        let _g = self.held();
        *self.0.value.write() = Arg::Int(0);
    }

    /// `with i.atomic() as locked:` -- a context manager holding the lock;
    /// `__enter__` yields the counter itself.
    #[pysig("atomic(self)")]
    pub fn atomic(&self, args: impl Into<Args>) -> Result<IntAtomic> {
        Ok(IntAtomic(self.clone()))
    }

    /// `int(i)`: allow use in integer expressions.
    pub fn __int__(&self) -> Result<i64> {
        crate::python::builtins::int(self.get(Args::new())?)
    }

    /// `repr(i)` -> `AtomicInt(5)`
    pub fn __repr__(&self) -> String {
        alloc::format!(
            "AtomicInt({})",
            self.get(Args::new())
                .map(|v| v.__repr__())
                .unwrap_or_default()
        )
    }

    /// `str(i)` -> `value=5` (pydantic's default `__str__` lists the fields).
    pub fn __str__(&self) -> String {
        alloc::format!(
            "value={}",
            self.get(Args::new())
                .map(|v| v.__repr__())
                .unwrap_or_default()
        )
    }

    /// `i.name`
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "value" => Ok(self.value()),
            "_lock" => Ok(self._lock().into_arg()),
            "creation_timestamp" | "_creation_timestamp" => {
                Ok(_LAILA_LOCALLY_ATOMIC_OBJECT::creation_timestamp(self))
            }
            _ => self.0.attrs.__getattr__(name).map_err(|_| {
                LailaError::attribute_error(alloc::format!(
                    "'AtomicInt' object has no attribute '{name}'"
                ))
            }),
        }
    }

    /// `i.name = value`
    pub fn __setattr__(&self, name: &str, value: impl IntoArg) -> Result<()> {
        match name {
            "value" => self.set_value(value),
            _ => self.0.attrs.__setattr__(name, value),
        }
    }

    /// `copy.deepcopy(i)`: same value, fresh lock.
    pub fn deepcopy(&self) -> AtomicInt {
        let _g = self.held();
        let out = Self::from_value(self.0.value.read().clone());
        let _ = crate::atomic::definitions::locally_atomic_object::deepcopy_attrs(
            &self.0.attrs,
            &out.0.attrs,
        );
        out
    }
}

impl Default for AtomicInt {
    fn default() -> Self {
        Self::from_value(Arg::Int(0))
    }
}

impl core::fmt::Debug for AtomicInt {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__repr__())
    }
}

impl HasAttrs for AtomicInt {
    fn __attrs__(&self) -> &PyAttrs {
        &self.0.attrs
    }
    fn __class_name__(&self) -> &'static str {
        "AtomicInt"
    }
}
impl _LAILA_LOCALLY_ATOMIC_OBJECT for AtomicInt {}

/// Context manager that holds the integer's lock (`AtomicInt._Atomic`).
pub struct IntAtomic(AtomicInt);

/// The locked counter yielded by `with i.atomic() as locked:`; releases on drop.
pub struct IntGuard {
    int: AtomicInt,
    _held: Held,
}

impl IntAtomic {
    pub fn __enter__(&self) -> Result<IntGuard> {
        Ok(IntGuard {
            int: self.0.clone(),
            _held: Held::acquire(&self.0 .0.lock),
        })
    }
}
impl IntGuard {
    pub fn __exit__(self) -> Result<()> {
        drop(self);
        Ok(())
    }
}
/// `with x.atomic() as inner: assert inner is x` -- the guard is the
/// parent's handle.
impl PyArg for IntGuard {
    fn to_arg(&self) -> Arg {
        self.int.to_arg()
    }
}
impl core::ops::Deref for IntGuard {
    type Target = AtomicInt;
    fn deref(&self) -> &AtomicInt {
        &self.int
    }
}

impl PyObject for AtomicInt {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    /// `a is b`: identity of the shared handle, not of the boxing.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<AtomicInt>()
            .is_some_and(|o| self.is(o))
    }
    /// `copy.deepcopy(x)`: a new object with fresh locks.
    fn __deepcopy__(&self) -> Option<Arg> {
        Some(AtomicInt::deepcopy(self).into_arg())
    }
    /// `isinstance(x, _LAILA_OBJECT)`: the atomic types are laila objects.
    fn __bases__(&self) -> &'static [&'static str] {
        &["_LAILA_LOCALLY_ATOMIC_OBJECT", "_LAILA_OBJECT"]
    }
    fn type_name(&self) -> &'static str {
        "AtomicInt"
    }
    fn __repr__(&self) -> String {
        AtomicInt::__repr__(self)
    }
    fn __str__(&self) -> String {
        AtomicInt::__str__(self)
    }
    /// pydantic compares private attributes too (`_lock` differs): identity.
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<AtomicInt>()
            .is_some_and(|o| self.is(o))
    }
    fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error("unhashable type: 'AtomicInt'"))
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        AtomicInt::__getattr__(self, name)
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        AtomicInt::__setattr__(self, name, value)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let first = |what: &str| -> Result<Arg> {
            args.positional.first().cloned().ok_or_else(|| {
                LailaError::type_error(alloc::format!(
                    "AtomicInt.{name}() missing 1 required positional argument: '{what}'"
                ))
            })
        };
        Ok(match name {
            "atomic" => IntoArg::into_arg(self.atomic(args)?),
            "set_to" => {
                self.set_to(first("new_value")?)?;
                Arg::None
            }
            "get" => self.get(args)?,
            "add" => self.add(first("delta")?)?,
            "increment" => self.increment(args)?,
            "decrement" => self.decrement()?,
            "reset" => {
                self.reset();
                Arg::None
            }
            "__int__" => Arg::Int(self.__int__()?),
            "lock" => Arg::Bool(_LAILA_LOCALLY_ATOMIC_OBJECT::lock(self, args)?),
            "unlock" => {
                _LAILA_LOCALLY_ATOMIC_OBJECT::unlock(self)?;
                Arg::None
            }
            "locked" => Arg::Bool(_LAILA_LOCALLY_ATOMIC_OBJECT::locked(self, args)),
            _ => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'AtomicInt' object has no attribute '{name}'"
                )))
            }
        })
    }
}

impl IntoArg for AtomicInt {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for AtomicInt {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl IsInstance for AtomicInt {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<AtomicInt>())
    }
}
impl PartialEq for AtomicInt {
    fn eq(&self, other: &AtomicInt) -> bool {
        self.is(other)
    }
}

super::context_manager_py_object!(IntAtomic, "AtomicInt._Atomic");
super::guard_py_object!(IntGuard, "AtomicInt");
