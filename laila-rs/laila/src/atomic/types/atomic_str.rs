//! Thread-safe mutable string.
//!
//! [`AtomicStr`] wraps a Python string with an `RLock` so set / append /
//! clear / get operations are race-free between threads. Useful for shared
//! status text (e.g. "what is the worker doing right now") where multiple
//! producers update a human-readable label and consumers read it without
//! locking themselves.
//!
//! Strings in Python are immutable, so every "mutation" is really a re-bind
//! of the underlying `value` field; the lock ensures readers always see a
//! consistent snapshot rather than a partially-updated intermediate.
//!
//! `append` is amortised linear (F-27). CPython pops the value out of the
//! instance dict so the local holds the *only* reference and the string is
//! extended in place; a caller that keeps the returned string alive pays one
//! copy on the next call. The Rust twin is the same idea with reference
//! counts: the value is an `Arc<String>`, `append` extends it in place with
//! `Arc::make_mut` (no other handle alive) and hands out a [`PyStr`] handle
//! -- if the caller kept the previous one, the next append copies once.

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

struct StrInner {
    value: RwLock<Arc<String>>,
    lock: RLock,
    attrs: PyAttrs,
}

/// A Python `str` value handed out by [`AtomicStr`]: an immutable, shared
/// string (`Deref<Target = str>`); converting it to an `Arg` copies once.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PyStr(Arc<String>);

impl PyStr {
    /// The text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
    /// `len(s)` in code points.
    pub fn __len__(&self) -> usize {
        self.0.chars().count()
    }
}
impl core::ops::Deref for PyStr {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}
impl core::fmt::Display for PyStr {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.0)
    }
}
impl core::fmt::Debug for PyStr {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&crate::python::pyrepr::str_repr(&self.0))
    }
}
impl PartialEq<str> for PyStr {
    fn eq(&self, other: &str) -> bool {
        self.0.as_str() == other
    }
}
impl PartialEq<&str> for PyStr {
    fn eq(&self, other: &&str) -> bool {
        self.0.as_str() == *other
    }
}
impl PartialEq<String> for PyStr {
    fn eq(&self, other: &String) -> bool {
        *self.0 == *other
    }
}
impl From<PyStr> for String {
    fn from(s: PyStr) -> String {
        (*s.0).clone()
    }
}
impl IntoArg for PyStr {
    fn into_arg(self) -> Arg {
        Arg::Str(Arc::from(self.0.as_str()))
    }
}
impl PyArg for PyStr {
    fn to_arg(&self) -> Arg {
        Arg::Str(Arc::from(self.0.as_str()))
    }
}
impl crate::python::builtins::PyLen for PyStr {
    fn __len__(&self) -> Result<usize> {
        Ok(PyStr::__len__(self))
    }
}
impl crate::python::builtins::Truthy for PyStr {
    fn __bool__(&self) -> bool {
        !self.0.is_empty()
    }
}
impl crate::python::stdlib::PyStrExt for PyStr {
    fn as_pystr(&self) -> &str {
        &self.0
    }
}

/// Thread-safe string with atomic set, append, clear, and get operations.
#[derive(Clone)]
pub struct AtomicStr(Arc<StrInner>);

impl AtomicStr {
    /// `AtomicStr(value="")`
    pub fn new(args: impl Into<Args>) -> Result<AtomicStr> {
        let mut a: Args = args.into();
        reject_positional("AtomicStr", &a)?;
        let value = a.pop_or("value", "");
        let value = match &value {
            Arg::Str(s) => String::from(&**s),
            other => return Err(validation_error("AtomicStr", "value", "string", other)),
        };
        Ok(Self::from_value(value))
    }

    fn from_value(value: String) -> AtomicStr {
        let attrs = PyAttrs::new();
        let _ = attrs.__setattr__("_creation_timestamp", _now_creation_timestamp());
        AtomicStr(Arc::new(StrInner {
            value: RwLock::new(Arc::new(value)),
            lock: RLock::new(),
            attrs,
        }))
    }

    /// Python `is`.
    pub fn is(&self, other: &AtomicStr) -> bool {
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
    pub fn value(&self) -> PyStr {
        PyStr(self.0.value.read().clone())
    }

    /// `self.value = v` (`locked.value = "locked"` inside `with s.atomic()`).
    pub fn set_value(&self, v: impl IntoArg) -> Result<()> {
        *self.0.value.write() = Arc::new(v.into_arg().__str__());
        Ok(())
    }

    /// Replace the string with `str(new_value)`.
    #[pysig("set(self, new_value)")]
    pub fn set(&self, args: impl Into<Args>) -> Result<()> {
        let _g = self.held();
        *self.0.value.write() = Arc::new(new_value.__str__());
        Ok(())
    }

    /// Return the current string value.
    #[pysig("get(self)")]
    pub fn get(&self, args: impl Into<Args>) -> Result<PyStr> {
        let _g = self.held();
        Ok(PyStr(self.0.value.read().clone()))
    }

    /// Append `str(suffix)` and return the resulting string.
    ///
    /// Amortised O(len(suffix)): when no caller holds a previous result the
    /// stored buffer is the only reference and grows in place; a caller that
    /// keeps the returned string alive across appends pays one copy on the
    /// next call (`Arc::make_mut`) -- CPython's behaviour exactly.
    pub fn append(&self, suffix: impl IntoArg) -> Result<PyStr> {
        let _g = self.held();
        let mut v = self.0.value.write();
        Arc::make_mut(&mut *v).push_str(&suffix.into_arg().__str__());
        Ok(PyStr(v.clone()))
    }

    /// Reset the string to empty.
    #[pysig("clear(self)")]
    pub fn clear(&self, args: impl Into<Args>) -> Result<()> {
        let _g = self.held();
        *self.0.value.write() = Arc::new(String::new());
        Ok(())
    }

    /// Return the length of the string (in code points, like `len(str)`).
    pub fn length(&self) -> i64 {
        let _g = self.held();
        self.0.value.read().chars().count() as i64
    }

    /// `with s.atomic() as locked:` -- a context manager holding the lock;
    /// `__enter__` yields the string object itself.
    #[pysig("atomic(self)")]
    pub fn atomic(&self, args: impl Into<Args>) -> Result<StrAtomic> {
        Ok(StrAtomic(self.clone()))
    }

    /// `str(s)`: the string value.
    pub fn __str__(&self) -> String {
        self.get(Args::new())
            .map(|s| s.as_str().into())
            .unwrap_or_default()
    }

    /// `repr(s)` -> `AtomicStr('abc')`
    pub fn __repr__(&self) -> String {
        alloc::format!(
            "AtomicStr({})",
            crate::python::pyrepr::str_repr(&self.__str__())
        )
    }

    /// `s.name`
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "value" => Ok(self.value().into_arg()),
            "_lock" => Ok(self._lock().into_arg()),
            "creation_timestamp" | "_creation_timestamp" => {
                Ok(_LAILA_LOCALLY_ATOMIC_OBJECT::creation_timestamp(self))
            }
            _ => self.0.attrs.__getattr__(name).map_err(|_| {
                LailaError::attribute_error(alloc::format!(
                    "'AtomicStr' object has no attribute '{name}'"
                ))
            }),
        }
    }

    /// `s.name = value`
    pub fn __setattr__(&self, name: &str, value: impl IntoArg) -> Result<()> {
        match name {
            "value" => self.set_value(value),
            _ => self.0.attrs.__setattr__(name, value),
        }
    }

    /// `copy.deepcopy(s)`: same value, fresh lock.
    pub fn deepcopy(&self) -> AtomicStr {
        let _g = self.held();
        let out = Self::from_value((**self.0.value.read()).clone());
        let _ = crate::atomic::definitions::locally_atomic_object::deepcopy_attrs(
            &self.0.attrs,
            &out.0.attrs,
        );
        out
    }
}

impl Default for AtomicStr {
    fn default() -> Self {
        Self::from_value(String::new())
    }
}

impl core::fmt::Debug for AtomicStr {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__repr__())
    }
}
impl core::fmt::Display for AtomicStr {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__str__())
    }
}

impl HasAttrs for AtomicStr {
    fn __attrs__(&self) -> &PyAttrs {
        &self.0.attrs
    }
    fn __class_name__(&self) -> &'static str {
        "AtomicStr"
    }
}
impl _LAILA_LOCALLY_ATOMIC_OBJECT for AtomicStr {}

/// Context manager that holds the string's lock (`AtomicStr._Atomic`).
pub struct StrAtomic(AtomicStr);

/// The locked string yielded by `with s.atomic() as locked:`; releases on drop.
pub struct StrGuard {
    s: AtomicStr,
    _held: Held,
}

impl StrAtomic {
    pub fn __enter__(&self) -> Result<StrGuard> {
        Ok(StrGuard {
            s: self.0.clone(),
            _held: Held::acquire(&self.0 .0.lock),
        })
    }
}
impl StrGuard {
    pub fn __exit__(self) -> Result<()> {
        drop(self);
        Ok(())
    }
}
/// `with x.atomic() as inner: assert inner is x` -- the guard is the
/// parent's handle.
impl PyArg for StrGuard {
    fn to_arg(&self) -> Arg {
        self.s.to_arg()
    }
}
impl core::ops::Deref for StrGuard {
    type Target = AtomicStr;
    fn deref(&self) -> &AtomicStr {
        &self.s
    }
}

impl PyObject for AtomicStr {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    /// `a is b`: identity of the shared handle, not of the boxing.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<AtomicStr>()
            .is_some_and(|o| self.is(o))
    }
    /// `copy.deepcopy(x)`: a new object with fresh locks.
    fn __deepcopy__(&self) -> Option<Arg> {
        Some(AtomicStr::deepcopy(self).into_arg())
    }
    /// `isinstance(x, _LAILA_OBJECT)`: the atomic types are laila objects.
    fn __bases__(&self) -> &'static [&'static str] {
        &["_LAILA_LOCALLY_ATOMIC_OBJECT", "_LAILA_OBJECT"]
    }
    fn type_name(&self) -> &'static str {
        "AtomicStr"
    }
    fn __repr__(&self) -> String {
        AtomicStr::__repr__(self)
    }
    fn __str__(&self) -> String {
        AtomicStr::__str__(self)
    }
    /// pydantic compares private attributes too (`_lock` differs): identity.
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<AtomicStr>()
            .is_some_and(|o| self.is(o))
    }
    fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error("unhashable type: 'AtomicStr'"))
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        AtomicStr::__getattr__(self, name)
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        AtomicStr::__setattr__(self, name, value)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        Ok(match name {
            "atomic" => IntoArg::into_arg(self.atomic(args)?),
            "set" => {
                self.set(args)?;
                Arg::None
            }
            "get" => self.get(args)?.into_arg(),
            "append" => {
                let suffix = args.positional.first().cloned().ok_or_else(|| {
                    LailaError::type_error(
                        "AtomicStr.append() missing 1 required positional argument: 'suffix'",
                    )
                })?;
                self.append(suffix)?.into_arg()
            }
            "clear" => {
                self.clear(args)?;
                Arg::None
            }
            "length" => Arg::Int(self.length()),
            "lock" => Arg::Bool(_LAILA_LOCALLY_ATOMIC_OBJECT::lock(self, args)?),
            "unlock" => {
                _LAILA_LOCALLY_ATOMIC_OBJECT::unlock(self)?;
                Arg::None
            }
            "locked" => Arg::Bool(_LAILA_LOCALLY_ATOMIC_OBJECT::locked(self, args)),
            _ => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'AtomicStr' object has no attribute '{name}'"
                )))
            }
        })
    }
}

impl IntoArg for AtomicStr {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
/// `len(s)`: an `AtomicStr` has no `__len__` (use `s.length()`).
impl crate::python::builtins::PyLen for AtomicStr {
    fn __len__(&self) -> Result<usize> {
        Err(LailaError::type_error(
            "object of type 'AtomicStr' has no len()",
        ))
    }
}
impl PyArg for AtomicStr {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl IsInstance for AtomicStr {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<AtomicStr>())
    }
}
impl PartialEq for AtomicStr {
    fn eq(&self, other: &AtomicStr) -> bool {
        self.is(other)
    }
}

super::context_manager_py_object!(StrAtomic, "AtomicStr._Atomic");
super::guard_py_object!(StrGuard, "AtomicStr");
