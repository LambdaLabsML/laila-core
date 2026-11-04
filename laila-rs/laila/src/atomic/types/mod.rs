//! Thread-safe atomic wrapper types for common Python data structures.

pub mod atomic_dict;
pub mod atomic_dotmap;
pub mod atomic_flag;
pub mod atomic_int;
pub mod atomic_list;
pub mod atomic_str;

pub use atomic_dict::AtomicDict;
pub use atomic_dotmap::AtomicDotMap;
pub use atomic_flag::AtomicFlag;
pub use atomic_int::AtomicInt;
pub use atomic_list::AtomicList;
pub use atomic_str::AtomicStr;

use crate::python::args::Args;
use crate::python::errors::{LailaError, Result};
use crate::python::object::Arg;
use crate::python::sync::RLock;

/// Hold an `RLock` for a Rust scope (`with self._lock:`); releases on drop.
pub(crate) struct Held(RLock);

impl Held {
    pub(crate) fn acquire(lock: &RLock) -> Held {
        lock.acquire(None);
        Held(lock.clone())
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        let _ = self.0.release();
    }
}

/// `BaseModel.__init__` rejects positional arguments; the Atomic* models are
/// built from keywords only (`AtomicFlag(value=True)`).
pub(crate) fn reject_positional(class: &str, a: &Args) -> Result<()> {
    if a.positional.is_empty() {
        return Ok(());
    }
    Err(LailaError::type_error(alloc::format!(
        "{class}.__init__() takes 1 positional argument but {} were given",
        a.positional.len() + 1
    )))
}

/// pydantic's `ValidationError` text for a field of the wrong type.
pub(crate) fn validation_error(class: &str, field: &str, expected: &str, got: &Arg) -> LailaError {
    LailaError::new(
        crate::python::errors::ExcKind::ValidationError,
        alloc::format!(
            "1 validation error for {class}\n{field}\n  Input should be a valid {expected} \
             [type={expected}_type, input_value={}, input_type={}]",
            got.__repr__(),
            got.type_name()
        ),
    )
}

/// `isinstance(x, str)` check shared by the `hint=` parameters.
pub(crate) fn require_str(v: &Arg, msg: &str) -> Result<()> {
    match v {
        Arg::Str(_) => Ok(()),
        _ => Err(LailaError::type_error(msg)),
    }
}

/// The dynamic surface of a context manager (`obj.atomic()` as an `Arg`):
/// `__enter__` yields the guard as an `Arg`; `__exit__` is the guard's drop.
macro_rules! context_manager_py_object {
    ($cm:ty, $name:literal) => {
        impl $crate::python::object::PyObject for $cm {
            fn as_any(&self) -> &dyn core::any::Any {
                self
            }
            fn type_name(&self) -> &'static str {
                $name
            }
            fn __repr__(&self) -> alloc::string::String {
                alloc::format!("<{} object>", $name)
            }
            fn __eq__(&self, other: &dyn $crate::python::object::PyObject) -> bool {
                core::ptr::eq(
                    other.as_any() as *const dyn core::any::Any as *const (),
                    self as *const Self as *const (),
                )
            }
            fn __hash__(&self) -> $crate::python::Result<i64> {
                Ok(self as *const Self as usize as i64)
            }
            fn __call_method__(
                &self,
                name: &str,
                args: $crate::python::Args,
            ) -> $crate::python::Result<$crate::python::Arg> {
                match name {
                    "__enter__" => Ok($crate::python::IntoArg::into_arg(self.__enter__()?)),
                    "__exit__" => {
                        let _ = args;
                        Ok($crate::python::Arg::Bool(false))
                    }
                    _ => Err($crate::python::LailaError::attribute_error(alloc::format!(
                        "'{}' object has no attribute '{name}'",
                        $name
                    ))),
                }
            }
        }
        impl $crate::python::IntoArg for $cm {
            fn into_arg(self) -> $crate::python::Arg {
                $crate::python::Arg::Object(alloc::sync::Arc::new(self))
            }
        }
    };
}

/// The dynamic surface of a lock guard: every protocol call is forwarded to
/// the object it derefs to (so `locked.value = True` works on an `Arg` too).
macro_rules! guard_py_object {
    ($guard:ty, $name:literal) => {
        impl $crate::python::object::PyObject for $guard {
            fn as_any(&self) -> &dyn core::any::Any {
                self
            }
            fn type_name(&self) -> &'static str {
                $name
            }
            fn __repr__(&self) -> alloc::string::String {
                $crate::python::object::PyObject::__repr__(&**self)
            }
            fn __str__(&self) -> alloc::string::String {
                $crate::python::object::PyObject::__str__(&**self)
            }
            fn __eq__(&self, other: &dyn $crate::python::object::PyObject) -> bool {
                $crate::python::object::PyObject::__eq__(&**self, other)
            }
            fn __eq_arg__(&self, other: &$crate::python::Arg) -> bool {
                $crate::python::object::PyObject::__eq_arg__(&**self, other)
            }
            fn __hash__(&self) -> $crate::python::Result<i64> {
                $crate::python::object::PyObject::__hash__(&**self)
            }
            fn __getattr__(&self, name: &str) -> $crate::python::Result<$crate::python::Arg> {
                $crate::python::object::PyObject::__getattr__(&**self, name)
            }
            fn __setattr__(
                &self,
                name: &str,
                value: $crate::python::Arg,
            ) -> $crate::python::Result<()> {
                $crate::python::object::PyObject::__setattr__(&**self, name, value)
            }
            fn __delattr__(&self, name: &str) -> $crate::python::Result<()> {
                $crate::python::object::PyObject::__delattr__(&**self, name)
            }
            fn __call_method__(
                &self,
                name: &str,
                args: $crate::python::Args,
            ) -> $crate::python::Result<$crate::python::Arg> {
                $crate::python::object::PyObject::__call_method__(&**self, name, args)
            }
            fn __len__(&self) -> $crate::python::Result<usize> {
                $crate::python::object::PyObject::__len__(&**self)
            }
            fn __bool__(&self) -> bool {
                $crate::python::object::PyObject::__bool__(&**self)
            }
            fn __contains__(&self, item: &$crate::python::Arg) -> $crate::python::Result<bool> {
                $crate::python::object::PyObject::__contains__(&**self, item)
            }
            fn __iter__(&self) -> $crate::python::Result<alloc::vec::Vec<$crate::python::Arg>> {
                $crate::python::object::PyObject::__iter__(&**self)
            }
            fn __getitem__(
                &self,
                key: &$crate::python::Arg,
            ) -> $crate::python::Result<$crate::python::Arg> {
                $crate::python::object::PyObject::__getitem__(&**self, key)
            }
            fn __setitem__(
                &self,
                key: &$crate::python::Arg,
                value: $crate::python::Arg,
            ) -> $crate::python::Result<()> {
                $crate::python::object::PyObject::__setitem__(&**self, key, value)
            }
            fn __delitem__(&self, key: &$crate::python::Arg) -> $crate::python::Result<()> {
                $crate::python::object::PyObject::__delitem__(&**self, key)
            }
        }
        impl $crate::python::IntoArg for $guard {
            fn into_arg(self) -> $crate::python::Arg {
                $crate::python::Arg::Object(alloc::sync::Arc::new(self))
            }
        }
    };
}

pub(crate) use context_manager_py_object;
pub(crate) use guard_py_object;
