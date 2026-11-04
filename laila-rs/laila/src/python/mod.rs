//! Python's object model, calling convention and builtins for the port.
//!
//! This module is what makes a `#[laila::python]` body compile: `Arg`
//! (Python's `object`), `Args` (`*args, **kwargs`), `py_try!` (`?` without
//! knowing the callee's type), the builtins, and `unittest.TestCase`.

pub mod args;
pub mod builtins;
pub mod context;
pub mod errors;
pub mod fstr;
pub mod io;
pub mod methods;
pub mod mock;
pub mod object;
pub mod ops;
pub mod pyclass;
pub mod pyrepr;
pub mod pytry;
pub mod stdlib;
pub mod strfmt;
pub mod sync;
pub mod unittest;

pub use args::{Args, Bound, IntoArgs, Param, ParamKind, Signature};
pub use errors::{ExcKind, ExcMarker, LailaError, Result};
pub use methods::PyDictBuilder;
pub use object::{
    arg, callable, class_attrs, slice, Arg, Callable, Cell, FromArg, HasAttrs, IntoArg, PyArg,
    PyAttrs, PyDict, PyList, PyObject, RetInto, Slice,
};
pub use pyclass::PyClass;

/// The methods a `py_class!` instance answers when called dynamically
/// (`obj.__call_method__(name, args)` on an instance passed as a value):
/// `laila::python!` implements it from the class's `impl` block.
pub trait PyClassDispatch {
    /// `obj.name(*args, **kwargs)`; `None` when the class has no such method.
    fn __laila_dispatch__(&self, name: &str, args: Args) -> Option<Result<object::Arg>>;
    /// `obj.name` for a `#[property]` method; `None` when not a property.
    fn __laila_property__(&self, _name: &str) -> Option<Result<object::Arg>> {
        None
    }
    /// The names `__laila_dispatch__` answers (a read of one is a bound method).
    fn __laila_method_names__() -> &'static [&'static str] {
        &[]
    }
}
pub use unittest::pytest;

/// Runtime paths used by the proc-macro expansions (so user crates need no
/// `extern crate alloc`).
#[doc(hidden)]
pub mod __rt {
    pub use alloc::format;
    pub use alloc::string::{String, ToString};
    pub use alloc::sync::Arc;
    pub use alloc::vec;
    pub use alloc::vec::Vec;
}

/// Re-exported proc-macros: `#[laila::python]`, `#[laila::pysig]`, `args!`, `py!`.
pub use laila_macros::{args, class_attr, property, py, pysig, python};

/// Declares a Python-style class with a dynamic `__dict__`: fields are set
/// and read through `__setattr__`/`__getattr__` (unittest fixtures, scripts).
///
/// Instances are handles (`Clone` aliases the same `__dict__`, as a Python
/// reference does), `Name(k=v, ..)` is `Name::new(args)` (keyword arguments
/// become attributes, like pydantic fields), and the struct implements
/// [`HasAttrs`](crate::python::HasAttrs) so the `laila.atomic` mixin traits
/// can be implemented for it with an empty `impl Mixin for Name {}`.
#[macro_export]
macro_rules! py_class {
    ($(#[$m:meta])* $vis:vis $name:ident) => {
        $crate::py_class!(@kw $(#[$m])* $vis $name; @nodispatch);
    };
    // `py_class!(Name; init)`: the class defines `__init__(self, ..)` (a
    // `#[pysig]` method taking `Args`); `Name(..)` runs it on the fresh
    // instance.
    ($(#[$m:meta])* $vis:vis $name:ident; init) => {
        $crate::py_class!(@init $(#[$m])* $vis $name; @nodispatch);
    };
    // `; dispatch` (emitted by `laila::python!`): the class implements
    // `PyClassDispatch`, so its `impl` methods are reachable from a dynamic
    // call on an instance passed as a value.
    ($(#[$m:meta])* $vis:vis $name:ident; dispatch) => {
        $crate::py_class!(@kw $(#[$m])* $vis $name; @dispatch);
    };
    ($(#[$m:meta])* $vis:vis $name:ident; init; dispatch) => {
        $crate::py_class!(@init $(#[$m])* $vis $name; @dispatch);
    };
    (@kw $(#[$m:meta])* $vis:vis $name:ident; @$d:ident) => {
        $crate::py_class!(@define $(#[$m])* $vis $name; @$d; |a, me| {
            if !a.positional.is_empty() {
                return Err($crate::python::LailaError::type_error(
                    $crate::python::__rt::format!(
                        "{}() takes no positional arguments",
                        stringify!($name)
                    ),
                ));
            }
            for (k, v) in a.keywords {
                me.__attrs.__setattr__(&k, v)?;
            }
        });
    };
    (@init $(#[$m:meta])* $vis:vis $name:ident; @$d:ident) => {
        $crate::py_class!(@define $(#[$m])* $vis $name; @$d; |a, me| {
            me.__init__(a)?;
        });
    };
    (@getattr nodispatch $me:ident $name:ident $n:ident) => {
        ::core::option::Option::None
    };
    (@getattr dispatch $me:ident $name:ident $n:ident) => {
        match <$name as $crate::python::PyClassDispatch>::__laila_property__($me, $n) {
            Some(r) => Some(r),
            None if <$name as $crate::python::PyClassDispatch>::__laila_method_names__().contains(&$n) => {
                let __laila_self = $me.clone();
                let __laila_name = $crate::python::__rt::String::from($n);
                Some(Ok($crate::python::callable(move |a: $crate::python::Args| {
                    <$name as $crate::python::PyClassDispatch>::__laila_dispatch__(&__laila_self, &__laila_name, a)
                        .unwrap_or_else(|| Err($crate::python::LailaError::attribute_error(__laila_name.clone())))
                })))
            }
            None => None,
        }
    };
    (@call_method nodispatch $me:ident $name:ident $n:ident $args:ident) => {
        Err($crate::python::LailaError::attribute_error($crate::python::__rt::format!(
            "'{}' object has no attribute '{}'",
            stringify!($name),
            $n
        )))
    };
    (@call_method dispatch $me:ident $name:ident $n:ident $args:ident) => {
        match <$name as $crate::python::PyClassDispatch>::__laila_dispatch__($me, $n, $args) {
            Some(r) => r,
            None => Err($crate::python::LailaError::attribute_error($crate::python::__rt::format!(
                "'{}' object has no attribute '{}'",
                stringify!($name),
                $n
            ))),
        }
    };
    (@define $(#[$m:meta])* $vis:vis $name:ident; @$d:ident; |$a:ident, $me:ident| { $($init:tt)* }) => {
        $(#[$m])*
        #[derive(Clone, Default)]
        $vis struct $name {
            pub __attrs: $crate::python::__rt::Arc<$crate::python::PyAttrs>,
        }
        impl $name {
            /// `Name(..)`: keyword arguments become attributes, or `__init__` runs.
            pub fn new(args: impl Into<$crate::python::Args>) -> $crate::python::Result<Self> {
                let $a: $crate::python::Args = args.into();
                let $me = Self::default();
                { $($init)* }
                Ok($me)
            }
            /// The class `__dict__` (`cls.x = v` in a classmethod writes here;
            /// an instance read falls back here).
            pub fn __class_attrs__() -> $crate::python::__rt::Arc<$crate::python::PyAttrs> {
                $crate::python::class_attrs(stringify!($name))
            }
            /// The class as a handle: `cls` of a classmethod -- a `Self` whose
            /// `__dict__` *is* the class `__dict__`, so `cls.x = v` / `cls.x`
            /// are class attribute writes / reads.
            pub fn __class__() -> Self {
                Self { __attrs: Self::__class_attrs__() }
            }
            /// `obj.name`: the instance `__dict__`, then the class `__dict__`,
            /// then (with `dispatch`) the class's `#[class_attr]` / `#[property]`
            /// getters and its methods as bound callables.
            pub fn __getattr__(&self, name: &str) -> $crate::python::Result<$crate::python::Arg> {
                match self.__attrs.__getattr__(name) {
                    Ok(v) => Ok(v),
                    Err(e) => match Self::__class_attrs__().get(name) {
                        Some(v) => Ok(v),
                        None => match $crate::py_class!(@getattr $d self $name name) {
                            Some(r) => r,
                            None => Err(e),
                        },
                    },
                }
            }
            pub fn __setattr__(&self, name: &str, value: impl $crate::python::IntoArg) -> $crate::python::Result<()> {
                self.__attrs.__setattr__(name, value)
            }
            pub fn __hasattr__(&self, name: &str) -> bool {
                self.__attrs.__hasattr__(name) || Self::__class_attrs__().__hasattr__(name)
            }
            /// Python `is`: both handles share one `__dict__`.
            pub fn is(&self, other: &Self) -> bool {
                $crate::python::__rt::Arc::ptr_eq(&self.__attrs, &other.__attrs)
            }
        }
        impl $crate::python::HasAttrs for $name {
            fn __attrs__(&self) -> &$crate::python::PyAttrs {
                &self.__attrs
            }
            fn __class_name__(&self) -> &'static str {
                stringify!($name)
            }
        }
        // The instance as a Python object (`repr(c)`, `isinstance(c, Name)`,
        // a callable's argument, a list element): identity equality,
        // `Name(a=1, b=2)` repr from `__dict__`, attributes through it.
        impl $crate::python::PyObject for $name {
            fn as_any(&self) -> &dyn core::any::Any {
                self
            }
            fn type_name(&self) -> &'static str {
                stringify!($name)
            }
            fn __repr__(&self) -> $crate::python::__rt::String {
                let fields: $crate::python::__rt::Vec<$crate::python::__rt::String> = self
                    .__attrs
                    .__dict__()
                    .into_iter()
                    .filter(|(k, _)| !k.starts_with('_'))
                    .map(|(k, v)| $crate::python::__rt::format!("{k}={}", v.__repr__()))
                    .collect();
                $crate::python::__rt::format!("{}({})", stringify!($name), fields.join(", "))
            }
            fn __eq__(&self, other: &dyn $crate::python::PyObject) -> bool {
                other
                    .as_any()
                    .downcast_ref::<$name>()
                    .is_some_and(|o| self.is(o))
            }
            fn __hash__(&self) -> $crate::python::Result<i64> {
                Ok($crate::python::__rt::Arc::as_ptr(&self.__attrs) as usize as i64)
            }
            fn __getattr__(&self, name: &str) -> $crate::python::Result<$crate::python::Arg> {
                match $name::__getattr__(self, name) {
                    Ok(v) => Ok(v),
                    Err(_) => match $crate::py_class!(@getattr $d self $name name) {
                        Some(r) => r,
                        None => Err($crate::python::LailaError::attribute_error($crate::python::__rt::format!(
                            "'{}' object has no attribute '{name}'",
                            stringify!($name)
                        ))),
                    },
                }
            }
            fn __setattr__(&self, name: &str, value: $crate::python::Arg) -> $crate::python::Result<()> {
                self.__attrs.__setattr__(name, value)
            }
            fn __delattr__(&self, name: &str) -> $crate::python::Result<()> {
                self.__attrs.__delattr__(name)
            }
            /// `obj.m(..)` from a dynamic call: a callable attribute of the
            /// instance first, then the class's `impl` methods (`dispatch`).
            fn __call_method__(&self, name: &str, args: $crate::python::Args) -> $crate::python::Result<$crate::python::Arg> {
                if let Ok(f @ $crate::python::Arg::Callable(_)) = self.__attrs.__getattr__(name) {
                    return f.__call__(args);
                }
                let __laila_args = args.clone();
                match $crate::py_class!(@call_method $d self $name name args) {
                    // The locally-atomic mixin's verbs on a handle reached as
                    // a value (`a, b = _Obj(), _Obj(); a.lock()`).
                    Err(e) if e.kind == $crate::python::ExcKind::AttributeError => {
                        $crate::atomic::definitions::locally_atomic_object::dyn_atomic_call_method(
                            &self.__attrs, stringify!($name), name, __laila_args,
                        )
                        .unwrap_or(Err(e))
                    }
                    other => other,
                }
            }
        }
        impl $crate::python::IntoArg for $name {
            fn into_arg(self) -> $crate::python::Arg {
                $crate::python::Arg::Object($crate::python::__rt::Arc::new(self))
            }
        }
        impl $crate::python::PyArg for $name {
            fn to_arg(&self) -> $crate::python::Arg {
                $crate::python::Arg::Object($crate::python::__rt::Arc::new(self.clone()))
            }
        }
        impl $crate::python::builtins::IsInstance for $name {
            fn isinstance(x: &$crate::python::Arg) -> bool {
                matches!(x, $crate::python::Arg::Object(o) if o.as_any().is::<$name>())
            }
        }
    };
}

/// `use laila::python::prelude::*` -- everything a Python-shaped body needs.
pub mod prelude {
    pub use super::args::{Args, IntoArgs};
    pub use super::builtins::*;
    pub use super::errors::*;
    pub use super::object::{arg, callable, slice, Arg, IntoArg, PyArg, PyAttrs, Slice};
    // The object protocol on typed values held by hand-written twins
    // (`p[0].types` on a `ParsedAU` reached through an `Arg` local).
    pub use super::object::PyObject as _;
    // Module statics (`uuid`, `json`, `time`, `pytest`) are deliberately NOT
    // exported: Rust forbids a `let`/parameter named like a static in scope,
    // and Python code routinely binds `uuid = ...`. `laila::python!` resolves
    // bare module names to `laila::python::stdlib::<name>` itself.
    pub use super::stdlib::PyBytesExt;
    pub use super::stdlib::PyStrExt;
    pub use super::stdlib::PyVecExt;
    // Python *classes* from the compat layer (CamelCase, so they never
    // collide with `let` bindings): `DotMap(..)`, `Fernet(key)`, `BytesIO`.
    pub use super::stdlib::{BytesIO, DotMap, Fernet, MultiFernet};
    // `from pathlib import Path`
    #[cfg(feature = "std")]
    pub use super::stdlib::Path;
    // `from collections import Counter`, `from pydantic import Field, PrivateAttr`,
    // `from unittest.mock import patch`, `isinstance(x, type)` / `type(x)`.
    pub use super::pyclass::r#type;
    pub use super::stdlib::Counter;
    // `from unittest.mock import Mock, MagicMock`
    pub use super::mock::{MagicMock, Mock};
    // `from unittest import mock`
    pub use super::mock;
    pub use super::unittest::patch;
    pub use super::unittest::TestCase;
    pub use crate::_compat::pydantic::{Field, PrivateAttr, PydanticUndefined};
    pub use crate::{py_class, py_object, py_try};
    pub use laila_macros::{args, class_attr, property, py, pysig, python};
}
