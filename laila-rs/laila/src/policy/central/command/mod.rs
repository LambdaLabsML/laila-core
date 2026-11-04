//! `laila.policy.central.command` -- task-forces, futures, and submission
//! machinery.
//!
//! - [`schema`] -- the central command class itself ([`Command`]), the
//!   future hierarchy ([`Future`] tagged by [`FutureKind`]: `Future`,
//!   `GroupFuture`, `ComplexFuture`, `ConcurrentPackageFuture`,
//!   `_LAILA_IDENTIFIABLE_FUTURE`), slot parking and the exceptions.
//! - [`taskforce`] -- the `_LAILA_IDENTIFIABLE_TASK_FORCE` base and the
//!   async-thread-pool backend ([`TaskForce`]): a pool of threads each
//!   running its own event loop; coroutines are routed to a loop and
//!   awaited there.

/// A Python class marker for one member of the family: `isinstance`,
/// `Cls(**kwargs)` construction, and the class object for the dynamic
/// layer.
macro_rules! future_class_marker {
    ($ty:ident, $name:literal, $module:literal, $kind:expr, |$k:ident| $is:expr) => {
        #[allow(non_camel_case_types)]
        #[derive(Clone, Copy, Debug, Default)]
        pub struct $ty;

        impl $ty {
            #[allow(clippy::new_ret_no_self)]
            pub fn new(args: impl Into<$crate::python::Args>) -> $crate::python::Result<Future> {
                Future::construct($kind, args.into())
            }
            pub fn class_object() -> $crate::python::PyClass {
                $crate::python::PyClass::with_ctor(
                    $name,
                    $module,
                    ::alloc::sync::Arc::new(|a: $crate::python::Args| {
                        Future::construct($kind, a).map($crate::python::Arg::Future)
                    }),
                )
            }
        }
        impl $crate::python::builtins::IsInstance for $ty {
            fn isinstance(x: &$crate::python::Arg) -> bool {
                match x {
                    $crate::python::Arg::Future(f) => {
                        let $k = f.kind();
                        $is
                    }
                    _ => false,
                }
            }
        }
        impl $crate::python::IntoArg for $ty {
            fn into_arg(self) -> $crate::python::Arg {
                $crate::python::IntoArg::into_arg(Self::class_object())
            }
        }
        impl $crate::python::PyArg for $ty {
            fn to_arg(&self) -> $crate::python::Arg {
                $crate::python::IntoArg::into_arg(Self::class_object())
            }
        }
        impl $crate::python::builtins::CallValue for $ty {
            fn call_value(
                &self,
                args: $crate::python::Args,
            ) -> $crate::python::Result<$crate::python::Arg> {
                Future::construct($kind, args).map($crate::python::Arg::Future)
            }
        }
        impl $crate::python::object::PyObject for $ty {
            fn as_any(&self) -> &dyn core::any::Any {
                self
            }
            fn type_name(&self) -> &'static str {
                "type"
            }
            fn __repr__(&self) -> ::alloc::string::String {
                ::alloc::format!("<class '{}.{}'>", $module, $name)
            }
            fn __eq__(&self, other: &dyn $crate::python::object::PyObject) -> bool {
                other.as_any().is::<$ty>()
            }
            fn __hash__(&self) -> $crate::python::Result<i64> {
                $crate::python::Arg::Str($name.into()).__hash__()
            }
        }
    };
}

pub mod schema;
pub mod taskforce;

pub use schema::base::{Command, _LAILA_IDENTIFIABLE_CENTRAL_COMMAND};
pub use schema::exceptions::{LoopBlockingWaitError, NestedCommandSubmitError};
pub use schema::future::{
    AnyFuture, ComplexFuture, Future, FutureClass, FutureKind, FutureStatus, GroupFuture,
    _LAILA_IDENTIFIABLE_FUTURE,
};
pub use schema::parking::CyclicDependencyError;
pub use taskforce::{
    ConcurrentPackageFuture, PythonAsyncThreadPoolTaskForce, PythonProcessPoolTaskForce, TaskForce,
    TaskForceStatus, _LAILA_IDENTIFIABLE_TASK_FORCE,
};
