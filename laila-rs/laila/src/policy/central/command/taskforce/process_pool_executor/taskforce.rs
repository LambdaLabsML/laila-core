//! `PythonProcessPoolTaskForce` -- the process-pool taskforce.
//!
//! // MERGE: subprocess workers need an interpreter to fork; the class is a
//! marker whose constructor raises `NotImplementedError` (the environment
//! and `laila.TaskForce` namespaces still carry the name).

use crate::python::{Arg, Args, IntoArg, LailaError, Result};

/// Top-level picklable function that executes a task in a worker process.
pub fn _process_runner(task: &Arg, args: Arg, kwargs: Arg) -> Result<Arg> {
    let mut a = Args::new();
    a.extend_positional(args)?;
    a.extend_keywords(kwargs)?;
    task.__call__(a)
}

/// `PythonProcessPoolTaskForce` as a class marker.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct PythonProcessPoolTaskForce;

impl PythonProcessPoolTaskForce {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(_args: impl Into<Args>) -> Result<super::super::TaskForce> {
        Err(LailaError::not_implemented(
            "PythonProcessPoolTaskForce (subprocess workers) is not available in the Rust port; \
             use PythonAsyncThreadPoolTaskForce.",
        ))
    }
    pub fn class_object() -> crate::python::PyClass {
        crate::python::PyClass::with_ctor(
            "PythonProcessPoolTaskForce",
            "laila.policy.central.command.taskforce.process_pool_executor.taskforce",
            alloc::sync::Arc::new(|a: Args| Self::new(a).map(IntoArg::into_arg)),
        )
    }
}
impl crate::python::builtins::IsInstance for PythonProcessPoolTaskForce {
    fn isinstance(_x: &Arg) -> bool {
        false
    }
}
impl IntoArg for PythonProcessPoolTaskForce {
    fn into_arg(self) -> Arg {
        Self::class_object().into_arg()
    }
}
