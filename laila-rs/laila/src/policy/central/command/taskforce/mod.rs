//! Taskforce sub-package -- the async-thread-pool backend.
//!
//! The async-thread-pool taskforce is the canonical general-purpose
//! backend; sync callables submitted to it are auto-wrapped at submission
//! time and offloaded to the taskforce's sync executor so they never block
//! a loop thread. Tasks that wait on other laila futures *park* their slot
//! for the duration (see `schema::parking`), which makes nested
//! submissions deadlock-free.

pub mod async_thread_pool_executor;
pub mod base;
pub mod process_pool_executor;
pub mod status;
pub mod thread_pool_executor;

pub use async_thread_pool_executor::PythonAsyncThreadPoolTaskForce;
pub use base::{
    _live_taskforces_snapshot, _register_live_taskforce, _unregister_live_taskforce, TaskForce,
    _LAILA_IDENTIFIABLE_TASK_FORCE,
};
pub use process_pool_executor::PythonProcessPoolTaskForce;
pub use status::TaskForceStatus;
pub use thread_pool_executor::future::ConcurrentPackageFuture;

/// `laila.policy.central.command.taskforce` as a value (`laila.TaskForce`):
/// the taskforce classes as constructors.
#[derive(Clone, Copy, Debug, Default)]
pub struct taskforce_mod;
/// `laila.TaskForce`
pub static taskforce: taskforce_mod = taskforce_mod;

impl taskforce_mod {
    /// `laila.TaskForce.PythonAsyncThreadPoolTaskForce(**kwargs)`
    #[allow(non_snake_case)]
    pub fn PythonAsyncThreadPoolTaskForce(
        &self,
        args: impl Into<crate::python::Args>,
    ) -> crate::python::Result<TaskForce> {
        TaskForce::new(args)
    }
    /// `laila.TaskForce.PythonProcessPoolTaskForce(**kwargs)`
    #[allow(non_snake_case)]
    pub fn PythonProcessPoolTaskForce(
        &self,
        args: impl Into<crate::python::Args>,
    ) -> crate::python::Result<TaskForce> {
        PythonProcessPoolTaskForce::new(args)
    }
}
