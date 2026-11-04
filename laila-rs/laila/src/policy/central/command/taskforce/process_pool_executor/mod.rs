//! `laila.policy.central.command.taskforce.process_pool_executor` --
//! process-pool executor sub-package: taskforce and future types.
//!
//! // MERGE: `PythonProcessPoolTaskForce` runs picklable callables in
//! subprocess workers; there is no interpreter to fork from Rust, so the
//! class is a marker whose constructor raises `NotImplementedError`
//! (`BrokenProcessPool` is carried in `python::errors` for parity).

pub mod future;
pub mod taskforce;

pub use future::ProcessPackageFuture;
pub use taskforce::PythonProcessPoolTaskForce;
