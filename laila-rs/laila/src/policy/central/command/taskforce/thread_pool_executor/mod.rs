//! `laila.policy.central.command.taskforce.thread_pool_executor` -- the
//! `ConcurrentPackageFuture` (the legacy thread-pool taskforce itself was
//! removed upstream in favour of the unified async backend).
pub mod future;

pub use future::ConcurrentPackageFuture;
