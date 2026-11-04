//! Concrete future wrapping `concurrent.futures.Future` for process-pool execution.

use crate::policy::central::command::schema::future::future::future::{Future, FutureKind};

future_class_marker!(
    ProcessPackageFuture,
    "ProcessPackageFuture",
    "laila.policy.central.command.taskforce.process_pool_executor.future",
    FutureKind::Process,
    |k| k == FutureKind::Process
);
