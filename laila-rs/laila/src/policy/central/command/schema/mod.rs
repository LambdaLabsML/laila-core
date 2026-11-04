//! `laila.policy.central.command.schema` -- the central command class, the
//! future hierarchy, slot parking and the command exceptions.
pub mod base;
pub mod exceptions;
pub mod future;
pub mod parking;

pub use base::{Command, _LAILA_IDENTIFIABLE_CENTRAL_COMMAND};
pub use exceptions::{
    ensure_coroutine_function, no_command_submit, LoopBlockingWaitError, NestedCommandSubmitError,
};
pub use future::{
    ComplexFuture, Future, FutureClass, FutureKind, FutureStatus, GroupFuture,
    _LAILA_IDENTIFIABLE_FUTURE,
};
pub use parking::{park_async, park_sync, CyclicDependencyError};
