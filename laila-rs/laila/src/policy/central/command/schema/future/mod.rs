//! `laila.policy.central.command.schema.future`
#[allow(clippy::module_inception)]
pub mod future;

pub use future::{
    AnyFuture, ComplexFuture, Future, FutureClass, FutureKind, FutureStatus, GroupFuture,
    _LAILA_IDENTIFIABLE_FUTURE,
};
