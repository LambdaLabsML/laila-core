//! `laila.policy.central.command.schema.future.future` -- the future family.
//!
//! One handle type ([`Future`]) tagged with a [`FutureKind`] stands in for
//! Python's class hierarchy; each class-named module next to this one
//! holds that class's constructor and the kind-specific methods.

pub mod any_future;
pub mod complex_future;
#[allow(clippy::module_inception)]
pub mod future;
pub mod future_identity;
pub mod future_status;
pub mod group_future;
#[cfg(feature = "std")]
pub mod remote_future;

pub use any_future::AnyFuture;
pub use complex_future::ComplexFuture;
pub use future::{Future, FutureClass, FutureKind, _DEFAULT_STATUS_CALLBACKS};
pub use future_identity::_LAILA_IDENTIFIABLE_FUTURE;
pub use future_status::FutureStatus;
pub use group_future::GroupFuture;
#[cfg(feature = "std")]
pub use remote_future::RemoteFuture;
