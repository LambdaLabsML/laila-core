//! Base definitions for identifiable and atomic object hierarchies.

pub mod globally_atomic_identifiable_object;
pub mod locally_atomic_identifiable_object;
pub mod locally_atomic_object;

pub use crate::utils::decorators::synchronized::synchronized;
pub use globally_atomic_identifiable_object::{
    HeldSecret, HeldSecretToken, _HELD_SECRET, _LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT,
    UNGATED_NAMES,
};
pub use locally_atomic_identifiable_object::{
    atomic_lock_of, dict_identity, is_locally_atomic_identifiable, synchronized_call,
    synchronized_on, HasIdentity, LocalLockHandle, _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT,
};
pub use locally_atomic_object::{
    _now_creation_timestamp, _split_locks, AtomicContext, AtomicGuard, LockKind, RLock,
    _LAILA_LOCALLY_ATOMIC_OBJECT, LOCAL_LOCK_ATTR,
};
// Python's ``laila.atomic.definitions`` also re-exports the ``basics`` bases.
pub use crate::basics::definitions::{_LAILA_IDENTIFIABLE_OBJECT, _LAILA_OBJECT};
