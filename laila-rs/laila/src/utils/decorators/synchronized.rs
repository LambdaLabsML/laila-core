//! `laila.utils.decorators.synchronized` -- the `@synchronized` decorator.
//!
//! Wraps a method so every `_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT`
//! argument of a call (`self`, positionals, keyword values) is locked -- in
//! `id()`-sorted order, which keeps nested calls deadlock-free -- before the
//! body runs and released on exit (or on exception). Only `scope="local"`
//! is implemented; `"global"` raises `NotImplementedError` when the wrapper
//! is called.
//!
//! The lock helpers (`synchronized_call`, `synchronized_on`,
//! `atomic_lock_of`) live next to the mixin they inspect
//! (`laila::atomic::definitions::locally_atomic_identifiable_object`).

use alloc::vec::Vec;

use crate::atomic::definitions::locally_atomic_object::RLock;
use crate::python::{pysig, Arg, Args, LailaError, Result};

pub use crate::atomic::definitions::locally_atomic_identifiable_object::{
    atomic_lock_of, synchronized_call, synchronized_on,
};

/// `synchronized(method, *, scope="local")`: wrap *method* so all
/// identifiable-atomic arguments (`self`, positionals, keyword values) are
/// locked -- in `id()`-sorted order -- before invocation and released on
/// exit (or on exception). Only `"local"` is implemented; `"global"` raises
/// `NotImplementedError` when the wrapper is called, as in Python.
#[pysig("synchronized(method, *, scope=\"local\")")]
pub fn synchronized(args: impl Into<Args>) -> Result<Arg> {
    let method = method;
    let scope = scope.__str__();
    let wrapper: crate::python::object::Callable = alloc::sync::Arc::new(move |a: Args| {
        if scope != "local" {
            return Err(LailaError::not_implemented(
                "Global synchronization is not implemented.",
            ));
        }
        let candidates = a.positional.iter().chain(a.keywords.iter().map(|(_, v)| v));
        let lock_targets: Vec<(usize, RLock)> = candidates.filter_map(atomic_lock_of).collect();
        if lock_targets.is_empty() {
            return method.__call__(a);
        }
        synchronized_call(&lock_targets, || method.__call__(a))
    });
    Ok(Arg::Callable(wrapper))
}
