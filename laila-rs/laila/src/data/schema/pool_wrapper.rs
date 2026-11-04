//! `PoolWrapper` -- lightweight proxy binding a pool to a manifest for
//! scoped copies.
//!
//! Returned by `_LAILA_IDENTIFIABLE_POOL.__getitem__` when the key is a
//! `Manifest`. The wrapper exists so that the `<=` operator can copy *only
//! the entries listed by the manifest* between two pools, instead of
//! copying everything:
//!
//! ```text
//! my_future = pool_dest[manifest] <= pool_src[manifest]
//! ```
//!
//! How the copy works
//! ------------------
//! The right-hand side selects the source: `pool_src[manifest]` returns a
//! [`PoolWrapper`] over `pool_src`. The left-hand side selects the
//! destination the same way. `__le__` then walks the manifest's leaf
//! entries, fetching each one from the source pool via `memory.remember`
//! (which handles serialisation and proxy chains for us), then storing it
//! in the destination pool via `memory.memorize`. The whole copy runs
//! concurrently with a 4-way semaphore on a daemon thread, returning a
//! `GroupFuture` whose children correspond to per-entry copy operations.
//!
//! Concurrency model
//! -----------------
//! [`PoolWrapper`] itself holds no lock -- all I/O is delegated to the
//! wrapped pool's existing public methods, which already take care of
//! their own atomic-lock coverage.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::python::object::PyObject;
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, PyArg, Result};

use super::base::Pool;

/// Manifest-scoped view over a pool, used as the LHS / RHS of `<=` copies.
///
/// A wrapper is conceptually "this pool, but only the entries listed by
/// *manifest*". The class itself is intentionally minimal: just two slots
/// (`pool` and `manifest`) and one operator (`<=`) that performs the actual
/// cross-pool copy.
#[derive(Clone)]
pub struct PoolWrapper(Arc<PoolWrapperInner>);

struct PoolWrapperInner {
    pool: Pool,
    manifest: Arg,
}

impl PoolWrapper {
    /// `PoolWrapper(pool, manifest)`
    #[pysig("PoolWrapper(pool, manifest)")]
    pub fn new_py(args: impl Into<Args>) -> Result<PoolWrapper> {
        match pool {
            Arg::Pool(p) => Ok(PoolWrapper::new(p, manifest)),
            other => Err(LailaError::type_error(alloc::format!(
                "PoolWrapper.pool must be a pool, got {}",
                other.type_name()
            ))),
        }
    }
    /// The wrapper over *pool* scoped by *manifest* (typed).
    pub fn new(pool: Pool, manifest: Arg) -> PoolWrapper {
        PoolWrapper(Arc::new(PoolWrapperInner { pool, manifest }))
    }
    /// `wrapper.pool`
    pub fn pool(&self) -> Pool {
        self.0.pool.clone()
    }
    /// `wrapper.manifest`
    pub fn manifest(&self) -> Arg {
        self.0.manifest.clone()
    }
    /// Python `is`.
    pub fn is(&self, other: &PoolWrapper) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    /// `self <= other` -- copy *other*'s manifest entries into this
    /// wrapper's pool. `NotImplemented` unless *other* is a `PoolWrapper`.
    pub fn __le__(&self, other: impl IntoArg) -> Result<Arg> {
        let other = other.into_arg();
        let Some(src) = other.downcast_ref::<PoolWrapper>() else {
            return Ok(crate::python::builtins::not_implemented());
        };
        // One `ConcurrentPackageFuture` per manifest entry under a
        // `GroupFuture`; a daemon thread copies each entry from *other*'s
        // pool into this wrapper's pool, four at a time.
        let entry_ids: Vec<String> = self
            .manifest()
            .__iter__()?
            .iter()
            .map(|k| k.__str__())
            .collect();
        let memory = crate::policy::get_active_policy().central().memory();
        let fut = memory._duplicate_entries(
            entry_ids,
            src.pool(),
            self.pool(),
            4,
            "manifest_copy",
            "ManifestCopy",
        )?;
        Ok(Arg::Future(fut))
    }
}

impl core::fmt::Debug for PoolWrapper {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&PyObject::__repr__(self))
    }
}

impl PyObject for PoolWrapper {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    /// `a is b`: identity of the shared handle, not of the boxing.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<PoolWrapper>()
            .is_some_and(|o| self.is(o))
    }
    fn type_name(&self) -> &'static str {
        "PoolWrapper"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<laila.data.schema.pool_wrapper.PoolWrapper object at {:#x}>",
            Arc::as_ptr(&self.0) as usize
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<PoolWrapper>()
            .is_some_and(|o| self.is(o))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "pool" => Ok(Arg::Pool(self.pool())),
            "manifest" => Ok(self.manifest()),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'PoolWrapper' object has no attribute '{other}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "__le__" => self.__le__(args.positional.first().cloned().unwrap_or(Arg::None)),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'PoolWrapper' object has no attribute '{other}'"
            ))),
        }
    }
    fn __bool__(&self) -> bool {
        true
    }
}
impl IntoArg for PoolWrapper {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for PoolWrapper {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl crate::python::builtins::IsInstance for PoolWrapper {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<PoolWrapper>())
    }
}
