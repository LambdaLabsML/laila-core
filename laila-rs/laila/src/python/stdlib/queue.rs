//! `queue.PriorityQueue` -- the slice the pool router needs.
//!
//! The router pushes `(-affinity, pool_global_id)` tuples and reads the
//! queue only through `qsize()` / `empty()` / `get()`; the heap invariant
//! is kept by ordering the items with Python's `<` on insertion
//! (`Arg::py_cmp`), so `get()` always returns the smallest item.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::python::args::Args;
use crate::python::errors::{LailaError, Result};
use crate::python::object::{Arg, IntoArg, PyArg, PyObject};
use crate::python::pysig;
use crate::python::sync::RwLock;

/// `queue.PriorityQueue(maxsize=0)` -- a handle; clones share one queue.
#[derive(Clone, Default)]
pub struct PriorityQueue(Arc<RwLock<Vec<Arg>>>);

impl PriorityQueue {
    /// `PriorityQueue(maxsize=0)` (`maxsize` is accepted and ignored: the
    /// router never bounds its queue).
    #[pysig("PriorityQueue(maxsize=0)")]
    pub fn new(args: impl Into<Args>) -> Result<PriorityQueue> {
        let _ = maxsize;
        Ok(PriorityQueue::default())
    }

    /// `q.put(item, block=True, timeout=None)`: insert keeping the items ordered.
    #[pysig("put(item, block=True, timeout=None)")]
    pub fn put(&self, args: impl Into<Args>) -> Result<()> {
        let _ = (block, timeout);
        let mut g = self.0.write();
        let pos = g
            .iter()
            .position(|x| matches!(item.py_cmp(x), Some(core::cmp::Ordering::Less)))
            .unwrap_or(g.len());
        g.insert(pos, item);
        Ok(())
    }

    /// `q.get(block=True, timeout=None)`: pop the smallest item (`queue.Empty`
    /// -- raised as `Exception('Empty')` -- when the queue is empty).
    #[pysig("get(block=True, timeout=None)")]
    pub fn get(&self, args: impl Into<Args>) -> Result<Arg> {
        let _ = (block, timeout);
        let mut g = self.0.write();
        if g.is_empty() {
            return Err(LailaError::new(crate::python::ExcKind::Exception, ""));
        }
        Ok(g.remove(0))
    }

    /// `q.qsize()`
    pub fn qsize(&self) -> usize {
        self.0.read().len()
    }

    /// `q.empty()`
    pub fn empty(&self, _args: impl Into<Args>) -> bool {
        self.0.read().is_empty()
    }

    /// `q.queue` -- the underlying heap as a list snapshot.
    pub fn queue(&self) -> Arg {
        self.0.read().clone().into_arg()
    }

    /// Python `is`.
    pub fn is(&self, other: &PriorityQueue) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl core::fmt::Debug for PriorityQueue {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&PyObject::__repr__(self))
    }
}

impl PyObject for PriorityQueue {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    /// `a is b`: identity of the shared handle, not of the boxing.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<PriorityQueue>()
            .is_some_and(|o| self.is(o))
    }
    fn type_name(&self) -> &'static str {
        "PriorityQueue"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<queue.PriorityQueue object at {:#x}>",
            Arc::as_ptr(&self.0) as usize
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<PriorityQueue>()
            .is_some_and(|o| self.is(o))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "queue" => Ok(self.queue()),
            "maxsize" => Ok(Arg::Int(0)),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'PriorityQueue' object has no attribute '{other}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "put" => self.put(args).map(|()| Arg::None),
            "get" => self.get(args),
            "qsize" => Ok(Arg::Int(self.qsize() as i64)),
            "empty" => Ok(Arg::Bool(self.empty(args))),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'PriorityQueue' object has no attribute '{other}'"
            ))),
        }
    }
    fn __bool__(&self) -> bool {
        true
    }
}

impl IntoArg for PriorityQueue {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for PriorityQueue {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl crate::python::builtins::IsInstance for PriorityQueue {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<PriorityQueue>())
    }
}

/// `import queue`
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct queue_mod;
/// `queue` module object.
#[allow(non_upper_case_globals)]
pub static queue: queue_mod = queue_mod;
impl queue_mod {
    /// `queue.PriorityQueue(maxsize=0)`
    #[allow(non_snake_case)]
    pub fn PriorityQueue(&self, args: impl Into<Args>) -> Result<PriorityQueue> {
        PriorityQueue::new(args)
    }
}
