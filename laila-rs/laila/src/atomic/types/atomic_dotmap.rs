//! Thread-safe dot-notation attribute map.
//!
//! [`AtomicDotMap`] is the threaded sibling of `dotmap.DotMap`. It exposes
//! attribute access (`m.a.b.c = 1`) over an underlying dict, but every read
//! and write goes through an `RLock` so concurrent producers and consumers
//! never observe a half-written tree.
//!
//! Used by laila for live, nested configuration that may be mutated at
//! runtime -- the prototypical example is `laila.args`, which is read by
//! every CLI-capable class during construction and may be written by
//! environment-load workflows or by user code rebinding
//! `laila.args.foo.bar = ...`.
//!
//! Python's `__getattr__` only runs when normal lookup fails, so the class's
//! own names (`to_dict`, `keys`, the lock helpers, `_data`, `_lock`) are
//! never looked up in the data; the Rust `__getattr__` keeps that order.
//! `laila::python!` treats `AtomicDotMap` locals as attribute-dynamic
//! (`dm.x` -> `dm.__getattr__("x")`), like `DotMap`.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::atomic::definitions::locally_atomic_object::_now_creation_timestamp;
use crate::atomic::definitions::_LAILA_LOCALLY_ATOMIC_OBJECT;
use crate::python::args::Args;
use crate::python::builtins::IsInstance;
use crate::python::object::{Arg, HasAttrs, IntoArg, PyArg, PyAttrs, PyObject};
use crate::python::sync::{RLock, RwLock};
use crate::python::{pysig, LailaError, Result};

use super::Held;

/// Internal attributes stored directly on the instance rather than in the
/// user-visible `_data` mapping.
pub const _INTERNAL_ATTRS: &[&str] = &["_data", "_lock", "_creation_timestamp"];

/// Names defined on the class (methods / helpers): attribute lookup finds
/// them before `__getattr__` is consulted, so they are never read from `_data`.
const CLASS_ATTRS: &[&str] = &[
    "to_dict",
    "keys",
    "values",
    "items",
    "lock",
    "unlock",
    "locked",
    "atomic",
    "creation_timestamp",
];

struct DotMapInner {
    data: RwLock<Arg>,
    lock: RwLock<RLock>,
    attrs: PyAttrs,
}

/// Thread-safe mapping accessed via attribute syntax.
///
/// All reads and writes to dynamic attributes are guarded by a reentrant
/// lock, making the map safe for concurrent use.
#[derive(Clone)]
pub struct AtomicDotMap(Arc<DotMapInner>);

fn empty_dict() -> Arg {
    Arg::Dict(Arc::new(RwLock::new(
        crate::python::object::PyMap::default(),
    )))
}

impl AtomicDotMap {
    /// `AtomicDotMap()`: an empty data dict and a reentrant lock. Deliberately
    /// does not chain to the pydantic initialiser (the map has no declared
    /// fields), so the `_LAILA_OBJECT` creation timestamp is stamped here.
    pub fn new(args: impl Into<Args>) -> Result<AtomicDotMap> {
        let a: Args = args.into();
        if !a.positional.is_empty() {
            return Err(LailaError::type_error(alloc::format!(
                "AtomicDotMap.__init__() takes 1 positional argument but {} were given",
                a.positional.len() + 1
            )));
        }
        if let Some((k, _)) = a.keywords.first() {
            return Err(LailaError::type_error(alloc::format!(
                "AtomicDotMap.__init__() got an unexpected keyword argument '{k}'"
            )));
        }
        Ok(Self::empty())
    }

    fn empty() -> AtomicDotMap {
        let attrs = PyAttrs::new();
        let _ = attrs.__setattr__("_creation_timestamp", _now_creation_timestamp());
        AtomicDotMap(Arc::new(DotMapInner {
            data: RwLock::new(empty_dict()),
            lock: RwLock::new(RLock::new()),
            attrs,
        }))
    }

    /// Python `is`.
    pub fn is(&self, other: &AtomicDotMap) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// `self._data`: the backing dict handle.
    pub fn _data(&self) -> Arg {
        self.0.data.read().clone()
    }

    /// `self._lock`
    pub fn _lock(&self) -> RLock {
        self.0.lock.read().clone()
    }

    fn held(&self) -> Held {
        let lock = self._lock();
        Held::acquire(&lock)
    }

    /// Return the value for *key*, or `None` if absent (`m.key`).
    pub fn __getattr__(&self, key: impl IntoArg) -> Result<Arg> {
        let key = key.into_arg().__str__();
        let key = key.as_str();
        match key {
            "_data" => return Ok(self._data()),
            "_lock" => return Ok(self._lock().into_arg()),
            "_creation_timestamp" | "creation_timestamp" => {
                return Ok(_LAILA_LOCALLY_ATOMIC_OBJECT::creation_timestamp(self))
            }
            _ => {}
        }
        if let Some(v) = self.0.attrs.get(key) {
            // instance `__dict__` entries (`_local_lock`, ...) win over `__getattr__`
            return Ok(v);
        }
        if CLASS_ATTRS.contains(&key) {
            return Ok(self.bound_method(key));
        }
        let _g = self.held();
        let mut a = Args::from(Arg::Str(key.into()));
        a.push_positional(Arg::None);
        self._data().get(a)
    }

    /// Set *key* to *value*, bypassing the lock for internal attrs (`m.key = value`).
    pub fn __setattr__(&self, key: impl IntoArg, value: impl IntoArg) -> Result<()> {
        let key = key.into_arg().__str__();
        let key = key.as_str();
        let value = value.into_arg();
        if _INTERNAL_ATTRS.contains(&key) {
            match key {
                "_data" => *self.0.data.write() = value,
                "_lock" => {
                    if let Some(l) = value.downcast_ref::<RLock>() {
                        *self.0.lock.write() = l.clone();
                    } else {
                        return Err(LailaError::type_error(alloc::format!(
                            "_lock must be an RLock, not {}",
                            value.type_name()
                        )));
                    }
                }
                _ => self.0.attrs.__setattr__(key, value)?,
            }
            return Ok(());
        }
        let _g = self.held();
        self._data().__setitem__(Arg::Str(key.into()), value)
    }

    /// Delete the attribute *key* (`del m.key`; `KeyError` when absent, as
    /// the Python `del self._data[key]` raises).
    pub fn __delattr__(&self, key: impl IntoArg) -> Result<()> {
        let key = key.into_arg().__str__();
        let _g = self.held();
        self._data().__delitem__(Arg::Str(key.into()))
    }

    /// Return a shallow copy of the internal data as a plain dict.
    pub fn to_dict(&self) -> Result<Arg> {
        let _g = self.held();
        let data = self._data();
        let mut m = crate::python::PyDictBuilder::new();
        for k in data.__iter__()? {
            let v = data.__getitem__(k.clone())?;
            m.insert(k, v);
        }
        Ok(m.finish())
    }

    /// Return a snapshot list of keys.
    #[pysig("keys(self)")]
    pub fn keys(&self, args: impl Into<Args>) -> Result<Arg> {
        let _g = self.held();
        Ok(self._data().__iter__()?.into_arg())
    }

    /// Return a snapshot list of values.
    pub fn values(&self) -> Result<Arg> {
        let _g = self.held();
        let data = self._data();
        let mut out = Vec::new();
        for k in data.__iter__()? {
            out.push(data.__getitem__(k)?);
        }
        Ok(out.into_arg())
    }

    /// Return a snapshot list of `(key, value)` pairs.
    pub fn items(&self) -> Result<Arg> {
        let _g = self.held();
        let data = self._data();
        let mut out = Vec::new();
        for k in data.__iter__()? {
            let v = data.__getitem__(k.clone())?;
            out.push((k, v).into_arg());
        }
        Ok(out.into_arg())
    }

    /// `repr(m)` -> `AtomicDotMap({'x': 3})`
    pub fn __repr__(&self) -> String {
        let _g = self.held();
        alloc::format!("AtomicDotMap({})", self._data().__str__())
    }

    /// `copy.deepcopy(m)`: deep-copied data, fresh lock.
    pub fn deepcopy(&self) -> AtomicDotMap {
        let _g = self.held();
        let out = Self::empty();
        *out.0.data.write() = self._data().deepcopy();
        let _ = crate::atomic::definitions::locally_atomic_object::deepcopy_attrs(
            &self.0.attrs,
            &out.0.attrs,
        );
        out
    }

    /// Bound class methods as Python callables (`m.keys`, `m.to_dict`, ...).
    fn bound_method(&self, name: &str) -> Arg {
        let me = self.clone();
        let name_owned = String::from(name);
        let f: crate::python::object::Callable =
            Arc::new(move |a: Args| me.__call_method__(&name_owned, a));
        Arg::Callable(f)
    }
}

impl Default for AtomicDotMap {
    fn default() -> Self {
        Self::empty()
    }
}

impl core::fmt::Debug for AtomicDotMap {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__repr__())
    }
}

impl HasAttrs for AtomicDotMap {
    fn __attrs__(&self) -> &PyAttrs {
        &self.0.attrs
    }
    fn __class_name__(&self) -> &'static str {
        "AtomicDotMap"
    }
}
impl _LAILA_LOCALLY_ATOMIC_OBJECT for AtomicDotMap {}

impl PyObject for AtomicDotMap {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    /// `a is b`: identity of the shared handle, not of the boxing.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<AtomicDotMap>()
            .is_some_and(|o| self.is(o))
    }
    /// `copy.deepcopy(x)`: a new object with fresh locks.
    fn __deepcopy__(&self) -> Option<Arg> {
        Some(AtomicDotMap::deepcopy(self).into_arg())
    }
    /// `isinstance(x, _LAILA_OBJECT)`: the atomic types are laila objects.
    fn __bases__(&self) -> &'static [&'static str] {
        &["_LAILA_LOCALLY_ATOMIC_OBJECT", "_LAILA_OBJECT"]
    }
    fn type_name(&self) -> &'static str {
        "AtomicDotMap"
    }
    fn __repr__(&self) -> String {
        AtomicDotMap::__repr__(self)
    }
    /// pydantic compares private attributes too (`_lock` differs): identity.
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<AtomicDotMap>()
            .is_some_and(|o| self.is(o))
    }
    fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error("unhashable type: 'AtomicDotMap'"))
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        AtomicDotMap::__getattr__(self, name)
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        AtomicDotMap::__setattr__(self, name, value)
    }
    fn __delattr__(&self, name: &str) -> Result<()> {
        AtomicDotMap::__delattr__(self, name)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        Ok(match name {
            "to_dict" => self.to_dict()?,
            "keys" => self.keys(args)?,
            "values" => self.values()?,
            "items" => self.items()?,
            "lock" => Arg::Bool(_LAILA_LOCALLY_ATOMIC_OBJECT::lock(self, args)?),
            "unlock" => {
                _LAILA_LOCALLY_ATOMIC_OBJECT::unlock(self)?;
                Arg::None
            }
            "locked" => Arg::Bool(_LAILA_LOCALLY_ATOMIC_OBJECT::locked(self, args)),
            "atomic" => _LAILA_LOCALLY_ATOMIC_OBJECT::atomic(self, args)?.into_arg(),
            "__getattr__" => {
                let k = args.positional.first().cloned().unwrap_or(Arg::None);
                AtomicDotMap::__getattr__(self, k)?
            }
            "__setattr__" => {
                let k = args.positional.first().cloned().unwrap_or(Arg::None);
                let v = args.positional.get(1).cloned().unwrap_or(Arg::None);
                AtomicDotMap::__setattr__(self, k, v)?;
                Arg::None
            }
            "__delattr__" => {
                let k = args.positional.first().cloned().unwrap_or(Arg::None);
                AtomicDotMap::__delattr__(self, k)?;
                Arg::None
            }
            _ => {
                // Any other name is a data lookup (`m.key`); calling the
                // result follows Python: `None` is not callable.
                let v = AtomicDotMap::__getattr__(self, name)?;
                return v.__call__(args);
            }
        })
    }
}

impl IntoArg for AtomicDotMap {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for AtomicDotMap {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl IsInstance for AtomicDotMap {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<AtomicDotMap>())
    }
}
impl PartialEq for AtomicDotMap {
    fn eq(&self, other: &AtomicDotMap) -> bool {
        self.is(other)
    }
}
