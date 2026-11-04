//! Thread-safe generic list with snapshot iterators and atomic batching.
//!
//! [`AtomicList`] wraps a Python list with an `RLock` and provides:
//!
//! - The standard mutable-sequence API (`__getitem__`, `__setitem__`,
//!   `append`, `extend`, `pop`, `remove`, `clear`, ...) -- all guarded by
//!   the lock.
//! - *Snapshot* iteration: `__iter__` returns a copy of the list taken under
//!   the lock, so external code can iterate without holding the lock and
//!   without seeing the list mutate mid-iteration.
//! - An [`atomic`](AtomicList::atomic) context manager that yields the raw
//!   list while the lock is held, so a sequence of operations appears atomic
//!   to other threads.
//!
//! Use it where multiple threads need to share a mutable list (job queues,
//! deferred-callback registries, observer lists) without playing
//! whack-a-mole with manual locking.
//!
//! `value` is the real Python list (`Arg::List`): `with lst.atomic() as L`
//! hands out that very handle, exactly as Python exposes `self.value`.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::atomic::definitions::locally_atomic_object::_now_creation_timestamp;
use crate::atomic::definitions::_LAILA_LOCALLY_ATOMIC_OBJECT;
use crate::python::args::Args;
use crate::python::builtins::IsInstance;
use crate::python::object::{Arg, HasAttrs, IntoArg, PyArg, PyAttrs, PyObject, Slice};
use crate::python::sync::{RLock, RwLock};
use crate::python::{pysig, LailaError, Result};

use super::{reject_positional, validation_error, Held};

struct ListInner {
    /// The `value: list[T]` field (a shared list handle, like Python's).
    value: RwLock<Arg>,
    lock: RLock,
    attrs: PyAttrs,
}

/// Thread-safe list with common list operations.
///
/// - All mutations are guarded by a re-entrant lock.
/// - Reads that return iterators/slices produce *snapshots* to avoid surprises.
/// - Use `with lst.atomic():` to batch multiple mutations under one lock.
#[derive(Clone)]
pub struct AtomicList(Arc<ListInner>);

fn new_list(items: Vec<Arg>) -> Arg {
    items.into_arg()
}

impl AtomicList {
    /// `AtomicList(value=[])` -- `value` may be any iterable (pydantic
    /// copies it into a fresh list).
    pub fn new(args: impl Into<Args>) -> Result<AtomicList> {
        let mut a: Args = args.into();
        reject_positional("AtomicList", &a)?;
        let value = a.pop_or("value", Arg::None);
        let items = match &value {
            Arg::None => Vec::new(),
            Arg::List(_) | Arg::Tuple(_) | Arg::Set(_) | Arg::FrozenSet(_) => value.__iter__()?,
            other => return Err(validation_error("AtomicList", "value", "list", other)),
        };
        Ok(Self::from_items(items))
    }

    fn from_items(items: Vec<Arg>) -> AtomicList {
        let attrs = PyAttrs::new();
        let _ = attrs.__setattr__("_creation_timestamp", _now_creation_timestamp());
        AtomicList(Arc::new(ListInner {
            value: RwLock::new(new_list(items)),
            lock: RLock::new(),
            attrs,
        }))
    }

    /// Python `is`.
    pub fn is(&self, other: &AtomicList) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// `self._lock`
    pub fn _lock(&self) -> RLock {
        self.0.lock.clone()
    }

    fn held(&self) -> Held {
        Held::acquire(&self.0.lock)
    }

    /// `self.value`: the backing list handle (aliasing, like Python).
    pub fn value(&self) -> Arg {
        self.0.value.read().clone()
    }

    /// `self.value = items`: rebind the backing list.
    pub fn set_value(&self, items: impl IntoArg) -> Result<()> {
        let v = items.into_arg();
        match &v {
            Arg::List(_) => {
                *self.0.value.write() = v;
                Ok(())
            }
            other => Err(validation_error("AtomicList", "value", "list", other)),
        }
    }

    /// Return the number of elements.
    pub fn __len__(&self) -> usize {
        let _g = self.held();
        self.value().__len__().unwrap_or(0)
    }

    /// `repr(lst)` -> `AtomicList([1, 2])`
    pub fn __repr__(&self) -> String {
        let _g = self.held();
        alloc::format!("AtomicList({})", self.value().__repr__())
    }

    /// `str(lst)` -> `value=[1, 2]` (pydantic's default `__str__`).
    pub fn __str__(&self) -> String {
        let _g = self.held();
        alloc::format!("value={}", self.value().__repr__())
    }

    /// Iterate over a snapshot of the list.
    pub fn __iter__(&self) -> Result<Vec<Arg>> {
        let _g = self.held();
        self.value().__iter__()
    }

    /// Return the element at *idx*, or a snapshot slice.
    pub fn __getitem__(&self, idx: impl IntoArg) -> Result<Arg> {
        let _g = self.held();
        self.value().__getitem__(idx)
    }

    /// Set element(s) at *idx* (an index or a slice).
    pub fn __setitem__(&self, idx: impl IntoArg, item: impl IntoArg) -> Result<()> {
        let _g = self.held();
        let idx = idx.into_arg();
        let value = self.value();
        if let Some(s) = idx.downcast_ref::<Slice>() {
            let items = item.into_arg();
            let items = items
                .__iter__()
                .map_err(|_| LailaError::type_error("must assign iterable to extended slice"))?;
            if let Arg::List(l) = &value {
                let mut g = l.write();
                let (start, stop, step) = s.indices(g.len());
                if step != 1 {
                    return Err(LailaError::value_error(alloc::format!(
                        "attempt to assign sequence of size {} to extended slice of size {}",
                        items.len(),
                        slice_len(start, stop, step)
                    )));
                }
                let (lo, hi) = (start.max(0) as usize, stop.max(start) as usize);
                g.splice(lo..hi, items);
            }
            return Ok(());
        }
        value.__setitem__(idx, item)
    }

    /// Delete element(s) at *idx* (an index or a slice).
    pub fn __delitem__(&self, idx: impl IntoArg) -> Result<()> {
        let _g = self.held();
        let idx = idx.into_arg();
        let value = self.value();
        if let Some(s) = idx.downcast_ref::<Slice>() {
            if let Arg::List(l) = &value {
                let mut g = l.write();
                let (start, stop, step) = s.indices(g.len());
                let mut remove: Vec<usize> = Vec::new();
                let mut i = start;
                if step > 0 {
                    while i < stop {
                        remove.push(i as usize);
                        i += step;
                    }
                } else {
                    while i > stop {
                        remove.push(i as usize);
                        i += step;
                    }
                }
                remove.sort_unstable();
                for r in remove.into_iter().rev() {
                    g.remove(r);
                }
            }
            return Ok(());
        }
        value.__delitem__(idx)
    }

    /// Append *item* to the end of the list.
    pub fn append(&self, item: impl IntoArg) -> Result<()> {
        let _g = self.held();
        self.value().append(item)
    }

    /// Extend the list with elements from *items*.
    #[pysig("extend(self, items)")]
    pub fn extend(&self, args: impl Into<Args>) -> Result<()> {
        let _g = self.held();
        self.value().extend(Args::from(items))
    }

    /// Insert *item* before *index*.
    pub fn insert(&self, index: impl IntoArg, item: impl IntoArg) -> Result<()> {
        let _g = self.held();
        self.value().insert(index, item)
    }

    /// Remove all elements.
    #[pysig("clear(self)")]
    pub fn clear(&self, args: impl Into<Args>) -> Result<()> {
        let _g = self.held();
        self.value().clear(Args::new())
    }

    /// Remove the first occurrence of *item*.
    pub fn remove(&self, item: impl Into<Args>) -> Result<()> {
        let a: Args = item.into();
        let item = a.positional.into_iter().next().ok_or_else(|| {
            LailaError::type_error("remove() takes exactly one argument (0 given)")
        })?;
        let _g = self.held();
        self.value().remove(item)
    }

    /// Remove and return the item at *index* (default last).
    #[pysig("pop(self, index=-1)")]
    pub fn pop(&self, args: impl Into<Args>) -> Result<Arg> {
        let _g = self.held();
        self.value().pop(Args::from(index))
    }

    /// Return the number of occurrences of *item*.
    pub fn count(&self, item: impl IntoArg) -> Result<Arg> {
        let _g = self.held();
        self.value().count(item)
    }

    /// Return the index of the first occurrence of *item*.
    #[pysig("index(self, item, start=0, stop=None)")]
    pub fn index(&self, args: impl Into<Args>) -> Result<Arg> {
        let _g = self.held();
        let mut a = Args::from(item);
        a.push_positional(start);
        if !stop.is_none() {
            a.push_positional(stop);
        }
        self.value().index(a)
    }

    /// Return a snapshot (shallow copy) of the list.
    pub fn to_list(&self) -> Result<Arg> {
        let _g = self.held();
        Ok(new_list(self.value().__iter__()?))
    }

    /// Set the element at *index* to *item*.
    pub fn set_at(&self, index: impl IntoArg, item: impl IntoArg) -> Result<()> {
        let _g = self.held();
        self.value().__setitem__(index, item)
    }

    /// Return the element at *index*.
    pub fn get_at(&self, index: impl IntoArg) -> Result<Arg> {
        let _g = self.held();
        self.value().__getitem__(index)
    }

    /// Snapshot slice, equivalent to `self.value[start:stop:step]`.
    #[pysig("slice(self, start, stop, step=None)")]
    pub fn slice(&self, args: impl Into<Args>) -> Result<Arg> {
        let _g = self.held();
        let opt = |v: &Arg| -> Result<Option<i64>> {
            match v {
                Arg::None => Ok(None),
                x => x.as_i64().map(Some).ok_or_else(|| {
                    LailaError::type_error(
                        "slice indices must be integers or None or have an __index__ method",
                    )
                }),
            }
        };
        let s = Slice {
            start: opt(&start)?,
            stop: opt(&stop)?,
            step: opt(&step)?,
        };
        self.value().__getitem__(s)
    }

    /// In-place: keep only items in `[start:stop]`, like list slicing,
    /// shrink the list. Supports negative indices.
    #[pysig("trim(self, start, stop)")]
    pub fn trim(&self, args: impl Into<Args>) -> Result<()> {
        let _g = self.held();
        let value = self.value();
        let n = value.__len__()? as i64;
        let bound = |v: &Arg, default: i64| -> Result<i64> {
            match v {
                Arg::None => Ok(default),
                x => {
                    let i = x.as_i64().ok_or_else(|| {
                        LailaError::type_error(alloc::format!(
                            "'>=' not supported between instances of '{}' and 'int'",
                            x.type_name()
                        ))
                    })?;
                    Ok(if i >= 0 { i } else { n + i })
                }
            }
        };
        let s = bound(&start, 0)?.clamp(0, n);
        let e = bound(&stop, n)?.clamp(0, n);
        if let Arg::List(l) = &value {
            let mut g = l.write();
            if s >= e {
                g.clear();
            } else {
                let kept: Vec<Arg> = g[s as usize..e as usize].to_vec();
                *g = kept;
            }
        }
        Ok(())
    }

    /// `with lst.atomic() as L:` -- `L` is the raw list while the lock is held:
    ///
    /// ```text
    /// with lst.atomic() as L:
    ///     L.append(x)
    ///     L.extend([...])
    ///     # Several ops under one lock acquisition
    /// ```
    #[pysig("atomic(self)")]
    pub fn atomic(&self, args: impl Into<Args>) -> Result<ListAtomic> {
        Ok(ListAtomic(self.clone()))
    }

    /// `lst.name`
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "value" => Ok(self.value()),
            "_lock" => Ok(self._lock().into_arg()),
            "creation_timestamp" | "_creation_timestamp" => {
                Ok(_LAILA_LOCALLY_ATOMIC_OBJECT::creation_timestamp(self))
            }
            _ => self.0.attrs.__getattr__(name).map_err(|_| {
                LailaError::attribute_error(alloc::format!(
                    "'AtomicList' object has no attribute '{name}'"
                ))
            }),
        }
    }

    /// `lst.name = value`
    pub fn __setattr__(&self, name: &str, value: impl IntoArg) -> Result<()> {
        match name {
            "value" => self.set_value(value),
            _ => self.0.attrs.__setattr__(name, value),
        }
    }

    /// `copy.deepcopy(lst)`: deep-copied items, fresh lock.
    pub fn deepcopy(&self) -> AtomicList {
        let _g = self.held();
        let items = match self.value().deepcopy() {
            Arg::List(l) => l.read().clone(),
            _ => Vec::new(),
        };
        let out = Self::from_items(items);
        let _ = crate::atomic::definitions::locally_atomic_object::deepcopy_attrs(
            &self.0.attrs,
            &out.0.attrs,
        );
        out
    }
}

fn slice_len(start: i64, stop: i64, step: i64) -> i64 {
    if step > 0 {
        ((stop - start + step - 1) / step).max(0)
    } else {
        ((start - stop - step - 1) / -step).max(0)
    }
}

impl Default for AtomicList {
    fn default() -> Self {
        Self::from_items(Vec::new())
    }
}

impl core::fmt::Debug for AtomicList {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__repr__())
    }
}

impl HasAttrs for AtomicList {
    fn __attrs__(&self) -> &PyAttrs {
        &self.0.attrs
    }
    fn __class_name__(&self) -> &'static str {
        "AtomicList"
    }
}
impl _LAILA_LOCALLY_ATOMIC_OBJECT for AtomicList {}

/// Context manager that exposes the underlying list while the lock is held
/// (`AtomicList._Atomic`).
pub struct ListAtomic(AtomicList);

/// The raw list yielded by `with lst.atomic() as L:` (derefs to the
/// `Arg::List` handle); releases the lock on drop.
pub struct ListGuard {
    raw: Arg,
    _held: Held,
}

impl ListAtomic {
    /// Acquire the lock and return the raw list.
    pub fn __enter__(&self) -> Result<ListGuard> {
        let held = Held::acquire(&self.0 .0.lock);
        Ok(ListGuard {
            raw: self.0.value(),
            _held: held,
        })
    }
}
impl ListGuard {
    /// Release the lock.
    pub fn __exit__(self) -> Result<()> {
        drop(self);
        Ok(())
    }
}
impl core::ops::Deref for ListGuard {
    type Target = Arg;
    fn deref(&self) -> &Arg {
        &self.raw
    }
}

/// The raw list as a dynamic value: every protocol call goes to the list
/// handle (`L.append(x)`, `L[0]`, `len(L)`), the lock is released on drop.
impl PyObject for ListGuard {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "list"
    }
    fn __repr__(&self) -> String {
        self.raw.__repr__()
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<ListGuard>()
            .is_some_and(|o| o.raw.is(&self.raw))
    }
    fn __eq_arg__(&self, other: &Arg) -> bool {
        self.raw == *other
    }
    fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error("unhashable type: 'list'"))
    }
    fn __len__(&self) -> Result<usize> {
        self.raw.__len__()
    }
    fn __iter__(&self) -> Result<Vec<Arg>> {
        self.raw.__iter__()
    }
    fn __contains__(&self, item: &Arg) -> Result<bool> {
        self.raw.__contains__(item.clone())
    }
    fn __getitem__(&self, key: &Arg) -> Result<Arg> {
        self.raw.__getitem__(key.clone())
    }
    fn __setitem__(&self, key: &Arg, value: Arg) -> Result<()> {
        self.raw.__setitem__(key.clone(), value)
    }
    fn __delitem__(&self, key: &Arg) -> Result<()> {
        self.raw.__delitem__(key.clone())
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let pos = |i: usize| -> Result<Arg> {
            args.positional.get(i).cloned().ok_or_else(|| {
                LailaError::type_error(alloc::format!(
                    "list.{name}() takes exactly one argument ({} given)",
                    args.positional.len()
                ))
            })
        };
        Ok(match name {
            "append" => {
                self.raw.append(pos(0)?)?;
                Arg::None
            }
            "extend" => {
                self.raw.extend(args)?;
                Arg::None
            }
            "insert" => {
                self.raw.insert(pos(0)?, pos(1)?)?;
                Arg::None
            }
            "pop" => self.raw.pop(args)?,
            "remove" => {
                self.raw.remove(pos(0)?)?;
                Arg::None
            }
            "clear" => {
                self.raw.clear(args)?;
                Arg::None
            }
            "index" => self.raw.index(args)?,
            "count" => self.raw.count(pos(0)?)?,
            "reverse" => {
                self.raw.reverse()?;
                Arg::None
            }
            "sort" => {
                self.raw.sort_in_place()?;
                Arg::None
            }
            "copy" => self.raw.copy()?,
            _ => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'list' object has no attribute '{name}'"
                )))
            }
        })
    }
}
impl IntoArg for ListGuard {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

impl PyObject for AtomicList {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    /// `a is b`: identity of the shared handle, not of the boxing.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<AtomicList>()
            .is_some_and(|o| self.is(o))
    }
    /// `copy.deepcopy(x)`: a new object with fresh locks.
    fn __deepcopy__(&self) -> Option<Arg> {
        Some(AtomicList::deepcopy(self).into_arg())
    }
    /// `isinstance(x, _LAILA_OBJECT)`: the atomic types are laila objects.
    fn __bases__(&self) -> &'static [&'static str] {
        &["_LAILA_LOCALLY_ATOMIC_OBJECT", "_LAILA_OBJECT"]
    }
    fn type_name(&self) -> &'static str {
        "AtomicList"
    }
    fn __repr__(&self) -> String {
        AtomicList::__repr__(self)
    }
    fn __str__(&self) -> String {
        AtomicList::__str__(self)
    }
    /// pydantic compares private attributes too (`_lock` differs): identity.
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<AtomicList>()
            .is_some_and(|o| self.is(o))
    }
    fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error("unhashable type: 'AtomicList'"))
    }
    fn __len__(&self) -> Result<usize> {
        Ok(AtomicList::__len__(self))
    }
    fn __iter__(&self) -> Result<Vec<Arg>> {
        AtomicList::__iter__(self)
    }
    fn __getitem__(&self, key: &Arg) -> Result<Arg> {
        AtomicList::__getitem__(self, key.clone())
    }
    fn __setitem__(&self, key: &Arg, value: Arg) -> Result<()> {
        AtomicList::__setitem__(self, key.clone(), value)
    }
    fn __delitem__(&self, key: &Arg) -> Result<()> {
        AtomicList::__delitem__(self, key.clone())
    }
    /// `x in lst` -- `MutableSequence` falls back to iteration.
    fn __contains__(&self, item: &Arg) -> Result<bool> {
        Ok(AtomicList::__iter__(self)?.iter().any(|v| v == item))
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        AtomicList::__getattr__(self, name)
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        AtomicList::__setattr__(self, name, value)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let pos = |i: usize, what: &str| -> Result<Arg> {
            args.positional.get(i).cloned().ok_or_else(|| {
                LailaError::type_error(alloc::format!(
                    "AtomicList.{name}() missing 1 required positional argument: '{what}'"
                ))
            })
        };
        Ok(match name {
            "atomic" => IntoArg::into_arg(self.atomic(args)?),
            "append" => {
                self.append(pos(0, "item")?)?;
                Arg::None
            }
            "extend" => {
                self.extend(args)?;
                Arg::None
            }
            "insert" => {
                self.insert(pos(0, "index")?, pos(1, "item")?)?;
                Arg::None
            }
            "clear" => {
                self.clear(args)?;
                Arg::None
            }
            "remove" => {
                self.remove(pos(0, "item")?)?;
                Arg::None
            }
            "pop" => self.pop(args)?,
            "count" => self.count(pos(0, "item")?)?,
            "index" => self.index(args)?,
            "to_list" => self.to_list()?,
            "set_at" => {
                self.set_at(pos(0, "index")?, pos(1, "item")?)?;
                Arg::None
            }
            "get_at" => self.get_at(pos(0, "index")?)?,
            "slice" => self.slice(args)?,
            "trim" => {
                self.trim(args)?;
                Arg::None
            }
            "lock" => Arg::Bool(_LAILA_LOCALLY_ATOMIC_OBJECT::lock(self, args)?),
            "unlock" => {
                _LAILA_LOCALLY_ATOMIC_OBJECT::unlock(self)?;
                Arg::None
            }
            "locked" => Arg::Bool(_LAILA_LOCALLY_ATOMIC_OBJECT::locked(self, args)),
            _ => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'AtomicList' object has no attribute '{name}'"
                )))
            }
        })
    }
}

impl IntoArg for AtomicList {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for AtomicList {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl IsInstance for AtomicList {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<AtomicList>())
    }
}
impl PartialEq for AtomicList {
    fn eq(&self, other: &AtomicList) -> bool {
        self.is(other)
    }
}
impl crate::python::builtins::PyLen for AtomicList {
    fn __len__(&self) -> Result<usize> {
        Ok(AtomicList::__len__(self))
    }
}
impl crate::python::builtins::PyIter for &AtomicList {
    type Item = Arg;
    fn py_iter(self) -> Result<crate::python::builtins::PyIterator<Arg>> {
        Ok(crate::python::builtins::PyIterator::eager(
            AtomicList::__iter__(self)?,
        ))
    }
}
impl crate::python::builtins::PyContains for AtomicList {
    fn py_contains(&self, item: Arg) -> Result<bool> {
        Ok(AtomicList::__iter__(self)?.contains(&item))
    }
}
impl crate::python::builtins::Truthy for AtomicList {
    /// `bool(lst)` goes through `__len__`.
    fn __bool__(&self) -> bool {
        AtomicList::__len__(self) != 0
    }
}

super::context_manager_py_object!(ListAtomic, "AtomicList._Atomic");
