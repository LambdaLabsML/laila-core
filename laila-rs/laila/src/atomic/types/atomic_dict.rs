//! Thread-safe, insertion-ordered dictionary with atomic context support.
//!
//! [`AtomicDict`] is a drop-in `MutableMapping` whose every mutation is
//! guarded by an internal `RLock`. On top of the standard mapping API it
//! adds:
//!
//! - *Insertion-ordered* iteration: the backing `data` dict (itself
//!   insertion-ordered) is the single order authority, so keyed deletes are
//!   O(1) and iteration never goes stale after out-of-band edits of `data`.
//!   A positional index (`_order`) is rebuilt lazily only for the positional
//!   accessors. Useful when callers need to act on "the next item to come
//!   in" without scanning.
//! - Positional accessors ([`item_at`](AtomicDict::item_at),
//!   [`key_at`](AtomicDict::key_at), [`value_at`](AtomicDict::value_at)) and
//!   an in-place slice ([`trim`](AtomicDict::trim)) that work against the
//!   insertion order.
//! - An [`atomic`](AtomicDict::atomic) context manager that yields a "view"
//!   object capable of doing batch mutations *under the lock* (so the whole
//!   batch appears atomic to other threads). Combined with the context-local
//!   [`AtomicDict::current`] accessor, code inside the block can also reach
//!   the dict without re-receiving it as a parameter.
//! - Atomic compute-and-set ([`compute`](AtomicDict::compute)) and increment
//!   ([`increment`](AtomicDict::increment)) helpers for common
//!   read-modify-write patterns.
//!
//! Used heavily inside laila for things like the active-policy registry,
//! future-bank tables, taskforce queues, and the central memory hint /
//! record indexes -- anywhere a plain dict would be a race-condition waiting
//! to happen.
//!
//! `data` is the real Python dict (`Arg::Dict`): `AtomicDict(d)` aliases `d`
//! (`model_construct` stores the object as-is) and `d.data["k"] = v` edits
//! are visible to every accessor, as the Python docstring promises.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::atomic::definitions::locally_atomic_object::_now_creation_timestamp;
use crate::atomic::definitions::_LAILA_LOCALLY_ATOMIC_OBJECT;
use crate::python::args::Args;
use crate::python::builtins::IsInstance;
use crate::python::errors::{ExcKind, KeyError};
use crate::python::object::{Arg, HasAttrs, IntoArg, PyArg, PyAttrs, PyObject};
use crate::python::sync::{RLock, RwLock};
use crate::python::{pysig, LailaError, Param, ParamKind, Result, Signature};

use super::{require_str, Held};

struct DictInner {
    /// `data: dict[K, V]` -- the order authority (a shared dict handle).
    data: RwLock<Arg>,
    /// `_lock: RLock`
    lock: RLock,
    /// `_order: list[K] | None` -- positional cache, dropped by mutations.
    order: RwLock<Option<Vec<Arg>>>,
    attrs: PyAttrs,
}

/// Thread-safe, insertion-ordered dict with index/slice ops and robust order syncing.
///
/// Supports:
/// - `with d.atomic(hint="..."):` mutual exclusion for the instance; `hint` is a str
/// - `AtomicDict.current()`: access the instance currently under an atomic block (thread/async local)
/// - `d.run_atomic(fn, hint="...")`: run a no-arg function with the lock held
#[derive(Clone)]
pub struct AtomicDict(Arc<DictInner>);

// ---------------------------------------------------------------------------
// context-local "current" AtomicDict (thread- & async-task-safe)
// ---------------------------------------------------------------------------

#[cfg(feature = "std")]
std::thread_local! {
    static CURRENT: core::cell::RefCell<Option<AtomicDict>> = const { core::cell::RefCell::new(None) };
}
#[cfg(not(feature = "std"))]
static CURRENT: crate::python::sync::Mutex<Option<AtomicDict>> =
    crate::python::sync::Mutex::new(None);

fn current_get() -> Option<AtomicDict> {
    #[cfg(feature = "std")]
    {
        CURRENT.with(|c| c.borrow().clone())
    }
    #[cfg(not(feature = "std"))]
    {
        CURRENT.lock().clone()
    }
}
fn current_set(v: Option<AtomicDict>) -> Option<AtomicDict> {
    #[cfg(feature = "std")]
    {
        CURRENT.with(|c| core::mem::replace(&mut *c.borrow_mut(), v))
    }
    #[cfg(not(feature = "std"))]
    {
        core::mem::replace(&mut *CURRENT.lock(), v)
    }
}

fn empty_dict() -> Arg {
    Arg::Dict(Arc::new(RwLock::new(
        crate::python::object::PyMap::default(),
    )))
}

/// `KeyError("AtomicDict is empty")`
fn empty_error() -> LailaError {
    KeyError::new(Args::from("AtomicDict is empty"))
}

/// `hasattr(other, "keys")`: a mapping (dict, AtomicDict, DotMap).
fn is_mapping(a: &Arg) -> bool {
    matches!(a, Arg::Dict(_))
        || AtomicDict::isinstance(a)
        || crate::_compat::dotmap::DotMap::isinstance(a)
}

/// `index` must be an int (`index < 0` on anything else raises in Python).
fn int_index(v: &Arg) -> Result<i64> {
    v.as_i64().ok_or_else(|| {
        LailaError::type_error(alloc::format!(
            "'<' not supported between instances of '{}' and 'int'",
            v.type_name()
        ))
    })
}

impl AtomicDict {
    /// `AtomicDict(*args, **kwargs)`: an `AtomicDict`, optionally from a plain
    /// dict (at most one positional `dict`, or `data=`).
    pub fn new(args: impl Into<Args>) -> Result<AtomicDict> {
        let mut a: Args = args.into();
        let mut positional = core::mem::take(&mut a.positional);
        if matches!(positional.first(), Some(Arg::Dict(_))) {
            if a.__contains__("data")? {
                return Err(LailaError::type_error(
                    "Cannot pass both dict positional arg and data keyword arg",
                ));
            }
            let d = positional.remove(0);
            a.push_keyword("data", d);
        }
        if !positional.is_empty() {
            return Err(LailaError::type_error(
                "AtomicDict accepts at most one positional dict argument",
            ));
        }
        let data = if a.__contains__("data")? {
            a.pop("data")?
        } else {
            empty_dict()
        };
        if !a.keywords.is_empty() {
            let mut names: Vec<&str> = a.keywords.iter().map(|(k, _)| k.as_str()).collect();
            names.sort_unstable();
            return Err(LailaError::type_error(alloc::format!(
                "Unexpected keyword arguments: {}",
                names.join(", ")
            )));
        }
        Ok(Self::from_data(data))
    }

    fn from_data(data: Arg) -> AtomicDict {
        let attrs = PyAttrs::new();
        // `model_construct` skips `_LAILA_OBJECT.__init__`; stamp here.
        let _ = attrs.__setattr__("_creation_timestamp", _now_creation_timestamp());
        AtomicDict(Arc::new(DictInner {
            data: RwLock::new(data),
            lock: RLock::new(),
            order: RwLock::new(None),
            attrs,
        }))
    }

    /// Python `is`.
    pub fn is(&self, other: &AtomicDict) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// `self._lock`
    pub fn _lock(&self) -> RLock {
        self.0.lock.clone()
    }

    fn held(&self) -> Held {
        Held::acquire(&self.0.lock)
    }

    /// `self.data`: the backing dict handle (aliasing, like Python).
    pub fn data(&self) -> Arg {
        self.0.data.read().clone()
    }

    /// `self.data = d`: rebind the backing dict (pydantic validates `dict`).
    pub fn set_data(&self, d: impl IntoArg) -> Result<()> {
        let d = d.into_arg();
        if !matches!(d, Arg::Dict(_)) {
            return Err(super::validation_error(
                "AtomicDict",
                "data",
                "dictionary",
                &d,
            ));
        }
        *self.0.data.write() = d;
        *self.0.order.write() = None;
        Ok(())
    }

    /// `self._order`
    pub fn _order(&self) -> Arg {
        match &*self.0.order.read() {
            Some(v) => v.clone().into_arg(),
            None => Arg::None,
        }
    }

    fn data_keys(&self) -> Result<Vec<Arg>> {
        self.data().__iter__()
    }

    fn data_len(&self) -> Result<usize> {
        self.data().__len__()
    }

    /// Return the positional key index, rebuilding it from `data` when stale.
    fn _positional(&self) -> Result<Vec<Arg>> {
        let mut order = self.0.order.write();
        let n = self.data_len()?;
        match &*order {
            Some(o) if o.len() == n => Ok(o.clone()),
            _ => {
                let keys = self.data_keys()?;
                *order = Some(keys.clone());
                Ok(keys)
            }
        }
    }

    /// Rebuild the positional index from the underlying dict.
    ///
    /// Kept for callers that edit `self.data` out of band; iteration and
    /// `keys()` read `data` directly and never need it.
    pub fn reindex(&self) -> Result<()> {
        let _g = self.held();
        *self.0.order.write() = Some(self.data_keys()?);
        Ok(())
    }

    /// Insert or update *key* without acquiring the lock.
    pub(crate) fn _set_nolock(&self, key: Arg, value: Arg) -> Result<()> {
        let data = self.data();
        if !data.__contains__(key.clone())? {
            let mut order = self.0.order.write();
            let n = data.__len__()?;
            match &mut *order {
                Some(o) if o.len() == n => o.push(key.clone()),
                _ => *order = None,
            }
        }
        data.__setitem__(key, value)
    }

    /// Delete *key* without acquiring the lock (O(1)).
    pub(crate) fn _del_nolock(&self, key: Arg) -> Result<()> {
        self.data().__delitem__(key)?;
        *self.0.order.write() = None;
        Ok(())
    }

    /// Return the value for *key*, raising `KeyError` if missing.
    pub fn __getitem__(&self, key: impl IntoArg) -> Result<Arg> {
        let _g = self.held();
        self.data().__getitem__(key)
    }

    /// Set *key* to *value* under the lock.
    pub fn __setitem__(&self, key: impl IntoArg, value: impl IntoArg) -> Result<()> {
        let _g = self.held();
        self._set_nolock(key.into_arg(), value.into_arg())
    }

    /// Delete *key* under the lock.
    pub fn __delitem__(&self, key: impl IntoArg) -> Result<()> {
        let _g = self.held();
        self._del_nolock(key.into_arg())
    }

    /// Return the number of items.
    pub fn __len__(&self) -> Result<usize> {
        let _g = self.held();
        self.data_len()
    }

    /// Iterate over keys in insertion order (snapshot).
    pub fn __iter__(&self) -> Result<Vec<Arg>> {
        let _g = self.held();
        self.data_keys()
    }

    /// Return `True` if *key* is present.
    pub fn __contains__(&self, key: impl IntoArg) -> Result<bool> {
        let _g = self.held();
        self.data().__contains__(key)
    }

    /// `repr(d)` -> `AtomicDict({'a': 1})`
    pub fn __repr__(&self) -> String {
        let _g = self.held();
        alloc::format!("AtomicDict({})", self.data().__repr__())
    }

    /// `str(d)` -> `data={'a': 1}` (pydantic's default `__str__` lists the fields).
    pub fn __str__(&self) -> String {
        let _g = self.held();
        alloc::format!("data={}", self.data().__repr__())
    }

    /// Return the value for *key*, or *default* if absent.
    #[pysig("get(self, key, default=None)")]
    pub fn get(&self, args: impl Into<Args>) -> Result<Arg> {
        let _g = self.held();
        let mut a = Args::from(key);
        a.push_positional(default);
        self.data().get(a)
    }

    /// Return the value for *key*, inserting *default* if absent.
    #[pysig("setdefault(self, key, default)")]
    pub fn setdefault(&self, args: impl Into<Args>) -> Result<Arg> {
        let _g = self.held();
        let data = self.data();
        if data.__contains__(key.clone())? {
            return data.__getitem__(key);
        }
        self._set_nolock(key, default.clone())?;
        Ok(default)
    }

    /// `pop(key, default=...)`: remove and return the value for *key*, or
    /// *default* if absent (`KeyError` without a default).
    pub fn pop(&self, args: impl Into<Args>) -> Result<Arg> {
        const PARAMS: &[Param] = &[
            Param {
                name: "key",
                kind: ParamKind::PositionalOrKeyword,
                has_default: false,
                default: None,
            },
            Param {
                name: "default",
                kind: ParamKind::PositionalOrKeyword,
                has_default: true,
                default: None,
            },
        ];
        const SIG: Signature = Signature {
            name: "pop",
            params: PARAMS,
        };
        let mut bound = SIG.bind(args.into())?;
        let key = bound.take(0, || Arg::None);
        let default = bound.values[1].take();
        let _g = self.held();
        let data = self.data();
        if data.__contains__(key.clone())? {
            let val = data.__getitem__(key.clone())?;
            self._del_nolock(key)?;
            return Ok(val);
        }
        match default {
            None => Err(KeyError::new(Args::from(key))),
            Some(d) => Ok(d),
        }
    }

    /// Remove and return the last inserted `(key, value)` pair.
    pub fn popitem(&self) -> Result<(Arg, Arg)> {
        let _g = self.held();
        let data = self.data();
        if !data.__bool__() {
            return Err(empty_error());
        }
        let (k, v) = match &data {
            Arg::Dict(d) => d.write().pop().ok_or_else(empty_error)?,
            other => {
                let item = other.popitem()?;
                let pair = item.__iter__()?;
                (pair[0].clone(), pair[1].clone())
            }
        };
        let mut order = self.0.order.write();
        match &mut *order {
            Some(o) if !o.is_empty() && o[o.len() - 1] == k => {
                o.pop();
            }
            _ => *order = None,
        }
        Ok((k, v))
    }

    /// Remove and return the *first* inserted `(key, value)` pair.
    ///
    /// Hot path for queue-style consumers (the taskforce dispatcher pops one
    /// item per task): the backing dict is insertion-ordered and
    /// authoritative, so its first key *is* the oldest entry. O(1).
    pub fn pop_next(&self) -> Result<(Arg, Arg)> {
        let _g = self.held();
        let data = self.data();
        if !data.__bool__() {
            return Err(empty_error());
        }
        let k = data.__iter__()?[0].clone();
        let v = data.pop(Args::from(k.clone()))?;
        *self.0.order.write() = None;
        Ok((k, v))
    }

    /// Remove all items.
    #[pysig("clear(self)")]
    pub fn clear(&self, args: impl Into<Args>) -> Result<()> {
        let _g = self.held();
        self.data().clear(Args::new())?;
        *self.0.order.write() = None;
        Ok(())
    }

    /// Merge items from *other* (a mapping or an iterable of pairs) and/or
    /// keyword arguments.
    #[pysig("update(self, other=None, /, **kwargs)")]
    pub fn update(&self, args: impl Into<Args>) -> Result<()> {
        let _g = self.held();
        self.update_nolock(other, kwargs)
    }

    fn update_nolock(&self, other: Arg, kwargs: Args) -> Result<()> {
        if !other.is_none() {
            if is_mapping(&other) {
                for k in other.__iter__()? {
                    let v = other.__getitem__(k.clone())?;
                    self._set_nolock(k, v)?;
                }
            } else {
                for item in other.__iter__()? {
                    let (k, v) = crate::python::builtins::unpack2(item)?;
                    self._set_nolock(k, v)?;
                }
            }
        }
        for (k, v) in kwargs.keywords {
            self._set_nolock(Arg::Str(k.into()), v)?;
        }
        Ok(())
    }

    /// Return a snapshot list of keys in insertion order.
    #[pysig("keys(self)")]
    pub fn keys(&self, args: impl Into<Args>) -> Result<Arg> {
        let _g = self.held();
        Ok(self.data_keys()?.into_arg())
    }

    /// Return a snapshot list of values in insertion order.
    pub fn values(&self) -> Result<Arg> {
        let _g = self.held();
        let data = self.data();
        let mut out = Vec::new();
        for k in data.__iter__()? {
            out.push(data.__getitem__(k)?);
        }
        Ok(out.into_arg())
    }

    /// Return a snapshot list of `(key, value)` pairs in insertion order.
    pub fn items(&self) -> Result<Arg> {
        let _g = self.held();
        Ok(self.pairs()?.into_arg())
    }

    fn pairs(&self) -> Result<Vec<(Arg, Arg)>> {
        let data = self.data();
        let mut out = Vec::new();
        for k in data.__iter__()? {
            let v = data.__getitem__(k.clone())?;
            out.push((k, v));
        }
        Ok(out)
    }

    /// Return the `(key, value)` pair at positional *index* (negative
    /// indexing supported); `IndexError` if out of range.
    pub fn item_at(&self, index: impl IntoArg) -> Result<(Arg, Arg)> {
        let _g = self.held();
        let mut index = int_index(&index.into_arg())?;
        let order = self._positional()?;
        let n = order.len() as i64;
        if index < 0 {
            index += n;
        }
        if index < 0 || index >= n {
            return Err(LailaError::new(ExcKind::IndexError, "Index out of range"));
        }
        let data = self.data();
        let mut k = order[index as usize].clone();
        if !data.__contains__(k.clone())? {
            // same-length out-of-band edit of ``data``
            let keys = self.data_keys()?;
            k = keys[index as usize].clone();
            *self.0.order.write() = Some(keys);
        }
        let v = data.__getitem__(k.clone())?;
        Ok((k, v))
    }

    /// Return the key at positional *index*.
    pub fn key_at(&self, index: impl IntoArg) -> Result<Arg> {
        let _g = self.held();
        Ok(self.item_at(index)?.0)
    }

    /// Return the value at positional *index*.
    pub fn value_at(&self, index: impl IntoArg) -> Result<Arg> {
        let _g = self.held();
        Ok(self.item_at(index)?.1)
    }

    /// Keep only items in `[start:end)` by insertion order (end exclusive).
    #[pysig("trim(self, start=None, end=None)")]
    pub fn trim(&self, args: impl Into<Args>) -> Result<()> {
        let _g = self.held();
        let order = self.data_keys()?;
        *self.0.order.write() = Some(order.clone());
        let n = order.len() as i64;

        let mut s = if start.is_none() {
            0
        } else {
            int_index(&start)?
        };
        let mut e = if end.is_none() { n } else { int_index(&end)? };
        if s < 0 {
            s += n;
        }
        if e < 0 {
            e += n;
        }
        s = s.max(0);
        e = e.min(n);

        let data = self.data();
        if s >= e {
            data.clear(Args::new())?;
            *self.0.order.write() = None;
            return Ok(());
        }

        let keep_keys: Vec<Arg> = order[s as usize..e as usize].to_vec();
        for k in &order {
            if !keep_keys.contains(k) {
                data.__delitem__(k.clone())?;
            }
        }
        *self.0.order.write() = Some(keep_keys);
        Ok(())
    }

    /// Atomically compute a new value for *key*: *fn* receives the current
    /// value (or `None`) and returns the new value; `None` removes the key.
    /// Returns the new value, or `None` if the key was removed.
    pub fn compute(&self, key: impl IntoArg, f: impl IntoArg) -> Result<Arg> {
        let _g = self.held();
        let key = key.into_arg();
        let f = f.into_arg();
        let data = self.data();
        let mut a = Args::from(key.clone());
        a.push_positional(Arg::None);
        let cur = data.get(a)?;
        let new = f.__call__(Args::from(cur))?;
        if new.is_none() {
            if data.__contains__(key.clone())? {
                self._del_nolock(key)?;
            }
            return Ok(Arg::None);
        }
        self._set_nolock(key, new.clone())?;
        Ok(new)
    }

    /// Add *delta* to the value at *key* (starting from *default*).
    #[pysig("increment(self, key, delta=1, default=0)")]
    pub fn increment(&self, args: impl Into<Args>) -> Result<Arg> {
        let _g = self.held();
        let data = self.data();
        let mut a = Args::from(key.clone());
        a.push_positional(default);
        let val = data.get(a)?;
        let val = (val + delta)?;
        self._set_nolock(key, val.clone())?;
        Ok(val)
    }

    /// Return a human-readable, multi-line string representation.
    #[pysig("pretty(self, indent=2)")]
    pub fn pretty(&self, args: impl Into<Args>) -> Result<String> {
        let _g = self.held();
        let indent = indent.as_i64().ok_or_else(|| {
            LailaError::type_error(alloc::format!(
                "can't multiply sequence by non-int of type '{}'",
                indent.type_name()
            ))
        })?;
        let pad: String = " ".repeat(indent.max(0) as usize);
        let mut lines: Vec<String> = alloc::vec![String::from("AtomicDict {")];
        for (k, v) in self.pairs()? {
            lines.push(alloc::format!("{pad}{}: {},", k.__repr__(), v.__repr__()));
        }
        lines.push(String::from("}"));
        Ok(lines.join("\n"))
    }

    /// `AtomicDict._AtomicView(parent)`: the proxy yielded by `atomic()`.
    #[allow(non_snake_case)]
    pub fn _AtomicView(args: impl Into<Args>) -> Result<AtomicView> {
        let a: Args = args.into();
        let p = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("parent"));
        match p.downcast_ref::<AtomicDict>() {
            Some(d) => Ok(AtomicView { _p: d.clone() }),
            None => Err(LailaError::attribute_error(alloc::format!(
                "'{}' object has no attribute '_set_nolock'",
                p.type_name()
            ))),
        }
    }

    /// `AtomicDict._Atomic(parent, hint="")`: the context manager class.
    #[allow(non_snake_case)]
    pub fn _Atomic(args: impl Into<Args>) -> Result<DictAtomic> {
        let a: Args = args.into();
        let parent = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("parent"));
        let hint = a
            .positional
            .get(1)
            .cloned()
            .unwrap_or_else(|| match a.get("hint") {
                Arg::None => Arg::Str("".into()),
                h => h,
            });
        require_str(&hint, "hint must be a str")?;
        let d = parent
            .downcast_ref::<AtomicDict>()
            .cloned()
            .ok_or_else(|| LailaError::type_error("_Atomic parent must be an AtomicDict"))?;
        Ok(DictAtomic {
            _p: d,
            hint: hint.__str__(),
        })
    }

    /// Return a context manager for batched, lock-held operations; *hint*
    /// is a descriptive label (reserved for future diagnostics).
    #[pysig("atomic(self, hint=\"\")")]
    pub fn atomic(&self, args: impl Into<Args>) -> Result<DictAtomic> {
        require_str(&hint, "hint must be a str")?;
        Ok(DictAtomic {
            _p: self.clone(),
            hint: hint.__str__(),
        })
    }

    /// Return the `AtomicDict` currently held in an atomic block;
    /// `RuntimeError` if called outside an `atomic` context.
    pub fn current() -> Result<AtomicDict> {
        current_get().ok_or_else(|| {
            LailaError::runtime_error("AtomicDict.current() called outside an AtomicDict context")
        })
    }

    /// Run *fn* while holding the lock and return its result.
    #[pysig("run_atomic(self, fn, hint=\"\")")]
    pub fn run_atomic(&self, args: impl Into<Args>) -> Result<Arg> {
        require_str(&hint, "hint must be a str")?;
        let cm = self.atomic(Args::new().with_keyword("hint", hint))?;
        let _guard = cm.__enter__()?;
        r#fn.__call__(Args::new())
    }

    /// `d.name`
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "data" => Ok(self.data()),
            "_lock" => Ok(self._lock().into_arg()),
            "_order" => Ok(self._order()),
            "creation_timestamp" | "_creation_timestamp" => {
                Ok(_LAILA_LOCALLY_ATOMIC_OBJECT::creation_timestamp(self))
            }
            _ => self.0.attrs.__getattr__(name).map_err(|_| {
                LailaError::attribute_error(alloc::format!(
                    "'AtomicDict' object has no attribute '{name}'"
                ))
            }),
        }
    }

    /// `d.name = value`
    pub fn __setattr__(&self, name: &str, value: impl IntoArg) -> Result<()> {
        match name {
            "data" => self.set_data(value),
            "_order" => {
                let v = value.into_arg();
                *self.0.order.write() = if v.is_none() {
                    None
                } else {
                    Some(v.__iter__()?)
                };
                Ok(())
            }
            _ => self.0.attrs.__setattr__(name, value),
        }
    }

    /// `copy.deepcopy(d)`: deep-copied data, fresh lock.
    pub fn deepcopy(&self) -> AtomicDict {
        let _g = self.held();
        let out = Self::from_data(self.data().deepcopy());
        let _ = crate::atomic::definitions::locally_atomic_object::deepcopy_attrs(
            &self.0.attrs,
            &out.0.attrs,
        );
        out
    }
}

trait WithKeyword {
    fn with_keyword(self, name: &str, v: Arg) -> Self;
}
impl WithKeyword for Args {
    fn with_keyword(mut self, name: &str, v: Arg) -> Self {
        self.push_keyword(name, v);
        self
    }
}

impl Default for AtomicDict {
    fn default() -> Self {
        Self::from_data(empty_dict())
    }
}

impl core::fmt::Debug for AtomicDict {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__repr__())
    }
}

impl HasAttrs for AtomicDict {
    fn __attrs__(&self) -> &PyAttrs {
        &self.0.attrs
    }
    fn __class_name__(&self) -> &'static str {
        "AtomicDict"
    }
}
impl _LAILA_LOCALLY_ATOMIC_OBJECT for AtomicDict {}

// ---------------------------------------------------------------------------
// `_AtomicView` / `_Atomic`
// ---------------------------------------------------------------------------

/// Proxy object yielded by `AtomicDict.atomic()` for lock-held mutations.
#[derive(Clone)]
pub struct AtomicView {
    _p: AtomicDict,
}

impl AtomicView {
    pub fn __setitem__(&self, key: impl IntoArg, value: impl IntoArg) -> Result<()> {
        self._p._set_nolock(key.into_arg(), value.into_arg())
    }
    pub fn __delitem__(&self, key: impl IntoArg) -> Result<()> {
        self._p._del_nolock(key.into_arg())
    }
    #[pysig("update(self, other=None, /, **kwargs)")]
    pub fn update(&self, args: impl Into<Args>) -> Result<()> {
        self._p.update_nolock(other, kwargs)
    }
    /// `view._p`
    pub fn _p(&self) -> AtomicDict {
        self._p.clone()
    }
}

/// Context manager that holds the dict lock and exposes an [`AtomicView`].
pub struct DictAtomic {
    _p: AtomicDict,
    pub hint: String,
}

/// The view yielded by `with d.atomic() as v:`; resets `AtomicDict.current()`
/// and releases the lock on drop.
pub struct DictGuard {
    view: AtomicView,
    token: Option<Option<AtomicDict>>,
    _held: Held,
}

impl DictAtomic {
    pub fn __enter__(&self) -> Result<DictGuard> {
        let held = Held::acquire(&self._p.0.lock);
        // expose this instance as "current"
        let token = current_set(Some(self._p.clone()));
        Ok(DictGuard {
            view: AtomicView {
                _p: self._p.clone(),
            },
            token: Some(token),
            _held: held,
        })
    }
    /// `cm.hint`
    pub fn hint(&self) -> String {
        self.hint.clone()
    }
}
impl DictGuard {
    pub fn __exit__(self) -> Result<()> {
        drop(self);
        Ok(())
    }
    /// `view[k] = v` (the inherent, Python-shaped item protocol -- the
    /// `PyObject` forwarders take `&Arg`).
    pub fn __setitem__(&self, key: impl IntoArg, value: impl IntoArg) -> Result<()> {
        self.view.__setitem__(key, value)
    }
    /// `del view[k]`
    pub fn __delitem__(&self, key: impl IntoArg) -> Result<()> {
        self.view.__delitem__(key)
    }
    /// `view[k]`
    pub fn __getitem__(&self, key: impl IntoArg) -> Result<Arg> {
        self.view._p.data().__getitem__(key)
    }
}
impl Drop for DictGuard {
    fn drop(&mut self) {
        if let Some(prev) = self.token.take() {
            let _ = current_set(prev);
        }
    }
}
impl core::ops::Deref for DictGuard {
    type Target = AtomicView;
    fn deref(&self) -> &AtomicView {
        &self.view
    }
}

impl PyObject for AtomicView {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "_AtomicView"
    }
    fn __repr__(&self) -> String {
        alloc::format!("<AtomicDict._AtomicView of {}>", self._p.__repr__())
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<AtomicView>()
            .is_some_and(|o| self._p.is(&o._p))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self._p.0) as *const () as usize as i64)
    }
    fn __setitem__(&self, key: &Arg, value: Arg) -> Result<()> {
        AtomicView::__setitem__(self, key.clone(), value)
    }
    fn __delitem__(&self, key: &Arg) -> Result<()> {
        AtomicView::__delitem__(self, key.clone())
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "update" => {
                self.update(args)?;
                Ok(Arg::None)
            }
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'_AtomicView' object has no attribute '{name}'"
            ))),
        }
    }
}
impl IntoArg for AtomicView {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for AtomicView {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}

// ---------------------------------------------------------------------------
// Dynamic surface
// ---------------------------------------------------------------------------

impl PyObject for AtomicDict {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    /// `a is b`: identity of the shared handle, not of the boxing.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<AtomicDict>()
            .is_some_and(|o| self.is(o))
    }
    /// `copy.deepcopy(x)`: a new object with fresh locks.
    fn __deepcopy__(&self) -> Option<Arg> {
        Some(AtomicDict::deepcopy(self).into_arg())
    }
    /// `isinstance(x, _LAILA_OBJECT)`: the atomic types are laila objects.
    fn __bases__(&self) -> &'static [&'static str] {
        &["_LAILA_LOCALLY_ATOMIC_OBJECT", "_LAILA_OBJECT"]
    }
    fn type_name(&self) -> &'static str {
        "AtomicDict"
    }
    fn __repr__(&self) -> String {
        AtomicDict::__repr__(self)
    }
    fn __str__(&self) -> String {
        AtomicDict::__str__(self)
    }
    /// pydantic compares private attributes too (`_lock` differs): identity.
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<AtomicDict>()
            .is_some_and(|o| self.is(o))
    }
    fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error("unhashable type: 'AtomicDict'"))
    }
    fn __len__(&self) -> Result<usize> {
        AtomicDict::__len__(self)
    }
    fn __iter__(&self) -> Result<Vec<Arg>> {
        AtomicDict::__iter__(self)
    }
    fn __contains__(&self, item: &Arg) -> Result<bool> {
        AtomicDict::__contains__(self, item.clone())
    }
    fn __getitem__(&self, key: &Arg) -> Result<Arg> {
        AtomicDict::__getitem__(self, key.clone())
    }
    fn __setitem__(&self, key: &Arg, value: Arg) -> Result<()> {
        AtomicDict::__setitem__(self, key.clone(), value)
    }
    fn __delitem__(&self, key: &Arg) -> Result<()> {
        AtomicDict::__delitem__(self, key.clone())
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        AtomicDict::__getattr__(self, name)
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        AtomicDict::__setattr__(self, name, value)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let pos = |i: usize, what: &str| -> Result<Arg> {
            args.positional.get(i).cloned().ok_or_else(|| {
                LailaError::type_error(alloc::format!(
                    "AtomicDict.{name}() missing 1 required positional argument: '{what}'"
                ))
            })
        };
        Ok(match name {
            "atomic" => IntoArg::into_arg(self.atomic(args)?),
            "get" => self.get(args)?,
            "setdefault" => self.setdefault(args)?,
            "pop" => self.pop(args)?,
            "popitem" => self.popitem()?.into_arg(),
            "pop_next" => self.pop_next()?.into_arg(),
            "clear" => {
                self.clear(args)?;
                Arg::None
            }
            "update" => {
                self.update(args)?;
                Arg::None
            }
            "keys" => self.keys(args)?,
            "values" => self.values()?,
            "items" => self.items()?,
            "item_at" => self.item_at(pos(0, "index")?)?.into_arg(),
            "key_at" => self.key_at(pos(0, "index")?)?,
            "value_at" => self.value_at(pos(0, "index")?)?,
            "trim" => {
                self.trim(args)?;
                Arg::None
            }
            "compute" => self.compute(pos(0, "key")?, pos(1, "fn")?)?,
            "increment" => self.increment(args)?,
            "pretty" => Arg::Str(self.pretty(args)?.into()),
            "reindex" => {
                self.reindex()?;
                Arg::None
            }
            "run_atomic" => self.run_atomic(args)?,
            "lock" => Arg::Bool(_LAILA_LOCALLY_ATOMIC_OBJECT::lock(self, args)?),
            "unlock" => {
                _LAILA_LOCALLY_ATOMIC_OBJECT::unlock(self)?;
                Arg::None
            }
            "locked" => Arg::Bool(_LAILA_LOCALLY_ATOMIC_OBJECT::locked(self, args)),
            _ => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'AtomicDict' object has no attribute '{name}'"
                )))
            }
        })
    }
}

impl IntoArg for AtomicDict {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for AtomicDict {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl IsInstance for AtomicDict {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<AtomicDict>())
    }
}
impl PartialEq for AtomicDict {
    fn eq(&self, other: &AtomicDict) -> bool {
        self.is(other)
    }
}
impl crate::python::builtins::PyLen for AtomicDict {
    fn __len__(&self) -> Result<usize> {
        AtomicDict::__len__(self)
    }
}
impl crate::python::builtins::PyIter for &AtomicDict {
    type Item = Arg;
    fn py_iter(self) -> Result<crate::python::builtins::PyIterator<Arg>> {
        Ok(crate::python::builtins::PyIterator::eager(
            AtomicDict::__iter__(self)?,
        ))
    }
}
impl crate::python::builtins::PyContains for AtomicDict {
    fn py_contains(&self, item: Arg) -> Result<bool> {
        AtomicDict::__contains__(self, item)
    }
}
impl crate::python::builtins::Truthy for AtomicDict {
    /// `bool(d)` goes through `__len__`.
    fn __bool__(&self) -> bool {
        AtomicDict::__len__(self).map(|n| n != 0).unwrap_or(true)
    }
}

super::context_manager_py_object!(DictAtomic, "AtomicDict._Atomic");
super::guard_py_object!(DictGuard, "AtomicDict");
