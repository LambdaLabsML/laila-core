//! `dotmap.DotMap` (1.3.30) emulation.
//!
//! Attribute and item access share one ordered store. Reading a missing key
//! *autovivifies* a nested `DotMap` (`args.policy.central.memory` just works)
//! while `.get(k)` does not. Nested dicts passed to the constructor are
//! converted recursively (also inside lists), exactly like dotmap; values
//! assigned later (`d.c = {..}`, `d["c"] = {..}`, `update`, keyword arguments)
//! are stored as-is.
//!
//! A `DotMap` is a handle: `Clone` aliases the same store (Python reference
//! semantics), `copy()` builds a new one. `IntoArg` wraps the handle in
//! `Arg::Object`, so a `DotMap` travels through the dynamic layer with
//! `__eq__` (against `DotMap` *and* `dict`, recursively), `__repr__`
//! (`DotMap(a=1, b=DotMap(c=2))`), `__getattr__` / `__setattr__`
//! (autovivifying when `_dynamic`) and `hash()` raising
//! `TypeError: unhashable type: 'DotMap'` as in Python.
//!
//! Python's `dotmap` source is the spec; where the JS port (`_compat/dotmap.js`)
//! differs from it (nested `DotMap` values are *copied* by the constructor in
//! Python, kept by reference in JS; `_dynamic` propagates to nested maps in
//! Python) this file follows Python.
//!
//! Not ported: `pprint`, `bannerStr` (stdout / pretty-printing), ordering
//! operators (`OrderedDict` ordering raises `TypeError` anyway) and pickling.

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::python::args::Args;
use crate::python::builtins::IsInstance;
use crate::python::object::{Arg, IntoArg, PyArg, PyMap, PyObject};
use crate::python::sync::RwLock;
use crate::python::{LailaError, Param, ParamKind, Result, Signature};

/// `dotmap.reserved_keys` (dotmap 1.3.30 under CPython 3.14): the names
/// `_prevent_method_masking=True` refuses as keys.
pub const RESERVED_KEYS: &[&str] = &[
    "_DotMap__call_items",
    "_MutableMapping__marker",
    "_abc_impl",
    "_getListStr",
    "_getSubMapDotList",
    "_getSubMapStr",
    "_getValueStr",
    "bannerStr",
    "clear",
    "copy",
    "empty",
    "fromkeys",
    "get",
    "has_key",
    "items",
    "iteritems",
    "iterkeys",
    "itervalues",
    "keys",
    "move_to_end",
    "next",
    "parseOther",
    "pop",
    "popitem",
    "pprint",
    "setdefault",
    "toDict",
    "update",
    "values",
    "viewitems",
    "viewkeys",
    "viewvalues",
];

const CANARY: &str = "_ipython_canary_method_should_not_exist_";

struct State {
    map: PyMap<Arg, Arg>,
    dynamic: bool,
    prevent_method_masking: bool,
}

/// `dotmap.DotMap`: an ordered, dynamically-expandable dot-access dictionary.
#[derive(Clone)]
pub struct DotMap(Arc<RwLock<State>>);

/// Identity-keyed memo (`id(obj) -> value`) used for cycle handling, the
/// twin of dotmap's `_trackedIDs` / `seen` dicts.
type Seen<T> = Vec<(usize, T)>;

fn seen_get<T: Clone>(seen: &Seen<T>, id: usize) -> Option<T> {
    seen.iter().find(|(k, _)| *k == id).map(|(_, v)| v.clone())
}

fn arg_id(a: &Arg) -> usize {
    match a {
        Arg::Dict(d) => Arc::as_ptr(d) as *const () as usize,
        Arg::List(l) => Arc::as_ptr(l) as *const () as usize,
        Arg::Object(o) => Arc::as_ptr(o) as *const () as usize,
        _ => 0,
    }
}

/// `k in reserved_keys` (only `str` keys can match).
fn is_reserved(k: &Arg) -> bool {
    matches!(k, Arg::Str(s) if RESERVED_KEYS.contains(&&**s))
}

fn reserved_error(k: &Arg) -> LailaError {
    // KeyError('"k" is reserved'): str(exc) is repr of the message.
    LailaError::key_error(crate::python::pyrepr::str_repr(&alloc::format!(
        "\"{}\" is reserved",
        k.__str__()
    )))
}

/// `isinstance(v, dict)` -- `DotMap` is a `dict` subclass in Python.
fn is_dict_like(v: &Arg) -> bool {
    matches!(v, Arg::Dict(_)) || DotMap::isinstance(v)
}

/// Python's `(k, v) = item` with CPython's error texts.
fn unpack_pair(item: &Arg) -> Result<(Arg, Arg)> {
    let v = match item.__iter__() {
        Ok(v) => v,
        Err(_) => {
            return Err(LailaError::type_error(alloc::format!(
                "cannot unpack non-iterable {} object",
                item.type_name()
            )))
        }
    };
    match v.len() {
        2 => {
            let mut it = v.into_iter();
            Ok((it.next().unwrap(), it.next().unwrap()))
        }
        n if n > 2 => Err(LailaError::value_error(alloc::format!(
            "too many values to unpack (expected 2, got {n})"
        ))),
        n => Err(LailaError::value_error(alloc::format!(
            "not enough values to unpack (expected 2, got {n})"
        ))),
    }
}

/// `obj.items()` for a mapping (`dict` / `DotMap`) or the `(k, v)` pairs of
/// an iterable; a non-iterable yields no pairs (dotmap: `src = []`).
fn source_pairs(d: &Arg) -> Result<Vec<(Arg, Arg)>> {
    if let Some(dm) = DotMap::downcast(d) {
        return Ok(dm.snapshot());
    }
    if let Arg::Dict(m) = d {
        return Ok(m
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect());
    }
    match d.__iter__() {
        Ok(items) => items.iter().map(unpack_pair).collect(),
        Err(_) => Ok(Vec::new()),
    }
}

impl DotMap {
    /// `DotMap(*args, **kwargs)`: an empty map, or one built from a mapping
    /// (`dict` / `DotMap`) or an iterable of `(key, value)` pairs, plus
    /// keyword entries. Options: `_dynamic=True`, `_prevent_method_masking=False`.
    ///
    /// Nested dicts (and dicts inside lists) of the positional source are
    /// converted to `DotMap`s recursively; keyword values are stored as-is.
    pub fn new(args: impl Into<Args>) -> Result<DotMap> {
        let mut a: Args = args.into();
        let dynamic = a.pop_or("_dynamic", true).__bool__();
        let prevent = a.pop_or("_prevent_method_masking", false).__bool__();
        let mut tracked: Seen<Arg> = Vec::new();
        Ok(Self::build(a, dynamic, prevent, &mut tracked)?.0)
    }

    /// `DotMap(_dynamic=.., _prevent_method_masking=..)`.
    pub fn with_options(dynamic: bool, prevent_method_masking: bool) -> DotMap {
        DotMap(Arc::new(RwLock::new(State {
            map: PyMap::default(),
            dynamic,
            prevent_method_masking,
        })))
    }

    /// Returns the map and its `Arg::Object` wrapper -- the *same* wrapper is
    /// memoised for shared / cyclic sources so identity survives conversion.
    fn build(
        a: Args,
        dynamic: bool,
        prevent: bool,
        tracked: &mut Seen<Arg>,
    ) -> Result<(DotMap, Arg)> {
        let out = Self::with_options(dynamic, prevent);
        let out_arg = out.clone().into_arg();
        if let Some(d) = a.positional.first() {
            // for recursive assignment handling
            tracked.push((arg_id(d), out_arg.clone()));
            for (k, v) in source_pairs(d)? {
                if prevent && is_reserved(&k) {
                    return Err(reserved_error(&k));
                }
                let v = Self::convert(v, dynamic, prevent, tracked)?;
                out.__setitem__(k, v)?;
            }
        }
        for (k, v) in a.keywords {
            let k = Arg::Str(k.into());
            if prevent && is_reserved(&k) {
                return Err(reserved_error(&k));
            }
            out.__setitem__(k, v)?;
        }
        Ok((out, out_arg))
    }

    /// Constructor-time value conversion: `isinstance(v, dict)` -> `DotMap`,
    /// `type(v) is list` -> new list with its dict items converted.
    fn convert(v: Arg, dynamic: bool, prevent: bool, tracked: &mut Seen<Arg>) -> Result<Arg> {
        if is_dict_like(&v) {
            return Self::convert_dict(v, dynamic, prevent, tracked);
        }
        if let Arg::List(l) = &v {
            let items = l.read().clone();
            let mut out = Vec::with_capacity(items.len());
            for i in items {
                out.push(if is_dict_like(&i) {
                    Self::convert_dict(i, dynamic, prevent, tracked)?
                } else {
                    i
                });
            }
            return Ok(out.into_arg());
        }
        Ok(v)
    }

    fn convert_dict(v: Arg, dynamic: bool, prevent: bool, tracked: &mut Seen<Arg>) -> Result<Arg> {
        if let Some(seen) = seen_get(tracked, arg_id(&v)) {
            return Ok(seen);
        }
        Ok(Self::build(Args::from(v), dynamic, prevent, tracked)?.1)
    }

    /// `DotMap.fromkeys(seq, value=None)`.
    pub fn fromkeys(seq: impl IntoArg, value: impl IntoArg) -> Result<DotMap> {
        let out = DotMap::default();
        let value = value.into_arg();
        for k in seq.into_arg().__iter__()? {
            out.__setitem__(k, value.clone())?;
        }
        Ok(out)
    }

    /// The `DotMap` inside an `Arg::Object`, if it is one (aliasing handle).
    pub fn downcast(a: &Arg) -> Option<DotMap> {
        a.downcast_ref::<DotMap>().cloned()
    }

    /// Python `is`: both handles share one store.
    pub fn is(&self, other: &DotMap) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// `id(self)`.
    fn id(&self) -> usize {
        Arc::as_ptr(&self.0) as *const () as usize
    }

    /// `self._dynamic`.
    pub fn _dynamic(&self) -> bool {
        self.0.read().dynamic
    }

    /// `self._prevent_method_masking`.
    pub fn _prevent_method_masking(&self) -> bool {
        self.0.read().prevent_method_masking
    }

    /// A snapshot of `(key, value)` pairs (taken without holding the lock
    /// across nested calls, so self-referencing maps cannot deadlock).
    fn snapshot(&self) -> Vec<(Arg, Arg)> {
        self.0
            .read()
            .map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    // -- Python protocol ---------------------------------------------------

    /// `self[k]`: autovivifies a nested `DotMap` for a missing key when
    /// `_dynamic`; `KeyError` otherwise.
    pub fn __getitem__(&self, k: impl IntoArg) -> Result<Arg> {
        let k = k.into_arg();
        k.__hash__()?;
        let (found, dynamic) = {
            let g = self.0.read();
            (g.map.get(&k).cloned(), g.dynamic)
        };
        if let Some(v) = found {
            return Ok(v);
        }
        if dynamic && k != CANARY {
            // automatically extend to new DotMap
            let child = DotMap::default();
            self.__setitem__(k.clone(), child.clone())?;
            return Ok(child.into_arg());
        }
        Err(LailaError::key_error(k.__repr__()))
    }

    /// `self[k] = v` (stored as-is, no conversion).
    pub fn __setitem__(&self, k: impl IntoArg, v: impl IntoArg) -> Result<()> {
        let k = k.into_arg();
        k.__hash__()?;
        self.0.write().map.insert(k, v.into_arg());
        Ok(())
    }

    /// `del self[k]`.
    pub fn __delitem__(&self, k: impl IntoArg) -> Result<()> {
        let k = k.into_arg();
        k.__hash__()?;
        match self.0.write().map.shift_remove(&k) {
            Some(_) => Ok(()),
            None => Err(LailaError::key_error(k.__repr__())),
        }
    }

    /// `self.k`: dunder names raise `AttributeError(k)`; `_dynamic` /
    /// `_prevent_method_masking` / `_map` read the instance attributes; the
    /// dict-API method names return bound callables (Python returns the bound
    /// method); anything else is `self[k]` with `KeyError` turned into
    /// `AttributeError("'DotMap' object has no attribute 'k'")`.
    pub fn __getattr__(&self, k: &str) -> Result<Arg> {
        if k.starts_with("__") && k.ends_with("__") {
            return Err(LailaError::attribute_error(k));
        }
        match k {
            "_dynamic" => return Ok(Arg::Bool(self._dynamic())),
            "_prevent_method_masking" => return Ok(Arg::Bool(self._prevent_method_masking())),
            "_map" => return Ok(self.shallow_dict()),
            _ => {}
        }
        if RESERVED_KEYS.contains(&k) {
            return match self.bound_method(k) {
                Some(f) => Ok(f),
                None => Err(LailaError::attribute_error(alloc::format!(
                    "'DotMap' object has no attribute '{k}'"
                ))),
            };
        }
        match self.__getitem__(k) {
            Ok(v) => Ok(v),
            Err(e) if e.kind == crate::python::ExcKind::KeyError => {
                Err(LailaError::attribute_error(alloc::format!(
                    "'DotMap' object has no attribute '{k}'"
                )))
            }
            Err(e) => Err(e),
        }
    }

    /// `self.k = v`.
    pub fn __setattr__(&self, k: &str, v: impl IntoArg) -> Result<()> {
        let v = v.into_arg();
        match k {
            "_dynamic" => {
                self.0.write().dynamic = v.__bool__();
                return Ok(());
            }
            "_prevent_method_masking" => {
                self.0.write().prevent_method_masking = v.__bool__();
                return Ok(());
            }
            "_map" | CANARY => {
                if k == "_map" {
                    let pairs = source_pairs(&v)?;
                    let mut g = self.0.write();
                    g.map.clear();
                    for (kk, vv) in pairs {
                        kk.__hash__()?;
                        g.map.insert(kk, vv);
                    }
                }
                return Ok(());
            }
            _ => {}
        }
        if self._prevent_method_masking() && RESERVED_KEYS.contains(&k) {
            return Err(reserved_error(&Arg::Str(k.into())));
        }
        self.__setitem__(k, v)
    }

    /// `del self.k`.
    pub fn __delattr__(&self, k: &str) -> Result<()> {
        self.__delitem__(k)
    }

    /// `k in self`.
    pub fn __contains__(&self, k: impl IntoArg) -> Result<bool> {
        let k = k.into_arg();
        k.__hash__()?;
        Ok(self.0.read().map.contains_key(&k))
    }

    /// `len(self)`.
    pub fn __len__(&self) -> usize {
        self.0.read().map.len()
    }

    /// `bool(self)`.
    pub fn __bool__(&self) -> bool {
        self.__len__() > 0
    }

    /// `iter(self)`: the keys.
    pub fn __iter__(&self) -> Vec<Arg> {
        self.0.read().map.keys().cloned().collect()
    }

    /// `self + other`: `other` when empty, `TypeError` otherwise.
    pub fn __add__(&self, other: impl IntoArg) -> Result<Arg> {
        let other = other.into_arg();
        if self.empty() {
            Ok(other)
        } else {
            Err(LailaError::type_error(alloc::format!(
                "unsupported operand type(s) for +: 'DotMap' and '{}'",
                other.type_name()
            )))
        }
    }

    /// `self == other`: against a `DotMap` (ordered, like `OrderedDict`) or a
    /// `dict` (unordered); nested `DotMap` / `dict` values compare recursively.
    /// Anything else is `False`.
    pub fn __eq__(&self, other: impl IntoArg) -> bool {
        let other = other.into_arg();
        if let Some(o) = DotMap::downcast(&other) {
            return self.eq_dotmap(&o);
        }
        match &other {
            Arg::Dict(d) => {
                let theirs: Vec<(Arg, Arg)> = d
                    .read()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                self.eq_pairs(&theirs)
            }
            _ => false,
        }
    }

    fn eq_dotmap(&self, o: &DotMap) -> bool {
        if self.is(o) {
            return true;
        }
        let theirs = o.snapshot();
        // OrderedDict == OrderedDict: dict equality *and* same key order.
        self.eq_pairs(&theirs)
            && self
                .__iter__()
                .iter()
                .zip(theirs.iter())
                .all(|(a, (b, _))| a == b)
    }

    fn eq_pairs(&self, theirs: &[(Arg, Arg)]) -> bool {
        let mine = self.snapshot();
        if mine.len() != theirs.len() {
            return false;
        }
        mine.iter().all(|(k, v)| {
            theirs
                .iter()
                .find(|(tk, _)| tk == k)
                .is_some_and(|(_, tv)| deep_eq(v, tv))
        })
    }

    /// `str(self)`: `DotMap(a=1, b=DotMap(c=2))`; a map reached again on
    /// the current path prints as `DotMap(...)`.
    pub fn __str__(&self) -> String {
        let mut seen = alloc::vec![self.id()];
        self.str_inner(&mut seen)
    }

    fn str_inner(&self, seen: &mut Vec<usize>) -> String {
        let mut items: Vec<String> = Vec::new();
        for (k, v) in self.snapshot() {
            let text = match DotMap::downcast(&v) {
                // circular assignment case
                Some(sub) if seen.contains(&sub.id()) => "DotMap(...)".to_string(),
                Some(sub) => {
                    seen.push(sub.id());
                    sub.str_inner(seen)
                }
                None => v.__repr__(),
            };
            items.push(alloc::format!("{}={}", k.__str__(), text));
        }
        alloc::format!("DotMap({})", items.join(", "))
    }

    /// `repr(self)` is `str(self)`.
    pub fn __repr__(&self) -> String {
        self.__str__()
    }

    /// `hash(self)`: `TypeError: unhashable type: 'DotMap'`.
    pub fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error("unhashable type: 'DotMap'"))
    }

    // -- dict API ----------------------------------------------------------

    /// `self.get(key, default=None)` (never autovivifies).
    pub fn get(&self, key: impl IntoArg, default: impl IntoArg) -> Result<Arg> {
        let key = key.into_arg();
        key.__hash__()?;
        Ok(self
            .0
            .read()
            .map
            .get(&key)
            .cloned()
            .unwrap_or_else(|| default.into_arg()))
    }

    /// `self.setdefault(key, default=None)`.
    pub fn setdefault(&self, key: impl IntoArg, default: impl IntoArg) -> Result<Arg> {
        let key = key.into_arg();
        key.__hash__()?;
        let mut g = self.0.write();
        Ok(g.map
            .entry(key)
            .or_insert_with(|| default.into_arg())
            .clone())
    }

    /// `self.has_key(key)`.
    pub fn has_key(&self, key: impl IntoArg) -> Result<bool> {
        self.__contains__(key)
    }

    /// `list(self.keys())`.
    pub fn keys(&self) -> Arg {
        self.__iter__().into_arg()
    }

    /// `list(self.values())`.
    pub fn values(&self) -> Arg {
        self.0
            .read()
            .map
            .values()
            .cloned()
            .collect::<Vec<_>>()
            .into_arg()
    }

    /// `list(self.items())`: a list of `(key, value)` tuples.
    pub fn items(&self) -> Arg {
        self.snapshot().into_arg()
    }

    /// `self.update(*args, **kwargs)`: a mapping or an iterable of pairs,
    /// then keywords. Values are stored as-is.
    pub fn update(&self, args: impl Into<Args>) -> Result<()> {
        let a: Args = args.into();
        if a.positional.len() > 1 {
            return Err(LailaError::type_error(alloc::format!(
                "update expected at most 1 argument, got {}",
                a.positional.len()
            )));
        }
        if let Some(src) = a.positional.first() {
            let pairs = if is_dict_like(src) {
                source_pairs(src)?
            } else {
                src.__iter__()?
                    .iter()
                    .map(unpack_pair)
                    .collect::<Result<Vec<_>>>()?
            };
            for (k, v) in pairs {
                self.__setitem__(k, v)?;
            }
        }
        for (k, v) in a.keywords {
            self.__setitem__(Arg::Str(k.into()), v)?;
        }
        Ok(())
    }

    /// `self.pop(key, default=None)` -- never raises for a missing key.
    pub fn pop(&self, key: impl IntoArg, default: impl IntoArg) -> Result<Arg> {
        let key = key.into_arg();
        key.__hash__()?;
        Ok(self
            .0
            .write()
            .map
            .shift_remove(&key)
            .unwrap_or_else(|| default.into_arg()))
    }

    /// `self.popitem()`: the last `(key, value)` pair.
    pub fn popitem(&self) -> Result<Arg> {
        match self.0.write().map.pop() {
            Some((k, v)) => Ok((k, v).into_arg()),
            None => Err(LailaError::key_error("'dictionary is empty'")),
        }
    }

    /// `self.clear()`.
    pub fn clear(&self) {
        self.0.write().map.clear();
    }

    /// `self.copy()`: `DotMap(self)` (nested maps and lists are rebuilt,
    /// options reset to the defaults -- as in dotmap).
    pub fn copy(&self) -> Result<DotMap> {
        DotMap::new(self.clone())
    }

    /// `self.empty()`: dotmap's `not any(self)` (truthiness of the *keys*).
    pub fn empty(&self) -> bool {
        !self.__iter__().iter().any(Arg::__bool__)
    }

    /// `self.toDict()`: recursive conversion to plain dicts (lists and
    /// tuples are rebuilt with their `DotMap` items converted; shared and
    /// cyclic maps are preserved through a `seen` memo).
    pub fn to_dict(&self) -> Arg {
        let mut seen: Seen<Arg> = Vec::new();
        self.to_dict_inner(&mut seen)
    }

    /// `self.toDict()` under its Python spelling.
    #[allow(non_snake_case)]
    pub fn toDict(&self) -> Arg {
        self.to_dict()
    }

    fn to_dict_inner(&self, seen: &mut Seen<Arg>) -> Arg {
        let out = Arg::Dict(Arc::new(RwLock::new(PyMap::default())));
        seen.push((self.id(), out.clone()));
        let conv = |v: Arg, seen: &mut Seen<Arg>| -> Arg {
            match DotMap::downcast(&v) {
                Some(sub) => seen_get(seen, sub.id()).unwrap_or_else(|| sub.to_dict_inner(seen)),
                None => v,
            }
        };
        for (k, v) in self.snapshot() {
            let v = match &v {
                Arg::Object(_) => conv(v, seen),
                Arg::List(l) => {
                    let items = l.read().clone();
                    items
                        .into_iter()
                        .map(|i| conv(i, seen))
                        .collect::<Vec<_>>()
                        .into_arg()
                }
                Arg::Tuple(t) => Arg::Tuple(
                    t.iter()
                        .cloned()
                        .map(|i| conv(i, seen))
                        .collect::<Vec<_>>()
                        .into(),
                ),
                _ => v,
            };
            if let Arg::Dict(d) = &out {
                d.write().insert(k, v);
            }
        }
        out
    }

    /// `dict(self._map)`: a shallow plain-dict copy (values untouched).
    fn shallow_dict(&self) -> Arg {
        let mut m = PyMap::default();
        for (k, v) in self.snapshot() {
            m.insert(k, v);
        }
        Arg::Dict(Arc::new(RwLock::new(m)))
    }

    /// Bound dict-API methods as Python callables (`d.get`, `d.items`, ...)
    /// so `hasattr(d, "get")` / `d.get("k")` work through the dynamic layer.
    fn bound_method(&self, name: &str) -> Option<Arg> {
        const KEY: Param = Param {
            name: "key",
            kind: ParamKind::PositionalOrKeyword,
            has_default: false,
            default: None,
        };
        const DEFAULT: Param = Param {
            name: "default",
            kind: ParamKind::PositionalOrKeyword,
            has_default: true,
            default: None,
        };
        const KEY_DEFAULT: &[Param] = &[KEY, DEFAULT];
        const NONE: &[Param] = &[];
        let me = self.clone();
        let bind = |sig: Signature, a: Args| sig.bind(a);
        let f: crate::python::object::Callable = match name {
            "get" => Arc::new(move |a: Args| {
                let mut b = bind(
                    Signature {
                        name: "get",
                        params: KEY_DEFAULT,
                    },
                    a,
                )?;
                let key = b.take(0, || Arg::None);
                me.get(key, b.take(1, || Arg::None))
            }),
            "pop" => Arc::new(move |a: Args| {
                let mut b = bind(
                    Signature {
                        name: "pop",
                        params: KEY_DEFAULT,
                    },
                    a,
                )?;
                let key = b.take(0, || Arg::None);
                me.pop(key, b.take(1, || Arg::None))
            }),
            "setdefault" => Arc::new(move |a: Args| {
                let mut b = bind(
                    Signature {
                        name: "setdefault",
                        params: KEY_DEFAULT,
                    },
                    a,
                )?;
                let key = b.take(0, || Arg::None);
                me.setdefault(key, b.take(1, || Arg::None))
            }),
            "has_key" => Arc::new(move |a: Args| {
                let mut b = bind(
                    Signature {
                        name: "has_key",
                        params: &[KEY],
                    },
                    a,
                )?;
                Ok(Arg::Bool(me.has_key(b.take(0, || Arg::None))?))
            }),
            "keys" => Arc::new(move |a: Args| {
                bind(
                    Signature {
                        name: "keys",
                        params: NONE,
                    },
                    a,
                )?;
                Ok(me.keys())
            }),
            "values" => Arc::new(move |a: Args| {
                bind(
                    Signature {
                        name: "values",
                        params: NONE,
                    },
                    a,
                )?;
                Ok(me.values())
            }),
            "items" | "iteritems" => Arc::new(move |a: Args| {
                bind(
                    Signature {
                        name: "items",
                        params: NONE,
                    },
                    a,
                )?;
                Ok(me.items())
            }),
            "update" => Arc::new(move |a: Args| {
                me.update(a)?;
                Ok(Arg::None)
            }),
            "copy" => Arc::new(move |a: Args| {
                bind(
                    Signature {
                        name: "copy",
                        params: NONE,
                    },
                    a,
                )?;
                Ok(me.copy()?.into_arg())
            }),
            "clear" => Arc::new(move |a: Args| {
                bind(
                    Signature {
                        name: "clear",
                        params: NONE,
                    },
                    a,
                )?;
                me.clear();
                Ok(Arg::None)
            }),
            "toDict" => Arc::new(move |a: Args| {
                bind(
                    Signature {
                        name: "toDict",
                        params: NONE,
                    },
                    a,
                )?;
                Ok(me.to_dict())
            }),
            "empty" => Arc::new(move |a: Args| {
                bind(
                    Signature {
                        name: "empty",
                        params: NONE,
                    },
                    a,
                )?;
                Ok(Arg::Bool(me.empty()))
            }),
            "popitem" => Arc::new(move |a: Args| {
                bind(
                    Signature {
                        name: "popitem",
                        params: NONE,
                    },
                    a,
                )?;
                me.popitem()
            }),
            _ => return None,
        };
        Some(Arg::Callable(f))
    }
}

/// Python equality where `DotMap`s may sit on either side at any depth
/// (`DotMap.__eq__` is tried for a `DotMap` operand, lists / tuples / dicts
/// recurse, everything else is `Arg == Arg`).
pub fn deep_eq(a: &Arg, b: &Arg) -> bool {
    if let Some(dm) = DotMap::downcast(a) {
        return dm.__eq__(b.clone());
    }
    if let Some(dm) = DotMap::downcast(b) {
        return dm.__eq__(a.clone());
    }
    match (a, b) {
        (Arg::List(x), Arg::List(y)) => {
            if Arc::ptr_eq(x, y) {
                return true;
            }
            let (gx, gy) = (x.read().clone(), y.read().clone());
            gx.len() == gy.len() && gx.iter().zip(gy.iter()).all(|(p, q)| deep_eq(p, q))
        }
        (Arg::Tuple(x), Arg::Tuple(y)) => {
            x.len() == y.len() && x.iter().zip(y.iter()).all(|(p, q)| deep_eq(p, q))
        }
        (Arg::Dict(x), Arg::Dict(y)) => {
            if Arc::ptr_eq(x, y) {
                return true;
            }
            let gx: Vec<(Arg, Arg)> = x
                .read()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            let gy: Vec<(Arg, Arg)> = y
                .read()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            gx.len() == gy.len()
                && gx.iter().all(|(k, v)| {
                    gy.iter()
                        .find(|(tk, _)| tk == k)
                        .is_some_and(|(_, tv)| deep_eq(v, tv))
                })
        }
        _ => a == b,
    }
}

/// `isinstance(x, DotMap)`.
pub fn is_dotmap(x: &Arg) -> bool {
    DotMap::isinstance(x)
}

impl Default for DotMap {
    /// `DotMap()` with default options.
    fn default() -> DotMap {
        Self::with_options(true, false)
    }
}

impl PartialEq for DotMap {
    fn eq(&self, other: &DotMap) -> bool {
        self.eq_dotmap(other)
    }
}

impl core::fmt::Debug for DotMap {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__str__())
    }
}

impl PyObject for DotMap {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    /// `a is b`: identity of the shared handle, not of the boxing.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<DotMap>()
            .is_some_and(|o| self.is(o))
    }
    fn type_name(&self) -> &'static str {
        "DotMap"
    }
    /// `class DotMap(OrderedDict)`: `isinstance(dm, dict)` holds.
    fn __bases__(&self) -> &'static [&'static str] {
        &["OrderedDict", "dict"]
    }
    fn __repr__(&self) -> String {
        DotMap::__repr__(self)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<DotMap>()
            .is_some_and(|o| self.eq_dotmap(o))
    }
    fn __hash__(&self) -> Result<i64> {
        DotMap::__hash__(self)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        DotMap::__getattr__(self, name)
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        DotMap::__setattr__(self, name, value)
    }
    /// `d.get(k)`, `d.items()`, ... on a `DotMap` reached through an `Arg`:
    /// the dict-API bound methods; any other name is `d.<name>(..)` on a
    /// *value* stored under that key (a callable attribute).
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        if let Some(f) = self.bound_method(name) {
            return f.__call__(args);
        }
        DotMap::__getattr__(self, name)?.__call__(args)
    }
    fn __dir__(&self) -> Vec<String> {
        self.__iter__().iter().map(|k| k.__str__()).collect()
    }
    // The mapping protocol on a `DotMap` reached through an `Arg`
    // (`k in d`, `len(d)`, `for k in d`, `d[k]`).
    fn __contains__(&self, item: &Arg) -> Result<bool> {
        DotMap::__contains__(self, item.clone())
    }
    fn __len__(&self) -> Result<usize> {
        Ok(DotMap::__len__(self))
    }
    fn __bool__(&self) -> bool {
        DotMap::__bool__(self)
    }
    fn __iter__(&self) -> Result<Vec<Arg>> {
        Ok(DotMap::__iter__(self))
    }
    fn __getitem__(&self, key: &Arg) -> Result<Arg> {
        DotMap::__getitem__(self, key.clone())
    }
    fn __setitem__(&self, key: &Arg, value: Arg) -> Result<()> {
        DotMap::__setitem__(self, key.clone(), value)
    }
    fn __delitem__(&self, key: &Arg) -> Result<()> {
        DotMap::__delitem__(self, key.clone())
    }
}

impl IntoArg for DotMap {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

impl PyArg for DotMap {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}

impl IsInstance for DotMap {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<DotMap>())
    }
}

impl PartialEq<Arg> for DotMap {
    fn eq(&self, other: &Arg) -> bool {
        self.__eq__(other.clone())
    }
}
