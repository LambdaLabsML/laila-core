//! `import collections`: `OrderedDict` (a `dict` subclass that keeps its
//! type through pickling) and the `Counter` constructor.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::python::args::Args;
use crate::python::builtins::IsInstance;
use crate::python::errors::{LailaError, Result};
use crate::python::object::{Arg, IntoArg, PyDict, PyMap, PyObject};
use crate::python::sync::RwLock;

/// `collections.OrderedDict`: an insertion-ordered mapping whose *type*
/// survives a round trip. `Arg::Dict` is already ordered, so the only
/// difference from a plain dict is identity -- `type(x).__name__`,
/// `isinstance(x, OrderedDict)`, the `collections.OrderedDict` global in
/// a pickle, and order-sensitive equality between two `OrderedDict`s
/// (against a plain `dict` the comparison is order-insensitive, as in
/// Python).
#[derive(Clone, Debug)]
pub struct OrderedDict(pub PyDict);

impl OrderedDict {
    /// `OrderedDict()` / `OrderedDict(mapping)` / `OrderedDict(iterable of
    /// pairs)` / `OrderedDict(**kw)`.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let od = OrderedDict::empty();
        if let Some(src) = a.positional.first() {
            od.update_from(src)?;
        }
        for (k, v) in a.keywords {
            od.0.write().insert(Arg::Str(k.into()), v);
        }
        Ok(od.into_arg())
    }
    pub fn empty() -> OrderedDict {
        OrderedDict(Arc::new(RwLock::new(PyMap::default())))
    }
    fn update_from(&self, src: &Arg) -> Result<()> {
        match src {
            Arg::Dict(d) => {
                for (k, v) in d.read().iter() {
                    self.0.write().insert(k.clone(), v.clone());
                }
            }
            Arg::Object(o) if o.as_any().is::<OrderedDict>() => {
                let o = o.as_any().downcast_ref::<OrderedDict>().expect("checked");
                for (k, v) in o.0.read().iter() {
                    self.0.write().insert(k.clone(), v.clone());
                }
            }
            other => {
                for item in other.__iter__()? {
                    let pair = item.__iter__()?;
                    if pair.len() != 2 {
                        return Err(LailaError::value_error(alloc::format!(
                            "dictionary update sequence element has length {}; 2 is required",
                            pair.len()
                        )));
                    }
                    pair[0].__hash__()?;
                    self.0.write().insert(pair[0].clone(), pair[1].clone());
                }
            }
        }
        Ok(())
    }
    /// The entries as a plain `dict` value sharing nothing.
    pub fn as_dict(&self) -> Arg {
        Arg::Dict(Arc::new(RwLock::new(self.0.read().clone())))
    }
    /// The `(key, value)` pairs in order.
    pub fn pairs(&self) -> Vec<(Arg, Arg)> {
        self.0
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

impl PyObject for OrderedDict {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "OrderedDict"
    }
    fn __bases__(&self) -> &'static [&'static str] {
        &["dict"]
    }
    fn __repr__(&self) -> String {
        let d = self.0.read();
        if d.is_empty() {
            return String::from("OrderedDict()");
        }
        let items: Vec<String> = d
            .iter()
            .map(|(k, v)| alloc::format!("({}, {})", k.__repr__(), v.__repr__()))
            .collect();
        alloc::format!("OrderedDict([{}])", items.join(", "))
    }
    /// `OrderedDict == OrderedDict` is order-sensitive.
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        match other.as_any().downcast_ref::<OrderedDict>() {
            Some(o) => {
                let (a, b) = (self.0.read(), o.0.read());
                a.len() == b.len()
                    && a.iter()
                        .zip(b.iter())
                        .all(|((ka, va), (kb, vb))| ka == kb && va == vb)
            }
            None => false,
        }
    }
    /// `OrderedDict == dict` is order-insensitive.
    fn __eq_arg__(&self, other: &Arg) -> bool {
        match other {
            Arg::Object(o) => self.__eq__(&**o),
            Arg::Dict(_) => self.as_dict() == *other,
            _ => false,
        }
    }
    fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error("unhashable type: 'OrderedDict'"))
    }
    fn __deepcopy__(&self) -> Option<Arg> {
        let m: PyMap<Arg, Arg> = self
            .0
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.deepcopy()))
            .collect();
        Some(OrderedDict(Arc::new(RwLock::new(m))).into_arg())
    }
    /// The `dict` API: `items`, `keys`, `values`, `get`, `pop`, `update`,
    /// `setdefault`, `copy`, `clear`, `move_to_end`, `popitem`.
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let d = self.as_dict();
        match name {
            "items" => d.items(),
            "keys" => d.keys(args),
            "values" => d.values(),
            "get" => d.get(args),
            "copy" => Ok(OrderedDict(Arc::new(RwLock::new(self.0.read().clone()))).into_arg()),
            "pop" => {
                let key = args.positional.first().cloned().unwrap_or(Arg::None);
                let mut g = self.0.write();
                match g.shift_remove(&key) {
                    Some(v) => Ok(v),
                    None => match args.positional.get(1) {
                        Some(default) => Ok(default.clone()),
                        None => Err(LailaError::key_error(key.__repr__())),
                    },
                }
            }
            "popitem" => {
                let last = args
                    .positional
                    .first()
                    .cloned()
                    .unwrap_or_else(|| args.get("last"))
                    .__bool__()
                    || args.positional.is_empty() && matches!(args.get("last"), Arg::None);
                let mut g = self.0.write();
                let key = if last {
                    g.keys().next_back().cloned()
                } else {
                    g.keys().next().cloned()
                };
                let key = key.ok_or_else(|| LailaError::key_error("'dictionary is empty'"))?;
                let v = g.shift_remove(&key).expect("present");
                Ok(Arg::Tuple(alloc::vec![key, v].into()))
            }
            "move_to_end" => {
                let key = args.positional.first().cloned().unwrap_or(Arg::None);
                let last = match args.positional.get(1) {
                    Some(v) => v.__bool__(),
                    None => !matches!(args.get("last"), Arg::Bool(false)),
                };
                let mut g = self.0.write();
                let v = g
                    .shift_remove(&key)
                    .ok_or_else(|| LailaError::key_error(key.__repr__()))?;
                if last {
                    g.insert(key, v);
                } else {
                    let rest: Vec<(Arg, Arg)> = g.drain(..).collect();
                    g.insert(key, v);
                    for (k, v) in rest {
                        g.insert(k, v);
                    }
                }
                Ok(Arg::None)
            }
            "update" => {
                if let Some(src) = args.positional.first() {
                    self.update_from(src)?;
                }
                for (k, v) in args.keywords {
                    self.0.write().insert(Arg::Str(k.into()), v);
                }
                Ok(Arg::None)
            }
            "setdefault" => {
                let key = args.positional.first().cloned().unwrap_or(Arg::None);
                let default = args.positional.get(1).cloned().unwrap_or(Arg::None);
                key.__hash__()?;
                let mut g = self.0.write();
                Ok(g.entry(key).or_insert(default).clone())
            }
            "clear" => {
                self.0.write().clear();
                Ok(Arg::None)
            }
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'OrderedDict' object has no attribute '{name}'"
            ))),
        }
    }
    fn __contains__(&self, item: &Arg) -> Result<bool> {
        item.__hash__()?;
        Ok(self.0.read().contains_key(item))
    }
    fn __len__(&self) -> Result<usize> {
        Ok(self.0.read().len())
    }
    fn __iter__(&self) -> Result<Vec<Arg>> {
        Ok(self.0.read().keys().cloned().collect())
    }
    fn __getitem__(&self, key: &Arg) -> Result<Arg> {
        key.__hash__()?;
        self.0
            .read()
            .get(key)
            .cloned()
            .ok_or_else(|| LailaError::key_error(key.__repr__()))
    }
    fn __setitem__(&self, key: &Arg, value: Arg) -> Result<()> {
        key.__hash__()?;
        self.0.write().insert(key.clone(), value);
        Ok(())
    }
    fn __delitem__(&self, key: &Arg) -> Result<()> {
        key.__hash__()?;
        self.0
            .write()
            .shift_remove(key)
            .map(|_| ())
            .ok_or_else(|| LailaError::key_error(key.__repr__()))
    }
}

impl IntoArg for OrderedDict {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

impl IsInstance for OrderedDict {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<OrderedDict>())
    }
}

/// `import collections` as a value.
#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)]
pub struct collections_mod;
#[allow(non_upper_case_globals)]
pub static collections: collections_mod = collections_mod;
impl collections_mod {
    /// `collections.OrderedDict(..)`
    pub fn OrderedDict(&self, args: impl Into<Args>) -> Result<Arg> {
        OrderedDict::new(args)
    }
    /// `collections.deque(iterable=(), maxlen=None)`: a list (`append`,
    /// `popleft`, `appendleft`, `len`, truthiness) -- `maxlen` is ignored.
    pub fn deque(&self, args: impl Into<Args>) -> Result<Arg> {
        deque(args)
    }
    /// `collections.Counter(..)`
    pub fn Counter(&self, args: impl Into<Args>) -> Result<Arg> {
        super::Counter::new(args)
    }
}

/// `from collections import deque`: `deque(iterable=())` as a list.
pub fn deque(args: impl Into<Args>) -> Result<Arg> {
    let a: Args = args.into();
    let items = match a.positional.first().cloned() {
        None | Some(Arg::None) => alloc::vec::Vec::new(),
        Some(it) => it.__iter__()?,
    };
    Ok(items.into_arg())
}
