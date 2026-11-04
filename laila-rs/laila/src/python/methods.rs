//! Python `list` / `dict` / `str` methods on a dynamic `Arg`
//! (`x.append(v)`, `d.get(k)`, `d.items()`, `s.startswith(..)`), raising
//! `AttributeError` with CPython's text when the receiver has no such
//! method. `PyStrExt` covers the same string methods on `&str`/`String`.

use super::object::PyMap;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use super::args::Args;
use super::errors::{LailaError, Result};
use super::object::{Arg, IntoArg, PyDict, Slice};
use super::stdlib::PyStrExt;
use super::sync::RwLock;

fn one(v: Arg) -> Args {
    let mut a = Args::new();
    a.push_positional(v);
    a
}
fn two(v: Arg, w: Arg) -> Args {
    let mut a = one(v);
    a.push_positional(w);
    a
}

fn no_attr(a: &Arg, name: &str) -> LailaError {
    LailaError::attribute_error(alloc::format!(
        "'{}' object has no attribute '{name}'",
        a.type_name()
    ))
}

/// Builds a `dict` `Arg` in insertion order.
#[derive(Default)]
pub struct PyDictBuilder(PyMap<Arg, Arg>);
impl PyDictBuilder {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert(&mut self, key: impl IntoArg, value: impl IntoArg) -> &mut Self {
        self.0.insert(key.into_arg(), value.into_arg());
        self
    }
    pub fn finish(self) -> Arg {
        Arg::Dict(Arc::new(RwLock::new(self.0)))
    }
}

/// The method names a builtin container answers through
/// `Arg::__call_method__` (a read of one, `d.update`, is a bound method).
const DICT_METHODS: &[&str] = &[
    "get",
    "keys",
    "values",
    "items",
    "update",
    "setdefault",
    "pop",
    "clear",
    "copy",
];
const LIST_METHODS: &[&str] = &[
    "append", "extend", "insert", "pop", "remove", "clear", "index", "count", "reverse", "sort",
    "copy",
];
const SET_METHODS: &[&str] = &["add", "discard", "remove", "clear", "update", "pop", "copy"];
const STR_METHODS: &[&str] = &[
    "isdigit",
    "isalpha",
    "isalnum",
    "isspace",
    "isupper",
    "islower",
    "startswith",
    "endswith",
    "upper",
    "lower",
    "strip",
    "split",
    "format",
    "join",
    "replace",
    "partition",
    "encode",
];

fn set_only(name: &str, a: &Args, n: usize) -> Result<()> {
    if a.positional.len() != n || !a.keywords.is_empty() {
        return Err(LailaError::type_error(alloc::format!(
            "set.{name}() takes exactly {n} argument{} ({} given)",
            if n == 1 { "" } else { "s" },
            a.positional.len()
        )));
    }
    Ok(())
}

impl Arg {
    /// The method names of a builtin container (`dict`, `list`, `set`,
    /// `str`); empty for other values.
    pub fn builtin_method_names(&self) -> &'static [&'static str] {
        match self {
            Arg::Dict(_) => DICT_METHODS,
            Arg::List(_) => LIST_METHODS,
            Arg::Set(_) => SET_METHODS,
            Arg::Str(_) => STR_METHODS,
            _ => &[],
        }
    }

    /// `x.name(..)` on a builtin container; `None` when `x` is not one or
    /// has no such method.
    pub(crate) fn builtin_call_method(&self, name: &str, a: Args) -> Option<Result<Arg>> {
        if !self.builtin_method_names().contains(&name) {
            return None;
        }
        fn first(a: &Args, what: &str) -> Result<Arg> {
            a.positional.first().cloned().ok_or_else(|| {
                LailaError::type_error(alloc::format!(
                    "{what}() takes exactly one argument (0 given)"
                ))
            })
        }
        let r: Result<Arg> = match (self, name) {
            (Arg::Set(s), "add") => set_only(name, &a, 1).map(|_| {
                s.write().insert(a.positional[0].clone());
                Arg::None
            }),
            (Arg::Set(s), "discard") => set_only(name, &a, 1).map(|_| {
                s.write().shift_remove(&a.positional[0]);
                Arg::None
            }),
            (Arg::Set(s), "remove") => set_only(name, &a, 1).and_then(|_| {
                if s.write().shift_remove(&a.positional[0]) {
                    Ok(Arg::None)
                } else {
                    Err(LailaError::new(
                        super::errors::ExcKind::KeyError,
                        a.positional[0].__repr__(),
                    ))
                }
            }),
            (Arg::Set(s), "clear") => set_only(name, &a, 0).map(|_| {
                s.write().clear();
                Arg::None
            }),
            (Arg::Set(s), "update") => (|| {
                for it in a.positional.iter() {
                    for v in it.__iter__()? {
                        s.write().insert(v);
                    }
                }
                Ok(Arg::None)
            })(),
            (Arg::Set(s), "pop") => set_only(name, &a, 0).and_then(|_| {
                s.write().pop().ok_or_else(|| {
                    LailaError::new(super::errors::ExcKind::KeyError, "pop from an empty set")
                })
            }),
            (Arg::Set(s), "copy") => Ok(Arg::Set(Arc::new(RwLock::new(s.read().clone())))),
            (_, "copy") => self.copy(),
            (_, "append") => first(&a, name)
                .and_then(|v| self.append(v))
                .map(|_| Arg::None),
            (_, "extend") => self.extend(a).map(|_| Arg::None),
            (_, "insert") => {
                if a.positional.len() != 2 {
                    Err(LailaError::type_error(alloc::format!(
                        "insert expected 2 arguments, got {}",
                        a.positional.len()
                    )))
                } else {
                    self.insert(a.positional[0].clone(), a.positional[1].clone())
                        .map(|_| Arg::None)
                }
            }
            (_, "pop") => self.pop(a),
            (_, "remove") => first(&a, name)
                .and_then(|v| self.remove(v))
                .map(|_| Arg::None),
            (_, "clear") => self.clear(a).map(|_| Arg::None),
            (_, "index") => self.index(a),
            (_, "count") => first(&a, name).and_then(|v| self.count(v)),
            (_, "reverse") => self.reverse().map(|_| Arg::None),
            (_, "sort") => self.sort(a),
            (_, "get") => self.get(a),
            (_, "keys") => self.keys(a),
            (_, "values") => self.values(),
            (_, "items") => self.items(),
            (_, "update") => self.update(a).map(|_| Arg::None),
            (_, "setdefault") => self.setdefault(a),
            (Arg::Str(_), _) => self.str_call_method(name, a),
            _ => return None,
        };
        Some(r)
    }

    fn str_call_method(&self, name: &str, a: Args) -> Result<Arg> {
        fn first(a: &Args, what: &str) -> Result<Arg> {
            a.positional.first().cloned().ok_or_else(|| {
                LailaError::type_error(alloc::format!(
                    "{what}() takes at least 1 argument (0 given)"
                ))
            })
        }
        match name {
            "startswith" => first(&a, name)
                .and_then(|v| self.startswith(v))
                .map(Arg::Bool),
            "endswith" => first(&a, name)
                .and_then(|v| self.endswith(v))
                .map(Arg::Bool),
            "upper" => self.upper(),
            "lower" => self.lower(),
            "isdigit" => self.isdigit().map(Arg::Bool),
            "isalpha" => self.isalpha().map(Arg::Bool),
            "isalnum" => self.isalnum().map(Arg::Bool),
            "isspace" => self.isspace().map(Arg::Bool),
            "isupper" => self.isupper().map(Arg::Bool),
            "islower" => self.islower().map(Arg::Bool),
            "strip" => self.strip(),
            "split" => self.split(a),
            "format" => self.format(a),
            "join" => self.join(a),
            "replace" => {
                if a.positional.len() < 2 {
                    return Err(LailaError::type_error(
                        "replace expected at least 2 arguments",
                    ));
                }
                self.replace(a.positional[0].clone(), a.positional[1].clone())
            }
            "partition" => first(&a, name).and_then(|v| self.partition(v)),
            "encode" => encode_with(self, a),
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'str' object has no attribute '{name}'"
            ))),
        }
    }

    /// `copy.deepcopy(x)`: containers are copied recursively; scalars,
    /// strings, bytes and handles are shared (immutable or identity-bearing).
    pub fn deepcopy(&self) -> Arg {
        match self {
            Arg::List(l) => Arg::List(Arc::new(RwLock::new(
                l.read().iter().map(Arg::deepcopy).collect(),
            ))),
            Arg::Tuple(t) => Arg::Tuple(t.iter().map(Arg::deepcopy).collect::<Vec<_>>().into()),
            Arg::Dict(d) => {
                let m: PyMap<Arg, Arg> = d
                    .read()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.deepcopy()))
                    .collect();
                Arg::Dict(Arc::new(RwLock::new(m)))
            }
            Arg::Object(o) => o.__deepcopy__().unwrap_or_else(|| self.clone()),
            other => other.clone(),
        }
    }

    /// `x.copy()` (shallow)
    pub fn copy(&self) -> Result<Arg> {
        match self {
            Arg::List(l) => Ok(Arg::List(Arc::new(RwLock::new(l.read().clone())))),
            Arg::Dict(d) => Ok(Arg::Dict(Arc::new(RwLock::new(d.read().clone())))),
            other => other.__call_method__("copy", Args::new()),
        }
    }

    // ---- list ---------------------------------------------------------------

    pub fn append(&self, value: impl IntoArg) -> Result<()> {
        let value = value.into_arg();
        match self {
            Arg::List(l) => {
                l.write().push(value);
                Ok(())
            }
            other => other.__call_method__("append", one(value)).map(|_| ()),
        }
    }
    /// `list.extend(iterable)`; on a dynamic object, `obj.extend(..)`.
    pub fn extend(&self, args: impl Into<Args>) -> Result<()> {
        let a: Args = args.into();
        match self {
            Arg::List(l) => {
                if a.positional.len() != 1 || !a.keywords.is_empty() {
                    return Err(LailaError::type_error(alloc::format!(
                        "list.extend() takes exactly one argument ({} given)",
                        a.__len__()
                    )));
                }
                let items = a.positional[0].__iter__()?;
                l.write().extend(items);
                Ok(())
            }
            other => other.__call_method__("extend", a).map(|_| ()),
        }
    }
    pub fn insert(&self, index: impl IntoArg, value: impl IntoArg) -> Result<()> {
        let (index, value) = (index.into_arg(), value.into_arg());
        match self {
            Arg::List(l) => {
                let mut g = l.write();
                let n = g.len() as i64;
                let mut i = index.as_i64().ok_or_else(|| {
                    LailaError::type_error("'str' object cannot be interpreted as an integer")
                })?;
                if i < 0 {
                    i = (i + n).max(0);
                }
                let i = (i.min(n)) as usize;
                g.insert(i, value);
                Ok(())
            }
            other => other
                .__call_method__("insert", two(index, value))
                .map(|_| ()),
        }
    }
    /// `deque.popleft()` (a deque is a list here).
    pub fn popleft(&self) -> Result<Arg> {
        match self {
            Arg::List(l) => {
                let mut g = l.write();
                if g.is_empty() {
                    return Err(LailaError::new(
                        super::errors::ExcKind::IndexError,
                        "pop from an empty deque",
                    ));
                }
                Ok(g.remove(0))
            }
            other => other.__call_method__("popleft", Args::new()),
        }
    }
    /// `deque.appendleft(x)` (a deque is a list here).
    pub fn appendleft(&self, value: impl IntoArg) -> Result<()> {
        let value = value.into_arg();
        match self {
            Arg::List(l) => {
                l.write().insert(0, value);
                Ok(())
            }
            other => other.__call_method__("appendleft", one(value)).map(|_| ()),
        }
    }
    /// `list.pop([i])` / `dict.pop(key[, default])`
    pub fn pop(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        match self {
            Arg::List(l) => {
                let mut g = l.write();
                if g.is_empty() {
                    return Err(LailaError::new(
                        super::errors::ExcKind::IndexError,
                        "pop from empty list",
                    ));
                }
                let n = g.len() as i64;
                let i = match a.positional.first() {
                    None => n - 1,
                    Some(x) => {
                        let i = x
                            .as_i64()
                            .ok_or_else(|| LailaError::type_error("integer argument expected"))?;
                        if i < 0 {
                            i + n
                        } else {
                            i
                        }
                    }
                };
                if i < 0 || i >= n {
                    return Err(LailaError::new(
                        super::errors::ExcKind::IndexError,
                        "pop index out of range",
                    ));
                }
                Ok(g.remove(i as usize))
            }
            Arg::Dict(d) => {
                let key = a.positional.first().cloned().ok_or_else(|| {
                    LailaError::type_error("pop expected at least 1 argument, got 0")
                })?;
                let mut g = d.write();
                match g.shift_remove(&key) {
                    Some(v) => Ok(v),
                    None => match a.positional.get(1) {
                        Some(default) => Ok(default.clone()),
                        None => Err(LailaError::key_error(key.__repr__())),
                    },
                }
            }
            other => other.__call_method__("pop", a),
        }
    }
    pub fn remove(&self, value: impl IntoArg) -> Result<()> {
        let value = value.into_arg();
        match self {
            Arg::List(l) => {
                let v = value.clone();
                let mut g = l.write();
                match g.iter().position(|x| *x == v) {
                    Some(i) => {
                        g.remove(i);
                        Ok(())
                    }
                    None => Err(LailaError::value_error("list.remove(x): x not in list")),
                }
            }
            other => other.__call_method__("remove", one(value)).map(|_| ()),
        }
    }
    /// `list.clear()` / `dict.clear()`; on a dynamic object, `obj.clear(..)`
    /// (`threading.Event.clear` and friends take arguments).
    pub fn clear(&self, args: impl Into<Args>) -> Result<()> {
        let a: Args = args.into();
        match self {
            Arg::List(_) | Arg::Dict(_) if a.__len__() != 0 => {
                Err(LailaError::type_error(alloc::format!(
                    "{}.clear() takes no arguments ({} given)",
                    self.type_name(),
                    a.__len__()
                )))
            }
            Arg::List(l) => {
                l.write().clear();
                Ok(())
            }
            Arg::Dict(d) => {
                d.write().clear();
                Ok(())
            }
            other => other.__call_method__("clear", a).map(|_| ()),
        }
    }
    /// `seq.index(value[, start[, stop]])` / `str.index(sub[, start[, end]])`;
    /// on a dynamic object, `obj.index(..)`.
    pub fn index(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        if !matches!(self, Arg::List(_) | Arg::Tuple(_) | Arg::Str(_)) {
            return self.__call_method__("index", a);
        }
        let v =
            a.positional.first().cloned().ok_or_else(|| {
                LailaError::type_error("index expected at least 1 argument, got 0")
            })?;
        let int_at = |i: usize| -> Result<Option<i64>> {
            match a.positional.get(i) {
                None | Some(Arg::None) => Ok(None),
                Some(x) => x.as_i64().map(Some).ok_or_else(|| {
                    LailaError::type_error(
                        "slice indices must be integers or have an __index__ method",
                    )
                }),
            }
        };
        let (start, stop) = (int_at(1)?, int_at(2)?);
        let window = |n: usize| -> (usize, usize) {
            let s = Slice {
                start,
                stop,
                step: None,
            };
            let (lo, hi, _) = s.indices(n);
            (lo.max(0) as usize, hi.max(0) as usize)
        };
        match self {
            Arg::List(l) => {
                let g = l.read();
                let (lo, hi) = window(g.len());
                g.iter()
                    .enumerate()
                    .skip(lo)
                    .take(hi.saturating_sub(lo))
                    .find(|(_, x)| **x == v)
                    .map(|(i, _)| Arg::Int(i as i64))
                    .ok_or_else(|| {
                        LailaError::value_error(alloc::format!("{} is not in list", v.__repr__()))
                    })
            }
            Arg::Tuple(t) => {
                let (lo, hi) = window(t.len());
                t.iter()
                    .enumerate()
                    .skip(lo)
                    .take(hi.saturating_sub(lo))
                    .find(|(_, x)| **x == v)
                    .map(|(i, _)| Arg::Int(i as i64))
                    .ok_or_else(|| LailaError::value_error("tuple.index(x): x not in tuple"))
            }
            Arg::Str(s) => {
                let needle = v.__str__();
                let chars: Vec<char> = s.chars().collect();
                let (lo, hi) = window(chars.len());
                let hay: String = chars[lo.min(hi)..hi].iter().collect();
                match str::find(hay.as_str(), needle.as_str()) {
                    Some(i) => Ok(Arg::Int((lo + hay[..i].chars().count()) as i64)),
                    None => Err(LailaError::value_error("substring not found")),
                }
            }
            _ => unreachable!(),
        }
    }
    pub fn count(&self, value: impl IntoArg) -> Result<Arg> {
        let v = value.into_arg();
        match self {
            Arg::List(l) => Ok(Arg::Int(l.read().iter().filter(|x| **x == v).count() as i64)),
            Arg::Tuple(t) => Ok(Arg::Int(t.iter().filter(|x| **x == v).count() as i64)),
            Arg::Str(s) => Ok(Arg::Int(s.matches(v.__str__().as_str()).count() as i64)),
            other => other.__call_method__("count", one(v)),
        }
    }
    pub fn reverse(&self) -> Result<()> {
        match self {
            Arg::List(l) => {
                l.write().reverse();
                Ok(())
            }
            other => other.__call_method__("reverse", Args::new()).map(|_| ()),
        }
    }
    /// `list.sort()` (natural Python order; incomparable elements raise
    /// `TypeError`); with arguments, or on any other object, the dynamic
    /// `sort(..)` method (a remote `sort(lst)` installed on a peer).
    pub fn sort(&self, args: impl Into<Args>) -> Result<Arg> {
        let args: Args = args.into();
        if !(args.positional.is_empty() && args.keywords.is_empty()) {
            return self.__call_method__("sort", args);
        }
        self.sort_in_place().map(|_| Arg::None)
    }
    /// `list.sort()` proper.
    pub fn sort_in_place(&self) -> Result<()> {
        match self {
            Arg::List(l) => {
                let mut g = l.write();
                let mut err = None;
                g.sort_by(|a, b| match a.py_cmp(b) {
                    Some(o) => o,
                    None => {
                        err.get_or_insert_with(|| {
                            LailaError::type_error(alloc::format!(
                                "'<' not supported between instances of '{}' and '{}'",
                                a.type_name(),
                                b.type_name()
                            ))
                        });
                        core::cmp::Ordering::Equal
                    }
                });
                match err {
                    Some(e) => Err(e),
                    None => Ok(()),
                }
            }
            other => other.__call_method__("sort", Args::new()).map(|_| ()),
        }
    }

    // ---- dict ---------------------------------------------------------------

    /// `dict.get(key[, default])`
    pub fn get(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        match self {
            Arg::Dict(d) => {
                let key = a.positional.first().cloned().ok_or_else(|| {
                    LailaError::type_error("get expected at least 1 argument, got 0")
                })?;
                Ok(d.read()
                    .get(&key)
                    .cloned()
                    .unwrap_or_else(|| a.positional.get(1).cloned().unwrap_or(Arg::None)))
            }
            other => other.__call_method__("get", a),
        }
    }
    /// `dict.keys()`; on a dynamic object, `obj.keys(..)` (pools take arguments).
    pub fn keys(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        match self {
            Arg::Dict(d) => {
                if a.__len__() != 0 {
                    return Err(LailaError::type_error(alloc::format!(
                        "dict.keys() takes no arguments ({} given)",
                        a.__len__()
                    )));
                }
                Ok(d.read().keys().cloned().collect::<Vec<_>>().into_arg())
            }
            other => other.__call_method__("keys", a),
        }
    }
    pub fn values(&self) -> Result<Arg> {
        match self {
            Arg::Dict(d) => Ok(d.read().values().cloned().collect::<Vec<_>>().into_arg()),
            other => other.__call_method__("values", Args::new()),
        }
    }
    /// `dict.items()` -> list of `(k, v)` tuples
    pub fn items(&self) -> Result<Arg> {
        match self {
            Arg::Dict(d) => Ok(d
                .read()
                .iter()
                .map(|(k, v)| Arg::Tuple(alloc::vec![k.clone(), v.clone()].into()))
                .collect::<Vec<_>>()
                .into_arg()),
            other => other.__call_method__("items", Args::new()),
        }
    }
    /// `dict.update(other)` / `dict.update(k=v, ..)`
    pub fn update(&self, args: impl Into<Args>) -> Result<()> {
        let a: Args = args.into();
        match self {
            Arg::Dict(d) => {
                let mut g = d.write();
                if let Some(other) = a.positional.first() {
                    match other {
                        Arg::Dict(o) => {
                            if Arc::ptr_eq(d, o) {
                                return Ok(());
                            }
                            for (k, v) in o.read().iter() {
                                g.insert(k.clone(), v.clone());
                            }
                        }
                        seq => {
                            for pair in seq.__iter__()? {
                                let kv = pair.__iter__()?;
                                if kv.len() != 2 {
                                    return Err(LailaError::value_error(
                                        "dictionary update sequence element has wrong length",
                                    ));
                                }
                                g.insert(kv[0].clone(), kv[1].clone());
                            }
                        }
                    }
                }
                for (k, v) in a.keywords {
                    g.insert(Arg::Str(k.into()), v);
                }
                Ok(())
            }
            other => other.__call_method__("update", a).map(|_| ()),
        }
    }
    pub fn setdefault(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        match self {
            Arg::Dict(d) => {
                let key = a.positional.first().cloned().ok_or_else(|| {
                    LailaError::type_error("setdefault expected at least 1 argument, got 0")
                })?;
                let default = a.positional.get(1).cloned().unwrap_or(Arg::None);
                let mut g = d.write();
                Ok(g.entry(key).or_insert(default).clone())
            }
            other => other.__call_method__("setdefault", a),
        }
    }

    // ---- re.Pattern ---------------------------------------------------------

    /// `pattern.match(s)` on an `re.Pattern` value.
    pub fn r#match(&self, string: impl IntoArg) -> Result<Arg> {
        self.__call_method__("match", Args::from(string.into_arg()))
    }
    /// `pattern.search(s)` on an `re.Pattern` value.
    pub fn search(&self, string: impl IntoArg) -> Result<Arg> {
        self.__call_method__("search", Args::from(string.into_arg()))
    }
    /// `pattern.fullmatch(s)` on an `re.Pattern` value.
    pub fn fullmatch(&self, string: impl IntoArg) -> Result<Arg> {
        self.__call_method__("fullmatch", Args::from(string.into_arg()))
    }

    // ---- str ----------------------------------------------------------------

    fn as_str_method(&self, name: &str) -> Result<&str> {
        match self {
            Arg::Str(s) => Ok(s),
            other => Err(no_attr(other, name)),
        }
    }
    pub fn startswith(&self, prefix: impl IntoArg) -> Result<bool> {
        Ok(self
            .as_str_method("startswith")?
            .startswith(prefix.into_arg().__str__()))
    }
    pub fn endswith(&self, suffix: impl IntoArg) -> Result<bool> {
        Ok(self
            .as_str_method("endswith")?
            .endswith(suffix.into_arg().__str__()))
    }
    pub fn upper(&self) -> Result<Arg> {
        Ok(Arg::Str(self.as_str_method("upper")?.to_uppercase().into()))
    }
    /// `str.isdigit()` (ASCII digits; non-empty).
    pub fn isdigit(&self) -> Result<bool> {
        let s = self.as_str_method("isdigit")?;
        Ok(!s.is_empty() && s.chars().all(|c| c.is_ascii_digit()))
    }
    /// `str.isalpha()`
    pub fn isalpha(&self) -> Result<bool> {
        let s = self.as_str_method("isalpha")?;
        Ok(!s.is_empty() && s.chars().all(char::is_alphabetic))
    }
    /// `str.isalnum()`
    pub fn isalnum(&self) -> Result<bool> {
        let s = self.as_str_method("isalnum")?;
        Ok(!s.is_empty() && s.chars().all(char::is_alphanumeric))
    }
    /// `str.isspace()`
    pub fn isspace(&self) -> Result<bool> {
        let s = self.as_str_method("isspace")?;
        Ok(!s.is_empty() && s.chars().all(char::is_whitespace))
    }
    /// `str.isupper()` / `str.islower()`: at least one cased character and
    /// every cased character in that case.
    pub fn isupper(&self) -> Result<bool> {
        let s = self.as_str_method("isupper")?;
        let cased: Vec<char> = s.chars().filter(|c| c.is_alphabetic()).collect();
        Ok(!cased.is_empty() && cased.iter().all(|c| c.is_uppercase()))
    }
    pub fn islower(&self) -> Result<bool> {
        let s = self.as_str_method("islower")?;
        let cased: Vec<char> = s.chars().filter(|c| c.is_alphabetic()).collect();
        Ok(!cased.is_empty() && cased.iter().all(|c| c.is_lowercase()))
    }
    pub fn lower(&self) -> Result<Arg> {
        Ok(Arg::Str(self.as_str_method("lower")?.to_lowercase().into()))
    }
    pub fn strip(&self) -> Result<Arg> {
        Ok(Arg::Str(self.as_str_method("strip")?.trim().into()))
    }
    pub fn split(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let s = self.as_str_method("split")?;
        let parts: Vec<String> = match a.positional.first() {
            None | Some(Arg::None) => s.split_whitespace().map(|x| x.to_string()).collect(),
            Some(sep) => s
                .split(sep.__str__().as_str())
                .map(|x| x.to_string())
                .collect(),
        };
        Ok(parts.into_arg())
    }
    /// `template.format(*args, **kwargs)`; on a dynamic object, `obj.format(..)`.
    pub fn format(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        match self {
            Arg::Str(s) => Ok(Arg::Str(super::strfmt::str_format(s, &a)?.into())),
            other => other.__call_method__("format", a),
        }
    }
    /// `sep.join(iterable)`; on a dynamic object, `obj.join(..)` (e.g. `Thread.join`).
    pub fn join(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        if !matches!(self, Arg::Str(_)) {
            return self.__call_method__("join", a);
        }
        let sep = self.as_str_method("join")?;
        let parts: Vec<String> = a
            .positional
            .first()
            .cloned()
            .unwrap_or(Arg::None)
            .__iter__()?
            .iter()
            .map(|x| x.__str__())
            .collect();
        Ok(Arg::Str(parts.join(sep).into()))
    }
    pub fn replace(&self, old: impl IntoArg, new: impl IntoArg) -> Result<Arg> {
        let s = self.as_str_method("replace")?;
        Ok(Arg::Str(
            s.replace(
                old.into_arg().__str__().as_str(),
                new.into_arg().__str__().as_str(),
            )
            .into(),
        ))
    }
    /// `str.partition(sep)` -> `(head, sep, tail)`.
    pub fn partition(&self, sep: impl IntoArg) -> Result<Arg> {
        let sep = sep.into_arg();
        match self {
            Arg::Str(s) => Ok(crate::python::stdlib::PyStrExt::partition(&**s, sep)),
            other => other.__call_method__("partition", one(sep)),
        }
    }
    pub fn encode(&self) -> Result<Arg> {
        Ok(Arg::Bytes(self.as_str_method("encode")?.as_bytes().into()))
    }
    pub fn decode(&self) -> Result<Arg> {
        match self {
            Arg::Bytes(b) => Ok(Arg::Str(utf8_decode(b)?.into())),
            Arg::ByteArray(b) => Ok(Arg::Str(utf8_decode(&b.read())?.into())),
            other => Err(no_attr(other, "decode")),
        }
    }
    /// `bytes.hex()` / `uuid.hex` (as a method call on a dynamic value).
    pub fn hex(&self) -> Result<Arg> {
        match self {
            Arg::Bytes(b) => Ok(Arg::Str(
                b.iter()
                    .map(|x| alloc::format!("{x:02x}"))
                    .collect::<String>()
                    .into(),
            )),
            Arg::ByteArray(b) => Ok(Arg::Str(
                b.read()
                    .iter()
                    .map(|x| alloc::format!("{x:02x}"))
                    .collect::<String>()
                    .into(),
            )),
            other => other.__getattr__("hex"),
        }
    }
}

/// Strict `bytes.decode("utf-8")` with CPython's `UnicodeDecodeError` text.
/// The codec named by `s.encode(encoding)` / `b.decode(encoding)`: only the
/// UTF-8 / ASCII spellings have a twin here (`LookupError` otherwise, as
/// CPython raises for an unknown codec).
fn text_codec(args: &Args, what: &str) -> Result<bool> {
    let encoding = match args.positional.first() {
        Some(v) => v.clone(),
        None => args.get("encoding"),
    };
    let name = match &encoding {
        Arg::None => "utf-8".into(),
        Arg::Str(s) => str::replace(&str::to_lowercase(s), "_", "-"),
        other => {
            return Err(LailaError::type_error(alloc::format!(
                "{what}() argument 'encoding' must be str, not {}",
                other.type_name()
            )))
        }
    };
    match name.as_str() {
        "utf-8" | "utf8" | "u8" | "utf" | "cp65001" => Ok(false),
        "ascii" | "us-ascii" | "646" => Ok(true),
        _ => Err(LailaError::new(
            super::errors::ExcKind::LookupError,
            alloc::format!("unknown encoding: {name}"),
        )),
    }
}

/// `s.encode(encoding="utf-8", errors="strict")` with an explicit codec.
pub fn encode_with(s: &Arg, args: impl Into<Args>) -> Result<Arg> {
    let args: Args = args.into();
    let ascii = text_codec(&args, "encode")?;
    let text = s.as_str_method("encode")?;
    if ascii {
        if let Some((pos, c)) = text.char_indices().find(|(_, c)| !c.is_ascii()) {
            return Err(LailaError::new(
                super::errors::ExcKind::UnicodeEncodeError,
                alloc::format!(
                    "'ascii' codec can't encode character '\\u{:04x}' in position {pos}: ordinal not in range(128)",
                    c as u32
                ),
            ));
        }
    }
    Ok(Arg::Bytes(text.as_bytes().into()))
}

/// `b.decode(encoding="utf-8", errors="strict")` with an explicit codec.
pub fn decode_with(b: &Arg, args: impl Into<Args>) -> Result<Arg> {
    let args: Args = args.into();
    let ascii = text_codec(&args, "decode")?;
    let bytes: Vec<u8> = match b {
        Arg::Bytes(b) => b.to_vec(),
        Arg::ByteArray(b) => b.read().clone(),
        other => return Err(no_attr(other, "decode")),
    };
    if ascii {
        if let Some(pos) = bytes.iter().position(|x| !x.is_ascii()) {
            return Err(LailaError::new(
                super::errors::ExcKind::UnicodeDecodeError,
                alloc::format!(
                    "'ascii' codec can't decode byte 0x{:02x} in position {pos}: ordinal not in range(128)",
                    bytes[pos]
                ),
            ));
        }
    }
    Ok(Arg::Str(utf8_decode(&bytes)?.into()))
}

pub fn utf8_decode(b: &[u8]) -> Result<String> {
    match core::str::from_utf8(b) {
        Ok(s) => Ok(s.into()),
        Err(e) => {
            let start = e.valid_up_to();
            let msg = match e.error_len() {
                None => alloc::format!(
                    "'utf-8' codec can't decode bytes in position {}-{}: unexpected end of data",
                    start,
                    b.len() - 1
                ),
                Some(1) => {
                    let byte = b[start];
                    let reason = if (0x80..0xC2).contains(&byte) || byte >= 0xF5 {
                        "invalid start byte"
                    } else {
                        "invalid continuation byte"
                    };
                    alloc::format!(
                        "'utf-8' codec can't decode byte {:#04x} in position {}: {}",
                        byte,
                        start,
                        reason
                    )
                }
                Some(n) => alloc::format!(
                    "'utf-8' codec can't decode bytes in position {}-{}: invalid continuation byte",
                    start,
                    start + n - 1
                ),
            };
            Err(LailaError::new(
                super::errors::ExcKind::UnicodeDecodeError,
                msg,
            ))
        }
    }
}

/// `PyDict` helper: build from pairs.
pub fn dict_from_pairs<I: IntoIterator<Item = (Arg, Arg)>>(pairs: I) -> PyDict {
    Arc::new(RwLock::new(pairs.into_iter().collect()))
}
