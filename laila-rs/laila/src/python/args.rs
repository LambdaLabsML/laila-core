//! `Args`: Python's `(*args, **kwargs)` as a value, and Python's argument
//! binding algorithm with Python's `TypeError` texts.

use alloc::string::String;
use alloc::vec::Vec;

use super::errors::{LailaError, Result};
use super::object::{Arg, IntoArg};

/// An ordered bag of positional and keyword arguments.
#[derive(Clone, Default, Debug)]
pub struct Args {
    pub positional: Vec<Arg>,
    pub keywords: Vec<(String, Arg)>,
}

impl Args {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn push_positional(&mut self, v: impl IntoArg) {
        self.positional.push(v.into_arg());
    }
    pub fn push_keyword(&mut self, name: &str, v: impl IntoArg) {
        let name = name.strip_prefix("r#").unwrap_or(name);
        self.keywords.push((name.into(), v.into_arg()));
    }
    /// `*iterable` in a call.
    pub fn extend_positional(&mut self, v: impl IntoArg) -> Result<()> {
        for item in v.into_arg().__iter__()? {
            self.positional.push(item);
        }
        Ok(())
    }
    /// `**mapping` in a call.
    pub fn extend_keywords(&mut self, v: impl IntoArgs) -> Result<()> {
        let other = v.into_args()?;
        for (k, v) in other.keywords {
            self.keywords.push((k, v));
        }
        Ok(())
    }
    /// `kwargs[name] = value` -- set (or replace) a keyword.
    pub fn __setitem__(&mut self, name: impl IntoArg, value: impl IntoArg) -> Result<()> {
        let name = name.into_arg();
        let name = name
            .as_str()
            .ok_or_else(|| LailaError::type_error("keywords must be strings"))?;
        let value = value.into_arg();
        if let Some(slot) = self.keywords.iter_mut().find(|(k, _)| &**k == name) {
            slot.1 = value;
        } else {
            self.keywords.push((name.into(), value));
        }
        Ok(())
    }
    /// `kwargs[name]` -- `KeyError` when absent.
    pub fn __getitem__(&self, name: impl IntoArg) -> Result<Arg> {
        let name = name.into_arg();
        let key = name
            .as_str()
            .ok_or_else(|| LailaError::type_error("keywords must be strings"))?;
        self.keywords
            .iter()
            .find(|(k, _)| &**k == key)
            .map(|(_, v)| v.clone())
            .ok_or_else(|| LailaError::key_error(name.__repr__()))
    }
    /// `name in kwargs`.
    pub fn __contains__(&self, name: impl IntoArg) -> Result<bool> {
        let name = name.into_arg();
        let name = name
            .as_str()
            .ok_or_else(|| LailaError::type_error("keywords must be strings"))?;
        Ok(self.keywords.iter().any(|(k, _)| k == name))
    }
    /// `kwargs.get(name)` -> `None` when absent.
    pub fn get(&self, key: impl ArgsGetKey) -> Arg {
        let (name, default) = key.into_key_default();
        self.keywords
            .iter()
            .find(|(k, _)| **k == *name)
            .map(|(_, v)| v.clone())
            .unwrap_or(default)
    }
    /// `kwargs.pop(name)` (`KeyError` when absent) / `kwargs.pop(name, default)`.
    pub fn pop(&mut self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let mut it = a.positional.into_iter();
        let name = it
            .next()
            .ok_or_else(|| LailaError::type_error("pop expected at least 1 argument, got 0"))?;
        let default = it.next();
        let key = name
            .as_str()
            .ok_or_else(|| LailaError::type_error("keywords must be strings"))?;
        match self.keywords.iter().position(|(k, _)| k == key) {
            Some(i) => Ok(self.keywords.remove(i).1),
            None => default.ok_or_else(|| LailaError::key_error(name.__repr__())),
        }
    }
    /// `kwargs.setdefault(name, default=None)`.
    pub fn setdefault(&mut self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let mut it = a.positional.into_iter();
        let name = it.next().ok_or_else(|| {
            LailaError::type_error("setdefault expected at least 1 argument, got 0")
        })?;
        let default = it.next().unwrap_or(Arg::None);
        let key = name
            .as_str()
            .ok_or_else(|| LailaError::type_error("keywords must be strings"))?;
        if let Some((_, v)) = self.keywords.iter().find(|(k, _)| k == key) {
            return Ok(v.clone());
        }
        self.keywords.push((key.into(), default.clone()));
        Ok(default)
    }
    /// `kwargs.pop(name, default)`.
    pub fn pop_or(&mut self, name: &str, default: impl IntoArg) -> Arg {
        match self.keywords.iter().position(|(k, _)| k == name) {
            Some(i) => self.keywords.remove(i).1,
            None => default.into_arg(),
        }
    }
    pub fn __len__(&self) -> usize {
        self.positional.len() + self.keywords.len()
    }
    pub fn __bool__(&self) -> bool {
        self.__len__() > 0
    }
    /// The keyword part as a Python dict `Arg`.
    /// `kwargs.items()` -- the keyword arguments as `(name, value)` pairs.
    pub fn items(&self) -> Result<Arg> {
        self.as_dict().items()
    }
    /// `kwargs.keys()`
    pub fn keys(&self) -> Result<Arg> {
        self.as_dict().keys(())
    }
    /// `kwargs.values()`
    pub fn values(&self) -> Result<Arg> {
        self.as_dict().values()
    }
    pub fn as_dict(&self) -> Arg {
        let mut m = super::object::PyMap::default();
        for (k, v) in &self.keywords {
            m.insert(Arg::Str(k.as_str().into()), v.clone());
        }
        Arg::Dict(alloc::sync::Arc::new(super::sync::RwLock::new(m)))
    }
}

/// A `**kwargs` bag read as a value (`defaults.update(kw)`) is its dict.
impl super::PyArg for Args {
    fn to_arg(&self) -> Arg {
        self.as_dict()
    }
}

/// Anything accepted where Python accepts a call's arguments.
pub trait IntoArgs {
    fn into_args(self) -> Result<Args>;
}
impl IntoArgs for Args {
    fn into_args(self) -> Result<Args> {
        Ok(self)
    }
}
impl IntoArgs for &Args {
    fn into_args(self) -> Result<Args> {
        Ok(self.clone())
    }
}
impl IntoArgs for Arg {
    fn into_args(self) -> Result<Args> {
        match self {
            Arg::Dict(d) => {
                let mut a = Args::new();
                for (k, v) in d.read().iter() {
                    let k = k
                        .as_str()
                        .ok_or_else(|| LailaError::type_error("keywords must be strings"))?;
                    a.keywords.push((k.into(), v.clone()));
                }
                Ok(a)
            }
            other => Err(LailaError::type_error(alloc::format!(
                "argument after ** must be a mapping, not {}",
                other.type_name()
            ))),
        }
    }
}

/// A single value is one positional argument: `laila.remember(gid)`.
impl<T: IntoArg> From<T> for Args {
    fn from(v: T) -> Args {
        Args {
            positional: alloc::vec![v.into_arg()],
            keywords: Vec::new(),
        }
    }
}

// ---- Python's binding algorithm ----

/// Kind of a Python parameter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParamKind {
    PositionalOrKeyword,
    VarPositional,
    KeywordOnly,
    VarKeyword,
}

/// One parameter of a Python signature.
#[derive(Clone, Copy, Debug)]
pub struct Param {
    pub name: &'static str,
    pub kind: ParamKind,
    pub has_default: bool,
    /// The default value (`inspect.Parameter.default`) when the signature
    /// text carries one; `None` for `has_default` parameters whose default
    /// is only known to the function body.
    pub default: Option<fn() -> Arg>,
}

/// A Python function signature (`#[pysig]` generates one per function).
#[derive(Clone, Copy, Debug)]
pub struct Signature {
    pub name: &'static str,
    pub params: &'static [Param],
}

/// The result of binding `Args` to a `Signature`: one slot per parameter
/// (`None` when a default applies), the `*args` tail, the `**kwargs` rest.
#[derive(Debug, Default)]
pub struct Bound {
    pub values: Vec<Option<Arg>>,
    pub varargs: Vec<Arg>,
    pub kwargs: Args,
}

impl Bound {
    /// Take the bound value of parameter `i`, or evaluate its default.
    pub fn take(&mut self, i: usize, default: impl FnOnce() -> Arg) -> Arg {
        self.values[i].take().unwrap_or_else(default)
    }
    /// `BoundArguments.apply_defaults()`: fill every unbound parameter
    /// whose signature carries a default value.
    pub fn apply_defaults(&mut self, sig: &Signature) {
        for (i, p) in sig.params.iter().enumerate() {
            if self.values[i].is_none() {
                if let Some(d) = p.default {
                    self.values[i] = Some(d());
                }
            }
        }
    }
    /// `BoundArguments.arguments[name]` (bound or defaulted).
    pub fn get(&self, sig: &Signature, name: &str) -> Option<&Arg> {
        sig.params
            .iter()
            .position(|p| p.name == name)
            .and_then(|i| self.values[i].as_ref())
    }
    /// `BoundArguments.arguments[name] = value`.
    pub fn set(&mut self, sig: &Signature, name: &str, value: Arg) -> bool {
        match sig.params.iter().position(|p| p.name == name) {
            Some(i) => {
                self.values[i] = Some(value);
                true
            }
            None => false,
        }
    }
    /// `fn(*bound.args, **bound.kwargs)`: the bound values back as call
    /// arguments (positional-or-keyword parameters positionally up to the
    /// first unbound one, then by keyword; `*args` / keyword-only /
    /// `**kwargs` as in Python).
    pub fn into_args(self, sig: &Signature) -> Args {
        let mut out = Args::new();
        let mut positional_open = true;
        for (i, p) in sig.params.iter().enumerate() {
            match p.kind {
                ParamKind::PositionalOrKeyword => match &self.values[i] {
                    Some(v) if positional_open => out.push_positional(v.clone()),
                    Some(v) => out.push_keyword(p.name, v.clone()),
                    None => positional_open = false,
                },
                ParamKind::VarPositional => {
                    for v in &self.varargs {
                        out.push_positional(v.clone());
                    }
                }
                ParamKind::KeywordOnly => {
                    if let Some(v) = &self.values[i] {
                        out.push_keyword(p.name, v.clone());
                    }
                }
                ParamKind::VarKeyword => {
                    for (k, v) in &self.kwargs.keywords {
                        out.push_keyword(k.as_str(), v.clone());
                    }
                }
            }
        }
        out
    }
}

fn join_names(names: &[&str]) -> String {
    let q: Vec<String> = names.iter().map(|n| alloc::format!("'{n}'")).collect();
    match q.len() {
        0 => String::new(),
        1 => q[0].clone(),
        2 => alloc::format!("{} and {}", q[0], q[1]),
        n => alloc::format!("{}, and {}", q[..n - 1].join(", "), q[n - 1]),
    }
}

impl Signature {
    /// Bind call arguments with CPython's rules and error texts.
    pub fn bind(&self, args: Args) -> Result<Bound> {
        let fname = self.name;
        let mut values: Vec<Option<Arg>> = alloc::vec![None; self.params.len()];
        let mut varargs = Vec::new();
        let mut kwargs = Args::new();
        let has_varargs = self
            .params
            .iter()
            .any(|p| p.kind == ParamKind::VarPositional);
        let has_varkw = self.params.iter().any(|p| p.kind == ParamKind::VarKeyword);
        let positional_slots: Vec<usize> = self
            .params
            .iter()
            .enumerate()
            .filter(|(_, p)| p.kind == ParamKind::PositionalOrKeyword)
            .map(|(i, _)| i)
            .collect();

        // Positionals.
        let mut pos = args.positional.into_iter();
        for &slot in &positional_slots {
            match pos.next() {
                Some(v) => values[slot] = Some(v),
                None => break,
            }
        }
        let extra: Vec<Arg> = pos.collect();
        if !extra.is_empty() {
            if has_varargs {
                varargs = extra;
            } else {
                let given = positional_slots.len() + extra.len();
                let required = positional_slots
                    .iter()
                    .filter(|&&i| !self.params[i].has_default)
                    .count();
                let takes = if required == positional_slots.len() {
                    alloc::format!("{}", positional_slots.len())
                } else {
                    alloc::format!("from {} to {}", required, positional_slots.len())
                };
                let plural = if positional_slots.len() == 1 {
                    "argument"
                } else {
                    "arguments"
                };
                let were = if given == 1 { "was" } else { "were" };
                return Err(LailaError::type_error(alloc::format!(
                    "{fname}() takes {takes} positional {plural} but {given} {were} given"
                )));
            }
        }

        // Keywords.
        for (name, v) in args.keywords {
            match self.params.iter().position(|p| {
                p.name == name
                    && matches!(
                        p.kind,
                        ParamKind::PositionalOrKeyword | ParamKind::KeywordOnly
                    )
            }) {
                Some(i) => {
                    if values[i].is_some() {
                        return Err(LailaError::type_error(alloc::format!(
                            "{fname}() got multiple values for argument '{name}'"
                        )));
                    }
                    values[i] = Some(v);
                }
                None => {
                    if has_varkw {
                        kwargs.keywords.push((name, v));
                    } else {
                        return Err(LailaError::type_error(alloc::format!(
                            "{fname}() got an unexpected keyword argument '{name}'"
                        )));
                    }
                }
            }
        }

        // Missing.
        let missing_pos: Vec<&str> = self
            .params
            .iter()
            .enumerate()
            .filter(|(i, p)| {
                p.kind == ParamKind::PositionalOrKeyword && !p.has_default && values[*i].is_none()
            })
            .map(|(_, p)| p.name)
            .collect();
        if !missing_pos.is_empty() {
            let n = missing_pos.len();
            let plural = if n == 1 { "argument" } else { "arguments" };
            return Err(LailaError::type_error(alloc::format!(
                "{fname}() missing {n} required positional {plural}: {}",
                join_names(&missing_pos)
            )));
        }
        let missing_kw: Vec<&str> = self
            .params
            .iter()
            .enumerate()
            .filter(|(i, p)| {
                p.kind == ParamKind::KeywordOnly && !p.has_default && values[*i].is_none()
            })
            .map(|(_, p)| p.name)
            .collect();
        if !missing_kw.is_empty() {
            let n = missing_kw.len();
            let plural = if n == 1 { "argument" } else { "arguments" };
            return Err(LailaError::type_error(alloc::format!(
                "{fname}() missing {n} required keyword-only {plural}: {}",
                join_names(&missing_kw)
            )));
        }

        Ok(Bound {
            values,
            varargs,
            kwargs,
        })
    }
}

/// The key of `kwargs.get(name)` / `kwargs.get(name, default)`: a name, or
/// the Python-shaped call's argument bag (`laila::python!` passes one when
/// `kwargs` is a `**kwargs` bag).
pub trait ArgsGetKey {
    fn into_key_default(self) -> (alloc::string::String, Arg);
}
impl ArgsGetKey for &str {
    fn into_key_default(self) -> (alloc::string::String, Arg) {
        (alloc::string::String::from(self), Arg::None)
    }
}
impl ArgsGetKey for &alloc::string::String {
    fn into_key_default(self) -> (alloc::string::String, Arg) {
        (self.clone(), Arg::None)
    }
}
impl ArgsGetKey for alloc::string::String {
    fn into_key_default(self) -> (alloc::string::String, Arg) {
        (self, Arg::None)
    }
}
impl ArgsGetKey for Args {
    fn into_key_default(self) -> (alloc::string::String, Arg) {
        let name = self
            .positional
            .first()
            .map(|k| k.__str__())
            .unwrap_or_default();
        let default = self.positional.get(1).cloned().unwrap_or(Arg::None);
        (name, default)
    }
}
