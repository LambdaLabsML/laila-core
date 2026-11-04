//! Scope-based build dispatch for serialized entries.
//!
//! Different `Entry` subclasses (the base `Entry`, the `Manifest`, future
//! user-defined subclasses, ...) need different deserialization logic.
//! Hard-coding the dispatch in `Entry.from_dict` would couple the base
//! class to every subclass; instead each subclass registers a *builder
//! pair* `(sync_fn, async_fn)` keyed by its scope string in [`BUILDER_MAP`],
//! and [`build_by_scope`] reads the `_scopes` field of a serialized dict to
//! look up the right builder.
//!
//! This is also what makes the deserialization side of pool reads
//! extensible -- third-party Entry subclasses can be deserialized cleanly as
//! long as they register their own builders.
//!
//! Async in Rust: the async builder returns a coroutine object
//! (`laila::python::stdlib::asyncio::coroutine`, awaitable with `.await`
//! through `IntoFuture`), exactly as Python's `async def` builders return a
//! coroutine -- nothing is registered in the future bank;
//! `build_by_scope(..., asynchronous=True)` hands it back as the `Arg`.
//! Registration of the `ENTRY` /
//! `POOL_INDEX` / `MANIFEST` builders is `crate::entry::__init__` (the
//! import-time side effect of the Entry and Manifest modules), which every
//! accessor of the map runs first.

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::python::sync::RwLock;
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, Result};

/// `sync_fn(in_dict, **kwargs) -> Entry`
pub type SyncBuilder = Arc<dyn Fn(Arg, Args) -> Result<Arg> + Send + Sync>;
/// `async_fn(in_dict, **kwargs) -> Coroutine[Entry]` -- a coroutine object
/// (`asyncio::coroutine(..)`), awaited by the caller. Like Python's
/// `async def` builders it registers nothing in the future bank.
pub type AsyncBuilder = Arc<dyn Fn(Arg, Args) -> Result<Arg> + Send + Sync>;

// Each value is (sync_builder, async_builder).
static BUILDER_MAP_STATE: RwLock<Vec<(String, (SyncBuilder, AsyncBuilder))>> =
    RwLock::new(Vec::new());

/// `list(BUILDER_MAP.keys())` -- the registered scopes, typed.
pub fn builder_scopes() -> Vec<String> {
    crate::entry::__init__();
    BUILDER_MAP_STATE
        .read()
        .iter()
        .map(|(s, _)| s.clone())
        .collect()
}

/// The `BUILDER_MAP` module global as a Python value: a live dict view over
/// the registry (`"ENTRY" in BUILDER_MAP`, `BUILDER_MAP["X"]`,
/// `del BUILDER_MAP["X"]`, `BUILDER_MAP.keys()`, `len(BUILDER_MAP)`).
/// Items are `(sync_fn, async_fn)` tuples of callables, as in Python.
pub fn BUILDER_MAP() -> Arg {
    crate::entry::__init__();
    Arg::Object(Arc::new(BuilderMapView))
}

/// Remove a scope's builder pair (`del BUILDER_MAP[scope]`); `KeyError` if
/// the scope is not registered.
pub fn unregister_builder(scope: &str) -> Result<()> {
    let mut g = BUILDER_MAP_STATE.write();
    match g.iter().position(|(s, _)| s == scope) {
        Some(i) => {
            g.remove(i);
            Ok(())
        }
        None => Err(LailaError::key_error(Arg::Str(scope.into()).__repr__())),
    }
}

/// `BUILDER_MAP` seen through the dynamic layer.
#[derive(Debug)]
struct BuilderMapView;

/// `(in_dict, **kwargs)` <-> `Args`: pop the leading positional.
fn split_in_dict(mut args: Args) -> (Arg, Args) {
    let d = if args.positional.is_empty() {
        Arg::None
    } else {
        args.positional.remove(0)
    };
    (d, args)
}

fn builder_item(pair: &(SyncBuilder, AsyncBuilder)) -> Arg {
    let (s, a) = (pair.0.clone(), pair.1.clone());
    (
        crate::python::callable(move |args: Args| {
            let (d, rest) = split_in_dict(args);
            s(d, rest)
        }),
        crate::python::callable(move |args: Args| {
            let (d, rest) = split_in_dict(args);
            a(d, rest)
        }),
    )
        .into_arg()
}

impl crate::python::PyObject for BuilderMapView {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "dict"
    }
    fn __repr__(&self) -> String {
        let g = BUILDER_MAP_STATE.read();
        let items: Vec<String> = g
            .iter()
            .map(|(s, p)| {
                alloc::format!(
                    "{}: {}",
                    Arg::Str(s.clone().into()).__repr__(),
                    builder_item(p).__repr__()
                )
            })
            .collect();
        alloc::format!("{{{}}}", items.join(", "))
    }
    fn __eq__(&self, other: &dyn crate::python::PyObject) -> bool {
        other.as_any().downcast_ref::<BuilderMapView>().is_some()
    }
    fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error("unhashable type: 'dict'"))
    }
    fn __len__(&self) -> Result<usize> {
        Ok(BUILDER_MAP_STATE.read().len())
    }
    fn __contains__(&self, item: &Arg) -> Result<bool> {
        item.__hash__()?;
        Ok(item
            .as_str()
            .map(|k| BUILDER_MAP_STATE.read().iter().any(|(s, _)| s == k))
            .unwrap_or(false))
    }
    fn __iter__(&self) -> Result<Vec<Arg>> {
        Ok(builder_scopes()
            .into_iter()
            .map(|s| Arg::Str(s.into()))
            .collect())
    }
    fn __getitem__(&self, key: &Arg) -> Result<Arg> {
        key.__hash__()?;
        key.as_str()
            .and_then(builder_pair)
            .map(|p| builder_item(&p))
            .ok_or_else(|| LailaError::key_error(key.__repr__()))
    }
    fn __delitem__(&self, key: &Arg) -> Result<()> {
        key.__hash__()?;
        match key.as_str() {
            Some(k) => unregister_builder(k),
            None => Err(LailaError::key_error(key.__repr__())),
        }
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "keys" => Ok(self.__iter__()?.into_arg()),
            "values" => Ok(BUILDER_MAP_STATE
                .read()
                .iter()
                .map(|(_, p)| builder_item(p))
                .collect::<Vec<_>>()
                .into_arg()),
            "items" => Ok(BUILDER_MAP_STATE
                .read()
                .iter()
                .map(|(s, p)| (Arg::Str(s.clone().into()), builder_item(p)).into_arg())
                .collect::<Vec<_>>()
                .into_arg()),
            "get" => {
                let (key, rest) = split_in_dict(args);
                key.__hash__()?;
                Ok(match key.as_str().and_then(builder_pair) {
                    Some(p) => builder_item(&p),
                    None => rest.positional.first().cloned().unwrap_or(Arg::None),
                })
            }
            "pop" => {
                let (key, rest) = split_in_dict(args);
                let item = self.__getitem__(&key);
                match item {
                    Ok(v) => {
                        self.__delitem__(&key)?;
                        Ok(v)
                    }
                    Err(e) => match rest.positional.first() {
                        Some(d) => Ok(d.clone()),
                        None => Err(e),
                    },
                }
            }
            other => Err(LailaError::attribute_error(alloc::format!(
                "'dict' object has no attribute '{other}'"
            ))),
        }
    }
}

/// `BUILDER_MAP.get(scope)`
pub fn builder_pair(scope: &str) -> Option<(SyncBuilder, AsyncBuilder)> {
    crate::entry::__init__();
    BUILDER_MAP_STATE
        .read()
        .iter()
        .find(|(s, _)| s == scope)
        .map(|(_, p)| p.clone())
}

/// Anything accepted as a builder by [`register_builder`]: a typed
/// [`SyncBuilder`] / [`AsyncBuilder`] (`Arc<dyn Fn(in_dict, **kwargs)>`), or
/// a Python callable value (a `lambda d, **k: ...` as an `Arg`).
pub trait IntoBuilder {
    fn into_builder(self) -> SyncBuilder;
}
impl IntoBuilder for SyncBuilder {
    fn into_builder(self) -> SyncBuilder {
        self
    }
}
impl IntoBuilder for Arg {
    fn into_builder(self) -> SyncBuilder {
        Arc::new(move |d: Arg, mut kwargs: Args| {
            kwargs.positional.insert(0, d);
            self.__call__(kwargs)
        })
    }
}

/// Register a (sync, async) builder pair for the given scope.
///
/// Both builders take the serialized dict and return a hydrated `Entry`
/// (sync) or a coroutine that resolves to one (async). Re-registering the
/// same scope silently overwrites the previous pair.
pub fn register_builder(scope: &str, sync_fn: impl IntoBuilder, async_fn: impl IntoBuilder) {
    let (sync_fn, async_fn) = (sync_fn.into_builder(), async_fn.into_builder());
    let mut g = BUILDER_MAP_STATE.write();
    if let Some(slot) = g.iter_mut().find(|(s, _)| s == scope) {
        slot.1 = (sync_fn, async_fn);
    } else {
        g.push((scope.to_string(), (sync_fn, async_fn)));
    }
}

/// Dispatch hydration based on the `_scopes` field of *in_dict*.
///
/// Accepted input shapes: a `dict` (the standard serialized form; the first
/// scope is looked up, defaulting to `"ENTRY"`); a `str` (JSON-encoded
/// dict, parsed before dispatch); a live `Entry` (returned as-is in the
/// sync path, wrapped in an already-resolved future in the async path).
///
/// With `asynchronous=True` the async builder is called and its coroutine
/// is returned; otherwise the sync builder runs inline and the
/// hydrated entry is returned. `**kwargs` are forwarded to the builder.
///
/// `ValueError` if the dispatched scope has no registered builder;
/// `RuntimeError` if *in_dict* is not a dict, str, or live Entry;
/// `json.JSONDecodeError` if *in_dict* is a string that fails to parse.
#[pysig("build_by_scope(in_dict, *, asynchronous=False, **kwargs)")]
pub fn build_by_scope(args: impl Into<Args>) -> Result<Arg> {
    let asynchronous = asynchronous.__bool__();
    let mut in_dict = in_dict;
    if !matches!(in_dict, Arg::Dict(_)) {
        match &in_dict {
            Arg::Str(s) => in_dict = crate::_compat::pyjson::loads(s)?,
            Arg::Entry(_) => {
                if asynchronous {
                    return Ok(crate::python::stdlib::asyncio::coroutine(async move {
                        Ok(in_dict)
                    }));
                }
                return Ok(in_dict);
            }
            _ => return Err(LailaError::runtime_error("Invalid input for entry build.")),
        }
    }
    if !matches!(in_dict, Arg::Dict(_)) {
        // ``in_dict.get(...)`` on the parsed JSON value.
        return Err(LailaError::attribute_error(alloc::format!(
            "'{}' object has no attribute 'get'",
            in_dict.type_name()
        )));
    }

    let scopes = if in_dict.__contains__("_scopes")? {
        in_dict.__getitem__("_scopes")?
    } else {
        alloc::vec![Arg::Str("ENTRY".into())].into_arg()
    };
    let scope = if scopes.__bool__() {
        scopes.__getitem__(0)?
    } else {
        Arg::Str("ENTRY".into())
    };

    // ``BUILDER_MAP.get(scope)`` -- an unhashable scope raises TypeError.
    scope.__hash__()?;
    let pair = scope.as_str().and_then(builder_pair);
    let Some((sync_fn, async_fn)) = pair else {
        return Err(LailaError::value_error(alloc::format!(
            "No builder registered for scope '{}'. Registered scopes: {}",
            scope.__str__(),
            builder_scopes()
                .into_iter()
                .map(|s| Arg::Str(s.into()))
                .collect::<Vec<_>>()
                .into_arg()
                .__repr__()
        )));
    };

    if asynchronous {
        async_fn(in_dict, kwargs)
    } else {
        sync_fn(in_dict, kwargs)
    }
}
