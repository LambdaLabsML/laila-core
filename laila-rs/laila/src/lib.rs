//! Rust port of the `laila` Python package.
//!
//! The crate is the Python package: same module tree, same names, same
//! defaults. `import laila` is `use laila::prelude::*;`, after which the
//! facade value `laila` has the top-level verbs (`laila.memorize(...)`,
//! `laila.remember(...)`) and live accessors (`laila.memory()`,
//! `laila.alpha_pool()`).
//!
//! Bodies can be written in Python's expression syntax under
//! `#[laila::python]`; see `docs/PORT_RULES.md`.

#![cfg_attr(not(feature = "std"), no_std)]
#![allow(non_upper_case_globals, non_camel_case_types, non_snake_case)]
#![allow(clippy::needless_doctest_main)]

extern crate alloc;
#[cfg(feature = "std")]
extern crate std;

// Lets the proc-macros emit `::laila::...` paths inside this crate too.
extern crate self as laila;

pub mod _codecs;
pub mod _compat;
pub mod atomic;
pub mod basics;
pub mod data;
pub mod entry;
pub mod logger;
pub mod macros;
/// `from laila import macros` as a value.
#[allow(non_upper_case_globals)]
pub static macros: self::macros::macros_mod = self::macros::macros_mod;
pub mod policy;
pub mod python;
pub mod runtime;
pub mod utils;

pub use entry::{Entry, EntryState};
pub use policy::central::memory::schema::Manifest;
/// `laila.manifest = Manifest` -- the class alias (`laila.manifest(data=..)`
/// is `Manifest(data=..)`).
pub fn manifest(args: impl Into<Args>) -> Result<Manifest> {
    Manifest::new(args)
}
pub use python::{Arg, Args, LailaError, Result};
// ---------------------------------------------------------------------------
// ``__LailaArgs``

/// Decide whether assigning *value* to `laila.args.environment` should
/// trigger a full [`_load_environment`] cycle: a plain, non-empty `dict`
/// always does; a `_compat::dotmap::DotMap` only when it carries a non-empty `policies`
/// mapping or an `active_gid`; anything else is inert.
pub fn _is_env_load_trigger(value: &Arg) -> bool {
    if let Some(dm) = _compat::dotmap::DotMap::downcast(value) {
        let policies = dm.get("policies", Arg::None).unwrap_or(Arg::None);
        let active = dm.get("active_gid", Arg::None).unwrap_or(Arg::None);
        if !active.is_none() {
            return true;
        }
        if policies.is_none() {
            return false;
        }
        return policies.__len__().map(|n| n > 0).unwrap_or(false);
    }
    if let Arg::Dict(_) = value {
        return value.__len__().map(|n| n > 0).unwrap_or(false);
    }
    false
}

/// `laila.args`: a `_compat::dotmap::DotMap` subclass that watches for assignments to
/// `environment` (a populated payload triggers [`_load_environment`]) and
/// flushes the lazy policy mirror on every read of `environment`
/// (`laila.args.environment`, `["environment"]`, `.get("environment")`,
/// `items()`, `toDict()`). Plain `dict` values assigned to any other key are
/// converted to `_compat::dotmap::DotMap`s, as dotmap does.
#[allow(non_camel_case_types)]
#[derive(Clone, Default)]
pub struct _LailaArgs(pub _compat::dotmap::DotMap);

impl _LailaArgs {
    /// `__LailaArgs(*args, **kwargs)` -- a `_compat::dotmap::DotMap` constructor.
    pub fn new(args: impl Into<Args>) -> Result<_LailaArgs> {
        Ok(_LailaArgs(_compat::dotmap::DotMap::new(args)?))
    }

    fn _flush_mirror() {
        let _ = basics::definitions::cli_capable::_flush_environment_mirror();
    }

    /// `args[key]`
    pub fn __getitem__(&self, key: impl IntoArg) -> Result<Arg> {
        let key = key.into_arg();
        if key == "environment" {
            Self::_flush_mirror();
        }
        self.0.__getitem__(key)
    }

    /// `args.get(key, default=None)`
    pub fn get(&self, key: impl IntoArg, default: impl IntoArg) -> Result<Arg> {
        let key = key.into_arg();
        if key == "environment" {
            Self::_flush_mirror();
        }
        self.0.get(key, default)
    }

    /// `args.items()`
    pub fn items(&self) -> Arg {
        Self::_flush_mirror();
        self.0.items()
    }

    /// `args.toDict()`
    #[allow(non_snake_case)]
    pub fn toDict(&self) -> Arg {
        Self::_flush_mirror();
        self.0.to_dict()
    }

    /// `args.key = value`
    pub fn __setattr__(&self, key: &str, value: impl IntoArg) -> Result<()> {
        let value = value.into_arg();
        if key == "environment" && _is_env_load_trigger(&value) {
            return basics::definitions::cli_capable::_load_environment(Args::from(value));
        }
        let value = match &value {
            Arg::Dict(_) => _compat::dotmap::DotMap::new(Args::from(value.clone()))?.into_arg(),
            _ => value,
        };
        self.0.__setattr__(key, value)
    }

    /// `args[key] = value`
    pub fn __setitem__(&self, key: impl IntoArg, value: impl IntoArg) -> Result<()> {
        let key = key.into_arg();
        let value = value.into_arg();
        if key == "environment" && _is_env_load_trigger(&value) {
            return basics::definitions::cli_capable::_load_environment(Args::from(value));
        }
        let value = match &value {
            Arg::Dict(_) => _compat::dotmap::DotMap::new(Args::from(value.clone()))?.into_arg(),
            _ => value,
        };
        self.0.__setitem__(key, value)
    }

    /// `args.key` (autovivifying, like `_compat::dotmap::DotMap`; `environment` flushes the mirror first).
    pub fn __getattr__(&self, key: &str) -> Result<Arg> {
        if key == "environment" {
            Self::_flush_mirror();
        }
        self.0.__getattr__(key)
    }

    /// The underlying `_compat::dotmap::DotMap`.
    pub fn dotmap(&self) -> &_compat::dotmap::DotMap {
        &self.0
    }
}

impl python::PyObject for _LailaArgs {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "__LailaArgs"
    }
    fn __repr__(&self) -> alloc::string::String {
        self.0.__repr__()
    }
    fn __eq__(&self, other: &dyn python::PyObject) -> bool {
        if let Some(o) = other.as_any().downcast_ref::<_LailaArgs>() {
            return self.0 == o.0;
        }
        other
            .as_any()
            .downcast_ref::<_compat::dotmap::DotMap>()
            .is_some_and(|o| self.0 == *o)
    }
    fn __eq_arg__(&self, other: &Arg) -> bool {
        self.0.__eq__(other.clone())
    }
    fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error("unhashable type: '_LailaArgs'"))
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        _LailaArgs::__getattr__(self, name)
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        _LailaArgs::__setattr__(self, name, value)
    }
    fn __delattr__(&self, name: &str) -> Result<()> {
        self.0.__delattr__(name)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "get" => {
                let key = args.positional.first().cloned().unwrap_or(Arg::None);
                let default = args
                    .positional
                    .get(1)
                    .cloned()
                    .unwrap_or_else(|| args.get("default"));
                self.get(key, default)
            }
            "items" => Ok(self.items()),
            "toDict" => Ok(self.toDict()),
            "__getitem__" => {
                self.__getitem__(args.positional.first().cloned().unwrap_or(Arg::None))
            }
            "__setitem__" => {
                let k = args.positional.first().cloned().unwrap_or(Arg::None);
                let v = args.positional.get(1).cloned().unwrap_or(Arg::None);
                self.__setitem__(k, v)?;
                Ok(Arg::None)
            }
            other => self.0.__call_method__(other, args),
        }
    }
    fn __dir__(&self) -> Vec<alloc::string::String> {
        self.0.__dir__()
    }
    // The mapping protocol (`k in laila.args`, `len(..)`, iteration, `[k]`).
    fn __contains__(&self, item: &Arg) -> Result<bool> {
        self.0.__contains__(item.clone())
    }
    fn __len__(&self) -> Result<usize> {
        Ok(self.0.__len__())
    }
    fn __bool__(&self) -> bool {
        self.0.__bool__()
    }
    fn __iter__(&self) -> Result<Vec<Arg>> {
        Ok(self.0.__iter__())
    }
    fn __getitem__(&self, key: &Arg) -> Result<Arg> {
        _LailaArgs::__getitem__(self, key.clone())
    }
    fn __setitem__(&self, key: &Arg, value: Arg) -> Result<()> {
        _LailaArgs::__setitem__(self, key.clone(), value)
    }
    fn __delitem__(&self, key: &Arg) -> Result<()> {
        self.0.__delitem__(key.clone())
    }
}

impl IntoArg for _LailaArgs {
    fn into_arg(self) -> Arg {
        Arg::Object(alloc::sync::Arc::new(self))
    }
}
impl crate::python::PyArg for _LailaArgs {
    fn to_arg(&self) -> Arg {
        Arg::Object(alloc::sync::Arc::new(self.clone()))
    }
}

/// `laila.get_logger` / `laila.enable_logging` / `laila.disable_logging` /
/// `laila.set_log_level` -- the `laila.logger` convenience functions.
pub use logger::{disable_logging, enable_logging, get_logger, set_log_level};

/// `laila.DefaultPool` (= `_LAILA_IDENTIFIABLE_POOL`), `laila.DefaultPoolRouter`,
/// `laila.DefaultMultiBuffer` (= `MultiBuffer`), `laila.DefaultPolicy`.
pub use macros::defaults::{DefaultMultiBuffer, DefaultPolicy, DefaultPool, DefaultPoolRouter};

/// `#[laila::python]`, `#[laila::pysig("...")]`, `laila::args!`, `laila::py!`.
pub use laila_macros::{args, class_attr, property, py, pysig, python};

use alloc::vec::Vec;
use policy::central::command::AnyFuture;
use python::IntoArg;

/// The `laila` facade: a value, so `laila.memorize(...)` reads as in Python.
#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)]
pub struct _LailaModule;

/// Holds the facade value (kept in a module so `prelude` can re-export the
/// value without touching the `extern crate self as laila` alias).
pub mod facade {
    /// `import laila`
    pub static laila: super::_LailaModule = super::_LailaModule;
}
pub use facade::laila;

/// `laila.memory`
pub use policy::central::memory::Memory;

/// `laila.alpha_pool` proxy: a `Copy` view that resolves the active policy's
/// alpha pool on every call (so `laila.alpha_pool << x` works on a static).
#[derive(Clone, Copy, Debug, Default)]
pub struct AlphaPool;

impl AlphaPool {
    /// The active policy's alpha pool.
    pub fn pool(&self) -> data::Pool {
        policy::get_active_policy().alpha_pool()
    }
    /// `laila.alpha_pool.exists(key)`
    pub fn exists(&self, args: impl Into<Args>) -> Result<bool> {
        self.pool().exists(args)
    }
    /// `laila.alpha_pool.write(key, value)`
    pub fn write(&self, args: impl Into<Args>) -> Result<()> {
        self.pool().write(args)
    }
    pub fn delete(&self, key: impl IntoArg) -> Result<()> {
        self.pool().delete(key)
    }
    pub fn keys(&self, args: impl Into<Args>) -> Result<Arg> {
        self.pool().keys(args)
    }
    pub fn global_id(&self) -> alloc::string::String {
        self.pool().global_id()
    }
    pub fn pool_id(&self) -> alloc::string::String {
        self.pool().pool_id()
    }
    pub fn __getitem__(&self, key: impl IntoArg) -> Result<Arg> {
        self.pool().__getitem__(key)
    }
    pub fn __setitem__(&self, key: impl IntoArg, value: impl IntoArg) -> Result<()> {
        self.pool().__setitem__(key, value)
    }
    pub fn __delitem__(&self, key: impl IntoArg) -> Result<()> {
        self.pool().__delitem__(key)
    }
    pub fn __contains__(&self, key: impl IntoArg) -> Result<bool> {
        self.pool().__contains__(key)
    }
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        self.pool().__getattr__(name)
    }
    /// `laila.alpha_pool.index_enabled = False`
    pub fn __setattr__(&self, name: &str, value: impl IntoArg) -> Result<()> {
        self.pool().__setattr__(name, value)
    }
    pub fn __lshift__(&self, other: impl data::AsPool) -> Result<data::Pool> {
        self.pool().__lshift__(other)
    }
    pub fn __rshift__(&self, other: impl data::AsPool) -> Result<data::Pool> {
        self.pool().__rshift__(other)
    }
}
impl data::AsPool for AlphaPool {
    fn as_pool(&self) -> data::Pool {
        self.pool()
    }
}
impl<'b, B: data::AsPool> core::ops::Shl<&'b B> for AlphaPool {
    type Output = &'b B;
    fn shl(self, rhs: &'b B) -> &'b B {
        self.__lshift__(rhs).expect("proxy chain");
        rhs
    }
}
impl<'b, B: data::AsPool> core::ops::Shl<&'b B> for &AlphaPool {
    type Output = &'b B;
    fn shl(self, rhs: &'b B) -> &'b B {
        self.__lshift__(rhs).expect("proxy chain");
        rhs
    }
}
impl python::PyArg for AlphaPool {
    fn to_arg(&self) -> Arg {
        Arg::Pool(self.pool())
    }
}
impl python::IntoArg for AlphaPool {
    fn into_arg(self) -> Arg {
        Arg::Pool(self.pool())
    }
}

impl _LailaModule {
    /// `laila.__file__` -- the package's root source file (`lib.rs` here,
    /// `laila/__init__.py` in Python).
    pub fn __file__(&self) -> Arg {
        Arg::Str(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs").into())
    }
    /// `laila.memory` -- the active policy's central memory.
    pub fn memory(&self) -> Memory {
        Memory::default()
    }
    /// `laila.alpha_pool`
    pub fn alpha_pool(&self) -> AlphaPool {
        AlphaPool
    }
    /// `laila.constant(data, *, global_id=None, uuid=None, nickname=None)` (= `Entry.constant`)
    pub fn constant(&self, args: impl Into<Args>) -> Result<entry::Entry> {
        entry::Entry::constant(args)
    }
    /// `laila.variable(data=None, ...)` (= `Entry.variable`)
    pub fn variable(&self, args: impl Into<Args>) -> Result<entry::Entry> {
        entry::Entry::variable(args)
    }
    /// `laila.memorize(*args, src_policy=None, src_pool=None, dst_policy=None, dst_pool=None, comm=None, policy_id=None, pool_id=None, pool_nickname=None, affinity=None, **kwargs)`:
    /// persist one or more entries into a policy's memory.
    ///
    /// - `src_policy` names another policy -> a 3-party relay (the source
    ///   pushes `src_pool` -> `dst_policy`'s `dst_pool` over its own link);
    /// - `dst_policy` names a peer (or another local policy) -> the
    ///   entries are shipped to / stored by that policy;
    /// - otherwise the active policy's `central.memory.memorize` (in morph
    ///   mode: the peer's, as a `RemoteFuture`).
    #[pysig("memorize(*args, src_policy=None, src_pool=None, dst_policy=None, dst_pool=None, comm=None, policy_id=None, pool_id=None, pool_nickname=None, affinity=None, **kwargs)")]
    pub fn memorize(&self, args: impl Into<Args>) -> Result<AnyFuture> {
        // Accept the leading positional via the ``entries=`` keyword too.
        let mut args = args;
        let mut kwargs = kwargs;
        if args.is_empty() && kwargs.__contains__("entries")? {
            args = alloc::vec![kwargs.pop("entries")?];
        }
        // Back-compat: policy_id -> dst_policy, pool_id/pool_nickname -> dst_pool.
        let dst_policy = if dst_policy.is_none() {
            policy_id
        } else {
            dst_policy
        };
        let dst_pool = if dst_pool.is_none() {
            if !pool_id.is_none() {
                pool_id
            } else {
                pool_nickname
            }
        } else {
            dst_pool
        };
        let src_gid = _resolve_policy_ref(&src_policy)?;
        let dst_gid = _resolve_policy_ref(&dst_policy)?;
        let active = policy::get_active_policy_any()?;
        let active_gid = active.global_id();

        // Source is another policy -> 3-party relay (B pushes src->dst).
        if let Some(src_gid) = src_gid.filter(|g| *g != active_gid) {
            return _relay_transfer(
                "memorize",
                &src_gid,
                &args,
                &kwargs.as_dict(),
                &src_pool,
                dst_gid.as_deref(),
                &dst_pool,
                &comm,
                true,
            );
        }
        // Source is the active policy; destination a peer -> 2-party push.
        if let Some(dst_gid) = dst_gid.filter(|g| *g != active_gid) {
            let mut kw = python::PyDictBuilder::new();
            kw.insert("pool", dst_pool);
            return _route_to_policy(&dst_gid, "memorize", &args, &kw.finish(), &comm);
        }
        // A bare Manifest -> its bulk operation (pending entries + itself).
        if let Some(man) = _lone_manifest(&args) {
            let mut fwd = Args {
                positional: Vec::new(),
                keywords: kwargs.keywords,
            };
            fwd.push_keyword("pool", dst_pool);
            return man.memorize(fwd).map(AnyFuture::from);
        }
        let mut fwd = Args {
            positional: args,
            keywords: kwargs.keywords,
        };
        fwd.push_keyword("pool", dst_pool);
        fwd.push_keyword("affinity", affinity);
        match active {
            policy::ActivePolicy::Local(p) => _local_memory_verb(&p, "memorize", fwd),
            policy::ActivePolicy::Remote(proxy) => _morph_memory_call(&proxy, "memorize", fwd),
        }
    }
    /// `laila.remember(*args, persist=True, src_policy=None, src_pool=None, dst_policy=None, dst_pool=None, comm=None, prefix_scopes=None, policy_id=None, pool_id=None, pool_nickname=None, **kwargs)`:
    /// fetch entries from a policy's memory (routing as [`_LailaModule::memorize`]).
    #[pysig("remember(*args, persist=True, src_policy=None, src_pool=None, dst_policy=None, dst_pool=None, comm=None, prefix_scopes=None, policy_id=None, pool_id=None, pool_nickname=None, **kwargs)")]
    pub fn remember(&self, args: impl Into<Args>) -> Result<AnyFuture> {
        let mut args = args;
        let mut kwargs = kwargs;
        if kwargs.__contains__("nickname")? {
            let ids = __resolve_nickname(&mut kwargs, &prefix_scopes)?;
            args = alloc::vec![ids];
        }
        // Accept the leading positional via the ``entry_ids=`` keyword too.
        if args.is_empty() && kwargs.__contains__("entry_ids")? {
            args = alloc::vec![kwargs.pop("entry_ids")?];
        }
        // Expand nickname shorthands ("MANIFEST:my_dataset") into full gids
        // before any routing so peers always receive canonical ids.
        let args = _resolve_entry_refs(args, &prefix_scopes)?;
        // Back-compat: policy_id -> dst_policy, pool_id/pool_nickname -> dst_pool.
        let dst_policy = if dst_policy.is_none() {
            policy_id
        } else {
            dst_policy
        };
        let dst_pool = if dst_pool.is_none() {
            if !pool_id.is_none() {
                pool_id
            } else {
                pool_nickname
            }
        } else {
            dst_pool
        };
        let src_gid = _resolve_policy_ref(&src_policy)?;
        let dst_gid = _resolve_policy_ref(&dst_policy)?;
        let active = policy::get_active_policy_any()?;
        let active_gid = active.global_id();

        // Source policy is another policy -> 3-party relay (B pulls from dst).
        if let Some(src_gid) = src_gid.filter(|g| *g != active_gid) {
            return _relay_transfer(
                "remember",
                &src_gid,
                &args,
                &kwargs.as_dict(),
                &src_pool,
                dst_gid.as_deref(),
                &dst_pool,
                &comm,
                persist.__bool__(),
            );
        }
        // Active policy pulls from a peer (2-party).
        if let Some(dst_gid) = dst_gid.filter(|g| *g != active_gid) {
            let mut kw = python::PyDictBuilder::new();
            kw.insert("persist", persist.__bool__());
            kw.insert("pool", dst_pool);
            return _route_to_policy(&dst_gid, "remember", &args, &kw.finish(), &comm);
        }
        // A bare Manifest -> fetch every entry it references.
        if let Some(man) = _lone_manifest(&args) {
            let mut fwd = Args {
                positional: Vec::new(),
                keywords: kwargs.keywords,
            };
            fwd.push_keyword("pool", dst_pool);
            fwd.push_keyword("persist", persist);
            return man.remember(fwd).map(AnyFuture::from);
        }
        let mut fwd = Args {
            positional: args,
            keywords: kwargs.keywords,
        };
        fwd.push_keyword("pool", dst_pool);
        fwd.push_keyword("persist", persist);
        match active {
            policy::ActivePolicy::Local(p) => _local_memory_verb(&p, "remember", fwd),
            policy::ActivePolicy::Remote(proxy) => _morph_memory_call(&proxy, "remember", fwd),
        }
    }
    /// `laila.forget(*args, policy=None, pool=None, comm=None, prefix_scopes=None, policy_id=None, pool_id=None, pool_nickname=None, **kwargs)`:
    /// delete entries from a policy's pool (`policy` names a peer or another
    /// local policy; else the active one).
    #[pysig("forget(*args, policy=None, pool=None, comm=None, prefix_scopes=None, policy_id=None, pool_id=None, pool_nickname=None, **kwargs)")]
    pub fn forget(&self, args: impl Into<Args>) -> Result<AnyFuture> {
        let mut args = args;
        let mut kwargs = kwargs;
        if kwargs.__contains__("nickname")? {
            let ids = __resolve_nickname(&mut kwargs, &prefix_scopes)?;
            args = alloc::vec![ids];
        }
        if args.is_empty() && kwargs.__contains__("entry_ids")? {
            args = alloc::vec![kwargs.pop("entry_ids")?];
        }
        let args = _resolve_entry_refs(args, &prefix_scopes)?;
        // Back-compat: policy_id -> policy, pool_id/pool_nickname -> pool.
        let policy = if policy.is_none() { policy_id } else { policy };
        let pool = if pool.is_none() {
            if !pool_id.is_none() {
                pool_id
            } else {
                pool_nickname
            }
        } else {
            pool
        };
        let target_gid = _resolve_policy_ref(&policy)?;
        let active = policy::get_active_policy_any()?;
        let active_gid = active.global_id();
        if let Some(target_gid) = target_gid.filter(|g| *g != active_gid) {
            let mut kw = python::PyDictBuilder::new();
            kw.insert("pool", pool);
            return _route_to_policy(&target_gid, "forget", &args, &kw.finish(), &comm);
        }
        // A bare Manifest -> delete every referenced entry plus the manifest.
        if let Some(man) = _lone_manifest(&args) {
            let mut fwd = Args {
                positional: Vec::new(),
                keywords: kwargs.keywords,
            };
            fwd.push_keyword("pool", pool);
            return man.forget(fwd).map(AnyFuture::from);
        }
        let mut fwd = Args {
            positional: args,
            keywords: kwargs.keywords,
        };
        fwd.push_keyword("pool", pool);
        match active {
            policy::ActivePolicy::Local(p) => _local_memory_verb(&p, "forget", fwd),
            policy::ActivePolicy::Remote(proxy) => _morph_memory_call(&proxy, "forget", fwd),
        }
    }
    /// `laila.wait(future_ref, timeout=None)` -- the backwards-compatible
    /// shim over `laila.runtime.wait` (any future reference shape).
    pub fn wait(&self, args: impl Into<Args>) -> Result<Arg> {
        runtime::runtime.wait(args)
    }
    /// `laila.status(future_ref)` -- shim over `laila.runtime.status`.
    pub fn status(&self, future_ref: impl IntoArg) -> Result<Arg> {
        runtime::runtime.status(future_ref)
    }
    /// `laila.runtime` -- futures introspection (`status` / `result` /
    /// `exception` / `wait`).
    pub fn runtime(&self) -> runtime::runtime_mod {
        runtime::runtime
    }
    /// `laila._resolve_future(future_ref)` -- any reference shape to the
    /// concrete future.
    pub fn _resolve_future(
        &self,
        future_ref: impl IntoArg,
    ) -> Result<policy::central::command::Future> {
        runtime::_resolve_future(future_ref)
    }
    /// `laila.args` -- the process-wide `_LailaArgs` (a `DotMap` with the
    /// `environment` hooks; see `basics::definitions::cli_capable`).
    pub fn args(&self) -> Arg {
        basics::definitions::cli_capable::args()
    }
    /// `laila.logger` -- the process-wide `laila.logger.Logger` singleton
    /// (lazy). A singleton because every `record_*` call writes to the
    /// same underlying handlers.
    pub fn logger(&self) -> Result<Arg> {
        logger::get_logger(Args::new())
    }
    /// `laila.get_logger()`
    pub fn get_logger(&self, args: impl Into<Args>) -> Result<Arg> {
        logger::get_logger(args)
    }
    /// `laila.enable_logging(level='DEBUG', *, pool_nickname=None, pool_id=None, display=False, capture_traceback=False)`
    pub fn enable_logging(&self, args: impl Into<Args>) -> Result<Arg> {
        logger::enable_logging(args)
    }
    /// `laila.disable_logging()`
    pub fn disable_logging(&self, args: impl Into<Args>) -> Result<Arg> {
        logger::disable_logging(args)
    }
    /// `laila.set_log_level(level)`
    pub fn set_log_level(&self, args: impl Into<Args>) -> Result<Arg> {
        logger::set_log_level(args)
    }
    /// `laila.read_args(source, *, terminal_args=None)` -- load TOML / JSON /
    /// `.env` / `.xml` / terminal arguments into `laila.args`.
    #[pysig("read_args(source, *, terminal_args=None)")]
    pub fn read_args(&self, args: impl Into<Args>) -> Result<()> {
        let mut fwd = Args::new();
        fwd.push_positional(source);
        fwd.push_keyword("terminal_args", terminal_args);
        utils::args::ArgReader::process_wide().load(fwd)
    }
    /// `laila.arg_reader` -- the process-wide `ArgReader` behind
    /// `read_args` (its `target` is `laila.args` unless rebound).
    pub fn arg_reader(&self) -> utils::args::ArgReader {
        utils::args::ArgReader::process_wide()
    }
    /// `laila.get_active_namespace()` -> the UUID-5 namespace used to derive
    /// deterministic IDs from nicknames (`LAILA_UNIVERSAL_NAMESPACE` until
    /// overridden).
    pub fn get_active_namespace(&self) -> python::stdlib::UUID {
        python::stdlib::UUID {
            bytes: _compat::uuid_ns::active_bytes(),
        }
    }
    /// `laila.set_active_namespace(namespace_key)`: replace the active
    /// UUID-5 namespace with `uuid.uuid5(uuid.NAMESPACE_DNS, namespace_key)`.
    /// (To activate a namespace *UUID* directly -- the fixture pins of the
    /// vectors -- use `_compat::uuid_ns::set_active_namespace_uuid`.)
    pub fn set_active_namespace(&self, namespace_key: impl IntoArg) -> Result<()> {
        let key = namespace_key.into_arg();
        let Arg::Str(key) = key else {
            return Err(LailaError::type_error(alloc::format!(
                "set_active_namespace() argument must be str, not {}",
                key.type_name()
            )));
        };
        _compat::uuid_ns::set_active_namespace(&key)
    }
    /// `laila.get_active_policy()` -- the active policy: the local policy,
    /// or the peer proxy after `activate_policy(laila.peers[gid])` (morph
    /// mode). A `DefaultPolicy` is created and activated on first use.
    pub fn get_active_policy(&self) -> Result<policy::ActivePolicy> {
        policy::get_active_policy_any()
    }
    /// `laila.active_policy` -- the active policy (set it with
    /// `laila.__setattr__("active_policy", p)` / `activate_policy`).
    pub fn active_policy(&self) -> Result<policy::ActivePolicy> {
        policy::get_active_policy_any()
    }
    /// `laila._get_active_local_policy()` -- the local policy that owns
    /// newly-created futures (in morph mode: the one holding the peer link).
    pub fn _get_active_local_policy(&self) -> Result<policy::Policy> {
        policy::_get_active_local_policy_checked()
    }
    /// `laila._active_policy_gid` (`None` before the first activation).
    pub fn _active_policy_gid(&self) -> Arg {
        match policy::_active_policy_gid() {
            Some(g) => Arg::Str(g.into()),
            None => Arg::None,
        }
    }
    /// `laila._local_policies` / `laila.local_policies` -- `{gid: policy}`
    /// of every policy activated (or registered) in this process: a live
    /// dict view, `laila._local_policies[gid] = policy` registers.
    pub fn _local_policies(&self) -> Arg {
        policy::LocalPolicies.into_arg()
    }
    /// `laila.local_policies`
    pub fn local_policies(&self) -> Arg {
        self._local_policies()
    }
    /// `laila._live_policies` -- `{gid: policy}` of every policy
    /// constructed and still alive.
    pub fn _live_policies(&self) -> Arg {
        let mut d = python::PyDictBuilder::new();
        for (gid, p) in policy::_live_policies() {
            d.insert(gid, p.into_arg());
        }
        d.finish()
    }
    /// `laila._future_bank_policies()`
    pub fn _future_bank_policies(&self) -> Arg {
        policy::_future_bank_policies()
            .into_iter()
            .map(IntoArg::into_arg)
            .collect::<alloc::vec::Vec<Arg>>()
            .into_arg()
    }
    /// `laila._owning_local_policy(policy_id)`
    pub fn _owning_local_policy(&self, policy_id: impl IntoArg) -> Result<policy::Policy> {
        policy::_owning_local_policy(&policy_id.into_arg())
    }
    /// `laila.command` -- the active policy's central command.
    pub fn command(&self) -> policy::central::command::Command {
        policy::get_active_policy().central().command()
    }
    /// `laila.guarantee` -- `with laila.guarantee:` waits for every future
    /// submitted inside the block.
    pub fn guarantee(&self) -> utils::guarantee::_Guarantee {
        utils::guarantee::guarantee
    }
    /// `laila.guarantee_async` -- `async with laila.guarantee_async:`.
    pub fn guarantee_async(&self) -> utils::guarantee::_AsyncGuarantee {
        utils::guarantee::guarantee_async
    }
    /// `laila.TaskForce` -- the `policy.central.command.taskforce` package
    /// (`laila.TaskForce.PythonAsyncThreadPoolTaskForce(...)`).
    #[allow(non_snake_case)]
    pub fn TaskForce(&self) -> policy::central::command::taskforce::taskforce_mod {
        policy::central::command::taskforce::taskforce
    }
    /// `laila.set_default_directory(directory)` -- relocate every on-disk
    /// sub-directory laila uses by default (mutates
    /// `LAILA_DEFAULT_DIRECTORIES` in place; see
    /// [`macros::defaults::set_default_directory`]).
    pub fn set_default_directory(&self, directory: impl IntoArg) -> Result<()> {
        macros::defaults::set_default_directory(directory)
    }
    /// `laila.activate_policy(policy)` -- make *policy* the active one: a
    /// `Policy` handle (or the policy as an `Arg`), or a `RemotePolicyProxy`
    /// from `laila.peers` for morph mode.
    pub fn activate_policy(&self, policy: impl IntoArg) -> Result<()> {
        policy::activate_policy_arg(&policy.into_arg())
    }
    /// `laila.communication` -- the active policy's central communication
    /// (peers, protocols, RPC).
    #[cfg(feature = "std")]
    pub fn communication(&self) -> Result<policy::central::communication::Communication> {
        match policy::get_active_policy_any()? {
            policy::ActivePolicy::Local(p) => Ok(p.communication()),
            policy::ActivePolicy::Remote(_) => Err(LailaError::runtime_error(
                "laila.communication: the active policy is a remote proxy (morph mode)",
            )),
        }
    }
    /// `laila.peers` -- the active policy's peer registry (`{gid: PeerProxy}`;
    /// `laila.peers[gid][name]` is a stream channel).
    #[cfg(feature = "std")]
    pub fn peers(&self) -> Result<policy::central::communication::PeerRegistry> {
        Ok(self.communication()?.peers())
    }
    /// `laila.resolve_global_id(ref, *, evolution=None, prefix_scopes=None, parse_evolution=True)`:
    /// an *entry* reference (full gid, `SCOPE:<uuid | nickname>[@attrs]`,
    /// bare nickname) as a full `global_id` (= `Entry.resolve_global_id`).
    #[pysig("resolve_global_id(ref, *, evolution=None, prefix_scopes=None, parse_evolution=True)")]
    pub fn resolve_global_id(&self, args: impl Into<Args>) -> Result<alloc::string::String> {
        resolve_entry_global_id(
            &r#ref,
            &evolution,
            &prefix_scopes,
            parse_evolution.__bool__(),
        )
    }
    /// `laila._resolve_policy_ref(ref)`: a policy reference (object / gid /
    /// nickname) as a `global_id`, or `None`.
    pub fn _resolve_policy_ref(&self, r#ref: impl IntoArg) -> Result<Arg> {
        Ok(_resolve_policy_ref(&r#ref.into_arg())?.into_arg())
    }
    /// `laila._remote_policies` / `laila.remote_policies` -- `{gid: proxy}`
    /// of every peer any local policy is connected to (a live dict view).
    #[cfg(feature = "std")]
    pub fn _remote_policies(&self) -> policy::central::communication::RemotePolicyRegistry {
        policy::central::communication::RemotePolicyRegistry
    }
    /// `laila.remote_policies`
    #[cfg(feature = "std")]
    pub fn remote_policies(&self) -> policy::central::communication::RemotePolicyRegistry {
        self._remote_policies()
    }
    /// `laila.universe` -- `{**remote_policies, **local_policies}`.
    #[cfg(feature = "std")]
    pub fn universe(&self) -> Arg {
        let mut d = python::PyDictBuilder::new();
        for (gid, p) in policy::central::communication::_remote_policies() {
            d.insert(gid, p);
        }
        for (gid, p) in policy::_local_policies() {
            d.insert(gid, p.into_arg());
        }
        d.finish()
    }
    /// `laila.add_peer(uri, secret)` -- connect to a remote policy and
    /// register it as a peer of the active policy; its `global_id`.
    #[cfg(feature = "std")]
    pub fn add_peer(
        &self,
        uri: impl IntoArg,
        secret: impl IntoArg,
    ) -> Result<alloc::string::String> {
        self.communication()?
            .add_peer(uri.into_arg().__str__(), secret.into_arg().__str__())
    }
    /// `laila.request(policy_id, comm_protocol=None)` -- a transport-bound
    /// proxy for a connected peer (`ConnectionError` when unknown).
    #[cfg(feature = "std")]
    #[pysig("request(policy_id, comm_protocol=None)")]
    pub fn request(&self, args: impl Into<Args>) -> Result<Arg> {
        let pid = policy_id.__str__();
        let Some(proxy) = policy::central::communication::_remote_policy(&pid) else {
            return Err(LailaError::connection_error(alloc::format!(
                "Unknown peer {}: connect first with laila.add_peer().",
                python::pyrepr::str_repr(&pid)
            )));
        };
        if comm_protocol.is_none() {
            return Ok(proxy);
        }
        proxy.__call_method__("via", Args::from(comm_protocol))
    }
    /// `laila.relay(channel, name=None, *, timeout=None)` -- iterate the
    /// messages of a peer stream channel on the calling thread: a
    /// `Channel` (from `laila.peers[gid][name]`), or a peer gid plus a
    /// channel name.
    #[cfg(feature = "std")]
    #[pysig("relay(channel, name=None, *, timeout=None)")]
    pub fn relay(&self, args: impl Into<Args>) -> Result<policy::central::communication::Relay> {
        use policy::central::communication::Channel;
        if let Some(ch) = channel.downcast_ref::<Channel>().cloned() {
            if !name.is_none() {
                return Err(LailaError::type_error(
                    "Pass either a Channel or (peer_id, name), not both.",
                ));
            }
            return Ok(ch.relay(timeout.as_f64()));
        }
        if name.is_none() {
            return Err(LailaError::type_error(
                "laila.relay(peer_id, name) requires a channel name.",
            ));
        }
        let active = match policy::get_active_policy_any()? {
            policy::ActivePolicy::Local(p) => p,
            policy::ActivePolicy::Remote(_) => {
                return Err(LailaError::runtime_error(
                    "laila.relay needs a local active policy; the active policy is a remote proxy (morph mode). Activate the local policy that holds the peer.",
                ))
            }
        };
        let peer_gid = _resolve_policy_ref(&channel)?.unwrap_or_default();
        let Some(proxy) = active.communication().peers().get(&peer_gid) else {
            return Err(LailaError::connection_error(alloc::format!(
                "Unknown peer {}: connect first with laila.add_peer().",
                python::pyrepr::str_repr(&peer_gid)
            )));
        };
        Ok(proxy.channel(&name.__str__())?.relay(timeout.as_f64()))
    }
    /// `laila.DefaultPolicy(**kwargs)` -- the concrete policy class
    /// re-exported by the package (`laila.DefaultPolicy()`).
    #[allow(non_snake_case)]
    pub fn DefaultPolicy(&self, args: impl Into<Args>) -> Result<policy::Policy> {
        DefaultPolicy::new(args)
    }
    /// `laila.encryption_key`: process-wide Fernet key used by
    /// `FernetEncryption` when no explicit `key=` is given. Alias of
    /// `laila.args.encryption.key`; `None` when unset.
    pub fn encryption_key(&self) -> Arg {
        entry::compdata::transformation::get_encryption_key()
    }
    /// `laila.encryption_key = value`: store *value* under `laila.args.encryption.key`.
    pub fn set_encryption_key(&self, value: impl IntoArg) {
        entry::compdata::transformation::set_encryption_key(value)
    }
    /// `laila.<name> = value` -- the facade's settable properties
    /// (`encryption_key`, `args`).
    pub fn __setattr__(&self, name: &str, value: impl IntoArg) -> Result<()> {
        match name {
            "encryption_key" => {
                self.set_encryption_key(value);
                Ok(())
            }
            "active_policy" => self.activate_policy(value),
            "_active_policy_gid" => {
                policy::_set_active_policy_gid(match value.into_arg() {
                    Arg::None => None,
                    v => Some(v.__str__()),
                });
                Ok(())
            }
            "args" => {
                basics::definitions::cli_capable::_replace_args(value.into_arg());
                Ok(())
            }
            // `laila._active_namespace = None` (a test's tearDown) /
            // `= uuid.UUID(..)` (the cross-implementation vectors).
            "_active_namespace" => match value.into_arg() {
                Arg::None => {
                    _compat::uuid_ns::reset_active_namespace();
                    Ok(())
                }
                v => _compat::uuid_ns::set_active_namespace_uuid(&v.__str__()),
            },
            other => Err(LailaError::attribute_error(alloc::format!(
                "module 'laila' has no attribute '{other}'"
            ))),
        }
    }
    /// `laila.build(entry, *, taskforce_id=None)`: materialize *entry* by
    /// running its constitution on a taskforce.
    ///
    /// Submits the entry's `_build_async` coroutine to the chosen taskforce
    /// (alpha by default) and returns its future: `build(entry).wait()`
    /// from sync callers, `await build(entry)` from async ones. On
    /// completion the entry is mutated in place (`READY`) and the future
    /// resolves to the same entry. Building a `READY` entry is a no-op;
    /// concurrent builds of the *same* entry object coalesce on the
    /// in-flight future. `CyclicDependencyError` is raised synchronously
    /// when called from inside a resolution of the same entry.
    #[pysig("build(entry, *, taskforce_id=None)")]
    pub fn build(&self, args: impl Into<Args>) -> Result<policy::central::command::Future> {
        use policy::central::command::schema::parking::{
            check_resolve_cycle, resolve_chain_with, _RESOLVE_CHAIN,
        };
        use policy::central::command::FutureStatus;
        let Arg::Entry(e) = entry else {
            return Err(LailaError::attribute_error(alloc::format!(
                "'{}' object has no attribute 'global_id'",
                entry.type_name()
            )));
        };
        let gid = e.global_id();
        // A build that (transitively, through its constitution body)
        // triggers a build of the same entry can never finish; fail fast.
        check_resolve_cycle(&[gid.as_str()])?;
        let command = policy::get_active_policy().central().command();
        // ``key = (entry.global_id, id(entry))``
        let key = alloc::format!("{gid}@{:#x}", e.id());
        // A process pool cannot run the (local) build coroutine: the
        // orchestration stays on the alpha taskforce and only the body is
        // shipped (``Entry._run_body_in_process``).
        let mut taskforce_id = taskforce_id;
        let mut process_tf_id: Option<alloc::string::String> = None;
        if !taskforce_id.is_none() {
            let tf_gid = taskforce_id.__str__();
            let tf = command
                .taskforce(&tf_gid)
                .ok_or_else(|| LailaError::key_error(python::pyrepr::str_repr(&tf_gid)))?;
            if !tf._wraps_sync() {
                process_tf_id = Some(tf_gid);
                taskforce_id = Arg::None;
            }
        }
        let inflight_lock = command.inflight_lock();
        let _g = inflight_lock.lock();
        if let Some(inflight) = command._inflight_build(&key) {
            if !inflight._status().is_terminal() {
                return Ok(inflight);
            }
        }
        let fut_box: alloc::sync::Arc<
            python::sync::Mutex<Option<policy::central::command::Future>>,
        > = alloc::sync::Arc::new(python::sync::Mutex::new(None));
        let (e2, cmd2, key2, box2) = (e.clone(), command.clone(), key.clone(), fut_box.clone());
        let chain = resolve_chain_with(&[gid.as_str()]);
        let body =
            python::stdlib::asyncio::coroutine_function("_build_on_chain", move |_a: Args| {
                let (e, cmd, key, fut_box, chain, process_tf_id) = (
                    e2.clone(),
                    cmd2.clone(),
                    key2.clone(),
                    box2.clone(),
                    chain.clone(),
                    process_tf_id.clone(),
                );
                async move {
                    let token = _RESOLVE_CHAIN.set(chain);
                    let out = e._build_async_with(process_tf_id.as_deref()).await;
                    _RESOLVE_CHAIN.reset(token);
                    {
                        let _g = cmd.inflight_lock().lock();
                        let mine = fut_box.lock().clone();
                        if let (Some(cur), Some(mine)) = (cmd._inflight_build(&key), mine) {
                            if cur.is(&mine) {
                                cmd._set_inflight_build(&key, None);
                            }
                        }
                    }
                    out.map(Arg::Entry)
                }
            });
        let mut a = Args::new();
        a.push_positional(alloc::vec![body].into_arg());
        a.push_keyword("taskforce_id", taskforce_id);
        let fut = match command.submit(a)? {
            Arg::Future(f) => f,
            other => {
                return Err(LailaError::type_error(alloc::format!(
                    "command.submit returned {}",
                    other.type_name()
                )))
            }
        };
        *fut_box.lock() = Some(fut.clone());
        command._set_inflight_build(&key, Some(fut.clone()));
        let _ = FutureStatus::FINISHED;
        Ok(fut)
    }
    /// `laila.terminate(*, wait=True, cancel_pending=False)`: gracefully
    /// tear down everything laila has spawned in this process -- every
    /// local policy's command taskforces and pools, the policy registries,
    /// orphan taskforces -- and return the list of `"<step>[<id>]: <repr>"`
    /// failure strings (best-effort; never raises).
    #[pysig("terminate(*, wait=True, cancel_pending=False)")]
    pub fn terminate(&self, args: impl Into<Args>) -> Result<Arg> {
        Ok(
            policy::terminate(wait.__bool__(), cancel_pending.__bool__())
                .into_iter()
                .map(|s| Arg::Str(s.into()))
                .collect::<alloc::vec::Vec<Arg>>()
                .into_arg(),
        )
    }
}

/// The package as a Python module object (`hasattr(laila, "DefaultPolicy")`,
/// `getattr(laila, name)`): the `Default*` class aliases, the entry
/// builders and the module metadata.
impl python::object::PyObject for _LailaModule {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "module"
    }
    fn __repr__(&self) -> alloc::string::String {
        alloc::format!("<module 'laila' from '{}'>", self.__file__().__str__())
    }
    fn __eq__(&self, other: &dyn python::object::PyObject) -> bool {
        other.as_any().is::<_LailaModule>()
    }
    fn __hash__(&self) -> Result<i64> {
        Arg::Str("laila".into()).__hash__()
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "__name__" => Arg::Str("laila".into()),
            "__file__" => self.__file__(),
            "memory" => self.memory().into_arg(),
            "args" => self.args(),
            "Entry" | "Future" | "constant" | "variable" | "contingent" | "future" => {
                macros::aliases.__getattr__(name)?
            }
            _ => {
                return macros::default_class_object(name).ok_or_else(|| {
                    LailaError::attribute_error(alloc::format!(
                        "module 'laila' has no attribute '{name}'"
                    ))
                })
            }
        })
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "DefaultPolicy" => self.DefaultPolicy(args).map(IntoArg::into_arg),
            "constant" => self.constant(args).map(IntoArg::into_arg),
            "variable" => self.variable(args).map(IntoArg::into_arg),
            "memorize" => self.memorize(args).map(IntoArg::into_arg),
            "remember" => self.remember(args).map(IntoArg::into_arg),
            "forget" => self.forget(args).map(IntoArg::into_arg),
            _ => {
                let cls = macros::default_class_object(name).ok_or_else(|| {
                    LailaError::attribute_error(alloc::format!(
                        "module 'laila' has no attribute '{name}'"
                    ))
                })?;
                cls.__call__(args)
            }
        }
    }
}
impl python::PyArg for _LailaModule {
    fn to_arg(&self) -> Arg {
        Arg::Object(alloc::sync::Arc::new(*self))
    }
}

// ---------------------------------------------------------------------------
// Cross-policy routing of the memory verbs (`laila/__init__.py`)

/// Resolve a policy reference to a `global_id` string, or `None`: `None`
/// -> `None`; anything exposing `global_id` -> it; a gid string -> as-is;
/// a scoped shorthand (`"POLICY:trainer"`) / any other string -> a
/// deterministic `LAILA:POLICY:<uuid5>`.
/// `Entry.resolve_global_id(ref, evolution=, prefix_scopes=, parse_evolution=)`.
fn resolve_entry_global_id(
    r#ref: &Arg,
    evolution: &Arg,
    prefix_scopes: &Arg,
    parse_evolution: bool,
) -> Result<alloc::string::String> {
    use basics::definitions::identifiable_object::{resolve_global_id_for, ResolveOptions};
    let text = match r#ref {
        Arg::Str(s) => alloc::string::String::from(&**s),
        other => {
            return Err(LailaError::value_error(alloc::format!(
                "global id reference must be a string, got {}",
                other.type_name()
            )))
        }
    };
    let prefix: Option<Vec<alloc::string::String>> = if prefix_scopes.is_none() {
        None
    } else {
        Some(
            prefix_scopes
                .__iter__()?
                .iter()
                .map(|s| s.__str__())
                .collect(),
        )
    };
    let prefix_refs: Option<Vec<&str>> = prefix
        .as_ref()
        .map(|p| p.iter().map(|s| s.as_str()).collect());
    let opts = ResolveOptions {
        evolution: evolution.as_i64(),
        prefix_scopes: prefix_refs.as_deref(),
        default_scopes: None,
        parse_evolution,
    };
    resolve_global_id_for(entry::_DEFAULT_SCOPES, &text, &opts)
}

/// Expand nickname shorthands (`"MANIFEST:my_dataset"`) in the leading
/// `entry_ids` positional: strings (and strings inside a list / tuple) go
/// through `resolve_global_id`; anything else passes through untouched.
/// The `Manifest` when *args* is exactly one bare manifest. Only a *bare*
/// manifest triggers the bulk operation -- a manifest inside a list is a
/// regular entry (its payload is the blueprint), which is also how
/// `Manifest.memorize` stores the manifest itself without recursing back
/// into the shim.
fn _lone_manifest(args: &[Arg]) -> Option<Manifest> {
    match args {
        [one] => Manifest::from_arg(one),
        _ => None,
    }
}

fn _resolve_entry_refs(args: Vec<Arg>, prefix_scopes: &Arg) -> Result<Vec<Arg>> {
    let mut args = args;
    let Some(head) = args.first().cloned() else {
        return Ok(args);
    };
    let one = |x: &Arg| -> Result<Arg> {
        Ok(match x {
            Arg::Str(_) => {
                Arg::Str(resolve_entry_global_id(x, &Arg::None, prefix_scopes, true)?.into())
            }
            other => other.clone(),
        })
    };
    args[0] = match &head {
        Arg::Str(_) => one(&head)?,
        Arg::List(l) => {
            let items = l.read().clone();
            items
                .iter()
                .map(one)
                .collect::<Result<Vec<_>>>()?
                .into_arg()
        }
        Arg::Tuple(t) => Arg::Tuple(t.iter().map(one).collect::<Result<Vec<_>>>()?.into()),
        _ => head,
    };
    Ok(args)
}

/// `nickname=` (and optional `evolution=`) kwargs as the canonical
/// `[global_id]` shape `memory.remember` / `memory.forget` consume.
/// `policy.central.memory.<verb>(..)` on the local policy, through the
/// dynamic layer so an instance-bound stand-in (`object.__setattr__(mem,
/// "remember", spy)`) is honoured like in Python.
fn _local_memory_verb(p: &policy::Policy, verb: &str, fwd: Args) -> Result<AnyFuture> {
    p.memory()
        .__call_method__(verb, fwd)
        .map(AnyFuture::from_arg)
}

fn __resolve_nickname(kwargs: &mut Args, prefix_scopes: &Arg) -> Result<Arg> {
    let nickname = kwargs.pop("nickname")?;
    let evolution = kwargs.pop_or("evolution", Arg::None);
    if !matches!(nickname, Arg::Str(_)) {
        return Err(LailaError::value_error("nickname must be a string"));
    }
    let gid = resolve_entry_global_id(&nickname, &evolution, prefix_scopes, false)?;
    Ok(alloc::vec![Arg::Str(gid.into())].into_arg())
}

pub fn _resolve_policy_ref(r#ref: &Arg) -> Result<Option<alloc::string::String>> {
    use basics::definitions::identifiable_object::{resolve_global_id_for, ResolveOptions};
    if r#ref.is_none() {
        return Ok(None);
    }
    if let Ok(Arg::Str(gid)) = r#ref.__getattr__("global_id") {
        return Ok(Some(alloc::string::String::from(&*gid)));
    }
    let text = r#ref.__str__();
    let opts = ResolveOptions {
        evolution: None,
        prefix_scopes: None,
        default_scopes: Some(&[macros::strings::_POLICY_SCOPE]),
        parse_evolution: false,
    };
    resolve_global_id_for(&[macros::strings::_POLICY_SCOPE], &text, &opts).map(Some)
}

/// Pull a list of entry global ids out of a `(entry_ids,)` arg shape.
pub fn _extract_gids(args: &[Arg], kwargs: &Arg) -> Result<Vec<Arg>> {
    let val = match args.first() {
        Some(v) => v.clone(),
        None => {
            let v = kwargs.get("entry_ids")?;
            if v.is_none() {
                kwargs.get("entries")?
            } else {
                v
            }
        }
    };
    let items = match &val {
        Arg::List(_) => val.__iter__()?,
        _ => alloc::vec![val],
    };
    let mut out = Vec::new();
    for x in items {
        out.push(match x.__getattr__("global_id") {
            Ok(g) => g,
            Err(_) => Arg::Str(x.__str__().into()),
        });
    }
    Ok(out)
}

/// `laila.universe.get(gid)`: locals take precedence over remotes.
fn _universe_get(gid: &str) -> Option<Arg> {
    if let Some(p) = policy::_local_policy_or_live(gid) {
        if policy::_local_policies().iter().any(|(g, _)| g == gid) {
            return Some(p.into_arg());
        }
    }
    #[cfg(feature = "std")]
    {
        policy::central::communication::_remote_policy(gid)
    }
    #[cfg(not(feature = "std"))]
    None
}

fn _one_positional(op: &str, args: &[Arg]) -> Result<()> {
    if args.len() > 1 {
        return Err(LailaError::type_error(alloc::format!(
            "laila.{op}() takes one positional argument (an entry/gid or a list of them) but {} were given; pass a list to {op} several at once.",
            args.len()
        )));
    }
    Ok(())
}

/// Route a memory op to a remote peer as a LOCAL (active-policy-owned)
/// `Future`: one internal-taskforce task per entry, each offloading the
/// blocking wire RPC to a thread (parking its slot meanwhile). Entries
/// cross the wire as their canonical `Entry.serialize(transformation_base64)`
/// blob and are rebuilt with `build_by_scope` -- real entry data moves, no
/// shared pool needed.
#[cfg(feature = "std")]
pub fn _route_memory_to_peer(
    proxy: &Arg,
    op: &str,
    args: &[Arg],
    kwargs: &Arg,
) -> Result<AnyFuture> {
    use policy::central::command::schema::parking::park_async;
    use python::stdlib::asyncio::{coroutine_function, to_thread};

    let pool = kwargs.get("pool")?;
    if !pool.is_none() && !matches!(pool, Arg::Str(_)) {
        return Err(LailaError::type_error(
            "Remote memory ops need a pool gid or nickname *string* for the peer-side pool; a standalone pool object cannot be shipped to a peer.",
        ));
    }
    let command = policy::get_active_policy().central().command();
    if ["memorize", "remember", "forget"].contains(&op) {
        _one_positional(op, args)?;
    }
    let submit = |factories: Vec<Arg>| -> Result<AnyFuture> {
        let mut a = Args::new();
        a.push_keyword("tasks", factories.into_arg());
        a.push_keyword("taskforce_id", command.internal_taskforce());
        Ok(AnyFuture::from_arg(command.submit(a)?))
    };
    let memory = proxy.__getattr__("central")?.__getattr__("memory")?;
    let mem_kwargs = |extra: Vec<(&str, Arg)>| {
        let mut a = Args::new();
        if !pool.is_none() {
            a.push_keyword("pool", pool.clone());
        }
        for (k, v) in extra {
            a.push_keyword(k, v);
        }
        a
    };
    match op {
        "memorize" => {
            let entries = match args.first() {
                Some(e) => e.clone(),
                None => kwargs.get("entries")?,
            };
            let entries = match &entries {
                Arg::List(_) => entries.__iter__()?,
                _ => alloc::vec![entries],
            };
            let mut factories = Vec::new();
            for entry in entries {
                let mut sa = Args::new();
                sa.push_keyword(
                    "transformations",
                    entry::compdata::transformation::transformation_base64(),
                );
                let blob = entry.__call_method__("serialize", sa)?;
                let (memory, kw) = (memory.clone(), mem_kwargs(Vec::new()));
                factories.push(coroutine_function("_store", move |_a: Args| {
                    let (memory, blob, mut kw) = (memory.clone(), blob.clone(), kw.clone());
                    async move {
                        kw.positional.push(alloc::vec![blob].into_arg());
                        let gids = park_async(to_thread(move || {
                            memory.__call_method__("_remote_memorize", kw)
                        }))
                        .await??;
                        gids.__getitem__(0)
                    }
                }));
            }
            submit(factories)
        }
        "remember" => {
            let ids = _extract_gids(args, kwargs)?;
            let mut extra = Vec::new();
            if kwargs.__contains__("persist")? {
                extra.push(("persist", Arg::Bool(kwargs.get("persist")?.__bool__())));
            }
            let mut factories = Vec::new();
            for eid in ids {
                let (memory, kw) = (memory.clone(), mem_kwargs(extra.clone()));
                factories.push(coroutine_function("_fetch", move |_a: Args| {
                    let (memory, eid, mut kw) = (memory.clone(), eid.clone(), kw.clone());
                    async move {
                        kw.positional.push(alloc::vec![eid].into_arg());
                        let blobs = park_async(to_thread(move || {
                            memory.__call_method__("_remote_remember", kw)
                        }))
                        .await??;
                        let mut b = Args::new();
                        b.push_positional(blobs.__getitem__(0)?);
                        b.push_keyword("asynchronous", false);
                        entry::constitution::build_maps::build_by_scope(b)
                    }
                }));
            }
            submit(factories)
        }
        "forget" => {
            let ids = _extract_gids(args, kwargs)?;
            let (memory, kw) = (memory.clone(), mem_kwargs(Vec::new()));
            let f = coroutine_function("_delete_async", move |_a: Args| {
                let (memory, ids, mut kw) = (memory.clone(), ids.clone(), kw.clone());
                async move {
                    kw.positional.push(ids.into_arg());
                    park_async(to_thread(move || {
                        memory.__call_method__("_remote_forget", kw)
                    }))
                    .await?
                }
            });
            submit(alloc::vec![f])
        }
        // Any other op: pass straight through (gids are JSON-safe).
        other => {
            let mut a = Args {
                positional: args.to_vec(),
                keywords: Vec::new(),
            };
            a.extend_keywords(kwargs.clone())?;
            Ok(AnyFuture::from_arg(memory.__call_method__(other, a)?))
        }
    }
}

/// Run a central-memory operation against an arbitrary policy by gid: a
/// *remote* peer (one RPC over its transport, optionally via *comm*) or
/// another *local* policy (its own owner-bound memory). `ConnectionError`
/// when *policy_id* names neither.
pub fn _route_to_policy(
    policy_id: &str,
    op: &str,
    args: &[Arg],
    kwargs: &Arg,
    comm: &Arg,
) -> Result<AnyFuture> {
    let Some(target) = _universe_get(policy_id) else {
        return Err(LailaError::connection_error(alloc::format!(
            "Unknown policy_id {}: not a local policy or a connected peer. Connect to the peer first with laila.add_peer()/add_tcpip_peer().",
            python::pyrepr::str_repr(policy_id)
        )));
    };
    #[cfg(not(feature = "std"))]
    let _ = comm;
    #[cfg(feature = "std")]
    if policy::central::communication::RemotePolicyProxy::of(&target).is_some() {
        let target = if comm.is_none() {
            target
        } else {
            target.__call_method__("via", Args::from(comm.clone()))?
        };
        return _route_memory_to_peer(&target, op, args, kwargs);
    }
    // The target's memory is owner-bound (it submits to *its own* command),
    // so no transient ``activate_policy(target)`` is needed.
    let memory = target.__getattr__("central")?.__getattr__("memory")?;
    let mut a = Args {
        positional: args.to_vec(),
        keywords: Vec::new(),
    };
    a.extend_keywords(kwargs.clone())?;
    Ok(AnyFuture::from_arg(memory.__call_method__(op, a)?))
}

/// Drive a 3-party transfer by commanding the *source* policy: with active
/// policy A, `src_gid` = B, `dst_gid` = C, A asks B (over the A<->B link)
/// to push (`memorize`) / pull (`remember`) the entries to / from C over
/// B's *own* B<->C link.
#[allow(clippy::too_many_arguments)]
pub fn _relay_transfer(
    verb: &str,
    src_gid: &str,
    args: &[Arg],
    kwargs: &Arg,
    src_pool: &Arg,
    dst_gid: Option<&str>,
    dst_pool: &Arg,
    comm: &Arg,
    persist: bool,
) -> Result<AnyFuture> {
    let Some(src) = _universe_get(src_gid) else {
        return Err(LailaError::connection_error(alloc::format!(
            "src_policy {} is not a connected peer or a local policy. The active policy must be peered to the source policy (connect first with laila.add_peer()).",
            python::pyrepr::str_repr(src_gid)
        )));
    };
    let gids = _extract_gids(args, kwargs)?;
    #[cfg(feature = "std")]
    let src = if comm.is_none()
        || policy::central::communication::RemotePolicyProxy::of(&src).is_none()
    {
        src
    } else {
        src.__call_method__("via", Args::from(comm.clone()))?
    };
    // Owner-bound memory: no transient activation of ``src`` required.
    let memory = src.__getattr__("central")?.__getattr__("memory")?;
    let dst_policy = match dst_gid {
        Some(g) => Arg::Str(g.into()),
        None => Arg::None,
    };
    let mut a = Args::new();
    a.push_positional(gids.into_arg());
    let out = if verb == "memorize" {
        a.push_keyword("src_pool", src_pool.clone());
        a.push_keyword("dst_policy", dst_policy);
        a.push_keyword("dst_pool", dst_pool.clone());
        a.push_keyword("comm", comm.clone());
        memory.__call_method__("_relay_memorize", a)?
    } else {
        a.push_keyword("dst_policy", dst_policy);
        a.push_keyword("dst_pool", dst_pool.clone());
        a.push_keyword("src_pool", src_pool.clone());
        a.push_keyword("comm", comm.clone());
        a.push_keyword("persist", persist);
        memory.__call_method__("_relay_remember", a)?
    };
    Ok(AnyFuture::from_arg(out))
}

/// Morph mode: `get_active_policy().central.memory.<op>(*args, **kwargs)`
/// on the peer proxy -- one RPC, a `RemoteFuture` back.
fn _morph_memory_call(proxy: &Arg, op: &str, args: Args) -> Result<AnyFuture> {
    let memory = proxy.__getattr__("central")?.__getattr__("memory")?;
    Ok(AnyFuture::from_arg(memory.__call_method__(op, args)?))
}

/// `use laila::prelude::*` -- `import laila` plus the Python builtins.
pub mod prelude {
    // `obj.global_id` / `obj.uuid` on the `laila.atomic` mixin implementors
    // (the identity half of `_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT`).
    pub use crate::atomic::definitions::HasIdentity;
    // The provided methods of the `laila.atomic` mixins (`obj.atomic()`,
    // `obj.creation_timestamp`, ..) on their typed implementors.
    pub use crate::atomic::definitions::{
        _LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT as _,
        _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT as _, _LAILA_LOCALLY_ATOMIC_OBJECT as _,
    };
    pub use crate::facade::laila;
    pub use crate::python::prelude::*;
    pub use crate::{Arg, Args, LailaError, Result};
}
