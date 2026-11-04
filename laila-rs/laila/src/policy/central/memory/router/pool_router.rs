//! Pool router -- resolves a memory request to a concrete `Pool`.
//!
//! The router maintains three coupled data structures:
//!
//! - `pools` -- `{global_id: Pool}` -- the authoritative pool registry.
//! - `pools_pq` -- a `(-affinity, global_id)` priority queue used as the
//!   fallback ordering when nothing more specific is requested.
//! - `pools_nicknames` -- `{nickname: global_id}` -- a friendly-name index
//!   so callers can pass `pool_nickname="cache"` instead of a full
//!   UUID-bearing gid.
//!
//! Routing precedence used by [`PoolRouter::route`]:
//!
//! 1. Explicit `pool_id` -- always wins.
//! 2. `pool_nickname` -- resolved through `pools_nicknames`.
//! 3. (Future) `affinity` -- not yet implemented; falls through to (4).
//! 4. The `DEFAULT` nickname's pool, which is auto-registered if missing.
//!
//! Rust shape: [`_LAILA_IDENTIFIABLE_POOL_ROUTER`] is the class (a
//! pydantic-style descriptor; `DefaultPoolRouter` is the same type),
//! [`PoolRouter`] the instance handle.

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::_compat::pydantic::{
    FieldInfo, InitState, LazyClass, ModelBase, ModelClass, ModelHooks, ModelInstance,
};
use crate::atomic::definitions::HasIdentity;
use crate::basics::definitions::cli_capable::{CLIExempt, _LAILA_CLI_CAPABLE_CLASS};
use crate::basics::definitions::identifiable_object::_LAILA_IDENTIFIABLE_OBJECT;
use crate::data::schema::data_container::scopes_private_attr;
use crate::data::Pool;
use crate::macros::strings::{_DEFAULT_POOL_NICKNAME, _POOL_ROUTER_SCOPE};
use crate::python::builtins::IsInstance;
use crate::python::object::{PyAttrs, PyObject};
use crate::python::stdlib::PriorityQueue;
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, PyArg, Result};

fn with_annotation(mut f: FieldInfo, ann: &str) -> FieldInfo {
    f.annotation = ann.to_string();
    f
}

fn router_fields() -> Vec<(String, FieldInfo)> {
    let dict_factory = || {
        let f: crate::python::object::Callable =
            Arc::new(|_a: Args| crate::python::builtins::dict(Args::new()));
        Arg::Callable(f)
    };
    let pq_factory: crate::python::object::Callable =
        Arc::new(|_a: Args| PriorityQueue::new(Args::new()).map(IntoArg::into_arg));
    let mut pools = Args::new();
    pools.push_keyword("default_factory", dict_factory());
    let mut pq = Args::new();
    pq.push_keyword("default_factory", Arg::Callable(pq_factory));
    let mut nicknames = Args::new();
    nicknames.push_keyword("default_factory", dict_factory());
    alloc::vec![
        (
            String::from("pools"),
            with_annotation(
                CLIExempt(pools).expect("CLIExempt"),
                "dict[str, _LAILA_IDENTIFIABLE_POOL] | None"
            )
        ),
        (
            String::from("pools_pq"),
            with_annotation(CLIExempt(pq).expect("CLIExempt"), "PriorityQueue | None")
        ),
        (
            String::from("pools_nicknames"),
            with_annotation(
                crate::_compat::pydantic::Field(nicknames).expect("Field"),
                "dict[str, str] | None"
            )
        ),
    ]
}

/// `extend` over the model instance (shared by the constructor hook and the handle).
fn extend_inst(inst: &ModelInstance, pool: &Pool, affinity: Arg, pool_nickname: Arg) -> Result<()> {
    let affinity = if affinity.is_none() {
        Arg::Int(0) // farthest away
    } else {
        affinity
    };
    let pq = inst.field("pools_pq").unwrap_or(Arg::None);
    let neg = Arg::Int(0).__sub__(&affinity)?;
    let item = Arg::Tuple(alloc::vec![neg, Arg::Str(pool.global_id().into())].into());
    pq.__call_method__("put", Args::from(item))?;
    inst.field("pools")
        .unwrap_or(Arg::None)
        .__setitem__(pool.global_id().as_str(), Arg::Pool(pool.clone()))?;
    if !pool_nickname.is_none() {
        inst.field("pools_nicknames")
            .unwrap_or(Arg::None)
            .__setitem__(pool_nickname, Arg::Str(pool.global_id().into()))?;
    }
    Ok(())
}

struct Hooks;

impl ModelHooks for Hooks {
    /// Auto-register a `DefaultPool` (in-memory) when no pools were supplied.
    ///
    /// This is what makes a fresh `DefaultPolicy()` immediately usable
    /// without any pool wiring -- the in-memory default is good enough for
    /// tests and quickstart, and users override it by calling
    /// `Policy.extend` with a "real" pool (filesystem, S3, postgres, ...).
    fn model_post_init(&self, inst: &ModelInstance, _st: &InitState) -> Result<()> {
        let pools = inst.field("pools").unwrap_or(Arg::None);
        if pools.__len__()? == 0 {
            let pool = crate::macros::defaults::DefaultPool::new(Args::new())?;
            extend_inst(
                inst,
                &pool,
                Arg::Int(1),
                Arg::Str(_DEFAULT_POOL_NICKNAME.into()),
            )?;
        }
        Ok(())
    }
    fn method_names(&self) -> &'static [&'static str] {
        &["extend", "route", "_route_by_nickname"]
    }
}

static HOOKS: Hooks = Hooks;

static CLASS: LazyClass = LazyClass::new(|| ModelClass {
    name: "_LAILA_IDENTIFIABLE_POOL_ROUTER",
    module: "laila.policy.central.memory.router.pool_router",
    bases: alloc::vec![
        _LAILA_CLI_CAPABLE_CLASS::__class__(),
        _LAILA_IDENTIFIABLE_OBJECT::__class__(),
    ],
    fields: router_fields(),
    private_attrs: alloc::vec![scopes_private_attr(_POOL_ROUTER_SCOPE)],
    class_vars: Vec::new(),
    hooks: Some(&HOOKS),
});

/// Selects the destination pool for `memorize` / `remember` / `forget` calls.
///
/// Constructed automatically by every `_LAILA_IDENTIFIABLE_CENTRAL_MEMORY`
/// and exposed as `policy.central.memory.pool_router`. Users typically
/// interact with it indirectly through `Policy.extend` and the `pool_id` /
/// `pool_nickname` kwargs on the high-level memory API, but it is also
/// usable directly for advanced multi-pool setups.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct _LAILA_IDENTIFIABLE_POOL_ROUTER;

impl ModelBase for _LAILA_IDENTIFIABLE_POOL_ROUTER {
    fn __class__() -> &'static ModelClass {
        _LAILA_IDENTIFIABLE_POOL_ROUTER::__class__()
    }
}

impl _LAILA_IDENTIFIABLE_POOL_ROUTER {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        let c: &'static ModelClass = &CLASS;
        crate::basics::definitions::cli_capable::register_class(c);
        c
    }
    /// `_LAILA_IDENTIFIABLE_POOL_ROUTER(**kwargs)`
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<PoolRouter> {
        let inst = ModelInstance::construct(Self::__class__(), args.into())?;
        Ok(PoolRouter(Arc::new(RouterInner {
            inst,
            __dict__: PyAttrs::new(),
        })))
    }
    /// `cls.model_fields`
    pub fn model_fields() -> Arg {
        Self::__class__().model_fields()
    }
    /// `cls.__name__`
    pub fn __name__() -> Arg {
        Arg::Str(Self::__class__().name.into())
    }
}

impl PyArg for _LAILA_IDENTIFIABLE_POOL_ROUTER {
    fn to_arg(&self) -> Arg {
        Self::__class__().as_pyclass().into_arg()
    }
}
impl IntoArg for _LAILA_IDENTIFIABLE_POOL_ROUTER {
    fn into_arg(self) -> Arg {
        Self::__class__().as_pyclass().into_arg()
    }
}
impl crate::python::builtins::CallValue for _LAILA_IDENTIFIABLE_POOL_ROUTER {
    fn call_value(&self, args: Args) -> Result<Arg> {
        Self::new(args).map(IntoArg::into_arg)
    }
}
impl IsInstance for _LAILA_IDENTIFIABLE_POOL_ROUTER {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<PoolRouter>())
    }
}

struct RouterInner {
    inst: Arg,
    __dict__: PyAttrs,
}

/// A pool-router instance (handle; clones alias one router).
#[derive(Clone)]
pub struct PoolRouter(Arc<RouterInner>);

impl PoolRouter {
    fn inst(&self) -> &ModelInstance {
        ModelInstance::downcast(&self.0.inst).expect("router model instance")
    }
    /// `router.pools` -- `{global_id: Pool}`.
    pub fn pools(&self) -> Arg {
        self.inst().field("pools").unwrap_or(Arg::None)
    }
    /// `router.pools_pq`
    pub fn pools_pq(&self) -> Arg {
        self.inst().field("pools_pq").unwrap_or(Arg::None)
    }
    /// `router.pools_nicknames` -- `{nickname: global_id}`.
    pub fn pools_nicknames(&self) -> Arg {
        self.inst().field("pools_nicknames").unwrap_or(Arg::None)
    }
    /// `router._scopes`
    pub fn _scopes(&self) -> Arg {
        self.inst().private("_scopes").unwrap_or(Arg::None)
    }
    /// `router.global_id`
    pub fn global_id(&self) -> String {
        HasIdentity::global_id(self.inst())
    }
    /// `router.uuid`
    pub fn uuid(&self) -> String {
        HasIdentity::uuid(self.inst())
    }
    /// Python `is`.
    pub fn is(&self, other: &PoolRouter) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Register a pool with optional affinity priority and nickname.
    ///
    /// Stores *pool* under its `global_id` in `pools`, pushes a
    /// `(-affinity, gid)` entry into the affinity priority queue (negated
    /// so higher affinity sorts first), and -- if a nickname was given --
    /// adds a `nickname -> gid` entry to `pools_nicknames`.
    ///
    /// Re-registering an existing nickname overwrites the previous
    /// binding; re-registering an existing gid overwrites the pool
    /// instance.
    #[pysig("extend(pool, *, affinity=None, pool_nickname=None)")]
    pub fn extend(&self, args: impl Into<Args>) -> Result<()> {
        let Arg::Pool(p) = &pool else {
            return Err(LailaError::attribute_error(alloc::format!(
                "'{}' object has no attribute 'global_id'",
                pool.type_name()
            )));
        };
        extend_inst(self.inst(), p, affinity, pool_nickname)?;
        // The owning policy's mirror entry is stale.
        let _ = crate::basics::definitions::cli_capable::_mark_environment_dirty(Args::from(
            self.to_arg(),
        ));
        Ok(())
    }

    /// Resolve the destination pool for *entries*.
    ///
    /// Today the routing decision does not actually depend on *entries*
    /// -- it is purely based on the explicit `pool_id` / `pool_nickname`
    /// arguments and the default-pool fallback. `KeyError` if `pool_id`
    /// is given but not registered, or if `pool_nickname` does not
    /// resolve.
    #[pysig("route(entries, *, pool_id=None, pool_nickname=None, affinity=None)")]
    pub fn route(&self, args: impl Into<Args>) -> Result<Pool> {
        let _ = (entries, affinity);
        if !pool_id.is_none() {
            return as_pool(self.pools().__getitem__(pool_id)?);
        }
        let mut a = Args::new();
        a.push_keyword("pool_nickname", pool_nickname);
        self._route_by_nickname(a)
    }

    /// Resolve a pool by nickname, falling back to the default pool.
    ///
    /// When *pool_nickname* is `None` the fallback is the pool registered
    /// under `_DEFAULT_POOL_NICKNAME` (always present because the
    /// constructor auto-registers one if missing). `KeyError` if
    /// *pool_nickname* is set but not registered, or if the default
    /// nickname has been removed and no fallback exists.
    #[pysig("_route_by_nickname(*, pool_nickname=None)")]
    pub fn _route_by_nickname(&self, args: impl Into<Args>) -> Result<Pool> {
        let nick = if pool_nickname.is_none() {
            Arg::Str(_DEFAULT_POOL_NICKNAME.into())
        } else {
            pool_nickname
        };
        as_pool(
            self.pools()
                .__getitem__(self.pools_nicknames().__getitem__(nick)?)?,
        )
    }

    /// Typed `route(pool_id=.., pool_nickname=..)`.
    pub fn route_pool(&self, pool_id: Option<&str>, pool_nickname: Option<&str>) -> Result<Pool> {
        let mut a = Args::new();
        a.push_positional(Arg::None);
        a.push_keyword("pool_id", pool_id);
        a.push_keyword("pool_nickname", pool_nickname);
        self.route(a)
    }

    /// `router.<name>` for the dynamic layer.
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        if name == "_scopes" {
            return Ok(self._scopes());
        }
        if let Some(v) = self.0.__dict__.get(name) {
            return Ok(v);
        }
        // `router.route` read as a value: the bound method of *this*
        // handle (the model instance alone cannot dispatch it).
        if HOOKS.method_names().contains(&name) {
            let me = self.clone();
            let n = String::from(name);
            return Ok(Arg::Callable(Arc::new(move |a: Args| {
                me.__call_method__(&n, a)
            })));
        }
        PyObject::__getattr__(self.inst(), name)
    }
    /// `router.<name> = value`
    pub fn __setattr__(&self, name: &str, value: impl IntoArg) -> Result<()> {
        PyObject::__setattr__(self.inst(), name, value.into_arg())
    }
    /// `router.<name>(*args, **kwargs)` for the dynamic layer.
    pub fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "extend" => self.extend(args).map(|()| Arg::None),
            "route" => self.route(args).map(Arg::Pool),
            "_route_by_nickname" => self._route_by_nickname(args).map(Arg::Pool),
            _ => PyObject::__call_method__(self.inst(), name, args),
        }
    }
}

fn as_pool(v: Arg) -> Result<Pool> {
    match v {
        Arg::Pool(p) => Ok(p),
        other => Err(LailaError::type_error(alloc::format!(
            "pool_router.pools holds a {}, not a pool",
            other.type_name()
        ))),
    }
}

impl HasIdentity for PoolRouter {
    fn global_id(&self) -> String {
        HasIdentity::global_id(self.inst())
    }
    fn uuid(&self) -> String {
        HasIdentity::uuid(self.inst())
    }
}

impl core::fmt::Debug for PoolRouter {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&PyObject::__repr__(self))
    }
}

impl PyObject for PoolRouter {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    /// `a is b`: identity of the shared handle, not of the boxing.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<PoolRouter>()
            .is_some_and(|o| self.is(o))
    }
    fn type_name(&self) -> &'static str {
        "_LAILA_IDENTIFIABLE_POOL_ROUTER"
    }
    fn __repr__(&self) -> String {
        HasIdentity::global_id(self)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<PoolRouter>()
            .is_some_and(|o| HasIdentity::global_id(o) == HasIdentity::global_id(self))
    }
    fn __hash__(&self) -> Result<i64> {
        Arg::Str(HasIdentity::global_id(self).into()).__hash__()
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        PoolRouter::__getattr__(self, name)
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        PoolRouter::__setattr__(self, name, value)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        PoolRouter::__call_method__(self, name, args)
    }
    fn __class__(&self) -> Option<crate::python::PyClass> {
        Some(self.inst().class.as_pyclass())
    }
    fn __dir__(&self) -> Vec<String> {
        PyObject::__dir__(self.inst())
    }
    /// The `laila.args.environment` mirror fields of the underlying model
    /// (`pools` / `pools_pq` are CLI-exempt; `pools` is walked by
    /// `build_environment` itself).
    fn __cli_fields__(&self) -> Option<Vec<(String, Arg)>> {
        let mut out: Vec<(String, Arg)> = Vec::new();
        for name in
            crate::basics::definitions::cli_capable::_eligible_model_fields_of(self.inst().class)
        {
            if let Ok(v) = PoolRouter::__getattr__(self, &name) {
                out.push((name, v));
            }
        }
        Some(out)
    }
}
impl IntoArg for PoolRouter {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for PoolRouter {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
