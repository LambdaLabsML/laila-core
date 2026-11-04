//! Base schema for all LAILA storage-pool implementations.
//!
//! A *pool* is laila's name for a key-value store that persists
//! serialized `Entry` blobs. Pools are the leaves of the storage hierarchy
//! that the central memory subsystem routes `memorize` / `remember` /
//! `forget` calls to. Concrete subclasses plug into different backends --
//! in-memory, filesystem, S3, Redis, Postgres, SQLite, DuckDB, HDF5, GCS,
//! Azure, BackBlaze, Cloudflare, HuggingFace -- by overriding the small set
//! of "internal storage hooks" described below.
//!
//! Public surface (proxy-aware)
//! ----------------------------
//! - `pool[key]` -> stored blob, transparently falling back through the
//!   proxy chain on miss and caching the result on the way back.
//! - `pool[key] = entry` -> local-only write.
//! - `del pool[key]` -> local-only delete.
//! - `pool[manifest]` -> `PoolWrapper` view scoped by a manifest.
//! - `pool.exists(key)` / `key in pool` -> local-only existence check.
//! - `pool.keys(as_generator=...)` -> local-only key enumeration.
//! - `pool.empty()` -> local-only wipe.
//! - `pool.sync()` -> flush in-memory cache (raises if cacheless).
//! - `cache <= origin` / `origin <= cache` -> bulk duplicate via central
//!   memory.
//!
//! Subclass contract (override these)
//! ----------------------------------
//! Synchronous: `_read`, `_write`, `_delete`, `_exists`, `_keys`, `_empty`.
//! Default implementations operate on the in-memory `resource` dict.
//!
//! Asynchronous: `_read_async`, `_write_async`, `_delete_async`,
//! `_exists_async`. Default implementations just call the sync hook on the
//! calling event loop -- subclasses with a real async client should
//! override these.
//!
//! Proxy chains
//! ------------
//! Pools can be wired into proxy chains via `_proxy_to`. A read that misses
//! the local store falls through to the upstream pool, and a successful
//! upstream read is cached locally before being returned. This lets users
//! compose layered caches like
//!
//! ```text
//! in_memory << hdf5 << s3
//! ```
//!
//! so that hot reads never leave the in-memory tier.
//!
//! Rust shape
//! ----------
//! Python's single class is three pieces here:
//!
//! * [`PoolBackend`] -- the *subclass contract*: the storage hooks (and
//!   their `_async` twins, `sync`, `close`, backend-specific attributes).
//!   A backend implements only what it overrides; every default is the
//!   Python base-class body over the in-memory `resource` dict.
//! * [`PoolObject`] -- the *base class*: the pydantic instance (fields,
//!   identity, `laila.args` resolution), the proxy chain, the index and
//!   every public method, written once. Backends receive `&PoolObject`.
//! * [`Pool`] -- the handle (`Arc<PoolObject>`, `Arg::Pool`): what
//!   `_LAILA_IDENTIFIABLE_POOL()` returns, with the Python-shaped calling
//!   convention (`impl IntoArg` keys, `Args` for keyword-taking methods).
//!
//! A concrete pool is declared with its own [`ModelClass`] (fields, private
//! attributes, `_persistent_storage`) whose MRO root is
//! `_LAILA_IDENTIFIABLE_POOL::__class__()`, and constructed through
//! [`Pool::construct`] -- see `data::s3` for the smallest example.

use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::sync::{Arc, Weak};
use alloc::vec::Vec;
use core::any::Any;
use core::future::Future;
use core::pin::Pin;

use crate::_compat::pydantic::{
    FieldInfo, LazyClass, ModelBase, ModelClass, ModelHooks, ModelInstance, PrivateAttrInfo,
};
use crate::atomic::definitions::locally_atomic_object::ensure_local_lock;
use crate::atomic::definitions::{HasIdentity, _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT};
use crate::basics::definitions::cli_capable::CLIExempt;
use crate::macros::strings::_POOL_SCOPE;
use crate::python::builtins::IsInstance;
use crate::python::object::{HasAttrs, PyAttrs, PyObject};
use crate::python::sync::{RLock, RwLock};
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, PyArg, PyClass, Result};

use super::data_container::{scopes_private_attr, _LAILA_IDENTIFIABLE_DATA_CONTAINER};
use super::pool_index::{index_key, PoolIndex};
use super::pool_wrapper::PoolWrapper;

/// A boxed `async` result, the object-safe spelling of `async fn` in
/// [`PoolBackend`].
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// `with self.atomic():` held for the lifetime of the guard.
pub struct Held(RLock);
impl Held {
    /// Acquire *lock* (re-entrant) until the guard drops.
    pub fn acquire(lock: &RLock) -> Held {
        lock.acquire(None);
        Held(lock.clone())
    }
}
impl Drop for Held {
    fn drop(&mut self) {
        let _ = self.0.release();
    }
}

fn key_str(key: &Arg) -> String {
    key.__str__()
}

// ---------------------------------------------------------------------------
// The subclass contract

/// The storage hooks a concrete pool overrides (Python: the `_read` /
/// `_write` / ... methods of a `_LAILA_IDENTIFIABLE_POOL` subclass).
///
/// Every method has the Python base-class body as its default, so the
/// in-memory pool is `impl PoolBackend for MemoryBackend {}`. A backend is
/// handed the owning [`PoolObject`] on every call: that is where its
/// fields (`pool.field("file_path")`), the `resource` dict, the
/// transformations and the atomic lock live.
pub trait PoolBackend: Send + Sync + 'static {
    /// `&self` as `Any` (for `Pool::downcast_ref::<Backend>()`, i.e.
    /// `isinstance(pool, HDF5Pool)`).
    fn as_any(&self) -> &dyn Any;

    /// The tail of the subclass's `model_post_init` (open the file, create
    /// the table, ...). Runs once the handle exists, after the pydantic
    /// construction (fields coerced, `laila.args` applied, identity set,
    /// the persistent-storage check passed).
    fn model_post_init(&self, _pool: &PoolObject) -> Result<()> {
        Ok(())
    }

    /// Read *key* from this pool's own storage; `None` for a miss.
    ///
    /// The default implementation reads from the in-memory `resource`
    /// dict under the pool's atomic lock and returns `None` for missing
    /// keys (so the proxy-aware `__getitem__` can detect a miss and fall
    /// through to the upstream pool).
    fn _read(&self, pool: &PoolObject, key: &str) -> Result<Arg> {
        let _held = pool.held();
        let resource = pool.resource();
        if !resource.__contains__(key)? {
            return Ok(Arg::None);
        }
        let blob = resource.__getitem__(key)?;
        if blob.is_none() {
            return Ok(Arg::None);
        }
        Ok(blob)
    }

    /// Write *value* under *key* into this pool's own storage.
    ///
    /// Default implementation writes into `resource` under the atomic
    /// lock. Implementations should treat *value* as opaque bytes/dict
    /// (the central memory layer has already serialised the entry).
    fn _write(&self, pool: &PoolObject, key: &str, value: Arg) -> Result<()> {
        let _held = pool.held();
        pool.resource().__setitem__(key, value)
    }

    /// Delete *key* from this pool's own storage. Missing keys are
    /// silently tolerated.
    fn _delete(&self, pool: &PoolObject, key: &str) -> Result<()> {
        let _held = pool.held();
        let resource = pool.resource();
        if resource.__contains__(key)? {
            resource.__delitem__(key)?;
        }
        Ok(())
    }

    /// `True` if *key* is present in this pool's own storage (never
    /// consults the proxy chain).
    fn _exists(&self, pool: &PoolObject, key: &str) -> Result<bool> {
        let _held = pool.held();
        pool.resource().__contains__(key)
    }

    /// Enumerate this pool's own keys (a snapshot taken under the lock).
    fn _keys(&self, pool: &PoolObject) -> Result<Vec<String>> {
        let _held = pool.held();
        Ok(pool
            .resource()
            .__iter__()?
            .iter()
            .map(|k| k.__str__())
            .collect())
    }

    /// Local-only list of keys that are evolutions of *base_gid*: the
    /// exact key (a constant) and every `base_gid@...` key (variables).
    /// The default filters [`_keys`](Self::_keys); listing-based backends
    /// can push the prefix down.
    fn _candidate_keys(&self, pool: &PoolObject, base_gid: &str) -> Result<Vec<String>> {
        let prefix = alloc::format!("{base_gid}@");
        Ok(self
            ._keys(pool)?
            .into_iter()
            .filter(|k| k == base_gid || k.starts_with(&prefix))
            .collect())
    }

    /// Wipe this pool's own storage (never propagates to the proxy chain).
    fn _empty(&self, pool: &PoolObject) -> Result<()> {
        let _held = pool.held();
        pool.resource().clear(Args::new())
    }

    /// Async read; the default delegates to the sync [`_read`](Self::_read)
    /// inline. Backends with a native-async client should override this.
    fn _read_async<'a>(&'a self, pool: &'a PoolObject, key: &'a str) -> BoxFuture<'a, Result<Arg>> {
        Box::pin(async move { self._read(pool, key) })
    }
    /// Async write; default delegates to sync [`_write`](Self::_write).
    fn _write_async<'a>(
        &'a self,
        pool: &'a PoolObject,
        key: &'a str,
        value: Arg,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self._write(pool, key, value) })
    }
    /// Async delete; default delegates to sync [`_delete`](Self::_delete).
    fn _delete_async<'a>(
        &'a self,
        pool: &'a PoolObject,
        key: &'a str,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self._delete(pool, key) })
    }
    /// Async exists; default delegates to sync [`_exists`](Self::_exists).
    fn _exists_async<'a>(
        &'a self,
        pool: &'a PoolObject,
        key: &'a str,
    ) -> BoxFuture<'a, Result<bool>> {
        Box::pin(async move { self._exists(pool, key) })
    }

    /// `pool.sync()`: flush any in-memory write cache to the backing
    /// store. The base implementation always raises -- override in
    /// subclasses that maintain a write-back cache.
    fn sync(&self, _pool: &PoolObject) -> Result<()> {
        Err(LailaError::not_implemented(
            "Sync is not implemented for this pool, the pool is cacheless, i.e. operations are immediately executed on the underlying storage.",
        ))
    }

    /// `pool.close()`: `None` when the backend defines no `close` (the base
    /// class has none: `hasattr(pool, "close")` is `False`), otherwise the
    /// result of closing.
    fn close(&self, _pool: &PoolObject) -> Option<Result<()>> {
        None
    }

    /// A backend-specific property read (`pool.file_path` that is not a
    /// field, `pool.mount_dir`); `None` when the backend defines no `name`.
    fn __getattr__(&self, _pool: &PoolObject, _name: &str) -> Option<Result<Arg>> {
        None
    }
    /// A backend-specific attribute write; `None` when not handled.
    fn __setattr__(&self, _pool: &PoolObject, _name: &str, _value: Arg) -> Option<Result<()>> {
        None
    }
    /// A backend-specific method (`pool.checkpoint(compact=True)`); `None`
    /// when the backend defines no `name`.
    fn __call_method__(&self, _pool: &PoolObject, _name: &str, _args: Args) -> Option<Result<Arg>> {
        None
    }
    /// The backend's extra method names (`dir()`, `hasattr`).
    fn method_names(&self) -> &'static [&'static str] {
        &[]
    }
}

/// The in-memory backend: the Python base class with nothing overridden
/// (`DefaultPool = _LAILA_IDENTIFIABLE_POOL`).
#[derive(Clone, Copy, Debug, Default)]
pub struct MemoryBackend;

impl PoolBackend for MemoryBackend {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

// ---------------------------------------------------------------------------
// The class descriptor

/// `_persistent_storage: ClassVar[bool] = False`
pub const _PERSISTENT_STORAGE: &str = "_persistent_storage";

fn pool_fields() -> Vec<(String, FieldInfo)> {
    let exempt = |a: Args| CLIExempt(a).expect("CLIExempt");
    let field = |a: Args| crate::_compat::pydantic::Field(a).expect("Field");
    let dict_factory: crate::python::object::Callable =
        Arc::new(|_a: Args| crate::python::builtins::dict(Args::new()));
    let mut resource = Args::new();
    resource.push_keyword("default_factory", Arg::Callable(dict_factory));
    let mut batch = Args::new();
    batch.push_keyword("default", false);
    let mut transformations = Args::new();
    transformations.push_keyword("default", Arg::None);
    let mut index_enabled = Args::new();
    index_enabled.push_keyword("default", true);
    let mut index_pool = Args::new();
    index_pool.push_keyword("default", Arg::None);
    alloc::vec![
        (
            String::from("resource"),
            with_annotation(exempt(resource), "dict[str, Any]")
        ),
        (
            String::from("batch_accelerated"),
            with_annotation(field(batch), "bool")
        ),
        (
            String::from("transformations"),
            with_annotation(exempt(transformations), "TransformationSequence | None")
        ),
        (
            String::from("index_enabled"),
            with_annotation(field(index_enabled), "bool")
        ),
        (
            String::from("index_pool"),
            with_annotation(exempt(index_pool), "Any | None")
        ),
    ]
}

fn with_annotation(mut f: FieldInfo, ann: &str) -> FieldInfo {
    f.annotation = ann.to_string();
    f
}

fn none_private() -> PrivateAttrInfo {
    PrivateAttrInfo {
        default: Some(Arg::None),
        default_factory: None,
    }
}

struct PoolHooks;

impl ModelHooks for PoolHooks {
    /// Validate the configuration of pools backed by external storage.
    ///
    /// A persistent backend serialises records to JSON / bytes, so it
    /// needs a transformation sequence to turn an `Entry` into a storable
    /// blob; `transformations=None` there would make every `memorize`
    /// fail later with an opaque serialisation error. It is rejected up
    /// front instead. In-memory pools may use `None`.
    fn model_post_init(
        &self,
        inst: &ModelInstance,
        _st: &crate::_compat::pydantic::InitState,
    ) -> Result<()> {
        let persistent = inst
            .class
            .class_var(_PERSISTENT_STORAGE)
            .map(|v| v.__bool__())
            .unwrap_or(false);
        let transformations = inst.field("transformations").unwrap_or(Arg::None);
        if persistent && transformations.is_none() {
            return Err(LailaError::value_error(alloc::format!(
                "{} is backed by persistent storage and requires a transformation sequence (got transformations=None); use e.g. laila.transformations.transformation_base64",
                inst.class_name()
            )));
        }
        Ok(())
    }
    fn property_getters(&self) -> &'static [&'static str] {
        &["pool_id", "index", "proxy_to", "_persistent_index_identity"]
    }
    fn property_setters(&self) -> &'static [&'static str] {
        &["proxy", "proxy_to"]
    }
    fn method_names(&self) -> &'static [&'static str] {
        BASE_METHOD_NAMES
    }
}

static POOL_HOOKS: PoolHooks = PoolHooks;

static CLASS: LazyClass = LazyClass::new(|| ModelClass {
    name: "_LAILA_IDENTIFIABLE_POOL",
    module: "laila.data.schema.base",
    bases: alloc::vec![_LAILA_IDENTIFIABLE_DATA_CONTAINER::__class__()],
    fields: pool_fields(),
    private_attrs: alloc::vec![
        scopes_private_attr(_POOL_SCOPE),
        (String::from("_proxy_to"), none_private()),
        (String::from("_index"), none_private()),
    ],
    class_vars: alloc::vec![(String::from(_PERSISTENT_STORAGE), Arg::Bool(false))],
    hooks: Some(&POOL_HOOKS),
});

/// The public methods of the base class (for `dir()` / `hasattr` /
/// `getattr(pool, name)` returning a bound callable).
pub const BASE_METHOD_NAMES: &[&str] = &[
    "write",
    "delete",
    "write_async",
    "delete_async",
    "exists",
    "keys",
    "empty",
    "sync",
    "_read",
    "_write",
    "_delete",
    "_exists",
    "_keys",
    "_empty",
    "_candidate_keys",
    "_search_keys",
    "_read_async",
    "_write_async",
    "_delete_async",
    "_exists_async",
    "_read_through_async",
    "_index_record",
    "_index_remove",
    "_resolve_indexed",
    "_link_proxy",
    "_upstream_tiers",
    "__getitem__",
    "__setitem__",
    "__delitem__",
    "__contains__",
    "__lshift__",
    "__rshift__",
    "__le__",
    "atomic",
    "lock",
    "unlock",
    "locked",
];

/// Build the class descriptor of a concrete pool: *name* / *module* are
/// the Python class's, *fields* / *private_attrs* / *class_vars* are its
/// own declarations (the base's are inherited through *bases*, which
/// defaults to `[_LAILA_IDENTIFIABLE_POOL]`).
///
/// A persistent backend sets `class_vars = [("_persistent_storage", true)]`
/// and overrides `transformations` with
/// `Field(default=transformation_base64())` the way the Python class does.
pub fn pool_class(
    name: &'static str,
    module: &'static str,
    bases: Vec<&'static ModelClass>,
    fields: Vec<(String, FieldInfo)>,
    private_attrs: Vec<(String, PrivateAttrInfo)>,
    class_vars: Vec<(String, Arg)>,
) -> ModelClass {
    ModelClass {
        name,
        module,
        bases: if bases.is_empty() {
            alloc::vec![_LAILA_IDENTIFIABLE_POOL::__class__()]
        } else {
            bases
        },
        fields,
        private_attrs,
        class_vars,
        hooks: None,
    }
}

/// `name: T = Field(default=value)` for a pool subclass declaration.
pub fn field_default(annotation: &str, default: impl IntoArg) -> FieldInfo {
    let mut a = Args::new();
    a.push_keyword("default", default);
    with_annotation(
        crate::_compat::pydantic::Field(a).expect("Field"),
        annotation,
    )
}

/// `name: T = Field(...)` -- a required field of a pool subclass
/// declaration (`ValidationError: Field required` when omitted).
pub fn field_required(annotation: &str) -> FieldInfo {
    with_annotation(FieldInfo::undefined(), annotation)
}

/// `name: T = CLIExempt(default=value)` for a pool subclass declaration.
pub fn exempt_default(annotation: &str, default: impl IntoArg) -> FieldInfo {
    let mut a = Args::new();
    a.push_keyword("default", default);
    with_annotation(CLIExempt(a).expect("CLIExempt"), annotation)
}

/// `_name: T = PrivateAttr(default=value)` for a pool subclass declaration.
pub fn private_default(default: impl IntoArg) -> PrivateAttrInfo {
    PrivateAttrInfo {
        default: Some(default.into_arg()),
        default_factory: None,
    }
}

// ---------------------------------------------------------------------------
// The class object

/// Abstract base class for laila storage pools (and, as `DefaultPool`, the
/// in-memory pool that ships with every policy).
///
/// A pool is the *map* flavour of `_LAILA_IDENTIFIABLE_DATA_CONTAINER`:
/// keys are entry `global_id` strings.
///
/// Implements the proxy-aware public read/write/delete/exists/keys API and
/// provides default in-memory hook implementations so simple pools can be
/// created by just inheriting and overriding nothing. Real backends
/// override the `_read` / `_write` / `_delete` / `_keys` / `_exists` /
/// `_empty` hooks (and, optionally, their `_async` counterparts).
///
/// Attributes
/// ----------
/// `resource : dict[str, Any]` -- default in-memory backing store.
/// `batch_accelerated : bool` -- whether the backend can handle batched
/// writes more efficiently than individual ones.
/// `transformations : TransformationSequence | None` -- optional pipeline
/// applied to every blob before write and reversed on read.
/// `index_enabled : bool` -- maintain the per-base evolution /
/// creation-timestamp index on every write and delete.
/// `index_pool : _LAILA_IDENTIFIABLE_POOL | None` -- where the index
/// shards are stored (`None`: this pool itself).
///
/// Index bookkeeping keys (`LAILA:POOL_INDEX:...`) are hidden from `keys`
/// unless `include_index=True` is passed.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct _LAILA_IDENTIFIABLE_POOL;

impl ModelBase for _LAILA_IDENTIFIABLE_POOL {
    fn __class__() -> &'static ModelClass {
        _LAILA_IDENTIFIABLE_POOL::__class__()
    }
}

impl _LAILA_IDENTIFIABLE_POOL {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        let c: &'static ModelClass = &CLASS;
        crate::basics::definitions::cli_capable::register_class(c);
        c
    }
    /// `_LAILA_IDENTIFIABLE_POOL(**kwargs)` -- an in-memory pool.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<Pool> {
        Pool::construct(Self::__class__(), args.into(), |_| {
            Ok(Box::new(MemoryBackend))
        })
    }
    /// `cls.model_fields`
    pub fn model_fields() -> Arg {
        Self::__class__().model_fields()
    }
    /// `cls.__private_attributes__`
    pub fn __private_attributes__() -> Arg {
        Self::__class__().__private_attributes__()
    }
    /// `cls.__name__`
    pub fn __name__() -> Arg {
        Arg::Str(Self::__class__().name.into())
    }
    /// `cls._persistent_storage`
    pub fn _persistent_storage() -> bool {
        false
    }
    /// `_LAILA_IDENTIFIABLE_POOL._link_proxy(cache, origin)`: set
    /// `cache._proxy_to = origin` after checking the chain stays acyclic.
    ///
    /// `ValueError` when *cache* is *origin*, or *origin*'s upstream chain
    /// already leads back to *cache* (`a << b` then `b << a`).
    pub fn _link_proxy(cache: &Pool, origin: &Pool) -> Result<()> {
        if cache.is(origin) {
            return Err(LailaError::value_error(alloc::format!(
                "A pool cannot proxy itself ({})",
                cache.global_id()
            )));
        }
        let mut seen: Vec<usize> = Vec::new();
        let mut cur: Option<Pool> = Some(origin.clone());
        while let Some(c) = cur {
            if seen.contains(&c.id()) {
                break;
            }
            if c.is(cache) {
                return Err(LailaError::value_error(alloc::format!(
                    "Proxy cycle: {} already reaches {} through its proxy chain",
                    origin.global_id(),
                    cache.global_id()
                )));
            }
            seen.push(c.id());
            cur = c.proxy_to_pool();
        }
        *cache.0._proxy_to.write() = Some(origin.clone());
        Ok(())
    }
}

impl PyArg for _LAILA_IDENTIFIABLE_POOL {
    fn to_arg(&self) -> Arg {
        Self::__class__().as_pyclass().into_arg()
    }
}
impl IntoArg for _LAILA_IDENTIFIABLE_POOL {
    fn into_arg(self) -> Arg {
        Self::__class__().as_pyclass().into_arg()
    }
}
impl crate::python::builtins::CallValue for _LAILA_IDENTIFIABLE_POOL {
    fn call_value(&self, args: Args) -> Result<Arg> {
        Self::new(args).map(IntoArg::into_arg)
    }
}
impl IsInstance for _LAILA_IDENTIFIABLE_POOL {
    /// `isinstance(x, _LAILA_IDENTIFIABLE_POOL)`: every pool.
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Pool(_))
    }
}

// ---------------------------------------------------------------------------
// The object

/// The state and behaviour every pool shares (the Python base class body).
pub struct PoolObject {
    /// The pydantic instance: public fields (`resource`, `transformations`,
    /// `index_enabled`, `index_pool`, `batch_accelerated`, the backend's
    /// own) and private attributes (identity, `_scopes`).
    inst: Arg,
    backend: Box<dyn PoolBackend>,
    /// `_proxy_to` -- the origin pool this one is a cache for.
    _proxy_to: RwLock<Option<Pool>>,
    /// `_index` -- created on first access.
    _index: RwLock<Option<PoolIndex>>,
    /// `object.__setattr__` attributes: `_local_lock` (the atomic mixin)
    /// and anything a caller sets on the instance.
    __dict__: PyAttrs,
    this: Weak<PoolObject>,
}

impl PoolObject {
    /// The pydantic instance.
    pub fn inst(&self) -> &ModelInstance {
        ModelInstance::downcast(&self.inst).expect("pool model instance")
    }
    /// The backend (typed access through `as_any`).
    pub fn backend(&self) -> &dyn PoolBackend {
        &*self.backend
    }
    /// The class descriptor.
    pub fn class(&self) -> &'static ModelClass {
        self.inst().class
    }
    /// `type(self).__name__`
    pub fn class_name(&self) -> &'static str {
        self.inst().class_name()
    }
    /// The handle of this object (`self`).
    pub fn handle(&self) -> Pool {
        Pool(self.this.upgrade().expect("pool handle"))
    }
    /// `self.<field>` -- a pydantic field (`None` when not a field).
    pub fn field(&self, name: &str) -> Option<Arg> {
        self.inst().field(name)
    }
    /// `self.<field> = value` (no validation, as `validate_assignment=False`).
    pub fn set_field(&self, name: &str, value: impl IntoArg) {
        self.inst().set_field(name, value.into_arg())
    }
    /// The per-instance re-entrant lock.
    pub fn local_lock(&self) -> RLock {
        ensure_local_lock(&self.__dict__)
    }
    /// `with self.atomic():` -- hold the lock until the guard drops.
    pub fn held(&self) -> Held {
        Held::acquire(&self.local_lock())
    }
    /// `self.resource` -- the in-memory backing dict.
    pub fn resource(&self) -> Arg {
        self.field("resource").unwrap_or(Arg::None)
    }
    /// `self.batch_accelerated`
    pub fn batch_accelerated(&self) -> bool {
        self.field("batch_accelerated")
            .map(|v| v.__bool__())
            .unwrap_or(false)
    }
    /// `self.transformations` (`None` or a `TransformationSequence`).
    pub fn transformations(&self) -> Arg {
        self.field("transformations").unwrap_or(Arg::None)
    }
    /// `self.index_enabled`
    pub fn index_enabled(&self) -> bool {
        self.field("index_enabled")
            .map(|v| v.__bool__())
            .unwrap_or(true)
    }
    /// `self.index_pool` (`None` or a pool).
    pub fn index_pool(&self) -> Arg {
        self.field("index_pool").unwrap_or(Arg::None)
    }
    /// `type(self)._persistent_storage`
    pub fn _persistent_storage(&self) -> bool {
        self.class()
            .class_var(_PERSISTENT_STORAGE)
            .map(|v| v.__bool__())
            .unwrap_or(false)
    }
    /// Whether the `PoolIndex` persists its identity in this pool (see F-03).
    pub fn _persistent_index_identity(&self) -> bool {
        self._persistent_storage()
    }
    /// `self._scopes`
    pub fn _scopes(&self) -> Arg {
        self.inst().private("_scopes").unwrap_or(Arg::None)
    }
    /// Unique identifier for this pool. Alias for `global_id`.
    pub fn pool_id(&self) -> String {
        HasIdentity::global_id(self)
    }
    /// `id(self)`
    pub fn id(&self) -> usize {
        self as *const PoolObject as usize
    }

    // -------- Index --------

    /// This pool's `PoolIndex` (created on first access).
    pub fn index(&self) -> PoolIndex {
        if let Some(i) = self._index.read().as_ref() {
            return i.clone();
        }
        let mut g = self._index.write();
        if let Some(i) = g.as_ref() {
            return i.clone();
        }
        let idx = PoolIndex::of(&self.handle());
        *g = Some(idx.clone());
        idx
    }
    /// `self._index` (`None` until the index is first used).
    pub fn _index(&self) -> Option<PoolIndex> {
        self._index.read().clone()
    }
    pub fn _index_record(&self, key: &str, value: &Arg) -> Result<()> {
        if self.index_enabled() && !index_key(key) {
            self.index().record(key, value.clone())?;
        }
        Ok(())
    }
    pub fn _index_remove(&self, key: &str) -> Result<()> {
        if self.index_enabled() && !index_key(key) {
            self.index().remove(key)?;
        }
        Ok(())
    }
    /// Answer an attribute lookup from the index alone, or `None`.
    ///
    /// *base_gid* is `LAILA:ENTRY:<uuid>` (no `@` suffix); *attributes*
    /// are the search arguments of the reference: `evolution` (may be
    /// negative, `-1` = latest) and / or `creation_timestamp`. Returns a
    /// storage key the index believes exists (central memory validates
    /// it), or `None` when the index cannot answer and the caller should
    /// fall back to scanning.
    pub fn _resolve_indexed(&self, base_gid: &str, attributes: &Arg) -> Result<Arg> {
        if !self.index_enabled() {
            return Ok(Arg::None);
        }
        let raw_evolution = attributes.get(Args::from("evolution"))?;
        let evolution = if raw_evolution.is_none() {
            Arg::None
        } else {
            Arg::Int(crate::python::builtins::int(raw_evolution)?)
        };
        let timestamp = attributes.get(Args::from("creation_timestamp"))?;
        if !timestamp.is_none() {
            let mut a = Args::new();
            a.push_positional(Arg::Str(base_gid.into()));
            a.push_positional(timestamp);
            a.push_positional(evolution);
            return self.index().by_creation_timestamp(a);
        }
        let n = if evolution.is_none() {
            Arg::Int(-1)
        } else {
            evolution
        };
        self.index().nth(base_gid, n)
    }

    // -------- Index-maintaining write / delete wrappers --------
    // Every writer (central memory, proxy write-back, `pool[k] = v`) goes
    // through these so the index sees all mutations. Backends keep
    // overriding only the `_write` / `_delete` (`_async`) hooks.

    /// Store *value* under *key* locally and index it.
    pub fn write(&self, key: &str, value: Arg) -> Result<()> {
        self._write(key, value.clone())?;
        self._index_record(key, &value)
    }
    /// Delete *key* locally and un-index it.
    pub fn delete(&self, key: &str) -> Result<()> {
        self._delete(key)?;
        self._index_remove(key)
    }
    /// Async `write` (the index maintenance runs inline).
    pub async fn write_async(&self, key: &str, value: Arg) -> Result<()> {
        self.backend._write_async(self, key, value.clone()).await?;
        if self.index_enabled() && !index_key(key) {
            self.index().record(key, value)?;
        }
        Ok(())
    }
    /// Async `delete` (the index maintenance runs inline).
    pub async fn delete_async(&self, key: &str) -> Result<()> {
        self.backend._delete_async(self, key).await?;
        if self.index_enabled() && !index_key(key) {
            self.index().remove(key)?;
        }
        Ok(())
    }

    // -------- Proxy properties --------

    /// Write-only property used to wire proxy relationships: always
    /// `None` (`origin.proxy = cache` makes *cache* a proxy for *origin*).
    pub fn proxy(&self) -> Arg {
        Arg::None
    }
    /// The origin pool this one is a cache/proxy for, or `None`.
    pub fn proxy_to(&self) -> Arg {
        match self.proxy_to_pool() {
            Some(p) => Arg::Pool(p),
            None => Arg::None,
        }
    }
    /// `self._proxy_to` as a handle.
    pub fn proxy_to_pool(&self) -> Option<Pool> {
        self._proxy_to.read().clone()
    }
    /// `self.proxy = pool`
    pub fn set_proxy(&self, pool: &Arg) -> Result<()> {
        match pool {
            Arg::None => Ok(()),
            Arg::Pool(p) => _LAILA_IDENTIFIABLE_POOL::_link_proxy(p, &self.handle()),
            other => Err(not_a_pool(other)),
        }
    }
    /// `self.proxy_to = pool`
    pub fn set_proxy_to(&self, pool: &Arg) -> Result<()> {
        match pool {
            Arg::None => {
                *self._proxy_to.write() = None;
                Ok(())
            }
            Arg::Pool(p) => _LAILA_IDENTIFIABLE_POOL::_link_proxy(&self.handle(), p),
            other => Err(not_a_pool(other)),
        }
    }
    /// Upstream pools of this one, nearest first; stops at a repeated pool
    /// (defensive).
    pub fn _upstream_tiers(&self) -> Vec<Pool> {
        let mut seen: Vec<usize> = alloc::vec![self.id()];
        let mut out = Vec::new();
        let mut cur = self.proxy_to_pool();
        while let Some(p) = cur {
            if seen.contains(&p.id()) {
                break;
            }
            seen.push(p.id());
            cur = p.proxy_to_pool();
            out.push(p);
        }
        out
    }
    /// `cache << origin`: install `cache` as a proxy for `origin`; returns
    /// *other* (the origin) so the operator chains right-to-left
    /// (`mem << hdf5 << s3`).
    pub fn __lshift__(&self, other: &Pool) -> Result<Pool> {
        other.set_proxy(&Arg::Pool(self.handle()))?;
        Ok(other.clone())
    }
    /// `origin >> cache`: install `cache` as a proxy for `origin`; returns
    /// *other* (the cache) so the operator chains left-to-right
    /// (`s3 >> hdf5 >> mem`).
    pub fn __rshift__(&self, other: &Pool) -> Result<Pool> {
        self.set_proxy(&Arg::Pool(other.clone()))?;
        Ok(other.clone())
    }

    // -------- Internal storage hooks (typed; the backend's) --------

    pub fn _read(&self, key: &str) -> Result<Arg> {
        self.backend._read(self, key)
    }
    pub fn _write(&self, key: &str, value: Arg) -> Result<()> {
        self.backend._write(self, key, value)
    }
    pub fn _delete(&self, key: &str) -> Result<()> {
        self.backend._delete(self, key)
    }
    pub fn _exists(&self, key: &str) -> Result<bool> {
        self.backend._exists(self, key)
    }
    pub fn _keys(&self) -> Result<Vec<String>> {
        self.backend._keys(self)
    }
    pub fn _empty(&self) -> Result<()> {
        self.backend._empty(self)
    }
    pub fn _candidate_keys(&self, base_gid: &str) -> Result<Vec<String>> {
        self.backend._candidate_keys(self, base_gid)
    }
    /// Candidate keys for an attribute lookup (`remember("ENTRY:x@...")`):
    /// answered from this pool's index when it has a shard for *base_gid*,
    /// `None` otherwise ("no index, scan"). Returning a list -- even an
    /// empty one -- is authoritative and skips the scan; central memory
    /// still validates the key it picks against the store.
    pub fn _search_keys(&self, base_gid: &str, _attributes: &Arg) -> Result<Arg> {
        if !self.index_enabled() {
            return Ok(Arg::None);
        }
        self.index().candidates(base_gid)
    }
    pub async fn _read_async(&self, key: &str) -> Result<Arg> {
        self.backend._read_async(self, key).await
    }
    pub async fn _write_async(&self, key: &str, value: Arg) -> Result<()> {
        self.backend._write_async(self, key, value).await
    }
    pub async fn _delete_async(&self, key: &str) -> Result<()> {
        self.backend._delete_async(self, key).await
    }
    pub async fn _exists_async(&self, key: &str) -> Result<bool> {
        self.backend._exists_async(self, key).await
    }
    /// Async, proxy-aware read: the coroutine counterpart of `__getitem__`.
    ///
    /// Reads from this pool's own async storage and, on a miss, falls
    /// through the `_proxy_to` chain. A successful upstream read is cached
    /// back into this pool (via `_write_async`) before being returned.
    pub async fn _read_through_async(&self, key: &str) -> Result<Arg> {
        let value = self._read_async(key).await?;
        if !value.is_none() {
            return Ok(value);
        }
        // Walk the chain iteratively (cycle-safe) and write the hit back
        // into every tier in front of the one that held it.
        let mut missed: Vec<Pool> = alloc::vec![self.handle()];
        for tier in self._upstream_tiers() {
            let value = tier._read_async(key).await?;
            if !value.is_none() {
                for front in missed.iter().rev() {
                    front.write_async(key, value.clone()).await?;
                }
                return Ok(value);
            }
            missed.push(tier);
        }
        Ok(Arg::None)
    }

    // -------- Proxy-aware public API --------

    /// Retrieve the blob for *key*, with proxy fall-through and write-back.
    ///
    /// Two special cases: a `Manifest` key short-circuits to a
    /// `PoolWrapper` view scoped by that manifest; a local miss with
    /// `_proxy_to` set is forwarded upstream and the hit cached into every
    /// front tier before being returned.
    pub fn __getitem__(&self, key: &Arg) -> Result<Arg> {
        if let Arg::Entry(e) = key {
            if crate::policy::central::memory::schema::manifest::Manifest::from_entry(e).is_some() {
                return Ok(PoolWrapper::new(self.handle(), key.clone()).into_arg());
            }
        }
        let k = key_str(key);
        let value = self._read(&k)?;
        if !value.is_none() {
            return Ok(value);
        }
        let mut missed: Vec<Pool> = alloc::vec![self.handle()];
        for tier in self._upstream_tiers() {
            let value = tier._read(&k)?;
            if !value.is_none() {
                for front in missed.iter().rev() {
                    front.0.write(&k, value.clone())?;
                }
                return Ok(value);
            }
            missed.push(tier);
        }
        Ok(Arg::None)
    }
    /// Store *entry* under *key*. Local-only; never propagates to a proxy origin.
    pub fn __setitem__(&self, key: &Arg, entry: Arg) -> Result<()> {
        self.write(&key_str(key), entry)
    }
    /// Delete the entry for *key*. Local-only; never propagates to a proxy origin.
    pub fn __delitem__(&self, key: &Arg) -> Result<()> {
        self.delete(&key_str(key))
    }
    /// Remove every entry from this pool (local-only). Also drops this
    /// pool's index shards, including those held by a separate `index_pool`.
    pub fn empty(&self) -> Result<()> {
        if self.index_enabled() {
            self.index().clear()?;
        }
        self._empty()
    }
    /// `True` if *key* is present in this pool. Local-only check.
    pub fn exists(&self, key: &str) -> Result<bool> {
        self._exists(key)
    }
    /// `key in pool` -- thin alias for `exists`.
    pub fn __contains__(&self, key: &Arg) -> Result<bool> {
        self.exists(&key_str(key))
    }
    /// The keys stored in this pool (local-only; index bookkeeping keys
    /// hidden unless *include_index*).
    pub fn keys_vec(&self, include_index: bool) -> Result<Vec<String>> {
        let raw = self._keys()?;
        if include_index {
            return Ok(raw);
        }
        Ok(raw.into_iter().filter(|k| !index_key(k)).collect())
    }
    /// `pool.sync()`
    pub fn sync(&self) -> Result<()> {
        self.backend.sync(self)
    }
    /// `pool.close()` -- `AttributeError` when the backend has no `close`.
    pub fn close(&self) -> Result<()> {
        match self.backend.close(self) {
            Some(r) => r,
            None => Err(LailaError::attribute_error(alloc::format!(
                "'{}' object has no attribute 'close'",
                self.class_name()
            ))),
        }
    }
    /// `self <= other` -- bulk-copy *other* pool's contents into this one
    /// through the active policy's central memory (`NotImplemented` for
    /// anything but a pool or a pool nickname / global-id string).
    pub fn __le__(&self, other: &Arg) -> Result<Arg> {
        if !matches!(other, Arg::Pool(_) | Arg::Str(_)) {
            return Ok(crate::python::builtins::not_implemented());
        }
        let mut a = Args::new();
        a.push_keyword("pool_src", other.clone());
        a.push_keyword("pool_dest", Arg::Pool(self.handle()));
        crate::policy::get_active_policy()
            .memory()
            ._duplicate_pool(a)
            .map(Arg::Future)
    }

    // -------- Python protocol --------

    /// `pool.<name>` for the dynamic layer.
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        if let Some(v) = self.field(name) {
            return Ok(v);
        }
        match name {
            "pool_id" => return Ok(Arg::Str(self.pool_id().into())),
            "proxy" => return Ok(Arg::None),
            "proxy_to" | "_proxy_to" => return Ok(self.proxy_to()),
            "index" => return Ok(self.index().into_arg()),
            "_index" => {
                return Ok(match self._index() {
                    Some(i) => i.into_arg(),
                    None => Arg::None,
                })
            }
            "_persistent_storage" => return Ok(Arg::Bool(self._persistent_storage())),
            "_persistent_index_identity" => {
                return Ok(Arg::Bool(self._persistent_index_identity()))
            }
            "_scopes" => return Ok(self._scopes()),
            _ => {}
        }
        if let Some(r) = self.backend.__getattr__(self, name) {
            return r;
        }
        if let Some(v) = self.__dict__.get(name) {
            return Ok(v);
        }
        // ``hasattr(pool, "close")`` is ``False`` on the base class: only a
        // backend that defines ``close`` has the bound method.
        let has_close = name == "close" && self.backend.method_names().contains(&"close");
        if BASE_METHOD_NAMES.contains(&name)
            || self.backend.method_names().contains(&name)
            || has_close
        {
            let me = self.handle();
            let n = name.to_string();
            return Ok(Arg::Callable(Arc::new(move |a: Args| {
                me.__call_method__(&n, a)
            })));
        }
        PyObject::__getattr__(self.inst(), name)
    }
    /// `pool.<name> = value` for the dynamic layer.
    pub fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        match name {
            "proxy" => return self.set_proxy(&value),
            "proxy_to" => return self.set_proxy_to(&value),
            "_proxy_to" => {
                *self._proxy_to.write() = match value {
                    Arg::Pool(p) => Some(p),
                    _ => None,
                };
                return Ok(());
            }
            "_index" => {
                *self._index.write() = value.downcast_ref::<PoolIndex>().cloned();
                return Ok(());
            }
            _ => {}
        }
        if let Some(r) = self.backend.__setattr__(self, name, value.clone()) {
            return r;
        }
        if self.inst().fields.read().contains_key(name) {
            self.set_field(name, value);
            return Ok(());
        }
        match PyObject::__setattr__(self.inst(), name, value.clone()) {
            Ok(()) => Ok(()),
            // ``object.__setattr__`` for a name pydantic rejects: keep it on
            // the instance (``pool._keys = counting``-style monkeypatches).
            Err(_) if name.starts_with('_') => self.__dict__.__setattr__(name, value),
            Err(e) => Err(e),
        }
    }
    /// `pool.<name>(*args, **kwargs)` for the dynamic layer.
    pub fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let me = self.handle();
        let first = || args.positional.first().cloned().unwrap_or(Arg::None);
        let second = || args.positional.get(1).cloned().unwrap_or(Arg::None);
        Ok(match name {
            "write" => return me.write(args).map(|()| Arg::None),
            "delete" => return me.delete(first()).map(|()| Arg::None),
            "exists" => Arg::Bool(me.exists(first())?),
            "keys" => return me.keys(args),
            "empty" => return me.empty(args).map(|()| Arg::None),
            "sync" => return me.sync(args).map(|()| Arg::None),
            "close" => return self.close().map(|()| Arg::None),
            "_read" => return me._read(first()),
            "_write" => return me._write(first(), second()).map(|()| Arg::None),
            "_delete" => return me._delete(first()).map(|()| Arg::None),
            "_exists" => Arg::Bool(me._exists(first())?),
            "_keys" => return me._keys(args),
            "_empty" => return me._empty().map(|()| Arg::None),
            "_candidate_keys" => return me._candidate_keys(first()),
            "_search_keys" => return me._search_keys(first(), second()),
            "_index_record" => return me._index_record(first(), second()).map(|()| Arg::None),
            "_index_remove" => return me._index_remove(first()).map(|()| Arg::None),
            "_resolve_indexed" => return me._resolve_indexed(first(), second()),
            // The async surface as awaitables (`await pool.write_async(k, v)`).
            "write_async"
            | "delete_async"
            | "_read_async"
            | "_write_async"
            | "_delete_async"
            | "_exists_async"
            | "_read_through_async" => {
                let key = first().__str__();
                let value = second();
                let name: String = name.into();
                return Ok(crate::python::stdlib::asyncio::coroutine(async move {
                    match name.as_str() {
                        "write_async" => me.write_async(&key, value).await.map(|()| Arg::None),
                        "delete_async" => me.delete_async(&key).await.map(|()| Arg::None),
                        "_read_async" => me._read_async(&key).await,
                        "_write_async" => me._write_async(&key, value).await.map(|()| Arg::None),
                        "_delete_async" => me._delete_async(&key).await.map(|()| Arg::None),
                        "_exists_async" => me._exists_async(&key).await.map(Arg::Bool),
                        _ => me._read_through_async(&key).await,
                    }
                }));
            }
            "_upstream_tiers" => me
                ._upstream_tiers()
                .into_iter()
                .map(Arg::Pool)
                .collect::<Vec<_>>()
                .into_arg(),
            "__getitem__" => return me.__getitem__(first()),
            "__setitem__" => return me.__setitem__(first(), second()).map(|()| Arg::None),
            "__delitem__" => return me.__delitem__(first()).map(|()| Arg::None),
            "__contains__" => Arg::Bool(me.__contains__(first())?),
            "__lshift__" => return me.__lshift__(pool_arg(first())?).map(Arg::Pool),
            "__rshift__" => return me.__rshift__(pool_arg(first())?).map(Arg::Pool),
            "__le__" => return me.__le__(first()),
            "atomic" => {
                return crate::atomic::definitions::locally_atomic_object::DynAtomicContext::new(
                    self.local_lock(),
                    self.class_name(),
                    args,
                )
                .map(IntoArg::into_arg)
            }
            "lock" => {
                let timeout = args
                    .positional
                    .first()
                    .cloned()
                    .unwrap_or_else(|| args.get("timeout_s"));
                let t = crate::atomic::definitions::locally_atomic_object::timeout_arg(&timeout)?;
                Arg::Bool(
                    crate::atomic::definitions::locally_atomic_object::lock_attrs(
                        self.__attrs__(),
                        t,
                    ),
                )
            }
            "unlock" => {
                crate::atomic::definitions::locally_atomic_object::unlock_attrs(self.__attrs__())?;
                Arg::None
            }
            "locked" => Arg::Bool(self.local_lock().locked()),
            _ => {
                if let Some(r) = self.backend.__call_method__(self, name, args.clone()) {
                    return r;
                }
                if let Some(v) = self.__dict__.get(name) {
                    return v.__call__(args);
                }
                return PyObject::__call_method__(self.inst(), name, args);
            }
        })
    }
    /// `repr(pool)` / `str(pool)` -- the global id.
    pub fn __repr__(&self) -> String {
        HasIdentity::global_id(self)
    }
}

fn pool_arg(a: Arg) -> Result<Pool> {
    match a {
        Arg::Pool(p) => Ok(p),
        other => Err(not_a_pool(&other)),
    }
}

fn not_a_pool(other: &Arg) -> LailaError {
    LailaError::attribute_error(alloc::format!(
        "'{}' object has no attribute '_proxy_to'",
        other.type_name()
    ))
}

impl HasAttrs for PoolObject {
    fn __attrs__(&self) -> &PyAttrs {
        &self.__dict__
    }
    fn __class_name__(&self) -> &'static str {
        self.class_name()
    }
}
impl HasIdentity for PoolObject {
    fn global_id(&self) -> String {
        HasIdentity::global_id(self.inst())
    }
    fn uuid(&self) -> String {
        HasIdentity::uuid(self.inst())
    }
}

// ---------------------------------------------------------------------------
// The handle

/// Handle to any pool (`Arc<PoolObject>`): clones alias one pool, as a
/// Python reference does. Derefs to [`PoolObject`] for the typed surface;
/// the methods below are the Python-shaped twins.
#[derive(Clone)]
pub struct Pool(pub Arc<PoolObject>);

impl core::ops::Deref for Pool {
    type Target = PoolObject;
    fn deref(&self) -> &PoolObject {
        &self.0
    }
}

impl HasAttrs for Pool {
    fn __attrs__(&self) -> &PyAttrs {
        &self.0.__dict__
    }
    fn __class_name__(&self) -> &'static str {
        self.0.class_name()
    }
}
impl HasIdentity for Pool {
    fn global_id(&self) -> String {
        HasIdentity::global_id(&*self.0)
    }
    fn uuid(&self) -> String {
        HasIdentity::uuid(&*self.0)
    }
}
impl _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT for Pool {}

impl Pool {
    /// Construct a pool of class *cls*: the pydantic pipeline
    /// (`laila.args` resolution, identity, field coercion, the
    /// persistent-storage check), then *make_backend* on the finished
    /// instance (read the fields it needs), then the backend's
    /// `model_post_init` on the handle.
    pub fn construct(
        cls: &'static ModelClass,
        args: Args,
        make_backend: impl FnOnce(&ModelInstance) -> Result<Box<dyn PoolBackend>>,
    ) -> Result<Pool> {
        crate::entry::__init__();
        let inst = ModelInstance::construct(cls, args)?;
        let backend = make_backend(ModelInstance::downcast(&inst).expect("model instance"))?;
        let pool = Pool(Arc::new_cyclic(|this| PoolObject {
            inst,
            backend,
            _proxy_to: RwLock::new(None),
            _index: RwLock::new(None),
            __dict__: PyAttrs::new(),
            this: this.clone(),
        }));
        pool.0.backend.model_post_init(&pool.0)?;
        Ok(pool)
    }
    /// `Cls.__new__(Cls)`: an instance whose `__init__` did not run -- the
    /// field defaults are in place, `model_post_init` (the connection /
    /// server start) is not. Tests drive the private methods on it directly.
    pub fn __new__(
        cls: &'static ModelClass,
        make_backend: impl FnOnce(&ModelInstance) -> Result<Box<dyn PoolBackend>>,
    ) -> Result<Pool> {
        crate::entry::__init__();
        let inst = ModelInstance::construct(cls, Args::new())?;
        let backend = make_backend(ModelInstance::downcast(&inst).expect("model instance"))?;
        Ok(Pool(Arc::new_cyclic(|this| PoolObject {
            inst,
            backend,
            _proxy_to: RwLock::new(None),
            _index: RwLock::new(None),
            __dict__: PyAttrs::new(),
            this: this.clone(),
        })))
    }
    /// The object behind the handle.
    pub fn obj(&self) -> &PoolObject {
        &self.0
    }
    /// `type(pool)` as a class object.
    pub fn __class__(&self) -> PyClass {
        self.0.class().as_pyclass()
    }
    /// `isinstance(pool, <Backend's pool class>)`: the backend, typed.
    pub fn downcast_ref<B: PoolBackend>(&self) -> Option<&B> {
        self.0.backend.as_any().downcast_ref::<B>()
    }
    /// Python `is`.
    pub fn is(&self, other: &Pool) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    /// `pool.global_id`
    pub fn global_id(&self) -> String {
        HasIdentity::global_id(self)
    }
    /// `pool.uuid`
    pub fn uuid(&self) -> String {
        HasIdentity::uuid(self)
    }
    /// `pool.scopes` (a list of str).
    pub fn scopes(&self) -> Arg {
        PyObject::__getattr__(self.0.inst(), "scopes").unwrap_or(Arg::None)
    }
    /// `pool.evolution` (`None` or an int).
    pub fn evolution(&self) -> Arg {
        PyObject::__getattr__(self.0.inst(), "evolution").unwrap_or(Arg::None)
    }
    /// `pool.nickname` (`None` or a str).
    pub fn nickname(&self) -> Arg {
        PyObject::__getattr__(self.0.inst(), "nickname").unwrap_or(Arg::None)
    }

    // ---- Python-shaped twins of the typed surface ----

    pub fn _read(&self, key: impl IntoArg) -> Result<Arg> {
        self.0._read(&key_str(&key.into_arg()))
    }
    pub fn _write(&self, key: impl IntoArg, value: impl IntoArg) -> Result<()> {
        self.0._write(&key_str(&key.into_arg()), value.into_arg())
    }
    pub fn _delete(&self, key: impl IntoArg) -> Result<()> {
        self.0._delete(&key_str(&key.into_arg()))
    }
    pub fn _exists(&self, key: impl IntoArg) -> Result<bool> {
        self.0._exists(&key_str(&key.into_arg()))
    }
    /// `pool._keys(as_generator=False)`: a list, or a generator over a
    /// snapshot when *as_generator*.
    #[pysig("_keys(as_generator=False)")]
    pub fn _keys(&self, args: impl Into<Args>) -> Result<Arg> {
        let keys = strs(self.0._keys()?);
        Ok(if as_generator.__bool__() {
            crate::python::builtins::Generator::new("_gen", keys).into_arg()
        } else {
            keys.into_arg()
        })
    }
    pub fn _empty(&self) -> Result<()> {
        self.0._empty()
    }
    pub fn _candidate_keys(&self, base_gid: impl IntoArg) -> Result<Arg> {
        Ok(strs(self.0._candidate_keys(&key_str(&base_gid.into_arg()))?).into_arg())
    }
    pub fn _search_keys(&self, base_gid: impl IntoArg, attributes: impl IntoArg) -> Result<Arg> {
        self.0
            ._search_keys(&key_str(&base_gid.into_arg()), &attributes.into_arg())
    }
    /// `pool.write(key, value)`: store *value* under *key* locally and index it.
    #[pysig("write(key, value)")]
    pub fn write(&self, args: impl Into<Args>) -> Result<()> {
        self.0.write(&key_str(&key), value)
    }
    pub fn delete(&self, key: impl IntoArg) -> Result<()> {
        self.0.delete(&key_str(&key.into_arg()))
    }
    /// `pool.exists(key)`: local-only existence check.
    #[pysig("exists(key)")]
    pub fn exists(&self, args: impl Into<Args>) -> Result<bool> {
        self.0.exists(&key_str(&key))
    }
    /// `pool.keys(as_generator=False, include_index=False)`: the keys
    /// stored in this pool (local-only). A snapshot list, or a generator
    /// over the snapshot when *as_generator*; the pool's own index
    /// bookkeeping keys (`LAILA:POOL_INDEX:...`) only with *include_index*.
    #[pysig("keys(as_generator=False, include_index=False)")]
    pub fn keys(&self, args: impl Into<Args>) -> Result<Arg> {
        let keys = strs(self.0.keys_vec(include_index.__bool__())?);
        Ok(if as_generator.__bool__() {
            crate::python::builtins::Generator::new("<genexpr>", keys).into_arg()
        } else {
            keys.into_arg()
        })
    }
    pub fn empty(&self, _args: impl Into<Args>) -> Result<()> {
        self.0.empty()
    }
    pub fn sync(&self, _args: impl Into<Args>) -> Result<()> {
        self.0.sync()
    }
    pub fn close(&self, _args: impl Into<Args>) -> Result<()> {
        self.0.close()
    }
    pub fn __getitem__(&self, key: impl IntoArg) -> Result<Arg> {
        self.0.__getitem__(&key.into_arg())
    }
    pub fn __setitem__(&self, key: impl IntoArg, value: impl IntoArg) -> Result<()> {
        self.0.__setitem__(&key.into_arg(), value.into_arg())
    }
    pub fn __delitem__(&self, key: impl IntoArg) -> Result<()> {
        self.0.__delitem__(&key.into_arg())
    }
    pub fn __contains__(&self, key: impl IntoArg) -> Result<bool> {
        self.0.__contains__(&key.into_arg())
    }
    pub fn __lshift__(&self, other: impl AsPool) -> Result<Pool> {
        self.0.__lshift__(&other.as_pool())
    }
    pub fn __rshift__(&self, other: impl AsPool) -> Result<Pool> {
        self.0.__rshift__(&other.as_pool())
    }
    pub fn __le__(&self, other: impl IntoArg) -> Result<Arg> {
        self.0.__le__(&other.into_arg())
    }
    pub fn _index_record(&self, key: impl IntoArg, value: impl IntoArg) -> Result<()> {
        self.0
            ._index_record(&key_str(&key.into_arg()), &value.into_arg())
    }
    pub fn _index_remove(&self, key: impl IntoArg) -> Result<()> {
        self.0._index_remove(&key_str(&key.into_arg()))
    }
    pub fn _resolve_indexed(
        &self,
        base_gid: impl IntoArg,
        attributes: impl IntoArg,
    ) -> Result<Arg> {
        self.0
            ._resolve_indexed(&key_str(&base_gid.into_arg()), &attributes.into_arg())
    }
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        self.0.__getattr__(name)
    }
    pub fn __setattr__(&self, name: &str, value: impl IntoArg) -> Result<()> {
        self.0.__setattr__(name, value.into_arg())
    }
    pub fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        self.0.__call_method__(name, args)
    }
    pub fn __repr__(&self) -> String {
        self.0.__repr__()
    }
    pub fn __str__(&self) -> String {
        self.0.__repr__()
    }
    /// `hash(pool)` -> `hash(global_id)`.
    pub fn __hash__(&self) -> Result<i64> {
        Arg::Str(self.global_id().into()).__hash__()
    }
    /// `pool == other` -- same global id.
    pub fn __eq__(&self, other: &Pool) -> bool {
        self.global_id() == other.global_id()
    }
}

fn strs(v: Vec<String>) -> Vec<Arg> {
    v.into_iter().map(|s| Arg::Str(s.into())).collect()
}

impl PyArg for Pool {
    fn to_arg(&self) -> Arg {
        Arg::Pool(self.clone())
    }
}
impl IntoArg for Pool {
    fn into_arg(self) -> Arg {
        Arg::Pool(self)
    }
}
impl core::fmt::Debug for Pool {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__repr__())
    }
}
impl PartialEq for Pool {
    fn eq(&self, other: &Pool) -> bool {
        self.__eq__(other)
    }
}

/// Anything that can stand on the right of `<<` / `>>` or be passed where a pool is expected.
pub trait AsPool {
    fn as_pool(&self) -> Pool;
}
impl AsPool for Pool {
    fn as_pool(&self) -> Pool {
        self.clone()
    }
}
impl<T: AsPool + ?Sized> AsPool for &T {
    fn as_pool(&self) -> Pool {
        (**self).as_pool()
    }
}

/// `&cache << &origin` for typed Rust: the fallible [`Pool::__lshift__`]
/// behind the operator. A proxy cycle is a programming error here and
/// panics with the `ValueError` text; Python-shaped code gets the error
/// from `__lshift__`.
impl<'b, B: AsPool> core::ops::Shl<&'b B> for &Pool {
    type Output = &'b B;
    fn shl(self, rhs: &'b B) -> &'b B {
        self.__lshift__(rhs).expect("proxy chain");
        rhs
    }
}
/// `&origin >> &cache` (see `Shl`).
impl<'b, B: AsPool> core::ops::Shr<&'b B> for &Pool {
    type Output = &'b B;
    fn shr(self, rhs: &'b B) -> &'b B {
        self.__rshift__(rhs).expect("proxy chain");
        rhs
    }
}

/// Declares a concrete pool handle: a newtype over [`Pool`] with `Deref`,
/// `Shl` / `Shr`, [`AsPool`], `IntoArg` and `isinstance` support
/// (`isinstance(x, HDF5Pool)` tests the backend type `$backend`).
#[macro_export]
macro_rules! laila_pool_handle {
    ($(#[$m:meta])* $name:ident, $backend:ty) => {
        $(#[$m])*
        #[derive(Clone)]
        pub struct $name(pub $crate::data::Pool);
        impl core::ops::Deref for $name {
            type Target = $crate::data::Pool;
            fn deref(&self) -> &$crate::data::Pool {
                &self.0
            }
        }
        impl $crate::data::AsPool for $name {
            fn as_pool(&self) -> $crate::data::Pool {
                self.0.clone()
            }
        }
        impl<'b, B: $crate::data::AsPool> core::ops::Shl<&'b B> for &$name {
            type Output = &'b B;
            fn shl(self, rhs: &'b B) -> &'b B {
                self.0.__lshift__(rhs).expect("proxy chain");
                rhs
            }
        }
        impl<'b, B: $crate::data::AsPool> core::ops::Shr<&'b B> for &$name {
            type Output = &'b B;
            fn shr(self, rhs: &'b B) -> &'b B {
                self.0.__rshift__(rhs).expect("proxy chain");
                rhs
            }
        }
        impl $crate::python::PyArg for $name {
            fn to_arg(&self) -> $crate::python::Arg {
                $crate::python::Arg::Pool(self.0.clone())
            }
        }
        impl $crate::python::IntoArg for $name {
            fn into_arg(self) -> $crate::python::Arg {
                $crate::python::Arg::Pool(self.0)
            }
        }
        impl $crate::python::builtins::IsInstance for $name {
            fn isinstance(x: &$crate::python::Arg) -> bool {
                matches!(x, $crate::python::Arg::Pool(p) if p.downcast_ref::<$backend>().is_some())
            }
        }
        /// `key in pool` (local-only, like `_LAILA_IDENTIFIABLE_POOL.__contains__`).
        impl $crate::python::builtins::PyContains for $name {
            fn py_contains(&self, item: $crate::python::Arg) -> $crate::Result<bool> {
                self.0.__contains__(item)
            }
        }
        impl core::fmt::Debug for $name {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                f.write_str(&self.0.__repr__())
            }
        }
    };
}
