//! Entry sub-package: the [`Entry`] data unit and ready-made transformation pipelines.
//!
//! The [`Entry`] class is the fundamental data unit of LAILA -- it wraps any
//! Python object (tensor, image, dict, dataset, etc.) with a stable identity
//! (UUID + optional evolution counter), an explicit lifecycle state, and
//! either a concrete payload or a `Constitution` describing how to
//! materialize one.
//!
//! An `Entry` couples three concerns into one identifiable object:
//!
//! 1. **Identity.** Each entry has a UUID (random, explicit, or
//!    nickname-derived), a list of hierarchical scopes, and an optional
//!    *evolution* counter. Together they form a canonical `global_id`
//!    string that makes the entry addressable across processes and
//!    machines (see `_LAILA_IDENTIFIABLE_OBJECT` / [`Identity`]).
//! 2. **Lifecycle state.** A small [`EntryState`] enum captures whether the
//!    entry's payload is materialized (`READY`), staged for build
//!    (`STAGED`), in flight to a pool (`POOLING`), pooled (`POOLED`),
//!    stale, or "not applicable" for subclasses that don't carry a payload
//!    (`NA`).
//! 3. **Either** a concrete payload (wrapped in a [`ComputationalData`] for
//!    type-aware serialization) **or** a [`Constitution`] describing how to
//!    build the payload. The two are mutually exclusive at construction
//!    time but may both be transiently present during a build.
//!
//! Two flavors
//! -----------
//! - **Constants** (`Entry.constant(data, ...)`): immutable. The
//!   `evolution` field is `None` and `evolve()` raises.
//! - **Variables** (`Entry.variable(data, ...)`): mutable in place via
//!   [`Entry::evolve`] which bumps the `evolution` counter. Two entries
//!   with the same UUID but different evolutions denote different versions
//!   of the same logical thing.
//!
//! Build paths
//! -----------
//! When created from a constitution, an entry starts `STAGED` and must be
//! materialized before its `data` can be read:
//!
//! - `laila.build(entry).wait()` -- sync caller; returns the same entry
//!   with `state=READY`.
//! - `laila.build(entry, asynchronous=True)` / [`Entry::_build_async`] --
//!   async caller; non-blocking.
//! - `laila.remember(entry_id)` -- if the entry is already memorized,
//!   fetching it routes through the pool's `SimpleConstitution` (the
//!   inverse-transformation chain) and returns a `READY` entry.
//!
//! This module also re-exports the pre-built [`TransformationSequence`]
//! presets Python defines in `laila/entry/__init__.py`
//! (`transformation_base64`, `transformation_base64_compression`,
//! `transformation_base64_compression_encryption(key=None)`,
//! `transformation_encryption(key=None)`). They are functions returning a
//! fresh pipeline each time (a Rust static cannot hold the lock-bearing
//! value); `laila::python!` reads the bare name as the value.
//!
//! Rust shape: `Entry` is a cheap `Clone` handle (`Arc`); clones alias the
//! same object, as a Python reference does. `Manifest` -- the one `Entry`
//! subclass -- is the same handle with [`EntryKind::Manifest`] as its class
//! tag (see `policy::central::memory::schema::manifest`), so `Arg::Entry`
//! stays the only dynamic representation and `isinstance(x, Manifest)`,
//! `type(x).__name__` and the subclass-specific behaviour dispatch on the
//! tag.

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::any::Any;
use core::sync::atomic::{AtomicBool, Ordering};

use crate::atomic::definitions::locally_atomic_object::ensure_local_lock;
use crate::atomic::definitions::{HasIdentity, _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT};
use crate::basics::definitions::identifiable_object::{
    is_valid_uuid, process_global_id_arg, Identity,
};
use crate::basics::definitions::laila_object::_now_creation_timestamp;
use crate::macros::strings::{_ENTRY_SCOPE, _MANIFEST_SCOPE, _POOL_INDEX_SCOPE};
use crate::python::builtins::IsInstance;
use crate::python::object::{HasAttrs, PyAttrs, PyObject};
use crate::python::sync::{Mutex, RLock, RwLock};
use crate::python::{pysig, Arg, Args, ExcKind, IntoArg, LailaError, PyArg, PyDictBuilder, Result};

pub mod compdata;
pub mod constitution;
pub mod entry_metadata;
pub mod entry_state;
pub mod exceptions;

/// `laila.entry.entry` -- the module the class lives in (`from laila.entry.entry import Entry`).
#[allow(clippy::module_inception)]
pub mod entry {
    pub use super::{Entry, EntryKind};
}

pub use compdata::transformation::{
    transformation_base64, transformation_base64_compression,
    transformation_base64_compression_encryption, transformation_encryption, Base64,
    FernetEncryption, JsonString, MsgpackSerializer, NumpySerializer, PickleSerializer,
    TorchSerializer, Transformation, TransformationSequence, Zlib,
};
pub use compdata::ComputationalData;
pub use constitution::{ComplexConstitution, Constitution, SimpleConstitution};
pub use entry_metadata::{EntryHolisticView, EntryIdentityView};
pub use entry_state::EntryState;
pub use exceptions::EntryNotBuiltError;

// ---------------------------------------------------------------------------
// Module initialisation (Python runs it at import time)

static INITIALISED: Mutex<bool> = Mutex::new(false);

/// The import-time side effects of `laila/entry/entry.py` and
/// `laila/policy/central/memory/schema/manifest.py`: the `ENTRY`,
/// `POOL_INDEX` and `MANIFEST` scope builders in `build_maps.BUILDER_MAP`
/// and the `Manifest` hooks of `ComplexConstitution`. Idempotent; called
/// by every entry point that depends on them (`Entry` construction,
/// `build_by_scope`, `ComplexConstitution.manifest = ...`).
pub fn __init__() {
    {
        let mut g = INITIALISED.lock();
        if *g {
            return;
        }
        *g = true;
    }
    use constitution::build_maps::register_builder;
    let sync_entry: constitution::build_maps::SyncBuilder =
        Arc::new(|d, _kw| Ok(Arg::Entry(Entry::_build_from_dict_sync(d)?)));
    let async_entry: constitution::build_maps::AsyncBuilder = Arc::new(|d, _kw| {
        Ok(crate::python::stdlib::asyncio::coroutine(async move {
            Entry::_build_from_dict_async(d).await.map(Arg::Entry)
        }))
    });
    register_builder(_ENTRY_SCOPE, sync_entry.clone(), async_entry.clone());
    // Pool index shards (data/schema/pool_index.py) are plain entries whose
    // payload is a dict; registering them keeps a raw read of a shard from
    // tripping ``build_by_scope``.
    register_builder(_POOL_INDEX_SCOPE, sync_entry, async_entry);
    crate::policy::central::memory::schema::manifest::__init__();
}

// ---------------------------------------------------------------------------
// The class tag

/// The `Entry` class hierarchy as a tag: `type(entry)` is either `Entry`
/// itself or its `Manifest` subclass. Class-level attributes
/// (`_DEFAULT_SCOPES`, `_ALLOWS_NON_NA_STATE`) are read through it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EntryKind {
    /// `laila.entry.entry.Entry`
    Entry,
    /// `laila.policy.central.memory.schema.manifest.Manifest`
    Manifest,
}

impl EntryKind {
    /// `cls.__name__`
    pub fn __name__(self) -> &'static str {
        match self {
            EntryKind::Entry => "Entry",
            EntryKind::Manifest => "Manifest",
        }
    }
    /// `cls.__module__`
    pub fn __module__(self) -> &'static str {
        match self {
            EntryKind::Entry => "laila.entry.entry",
            EntryKind::Manifest => "laila.policy.central.memory.schema.manifest",
        }
    }
    /// `cls._DEFAULT_SCOPES`
    pub fn _DEFAULT_SCOPES(self) -> &'static [&'static str] {
        match self {
            EntryKind::Entry => &[_ENTRY_SCOPE],
            EntryKind::Manifest => &[_MANIFEST_SCOPE],
        }
    }
    /// `cls._ALLOWS_NON_NA_STATE` -- `True` on `Entry` and on `Manifest`
    /// (a manifest is a normal `READY` entry whose payload is its blueprint).
    pub fn _ALLOWS_NON_NA_STATE(self) -> bool {
        match self {
            EntryKind::Entry | EntryKind::Manifest => true,
        }
    }
    /// `cls` as a `PyClass` value (`type(entry)`).
    pub fn __class__(self) -> crate::python::PyClass {
        crate::python::PyClass::with_ctor(
            self.__name__(),
            self.__module__(),
            Arc::new(move |a: Args| Entry::__init__(self, a).map(Arg::Entry)),
        )
    }
}

// ---------------------------------------------------------------------------
// The object

struct EntryInner {
    kind: EntryKind,
    /// `_uuid`, `_scopes`, `_evolution`, `_creation_timestamp`.
    identity: RwLock<Identity>,
    _state: RwLock<EntryState>,
    _constitution: RwLock<Option<Constitution>>,
    _payload: RwLock<Option<ComputationalData>>,
    // Process-local "payload changed since the last memorize" marker. Set
    // by the ``data`` setter, cleared by ``mark_memorized`` (called from
    // central memory after a successful write) and reset to ``False`` at
    // the end of construction / deserialization / build so that only
    // *user* mutations count. Never serialized.
    _locally_modified: AtomicBool,
    // Process-local hint: the ``global_id`` of the pool this entry was last
    // read from or written to. ``Manifest.realized`` and constitution
    // builds use it to look for referenced entries in the same pool
    // before falling back to the alpha pool and the rest of the router,
    // so a manifest memorized into a named pool can be realized. Never
    // serialized.
    _source_pool_gid: RwLock<Option<String>>,
    /// The instance `__dict__`: `_local_lock` (the atomic mixin), the
    /// subclass private attributes (`Manifest._pending_entries`) and any
    /// `object.__setattr__`-style private attribute.
    __dict__: PyAttrs,
}

/// The fundamental unit of data in LAILA.
///
/// An `Entry` wraps arbitrary Python data (tensors, images, dicts,
/// DataFrames, Manifests, etc.) with three things:
///
/// - **Identity** -- UUID + scopes + optional evolution counter; see
///   [`Identity`].
/// - **State** -- an [`EntryState`] reflecting where the entry is in its
///   lifecycle (`READY` / `STAGED` / `POOLING` / `POOLED` / `STALE` / `NA`).
/// - **Payload XOR Constitution** -- either a materialized
///   [`ComputationalData`] payload, or a [`Constitution`] describing how to
///   build one (see `laila.build`).
///
/// Mutability
/// ----------
/// - Use [`Entry::constant`] to create an *immutable* entry whose
///   `evolution` is `None`. [`Entry::evolve`] raises on these.
/// - Use [`Entry::variable`] to create a *mutable* entry. Each call to
///   [`Entry::evolve`] bumps `evolution` by 1 and replaces the payload,
///   leaving the UUID stable.
///
/// Public construction
/// -------------------
/// Prefer the class-method factories rather than the raw constructor:
///
/// ```text
/// const = Entry.constant({"x": 1}, nickname="config")
/// var   = Entry.variable(np.zeros(8), evolution=0)
/// lazy  = Entry.variable(constitution=src, manifest=m)
/// ```
///
/// Each factory enforces the right combination of identity / payload /
/// constitution kwargs and gives clearer errors than the constructor.
///
/// Thread safety
/// -------------
/// All mutators (data setter, constitution setter, state setter,
/// [`evolve`](Entry::evolve)) are `synchronized` and run under the entry's
/// per-instance `atomic` lock (`_local_lock`, the
/// `_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT` mixin), so concurrent
/// threads can read and update the same entry without external locking.
#[derive(Clone)]
pub struct Entry(Arc<EntryInner>);

/// `_ALLOWS_NON_NA_STATE` of the base class.
pub const _ALLOWS_NON_NA_STATE: bool = true;
/// `_DEFAULT_SCOPES` of the base class.
pub const _DEFAULT_SCOPES: &[&str] = &[_ENTRY_SCOPE];

/// `with self.atomic(scope="local"):` -- the re-entrant local lock held
/// for the lifetime of the guard (`@synchronized` on a single receiver).
struct Held(RLock);
impl Held {
    fn acquire(lock: &RLock) -> Held {
        lock.acquire(None);
        Held(lock.clone())
    }
}
impl Drop for Held {
    fn drop(&mut self) {
        let _ = self.0.release();
    }
}

fn opt_str(a: &Arg) -> Option<String> {
    match a {
        Arg::None => None,
        other => Some(other.__str__()),
    }
}

fn strs_to_arg(v: &[String]) -> Arg {
    v.iter()
        .map(|s| Arg::Str(s.as_str().into()))
        .collect::<Vec<_>>()
        .into_arg()
}

impl Entry {
    /// `Entry.resolve_global_id(ref, *, evolution=None, prefix_scopes=None, default_scopes=None, parse_evolution=True)`:
    /// the identifiable-object classmethod with the entry's default scope.
    #[pysig("resolve_global_id(ref, *, evolution=None, prefix_scopes=None, default_scopes=None, parse_evolution=True)")]
    pub fn resolve_global_id(args: impl Into<Args>) -> Result<String> {
        crate::basics::definitions::identifiable_object::resolve_global_id_bound(
            EntryKind::Entry._DEFAULT_SCOPES(),
            &r#ref,
            &evolution,
            &prefix_scopes,
            &default_scopes,
            &parse_evolution,
        )
    }

    // ---------------------------------------------------------------------
    // Construction

    /// `Entry(**data)`: initialise an Entry from keyword arguments.
    ///
    /// Most callers should use the higher-level factories
    /// [`Entry::constant`] / [`Entry::variable`] / [`Entry::contingent`]
    /// instead -- they validate argument combinations and provide clearer
    /// errors. The raw constructor is only invoked directly by
    /// deserialization paths ([`from_dict`](Entry::from_dict), the build
    /// pipeline, etc.).
    ///
    /// Initialisation is split across four helpers so subclasses can
    /// override individual phases without rewriting the whole constructor:
    ///
    /// 1. `_initialize_identity` parses `global_id` / `uuid` / `evolution`
    ///    / `nickname` / `scopes` and threads them through the parent
    ///    identifiable-object machinery.
    /// 2. `_initialize_payload` wraps any `data` / `payload` value in a
    ///    [`ComputationalData`] (subclass dispatched by payload type).
    /// 3. `_initialize_constitution` accepts a live [`Constitution`], a
    ///    list of source strings (auto-wrapped in [`SimpleConstitution`]),
    ///    or a single source string plus a `manifest` (auto-wrapped in
    ///    [`ComplexConstitution`]).
    /// 4. `_initialize_state` picks the initial [`EntryState`] -- `STAGED`
    ///    when a constitution is attached, otherwise the explicit `state`
    ///    kwarg or `STAGED`.
    ///
    /// Accepted keys (all optional unless required by the chosen factory):
    /// `data` / `payload` (raw payload value, wrapped automatically),
    /// `uuid` (explicit UUID string, mutually exclusive with `global_id`),
    /// `evolution` (integer counter, or `None` for constants), `global_id`
    /// (composite `"LAILA:<scopes>:<uuid>[@evolution=<n>]"` string),
    /// `nickname` (deterministically converted to a UUID-5 against the
    /// active namespace), `scopes` (override the default `[ENTRY]` scope
    /// list), `constitution` (a [`Constitution`], a `list[str]` ->
    /// `SimpleConstitution`, or a `str` -> `ComplexConstitution`, requires
    /// `manifest`), `manifest` (a `Manifest` to bind to a complex
    /// constitution), `state` (initial [`EntryState`]).
    #[pysig("Entry(**data)")]
    pub fn new(args: impl Into<Args>) -> Result<Entry> {
        Self::__init__(EntryKind::Entry, data)
    }

    /// `cls.__new__(cls)`: an uninitialised instance of `kind` (identity,
    /// payload, state and constitution are all unset; `from_dict` fills
    /// them in explicitly).
    pub(crate) fn __new__(kind: EntryKind) -> Entry {
        __init__();
        Entry(Arc::new(EntryInner {
            kind,
            identity: RwLock::new(Identity {
                uuid: String::new(),
                scopes: Vec::new(),
                evolution: None,
                nickname: None,
                creation_timestamp: None,
            }),
            _state: RwLock::new(EntryState::STAGED),
            _constitution: RwLock::new(None),
            _payload: RwLock::new(None),
            _locally_modified: AtomicBool::new(false),
            _source_pool_gid: RwLock::new(None),
            __dict__: PyAttrs::new(),
        }))
    }

    /// `Entry.__init__(self, **data)` on a fresh instance of `kind`.
    pub(crate) fn __init__(kind: EntryKind, data: Args) -> Result<Entry> {
        let this = Self::__new__(kind);
        this._initialize_identity(&data)?;
        this._initialize_payload(&data)?;
        this._initialize_constitution(&data)?;
        this._initialize_state(&data)?;
        // The initial payload is the baseline, not a change.
        this.0._locally_modified.store(false, Ordering::SeqCst);
        Ok(this)
    }

    /// Parse identity fields from *data* and initialise the parent identity.
    ///
    /// `global_id` is preferred over the (uuid, evolution, scopes) triple
    /// when present -- it is parsed via
    /// `_LAILA_IDENTIFIABLE_OBJECT.process_global_id` and the decoded
    /// fields populate the identity directly. Otherwise the explicit
    /// `uuid`, `evolution`, `nickname`, and `scopes` kwargs are forwarded to
    /// the parent constructor (which itself applies nickname-to-UUID-5
    /// derivation when relevant).
    pub fn _initialize_identity(&self, data: &Args) -> Result<()> {
        let defaults = strs_to_arg(
            &self
                .kind()
                ._DEFAULT_SCOPES()
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
        );
        let global_id = data.get("global_id");
        let mut identity_data = Args::new();
        identity_data.push_keyword("default_scopes", defaults);
        if !global_id.is_none() {
            let parsed = process_global_id_arg(&global_id)?;
            identity_data.push_keyword("uuid", parsed.uuid);
            identity_data.push_keyword("scopes", strs_to_arg(&parsed.scopes));
            identity_data.push_keyword("evolution", parsed.evolution);
            *self.0.identity.write() = Identity::from_args(identity_data)?;
            return Ok(());
        }
        for key in ["uuid", "evolution", "nickname", "scopes"] {
            let v = data.get(key);
            if !v.is_none() {
                identity_data.push_keyword(key, v);
            }
        }
        *self.0.identity.write() = Identity::from_args(identity_data)?;
        Ok(())
    }

    /// Set the entry's payload from `payload` or `data` kwargs.
    ///
    /// Accepts either keyword for symmetry with the serialized
    /// representation (`payload` is the on-disk name) and the in-memory
    /// convention (`data`). The setter wraps the raw value in a
    /// [`ComputationalData`] of the appropriate subclass.
    pub fn _initialize_payload(&self, data: &Args) -> Result<()> {
        let value = if data.__contains__("payload")? {
            data.get("payload")
        } else {
            data.get("data")
        };
        self.set_data(value)
    }

    /// Set up a [`Constitution`] if one was provided.
    ///
    /// Three input shapes are normalized into the right subclass: a live
    /// `Constitution` is used as-is (a `ComplexConstitution` whose
    /// `manifest` is unset picks up a separate `manifest` kwarg); a
    /// `list[str]` is wrapped in a [`SimpleConstitution`]; a single `str`
    /// is the source of a [`ComplexConstitution`]'s callable (the optional
    /// `manifest` kwarg is forwarded along).
    ///
    /// `TypeError` if `constitution` is not a `Constitution`, `list[str]`,
    /// or `str`.
    pub fn _initialize_constitution(&self, data: &Args) -> Result<()> {
        let constitution = data.get("constitution");
        let manifest = data.get("manifest");
        if constitution.is_none() {
            return Ok(());
        }
        if let Some(c) = Constitution::from_arg(&constitution) {
            if !manifest.is_none() {
                if let Constitution::Complex(cc) = &c {
                    if cc.manifest().is_none() {
                        cc.set_manifest(manifest)?;
                    }
                }
            }
            return self.set_constitution(c.into_arg());
        }
        if let Arg::List(_) = &constitution {
            let mut a = Args::new();
            a.push_keyword("codes", constitution);
            return self.set_constitution(SimpleConstitution::new(a)?.into_arg());
        }
        if let Arg::Str(_) = &constitution {
            let mut kwargs = Args::new();
            kwargs.push_keyword("code", constitution);
            if !manifest.is_none() {
                kwargs.push_keyword("manifest", manifest);
            }
            return self.set_constitution(ComplexConstitution::new(kwargs)?.into_arg());
        }
        Err(LailaError::type_error(
            "constitution must be a Constitution, list[str], or str",
        ))
    }

    /// Determine the initial [`EntryState`] from *data*.
    ///
    /// - Subclasses with `_ALLOWS_NON_NA_STATE = False` are pinned to
    ///   `EntryState.NA` regardless of input.
    /// - When a constitution is attached, the entry starts `STAGED` (its
    ///   payload must be built before access). The user's explicit `state`
    ///   kwarg is ignored in this case.
    /// - Otherwise the explicit `state` kwarg wins, defaulting to `STAGED`
    ///   if absent (`Entry.constant` / `Entry.variable` override this to
    ///   `READY` for the common case of inline data).
    pub fn _initialize_state(&self, data: &Args) -> Result<()> {
        if !self.kind()._ALLOWS_NON_NA_STATE() {
            return self.set_state(EntryState::NA);
        }
        if !data.get("constitution").is_none() {
            return self.set_state(EntryState::STAGED);
        }
        if data.__contains__("state")? {
            self.set_state(data.get("state"))
        } else {
            self.set_state(EntryState::STAGED)
        }
    }

    // ---------------------------------------------------------------------
    // Identity

    /// `type(self)` as a tag.
    pub fn kind(&self) -> EntryKind {
        self.0.kind
    }

    /// `type(self).__name__`
    pub fn __class_name__(&self) -> &'static str {
        self.0.kind.__name__()
    }

    /// A snapshot of the identity triple plus creation stamp.
    pub fn identity_value(&self) -> Identity {
        self.0.identity.read().clone()
    }

    /// `self._uuid`
    pub fn uuid(&self) -> String {
        self.0.identity.read().uuid.clone()
    }
    /// `self._scopes` -> `["ENTRY"]`
    pub fn scopes(&self) -> Arg {
        strs_to_arg(&self.0.identity.read().scopes)
    }
    /// `self._scopes` as Rust strings.
    pub fn scopes_vec(&self) -> Vec<String> {
        self.0.identity.read().scopes.clone()
    }
    /// `self._evolution` as a Python value.
    pub fn evolution(&self) -> Arg {
        self.0.identity.read().evolution.into_arg()
    }
    /// `self._evolution`
    pub fn evolution_opt(&self) -> Option<i64> {
        self.0.identity.read().evolution
    }
    /// `self.global_id` -> `LAILA:<scopes>:<uuid>[@evolution=<n>]`
    pub fn global_id(&self) -> String {
        self.0.identity.read().global_id()
    }
    /// `self.global_id = value`: re-parse and re-assign uuid / scopes / evolution.
    pub fn set_global_id(&self, value: impl IntoArg) -> Result<()> {
        let parsed = process_global_id_arg(&value.into_arg())?;
        let mut id = self.0.identity.write();
        id.uuid = parsed.uuid;
        id.scopes = parsed.scopes;
        id.evolution = parsed.evolution;
        Ok(())
    }
    /// `self.creation_timestamp` (from `_LAILA_OBJECT`): the ISO-8601 UTC
    /// stamp recorded at construction (`None` only on a bare `__new__`).
    pub fn creation_timestamp(&self) -> Arg {
        self.0.identity.read().creation_timestamp.clone().into_arg()
    }
    /// `self.has_evolution()`
    pub fn has_evolution(&self) -> bool {
        self.0.identity.read().has_evolution()
    }
    /// `self.identity()`
    pub fn identity(&self) -> Arg {
        self.0.identity.read().identity()
    }
    /// `self.identity_as_json()`
    pub fn identity_as_json(&self) -> Result<String> {
        self.0.identity.read().identity_as_json()
    }

    // ---------------------------------------------------------------------
    // Properties

    /// `self._local_lock` (created on first use).
    fn local_lock(&self) -> RLock {
        ensure_local_lock(&self.0.__dict__)
    }

    /// `self.data`: the unwrapped payload value (or `None` if unset).
    ///
    /// This getter is **read-only** -- it never triggers an implicit build,
    /// even when a constitution is attached (deadlock-avoidance: a
    /// taskforce loop thread must not block on its own loop). Materialize
    /// explicitly with `laila.build(entry).wait()` /
    /// `laila.build(entry, asynchronous=True)` / `laila.remember(entry_id)`.
    ///
    /// `EntryNotBuiltError` if a constitution is attached but the entry
    /// has not been built (no payload yet).
    pub fn data(&self) -> Result<Arg> {
        let _held = Held::acquire(&self.local_lock());
        if let Some(p) = &*self.0._payload.read() {
            return Ok(p.data());
        }
        if self.0._constitution.read().is_some() {
            return Err(LailaError::new(
                ExcKind::EntryNotBuiltError,
                alloc::format!(
                    "Entry {} is not built. Use `laila.build(entry).wait()` (or \
                     `await laila.build(entry, asynchronous=True)`) to materialize it \
                     before accessing .data.",
                    self.global_id()
                ),
            ));
        }
        Ok(Arg::None)
    }

    /// `self.data = new_data`: replace the payload value.
    ///
    /// Raw Python values are auto-wrapped in the appropriate
    /// [`ComputationalData`] subclass; already-wrapped values pass through
    /// unchanged. Pass `None` to clear the payload entirely. Synchronized
    /// on the entry's atomic lock.
    ///
    /// Assigning marks the entry *locally modified*: the next
    /// `laila.memorize` of a variable entry will bump its evolution (see
    /// [`bump_evolution_if_locally_modified`](Entry::bump_evolution_if_locally_modified)).
    /// In-place mutation of the payload object is not observed; re-assign
    /// `entry.data = value` to flag the change.
    pub fn set_data(&self, new_data: impl IntoArg) -> Result<()> {
        let new_data = new_data.into_arg();
        let _held = Held::acquire(&self.local_lock());
        let wrapped = if new_data.is_none() {
            None
        } else if let Some(cd) = ComputationalData::from_arg(&new_data) {
            Some(cd)
        } else {
            Some(ComputationalData::new(new_data)?)
        };
        *self.0._payload.write() = wrapped;
        self.0._locally_modified.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// The wrapped payload, if any (typed).
    pub fn payload_opt(&self) -> Option<ComputationalData> {
        self.0._payload.read().clone()
    }

    /// `self._payload`: the wrapped `ComputationalData`, or `None`.
    pub fn _payload(&self) -> Arg {
        match self.payload_opt() {
            Some(p) => p.into_arg(),
            None => Arg::None,
        }
    }

    /// `self.locally_modified`: `True` when the payload was re-assigned
    /// since the last memorize. Process-local bookkeeping only -- never
    /// serialized, never restored from a pool.
    pub fn locally_modified(&self) -> bool {
        self.0._locally_modified.load(Ordering::SeqCst)
    }

    /// Clear the locally-modified flag. Called by central memory after a successful write.
    pub fn mark_memorized(&self) {
        self.0._locally_modified.store(false, Ordering::SeqCst);
    }

    /// Advance `evolution` in place if the payload changed since the last memorize.
    ///
    /// This is the hook `laila.memorize` uses to decide whether a variable
    /// entry is being re-written as the *same* evolution (payload untouched
    /// -> idempotent overwrite of the same key) or as a *new* one (payload
    /// re-assigned -> `evolution += 1` and a fresh `creation_timestamp`).
    /// Constants (`evolution is None`) are never bumped.
    ///
    /// Unlike [`evolve`](Entry::evolve), this mutates `self`: its
    /// `global_id` (and therefore its hash) changes.
    ///
    /// Returns `True` if the evolution was incremented.
    pub fn bump_evolution_if_locally_modified(&self) -> bool {
        let _held = Held::acquire(&self.local_lock());
        let mut id = self.0.identity.write();
        let Some(e) = id.evolution else {
            return false;
        };
        if !self.locally_modified() {
            return false;
        }
        id.evolution = Some(e + 1);
        id.creation_timestamp = Some(_now_creation_timestamp());
        true
    }

    /// `self.constitution`: the attached [`Constitution`], or `None` once
    /// the entry has been built.
    pub fn constitution(&self) -> Arg {
        let _held = Held::acquire(&self.local_lock());
        match &*self.0._constitution.read() {
            Some(c) => c.clone().into_arg(),
            None => Arg::None,
        }
    }

    /// `self._constitution` as the typed enum.
    pub fn constitution_opt(&self) -> Option<Constitution> {
        self.0._constitution.read().clone()
    }

    /// `self.constitution = new_constitution`: attach or clear the entry's
    /// [`Constitution`]. Pass `None` to detach.
    ///
    /// `TypeError` if *new_constitution* is neither a `Constitution` nor `None`.
    pub fn set_constitution(&self, new_constitution: impl IntoArg) -> Result<()> {
        let new_constitution = new_constitution.into_arg();
        let _held = Held::acquire(&self.local_lock());
        let value = if new_constitution.is_none() {
            None
        } else {
            match Constitution::from_arg(&new_constitution) {
                Some(c) => Some(c),
                None => {
                    return Err(LailaError::type_error(
                        "constitution must be a Constitution or None",
                    ))
                }
            }
        };
        *self.0._constitution.write() = value;
        Ok(())
    }

    /// `self.state`: current [`EntryState`] of this Entry.
    pub fn state(&self) -> EntryState {
        let _held = Held::acquire(&self.local_lock());
        *self.0._state.read()
    }

    /// `self.state = new_state`: replace the lifecycle state.
    ///
    /// `ValueError` if *new_state* is not an `EntryState`, or if this
    /// subclass disallows non-`NA` states.
    pub fn set_state(&self, new_state: impl IntoArg) -> Result<()> {
        let new_state = new_state.into_arg();
        let _held = Held::acquire(&self.local_lock());
        let Some(s) = EntryState::from_arg(&new_state) else {
            return Err(LailaError::value_error("state must be an EntryState"));
        };
        if !self.kind()._ALLOWS_NON_NA_STATE() && s != EntryState::NA {
            return Err(LailaError::value_error(alloc::format!(
                "{} only accepts EntryState.NA",
                self.__class_name__()
            )));
        }
        *self.0._state.write() = s;
        Ok(())
    }

    /// `self.metadata`: entry metadata (not yet implemented).
    pub fn metadata(&self) -> Result<Arg> {
        let _held = Held::acquire(&self.local_lock());
        Err(LailaError::not_implemented(
            "Metadata is not implemented for Entry",
        ))
    }

    /// `self._source_pool_gid`
    pub fn _source_pool_gid(&self) -> Option<String> {
        self.0._source_pool_gid.read().clone()
    }
    /// `self._source_pool_gid = gid`
    pub fn set_source_pool_gid(&self, gid: Option<String>) {
        *self.0._source_pool_gid.write() = gid;
    }

    // ---------------------------------------------------------------------
    // Constitution Operations

    /// Synchronously materialize this entry by running its constitution.
    ///
    /// Runs [`_build_inplace`](Entry::_build_inplace) directly on the
    /// calling thread -- no command submission, no future. Idempotent: an
    /// entry that is already `READY` is returned as-is. Returns `self`.
    ///
    /// `RuntimeError` if the entry is not built and has no constitution attached.
    pub fn _build_sync(&self) -> Result<Entry> {
        if *self.0._state.read() == EntryState::READY {
            return Ok(self.clone());
        }
        if self.0._constitution.read().is_none() {
            return Err(LailaError::runtime_error(
                "Entry has no constitution attached.",
            ));
        }
        self._build_inplace()?;
        Ok(self.clone())
    }

    /// Asynchronously materialize this entry by running its constitution.
    ///
    /// - **Simple constitutions** are pure-CPU inverse-transformation chains
    ///   with no I/O to `await`: [`_build_inplace`](Entry::_build_inplace)
    ///   runs inline.
    /// - **Complex constitutions** are user-defined builders bound to a
    ///   `Manifest`: the manifest is resolved
    ///   (`ComplexConstitution._resolve_manifest_async`), its referenced
    ///   entries are materialized (`await target.async_realized`), and the
    ///   builder token runs against the live manifest.
    ///
    /// `_process_taskforce_id` routes the constitution body to a process
    /// pool in Python; process pools arrive with the command taskforces
    /// (Phase 3), so a non-`None` value raises `NotImplementedError`.
    ///
    /// On success the entry is mutated in place: `_payload` is set,
    /// `_constitution` becomes `None`, `_state` becomes `READY`. Resolves
    /// to `self`. Idempotent for `READY` entries.
    ///
    /// `_build_async(*, _process_taskforce_id=None)`: the Python-shaped
    /// entry point (`asyncio.run(e._build_async())`); the typed body is
    /// [`_build_async_with`](Entry::_build_async_with).
    pub fn _build_async(
        &self,
        args: impl Into<Args>,
    ) -> impl core::future::Future<Output = Result<Entry>> + Send + '_ {
        let args: Args = args.into();
        let pid = args
            .get("_process_taskforce_id")
            .as_str()
            .map(alloc::string::ToString::to_string);
        async move { self._build_async_with(pid.as_deref()).await }
    }

    /// [`_build_async`](Entry::_build_async) with a typed
    /// `_process_taskforce_id`.
    pub async fn _build_async_with(&self, _process_taskforce_id: Option<&str>) -> Result<Entry> {
        if *self.0._state.read() == EntryState::READY {
            return Ok(self.clone());
        }
        let Some(constitution) = self.constitution_opt() else {
            return Err(LailaError::runtime_error(
                "Entry has no constitution attached.",
            ));
        };
        if let Constitution::Complex(c) = &constitution {
            let code = c.code();
            if code.is_none() {
                return Err(LailaError::runtime_error("no constitution code defined."));
            }
            let hint = self._source_pool_gid();
            let target = c._resolve_manifest_async(hint.as_deref()).await?;
            if target.is_none() {
                return Err(LailaError::runtime_error(
                    "no manifest available to run constitution.",
                ));
            }
            if let Some(m) =
                crate::policy::central::memory::schema::manifest::Manifest::from_arg(&target)
            {
                let _realized = m.async_realized().await?;
            }
            if _process_taskforce_id.is_some() {
                return Err(LailaError::not_implemented(
                    "laila.build on a process pool (taskforce_id=<PythonProcessPoolTaskForce>) \
                     needs the command taskforces; process pools arrive in Phase 3",
                ));
            }
            // User constitution code is sync and may itself call
            // `manifest.realized` (a sync wait). Offload to an executor
            // thread so it doesn't block the loop; blocking ``wait()``
            // paths inside the body park the taskforce slot.
            let body = constitution::constitution::_exec_one_fn(&code.__str__())?;
            let slot = crate::policy::central::command::schema::parking::_CURRENT_SLOT.get();
            let result = match slot {
                Some(slot) => {
                    let (body, target) = (body.clone(), target.clone());
                    slot.tf
                        .run_sync(alloc::boxed::Box::new(move || body(&target)))?
                        .await??
                }
                None => {
                    #[cfg(feature = "std")]
                    {
                        let (body, target) = (body.clone(), target.clone());
                        crate::python::stdlib::asyncio::to_thread(move || body(&target)).await??
                    }
                    #[cfg(not(feature = "std"))]
                    {
                        body(&target)?
                    }
                }
            };
            self._post_build(result)?;
            self.set_constitution(Arg::None)?;
            self.set_state(EntryState::READY)?;
            self.0._locally_modified.store(false, Ordering::SeqCst);
            return Ok(self.clone());
        }
        self._build_inplace()?;
        Ok(self.clone())
    }

    /// `_build(*, asynchronous=False)`: dispatch to
    /// [`_build_async`](Entry::_build_async) -- returned as a coroutine
    /// object exactly like Python's `async def _build_async`, so
    /// `asyncio.run(e._build(asynchronous=True))` works (typed callers
    /// `await entry._build_async(None)` directly) -- or to
    /// [`_build_sync`](Entry::_build_sync) (runs inline; returns the entry).
    #[pysig("_build(*, asynchronous=False)")]
    pub fn _build(&self, args: impl Into<Args>) -> Result<Arg> {
        if asynchronous.__bool__() {
            let this = self.clone();
            return Ok(crate::python::stdlib::asyncio::coroutine(async move {
                this._build_async_with(None).await.map(Arg::Entry)
            }));
        }
        self._build_sync().map(Arg::Entry)
    }

    /// Run the attached constitution synchronously, in place.
    ///
    /// Threads the current payload (if any) into the constitution as
    /// `payload_input` and routes the result through
    /// [`_post_build`](Entry::_post_build). After a successful build the
    /// constitution is detached and the state flips to `READY`.
    ///
    /// `RuntimeError` if the entry has no constitution attached.
    pub fn _build_inplace(&self) -> Result<()> {
        let Some(constitution) = self.constitution_opt() else {
            return Err(LailaError::runtime_error(
                "Entry has no constitution attached.",
            ));
        };
        let payload_input = match self.payload_opt() {
            Some(p) => p.data(),
            None => Arg::None,
        };
        let result = constitution.build(payload_input)?;
        self._post_build(result)?;
        self.set_constitution(Arg::None)?;
        self.set_state(EntryState::READY)?;
        // Materializing stored bytes is not a user change.
        self.0._locally_modified.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// Place the build result into the entry's payload slot (assign it to
    /// `data`, which wraps it in a [`ComputationalData`]).
    pub fn _post_build(&self, result: impl IntoArg) -> Result<()> {
        self.set_data(result)
    }

    // ---------------------------------------------------------------------
    // Variable

    /// `Entry.variable(data=None, *, uuid=None, evolution=None, state=None, constitution=None, manifest=None, global_id=None, nickname=None)`:
    /// create a *mutable* (evolvable) Entry.
    ///
    /// A variable entry has an integer `evolution` counter starting at `0`
    /// (or the user-supplied value). Pass *exactly one* payload shape --
    /// `data=` (the entry is `READY` immediately) or `constitution=` *and*
    /// `manifest=` (the entry is `STAGED`; `laila.build(entry)` materializes
    /// it) -- and *at most one* identity shape (`global_id=`, `uuid=` with
    /// optional `evolution=`, `nickname=`, or none for a fresh UUID-4).
    ///
    /// `RuntimeError` if conflicting identity arguments are provided, or
    /// if both `constitution` and `data` are set; `ValueError` if
    /// `constitution` is given without `manifest` or vice versa.
    #[pysig("variable(data=None, *, uuid=None, evolution=None, state=None, constitution=None, manifest=None, global_id=None, nickname=None)")]
    pub fn variable(args: impl Into<Args>) -> Result<Entry> {
        if !global_id.is_none() && (!uuid.is_none() || !evolution.is_none()) {
            return Err(LailaError::runtime_error(
                "Cannot set both global_id and <uuid, evolution> at the same time.",
            ));
        }
        if !constitution.is_none() && !data.is_none() {
            return Err(LailaError::runtime_error(
                "Cannot set both constitution and data.",
            ));
        }
        if constitution.is_none() != manifest.is_none() {
            return Err(LailaError::value_error(
                "`constitution` and `manifest` must both be provided together.",
            ));
        }

        let mut uuid = uuid;
        let mut evolution = evolution;
        let mut scopes: Option<Vec<String>> = None;
        if !global_id.is_none() {
            let identity_data = process_global_id_arg(&global_id)?;
            uuid = Arg::Str(identity_data.uuid.into());
            evolution = identity_data.evolution.into_arg();
            scopes = if identity_data.scopes.is_empty() {
                None
            } else {
                Some(identity_data.scopes)
            };
        }

        let evolution = if evolution.is_none() {
            Arg::Int(0)
        } else {
            evolution
        };

        let uuid = Self::_merge_nickname_identity(uuid, nickname);

        let mut identity_kwargs = Args::new();
        identity_kwargs.push_keyword("uuid", uuid);
        identity_kwargs.push_keyword("evolution", evolution);
        if let Some(s) = scopes {
            identity_kwargs.push_keyword("scopes", strs_to_arg(&s));
        }

        if !constitution.is_none() {
            let mut a = Args::new();
            a.push_keyword("constitution", constitution);
            a.push_keyword("manifest", manifest);
            a.keywords.extend(identity_kwargs.keywords);
            return Entry::new(a);
        }

        let state = if state.is_none() {
            EntryState::READY.into_arg()
        } else {
            state
        };

        let mut a = Args::new();
        a.push_keyword("data", data);
        a.push_keyword("state", state);
        a.keywords.extend(identity_kwargs.keywords);
        Entry::new(a)
    }

    /// Combine an explicit `uuid` with a `nickname`.
    ///
    /// A nickname is a deterministic uuid generator and, exactly as in
    /// `Entry.__init__`, it takes precedence over an explicit `uuid` /
    /// `global_id` uuid when both are given.
    pub fn _merge_nickname_identity(uuid: impl IntoArg, nickname: impl IntoArg) -> Arg {
        let nickname = nickname.into_arg();
        if nickname.is_none() {
            return uuid.into_arg();
        }
        Arg::Str(Self::generate_uuid_from_nickname(nickname).into())
    }

    /// `Entry.generate_uuid_from_nickname(nickname)` ->
    /// `str(uuid5(laila.get_active_namespace(), nickname))`.
    pub fn generate_uuid_from_nickname(nickname: impl IntoArg) -> String {
        crate::basics::definitions::identifiable_object::generate_uuid_from_nickname(
            &nickname.into_arg().__str__(),
        )
    }

    /// `entry.evolve(data=None)`: return a new Entry representing the next evolution.
    ///
    /// Does **not** modify `self`: returns a freshly constructed `Entry`
    /// that shares the original's identity -- same `uuid` and `scopes` --
    /// but carries the new payload supplied via *data* and
    /// `evolution = self.evolution + 1`. The new entry is `READY` and
    /// carries its own `creation_timestamp`.
    ///
    /// `RuntimeError` if the Entry is a constant (`evolution is None`);
    /// `NotImplementedError` if the entry still carries an unbuilt
    /// constitution.
    #[pysig("evolve(data=None)")]
    pub fn evolve(&self, args: impl Into<Args>) -> Result<Entry> {
        if self.evolution_opt().is_none() {
            return Err(LailaError::runtime_error("Can't evolve a constant."));
        }
        if !self.constitution().is_none() {
            return Err(LailaError::not_implemented(
                "Entry has not been built yet, internal logic cannot change \
                 payload while constitution is not None.",
            ));
        }
        let (uuid_snapshot, scopes_snapshot, next_evolution) = {
            let _held = Held::acquire(&self.local_lock());
            let id = self.0.identity.read();
            (
                id.uuid.clone(),
                id.scopes.clone(),
                id.evolution.unwrap_or(0) + 1,
            )
        };
        let mut a = Args::new();
        a.push_keyword("uuid", uuid_snapshot);
        a.push_keyword("scopes", strs_to_arg(&scopes_snapshot));
        a.push_keyword("evolution", next_evolution);
        a.push_keyword("data", data);
        a.push_keyword("state", EntryState::READY);
        Entry::contingent(a)
    }

    // ---------------------------------------------------------------------
    // Constant

    /// `Entry.constant(data, *, global_id=None, uuid=None, nickname=None)`:
    /// create an *immutable* Entry whose `evolution` is `None`.
    ///
    /// Constants do not support [`evolve`](Entry::evolve) and therefore do
    /// not carry an `@evolution=` attribute in their global_id.
    ///
    /// `RuntimeError` if both `global_id` and `uuid` are supplied, or if
    /// `global_id` includes an evolution component.
    #[pysig("constant(data, *, global_id=None, uuid=None, nickname=None)")]
    pub fn constant(args: impl Into<Args>) -> Result<Entry> {
        if !global_id.is_none() && !uuid.is_none() {
            return Err(LailaError::runtime_error(
                "Cannot set both global_id and uuid at the same time.",
            ));
        }

        let mut uuid = uuid;
        let mut scopes: Option<Vec<String>> = None;
        if uuid.is_none() && !global_id.is_none() {
            let identity_data = process_global_id_arg(&global_id)?;
            uuid = Arg::Str(identity_data.uuid.into());
            scopes = if identity_data.scopes.is_empty() {
                None
            } else {
                Some(identity_data.scopes)
            };
            if identity_data.evolution.is_some() {
                return Err(LailaError::runtime_error(
                    "Cannot have a constant with an evolution.",
                ));
            }
        }

        let uuid = Self::_merge_nickname_identity(uuid, nickname);

        let mut a = Args::new();
        a.push_keyword("data", data);
        a.push_keyword("state", EntryState::READY);
        a.push_keyword("uuid", uuid);
        a.push_keyword("evolution", Arg::None);
        if let Some(s) = scopes {
            a.push_keyword("scopes", strs_to_arg(&s));
        }
        Entry::new(a)
    }

    // ---------------------------------------------------------------------
    // Contingent

    /// `Entry.contingent(**kwargs)`: create an Entry from raw keyword
    /// arguments, bypassing the consistency checks performed by
    /// [`constant`](Entry::constant) / [`variable`](Entry::variable).
    pub fn contingent(args: impl Into<Args>) -> Result<Entry> {
        Entry::new(args)
    }

    // ---------------------------------------------------------------------
    // Serialize and Recovery

    /// `entry.as_dict()`: the unified serialized shape for this entry.
    ///
    /// Unlike [`serialize`](Entry::serialize) this does NOT apply any
    /// [`TransformationSequence`] -- it embeds the raw payload value
    /// directly. Keys (mirroring the on-disk schema): `_uuid`,
    /// `_evolution`, `_scopes`, `_state` (the member name),
    /// `_creation_timestamp`, `payload`, `constitution`
    /// (`Constitution.as_dict()` or `None`).
    pub fn as_dict(&self) -> Result<Arg> {
        let constitution_dict = match self.constitution_opt() {
            Some(c) => c.as_dict()?,
            None => Arg::None,
        };
        let payload_value = match self.payload_opt() {
            Some(p) => p.data(),
            None => Arg::None,
        };
        let id = self.identity_value();
        let mut d = PyDictBuilder::new();
        d.insert("_uuid", id.uuid.as_str());
        d.insert("_evolution", id.evolution);
        d.insert("_scopes", strs_to_arg(&id.scopes));
        d.insert("_state", self.0._state.read().name());
        d.insert("_creation_timestamp", id.creation_timestamp);
        d.insert("payload", payload_value);
        d.insert("constitution", constitution_dict);
        Ok(d.finish())
    }

    /// `entry.serialize(transformations=None)`: serialize the Entry into a
    /// plain dict suitable for pool storage.
    ///
    /// With *transformations*: the payload is serialized to bytes via its
    /// [`ComputationalData`] `.serialize()` (which also emits an *inverse*
    /// code string), the bytes are fed through the
    /// [`TransformationSequence`] (each step emitting its own inverse), and
    /// all inverse codes are bundled into a fresh [`SimpleConstitution`]
    /// under `constitution`. With `transformations=None` (in-memory pools)
    /// an independent [`_snapshot`](Entry::_snapshot) of the live `Entry`
    /// is returned instead of a dict.
    ///
    /// `RuntimeError` if the entry is not `READY`.
    #[pysig("serialize(transformations=None)")]
    pub fn serialize(&self, args: impl Into<Args>) -> Result<Arg> {
        let state = *self.0._state.read();
        if state != EntryState::READY {
            return Err(LailaError::runtime_error(alloc::format!(
                "Cannot serialize entry in state {}; only READY entries can be serialized.",
                crate::python::pyrepr::str_repr(state.name())
            )));
        }

        if transformations.is_none() {
            // In-memory pools store the object itself: hand them an
            // independent snapshot so later mutation of this entry (or of
            // its payload) never changes what ``remember`` returns.
            return Ok(Arg::Entry(self._snapshot()?));
        }

        let (transformed_payload, codes) = match self.payload_opt() {
            Some(payload) => {
                let (serialized_payload, payload_backward_code) = payload.serialize(Args::new())?;
                let (transformed_payload, mut codes) =
                    forward_through(&transformations, serialized_payload)?;
                codes.push(payload_backward_code);
                (transformed_payload, codes)
            }
            None => (Arg::None, Vec::new()),
        };

        let constitution = SimpleConstitution::from_codes(
            codes
                .into_iter()
                .map(|c| Arg::Str(c.into()))
                .collect::<Vec<_>>()
                .into_arg(),
        )?;

        let id = self.identity_value();
        let mut d = PyDictBuilder::new();
        d.insert("_uuid", id.uuid.as_str());
        d.insert("_evolution", id.evolution);
        d.insert("_scopes", strs_to_arg(&id.scopes));
        d.insert("_state", state.name());
        d.insert("_creation_timestamp", id.creation_timestamp);
        d.insert("payload", transformed_payload);
        d.insert("constitution", constitution.as_dict()?);
        Ok(d.finish())
    }

    /// Independent copy of this entry with a deep-copied payload.
    ///
    /// Used by [`serialize`](Entry::serialize) for entries stored without
    /// transformations (in-memory pools). Identity fields, state,
    /// constitution and creation_timestamp are copied; the payload is
    /// deep-copied (`ComputationalData.__deepcopy__`) so in-place mutation
    /// of the original after `memorize` does not leak into the stored
    /// evolution. Payloads that cannot be deep-copied are kept by
    /// reference, best effort. The copy gets its own atomic lock and
    /// starts clean (not locally modified).
    pub fn _snapshot(&self) -> Result<Entry> {
        let clone = Self::__new__(self.kind());
        // ``self.model_copy()``: every private attribute, shallowly.
        *clone.0.identity.write() = self.identity_value();
        *clone.0._state.write() = *self.0._state.read();
        *clone.0._constitution.write() = self.constitution_opt();
        *clone.0._source_pool_gid.write() = self._source_pool_gid();
        for (k, v) in self.0.__dict__.__dict__() {
            // ``object.__setattr__(clone, "_local_lock", None)``: own lock.
            if k != crate::atomic::definitions::LOCAL_LOCK_ATTR {
                clone.0.__dict__.__setattr__(&k, v)?;
            }
        }
        if let Some(payload) = self.payload_opt() {
            *clone.0._payload.write() = Some(match payload.__deepcopy__() {
                Ok(copied) => copied,
                // ``get_logger().debug(f"Entry {gid}: payload not deep-copyable
                // ({exc!r}); stored by reference")`` -- kept by reference.
                Err(_) => payload,
            });
        }
        clone.0._locally_modified.store(false, Ordering::SeqCst);
        Ok(clone)
    }

    /// `Entry.from_dict(in_dict)`: hydrate an Entry from a serialized dict
    /// *without* running its constitution.
    ///
    /// Reconstructs identity (validated: a corrupt record fails loudly),
    /// attaches the stored payload as-is, and re-attaches the constitution
    /// if one was serialized. The recovery chain is NOT executed; callers
    /// must invoke [`_build_inplace`](Entry::_build_inplace) themselves (or
    /// use [`_build_from_dict_sync`](Entry::_build_from_dict_sync)). The
    /// original `creation_timestamp` is restored when present; dicts that
    /// predate the field are stamped with the current time instead.
    pub fn from_dict(in_dict: impl IntoArg) -> Result<Entry> {
        Self::from_dict_as(EntryKind::Entry, in_dict)
    }

    /// `cls.from_dict(in_dict)` for `cls` = `Entry` or its `Manifest` subclass.
    pub fn from_dict_as(kind: EntryKind, in_dict: impl IntoArg) -> Result<Entry> {
        let in_dict = in_dict.into_arg();
        let get = |k: &str| -> Result<Arg> {
            if in_dict.__contains__(k)? {
                in_dict.__getitem__(k)
            } else {
                Ok(Arg::None)
            }
        };
        // Identity fields come from storage / the wire: validate them so a
        // corrupt record fails loudly instead of being rebuilt silently.
        let raw_uuid = in_dict.__getitem__("_uuid")?;
        let valid_uuid = matches!(&raw_uuid, Arg::Str(s) if is_valid_uuid(s));
        if !valid_uuid {
            return Err(LailaError::value_error(alloc::format!(
                "Entry.from_dict: invalid _uuid {}",
                raw_uuid.__repr__()
            )));
        }
        let raw_evolution = get("_evolution")?;
        let valid_evolution = match &raw_evolution {
            Arg::None => true,
            Arg::Int(i) => *i >= 0,
            _ => false,
        };
        if !valid_evolution {
            return Err(LailaError::value_error(alloc::format!(
                "Entry.from_dict: invalid _evolution {}",
                raw_evolution.__repr__()
            )));
        }
        let entry = Self::__new__(kind);
        let mut identity = Args::new();
        identity.push_keyword("uuid", raw_uuid);
        identity.push_keyword("evolution", raw_evolution);
        identity.push_keyword("scopes", get("_scopes")?);
        entry._initialize_identity(&identity)?;
        // ``cls.__new__`` bypasses ``_LAILA_OBJECT.__init__``, so the
        // creation_timestamp must be restored (or freshly stamped) explicitly.
        let stamp = get("_creation_timestamp")?;
        entry.0.identity.write().creation_timestamp = Some(if stamp.__bool__() {
            stamp.__str__()
        } else {
            _now_creation_timestamp()
        });
        entry.set_data(get("payload")?)?;
        let state_name = if in_dict.__contains__("_state")? {
            in_dict.__getitem__("_state")?
        } else {
            Arg::Str("STAGED".into())
        };
        entry.set_state(EntryState::__getitem__(state_name)?)?;
        entry.set_constitution(Constitution::from_dict(get("constitution")?)?.into_arg())?;
        // A pool round-trip restores the memorized baseline: not locally modified.
        entry.0._locally_modified.store(false, Ordering::SeqCst);
        Ok(entry)
    }

    /// `Entry._build_from_dict_sync(in_dict)`: synchronously hydrate an
    /// entry from a serialized dict.
    ///
    /// A `str` is parsed as JSON; a `dict` goes through
    /// [`from_dict`](Entry::from_dict) followed by an inline
    /// [`_build_inplace`](Entry::_build_inplace) if the constitution is a
    /// [`SimpleConstitution`] (`ComplexConstitution` entries are left
    /// `STAGED`); a live `Entry` is returned as-is.
    ///
    /// `ValueError` if *in_dict* is a string that fails to parse as JSON;
    /// `RuntimeError` for any other unsupported input type.
    pub fn _build_from_dict_sync(in_dict: impl IntoArg) -> Result<Entry> {
        Self::_build_from_dict_sync_as(EntryKind::Entry, in_dict)
    }

    /// `cls._build_from_dict_sync(in_dict)` for `cls` = `Entry` / `Manifest`.
    pub fn _build_from_dict_sync_as(kind: EntryKind, in_dict: impl IntoArg) -> Result<Entry> {
        let mut local = in_dict.into_arg();
        if let Arg::Str(s) = &local {
            local = crate::_compat::pyjson::loads(s)
                .map_err(|_| LailaError::value_error("Invalid JSON string"))?;
        }
        if let Arg::Entry(e) = &local {
            return Ok(e.clone());
        }
        if !matches!(local, Arg::Dict(_)) {
            return Err(LailaError::runtime_error("Invalid input for entry build."));
        }
        let entry = Self::from_dict_as(kind, local)?;
        if matches!(entry.constitution_opt(), Some(Constitution::Simple(_))) {
            entry._build_inplace()?;
        }
        Ok(entry)
    }

    /// Async variant of [`_build_from_dict_sync`](Entry::_build_from_dict_sync).
    ///
    /// Both paths *always* run the `SimpleConstitution` chain on the
    /// freshly-hydrated entry, even when the serialized `_state` is already
    /// `READY`: the constitution attached on serialize *is* the recipe for
    /// reversing the pool's storage transformations.
    pub async fn _build_from_dict_async(in_dict: impl IntoArg) -> Result<Entry> {
        Self::_build_from_dict_sync_as(EntryKind::Entry, in_dict)
    }

    /// `cls._build_from_dict(in_dict, *, asynchronous=False)`: router.
    #[pysig("_build_from_dict(in_dict, *, asynchronous=False)")]
    pub fn _build_from_dict(args: impl Into<Args>) -> Result<Entry> {
        if asynchronous.__bool__() {
            return crate::python::stdlib::asyncio::run(Self::_build_from_dict_async(in_dict));
        }
        Self::_build_from_dict_sync(in_dict)
    }

    // ---------------------------------------------------------------------
    // String Representation

    /// `str(entry)`: the global identifier string.
    pub fn __str__(&self) -> String {
        self.global_id()
    }
    /// `repr(entry)`: the global identifier string.
    pub fn __repr__(&self) -> String {
        match self.kind() {
            EntryKind::Entry => self.global_id(),
            EntryKind::Manifest => {
                crate::policy::central::memory::schema::manifest::Manifest(self.clone()).__repr__()
            }
        }
    }
    /// `hash(entry)` -> `hash(global_id)`.
    pub fn __hash__(&self) -> Result<i64> {
        Arg::Str(self.global_id().into()).__hash__()
    }
    /// Python `is`.
    pub fn is(&self, other: &Entry) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    /// `id(entry)`
    pub fn id(&self) -> usize {
        Arc::as_ptr(&self.0) as *const () as usize
    }
    /// `bool(entry)`: `Manifest` has `__len__`; a plain entry is always `True`.
    pub fn __bool__(&self) -> bool {
        match self.kind() {
            EntryKind::Entry => true,
            EntryKind::Manifest => self.__len__().map(|n| n != 0).unwrap_or(true),
        }
    }

    // ---------------------------------------------------------------------
    // Python protocol (the dynamic surface of `Arg::Entry`)

    /// `entry.__dict__`: a snapshot of the instance attributes (the extra
    /// attributes set on the object, e.g. a `StreamEntry`'s `stream`).
    pub fn __dict__(&self) -> Arg {
        let mut d = crate::python::PyDictBuilder::new();
        for (k, v) in self.0.__dict__.__dict__() {
            d.insert(k, v);
        }
        d.finish()
    }

    /// `entry.name` for the dynamic layer: properties, private attributes,
    /// class variables, then the instance `__dict__`.
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "data" => self.data()?,
            "state" => self.state().into_arg(),
            "constitution" => self.constitution(),
            "metadata" => self.metadata()?,
            "locally_modified" => Arg::Bool(self.locally_modified()),
            "uuid" | "_uuid" => Arg::Str(self.uuid().into()),
            "scopes" | "_scopes" => self.scopes(),
            "evolution" | "_evolution" => self.evolution(),
            "global_id" => Arg::Str(self.global_id().into()),
            "creation_timestamp" | "_creation_timestamp" => self.creation_timestamp(),
            "_state" => (*self.0._state.read()).into_arg(),
            "_constitution" => match self.constitution_opt() {
                Some(c) => c.into_arg(),
                None => Arg::None,
            },
            "_payload" => self._payload(),
            "_locally_modified" => Arg::Bool(self.locally_modified()),
            "_source_pool_gid" => self._source_pool_gid().into_arg(),
            "_DEFAULT_SCOPES" => strs_to_arg(
                &self
                    .kind()
                    ._DEFAULT_SCOPES()
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>(),
            ),
            "_ALLOWS_NON_NA_STATE" => Arg::Bool(self.kind()._ALLOWS_NON_NA_STATE()),
            "__class__" => self.kind().__class__().into_arg(),
            "__dict__" => self.__dict__(),
            _ => {
                if self.kind() == EntryKind::Manifest {
                    if let Some(v) =
                        crate::policy::central::memory::schema::manifest::getattr(self, name)?
                    {
                        return Ok(v);
                    }
                }
                if let Some(v) = self.0.__dict__.get(name) {
                    return Ok(v);
                }
                return Err(LailaError::attribute_error(alloc::format!(
                    "'{}' object has no attribute '{name}'",
                    self.__class_name__()
                )));
            }
        })
    }

    /// `entry.name = value` for the dynamic layer: the property setters
    /// (`data`, `state`, `constitution`, `global_id`), the private
    /// attributes, `object.__setattr__` for other private names; a public
    /// name that is not a field is pydantic's `ValueError`.
    pub fn __setattr__(&self, name: &str, value: impl IntoArg) -> Result<()> {
        let value = value.into_arg();
        match name {
            // `Manifest.data` overrides the setter: the blueprint changed, so
            // any cached SQL index is stale.
            "data" if self.kind() == EntryKind::Manifest => {
                crate::policy::central::memory::schema::manifest::Manifest(self.clone())
                    .set_data(value)
            }
            "data" => self.set_data(value),
            "state" => self.set_state(value),
            "constitution" => self.set_constitution(value),
            "global_id" => self.set_global_id(value),
            "_state" => {
                let s = EntryState::from_arg(&value)
                    .ok_or_else(|| LailaError::value_error("state must be an EntryState"))?;
                *self.0._state.write() = s;
                Ok(())
            }
            "_constitution" => {
                let c = if value.is_none() {
                    None
                } else {
                    Some(Constitution::from_arg(&value).ok_or_else(|| {
                        LailaError::type_error("constitution must be a Constitution or None")
                    })?)
                };
                *self.0._constitution.write() = c;
                Ok(())
            }
            "_payload" => {
                let p = if value.is_none() {
                    None
                } else {
                    Some(ComputationalData::from_arg(&value).ok_or_else(|| {
                        LailaError::type_error("_payload must be a ComputationalData or None")
                    })?)
                };
                *self.0._payload.write() = p;
                Ok(())
            }
            "_locally_modified" => {
                self.0
                    ._locally_modified
                    .store(value.__bool__(), Ordering::SeqCst);
                Ok(())
            }
            "_source_pool_gid" => {
                self.set_source_pool_gid(opt_str(&value));
                Ok(())
            }
            "_uuid" => {
                self.0.identity.write().uuid = value.__str__();
                Ok(())
            }
            "_scopes" => {
                self.0.identity.write().scopes =
                    value.__iter__()?.iter().map(|x| x.__str__()).collect();
                Ok(())
            }
            "_evolution" => {
                self.0.identity.write().evolution = value.as_i64();
                Ok(())
            }
            "_creation_timestamp" => {
                self.0.identity.write().creation_timestamp = opt_str(&value);
                Ok(())
            }
            "uuid" | "scopes" | "evolution" | "creation_timestamp" | "locally_modified"
            | "metadata" => Err(LailaError::attribute_error(alloc::format!(
                "property '{name}' of '{}' object has no setter",
                self.__class_name__()
            ))),
            other if other.starts_with('_') => self.0.__dict__.__setattr__(other, value),
            other => Err(LailaError::value_error(alloc::format!(
                "\"{}\" object has no field \"{other}\"",
                self.__class_name__()
            ))),
        }
    }

    /// `del entry._name`
    pub fn __delattr__(&self, name: &str) -> Result<()> {
        self.0.__dict__.__delattr__(name).map_err(|_| {
            LailaError::attribute_error(alloc::format!(
                "'{}' object has no attribute '{name}'",
                self.__class_name__()
            ))
        })
    }

    /// `entry.method(*args, **kwargs)` for the dynamic layer.
    pub fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        if self.kind() == EntryKind::Manifest {
            if let Some(v) =
                crate::policy::central::memory::schema::manifest::call_method(self, name, &args)?
            {
                return Ok(v);
            }
        }
        Ok(match name {
            "evolve" => Arg::Entry(self.evolve(args)?),
            "serialize" => self.serialize(args)?,
            "as_dict" => self.as_dict()?,
            "mark_memorized" => {
                self.mark_memorized();
                Arg::None
            }
            "bump_evolution_if_locally_modified" => {
                Arg::Bool(self.bump_evolution_if_locally_modified())
            }
            "_build_sync" => Arg::Entry(self._build_sync()?),
            "_build_async" => {
                let pid = args
                    .get("_process_taskforce_id")
                    .as_str()
                    .map(alloc::string::ToString::to_string);
                let this = self.clone();
                crate::python::stdlib::asyncio::coroutine(async move {
                    this._build_async_with(pid.as_deref()).await.map(Arg::Entry)
                })
            }
            "_build" => self._build(args)?,
            "_build_inplace" => {
                self._build_inplace()?;
                Arg::None
            }
            "_post_build" => {
                self._post_build(args.positional.first().cloned().unwrap_or(Arg::None))?;
                Arg::None
            }
            "_snapshot" => Arg::Entry(self._snapshot()?),
            "has_evolution" => Arg::Bool(self.has_evolution()),
            "identity" => self.identity(),
            "identity_as_json" => Arg::Str(self.identity_as_json()?.into()),
            "from_dict" => Arg::Entry(Self::from_dict_as(
                self.kind(),
                args.positional.first().cloned().unwrap_or(Arg::None),
            )?),
            "_build_from_dict_sync" | "_build_from_dict_async" => {
                Arg::Entry(Self::_build_from_dict_sync_as(
                    self.kind(),
                    args.positional.first().cloned().unwrap_or(Arg::None),
                )?)
            }
            "_initialize_identity" => {
                let data = args
                    .positional
                    .first()
                    .cloned()
                    .unwrap_or(Arg::None)
                    .into_args_dict()?;
                self._initialize_identity(&data)?;
                Arg::None
            }
            "atomic" => self.atomic(args)?.into_arg(),
            "lock" => Arg::Bool(self.lock(args)?),
            "unlock" => {
                self.unlock()?;
                Arg::None
            }
            "locked" => Arg::Bool(self.locked(args)),
            "_ensure_local_lock" => self._ensure_local_lock().into_arg(),
            "__getstate__" => self.__getstate__(),
            other => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'{}' object has no attribute '{other}'",
                    self.__class_name__()
                )))
            }
        })
    }

    /// `entry[key]`: `Manifest` looks up a top-level blueprint key; a plain
    /// `Entry` is not subscriptable (read `entry.data[key]`).
    pub fn __getitem__(&self, key: impl IntoArg) -> Result<Arg> {
        match self.kind() {
            EntryKind::Manifest => {
                crate::policy::central::memory::schema::manifest::Manifest(self.clone())
                    .__getitem__(key)
            }
            EntryKind::Entry => Err(LailaError::type_error(
                "'Entry' object is not subscriptable",
            )),
        }
    }
    /// `entry[key] = value` -- neither class supports item assignment.
    pub fn __setitem__(&self, _key: impl IntoArg, _value: impl IntoArg) -> Result<()> {
        Err(LailaError::type_error(alloc::format!(
            "'{}' object does not support item assignment",
            self.__class_name__()
        )))
    }
    /// `key in entry`: `Manifest` membership of a leaf `global_id`.
    pub fn __contains__(&self, key: impl IntoArg) -> Result<bool> {
        match self.kind() {
            EntryKind::Manifest => Ok(crate::policy::central::memory::schema::manifest::Manifest(
                self.clone(),
            )
            .__contains__(key)),
            EntryKind::Entry => Err(LailaError::type_error(
                "argument of type 'Entry' is not iterable",
            )),
        }
    }
    /// `len(entry)`: `Manifest` top-level key count.
    pub fn __len__(&self) -> Result<usize> {
        match self.kind() {
            EntryKind::Manifest => {
                crate::policy::central::memory::schema::manifest::Manifest(self.clone()).__len__()
            }
            EntryKind::Entry => Err(LailaError::type_error(
                "object of type 'Entry' has no len()",
            )),
        }
    }
    /// `iter(entry)`: `Manifest` leaf `global_id`s.
    pub fn __iter__(&self) -> Result<Vec<Arg>> {
        match self.kind() {
            EntryKind::Manifest => {
                crate::policy::central::memory::schema::manifest::Manifest(self.clone()).__iter__()
            }
            EntryKind::Entry => Err(LailaError::type_error("'Entry' object is not iterable")),
        }
    }
}

/// `transformations.forward(value)` on a dynamic value: the typed
/// [`TransformationSequence`] directly, anything else through its
/// `forward` method (`(value, [inverse codes])`).
fn forward_through(transformations: &Arg, value: Arg) -> Result<(Arg, Vec<String>)> {
    if let Some(seq) = transformations.downcast_ref::<TransformationSequence>() {
        return seq.forward(value);
    }
    let out = transformations.__call_method__("forward", Args::from(value))?;
    let pair = out.__iter__()?;
    if pair.len() != 2 {
        return Err(LailaError::type_error(
            "transformations.forward must return (value, inverse_codes)",
        ));
    }
    let codes = pair[1].__iter__()?.iter().map(|c| c.__str__()).collect();
    Ok((pair[0].clone(), codes))
}

/// `dict` -> `Args` (keywords) for the `_initialize_*(data)` helpers.
trait IntoArgsDict {
    fn into_args_dict(self) -> Result<Args>;
}
impl IntoArgsDict for Arg {
    fn into_args_dict(self) -> Result<Args> {
        crate::python::args::IntoArgs::into_args(self)
    }
}

// ---------------------------------------------------------------------------
// Mixins: `_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT`

impl HasAttrs for Entry {
    fn __attrs__(&self) -> &PyAttrs {
        &self.0.__dict__
    }
    fn __class_name__(&self) -> &'static str {
        self.0.kind.__name__()
    }
}

impl HasIdentity for Entry {
    fn global_id(&self) -> String {
        Entry::global_id(self)
    }
    fn uuid(&self) -> String {
        Entry::uuid(self)
    }
}

impl _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT for Entry {}

// ---------------------------------------------------------------------------
// Rust trait plumbing

impl PartialEq for Entry {
    /// Equality follows identity: same global id means the same object,
    /// regardless of payload, timestamps or process-local state.
    fn eq(&self, other: &Entry) -> bool {
        self.is(other) || self.global_id() == other.global_id()
    }
}
impl Eq for Entry {}

impl core::hash::Hash for Entry {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        state.write_i64(self.__hash__().unwrap_or(0));
    }
}

impl PyArg for Entry {
    fn to_arg(&self) -> Arg {
        Arg::Entry(self.clone())
    }
}

impl IsInstance for Entry {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Entry(_))
    }
}

impl core::fmt::Debug for Entry {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__repr__())
    }
}

impl core::fmt::Display for Entry {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__str__())
    }
}

/// The `PyObject` vtable (used by the atomic context manager yielded by
/// `with entry.atomic() as locked:`); the entry itself travels as
/// `Arg::Entry`, never as `Arg::Object`.
impl PyObject for Entry {
    fn as_any(&self) -> &dyn Any {
        self
    }
    /// `a is b`: identity of the shared handle, not of the boxing.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Entry>()
            .is_some_and(|o| self.is(o))
    }
    fn type_name(&self) -> &'static str {
        self.__class_name__()
    }
    fn __repr__(&self) -> String {
        Entry::__repr__(self)
    }
    fn __str__(&self) -> String {
        Entry::__str__(self)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Entry>()
            .is_some_and(|o| self == o)
    }
    fn __eq_arg__(&self, other: &Arg) -> bool {
        match other {
            Arg::Entry(e) => self == e,
            Arg::Object(o) => self.__eq__(&**o),
            _ => false,
        }
    }
    fn __hash__(&self) -> Result<i64> {
        Entry::__hash__(self)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        Entry::__getattr__(self, name)
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        Entry::__setattr__(self, name, value)
    }
    fn __delattr__(&self, name: &str) -> Result<()> {
        Entry::__delattr__(self, name)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        Entry::__call_method__(self, name, args)
    }
    fn __len__(&self) -> Result<usize> {
        Entry::__len__(self)
    }
    fn __bool__(&self) -> bool {
        Entry::__bool__(self)
    }
    fn __contains__(&self, item: &Arg) -> Result<bool> {
        Entry::__contains__(self, item.clone())
    }
    fn __iter__(&self) -> Result<Vec<Arg>> {
        Entry::__iter__(self)
    }
    fn __getitem__(&self, key: &Arg) -> Result<Arg> {
        Entry::__getitem__(self, key.clone())
    }
    fn __class__(&self) -> Option<crate::python::PyClass> {
        Some(self.kind().__class__())
    }
    fn __atomic_lock__(&self) -> Option<RLock> {
        Some(self.local_lock())
    }
}

/// `isinstance(x, Entry)` -> the handle.
pub fn entry_of(x: &Arg) -> Option<&Entry> {
    match x {
        Arg::Entry(e) => Some(e),
        _ => None,
    }
}
