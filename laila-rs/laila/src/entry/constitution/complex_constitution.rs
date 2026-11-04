//! Complex constitution: a single source string driven by a `Manifest`.
//!
//! A [`ComplexConstitution`] carries one Python source string that defines a
//! single callable `f(manifest) -> payload`. At build time it resolves its
//! bound `Manifest` from the active policy's memory (using a stored
//! `global_id` if the live object is not on hand), forces every entry the
//! manifest references to materialize inside a `laila.guarantee` scope, and
//! then applies the callable to the manifest.
//!
//! # Why complex constitutions exist
//! `SimpleConstitution` is a fine vehicle for inverse serialization chains,
//! but it cannot express *derivations* -- "this entry's payload is
//! `f(other_entry_a, other_entry_b)`". A complex constitution carries both
//! the recipe (the source string) and the inputs (the manifest) so the
//! derivation can be re-executed in any process that can fetch the inputs
//! from a shared pool.
//!
//! # Identity preservation across serialization
//! A complex constitution does not embed the manifest's *contents* in its
//! serialized form; it embeds only the manifest's `global_id`. The live
//! manifest is then re-fetched from the active policy's memory at build
//! time.
//!
//! # Rust
//! `code` is a *builder token* resolved through
//! `constitution::register_builder_token` (see `_exec_one_fn`). The
//! `Manifest` type lives in `policy::central::memory::schema::manifest` and
//! registers the two hooks below; `laila.guarantee` and the future bank
//! are Phase 3 (the hook realizes the manifest inline).

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::any::Any;

use crate::python::builtins::IsInstance;
use crate::python::object::PyObject;
use crate::python::pyrepr::str_repr;
use crate::python::sync::RwLock;
use crate::python::{pysig, Arg, Args, ExcKind, IntoArg, LailaError, PyArg, PyDictBuilder, Result};

use super::constitution::{_exec_one_fn, Constitution};
use super::trust::{sign_complex, verify_complex};

struct Inner {
    _code: RwLock<Option<String>>,
    _manifest: RwLock<Arg>,
    _manifest_global_id: RwLock<Option<String>>,
}

/// `isinstance(value, Manifest)` predicate.
pub type ManifestCheck = fn(&Arg) -> bool;
/// `with laila.guarantee: target.realized` implementation.
pub type RealizeHook = fn(&Arg) -> Result<()>;

/// `isinstance(value, Manifest)` hook, registered by
/// `policy::central::memory::schema::manifest::__init__` (the `Manifest`
/// module's import-time side effect; `crate::entry::__init__` runs it).
/// Before registration any object exposing a `global_id` is accepted.
static MANIFEST_CHECK: RwLock<Option<ManifestCheck>> = RwLock::new(None);

/// `with laila.guarantee: target.realized` hook, registered by the
/// `Manifest` module (`laila.guarantee` -- the resolution guard of the
/// command runtime -- is Phase 3; the hook realizes the manifest inline).
static REALIZE_HOOK: RwLock<Option<RealizeHook>> = RwLock::new(None);

/// Register `isinstance(value, Manifest)` (called by the Manifest module).
pub fn register_manifest_check(check: ManifestCheck) {
    *MANIFEST_CHECK.write() = Some(check);
}

/// Register the `with laila.guarantee: target.realized` implementation.
pub fn register_realize_hook(hook: RealizeHook) {
    *REALIZE_HOOK.write() = Some(hook);
}

fn is_manifest(value: &Arg) -> bool {
    crate::entry::__init__();
    if let Some(check) = *MANIFEST_CHECK.read() {
        return check(value);
    }
    match value {
        Arg::Entry(_) => true,
        Arg::Object(_) => value.__getattr__("global_id").is_ok(),
        _ => false,
    }
}

fn manifest_global_id_of(value: &Arg) -> Result<String> {
    match value {
        Arg::Entry(e) => Ok(e.global_id()),
        other => Ok(other.__getattr__("global_id")?.__str__()),
    }
}

fn realize(target: &Arg) -> Result<()> {
    crate::entry::__init__();
    if let Some(hook) = *REALIZE_HOOK.read() {
        return hook(target);
    }
    match target.__getattr__("realized") {
        Ok(_) => Ok(()),
        Err(e) if e.kind == ExcKind::AttributeError => Ok(()),
        Err(e) => Err(e),
    }
}

/// A one-shot constitution function bound to a `Manifest`.
///
/// Once assigned, neither the code nor the manifest can be reassigned --
/// the constitution is intentionally immutable so that the gid derived from
/// it is stable. The manifest may be provided directly as a live `Manifest`
/// instance or, when rehydrating a serialized entry, as a
/// `manifest_global_id` to be resolved lazily from the active policy's
/// memory at build time. Clones alias the same object (a Python reference).
///
/// ```text
/// # Direct: manifest object in hand
/// c = ComplexConstitution(code=src, manifest=m)
/// # Lazy: only the gid (typical after deserialization)
/// c = ComplexConstitution(code=src, manifest_global_id=gid)
/// ```
#[derive(Clone)]
pub struct ComplexConstitution(Arc<Inner>);

impl ComplexConstitution {
    /// `@_register_kind("complex")`
    pub const _KIND: &'static str = "complex";

    /// Initialise from optional `code`, `manifest`, and `manifest_global_id`
    /// kwargs. Either `manifest` or `manifest_global_id` may be supplied --
    /// a live manifest takes precedence when both are present (the gid is
    /// derived from it). Omitting both is allowed but the resulting
    /// constitution cannot be built until a manifest is attached.
    #[pysig("ComplexConstitution(**data)")]
    pub fn new(args: impl Into<Args>) -> Result<ComplexConstitution> {
        let code = data.get("code");
        let manifest = data.get("manifest");
        let manifest_global_id = data.get("manifest_global_id");
        let c = ComplexConstitution(Arc::new(Inner {
            _code: RwLock::new(None),
            _manifest: RwLock::new(Arg::None),
            _manifest_global_id: RwLock::new(None),
        }));
        if !code.is_none() {
            c.set_code(code)?;
        }
        if !manifest.is_none() {
            c.set_manifest(manifest)?;
        } else if !manifest_global_id.is_none() {
            *c.0._manifest_global_id.write() = Some(manifest_global_id.__str__());
        }
        Ok(c)
    }

    /// Python `is`.
    pub fn is(&self, other: &ComplexConstitution) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// The single Python source string (builder token) defining the
    /// constitution callable, or `None`. Read-only after first assignment.
    pub fn code(&self) -> Arg {
        match &*self.0._code.read() {
            Some(c) => Arg::Str(c.as_str().into()),
            None => Arg::None,
        }
    }

    /// Assign the constitution code. One-time only.
    ///
    /// `AttributeError` if `code` is already set; `TypeError` if `value` is
    /// not a string.
    pub fn set_code(&self, value: impl IntoArg) -> Result<()> {
        let value = value.into_arg();
        if self.0._code.read().is_some() {
            return Err(LailaError::attribute_error(
                "ComplexConstitution code is already set and cannot be reassigned.",
            ));
        }
        let Arg::Str(s) = value else {
            return Err(LailaError::type_error(
                "code must be a Python source string",
            ));
        };
        *self.0._code.write() = Some(s.to_string());
        Ok(())
    }

    /// The bound `Manifest`, if available (`None` immediately after
    /// deserialization; the first `_resolve_manifest_*` populates it).
    pub fn manifest(&self) -> Arg {
        self.0._manifest.read().clone()
    }

    /// Bind a live `Manifest`. One-time only. Also records the manifest's
    /// `global_id` so the binding can be re-established after deserialization.
    ///
    /// `AttributeError` if a manifest is already bound; `TypeError` if
    /// `value` is not a `Manifest`.
    pub fn set_manifest(&self, value: impl IntoArg) -> Result<()> {
        let value = value.into_arg();
        if !self.0._manifest.read().is_none() {
            return Err(LailaError::attribute_error(
                "ComplexConstitution manifest is already set and cannot be reassigned.",
            ));
        }
        if !is_manifest(&value) {
            return Err(LailaError::type_error(
                "manifest must be a Manifest instance",
            ));
        }
        let gid = manifest_global_id_of(&value)?;
        *self.0._manifest.write() = value;
        *self.0._manifest_global_id.write() = Some(gid);
        Ok(())
    }

    /// The bound manifest's `global_id`, if known.
    pub fn manifest_global_id(&self) -> Arg {
        match &*self.0._manifest_global_id.read() {
            Some(g) => Arg::Str(g.as_str().into()),
            None => Arg::None,
        }
    }

    fn gid_repr(&self) -> String {
        match &*self.0._manifest_global_id.read() {
            Some(g) => str_repr(g),
            None => "None".into(),
        }
    }

    /// Pools to try for the manifest: the hinted pool, alpha, then the rest
    /// (`laila.get_active_policy().central.memory.pool_router.pools`).
    pub fn _manifest_candidate_pools(pool_hint: Option<&str>) -> Vec<Option<String>> {
        let policy = crate::policy::get_active_policy();
        let alpha = policy.alpha_pool_gid();
        // ``pool_router.pools`` holds every registered pool, the alpha included.
        let known: Vec<String> = policy
            .pool_router()
            .pools()
            .__iter__()
            .map(|ks| ks.iter().map(|k| k.__str__()).collect())
            .unwrap_or_default();
        let mut order: Vec<Option<String>> = Vec::new();
        if let Some(h) = pool_hint {
            if known.iter().any(|k| k == h) {
                order.push(Some(h.to_string()));
            }
        }
        if pool_hint != Some(alpha.as_str()) {
            order.push(None);
        }
        order.extend(
            known
                .into_iter()
                .filter(|g| Some(g.as_str()) != pool_hint && *g != alpha)
                .map(Some),
        );
        order
    }

    fn remember_from(
        &self,
        gid: &str,
        pool: &Option<String>,
    ) -> Result<crate::policy::central::command::Future> {
        let mut a = Args::new();
        a.push_positional(Arg::Str(gid.into()));
        a.push_keyword("dst_pool", pool.clone());
        crate::laila.remember(a)?.into_local()
    }

    fn unresolved(&self, last_exc: Option<LailaError>) -> LailaError {
        let _ = last_exc; // ``raise ... from last_exc``
        LailaError::key_error(str_repr(&alloc::format!(
            "Could not resolve manifest {} from any registered pool",
            self.gid_repr()
        )))
    }

    /// Synchronously resolve `_manifest` from `_manifest_global_id`.
    ///
    /// Cache hit: return the bound manifest without any I/O. Cache miss:
    /// submit a `laila.remember` for the stored gid on each candidate pool
    /// and block on the resulting future. Returns `None` if neither a live
    /// manifest nor a gid is bound.
    #[pysig("_resolve_manifest_sync(pool_hint=None)")]
    pub fn _resolve_manifest_sync(&self, args: impl Into<Args>) -> Result<Arg> {
        let pool_hint = pool_hint.as_str();
        let cached = self.manifest();
        if !cached.is_none() {
            return Ok(cached);
        }
        let Some(gid) = self.0._manifest_global_id.read().clone() else {
            return Ok(Arg::None);
        };
        let mut last_exc = None;
        for pool in Self::_manifest_candidate_pools(pool_hint) {
            let fut = self.remember_from(&gid, &pool)?;
            let resolved = fut.wait(Args::from(Arg::None));
            // Internal, self-consumed future: release it from the bank.
            let _ = fut.release(Args::new());
            match resolved {
                Ok(r) => return self._record_resolved_manifest(r),
                Err(e) if e.kind.is_subclass_of(ExcKind::KeyError) => {
                    last_exc = Some(e);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(self.unresolved(last_exc))
    }

    /// Asynchronously resolve `_manifest` from its global id (`await`s the
    /// `laila.remember` future so the calling task can yield).
    pub async fn _resolve_manifest_async(&self, pool_hint: Option<&str>) -> Result<Arg> {
        let cached = self.manifest();
        if !cached.is_none() {
            return Ok(cached);
        }
        let Some(gid) = self.0._manifest_global_id.read().clone() else {
            return Ok(Arg::None);
        };
        let mut last_exc = None;
        for pool in Self::_manifest_candidate_pools(pool_hint) {
            let fut = self.remember_from(&gid, &pool)?;
            let resolved = fut.__await__().await;
            // Internal, self-consumed future: release it from the bank.
            let _ = fut.release(Args::new());
            match resolved {
                Ok(r) => return self._record_resolved_manifest(r),
                Err(e) if e.kind.is_subclass_of(ExcKind::KeyError) => {
                    last_exc = Some(e);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(self.unresolved(last_exc))
    }

    /// `_resolve_manifest(*, asynchronous=False, pool_hint=None)`: dispatch
    /// to `_resolve_manifest_async` (returned as a coroutine object) or
    /// `_resolve_manifest_sync` (blocks).
    #[pysig("_resolve_manifest(*, asynchronous=False, pool_hint=None)")]
    pub fn _resolve_manifest(&self, args: impl Into<Args>) -> Result<Arg> {
        if asynchronous.__bool__() {
            let this = self.clone();
            let hint = pool_hint.as_str().map(alloc::string::ToString::to_string);
            return Ok(crate::python::stdlib::asyncio::coroutine(async move {
                this._resolve_manifest_async(hint.as_deref()).await
            }));
        }
        self._resolve_manifest_sync(Args::from(pool_hint))
    }

    /// Cache the resolved manifest in `_manifest` and return it.
    ///
    /// `laila.remember` returns a list when it was given a list of ids; a
    /// single-element list collapses to its lone element. `RuntimeError` if
    /// the lookup returned `None`.
    pub fn _record_resolved_manifest(&self, resolved: impl IntoArg) -> Result<Arg> {
        let mut resolved = resolved.into_arg();
        if let Arg::List(_) = &resolved {
            resolved = resolved.__iter__()?.into_iter().next().unwrap_or(Arg::None);
        }
        if resolved.is_none() {
            return Err(LailaError::runtime_error(alloc::format!(
                "Could not resolve manifest from global_id {}: not found in active memory.",
                self.gid_repr()
            )));
        }
        *self.0._manifest.write() = resolved.clone();
        Ok(resolved)
    }

    /// Resolve the manifest and apply the single constitution callable.
    ///
    /// `payload_input` is ignored -- complex constitutions read from their
    /// bound manifest, never from a serialized blob. Always uses the *sync*
    /// manifest-resolution path (coroutine-aware callers pre-resolve with
    /// `_resolve_manifest_async` so this is a cache hit).
    ///
    /// `RuntimeError` if no constitution code is set, or the manifest cannot
    /// be resolved.
    #[pysig("build(self, payload_input=None)")]
    pub fn build(&self, args: impl Into<Args>) -> Result<Arg> {
        let _ = payload_input;
        let Some(code) = self.0._code.read().clone() else {
            return Err(LailaError::runtime_error("no constitution code defined."));
        };

        let target = self._resolve_manifest_sync(())?;
        if target.is_none() {
            return Err(LailaError::runtime_error(
                "no manifest available to run constitution.",
            ));
        }

        // with laila.guarantee: target.realized
        realize(&target)?;

        _exec_one_fn(&code)?(&target)
    }

    /// Serialize as `{"_kind": "complex", "code", "manifest_global_id", "_hmac"}`.
    ///
    /// `_hmac` is an HMAC-SHA256 over the canonical code/manifest pair keyed
    /// by the per-user constitution secret; `_from_dict` refuses dicts whose
    /// signature does not verify.
    pub fn as_dict(&self) -> Result<Arg> {
        let code = self.code();
        let gid = self.manifest_global_id();
        let mut d = PyDictBuilder::new();
        d.insert("_kind", Arg::Str(Self::_KIND.into()));
        d.insert("code", code.clone());
        d.insert("manifest_global_id", gid.clone());
        d.insert("_hmac", Arg::Str(sign_complex(code, gid).into()));
        Ok(d.finish())
    }

    /// Build a `ComplexConstitution` from its serialized dict.
    ///
    /// `ValueError` if the `_hmac` signature is missing or does not verify
    /// against this user's constitution secret.
    pub fn _from_dict(in_dict: &Arg) -> Result<ComplexConstitution> {
        let get = |k: &str| -> Result<Arg> {
            if in_dict.__contains__(k)? {
                in_dict.__getitem__(k)
            } else {
                Ok(Arg::None)
            }
        };
        let code = get("code")?;
        let manifest_global_id = get("manifest_global_id")?;
        if !verify_complex(code.clone(), manifest_global_id.clone(), get("_hmac")?) {
            return Err(LailaError::value_error(
                "untrusted constitution: complex constitution signature missing or invalid \
                 (written by a process without this user's ~/.laila/constitution.key)",
            ));
        }
        let mut a = Args::new();
        a.push_keyword("code", code);
        a.push_keyword("manifest_global_id", manifest_global_id);
        ComplexConstitution::new(a)
    }

    /// pydantic `repr`: private attributes are not shown.
    pub fn __repr__(&self) -> String {
        "ComplexConstitution()".into()
    }
}

impl core::fmt::Debug for ComplexConstitution {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__repr__())
    }
}

impl PartialEq for ComplexConstitution {
    /// pydantic equality over the private attributes.
    fn eq(&self, other: &ComplexConstitution) -> bool {
        self.is(other)
            || (*self.0._code.read() == *other.0._code.read()
                && *self.0._manifest_global_id.read() == *other.0._manifest_global_id.read()
                && self.manifest() == other.manifest())
    }
}

impl PyObject for ComplexConstitution {
    fn as_any(&self) -> &dyn Any {
        self
    }
    /// `a is b`: identity of the shared handle, not of the boxing.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<ComplexConstitution>()
            .is_some_and(|o| self.is(o))
    }
    fn type_name(&self) -> &'static str {
        "ComplexConstitution"
    }
    fn __repr__(&self) -> String {
        ComplexConstitution::__repr__(self)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<ComplexConstitution>()
            .is_some_and(|o| self == o)
    }
    fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error(
            "unhashable type: 'ComplexConstitution'",
        ))
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "code" => Ok(self.code()),
            "manifest" => Ok(self.manifest()),
            "manifest_global_id" => Ok(self.manifest_global_id()),
            "_kind" => Ok(Arg::Str(Self::_KIND.into())),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'ComplexConstitution' object has no attribute '{other}'"
            ))),
        }
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        match name {
            "code" => self.set_code(value),
            "manifest" => self.set_manifest(value),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'ComplexConstitution' object has no attribute '{other}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        super::constitution::call_method(&Constitution::Complex(self.clone()), name, args)
    }
}

impl IntoArg for ComplexConstitution {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for ComplexConstitution {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl IsInstance for ComplexConstitution {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<ComplexConstitution>())
    }
}
