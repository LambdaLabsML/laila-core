//! Globally-atomic identifiable object with secret-gated locking.
//!
//! Extends [`_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT`](super::_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT)
//! with a *global* (policy-scoped) lock layered on top of the local one. The
//! global lock is bound to a single `policy_id` while it is held; a secret
//! token returned from `lock_global` is the only key that can release the
//! lock or pass through the secret gate.
//!
//! Use this for resources that need to be coordinated across policies in the
//! same process (or, in the future, across hosts via the distributed lock
//! manager). For the much more common per-instance, single-policy case,
//! prefer the locally-atomic mixin -- the global machinery here adds overhead
//! that is wasted unless multiple policies genuinely contend for the same
//! object.
//!
//! # Gate semantics
//!
//! Only *public* callables defined by a subclass (`bump`, `write`, ...) are
//! gated. Private / dunder names and everything defined on this class or its
//! bases (`lock_global`, `atomic`, `model_dump`, ...) are never wrapped, so
//! the lock machinery can call itself. While no global lock is held the gate
//! is a cheap name check and the call proceeds untouched.
//!
//! While a global lock is held, a gated call passes when either the
//! `secret=` keyword matches the holder token or the call happens inside the
//! holder's own `with obj.atomic(scope="global", ...)` block (the secret
//! travels in a context variable). Otherwise `PermissionError` is **raised**.
//!
//! Python installs the gate from `__getattribute__`; Rust has no attribute
//! interception, so `laila::python!` installs it when it expands a class
//! that implements this trait: every public method of the `impl` block is
//! wrapped in a call to [`__gate__`](_LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT::__gate__)
//! that strips `secret=` and verifies it (see `laila-macros`, "gated types").
//!
//! `lock_global` is reference-counted per holder policy: a nested acquire by
//! the holder returns the same token and must be matched by its own
//! `unlock_global`; the lock is released when the count reaches zero.
//!
//! The global state is kept in the object's `__dict__` under Python's
//! private-attribute names (`_global_lock`, `_holder_policy_id`,
//! `_holder_secret`, `_hold_count`), every access to it under the local lock
//! -- exactly the Python layout.
//!
// MERGE: the "global" lock is process-wide here, as in the Python source
// (policy ids are plain strings; no distributed lock manager yet). When the
// policy / central-memory layer lands, `lock_global_attrs` is the single
// seam to route through a distributed lock manager.

use alloc::string::String;

use crate::python::args::Args;
use crate::python::errors::{ExcKind, LailaError, Result};
use crate::python::object::{Arg, HasAttrs, IntoArg, PyAttrs};
use crate::python::pysig;

use super::locally_atomic_identifiable_object::HasIdentity;
use super::locally_atomic_object::{atomic_context, lock_attrs, unlock_attrs, AtomicContext};

// ---------------------------------------------------------------------------
// `_HELD_SECRET: ContextVar[str | None]`
// ---------------------------------------------------------------------------

/// Token returned by [`HeldSecret::set`]; restores the previous value on `reset`.
#[derive(Debug)]
pub struct HeldSecretToken(Option<String>);

/// Secret of the global lock held by the *current* context (set inside
/// `atomic(scope="global")` so the holder's body passes the gate). A
/// thread-local on `std` (the context is the thread); a single global slot
/// on `no_std` (one execution context).
pub struct HeldSecret;

#[cfg(feature = "std")]
std::thread_local! {
    static HELD_SECRET: core::cell::RefCell<Option<String>> = const { core::cell::RefCell::new(None) };
}
#[cfg(not(feature = "std"))]
static HELD_SECRET: crate::python::sync::Mutex<Option<String>> =
    crate::python::sync::Mutex::new(None);

impl HeldSecret {
    /// `_HELD_SECRET.get()`
    pub fn get(&self) -> Option<String> {
        #[cfg(feature = "std")]
        {
            HELD_SECRET.with(|c| c.borrow().clone())
        }
        #[cfg(not(feature = "std"))]
        {
            HELD_SECRET.lock().clone()
        }
    }
    /// `_HELD_SECRET.set(value)` -> token
    pub fn set(&self, value: Option<String>) -> HeldSecretToken {
        #[cfg(feature = "std")]
        {
            HeldSecretToken(HELD_SECRET.with(|c| core::mem::replace(&mut *c.borrow_mut(), value)))
        }
        #[cfg(not(feature = "std"))]
        {
            HeldSecretToken(core::mem::replace(&mut *HELD_SECRET.lock(), value))
        }
    }
    /// `_HELD_SECRET.reset(token)`
    pub fn reset(&self, token: HeldSecretToken) {
        let _ = self.set(token.0);
    }
}

/// `_HELD_SECRET`
#[allow(non_upper_case_globals)]
pub static _HELD_SECRET: HeldSecret = HeldSecret;

// ---------------------------------------------------------------------------
// Private state in `__dict__` (pydantic `PrivateAttr` defaults)
// ---------------------------------------------------------------------------

fn get_bool(attrs: &PyAttrs, name: &str) -> bool {
    attrs.get(name).map(|v| v.__bool__()).unwrap_or(false)
}
fn get_opt_str(attrs: &PyAttrs, name: &str) -> Option<String> {
    match attrs.get(name) {
        None | Some(Arg::None) => None,
        Some(v) => Some(v.__str__()),
    }
}
fn get_count(attrs: &PyAttrs, name: &str) -> i64 {
    attrs.get(name).and_then(|v| v.as_i64()).unwrap_or(0)
}
fn set(attrs: &PyAttrs, name: &str, v: impl IntoArg) {
    let _ = attrs.__setattr__(name, v);
}

/// `secrets.token_hex(8)`
fn token_hex8() -> String {
    let bytes = crate::_compat::random::urandom(8);
    let mut s = String::with_capacity(16);
    for b in bytes {
        s.push_str(&alloc::format!("{b:02x}"));
    }
    s
}

/// Hold the local lock (with `timeout_s`) around `f`; `TimeoutError` as the
/// local `atomic()` raises it.
fn with_local<R>(
    attrs: &PyAttrs,
    timeout_s: Option<f64>,
    f: impl FnOnce() -> Result<R>,
) -> Result<R> {
    if !lock_attrs(attrs, timeout_s) {
        return Err(LailaError::new(
            ExcKind::TimeoutError,
            "Timed out acquiring local lock.",
        ));
    }
    let r = f();
    unlock_attrs(attrs)?;
    r
}

/// `lock_global(policy_id, *, timeout_s=None)` on an object's `__dict__`:
/// the holder token (`Some`), or `None` when another policy holds the lock.
pub fn lock_global_attrs(
    attrs: &PyAttrs,
    policy_id: &str,
    timeout_s: Option<f64>,
) -> Result<Option<String>> {
    with_local(attrs, timeout_s, || {
        if get_bool(attrs, "_global_lock") {
            if get_opt_str(attrs, "_holder_policy_id").as_deref() == Some(policy_id) {
                if let Some(secret) = get_opt_str(attrs, "_holder_secret") {
                    set(attrs, "_hold_count", get_count(attrs, "_hold_count") + 1);
                    return Ok(Some(secret));
                }
            }
            return Ok(None);
        }
        set(attrs, "_global_lock", true);
        set(attrs, "_holder_policy_id", policy_id);
        let secret = token_hex8();
        set(attrs, "_holder_secret", secret.as_str());
        set(attrs, "_hold_count", 1i64);
        Ok(Some(secret))
    })
}

/// `unlock_global(secret)` on an object's `__dict__`: release one hold if
/// *secret* matches the holder token; a wrong secret is a silent no-op.
pub fn unlock_global_attrs(attrs: &PyAttrs, secret: &Arg) -> Result<()> {
    with_local(attrs, None, || {
        let holder = get_opt_str(attrs, "_holder_secret");
        let given = match secret {
            Arg::None => None,
            s => Some(s.__str__()),
        };
        if given.is_none() || given != holder {
            return Ok(());
        }
        let count = get_count(attrs, "_hold_count") - 1;
        set(attrs, "_hold_count", count);
        if count > 0 {
            return Ok(());
        }
        set(attrs, "_global_lock", false);
        set(attrs, "_holder_policy_id", Arg::None);
        set(attrs, "_holder_secret", Arg::None);
        set(attrs, "_hold_count", 0i64);
        Ok(())
    })
}

/// `global_locked(*, timeout_s=None)`: `True` if the global lock is held
/// (or the local lock times out).
pub fn global_locked_attrs(attrs: &PyAttrs, timeout_s: Option<f64>) -> bool {
    match with_local(attrs, timeout_s, || Ok(get_bool(attrs, "_global_lock"))) {
        Ok(v) => v,
        Err(e) if e.kind == ExcKind::TimeoutError => true,
        Err(_) => true,
    }
}

/// `_verify_secret(secret, *, timeout_s=None)`: *secret* (or the context's
/// held secret) matches the holder token; `False` on local-lock timeout.
pub fn verify_secret_attrs(attrs: &PyAttrs, secret: Option<&str>, timeout_s: Option<f64>) -> bool {
    let r = with_local(attrs, timeout_s, || {
        if !get_bool(attrs, "_global_lock") {
            return Ok(true);
        }
        let holder = get_opt_str(attrs, "_holder_secret");
        if secret.is_some() && secret.map(String::from) == holder {
            return Ok(true);
        }
        let held = _HELD_SECRET.get();
        Ok(held.is_some() && held == holder)
    });
    r.unwrap_or(false)
}

/// Names that are never gated: everything this class and its bases define
/// (lock machinery, pydantic API, identity helpers). `laila::python!` consults
/// the same list when it wraps a subclass's public methods.
pub const UNGATED_NAMES: &[&str] = &[
    // _LAILA_LOCALLY_ATOMIC_OBJECT / _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT
    "atomic",
    "lock",
    "unlock",
    "locked",
    "creation_timestamp",
    // _LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT
    "lock_global",
    "unlock_global",
    "global_locked",
    // _LAILA_IDENTIFIABLE_OBJECT
    "global_id",
    "uuid",
    "scopes",
    "evolution",
    "process_global_id",
    "generate_uuid_from_nickname",
    "resolve_global_id",
    "as_dict",
    "from_dict",
    // pydantic BaseModel
    "model_dump",
    "model_dump_json",
    "model_validate",
    "model_validate_json",
    "model_construct",
    "model_copy",
    "model_fields",
    "model_post_init",
    "model_json_schema",
    "dict",
    "json",
    "copy",
    "new",
];

/// Identifiable object that coordinates access via a policy-scoped global lock.
///
/// Adds three machinery pieces on top of the local-lock base:
///
/// - [`lock_global`](Self::lock_global) / [`unlock_global`](Self::unlock_global)
///   -- explicit, reference-counted global acquire/release driven by a
///   per-policy `policy_id` and a secret token returned at acquire-time.
/// - [`atomic`](Self::atomic) (overridden) -- accepts `scope="global"` to hold
///   the global lock for a `with` block, raising `PermissionError` when
///   another policy holds it. Inside the block the holder's own calls pass
///   the gate without `secret=`.
/// - [`__gate__`](Self::__gate__) -- while a global lock is held, public
///   subclass callables require the holder secret (`secret=` kwarg or the
///   enclosing global block) and raise `PermissionError` otherwise.
///
/// Global state (private, in `__dict__`): `_global_lock`, `_holder_policy_id`,
/// `_holder_secret`, `_hold_count`.
#[allow(non_camel_case_types)]
pub trait _LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT: HasAttrs + HasIdentity {
    crate::local_atomic_methods!();

    /// `_ungated()`: the names the gate never wraps.
    fn _ungated(&self) -> &'static [&'static str] {
        UNGATED_NAMES
    }

    /// `lock_global(policy_id, *, timeout_s=None) -> str | None`: acquire (or
    /// re-acquire) the global lock for a policy. Returns the secret token
    /// (the *same* token, hold count incremented, when the holder
    /// re-acquires), `None` if another policy holds the lock.
    #[pysig("lock_global(self, policy_id, *, timeout_s=None)")]
    fn lock_global(&self, args: impl Into<Args>) -> Result<Arg> {
        let timeout = super::locally_atomic_object::timeout_arg(&timeout_s)?;
        Ok(
            match lock_global_attrs(self.__attrs__(), &policy_id.__str__(), timeout)? {
                Some(s) => Arg::Str(s.into()),
                None => Arg::None,
            },
        )
    }

    /// `unlock_global(secret)`: release one hold if *secret* matches the
    /// holder token; released for real when the hold count reaches zero. A
    /// wrong secret is a silent no-op.
    fn unlock_global(&self, secret: impl IntoArg) -> Result<()> {
        unlock_global_attrs(self.__attrs__(), &secret.into_arg())
    }

    /// `global_locked(*, timeout_s=None) -> bool`
    #[pysig("global_locked(self, *, timeout_s=None)")]
    fn global_locked(&self, args: impl Into<Args>) -> Result<bool> {
        let timeout = super::locally_atomic_object::timeout_arg(&timeout_s)?;
        Ok(global_locked_attrs(self.__attrs__(), timeout))
    }

    /// `_verify_secret(secret, *, timeout_s=None) -> bool`
    #[pysig("_verify_secret(self, secret, *, timeout_s=None)")]
    fn _verify_secret(&self, args: impl Into<Args>) -> Result<bool> {
        let timeout = super::locally_atomic_object::timeout_arg(&timeout_s)?;
        let s = match &secret {
            Arg::None => None,
            v => Some(v.__str__()),
        };
        Ok(verify_secret_attrs(self.__attrs__(), s.as_deref(), timeout))
    }

    /// `with obj.atomic(*, scope="local", timeout_s=None, policy_id=None) as locked:`
    /// -- `"local"` for the local lock, `"global"` for the policy-scoped
    /// global lock (`policy_id` required; `PermissionError` when another
    /// policy holds it). Inside a global block the holder's calls on the
    /// object pass the gate without `secret=`.
    #[pysig("atomic(self, *, scope=\"local\", timeout_s=None, policy_id=None)")]
    fn atomic(&self, args: impl Into<Args>) -> Result<AtomicContext<Self>>
    where
        Self: Clone + Sized,
    {
        atomic_context(
            self,
            scope,
            timeout_s,
            policy_id,
            "_LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT",
            true,
        )
    }

    /// `self._global_lock`: whether the global lock is currently held.
    fn _global_lock(&self) -> bool {
        get_bool(self.__attrs__(), "_global_lock")
    }
    /// `self._holder_policy_id`: which policy currently holds it.
    fn _holder_policy_id(&self) -> Arg {
        self.__attrs__()
            .get("_holder_policy_id")
            .unwrap_or(Arg::None)
    }
    /// `self._holder_secret`: the token that proves you're the holder.
    fn _holder_secret(&self) -> Arg {
        self.__attrs__().get("_holder_secret").unwrap_or(Arg::None)
    }
    /// `self._hold_count`: nested acquisitions by the holder.
    fn _hold_count(&self) -> i64 {
        get_count(self.__attrs__(), "_hold_count")
    }

    /// The secret gate (Python's `__getattribute__` wrapper): called by the
    /// generated wrapper of every public subclass method with the method
    /// name and its call arguments. While no global lock is held the
    /// arguments pass untouched. While it is held, `secret=` is popped from
    /// the keywords and verified (or the enclosing global block's secret is);
    /// on failure `PermissionError` is raised.
    fn __gate__(&self, name: &str, args: Args) -> Result<Args> {
        if name.starts_with('_') || self._ungated().contains(&name) {
            return Ok(args);
        }
        let attrs = self.__attrs__();
        // Benign unlocked read: a lock taken between this check and the
        // call is the same race a caller would have with any lock.
        if !get_bool(attrs, "_global_lock") {
            return Ok(args);
        }
        let mut args = args;
        let secret = args.pop_or("secret", Arg::None);
        let s = match &secret {
            Arg::None => None,
            v => Some(v.__str__()),
        };
        if !verify_secret_attrs(attrs, s.as_deref(), None) {
            return Err(LailaError::new(
                ExcKind::PermissionError,
                alloc::format!(
                    "{}.{name}() is globally locked by policy {}; pass secret=<token> or call it \
                     inside the holder's atomic(scope='global') block.",
                    self.__class_name__(),
                    self._holder_policy_id().__repr__()
                ),
            ));
        }
        Ok(args)
    }
}
