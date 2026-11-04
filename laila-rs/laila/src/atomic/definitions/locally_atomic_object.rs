//! Locally-atomic base class providing reentrant per-instance locking.
//!
//! The single trait here, [`_LAILA_LOCALLY_ATOMIC_OBJECT`], mixes a
//! lazily-created [`RLock`] into any object with a Python `__dict__`
//! ([`HasAttrs`]) and exposes a uniform critical-section API (`lock` /
//! `unlock` / `atomic`). It is the foundation for every laila object that
//! needs to coordinate access from multiple threads -- pools, central
//! memory, the future bank, taskforces, and so on.
//!
//! "Locally" means *one lock per instance, in the current process*. For
//! cross-process coordination, see
//! [`_LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT`](super::_LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT)
//! which adds a distributed lock alongside the local one.
//!
//! The lock is a re-entrant `threading.RLock` so the same thread can nest
//! critical sections (e.g. `with self.atomic(): self.helper()` where
//! `helper` also calls `with self.atomic(): ...`) without dead-locking.
//!
//! Python stores the lock under the private `_local_lock` attribute via
//! `object.__setattr__`; the Rust twin stores it under the same name in the
//! object's [`PyAttrs`], so `py_class!` structs get the behaviour with an
//! empty `impl _LAILA_LOCALLY_ATOMIC_OBJECT for Name {}` and typed structs
//! (`Entry`, pools, the `Atomic*` wrappers) by carrying a `PyAttrs`.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::python::args::Args;
use crate::python::errors::{ExcKind, LailaError, Result};
use crate::python::object::{Arg, HasAttrs, IntoArg, PyAttrs, PyObject};
use crate::python::pysig;
use crate::python::sync::Mutex;

pub use crate::python::sync::RLock;

/// The `__dict__` key Python keeps the per-instance lock under.
pub const LOCAL_LOCK_ATTR: &str = "_local_lock";

// The creation stamp comes from `basics::definitions::laila_object`
// (`_LAILA_OBJECT`); this wrapper only turns it into the `Arg` the `Atomic*`
// constructors store under `_creation_timestamp`.
/// `datetime.now(UTC).isoformat(timespec="milliseconds")`.
pub fn _now_creation_timestamp() -> Arg {
    Arg::Str(crate::basics::definitions::laila_object::_now_creation_timestamp().into())
}

// ---------------------------------------------------------------------------
// `threading.RLock` as a Python object, so the lock can live in `__dict__`.
// ---------------------------------------------------------------------------

impl PyObject for RLock {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "RLock"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<{} _thread.RLock object count={}>",
            if self.locked() { "locked" } else { "unlocked" },
            self.recursion_count()
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<RLock>()
            .is_some_and(|o| o.is(self))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(self.id() as i64)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        Ok(match name {
            "acquire" => {
                let blocking = args
                    .positional
                    .first()
                    .cloned()
                    .unwrap_or_else(|| args.get("blocking"));
                let blocking = blocking.is_none() || blocking.__bool__();
                let timeout = args
                    .positional
                    .get(1)
                    .cloned()
                    .unwrap_or_else(|| args.get("timeout"))
                    .as_f64()
                    .unwrap_or(-1.0);
                let t = if !blocking {
                    Some(0.0)
                } else if timeout >= 0.0 {
                    Some(timeout)
                } else {
                    None
                };
                Arg::Bool(self.acquire(t))
            }
            "release" => {
                self.release()?;
                Arg::None
            }
            "locked" => Arg::Bool(self.locked()),
            "_is_owned" => Arg::Bool(self.is_owned()),
            "_recursion_count" => Arg::Int(self.recursion_count() as i64),
            _ => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'RLock' object has no attribute '{name}'"
                )))
            }
        })
    }
}
impl IntoArg for RLock {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl crate::python::object::PyArg for RLock {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}

/// `_LOCK_FACTORIES` / `_LOCK_NAMES`: the lock kinds a laila object may hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockKind {
    /// `threading.RLock` (`"rlock"`)
    RLock,
    /// `threading.Lock` (`"lock"`)
    Lock,
}

impl LockKind {
    pub fn name(self) -> &'static str {
        match self {
            LockKind::RLock => "rlock",
            LockKind::Lock => "lock",
        }
    }
    pub fn from_name(name: &str) -> Option<LockKind> {
        match name {
            "rlock" => Some(LockKind::RLock),
            "lock" => Some(LockKind::Lock),
            _ => None,
        }
    }
    /// `_LOCK_FACTORIES[name]()`: a fresh lock of this kind.
    pub fn new_lock(self) -> Arg {
        match self {
            LockKind::RLock => RLock::new().into_arg(),
            #[cfg(feature = "std")]
            LockKind::Lock => crate::python::stdlib::Lock::default().into_arg(),
            // No non-reentrant `threading.Lock` shim without threads; an
            // `RLock` is the closest primitive.
            #[cfg(not(feature = "std"))]
            LockKind::Lock => RLock::new().into_arg(),
        }
    }
    /// `_LOCK_NAMES[type(v)]`: the kind of a lock value, `None` for non-locks.
    pub fn of(v: &Arg) -> Option<LockKind> {
        let o = match v {
            Arg::Object(o) => o,
            _ => return None,
        };
        if o.as_any().is::<RLock>() {
            return Some(LockKind::RLock);
        }
        #[cfg(feature = "std")]
        {
            if o.as_any().is::<crate::python::stdlib::RLock>() {
                return Some(LockKind::RLock);
            }
            if o.as_any().is::<crate::python::stdlib::Lock>() {
                return Some(LockKind::Lock);
            }
        }
        None
    }
}

/// The copyable entries of a `__dict__` snapshot.
pub type PlainAttrs = Vec<(String, Arg)>;
/// The lock entries of a `__dict__` snapshot, by kind.
pub type LockAttrs = Vec<(String, LockKind)>;

/// `_split_locks(mapping)`: split a `__dict__` snapshot into
/// `(copyable entries, {key: lock kind name})`.
pub fn _split_locks(mapping: &[(String, Arg)]) -> (PlainAttrs, LockAttrs) {
    let mut plain = Vec::new();
    let mut locks = Vec::new();
    for (k, v) in mapping {
        match LockKind::of(v) {
            Some(kind) => locks.push((k.clone(), kind)),
            None => plain.push((k.clone(), v.clone())),
        }
    }
    (plain, locks)
}

// ---------------------------------------------------------------------------
// Shared lock plumbing (free functions so the three mixin traits -- local,
// local+identifiable, global -- share one implementation without inheriting
// from each other, which would make `obj.atomic()` ambiguous in Rust).
// ---------------------------------------------------------------------------

/// `_ensure_local_lock`: the instance's `RLock`, created on first use and
/// stored under `_local_lock` (one atomic check-and-insert).
pub fn ensure_local_lock(attrs: &PyAttrs) -> RLock {
    let found = attrs.__setdefault__(LOCAL_LOCK_ATTR, || RLock::new().into_arg());
    match found.downcast_ref::<RLock>() {
        Some(l) => l.clone(),
        None => {
            // `_local_lock` was overwritten with a foreign value (Python would
            // fail on `.acquire()`); replace it with a lock, as `object.__setattr__` did.
            let l = RLock::new();
            let _ = attrs.__setattr__(LOCAL_LOCK_ATTR, l.clone());
            l
        }
    }
}

/// `Optional[float]` from a `timeout_s` argument.
pub(crate) fn timeout_arg(v: &Arg) -> Result<Option<f64>> {
    match v {
        Arg::None => Ok(None),
        other => other.as_f64().map(Some).ok_or_else(|| {
            LailaError::type_error(alloc::format!(
                "'{}' object cannot be interpreted as a float",
                other.type_name()
            ))
        }),
    }
}

/// `lock(timeout_s)` on an object's `__dict__`.
pub fn lock_attrs(attrs: &PyAttrs, timeout_s: Option<f64>) -> bool {
    let local_lock = ensure_local_lock(attrs);
    match timeout_s {
        None => {
            local_lock.acquire(None);
            true
        }
        Some(t) => local_lock.acquire(Some(t)),
    }
}

/// `unlock()` on an object's `__dict__`.
pub fn unlock_attrs(attrs: &PyAttrs) -> Result<()> {
    ensure_local_lock(attrs).release()
}

/// The locally-atomic verbs (`lock` / `unlock` / `locked` / `atomic`)
/// called on the *dynamic* layer of any `__dict__`-backed object (a
/// `py_class!` instance that mixes in `_LAILA_LOCALLY_ATOMIC_OBJECT`
/// reached through an `Arg`). `None` for any other name.
#[doc(hidden)]
pub fn dyn_atomic_call_method(
    attrs: &PyAttrs,
    class: &'static str,
    name: &str,
    args: Args,
) -> Option<Result<Arg>> {
    Some(match name {
        "lock" => {
            let timeout = args
                .positional
                .first()
                .cloned()
                .unwrap_or_else(|| args.get("timeout_s"));
            match timeout_arg(&timeout) {
                Ok(t) => Ok(Arg::Bool(lock_attrs(attrs, t))),
                Err(e) => Err(e),
            }
        }
        "unlock" => unlock_attrs(attrs).map(|()| Arg::None),
        "locked" => Ok(Arg::Bool(ensure_local_lock(attrs).locked())),
        "atomic" => {
            DynAtomicContext::new(ensure_local_lock(attrs), class, args).map(IntoArg::into_arg)
        }
        _ => return None,
    })
}

// ---------------------------------------------------------------------------
// The `with obj.atomic(...)` context manager
// ---------------------------------------------------------------------------

/// The context manager returned by `obj.atomic(...)`; `__enter__` acquires
/// and yields the locked instance (as [`AtomicGuard`]), `__exit__` is the
/// guard's drop. Scope validation happens on `__enter__`, as the Python
/// `@contextmanager` generator body does.
pub struct AtomicContext<T> {
    pub(crate) obj: T,
    pub(crate) scope: String,
    pub(crate) timeout_s: Option<f64>,
    pub(crate) policy_id: Option<String>,
    /// The class whose `atomic()` was called (for the `ValueError` text).
    pub(crate) class: &'static str,
    /// Whether `scope="global"` is understood (the global mixin).
    pub(crate) allows_global: bool,
}

/// How an [`AtomicGuard`] releases on drop.
pub(crate) enum Release {
    Local,
    Global {
        secret: String,
        token: super::globally_atomic_identifiable_object::HeldSecretToken,
    },
}

/// The locked instance yielded by `with obj.atomic() as locked:`; derefs to
/// the object and releases the lock when dropped (`__exit__`).
pub struct AtomicGuard<T: HasAttrs> {
    pub(crate) obj: T,
    pub(crate) release: Option<Release>,
}

impl<T: HasAttrs> core::ops::Deref for AtomicGuard<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.obj
    }
}

impl<T: HasAttrs> AtomicGuard<T> {
    /// `__exit__`: release now (also happens on drop).
    pub fn __exit__(mut self) -> Result<()> {
        self.release_now()
    }
    fn release_now(&mut self) -> Result<()> {
        match self.release.take() {
            None => Ok(()),
            Some(Release::Local) => unlock_attrs(self.obj.__attrs__()),
            Some(Release::Global { secret, token }) => {
                super::globally_atomic_identifiable_object::_HELD_SECRET.reset(token);
                super::globally_atomic_identifiable_object::unlock_global_attrs(
                    self.obj.__attrs__(),
                    &Arg::Str(secret.into()),
                )
            }
        }
    }
}

impl<T: HasAttrs> Drop for AtomicGuard<T> {
    fn drop(&mut self) {
        let _ = self.release_now();
    }
}

impl<T: HasAttrs + Clone> AtomicContext<T> {
    /// `__enter__`: validate the scope, acquire, yield the locked instance.
    pub fn __enter__(&self) -> Result<AtomicGuard<T>> {
        if self.scope == "local" {
            if !lock_attrs(self.obj.__attrs__(), self.timeout_s) {
                return Err(LailaError::new(
                    ExcKind::TimeoutError,
                    "Timed out acquiring local lock.",
                ));
            }
            return Ok(AtomicGuard {
                obj: self.obj.clone(),
                release: Some(Release::Local),
            });
        }
        if !self.allows_global || self.scope != "global" {
            return Err(LailaError::value_error(alloc::format!(
                "Invalid scope for {}.atomic() call.",
                self.class
            )));
        }
        let policy_id = match &self.policy_id {
            Some(p) => p.clone(),
            None => {
                return Err(LailaError::value_error(
                    "policy_id is required for global atomic scope.",
                ))
            }
        };
        let secret = super::globally_atomic_identifiable_object::lock_global_attrs(
            self.obj.__attrs__(),
            &policy_id,
            self.timeout_s,
        )?;
        let secret = match secret {
            Some(s) => s,
            None => {
                return Err(LailaError::new(
                    ExcKind::PermissionError,
                    "Global lock already held by another policy.",
                ))
            }
        };
        let token =
            super::globally_atomic_identifiable_object::_HELD_SECRET.set(Some(secret.clone()));
        Ok(AtomicGuard {
            obj: self.obj.clone(),
            release: Some(Release::Global { secret, token }),
        })
    }
}

/// `obj.atomic(..)` on the *dynamic* layer (an `Arg` holding a pool, a data
/// container, a model instance): the context manager as a Python object.
/// `__enter__` acquires the local lock and returns a guard object whose
/// drop (or `__exit__`) releases it.
#[derive(Clone)]
pub struct DynAtomicContext {
    lock: RLock,
    scope: String,
    timeout_s: Option<f64>,
    class: &'static str,
}

impl DynAtomicContext {
    /// `atomic(*, scope="local", timeout_s=None)` bound from *args* over *lock*.
    pub fn new(lock: RLock, class: &'static str, args: Args) -> Result<DynAtomicContext> {
        let scope = args.get("scope");
        let scope = if scope.is_none() {
            String::from("local")
        } else {
            scope.__str__()
        };
        Ok(DynAtomicContext {
            lock,
            scope,
            timeout_s: timeout_arg(&args.get("timeout_s"))?,
            class,
        })
    }
    /// `__enter__`
    pub fn __enter__(&self) -> Result<Arg> {
        if self.scope != "local" {
            return Err(LailaError::value_error(alloc::format!(
                "Invalid scope for {}.atomic() call.",
                self.class
            )));
        }
        let acquired = match self.timeout_s {
            None => {
                self.lock.acquire(None);
                true
            }
            Some(t) => self.lock.acquire(Some(t)),
        };
        if !acquired {
            return Err(LailaError::new(
                ExcKind::TimeoutError,
                "Timed out acquiring local lock.",
            ));
        }
        Ok(DynAtomicGuard {
            lock: self.lock.clone(),
            held: Mutex::new(true),
        }
        .into_arg())
    }
}

impl PyObject for DynAtomicContext {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "_GeneratorContextManager"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<contextlib._GeneratorContextManager object ({}.atomic)>",
            self.class
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        core::ptr::eq(
            other
                .as_any()
                .downcast_ref::<DynAtomicContext>()
                .map_or(core::ptr::null(), |o| o),
            self,
        )
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(self as *const DynAtomicContext as usize as i64)
    }
    fn __call_method__(&self, name: &str, _args: Args) -> Result<Arg> {
        match name {
            "__enter__" => self.__enter__(),
            "__exit__" => Ok(Arg::Bool(false)),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'_GeneratorContextManager' object has no attribute '{other}'"
            ))),
        }
    }
    fn __bool__(&self) -> bool {
        true
    }
}
impl IntoArg for DynAtomicContext {
    fn into_arg(self) -> Arg {
        Arg::Object(alloc::sync::Arc::new(self))
    }
}

/// The locked instance of a [`DynAtomicContext`]: releases on drop / `__exit__`.
pub struct DynAtomicGuard {
    lock: RLock,
    held: Mutex<bool>,
}

impl DynAtomicGuard {
    fn release(&self) -> Result<()> {
        let mut g = self.held.lock();
        if *g {
            *g = false;
            return self.lock.release();
        }
        Ok(())
    }
}
impl Drop for DynAtomicGuard {
    fn drop(&mut self) {
        let _ = self.release();
    }
}
impl PyObject for DynAtomicGuard {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "AtomicGuard"
    }
    fn __repr__(&self) -> String {
        String::from("<locked instance>")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        core::ptr::eq(
            other
                .as_any()
                .downcast_ref::<DynAtomicGuard>()
                .map_or(core::ptr::null(), |o| o),
            self,
        )
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(self as *const DynAtomicGuard as usize as i64)
    }
    fn __call_method__(&self, name: &str, _args: Args) -> Result<Arg> {
        match name {
            "__exit__" => self.release().map(|()| Arg::Bool(false)),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'AtomicGuard' object has no attribute '{other}'"
            ))),
        }
    }
    fn __bool__(&self) -> bool {
        true
    }
}
impl IntoArg for DynAtomicGuard {
    fn into_arg(self) -> Arg {
        Arg::Object(alloc::sync::Arc::new(self))
    }
}

/// Build the context from bound `atomic(...)` keywords (shared by the mixins).
pub(crate) fn atomic_context<T: Clone>(
    obj: &T,
    scope: Arg,
    timeout_s: Arg,
    policy_id: Arg,
    class: &'static str,
    allows_global: bool,
) -> Result<AtomicContext<T>> {
    Ok(AtomicContext {
        obj: obj.clone(),
        scope: scope.__str__(),
        timeout_s: timeout_arg(&timeout_s)?,
        policy_id: match policy_id {
            Arg::None => None,
            p => Some(p.__str__()),
        },
        class,
        allows_global,
    })
}

// ---------------------------------------------------------------------------
// copy / pickle support
// ---------------------------------------------------------------------------
// Locks are process-local primitives: they cannot be pickled or deep-copied,
// and a copy must never share its lock with the original. `__deepcopy__` /
// `__getstate__` therefore skip every lock found on the instance
// (`_local_lock` in `__dict__`, `_lock` on the Atomic* types, ...) and the
// copy gets fresh ones of the same type.

/// `__deepcopy__`: copy `src`'s `__dict__` into `dst`'s -- plain entries
/// deep-copied, locks replaced by fresh ones of the same kind.
pub fn deepcopy_attrs(src: &PyAttrs, dst: &PyAttrs) -> Result<()> {
    let (plain, locks) = _split_locks(&src.__dict__());
    for (k, v) in plain {
        dst.__setattr__(&k, v.deepcopy())?;
    }
    for (k, kind) in locks {
        dst.__setattr__(&k, kind.new_lock())?;
    }
    Ok(())
}

/// `__getstate__`: `{"__dict__": {plain}, "__laila_dict_locks__": {key: kind}}`.
pub fn getstate_attrs(attrs: &PyAttrs) -> Arg {
    let (plain, locks) = _split_locks(&attrs.__dict__());
    let mut d = crate::python::PyDictBuilder::new();
    for (k, v) in plain {
        d.insert(k, v);
    }
    let mut l = crate::python::PyDictBuilder::new();
    for (k, kind) in locks {
        l.insert(k, kind.name());
    }
    let mut state = crate::python::PyDictBuilder::new();
    state.insert("__dict__", d.finish());
    state.insert("__laila_dict_locks__", l.finish());
    state.finish()
}

/// `__setstate__`: restore the plain entries and create fresh locks.
pub fn setstate_attrs(attrs: &PyAttrs, state: &Arg) -> Result<()> {
    let plain = state.get(Args::from("__dict__"))?;
    if !plain.is_none() {
        for k in plain.__iter__()? {
            attrs.__setattr__(&k.__str__(), plain.__getitem__(k.clone())?)?;
        }
    }
    let locks = state.get(Args::from("__laila_dict_locks__"))?;
    if !locks.is_none() {
        for k in locks.__iter__()? {
            let name = locks.__getitem__(k.clone())?.__str__();
            let kind = LockKind::from_name(&name)
                .ok_or_else(|| LailaError::key_error(crate::python::pyrepr::str_repr(&name)))?;
            attrs.__setattr__(&k.__str__(), kind.new_lock())?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The mixin
// ---------------------------------------------------------------------------

/// The provided methods every locally-atomic mixin trait exposes (`lock`,
/// `unlock`, `locked`, `_ensure_local_lock`, `creation_timestamp`, the copy /
/// pickle hooks). Rust has no multiple inheritance, so the three mixin
/// traits (local, local + identifiable, global) each expand this once instead
/// of inheriting from one another -- that keeps `obj.atomic()` unambiguous.
#[macro_export]
#[doc(hidden)]
macro_rules! local_atomic_methods {
    () => {
        /// Return the instance's `RLock`, creating it lazily (stored under
        /// `_local_lock` in the object's `__dict__`).
        fn _ensure_local_lock(&self) -> $crate::atomic::definitions::RLock {
            $crate::atomic::definitions::locally_atomic_object::ensure_local_lock(self.__attrs__())
        }

        /// `lock(timeout_s=None) -> bool`: acquire the per-instance reentrant
        /// lock; `False` if the timeout elapsed first.
        #[$crate::python::pysig("lock(self, timeout_s=None)")]
        fn lock(&self, args: impl Into<$crate::python::Args>) -> $crate::python::Result<bool> {
            Ok(
                $crate::atomic::definitions::locally_atomic_object::lock_attrs(
                    self.__attrs__(),
                    $crate::atomic::definitions::locally_atomic_object::timeout_arg(&timeout_s)?,
                ),
            )
        }

        /// `unlock()`: release the per-instance reentrant lock once (a thread
        /// that called `lock` *N* times must call `unlock` *N* times).
        fn unlock(&self) -> $crate::python::Result<()> {
            $crate::atomic::definitions::locally_atomic_object::unlock_attrs(self.__attrs__())
        }

        /// `locked() -> bool`: `True` if the lock is currently held by some thread.
        fn locked(&self, _args: impl Into<$crate::python::Args>) -> bool {
            self._ensure_local_lock().locked()
        }

        /// `creation_timestamp` (from `_LAILA_OBJECT`): the ISO-8601 UTC stamp
        /// recorded at construction, `None` when the object was built without one.
        fn creation_timestamp(&self) -> $crate::python::Arg {
            self.__attrs__()
                .get("_creation_timestamp")
                .unwrap_or($crate::python::Arg::None)
        }

        /// `copy.deepcopy(obj)`: a new instance whose `__dict__` is a deep
        /// copy with fresh locks of the same kinds.
        fn __deepcopy__(&self) -> $crate::python::Result<Self>
        where
            Self: Default + Sized,
        {
            let m = Self::default();
            $crate::atomic::definitions::locally_atomic_object::deepcopy_attrs(
                self.__attrs__(),
                m.__attrs__(),
            )?;
            Ok(m)
        }

        /// `pickle` support: the state without the locks (their kinds are
        /// recorded under `__laila_dict_locks__`).
        fn __getstate__(&self) -> $crate::python::Arg {
            $crate::atomic::definitions::locally_atomic_object::getstate_attrs(self.__attrs__())
        }

        /// `pickle` support: restore the state and create fresh locks.
        fn __setstate__(&self, state: impl $crate::python::IntoArg) -> $crate::python::Result<()> {
            $crate::atomic::definitions::locally_atomic_object::setstate_attrs(
                self.__attrs__(),
                &$crate::python::IntoArg::into_arg(state),
            )
        }
    };
}

/// Base class giving implementors thread-safe per-instance locking.
///
/// Derives from `_LAILA_OBJECT`, so every atomic object also carries a
/// creation `creation_timestamp`.
///
/// Implementors inherit a lazily-allocated [`RLock`] and three helpers:
///
/// - [`lock`](Self::lock) -- acquire the lock, optionally with a timeout.
/// - [`unlock`](Self::unlock) -- release the lock once.
/// - [`atomic`](Self::atomic) -- context manager wrapping `lock` and
///   `unlock` so a critical section reads naturally as
///   `with self.atomic(): ...`.
///
/// The lock is created on first access and stored under `_local_lock` in
/// the object's `__dict__` ([`HasAttrs`]), bypassing any attribute machinery.
#[allow(non_camel_case_types)]
pub trait _LAILA_LOCALLY_ATOMIC_OBJECT: HasAttrs {
    crate::local_atomic_methods!();

    /// `with obj.atomic(*, scope="local", timeout_s=None) as locked:` --
    /// hold the local lock for the duration of the block.
    ///
    /// This base class only knows about `"local"` and rejects anything else
    /// loudly (`ValueError`) so a typo cannot silently degrade cross-process
    /// safety; `TimeoutError` if the lock cannot be acquired within `timeout_s`.
    #[pysig("atomic(self, *, scope=\"local\", timeout_s=None)")]
    fn atomic(&self, args: impl Into<Args>) -> Result<AtomicContext<Self>>
    where
        Self: Clone + Sized,
    {
        atomic_context(
            self,
            scope,
            timeout_s,
            Arg::None,
            "_LAILA_LOCALLY_ATOMIC_OBJECT",
            false,
        )
    }
}

/// `id(obj)` for lock ordering: the address of the object's `__dict__`
/// (shared by every clone of the handle, as Python's `id` is per object).
pub fn attrs_id(attrs: &PyAttrs) -> usize {
    attrs as *const PyAttrs as usize
}

impl<T: HasAttrs> core::fmt::Debug for AtomicGuard<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "<locked {}>", self.obj.__class_name__())
    }
}

// ---------------------------------------------------------------------------
// Dynamic surface: `obj.atomic(..)` on an `Arg` yields these as objects
// ---------------------------------------------------------------------------

impl<T: HasAttrs + Clone + PyObject> PyObject for AtomicContext<T> {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "_GeneratorContextManager"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<contextlib._GeneratorContextManager object for {}.atomic>",
            self.class
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        core::ptr::eq(
            other.as_any() as *const dyn core::any::Any as *const (),
            self as *const Self as *const (),
        )
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(self as *const Self as usize as i64)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "__enter__" => Ok(self.__enter__()?.into_arg()),
            "__exit__" => {
                let _ = args;
                Ok(Arg::Bool(false))
            }
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'_GeneratorContextManager' object has no attribute '{name}'"
            ))),
        }
    }
}
impl<T: HasAttrs + Clone + PyObject> IntoArg for AtomicContext<T> {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

/// The locked instance as a dynamic value: every protocol call is forwarded
/// to the object (`locked.bump()`, `locked.value = 1`).
impl<T: HasAttrs + PyObject> PyObject for AtomicGuard<T> {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        self.obj.type_name()
    }
    fn __repr__(&self) -> String {
        self.obj.__repr__()
    }
    fn __str__(&self) -> String {
        self.obj.__str__()
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        self.obj.__eq__(other)
    }
    fn __eq_arg__(&self, other: &Arg) -> bool {
        self.obj.__eq_arg__(other)
    }
    fn __hash__(&self) -> Result<i64> {
        self.obj.__hash__()
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        self.obj.__getattr__(name)
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        self.obj.__setattr__(name, value)
    }
    fn __delattr__(&self, name: &str) -> Result<()> {
        self.obj.__delattr__(name)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        self.obj.__call_method__(name, args)
    }
    fn __len__(&self) -> Result<usize> {
        self.obj.__len__()
    }
    fn __bool__(&self) -> bool {
        self.obj.__bool__()
    }
    fn __contains__(&self, item: &Arg) -> Result<bool> {
        self.obj.__contains__(item)
    }
    fn __iter__(&self) -> Result<Vec<Arg>> {
        self.obj.__iter__()
    }
    fn __getitem__(&self, key: &Arg) -> Result<Arg> {
        self.obj.__getitem__(key)
    }
    fn __setitem__(&self, key: &Arg, value: Arg) -> Result<()> {
        self.obj.__setitem__(key, value)
    }
    fn __delitem__(&self, key: &Arg) -> Result<()> {
        self.obj.__delitem__(key)
    }
    fn __atomic_lock__(&self) -> Option<RLock> {
        self.obj.__atomic_lock__()
    }
}
impl<T: HasAttrs + PyObject> IntoArg for AtomicGuard<T> {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
