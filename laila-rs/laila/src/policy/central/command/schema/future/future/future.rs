//! Abstract `Future` base class -- identity, status, callbacks, lifecycle.
//!
//! A `Future` is the laila-side analogue of `concurrent.futures.Future` /
//! `asyncio.Future`, extended with identity (a stable `global_id` looked
//! up in the owning policy's `future_bank`), result-as-Entry (non-Entry
//! results are wrapped in `Entry.constant` lazily on first read), explicit
//! `release()` from the bank, and per-status callbacks.
//!
//! Python's class hierarchy -- `_LAILA_IDENTIFIABLE_FUTURE` <- `Future` <-
//! `ConcurrentPackageFuture` / `ComplexFuture`, and `GroupFuture` beside
//! them -- is one handle type here, [`Future`], tagged with its
//! [`FutureKind`]; `isinstance(x, ConcurrentPackageFuture)` is a tag test
//! and the kind-specific behaviour (`wait`, `__await__`, `release`,
//! `status`) dispatches on it. The class-named modules next to this one
//! hold each class's constructor and its own methods.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::task::Waker;

use crate::atomic::definitions::HasIdentity;
use crate::basics::definitions::identifiable_object::Identity;
use crate::macros::strings::{_COMPLEX_FUTURE_SCOPE, _FUTURE_SCOPE, _GROUP_FUTURE_SCOPE};
use crate::python::object::PyObject;
use crate::python::sync::{Mutex, RLock};
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, PyArg, Result};

pub use super::future_status::FutureStatus;

/// Which Python class a [`Future`] handle is an instance of.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FutureKind {
    /// `_LAILA_IDENTIFIABLE_FUTURE` -- identity only; resolves through the bank.
    Identity,
    /// `Future` -- the abstract base (constructible; `wait` raises).
    Base,
    /// `ConcurrentPackageFuture`.
    Concurrent,
    /// `ProcessPackageFuture` (a `ConcurrentPackageFuture` subclass; no
    /// process pool produces one in the Rust port, but the class exists).
    Process,
    /// `ComplexFuture`.
    Complex,
    /// `GroupFuture`.
    Group,
}

impl FutureKind {
    pub fn class_name(self) -> &'static str {
        match self {
            FutureKind::Identity => "_LAILA_IDENTIFIABLE_FUTURE",
            FutureKind::Base => "Future",
            FutureKind::Concurrent => "ConcurrentPackageFuture",
            FutureKind::Process => "ProcessPackageFuture",
            FutureKind::Complex => "ComplexFuture",
            FutureKind::Group => "GroupFuture",
        }
    }
    pub fn default_scopes(self) -> &'static [&'static str] {
        match self {
            FutureKind::Group => &[_GROUP_FUTURE_SCOPE],
            FutureKind::Complex => &[_COMPLEX_FUTURE_SCOPE],
            _ => &[_FUTURE_SCOPE],
        }
    }
    /// `issubclass(self, Future)`.
    pub fn is_future(self) -> bool {
        matches!(
            self,
            FutureKind::Base | FutureKind::Concurrent | FutureKind::Process | FutureKind::Complex
        )
    }
}

pub(crate) struct FutureFields {
    pub taskforce_id: Arg,
    pub policy_id: Arg,
    pub future_group_id: Arg,
    pub precedence: Arg,
    pub purpose: Arg,
    /// The public `callbacks` field: the *last* callback per status.
    pub callbacks: Arg,
    /// `GroupFuture.future_ids` (a list).
    pub future_ids: Arg,
    /// `ComplexFuture.stage_future_ids` (a list).
    pub stage_future_ids: Arg,
}

pub(crate) struct FutureState {
    pub status: FutureStatus,
    pub return_value: Arg,
    pub exception: Option<Arc<LailaError>>,
    pub result_global_id: Option<String>,
    pub result_pending_wrap: bool,
    pub timeout_ms: i64,
    /// `_status_callbacks`: per-status registry, in insertion order.
    pub status_callbacks: Vec<(FutureStatus, Arg)>,
    /// `ConcurrentPackageFuture._native_future`.
    #[cfg(feature = "std")]
    pub native: Option<crate::python::stdlib::NativeFuture>,
    /// Tasks awaiting a terminal status.
    pub wakers: Vec<Waker>,
}

pub struct FutureInner {
    pub(crate) kind: FutureKind,
    pub(crate) identity: Mutex<Identity>,
    /// `self.atomic()` -- the re-entrant lock of the locally atomic mixin.
    pub(crate) lock: RLock,
    pub(crate) fields: Mutex<FutureFields>,
    pub(crate) state: Mutex<FutureState>,
    /// `ComplexFuture` privates.
    pub(crate) complex: Mutex<super::complex_future::ComplexState>,
}

/// A laila future (handle; clones alias one future).
#[derive(Clone)]
pub struct Future(pub(crate) Arc<FutureInner>);

const TERMINAL: [FutureStatus; 3] = [
    FutureStatus::FINISHED,
    FutureStatus::ERROR,
    FutureStatus::CANCELLED,
];

/// `_DEFAULT_STATUS_CALLBACKS`: the shared "set my status to X" table
/// (one dict object serves every instance).
pub fn _DEFAULT_STATUS_CALLBACKS() -> Arg {
    static TABLE: crate::python::sync::RwLock<Option<Arg>> = crate::python::sync::RwLock::new(None);
    if let Some(t) = TABLE.read().as_ref() {
        return t.clone();
    }
    let mut d = crate::python::PyDictBuilder::new();
    for st in FutureStatus::MEMBERS {
        d.insert(
            st,
            Arg::Callable(Arc::new(move |a: Args| -> Result<Arg> {
                let f = a.positional.first().cloned().unwrap_or(Arg::None);
                match f {
                    Arg::Future(f) => f.set_status(st),
                    other => {
                        return Err(LailaError::type_error(alloc::format!(
                            "_set_status_{}() expects a Future, got {}",
                            st.name().to_lowercase(),
                            other.type_name()
                        )))
                    }
                }
                Ok(Arg::None)
            })),
        );
    }
    let t = d.finish();
    let mut g = TABLE.write();
    if g.is_none() {
        *g = Some(t);
    }
    g.as_ref().cloned().expect("table")
}

fn gid_of(x: &Arg) -> Arg {
    match x {
        Arg::None => Arg::None,
        other => match other.__getattr__("global_id") {
            Ok(g) => g,
            Err(_) => other.clone(),
        },
    }
}

impl Future {
    // ---------------------------------------------------------------
    // construction

    /// Build a handle of *kind* from the Python constructor arguments
    /// (`taskforce_id=`, `policy_id=`, `future_group_id=`, `precedence=`,
    /// `purpose=`, `uuid=`, `scopes=`, `evolution=`, `nickname=`, plus
    /// `future_ids=` for groups) and run `model_post_init`: register in
    /// every open guarantee scope on this thread and in the owning local
    /// policy's `future_bank`.
    /// `Future(taskforce_id=.., policy_id=.., ..)`: the abstract base class
    /// itself (constructible; `wait` raises `NotImplementedError`).
    pub fn new(args: impl Into<Args>) -> Result<Future> {
        Self::construct(FutureKind::Base, args.into())
    }
    /// `fut.identity()`: `{"uuid": .., "scopes": .., "evolution": ..}`.
    pub fn identity(&self) -> Arg {
        self.0.identity.lock().identity()
    }
    pub(crate) fn construct(kind: FutureKind, args: Args) -> Result<Future> {
        let mut rest = args;
        let taskforce_id = rest.pop_or_none("taskforce_id");
        let policy_id = rest.pop_or_none("policy_id");
        let future_group_id = rest.pop_or_none("future_group_id");
        let precedence = rest.pop_or_none("precedence");
        let purpose = rest.pop_or_none("purpose");
        let callbacks = rest.pop_or_none("callbacks");
        let future_ids = rest.pop_or_none("future_ids");
        let stage_future_ids = rest.pop_or_none("stage_future_ids");
        let stage_fns = rest.pop_or_none("stage_fns");
        let complex = super::complex_future::ComplexState::new(kind, stage_fns)?;
        for required in ["taskforce_id", "policy_id"] {
            let v = if required == "taskforce_id" {
                &taskforce_id
            } else {
                &policy_id
            };
            if v.is_none() {
                return Err(crate::_compat::pydantic::validation_error(
                    kind.class_name(),
                    required,
                    "Field required",
                    "missing",
                    &Arg::None,
                ));
            }
        }
        let mut id = Args::new();
        id.push_keyword(
            "default_scopes",
            kind.default_scopes()
                .iter()
                .map(|s| Arg::Str((*s).into()))
                .collect::<Vec<_>>(),
        );
        for key in ["uuid", "nickname", "scopes", "evolution", "global_id"] {
            let v = rest.pop_or_none(key);
            if !v.is_none() {
                id.push_keyword(key, v);
            }
        }
        if let Some(k) = rest.keywords.first() {
            return Err(LailaError::type_error(alloc::format!(
                "{}() got an unexpected keyword argument '{}'",
                kind.class_name(),
                k.0
            )));
        }
        let identity = Identity::from_args(id)?;
        let list_or_empty = |a: Arg| -> Result<Arg> {
            Ok(match a {
                Arg::None => Vec::<Arg>::new().into_arg(),
                Arg::List(l) => Arg::List(l),
                other => other.__iter__()?.into_arg(),
            })
        };
        let f = Future(Arc::new(FutureInner {
            kind,
            identity: Mutex::new(identity),
            lock: RLock::new(),
            fields: Mutex::new(FutureFields {
                taskforce_id,
                policy_id,
                future_group_id,
                precedence,
                purpose,
                callbacks: if callbacks.is_none() {
                    crate::python::PyDictBuilder::new().finish()
                } else {
                    callbacks
                },
                future_ids: list_or_empty(future_ids)?,
                stage_future_ids: list_or_empty(stage_future_ids)?,
            }),
            state: Mutex::new(FutureState {
                status: FutureStatus::NOT_STARTED,
                return_value: Arg::None,
                exception: None,
                result_global_id: None,
                result_pending_wrap: false,
                timeout_ms: 100,
                status_callbacks: Vec::new(),
                #[cfg(feature = "std")]
                native: None,
                wakers: Vec::new(),
            }),
            complex: Mutex::new(complex),
        }));
        f.model_post_init()?;
        Ok(f)
    }

    /// `model_post_init`: register with every open guarantee scope on this
    /// thread and with the owning local policy's `future_bank` (identity
    /// handles are not registered -- they only *resolve* through the bank).
    fn model_post_init(&self) -> Result<()> {
        if self.kind() == FutureKind::Identity {
            return Ok(());
        }
        let policy = crate::policy::_owning_local_policy(&self.policy_id())?;
        policy
            .central()
            .command()
            ._register_future_with_active_guarantees(self);
        policy
            .future_bank()
            .__setitem__(self.global_id().as_str(), Arg::Future(self.clone()))?;
        if self.kind() == FutureKind::Group {
            crate::logger::hook_group_future_created(Arg::Future(self.clone()));
        } else {
            crate::logger::hook_future_created(Arg::Future(self.clone()));
        }
        if self.kind() == FutureKind::Complex {
            super::complex_future::kick_off_first_stage(self);
        }
        Ok(())
    }

    /// An already-settled `ConcurrentPackageFuture` on the active policy's
    /// alpha taskforce: `Ok` is the result, `Err` the stored exception
    /// (raised by `.wait()` / `.result`), as a task that finished.
    pub fn from_result(r: Result<Arg>) -> Future {
        let f = Future::pending();
        match r {
            Ok(v) => {
                f.set_result(v);
                f.set_status(FutureStatus::FINISHED);
            }
            Err(e) => {
                f.set_exception(Some(Arc::new(e)));
                f.set_status(FutureStatus::ERROR);
            }
        }
        f
    }
    /// A settled `ConcurrentPackageFuture` holding *result*.
    pub fn done(result: impl IntoArg) -> Future {
        Future::from_result(Ok(result.into_arg()))
    }
    /// A `NOT_STARTED` `ConcurrentPackageFuture` on the active policy's
    /// alpha taskforce (registered in its bank).
    pub fn pending() -> Future {
        let policy = crate::policy::get_active_policy();
        let mut a = Args::new();
        a.push_keyword("taskforce_id", policy.central().command().alpha_taskforce());
        a.push_keyword("policy_id", policy.global_id());
        Future::construct(FutureKind::Concurrent, a).expect("future")
    }

    // ---------------------------------------------------------------
    // identity

    pub fn kind(&self) -> FutureKind {
        self.0.kind
    }
    /// Python `is`.
    pub fn is(&self, other: &Future) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub fn global_id(&self) -> String {
        self.0.identity.lock().global_id()
    }
    pub fn uuid(&self) -> String {
        self.0.identity.lock().uuid.clone()
    }
    pub fn scopes(&self) -> Arg {
        self.0.identity.lock().scopes_arg()
    }
    pub fn evolution(&self) -> Arg {
        self.0.identity.lock().evolution_arg()
    }
    pub fn taskforce_id(&self) -> Arg {
        self.0.fields.lock().taskforce_id.clone()
    }
    pub fn policy_id(&self) -> Arg {
        self.0.fields.lock().policy_id.clone()
    }
    pub fn future_group_id(&self) -> Arg {
        self.0.fields.lock().future_group_id.clone()
    }
    pub fn precedence(&self) -> Arg {
        self.0.fields.lock().precedence.clone()
    }
    pub fn purpose(&self) -> Arg {
        self.0.fields.lock().purpose.clone()
    }
    /// `self.atomic()` -- `with fut.atomic():`.
    pub fn atomic(&self) -> crate::python::stdlib::RLock {
        crate::python::stdlib::RLock(self.0.lock.clone())
    }
    /// The class name (`type(fut).__name__`).
    pub fn class_name(&self) -> &'static str {
        self.0.kind.class_name()
    }

    // ---------------------------------------------------------------
    // status

    /// `fut.status` (`@synchronized`); a `GroupFuture` reports its
    /// percentage breakdown instead (see `group_future`).
    pub fn status(&self) -> Arg {
        if self.kind() == FutureKind::Group {
            return self.group_status();
        }
        if self.kind() == FutureKind::Identity {
            return self
                .resolve_in_bank()
                .map(|f| f.status())
                .unwrap_or(Arg::None);
        }
        self._status().into_arg()
    }
    /// The raw `_status` of a concrete future.
    pub fn _status(&self) -> FutureStatus {
        self.0.state.lock().status
    }
    /// `fut.status = status` (`@synchronized`): after updating the state,
    /// fire every callback registered for *status*. Callback exceptions
    /// are swallowed.
    pub fn set_status(&self, status: FutureStatus) {
        let _g = self.0.lock.acquire(None);
        let (prev, cbs, wakers) = {
            let mut s = self.0.state.lock();
            let prev = s.status;
            s.status = status;
            let cbs: Vec<Arg> = if prev != status {
                s.status_callbacks
                    .iter()
                    .filter(|(st, _)| *st == status)
                    .map(|(_, cb)| cb.clone())
                    .collect()
            } else {
                Vec::new()
            };
            let wakers = if status.is_terminal() {
                core::mem::take(&mut s.wakers)
            } else {
                Vec::new()
            };
            (prev, cbs, wakers)
        };
        if prev != status {
            crate::logger::hook_future_transition(
                Arg::Future(self.clone()),
                status.into_arg(),
                prev.into_arg(),
            );
            for cb in cbs {
                let _ = cb.__call__(Args::from(Arg::Future(self.clone())));
            }
        }
        let _ = self.0.lock.release();
        wakers.into_iter().for_each(Waker::wake);
    }
    /// `fut.add_status_callback(status, fn)`: fires when the future
    /// transitions into *status*; at once if it already is in it.
    pub fn add_status_callback(&self, status: impl IntoArg, fn_: impl IntoArg) -> Result<()> {
        let status = status_arg(&status.into_arg())?;
        let fn_ = fn_.into_arg();
        let fire_now = {
            let mut s = self.0.state.lock();
            s.status_callbacks.push((status, fn_.clone()));
            s.status == status
        };
        if fire_now {
            let _ = fn_.__call__(Args::from(Arg::Future(self.clone())));
        }
        Ok(())
    }
    /// `fut.add_callback(status, fn)`: `callbacks[status] = fn` plus
    /// `add_status_callback`.
    pub fn add_callback(&self, status: impl IntoArg, fn_: impl IntoArg) -> Result<()> {
        let status = status.into_arg();
        let fn_ = fn_.into_arg();
        let st = status_arg(&status)?;
        self.0
            .fields
            .lock()
            .callbacks
            .__setitem__(st, fn_.clone())?;
        self.add_status_callback(st, fn_)
    }
    /// `fut.remove_callback(status, fn)`.
    pub fn remove_callback(&self, status: impl IntoArg, fn_: impl IntoArg) -> Result<()> {
        let st = status_arg(&status.into_arg())?;
        let fn_ = fn_.into_arg();
        self.0
            .state
            .lock()
            .status_callbacks
            .retain(|(s, cb)| !(*s == st && cb.is(&fn_)));
        let fields = self.0.fields.lock();
        if fields.callbacks.get(st)?.is(&fn_) {
            fields.callbacks.pop(two(st, Arg::None))?;
        }
        Ok(())
    }
    /// `fut.clear_callbacks(status)`.
    pub fn clear_callbacks(&self, status: impl IntoArg) -> Result<()> {
        let st = status_arg(&status.into_arg())?;
        self.0
            .state
            .lock()
            .status_callbacks
            .retain(|(s, _)| *s != st);
        self.0.fields.lock().callbacks.pop(two(st, Arg::None))?;
        Ok(())
    }
    /// `fut.clear_all_callbacks()`.
    pub fn clear_all_callbacks(&self) -> Result<()> {
        self.0.state.lock().status_callbacks.clear();
        self.0.fields.lock().callbacks.clear(Args::new())?;
        Ok(())
    }
    /// `fut.trigger_callback(status)`: fire every callback for *status*.
    pub fn trigger_callback(&self, status: impl IntoArg) -> Result<()> {
        let st = status_arg(&status.into_arg())?;
        let cbs: Vec<Arg> = self
            .0
            .state
            .lock()
            .status_callbacks
            .iter()
            .filter(|(s, _)| *s == st)
            .map(|(_, cb)| cb.clone())
            .collect();
        for cb in cbs {
            let _ = cb.__call__(Args::from(Arg::Future(self.clone())));
        }
        Ok(())
    }
    /// `fut._status_callbacks` as `{status: [fn, ...]}` (a snapshot).
    pub fn _status_callbacks(&self) -> Arg {
        let s = self.0.state.lock();
        let mut d = crate::python::PyDictBuilder::new();
        for st in FutureStatus::MEMBERS {
            let v: Vec<Arg> = s
                .status_callbacks
                .iter()
                .filter(|(x, _)| *x == st)
                .map(|(_, cb)| cb.clone())
                .collect();
            if !v.is_empty() {
                d.insert(st, v);
            }
        }
        d.finish()
    }
    /// `fut._default_callbacks` -- the shared table.
    pub fn _default_callbacks(&self) -> Arg {
        _DEFAULT_STATUS_CALLBACKS()
    }
    /// `fut.callbacks` -- the single-slot public view.
    pub fn callbacks(&self) -> Arg {
        self.0.fields.lock().callbacks.clone()
    }

    // ---------------------------------------------------------------
    // result / exception

    /// `fut.result = value`: `None` clears; a live `Entry` is stored as-is
    /// (its gid captured); anything else is stored raw and wrapped by
    /// `Entry.constant` on first read.
    pub fn set_result(&self, result: Arg) {
        let _g = self.0.lock.acquire(None);
        {
            let mut s = self.0.state.lock();
            match &result {
                Arg::None => {
                    s.return_value = Arg::None;
                    s.result_global_id = None;
                    s.result_pending_wrap = false;
                }
                Arg::Entry(e) => {
                    s.result_global_id = Some(e.global_id());
                    s.return_value = result.clone();
                    s.result_pending_wrap = false;
                }
                _ => {
                    s.return_value = result.clone();
                    s.result_global_id = None;
                    s.result_pending_wrap = true;
                }
            }
        }
        let _ = self.0.lock.release();
    }
    /// `fut.exception = exc`.
    pub fn set_exception(&self, exception: Option<Arc<LailaError>>) {
        let _g = self.0.lock.acquire(None);
        self.0.state.lock().exception = exception;
        let _ = self.0.lock.release();
    }
    /// `fut.exception` -- the stored exception (`None` when none). A
    /// `GroupFuture` reports the first failed child's.
    pub fn exception(&self) -> Arg {
        match self.kind() {
            FutureKind::Group => self.group_exception(),
            FutureKind::Identity => self
                .resolve_in_bank()
                .map(|f| f.exception())
                .unwrap_or(Arg::None),
            _ => match &self.0.state.lock().exception {
                Some(e) => Arg::Exception(e.clone()),
                None => Arg::None,
            },
        }
    }
    #[allow(dead_code)]
    pub(crate) fn exception_err(&self) -> Option<LailaError> {
        self.0.state.lock().exception.as_deref().cloned()
    }
    /// `_materialize_result`: the result as an `Entry`, wrapping a raw
    /// value on first use.
    pub fn _materialize_result(&self) -> Result<Arg> {
        let _g = self.0.lock.acquire(None);
        let out = (|| {
            let pending = self.0.state.lock().result_pending_wrap;
            if pending {
                let raw = self.0.state.lock().return_value.clone();
                let mut a = Args::new();
                a.push_keyword("data", raw);
                let wrapped = crate::entry::Entry::constant(a)?;
                let mut s = self.0.state.lock();
                s.result_global_id = Some(wrapped.global_id());
                s.return_value = Arg::Entry(wrapped);
                s.result_pending_wrap = false;
            }
            Ok(self.0.state.lock().return_value.clone())
        })();
        let _ = self.0.lock.release();
        out
    }
    /// `fut.result`: blocks until completion; re-raises on error /
    /// cancellation. A `GroupFuture` collects its children's results.
    pub fn result(&self) -> Result<Arg> {
        match self.kind() {
            FutureKind::Group => return self.group_result(),
            FutureKind::Identity => return self.resolve_in_bank()?.result(),
            _ => {}
        }
        {
            let s = self.0.state.lock();
            match s.status {
                FutureStatus::ERROR | FutureStatus::CANCELLED => {
                    return Err(stored_exception(&s));
                }
                FutureStatus::FINISHED => {
                    drop(s);
                    return self._materialize_result();
                }
                _ => {}
            }
        }
        self.wait(Args::new())?;
        let s = self.0.state.lock();
        if matches!(s.status, FutureStatus::ERROR | FutureStatus::CANCELLED) {
            return Err(stored_exception(&s));
        }
        drop(s);
        self._materialize_result()
    }
    /// `fut.result_global_id`: the result entry's gid (forces the wrap).
    pub fn result_global_id(&self) -> Result<Arg> {
        let _g = self.0.lock.acquire(None);
        let out = (|| {
            if self.0.state.lock().result_pending_wrap {
                self._materialize_result()?;
            }
            Ok(match &self.0.state.lock().result_global_id {
                Some(g) => Arg::Str(g.as_str().into()),
                None => Arg::None,
            })
        })();
        let _ = self.0.lock.release();
        out
    }
    /// `fut.data` -- `fut.result.data`.
    pub fn data(&self) -> Result<Arg> {
        match self.kind() {
            FutureKind::Group => return self.group_data(),
            FutureKind::Identity => return self.identity_data(),
            _ => {}
        }
        let result = self.result()?;
        match result {
            Arg::Entry(e) => e.data(),
            other => Err(LailaError::runtime_error(alloc::format!(
                "Future result is not an Entry (got {}); cannot access .data",
                other.type_name()
            ))),
        }
    }
    pub fn _return_value(&self) -> Arg {
        self.0.state.lock().return_value.clone()
    }
    pub fn _result_pending_wrap(&self) -> bool {
        self.0.state.lock().result_pending_wrap
    }
    pub fn _result_global_id(&self) -> Arg {
        match &self.0.state.lock().result_global_id {
            Some(g) => Arg::Str(g.as_str().into()),
            None => Arg::None,
        }
    }
    pub fn _exception(&self) -> Arg {
        match &self.0.state.lock().exception {
            Some(e) => Arg::Exception(e.clone()),
            None => Arg::None,
        }
    }

    // ---------------------------------------------------------------
    // predicates

    pub fn finished(&self) -> bool {
        self._status() == FutureStatus::FINISHED
    }
    pub fn cancelled(&self) -> bool {
        self._status() == FutureStatus::CANCELLED
    }
    pub fn error(&self) -> bool {
        self._status() == FutureStatus::ERROR
    }
    pub fn not_started(&self) -> bool {
        self._status() == FutureStatus::NOT_STARTED
    }
    pub fn running(&self) -> bool {
        self._status() == FutureStatus::RUNNING
    }
    /// `_is_terminal`: the native future is done, or the status is terminal.
    pub fn _is_terminal(&self) -> bool {
        let s = self.0.state.lock();
        #[cfg(feature = "std")]
        if let Some(n) = &s.native {
            return n.done();
        }
        TERMINAL.contains(&s.status)
    }

    // ---------------------------------------------------------------
    // identity handle / bank

    /// `fut.future_identity` -- a lightweight identity handle.
    pub fn future_identity(&self) -> Future {
        let f = self.0.fields.lock();
        let identity = self.0.identity.lock().clone();
        Future(Arc::new(FutureInner {
            kind: FutureKind::Identity,
            identity: Mutex::new(identity),
            lock: RLock::new(),
            fields: Mutex::new(FutureFields {
                taskforce_id: f.taskforce_id.clone(),
                policy_id: f.policy_id.clone(),
                future_group_id: f.future_group_id.clone(),
                precedence: f.precedence.clone(),
                purpose: f.purpose.clone(),
                callbacks: crate::python::PyDictBuilder::new().finish(),
                future_ids: Vec::<Arg>::new().into_arg(),
                stage_future_ids: Vec::<Arg>::new().into_arg(),
            }),
            state: Mutex::new(FutureState {
                status: FutureStatus::NOT_STARTED,
                return_value: Arg::None,
                exception: None,
                result_global_id: None,
                result_pending_wrap: false,
                timeout_ms: 100,
                status_callbacks: Vec::new(),
                #[cfg(feature = "std")]
                native: None,
                wakers: Vec::new(),
            }),
            complex: Mutex::new(super::complex_future::ComplexState::empty()),
        }))
    }
    /// The concrete future this gid names in some local policy's bank.
    pub(crate) fn resolve_in_bank(&self) -> Result<Future> {
        let gid = self.global_id();
        for policy in crate::policy::_future_bank_policies() {
            let bank = policy.future_bank();
            if bank.__contains__(gid.as_str())? {
                if let Arg::Future(f) = bank.__getitem__(gid.as_str())? {
                    return Ok(f);
                }
            }
        }
        Err(LailaError::key_error(crate::python::pyrepr::str_repr(
            &alloc::format!("Future {gid} not found in any local policy bank"),
        )))
    }
    fn identity_data(&self) -> Result<Arg> {
        let pid = gid_of(&self.policy_id());
        let policy = match pid.as_str() {
            Some(p) => crate::policy::_local_policy_or_live(p),
            None => None,
        };
        let Some(policy) = policy else {
            return Err(LailaError::key_error(crate::python::pyrepr::str_repr(
                &alloc::format!(
                    "Owning policy {} for future {} not found locally",
                    pid.__str__(),
                    self.global_id()
                ),
            )));
        };
        let bank = policy.future_bank();
        let gid = self.global_id();
        match bank.get(gid.as_str())? {
            Arg::Future(f) => f.data(),
            _ => Err(LailaError::key_error(crate::python::pyrepr::str_repr(
                &alloc::format!(
                    "Future {gid} not found in policy {}'s future bank",
                    pid.__str__()
                ),
            ))),
        }
    }
    /// `fut.release()`: remove this future from its owning policy's
    /// `future_bank` (idempotent, legal in any state). A group releases
    /// its children too; a complex future its stages; an identity handle
    /// delegates to the concrete future.
    #[pysig("release(*, children=True)")]
    pub fn release(&self, args: impl Into<Args>) -> Result<()> {
        match self.kind() {
            FutureKind::Group => return self.group_release(children.__bool__()),
            FutureKind::Identity => return self.identity_release(),
            FutureKind::Complex => {
                for stage in self.stage_futures()? {
                    stage.release(Args::new())?;
                }
            }
            _ => {}
        }
        self.release_self()
    }
    pub(crate) fn release_self(&self) -> Result<()> {
        let gid = self.global_id();
        let pid = gid_of(&self.policy_id());
        if let Some(p) = pid.as_str() {
            if let Some(policy) = crate::policy::_local_policy_or_live(p) {
                let bank = policy.future_bank();
                if let Arg::Future(f) = bank.get(gid.as_str())? {
                    if f.is(self) {
                        bank.pop(two(gid.as_str(), Arg::None))?;
                        return Ok(());
                    }
                }
            }
        }
        for policy in crate::policy::_future_bank_policies() {
            let bank = policy.future_bank();
            if let Arg::Future(f) = bank.get(gid.as_str())? {
                if f.is(self) {
                    bank.pop(two(gid.as_str(), Arg::None))?;
                    return Ok(());
                }
            }
        }
        Ok(())
    }
    fn identity_release(&self) -> Result<()> {
        let gid = self.global_id();
        for policy in crate::policy::_future_bank_policies() {
            let bank = policy.future_bank();
            let fut = bank.get(gid.as_str())?;
            if let Arg::Future(f) = fut {
                if f.is(self) {
                    bank.pop(two(gid.as_str(), Arg::None))?;
                } else {
                    f.release(Args::new())?;
                }
                return Ok(());
            }
        }
        Ok(())
    }

    // ---------------------------------------------------------------
    // waiting

    /// `fut.wait(timeout=None)`: block until the future completes.
    #[pysig("wait(timeout=None)")]
    pub fn wait(&self, args: impl Into<Args>) -> Result<Arg> {
        let timeout = timeout.as_f64();
        match self.kind() {
            FutureKind::Identity => {
                super::super::super::exceptions::_check_not_loop_thread()?;
                let mut a = Args::new();
                a.push_keyword("timeout", timeout);
                self.resolve_in_bank()?.wait(a)
            }
            FutureKind::Base => Err(LailaError::not_implemented(
                "Future does not implement wait(); use a concrete subclass such as ConcurrentPackageFuture.",
            )),
            FutureKind::Group => self.group_wait(timeout),
            FutureKind::Complex => self.complex_wait(timeout),
            FutureKind::Concurrent | FutureKind::Process => self.concurrent_wait(timeout),
        }
    }
    /// `await fut` -- the kind's non-blocking completion signal, parking
    /// the current taskforce slot (if any) while pending.
    pub fn __await__(&self) -> crate::python::stdlib::asyncio::BoxFut {
        let me = self.clone();
        match self.kind() {
            FutureKind::Identity => {
                alloc::boxed::Box::pin(async move { me.resolve_in_bank()?.__await__().await })
            }
            FutureKind::Base => alloc::boxed::Box::pin(async move {
                Err(LailaError::not_implemented(alloc::format!(
                    "{} does not implement __await__()",
                    me.class_name()
                )))
            }),
            FutureKind::Group => alloc::boxed::Box::pin(me.group_await()),
            FutureKind::Complex => alloc::boxed::Box::pin(me.complex_await()),
            FutureKind::Concurrent | FutureKind::Process => {
                alloc::boxed::Box::pin(me.concurrent_await())
            }
        }
    }
    /// Register *waker* to be woken on the next terminal transition.
    pub(crate) fn register_waker(&self, waker: &Waker) {
        let mut s = self.0.state.lock();
        if !s.wakers.iter().any(|w| w.will_wake(waker)) {
            s.wakers.push(waker.clone());
        }
    }
    /// Snapshot `(status, exception)` under the atomic lock.
    pub(crate) fn snapshot(&self) -> (FutureStatus, Option<Arc<LailaError>>) {
        let s = self.0.state.lock();
        (s.status, s.exception.clone())
    }
    /// The value or the raise for a terminal *status*.
    pub(crate) fn _outcome(
        &self,
        status: FutureStatus,
        exc: Option<Arc<LailaError>>,
    ) -> Result<Arg> {
        if status == FutureStatus::FINISHED {
            return self._materialize_result();
        }
        if let Some(e) = exc {
            return Err((*e).clone());
        }
        Err(LailaError::runtime_error(alloc::format!(
            "Future ended with status={} and no exception.",
            status.__str__()
        )))
    }

    // ---------------------------------------------------------------
    // dict / json

    /// `as_dict()` -- the identity fields.
    pub fn as_dict(&self) -> Arg {
        let f = self.0.fields.lock();
        let mut d = crate::python::PyDictBuilder::new();
        d.insert("global_id", self.global_id().as_str());
        d.insert("taskforce_id", gid_of(&f.taskforce_id));
        d.insert("policy_id", gid_of(&f.policy_id));
        d.insert("future_group_id", gid_of(&f.future_group_id));
        d.insert("precedence", gid_of(&f.precedence));
        d.insert("purpose", f.purpose.clone());
        d.finish()
    }
    /// `__json__()` -- the identity fields as JSON.
    pub fn __json__(&self) -> Result<String> {
        crate::python::stdlib::json.dumps(self.as_dict())
    }
    /// `str(fut)`: the identity lines; a `GroupFuture` dumps `what`.
    pub fn __str__(&self) -> String {
        if self.kind() == FutureKind::Group {
            return self.group_str();
        }
        let f = self.0.fields.lock();
        alloc::format!(
            "future_id: {}\ntaskforce_id: {}\npolicy_id: {}\nfuture_group_id: {}\nprecedence: {}\npurpose: {}",
            self.global_id(),
            f.taskforce_id.__str__(),
            f.policy_id.__str__(),
            f.future_group_id.__str__(),
            f.precedence.__str__(),
            f.purpose.__str__()
        )
    }
    pub fn __repr__(&self) -> String {
        self.__str__()
    }
    pub fn __hash__(&self) -> i64 {
        Arc::as_ptr(&self.0) as usize as i64
    }

    // ---------------------------------------------------------------
    // dynamic layer

    /// `fut.<name>` for the dynamic layer.
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "global_id" => Arg::Str(self.global_id().into()),
            "uuid" | "_uuid" => Arg::Str(self.uuid().into()),
            "scopes" | "_scopes" => self.scopes(),
            "evolution" | "_evolution" => self.evolution(),
            "taskforce_id" => self.taskforce_id(),
            "policy_id" => self.policy_id(),
            "future_group_id" => self.future_group_id(),
            "precedence" => self.precedence(),
            "purpose" => self.purpose(),
            // An identity resolves through the bank: a released future is
            // a `KeyError`, like Python's property.
            "status" if self.kind() == FutureKind::Identity => self.resolve_in_bank()?.status(),
            "status" => self.status(),
            "_status" => self._status().into_arg(),
            "result" => self.result()?,
            "data" => self.data()?,
            "exception" => self.exception(),
            "_exception" => self._exception(),
            "result_global_id" => self.result_global_id()?,
            "_result_global_id" => self._result_global_id(),
            "_return_value" => self._return_value(),
            "_result_pending_wrap" => Arg::Bool(self._result_pending_wrap()),
            "_timeout_ms" => Arg::Int(self.0.state.lock().timeout_ms),
            "callbacks" => self.callbacks(),
            "_status_callbacks" => self._status_callbacks(),
            "_default_callbacks" => self._default_callbacks(),
            "future_identity" => Arg::Future(self.future_identity()),
            "future_ids" => self.future_ids(),
            "what" => self.what()?,
            "stage_future_ids" => self.stage_future_ids(),
            "current_stage_index" => Arg::Int(self.current_stage_index()),
            "num_stages" => Arg::Int(self.num_stages()),
            "native_future" => self.native_future(),
            "_native_future" => self.native_future(),
            "__class__" => self.class_object().into_arg(),
            // `fut.wait` read as a value: a bound method
            other if FUTURE_METHODS.contains(&other) => {
                let this = self.clone();
                let n = String::from(other);
                crate::python::callable(move |a: Args| this.__call_method__(&n, a))
            }
            other => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'{}' object has no attribute '{other}'",
                    self.class_name()
                )))
            }
        })
    }
    /// `fut.<name> = value` for the dynamic layer / Tier 2.
    pub fn __setattr__(&self, name: &str, value: impl IntoArg) -> Result<()> {
        let value = value.into_arg();
        match name {
            "status" => {
                self.set_status(status_arg(&value)?);
                Ok(())
            }
            "result" => {
                self.set_result(value);
                Ok(())
            }
            "exception" => {
                let e = match value {
                    Arg::None => None,
                    Arg::Exception(e) => Some(e),
                    other => {
                        return Err(LailaError::type_error(alloc::format!(
                            "exception must be an exception or None, got {}",
                            other.type_name()
                        )))
                    }
                };
                self.set_exception(e);
                Ok(())
            }
            "taskforce_id" => {
                self.0.fields.lock().taskforce_id = value;
                Ok(())
            }
            "policy_id" => {
                self.0.fields.lock().policy_id = value;
                Ok(())
            }
            "future_group_id" => {
                self.0.fields.lock().future_group_id = value;
                Ok(())
            }
            "precedence" => {
                self.0.fields.lock().precedence = value;
                Ok(())
            }
            "purpose" => {
                self.0.fields.lock().purpose = value;
                Ok(())
            }
            "native_future" => self.set_native_future(value),
            // `fut._return_value = x`: the raw private slot (Tier 2 tests
            // poke it to simulate a missing result).
            "_return_value" => {
                let mut s = self.0.state.lock();
                s.return_value = value;
                s.result_pending_wrap = false;
                s.result_global_id = None;
                Ok(())
            }
            "_timeout_ms" => {
                self.0.state.lock().timeout_ms = value.as_i64().unwrap_or(100);
                Ok(())
            }
            other => Err(LailaError::attribute_error(alloc::format!(
                "'{}' object has no attribute '{other}'",
                self.class_name()
            ))),
        }
    }
    /// `fut.<name>(*args, **kwargs)` for the dynamic layer.
    pub fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        debug_assert!(FUTURE_METHODS.contains(&"wait"));
        let first = args.positional.first().cloned().unwrap_or(Arg::None);
        let second = args.positional.get(1).cloned().unwrap_or(Arg::None);
        Ok(match name {
            "wait" => self.wait(args)?,
            "release" => {
                self.release(args)?;
                Arg::None
            }
            "finished" => Arg::Bool(self.finished()),
            "cancelled" => Arg::Bool(self.cancelled()),
            "error" => Arg::Bool(self.error()),
            "not_started" => Arg::Bool(self.not_started()),
            "running" => Arg::Bool(self.running()),
            "add_status_callback" => {
                self.add_status_callback(first, second)?;
                Arg::None
            }
            "add_callback" => {
                self.add_callback(first, second)?;
                Arg::None
            }
            "remove_callback" => {
                self.remove_callback(first, second)?;
                Arg::None
            }
            "clear_callbacks" => {
                self.clear_callbacks(first)?;
                Arg::None
            }
            "clear_all_callbacks" => {
                self.clear_all_callbacks()?;
                Arg::None
            }
            "trigger_callback" => {
                self.trigger_callback(first)?;
                Arg::None
            }
            "as_dict" => self.as_dict(),
            "__json__" => Arg::Str(self.__json__()?.into()),
            "atomic" => self.atomic().into_arg(),
            "_materialize_result" => self._materialize_result()?,
            "_is_terminal" => Arg::Bool(self._is_terminal()),
            "append" => {
                self.append(first)?;
                Arg::None
            }
            "__add__" => self.__add__(first)?,
            "stage_futures" => self
                .stage_futures()?
                .into_iter()
                .map(Arg::Future)
                .collect::<Vec<_>>()
                .into_arg(),
            "__await__" => crate::python::stdlib::asyncio::coroutine(self.__await__()),
            // A property read spelled as a zero-argument method on the
            // dynamic layer (`fut.future_identity`, `fut.what`).
            other if args.positional.is_empty() && args.keywords.is_empty() => {
                self.__getattr__(other)?
            }
            other => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'{}' object has no attribute '{other}'",
                    self.class_name()
                )))
            }
        })
    }
    /// `type(fut)` as a class value.
    pub fn class_object(&self) -> crate::python::PyClass {
        crate::python::PyClass::named(
            self.class_name(),
            "laila.policy.central.command.schema.future.future",
        )
    }
}

fn two(a: impl IntoArg, b: impl IntoArg) -> Args {
    let mut x = Args::from(a.into_arg());
    x.push_positional(b.into_arg());
    x
}

trait PopOrNone {
    fn pop_or_none(&mut self, name: &str) -> Arg;
}
impl PopOrNone for Args {
    fn pop_or_none(&mut self, name: &str) -> Arg {
        self.pop_or(name, Arg::None)
    }
}

fn stored_exception(s: &FutureState) -> LailaError {
    match &s.exception {
        Some(e) => (**e).clone(),
        None => LailaError::type_error("exceptions must derive from BaseException"),
    }
}

/// A `FutureStatus` from a Python value (`TypeError` otherwise).
pub(crate) fn status_arg(x: &Arg) -> Result<FutureStatus> {
    FutureStatus::from_arg(x).ok_or_else(|| {
        LailaError::type_error(alloc::format!(
            "status must be a FutureStatus, got {}",
            x.type_name()
        ))
    })
}

/// The handle's terminal-status signal as a raw future (no parking):
/// `fut.settled().await` resolves on the terminal status (the `__await__`
/// implementations build on it). `fut.await` / `await fut` go through
/// [`Future::__await__`], which parks the current taskforce slot while
/// pending.
pub struct Settled(pub Future);
impl core::future::Future for Settled {
    type Output = Result<Arg>;
    fn poll(
        self: core::pin::Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Self::Output> {
        let (status, exc) = self.0.snapshot();
        if status.is_terminal() {
            return core::task::Poll::Ready(self.0._outcome(status, exc));
        }
        self.0.register_waker(cx.waker());
        core::task::Poll::Pending
    }
}
impl Future {
    /// The terminal-status signal without parking (see [`Settled`]).
    pub fn settled(&self) -> Settled {
        Settled(self.clone())
    }
}
/// `fut.await` on the typed handle (Tier 1) and `await fut` in
/// `laila::python!` both go through `__await__`: the kind's completion
/// signal, parking the current taskforce slot while pending.
impl core::future::IntoFuture for Future {
    type Output = Result<Arg>;
    type IntoFuture = crate::python::stdlib::asyncio::BoxFut;
    fn into_future(self) -> Self::IntoFuture {
        self.__await__()
    }
}
impl core::future::IntoFuture for &Future {
    type Output = Result<Arg>;
    type IntoFuture = crate::python::stdlib::asyncio::BoxFut;
    fn into_future(self) -> Self::IntoFuture {
        self.__await__()
    }
}

impl core::fmt::Debug for Future {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "{}({}, status={})",
            self.class_name(),
            self.global_id(),
            self.status().__str__()
        )
    }
}
impl PyArg for Future {
    fn to_arg(&self) -> Arg {
        Arg::Future(self.clone())
    }
}
impl HasIdentity for Future {
    fn global_id(&self) -> String {
        Future::global_id(self)
    }
    fn uuid(&self) -> String {
        Future::uuid(self)
    }
}

/// `laila.policy.central.command.schema.future.future.Future` as a class
/// marker: `isinstance(x, Future)`, `Future(taskforce_id=.., policy_id=..)`.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct FutureClass;

impl FutureClass {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<Future> {
        Future::construct(FutureKind::Base, args.into())
    }
    pub fn class_object() -> crate::python::PyClass {
        crate::python::PyClass::with_ctor(
            "Future",
            "laila.policy.central.command.schema.future.future.future",
            Arc::new(|a: Args| Future::construct(FutureKind::Base, a).map(Arg::Future)),
        )
    }
}
impl crate::python::builtins::IsInstance for FutureClass {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Future(f) if f.kind().is_future())
    }
}
impl crate::python::builtins::IsInstance for Future {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Future(f) if f.kind().is_future())
    }
}
impl IntoArg for FutureClass {
    fn into_arg(self) -> Arg {
        Self::class_object().into_arg()
    }
}
impl PyArg for FutureClass {
    fn to_arg(&self) -> Arg {
        Self::class_object().into_arg()
    }
}
impl crate::python::builtins::CallValue for FutureClass {
    fn call_value(&self, args: Args) -> Result<Arg> {
        Future::construct(FutureKind::Base, args).map(Arg::Future)
    }
}
impl PyObject for FutureClass {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "type"
    }
    fn __repr__(&self) -> String {
        "<class 'laila.policy.central.command.schema.future.future.future.Future'>".into()
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other.as_any().is::<FutureClass>()
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(0x4655)
    }
}

/// `len(gf)`: a `GroupFuture`'s child count; `TypeError` for other futures.
impl crate::python::builtins::PyLen for Future {
    fn __len__(&self) -> Result<usize> {
        if self.kind() == FutureKind::Group {
            return Ok(Future::__len__(self));
        }
        Err(LailaError::type_error(alloc::format!(
            "object of type '{}' has no len()",
            self.class_name()
        )))
    }
}
/// `g1 + g2`: `GroupFuture.__add__` (merge in place; `NotImplemented`
/// otherwise, which Python turns into `TypeError`).
impl core::ops::Add for Future {
    type Output = Result<Arg>;
    fn add(self, other: Future) -> Result<Arg> {
        let out = self.__add__(other.clone())?;
        if crate::python::builtins::is_not_implemented(&out) {
            return Err(LailaError::type_error(alloc::format!(
                "unsupported operand type(s) for +: '{}' and '{}'",
                self.class_name(),
                other.class_name()
            )));
        }
        Ok(out)
    }
}

/// The methods `Future::__call_method__` answers (a read of one is a
/// bound method).
const FUTURE_METHODS: &[&str] = &[
    "wait",
    "release",
    "finished",
    "cancelled",
    "error",
    "not_started",
    "running",
    "add_status_callback",
    "add_callback",
    "remove_callback",
    "clear_callbacks",
    "clear_all_callbacks",
    "trigger_callback",
    "as_dict",
    "__json__",
    "atomic",
    "_materialize_result",
    "_is_terminal",
    "append",
    "__add__",
    "stage_futures",
    "__await__",
];
