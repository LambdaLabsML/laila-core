//! `ComplexFuture` -- sequential composition of futures (a declarative
//! pipeline).
//!
//! A pipeline of stages executed one at a time in declared order. The
//! number of stages and the function that produces each stage's future
//! (given the prior stage's result) is fixed at construction; the stage
//! futures themselves are built lazily -- stage `k` is constructed when
//! stage `k-1` flips to `FINISHED`. A stage function may return another
//! `ComplexFuture`, a `GroupFuture` (fan-out whose aggregated result feeds
//! the next stage) or a future identity. If any stage ends `ERROR` /
//! `CANCELLED` the parent propagates that status (and exception) and no
//! further stages are constructed; a stage function that raises or returns
//! a non-future transitions the parent to `ERROR`.
//!
//! Leaf stages are chained through `Future.add_status_callback`; a
//! `GroupFuture` stage (whose status is a percentage breakdown) is polled
//! on a daemon thread. A re-entrant `_stage_lock` keeps the state machine
//! consistent under concurrent callback delivery from worker threads.

use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::python::sync::{Mutex, RLock};
use crate::python::{Arg, Args, IntoArg, LailaError, Result};

use super::future::{Future, FutureKind};
use super::future_status::FutureStatus;

future_class_marker!(
    ComplexFuture,
    "ComplexFuture",
    "laila.policy.central.command.schema.future.future.complex_future",
    FutureKind::Complex,
    |k| k == FutureKind::Complex
);

/// `StageFn = Callable[[Any], Future | GroupFuture | _LAILA_IDENTIFIABLE_FUTURE]`
/// -- a stage function value (any callable `Arg`).
pub type StageFn = Arg;

/// The private attributes of a `ComplexFuture` (`_stage_fns`,
/// `_stage_lock`, `_current_idx`, `_terminated`).
pub struct ComplexState {
    pub stage_fns: Vec<StageFn>,
    pub lock: RLock,
    pub current_idx: Mutex<i64>,
    pub terminated: Mutex<bool>,
}

impl ComplexState {
    /// `ComplexFuture.__init__`'s validation of `stage_fns` (only a
    /// complex future has any; the other kinds carry an empty state).
    pub(crate) fn new(kind: FutureKind, stage_fns: Arg) -> Result<ComplexState> {
        let fns: Vec<Arg> = match &stage_fns {
            Arg::None => Vec::new(),
            Arg::List(_) | Arg::Tuple(_) => stage_fns.__iter__()?,
            _ => {
                return Err(LailaError::type_error(
                    "stage_fns must be a list or tuple of callables",
                ))
            }
        };
        if kind == FutureKind::Complex {
            if fns.is_empty() {
                return Err(LailaError::value_error(
                    "ComplexFuture requires at least one stage",
                ));
            }
            for (idx, f) in fns.iter().enumerate() {
                if !crate::python::builtins::is_callable(f) {
                    return Err(LailaError::type_error(alloc::format!(
                        "stage_fns[{idx}] must be callable, got {}",
                        f.type_name()
                    )));
                }
            }
        }
        Ok(ComplexState {
            stage_fns: fns,
            lock: RLock::new(),
            current_idx: Mutex::new(-1),
            terminated: Mutex::new(false),
        })
    }
    pub(crate) fn empty() -> ComplexState {
        ComplexState {
            stage_fns: Vec::new(),
            lock: RLock::new(),
            current_idx: Mutex::new(-1),
            terminated: Mutex::new(false),
        }
    }
}

/// `model_post_init`'s tail: construct stage 0 with `prior_result=None`.
pub(crate) fn kick_off_first_stage(f: &Future) {
    f._kick_off_stage(0, Arg::None);
}

/// A one-shot callback value for `add_status_callback`.
fn stage_callback(f: impl Fn(&Future) + Send + Sync + 'static) -> Arg {
    Arg::Callable(Arc::new(move |a: Args| -> Result<Arg> {
        if let Some(Arg::Future(fut)) = a.positional.first() {
            f(fut);
        }
        Ok(Arg::None)
    }))
}

impl Future {
    /// `cf.current_stage_index`: 0-based index of the most recently
    /// constructed stage (`-1` before the first).
    pub fn current_stage_index(&self) -> i64 {
        *self.0.complex.lock().current_idx.lock()
    }
    /// `cf.num_stages`.
    pub fn num_stages(&self) -> i64 {
        self.0.complex.lock().stage_fns.len() as i64
    }
    /// `cf.stage_future_ids`.
    pub fn stage_future_ids(&self) -> Arg {
        self.0.fields.lock().stage_future_ids.clone()
    }
    /// `cf.stage_futures()`: the stage futures constructed so far, looked
    /// up through the local policies' banks.
    pub fn stage_futures(&self) -> Result<Vec<Future>> {
        let mut out = Vec::new();
        for fid in self.stage_future_ids().__iter__()? {
            let fid = fid.__str__();
            for policy in crate::policy::_future_bank_policies() {
                let bank = policy.future_bank();
                if bank.__contains__(fid.as_str())? {
                    if let Arg::Future(f) = bank.__getitem__(fid.as_str())? {
                        out.push(f);
                    }
                    break;
                }
            }
        }
        Ok(out)
    }
    /// `_resolve_future(value)`: the `Future` / `GroupFuture` behind a
    /// stage function's return value (an identity resolves through the
    /// banks); `TypeError` for anything else.
    fn _resolve_stage_value(&self, value: Arg) -> Result<Future> {
        match value {
            Arg::Future(f) if f.kind() == FutureKind::Identity => f.resolve_in_bank(),
            Arg::Future(f) => Ok(f),
            other => Err(LailaError::type_error(alloc::format!(
                "stage_fn must return a Future / GroupFuture / future_identity, got {}",
                other.type_name()
            ))),
        }
    }
    fn complex_lock(&self) -> RLock {
        self.0.complex.lock().lock.clone()
    }
    fn is_terminated(&self) -> bool {
        *self.0.complex.lock().terminated.lock()
    }
    /// `_kick_off_stage(idx, prior_result=...)`: construct stage *idx* and
    /// register the chaining callbacks on it.
    pub(crate) fn _kick_off_stage(&self, idx: usize, prior_result: Arg) {
        let lock = self.complex_lock();
        lock.acquire(None);
        if self.is_terminated() {
            let _ = lock.release();
            return;
        }
        let fn_ = self.0.complex.lock().stage_fns.get(idx).cloned();
        let produced = match fn_ {
            Some(f) => f
                .__call__(Args::from(prior_result))
                .and_then(|v| self._resolve_stage_value(v)),
            None => Err(LailaError::new(
                crate::python::ExcKind::IndexError,
                "list index out of range",
            )),
        };
        let stage_fut = match produced {
            Ok(f) => f,
            Err(e) => {
                self._fail(Some(Arc::new(e)), FutureStatus::ERROR);
                let _ = lock.release();
                return;
            }
        };
        let _ = self
            .0
            .fields
            .lock()
            .stage_future_ids
            .append(Arg::Str(stage_fut.global_id().as_str().into()));
        *self.0.complex.lock().current_idx.lock() = idx as i64;

        if stage_fut.kind() == FutureKind::Group {
            self._watch_group_future(idx, stage_fut);
        } else {
            let me = self.clone();
            let _ = stage_fut.add_status_callback(
                FutureStatus::FINISHED,
                stage_callback(move |f| me._on_stage_done(idx, f.result())),
            );
            let me = self.clone();
            let _ = stage_fut.add_status_callback(
                FutureStatus::ERROR,
                stage_callback(move |f| me._on_stage_failed(idx, f)),
            );
            let me = self.clone();
            let _ = stage_fut.add_status_callback(
                FutureStatus::CANCELLED,
                stage_callback(move |f| me._on_stage_failed(idx, f)),
            );
        }
        if self._status() == FutureStatus::NOT_STARTED {
            self.set_status(FutureStatus::RUNNING);
        }
        let _ = lock.release();
    }
    /// Drive a `GroupFuture` stage from a daemon-thread poll loop: once
    /// every child is terminal, forward the aggregated outcome exactly as
    /// a leaf's callbacks would.
    fn _watch_group_future(&self, idx: usize, gf: Future) {
        let me = self.clone();
        #[cfg(feature = "std")]
        {
            let _ = std::thread::Builder::new()
                .name(alloc::format!("ComplexFuture-GF-watch-{idx}"))
                .spawn(move || {
                    while gf.group_terminal_pct() < 100.0 {
                        std::thread::sleep(core::time::Duration::from_millis(10));
                    }
                    match gf.group_result() {
                        Ok(results) => me._on_stage_done(idx, Ok(results)),
                        Err(exc) => me._fail_stage(idx, Some(Arc::new(exc)), FutureStatus::ERROR),
                    }
                });
        }
        #[cfg(not(feature = "std"))]
        {
            // No threads: resolve the group synchronously.
            match gf.group_result() {
                Ok(results) => me._on_stage_done(idx, Ok(results)),
                Err(exc) => me._fail_stage(idx, Some(Arc::new(exc)), FutureStatus::ERROR),
            }
        }
    }
    /// A stage finished: kick off the next one or finalize the pipeline.
    fn _on_stage_done(&self, idx: usize, value: Result<Arg>) {
        let lock = self.complex_lock();
        lock.acquire(None);
        if self.is_terminated() || self.current_stage_index() != idx as i64 {
            let _ = lock.release();
            return;
        }
        let value = match value {
            Ok(v) => v,
            Err(e) => {
                self._fail(Some(Arc::new(e)), FutureStatus::ERROR);
                let _ = lock.release();
                return;
            }
        };
        let nxt = idx + 1;
        if nxt >= self.0.complex.lock().stage_fns.len() {
            *self.0.complex.lock().terminated.lock() = true;
            self.set_exception(None);
            self.set_result(value);
            self.set_status(FutureStatus::FINISHED);
            let _ = lock.release();
            return;
        }
        let _ = lock.release();
        self._kick_off_stage(nxt, value);
    }
    /// A stage ended `ERROR` / `CANCELLED`: propagate to the parent.
    fn _on_stage_failed(&self, idx: usize, fut: &Future) {
        let (status, exc) = fut.snapshot();
        self._fail_stage(idx, exc, status);
    }
    fn _fail_stage(&self, idx: usize, exc: Option<Arc<LailaError>>, status: FutureStatus) {
        let lock = self.complex_lock();
        lock.acquire(None);
        if !(self.is_terminated() || self.current_stage_index() != idx as i64) {
            self._fail(exc, status);
        }
        let _ = lock.release();
    }
    /// `_fail(exc, status)`: mark the parent failed / cancelled and seal
    /// further progress.
    fn _fail(&self, exc: Option<Arc<LailaError>>, status: FutureStatus) {
        *self.0.complex.lock().terminated.lock() = true;
        self.set_exception(exc);
        self.set_result(Arg::None);
        self.set_status(status);
    }
    /// `cf.wait(timeout)`: poll the parent status until terminal.
    pub(crate) fn complex_wait(&self, timeout: Option<f64>) -> Result<Arg> {
        super::super::super::exceptions::_check_not_loop_thread()?;
        let me = self.clone();
        super::super::super::parking::park_sync(move || me._complex_wait_impl(timeout))
    }
    fn _complex_wait_impl(&self, timeout: Option<f64>) -> Result<Arg> {
        #[cfg(feature = "std")]
        let deadline = timeout
            .map(|t| std::time::Instant::now() + core::time::Duration::from_secs_f64(t.max(0.0)));
        #[cfg_attr(not(feature = "std"), allow(clippy::never_loop))]
        loop {
            let (status, exc) = self.snapshot();
            if status == FutureStatus::FINISHED {
                return self._materialize_result();
            }
            if matches!(status, FutureStatus::ERROR | FutureStatus::CANCELLED) {
                return Err(match exc {
                    Some(e) => (*e).clone(),
                    None => LailaError::runtime_error(alloc::format!(
                        "ComplexFuture ended with status={} and no exception.",
                        status.__str__()
                    )),
                });
            }
            #[cfg(feature = "std")]
            {
                if deadline.is_some_and(|d| std::time::Instant::now() >= d) {
                    self.set_status(FutureStatus::POLL_TIMEOUT);
                    return Err(LailaError::new(
                        crate::python::ExcKind::TimeoutError,
                        alloc::string::String::new(),
                    ));
                }
                std::thread::sleep(core::time::Duration::from_millis(10));
            }
            #[cfg(not(feature = "std"))]
            {
                let _ = timeout;
                return Err(LailaError::new(
                    crate::python::ExcKind::TimeoutError,
                    alloc::string::String::new(),
                ));
            }
        }
    }
    /// `await cf`: yield to the loop until the pipeline is terminal,
    /// parking the slot meanwhile.
    pub(crate) async fn complex_await(self) -> Result<Arg> {
        let me = self.clone();
        super::super::super::parking::park_async(async move {
            loop {
                let (status, exc) = me.snapshot();
                if status == FutureStatus::FINISHED {
                    return me._materialize_result();
                }
                if matches!(status, FutureStatus::ERROR | FutureStatus::CANCELLED) {
                    return Err(match exc {
                        Some(e) => (*e).clone(),
                        None => LailaError::runtime_error(alloc::format!(
                            "ComplexFuture ended with status={} and no exception.",
                            status.__str__()
                        )),
                    });
                }
                let _ = crate::python::stdlib::asyncio::asyncio.sleep(0.01).await;
            }
        })
        .await
    }
}

impl ComplexFuture {
    /// `ComplexFuture(stage_fns=[...], taskforce_id=.., policy_id=..)` with
    /// Rust closures as stages.
    pub fn from_stages(
        stages: Vec<Arg>,
        taskforce_id: impl IntoArg,
        policy_id: impl IntoArg,
    ) -> Result<Future> {
        let mut a = Args::new();
        a.push_keyword("stage_fns", stages);
        a.push_keyword("taskforce_id", taskforce_id);
        a.push_keyword("policy_id", policy_id);
        Future::construct(FutureKind::Complex, a)
    }
}
