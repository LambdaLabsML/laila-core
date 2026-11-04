//! Async-thread-pool taskforce -- N owned threads, each running its own
//! asyncio loop.
//!
//! Each loop hosts up to `max_async_per_thread` concurrent in-flight tasks
//! (*slots*). Submitted callables are coroutine functions (zero-arg);
//! plain sync callables are wrapped (at the `Command.submit` boundary and
//! again, idempotently, in `_queue_submit`) into shims that offload the
//! body to this taskforce's sync executor (see `ensure_coroutine_function`)
//! so a sync body never blocks a loop thread.
//!
//! The dispatcher routes each task to the loop with the lowest in-flight
//! count below the cap; when every loop is at the cap it blocks until one
//! frees up. A task that awaits a laila future while holding a slot
//! *parks* (see `schema::parking`): the slot is released for the duration
//! and re-acquired afterwards, parked tasks being resumed with priority
//! over fresh roots -- nested submissions are deadlock-free regardless of
//! `num_workers` and `max_async_per_thread`.
//!
//! Sync bodies run on their own thread (the executor's thread count is
//! effectively unbounded) while the number of bodies *executing* at any
//! time is bounded by `sync_workers` compute permits; a body that blocks
//! on a laila future releases both its slot and its permit.
//!
//! `shutdown(wait=True)` is a graceful stop: every task already queued is
//! still dispatched and run to completion before the threads are joined.
//! `cancel_pending=True` drops queued tasks (their futures end
//! `CANCELLED`) and cancels in-flight coroutines. Calling
//! `shutdown(wait=True)` from a task running on the taskforce itself
//! cannot drain and falls back to cancelling queued tasks with a warning.
//!
//! Without the `std` feature (single-core bare metal) there are no
//! threads: a submitted task runs to completion inline, at submission.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::python::stdlib::asyncio::{Coroutine, OneShot};

/// The body of `run_sync`: a
/// boxed closure, or a Python callable value (`t.run_sync(lambda: 1)`)
/// called with no arguments.
pub trait IntoSyncBody {
    fn into_sync_body(self) -> alloc::boxed::Box<dyn FnOnce() -> Result<Arg> + Send>;
}
impl IntoSyncBody for alloc::boxed::Box<dyn FnOnce() -> Result<Arg> + Send> {
    fn into_sync_body(self) -> alloc::boxed::Box<dyn FnOnce() -> Result<Arg> + Send> {
        self
    }
}
impl IntoSyncBody for Arg {
    fn into_sync_body(self) -> alloc::boxed::Box<dyn FnOnce() -> Result<Arg> + Send> {
        alloc::boxed::Box::new(move || self.__call__(Args::new()))
    }
}
impl IntoSyncBody for &Arg {
    fn into_sync_body(self) -> alloc::boxed::Box<dyn FnOnce() -> Result<Arg> + Send> {
        self.clone().into_sync_body()
    }
}
use crate::python::sync::Mutex;
use crate::python::{Arg, Args, IntoArg, LailaError, Result};

use crate::policy::central::command::schema::exceptions::ensure_coroutine_function;
use crate::policy::central::command::schema::future::future::future::{Future, FutureKind};
use crate::policy::central::command::schema::future::future::future_status::FutureStatus;
#[cfg(feature = "std")]
use crate::policy::central::command::schema::parking::_CURRENT_PERMIT;
use crate::policy::central::command::schema::parking::{
    _Permit, _SlotCtx, ResumeCallback, SlotLoop, SlotTaskForce, _CURRENT_SLOT, _RESOLVE_CHAIN,
};

use super::super::base::{Queued, TaskForce};
use super::super::status::TaskForceStatus;

/// Python's `PythonAsyncThreadPoolTaskForce` class: the one backend.
pub type PythonAsyncThreadPoolTaskForce = TaskForce;

// ---------------------------------------------------------------------------
// _LoopThread

/// A single owned thread running its own asyncio event loop.
///
/// `_resume_waiters` holds callbacks of parked tasks waiting to get a
/// slot back; `release_slot` pops the oldest waiter *instead of*
/// decrementing the in-flight count, so the slot is transferred directly
/// to that parked task and the dispatcher never sees it as free.
pub struct LoopThread {
    pub name: String,
    pub cap: usize,
    pub r#loop: crate::python::stdlib::EventLoop,
    inflight: Mutex<usize>,
    resume_waiters: Mutex<alloc::collections::VecDeque<ResumeCallback>>,
    thread_ident: Mutex<Option<i64>>,
    #[cfg(feature = "std")]
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl LoopThread {
    #[cfg(feature = "std")]
    pub fn new(name: String, cap: usize) -> Arc<LoopThread> {
        let lt = Arc::new(LoopThread {
            name: name.clone(),
            cap,
            r#loop: crate::python::stdlib::EventLoop::new(),
            inflight: Mutex::new(0),
            resume_waiters: Mutex::new(alloc::collections::VecDeque::new()),
            thread_ident: Mutex::new(None),
            thread: Mutex::new(None),
        });
        let ready = crate::python::stdlib::Event::new(Args::new());
        let ready2 = ready.clone();
        let me = lt.clone();
        let handle = crate::python::stdlib::spawn_native_thread(name, move || me._run(ready2))
            .expect("loop thread");
        *lt.thread.lock() = Some(handle);
        let _ = ready.wait(Args::new());
        lt
    }
    #[cfg(feature = "std")]
    fn _run(&self, ready: crate::python::stdlib::Event) {
        let ident = crate::policy::central::command::schema::exceptions::current_thread_ident();
        *self.thread_ident.lock() = Some(ident);
        crate::policy::central::command::schema::exceptions::_register_async_loop_thread(ident);
        self.r#loop
            .call_soon_threadsafe(move || ready.set(Args::new()));
        self.r#loop.run_forever();
        // Cancel whatever is still pending (their guards run the
        // ``finally`` bookkeeping) and close the loop.
        self.r#loop.close();
        crate::policy::central::command::schema::exceptions::_unregister_async_loop_thread(ident);
    }
    pub fn thread_ident(&self) -> Option<i64> {
        *self.thread_ident.lock()
    }
    pub fn inflight_count(&self) -> usize {
        *self.inflight.lock()
    }
    /// Parked tasks currently waiting to re-acquire a slot.
    pub fn parked_waiting(&self) -> usize {
        self.resume_waiters.lock().len()
    }
    /// Claim a slot for a new root task if under the cap.
    pub fn try_reserve(&self) -> bool {
        let mut g = self.inflight.lock();
        if *g >= self.cap {
            return false;
        }
        *g += 1;
        true
    }
    pub fn submit_coro<F: core::future::Future<Output = ()> + Send + 'static>(&self, coro: F) {
        self.r#loop.spawn(coro);
    }
    /// Ask the loop to stop without joining; with `drain=true` only once
    /// every task scheduled on it has completed.
    pub fn request_stop(&self, drain: bool) {
        if drain {
            let lp = self.r#loop.clone();
            self.r#loop.spawn(async move {
                loop {
                    // ``all_tasks`` counts the drainer itself.
                    if lp.all_tasks() <= 1 {
                        break;
                    }
                    let _ = crate::python::stdlib::asyncio::asyncio.sleep(0.01).await;
                }
                lp.stop();
            });
        } else {
            self.r#loop.stop();
        }
    }
    /// Join the loop thread (no-op from that very thread).
    #[cfg(feature = "std")]
    pub fn join(&self, timeout: Option<f64>) {
        if self.thread_ident()
            == Some(crate::policy::central::command::schema::exceptions::current_thread_ident())
        {
            return;
        }
        let handle = self.thread.lock().take();
        if let Some(h) = handle {
            match timeout {
                None => {
                    let _ = h.join();
                }
                Some(t) => {
                    let deadline =
                        std::time::Instant::now() + core::time::Duration::from_secs_f64(t.max(0.0));
                    while !h.is_finished() && std::time::Instant::now() < deadline {
                        std::thread::sleep(core::time::Duration::from_millis(5));
                    }
                    if h.is_finished() {
                        let _ = h.join();
                    } else {
                        *self.thread.lock() = Some(h);
                    }
                }
            }
        }
    }
    /// `request_stop` followed by `join`.
    #[cfg(feature = "std")]
    pub fn stop(&self, timeout: Option<f64>, drain: bool) {
        self.request_stop(drain);
        self.join(timeout);
    }
}

impl SlotLoop for LoopThread {
    fn release_slot(&self) -> bool {
        let cb = {
            let mut waiters = self.resume_waiters.lock();
            match waiters.pop_front() {
                Some(cb) => Some(cb),
                None => {
                    let mut g = self.inflight.lock();
                    if *g > 0 {
                        *g -= 1;
                    }
                    None
                }
            }
        };
        match cb {
            Some(cb) => {
                cb();
                false
            }
            None => true,
        }
    }
    fn try_reserve_or_wait(&self, cb: ResumeCallback) -> bool {
        let mut g = self.inflight.lock();
        if *g < self.cap {
            *g += 1;
            return true;
        }
        self.resume_waiters.lock().push_back(cb);
        false
    }
    fn cancel_wait(&self, cb: &ResumeCallback) -> bool {
        let mut waiters = self.resume_waiters.lock();
        match waiters.iter().position(|w| Arc::ptr_eq(w, cb)) {
            Some(i) => {
                waiters.remove(i);
                true
            }
            None => false,
        }
    }
}

impl core::fmt::Debug for LoopThread {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "<_LoopThread {} inflight={}>",
            self.name,
            self.inflight_count()
        )
    }
}
impl crate::python::object::PyObject for LoopThread {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "_LoopThread"
    }
    fn __repr__(&self) -> String {
        alloc::format!("{self:?}")
    }
    fn __eq__(&self, other: &dyn crate::python::object::PyObject) -> bool {
        core::ptr::eq(
            other.as_any() as *const dyn core::any::Any as *const (),
            self as *const _ as *const (),
        )
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(self as *const _ as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "name" => Ok(Arg::Str(self.name.as_str().into())),
            "cap" => Ok(Arg::Int(self.cap as i64)),
            "_inflight" => Ok(Arg::Int(self.inflight_count() as i64)),
            "_thread_ident" => Ok(self.thread_ident().map(Arg::Int).unwrap_or(Arg::None)),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'_LoopThread' object has no attribute '{other}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, _args: Args) -> Result<Arg> {
        match name {
            "inflight_count" => Ok(Arg::Int(self.inflight_count() as i64)),
            "parked_waiting" => Ok(Arg::Int(self.parked_waiting() as i64)),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'_LoopThread' object has no attribute '{other}'"
            ))),
        }
    }
}
impl IntoArg for Arc<LoopThread> {
    fn into_arg(self) -> Arg {
        Arg::Object(self)
    }
}

// ---------------------------------------------------------------------------
// Runtime (the private attributes of a started taskforce)

/// `tf._dispatcher`: the dispatcher thread (`threading.Thread`) as a value.
#[cfg(feature = "std")]
pub struct DispatcherHandle {
    pub name: String,
}

#[cfg(feature = "std")]
impl crate::python::PyObject for DispatcherHandle {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "Thread"
    }
    fn __repr__(&self) -> String {
        alloc::format!("<Thread({}, started daemon)>", self.name)
    }
    fn __eq__(&self, other: &dyn crate::python::PyObject) -> bool {
        core::ptr::eq(
            other.as_any() as *const dyn core::any::Any as *const (),
            self as *const _ as *const (),
        )
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(self as *const _ as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "name" => Ok(Arg::Str(self.name.as_str().into())),
            "daemon" => Ok(Arg::Bool(true)),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'Thread' object has no attribute '{other}'"
            ))),
        }
    }
}

/// The backend state allocated by `_on_start`.
pub struct Runtime {
    #[cfg(feature = "std")]
    pub cv: crate::python::stdlib::Condition,
    #[cfg(feature = "std")]
    pub capacity_cv: crate::python::stdlib::Condition,
    #[cfg(feature = "std")]
    pub stop: crate::python::stdlib::Event,
    #[cfg(feature = "std")]
    pub dispatcher: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// `tf._dispatcher`: the dispatcher thread as a value (one object for
    /// the runtime's lifetime, so `start()` twice reads the same handle).
    #[cfg(feature = "std")]
    pub dispatcher_obj: Arc<DispatcherHandle>,
    pub dispatching: core::sync::atomic::AtomicBool,
    pub loops: Vec<Arc<LoopThread>>,
    pub compute_permits: Arc<crate::python::stdlib::Semaphore>,
    /// Sync bodies currently running on executor threads (joined by
    /// `shutdown(wait=True)`).
    pub sync_inflight: Mutex<usize>,
    #[cfg(feature = "std")]
    pub sync_idle: parking_lot::Condvar,
    /// The sync executor (`_executor`), swapped for a fresh one by
    /// `_maybe_recycle_executor` once the taskforce has been idle.
    #[cfg(feature = "std")]
    pub executor: Mutex<Arc<SyncExecutor>>,
    /// Bodies submitted to the current executor (`_executor_used`).
    #[cfg(feature = "std")]
    pub executor_used: Mutex<usize>,
    #[cfg(feature = "std")]
    pub executor_generation: Mutex<usize>,
    /// `_idle_since`: when the taskforce last became completely idle.
    #[cfg(feature = "std")]
    pub idle_since: Mutex<Option<f64>>,
}

/// Seconds of complete idleness (nothing queued, in flight or parked)
/// after which the sync executor is swapped for a fresh one so the
/// threads it spawned can exit.
pub const _EXECUTOR_IDLE_RECYCLE_S: f64 = 1.0;

/// The `ThreadPoolExecutor` of `run_sync`: a pool of worker threads fed
/// from one queue. A body is handed to an idle worker, or a new worker is
/// spawned (the pool is effectively unbounded, like Python's
/// `max_workers=1_000_000`); workers never exit on their own -- `shutdown`
/// (the executor recycle) wakes the idle ones so they can.
#[cfg(feature = "std")]
pub struct SyncExecutor {
    name: String,
    state: parking_lot::Mutex<ExecutorState>,
    cv: parking_lot::Condvar,
}
#[cfg(feature = "std")]
struct ExecutorState {
    queue: alloc::collections::VecDeque<alloc::boxed::Box<dyn FnOnce() + Send>>,
    idle: usize,
    workers: usize,
    shutdown: bool,
}
#[cfg(feature = "std")]
impl SyncExecutor {
    pub fn new(name: String) -> Arc<SyncExecutor> {
        Arc::new(SyncExecutor {
            name,
            state: parking_lot::Mutex::new(ExecutorState {
                queue: alloc::collections::VecDeque::new(),
                idle: 0,
                workers: 0,
                shutdown: false,
            }),
            cv: parking_lot::Condvar::new(),
        })
    }
    /// Live worker threads of this executor.
    pub fn workers(&self) -> usize {
        self.state.lock().workers
    }
    /// `executor.submit(job)`: an idle worker takes it, else a new one.
    pub fn submit(self: &Arc<Self>, job: alloc::boxed::Box<dyn FnOnce() + Send>) -> Result<()> {
        let mut g = self.state.lock();
        if g.shutdown {
            return Err(LailaError::runtime_error(
                "cannot schedule new futures after shutdown",
            ));
        }
        g.queue.push_back(job);
        if g.idle > 0 {
            self.cv.notify_one();
            return Ok(());
        }
        let n = g.workers;
        g.workers += 1;
        drop(g);
        let me = self.clone();
        let spawned = crate::python::stdlib::spawn_native_thread(
            alloc::format!("{}_{n}", self.name),
            move || me.worker(),
        );
        if spawned.is_err() {
            let mut g = self.state.lock();
            g.workers -= 1;
            g.queue.pop_back();
            return Err(LailaError::runtime_error("can't start new thread"));
        }
        Ok(())
    }
    fn worker(self: Arc<Self>) {
        loop {
            let job = {
                let mut g = self.state.lock();
                loop {
                    if let Some(job) = g.queue.pop_front() {
                        break Some(job);
                    }
                    if g.shutdown {
                        break None;
                    }
                    g.idle += 1;
                    self.cv.wait(&mut g);
                    g.idle -= 1;
                }
            };
            match job {
                Some(job) => job(),
                None => break,
            }
        }
        let mut g = self.state.lock();
        g.workers -= 1;
        self.cv.notify_all();
    }
    /// `executor.shutdown(wait)`: idle workers exit on the sentinel, a
    /// running body finishes and its thread exits afterwards; with
    /// `wait=True` the call returns once every worker has exited (what
    /// `ThreadPoolExecutor.shutdown(wait=True)` joins).
    pub fn shutdown(&self, wait: bool) {
        let mut g = self.state.lock();
        g.shutdown = true;
        self.cv.notify_all();
        if wait {
            while g.workers > 0 {
                self.cv.wait(&mut g);
            }
        }
    }
}

impl TaskForce {
    pub(crate) fn runtime(&self) -> Option<Arc<Runtime>> {
        self.0.rt.lock().clone()
    }
    pub(crate) fn loops(&self) -> Vec<Arc<LoopThread>> {
        self.runtime().map(|r| r.loops.clone()).unwrap_or_default()
    }

    // ---------- lifecycle hooks ----------

    pub(crate) fn _on_start(&self) -> Result<()> {
        if self.backend().to_lowercase() != "async_threads" {
            return Err(LailaError::value_error(
                "PythonAsyncThreadPoolTaskForce supports async_threads only.",
            ));
        }
        // ``pause()`` -> ``start()`` resumes the existing machinery.
        if self.runtime().is_some() {
            return Ok(());
        }
        #[cfg(feature = "std")]
        {
            let gid = self.global_id();
            let tag = &gid[gid.len().saturating_sub(8)..];
            let loops: Vec<Arc<LoopThread>> = (0..self.num_workers())
                .map(|i| {
                    LoopThread::new(
                        alloc::format!("AsyncTF-{tag}-Loop-{i}"),
                        self.max_async_per_thread(),
                    )
                })
                .collect();
            let rt = Arc::new(Runtime {
                cv: crate::python::stdlib::Condition::new(),
                capacity_cv: crate::python::stdlib::Condition::new(),
                stop: crate::python::stdlib::Event::new(Args::new()),
                dispatcher: Mutex::new(None),
                dispatcher_obj: Arc::new(DispatcherHandle {
                    name: alloc::format!("AsyncTF-{tag}-Dispatcher"),
                }),
                dispatching: core::sync::atomic::AtomicBool::new(false),
                loops,
                compute_permits: Arc::new(crate::python::stdlib::Semaphore::new(
                    self.sync_workers() as i64,
                )),
                sync_inflight: Mutex::new(0),
                sync_idle: parking_lot::Condvar::new(),
                executor: Mutex::new(SyncExecutor::new(alloc::format!("AsyncTF-{tag}-Sync1"))),
                executor_used: Mutex::new(0),
                executor_generation: Mutex::new(1),
                idle_since: Mutex::new(None),
            });
            *self.0.rt.lock() = Some(rt.clone());
            let me = self.clone();
            let rt2 = rt.clone();
            let handle = crate::python::stdlib::spawn_native_thread(
                alloc::format!("AsyncTF-{tag}-Dispatcher"),
                move || me._dispatch_loop(rt2),
            )
            .expect("dispatcher thread");
            *rt.dispatcher.lock() = Some(handle);
        }
        #[cfg(not(feature = "std"))]
        {
            *self.0.rt.lock() = Some(Arc::new(Runtime {
                dispatching: core::sync::atomic::AtomicBool::new(false),
                loops: Vec::new(),
                compute_permits: Arc::new(crate::python::stdlib::Semaphore::new(
                    self.sync_workers() as i64,
                )),
                sync_inflight: Mutex::new(0),
            }));
        }
        Ok(())
    }
    /// Safe no-op: the status flips to `PAUSED` so `_queue_submit`
    /// rejects new work; in-flight tasks keep running.
    pub(crate) fn _on_pause(&self) {}

    /// `True` from one of this taskforce's loop or body threads.
    pub(crate) fn _is_own_worker_thread(&self) -> bool {
        let ident = crate::policy::central::command::schema::exceptions::current_thread_ident();
        if self
            .loops()
            .iter()
            .any(|lt| lt.thread_ident() == Some(ident))
        {
            return true;
        }
        _CURRENT_SLOT
            .get()
            .is_some_and(|s| s.tf.ptr_id() == self.ptr_id())
    }

    pub(crate) fn _on_shutdown(&self, wait: bool, cancel_pending: bool) -> Result<()> {
        let Some(rt) = self.runtime() else {
            return Ok(());
        };
        #[cfg(feature = "std")]
        {
            let mut drain = wait && !cancel_pending;
            let self_call = self._is_own_worker_thread();
            if drain && self_call {
                std::eprintln!(
                    "WARNING: shutdown(wait=True) called from a task running on taskforce {}; \
                     queued tasks are cancelled instead of drained.",
                    self.global_id()
                );
                drain = false;
            }
            if drain {
                self._drain_queue(&rt);
                for lt in &rt.loops {
                    lt.request_stop(true);
                }
                for lt in &rt.loops {
                    lt.join(None);
                }
            }
            rt.stop.set(Args::new());
            {
                let _g = rt.cv.lock();
                rt.cv.notify_all();
            }
            {
                let _g = rt.capacity_cv.lock();
                rt.capacity_cv.notify_all();
            }
            // Let the dispatcher exit before the cancel pass so that pass
            // sees every undispatched task.
            let handle = rt.dispatcher.lock().take();
            if let Some(h) = handle {
                if wait {
                    let _ = h.join();
                } else {
                    let deadline = std::time::Instant::now() + core::time::Duration::from_secs(1);
                    while !h.is_finished() && std::time::Instant::now() < deadline {
                        std::thread::sleep(core::time::Duration::from_millis(5));
                    }
                    if h.is_finished() {
                        let _ = h.join();
                    }
                }
            }
            if cancel_pending || drain {
                self._cancel_queued("Task canceled before dispatch.");
            }
            if !drain {
                for lt in &rt.loops {
                    lt.stop(if wait { None } else { Some(0.0) }, false);
                }
            }
            // The sync executor: wait for running bodies (unless called
            // from one of them).
            if wait && !self_call {
                let mut g = rt.sync_inflight.lock();
                while *g > 0 {
                    rt.sync_idle.wait(&mut g);
                }
            }
            rt.executor.lock().shutdown(wait && !self_call);
            *self.0.rt.lock() = None;
        }
        #[cfg(not(feature = "std"))]
        {
            let _ = (wait, cancel_pending, rt);
            self._cancel_queued("Task canceled before dispatch.");
            *self.0.rt.lock() = None;
        }
        Ok(())
    }

    /// Block until the dispatcher has placed every queued task on a loop.
    #[cfg(feature = "std")]
    fn _drain_queue(&self, rt: &Arc<Runtime>) {
        loop {
            let alive = rt
                .dispatcher
                .lock()
                .as_ref()
                .is_some_and(|h| !h.is_finished());
            if !alive {
                return;
            }
            let busy =
                self.queue_len() > 0 || rt.dispatching.load(core::sync::atomic::Ordering::Acquire);
            if !busy || rt.stop.is_set(Args::new()) {
                return;
            }
            {
                let _g = rt.cv.lock();
                rt.cv.notify_all();
            }
            std::thread::sleep(core::time::Duration::from_millis(50));
        }
    }

    fn _cancel_queued(&self, reason: &str) {
        let items: Vec<(String, Queued)> = self.0.q.lock().drain(..).collect();
        for (_, q) in items {
            q.fut
                .set_exception(Some(Arc::new(LailaError::runtime_error(reason))));
            q.fut.set_result(Arg::None);
            q.fut.set_status(FutureStatus::CANCELLED);
        }
    }

    // ---------- sync offload ----------

    /// Run *body* on an executor thread under a compute permit, with the
    /// current context (slot, resolve chain) propagated so the body can
    /// park its slot -- and release its permit -- when it blocks on laila
    /// futures. Awaitable.
    pub fn run_sync(&self, body: impl IntoSyncBody) -> Result<OneShot<Result<Arg>>> {
        let body = body.into_sync_body();
        let Some(rt) = self.runtime() else {
            return Err(LailaError::runtime_error(
                "TaskForce must be running before offloading sync work.",
            ));
        };
        let slot: OneShot<Result<Arg>> = OneShot::new();
        let permits = rt.compute_permits.clone();
        let ctx = crate::python::stdlib::copy_context();
        #[cfg(feature = "std")]
        {
            *rt.sync_inflight.lock() += 1;
            let slot2 = slot.clone();
            let rt2 = rt.clone();
            let job: alloc::boxed::Box<dyn FnOnce() + Send> = alloc::boxed::Box::new(move || {
                let out = ctx.run(|| {
                    let permit = _Permit::new(permits);
                    let token = _CURRENT_PERMIT.set(Some(permit.clone()));
                    permit.acquire();
                    let r = body();
                    permit.release();
                    _CURRENT_PERMIT.reset(token);
                    r
                });
                slot2.set_result(out);
                let mut g = rt2.sync_inflight.lock();
                *g -= 1;
                rt2.sync_idle.notify_all();
            });
            // Submit under the executor lock so an idle recycle (see
            // ``_maybe_recycle_executor``) can never shut the executor
            // down between the read and the submit.
            let executor = rt.executor.lock();
            if let Err(e) = executor.submit(job) {
                *rt.sync_inflight.lock() -= 1;
                return Err(e);
            }
            *rt.executor_used.lock() += 1;
        }
        #[cfg(not(feature = "std"))]
        {
            let out = ctx.run(|| {
                let permit = _Permit::new(permits);
                permit.acquire();
                let r = body();
                permit.release();
                r
            });
            slot.set_result(out);
        }
        Ok(slot)
    }

    // ---------- observability ----------

    /// Slots currently occupied across all loops.
    pub fn inflight(&self) -> usize {
        self.loops().iter().map(|l| l.inflight_count()).sum()
    }
    /// Parked tasks currently waiting to re-acquire a slot.
    pub fn parked(&self) -> usize {
        self.loops().iter().map(|l| l.parked_waiting()).sum()
    }
    /// `_executor_generation`: how many sync executors this taskforce has
    /// created (bumps on each idle recycle); `0` when not running.
    pub fn executor_generation(&self) -> usize {
        #[cfg(feature = "std")]
        {
            self.runtime()
                .map(|rt| *rt.executor_generation.lock())
                .unwrap_or(0)
        }
        #[cfg(not(feature = "std"))]
        {
            0
        }
    }
    pub(crate) fn ptr_id(&self) -> usize {
        Arc::as_ptr(&self.0) as *const () as usize
    }

    // ---------- submission ----------

    pub(crate) fn _queue_submit(&self, task: Arg) -> Result<Future> {
        if self.status() != TaskForceStatus::RUNNING {
            return Err(LailaError::runtime_error(
                "TaskForce must be running before submitting tasks.",
            ));
        }
        let task = ensure_coroutine_function(task);
        let fut = super::super::thread_pool_executor::future::ConcurrentPackageFuture::create(
            Arg::Str(self.global_id().into()),
            self.policy_id(),
        )?;
        let queued = Queued {
            task,
            fut: fut.clone(),
            chain: _RESOLVE_CHAIN.get(),
        };
        #[cfg(feature = "std")]
        {
            let rt = self.runtime();
            if let Some(rt) = rt {
                let _g = rt.cv.lock();
                self.0.q.lock().push_back((fut.global_id(), queued));
                rt.cv.notify();
            } else {
                self.0.q.lock().push_back((fut.global_id(), queued));
            }
        }
        #[cfg(not(feature = "std"))]
        {
            // Single core, no threads: run the task to completion now.
            self.0.q.lock().push_back((fut.global_id(), queued));
            if let Some(rt) = self.runtime() {
                self._dispatch_inline(&rt);
            }
        }
        Ok(fut)
    }

    pub(crate) fn submit_impl(&self, tasks: Arg, wait: bool) -> Result<Arg> {
        let tasks = tasks.__iter__()?;
        let mut futures = Vec::with_capacity(tasks.len());
        for t in tasks {
            let f = self._queue_submit(t)?;
            f.__setattr__("taskforce_id", Arg::Str(self.global_id().into()))?;
            futures.push(f);
        }
        if futures.len() == 1 {
            let single = futures.pop().expect("one");
            if wait {
                let r = single.wait(Args::new());
                single.release(Args::new())?;
                return r;
            }
            return Ok(Arg::Future(single));
        }
        let mut a = Args::new();
        a.push_keyword("taskforce_id", Arg::Str(self.global_id().into()));
        a.push_keyword("policy_id", self.policy_id());
        a.push_keyword(
            "future_ids",
            futures
                .iter()
                .map(|f| Arg::Str(f.global_id().into()))
                .collect::<Vec<_>>(),
        );
        let gf = Future::construct(FutureKind::Group, a)?;
        for f in &futures {
            f.__setattr__("future_group_id", Arg::Str(gf.global_id().into()))?;
        }
        if !wait {
            return Ok(Arg::Future(gf));
        }
        let r = gf.wait(Args::new());
        gf.release(Args::new())?;
        r
    }

    // ---------- dispatcher ----------

    /// Reserve a slot on the loop with the lowest in-flight count.
    fn _pick_loop(&self, rt: &Runtime) -> Option<Arc<LoopThread>> {
        let cap = self.max_async_per_thread();
        let mut best: Option<&Arc<LoopThread>> = None;
        let mut best_count = cap;
        for lt in &rt.loops {
            let c = lt.inflight_count();
            if c < best_count {
                best_count = c;
                best = Some(lt);
            }
        }
        match best {
            Some(lt) if lt.try_reserve() => Some(lt.clone()),
            _ => None,
        }
    }

    #[cfg(feature = "std")]
    fn _dispatch_loop(&self, rt: Arc<Runtime>) {
        self._dispatch_loop_body(&rt);
    }
    #[cfg(feature = "std")]
    fn _dispatch_loop_body(&self, rt: &Arc<Runtime>) {
        while !rt.stop.is_set(Args::new()) {
            let item = {
                let mut g = rt.cv.lock();
                while !rt.stop.is_set(Args::new()) && self.queue_len() == 0 {
                    rt.cv.wait(&mut g, Some(0.1));
                    self._maybe_recycle_executor(rt);
                }
                if rt.stop.is_set(Args::new()) {
                    break;
                }
                rt.dispatching
                    .store(true, core::sync::atomic::Ordering::Release);
                self.0.q.lock().pop_front()
            };
            let Some((_, queued)) = item else {
                rt.dispatching
                    .store(false, core::sync::atomic::Ordering::Release);
                continue;
            };
            let mut picked = None;
            while !rt.stop.is_set(Args::new()) {
                picked = self._pick_loop(rt);
                if picked.is_some() {
                    break;
                }
                let mut g = rt.capacity_cv.lock();
                rt.capacity_cv.wait(&mut g, Some(0.1));
            }
            match picked {
                None => {
                    // Stopping: put the item back for the cancel pass.
                    self.0.q.lock().push_front((queued.fut.global_id(), queued));
                    rt.dispatching
                        .store(false, core::sync::atomic::Ordering::Release);
                    break;
                }
                Some(lt) => {
                    let coro = self._make_runner(queued, lt.clone());
                    lt.submit_coro(coro);
                }
            }
            rt.dispatching
                .store(false, core::sync::atomic::Ordering::Release);
        }
    }

    /// Retire idle sync threads: once nothing is in flight, parked or
    /// queued for `_EXECUTOR_IDLE_RECYCLE_S` and the current executor has
    /// run something, swap in a fresh executor and shut the old one down
    /// without waiting (its idle workers exit; a running body finishes).
    #[cfg(feature = "std")]
    fn _maybe_recycle_executor(&self, rt: &Arc<Runtime>) {
        let busy = self.queue_len() > 0 || self.inflight() > 0 || self.parked() > 0;
        let now = crate::python::stdlib::time.monotonic();
        if busy {
            *rt.idle_since.lock() = None;
            return;
        }
        {
            let mut since = rt.idle_since.lock();
            match *since {
                None => {
                    *since = Some(now);
                    return;
                }
                Some(t) if now - t < _EXECUTOR_IDLE_RECYCLE_S => return,
                Some(_) => {}
            }
        }
        let old = {
            let mut executor = rt.executor.lock();
            let mut used = rt.executor_used.lock();
            if *used == 0 {
                *rt.idle_since.lock() = Some(now);
                return;
            }
            let gid = self.global_id();
            let tag = &gid[gid.len().saturating_sub(8)..];
            let mut generation = rt.executor_generation.lock();
            *generation += 1;
            let fresh = SyncExecutor::new(alloc::format!("AsyncTF-{tag}-Sync{}", *generation));
            let old = core::mem::replace(&mut *executor, fresh);
            *used = 0;
            *rt.idle_since.lock() = Some(now);
            old
        };
        old.shutdown(false);
    }

    #[cfg(not(feature = "std"))]
    fn _dispatch_inline(&self, rt: &Arc<Runtime>) {
        let _ = rt;
        while let Some((_, queued)) = self.0.q.lock().pop_front() {
            let lt = Arc::new(LoopThread {
                name: String::from("inline"),
                cap: 1,
                r#loop: crate::python::stdlib::EventLoop::new(),
                inflight: Mutex::new(1),
                resume_waiters: Mutex::new(alloc::collections::VecDeque::new()),
                thread_ident: Mutex::new(None),
            });
            crate::python::stdlib::asyncio::run(self._make_runner(queued, lt));
        }
    }

    /// The runner coroutine for one task: publish the slot context, run
    /// the task (awaiting the coroutine it returns), record the outcome
    /// on the future and hand the slot back.
    fn _make_runner(
        &self,
        queued: Queued,
        lt: Arc<LoopThread>,
    ) -> impl core::future::Future<Output = ()> + Send + 'static {
        let tf = self.clone();
        async move {
            let Queued { task, fut, chain } = queued;
            let ctx = _SlotCtx::new(
                Arc::new(tf.clone()) as Arc<dyn SlotTaskForce>,
                lt.clone() as Arc<dyn SlotLoop>,
            );
            let slot_token = _CURRENT_SLOT.set(Some(ctx.clone()));
            let chain_token = _RESOLVE_CHAIN.set(chain);
            fut.set_status(FutureStatus::RUNNING);
            // Dropped without ``done``: the loop cancelled us (shutdown).
            struct Guard {
                fut: Future,
                ctx: Arc<_SlotCtx>,
                lt: Arc<LoopThread>,
                tf: TaskForce,
                done: bool,
            }
            impl Drop for Guard {
                fn drop(&mut self) {
                    if !self.done {
                        self.fut
                            .set_exception(Some(Arc::new(LailaError::runtime_error(
                                "Task cancelled during taskforce shutdown.",
                            ))));
                        self.fut.set_result(Arg::None);
                        self.fut.set_status(FutureStatus::CANCELLED);
                    }
                    if self.ctx.take_slot() && self.lt.release_slot() {
                        self.tf.notify_capacity();
                    }
                }
            }
            let mut guard = Guard {
                fut: fut.clone(),
                ctx,
                lt,
                tf,
                done: false,
            };
            let outcome: Result<Arg> = async {
                let out = task.__call__(Args::new())?;
                if Coroutine::of(&out).is_some() {
                    return out.await;
                }
                Ok(out)
            }
            .await;
            let status = match outcome {
                Ok(out) => {
                    fut.set_exception(None);
                    fut.set_result(out);
                    FutureStatus::FINISHED
                }
                Err(e) => {
                    fut.set_exception(Some(Arc::new(e)));
                    fut.set_result(Arg::None);
                    FutureStatus::ERROR
                }
            };
            guard.done = true;
            _RESOLVE_CHAIN.reset(chain_token);
            _CURRENT_SLOT.reset(slot_token);
            // The slot goes back *before* the terminal status wakes the
            // waiters (Python's `finally` runs right after the assignment
            // under the GIL; here the waiter is truly parallel), so a
            // `fut.wait()` that returns observes the slot released.
            drop(guard);
            fut.set_status(status);
        }
    }
}

impl SlotTaskForce for TaskForce {
    fn notify_capacity(&self) {
        #[cfg(feature = "std")]
        if let Some(rt) = self.runtime() {
            let _g = rt.capacity_cv.lock();
            rt.capacity_cv.notify();
        }
    }
    fn stop_requested(&self) -> bool {
        #[cfg(feature = "std")]
        {
            self.runtime().is_none_or(|rt| rt.stop.is_set(Args::new()))
        }
        #[cfg(not(feature = "std"))]
        {
            self.runtime().is_none()
        }
    }
    fn global_id(&self) -> String {
        TaskForce::global_id(self)
    }
    fn rank(&self) -> i64 {
        TaskForce::rank(self)
    }
    fn ptr_id(&self) -> usize {
        TaskForce::ptr_id(self)
    }
    fn run_sync(
        &self,
        body: alloc::boxed::Box<dyn FnOnce() -> Result<Arg> + Send>,
    ) -> Result<OneShot<Result<Arg>>> {
        TaskForce::run_sync(self, body)
    }
}
