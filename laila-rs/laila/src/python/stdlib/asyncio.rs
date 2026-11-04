//! `asyncio` -- the event loop the command taskforces run on, plus the
//! helpers the port's coroutines use (`sleep`, `gather`, `to_thread`,
//! `run`, `run_coroutine_threadsafe`, `get_running_loop`, `create_future`).
//!
//! laila-rs has no third-party runtime: an [`EventLoop`] is a ready queue
//! of tasks polled on the thread that called [`EventLoop::run_forever`],
//! each task under its own `contextvars` context (so `_CURRENT_SLOT` and
//! the resolve chain behave as in CPython); timers live on one lazily
//! spawned timer thread shared by every loop. [`run`] drives a future on
//! the calling thread without a loop (`asyncio.run`), parking the thread
//! between wake-ups.
//!
//! Cancellation is Rust's: dropping a task's future at loop teardown runs
//! its `Drop` guards, which is where the runner's `finally` bookkeeping
//! lives.

use alloc::boxed::Box;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::task::Wake;
use alloc::vec::Vec;
use core::future::Future;
use core::pin::{pin, Pin};
use core::task::{Context, Poll, Waker};

use super::contextvars;
use crate::python::object::PyObject;
use crate::python::sync::Mutex;
use crate::python::{Arg, Args, ExcKind, IntoArg, LailaError, PyArg, Result};

/// A boxed, sendable future resolving to a Python value.
pub type BoxFut = Pin<Box<dyn Future<Output = Result<Arg>> + Send + 'static>>;

/// Wakes the thread that is blocked in [`run`].
struct ThreadWaker {
    #[cfg(feature = "std")]
    thread: std::thread::Thread,
}

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        #[cfg(feature = "std")]
        self.thread.unpark();
    }
}

fn park() {
    #[cfg(feature = "std")]
    std::thread::park();
    #[cfg(not(feature = "std"))]
    core::hint::spin_loop();
}

/// The result of a future `asyncio.run` drives: a coroutine object's
/// `Result<Arg>` or a typed Rust `async fn`'s `Result<T>`.
pub trait RunOutput {
    fn into_py(self) -> Result<Arg>;
}
impl<T: IntoArg> RunOutput for Result<T> {
    fn into_py(self) -> Result<Arg> {
        self.map(IntoArg::into_arg)
    }
}

/// `asyncio.run(x)` from `laila::python!`: *x* is a coroutine object, an
/// awaitable `Arg`, or a typed Rust future; the result is a Python value.
pub fn run_value<F: core::future::IntoFuture>(fut: F) -> Result<Arg>
where
    F::Output: RunOutput,
{
    run(fut).into_py()
}

/// `asyncio.run(coro)`: block the calling thread until *fut* resolves.
pub fn run<F: core::future::IntoFuture>(fut: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWaker {
        #[cfg(feature = "std")]
        thread: std::thread::current(),
    }));
    let mut cx = Context::from_waker(&waker);
    let fut = fut.into_future();
    let mut fut = pin!(fut);
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
        park();
    }
}

// ---------------------------------------------------------------------------
// asyncio.Future (loop.create_future())

struct OneShotState<T> {
    value: Option<T>,
    wakers: Vec<Waker>,
    cancelled: bool,
}

/// `loop.create_future()` -- a one-shot settled from any thread.
pub struct OneShot<T>(Arc<Mutex<OneShotState<T>>>);
impl<T> Clone for OneShot<T> {
    fn clone(&self) -> Self {
        OneShot(self.0.clone())
    }
}
impl<T> Default for OneShot<T> {
    fn default() -> Self {
        Self::new()
    }
}
impl<T> OneShot<T> {
    pub fn new() -> OneShot<T> {
        OneShot(Arc::new(Mutex::new(OneShotState {
            value: None,
            wakers: Vec::new(),
            cancelled: false,
        })))
    }
    pub fn done(&self) -> bool {
        let g = self.0.lock();
        g.value.is_some() || g.cancelled
    }
    /// `fut.set_result(v)` (a no-op once done, like `if not fut.done()`).
    pub fn set_result(&self, v: T) {
        let wakers = {
            let mut g = self.0.lock();
            if g.value.is_some() || g.cancelled {
                return;
            }
            g.value = Some(v);
            core::mem::take(&mut g.wakers)
        };
        wakers.into_iter().for_each(Waker::wake);
    }
    pub fn cancel(&self) {
        let wakers = {
            let mut g = self.0.lock();
            if g.value.is_some() || g.cancelled {
                return;
            }
            g.cancelled = true;
            core::mem::take(&mut g.wakers)
        };
        wakers.into_iter().for_each(Waker::wake);
    }
}
impl<T: Clone> Future for OneShot<T> {
    type Output = Result<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut g = self.0.lock();
        if let Some(v) = &g.value {
            return Poll::Ready(Ok(v.clone()));
        }
        if g.cancelled {
            return Poll::Ready(Err(LailaError::new(ExcKind::CancelledError, String::new())));
        }
        if let Some(e) = cancel_requested(cx) {
            return Poll::Ready(Err(e));
        }
        if !g.wakers.iter().any(|w| w.will_wake(cx.waker())) {
            g.wakers.push(cx.waker().clone());
        }
        Poll::Pending
    }
}

// ---------------------------------------------------------------------------
// Task cancellation (`task.cancel()` from another task / thread)

/// A pending cancellation of the task being polled (Python's
/// `task.cancel()`): the next suspension point of that task (`sleep`,
/// `to_thread`, a one-shot) resolves to the stored exception instead.
/// Installed per task through the [`CANCEL_SCOPE`] context variable.
#[derive(Default)]
pub struct CancelToken {
    err: Mutex<Option<LailaError>>,
    wakers: Mutex<Vec<Waker>>,
}

impl CancelToken {
    pub fn new() -> Arc<CancelToken> {
        Arc::new(CancelToken::default())
    }
    /// Cancel with *err* (the first request wins) and wake the task.
    pub fn cancel_with(&self, err: LailaError) {
        {
            let mut g = self.err.lock();
            if g.is_some() {
                return;
            }
            *g = Some(err);
        }
        let wakers = core::mem::take(&mut *self.wakers.lock());
        wakers.into_iter().for_each(Waker::wake);
    }
    /// `task.cancel()`: a bare `CancelledError`.
    pub fn cancel(&self) {
        self.cancel_with(LailaError::new(ExcKind::CancelledError, String::new()));
    }
    pub fn is_cancelled(&self) -> bool {
        self.err.lock().is_some()
    }
    fn check(&self, cx: &mut Context<'_>) -> Option<LailaError> {
        if let Some(e) = self.err.lock().as_ref() {
            return Some(e.clone());
        }
        let mut w = self.wakers.lock();
        if !w.iter().any(|x| x.will_wake(cx.waker())) {
            w.push(cx.waker().clone());
        }
        None
    }
}

/// The cancellation token of the task currently being polled (`None`
/// outside a cancellable scope).
pub static CANCEL_SCOPE: contextvars::ContextVar<Option<Arc<CancelToken>>> =
    contextvars::ContextVar::new("_laila_cancel_scope", || None);

/// At a suspension point: the pending cancellation of the current task,
/// if any (registering the waker otherwise).
pub fn cancel_requested(cx: &mut Context<'_>) -> Option<LailaError> {
    CANCEL_SCOPE.get().and_then(|t| t.check(cx))
}

// ---------------------------------------------------------------------------
// Timers (`asyncio.sleep`)

#[cfg(feature = "std")]
mod timer {
    use alloc::collections::BinaryHeap;
    use alloc::sync::Arc;
    use core::cmp::Reverse;
    use core::task::Waker;
    use std::time::Instant;

    struct Entry(Reverse<Instant>, u64, Waker);
    impl PartialEq for Entry {
        fn eq(&self, o: &Self) -> bool {
            self.0 == o.0 && self.1 == o.1
        }
    }
    impl Eq for Entry {}
    impl PartialOrd for Entry {
        fn partial_cmp(&self, o: &Self) -> Option<core::cmp::Ordering> {
            Some(self.cmp(o))
        }
    }
    impl Ord for Entry {
        fn cmp(&self, o: &Self) -> core::cmp::Ordering {
            (self.0, Reverse(self.1)).cmp(&(o.0, Reverse(o.1)))
        }
    }

    struct Service {
        heap: parking_lot::Mutex<(BinaryHeap<Entry>, u64)>,
        cv: parking_lot::Condvar,
    }

    fn service() -> &'static Arc<Service> {
        static S: std::sync::OnceLock<Arc<Service>> = std::sync::OnceLock::new();
        S.get_or_init(|| {
            let s = Arc::new(Service {
                heap: parking_lot::Mutex::new((BinaryHeap::new(), 0)),
                cv: parking_lot::Condvar::new(),
            });
            let s2 = s.clone();
            std::thread::Builder::new()
                .name("laila-asyncio-timer".into())
                .spawn(move || s2.run())
                .expect("timer thread");
            s
        })
    }

    impl Service {
        fn run(&self) {
            let mut g = self.heap.lock();
            loop {
                let now = Instant::now();
                let mut due = alloc::vec::Vec::new();
                while let Some(top) = g.0.peek() {
                    if top.0 .0 <= now {
                        due.push(g.0.pop().expect("peeked").2);
                    } else {
                        break;
                    }
                }
                if !due.is_empty() {
                    parking_lot::MutexGuard::unlocked(&mut g, || {
                        due.into_iter().for_each(Waker::wake);
                    });
                    continue;
                }
                match g.0.peek() {
                    Some(top) => {
                        let d = top.0 .0.saturating_duration_since(Instant::now());
                        self.cv.wait_for(&mut g, d);
                    }
                    None => self.cv.wait(&mut g),
                }
            }
        }
    }

    /// Wake *waker* at *deadline*.
    pub fn register(deadline: Instant, waker: Waker) {
        let s = service();
        {
            let mut g = s.heap.lock();
            g.1 += 1;
            let seq = g.1;
            g.0.push(Entry(Reverse(deadline), seq, waker));
        }
        s.cv.notify_one();
    }
}

/// `asyncio.sleep(delay)` -- resolves after *delay* seconds.
pub struct Sleep {
    #[cfg(feature = "std")]
    deadline: std::time::Instant,
    registered: bool,
}
impl Future for Sleep {
    type Output = Result<Arg>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        #[cfg(feature = "std")]
        {
            if let Some(e) = cancel_requested(cx) {
                return Poll::Ready(Err(e));
            }
            if std::time::Instant::now() >= self.deadline {
                return Poll::Ready(Ok(Arg::None));
            }
            if !self.registered {
                self.registered = true;
            }
            // Re-register on every poll: a waker may change between polls.
            timer::register(self.deadline, cx.waker().clone());
            Poll::Pending
        }
        #[cfg(not(feature = "std"))]
        {
            let _ = cx;
            self.registered = true;
            Poll::Ready(Ok(Arg::None))
        }
    }
}

// ---------------------------------------------------------------------------
// Tasks and the loop

struct Task {
    fut: Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>>,
    #[cfg_attr(not(feature = "std"), allow(dead_code))]
    ctx: Mutex<contextvars::Context>,
    r#loop: alloc::sync::Weak<LoopInner>,
    done: core::sync::atomic::AtomicBool,
}

impl Wake for Task {
    fn wake(self: Arc<Self>) {
        if let Some(l) = self.r#loop.upgrade() {
            l.push(self);
        }
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.clone().wake();
    }
}

struct LoopInner {
    ready: Mutex<alloc::collections::VecDeque<Arc<Task>>>,
    tasks: Mutex<Vec<alloc::sync::Weak<Task>>>,
    #[cfg(feature = "std")]
    cv: parking_lot::Condvar,
    stopped: core::sync::atomic::AtomicBool,
    closed: core::sync::atomic::AtomicBool,
    running: core::sync::atomic::AtomicBool,
}

impl LoopInner {
    fn push(&self, t: Arc<Task>) {
        if t.done.load(core::sync::atomic::Ordering::Acquire) {
            return;
        }
        {
            let mut q = self.ready.lock();
            if q.iter().any(|x| Arc::ptr_eq(x, &t)) {
                return;
            }
            q.push_back(t);
        }
        #[cfg(feature = "std")]
        self.cv.notify_one();
    }
}

/// `asyncio.new_event_loop()` (handle; clones alias one loop).
#[derive(Clone)]
pub struct EventLoop(Arc<LoopInner>);

#[cfg(feature = "std")]
std::thread_local! {
    static RUNNING_LOOP: core::cell::RefCell<Option<EventLoop>> = const { core::cell::RefCell::new(None) };
}

/// `asyncio.get_running_loop()` -> the loop running on this thread.
pub fn get_running_loop() -> Option<EventLoop> {
    #[cfg(feature = "std")]
    {
        RUNNING_LOOP.with(|l| l.borrow().clone())
    }
    #[cfg(not(feature = "std"))]
    {
        None
    }
}

impl Default for EventLoop {
    fn default() -> Self {
        Self::new()
    }
}

impl EventLoop {
    /// `asyncio.new_event_loop()`.
    pub fn new() -> EventLoop {
        EventLoop(Arc::new(LoopInner {
            ready: Mutex::new(alloc::collections::VecDeque::new()),
            tasks: Mutex::new(Vec::new()),
            #[cfg(feature = "std")]
            cv: parking_lot::Condvar::new(),
            stopped: core::sync::atomic::AtomicBool::new(false),
            closed: core::sync::atomic::AtomicBool::new(false),
            running: core::sync::atomic::AtomicBool::new(false),
        }))
    }
    pub fn is(&self, other: &EventLoop) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    /// Schedule *fut* as a task under a copy of the current context
    /// (`loop.create_task` / `asyncio.run_coroutine_threadsafe`).
    pub fn spawn<F: Future<Output = ()> + Send + 'static>(&self, fut: F) {
        self.spawn_in(fut, contextvars::copy_context());
    }
    fn spawn_in<F: Future<Output = ()> + Send + 'static>(&self, fut: F, ctx: contextvars::Context) {
        if self.0.closed.load(core::sync::atomic::Ordering::Acquire) {
            // Loop closed: the task is dropped (cancelled), as a
            // ``RuntimeError('Event loop is closed')`` the callers swallow.
            return;
        }
        let t = Arc::new(Task {
            fut: Mutex::new(Some(Box::pin(fut))),
            ctx: Mutex::new(ctx),
            r#loop: Arc::downgrade(&self.0),
            done: core::sync::atomic::AtomicBool::new(false),
        });
        self.0.tasks.lock().push(Arc::downgrade(&t));
        self.0.push(t);
    }
    /// `asyncio.run_coroutine_threadsafe(coro, loop)` -> a
    /// `concurrent.futures.Future` settled with the coroutine's outcome.
    #[cfg(feature = "std")]
    pub fn run_coroutine_threadsafe<F: Future<Output = Result<Arg>> + Send + 'static>(
        &self,
        coro: F,
    ) -> super::concurrent::NativeFuture {
        let nf = super::concurrent::NativeFuture::default();
        let nf2 = nf.clone();
        self.spawn(async move {
            match coro.await {
                Ok(v) => nf2.set_result(v),
                Err(e) => nf2.set_exception(Arc::new(e)),
            }
        });
        nf
    }
    /// `loop.call_soon_threadsafe(fn)`.
    pub fn call_soon_threadsafe(&self, f: impl FnOnce() + Send + 'static) {
        let mut f = Some(f);
        self.spawn(core::future::poll_fn(move |_| {
            if let Some(f) = f.take() {
                f();
            }
            Poll::Ready(())
        }));
    }
    /// `len(asyncio.all_tasks(loop))`: tasks scheduled and not finished.
    pub fn all_tasks(&self) -> usize {
        let mut g = self.0.tasks.lock();
        g.retain(|w| {
            w.upgrade()
                .is_some_and(|t| !t.done.load(core::sync::atomic::Ordering::Acquire))
        });
        g.len()
    }
    /// `loop.stop()`: `run_forever` returns after the current iteration.
    pub fn stop(&self) {
        self.0
            .stopped
            .store(true, core::sync::atomic::Ordering::Release);
        #[cfg(feature = "std")]
        self.0.cv.notify_all();
    }
    pub fn is_running(&self) -> bool {
        self.0.running.load(core::sync::atomic::Ordering::Acquire)
    }
    pub fn is_closed(&self) -> bool {
        self.0.closed.load(core::sync::atomic::Ordering::Acquire)
    }
    /// `loop.close()`: drop (cancel) every task still scheduled.
    pub fn close(&self) {
        self.0
            .closed
            .store(true, core::sync::atomic::Ordering::Release);
        let tasks: Vec<Arc<Task>> = self
            .0
            .tasks
            .lock()
            .drain(..)
            .filter_map(|w| w.upgrade())
            .collect();
        self.0.ready.lock().clear();
        for t in tasks {
            t.done.store(true, core::sync::atomic::Ordering::Release);
            // Dropping the future is the cancellation: ``finally`` blocks
            // (Drop guards) run here, on the closing thread.
            let fut = t.fut.lock().take();
            drop(fut);
        }
    }
    #[cfg(feature = "std")]
    fn poll_task(&self, t: &Arc<Task>) {
        let Some(mut fut) = t.fut.lock().take() else {
            return;
        };
        let waker = Waker::from(t.clone());
        let mut cx = Context::from_waker(&waker);
        let ctx = t.ctx.lock().clone();
        let guard = ctx.enter();
        let r = fut.as_mut().poll(&mut cx);
        *t.ctx.lock() = guard.modified();
        drop(guard);
        match r {
            Poll::Ready(()) => {
                t.done.store(true, core::sync::atomic::Ordering::Release);
            }
            Poll::Pending => {
                *t.fut.lock() = Some(fut);
            }
        }
    }
    /// `loop.run_forever()`: poll ready tasks on this thread until `stop`.
    #[cfg(feature = "std")]
    pub fn run_forever(&self) {
        RUNNING_LOOP.with(|l| *l.borrow_mut() = Some(self.clone()));
        self.0
            .running
            .store(true, core::sync::atomic::Ordering::Release);
        self.0
            .stopped
            .store(false, core::sync::atomic::Ordering::Release);
        loop {
            let next = {
                let mut q = self.0.ready.lock();
                loop {
                    if self.0.stopped.load(core::sync::atomic::Ordering::Acquire) {
                        break None;
                    }
                    if let Some(t) = q.pop_front() {
                        break Some(t);
                    }
                    self.0
                        .cv
                        .wait_for(&mut q, core::time::Duration::from_millis(100));
                }
            };
            match next {
                Some(t) => self.poll_task(&t),
                None => break,
            }
        }
        self.0
            .running
            .store(false, core::sync::atomic::Ordering::Release);
        RUNNING_LOOP.with(|l| *l.borrow_mut() = None);
    }
    /// `loop.run_until_complete(coro)` on this thread (used by [`run`]
    /// callers that need a loop; the taskforces only `run_forever`).
    #[cfg(feature = "std")]
    pub fn run_until_complete<F: Future<Output = Result<Arg>> + Send + 'static>(
        &self,
        coro: F,
    ) -> Result<Arg> {
        let nf = self.run_coroutine_threadsafe(coro);
        let me = self.clone();
        nf.add_done_callback(move |_| me.stop());
        self.run_forever();
        nf.result(Some(0.0))
    }
}

// ---------------------------------------------------------------------------
// Coroutine objects

/// A coroutine as a Python value (`lambda: aiowork()` returns one; the
/// runner awaits it: `if inspect.iscoroutine(out): out = await out`).
pub struct Coroutine {
    fut: Mutex<Option<BoxFut>>,
}

impl Coroutine {
    pub fn new<F: Future<Output = Result<Arg>> + Send + 'static>(f: F) -> Coroutine {
        Coroutine {
            fut: Mutex::new(Some(Box::pin(f))),
        }
    }
    /// Take the future out (awaiting a coroutine twice is Python's
    /// `RuntimeError: cannot reuse already awaited coroutine`).
    pub fn take(&self) -> Result<BoxFut> {
        self.fut
            .lock()
            .take()
            .ok_or_else(|| LailaError::runtime_error("cannot reuse already awaited coroutine"))
    }
    /// `inspect.iscoroutine(x)`.
    pub fn of(x: &Arg) -> Option<&Coroutine> {
        match x {
            Arg::Object(o) => o.as_any().downcast_ref::<Coroutine>(),
            _ => None,
        }
    }
}
impl PyObject for Coroutine {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "coroutine"
    }
    fn __repr__(&self) -> String {
        alloc::format!("<coroutine object at {:#x}>", self as *const _ as usize)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        core::ptr::eq(
            other.as_any() as *const dyn core::any::Any as *const (),
            self as *const _ as *const (),
        )
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(self as *const _ as usize as i64)
    }
}
impl<F: Future<Output = Result<Arg>> + Send + 'static> From<F> for Coroutine {
    fn from(f: F) -> Coroutine {
        Coroutine::new(f)
    }
}

/// Wrap *fut* as a Python coroutine value.
pub fn coroutine<F: Future<Output = Result<Arg>> + Send + 'static>(f: F) -> Arg {
    Arg::Object(Arc::new(Coroutine::new(f)))
}

/// An `async def` as a value: a callable that returns a coroutine
/// (`inspect.iscoroutinefunction(x)` is `True`). Submitting one to a
/// taskforce skips the sync-offload thread hop.
pub struct CoroutineFunction {
    f: Arc<dyn Fn(Args) -> Result<Arg> + Send + Sync>,
    name: String,
}

impl CoroutineFunction {
    /// `inspect.iscoroutinefunction(x)`.
    pub fn of(x: &Arg) -> Option<&CoroutineFunction> {
        match x {
            Arg::Object(o) => o.as_any().downcast_ref::<CoroutineFunction>(),
            _ => None,
        }
    }
}
impl PyObject for CoroutineFunction {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "function"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<function {} at {:#x}>",
            self.name,
            self as *const _ as usize
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        core::ptr::eq(
            other.as_any() as *const dyn core::any::Any as *const (),
            self as *const _ as *const (),
        )
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(self as *const _ as usize as i64)
    }
    fn __call__(&self, args: Args) -> Result<Arg> {
        (self.f)(args)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "__name__" | "__qualname__" => Ok(Arg::Str(self.name.as_str().into())),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'function' object has no attribute '{other}'"
            ))),
        }
    }
}

/// `async def name(*args, **kwargs): ...` as a value: *f* builds the
/// coroutine's future from the call arguments.
pub fn coroutine_function<Fut>(name: &str, f: impl Fn(Args) -> Fut + Send + Sync + 'static) -> Arg
where
    Fut: Future<Output = Result<Arg>> + Send + 'static,
{
    Arg::Object(Arc::new(CoroutineFunction {
        f: Arc::new(move |a: Args| Ok(coroutine(f(a)))),
        name: name.into(),
    }))
}

/// `inspect.iscoroutinefunction(x)`.
pub fn iscoroutinefunction(x: &Arg) -> bool {
    CoroutineFunction::of(x).is_some()
}

/// `await x` on a Python value: a laila `Future` / `GroupFuture`, a
/// coroutine object, a `concurrent.futures.Future`, or (as Python's
/// `asyncio` rejects) anything else -> `TypeError`.
impl core::future::IntoFuture for Arg {
    type Output = Result<Arg>;
    type IntoFuture = BoxFut;
    fn into_future(self) -> BoxFut {
        match self {
            Arg::Future(f) => Box::pin(f.__await__()),
            Arg::Object(o) => {
                if let Some(c) = o.as_any().downcast_ref::<Coroutine>() {
                    return match c.take() {
                        Ok(f) => f,
                        Err(e) => Box::pin(async move { Err(e) }),
                    };
                }
                #[cfg(feature = "std")]
                if let Some(nf) = o.as_any().downcast_ref::<super::concurrent::NativeFuture>() {
                    return Box::pin(nf.clone());
                }
                let msg = alloc::format!(
                    "object {} can't be used in 'await' expression",
                    o.type_name()
                );
                Box::pin(async move { Err(LailaError::type_error(msg)) })
            }
            other => {
                let msg = alloc::format!(
                    "object {} can't be used in 'await' expression",
                    other.type_name()
                );
                Box::pin(async move { Err(LailaError::type_error(msg)) })
            }
        }
    }
}

/// `await x` in `laila::python!`: a laila `Future` handle awaits through
/// its `__await__` (parking the current taskforce slot while pending);
/// everything else (`Arg`, a coroutine, a plain Rust future) through
/// `IntoFuture`.
pub trait PyAwait {
    fn __laila_await(self) -> BoxFut;
}
impl<F> PyAwait for F
where
    F: core::future::IntoFuture<Output = Result<Arg>>,
    F::IntoFuture: Send + 'static,
{
    fn __laila_await(self) -> BoxFut {
        Box::pin(self.into_future())
    }
}

/// `await x` for an `Arg` held by reference (the rewriter's `(x).await`
/// on a bound name clones the handle).
pub fn await_arg(x: &Arg) -> BoxFut {
    core::future::IntoFuture::into_future(x.clone())
}

/// `asyncio.gather(*aws)` -> the list of results (first exception wins
/// after every awaitable settled).
pub async fn gather(aws: Vec<BoxFut>) -> Result<Arg> {
    let mut slots: Vec<(BoxFut, Option<Result<Arg>>)> =
        aws.into_iter().map(|f| (f, None)).collect();
    core::future::poll_fn(|cx| {
        let mut all = true;
        for (f, out) in slots.iter_mut() {
            if out.is_some() {
                continue;
            }
            match f.as_mut().poll(cx) {
                Poll::Ready(r) => *out = Some(r),
                Poll::Pending => all = false,
            }
        }
        if all {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    let mut values = Vec::with_capacity(slots.len());
    for (_, out) in slots {
        values.push(out.expect("settled")?);
    }
    Ok(values.into_arg())
}

/// `asyncio.to_thread(fn)` -- run *f* on a fresh thread under a copy of
/// the current context; awaitable.
#[cfg(feature = "std")]
pub fn to_thread<R>(f: impl FnOnce() -> R + Send + 'static) -> OneShot<R>
where
    R: Send + Clone + 'static,
{
    let slot = OneShot::new();
    let slot2 = slot.clone();
    let ctx = contextvars::copy_context();
    std::thread::Builder::new()
        .name("asyncio-to_thread".into())
        .spawn(move || {
            let r = ctx.run(f);
            slot2.set_result(r);
        })
        .expect("to_thread");
    slot
}

/// `import asyncio` as a value (`asyncio.sleep(0.01)`, `asyncio.run(coro)`).
#[derive(Clone, Copy, Debug, Default)]
pub struct asyncio_mod;
pub static asyncio: asyncio_mod = asyncio_mod;
impl asyncio_mod {
    /// `asyncio.sleep(delay)`.
    pub fn sleep(&self, delay: impl IntoArg) -> Sleep {
        let secs = delay.into_arg().as_f64().unwrap_or(0.0).max(0.0);
        #[cfg(not(feature = "std"))]
        let _ = secs;
        Sleep {
            #[cfg(feature = "std")]
            deadline: std::time::Instant::now() + core::time::Duration::from_secs_f64(secs),
            registered: false,
        }
    }
    /// `asyncio.run(coro)`.
    /// `asyncio.run(coro)`: drive a coroutine object (or any awaitable
    /// `Arg`) to completion on the calling thread.
    pub fn run(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let coro = a.positional.into_iter().next().ok_or_else(|| {
            LailaError::type_error("run() missing 1 required positional argument: 'main'")
        })?;
        run(core::future::IntoFuture::into_future(coro))
    }
    /// `asyncio.gather(*aws)`.
    pub fn gather(&self, aws: impl Into<Args>) -> impl Future<Output = Result<Arg>> {
        let aws: Args = aws.into();
        let futs: Vec<BoxFut> = aws
            .positional
            .into_iter()
            .map(core::future::IntoFuture::into_future)
            .collect();
        gather(futs)
    }
    /// `asyncio.new_event_loop()`.
    pub fn new_event_loop(&self) -> EventLoop {
        EventLoop::new()
    }
    /// `asyncio.get_running_loop()` (`RuntimeError` when none).
    pub fn get_running_loop(&self) -> Result<EventLoop> {
        get_running_loop().ok_or_else(|| LailaError::runtime_error("no running event loop"))
    }
    /// `asyncio.iscoroutine(x)`.
    pub fn iscoroutine(&self, x: impl PyArg) -> bool {
        Coroutine::of(&x.to_arg()).is_some()
    }
    /// `asyncio.to_thread(fn)`.
    #[cfg(feature = "std")]
    pub fn to_thread(&self, f: impl IntoArg) -> OneShot<Arg> {
        let f = f.into_arg();
        let slot = OneShot::new();
        let slot2 = slot.clone();
        let ctx = contextvars::copy_context();
        std::thread::Builder::new()
            .name("asyncio-to_thread".into())
            .spawn(move || {
                let r = ctx.run(|| f.__call__(Args::new()));
                slot2.set_result(r.unwrap_or_else(|e| Arg::Exception(Arc::new(e))));
            })
            .expect("to_thread");
        slot
    }
    /// `asyncio.CancelledError`
    #[allow(non_snake_case)]
    pub fn CancelledError(&self) -> crate::python::errors::CancelledError {
        crate::python::errors::CancelledError
    }
    /// `asyncio.TimeoutError`
    #[allow(non_snake_case)]
    pub fn TimeoutError(&self) -> crate::python::errors::TimeoutError {
        crate::python::errors::TimeoutError
    }
}
