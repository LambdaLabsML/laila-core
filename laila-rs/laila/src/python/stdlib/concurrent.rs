//! `concurrent.futures` -- the native `Future` the taskforces complete.
//!
//! `concurrent.futures.Future` is the thread-safe one-shot cell a worker
//! settles (`set_result` / `set_exception` / `cancel`) and a caller blocks
//! on (`result(timeout)`), with done-callbacks that fire on the settling
//! thread. It is also awaitable here (`asyncio.wrap_future` is implicit):
//! the Rust `Future` impl registers the task's waker.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::task::Waker;

use crate::python::object::PyObject;
use crate::python::{Arg, Args, ExcKind, IntoArg, LailaError, PyArg, Result};

type DoneCallback = Arc<dyn Fn(&NativeFuture) + Send + Sync>;

struct NativeState {
    outcome: Option<core::result::Result<Arg, Arc<LailaError>>>,
    cancelled: bool,
    running: bool,
    callbacks: Vec<DoneCallback>,
    wakers: Vec<Waker>,
}

struct NativeInner {
    state: parking_lot::Mutex<NativeState>,
    cv: parking_lot::Condvar,
}

/// `concurrent.futures.Future()` (handle; clones alias one future).
#[derive(Clone)]
pub struct NativeFuture(Arc<NativeInner>);

impl Default for NativeFuture {
    fn default() -> Self {
        Self::new(Args::new())
    }
}

impl NativeFuture {
    /// `concurrent.futures.Future()` (no arguments).
    pub fn new(_args: impl Into<Args>) -> NativeFuture {
        NativeFuture(Arc::new(NativeInner {
            state: parking_lot::Mutex::new(NativeState {
                outcome: None,
                cancelled: false,
                running: false,
                callbacks: Vec::new(),
                wakers: Vec::new(),
            }),
            cv: parking_lot::Condvar::new(),
        }))
    }
    pub fn is(&self, other: &NativeFuture) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    fn settle(&self, f: impl FnOnce(&mut NativeState) -> bool) -> bool {
        let (cbs, wakers) = {
            let mut g = self.0.state.lock();
            if g.outcome.is_some() || g.cancelled {
                return false;
            }
            if !f(&mut g) {
                return false;
            }
            (
                core::mem::take(&mut g.callbacks),
                core::mem::take(&mut g.wakers),
            )
        };
        self.0.cv.notify_all();
        for cb in cbs {
            cb(self);
        }
        wakers.into_iter().for_each(Waker::wake);
        true
    }
    /// `fut.set_result(value)`.
    pub fn set_result(&self, value: impl IntoArg) {
        let v = value.into_arg();
        self.settle(|s| {
            s.outcome = Some(Ok(v));
            true
        });
    }
    /// `fut.set_exception(exc)`.
    pub fn set_exception(&self, exc: impl Into<Arc<LailaError>>) {
        let e = exc.into();
        self.settle(|s| {
            s.outcome = Some(Err(e));
            true
        });
    }
    /// `fut.cancel()` -> `True` if the future was cancelled by this call
    /// (`False` once it has a result or is running).
    pub fn cancel(&self) -> bool {
        self.settle(|s| {
            if s.running {
                return false;
            }
            s.cancelled = true;
            true
        })
    }
    /// `fut.set_running_or_notify_cancel()`.
    pub fn set_running_or_notify_cancel(&self) -> bool {
        let mut g = self.0.state.lock();
        if g.cancelled {
            return false;
        }
        g.running = true;
        true
    }
    pub fn cancelled(&self) -> bool {
        self.0.state.lock().cancelled
    }
    pub fn done(&self) -> bool {
        let g = self.0.state.lock();
        g.cancelled || g.outcome.is_some()
    }
    pub fn running(&self) -> bool {
        let g = self.0.state.lock();
        g.running && !g.cancelled && g.outcome.is_none()
    }
    /// `fut.exception(timeout=None)` -> the stored exception (`None` on
    /// success); the same `TimeoutError` / `CancelledError` as `result`.
    pub fn exception(&self) -> Option<Arc<LailaError>> {
        match &self.0.state.lock().outcome {
            Some(Err(e)) => Some(e.clone()),
            _ => None,
        }
    }
    /// `fut.exception()` as a Python value (`None` when there is none).
    pub fn exception_arg(&self) -> Arg {
        match self.exception() {
            Some(e) => Arg::Exception(e),
            None => Arg::None,
        }
    }
    fn outcome_of(s: &NativeState) -> Option<Result<Arg>> {
        if s.cancelled {
            return Some(Err(LailaError::new(ExcKind::CancelledError, String::new())));
        }
        s.outcome.as_ref().map(|o| match o {
            Ok(v) => Ok(v.clone()),
            Err(e) => Err((**e).clone()),
        })
    }
    /// `fut.result(timeout=None)`: block until settled; raises the stored
    /// exception, `CancelledError` when cancelled, `TimeoutError` when
    /// *timeout* seconds elapse first.
    pub fn result(&self, timeout: Option<f64>) -> Result<Arg> {
        let mut g = self.0.state.lock();
        if let Some(o) = Self::outcome_of(&g) {
            return o;
        }
        match timeout {
            None => {
                while Self::outcome_of(&g).is_none() {
                    self.0.cv.wait(&mut g);
                }
            }
            Some(t) => {
                let deadline =
                    std::time::Instant::now() + core::time::Duration::from_secs_f64(t.max(0.0));
                while Self::outcome_of(&g).is_none() {
                    let now = std::time::Instant::now();
                    if now >= deadline {
                        return Err(LailaError::new(ExcKind::TimeoutError, String::new()));
                    }
                    self.0.cv.wait_for(&mut g, deadline - now);
                }
            }
        }
        Self::outcome_of(&g).expect("settled")
    }
    /// `fut.add_done_callback(fn)`: runs *fn(fut)* once settled (at once
    /// if it already is), on the settling thread.
    pub fn add_done_callback(&self, cb: impl Fn(&NativeFuture) + Send + Sync + 'static) {
        let cb: DoneCallback = Arc::new(cb);
        {
            let mut g = self.0.state.lock();
            if !(g.cancelled || g.outcome.is_some()) {
                g.callbacks.push(cb);
                return;
            }
        }
        cb(self);
    }
    /// The settled outcome without blocking (`None` while pending).
    pub fn peek(&self) -> Option<Result<Arg>> {
        Self::outcome_of(&self.0.state.lock())
    }
    /// `fut.<name>(*args)` for the dynamic layer.
    pub fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let first = args.positional.first().cloned().unwrap_or(Arg::None);
        Ok(match name {
            "set_result" => {
                self.set_result(first);
                Arg::None
            }
            "set_exception" => {
                match first {
                    Arg::Exception(e) => self.set_exception(e),
                    other => {
                        return Err(LailaError::type_error(alloc::format!(
                            "set_exception() argument must be an exception, not {}",
                            other.type_name()
                        )))
                    }
                }
                Arg::None
            }
            "cancel" => Arg::Bool(self.cancel()),
            "cancelled" => Arg::Bool(self.cancelled()),
            "done" => Arg::Bool(self.done()),
            "running" => Arg::Bool(self.running()),
            "exception" => self.exception_arg(),
            "result" => {
                let t = args.get("timeout");
                let t = if t.is_none() { first } else { t };
                return self.result(t.as_f64());
            }
            "set_running_or_notify_cancel" => Arg::Bool(self.set_running_or_notify_cancel()),
            "add_done_callback" => {
                let f = first;
                self.add_done_callback(move |nf| {
                    let _ = f.__call__(Args::from(nf.clone().into_arg()));
                });
                Arg::None
            }
            other => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'Future' object has no attribute '{other}'"
                )))
            }
        })
    }
}

impl core::future::Future for NativeFuture {
    type Output = Result<Arg>;
    fn poll(
        self: core::pin::Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Self::Output> {
        let mut g = self.0.state.lock();
        if let Some(o) = Self::outcome_of(&g) {
            return core::task::Poll::Ready(o);
        }
        if !g.wakers.iter().any(|w| w.will_wake(cx.waker())) {
            g.wakers.push(cx.waker().clone());
        }
        core::task::Poll::Pending
    }
}

impl core::fmt::Debug for NativeFuture {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&PyObject::__repr__(self))
    }
}
impl PyObject for NativeFuture {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    /// `a is b`: identity of the shared handle, not of the boxing.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<NativeFuture>()
            .is_some_and(|o| self.is(o))
    }
    fn type_name(&self) -> &'static str {
        "Future"
    }
    fn __repr__(&self) -> String {
        let state = {
            let g = self.0.state.lock();
            if g.cancelled {
                "cancelled"
            } else if g.outcome.is_some() {
                "finished"
            } else if g.running {
                "running"
            } else {
                "pending"
            }
        };
        alloc::format!(
            "<Future at {:#x} state={state}>",
            Arc::as_ptr(&self.0) as usize
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<NativeFuture>()
            .is_some_and(|o| self.is(o))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        NativeFuture::__call_method__(self, name, args)
    }
}
impl IntoArg for NativeFuture {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for NativeFuture {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl crate::python::builtins::IsInstance for NativeFuture {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<NativeFuture>())
    }
}

/// `concurrent.futures` as a module value (`concurrent.futures.Future()`).
#[derive(Clone, Copy, Debug, Default)]
pub struct futures_mod;
impl futures_mod {
    #[allow(non_snake_case)]
    pub fn Future(&self) -> NativeFuture {
        NativeFuture::default()
    }
    #[allow(non_snake_case)]
    pub fn TimeoutError(&self) -> crate::python::errors::TimeoutError {
        crate::python::errors::TimeoutError
    }
}
/// `import concurrent`
#[derive(Clone, Copy, Debug, Default)]
pub struct concurrent_mod;
pub static concurrent: concurrent_mod = concurrent_mod;
impl concurrent_mod {
    pub fn futures(&self) -> futures_mod {
        futures_mod
    }
}

// ---------------------------------------------------------------------------
// ThreadPoolExecutor

#[cfg(feature = "std")]
struct PoolState {
    /// Workers currently running a submitted callable.
    active: usize,
    /// Join handles of every worker started so far (joined on `shutdown`).
    handles: Vec<std::thread::JoinHandle<()>>,
    shut: bool,
}

#[cfg(feature = "std")]
struct PoolInner {
    max_workers: usize,
    state: parking_lot::Mutex<PoolState>,
    cv: parking_lot::Condvar,
}

/// `concurrent.futures.ThreadPoolExecutor(max_workers=None)`.
///
/// Each `submit` runs the callable on a worker thread (at most
/// `max_workers` at a time) and settles a [`NativeFuture`]. `shutdown(wait=True)`
/// -- also the `with` exit -- joins every worker.
#[cfg(feature = "std")]
#[derive(Clone)]
pub struct ThreadPoolExecutor(Arc<PoolInner>);

#[cfg(feature = "std")]
impl ThreadPoolExecutor {
    /// `ThreadPoolExecutor(max_workers=None, thread_name_prefix="")`.
    pub fn new(args: impl Into<Args>) -> Result<ThreadPoolExecutor> {
        let a: Args = args.into();
        let mw = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("max_workers"));
        let max_workers = match mw.as_i64() {
            Some(n) if n > 0 => n as usize,
            Some(_) => {
                return Err(LailaError::value_error(
                    "max_workers must be greater than 0",
                ))
            }
            None => {
                let cpus = std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(1);
                core::cmp::min(32, cpus + 4)
            }
        };
        Ok(ThreadPoolExecutor(Arc::new(PoolInner {
            max_workers,
            state: parking_lot::Mutex::new(PoolState {
                active: 0,
                handles: Vec::new(),
                shut: false,
            }),
            cv: parking_lot::Condvar::new(),
        })))
    }
    /// `pool.submit(fn, *args, **kwargs)` -> `Future`.
    pub fn submit(&self, args: impl Into<Args>) -> Result<NativeFuture> {
        let mut a: Args = args.into();
        if a.positional.is_empty() {
            return Err(LailaError::type_error(
                "submit expected at least 1 positional argument",
            ));
        }
        let f = a.positional.remove(0);
        let fut = NativeFuture::default();
        let inner = Arc::clone(&self.0);
        {
            let mut g = self.0.state.lock();
            if g.shut {
                return Err(LailaError::runtime_error(
                    "cannot schedule new futures after shutdown",
                ));
            }
            while g.active >= inner.max_workers {
                self.0.cv.wait(&mut g);
            }
            g.active += 1;
            let fut2 = fut.clone();
            let handle = std::thread::Builder::new()
                .name(String::from("ThreadPoolExecutor"))
                .spawn(move || {
                    if fut2.set_running_or_notify_cancel() {
                        match f.__call__(a) {
                            Ok(v) => fut2.set_result(v),
                            Err(e) => fut2.set_exception(e),
                        }
                    }
                    let mut g = inner.state.lock();
                    g.active -= 1;
                    inner.cv.notify_all();
                })
                .map_err(|e| {
                    LailaError::runtime_error(alloc::format!("can't start new thread: {e}"))
                })?;
            g.handles.push(handle);
        }
        Ok(fut)
    }
    /// `pool.map(fn, *iterables)` -> list of results (eager, in order).
    pub fn map(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let f = a.positional.first().cloned().unwrap_or(Arg::None);
        let iters: Vec<Vec<Arg>> = a.positional[1..]
            .iter()
            .map(|it| it.__iter__())
            .collect::<Result<_>>()?;
        let n = iters.iter().map(Vec::len).min().unwrap_or(0);
        let mut futs = Vec::with_capacity(n);
        for i in 0..n {
            let mut call = Args::new();
            call.push_positional(f.clone());
            for it in &iters {
                call.push_positional(it[i].clone());
            }
            futs.push(self.submit(call)?);
        }
        let mut out = Vec::with_capacity(n);
        for fu in futs {
            out.push(fu.result(None)?);
        }
        Ok(out.into_arg())
    }
    /// `pool.shutdown(wait=True)`.
    pub fn shutdown(&self, args: impl Into<Args>) -> Result<()> {
        let a: Args = args.into();
        let wait = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("wait"));
        let wait = !matches!(wait, Arg::Bool(false));
        let handles = {
            let mut g = self.0.state.lock();
            g.shut = true;
            core::mem::take(&mut g.handles)
        };
        if wait {
            let me = std::thread::current().id();
            for h in handles {
                if h.thread().id() != me {
                    let _ = h.join();
                }
            }
        }
        Ok(())
    }
    /// `with ThreadPoolExecutor(..) as pool:` binds the executor itself.
    pub fn __enter__(&self) -> Result<ThreadPoolExecutor> {
        Ok(self.clone())
    }
    /// `__exit__`: `shutdown(wait=True)`.
    pub fn __exit__(&self, _args: impl Into<Args>) -> Result<bool> {
        self.shutdown(Args::new())?;
        Ok(false)
    }
}

#[cfg(feature = "std")]
impl crate::python::context::RaisingExit for ThreadPoolExecutor {
    fn __laila_exit(&self) -> Result<()> {
        self.shutdown(Args::new())
    }
}

#[cfg(feature = "std")]
impl core::fmt::Debug for ThreadPoolExecutor {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "<ThreadPoolExecutor max_workers={}>", self.0.max_workers)
    }
}

#[cfg(feature = "std")]
impl PyObject for ThreadPoolExecutor {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "ThreadPoolExecutor"
    }
    fn __repr__(&self) -> String {
        alloc::format!("{self:?}")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<ThreadPoolExecutor>()
            .is_some_and(|o| Arc::ptr_eq(&self.0, &o.0))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "submit" => Ok(self.submit(args)?.into_arg()),
            "map" => self.map(args),
            "shutdown" => self.shutdown(args).map(|_| Arg::None),
            "__enter__" => Ok(self.clone().into_arg()),
            "__exit__" => self.__exit__(args).map(Arg::Bool),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'ThreadPoolExecutor' object has no attribute '{other}'"
            ))),
        }
    }
}
#[cfg(feature = "std")]
impl IntoArg for ThreadPoolExecutor {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
#[cfg(feature = "std")]
impl PyArg for ThreadPoolExecutor {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
#[cfg(feature = "std")]
impl crate::python::builtins::IsInstance for ThreadPoolExecutor {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<ThreadPoolExecutor>())
    }
}

#[cfg(feature = "std")]
impl futures_mod {
    /// `concurrent.futures.ThreadPoolExecutor(..)`
    #[allow(non_snake_case)]
    pub fn ThreadPoolExecutor(&self, args: impl Into<Args>) -> Result<ThreadPoolExecutor> {
        ThreadPoolExecutor::new(args)
    }
}
