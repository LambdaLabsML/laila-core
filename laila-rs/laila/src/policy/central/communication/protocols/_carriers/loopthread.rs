//! Shared event-loop-thread lifecycle for transports.
//!
//! Python's wire-backed transports each own a private asyncio loop on a
//! daemon thread; `start_loop_thread` boots it (blocking until the
//! transport's `_async_start(ready)` sets *ready* or raises) and
//! `stop_loop_thread` runs the shutdown coroutine, stops the loop, joins
//! the thread and **closes the loop** so nothing leaks per lifecycle.
//!
//! The Rust carriers do their socket I/O on plain blocking threads, so
//! they do not need a loop to move bytes; the helpers exist for API
//! parity (a transport subclass written against the Python contract, and
//! the teardown tests) and run on the `asyncio` shim's [`EventLoop`].

use alloc::string::String;
use alloc::sync::Arc;

use crate::python::object::PyObject;
use crate::python::stdlib::asyncio::{self, EventLoop};
use crate::python::stdlib::{Event, Thread};
use crate::python::sync::Mutex;
use crate::python::{Arg, Args, IntoArg, LailaError, PyArg, Result};

/// `log = logging.getLogger(__name__)` -- the module logger.
pub fn log() -> crate::python::stdlib::logging::Logger {
    crate::python::stdlib::logging::Logger::get(
        "laila.policy.central.communication.protocols._carriers.loopthread",
    )
}

/// An [`EventLoop`] as a Python value (`proto._event_loop`).
#[derive(Clone)]
pub struct LoopHandle(pub EventLoop);

impl LoopHandle {
    pub fn of(x: &Arg) -> Option<LoopHandle> {
        match x {
            Arg::Object(o) => o.as_any().downcast_ref::<LoopHandle>().cloned(),
            _ => None,
        }
    }
}
impl core::fmt::Debug for LoopHandle {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "<EventLoop running={} closed={}>",
            self.0.is_running(),
            self.0.is_closed()
        )
    }
}
impl PyObject for LoopHandle {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "EventLoop"
    }
    fn __repr__(&self) -> String {
        alloc::format!("{self:?}")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<LoopHandle>()
            .is_some_and(|o| o.0.is(&self.0))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(self as *const _ as usize as i64)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "is_running" => Ok(self.0.is_running().into_arg()),
            "is_closed" => Ok(self.0.is_closed().into_arg()),
            "stop" => {
                self.0.stop();
                Ok(Arg::None)
            }
            "close" => {
                self.0.close();
                Ok(Arg::None)
            }
            "call_soon_threadsafe" => {
                let f = args.positional.first().cloned().unwrap_or(Arg::None);
                let rest: alloc::vec::Vec<Arg> = args.positional.iter().skip(1).cloned().collect();
                self.0.call_soon_threadsafe(move || {
                    let _ = f.__call__(Args {
                        positional: rest,
                        keywords: alloc::vec::Vec::new(),
                    });
                });
                Ok(Arg::None)
            }
            other => Err(LailaError::attribute_error(alloc::format!(
                "'EventLoop' object has no attribute '{other}'"
            ))),
        }
    }
}
impl IntoArg for LoopHandle {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for LoopHandle {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}

fn type_name_of(proto: &Arg) -> String {
    match proto.__getattr__("__class__") {
        Ok(c) => c
            .__getattr__("__name__")
            .map(|n| n.__str__())
            .unwrap_or_else(|_| c.__str__()),
        Err(_) => String::from(proto.type_name()),
    }
}

/// `start_loop_thread(proto, async_start, *, ready_timeout)`: boot
/// *proto*'s private loop thread and block until it is ready. Sets
/// `proto._event_loop` / `proto._loop_thread`; re-raises the boot error
/// (with the loop already closed) when *async_start* failed.
pub fn start_loop_thread(proto: &Arg, async_start: Arg, ready_timeout: f64) -> Result<()> {
    let r#loop = EventLoop::new();
    proto.__setattr__("_event_loop", LoopHandle(r#loop.clone()))?;
    let ready = Event::new(Args::new());
    let boot: Arc<Mutex<Option<LailaError>>> = Arc::new(Mutex::new(None));

    let (l2, ready2, boot2) = (r#loop.clone(), ready.clone(), boot.clone());
    let run_loop: Arg = crate::python::callable(move |_a: Args| -> Result<Arg> {
        let coro = async_start.__call__(Args::from(ready2.clone().into_arg()));
        let outcome = match coro {
            Ok(c) => l2.run_until_complete(asyncio::await_arg(&c)),
            Err(e) => Err(e),
        };
        if let Err(e) = outcome {
            *boot2.lock() = Some(e);
            ready2.set(Args::new());
            return Ok(Arg::None);
        }
        l2.run_forever();
        Ok(Arg::None)
    });
    let mut targs = Args::new();
    targs.push_keyword("target", run_loop);
    targs.push_keyword("daemon", true);
    targs.push_keyword("name", alloc::format!("{}-loop", type_name_of(proto)));
    let thread = Thread::new(targs)?;
    proto.__setattr__("_loop_thread", thread.to_arg())?;
    thread.start(Args::new())?;
    let mut w = Args::new();
    w.push_keyword("timeout", ready_timeout);
    let _ = ready.wait(w)?;
    let err = boot.lock().take();
    if let Some(e) = err {
        let mut j = Args::new();
        j.push_keyword("timeout", ready_timeout);
        let _ = thread.join(j);
        proto.__setattr__("_loop_thread", Arg::None)?;
        close_loop(proto)?;
        return Err(e);
    }
    Ok(())
}

/// `stop_loop_thread(proto, shutdown, *, timeout=5.0)`: run *shutdown* on
/// the loop, stop it, join the thread, close the loop. Idempotent.
pub fn stop_loop_thread(proto: &Arg, shutdown: Arg, timeout: f64) -> Result<()> {
    let r#loop = LoopHandle::of(&proto.__getattr__("_event_loop").unwrap_or(Arg::None));
    let thread = proto.__getattr__("_loop_thread").unwrap_or(Arg::None);
    if let Some(LoopHandle(l)) = r#loop {
        if !l.is_closed() && l.is_running() {
            if !shutdown.is_none() {
                if let Ok(coro) = shutdown.__call__(Args::new()) {
                    let fut = l.run_coroutine_threadsafe(asyncio::await_arg(&coro));
                    let _ = fut.result(Some(timeout));
                }
            }
            let l2 = l.clone();
            l.call_soon_threadsafe(move || l2.stop());
        }
    }
    if let Arg::Object(o) = &thread {
        if let Some(t) = o.as_any().downcast_ref::<Thread>() {
            let mut j = Args::new();
            j.push_keyword("timeout", timeout);
            let _ = t.join(j);
        }
    }
    proto.__setattr__("_loop_thread", Arg::None)?;
    close_loop(proto)
}

/// `close_loop(proto)`: close the loop if its thread has exited; always
/// drop the reference.
pub fn close_loop(proto: &Arg) -> Result<()> {
    let r#loop = LoopHandle::of(&proto.__getattr__("_event_loop").unwrap_or(Arg::None));
    proto.__setattr__("_event_loop", Arg::None)?;
    let Some(LoopHandle(l)) = r#loop else {
        return Ok(());
    };
    if l.is_closed() {
        return Ok(());
    }
    if l.is_running() {
        let _ = crate::python::stdlib::logging::Logger::get(__name__).warning(
            alloc::format!(
                "{} event loop still running at close; leaking it",
                type_name_of(proto)
            ),
            alloc::vec::Vec::new(),
        );
        return Ok(());
    }
    l.close();
    Ok(())
}

/// `await cancel_pending_tasks()`: cancel every other task on the running
/// loop and let them observe the cancellation.
pub async fn cancel_pending_tasks_async() -> Result<Arg> {
    // The shim's tasks are cancelled by dropping their futures when the
    // loop closes; here we just yield once so finally-blocks of tasks
    // that already completed get to run.
    let _ = asyncio::asyncio.sleep(0.0).await;
    Ok(Arg::None)
}

/// `cancel_pending_tasks` as an `async def` value.
pub fn cancel_pending_tasks() -> Arg {
    asyncio::coroutine(cancel_pending_tasks_async())
}

/// `__name__` of the module.
pub const __name__: &str = "laila.policy.central.communication.protocols._carriers.loopthread";

// ---------------------------------------------------------------------------
// module object

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct loopthread_mod;
pub static loopthread: loopthread_mod = loopthread_mod;

impl loopthread_mod {
    pub fn start_loop_thread(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let proto = a.positional.first().cloned().unwrap_or(Arg::None);
        let async_start = a
            .positional
            .get(1)
            .cloned()
            .unwrap_or_else(|| a.get("async_start"));
        let ready_timeout = a.get("ready_timeout").as_f64().unwrap_or(10.0);
        start_loop_thread(&proto, async_start, ready_timeout).map(|()| Arg::None)
    }
    pub fn stop_loop_thread(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let proto = a.positional.first().cloned().unwrap_or(Arg::None);
        let shutdown = a
            .positional
            .get(1)
            .cloned()
            .unwrap_or_else(|| a.get("shutdown"));
        let timeout = a.get("timeout").as_f64().unwrap_or(5.0);
        stop_loop_thread(&proto, shutdown, timeout).map(|()| Arg::None)
    }
    pub fn close_loop(&self, proto: impl IntoArg) -> Result<Arg> {
        close_loop(&proto.into_arg()).map(|()| Arg::None)
    }
    /// `loopthread.cancel_pending_tasks` -- the `async def` as a value.
    pub fn cancel_pending_tasks(&self) -> Arg {
        asyncio::coroutine_function("cancel_pending_tasks", |_a: Args| {
            cancel_pending_tasks_async()
        })
    }
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "cancel_pending_tasks" => Ok(self.cancel_pending_tasks()),
            other => Err(LailaError::attribute_error(alloc::format!(
                "module 'loopthread' has no attribute '{other}'"
            ))),
        }
    }
}
