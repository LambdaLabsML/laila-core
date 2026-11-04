//! Exceptions and runtime guards for the central command system.
//!
//! - `LoopBlockingWaitError` -- raised when `Future.wait()` is called from
//!   a thread that is currently driving an async taskforce event loop.
//! - `NestedCommandSubmitError` -- raised when a function decorated
//!   `@no_command_submit` synchronously invokes `Command.submit`.
//! - [`no_command_submit`] -- the decorator, as a scoped runner.
//! - `_ASYNC_LOOP_THREAD_IDS` -- the thread ids that own an async loop.
//! - `_NO_SUBMIT_OWNER` -- the `ContextVar` carrying the qualname of the
//!   innermost `@no_command_submit` function currently executing.

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use crate::python::stdlib::contextvars::ContextVar;
use crate::python::sync::Mutex;
use crate::python::{Arg, Args, ExcKind, LailaError, Result};

pub use crate::python::errors::{LoopBlockingWaitError, NestedCommandSubmitError};

static _ASYNC_LOOP_THREAD_IDS: Mutex<Vec<i64>> = Mutex::new(Vec::new());

/// Mark *thread_ident* as owning an async event loop.
pub fn _register_async_loop_thread(thread_ident: i64) {
    let mut g = _ASYNC_LOOP_THREAD_IDS.lock();
    if !g.contains(&thread_ident) {
        g.push(thread_ident);
    }
}

/// Drop *thread_ident* from the async-loop registry.
pub fn _unregister_async_loop_thread(thread_ident: i64) {
    _ASYNC_LOOP_THREAD_IDS.lock().retain(|t| *t != thread_ident);
}

/// The calling thread's `threading.get_ident()`.
pub fn current_thread_ident() -> i64 {
    #[cfg(feature = "std")]
    {
        crate::python::stdlib::threading.get_ident()
    }
    #[cfg(not(feature = "std"))]
    {
        0
    }
}

/// Raise `LoopBlockingWaitError` if the current thread owns a loop.
pub fn _check_not_loop_thread() -> Result<()> {
    if _ASYNC_LOOP_THREAD_IDS
        .lock()
        .contains(&current_thread_ident())
    {
        return Err(LailaError::new(
            ExcKind::LoopBlockingWaitError,
            "Future.wait() called from inside an async taskforce loop thread. \
             Use `await fut` instead of `.wait()` from coroutines.",
        ));
    }
    Ok(())
}

pub static _NO_SUBMIT_OWNER: ContextVar<Option<String>> =
    ContextVar::new("_no_submit_owner", || None);

/// `@no_command_submit`: run *f* with the context var set to *qualname*
/// so `Command.submit` raises `NestedCommandSubmitError` inside it.
pub fn no_command_submit<R>(qualname: &str, f: impl FnOnce() -> R) -> R {
    _NO_SUBMIT_OWNER.scoped(Some(qualname.into()), f)
}

/// `@no_command_submit` on a Python callable value.
pub fn no_command_submit_fn(fn_: Arg, qualname: String) -> Arg {
    Arg::Callable(alloc::sync::Arc::new(move |a: Args| -> Result<Arg> {
        let out = _NO_SUBMIT_OWNER.scoped(Some(qualname.clone()), || fn_.__call__(a))?;
        // An `async def`: the body runs when the coroutine is awaited, so
        // the context var is set around every poll (Python's `_async_wrap`
        // holds it across the `await`).
        if let Some(co) = crate::python::stdlib::asyncio::Coroutine::of(&out) {
            let mut inner = co.take()?;
            let owner = qualname.clone();
            return Ok(crate::python::stdlib::asyncio::coroutine(
                core::future::poll_fn(move |cx| {
                    _NO_SUBMIT_OWNER.scoped(Some(owner.clone()), || inner.as_mut().poll(cx))
                }),
            ));
        }
        Ok(out)
    }))
}

/// Raise `NestedCommandSubmitError` if `@no_command_submit` is active.
pub fn _check_no_pending_submit_owner() -> Result<()> {
    if let Some(owner) = _NO_SUBMIT_OWNER.get() {
        return Err(LailaError::new(
            ExcKind::NestedCommandSubmitError,
            alloc::format!(
                "`{owner}` is decorated `@no_command_submit` but called \
                 `Command.submit`. Build functions must not nest command \
                 submissions; use `await` on a future passed in from outside \
                 the build, or restructure to submit from the orchestrator."
            ),
        ));
    }
    Ok(())
}

/// `ensure_coroutine_function(task)`: a coroutine function for *task*.
///
/// Coroutine functions (`coroutine_function(..)` values) are returned
/// unchanged. Plain callables are wrapped in an `async def` shim that
/// offloads the body to the owning taskforce's sync executor
/// (`PythonAsyncThreadPoolTaskForce.run_sync`) -- or `asyncio.to_thread`
/// when not running inside a taskforce slot -- so a CPU-bound or blocking
/// body never stalls the event loop. If the body itself returns a
/// coroutine the shim awaits it on the loop.
pub fn ensure_coroutine_function(task: Arg) -> Arg {
    if crate::python::stdlib::asyncio::iscoroutinefunction(&task) {
        return task;
    }
    // `functools.wraps(task)`: the wrapper keeps the wrapped name.
    let name: String = crate::python::object::Function::of(&task)
        .map(|f| f.name.to_string())
        .unwrap_or_else(|| String::from("_wrap"));
    crate::python::stdlib::asyncio::coroutine_function(&name, move |a: Args| {
        let task = task.clone();
        async move {
            let slot = super::parking::_CURRENT_SLOT.get();
            let t2 = task.clone();
            let body: alloc::boxed::Box<dyn FnOnce() -> Result<Arg> + Send> =
                alloc::boxed::Box::new(move || t2.__call__(a));
            let out = match slot {
                Some(s) => s.tf.run_sync(body)?.await??,
                None => offload(body).await??,
            };
            if crate::python::stdlib::asyncio::Coroutine::of(&out).is_some() {
                return out.await;
            }
            Ok(out)
        }
    })
}

/// `asyncio.to_thread(body)` (inline without threads).
fn offload(
    body: alloc::boxed::Box<dyn FnOnce() -> Result<Arg> + Send>,
) -> crate::python::stdlib::asyncio::OneShot<Result<Arg>> {
    #[cfg(feature = "std")]
    {
        crate::python::stdlib::asyncio::to_thread(body)
    }
    #[cfg(not(feature = "std"))]
    {
        let slot = crate::python::stdlib::asyncio::OneShot::new();
        slot.set_result(body());
        slot
    }
}
