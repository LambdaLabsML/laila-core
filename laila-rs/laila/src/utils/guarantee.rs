//! Guarantee context managers that block until tracked futures complete.
//!
//! The "guarantee" pattern is laila's lexical answer to "I want to be sure
//! every async operation kicked off in *this block* has finished before the
//! block returns". Two mirror implementations live here:
//!
//! - [`_Guarantee`] -- synchronous version. Use as `with laila.guarantee:`.
//!   On exit, every future created *inside* the block is awaited via
//!   `Future.wait`. The first exception observed is re-raised so the caller
//!   never silently outlives a failed background task.
//! - [`_AsyncGuarantee`] -- asynchronous version. Use as
//!   `async with laila.guarantee_async:`. Same idea, but with `await`
//!   instead of `wait`, plus a background watcher task that *immediately*
//!   cancels the parent task if any in-scope future errors.
//!
//! Both co-operate with `_LAILA_IDENTIFIABLE_CENTRAL_COMMAND`'s
//! `_guarantee_enter` / `_guarantee_exit` helpers, which keep a thread-local
//! stack of "active scopes" and register every newly-submitted future with
//! whichever scope is on top.
//!
//! Module-level singletons [`guarantee`] and [`guarantee_async`] are the
//! user-facing handles.
//!
//! Rust shape: `with` lowers to `__enter__` + a guard (P27). The guard's
//! `Drop` runs `__exit__` when the body raised (Python returns `False`
//! there, so the body's exception wins and the wait errors are dropped);
//! on the normal path the rewriter calls the guard's raising exit
//! ([`RaisingExit`] / [`RaisingAexit`]) so the first wait error propagates.

use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicBool, Ordering};

use crate::policy::central::command::schema::base::Command;
use crate::policy::central::command::Future;
use crate::python::context::{ExitFut, RaisingAexit, RaisingExit};
use crate::python::stdlib::{asyncio, contextvars};
use crate::python::{Arg, Args, LailaError, Result};

/// Synchronous context manager that waits for futures created in its scope.
///
/// Pushes a new guarantee scope on the active policy's command stack on
/// entry; on exit, blocks until every future registered while the scope was
/// active has terminated. If any of those futures errored, the first
/// observed exception is re-raised after the wait completes -- but only if
/// the body of the `with` block did not itself raise.
#[derive(Clone, Copy, Debug, Default)]
pub struct _Guarantee;

/// `with laila.guarantee:`
pub static guarantee: _Guarantee = _Guarantee;

impl _Guarantee {
    /// Push a fresh frame onto the active policy's guarantee stack.
    pub fn __enter__(&self) -> Result<GuaranteeGuard> {
        let command = crate::policy::get_active_policy().central().command();
        command._guarantee_enter();
        Ok(GuaranteeGuard {
            command,
            exited: AtomicBool::new(false),
        })
    }
    /// `__exit__(exc_type, exc, tb)` on the bare singleton (no open frame
    /// of its own): pops the top frame and waits, raising the first error.
    pub fn __exit__(&self) -> Result<()> {
        let command = crate::policy::get_active_policy().central().command();
        wait_all(command._guarantee_exit())
    }
}

/// The open `with laila.guarantee:` frame.
pub struct GuaranteeGuard {
    command: Command,
    exited: AtomicBool,
}

/// `laila.guarantee` as a Python value: `__enter__` returns `self` in
/// Python, so the open frame and the singleton are the same object
/// (`g is laila.guarantee`).
#[derive(Debug)]
struct GuaranteeObject;
impl crate::python::PyObject for GuaranteeObject {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "_Guarantee"
    }
    fn __repr__(&self) -> alloc::string::String {
        "<laila.utils.guarantee._Guarantee object>".into()
    }
    fn __eq__(&self, other: &dyn crate::python::PyObject) -> bool {
        other.as_any().is::<GuaranteeObject>()
    }
    fn __is__(&self, other: &dyn crate::python::PyObject) -> bool {
        other.as_any().is::<GuaranteeObject>()
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(0x6775_6172)
    }
}
impl crate::python::PyArg for _Guarantee {
    fn to_arg(&self) -> Arg {
        Arg::Object(alloc::sync::Arc::new(GuaranteeObject))
    }
}
impl crate::python::IntoArg for _Guarantee {
    fn into_arg(self) -> Arg {
        Arg::Object(alloc::sync::Arc::new(GuaranteeObject))
    }
}
impl crate::python::PyArg for GuaranteeGuard {
    fn to_arg(&self) -> Arg {
        Arg::Object(alloc::sync::Arc::new(GuaranteeObject))
    }
}

/// `future.wait(timeout=None)` for every future; the first error.
fn wait_all(created_inside: Vec<Future>) -> Result<()> {
    let mut first: Option<LailaError> = None;
    for f in created_inside {
        if let Err(e) = f.wait(Args::new()) {
            first.get_or_insert(e);
        }
    }
    match first {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

impl GuaranteeGuard {
    /// Pop the frame and synchronously wait for every registered future;
    /// the first wait error is raised.
    pub fn __exit__(&self) -> Result<()> {
        if self.exited.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        wait_all(self.command._guarantee_exit())
    }
}
impl RaisingExit for GuaranteeGuard {
    fn __laila_exit(&self) -> Result<()> {
        GuaranteeGuard::__exit__(self)
    }
}
impl Drop for GuaranteeGuard {
    /// The body raised (early `?`): pop the frame and wait, but let the
    /// caller's exception propagate (`__exit__` returns `False`).
    fn drop(&mut self) {
        let _ = self.__exit__();
    }
}

/// Asynchronous context manager that awaits futures created in its scope.
///
/// Same lexical guarantee as [`_Guarantee`], but for coroutine code: on
/// exit every in-scope future is `await`ed (sync-only ones through a
/// thread hop) and the first error re-raised. Like Python, a background
/// watcher cancels the parent task the moment an in-scope future errors:
/// the body's next suspension point (`asyncio.sleep`, `to_thread`, ..)
/// resolves to that future's exception, so a hung body does not outlive
/// the failure (`test_async_guarantee_cancels_hung_body_on_error`).
#[derive(Clone, Copy, Debug, Default)]
pub struct _AsyncGuarantee;

/// `async with laila.guarantee_async:`
pub static guarantee_async: _AsyncGuarantee = _AsyncGuarantee;

impl _AsyncGuarantee {
    /// Enter the async guarantee scope and start the watcher.
    pub fn __aenter__(&self) -> impl core::future::Future<Output = Result<_AsyncGuaranteeScope>> {
        let command = crate::policy::get_active_policy().central().command();
        command._guarantee_enter();
        #[cfg(feature = "std")]
        let index = command._guarantee_depth().saturating_sub(1);
        #[cfg(feature = "std")]
        let thread = crate::policy::central::command::schema::exceptions::current_thread_ident();
        let token = asyncio::CancelToken::new();
        let reset = asyncio::CANCEL_SCOPE.set(Some(token.clone()));
        let stop = Arc::new(AtomicBool::new(false));
        #[cfg(feature = "std")]
        {
            let cmd = command.clone();
            let tok = token.clone();
            let stop = stop.clone();
            let _ = std::thread::Builder::new()
                .name("laila-guarantee-watch".into())
                .spawn(move || {
                    while !stop.load(Ordering::Acquire) {
                        for f in cmd._guarantee_scope_futures(thread, index) {
                            if f.error() {
                                let err = match f.exception() {
                                    Arg::Exception(e) => (*e).clone(),
                                    _ => LailaError::runtime_error("in-scope future failed"),
                                };
                                tok.cancel_with(err);
                                return;
                            }
                        }
                        std::thread::sleep(core::time::Duration::from_millis(10));
                    }
                });
        }
        core::future::ready(Ok(_AsyncGuaranteeScope {
            command,
            exited: AtomicBool::new(false),
            stop,
            reset: crate::python::sync::Mutex::new(Some(reset)),
        }))
    }
    /// `await laila.guarantee_async.__aexit__(exc_type, exc, tb)` on the
    /// bare singleton: `False` when no frame is open on this thread;
    /// otherwise pops the top frame and awaits its futures (the first
    /// error is raised), then `False` so the caller's exception propagates.
    pub fn __aexit__(
        &self,
        _exc_type: impl crate::python::IntoArg,
        _exc: impl crate::python::IntoArg,
        _tb: impl crate::python::IntoArg,
    ) -> impl core::future::Future<Output = Result<Arg>> + Send {
        let command = crate::policy::get_active_policy().central().command();
        async move {
            if command._guarantee_depth() == 0 {
                return Ok(Arg::Bool(false));
            }
            await_all(command._guarantee_exit()).await?;
            Ok(Arg::Bool(false))
        }
    }
}

/// The open `async with laila.guarantee_async:` frame.
#[allow(non_camel_case_types)]
pub struct _AsyncGuaranteeScope {
    command: Command,
    exited: AtomicBool,
    /// Stops the watcher thread.
    stop: Arc<AtomicBool>,
    /// Restores the enclosing task's cancellation scope.
    reset:
        crate::python::sync::Mutex<Option<contextvars::Token<Option<Arc<asyncio::CancelToken>>>>>,
}

/// `await future` for every future (a non-awaitable base future is waited
/// on a thread); the first error.
async fn await_all(created_inside: Vec<Future>) -> Result<()> {
    let mut first: Option<LailaError> = None;
    for f in created_inside {
        let r: Result<Arg> = f.__await__().await;
        if let Err(e) = r {
            first.get_or_insert(e);
        }
    }
    match first {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

impl _AsyncGuaranteeScope {
    /// Stop the watcher and leave the cancellation scope.
    fn leave(&self) {
        self.stop.store(true, Ordering::Release);
        if let Some(t) = self.reset.lock().take() {
            asyncio::CANCEL_SCOPE.reset(t);
        }
    }
    /// Exit the scope and await all enclosed futures; the first error is
    /// raised.
    pub fn __aexit__(&self) -> ExitFut<'static> {
        if self.exited.swap(true, Ordering::AcqRel) {
            return Box::pin(core::future::ready(Ok(())));
        }
        self.leave();
        let created = self.command._guarantee_exit();
        Box::pin(await_all(created))
    }
}
impl RaisingAexit for _AsyncGuaranteeScope {
    fn __laila_aexit(&self) -> ExitFut<'_> {
        _AsyncGuaranteeScope::__aexit__(self)
    }
}
impl Drop for _AsyncGuaranteeScope {
    /// The body raised: pop the frame (the futures keep running; the
    /// caller's exception propagates).
    fn drop(&mut self) {
        if !self.exited.swap(true, Ordering::AcqRel) {
            self.leave();
            let _ = self.command._guarantee_exit();
        }
    }
}
