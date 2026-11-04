//! `ConcurrentPackageFuture` -- a laila future optionally backed by a
//! `concurrent.futures.Future`, with the laila status lifecycle.
//!
//! Status lifecycle: `NOT_STARTED` -> `RUNNING` (the runner started) ->
//! `FINISHED` / `ERROR` / `CANCELLED`. When a native future is attached
//! its done-callback mirrors the outcome into the laila status; the async
//! taskforce's runner sets `result` / `exception` / `status` directly and
//! leaves the native slot empty.

use crate::python::{Arg, Args, IntoArg, LailaError, Result};

use crate::policy::central::command::schema::future::future::future::{Future, FutureKind};
use crate::policy::central::command::schema::future::future::future_status::FutureStatus;

future_class_marker!(
    ConcurrentPackageFuture,
    "ConcurrentPackageFuture",
    "laila.policy.central.command.taskforce.thread_pool_executor.future",
    FutureKind::Concurrent,
    |k| matches!(k, FutureKind::Concurrent | FutureKind::Process)
);

const TERMINAL: [FutureStatus; 3] = [
    FutureStatus::FINISHED,
    FutureStatus::ERROR,
    FutureStatus::CANCELLED,
];

impl Future {
    /// `fut.native_future` -- the underlying `concurrent.futures.Future`
    /// (`None` when the runner drives the status directly).
    pub fn native_future(&self) -> Arg {
        #[cfg(feature = "std")]
        {
            match &self.0.state.lock().native {
                Some(n) => n.clone().into_arg(),
                None => Arg::None,
            }
        }
        #[cfg(not(feature = "std"))]
        {
            Arg::None
        }
    }
    /// `fut.native_future = nf` (one-shot; `RuntimeError` on reassignment).
    pub fn set_native_future(&self, value: Arg) -> Result<()> {
        #[cfg(feature = "std")]
        {
            let nf = match &value {
                Arg::Object(o) => o
                    .as_any()
                    .downcast_ref::<crate::python::stdlib::NativeFuture>()
                    .cloned(),
                _ => None,
            };
            let Some(nf) = nf else {
                return Err(LailaError::type_error(alloc::format!(
                    "native_future must be a concurrent.futures.Future, got {}",
                    value.type_name()
                )));
            };
            {
                let mut s = self.0.state.lock();
                if s.native.is_some() {
                    return Err(LailaError::runtime_error("Native future already set."));
                }
                s.native = Some(nf.clone());
            }
            self._add_default_concurrent_future_done_callback(&nf);
            Ok(())
        }
        #[cfg(not(feature = "std"))]
        {
            let _ = value;
            Err(LailaError::not_implemented(
                "native futures need the std feature",
            ))
        }
    }
    /// Attach the done-callback that syncs the native outcome to the laila
    /// status (`CANCELLED` / `ERROR` / `FINISHED`).
    #[cfg(feature = "std")]
    fn _add_default_concurrent_future_done_callback(
        &self,
        nf: &crate::python::stdlib::NativeFuture,
    ) {
        let me = self.clone();
        nf.add_done_callback(move |n| {
            if n.cancelled() {
                me.set_result(Arg::None);
                me.set_exception(None);
                me.set_status(FutureStatus::CANCELLED);
            } else if let Some(e) = n.exception() {
                me.set_exception(Some(e));
                me.set_result(Arg::None);
                me.set_status(FutureStatus::ERROR);
            } else {
                me.set_exception(None);
                me.set_result(n.result(Some(0.0)).unwrap_or(Arg::None));
                me.set_status(FutureStatus::FINISHED);
            }
        });
    }
    /// `_subscribe_terminal(fn)`: fire *fn(self)* once on any terminal
    /// status (at once if already terminal).
    pub(crate) fn _subscribe_terminal(&self, fn_: &Arg) {
        for st in TERMINAL {
            let _ = self.add_status_callback(st, fn_.clone());
        }
    }
    pub(crate) fn _unsubscribe_terminal(&self, fn_: &Arg) {
        self.0
            .state
            .lock()
            .status_callbacks
            .retain(|(s, cb)| !(TERMINAL.contains(s) && cb.is(fn_)));
    }
    /// `cpf.wait(timeout)`: block until the future completes.
    pub(crate) fn concurrent_wait(&self, timeout: Option<f64>) -> Result<Arg> {
        crate::policy::central::command::schema::exceptions::_check_not_loop_thread()?;
        if self._is_terminal() {
            return self._wait_impl(Some(0.0));
        }
        let me = self.clone();
        crate::policy::central::command::schema::parking::park_sync(move || me._wait_impl(timeout))
    }
    /// Event-driven blocking wait (no polling).
    fn _wait_impl(&self, timeout: Option<f64>) -> Result<Arg> {
        #[cfg(feature = "std")]
        {
            let deadline = timeout.map(|t| {
                std::time::Instant::now() + core::time::Duration::from_secs_f64(t.max(0.0))
            });
            let remaining = |deadline: Option<std::time::Instant>| -> Option<f64> {
                deadline.map(|d| {
                    d.saturating_duration_since(std::time::Instant::now())
                        .as_secs_f64()
                })
            };
            let native = self.0.state.lock().native.clone();
            if let Some(n) = native {
                return n.result(remaining(deadline));
            }
            let (status, exc) = self.snapshot();
            if TERMINAL.contains(&status) {
                return self._outcome(status, exc);
            }
            let done = crate::python::stdlib::Event::new(Args::new());
            let done2 = done.clone();
            let on_terminal =
                Arg::Callable(alloc::sync::Arc::new(move |_a: Args| -> Result<Arg> {
                    done2.set(Args::new());
                    Ok(Arg::None)
                }));
            self._subscribe_terminal(&on_terminal);
            let fired = done.wait(Args::from(remaining(deadline)))?;
            self._unsubscribe_terminal(&on_terminal);
            if !fired {
                // The native future may have been attached late.
                let native = self.0.state.lock().native.clone();
                if let Some(n) = native {
                    return n.result(Some(0.0));
                }
                let (status, _) = self.snapshot();
                if !TERMINAL.contains(&status) {
                    self.set_status(FutureStatus::POLL_TIMEOUT);
                    return Err(LailaError::new(
                        crate::python::ExcKind::TimeoutError,
                        alloc::string::String::new(),
                    ));
                }
            }
            let native = self.0.state.lock().native.clone();
            if let Some(n) = native {
                return n.result(Some(0.0));
            }
            let (status, exc) = self.snapshot();
            self._outcome(status, exc)
        }
        #[cfg(not(feature = "std"))]
        {
            let _ = timeout;
            let (status, exc) = self.snapshot();
            if TERMINAL.contains(&status) {
                return self._outcome(status, exc);
            }
            Err(LailaError::new(
                crate::python::ExcKind::TimeoutError,
                alloc::string::String::new(),
            ))
        }
    }
    /// `await cpf`: the native future or a terminal status (event-driven),
    /// parking the slot while pending.
    pub(crate) async fn concurrent_await(self) -> Result<Arg> {
        let me = self.clone();
        let body = async move {
            #[cfg(feature = "std")]
            {
                let native = me.0.state.lock().native.clone();
                if let Some(n) = native {
                    return n.await;
                }
            }
            me.settled().await
        };
        if self._is_terminal() {
            body.await
        } else {
            crate::policy::central::command::schema::parking::park_async(body).await
        }
    }
}

/// `ConcurrentPackageFuture(taskforce_id=.., policy_id=..)` helpers.
impl ConcurrentPackageFuture {
    /// A future on *taskforce* / *policy* with no native backing (the
    /// runner drives it).
    pub fn create(taskforce_id: impl IntoArg, policy_id: impl IntoArg) -> Result<Future> {
        let mut a = Args::new();
        a.push_keyword("taskforce_id", taskforce_id);
        a.push_keyword("policy_id", policy_id);
        Future::construct(FutureKind::Concurrent, a)
    }
    /// The same, backed by *native*.
    #[cfg(feature = "std")]
    pub fn with_native(
        taskforce_id: impl IntoArg,
        policy_id: impl IntoArg,
        native: crate::python::stdlib::NativeFuture,
    ) -> Result<Future> {
        let f = Self::create(taskforce_id, policy_id)?;
        f.set_native_future(native.into_arg())?;
        Ok(f)
    }
}
