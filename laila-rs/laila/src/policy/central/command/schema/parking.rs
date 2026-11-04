//! Slot parking -- release a taskforce slot while awaiting another future.
//!
//! A taskforce has a bounded number of execution *slots*. A task that
//! holds a slot and blocks on a child future that itself needs a slot on
//! the same taskforce is a hold-and-wait dependency; when every slot is
//! held that way nothing can progress. Every runner coroutine publishes a
//! [`_SlotCtx`] in [`_CURRENT_SLOT`]; whenever code running inside a slot
//! waits on a laila future the wait is wrapped in [`park_async`] /
//! [`park_sync`], which hand the slot back (`_park_enter`) and re-acquire
//! one before returning (`_park_exit_*`). Parking is reentrant per slot
//! (a depth counter), propagates to executor threads through the
//! `contextvars` context, and a sync body also releases its compute
//! *permit* ([`_CURRENT_PERMIT`]) while blocked.
//!
//! [`_RESOLVE_CHAIN`] carries the entry global-ids being resolved along
//! the causal chain of nested submissions; `laila.remember` / `laila.build`
//! consult it to raise `CyclicDependencyError` instead of recursing.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::python::stdlib::contextvars::ContextVar;
use crate::python::sync::Mutex;
use crate::python::{ExcKind, LailaError, Result};

pub use crate::python::errors::CyclicDependencyError;

/// A parked task's "hand me a slot" callback.
pub type ResumeCallback = Arc<dyn Fn() + Send + Sync>;

/// What parking needs from a loop thread (`_LoopThread`).
pub trait SlotLoop: Send + Sync {
    /// Give up one slot; `true` if the in-flight count was decremented
    /// (the dispatcher may place a new root), `false` if the slot was
    /// transferred to a parked waiter instead.
    fn release_slot(&self) -> bool;
    /// Claim a slot now (`true`) or enqueue *cb* to be handed one later.
    fn try_reserve_or_wait(&self, cb: ResumeCallback) -> bool;
    /// Remove *cb* from the waiters. `false` if it was already served.
    fn cancel_wait(&self, cb: &ResumeCallback) -> bool;
}

/// What parking needs from a taskforce (`_capacity_cv`, `_stop`, identity).
pub trait SlotTaskForce: Send + Sync {
    fn notify_capacity(&self);
    fn stop_requested(&self) -> bool;
    fn global_id(&self) -> String;
    fn rank(&self) -> i64;
    /// Identity, for `source is target` checks.
    fn ptr_id(&self) -> usize;
    /// `tf.run_sync(fn)`: run *body* on an executor thread under a compute
    /// permit; awaitable.
    fn run_sync(
        &self,
        body: alloc::boxed::Box<dyn FnOnce() -> Result<crate::python::Arg> + Send>,
    ) -> Result<crate::python::stdlib::asyncio::OneShot<Result<crate::python::Arg>>>;
}

struct SlotState {
    depth: i64,
    holds_slot: bool,
}

/// Bookkeeping for one occupied taskforce slot.
pub struct _SlotCtx {
    pub tf: Arc<dyn SlotTaskForce>,
    pub lt: Arc<dyn SlotLoop>,
    state: Mutex<SlotState>,
}

impl _SlotCtx {
    pub fn new(tf: Arc<dyn SlotTaskForce>, lt: Arc<dyn SlotLoop>) -> Arc<_SlotCtx> {
        Arc::new(_SlotCtx {
            tf,
            lt,
            state: Mutex::new(SlotState {
                depth: 0,
                holds_slot: true,
            }),
        })
    }
    pub fn depth(&self) -> i64 {
        self.state.lock().depth
    }
    pub fn holds_slot(&self) -> bool {
        self.state.lock().holds_slot
    }
    pub fn set_holds_slot(&self, v: bool) {
        self.state.lock().holds_slot = v;
    }
    /// `with ctx.lock: holds = ctx.holds_slot; ctx.holds_slot = False`
    pub fn take_slot(&self) -> bool {
        let mut g = self.state.lock();
        core::mem::replace(&mut g.holds_slot, false)
    }
}

/// Slot context of the runner coroutine the current code is executing under.
pub static _CURRENT_SLOT: ContextVar<Option<Arc<_SlotCtx>>> =
    ContextVar::new("_laila_current_slot", || None);

/// Tuple of entry global-ids being resolved along the current causal chain.
pub static _RESOLVE_CHAIN: ContextVar<Arc<Vec<String>>> =
    ContextVar::new("_laila_resolve_chain", || Arc::new(Vec::new()));

/// A compute permit held by a sync body running on an executor thread.
pub struct _Permit {
    pub sem: Arc<crate::python::stdlib::Semaphore>,
    held: Mutex<bool>,
}

impl _Permit {
    pub fn new(sem: Arc<crate::python::stdlib::Semaphore>) -> Arc<_Permit> {
        Arc::new(_Permit {
            sem,
            held: Mutex::new(false),
        })
    }
    pub fn held(&self) -> bool {
        *self.held.lock()
    }
    pub fn acquire(&self) {
        let mut h = self.held.lock();
        if !*h {
            self.sem.acquire_blocking();
            *h = true;
        }
    }
    pub fn release(&self) {
        let mut h = self.held.lock();
        if *h {
            *h = false;
            self.sem.release();
        }
    }
}

/// Compute permit of the executor thread the current sync body runs on.
pub static _CURRENT_PERMIT: ContextVar<Option<Arc<_Permit>>> =
    ContextVar::new("_laila_current_permit", || None);

// ---------------------------------------------------------------------------
// release / re-acquire primitives

/// Increment park depth; on the outermost park hand the slot back.
pub fn _park_enter(ctx: &_SlotCtx) {
    {
        let mut g = ctx.state.lock();
        g.depth += 1;
        if g.depth > 1 || !g.holds_slot {
            return;
        }
        g.holds_slot = false;
    }
    if ctx.lt.release_slot() {
        ctx.tf.notify_capacity();
    }
}

fn _stop_requested(ctx: &_SlotCtx) -> bool {
    ctx.tf.stop_requested()
}

/// Decrement park depth; on the outermost exit re-acquire a slot.
pub async fn _park_exit_async(ctx: Arc<_SlotCtx>) {
    {
        let mut g = ctx.state.lock();
        g.depth -= 1;
        if g.depth > 0 || g.holds_slot {
            return;
        }
    }
    if _stop_requested(&ctx) {
        return;
    }
    let fut: crate::python::stdlib::asyncio::OneShot<()> =
        crate::python::stdlib::asyncio::OneShot::new();
    let f2 = fut.clone();
    let cb: ResumeCallback = Arc::new(move || f2.set_result(()));
    if ctx.lt.try_reserve_or_wait(cb.clone()) {
        ctx.set_holds_slot(true);
        return;
    }
    // Cancelled while waiting (the future dropped): if the slot was
    // already handed to us we own it and the runner's guard releases it.
    struct CancelGuard<'a> {
        ctx: &'a _SlotCtx,
        cb: ResumeCallback,
        done: bool,
    }
    impl Drop for CancelGuard<'_> {
        fn drop(&mut self) {
            if !self.done && !self.ctx.lt.cancel_wait(&self.cb) {
                self.ctx.set_holds_slot(true);
            }
        }
    }
    let mut guard = CancelGuard {
        ctx: &ctx,
        cb,
        done: false,
    };
    let _ = fut.await;
    guard.done = true;
    ctx.set_holds_slot(true);
}

/// Sync counterpart of [`_park_exit_async`] for executor threads.
pub fn _park_exit_sync(ctx: &_SlotCtx) {
    {
        let mut g = ctx.state.lock();
        g.depth -= 1;
        if g.depth > 0 || g.holds_slot {
            return;
        }
    }
    if _stop_requested(ctx) {
        return;
    }
    #[cfg(feature = "std")]
    {
        let ev = crate::python::stdlib::Event::new(crate::python::Args::new());
        let ev2 = ev.clone();
        let cb: ResumeCallback = Arc::new(move || ev2.set(crate::python::Args::new()));
        if ctx.lt.try_reserve_or_wait(cb.clone()) {
            ctx.set_holds_slot(true);
            return;
        }
        loop {
            if ev.wait(0.05).unwrap_or(false) {
                break;
            }
            if _stop_requested(ctx) {
                if ctx.lt.cancel_wait(&cb) {
                    return;
                }
                break;
            }
        }
    }
    ctx.set_holds_slot(true);
}

// ---------------------------------------------------------------------------
// public helpers

/// Await *awaitable*, parking the current slot (if any) for the duration.
pub async fn park_async<T, F: core::future::Future<Output = T>>(awaitable: F) -> T {
    let Some(ctx) = _CURRENT_SLOT.get() else {
        return awaitable.await;
    };
    _park_enter(&ctx);
    let out = awaitable.await;
    _park_exit_async(ctx).await;
    out
}

/// Call *f*, parking the current slot (if any) and releasing the compute
/// permit (if any) while it blocks.
pub fn park_sync<T>(f: impl FnOnce() -> T) -> T {
    let ctx = _CURRENT_SLOT.get();
    let permit = _CURRENT_PERMIT.get();
    if ctx.is_none() && permit.is_none() {
        return f();
    }
    // Give up the compute permit first (never hold a permit while waiting),
    // then the slot.
    let released_permit = permit.as_ref().is_some_and(|p| p.held());
    if released_permit {
        permit.as_ref().expect("permit").release();
    }
    if let Some(c) = &ctx {
        _park_enter(c);
    }
    let out = f();
    // Re-acquire in the opposite order: slot, then permit.
    if let Some(c) = &ctx {
        _park_exit_sync(c);
    }
    if released_permit {
        permit.as_ref().expect("permit").acquire();
    }
    out
}

/// Raise `CyclicDependencyError` if any id is already being resolved.
pub fn check_resolve_cycle(global_ids: &[&str]) -> Result<()> {
    let chain = _RESOLVE_CHAIN.get();
    if chain.is_empty() {
        return Ok(());
    }
    for gid in global_ids {
        if chain.iter().any(|c| c == gid) {
            return Err(LailaError::new(
                ExcKind::CyclicDependencyError,
                alloc::format!(
                    "Cyclic dependency: {gid} is already being resolved on the \
                     current chain ({} -> {gid}).",
                    chain.join(" -> ")
                ),
            ));
        }
    }
    Ok(())
}

/// The current chain extended with *global_ids* (no mutation).
pub fn resolve_chain_with(global_ids: &[&str]) -> Arc<Vec<String>> {
    let mut v: Vec<String> = (*_RESOLVE_CHAIN.get()).clone();
    v.extend(global_ids.iter().map(|s| String::from(*s)));
    Arc::new(v)
}
