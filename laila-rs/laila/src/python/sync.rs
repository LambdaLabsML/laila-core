//! Lock primitives, switched by the HAL feature set.
//!
//! Host builds use `parking_lot`; the embedded HAL (Phase 4) swaps in a
//! `critical-section` based `lock_api` implementation behind the same names.
//!
//! [`RLock`] is Python's `threading.RLock`: a re-entrant lock whose state
//! outlives any one Rust borrow (`acquire()` / `release()` pair across
//! statements, nested `with obj.atomic():` blocks on one thread). It is the
//! lock every `laila.atomic` object carries.

use alloc::sync::Arc;

#[cfg(feature = "std")]
pub use parking_lot::{Mutex, RwLock};

#[cfg(not(feature = "std"))]
pub use spin::{Mutex, RwLock};

use super::errors::{LailaError, Result};

/// The guard type of [`Mutex`] (both lock implementations hand out a guard
/// that derefs to the value).
#[cfg(feature = "std")]
pub type MutexGuard<'a, T> = parking_lot::MutexGuard<'a, T>;
#[cfg(not(feature = "std"))]
pub type MutexGuard<'a, T> = spin::MutexGuard<'a, T>;

/// A condition variable paired with [`Mutex`].
///
/// On `std` this is `parking_lot::Condvar`. On `no_std` there is a single
/// execution context, so nobody can ever satisfy a wait: `wait` returns at
/// once (as a timeout) and the notifications are no-ops.
#[cfg(feature = "std")]
pub type Condvar = parking_lot::Condvar;
#[cfg(not(feature = "std"))]
#[derive(Default)]
pub struct Condvar;
#[cfg(not(feature = "std"))]
impl Condvar {
    pub const fn new() -> Self {
        Condvar
    }
    pub fn wait<T>(&self, _guard: &mut MutexGuard<'_, T>) {}
    pub fn notify_one(&self) {}
    pub fn notify_all(&self) {}
}

/// Block on *cv* until `pred(&*guard)` holds; `timeout` is seconds (`None`
/// blocks indefinitely). Returns whether the predicate holds on return.
///
/// `no_std`: the predicate is checked once -- a wait that would block can
/// never be satisfied on a single execution context.
pub fn wait_until<T>(
    cv: &Condvar,
    guard: &mut MutexGuard<'_, T>,
    timeout: Option<f64>,
    pred: impl Fn(&T) -> bool,
) -> bool {
    if pred(&**guard) {
        return true;
    }
    #[cfg(feature = "std")]
    {
        match timeout {
            None => {
                while !pred(&**guard) {
                    cv.wait(guard);
                }
                true
            }
            Some(secs) => {
                let deadline =
                    std::time::Instant::now() + core::time::Duration::from_secs_f64(secs.max(0.0));
                while !pred(&**guard) {
                    if cv.wait_until(guard, deadline).timed_out() {
                        return pred(&**guard);
                    }
                }
                true
            }
        }
    }
    #[cfg(not(feature = "std"))]
    {
        let _ = (cv, timeout);
        false
    }
}

/// Run *body* on a fresh named background thread (`std`), or inline on
/// the calling context (`no_std`, where there is nothing else to run it).
pub fn spawn_named(
    name: alloc::string::String,
    body: impl FnOnce() + Send + 'static,
) -> Result<()> {
    #[cfg(feature = "std")]
    {
        // Listed by `threading.active_count()` / `enumerate()` while it
        // runs (Python's twin is a `threading.Thread`).
        super::stdlib::spawn_native_thread(name, body)
            .map(|_| ())
            .map_err(|e| LailaError::runtime_error(alloc::format!("can't start new thread: {e}")))
    }
    #[cfg(not(feature = "std"))]
    {
        let _ = name;
        body();
        Ok(())
    }
}

/// Run `f` over *items*, each on its own scoped thread, and join them all
/// (`std`); one after the other on `no_std`.
pub fn for_each_parallel<T: Sync>(items: &[T], f: impl Fn(&T) + Sync) {
    #[cfg(feature = "std")]
    {
        std::thread::scope(|s| {
            for item in items {
                let f = &f;
                s.spawn(move || f(item));
            }
        });
    }
    #[cfg(not(feature = "std"))]
    {
        for item in items {
            f(item);
        }
    }
}

/// A lazily initialised static (`std::sync::LazyLock` on std, `spin::Lazy`
/// otherwise).
#[cfg(feature = "std")]
pub type Lazy<T> = std::sync::LazyLock<T>;
#[cfg(not(feature = "std"))]
pub type Lazy<T> = spin::Lazy<T>;

/// A process-wide monotonically increasing counter (object identities,
/// exception identities). 64-bit atomics are not available on every
/// embedded target (`thumbv7em`), so the `no_std` side counts in `u32`.
pub struct IdCounter {
    #[cfg(feature = "std")]
    next: core::sync::atomic::AtomicU64,
    #[cfg(not(feature = "std"))]
    next: core::sync::atomic::AtomicU32,
}

impl IdCounter {
    pub const fn new() -> Self {
        IdCounter {
            #[cfg(feature = "std")]
            next: core::sync::atomic::AtomicU64::new(1),
            #[cfg(not(feature = "std"))]
            next: core::sync::atomic::AtomicU32::new(1),
        }
    }
    /// The next id (starts at 1).
    #[allow(clippy::useless_conversion)]
    pub fn next(&self) -> u64 {
        u64::from(
            self.next
                .fetch_add(1, core::sync::atomic::Ordering::Relaxed),
        )
    }
}

impl Default for IdCounter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "std")]
type Owner = std::thread::ThreadId;
/// `no_std`: a single execution context, so one owner id.
#[cfg(not(feature = "std"))]
type Owner = u8;

#[cfg(feature = "std")]
fn current_owner() -> Owner {
    std::thread::current().id()
}
#[cfg(not(feature = "std"))]
fn current_owner() -> Owner {
    0
}

struct RLockState {
    owner: Option<Owner>,
    count: usize,
}

struct RLockInner {
    state: Mutex<RLockState>,
    #[cfg(feature = "std")]
    cv: parking_lot::Condvar,
}

/// `threading.RLock()` -- a re-entrant lock handle; clones share one lock.
///
/// On `std` the owner is the OS thread and waiters block on a condition
/// variable (with an optional timeout, like `RLock.acquire(timeout=..)`).
/// On `no_std` there is a single execution context, so the lock is a plain
/// recursion counter: `acquire` always succeeds and nests.
#[derive(Clone)]
pub struct RLock(Arc<RLockInner>);

impl Default for RLock {
    fn default() -> Self {
        RLock::new()
    }
}

impl core::fmt::Debug for RLock {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let g = self.0.state.lock();
        write!(
            f,
            "<{} _thread.RLock object count={}>",
            if g.count > 0 { "locked" } else { "unlocked" },
            g.count
        )
    }
}

impl RLock {
    pub fn new() -> RLock {
        RLock(Arc::new(RLockInner {
            state: Mutex::new(RLockState {
                owner: None,
                count: 0,
            }),
            #[cfg(feature = "std")]
            cv: parking_lot::Condvar::new(),
        }))
    }

    /// Python `is`: both handles are the same lock.
    pub fn is(&self, other: &RLock) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// `id(lock)`.
    pub fn id(&self) -> usize {
        Arc::as_ptr(&self.0) as *const () as usize
    }

    /// `lock.acquire(blocking=True, timeout=-1)`: `timeout` of `None` (or a
    /// negative number) blocks indefinitely; `blocking=False` is `Some(0.0)`.
    /// Returns `true` once the calling thread owns the lock.
    pub fn acquire(&self, timeout: Option<f64>) -> bool {
        let me = current_owner();
        let mut g = self.0.state.lock();
        if g.count > 0 && g.owner == Some(me) {
            g.count += 1;
            return true;
        }
        #[cfg(feature = "std")]
        {
            match timeout {
                Some(t) if t >= 0.0 => {
                    let deadline =
                        std::time::Instant::now() + core::time::Duration::from_secs_f64(t);
                    while g.count > 0 {
                        if self.0.cv.wait_until(&mut g, deadline).timed_out() && g.count > 0 {
                            return false;
                        }
                    }
                }
                _ => {
                    while g.count > 0 {
                        self.0.cv.wait(&mut g);
                    }
                }
            }
        }
        #[cfg(not(feature = "std"))]
        {
            // Single execution context: a held lock can only be ours.
            let _ = timeout;
        }
        g.owner = Some(me);
        g.count += 1;
        true
    }

    /// `lock.release()`: `RuntimeError("cannot release un-acquired lock")`
    /// when the caller does not own it.
    pub fn release(&self) -> Result<()> {
        let mut g = self.0.state.lock();
        if g.count == 0 || g.owner != Some(current_owner()) {
            return Err(LailaError::runtime_error("cannot release un-acquired lock"));
        }
        g.count -= 1;
        if g.count == 0 {
            g.owner = None;
            #[cfg(feature = "std")]
            self.0.cv.notify_one();
        }
        Ok(())
    }

    /// `lock.locked()`: held by *some* thread.
    pub fn locked(&self) -> bool {
        self.0.state.lock().count > 0
    }

    /// `lock._is_owned()`: held by the calling thread.
    pub fn is_owned(&self) -> bool {
        let g = self.0.state.lock();
        g.count > 0 && g.owner == Some(current_owner())
    }

    /// `lock._recursion_count()`: nesting depth of the calling thread's hold.
    pub fn recursion_count(&self) -> usize {
        let g = self.0.state.lock();
        if g.owner == Some(current_owner()) {
            g.count
        } else {
            0
        }
    }
}
