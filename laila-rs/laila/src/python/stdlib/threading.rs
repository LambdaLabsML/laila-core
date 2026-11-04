//! `import threading` -- `Thread`, `Lock`, `RLock`, `Event`, `Barrier`,
//! `enumerate`, `active_count`, `current_thread`, `get_ident` on `std`
//! threads. The spelling of every call is Python's:
//!
//! ```text
//! t = threading.Thread(target=f, args=(1, 2), daemon=True)
//! t.start(); t.join()
//! ```

use alloc::string::String;
#[cfg(feature = "std")]
use alloc::string::ToString;
use alloc::sync::Arc;
#[cfg(feature = "std")]
use alloc::vec::Vec;

use crate::python::args::Args;
#[cfg_attr(not(feature = "std"), allow(unused_imports))]
use crate::python::errors::{ExcKind, LailaError, Result};
use crate::python::object::{Arg, IntoArg, PyObject};
#[cfg_attr(not(feature = "std"), allow(unused_imports))]
use crate::python::pysig;
use crate::python::sync::{wait_until, Condvar, Mutex, MutexGuard};

#[cfg(feature = "std")]
static REGISTRY: Mutex<Vec<Thread>> = Mutex::new(Vec::new());
/// Live threads laila spawned outside `threading.Thread` (executor
/// workers, taskforce loops and dispatchers): `(ident, name)`, counted by
/// `threading.active_count()` and listed by `threading.enumerate()` (as
/// Python lists the `Thread` objects behind them).
#[cfg(feature = "std")]
static NATIVE: Mutex<Vec<(u64, String)>> = Mutex::new(Vec::new());
#[cfg(not(feature = "std"))]
static NATIVE_ALIVE: core::sync::atomic::AtomicIsize = core::sync::atomic::AtomicIsize::new(0);

/// A native thread started (call from that thread): count it in
/// `threading.active_count()` under its OS thread name.
pub fn _register_native_thread() {
    #[cfg(feature = "std")]
    {
        let cur = std::thread::current();
        let name = cur.name().unwrap_or("Thread").to_string();
        NATIVE.lock().push((thread_id_u64(cur.id()), name));
    }
    #[cfg(not(feature = "std"))]
    NATIVE_ALIVE.fetch_add(1, core::sync::atomic::Ordering::SeqCst);
}
/// A native thread exiting (call from that thread, as its last step).
pub fn _unregister_native_thread() {
    #[cfg(feature = "std")]
    {
        let id = thread_id_u64(std::thread::current().id());
        NATIVE.lock().retain(|(i, _)| *i != id);
    }
    #[cfg(not(feature = "std"))]
    NATIVE_ALIVE.fetch_sub(1, core::sync::atomic::Ordering::SeqCst);
}
/// Spawn a named native thread that `threading.active_count()` /
/// `enumerate()` list for as long as *body* runs. Like Python's
/// `Thread.start()`, this returns only once the thread is registered, so
/// a count taken right after it sees the new thread.
#[cfg(feature = "std")]
pub fn spawn_native_thread(
    name: String,
    body: impl FnOnce() + Send + 'static,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let handle = std::thread::Builder::new().name(name).spawn(move || {
        _register_native_thread();
        let _ = tx.send(());
        body();
        _unregister_native_thread();
    })?;
    let _ = rx.recv();
    Ok(handle)
}

#[cfg(feature = "std")]
static COUNTER: core::sync::atomic::AtomicUsize = core::sync::atomic::AtomicUsize::new(1);

#[cfg(feature = "std")]
struct ThreadState {
    name: Mutex<String>,
    daemon: bool,
    target: Option<Arg>,
    args: Vec<Arg>,
    kwargs: Vec<(String, Arg)>,
    handle: Mutex<Option<std::thread::JoinHandle<Result<Arg>>>>,
    started: core::sync::atomic::AtomicBool,
    finished: Arc<core::sync::atomic::AtomicBool>,
    ident: Mutex<Option<u64>>,
    result: Mutex<Option<Result<Arg>>>,
}

/// `threading.Thread`
#[cfg(feature = "std")]
#[derive(Clone)]
pub struct Thread(Arc<ThreadState>);

#[cfg(feature = "std")]
impl core::fmt::Debug for Thread {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__repr__())
    }
}

#[cfg(feature = "std")]
impl Thread {
    /// `threading.Thread(group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None)`
    pub fn new(args: impl Into<Args>) -> Result<Thread> {
        let a: Args = args.into();
        if !a.positional.is_empty() && !a.positional[0].is_none() {
            return Err(LailaError::new(
                crate::python::errors::ExcKind::AssertionError,
                "group argument must be None for now",
            ));
        }
        let target = a
            .positional
            .get(1)
            .cloned()
            .unwrap_or_else(|| a.get("target"));
        let target = if target.is_none() { None } else { Some(target) };
        let name = a.get("name");
        let name = if name.is_none() {
            alloc::format!(
                "Thread-{}",
                COUNTER.fetch_add(1, core::sync::atomic::Ordering::SeqCst)
            )
        } else {
            name.__str__()
        };
        let targs = a.get("args");
        let targs = if targs.is_none() {
            Vec::new()
        } else {
            targs.__iter__()?
        };
        let kwargs = a.get("kwargs");
        let kwargs: Vec<(String, Arg)> = if kwargs.is_none() {
            Vec::new()
        } else {
            kwargs
                .__iter__()?
                .into_iter()
                .map(|k| Ok((k.__str__(), kwargs.__getitem__(k.clone())?)))
                .collect::<Result<_>>()?
        };
        let daemon = a.get("daemon").__bool__();
        Ok(Thread(Arc::new(ThreadState {
            name: Mutex::new(name),
            daemon,
            target,
            args: targs,
            kwargs,
            handle: Mutex::new(None),
            started: core::sync::atomic::AtomicBool::new(false),
            finished: Arc::new(core::sync::atomic::AtomicBool::new(false)),
            ident: Mutex::new(None),
            result: Mutex::new(None),
        })))
    }

    /// The `Thread` view of a native laila thread (`threading.enumerate()`
    /// lists one per live executor / loop / dispatcher thread): started,
    /// not finished, with its OS name and ident; `join` is not supported.
    pub fn foreign(ident: u64, name: String) -> Thread {
        Thread(Arc::new(ThreadState {
            name: Mutex::new(name),
            daemon: true,
            target: None,
            args: Vec::new(),
            kwargs: Vec::new(),
            handle: Mutex::new(None),
            started: core::sync::atomic::AtomicBool::new(true),
            finished: Arc::new(core::sync::atomic::AtomicBool::new(false)),
            ident: Mutex::new(Some(ident)),
            result: Mutex::new(None),
        }))
    }

    /// `t.start()`
    pub fn start(&self, _args: impl Into<Args>) -> Result<()> {
        use core::sync::atomic::Ordering;
        if self.0.started.swap(true, Ordering::SeqCst) {
            return Err(LailaError::runtime_error(
                "threads can only be started once",
            ));
        }
        let st = self.0.clone();
        let finished = self.0.finished.clone();
        let this = self.clone();
        REGISTRY.lock().push(this.clone());
        let thread_name = st.name.lock().clone();
        let handle = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                let r = match &st.target {
                    Some(t) => {
                        let mut a = Args::new();
                        for v in &st.args {
                            a.push_positional(v.clone());
                        }
                        for (k, v) in &st.kwargs {
                            a.push_keyword(k, v.clone());
                        }
                        t.__call__(a)
                    }
                    None => Ok(Arg::None),
                };
                *st.result.lock() = Some(r.clone());
                finished.store(true, Ordering::SeqCst);
                REGISTRY.lock().retain(|t| !Arc::ptr_eq(&t.0, &st));
                r
            })
            .map_err(|e| {
                LailaError::runtime_error(alloc::format!("can't start new thread: {e}"))
            })?;
        *self.0.ident.lock() = Some(thread_id_u64(handle.thread().id()));
        *self.0.handle.lock() = Some(handle);
        Ok(())
    }

    /// `t.join(timeout=None)`
    pub fn join(&self, args: impl Into<Args>) -> Result<()> {
        let a: Args = args.into();
        if !self.0.started.load(core::sync::atomic::Ordering::SeqCst) {
            return Err(LailaError::runtime_error(
                "cannot join thread before it is started",
            ));
        }
        let timeout = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("timeout"));
        if let Some(secs) = timeout.as_f64() {
            let deadline =
                std::time::Instant::now() + core::time::Duration::from_secs_f64(secs.max(0.0));
            while !self.0.finished.load(core::sync::atomic::Ordering::SeqCst) {
                if std::time::Instant::now() >= deadline {
                    return Ok(());
                }
                std::thread::sleep(core::time::Duration::from_millis(1));
            }
        }
        let h = self.0.handle.lock().take();
        if let Some(h) = h {
            let _ = h.join();
        }
        Ok(())
    }

    pub fn is_alive(&self, _args: impl Into<Args>) -> bool {
        use core::sync::atomic::Ordering;
        self.0.started.load(Ordering::SeqCst) && !self.0.finished.load(Ordering::SeqCst)
    }
    pub fn name(&self) -> String {
        self.0.name.lock().clone()
    }
    pub fn set_name(&self, name: impl IntoArg) {
        *self.0.name.lock() = name.into_arg().__str__();
    }
    pub fn daemon(&self) -> bool {
        self.0.daemon
    }
    pub fn ident(&self) -> Arg {
        self.0
            .ident
            .lock()
            .map(|i| Arg::Int(i as i64))
            .unwrap_or(Arg::None)
    }
    pub fn native_id(&self) -> Arg {
        self.ident()
    }
    /// The target's return value / exception after `join()` (Rust convenience).
    pub fn result(&self) -> Option<Result<Arg>> {
        self.0.result.lock().clone()
    }
    pub fn is(&self, other: &Thread) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

#[cfg(feature = "std")]
fn thread_id_u64(id: std::thread::ThreadId) -> u64 {
    // ThreadId's Debug form is `ThreadId(n)`.
    let s = alloc::format!("{id:?}");
    s.trim_start_matches("ThreadId(")
        .trim_end_matches(')')
        .parse()
        .unwrap_or(0)
}

#[cfg(feature = "std")]
impl PyObject for Thread {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    /// `a is b`: identity of the shared handle, not of the boxing.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Thread>()
            .is_some_and(|o| self.is(o))
    }
    fn type_name(&self) -> &'static str {
        "Thread"
    }
    fn __repr__(&self) -> String {
        let state = if !self.0.started.load(core::sync::atomic::Ordering::SeqCst) {
            "initial"
        } else if self.is_alive(Args::new()) {
            "started"
        } else {
            "stopped"
        };
        let daemon = if self.0.daemon { " daemon" } else { "" };
        alloc::format!("<Thread({}, {state}{daemon})>", self.name())
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Thread>()
            .is_some_and(|o| self.is(o))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "name" => Ok(self.name().into_arg()),
            "daemon" => Ok(Arg::Bool(self.0.daemon)),
            "ident" | "native_id" => Ok(self.ident()),
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'Thread' object has no attribute '{name}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        Ok(match name {
            "start" => {
                self.start(args)?;
                Arg::None
            }
            "join" => {
                self.join(args)?;
                Arg::None
            }
            "is_alive" => Arg::Bool(self.is_alive(args)),
            "getName" => self.name().into_arg(),
            _ => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'Thread' object has no attribute '{name}'"
                )))
            }
        })
    }
}
#[cfg(feature = "std")]
impl IntoArg for Thread {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
#[cfg(feature = "std")]
impl crate::python::object::PyArg for Thread {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}

/// `threading.Lock()` -- a non-reentrant lock whose state outlives any one
/// Rust borrow (Python's `lock.acquire()` / `lock.release()` pair across
/// statements). `with lock:` returns a guard that releases on drop.
#[derive(Clone, Default)]
pub struct Lock(Arc<(Mutex<bool>, Condvar)>);
impl core::fmt::Debug for Lock {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__repr__())
    }
}
/// `with <lock as Arg>:` -- the dynamic layer's `__enter__` hands the guard
/// back as an object (released when the `Arg` is dropped).
macro_rules! guard_object {
    ($t:ident, $name:literal) => {
        impl PyObject for $t {
            fn as_any(&self) -> &dyn core::any::Any {
                self
            }
            fn type_name(&self) -> &'static str {
                $name
            }
            fn __repr__(&self) -> String {
                alloc::format!("<{} object>", $name)
            }
            fn __eq__(&self, other: &dyn PyObject) -> bool {
                core::ptr::eq(
                    other.as_any() as *const dyn core::any::Any as *const (),
                    self as *const $t as *const (),
                )
            }
            fn __hash__(&self) -> Result<i64> {
                Ok(self as *const $t as usize as i64)
            }
        }
    };
}
/// Guard returned by `lock.__enter__()`; releases on drop.
pub struct LockGuard(Lock);
impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = self.0.release(Args::new());
    }
}
guard_object!(LockGuard, "_LockGuard");
impl Lock {
    pub fn new(_args: impl Into<Args>) -> Lock {
        Lock::default()
    }
    /// `lock.acquire(blocking=True, timeout=-1)` -> bool
    pub fn acquire(&self, args: impl Into<Args>) -> Result<bool> {
        let a: Args = args.into();
        let blocking = if a.positional.is_empty() && !a.__contains__("blocking")? {
            true
        } else {
            a.positional
                .first()
                .cloned()
                .unwrap_or_else(|| a.get("blocking"))
                .__bool__()
        };
        let timeout = a
            .positional
            .get(1)
            .cloned()
            .unwrap_or_else(|| a.get("timeout"))
            .as_f64()
            .unwrap_or(-1.0);
        let mut locked = self.0 .0.lock();
        if !blocking {
            if *locked {
                return Ok(false);
            }
            *locked = true;
            return Ok(true);
        }
        let timeout = if timeout >= 0.0 { Some(timeout) } else { None };
        if !wait_until(&self.0 .1, &mut locked, timeout, |l| !*l) {
            return Ok(false);
        }
        *locked = true;
        Ok(true)
    }
    pub fn release(&self, _args: impl Into<Args>) -> Result<()> {
        let mut locked = self.0 .0.lock();
        if !*locked {
            return Err(LailaError::runtime_error("release unlocked lock"));
        }
        *locked = false;
        self.0 .1.notify_one();
        Ok(())
    }
    pub fn locked(&self, _args: impl Into<Args>) -> bool {
        *self.0 .0.lock()
    }
    pub fn __enter__(&self) -> Result<LockGuard> {
        self.acquire(Args::new())?;
        Ok(LockGuard(self.clone()))
    }
}
impl PyObject for Lock {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "lock"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<{} _thread.lock object>",
            if self.locked(Args::new()) {
                "locked"
            } else {
                "unlocked"
            }
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Lock>()
            .is_some_and(|o| Arc::ptr_eq(&o.0, &self.0))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        Ok(match name {
            "acquire" => Arg::Bool(self.acquire(args)?),
            "release" => {
                self.release(args)?;
                Arg::None
            }
            "__enter__" => Arg::Object(Arc::new(self.__enter__()?)),
            "__exit__" => {
                self.release(Args::new())?;
                Arg::Bool(false)
            }
            "locked" => Arg::Bool(self.locked(args)),
            _ => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'lock' object has no attribute '{name}'"
                )))
            }
        })
    }
}
impl IntoArg for Lock {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
/// `threading.RLock()` -- Python's re-entrant lock over
/// [`crate::python::sync::RLock`] (the primitive every `laila.atomic`
/// object carries). `with lock:` returns a guard that releases on drop.
#[derive(Clone, Default)]
pub struct RLock(pub crate::python::sync::RLock);
impl core::fmt::Debug for RLock {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__repr__())
    }
}
/// Guard returned by `rlock.__enter__()`; releases on drop.
pub struct RLockGuard(RLock);
impl Drop for RLockGuard {
    fn drop(&mut self) {
        let _ = self.0 .0.release();
    }
}
guard_object!(RLockGuard, "_RLockGuard");
impl RLock {
    pub fn new(_args: impl Into<Args>) -> RLock {
        RLock::default()
    }
    /// `rlock.acquire(blocking=True, timeout=-1)` -> bool
    pub fn acquire(&self, args: impl Into<Args>) -> Result<bool> {
        let a: Args = args.into();
        let blocking = if a.positional.is_empty() && !a.__contains__("blocking")? {
            true
        } else {
            a.positional
                .first()
                .cloned()
                .unwrap_or_else(|| a.get("blocking"))
                .__bool__()
        };
        let timeout = a
            .positional
            .get(1)
            .cloned()
            .unwrap_or_else(|| a.get("timeout"))
            .as_f64()
            .unwrap_or(-1.0);
        if !blocking && timeout != -1.0 {
            return Err(LailaError::value_error(
                "can't specify a timeout for a non-blocking call",
            ));
        }
        if timeout < 0.0 && timeout != -1.0 {
            return Err(LailaError::value_error(
                "timeout value must be a non-negative number",
            ));
        }
        let t = if !blocking {
            Some(0.0)
        } else if timeout >= 0.0 {
            Some(timeout)
        } else {
            None
        };
        Ok(self.0.acquire(t))
    }
    /// `rlock.release()`; `RuntimeError("cannot release un-acquired lock")`.
    pub fn release(&self, _args: impl Into<Args>) -> Result<()> {
        self.0.release()
    }
    pub fn locked(&self, _args: impl Into<Args>) -> bool {
        self.0.locked()
    }
    pub fn _is_owned(&self) -> bool {
        self.0.is_owned()
    }
    pub fn _recursion_count(&self) -> i64 {
        self.0.recursion_count() as i64
    }
    pub fn __enter__(&self) -> Result<RLockGuard> {
        self.0.acquire(None);
        Ok(RLockGuard(self.clone()))
    }
    /// The shared primitive (handle clone).
    pub fn inner(&self) -> crate::python::sync::RLock {
        self.0.clone()
    }
}
impl PyObject for RLock {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "RLock"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<{} _thread.RLock object count={}>",
            if self.0.locked() {
                "locked"
            } else {
                "unlocked"
            },
            self.0.recursion_count()
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<RLock>()
            .is_some_and(|o| o.0.is(&self.0))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(self.0.id() as i64)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        Ok(match name {
            "acquire" => Arg::Bool(self.acquire(args)?),
            "release" => {
                self.release(args)?;
                Arg::None
            }
            "__enter__" => Arg::Object(Arc::new(self.__enter__()?)),
            "__exit__" => {
                self.release(Args::new())?;
                Arg::Bool(false)
            }
            "locked" => Arg::Bool(self.locked(args)),
            "_is_owned" => Arg::Bool(self._is_owned()),
            "_recursion_count" => Arg::Int(self._recursion_count()),
            _ => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'RLock' object has no attribute '{name}'"
                )))
            }
        })
    }
}
impl IntoArg for RLock {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl crate::python::object::PyArg for RLock {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}

/// `threading.Event()`
#[derive(Clone, Default)]
pub struct Event(Arc<(Mutex<bool>, Condvar)>);
impl core::fmt::Debug for Event {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "<threading.Event: {}>",
            if self.is_set(Args::new()) {
                "set"
            } else {
                "unset"
            }
        )
    }
}
impl Event {
    pub fn new(_args: impl Into<Args>) -> Event {
        Event::default()
    }
    pub fn is_set(&self, _args: impl Into<Args>) -> bool {
        *self.0 .0.lock()
    }
    pub fn set(&self, _args: impl Into<Args>) {
        *self.0 .0.lock() = true;
        self.0 .1.notify_all();
    }
    pub fn clear(&self, _args: impl Into<Args>) {
        *self.0 .0.lock() = false;
    }
    /// `event.wait(timeout=None)` -> bool
    pub fn wait(&self, args: impl Into<Args>) -> Result<bool> {
        let a: Args = args.into();
        let timeout = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("timeout"));
        let mut g = self.0 .0.lock();
        Ok(wait_until(&self.0 .1, &mut g, timeout.as_f64(), |set| *set))
    }
}
impl PyObject for Event {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "Event"
    }
    fn __repr__(&self) -> String {
        alloc::format!("{self:?}")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Event>()
            .is_some_and(|o| Arc::ptr_eq(&o.0, &self.0))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    /// `ev.set` / `ev.wait` read as values: bound methods
    /// (`threading.Timer(0.05, ev.set)`).
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "set" | "clear" | "is_set" | "wait" => {
                let me = self.clone();
                let n: String = name.into();
                Ok(crate::python::object::callable(move |a: Args| {
                    me.__call_method__(&n, a)
                }))
            }
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'Event' object has no attribute '{name}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        Ok(match name {
            "set" => {
                self.set(args);
                Arg::None
            }
            "clear" => {
                self.clear(args);
                Arg::None
            }
            "is_set" => Arg::Bool(self.is_set(args)),
            "wait" => Arg::Bool(self.wait(args)?),
            _ => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'Event' object has no attribute '{name}'"
                )))
            }
        })
    }
}
impl IntoArg for Event {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

/// `threading.Barrier(parties, action=None, timeout=None)`
#[cfg(feature = "std")]
#[derive(Clone)]
pub struct Barrier(Arc<(Mutex<BarrierState>, Condvar)>, usize);
#[cfg(feature = "std")]
#[derive(Default)]
pub struct BarrierState {
    count: usize,
    generation: u64,
    broken: bool,
}
#[cfg(feature = "std")]
impl core::fmt::Debug for Barrier {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "<threading.Barrier parties={}>", self.1)
    }
}
#[cfg(feature = "std")]
impl Barrier {
    pub fn new(args: impl Into<Args>) -> Result<Barrier> {
        let a: Args = args.into();
        let n = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("parties"))
            .as_i64()
            .ok_or_else(|| {
                LailaError::type_error("Barrier() missing required argument 'parties'")
            })?;
        Ok(Barrier(
            Arc::new((Mutex::new(BarrierState::default()), Condvar::new())),
            n.max(0) as usize,
        ))
    }
    /// `barrier.wait(timeout=None)` -> arrival index (0..parties). A
    /// timeout (or a wait on a broken barrier) raises
    /// `threading.BrokenBarrierError` and breaks the barrier for every
    /// other waiter, as in Python.
    pub fn wait(&self, args: impl Into<Args>) -> Result<i64> {
        let a: Args = args.into();
        let timeout = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("timeout"))
            .as_f64();
        let (lock, cv) = &*self.0;
        let mut g = lock.lock();
        if g.broken {
            return Err(LailaError::new(ExcKind::BrokenBarrierError, ""));
        }
        let index = g.count;
        g.count += 1;
        if g.count >= self.1 {
            // The last arrival releases the generation.
            g.count = 0;
            g.generation += 1;
            cv.notify_all();
            return Ok(index as i64);
        }
        let gen = g.generation;
        let released = wait_until(cv, &mut g, timeout, |st| st.generation != gen || st.broken);
        if g.broken {
            return Err(LailaError::new(ExcKind::BrokenBarrierError, ""));
        }
        if !released {
            g.broken = true;
            cv.notify_all();
            return Err(LailaError::new(ExcKind::BrokenBarrierError, ""));
        }
        Ok(index as i64)
    }
    /// `barrier.reset()`: back to the empty state (waiters get
    /// `BrokenBarrierError`).
    pub fn reset(&self) {
        let (lock, cv) = &*self.0;
        let mut g = lock.lock();
        if g.count > 0 {
            g.broken = true;
            cv.notify_all();
        }
        *g = BarrierState::default();
    }
    /// `barrier.abort()`
    pub fn abort(&self) {
        let (lock, cv) = &*self.0;
        lock.lock().broken = true;
        cv.notify_all();
    }
    pub fn parties(&self) -> i64 {
        self.1 as i64
    }
    /// `barrier.n_waiting`
    pub fn n_waiting(&self) -> i64 {
        self.0 .0.lock().count as i64
    }
    /// `barrier.broken`
    pub fn broken(&self) -> bool {
        self.0 .0.lock().broken
    }
}
#[cfg(feature = "std")]
impl IntoArg for Barrier {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
#[cfg(feature = "std")]
impl PyObject for Barrier {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "Barrier"
    }
    fn __repr__(&self) -> String {
        alloc::format!("{self:?}")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Barrier>()
            .is_some_and(|o| Arc::ptr_eq(&o.0, &self.0))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
}

/// `threading.Semaphore(value=1)`
pub struct Semaphore(Mutex<i64>, Condvar);
impl core::fmt::Debug for Semaphore {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "<threading.Semaphore value={}>", self.value())
    }
}
impl Semaphore {
    pub fn new(value: i64) -> Semaphore {
        Semaphore(Mutex::new(value), Condvar::new())
    }
    pub fn value(&self) -> i64 {
        *self.0.lock()
    }
    /// `sem.acquire()` (blocking).
    pub fn acquire_blocking(&self) {
        self.acquire(true, None);
    }
    /// `sem.acquire(blocking=True, timeout=None)` -> bool
    pub fn acquire(&self, blocking: bool, timeout: Option<f64>) -> bool {
        let mut g = self.0.lock();
        if *g > 0 {
            *g -= 1;
            return true;
        }
        if !blocking {
            return false;
        }
        if !wait_until(&self.1, &mut g, timeout, |v| *v > 0) {
            return false;
        }
        *g -= 1;
        true
    }
    /// `sem.release(n=1)`
    pub fn release(&self) {
        *self.0.lock() += 1;
        self.1.notify_one();
    }
}

/// `threading.Condition()` -- a lock plus `wait(timeout)` / `notify` /
/// `notify_all`. `with cv:` is [`Condition::lock`]; waiting takes the
/// guard so the lock is released while blocked, as Python does.
pub struct Condition(Mutex<()>, Condvar);
impl Default for Condition {
    fn default() -> Self {
        Condition::new()
    }
}
impl core::fmt::Debug for Condition {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("<threading.Condition>")
    }
}
impl Condition {
    pub fn new() -> Condition {
        Condition(Mutex::new(()), Condvar::new())
    }
    /// `with cv:` -- acquire the underlying lock.
    pub fn lock(&self) -> MutexGuard<'_, ()> {
        self.0.lock()
    }
    /// `cv.wait(timeout=None)` -> `False` on timeout (`no_std`: always a
    /// timeout -- nothing else runs to notify).
    pub fn wait(&self, guard: &mut MutexGuard<'_, ()>, timeout: Option<f64>) -> bool {
        #[cfg(feature = "std")]
        {
            match timeout {
                None => {
                    self.1.wait(guard);
                    true
                }
                Some(secs) => !self
                    .1
                    .wait_for(guard, core::time::Duration::from_secs_f64(secs.max(0.0)))
                    .timed_out(),
            }
        }
        #[cfg(not(feature = "std"))]
        {
            let _ = (guard, timeout);
            false
        }
    }
    pub fn notify(&self) {
        self.1.notify_one();
    }
    pub fn notify_all(&self) {
        self.1.notify_all();
    }
}

/// `import threading`
#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)]
pub struct threading_mod;
pub static threading: threading_mod = threading_mod;
impl threading_mod {
    #[cfg(feature = "std")]
    pub fn Thread(&self, args: impl Into<Args>) -> Result<Thread> {
        Thread::new(args)
    }
    /// `threading.Timer(interval, function, args=None, kwargs=None)`: a
    /// thread that calls *function* after *interval* seconds (a plain
    /// `Thread` whose target sleeps first, as `Timer.run` does).
    #[cfg(feature = "std")]
    #[pysig("Timer(interval, function, args=None, kwargs=None)")]
    pub fn Timer(&self, args: impl Into<Args>) -> Result<Thread> {
        let secs = interval.as_f64().ok_or_else(|| {
            LailaError::type_error(alloc::format!(
                "'{}' object cannot be interpreted as a number",
                interval.type_name()
            ))
        })?;
        let function = function.clone();
        let call_args = if args.is_none() {
            Args::new()
        } else {
            let mut a = Args::new();
            a.extend_positional(args.clone())?;
            if !kwargs.is_none() {
                a.extend_keywords(kwargs.clone())?;
            }
            a
        };
        let target = crate::python::callable(move |_a: Args| -> Result<Arg> {
            std::thread::sleep(core::time::Duration::from_secs_f64(secs.max(0.0)));
            function.__call__(call_args.clone())
        });
        let mut a = Args::new();
        a.push_keyword("target", target);
        a.push_keyword("daemon", Arg::Bool(true));
        Thread::new(a)
    }
    pub fn Lock(&self) -> Lock {
        Lock::default()
    }
    pub fn RLock(&self) -> RLock {
        RLock::default()
    }
    pub fn Event(&self) -> Event {
        Event::default()
    }
    #[cfg(feature = "std")]
    pub fn Barrier(&self, args: impl Into<Args>) -> Result<Barrier> {
        Barrier::new(args)
    }
    /// `threading.BrokenBarrierError` (the class, for `except` clauses).
    pub fn BrokenBarrierError(&self) -> crate::python::errors::BrokenBarrierError {
        crate::python::errors::BrokenBarrierError
    }
    /// `threading.enumerate()`: the main thread plus every live `Thread`.
    #[cfg(feature = "std")]
    pub fn enumerate(&self, _args: impl Into<Args>) -> Arg {
        let mut v: Vec<Arg> = alloc::vec![self.main_thread().into_arg()];
        v.extend(
            REGISTRY
                .lock()
                .iter()
                .filter(|t| t.is_alive(Args::new()))
                .cloned()
                .map(IntoArg::into_arg),
        );
        // Native laila threads, as the `Thread` objects Python would list.
        let native: Vec<(u64, String)> = NATIVE.lock().clone();
        for (ident, name) in native {
            v.push(Thread::foreign(ident, name).into_arg());
        }
        v.into_arg()
    }
    #[cfg(feature = "std")]
    pub fn active_count(&self) -> i64 {
        1 + NATIVE.lock().len() as i64
            + REGISTRY
                .lock()
                .iter()
                .filter(|t| t.is_alive(Args::new()))
                .count() as i64
    }
    /// `no_std`: the single execution context.
    #[cfg(not(feature = "std"))]
    pub fn active_count(&self) -> i64 {
        1 + NATIVE_ALIVE.load(core::sync::atomic::Ordering::SeqCst) as i64
    }
    #[cfg(feature = "std")]
    pub fn get_ident(&self) -> i64 {
        thread_id_u64(std::thread::current().id()) as i64
    }
    /// `no_std`: the single execution context has id 0.
    #[cfg(not(feature = "std"))]
    pub fn get_ident(&self) -> i64 {
        0
    }
    pub fn get_native_id(&self) -> i64 {
        self.get_ident()
    }
    /// `threading.current_thread()`
    #[cfg(feature = "std")]
    pub fn current_thread(&self) -> Thread {
        let id = thread_id_u64(std::thread::current().id());
        if let Some(t) = REGISTRY
            .lock()
            .iter()
            .find(|t| *t.0.ident.lock() == Some(id))
        {
            return t.clone();
        }
        let main = self.main_thread();
        if *main.0.ident.lock() == Some(id) {
            return main;
        }
        // A native thread laila spawned (executor worker, taskforce loop /
        // dispatcher) or a foreign one: a `Thread` wrapper carrying the OS
        // thread's name and ident, like Python's `_DummyThread`.
        let t = Thread::new(Args::new()).expect("current thread");
        *t.0.name.lock() = std::thread::current().name().unwrap_or("Dummy").to_string();
        t.0.started
            .store(true, core::sync::atomic::Ordering::SeqCst);
        *t.0.ident.lock() = Some(id);
        t
    }
    #[cfg(feature = "std")]
    pub fn main_thread(&self) -> Thread {
        static MAIN: std::sync::OnceLock<Thread> = std::sync::OnceLock::new();
        MAIN.get_or_init(|| {
            let t = Thread::new(Args::new()).expect("main thread");
            *t.0.name.lock() = "MainThread".to_string();
            t.0.started
                .store(true, core::sync::atomic::Ordering::SeqCst);
            // The process' main thread is `ThreadId(1)` (the first handle
            // the runtime allocates). Resolving it this way keeps a first
            // call from a worker / libtest thread from adopting that
            // thread as "MainThread".
            let cur = std::thread::current();
            let id = if cur.name() == Some("main") {
                thread_id_u64(cur.id())
            } else {
                1
            };
            *t.0.ident.lock() = Some(id);
            t
        })
        .clone()
    }
}
