//! Abstract base task-force -- lifecycle management and a submission queue.
//!
//! A *task-force* is laila's name for a worker pool. The base nails down a
//! uniform [`TaskForceStatus`] lifecycle (`NOT_STARTED` -> `RUNNING` ->
//! [`PAUSED`] -> `STOPPED`), a process-wide registry of *live* taskforces
//! (swept by `laila.terminate`), the `submit` / `imap` contract and the
//! `with tf:` context-manager sugar. The only backend in the port is the
//! async thread pool (`PythonAsyncThreadPoolTaskForce`, Python's
//! `DefaultTaskForce`); one handle type, [`TaskForce`], carries it.
//!
//! // MERGE: `PythonProcessPoolTaskForce` (subprocess workers) is not
//! ported -- there is no interpreter to fork; `TaskForce.process_pool()`
//! raises `NotImplementedError`.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::atomic::definitions::HasIdentity;
use crate::basics::definitions::identifiable_object::Identity;
use crate::macros::strings::_TASK_FORCE_SCOPE;
use crate::python::object::PyObject;
use crate::python::sync::{Mutex, RLock};
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, PyArg, Result};

use super::async_thread_pool_executor::taskforce::Runtime;
use super::status::TaskForceStatus;

/// `_LIVE_TASKFORCES` -- every started, not yet shut down taskforce.
static LIVE: Mutex<Vec<TaskForce>> = Mutex::new(Vec::new());

/// Track *tf* in the process-wide live set (consumed by `laila.terminate`
/// to sweep taskforces never attached to a policy).
pub fn _register_live_taskforce(tf: &TaskForce) {
    let mut g = LIVE.lock();
    if !g.iter().any(|t| t.is(tf)) {
        g.push(tf.clone());
    }
}
/// Remove *tf* from the live set (idempotent).
pub fn _unregister_live_taskforce(tf: &TaskForce) {
    LIVE.lock().retain(|t| !t.is(tf));
}
/// A detached snapshot of the live set.
pub fn _live_taskforces_snapshot() -> Vec<TaskForce> {
    LIVE.lock().clone()
}

/// A queued submission: the (coroutine-function) task, its future and the
/// resolve chain snapshotted at submission.
pub(crate) struct Queued {
    pub task: Arg,
    pub fut: crate::policy::central::command::Future,
    pub chain: Arc<Vec<String>>,
}

pub struct TaskForceInner {
    pub(crate) identity: Mutex<Identity>,
    /// `self.atomic()`.
    pub(crate) lock: RLock,
    pub(crate) policy_id: Mutex<Arg>,
    pub(crate) rank: Mutex<i64>,
    pub(crate) status: Mutex<TaskForceStatus>,
    pub(crate) backend: Mutex<String>,
    pub(crate) num_workers: Mutex<usize>,
    pub(crate) max_async_per_thread: Mutex<usize>,
    pub(crate) sync_workers: Mutex<usize>,
    /// `_q`: the submission queue, in order (an `AtomicDict` in Python).
    pub(crate) q: Mutex<alloc::collections::VecDeque<(String, Queued)>>,
    /// The running backend (`None` before `start` / after `shutdown`).
    pub(crate) rt: Mutex<Option<Arc<Runtime>>>,
}

/// A taskforce (handle; clones alias one taskforce).
#[derive(Clone)]
pub struct TaskForce(pub(crate) Arc<TaskForceInner>);

fn int_field(cls: &str, name: &str, v: &Arg, default: i64, min: i64) -> Result<i64> {
    let n = match v {
        Arg::None => default,
        Arg::Int(i) => *i,
        Arg::Bool(b) => *b as i64,
        Arg::Float(f) if crate::_compat::fmath::fract(*f) == 0.0 => *f as i64,
        Arg::Str(s) => s.trim().parse::<i64>().map_err(|_| {
            crate::_compat::pydantic::validation_error(
                cls,
                name,
                "Input should be a valid integer, unable to parse string as an integer",
                "int_parsing",
                v,
            )
        })?,
        other => {
            return Err(crate::_compat::pydantic::validation_error(
                cls,
                name,
                "Input should be a valid integer",
                "int_type",
                other,
            ))
        }
    };
    if n < min {
        return Err(crate::_compat::pydantic::validation_error(
            cls,
            name,
            &alloc::format!("Input should be greater than or equal to {min}"),
            "greater_than_equal",
            v,
        ));
    }
    Ok(n)
}

impl TaskForce {
    pub const CLASS_NAME: &'static str = "PythonAsyncThreadPoolTaskForce";

    /// `PythonAsyncThreadPoolTaskForce(policy_id=None, rank=2, status=..,
    /// backend="async_threads", num_workers=.., max_async_per_thread=64,
    /// sync_workers=.., uuid=.., nickname=.., scopes=.., evolution=..)`;
    /// auto-started unless constructed `PAUSED`.
    pub fn new(args: impl Into<Args>) -> Result<TaskForce> {
        let mut a: Args = args.into();
        // The CLI-capable before-validator: fields not given explicitly
        // come from `laila.args.policy.central.command.taskforces.{gid}`
        // (`policy_id` and `status` are CLI-exempt).
        crate::basics::definitions::cli_capable::resolve_cli_fields_into(
            _TASK_FORCE_SCOPE,
            &[
                "rank",
                "backend",
                "num_workers",
                "max_async_per_thread",
                "sync_workers",
            ],
            &mut a,
        );
        Self::__init_resolved(a)
    }
    #[pysig(
        "__init__(*, policy_id=None, rank=2, status=None, backend='async_threads', num_workers=None, max_async_per_thread=64, sync_workers=None, uuid=None, nickname=None, scopes=None, evolution=None, global_id=None)"
    )]
    fn __init_resolved(args: impl Into<Args>) -> Result<TaskForce> {
        let cls = Self::CLASS_NAME;
        #[cfg(feature = "std")]
        let cpu = crate::python::stdlib::os
            .cpu_count()
            .as_i64()
            .unwrap_or(1)
            .max(1);
        #[cfg(not(feature = "std"))]
        let cpu: i64 = 1;
        let rank = int_field(cls, "rank", &rank, 2, 0)?;
        let num_workers = int_field(cls, "num_workers", &num_workers, cpu, 1)? as usize;
        let max_async_per_thread =
            int_field(cls, "max_async_per_thread", &max_async_per_thread, 64, 1)? as usize;
        let sync_workers =
            int_field(cls, "sync_workers", &sync_workers, (cpu * 2).max(4), 1)? as usize;
        let status = match &status {
            Arg::None => TaskForceStatus::NOT_STARTED,
            s => TaskForceStatus::from_arg(s).ok_or_else(|| {
                crate::_compat::pydantic::validation_error(
                    cls,
                    "status",
                    "Input should be 'not_started', 'running', 'paused', 'stopped' or 'crashed'",
                    "enum",
                    s,
                )
            })?,
        };
        let backend = match &backend {
            Arg::None => String::from("async_threads"),
            Arg::Str(s) => String::from(&**s),
            other => {
                return Err(crate::_compat::pydantic::validation_error(
                    cls,
                    "backend",
                    "Input should be a valid string",
                    "string_type",
                    other,
                ))
            }
        };
        let mut id = Args::new();
        id.push_keyword(
            "default_scopes",
            alloc::vec![Arg::Str(_TASK_FORCE_SCOPE.into())],
        );
        for (key, v) in [
            ("uuid", uuid),
            ("nickname", nickname),
            ("scopes", scopes),
            ("evolution", evolution),
            ("global_id", global_id),
        ] {
            if !v.is_none() {
                id.push_keyword(key, v);
            }
        }
        let tf = TaskForce(Arc::new(TaskForceInner {
            identity: Mutex::new(Identity::from_args(id)?),
            lock: RLock::new(),
            policy_id: Mutex::new(policy_id),
            rank: Mutex::new(rank),
            status: Mutex::new(status),
            backend: Mutex::new(backend),
            num_workers: Mutex::new(num_workers),
            max_async_per_thread: Mutex::new(max_async_per_thread),
            sync_workers: Mutex::new(sync_workers),
            q: Mutex::new(alloc::collections::VecDeque::new()),
            rt: Mutex::new(None),
        }));
        // model_post_init: auto-start when constructed NOT_STARTED.
        if tf.status() == TaskForceStatus::NOT_STARTED {
            tf.start(Args::new())?;
        }
        // The CLI-capable after-validator: the owning policy's mirror
        // entry is stale.
        let _ = crate::basics::definitions::cli_capable::_mark_environment_dirty(Args::from(
            tf.to_arg(),
        ));
        Ok(tf)
    }

    // ---------- identity ----------

    pub fn global_id(&self) -> String {
        self.0.identity.lock().global_id()
    }
    pub fn uuid(&self) -> String {
        self.0.identity.lock().uuid.clone()
    }
    pub fn is(&self, other: &TaskForce) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub fn policy_id(&self) -> Arg {
        self.0.policy_id.lock().clone()
    }
    pub fn rank(&self) -> i64 {
        *self.0.rank.lock()
    }
    pub fn status(&self) -> TaskForceStatus {
        *self.0.status.lock()
    }
    pub fn backend(&self) -> String {
        self.0.backend.lock().clone()
    }
    pub fn num_workers(&self) -> usize {
        *self.0.num_workers.lock()
    }
    pub fn max_async_per_thread(&self) -> usize {
        *self.0.max_async_per_thread.lock()
    }
    pub fn sync_workers(&self) -> usize {
        *self.0.sync_workers.lock()
    }
    /// `self.atomic()`.
    pub fn atomic(&self) -> crate::python::stdlib::RLock {
        crate::python::stdlib::RLock(self.0.lock.clone())
    }
    /// `_wraps_sync`: `Command.submit` wraps plain callables for this backend.
    pub fn _wraps_sync(&self) -> bool {
        true
    }

    // ---------- observability ----------

    /// `tf.queue_len`: submitted tasks not yet dispatched.
    pub fn queue_len(&self) -> usize {
        self.0.q.lock().len()
    }
    /// `len(tf)`.
    pub fn __len__(&self) -> usize {
        self.queue_len()
    }

    // ---------- lifecycle ----------

    /// `tf.start()`: idempotent; runs `_on_start`, flips to `RUNNING`,
    /// registers in the live set. (Takes `Args` like every Python-shaped
    /// method; `tf.start(())` from typed Rust.)
    pub fn start(&self, _args: impl Into<Args>) -> Result<()> {
        if self.status() == TaskForceStatus::RUNNING {
            return Ok(());
        }
        self._on_start()?;
        *self.0.status.lock() = TaskForceStatus::RUNNING;
        _register_live_taskforce(self);
        Ok(())
    }
    /// `tf.pause()`: quiesce without tearing down (no-op unless `RUNNING`).
    pub fn pause(&self) -> Result<()> {
        if self.status() != TaskForceStatus::RUNNING {
            return Ok(());
        }
        self._on_pause();
        *self.0.status.lock() = TaskForceStatus::PAUSED;
        Ok(())
    }
    /// `tf.shutdown(wait=True, cancel_pending=False)`: tear down; the
    /// status is always `STOPPED` afterwards and the live-set entry gone.
    #[pysig("shutdown(wait=True, cancel_pending=False)")]
    pub fn shutdown(&self, args: impl Into<Args>) -> Result<()> {
        if self.status() == TaskForceStatus::STOPPED {
            _unregister_live_taskforce(self);
            return Ok(());
        }
        let r = self._on_shutdown(wait.__bool__(), cancel_pending.__bool__());
        *self.0.status.lock() = TaskForceStatus::STOPPED;
        _unregister_live_taskforce(self);
        r
    }
    /// `with tf:` -- start and hand back the taskforce.
    pub fn __enter__(&self) -> Result<TaskForceGuard> {
        self.start(Args::new())?;
        Ok(TaskForceGuard(self.clone()))
    }
    /// `with tf:` exit -- `shutdown(wait=True)`.
    pub fn __exit__(&self) -> Result<()> {
        let mut a = Args::new();
        a.push_keyword("wait", true);
        self.shutdown(a)
    }

    // ---------- submit ----------

    /// `tf.submit(funcs, wait=False)`: one task -> its future; many -> a
    /// `GroupFuture`; `wait=True` -> the result(s), the futures released.
    #[pysig("submit(funcs, wait=False)")]
    pub fn submit(&self, args: impl Into<Args>) -> Result<Arg> {
        self.submit_impl(funcs, wait.__bool__())
    }
    /// `tf.imap(tasks)`: submit lazily, yielding a future per task.
    pub fn imap(
        &self,
        tasks: impl IntoArg,
    ) -> Result<Vec<crate::policy::central::command::Future>> {
        let mut out = Vec::new();
        for t in tasks.into_arg().__iter__()? {
            out.push(self._queue_submit(t)?);
        }
        Ok(out)
    }

    // ---------- dynamic layer ----------

    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "global_id" => Arg::Str(self.global_id().into()),
            "uuid" | "_uuid" => Arg::Str(self.uuid().into()),
            "scopes" | "_scopes" => self.0.identity.lock().scopes_arg(),
            "evolution" | "_evolution" => self.0.identity.lock().evolution_arg(),
            "policy_id" => self.policy_id(),
            "rank" => Arg::Int(self.rank()),
            "status" => self.status().into_arg(),
            "backend" => Arg::Str(self.backend().into()),
            "num_workers" => Arg::Int(self.num_workers() as i64),
            "max_async_per_thread" => Arg::Int(self.max_async_per_thread() as i64),
            "sync_workers" => Arg::Int(self.sync_workers() as i64),
            "queue_len" => Arg::Int(self.queue_len() as i64),
            "inflight" => Arg::Int(self.inflight() as i64),
            "parked" => Arg::Int(self.parked() as i64),
            "_wraps_sync" => Arg::Bool(true),
            "_loops" => self
                .loops()
                .into_iter()
                .map(|l| l.into_arg())
                .collect::<Vec<_>>()
                .into_arg(),
            #[cfg(feature = "std")]
            "_dispatcher" => self
                .runtime()
                .map(|rt| Arg::Object(rt.dispatcher_obj.clone()))
                .unwrap_or(Arg::None),
            "__class__" => Self::class_object().into_arg(),
            other => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'{}' object has no attribute '{other}'",
                    Self::CLASS_NAME
                )))
            }
        })
    }
    pub fn __setattr__(&self, name: &str, value: impl IntoArg) -> Result<()> {
        let value = value.into_arg();
        match name {
            "policy_id" => {
                *self.0.policy_id.lock() = value;
                Ok(())
            }
            "status" => {
                let st = TaskForceStatus::from_arg(&value).ok_or_else(|| {
                    LailaError::type_error(alloc::format!(
                        "status must be a TaskForceStatus, got {}",
                        value.type_name()
                    ))
                })?;
                *self.0.status.lock() = st;
                Ok(())
            }
            // The config fields are plain pydantic fields: assignable (a
            // test tunes `tf.num_workers = 16`; the running backend keeps
            // its pool until the next `start`).
            "rank" => {
                *self.0.rank.lock() = int_field(Self::CLASS_NAME, "rank", &value, 2, 0)?;
                Ok(())
            }
            "backend" => {
                *self.0.backend.lock() = value.__str__();
                Ok(())
            }
            "num_workers" => {
                *self.0.num_workers.lock() =
                    int_field(Self::CLASS_NAME, "num_workers", &value, 1, 1)? as usize;
                Ok(())
            }
            "max_async_per_thread" => {
                *self.0.max_async_per_thread.lock() =
                    int_field(Self::CLASS_NAME, "max_async_per_thread", &value, 64, 1)? as usize;
                Ok(())
            }
            "sync_workers" => {
                *self.0.sync_workers.lock() =
                    int_field(Self::CLASS_NAME, "sync_workers", &value, 4, 1)? as usize;
                Ok(())
            }
            other => Err(LailaError::attribute_error(alloc::format!(
                "'{}' object has no attribute '{other}'",
                Self::CLASS_NAME
            ))),
        }
    }
    pub fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let first = args.positional.first().cloned().unwrap_or(Arg::None);
        Ok(match name {
            "start" => {
                self.start(Args::new())?;
                Arg::None
            }
            "pause" => {
                self.pause()?;
                Arg::None
            }
            "shutdown" => {
                self.shutdown(args)?;
                Arg::None
            }
            "submit" => self.submit(args)?,
            "_queue_submit" => Arg::Future(self._queue_submit(first)?),
            "imap" => self
                .imap(first)?
                .into_iter()
                .map(Arg::Future)
                .collect::<Vec<_>>()
                .into_arg(),
            "__len__" => Arg::Int(self.__len__() as i64),
            "__enter__" => {
                // The dynamic protocol pairs it with an explicit `__exit__`.
                let g = self.__enter__()?;
                let tf = g.0.clone();
                core::mem::forget(g);
                tf.into_arg()
            }
            "__exit__" => {
                self.__exit__()?;
                Arg::None
            }
            "atomic" => self.atomic().into_arg(),
            "_is_own_worker_thread" => Arg::Bool(self._is_own_worker_thread()),
            other => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'{}' object has no attribute '{other}'",
                    Self::CLASS_NAME
                )))
            }
        })
    }
    pub fn class_object() -> crate::python::PyClass {
        crate::python::PyClass::with_ctor(
            Self::CLASS_NAME,
            "laila.policy.central.command.taskforce.async_thread_pool_executor.taskforce",
            Arc::new(|a: Args| TaskForce::new(a).map(IntoArg::into_arg)),
        )
    }
    /// The taskforce inside an `Arg`, if it is one.
    pub fn of(x: &Arg) -> Option<TaskForce> {
        match x {
            Arg::Object(o) => o.as_any().downcast_ref::<TaskForce>().cloned(),
            _ => None,
        }
    }
}

impl core::fmt::Debug for TaskForce {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "{}({}, status={})",
            Self::CLASS_NAME,
            self.global_id(),
            self.status().__str__()
        )
    }
}
/// `PythonAsyncThreadPoolTaskForce` as a value (`assertIs(type(tf), ..)`).
impl crate::python::builtins::ClassValue for TaskForce {
    fn class_value() -> Arg {
        Self::class_object().into_arg()
    }
}

impl PyObject for TaskForce {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    /// `a is b`: identity of the shared handle, not of the boxing.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<TaskForce>()
            .is_some_and(|o| self.is(o))
    }
    fn type_name(&self) -> &'static str {
        Self::CLASS_NAME
    }
    fn __repr__(&self) -> String {
        alloc::format!("{self:?}")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<TaskForce>()
            .is_some_and(|o| o.is(self))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    /// `type(tf)` / `tf.__class__`: the `PythonAsyncThreadPoolTaskForce` class.
    fn __class__(&self) -> Option<crate::python::PyClass> {
        Some(Self::class_object())
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        TaskForce::__getattr__(self, name)
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        TaskForce::__setattr__(self, name, value)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        TaskForce::__call_method__(self, name, args)
    }
    fn __len__(&self) -> Result<usize> {
        Ok(TaskForce::__len__(self))
    }
    fn __atomic_lock__(&self) -> Option<RLock> {
        Some(self.0.lock.clone())
    }
    /// The `laila.args.environment` mirror fields (`policy_id`, `status`
    /// and the queue counters are CLI-exempt).
    fn __cli_fields__(&self) -> Option<Vec<(String, Arg)>> {
        let identity = self.0.identity.lock();
        Some(alloc::vec![
            (String::from("rank"), Arg::Int(self.rank())),
            (String::from("backend"), Arg::Str(self.backend().into())),
            (
                String::from("num_workers"),
                Arg::Int(self.num_workers() as i64)
            ),
            (
                String::from("max_async_per_thread"),
                Arg::Int(self.max_async_per_thread() as i64)
            ),
            (
                String::from("sync_workers"),
                Arg::Int(self.sync_workers() as i64)
            ),
            (String::from("evolution"), identity.evolution_arg()),
            (
                String::from("global_id"),
                Arg::Str(identity.global_id().into())
            ),
            (String::from("scopes"), identity.scopes_arg()),
            (
                String::from("uuid"),
                Arg::Str(identity.uuid.as_str().into())
            ),
        ])
    }
}
impl IntoArg for TaskForce {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for TaskForce {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}

/// The handle yielded by `with tf as inner:` -- the taskforce itself
/// (`inner is tf`); leaving the block, normally or by an exception, is
/// `shutdown(wait=True)`.
pub struct TaskForceGuard(TaskForce);

impl TaskForceGuard {
    /// `tf.__exit__()` ahead of the drop.
    pub fn __exit__(self) -> Result<()> {
        let tf = self.0.clone();
        core::mem::forget(self);
        tf.__exit__()
    }
}
impl core::ops::Deref for TaskForceGuard {
    type Target = TaskForce;
    fn deref(&self) -> &TaskForce {
        &self.0
    }
}
impl Drop for TaskForceGuard {
    fn drop(&mut self) {
        let _ = self.0.__exit__();
    }
}
impl PyArg for TaskForceGuard {
    fn to_arg(&self) -> Arg {
        self.0.to_arg()
    }
}
impl IntoArg for TaskForceGuard {
    fn into_arg(self) -> Arg {
        self.0.to_arg()
    }
}
impl HasIdentity for TaskForce {
    fn global_id(&self) -> String {
        TaskForce::global_id(self)
    }
    fn uuid(&self) -> String {
        TaskForce::uuid(self)
    }
}
impl crate::python::builtins::IsInstance for TaskForce {
    fn isinstance(x: &Arg) -> bool {
        TaskForce::of(x).is_some()
    }
}

/// `_LAILA_IDENTIFIABLE_TASK_FORCE` as a class marker (`isinstance`).
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct _LAILA_IDENTIFIABLE_TASK_FORCE;
impl crate::python::builtins::IsInstance for _LAILA_IDENTIFIABLE_TASK_FORCE {
    fn isinstance(x: &Arg) -> bool {
        TaskForce::of(x).is_some()
    }
}
impl IntoArg for _LAILA_IDENTIFIABLE_TASK_FORCE {
    fn into_arg(self) -> Arg {
        crate::python::PyClass::named(
            "_LAILA_IDENTIFIABLE_TASK_FORCE",
            "laila.policy.central.command.taskforce.base",
        )
        .into_arg()
    }
}
