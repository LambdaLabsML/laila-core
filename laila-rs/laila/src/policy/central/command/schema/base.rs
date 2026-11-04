//! Base schema for the central command sub-system.
//!
//! Central command is the policy's submission hub. It owns a registry of
//! taskforces (keyed by gid) plus a designated `alpha_taskforce` that
//! receives work when the caller does not specify a target, an
//! `internal_taskforce` laila's own machinery runs on, and a per-thread
//! *guarantee stack* used by `with laila.guarantee:` to record every
//! future created inside a scope so the scope can block on them all at
//! exit. [`Command::submit`] is the single entry point for all queued
//! work: sync callables are auto-wrapped into coroutine functions so the
//! runner sees a uniform contract.

use alloc::collections::BTreeMap;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::atomic::definitions::HasIdentity;
use crate::basics::definitions::identifiable_object::Identity;
use crate::macros::strings::_CENTRAL_COMMAND_SCOPE;
use crate::python::object::PyObject;
use crate::python::sync::Mutex;
use crate::python::{pysig, Arg, Args, ExcKind, IntoArg, LailaError, PyArg, Result};

use super::exceptions::{_check_no_pending_submit_owner, ensure_coroutine_function};
use super::future::future::future::Future;
use crate::policy::central::command::taskforce::TaskForce;

/// One open `laila.guarantee` scope: the futures created while it was
/// open, keyed by gid in creation order.
pub type GuaranteeScope = Vec<(String, Future)>;

pub struct CommandInner {
    pub(crate) identity: Mutex<Identity>,
    /// `taskforces`: `{gid: TaskForce}` in registration order.
    pub(crate) taskforces: Mutex<Vec<(String, TaskForce)>>,
    pub(crate) alpha_taskforce: Mutex<Option<String>>,
    pub(crate) internal_taskforce: Mutex<Option<String>>,
    pub(crate) policy_id: Mutex<Arg>,
    /// `_guarantee_local.stack`, per thread.
    pub(crate) guarantee: Mutex<BTreeMap<i64, Vec<GuaranteeScope>>>,
    /// `laila.build` coalescing: entry gid -> in-flight future.
    pub(crate) inflight_builds: Mutex<BTreeMap<String, Future>>,
    /// `_inflight_lock`: serialises the check-and-submit of `laila.build`.
    pub(crate) inflight_lock: Mutex<()>,
}

/// `_LAILA_IDENTIFIABLE_CENTRAL_COMMAND` (handle; clones alias one command).
#[derive(Clone)]
pub struct Command(pub(crate) Arc<CommandInner>);

fn thread_key() -> i64 {
    super::exceptions::current_thread_ident()
}

impl Command {
    pub const CLASS_NAME: &'static str = "_LAILA_IDENTIFIABLE_CENTRAL_COMMAND";

    /// `DefaultCentralCommand(policy_id=.., taskforces=None, alpha_taskforce=None,
    /// internal_taskforce=None, uuid=.., nickname=.., scopes=.., evolution=..)`.
    ///
    /// `model_post_init`: with no taskforces registered, two
    /// `DefaultTaskForce`s are created -- the **alpha** (`rank=2`) for user
    /// work and the **internal** (`rank=1`) for laila's own machinery, so
    /// a flood of user jobs cannot starve the fetches they depend on.
    #[pysig(
        "__init__(*, policy_id=None, taskforces=None, alpha_taskforce=None, internal_taskforce=None, uuid=None, nickname=None, scopes=None, evolution=None, global_id=None)"
    )]
    pub fn new(args: impl Into<Args>) -> Result<Command> {
        let mut id = Args::new();
        id.push_keyword(
            "default_scopes",
            alloc::vec![Arg::Str(_CENTRAL_COMMAND_SCOPE.into())],
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
        let mut registry: Vec<(String, TaskForce)> = Vec::new();
        match &taskforces {
            Arg::None => {}
            Arg::Dict(_) => {
                for v in taskforces.values()?.__iter__()? {
                    let tf = TaskForce::of(&v).ok_or_else(|| {
                        LailaError::type_error(alloc::format!(
                            "taskforces values must be taskforces, got {}",
                            v.type_name()
                        ))
                    })?;
                    registry.push((tf.global_id(), tf));
                }
            }
            other => {
                return Err(crate::_compat::pydantic::validation_error(
                    Self::CLASS_NAME,
                    "taskforces",
                    "Input should be a valid dictionary",
                    "dict_type",
                    other,
                ))
            }
        }
        let cmd = Command(Arc::new(CommandInner {
            identity: Mutex::new(Identity::from_args(id)?),
            taskforces: Mutex::new(registry),
            alpha_taskforce: Mutex::new(alpha_taskforce.as_str().map(String::from)),
            internal_taskforce: Mutex::new(internal_taskforce.as_str().map(String::from)),
            policy_id: Mutex::new(policy_id),
            guarantee: Mutex::new(BTreeMap::new()),
            inflight_builds: Mutex::new(BTreeMap::new()),
            inflight_lock: Mutex::new(()),
        }));
        cmd.model_post_init()?;
        Ok(cmd)
    }

    fn model_post_init(&self) -> Result<()> {
        if self.0.taskforces.lock().is_empty() {
            let mut a = Args::new();
            a.push_keyword("policy_id", self.policy_id());
            a.push_keyword("rank", 2);
            let alpha = crate::macros::defaults::DefaultTaskForce::new(a)?;
            let mut a = Args::new();
            a.push_keyword("policy_id", self.policy_id());
            a.push_keyword("rank", 1);
            let internal = crate::macros::defaults::DefaultTaskForce::new(a)?;
            let mut reg = self.0.taskforces.lock();
            reg.push((alpha.global_id(), alpha.clone()));
            reg.push((internal.global_id(), internal.clone()));
            *self.0.alpha_taskforce.lock() = Some(alpha.global_id());
            *self.0.internal_taskforce.lock() = Some(internal.global_id());
        }
        let first = self.0.taskforces.lock().first().map(|(g, _)| g.clone());
        {
            let mut alpha = self.0.alpha_taskforce.lock();
            if alpha.as_ref().is_none_or(|g| self.taskforce(g).is_none()) {
                *alpha = first.clone();
            }
        }
        {
            let alpha = self.0.alpha_taskforce.lock().clone();
            let mut internal = self.0.internal_taskforce.lock();
            if internal
                .as_ref()
                .is_none_or(|g| self.taskforce(g).is_none())
            {
                *internal = alpha;
            }
        }
        Ok(())
    }

    // ---------- identity / fields ----------

    pub fn global_id(&self) -> String {
        self.0.identity.lock().global_id()
    }
    pub fn uuid(&self) -> String {
        self.0.identity.lock().uuid.clone()
    }
    /// `command.scopes`
    pub fn scopes(&self) -> Arg {
        self.0.identity.lock().scopes_arg()
    }
    pub fn is(&self, other: &Command) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub fn policy_id(&self) -> Arg {
        self.0.policy_id.lock().clone()
    }
    /// `command.alpha_taskforce` -- the default taskforce's gid.
    pub fn alpha_taskforce(&self) -> Arg {
        match self.0.alpha_taskforce.lock().as_ref() {
            Some(g) => Arg::Str(g.as_str().into()),
            None => Arg::None,
        }
    }
    /// `command.internal_taskforce` -- the internal taskforce's gid.
    pub fn internal_taskforce(&self) -> Arg {
        match self.0.internal_taskforce.lock().as_ref() {
            Some(g) => Arg::Str(g.as_str().into()),
            None => Arg::None,
        }
    }
    /// `command.taskforces` -- `{gid: TaskForce}`, a *live* dict view of the
    /// registry: `command.taskforces[gid] = tf` registers and
    /// `command.taskforces.pop(gid, None)` unregisters, like Python's
    /// mutable field.
    pub fn taskforces(&self) -> Arg {
        Arg::Object(Arc::new(TaskForceRegistry(self.clone())))
    }
    /// `{gid: TaskForce}` as a plain dict snapshot.
    pub fn taskforces_snapshot(&self) -> Arg {
        let mut d = crate::python::PyDictBuilder::new();
        for (g, tf) in self.0.taskforces.lock().iter() {
            d.insert(g.as_str(), tf.clone());
        }
        d.finish()
    }
    /// `command.taskforces.pop(gid)`: unregister; `None` when absent.
    pub fn remove_taskforce(&self, gid: &str) -> Option<TaskForce> {
        let mut reg = self.0.taskforces.lock();
        let i = reg.iter().position(|(g, _)| g == gid)?;
        Some(reg.remove(i).1)
    }
    /// `command.taskforces[gid]`.
    pub fn taskforce(&self, gid: &str) -> Option<TaskForce> {
        self.0
            .taskforces
            .lock()
            .iter()
            .find(|(g, _)| g == gid)
            .map(|(_, tf)| tf.clone())
    }
    /// The alpha taskforce itself.
    pub fn alpha(&self) -> Option<TaskForce> {
        let gid = self.0.alpha_taskforce.lock().clone()?;
        self.taskforce(&gid)
    }
    /// The internal taskforce itself.
    pub fn internal(&self) -> Option<TaskForce> {
        let gid = self.0.internal_taskforce.lock().clone()?;
        self.taskforce(&gid)
    }
    /// `command.add_taskforce(tf)`: register (re-registering overwrites).
    pub fn add_taskforce(&self, taskforce: impl IntoArg) -> Result<()> {
        let v = taskforce.into_arg();
        let tf = TaskForce::of(&v).ok_or_else(|| {
            LailaError::attribute_error(alloc::format!(
                "'{}' object has no attribute 'global_id'",
                v.type_name()
            ))
        })?;
        let gid = tf.global_id();
        {
            let mut reg = self.0.taskforces.lock();
            match reg.iter_mut().find(|(g, _)| *g == gid) {
                Some(slot) => slot.1 = tf,
                None => reg.push((gid, tf)),
            }
        }
        // The owning policy's mirror entry is stale.
        let _ = crate::basics::definitions::cli_capable::_mark_environment_dirty(Args::from(
            self.to_arg(),
        ));
        Ok(())
    }

    // ---------- guarantee scopes ----------

    /// `_guarantee_enter()`: push a fresh scope on this thread's stack.
    pub fn _guarantee_enter(&self) {
        self.0
            .guarantee
            .lock()
            .entry(thread_key())
            .or_default()
            .push(Vec::new());
    }
    /// `_guarantee_exit()`: pop the top scope and return its futures
    /// (`[]` when no scope is open).
    pub fn _guarantee_exit(&self) -> Vec<Future> {
        let mut g = self.0.guarantee.lock();
        let Some(stack) = g.get_mut(&thread_key()) else {
            return Vec::new();
        };
        let out = stack
            .pop()
            .map(|scope| scope.into_iter().map(|(_, f)| f).collect())
            .unwrap_or_default();
        if stack.is_empty() {
            g.remove(&thread_key());
        }
        out
    }
    /// Record *future* in every guarantee scope open on this thread.
    pub fn _register_future_with_active_guarantees(&self, future: &Future) {
        let mut g = self.0.guarantee.lock();
        let Some(stack) = g.get_mut(&thread_key()) else {
            return;
        };
        let gid = future.global_id();
        for scope in stack.iter_mut() {
            match scope.iter_mut().find(|(g, _)| *g == gid) {
                Some(slot) => slot.1 = future.clone(),
                None => scope.push((gid.clone(), future.clone())),
            }
        }
    }
    /// The futures of scope *index* (0 = outermost) of thread *thread*'s
    /// guarantee stack, read without popping (the async guarantee's
    /// watcher polls it from its own thread).
    pub(crate) fn _guarantee_scope_futures(&self, thread: i64, index: usize) -> Vec<Future> {
        self.0
            .guarantee
            .lock()
            .get(&thread)
            .and_then(|stack| stack.get(index))
            .map(|scope| scope.iter().map(|(_, f)| f.clone()).collect())
            .unwrap_or_default()
    }
    /// `len(stack)` for this thread (diagnostics).
    pub fn _guarantee_depth(&self) -> usize {
        self.0
            .guarantee
            .lock()
            .get(&thread_key())
            .map(Vec::len)
            .unwrap_or(0)
    }
    /// `_guarantee_stack()`: a snapshot of this thread's open guarantee
    /// scopes, each a `{global_id: future}` dict (innermost last).
    pub fn _guarantee_stack(&self) -> Arg {
        let g = self.0.guarantee.lock();
        let scopes: Vec<Arg> = g
            .get(&thread_key())
            .map(|stack| {
                stack
                    .iter()
                    .map(|scope| {
                        let mut d = crate::python::PyDictBuilder::new();
                        for (gid, f) in scope {
                            d.insert(gid.as_str(), Arg::Future(f.clone()));
                        }
                        d.finish()
                    })
                    .collect()
            })
            .unwrap_or_default();
        scopes.into_arg()
    }

    // ---------- build coalescing ----------

    /// `command._inflight_lock` -- held around the check-and-submit.
    pub fn inflight_lock(&self) -> &Mutex<()> {
        &self.0.inflight_lock
    }
    /// The in-flight build future for *gid*, if any.
    pub fn _inflight_build(&self, gid: &str) -> Option<Future> {
        self.0.inflight_builds.lock().get(gid).cloned()
    }
    pub fn _set_inflight_build(&self, gid: &str, fut: Option<Future>) {
        let mut g = self.0.inflight_builds.lock();
        match fut {
            Some(f) => {
                g.insert(gid.into(), f);
            }
            None => {
                g.remove(gid);
            }
        }
    }

    // ---------- submit / shutdown ----------

    /// `command.submit(tasks, wait=False, *, taskforce_id=None)`: submit
    /// zero-argument callables to a taskforce.
    ///
    /// One task and `wait=False` -> its `Future`; many -> a `GroupFuture`;
    /// `wait=True` -> the result (one task) or the list of results. Sync
    /// callables are auto-wrapped into coroutine functions whose body is
    /// offloaded to the taskforce's sync executor. Raises
    /// `NestedCommandSubmitError` inside a `@no_command_submit` function
    /// or when a task running on taskforce *A* submits to a taskforce of
    /// strictly higher rank; `KeyError` for an unknown `taskforce_id`.
    #[pysig("submit(tasks, wait=False, *, taskforce_id=None)")]
    pub fn submit(&self, args: impl Into<Args>) -> Result<Arg> {
        _check_no_pending_submit_owner()?;
        let tf_gid = match &taskforce_id {
            Arg::None => self.0.alpha_taskforce.lock().clone().unwrap_or_default(),
            other => other.__str__(),
        };
        let target = self
            .taskforce(&tf_gid)
            .ok_or_else(|| LailaError::key_error(crate::python::pyrepr::str_repr(&tf_gid)))?;
        self._check_rank(&target)?;
        let wrapped: Vec<Arg> = if target._wraps_sync() {
            tasks
                .__iter__()?
                .into_iter()
                .map(ensure_coroutine_function)
                .collect()
        } else {
            tasks.__iter__()?
        };
        target.submit_impl(wrapped.into_arg(), wait.__bool__())
    }
    /// Reject a submission that would point *up* the taskforce ranks
    /// (only when the caller runs inside a slot of one of our taskforces).
    fn _check_rank(&self, target: &TaskForce) -> Result<()> {
        let Some(slot) = super::parking::_CURRENT_SLOT.get() else {
            return Ok(());
        };
        let source_gid = slot.tf.global_id();
        if slot.tf.ptr_id() == target.ptr_id() || self.taskforce(&source_gid).is_none() {
            return Ok(());
        }
        let src_rank = slot.tf.rank();
        let dst_rank = target.rank();
        if dst_rank > src_rank {
            return Err(LailaError::new(
                ExcKind::NestedCommandSubmitError,
                alloc::format!(
                    "Task running on taskforce {source_gid} (rank {src_rank}) may not submit to \
                     taskforce {} (rank {dst_rank}): submissions must target equal or lower rank \
                     so the taskforce dependency graph stays acyclic.",
                    target.global_id()
                ),
            ));
        }
        Ok(())
    }
    /// `command.shutdown(wait=True, cancel_pending=False)`: shut every
    /// registered taskforce down (a failure does not stop the others;
    /// the first error propagates afterwards).
    #[pysig("shutdown(wait=True, cancel_pending=False)")]
    pub fn shutdown(&self, args: impl Into<Args>) -> Result<()> {
        let tfs: Vec<TaskForce> = self
            .0
            .taskforces
            .lock()
            .iter()
            .map(|(_, tf)| tf.clone())
            .collect();
        let mut first_err = None;
        for tf in tfs {
            let mut a = Args::new();
            a.push_keyword("wait", wait.clone());
            a.push_keyword("cancel_pending", cancel_pending.clone());
            if let Err(e) = tf.shutdown(a) {
                first_err.get_or_insert(e);
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    // ---------- dynamic layer ----------

    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "global_id" => Arg::Str(self.global_id().into()),
            "uuid" | "_uuid" => Arg::Str(self.uuid().into()),
            "scopes" | "_scopes" => self.0.identity.lock().scopes_arg(),
            "evolution" | "_evolution" => self.0.identity.lock().evolution_arg(),
            "policy_id" => self.policy_id(),
            "taskforces" => self.taskforces(),
            "alpha_taskforce" => self.alpha_taskforce(),
            "internal_taskforce" => self.internal_taskforce(),
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
            "alpha_taskforce" => {
                *self.0.alpha_taskforce.lock() = value.as_str().map(String::from);
                Ok(())
            }
            "internal_taskforce" => {
                *self.0.internal_taskforce.lock() = value.as_str().map(String::from);
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
            "submit" => self.submit(args)?,
            "shutdown" => {
                self.shutdown(args)?;
                Arg::None
            }
            "add_taskforce" => {
                self.add_taskforce(first)?;
                Arg::None
            }
            "_guarantee_enter" => {
                self._guarantee_enter();
                Arg::None
            }
            "_guarantee_exit" => self
                ._guarantee_exit()
                .into_iter()
                .map(Arg::Future)
                .collect::<Vec<_>>()
                .into_arg(),
            "_register_future_with_active_guarantees" => {
                if let Arg::Future(f) = &first {
                    self._register_future_with_active_guarantees(f);
                }
                Arg::None
            }
            "_guarantee_stack" => self._guarantee_stack(),
            "__await__" => {
                return Err(LailaError::not_implemented(""));
            }
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
            "laila.policy.central.command.schema.base",
            Arc::new(|a: Args| Command::new(a).map(IntoArg::into_arg)),
        )
    }
    /// The command inside an `Arg`, if it is one.
    pub fn of(x: &Arg) -> Option<Command> {
        match x {
            Arg::Object(o) => o.as_any().downcast_ref::<Command>().cloned(),
            _ => None,
        }
    }
}

impl core::fmt::Debug for Command {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}({})", Self::CLASS_NAME, self.global_id())
    }
}
impl PyObject for Command {
    fn as_any(&self) -> &dyn core::any::Any {
        self
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
            .downcast_ref::<Command>()
            .is_some_and(|o| o.is(self))
    }
    /// Two handles of one command are the same Python object.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        self.__eq__(other)
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        Command::__getattr__(self, name)
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        Command::__setattr__(self, name, value)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        Command::__call_method__(self, name, args)
    }
    /// The `laila.args.environment` mirror fields (`taskforces` is walked
    /// by `build_environment` itself; `policy_id` is CLI-exempt).
    fn __cli_fields__(&self) -> Option<Vec<(String, Arg)>> {
        let identity = self.0.identity.lock();
        Some(alloc::vec![
            (String::from("alpha_taskforce"), self.alpha_taskforce()),
            (
                String::from("internal_taskforce"),
                self.internal_taskforce()
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
impl IntoArg for Command {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for Command {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl HasIdentity for Command {
    fn global_id(&self) -> String {
        Command::global_id(self)
    }
    fn uuid(&self) -> String {
        Command::uuid(self)
    }
}
impl crate::python::builtins::IsInstance for Command {
    fn isinstance(x: &Arg) -> bool {
        Command::of(x).is_some()
    }
}

/// `_LAILA_IDENTIFIABLE_CENTRAL_COMMAND` as a class marker.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct _LAILA_IDENTIFIABLE_CENTRAL_COMMAND;
impl _LAILA_IDENTIFIABLE_CENTRAL_COMMAND {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<Command> {
        Command::new(args)
    }
}
impl crate::python::builtins::IsInstance for _LAILA_IDENTIFIABLE_CENTRAL_COMMAND {
    fn isinstance(x: &Arg) -> bool {
        Command::of(x).is_some()
    }
}
impl IntoArg for _LAILA_IDENTIFIABLE_CENTRAL_COMMAND {
    fn into_arg(self) -> Arg {
        Command::class_object().into_arg()
    }
}
impl crate::python::builtins::CallValue for _LAILA_IDENTIFIABLE_CENTRAL_COMMAND {
    fn call_value(&self, args: Args) -> Result<Arg> {
        Command::new(args).map(IntoArg::into_arg)
    }
}

/// `command.taskforces`: a live `{gid: TaskForce}` view of the registry
/// with the `dict` protocol (`[]`, `[] =`, `pop`, `get`, `keys`, `values`,
/// `items`, `len`, `in`, iteration).
pub struct TaskForceRegistry(Command);

impl TaskForceRegistry {
    fn key(k: &Arg) -> Result<String> {
        match k {
            Arg::Str(s) => Ok(s.to_string()),
            other => Err(LailaError::key_error(other.__repr__())),
        }
    }
    fn snapshot(&self) -> Arg {
        self.0.taskforces_snapshot()
    }
}

impl PyObject for TaskForceRegistry {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "dict"
    }
    fn __bases__(&self) -> &'static [&'static str] {
        &["dict"]
    }
    fn __repr__(&self) -> String {
        self.snapshot().__repr__()
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<TaskForceRegistry>()
            .is_some_and(|o| self.snapshot() == o.snapshot())
    }
    fn __eq_arg__(&self, other: &Arg) -> bool {
        match other {
            Arg::Object(o) => self.__eq__(&**o),
            other => self.snapshot() == *other,
        }
    }
    fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error("unhashable type: 'dict'"))
    }
    fn __len__(&self) -> Result<usize> {
        Ok(self.0 .0.taskforces.lock().len())
    }
    fn __contains__(&self, item: &Arg) -> Result<bool> {
        Ok(match item {
            Arg::Str(s) => self.0.taskforce(s).is_some(),
            _ => false,
        })
    }
    fn __iter__(&self) -> Result<Vec<Arg>> {
        Ok(self
            .0
             .0
            .taskforces
            .lock()
            .iter()
            .map(|(g, _)| Arg::Str(g.as_str().into()))
            .collect())
    }
    fn __getitem__(&self, key: &Arg) -> Result<Arg> {
        let k = Self::key(key)?;
        self.0
            .taskforce(&k)
            .map(IntoArg::into_arg)
            .ok_or_else(|| LailaError::key_error(key.__repr__()))
    }
    fn __setitem__(&self, key: &Arg, value: Arg) -> Result<()> {
        let k = Self::key(key)?;
        let tf = TaskForce::of(&value).ok_or_else(|| {
            LailaError::type_error(alloc::format!(
                "taskforces values must be taskforces, got {}",
                value.type_name()
            ))
        })?;
        let mut reg = self.0 .0.taskforces.lock();
        match reg.iter_mut().find(|(g, _)| *g == k) {
            Some(slot) => slot.1 = tf,
            None => reg.push((k, tf)),
        }
        Ok(())
    }
    fn __delitem__(&self, key: &Arg) -> Result<()> {
        let k = Self::key(key)?;
        self.0
            .remove_taskforce(&k)
            .map(|_| ())
            .ok_or_else(|| LailaError::key_error(key.__repr__()))
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "keys" => self.snapshot().keys(args),
            "values" => self.snapshot().values(),
            "items" => self.snapshot().items(),
            "copy" => Ok(self.snapshot()),
            "get" => {
                let key = args.positional.first().cloned().ok_or_else(|| {
                    LailaError::type_error("get expected at least 1 argument, got 0")
                })?;
                match self.__getitem__(&key) {
                    Ok(v) => Ok(v),
                    Err(_) => Ok(args.positional.get(1).cloned().unwrap_or(Arg::None)),
                }
            }
            "pop" => {
                let key = args.positional.first().cloned().ok_or_else(|| {
                    LailaError::type_error("pop expected at least 1 argument, got 0")
                })?;
                let k = Self::key(&key)?;
                match self.0.remove_taskforce(&k) {
                    Some(tf) => Ok(tf.into_arg()),
                    None => match args.positional.get(1) {
                        Some(default) => Ok(default.clone()),
                        None => Err(LailaError::key_error(key.__repr__())),
                    },
                }
            }
            "clear" => {
                self.0 .0.taskforces.lock().clear();
                Ok(Arg::None)
            }
            other => Err(LailaError::attribute_error(alloc::format!(
                "'dict' object has no attribute '{other}'"
            ))),
        }
    }
}
