//! `laila.policy` -- the policy runtime.
//!
//! A *policy* is the unit of ownership for everything stateful in laila:
//! storage pools, task-forces, futures and the manifests that drive memory
//! routing. [`Policy`] bundles its central memory (the pool router + alpha
//! pool), its central command (the taskforces, the guarantee scopes) and
//! the `future_bank` -- the registry every future self-registers into on
//! construction and stays in until the holder calls `release()`.
//!
//! This module also carries the process-wide registries of
//! `laila/__init__.py`: the activated local policies
//! ([`_local_policies`]), every constructed one held weakly
//! ([`_live_policies`]), the active policy ([`get_active_policy`] /
//! [`activate_policy`]) and the bank walkers futures resolve through
//! ([`_future_bank_policies`], [`_owning_local_policy`]).
//!
//! `central.communication` (peers, RPC, morph mode) lives in
//! [`central::communication`]; the active policy may be a *remote* proxy
//! (morph mode), see [`ActivePolicy`].

pub mod central;
pub mod environment;
pub mod schema;

use alloc::string::String;
use alloc::sync::{Arc, Weak};
use alloc::vec::Vec;

use crate::atomic::definitions::HasIdentity;
use crate::basics::definitions::identifiable_object::Identity;
use crate::data::Pool;
use crate::macros::strings::{_DEFAULT_POOL_NICKNAME, _POLICY_SCOPE};
use crate::python::object::PyObject;
use crate::python::sync::RwLock;
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, PyArg, Result};

use central::command::schema::base::Command;
use central::command::Future;
use central::memory::router::PoolRouter;
use central::memory::Memory;

/// `_LAILA_IDENTIFIABLE_CENTRAL_MEMORY`'s state: the router and the alpha
/// pool gid (`pool_router` / `alpha_pool` fields).
pub struct CentralMemoryState {
    pub pool_router: PoolRouter,
    pub alpha_pool: RwLock<String>,
    /// The memory's instance `__dict__` (`object.__setattr__(mem, ..)`).
    pub attrs: crate::python::PyAttrs,
}

impl CentralMemoryState {
    /// `model_post_init`: a default router (which auto-registers an
    /// in-memory `DefaultPool` under `_memory`) and the alpha pool
    /// adopted from that nickname.
    pub fn new() -> Result<CentralMemoryState> {
        let pool_router = crate::macros::defaults::DefaultPoolRouter::new(Args::new())?;
        let nicknames = pool_router.pools_nicknames();
        let alpha = if nicknames.__contains__(_DEFAULT_POOL_NICKNAME)? {
            nicknames.__getitem__(_DEFAULT_POOL_NICKNAME)?.__str__()
        } else {
            let alpha = crate::macros::defaults::DefaultPool::new(Args::new())?;
            let mut a = Args::new();
            a.push_positional(Arg::Pool(alpha.clone()));
            a.push_keyword("affinity", 1);
            a.push_keyword("pool_nickname", _DEFAULT_POOL_NICKNAME);
            pool_router.extend(a)?;
            alpha.global_id()
        };
        Ok(CentralMemoryState {
            pool_router,
            alpha_pool: RwLock::new(alpha),
            attrs: crate::python::PyAttrs::default(),
        })
    }
}

/// The state behind a [`Policy`] handle.
pub struct PolicyState {
    pub identity: RwLock<Identity>,
    /// `central.memory`'s state; swappable (`policy.central.memory = m`).
    pub memory: RwLock<Arc<CentralMemoryState>>,
    /// `central.command` (set once in `model_post_init`).
    pub command: RwLock<Option<Command>>,
    /// `central.communication` (set once in `model_post_init`).
    #[cfg(feature = "std")]
    pub communication: RwLock<Option<central::communication::Communication>>,
    /// Instance attributes stuck on the policy from the outside
    /// (`object.__setattr__(policy, "echo", fn)`): read by `getattr` --
    /// and so reachable over RPC -- like any other attribute.
    pub attrs: crate::python::object::PyAttrs,
    /// `future_bank`: `{gid: future}` -- strong references, nothing is
    /// evicted automatically.
    pub future_bank: Arg,
}

/// A policy (handle; clones alias one policy).
#[derive(Clone)]
pub struct Policy(pub Arc<PolicyState>);

impl Policy {
    /// `_LAILA_IDENTIFIABLE_POLICY(uuid=.., nickname=.., scopes=.., evolution=..)`:
    /// a fresh policy with its own central memory and command; registered
    /// (weakly) in [`_live_policies`] so futures stamped with its gid land
    /// in its bank before it is activated.
    pub fn new(args: Args) -> Result<Policy> {
        let mut id = Args::new();
        id.push_keyword(
            "default_scopes",
            alloc::vec![Arg::Str(_POLICY_SCOPE.into())],
        );
        for key in ["uuid", "nickname", "scopes", "evolution", "global_id"] {
            let v = args.get(key);
            if !v.is_none() {
                id.push_keyword(key, v);
            }
        }
        let policy = Policy(Arc::new(PolicyState {
            identity: RwLock::new(Identity::from_args(id)?),
            memory: RwLock::new(Arc::new(CentralMemoryState::new()?)),
            command: RwLock::new(None),
            #[cfg(feature = "std")]
            communication: RwLock::new(None),
            attrs: crate::python::object::PyAttrs::new(),
            future_bank: crate::python::PyDictBuilder::new().finish(),
        }));
        // model_post_init: wire the default command (its taskforces carry
        // our gid so their futures register in our bank).
        let mut a = Args::new();
        a.push_keyword("policy_id", Arg::Str(policy.global_id().into()));
        let command = Command::new(a)?;
        *policy.0.command.write() = Some(command);
        // .. and the default communication hub, back-referenced to us.
        #[cfg(feature = "std")]
        {
            let mut a = Args::new();
            a.push_keyword("policy_id", Arg::Str(policy.global_id().into()));
            let comm = central::communication::Communication::new(a)?;
            comm.set_local_policy(Some(&policy));
            *policy.0.communication.write() = Some(comm);
        }
        _register_live_policy(&policy);
        // The CLI-capable after-validator: the mirror entry is stale.
        policy._mark_environment_dirty();
        Ok(policy)
    }
    /// `_mark_environment_dirty(policy)`: schedule this policy's
    /// `laila.args.environment.policies[<gid>]` dump for the next read.
    pub fn _mark_environment_dirty(&self) {
        let _ = crate::basics::definitions::cli_capable::_mark_environment_dirty(Args::from(
            self.to_arg(),
        ));
    }
    /// `policy.global_id`
    pub fn global_id(&self) -> String {
        self.0.identity.read().global_id()
    }
    /// `policy.uuid`
    pub fn uuid(&self) -> String {
        self.0.identity.read().uuid.clone()
    }
    /// `policy.scopes`
    pub fn scopes(&self) -> Arg {
        self.0.identity.read().scopes_arg()
    }
    /// `policy.central`
    pub fn central(&self) -> Central {
        Central(self.clone())
    }
    /// `policy.central.memory` (bound to this policy).
    pub fn memory(&self) -> Memory {
        Memory::of(self.clone())
    }
    /// `policy.central.command`.
    pub fn command(&self) -> Command {
        self.0
            .command
            .read()
            .clone()
            .expect("policy command is wired in model_post_init")
    }
    /// `policy.central.communication`.
    #[cfg(feature = "std")]
    pub fn communication(&self) -> central::communication::Communication {
        self.0
            .communication
            .read()
            .clone()
            .expect("policy communication is wired in model_post_init")
    }
    /// `policy.central.communication` when it may have been unset
    /// (`central.communication = None`).
    #[cfg(feature = "std")]
    pub fn communication_opt(&self) -> Option<central::communication::Communication> {
        self.0.communication.read().clone()
    }
    /// `policy.central.communication = None`: drop the hub (the policy
    /// then runs without peers; `terminate()` skips the stop).
    #[cfg(feature = "std")]
    pub fn clear_communication(&self) {
        *self.0.communication.write() = None;
    }
    /// `policy.central.communication = comm`: adopt *comm* as this
    /// policy's hub (back-referenced to us).
    #[cfg(feature = "std")]
    pub fn set_communication(&self, comm: central::communication::Communication) {
        comm.set_local_policy(Some(self));
        if comm.policy_id().is_none() {
            comm.set_policy_id(Arg::Str(self.global_id().into()));
        }
        *self.0.communication.write() = Some(comm);
    }
    /// `policy.future_bank` -- the live `{gid: future}` dict.
    pub fn future_bank(&self) -> Arg {
        self.0.future_bank.clone()
    }
    /// The central memory's state (router + alpha pool gid).
    pub fn memory_state(&self) -> Arc<CentralMemoryState> {
        self.0.memory.read().clone()
    }
    /// `policy.central.memory = memory`: adopt *state* as this policy's
    /// central memory (the detached `_LAILA_IDENTIFIABLE_CENTRAL_MEMORY()`
    /// a test built, say).
    pub fn set_memory_state(&self, state: Arc<CentralMemoryState>) {
        *self.0.memory.write() = state;
    }
    /// `policy.central.memory.pool_router`
    pub fn pool_router(&self) -> PoolRouter {
        self.memory_state().pool_router.clone()
    }
    /// `policy.central.memory.alpha_pool` -- the alpha pool's global id.
    pub fn alpha_pool_gid(&self) -> String {
        self.memory_state().alpha_pool.read().clone()
    }
    /// The alpha pool itself (`pool_router.pools[alpha_pool]`).
    pub fn alpha_pool(&self) -> Pool {
        let gid = self.alpha_pool_gid();
        match self.pool_router().pools().__getitem__(gid.as_str()) {
            Ok(Arg::Pool(p)) => p,
            _ => {
                // The default nickname was removed and no fallback exists:
                // re-adopt a fresh in-memory pool, as ``model_post_init`` did.
                let alpha =
                    crate::macros::defaults::DefaultPool::new(Args::new()).expect("default pool");
                let mut a = Args::new();
                a.push_positional(Arg::Pool(alpha.clone()));
                a.push_keyword("affinity", 1);
                a.push_keyword("pool_nickname", _DEFAULT_POOL_NICKNAME);
                let _ = self.pool_router().extend(a);
                *self.memory_state().alpha_pool.write() = alpha.global_id();
                alpha
            }
        }
    }
    /// `memory._resolve_pool_ref(pool_ref)`: a pool object, gid string, or
    /// nickname -> a live pool instance (`KeyError` / `TypeError` as Python).
    pub fn resolve_pool(&self, pool_ref: &Arg) -> Result<Pool> {
        self.memory()._resolve_pool_ref(pool_ref)
    }
    /// Python `is`.
    pub fn is(&self, other: &Policy) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    /// `policy.extend(new_pool)`: register a pool with central memory.
    pub fn extend(&self, new_pool: impl IntoArg) -> Result<()> {
        self.memory().extend(Args::from(new_pool.into_arg()))
    }
    /// `policy.remember(global_id, *, global_fetch=False, pool_subset=None,
    /// hint=None)`: a blocking single-entry recall (`None` when missing).
    #[pysig("remember(global_id, *, global_fetch=False, pool_subset=None, hint=None, _remote_called=False)")]
    pub fn remember(&self, args: impl Into<Args>) -> Result<Arg> {
        let _ = _remote_called;
        if global_fetch.__bool__() {
            return Err(LailaError::not_implemented(""));
        }
        if !pool_subset.is_none() || !hint.is_none() {
            return Err(LailaError::not_implemented(
                "pool_subset / hint routing is not implemented yet",
            ));
        }
        let mut a = Args::from(alloc::vec![global_id].into_arg());
        a.push_keyword("persist", false);
        let fut = self.memory().remember(a)?;
        let entry = match fut.wait(Args::new()) {
            Ok(e) => e,
            Err(e) if e.kind.is_subclass_of(crate::python::ExcKind::KeyError) => {
                return Ok(Arg::None)
            }
            Err(e) => return Err(e),
        };
        Ok(match entry {
            Arg::List(_) => entry.__iter__()?.into_iter().next().unwrap_or(Arg::None),
            other => other,
        })
    }
    /// `policy.memorize(entries, *, require_local_update=False,
    /// require_global_update=False)`: a blocking write.
    #[pysig("memorize(entries, *, require_local_update=False, require_global_update=False)")]
    pub fn memorize(&self, args: impl Into<Args>) -> Result<()> {
        let _ = (require_local_update, require_global_update);
        self.memory()
            .memorize(Args::from(entries))?
            .wait(Args::new())?;
        Ok(())
    }

    // ---------- RPC helpers for remote future introspection ----------

    fn bank_future(&self, future_id: &str) -> Result<Future> {
        match self.future_bank().get(future_id)? {
            Arg::Future(f) => Ok(f),
            _ => Err(LailaError::key_error(crate::python::pyrepr::str_repr(
                &alloc::format!("Future {future_id} not in bank"),
            ))),
        }
    }
    /// `_get_future_status(future_id)`: the status's wire value.
    pub fn _get_future_status(&self, future_id: impl IntoArg) -> Result<Arg> {
        let future_id = future_id.into_arg().__str__();
        let future_id = future_id.as_str();
        let status = self.bank_future(future_id)?.status();
        Ok(match status.__getattr__("value") {
            Ok(v) => v,
            Err(_) => status,
        })
    }
    /// `_get_future_exception(future_id)`: `None` or `{"type", "message"}`.
    pub fn _get_future_exception(&self, future_id: impl IntoArg) -> Result<Arg> {
        let future_id = future_id.into_arg().__str__();
        let future_id = future_id.as_str();
        let exc = self.bank_future(future_id)?.exception();
        Ok(match exc {
            Arg::Exception(e) => {
                let mut d = crate::python::PyDictBuilder::new();
                d.insert("type", e.kind.name());
                d.insert("message", e.__str__());
                d.finish()
            }
            _ => Arg::None,
        })
    }
    /// `_get_future_result_id(future_id)`: the result entry's gid (a list
    /// of them for a group).
    pub fn _get_future_result_id(&self, future_id: impl IntoArg) -> Result<Arg> {
        let future_id = future_id.into_arg().__str__();
        let future_id = future_id.as_str();
        let fut = self.bank_future(future_id)?;
        if fut.kind() != central::command::FutureKind::Group {
            return fut.result_global_id();
        }
        let mut ids = Vec::new();
        for fid in fut.future_ids().__iter__()? {
            match self.future_bank().get(fid.__str__().as_str())? {
                Arg::Future(child) if child.kind() != central::command::FutureKind::Group => {
                    ids.push(child.result_global_id()?)
                }
                _ => ids.push(Arg::None),
            }
        }
        Ok(ids.into_arg())
    }
    /// `_wait_future(future_id, timeout=None)`: block, then the result gid.
    pub fn _wait_future(&self, future_id: impl IntoArg, timeout: Option<f64>) -> Result<Arg> {
        let future_id = future_id.into_arg().__str__();
        let future_id = future_id.as_str();
        let fut = self.bank_future(future_id)?;
        let mut a = Args::new();
        a.push_keyword("timeout", timeout);
        fut.wait(a)?;
        if fut.kind() == central::command::FutureKind::Group {
            return Ok(Arg::None);
        }
        fut.result_global_id()
    }
    /// A local future's result entry(ies), wire-serialized
    /// (`Entry.serialize(transformation_base64)`; a list of per-child blobs
    /// for a group) so a `RemoteFuture` on a peer rebuilds the real entry.
    fn _serialize_future_result(&self, fut: &central::command::Future) -> Result<Arg> {
        fn one(e: &Arg) -> Result<Arg> {
            match e {
                Arg::Entry(entry) => {
                    let mut a = Args::new();
                    a.push_keyword(
                        "transformations",
                        crate::entry::transformation_base64().into_arg(),
                    );
                    entry.serialize(a)
                }
                _ => Ok(Arg::None),
            }
        }
        if fut.kind() == central::command::FutureKind::Group {
            let mut out = Vec::new();
            for e in fut.result()?.__iter__()? {
                out.push(one(&e)?);
            }
            return Ok(out.into_arg());
        }
        one(&fut.result()?)
    }
    /// `_get_future_result_entry(future_id)`: the result entry(ies),
    /// serialized; blocks until the future resolves.
    pub fn _get_future_result_entry(&self, future_id: impl IntoArg) -> Result<Arg> {
        let future_id = future_id.into_arg().__str__();
        let fut = self.bank_future(future_id.as_str())?;
        self._serialize_future_result(&fut)
    }
    /// `_wait_future_entry(future_id, timeout=None)`: block, then the
    /// result entry(ies), serialized.
    pub fn _wait_future_entry(&self, future_id: impl IntoArg, timeout: Option<f64>) -> Result<Arg> {
        let future_id = future_id.into_arg().__str__();
        let fut = self.bank_future(future_id.as_str())?;
        let mut a = Args::new();
        a.push_keyword("timeout", timeout);
        fut.wait(a)?;
        self._serialize_future_result(&fut)
    }
    /// `policy.<name>` for the dynamic layer.
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "global_id" => Arg::Str(self.global_id().into()),
            "uuid" | "_uuid" => Arg::Str(self.uuid().into()),
            "central" => self.central().into_arg(),
            "future_bank" => self.future_bank(),
            "_scopes" | "scopes" => self.0.identity.read().scopes_arg(),
            "evolution" | "_evolution" => self.0.identity.read().evolution_arg(),
            "__class__" => schema::base::_LAILA_IDENTIFIABLE_POLICY::class_object().into_arg(),
            other => {
                if let Some(v) = self.0.attrs.get(other) {
                    return Ok(v);
                }
                return Err(LailaError::attribute_error(alloc::format!(
                    "'_LAILA_IDENTIFIABLE_POLICY' object has no attribute '{other}'"
                )));
            }
        })
    }
    /// `object.__setattr__(policy, name, value)`: an instance attribute
    /// (`uuid` / `scopes` / `evolution` re-key the identity).
    pub fn __setattr__(&self, name: &str, value: impl IntoArg) -> Result<()> {
        let value = value.into_arg();
        match name {
            "uuid" | "_uuid" => {
                self.0.identity.write().uuid = value.__str__();
                return Ok(());
            }
            "scopes" | "_scopes" => {
                self.0.identity.write().scopes =
                    value.__iter__()?.iter().map(|s| s.__str__()).collect();
                return Ok(());
            }
            "evolution" | "_evolution" => {
                self.0.identity.write().evolution = value.as_i64();
                return Ok(());
            }
            _ => {}
        }
        self.0.attrs.__setattr__(name, value)
    }
    /// `policy.<name>(*args, **kwargs)` for the dynamic layer.
    pub fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        if let Some(v) = self.0.attrs.get(name) {
            return v.__call__(args);
        }
        let first = args.positional.first().cloned().unwrap_or(Arg::None);
        let fid = || first.__str__();
        Ok(match name {
            "extend" => {
                self.extend(first)?;
                Arg::None
            }
            "remember" => self.remember(args)?,
            "memorize" => {
                self.memorize(args)?;
                Arg::None
            }
            "_get_future_status" => self._get_future_status(fid())?,
            "_get_future_exception" => self._get_future_exception(fid())?,
            "_get_future_result_id" => self._get_future_result_id(fid())?,
            "_wait_future" => {
                let timeout = args
                    .positional
                    .get(1)
                    .cloned()
                    .unwrap_or_else(|| args.get("timeout"));
                self._wait_future(fid(), timeout.as_f64())?
            }
            "_get_future_result_entry" => self._get_future_result_entry(fid())?,
            "_wait_future_entry" => {
                let timeout = args
                    .positional
                    .get(1)
                    .cloned()
                    .unwrap_or_else(|| args.get("timeout"));
                self._wait_future_entry(fid(), timeout.as_f64())?
            }
            other => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'_LAILA_IDENTIFIABLE_POLICY' object has no attribute '{other}'"
                )))
            }
        })
    }
    /// The policy inside an `Arg`, if it is one.
    pub fn of(x: &Arg) -> Option<Policy> {
        match x {
            Arg::Object(o) => o.as_any().downcast_ref::<Policy>().cloned(),
            _ => None,
        }
    }
}

impl core::fmt::Debug for Policy {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.global_id())
    }
}

impl PyObject for Policy {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "_LAILA_IDENTIFIABLE_POLICY"
    }
    fn __repr__(&self) -> String {
        self.global_id()
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Policy>()
            .is_some_and(|o| o.global_id() == self.global_id())
    }
    fn __hash__(&self) -> Result<i64> {
        Arg::Str(self.global_id().into()).__hash__()
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        Policy::__getattr__(self, name)
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        Policy::__setattr__(self, name, value)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        Policy::__call_method__(self, name, args)
    }
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Policy>()
            .is_some_and(|o| o.is(self))
    }
    /// The `laila.args.environment` mirror fields (`central` is walked
    /// by `build_environment` itself).
    fn __cli_fields__(&self) -> Option<Vec<(String, Arg)>> {
        Some(alloc::vec![
            (
                String::from("evolution"),
                self.0.identity.read().evolution_arg()
            ),
            (String::from("global_id"), Arg::Str(self.global_id().into())),
            (String::from("scopes"), self.0.identity.read().scopes_arg()),
            (String::from("uuid"), Arg::Str(self.uuid().into())),
        ])
    }
}
impl IntoArg for Policy {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for Policy {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl HasIdentity for Policy {
    fn global_id(&self) -> String {
        Policy::global_id(self)
    }
    fn uuid(&self) -> String {
        Policy::uuid(self)
    }
}

/// `policy.central`
#[derive(Clone)]
pub struct Central(pub Policy);

impl Central {
    /// `policy.central.memory`
    pub fn memory(&self) -> Memory {
        self.0.memory()
    }
    /// `policy.central.command`
    pub fn command(&self) -> Command {
        self.0.command()
    }
    /// `policy.central.communication`
    #[cfg(feature = "std")]
    pub fn communication(&self) -> central::communication::Communication {
        self.0.communication()
    }
    /// `central.<name>`
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "memory" => Ok(self.memory().into_arg()),
            "command" => Ok(self.command().into_arg()),
            #[cfg(feature = "std")]
            "communication" => Ok(self
                .0
                .communication_opt()
                .map(IntoArg::into_arg)
                .unwrap_or(Arg::None)),
            "logic" => Ok(Arg::None),
            #[cfg(not(feature = "std"))]
            "communication" => Ok(Arg::None),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'Central' object has no attribute '{other}'"
            ))),
        }
    }
    /// `central.memory = memory`: the policy adopts the memory's state
    /// (router and alpha pool); the handle then reads through the policy.
    pub fn __setattr__(&self, name: &str, value: impl IntoArg) -> Result<()> {
        let value = value.into_arg();
        match name {
            "memory" => match value.downcast_ref::<Memory>() {
                Some(m) => {
                    self.0.set_memory_state(m.state_arc());
                    Ok(())
                }
                None => Err(LailaError::type_error(alloc::format!(
                    "central.memory must be a _LAILA_IDENTIFIABLE_CENTRAL_MEMORY, got {}",
                    value.type_name()
                ))),
            },
            #[cfg(feature = "std")]
            "communication" if value.is_none() => {
                self.0.clear_communication();
                Ok(())
            }
            #[cfg(feature = "std")]
            "communication" => match central::communication::Communication::of(&value) {
                Some(c) => {
                    self.0.set_communication(c);
                    Ok(())
                }
                None => Err(LailaError::type_error(alloc::format!(
                    "central.communication must be a _LAILA_IDENTIFIABLE_COMMUNICATION, got {}",
                    value.type_name()
                ))),
            },
            other => Err(LailaError::attribute_error(alloc::format!(
                "'Central' object has no attribute '{other}'"
            ))),
        }
    }
}
impl core::fmt::Debug for Central {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "<central of {}>", self.0.global_id())
    }
}
impl PyObject for Central {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "Central"
    }
    fn __repr__(&self) -> String {
        alloc::format!("{self:?}")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Central>()
            .is_some_and(|o| o.0.is(&self.0))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0 .0) as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        Central::__getattr__(self, name)
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        Central::__setattr__(self, name, value)
    }
}
impl IntoArg for Central {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for Central {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}

// ---------------------------------------------------------------------------
// process-wide registries (laila/__init__.py)

/// `laila._local_policies`: activated policies, by gid, in activation order.
static LOCAL_POLICIES: RwLock<Vec<(String, Policy)>> = RwLock::new(Vec::new());
/// `laila._live_policies`: every constructed policy, held weakly.
static LIVE_POLICIES: RwLock<Vec<(String, Weak<PolicyState>)>> = RwLock::new(Vec::new());
/// `laila._active_policy_gid`.
static ACTIVE_GID: RwLock<Option<String>> = RwLock::new(None);

fn _register_live_policy(policy: &Policy) {
    let gid = policy.global_id();
    let mut g = LIVE_POLICIES.write();
    g.retain(|(k, w)| k != &gid && w.strong_count() > 0);
    g.push((gid, Arc::downgrade(&policy.0)));
}

/// `laila._local_policies` as `(gid, policy)` pairs (a snapshot).
pub fn _local_policies() -> Vec<(String, Policy)> {
    LOCAL_POLICIES.read().clone()
}

/// `laila._local_policies[gid] = policy` -- remove *gid* from the local
/// registry (`del` / `pop`); the removed policy, if any.
fn _remove_local_policy(gid: &str) -> Option<Policy> {
    let mut g = LOCAL_POLICIES.write();
    let i = g.iter().position(|(k, _)| k == gid)?;
    Some(g.remove(i).1)
}

/// `laila._local_policies` as a live dict view: reads see the registry,
/// `[gid] = policy` registers (without activating), `del` / `pop` /
/// `clear` unregister. Compares equal to a plain `{gid: policy}` dict.
#[derive(Clone, Default)]
pub struct LocalPolicies;

impl LocalPolicies {
    fn key(k: &Arg) -> Result<String> {
        match k {
            Arg::Str(s) => Ok(String::from(&**s)),
            other => other
                .__getattr__("global_id")
                .ok()
                .and_then(|g| g.as_str().map(String::from))
                .ok_or_else(|| {
                    LailaError::type_error(alloc::format!(
                        "policy registry keys are gid strings, got {}",
                        other.type_name()
                    ))
                }),
        }
    }
    /// `dict(laila._local_policies)`
    pub fn snapshot(&self) -> Arg {
        let mut d = crate::python::PyDictBuilder::new();
        for (gid, p) in _local_policies() {
            d.insert(gid, p.into_arg());
        }
        d.finish()
    }
}

impl PyObject for LocalPolicies {
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
            .downcast_ref::<LocalPolicies>()
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
        Ok(LOCAL_POLICIES.read().len())
    }
    fn __contains__(&self, item: &Arg) -> Result<bool> {
        let k = Self::key(item)?;
        Ok(LOCAL_POLICIES.read().iter().any(|(g, _)| *g == k))
    }
    fn __iter__(&self) -> Result<Vec<Arg>> {
        Ok(LOCAL_POLICIES
            .read()
            .iter()
            .map(|(g, _)| Arg::Str(g.as_str().into()))
            .collect())
    }
    fn __getitem__(&self, key: &Arg) -> Result<Arg> {
        let k = Self::key(key)?;
        LOCAL_POLICIES
            .read()
            .iter()
            .find(|(g, _)| *g == k)
            .map(|(_, p)| p.clone().into_arg())
            .ok_or_else(|| LailaError::key_error(key.__repr__()))
    }
    fn __setitem__(&self, key: &Arg, value: Arg) -> Result<()> {
        let k = Self::key(key)?;
        let p = Policy::of(&value).ok_or_else(|| {
            LailaError::type_error(alloc::format!(
                "_local_policies values must be policies, got {}",
                value.type_name()
            ))
        })?;
        {
            let mut g = LOCAL_POLICIES.write();
            match g.iter_mut().find(|(g, _)| *g == k) {
                Some(slot) => slot.1 = p.clone(),
                None => g.push((k, p.clone())),
            }
        }
        p._mark_environment_dirty();
        Ok(())
    }
    fn __delitem__(&self, key: &Arg) -> Result<()> {
        let k = Self::key(key)?;
        _remove_local_policy(&k)
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
                match _remove_local_policy(&k) {
                    Some(p) => Ok(p.into_arg()),
                    None => match args.positional.get(1) {
                        Some(default) => Ok(default.clone()),
                        None => Err(LailaError::key_error(key.__repr__())),
                    },
                }
            }
            "clear" => {
                LOCAL_POLICIES.write().clear();
                Ok(Arg::None)
            }
            other => Err(LailaError::attribute_error(alloc::format!(
                "'dict' object has no attribute '{other}'"
            ))),
        }
    }
}
impl IntoArg for LocalPolicies {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
/// `laila._live_policies` as `(gid, policy)` pairs (a snapshot of the
/// still-alive ones).
pub fn _live_policies() -> Vec<(String, Policy)> {
    LIVE_POLICIES
        .read()
        .iter()
        .filter_map(|(g, w)| w.upgrade().map(|s| (g.clone(), Policy(s))))
        .collect()
}
/// `_local_policies.get(gid) or _live_policies.get(gid)`.
pub fn _local_policy_or_live(gid: &str) -> Option<Policy> {
    if let Some((_, p)) = LOCAL_POLICIES.read().iter().find(|(g, _)| g == gid) {
        return Some(p.clone());
    }
    LIVE_POLICIES
        .read()
        .iter()
        .find(|(g, _)| g == gid)
        .and_then(|(_, w)| w.upgrade().map(Policy))
}
/// `laila._active_policy_gid`.
pub fn _active_policy_gid() -> Option<String> {
    ACTIVE_GID.read().clone()
}
/// `laila._active_policy_gid = gid | None`: rebinding the module global
/// (a test's reset -- the next `get_active_policy()` constructs a fresh
/// `DefaultPolicy` when it is `None`). The policies themselves stay
/// registered, as in Python.
pub fn _set_active_policy_gid(gid: Option<String>) {
    *ACTIVE_GID.write() = gid;
}

/// `laila.get_active_policy()`'s value: a local policy or, in *morph
/// mode* (a `RemotePolicyProxy` was activated), the peer proxy. Typed
/// callers reach the local half through [`ActivePolicy::local`]; the
/// dynamic layer (`.central.memory.remember(..)`) works on either.
#[derive(Clone)]
pub enum ActivePolicy {
    Local(Policy),
    Remote(Arg),
}

impl ActivePolicy {
    /// `active.global_id`
    pub fn global_id(&self) -> String {
        match self {
            ActivePolicy::Local(p) => p.global_id(),
            ActivePolicy::Remote(proxy) => proxy
                .__getattr__("global_id")
                .map(|g| g.__str__())
                .unwrap_or_default(),
        }
    }
    /// The local policy (`None` in morph mode).
    pub fn local(&self) -> Option<Policy> {
        match self {
            ActivePolicy::Local(p) => Some(p.clone()),
            ActivePolicy::Remote(_) => None,
        }
    }
    /// `isinstance(active, RemotePolicyProxy)`
    pub fn is_remote(&self) -> bool {
        matches!(self, ActivePolicy::Remote(_))
    }
    /// `active.central` (typed; morph mode has no local central).
    pub fn central(&self) -> Result<Central> {
        match self {
            ActivePolicy::Local(p) => Ok(p.central()),
            ActivePolicy::Remote(_) => Err(LailaError::runtime_error(
                "the active policy is a remote proxy (morph mode); use the dynamic layer",
            )),
        }
    }
    /// `active.future_bank`
    pub fn future_bank(&self) -> Result<Arg> {
        match self {
            ActivePolicy::Local(p) => Ok(p.future_bank()),
            ActivePolicy::Remote(proxy) => proxy.__getattr__("future_bank"),
        }
    }
    /// `active.uuid`
    pub fn uuid(&self) -> Result<String> {
        match self {
            ActivePolicy::Local(p) => Ok(p.uuid()),
            ActivePolicy::Remote(proxy) => proxy.__getattr__("uuid").map(|u| u.__str__()),
        }
    }
    /// `active.scopes`
    pub fn scopes(&self) -> Result<Arg> {
        match self {
            ActivePolicy::Local(p) => Ok(p.scopes()),
            ActivePolicy::Remote(proxy) => proxy.__getattr__("scopes"),
        }
    }
    /// `active._get_future_result_id(future_id)`
    pub fn _get_future_result_id(&self, future_id: impl IntoArg) -> Result<Arg> {
        match self {
            ActivePolicy::Local(p) => p._get_future_result_id(future_id),
            ActivePolicy::Remote(proxy) => {
                let mut args = Args::new();
                args.push_positional(future_id);
                proxy.__call_method__("_get_future_result_id", args)
            }
        }
    }
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        self.to_arg().__getattr__(name)
    }
    pub fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        self.to_arg().__call_method__(name, args)
    }
    /// Python `is`.
    pub fn is(&self, other: &Arg) -> bool {
        self.to_arg().is(other)
    }
}
impl core::fmt::Debug for ActivePolicy {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.to_arg().__repr__())
    }
}
impl IntoArg for ActivePolicy {
    fn into_arg(self) -> Arg {
        match self {
            ActivePolicy::Local(p) => p.into_arg(),
            ActivePolicy::Remote(a) => a,
        }
    }
}
impl PyArg for ActivePolicy {
    fn to_arg(&self) -> Arg {
        self.clone().into_arg()
    }
}
impl PartialEq for ActivePolicy {
    fn eq(&self, other: &ActivePolicy) -> bool {
        self.to_arg() == other.to_arg()
    }
}

fn active_local() -> Option<Policy> {
    match ACTIVE_GID.read().as_ref() {
        Some(gid) => LOCAL_POLICIES
            .read()
            .iter()
            .find(|(g, _)| g == gid)
            .map(|(_, p)| p.clone()),
        None => None,
    }
}

#[cfg(feature = "std")]
fn active_remote() -> Option<Arg> {
    let gid = ACTIVE_GID.read().clone()?;
    central::communication::_remote_policy(&gid)
}
#[cfg(not(feature = "std"))]
fn active_remote() -> Option<Arg> {
    None
}

/// The local policy that *owns the link* to the active peer proxy (morph
/// mode): the proxy's communication object belongs to exactly one local
/// policy.
#[cfg(feature = "std")]
fn proxy_owner(proxy: &Arg) -> Option<Policy> {
    let proxy = central::communication::proxy::RemotePolicyProxy::of(proxy)?;
    proxy.communication().ok()?.local_policy()
}
#[cfg(not(feature = "std"))]
fn proxy_owner(_proxy: &Arg) -> Option<Policy> {
    None
}

/// `laila.get_active_policy()` -- the active policy as the *local* policy
/// that owns newly-created futures (`laila._get_active_local_policy()`): a
/// `DefaultPolicy` is created and activated on first use; in morph mode
/// the local policy owning the link to the peer (else the only local one,
/// else the first). The proxy itself is [`get_active_policy_any`].
pub fn get_active_policy() -> Policy {
    // One thread at a time creates (and activates) the implicit default
    // policy, the others wait for it (re-entrant: constructing a policy
    // may itself ask for the active one).
    static CREATING: crate::python::sync::Lazy<crate::python::sync::RLock> =
        crate::python::sync::Lazy::new(crate::python::sync::RLock::new);
    let _ = CREATING.acquire(None);
    let out = match ACTIVE_GID.read().is_some() {
        true => active_local()
            .or_else(|| active_remote().and_then(|p| proxy_owner(&p)))
            .or_else(|| {
                let locals = LOCAL_POLICIES.read();
                locals.first().map(|(_, p)| p.clone())
            }),
        false => None,
    };
    let out = out.unwrap_or_else(|| {
        let fresh = Policy::new(Args::new()).expect("default policy");
        activate_policy(&fresh);
        fresh
    });
    let _ = CREATING.release();
    out
}

/// `laila.get_active_policy()` -- the active policy: the local policy, or
/// the peer proxy in morph mode.
pub fn get_active_policy_any() -> Result<ActivePolicy> {
    if ACTIVE_GID.read().is_none() {
        return Ok(ActivePolicy::Local(get_active_policy()));
    }
    if let Some(p) = active_local() {
        return Ok(ActivePolicy::Local(p));
    }
    if let Some(proxy) = active_remote() {
        return Ok(ActivePolicy::Remote(proxy));
    }
    let gid = ACTIVE_GID.read().clone().unwrap_or_default();
    Err(LailaError::key_error(crate::python::pyrepr::str_repr(&gid)))
}

/// `laila.activate_policy(policy)` -- make *policy* the active one and
/// register it in `_local_policies`.
pub fn activate_policy(policy: &Policy) {
    let gid = policy.global_id();
    {
        let mut g = LOCAL_POLICIES.write();
        match g.iter_mut().find(|(k, _)| *k == gid) {
            Some(slot) => slot.1 = policy.clone(),
            None => g.push((gid.clone(), policy.clone())),
        }
    }
    *ACTIVE_GID.write() = Some(gid);
    // Lazy: the dump happens on the next read of `laila.args.environment`.
    policy._mark_environment_dirty();
}

/// `laila.activate_policy(policy)` for a policy *or* a `RemotePolicyProxy`
/// (morph mode: the proxy's gid becomes active, nothing is registered).
pub fn activate_policy_arg(policy: &Arg) -> Result<()> {
    if let Some(p) = Policy::of(policy) {
        activate_policy(&p);
        return Ok(());
    }
    #[cfg(feature = "std")]
    if central::communication::proxy::RemotePolicyProxy::of(policy).is_some() {
        let gid = policy.__getattr__("global_id")?.__str__();
        *ACTIVE_GID.write() = Some(gid);
        return Ok(());
    }
    match policy.__getattr__("global_id") {
        Ok(gid) => {
            *ACTIVE_GID.write() = Some(gid.__str__());
            Ok(())
        }
        Err(_) => Err(LailaError::attribute_error(alloc::format!(
            "'{}' object has no attribute 'global_id'",
            policy.type_name()
        ))),
    }
}

/// `laila._local_policies[gid] = policy` -- register *policy* as local
/// without activating it (`_load_environment` rebuilding a dump).
pub fn _register_local_policy(policy: &Policy) {
    let gid = policy.global_id();
    let mut g = LOCAL_POLICIES.write();
    match g.iter_mut().find(|(k, _)| *k == gid) {
        Some(slot) => slot.1 = policy.clone(),
        None => g.push((gid, policy.clone())),
    }
}

/// `laila._get_active_local_policy()` -- a local policy suitable for
/// future registration: the active one; in morph mode the local policy
/// owning the link to the peer, else the only local one (`RuntimeError`
/// when several exist and none owns the proxy).
pub fn _get_active_local_policy() -> Policy {
    get_active_policy()
}

/// `laila._get_active_local_policy()` with Python's `RuntimeError` when
/// the active policy is a remote proxy no local policy owns and more than
/// one local policy exists.
pub fn _get_active_local_policy_checked() -> Result<Policy> {
    if ACTIVE_GID.read().is_none() {
        return Ok(get_active_policy());
    }
    if let Some(p) = active_local() {
        return Ok(p);
    }
    if let Some(owner) = active_remote().and_then(|p| proxy_owner(&p)) {
        return Ok(owner);
    }
    let locals = _local_policies();
    if locals.len() == 1 {
        return Ok(locals[0].1.clone());
    }
    Err(LailaError::runtime_error(
        "No local policy available for future registration; set `laila.active_policy` to a local policy first.",
    ))
}

/// `laila._future_bank_policies()`: activated policies first, then every
/// other live one.
pub fn _future_bank_policies() -> Vec<Policy> {
    let mut out: Vec<Policy> = Vec::new();
    for (_, p) in _local_policies() {
        if !out.iter().any(|o| o.is(&p)) {
            out.push(p);
        }
    }
    for (_, p) in _live_policies() {
        if !out.iter().any(|o| o.is(&p)) {
            out.push(p);
        }
    }
    out
}

/// `laila._owning_local_policy(policy_id)`: the local policy *policy_id*
/// names (activated or merely constructed), else the active local one.
pub fn _owning_local_policy(policy_id: &Arg) -> Result<Policy> {
    let pid = match policy_id {
        Arg::None => None,
        Arg::Str(s) => Some(String::from(&**s)),
        other => other
            .__getattr__("global_id")
            .ok()
            .and_then(|g| g.as_str().map(String::from))
            .or_else(|| other.as_str().map(String::from)),
    };
    if let Some(pid) = pid {
        if let Some(p) = _local_policy_or_live(&pid) {
            return Ok(p);
        }
    }
    Ok(_get_active_local_policy())
}

/// `laila.terminate(*, wait=True, cancel_pending=False)`: shut every
/// local policy's command down, close its pools, clear the registries and
/// sweep orphan taskforces. Best-effort: one short string per failed
/// step, never raises.
pub fn terminate(wait: bool, cancel_pending: bool) -> Vec<String> {
    let mut errors: Vec<String> = Vec::new();
    for (gid, pol) in _local_policies() {
        #[cfg(feature = "std")]
        if let Some(comm) = pol.communication_opt() {
            if let Err(e) = comm.stop(Args::new()) {
                errors.push(alloc::format!(
                    "communication.stop[{gid}]: {}",
                    Arg::Exception(Arc::new(e)).__repr__()
                ));
            }
        }
        let mut a = Args::new();
        a.push_keyword("wait", wait);
        a.push_keyword("cancel_pending", cancel_pending);
        if let Err(e) = pol.command().shutdown(a) {
            errors.push(alloc::format!(
                "command.shutdown[{gid}]: {}",
                Arg::Exception(Arc::new(e)).__repr__()
            ));
        }
        match pol.pool_router().pools().items() {
            Ok(items) => {
                for kv in items.__iter__().unwrap_or_default() {
                    let pair = kv.__iter__().unwrap_or_default();
                    if let [pool_id, pool] = pair.as_slice() {
                        if let Err(e) = pool.__call_method__("close", Args::new()) {
                            if e.kind != crate::python::ExcKind::AttributeError {
                                errors.push(alloc::format!(
                                    "pool.close[{}]: {}",
                                    pool_id.__str__(),
                                    Arg::Exception(Arc::new(e)).__repr__()
                                ));
                            }
                        }
                    }
                }
            }
            Err(e) => errors.push(alloc::format!(
                "memory.pools[{gid}]: {}",
                Arg::Exception(Arc::new(e)).__repr__()
            )),
        }
    }
    LOCAL_POLICIES.write().clear();
    #[cfg(feature = "std")]
    central::communication::_clear_remote_policies();
    LIVE_POLICIES.write().clear();
    *ACTIVE_GID.write() = None;
    for tf in central::command::taskforce::_live_taskforces_snapshot() {
        let mut a = Args::new();
        a.push_keyword("wait", wait);
        a.push_keyword("cancel_pending", cancel_pending);
        if let Err(e) = tf.shutdown(a) {
            errors.push(alloc::format!(
                "orphan_taskforce.shutdown[{}]: {}",
                tf.global_id(),
                Arg::Exception(Arc::new(e)).__repr__()
            ));
        }
    }
    if let Err(e) = crate::logger::reset_singleton() {
        errors.push(alloc::format!(
            "logger.reset_singleton: {}",
            Arg::Exception(Arc::new(e)).__repr__()
        ));
    }
    // The ``laila.args.environment.policies`` / ``.logger`` mirrors are
    // cleared so a later ``laila.args.environment = {...}`` is not
    // polluted by the torn-down policies.
    if let Err(e) = _clear_environment_mirror() {
        errors.push(alloc::format!(
            "clear environment mirror: {}",
            Arg::Exception(Arc::new(e)).__repr__()
        ));
    }
    errors
}

fn _clear_environment_mirror() -> Result<()> {
    crate::basics::definitions::cli_capable::_clear_environment_dirty();
    let args = crate::basics::definitions::cli_capable::args();
    let env = args.get(Args::from("environment")).unwrap_or(Arg::None);
    if env.is_none() {
        return Ok(());
    }
    let policies = env.get(Args::from("policies")).unwrap_or(Arg::None);
    if !policies.is_none() {
        let _ = policies.__call_method__("clear", Args::new());
    }
    let _ = env.__call_method__("pop", {
        let mut a = Args::from("logger");
        a.push_positional(Arg::None);
        a
    });
    Ok(())
}
