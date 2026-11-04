//! `RemoteFuture` -- a proxy for a future owned by a *peer* policy.
//!
//! Over the wire a future never travels as state: the peer returns the
//! `__laila_future__` envelope (gid, `taskforce_id`, `policy_id`, group
//! flag) and the local side builds a `RemoteFuture` with that identity.
//! Every query (`status`, `result`, `data`, `result_id`, `exception`,
//! `wait`) is an RPC back to the owning policy's bank; result entries
//! cross as their canonical serialized blob and are rebuilt locally with
//! `build_by_scope`. The proxy self-registers in the *active local*
//! policy's `future_bank` so `laila.wait(gid)` / `release()` find it.

use alloc::string::String;
use alloc::sync::{Arc, Weak};
use alloc::vec::Vec;

use crate::basics::definitions::identifiable_object::{process_global_id, Identity};
use crate::macros::strings::{_FUTURE_SCOPE, _GROUP_FUTURE_SCOPE};
use crate::policy::central::communication::schema::base::{CommInner, Communication};
use crate::python::object::PyObject;
use crate::python::sync::Mutex;
use crate::python::{Arg, Args, IntoArg, LailaError, PyArg, Result};

use super::future_status::FutureStatus;

pub struct RemoteFutureInner {
    identity: Identity,
    taskforce_id: Arg,
    policy_id: String,
    comm: Mutex<Option<Weak<CommInner>>>,
    /// A stand-in channel (any object with `_send_rpc(policy_id, path,
    /// args, kwargs, comm=..)`) bound instead of a hub -- the tests' fakes.
    comm_fake: Mutex<Option<Arg>>,
    is_group: Mutex<bool>,
    comm_selector: Mutex<Arg>,
    materialized: Mutex<Option<Arg>>,
}

/// A future living in a peer policy's bank (handle; clones alias one proxy).
#[derive(Clone)]
pub struct RemoteFuture(pub Arc<RemoteFutureInner>);

impl RemoteFuture {
    /// `RemoteFuture(taskforce_id=.., policy_id=.., uuid=.., scopes=.., evolution=..)`
    /// -- registered in the active local policy's `future_bank`.
    pub fn new(args: impl Into<Args>) -> Result<RemoteFuture> {
        let a: Args = args.into();
        let uuid = a.get("uuid");
        let scopes = a.get("scopes");
        let scopes: Option<Vec<String>> = if scopes.is_none() {
            None
        } else {
            Some(scopes.__iter__()?.iter().map(|s| s.__str__()).collect())
        };
        let evolution = a.get("evolution").as_i64();
        let identity = Identity::new(uuid.as_str(), scopes, evolution, None, &[_FUTURE_SCOPE]);
        let policy_id = a.get("policy_id").__str__();
        let rf = RemoteFuture(Arc::new(RemoteFutureInner {
            identity,
            taskforce_id: a.get("taskforce_id"),
            policy_id,
            comm: Mutex::new(None),
            comm_fake: Mutex::new(None),
            is_group: Mutex::new(false),
            comm_selector: Mutex::new(Arg::None),
            materialized: Mutex::new(None),
        }));
        // model_post_init: live in the active local policy's bank.
        let policy = crate::policy::_get_active_local_policy();
        policy
            .future_bank()
            .__setitem__(rf.global_id(), rf.to_arg())?;
        Ok(rf)
    }

    /// Build the proxy for a `__laila_future__` envelope and bind it.
    pub fn from_envelope(
        result: &Arg,
        peer_id: &str,
        comm: &Communication,
        selector: Arg,
    ) -> Result<RemoteFuture> {
        let remote_gid = result.get("global_id")?.__str__();
        let parsed = process_global_id(&remote_gid).map_err(|_| {
            LailaError::value_error(alloc::format!(
                "Invalid remote future gid: {}",
                crate::python::pyrepr::str_repr(&remote_gid)
            ))
        })?;
        let is_group = result.get("__is_group__")?.__bool__();
        let mut a = Args::new();
        let tf = result.get("taskforce_id")?;
        a.push_keyword(
            "taskforce_id",
            if tf.is_none() {
                Arg::Str(peer_id.into())
            } else {
                tf
            },
        );
        let pid = result.get("policy_id")?;
        a.push_keyword(
            "policy_id",
            if pid.is_none() {
                Arg::Str(peer_id.into())
            } else {
                pid
            },
        );
        a.push_keyword("uuid", parsed.uuid.as_str());
        a.push_keyword(
            "scopes",
            alloc::vec![Arg::Str(
                if is_group {
                    _GROUP_FUTURE_SCOPE
                } else {
                    _FUTURE_SCOPE
                }
                .into()
            )],
        );
        a.push_keyword("evolution", parsed.evolution);
        let rf = RemoteFuture::new(a)?;
        rf.bind_hub(comm, is_group, selector);
        Ok(rf)
    }

    /// `rf.bind(communication, is_group=.., comm=..)` with a hub.
    pub fn bind_hub(&self, communication: &Communication, is_group: bool, comm: Arg) {
        *self.0.comm.lock() = Some(Arc::downgrade(&communication.0));
        *self.0.comm_fake.lock() = None;
        *self.0.is_group.lock() = is_group;
        *self.0.comm_selector.lock() = comm;
    }

    /// `rf.bind(communication, *, is_group=False, comm=None)`: attach the
    /// channel (a hub, or any object answering `_send_rpc(..)`) after
    /// construction.
    pub fn bind(&self, args: impl Into<Args>) -> Result<()> {
        let a: Args = args.into();
        let channel = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("communication"));
        let is_group = a.get("is_group").__bool__();
        let selector = a.get("comm");
        match Communication::of(&channel) {
            Some(hub) => self.bind_hub(&hub, is_group, selector),
            None if channel.is_none() => {
                return Err(LailaError::type_error("bind() expects a communication hub"))
            }
            None => {
                *self.0.comm.lock() = None;
                *self.0.comm_fake.lock() = Some(channel);
                *self.0.is_group.lock() = is_group;
                *self.0.comm_selector.lock() = selector;
            }
        }
        Ok(())
    }

    fn comm(&self) -> Result<Communication> {
        self.0
            .comm
            .lock()
            .as_ref()
            .and_then(Weak::upgrade)
            .map(Communication)
            .ok_or_else(|| {
                LailaError::runtime_error("RemoteFuture is not bound to a communication hub.")
            })
    }

    fn rpc(&self, method: &str, extra_kwargs: Vec<(&str, Arg)>) -> Result<Arg> {
        let mut kwargs = crate::python::PyDictBuilder::new();
        for (k, v) in extra_kwargs {
            kwargs.insert(k, v);
        }
        let fake = self.0.comm_fake.lock().clone();
        if let Some(fake) = fake {
            let mut call = Args::new();
            call.push_positional(self.0.policy_id.as_str());
            call.push_positional(alloc::vec![Arg::Str(method.into())]);
            call.push_positional(alloc::vec![Arg::Str(self.global_id().into())]);
            call.push_positional(kwargs.finish());
            call.push_keyword("comm", self.0.comm_selector.lock().clone());
            return fake.__call_method__("_send_rpc", call);
        }
        let comm = self.comm()?;
        comm._send_rpc_via(
            &self.0.policy_id,
            alloc::vec![String::from(method)],
            alloc::vec![Arg::Str(self.global_id().into())],
            kwargs.finish(),
            self.0.comm_selector.lock().clone(),
        )
    }

    pub fn global_id(&self) -> String {
        self.0.identity.global_id()
    }
    pub fn uuid(&self) -> String {
        self.0.identity.uuid.clone()
    }
    pub fn policy_id(&self) -> String {
        self.0.policy_id.clone()
    }
    pub fn taskforce_id(&self) -> Arg {
        self.0.taskforce_id.clone()
    }
    /// `rf.is_group`
    pub fn is_group(&self) -> bool {
        *self.0.is_group.lock()
    }
    /// Python `is`.
    pub fn is(&self, other: &RemoteFuture) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// `rf.status` -- the peer's `FutureStatus` (the enum member when the
    /// wire value names one, else the raw value).
    pub fn status(&self) -> Result<Arg> {
        let raw = self.rpc("_get_future_status", Vec::new())?;
        Ok(match &raw {
            Arg::Str(s) => match FutureStatus::from_value(s) {
                Ok(st) => st.into_arg(),
                Err(_) => match FutureStatus::MEMBERS.iter().find(|m| m.name() == &**s) {
                    Some(st) => (*st).into_arg(),
                    None => raw,
                },
            },
            _ => raw,
        })
    }

    fn rebuild(&self, blob: Arg) -> Result<Arg> {
        use crate::entry::constitution::build_maps::build_by_scope;
        if self.is_group() {
            let items = if blob.is_none() {
                Vec::new()
            } else {
                blob.__iter__()?
            };
            let mut out = Vec::with_capacity(items.len());
            for b in items {
                if b.is_none() {
                    out.push(Arg::None);
                } else {
                    let mut a = Args::from(b);
                    a.push_keyword("asynchronous", false);
                    out.push(build_by_scope(a)?);
                }
            }
            return Ok(out.into_arg());
        }
        if blob.is_none() {
            return Ok(Arg::None);
        }
        let mut a = Args::from(blob);
        a.push_keyword("asynchronous", false);
        build_by_scope(a)
    }

    /// `rf.result` -- the peer's result entry (rebuilt locally; cached).
    pub fn result(&self) -> Result<Arg> {
        if let Some(m) = self.0.materialized.lock().clone() {
            return Ok(m);
        }
        let blob = self.rpc("_get_future_result_entry", Vec::new())?;
        let entry = self.rebuild(blob)?;
        *self.0.materialized.lock() = Some(entry.clone());
        Ok(entry)
    }

    /// `rf.data` -- `result.data` (a list of them for a group).
    pub fn data(&self) -> Result<Arg> {
        let result = self.result()?;
        if self.is_group() {
            let mut out = Vec::new();
            for e in result.__iter__()? {
                out.push(match e {
                    Arg::Entry(e) => e.data()?,
                    _ => Arg::None,
                });
            }
            return Ok(out.into_arg());
        }
        Ok(match result {
            Arg::Entry(e) => e.data()?,
            _ => Arg::None,
        })
    }

    /// `rf.result_id` -- the result entry gid(s) on the peer.
    pub fn result_id(&self) -> Result<Arg> {
        self.rpc("_get_future_result_id", Vec::new())
    }

    /// `rf.exception` -- `None` or `{"type", "message"}`.
    pub fn exception(&self) -> Result<Arg> {
        self.rpc("_get_future_exception", Vec::new())
    }

    /// `rf.wait(timeout=None)` -- block on the peer, then the rebuilt result.
    pub fn wait(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let timeout = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("timeout"));
        crate::policy::central::command::schema::exceptions::_check_not_loop_thread()?;
        if let Some(m) = self.0.materialized.lock().clone() {
            return Ok(m);
        }
        let me = self.clone();
        let blob = crate::policy::central::command::schema::parking::park_sync(move || {
            me.rpc("_wait_future_entry", alloc::vec![("timeout", timeout)])
        })?;
        let entry = self.rebuild(blob)?;
        *self.0.materialized.lock() = Some(entry.clone());
        Ok(entry)
    }

    /// `rf.release()` -- drop the proxy from every local bank.
    pub fn release(&self) -> Result<()> {
        let gid = self.global_id();
        for p in crate::policy::_future_bank_policies() {
            let bank = p.future_bank();
            if bank.__contains__(gid.as_str())? {
                bank.__delitem__(gid.as_str())?;
            }
        }
        Ok(())
    }

    pub fn __repr__(&self) -> String {
        let kind = if self.is_group() {
            "RemoteGroupFuture"
        } else {
            "RemoteFuture"
        };
        alloc::format!(
            "{kind}({}, policy={})",
            crate::python::pyrepr::str_repr(&self.global_id()),
            crate::python::pyrepr::str_repr(&self.0.policy_id)
        )
    }

    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "global_id" => Arg::Str(self.global_id().into()),
            "uuid" | "_uuid" => Arg::Str(self.uuid().into()),
            "scopes" | "_scopes" => self.0.identity.scopes_arg(),
            "evolution" | "_evolution" => self.0.identity.evolution_arg(),
            "policy_id" => Arg::Str(self.0.policy_id.as_str().into()),
            "taskforce_id" => self.0.taskforce_id.clone(),
            "is_group" | "_is_group" => self.is_group().into_arg(),
            "_comm_selector" => self.0.comm_selector.lock().clone(),
            "_comm" => match self.comm() {
                Ok(c) => c.into_arg(),
                Err(_) => self.0.comm_fake.lock().clone().unwrap_or(Arg::None),
            },
            "_materialized_set" => self.0.materialized.lock().is_some().into_arg(),
            "_materialized" => self.0.materialized.lock().clone().unwrap_or(Arg::None),
            "status" => self.status()?,
            "result" => self.result()?,
            "data" => self.data()?,
            "result_id" => self.result_id()?,
            "exception" => self.exception()?,
            "__class__" => Self::class_object().into_arg(),
            // `fut.wait` read as a value: a bound method
            other @ ("wait" | "release" | "bind") => {
                let this = self.clone();
                let n = String::from(other);
                crate::python::callable(move |a: Args| this.__call_method__(&n, a))
            }
            other => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'RemoteFuture' object has no attribute '{other}'"
                )))
            }
        })
    }

    pub fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "wait" => self.wait(args),
            "release" => self.release().map(|()| Arg::None),
            "bind" => self.bind(args).map(|()| Arg::None),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'RemoteFuture' object has no attribute '{other}'"
            ))),
        }
    }

    pub fn class_object() -> crate::python::PyClass {
        crate::python::PyClass::with_ctor(
            "RemoteFuture",
            "laila.policy.central.command.schema.future.future.remote_future",
            Arc::new(|a: Args| RemoteFuture::new(a).map(IntoArg::into_arg)),
        )
    }

    /// The proxy inside an `Arg`, if it is one.
    pub fn of(x: &Arg) -> Option<RemoteFuture> {
        match x {
            Arg::Object(o) => o.as_any().downcast_ref::<RemoteFuture>().cloned(),
            _ => None,
        }
    }
}

impl core::fmt::Debug for RemoteFuture {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&RemoteFuture::__repr__(self))
    }
}
impl PyObject for RemoteFuture {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    /// `a is b`: identity of the shared handle, not of the boxing.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<RemoteFuture>()
            .is_some_and(|o| self.is(o))
    }
    fn type_name(&self) -> &'static str {
        "RemoteFuture"
    }
    fn __repr__(&self) -> String {
        RemoteFuture::__repr__(self)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<RemoteFuture>()
            .is_some_and(|o| o.global_id() == self.global_id())
    }
    fn __hash__(&self) -> Result<i64> {
        Arg::Str(self.global_id().into()).__hash__()
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        RemoteFuture::__getattr__(self, name)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        RemoteFuture::__call_method__(self, name, args)
    }
}
impl IntoArg for RemoteFuture {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for RemoteFuture {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl crate::python::builtins::IsInstance for RemoteFuture {
    fn isinstance(x: &Arg) -> bool {
        RemoteFuture::of(x).is_some()
    }
}
