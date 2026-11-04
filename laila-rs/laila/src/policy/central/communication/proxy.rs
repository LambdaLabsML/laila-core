//! Transparent remote-policy proxy for inter-policy RPC.
//!
//! A [`RemotePolicyProxy`] is the local stand-in for a *remote* policy.
//! From the caller's point of view it looks (and quacks) like a local
//! `_LAILA_IDENTIFIABLE_POLICY` -- you can pass it to
//! `laila.activate_policy`, query its `global_id`, and reach its
//! `central.memory.memorize` / `central.command.submit` methods exactly as
//! though it were local. The difference is that every terminal call is
//! sent over the wire instead of executed in-process.
//!
//! The trick that keeps the call site clean is *attribute-chain
//! accumulation* via [`_RemoteAttrChain`]. Each attribute access on a proxy
//! returns a chain object that records the dotted path so far without
//! doing any I/O. Only when the chain is finally invoked (`__call__`) does
//! a single RPC frame get serialised and dispatched through the
//! communication layer. That means
//!
//! ```text
//! proxy.central.memory.remember("x")
//! ```
//!
//! results in exactly one network round-trip, with the remote side
//! receiving `["central", "memory", "remember"]` plus `args=("x",),
//! kwargs={}`.
//!
//! Rust shape: the proxy keeps a *weak* reference to the hub (the hub owns
//! the proxies through its peer registry, so a strong one would be a
//! cycle); a proxy whose hub is gone raises `RuntimeError` on use.

use alloc::string::String;
use alloc::sync::{Arc, Weak};
use alloc::vec::Vec;

use crate::python::object::PyObject;
use crate::python::{Arg, Args, IntoArg, LailaError, PyArg, Result};

use super::schema::base::{CommInner, Communication};

/// The shared state of a proxy / chain: the hub, the peer and the
/// optional transport selector.
pub struct ProxyInner {
    pub(crate) comm: Weak<CommInner>,
    pub(crate) peer_id: String,
    pub(crate) comm_selector: Arg,
}

impl ProxyInner {
    pub(crate) fn comm(&self) -> Result<Communication> {
        self.comm.upgrade().map(Communication).ok_or_else(|| {
            LailaError::runtime_error(alloc::format!(
                "the communication hub behind the proxy for {} is gone",
                self.peer_id
            ))
        })
    }
}

/// Client-side proxy representing a peered remote policy.
///
/// Exposes `global_id` so that `laila.activate_policy` and other identity
/// checks work transparently. All *other* attribute access is captured as
/// the start of a chain ([`_RemoteAttrChain`]) and only triggers a network
/// round-trip when the chain is finally invoked.
#[derive(Clone)]
pub struct RemotePolicyProxy(pub Arc<ProxyInner>);

impl RemotePolicyProxy {
    /// `RemotePolicyProxy(peer_id, communication, comm_selector=None)` (typed).
    pub fn build(peer_id: &str, communication: &Communication, comm_selector: Arg) -> Self {
        RemotePolicyProxy(Arc::new(ProxyInner {
            comm: Arc::downgrade(&communication.0),
            peer_id: String::from(peer_id),
            comm_selector,
        }))
    }
    /// `RemotePolicyProxy(peer_id, communication, comm_selector=None)`.
    pub fn new(args: impl Into<Args>) -> Result<Self> {
        let a: Args = args.into();
        let peer_id = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("peer_id"));
        let comm = a
            .positional
            .get(1)
            .cloned()
            .unwrap_or_else(|| a.get("communication"));
        let selector = a
            .positional
            .get(2)
            .cloned()
            .unwrap_or_else(|| a.get("comm_selector"));
        let comm = Communication::of(&comm).ok_or_else(|| {
            LailaError::type_error(alloc::format!(
                "RemotePolicyProxy() expects a communication hub, got {}",
                comm.type_name()
            ))
        })?;
        Ok(Self::build(&peer_id.__str__(), &comm, selector))
    }
    /// The remote policy's `global_id` (resolved without I/O).
    pub fn global_id(&self) -> String {
        self.0.peer_id.clone()
    }
    /// `proxy._peer_id`
    pub fn peer_id(&self) -> &str {
        &self.0.peer_id
    }
    /// `proxy._comm` (the hub; `RuntimeError` when it is gone).
    pub fn communication(&self) -> Result<Communication> {
        self.0.comm()
    }
    /// `proxy._comm_selector`
    pub fn comm_selector(&self) -> Arg {
        self.0.comm_selector.clone()
    }
    /// `proxy.via(comm)`: a proxy bound to a specific transport *comm* -- a
    /// registered connection's `global_id` or a protocol token (`"tcp"`,
    /// `"lora"`, ..). Every RPC issued through the returned proxy (and every
    /// follow-up call on the futures it returns) travels over that channel.
    /// The original proxy is unchanged.
    pub fn via(&self, comm: impl IntoArg) -> RemotePolicyProxy {
        RemotePolicyProxy(Arc::new(ProxyInner {
            comm: self.0.comm.clone(),
            peer_id: self.0.peer_id.clone(),
            comm_selector: comm.into_arg(),
        }))
    }
    /// `proxy.central`: the remote policy's `central` as an attribute chain.
    pub fn central(&self) -> _RemoteAttrChain {
        self.chain("central")
    }
    /// Begin a remote attribute chain rooted at *name*.
    pub fn chain(&self, name: &str) -> _RemoteAttrChain {
        _RemoteAttrChain {
            inner: self.0.clone(),
            path: alloc::vec![String::from(name)],
        }
    }
    /// `proxy.<name>`: `global_id` resolves locally; everything else is the
    /// start of a chain.
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "global_id" => Arg::Str(self.global_id().into()),
            "_peer_id" => Arg::Str(self.0.peer_id.as_str().into()),
            "_comm" => self.communication()?.into_arg(),
            "_comm_selector" => self.comm_selector(),
            "__class__" => Self::class_object().into_arg(),
            other => self.chain(other).into_arg(),
        })
    }
    /// `proxy.<name>(*args, **kwargs)`: one RPC.
    pub fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "via" => {
                let comm = args
                    .positional
                    .first()
                    .cloned()
                    .unwrap_or_else(|| args.get("comm"));
                Ok(self.via(comm).into_arg())
            }
            other => self.chain(other).__call__(args),
        }
    }
    pub fn __repr__(&self) -> String {
        if !self.0.comm_selector.is_none() {
            return alloc::format!(
                "RemotePolicyProxy({}, via={})",
                crate::python::pyrepr::str_repr(&self.0.peer_id),
                self.0.comm_selector.__repr__()
            );
        }
        alloc::format!(
            "RemotePolicyProxy({})",
            crate::python::pyrepr::str_repr(&self.0.peer_id)
        )
    }
    pub fn class_object() -> crate::python::PyClass {
        crate::python::PyClass::with_ctor(
            "RemotePolicyProxy",
            "laila.policy.central.communication.proxy",
            Arc::new(|a: Args| RemotePolicyProxy::new(a).map(IntoArg::into_arg)),
        )
    }
    /// The proxy inside an `Arg` (a plain proxy *or* a [`super::registry::PeerProxy`]).
    pub fn of(x: &Arg) -> Option<RemotePolicyProxy> {
        match x {
            Arg::Object(o) => {
                if let Some(p) = o.as_any().downcast_ref::<RemotePolicyProxy>() {
                    return Some(p.clone());
                }
                o.as_any()
                    .downcast_ref::<super::registry::PeerProxy>()
                    .map(|p| p.0.clone())
            }
            _ => None,
        }
    }
    /// Python `is`.
    pub fn is(&self, other: &RemotePolicyProxy) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl core::fmt::Debug for RemotePolicyProxy {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&RemotePolicyProxy::__repr__(self))
    }
}
impl PyObject for RemotePolicyProxy {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "RemotePolicyProxy"
    }
    fn __repr__(&self) -> String {
        RemotePolicyProxy::__repr__(self)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<RemotePolicyProxy>()
            .is_some_and(|o| o.is(self))
    }
    fn __is__(&self, other: &dyn PyObject) -> bool {
        self.__eq__(other)
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        RemotePolicyProxy::__getattr__(self, name)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        RemotePolicyProxy::__call_method__(self, name, args)
    }
    fn __class__(&self) -> Option<crate::python::PyClass> {
        Some(Self::class_object())
    }
}
impl IntoArg for RemotePolicyProxy {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for RemotePolicyProxy {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl crate::python::builtins::IsInstance for RemotePolicyProxy {
    fn isinstance(x: &Arg) -> bool {
        RemotePolicyProxy::of(x).is_some()
    }
}
impl crate::python::builtins::ClassValue for RemotePolicyProxy {
    fn class_value() -> Arg {
        Self::class_object().into_arg()
    }
}

/// Accumulator for a dotted attribute path; flushes as an RPC on `__call__`.
///
/// Each attribute access appends to the path *immutably* (a new chain
/// object is returned, the existing one is unchanged), so the chain is
/// safe to share across threads. Calling the chain sends one RPC frame
/// containing the full path plus the call's positional and keyword
/// arguments.
#[allow(non_camel_case_types)]
#[derive(Clone)]
pub struct _RemoteAttrChain {
    inner: Arc<ProxyInner>,
    path: Vec<String>,
}

impl _RemoteAttrChain {
    /// `_RemoteAttrChain(communication, peer_id, path, comm_selector=None)`
    pub fn new(
        communication: &Communication,
        peer_id: &str,
        path: Vec<String>,
        comm_selector: Arg,
    ) -> Self {
        _RemoteAttrChain {
            inner: Arc::new(ProxyInner {
                comm: Arc::downgrade(&communication.0),
                peer_id: String::from(peer_id),
                comm_selector,
            }),
            path,
        }
    }
    /// `chain._path`
    pub fn path(&self) -> &[String] {
        &self.path
    }
    /// `chain._peer_id`
    pub fn peer_id(&self) -> &str {
        &self.inner.peer_id
    }
    /// A *new* chain whose path is the current path plus *name*.
    pub fn extend(&self, name: &str) -> _RemoteAttrChain {
        let mut path = self.path.clone();
        path.push(String::from(name));
        _RemoteAttrChain {
            inner: self.inner.clone(),
            path,
        }
    }
    /// `chain.memory`: the attribute chain extended by `memory` (typed
    /// spelling of `proxy.central.memory`).
    pub fn memory(&self) -> _RemoteAttrChain {
        self.extend("memory")
    }
    /// `chain.memorize` / `chain.memorize(..)`: without arguments the
    /// attribute chain extended by `memorize` (Tier 2 lowers the attribute
    /// read to a call); with arguments the RPC.
    pub fn memorize(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        if a.positional.is_empty() && a.keywords.is_empty() {
            return Ok(self.extend("memorize").into_arg());
        }
        self.extend("memorize").__call__(a)
    }
    /// Dispatch the accumulated path + arguments as a single RPC. Blocks
    /// the calling thread until the remote responds. If the remote returns
    /// a future-shaped envelope, the communication layer transparently
    /// wraps it in a `RemoteFuture` before returning.
    pub fn __call__(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let comm = self.inner.comm()?;
        let mut kwargs = crate::python::PyDictBuilder::new();
        for (k, v) in &a.keywords {
            kwargs.insert(k.clone(), v.clone());
        }
        comm._send_rpc_via(
            &self.inner.peer_id,
            self.path.clone(),
            a.positional,
            kwargs.finish(),
            self.inner.comm_selector.clone(),
        )
    }
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "_path" => self
                .path
                .iter()
                .map(|s| Arg::Str(s.as_str().into()))
                .collect::<Vec<Arg>>()
                .into_arg(),
            "_peer_id" => Arg::Str(self.inner.peer_id.as_str().into()),
            "_comm" => self.inner.comm()?.into_arg(),
            "_comm_selector" => self.inner.comm_selector.clone(),
            "__class__" => Self::class_object().into_arg(),
            other => self.extend(other).into_arg(),
        })
    }
    pub fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        self.extend(name).__call__(args)
    }
    pub fn __repr__(&self) -> String {
        alloc::format!(
            "_RemoteAttrChain({}, {})",
            crate::python::pyrepr::str_repr(&self.inner.peer_id),
            crate::python::pyrepr::str_repr(&self.path.join("."))
        )
    }
    pub fn class_object() -> crate::python::PyClass {
        crate::python::PyClass::named(
            "_RemoteAttrChain",
            "laila.policy.central.communication.proxy",
        )
    }
    pub fn of(x: &Arg) -> Option<_RemoteAttrChain> {
        x.downcast_ref::<_RemoteAttrChain>().cloned()
    }
}

impl core::fmt::Debug for _RemoteAttrChain {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&_RemoteAttrChain::__repr__(self))
    }
}
impl PyObject for _RemoteAttrChain {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "_RemoteAttrChain"
    }
    fn __repr__(&self) -> String {
        _RemoteAttrChain::__repr__(self)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<_RemoteAttrChain>()
            .is_some_and(|o| Arc::ptr_eq(&o.inner, &self.inner) && o.path == self.path)
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.inner) as usize as i64 ^ self.path.len() as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        _RemoteAttrChain::__getattr__(self, name)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        _RemoteAttrChain::__call_method__(self, name, args)
    }
    fn __call__(&self, args: Args) -> Result<Arg> {
        _RemoteAttrChain::__call__(self, args)
    }
    fn __class__(&self) -> Option<crate::python::PyClass> {
        Some(Self::class_object())
    }
}
impl IntoArg for _RemoteAttrChain {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for _RemoteAttrChain {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl crate::python::builtins::IsInstance for _RemoteAttrChain {
    fn isinstance(x: &Arg) -> bool {
        _RemoteAttrChain::of(x).is_some()
    }
}
