//! Peer registry and channel-aware peer proxy.
//!
//! [`PeerRegistry`] is the `dict` subtype stored on `communication.peers`.
//! It keeps exact dict semantics (`len`, iteration, `in`, `.get` / `.pop`
//! / `.clear`) for every existing caller and adds exactly one resolver,
//! [`PeerRegistry::channel`], which turns `(peer, name,
//! transport-selector)` into a [`Channel`] by asking the carrier that
//! holds the peer. It holds no live stream state and no back-reference to
//! the communication object.
//!
//! [`PeerProxy`] is the value type stored in the registry. It is a
//! [`RemotePolicyProxy`] that additionally supports item access:
//!
//! ```text
//! laila.peers[gid]["default"]      # the ordinary RPC proxy itself
//! laila.peers[gid]["video"]        # a Channel (opened lazily, cached)
//! laila.peers[gid].via("uart")["video"]
//! ```
//!
//! Rust shape: the registry is a live view over the hub's peer table
//! (`comm.peers` is the same object every time); the hub owns the
//! proxies, the proxies refer back to the hub weakly.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::python::object::PyObject;
use crate::python::sync::Mutex;
use crate::python::{Arg, Args, IntoArg, LailaError, PyArg, Result};

use super::channel::Channel;
use super::proxy::RemotePolicyProxy;
use super::schema::base::Communication;

/// Reserved channel name that resolves to the RPC proxy itself.
pub const DEFAULT_CHANNEL: &str = "default";

/// The peer table behind `communication.peers`: `(gid, proxy)` in
/// registration order.
pub type PeerTable = Mutex<Vec<(String, PeerProxy)>>;

/// `dict[str, PeerProxy]` with a single channel resolver.
///
/// Channel names never select a transport; that stays on
/// [`PeerProxy::via`] / `laila.request(gid, comm_protocol=...)`.
#[derive(Clone)]
pub struct PeerRegistry(pub Arc<PeerTable>);

impl Default for PeerRegistry {
    fn default() -> Self {
        PeerRegistry::new()
    }
}

impl PeerRegistry {
    /// `PeerRegistry()` -- an empty registry.
    pub fn new() -> PeerRegistry {
        PeerRegistry(Arc::new(Mutex::new(Vec::new())))
    }
    /// `PeerRegistry(mapping)` from the dynamic layer.
    pub fn from_args(args: impl Into<Args>) -> Result<PeerRegistry> {
        let a: Args = args.into();
        let reg = PeerRegistry::new();
        if let Some(src) = a.positional.first() {
            if let Some(other) = src.downcast_ref::<PeerRegistry>() {
                *reg.0.lock() = other.0.lock().clone();
            } else {
                for pair in src.items()?.__iter__()? {
                    let k = pair.__getitem__(0)?;
                    let v = pair.__getitem__(1)?;
                    reg.__setitem__(&k, v)?;
                }
            }
        }
        Ok(reg)
    }
    fn key(k: &Arg) -> Result<String> {
        match k {
            Arg::Str(s) => Ok(s.to_string()),
            other => Err(LailaError::key_error(other.__repr__())),
        }
    }
    /// `len(peers)`
    pub fn len(&self) -> usize {
        self.0.lock().len()
    }
    /// `not peers`
    pub fn is_empty(&self) -> bool {
        self.0.lock().is_empty()
    }
    /// `list(peers)` -- the peer gids in registration order.
    pub fn keys_vec(&self) -> Vec<String> {
        self.0.lock().iter().map(|(g, _)| g.clone()).collect()
    }
    /// `list(peers.values())`
    pub fn values_vec(&self) -> Vec<PeerProxy> {
        self.0.lock().iter().map(|(_, p)| p.clone()).collect()
    }
    /// `peers.get(gid)`
    pub fn get(&self, gid: &str) -> Option<PeerProxy> {
        self.0
            .lock()
            .iter()
            .find(|(g, _)| g == gid)
            .map(|(_, p)| p.clone())
    }
    /// `gid in peers`
    pub fn contains(&self, gid: &str) -> bool {
        self.0.lock().iter().any(|(g, _)| g == gid)
    }
    /// `peers[gid] = proxy`
    pub fn insert(&self, gid: &str, proxy: PeerProxy) {
        let mut t = self.0.lock();
        match t.iter_mut().find(|(g, _)| g == gid) {
            Some(slot) => slot.1 = proxy,
            None => t.push((String::from(gid), proxy)),
        }
    }
    /// `peers.pop(gid, None)`
    pub fn remove(&self, gid: &str) -> Option<PeerProxy> {
        let mut t = self.0.lock();
        let i = t.iter().position(|(g, _)| g == gid)?;
        Some(t.remove(i).1)
    }
    /// `peers.clear()`
    pub fn clear(&self) {
        self.0.lock().clear();
    }
    /// A plain `{gid: proxy}` dict snapshot.
    pub fn snapshot(&self) -> Arg {
        let mut d = crate::python::PyDictBuilder::new();
        for (g, p) in self.0.lock().iter() {
            d.insert(g.clone(), p.to_arg());
        }
        d.finish()
    }
    /// Resolve `(peer_id, name)` to a [`Channel`] on the right carrier.
    ///
    /// 1. `proto = comm._select_protocol_for_peer(peer_id, selector)`.
    /// 2. Require `type(proto).supports_channels`.
    /// 3. Return `proto.open_channel(peer_id, name)` (cached inside the
    ///    carrier; the first call may perform one blocking RPC).
    ///
    /// `ConnectionError` if no (matching) transport holds the peer, the
    /// transport has no stream lanes, or the peer cannot open the lane;
    /// `ValueError` if *name* is the reserved `"default"`.
    pub fn channel(
        &self,
        comm: &Communication,
        peer_id: &str,
        name: &str,
        selector: Arg,
    ) -> Result<Channel> {
        if name == DEFAULT_CHANNEL {
            return Err(LailaError::value_error(alloc::format!(
                "{} is the RPC proxy, not a stream channel.",
                crate::python::pyrepr::str_repr(DEFAULT_CHANNEL)
            )));
        }
        let proto = comm._select_protocol_for_peer(peer_id, &selector)?;
        if !proto.supports_channels() {
            return Err(LailaError::connection_error(alloc::format!(
                "{} has no stream lanes; use a stream-capable transport (e.g. tcp://, serial, loopback) for laila.peers[gid][name].",
                crate::python::pyrepr::str_repr(proto.protocol_name())
            )));
        }
        proto.open_channel(peer_id, name)
    }
    pub fn __getitem__(&self, key: impl IntoArg) -> Result<Arg> {
        let key = key.into_arg();
        let k = Self::key(&key)?;
        self.get(&k)
            .map(IntoArg::into_arg)
            .ok_or_else(|| LailaError::key_error(key.__repr__()))
    }
    pub fn __setitem__(&self, key: &Arg, value: Arg) -> Result<()> {
        let k = Self::key(key)?;
        let proxy = PeerProxy::of(&value).ok_or_else(|| {
            LailaError::type_error(alloc::format!(
                "peers values must be PeerProxy objects, got {}",
                value.type_name()
            ))
        })?;
        self.insert(&k, proxy);
        Ok(())
    }
    pub fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
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
                match self.remove(&k) {
                    Some(p) => Ok(p.into_arg()),
                    None => match args.positional.get(1) {
                        Some(default) => Ok(default.clone()),
                        None => Err(LailaError::key_error(key.__repr__())),
                    },
                }
            }
            "clear" => {
                self.clear();
                Ok(Arg::None)
            }
            "channel" => {
                let comm = args
                    .positional
                    .first()
                    .cloned()
                    .unwrap_or_else(|| args.get("comm"));
                let comm = Communication::of(&comm).ok_or_else(|| {
                    LailaError::type_error("channel() expects a communication hub")
                })?;
                let peer_id = args
                    .positional
                    .get(1)
                    .cloned()
                    .unwrap_or_else(|| args.get("peer_id"))
                    .__str__();
                let name = args
                    .positional
                    .get(2)
                    .cloned()
                    .unwrap_or_else(|| args.get("name"))
                    .__str__();
                let selector = args
                    .positional
                    .get(3)
                    .cloned()
                    .unwrap_or_else(|| args.get("selector"));
                self.channel(&comm, &peer_id, &name, selector)
                    .map(IntoArg::into_arg)
            }
            other => Err(LailaError::attribute_error(alloc::format!(
                "'PeerRegistry' object has no attribute '{other}'"
            ))),
        }
    }
    pub fn class_object() -> crate::python::PyClass {
        crate::python::PyClass::with_ctor(
            "PeerRegistry",
            "laila.policy.central.communication.registry",
            Arc::new(|a: Args| PeerRegistry::from_args(a).map(IntoArg::into_arg)),
        )
    }
    pub fn of(x: &Arg) -> Option<PeerRegistry> {
        x.downcast_ref::<PeerRegistry>().cloned()
    }
}

impl core::fmt::Debug for PeerRegistry {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.snapshot().__repr__())
    }
}
impl PyObject for PeerRegistry {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "PeerRegistry"
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
            .downcast_ref::<PeerRegistry>()
            .is_some_and(|o| Arc::ptr_eq(&o.0, &self.0) || self.snapshot() == o.snapshot())
    }
    fn __eq_arg__(&self, other: &Arg) -> bool {
        match other {
            Arg::Object(o) => self.__eq__(&**o),
            other => self.snapshot() == *other,
        }
    }
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<PeerRegistry>()
            .is_some_and(|o| Arc::ptr_eq(&o.0, &self.0))
    }
    fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error("unhashable type: 'PeerRegistry'"))
    }
    fn __len__(&self) -> Result<usize> {
        Ok(self.len())
    }
    fn __contains__(&self, item: &Arg) -> Result<bool> {
        Ok(match item {
            Arg::Str(s) => self.contains(s),
            _ => false,
        })
    }
    fn __iter__(&self) -> Result<Vec<Arg>> {
        Ok(self
            .keys_vec()
            .into_iter()
            .map(|g| Arg::Str(g.into()))
            .collect())
    }
    fn __getitem__(&self, key: &Arg) -> Result<Arg> {
        PeerRegistry::__getitem__(self, key)
    }
    fn __setitem__(&self, key: &Arg, value: Arg) -> Result<()> {
        PeerRegistry::__setitem__(self, key, value)
    }
    fn __delitem__(&self, key: &Arg) -> Result<()> {
        let k = Self::key(key)?;
        self.remove(&k)
            .map(|_| ())
            .ok_or_else(|| LailaError::key_error(key.__repr__()))
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "__class__" => Ok(Self::class_object().into_arg()),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'PeerRegistry' object has no attribute '{other}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        PeerRegistry::__call_method__(self, name, args)
    }
    fn __class__(&self) -> Option<crate::python::PyClass> {
        Some(Self::class_object())
    }
}
impl IntoArg for PeerRegistry {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for PeerRegistry {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl crate::python::builtins::IsInstance for PeerRegistry {
    fn isinstance(x: &Arg) -> bool {
        PeerRegistry::of(x).is_some()
    }
}
impl crate::python::builtins::ClassValue for PeerRegistry {
    fn class_value() -> Arg {
        Self::class_object().into_arg()
    }
}

/// A [`RemotePolicyProxy`] with `[name]` channel access.
///
/// Adds only methods -- no new instance attributes -- so identity checks
/// (`isinstance(x, RemotePolicyProxy)`), `laila.activate_policy` (morph
/// mode) and `laila.request` keep working unchanged.
///
/// `proxy[name]` is *not* a cheap dict lookup: the first access to a
/// negotiated lane performs one blocking RPC (bounded by the carrier's
/// `rpc_timeout`) and may raise `ConnectionError`. Later accesses hit the
/// carrier's cache.
#[derive(Clone)]
pub struct PeerProxy(pub RemotePolicyProxy);

impl PeerProxy {
    /// `PeerProxy(peer_id, communication, comm_selector=None)` (typed).
    pub fn build(peer_id: &str, communication: &Communication, comm_selector: Arg) -> Self {
        PeerProxy(RemotePolicyProxy::build(
            peer_id,
            communication,
            comm_selector,
        ))
    }
    /// `PeerProxy(peer_id, communication, comm_selector=None)`.
    pub fn new(args: impl Into<Args>) -> Result<Self> {
        RemotePolicyProxy::new(args).map(PeerProxy)
    }
    pub fn global_id(&self) -> String {
        self.0.global_id()
    }
    pub fn peer_id(&self) -> &str {
        self.0.peer_id()
    }
    pub fn communication(&self) -> Result<Communication> {
        self.0.communication()
    }
    pub fn comm_selector(&self) -> Arg {
        self.0.comm_selector()
    }
    /// `proxy.via(comm)`: a [`PeerProxy`] bound to transport *comm*.
    pub fn via(&self, comm: impl IntoArg) -> PeerProxy {
        PeerProxy(self.0.via(comm))
    }
    /// `proxy[name]`: `"default"` -> this proxy; any other name -> a
    /// [`Channel`].
    pub fn __getitem__(&self, name: impl IntoArg) -> Result<Arg> {
        let name = name.into_arg();
        let Arg::Str(name) = &name else {
            return Err(LailaError::type_error(alloc::format!(
                "Channel names are strings, got {}; use 'default' for the RPC proxy.",
                name.type_name()
            )));
        };
        if &**name == DEFAULT_CHANNEL {
            return Ok(self.to_arg());
        }
        let comm = self.communication()?;
        comm.peers()
            .channel(&comm, self.peer_id(), name, self.comm_selector())
            .map(IntoArg::into_arg)
    }
    /// `proxy.channel(name)` -- the typed twin of `proxy[name]`.
    pub fn channel(&self, name: &str) -> Result<Channel> {
        let comm = self.communication()?;
        comm.peers()
            .channel(&comm, self.peer_id(), name, self.comm_selector())
    }
    /// Names of channels currently open to this peer (union over
    /// carriers). Never opens anything. With a `via()` selector only that
    /// transport is consulted.
    pub fn channels(&self) -> Result<Vec<String>> {
        let comm = self.communication()?;
        let selector = self.comm_selector();
        let protos = if !selector.is_none() {
            alloc::vec![comm._select_protocol_for_peer(self.peer_id(), &selector)?]
        } else {
            comm.connections_vec()
                .into_iter()
                .filter(|p| p.has_peer(self.peer_id()))
                .collect()
        };
        let mut names: Vec<String> = Vec::new();
        for proto in protos {
            for n in proto.channel_names(self.peer_id()) {
                if !names.contains(&n) {
                    names.push(n);
                }
            }
        }
        Ok(names)
    }
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "__class__" => Ok(Self::class_object().into_arg()),
            "__iter__" => Ok(Arg::None),
            other => self.0.__getattr__(other),
        }
    }
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
            "channels" => Ok(self
                .channels()?
                .into_iter()
                .map(|s| Arg::Str(s.into()))
                .collect::<Vec<Arg>>()
                .into_arg()),
            other => self.0.__call_method__(other, args),
        }
    }
    pub fn __repr__(&self) -> String {
        let sel = self.comm_selector();
        if !sel.is_none() {
            return alloc::format!(
                "PeerProxy({}, via={})",
                crate::python::pyrepr::str_repr(self.peer_id()),
                sel.__repr__()
            );
        }
        alloc::format!(
            "PeerProxy({})",
            crate::python::pyrepr::str_repr(self.peer_id())
        )
    }
    pub fn class_object() -> crate::python::PyClass {
        crate::python::PyClass::with_ctor(
            "PeerProxy",
            "laila.policy.central.communication.registry",
            Arc::new(|a: Args| PeerProxy::new(a).map(IntoArg::into_arg)),
        )
    }
    pub fn of(x: &Arg) -> Option<PeerProxy> {
        x.downcast_ref::<PeerProxy>().cloned()
    }
    pub fn is(&self, other: &PeerProxy) -> bool {
        self.0.is(&other.0)
    }
}

impl core::fmt::Debug for PeerProxy {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&PeerProxy::__repr__(self))
    }
}
impl PyObject for PeerProxy {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "PeerProxy"
    }
    fn __bases__(&self) -> &'static [&'static str] {
        &["RemotePolicyProxy"]
    }
    fn __repr__(&self) -> String {
        PeerProxy::__repr__(self)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        match other.as_any().downcast_ref::<PeerProxy>() {
            Some(o) => o.is(self),
            None => other
                .as_any()
                .downcast_ref::<RemotePolicyProxy>()
                .is_some_and(|o| o.is(&self.0)),
        }
    }
    fn __is__(&self, other: &dyn PyObject) -> bool {
        self.__eq__(other)
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0 .0) as usize as i64)
    }
    fn __getitem__(&self, key: &Arg) -> Result<Arg> {
        PeerProxy::__getitem__(self, key)
    }
    fn __iter__(&self) -> Result<Vec<Arg>> {
        Err(LailaError::type_error("'PeerProxy' object is not iterable"))
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        PeerProxy::__getattr__(self, name)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        PeerProxy::__call_method__(self, name, args)
    }
    fn __class__(&self) -> Option<crate::python::PyClass> {
        Some(Self::class_object())
    }
}
impl IntoArg for PeerProxy {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for PeerProxy {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl crate::python::builtins::IsInstance for PeerProxy {
    fn isinstance(x: &Arg) -> bool {
        PeerProxy::of(x).is_some()
    }
}
impl crate::python::builtins::ClassValue for PeerProxy {
    fn class_value() -> Arg {
        Self::class_object().into_arg()
    }
}
