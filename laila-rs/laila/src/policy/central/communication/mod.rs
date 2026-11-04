//! `laila.policy.central.communication` -- inter-policy peer-to-peer RPC.
//!
//! Four collaborating pieces:
//!
//! - [`schema::base::_LAILA_IDENTIFIABLE_COMMUNICATION`] -- the per-policy
//!   hub: transports (`connections`), peers (`peers`), the inbound
//!   dispatcher (`_execute_rpc`) and future virtualisation.
//! - [`protocols::base::_LAILA_IDENTIFIABLE_COMM_PROTOCOL`] -- the transport
//!   contract; the reusable carriers under [`protocols::_carriers`] implement
//!   it once for streams, datagrams and in-process loopback, and every
//!   concrete transport is a thin subclass.
//! - [`proxy::RemotePolicyProxy`] / [`registry::PeerProxy`] -- the local
//!   stand-in for a peer: attribute chains flush as one RPC, `[name]`
//!   resolves a stream [`channel::Channel`].
//! - [`protocol`] / [`wire`] -- the JSON-RPC envelope and the stream-lane
//!   frame layout shared with every other laila implementation.
//!
//! Rust shape: carriers run on plain `std::thread`s (an accept thread, a
//! reader thread per link, one writer thread per link, a small worker pool
//! for inbound RPCs) instead of an asyncio loop; the wire format and every
//! public method are identical.

pub mod channel;
pub mod connection;
/// `from laila.policy.central.communication import connection` as a value
/// (`mock.patch.object(connection, "MAX_FRAME_BYTES", ..)`).
#[allow(non_upper_case_globals)]
pub static connection: self::connection::connection_mod = self::connection::connection_mod;
pub mod protocol;
pub mod protocols;
pub mod proxy;
pub mod registry;
pub mod schema;
pub mod wire;

// The module *objects* (`from laila.policy.central.communication import
// protocol`): a `use` of the module path brings in both the module and the
// value of the same name.
pub use protocol::protocol;
pub use wire::wire;

pub use crate::policy::central::command::schema::future::future::RemoteFuture;
pub use channel::{Channel, Relay, StreamEntry, StreamMeta};
pub use protocols::base::_LAILA_IDENTIFIABLE_COMM_PROTOCOL;
pub use protocols::lpwan::_LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL;
pub use protocols::short_range::_LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL;
pub use protocols::tcpip::_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL;
pub use proxy::RemotePolicyProxy;
pub use registry::{PeerProxy, PeerRegistry};
pub use schema::base::{Communication, _LAILA_IDENTIFIABLE_COMMUNICATION};

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::python::sync::RwLock;
use crate::python::{Arg, Args, IntoArg, LailaError, PyObject, Result};

/// `laila._remote_policies`: `{peer gid: PeerProxy}` -- every peer any
/// local policy is currently connected to.
static REMOTE_POLICIES: RwLock<Vec<(String, Arg)>> = RwLock::new(Vec::new());

/// `laila._remote_policies` as `(gid, proxy)` pairs (a snapshot).
pub fn _remote_policies() -> Vec<(String, Arg)> {
    REMOTE_POLICIES.read().clone()
}

/// `laila._remote_policies.get(gid)`.
pub fn _remote_policy(gid: &str) -> Option<Arg> {
    REMOTE_POLICIES
        .read()
        .iter()
        .find(|(g, _)| g == gid)
        .map(|(_, p)| p.clone())
}

/// `laila._remote_policies[gid] = proxy`.
pub fn _register_remote_policy(gid: &str, proxy: Arg) {
    let mut g = REMOTE_POLICIES.write();
    match g.iter_mut().find(|(k, _)| k == gid) {
        Some(slot) => slot.1 = proxy,
        None => g.push((String::from(gid), proxy)),
    }
}

/// `laila._remote_policies.pop(gid, None)`.
pub fn _unregister_remote_policy(gid: &str) -> Option<Arg> {
    let mut g = REMOTE_POLICIES.write();
    let i = g.iter().position(|(k, _)| k == gid)?;
    Some(g.remove(i).1)
}

/// `laila._remote_policies.clear()`.
pub fn _clear_remote_policies() {
    REMOTE_POLICIES.write().clear();
}

/// `laila._remote_policies` as a plain dict snapshot.
pub fn _remote_policies_dict() -> Arg {
    let mut d = crate::python::PyDictBuilder::new();
    for (gid, p) in _remote_policies() {
        d.insert(gid, p);
    }
    d.finish()
}

/// `laila._remote_policies`: a live `{gid: proxy}` view of the registry
/// with the `dict` protocol (`laila._remote_policies[gid] = proxy`,
/// `gid in laila._remote_policies`, `.pop(gid, None)`, `.clear()`).
#[derive(Clone, Copy, Default)]
pub struct RemotePolicyRegistry;

impl RemotePolicyRegistry {
    fn key(k: &Arg) -> Result<String> {
        match k {
            Arg::Str(s) => Ok(s.to_string()),
            other => Err(LailaError::key_error(other.__repr__())),
        }
    }
    pub fn snapshot(&self) -> Arg {
        _remote_policies_dict()
    }
    /// `laila._remote_policies[gid]` on the typed view (any key spelling).
    pub fn __getitem__(&self, key: impl IntoArg) -> Result<Arg> {
        PyObject::__getitem__(self, &key.into_arg())
    }
    /// `laila._remote_policies[gid] = proxy`
    pub fn __setitem__(&self, key: impl IntoArg, value: impl IntoArg) -> Result<()> {
        PyObject::__setitem__(self, &key.into_arg(), value.into_arg())
    }
    /// `len(laila._remote_policies)`
    pub fn __len__(&self) -> Result<usize> {
        Ok(REMOTE_POLICIES.read().len())
    }
    /// `gid in laila._remote_policies`
    pub fn __contains__(&self, item: impl IntoArg) -> Result<bool> {
        PyObject::__contains__(self, &item.into_arg())
    }
    /// `laila._remote_policies.pop(gid, default)`
    pub fn pop(&self, args: impl Into<Args>) -> Result<Arg> {
        self.__call_method__("pop", args.into())
    }
    /// `laila._remote_policies.get(gid, default)`
    pub fn get(&self, args: impl Into<Args>) -> Result<Arg> {
        self.__call_method__("get", args.into())
    }
    /// `laila._remote_policies.clear()`
    pub fn clear(&self, _args: impl Into<Args>) -> Result<()> {
        _clear_remote_policies();
        Ok(())
    }
    /// `laila._remote_policies.keys()` / `.values()` / `.items()`
    pub fn keys(&self, args: impl Into<Args>) -> Result<Arg> {
        self.snapshot().keys(args)
    }
    pub fn values(&self) -> Result<Arg> {
        self.snapshot().values()
    }
    pub fn items(&self) -> Result<Arg> {
        self.snapshot().items()
    }
}

impl core::fmt::Debug for RemotePolicyRegistry {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.snapshot().__repr__())
    }
}
impl crate::python::builtins::PyLen for RemotePolicyRegistry {
    fn __len__(&self) -> Result<usize> {
        RemotePolicyRegistry::__len__(self)
    }
}
impl PyObject for RemotePolicyRegistry {
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
            .downcast_ref::<RemotePolicyRegistry>()
            .is_some()
    }
    fn __eq_arg__(&self, other: &Arg) -> bool {
        match other {
            Arg::Object(o) => self.__eq__(&**o),
            other => self.snapshot() == *other,
        }
    }
    fn __is__(&self, other: &dyn PyObject) -> bool {
        self.__eq__(other)
    }
    fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error("unhashable type: 'dict'"))
    }
    fn __len__(&self) -> Result<usize> {
        RemotePolicyRegistry::__len__(self)
    }
    fn __contains__(&self, item: &Arg) -> Result<bool> {
        Ok(match item {
            Arg::Str(s) => _remote_policy(s).is_some(),
            _ => false,
        })
    }
    fn __iter__(&self) -> Result<Vec<Arg>> {
        Ok(REMOTE_POLICIES
            .read()
            .iter()
            .map(|(g, _)| Arg::Str(g.as_str().into()))
            .collect())
    }
    fn __getitem__(&self, key: &Arg) -> Result<Arg> {
        let k = Self::key(key)?;
        _remote_policy(&k).ok_or_else(|| LailaError::key_error(key.__repr__()))
    }
    fn __setitem__(&self, key: &Arg, value: Arg) -> Result<()> {
        let k = Self::key(key)?;
        _register_remote_policy(&k, value);
        Ok(())
    }
    fn __delitem__(&self, key: &Arg) -> Result<()> {
        let k = Self::key(key)?;
        _unregister_remote_policy(&k)
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
                match PyObject::__getitem__(self, &key) {
                    Ok(v) => Ok(v),
                    Err(_) => Ok(args.positional.get(1).cloned().unwrap_or(Arg::None)),
                }
            }
            "pop" => {
                let key = args.positional.first().cloned().ok_or_else(|| {
                    LailaError::type_error("pop expected at least 1 argument, got 0")
                })?;
                let k = Self::key(&key)?;
                match _unregister_remote_policy(&k) {
                    Some(p) => Ok(p),
                    None => match args.positional.get(1) {
                        Some(default) => Ok(default.clone()),
                        None => Err(LailaError::key_error(key.__repr__())),
                    },
                }
            }
            "clear" => {
                _clear_remote_policies();
                Ok(Arg::None)
            }
            other => Err(LailaError::attribute_error(alloc::format!(
                "'dict' object has no attribute '{other}'"
            ))),
        }
    }
    fn __bool__(&self) -> bool {
        !REMOTE_POLICIES.read().is_empty()
    }
}
impl IntoArg for RemotePolicyRegistry {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl crate::python::PyArg for RemotePolicyRegistry {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(*self))
    }
}
