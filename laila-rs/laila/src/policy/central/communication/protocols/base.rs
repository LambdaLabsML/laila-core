//! `_LAILA_IDENTIFIABLE_COMM_PROTOCOL` -- the transport contract.
//!
//! Python models every transport as a pydantic class in a hierarchy
//! (`_CarrierRPCProtocol` -> `_StreamRPCProtocol` -> TCP -> Wi-Fi, ...).
//! The port keeps one handle type, [`Protocol`], tagged with a static
//! [`ProtocolSpec`] that carries what the class hierarchy carried: the
//! class name / module, `protocol_name`, the token aliases and URI
//! schemes it claims, the carrier it rides on ([`CarrierKind`]) and the
//! wire that carrier drives ([`Wire`]), its extra config fields, and the
//! ancestor class names `isinstance` answers for. Every method on the
//! Python classes is a method on `Protocol` that dispatches on the spec.
//!
//! Lifecycle contract (identical to Python):
//! `start()` binds the listener/endpoint, `connect(uri, secret) -> peer gid`
//! performs the `peer.connect` handshake, `send_rpc` runs one correlated
//! JSON-RPC call, `ping` is the liveness fast path, `disconnect` says
//! goodbye and drops the peer, `stop()` tears everything down.

use alloc::string::{String, ToString};
use alloc::sync::{Arc, Weak};
use alloc::vec::Vec;

use crate::atomic::definitions::HasIdentity;
use crate::basics::definitions::identifiable_object::Identity;
use crate::macros::strings::_COMM_PROTOCOL_SCOPE;
use crate::python::object::PyObject;
use crate::python::sync::{Mutex, RwLock};
use crate::python::{Arg, Args, IntoArg, LailaError, PyArg, PyDictBuilder, Result};

use super::super::channel::{Channel, ChannelOwner};
use super::super::schema::base::{CommInner, Communication};
use super::_carriers::base::CarrierState;
use super::_carriers::transport::Transport;

/// Which reusable carrier a transport rides on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CarrierKind {
    /// The abstract contract only (`_LAILA_IDENTIFIABLE_COMM_PROTOCOL`,
    /// LoRa / Bluetooth scaffolds): every verb raises `NotImplementedError`.
    Base,
    /// In-process zero-copy dispatch (`loopback`).
    Loopback,
    /// Reliable duplex byte stream (`_StreamRPCProtocol`).
    Stream,
    /// Packet link with fragmentation / ack / retry (`_DatagramRPCProtocol`).
    Datagram,
    /// Datagram carrier whose endpoint needs a third-party driver
    /// (`_LazyDatagramRPCProtocol`).
    LazyDatagram,
    /// Point-to-point serial-style stream over a driver (`_P2PStreamRPCProtocol`).
    P2P,
    /// Pub/sub broker (`_BrokerRPCProtocol`).
    Broker,
    /// Register-polling fieldbus (`_RegisterRPCProtocol`).
    Register,
    /// JSON text frames over WebSocket (`tcpip`).
    WebSocket,
}

/// The concrete wire a carrier drives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Wire {
    None,
    Loopback,
    Tcp,
    Tls,
    Unix,
    Udp,
    WebSocket,
    /// A serial device file (`/dev/ttyUSB0`) driven as a raw tty.
    Serial,
    /// A host-side driver / radio / bus the Rust build has no backend for.
    Driver,
}

/// A config field's default.
#[derive(Clone, Copy, Debug)]
pub enum FieldDefault {
    None,
    Str(&'static str),
    Int(i64),
    Float(f64),
    Bool(bool),
    Dict,
    List,
    /// `peer_secret_key`: `uuid4().hex`.
    SecretHex,
}

impl FieldDefault {
    pub fn value(&self) -> Arg {
        match self {
            FieldDefault::None => Arg::None,
            FieldDefault::Str(s) => Arg::Str((*s).into()),
            FieldDefault::Int(i) => Arg::Int(*i),
            FieldDefault::Float(f) => Arg::Float(*f),
            FieldDefault::Bool(b) => Arg::Bool(*b),
            FieldDefault::Dict => PyDictBuilder::new().finish(),
            FieldDefault::List => Vec::<Arg>::new().into_arg(),
            FieldDefault::SecretHex => Arg::Str(crate::python::stdlib::uuid.uuid4().hex().into()),
        }
    }
}

/// `_CarrierRPCProtocol`'s pydantic fields.
pub const CARRIER_FIELDS: &[(&str, FieldDefault)] = &[
    ("codec", FieldDefault::Str("json")),
    ("rpc_timeout", FieldDefault::Float(60.0)),
    ("handshake_timeout", FieldDefault::Float(10.0)),
    ("ping_timeout", FieldDefault::Float(5.0)),
    ("peer_secret_key", FieldDefault::SecretHex),
    ("rpc_backoff_base", FieldDefault::Float(0.05)),
    ("rpc_backoff_max", FieldDefault::Float(5.0)),
    ("max_rpc_retries", FieldDefault::Int(5)),
    ("channel_queue_size", FieldDefault::Int(256)),
    ("channel_queue_bytes", FieldDefault::Int(64 * 1024 * 1024)),
    ("stream_chunk_bytes", FieldDefault::Int(65536)),
    (
        "max_stream_frame_bytes",
        FieldDefault::Int(64 * 1024 * 1024),
    ),
    ("static_lanes", FieldDefault::Dict),
];

/// `_DatagramRPCProtocol`'s extra fields.
pub const DATAGRAM_FIELDS: &[(&str, FieldDefault)] = &[
    ("mtu", FieldDefault::Int(1200)),
    ("ack_timeout", FieldDefault::Float(0.5)),
    ("max_retries", FieldDefault::Int(5)),
];

/// `host` / `port` of the IP transports.
pub const HOST_PORT_FIELDS: &[(&str, FieldDefault)] = &[
    ("host", FieldDefault::Str("0.0.0.0")),
    ("port", FieldDefault::Int(0)),
];

/// The static description of one transport class.
#[derive(Clone, Copy, Debug)]
pub struct ProtocolSpec {
    /// `cls.__name__`
    pub class_name: &'static str,
    /// `cls.__module__`
    pub module: &'static str,
    /// `cls.protocol_name`
    pub protocol_name: &'static str,
    /// `cls._TOKEN_ALIASES` (lower-case).
    pub tokens: &'static [&'static str],
    /// URI prefixes `can_handle_uri` claims (`"tcp://"`).
    pub schemes: &'static [&'static str],
    pub carrier: CarrierKind,
    pub wire: Wire,
    pub persistent: bool,
    pub supports_ping: bool,
    pub supports_channels: bool,
    /// Fields *beyond* the carrier's, with defaults.
    pub fields: &'static [(&'static str, FieldDefault)],
    /// `_DRIVER_MODULES` / `_DRIVER_EXTRA` of the lazy / driver transports.
    pub driver_modules: &'static [&'static str],
    pub driver_extra: &'static str,
    /// Ancestor class names, nearest first (`isinstance`).
    pub bases: &'static [&'static str],
    /// Short tier note for the parity table (`"works"`, `"config-required"`,
    /// `"driver/hardware"`).
    pub tier: &'static str,
    /// The scaffold's `_NOT_IMPLEMENTED` text (`CarrierKind::Base`) or the
    /// hardware / configuration requirement a driver transport raises from
    /// `start()` once its drivers are present (`""` when not applicable).
    pub note: &'static str,
}

/// Defaults for `ProtocolSpec { .., ..SPEC_DEFAULTS }` (a carrier transport
/// with no extra fields).
pub const SPEC_DEFAULTS: ProtocolSpec = ProtocolSpec {
    class_name: "",
    module: "",
    protocol_name: "",
    tokens: &[],
    schemes: &[],
    carrier: CarrierKind::Base,
    wire: Wire::None,
    persistent: true,
    supports_ping: true,
    supports_channels: false,
    fields: &[],
    driver_modules: &[],
    driver_extra: "",
    bases: &[],
    tier: "works",
    note: "",
};

impl ProtocolSpec {
    /// `cls.matches_token(token)`.
    pub fn matches_token(&self, token: &str) -> bool {
        let t = token.to_lowercase();
        if self.tokens.is_empty() {
            return t == self.protocol_name.to_lowercase();
        }
        self.tokens.iter().any(|a| *a == t)
    }
    /// `cls.can_handle_uri(uri)`.
    pub fn can_handle_uri(&self, uri: &str) -> bool {
        self.schemes.iter().any(|s| uri.starts_with(s))
    }
    /// `issubclass(cls, <name>)`.
    pub fn is_subclass_of(&self, class_name: &str) -> bool {
        self.class_name == class_name || self.bases.contains(&class_name)
    }
    /// Does this transport carry the `_CarrierRPCProtocol` field set?
    pub fn is_carrier(&self) -> bool {
        !matches!(self.carrier, CarrierKind::Base | CarrierKind::WebSocket)
    }
    /// Every config field with its default, in declaration order.
    pub fn all_fields(&self) -> Vec<(&'static str, FieldDefault)> {
        let mut out: Vec<(&'static str, FieldDefault)> = Vec::new();
        if self.is_carrier() {
            out.extend_from_slice(CARRIER_FIELDS);
        }
        if matches!(
            self.carrier,
            CarrierKind::Datagram | CarrierKind::LazyDatagram
        ) {
            out.extend_from_slice(DATAGRAM_FIELDS);
        }
        if self.carrier == CarrierKind::WebSocket {
            out.extend_from_slice(HOST_PORT_FIELDS);
            out.push(("peer_secret_key", FieldDefault::SecretHex));
            out.push(("rpc_timeout", FieldDefault::Float(60.0)));
            out.push(("ping_timeout", FieldDefault::Float(5.0)));
        }
        // The class's own fields: a redeclared field overrides the base
        // default (`stream_chunk_bytes = 4096` on the serial carriers).
        for f in self.fields {
            match out.iter_mut().find(|(n, _)| n == &f.0) {
                Some(slot) => *slot = *f,
                None => out.push(*f),
            }
        }
        out
    }
    /// The class object (its class-level attributes and class methods
    /// answered through [`ClassHooks`]).
    pub fn class_object(&'static self) -> crate::python::PyClass {
        let spec: &'static ProtocolSpec = self;
        crate::python::PyClass::with_ctor(
            self.class_name,
            self.module,
            Arc::new(move |a: Args| Protocol::new(spec, a).map(IntoArg::into_arg)),
        )
        .with_hooks(Arc::new(SpecClassHooks(spec)))
    }
    /// The direct parent class in the transport tree (`__bases__[0]`
    /// among the `_LAILA_IDENTIFIABLE_*` classes; `None` for the root).
    pub fn parent_class(&self) -> Option<&'static str> {
        self.bases
            .iter()
            .copied()
            .find(|b| b.starts_with("_LAILA_IDENTIFIABLE_"))
    }
    /// `cls.__subclasses__()`: the direct subclasses in the loaded tree.
    pub fn subclasses(&self) -> Vec<&'static ProtocolSpec> {
        super::ALL_SPECS
            .iter()
            .copied()
            .filter(|s| s.parent_class() == Some(self.class_name))
            .collect()
    }
}

/// `cls.<attr>` / `cls.<method>(..)` of a transport class.
struct SpecClassHooks(&'static ProtocolSpec);

impl SpecClassHooks {
    fn getattr_impl(&self, name: &str) -> Option<Arg> {
        let s = self.0;
        Some(match name {
            "protocol_name" => Arg::Str(s.protocol_name.into()),
            "persistent" => Arg::Bool(s.persistent),
            "supports_ping" => Arg::Bool(s.supports_ping),
            "supports_channels" => Arg::Bool(s.supports_channels),
            // `cls.model_fields`: the public config fields as pydantic `FieldInfo`s
            "model_fields" => {
                let mut d = PyDictBuilder::new();
                for (name, default) in s.all_fields() {
                    let mut f = crate::_compat::pydantic::FieldInfo::undefined();
                    f.default = Some(default.value());
                    d.insert(name, f);
                }
                d.finish()
            }
            "__bases__" => s
                .parent_class()
                .and_then(super::spec_by_class_name)
                .map(|p| alloc::vec![p.class_object().into_arg()])
                .unwrap_or_default()
                .into_arg(),
            "__mro__" => {
                let mut out = alloc::vec![s.class_object().into_arg()];
                for b in s.bases {
                    if let Some(p) = super::spec_by_class_name(b) {
                        out.push(p.class_object().into_arg());
                    }
                }
                out.into_arg()
            }
            // an instance method read on the class: `cls.connect(proto, uri, secret)`
            m if INSTANCE_METHODS.contains(&m) => {
                let n = String::from(m);
                crate::python::callable(move |a: Args| -> Result<Arg> {
                    let mut a = a;
                    if a.positional.is_empty() {
                        return Err(LailaError::type_error(alloc::format!(
                            "{n}() missing 1 required positional argument: 'self'"
                        )));
                    }
                    let me = a.positional.remove(0);
                    me.__call_method__(&n, a)
                })
            }
            _ => return None,
        })
    }
}

/// The instance methods of every transport (the uniform connection
/// surface), reachable unbound on the class object.
const INSTANCE_METHODS: &[&str] = &[
    "start",
    "stop",
    "connect",
    "disconnect",
    "ping",
    "send_rpc",
    "has_peer",
    "add_peer",
    "remove_peer",
    "peer_ids",
    "open_channel",
    "close_channel",
    "_try_admit",
    "_handle_request_frame",
    "_register_peer",
    "_unregister_peer",
    "_fail_pending",
    "_register_pending",
    "_complete_pending",
    "_encode",
    "_decode",
    "_make_rpc_request",
];

impl SpecClassHooks {
    fn call_method_impl(&self, name: &str, args: Args) -> Option<Result<Arg>> {
        let s = self.0;
        let one = |what: &str| -> Result<String> {
            args.positional.first().map(|a| a.__str__()).ok_or_else(|| {
                LailaError::type_error(alloc::format!(
                    "{what}() missing 1 required positional argument"
                ))
            })
        };
        Some(match name {
            "matches_token" => one(name).map(|t| Arg::Bool(s.matches_token(&t))),
            "can_handle_uri" => one(name).map(|u| Arg::Bool(s.can_handle_uri(&u))),
            "__subclasses__" => Ok(s
                .subclasses()
                .into_iter()
                .map(|c| c.class_object().into_arg())
                .collect::<Vec<_>>()
                .into_arg()),
            "mro" => crate::python::pyclass::ClassHooks::getattr(self, "__mro__").map(Ok)?,
            _ => return None,
        })
    }
}

impl crate::python::pyclass::ClassHooks for SpecClassHooks {
    fn getattr(&self, name: &str) -> Option<Arg> {
        self.getattr_impl(name)
    }
    fn call_method(&self, name: &str, args: Args) -> Option<Result<Arg>> {
        self.call_method_impl(name, args)
    }
}

/// The base class's spec (`_LAILA_IDENTIFIABLE_COMM_PROTOCOL` itself).
pub static BASE_SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.base",
    protocol_name: "base",
    tokens: &[],
    schemes: &[],
    carrier: CarrierKind::Base,
    wire: Wire::None,
    persistent: true,
    supports_ping: true,
    supports_channels: false,
    fields: &[],
    driver_modules: &[],
    driver_extra: "",
    bases: &[],
    tier: "abstract",
    note: "",
};

/// The verbs a Python *subclass* of a transport may override (a stub
/// carrier whose `_send_once` is replaced, a `_DeadProtocol` whose `ping`
/// is always `False`, ...). Rust has no subclassing of a value, so the
/// overrides are installed on the instance with [`Protocol::set_hooks`];
/// every verb returns `None` to fall through to the spec's carrier.
#[allow(unused_variables)]
pub trait ProtocolHooks: Send + Sync {
    fn start(&self, proto: &Protocol) -> Option<Result<()>> {
        None
    }
    fn stop(&self, proto: &Protocol) -> Option<Result<()>> {
        None
    }
    fn connect(&self, proto: &Protocol, uri: &str, secret: &str) -> Option<Result<String>> {
        None
    }
    fn disconnect(&self, proto: &Protocol, peer_id: &str) -> Option<Result<()>> {
        None
    }
    fn ping(&self, proto: &Protocol, peer_id: &str, timeout: Option<f64>) -> Option<bool> {
        None
    }
    fn has_peer(&self, proto: &Protocol, peer_id: &str) -> Option<bool> {
        None
    }
    fn send_rpc(
        &self,
        proto: &Protocol,
        peer_id: &str,
        path: &[String],
        args: &[Arg],
        kwargs: &Arg,
    ) -> Option<Result<Arg>> {
        None
    }
    /// The carrier's `_send_once` (one attempt; `send_rpc` adds the
    /// backoff loop around it).
    fn send_once(
        &self,
        proto: &Protocol,
        peer_id: &str,
        path: &[String],
        args: &[Arg],
        kwargs: &Arg,
    ) -> Option<Result<Arg>> {
        None
    }
}

/// The state behind a [`Protocol`] handle.
pub struct ProtocolInner {
    /// Subclass overrides of the verbs (see [`ProtocolHooks`]).
    pub hooks: RwLock<Option<Arc<dyn ProtocolHooks>>>,
    pub spec: &'static ProtocolSpec,
    pub identity: Identity,
    /// The pydantic fields (`codec`, `host`, ...) plus any attribute a
    /// caller set on the instance.
    pub fields: RwLock<Vec<(String, Arg)>>,
    /// `_communication` -- the owning hub.
    pub comm: RwLock<Option<Weak<CommInner>>>,
    /// `_CarrierRPCProtocol`'s private state (connections, pending RPCs,
    /// stream lanes, counters).
    pub carrier: CarrierState,
    /// The wire-specific state (listener, links, sockets).
    pub transport: Mutex<Transport>,
    pub me: Weak<ProtocolInner>,
}

/// A transport (handle; clones alias one transport).
#[derive(Clone)]
pub struct Protocol(pub Arc<ProtocolInner>);

impl Protocol {
    /// `<Class>(**fields)`.
    pub fn new(spec: &'static ProtocolSpec, args: impl Into<Args>) -> Result<Protocol> {
        let mut a: Args = args.into();
        // The CLI-capable before-validator: config fields not given
        // explicitly come from
        // `laila.args.policy.central.communication.connections.{gid}`.
        let eligible: Vec<&str> = spec
            .all_fields()
            .into_iter()
            .map(|(n, _)| n)
            .filter(|n| !n.starts_with('_'))
            .collect();
        crate::basics::definitions::cli_capable::resolve_cli_fields_into(
            _COMM_PROTOCOL_SCOPE,
            &eligible,
            &mut a,
        );
        if !a.positional.is_empty() {
            return Err(LailaError::type_error(alloc::format!(
                "{}() takes no positional arguments",
                spec.class_name
            )));
        }
        let mut id = Args::new();
        id.push_keyword(
            "default_scopes",
            alloc::vec![Arg::Str(_COMM_PROTOCOL_SCOPE.into())],
        );
        for key in ["uuid", "nickname", "scopes", "evolution", "global_id"] {
            let v = a.get(key);
            if !v.is_none() {
                id.push_keyword(key, v);
            }
        }
        let identity = Identity::from_args(id)?;
        let mut fields: Vec<(String, Arg)> = Vec::new();
        for (name, default) in spec.all_fields() {
            let given = a.get(name);
            let value = if given.is_none() && !a.__contains__(name)? {
                default.value()
            } else {
                coerce_field(name, &default, given)?
            };
            fields.push((String::from(name), value));
        }
        for (k, v) in &a.keywords {
            if ["uuid", "nickname", "scopes", "evolution", "global_id"].contains(&k.as_str()) {
                continue;
            }
            if !fields.iter().any(|(n, _)| n == k) {
                fields.push((k.clone(), v.clone()));
            }
        }
        let proto = Protocol(Arc::new_cyclic(|me| ProtocolInner {
            hooks: RwLock::new(None),
            spec,
            identity,
            fields: RwLock::new(fields),
            comm: RwLock::new(None),
            carrier: CarrierState::default(),
            transport: Mutex::new(Transport::default()),
            me: me.clone(),
        }));
        Ok(proto)
    }

    pub fn spec(&self) -> &'static ProtocolSpec {
        self.0.spec
    }
    pub fn global_id(&self) -> String {
        self.0.identity.global_id()
    }
    pub fn uuid(&self) -> String {
        self.0.identity.uuid.clone()
    }
    /// `cls.protocol_name`
    pub fn protocol_name(&self) -> &'static str {
        self.0.spec.protocol_name
    }
    /// `type(self).__name__`
    pub fn class_name(&self) -> &'static str {
        self.0.spec.class_name
    }
    /// `type(self).supports_channels`
    pub fn supports_channels(&self) -> bool {
        self.0.spec.supports_channels
    }
    /// Python `is`.
    pub fn is(&self, other: &Protocol) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    // ---------- fields ----------

    /// A config field / instance attribute (`None` when unset).
    pub fn field(&self, name: &str) -> Arg {
        self.0
            .fields
            .read()
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.clone())
            .unwrap_or(Arg::None)
    }
    pub fn has_field(&self, name: &str) -> bool {
        self.0.fields.read().iter().any(|(n, _)| n == name)
    }
    pub fn set_field(&self, name: &str, value: Arg) {
        let mut g = self.0.fields.write();
        match g.iter_mut().find(|(n, _)| n == name) {
            Some(slot) => slot.1 = value,
            None => g.push((String::from(name), value)),
        }
    }
    pub fn f64_field(&self, name: &str, default: f64) -> f64 {
        self.field(name).as_f64().unwrap_or(default)
    }
    pub fn i64_field(&self, name: &str, default: i64) -> i64 {
        self.field(name).as_i64().unwrap_or(default)
    }
    pub fn str_field(&self, name: &str) -> String {
        match self.field(name) {
            Arg::None => String::new(),
            v => v.__str__(),
        }
    }
    /// `self.codec`
    pub fn codec(&self) -> String {
        let c = self.str_field("codec");
        if c.is_empty() {
            String::from("json")
        } else {
            c
        }
    }
    /// `self.rpc_timeout`
    pub fn rpc_timeout(&self) -> f64 {
        self.f64_field("rpc_timeout", 60.0)
    }
    /// `self.handshake_timeout`
    pub fn handshake_timeout(&self) -> f64 {
        self.f64_field("handshake_timeout", 10.0)
    }
    /// `self.ping_timeout`
    pub fn ping_timeout(&self) -> f64 {
        self.f64_field("ping_timeout", 5.0)
    }
    /// `self.peer_secret_key`
    pub fn peer_secret_key(&self) -> String {
        self.str_field("peer_secret_key")
    }
    /// `self.host` (the socket transports' bind / peer address field).
    pub fn host(&self) -> Arg {
        self.__getattr__("host").unwrap_or(Arg::None)
    }
    /// `self.port` (the socket transports' port, or a serial device path).
    pub fn port(&self) -> Arg {
        self.__getattr__("port").unwrap_or(Arg::None)
    }

    // ---------- pending table (typed spellings of the dynamic layer) ----------

    /// `proto._pending_rpcs`: `{request_id: slot}`.
    pub fn _pending_rpcs(&self) -> Arg {
        self.0.carrier.pending_dict()
    }
    /// `proto._register_pending(peer_id=None) -> (request_id, slot)`.
    pub fn _register_pending(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let peer = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("peer_id"));
        let peer = if peer.is_none() {
            None
        } else {
            Some(peer.__str__())
        };
        let (rid, slot) = self.0.carrier.register_pending(peer.as_deref());
        Ok(Arg::Tuple(Arc::from(alloc::vec![
            Arg::Str(rid.into()),
            Arg::Object(Arc::new(super::_carriers::base::PendingSlot(slot))),
        ])))
    }
    /// `proto._fail_pending(reason, peer_id=None)`.
    pub fn _fail_pending(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let reason = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("reason"))
            .__str__();
        let peer = a
            .positional
            .get(1)
            .cloned()
            .unwrap_or_else(|| a.get("peer_id"));
        let peer = if peer.is_none() {
            None
        } else {
            Some(peer.__str__())
        };
        self.0.carrier.fail_pending(&reason, peer.as_deref());
        Ok(Arg::None)
    }
    /// `proto._inbound_executor`: the Rust carriers run inbound requests
    /// on their own threads; there is no pool to expose (`None`, as after
    /// `stop()` in Python).
    pub fn _inbound_executor(&self) -> Arg {
        Arg::None
    }
    /// `proto._communication` (the hub, or `None`).
    pub fn _communication(&self) -> Arg {
        match self.communication() {
            Some(c) => c.into_arg(),
            None => Arg::None,
        }
    }

    // ---------- communication ----------

    /// `self._communication`
    pub fn communication(&self) -> Option<Communication> {
        self.0
            .comm
            .read()
            .as_ref()
            .and_then(Weak::upgrade)
            .map(Communication)
    }
    pub fn set_communication(&self, comm: Option<&Communication>) {
        *self.0.comm.write() = comm.map(|c| Arc::downgrade(&c.0));
    }
    /// `self._communication.policy_id if self._communication else None`
    pub fn policy_id(&self) -> Option<String> {
        self.communication().and_then(|c| c.policy_id())
    }

    // ---------- the contract ----------

    /// Install the subclass overrides of the verbs (see [`ProtocolHooks`]).
    pub fn set_hooks(&self, hooks: Arc<dyn ProtocolHooks>) {
        *self.0.hooks.write() = Some(hooks);
    }
    /// The installed overrides, if any.
    pub fn hooks(&self) -> Option<Arc<dyn ProtocolHooks>> {
        self.0.hooks.read().clone()
    }

    /// `cls.matches_token(token)`
    pub fn matches_token(&self, token: &str) -> bool {
        self.0.spec.matches_token(token)
    }
    /// `cls.can_handle_uri(uri)`
    pub fn can_handle_uri(&self, uri: &str) -> bool {
        self.0.spec.can_handle_uri(uri)
    }

    fn not_implemented(&self, verb: &str) -> LailaError {
        if !self.0.spec.note.is_empty() {
            return LailaError::not_implemented(self.0.spec.note);
        }
        LailaError::not_implemented(alloc::format!(
            "{}.{verb}() is abstract on {}",
            self.0.spec.class_name,
            crate::python::pyrepr::str_repr(self.0.spec.protocol_name)
        ))
    }

    /// `proto.start()` (the unused `Args` keep the dynamic-call spelling
    /// `proto.start()` of `laila::python!` uniform with the threading shim).
    pub fn start(&self, _args: impl Into<Args>) -> Result<()> {
        if let Some(r) = self.hooks().and_then(|h| h.start(self)) {
            return r;
        }
        match self.0.spec.carrier {
            CarrierKind::Base => Err(self.not_implemented("start")),
            CarrierKind::Loopback => super::local::loopback::start(self),
            CarrierKind::Stream => super::_carriers::stream::start(self),
            CarrierKind::Datagram | CarrierKind::LazyDatagram => {
                super::_carriers::datagram::start(self)
            }
            CarrierKind::P2P => super::_carriers::p2p::start(self),
            CarrierKind::Broker => super::_carriers::broker::start(self),
            CarrierKind::Register => super::_carriers::register::start(self),
            CarrierKind::WebSocket => super::tcpip::start(self),
        }
    }
    /// `proto.stop()`
    pub fn stop(&self, _args: impl Into<Args>) -> Result<()> {
        if let Some(r) = self.hooks().and_then(|h| h.stop(self)) {
            return r;
        }
        match self.0.spec.carrier {
            // A planned-protocol scaffold's `stop()` is a safe no-op.
            CarrierKind::Base if !self.0.spec.note.is_empty() => Ok(()),
            CarrierKind::Base => Err(self.not_implemented("stop")),
            CarrierKind::Loopback => super::local::loopback::stop(self),
            CarrierKind::Stream => super::_carriers::stream::stop(self),
            CarrierKind::Datagram | CarrierKind::LazyDatagram => {
                super::_carriers::datagram::stop(self)
            }
            CarrierKind::P2P => super::_carriers::p2p::stop(self),
            CarrierKind::Broker => super::_carriers::broker::stop(self),
            CarrierKind::Register => super::_carriers::register::stop(self),
            CarrierKind::WebSocket => super::tcpip::stop(self),
        }
    }
    /// `proto.connect(uri, secret) -> peer gid` (Python-shaped arguments).
    pub fn connect(&self, args: impl Into<Args>) -> Result<String> {
        let a: Args = args.into();
        let uri = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("uri"));
        let secret = a
            .positional
            .get(1)
            .cloned()
            .unwrap_or_else(|| a.get("secret"));
        self.connect_to(&uri.__str__(), &secret.__str__())
    }
    /// `proto.connect(uri, secret) -> peer gid`
    pub fn connect_to(&self, uri: &str, secret: &str) -> Result<String> {
        if let Some(r) = self.hooks().and_then(|h| h.connect(self, uri, secret)) {
            return r;
        }
        match self.0.spec.carrier {
            CarrierKind::Base => Err(self.not_implemented("connect")),
            CarrierKind::Loopback => super::local::loopback::connect(self, uri, secret),
            CarrierKind::Stream => super::_carriers::stream::connect(self, uri, secret),
            CarrierKind::Datagram | CarrierKind::LazyDatagram => {
                super::_carriers::datagram::connect(self, uri, secret)
            }
            CarrierKind::P2P => super::_carriers::p2p::connect(self, uri, secret),
            CarrierKind::Broker => super::_carriers::broker::connect(self, uri, secret),
            CarrierKind::Register => super::_carriers::register::connect(self, uri, secret),
            CarrierKind::WebSocket => super::tcpip::connect(self, uri, secret),
        }
    }
    /// `proto.disconnect(peer_id)`
    pub fn disconnect(&self, peer_id: &str) -> Result<()> {
        if let Some(r) = self.hooks().and_then(|h| h.disconnect(self, peer_id)) {
            return r;
        }
        match self.0.spec.carrier {
            CarrierKind::Base => Ok(()),
            CarrierKind::Loopback => super::local::loopback::disconnect(self, peer_id),
            CarrierKind::WebSocket => super::tcpip::disconnect(self, peer_id),
            _ => super::_carriers::base::disconnect(self, peer_id),
        }
    }
    /// `proto.ping(peer_id, timeout=None) -> bool`
    pub fn ping(&self, peer_id: &str, timeout: Option<f64>) -> bool {
        if let Some(r) = self.hooks().and_then(|h| h.ping(self, peer_id, timeout)) {
            return r;
        }
        match self.0.spec.carrier {
            CarrierKind::Base => false,
            _ => super::_carriers::base::ping(self, peer_id, timeout),
        }
    }
    /// `proto.send_rpc(peer_id, path, args, kwargs)`
    pub fn send_rpc(
        &self,
        peer_id: impl IntoArg,
        path: impl IntoArg,
        args: impl IntoArg,
        kwargs: impl IntoArg,
    ) -> Result<Arg> {
        let peer_id = &peer_id.into_arg().__str__();
        let path: Vec<String> = path
            .into_arg()
            .__iter__()?
            .iter()
            .map(|s| s.__str__())
            .collect();
        let args = args.into_arg();
        let args: Vec<Arg> = if args.is_none() {
            Vec::new()
        } else {
            args.__iter__()?
        };
        let kwargs = kwargs.into_arg();
        let kwargs = if kwargs.is_none() {
            crate::python::PyDictBuilder::new().finish()
        } else {
            kwargs
        };
        if let Some(r) = self
            .hooks()
            .and_then(|h| h.send_rpc(self, peer_id, &path, &args, &kwargs))
        {
            return r;
        }
        match self.0.spec.carrier {
            CarrierKind::Base => Err(self.not_implemented("send_rpc")),
            CarrierKind::Loopback => {
                super::local::loopback::send_rpc(self, peer_id, path, args, kwargs)
            }
            _ => super::_carriers::base::send_rpc(self, peer_id, path, args, kwargs),
        }
    }
    /// `proto.has_peer(peer_id)`
    pub fn has_peer(&self, peer_id: &str) -> bool {
        if let Some(r) = self.hooks().and_then(|h| h.has_peer(self, peer_id)) {
            return r;
        }
        match self.0.spec.carrier {
            CarrierKind::Base => false,
            _ => self.0.carrier.has_peer(peer_id),
        }
    }
    /// `proto.open_channel(peer_id, name)`
    pub fn open_channel(&self, peer_id: &str, name: &str) -> Result<Channel> {
        super::_carriers::base::open_channel(self, peer_id, name)
    }
    /// `proto.channel_names(peer_id)`
    pub fn channel_names(&self, peer_id: &str) -> Vec<String> {
        self.0.carrier.channel_names(peer_id)
    }
    /// `proto.bound_port` (IP transports).
    pub fn bound_port(&self) -> i64 {
        match self.0.transport.lock().bound_port() {
            Some(p) => p as i64,
            None => self.i64_field("port", 0),
        }
    }
    /// `proto.bound_path` (unix socket).
    pub fn bound_path(&self) -> String {
        match self.0.transport.lock().bound_path() {
            Some(p) => p,
            None => super::local::unixsocket::effective_path(self),
        }
    }

    // ---------- dynamic layer ----------

    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "global_id" => Arg::Str(self.global_id().into()),
            "uuid" | "_uuid" => Arg::Str(self.uuid().into()),
            "scopes" | "_scopes" => self.0.identity.scopes_arg(),
            "evolution" | "_evolution" => self.0.identity.evolution_arg(),
            "protocol_name" => Arg::Str(self.0.spec.protocol_name.into()),
            "persistent" => self.0.spec.persistent.into_arg(),
            "supports_ping" => self.0.spec.supports_ping.into_arg(),
            "supports_channels" => self.0.spec.supports_channels.into_arg(),
            "_TOKEN_ALIASES" => {
                let toks: Vec<Arg> = self
                    .0
                    .spec
                    .tokens
                    .iter()
                    .map(|t| Arg::Str((*t).into()))
                    .collect();
                crate::python::builtins::frozenset(Args::from(toks.into_arg()))?
            }
            "_DRIVER_MODULES" => Arg::Tuple(Arc::from(
                self.0
                    .spec
                    .driver_modules
                    .iter()
                    .map(|m| Arg::Str((*m).into()))
                    .collect::<Vec<_>>(),
            )),
            "_DRIVER_EXTRA" => Arg::Str(self.0.spec.driver_extra.into()),
            "_communication" => match self.communication() {
                Some(c) => c.into_arg(),
                None => Arg::None,
            },
            "_started" => self.0.carrier.started().into_arg(),
            "_connections" => self.0.carrier.connections_dict(),
            "_pending_rpcs" => self.0.carrier.pending_dict(),
            "_peer_caps" => self.0.carrier.peer_caps_dict(),
            "_lanes" => self.0.carrier.lanes_dict(),
            "_lane_names" => self.0.carrier.lane_names_dict(),
            "_unacked" => super::_carriers::datagram::unacked_dict(self),
            "_inbound_executor" => self._inbound_executor(),
            "_unknown_lane_drops" => self.0.carrier.unknown_lane_drops().into_arg(),
            "_reserved_marker_drops" => self.0.carrier.reserved_marker_drops().into_arg(),
            "_malformed_stream_drops" => self.0.carrier.malformed_stream_drops().into_arg(),
            "_event_loop" => self.0.carrier.event_loop.lock().clone(),
            "_loop_thread" => self.0.carrier.loop_thread.lock().clone(),
            // `proto._link` of a point-to-point carrier: the open link's
            // writer (queue state), `None` before `start()` / after `stop()`.
            // `proto._serial` of a serial carrier: the open device
            // (`fileno()`), `None` before `start()` / after `stop()`.
            "_serial" if matches!(self.0.spec.carrier, CarrierKind::P2P) => {
                super::_carriers::p2p::serial_view(self)
            }
            "_link" if matches!(self.0.spec.carrier, CarrierKind::P2P) => {
                match self.0.transport.lock().p2p.clone() {
                    Some(st) => super::_carriers::stream::LinkView(st.link.clone()).into_arg(),
                    None => Arg::None,
                }
            }
            "_bound_port" => match self.0.transport.lock().bound_port() {
                Some(p) => (p as i64).into_arg(),
                None => Arg::None,
            },
            "_bound_path" => match self.0.transport.lock().bound_path() {
                Some(p) => p.into_arg(),
                None => Arg::None,
            },
            "bound_port" => self.bound_port().into_arg(),
            "bound_path" => self.bound_path().into_arg(),
            "__class__" => self.0.spec.class_object().into_arg(),
            // the inbound hook as a bound method value (`orig = pb._build_response`),
            // or the override installed on the instance
            "_build_response" => {
                if self.has_field(name) {
                    return Ok(self.field(name));
                }
                let me = self.clone();
                crate::python::callable(move |a: Args| -> Result<Arg> {
                    me.__call_method__("_build_response", a)
                })
            }
            "matches_token" | "can_handle_uri" => {
                let spec = self.0.spec;
                let which = name == "matches_token";
                crate::python::callable(move |a: Args| -> Result<Arg> {
                    let s = a.positional.first().cloned().unwrap_or(Arg::None).__str__();
                    Ok(if which {
                        spec.matches_token(&s)
                    } else {
                        spec.can_handle_uri(&s)
                    }
                    .into_arg())
                })
            }
            other => {
                if self.has_field(other) {
                    return Ok(self.field(other));
                }
                return Err(LailaError::attribute_error(alloc::format!(
                    "'{}' object has no attribute '{other}'",
                    self.0.spec.class_name
                )));
            }
        })
    }

    pub fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        match name {
            "_communication" => {
                let c = Communication::of(&value);
                self.set_communication(c.as_ref());
            }
            "_started" => self.0.carrier.set_started(value.__bool__()),
            "_event_loop" => *self.0.carrier.event_loop.lock() = value,
            "_loop_thread" => *self.0.carrier.loop_thread.lock() = value,
            "_bound_port" => self
                .0
                .transport
                .lock()
                .set_bound_port(value.as_i64().map(|p| p as u16)),
            other => self.set_field(other, value),
        }
        Ok(())
    }

    pub fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let pos = |i: usize| args.positional.get(i).cloned().unwrap_or(Arg::None);
        let kw = |k: &str| args.get(k);
        Ok(match name {
            "start" => {
                self.start(Args::new())?;
                Arg::None
            }
            "stop" => {
                self.stop(Args::new())?;
                Arg::None
            }
            "connect" => {
                let uri = if pos(0).is_none() { kw("uri") } else { pos(0) };
                let secret = if pos(1).is_none() {
                    kw("secret")
                } else {
                    pos(1)
                };
                Arg::Str(self.connect_to(&uri.__str__(), &secret.__str__())?.into())
            }
            "connect_tcp" | "connect_udp" | "connect_tls" | "connect_wifi" | "connect_ethernet"
            | "connect_tcpip" | "connect_ws" => {
                let scheme = match name {
                    "connect_tcp" => "tcp",
                    "connect_udp" => "udp",
                    "connect_tls" => "tls",
                    "connect_wifi" => "wifi",
                    "connect_ethernet" => "ethernet",
                    _ => "ws",
                };
                let host = if pos(0).is_none() { kw("host") } else { pos(0) };
                let port = if pos(1).is_none() { kw("port") } else { pos(1) };
                let secret = if pos(2).is_none() {
                    kw("secret")
                } else {
                    pos(2)
                };
                let uri = alloc::format!("{scheme}://{}:{}", host.__str__(), port.__str__());
                Arg::Str(self.connect_to(&uri, &secret.__str__())?.into())
            }
            "connect_unix" => {
                let path = if pos(0).is_none() { kw("path") } else { pos(0) };
                let secret = if pos(1).is_none() {
                    kw("secret")
                } else {
                    pos(1)
                };
                Arg::Str(
                    self.connect_to(
                        &alloc::format!("unix://{}", path.__str__()),
                        &secret.__str__(),
                    )?
                    .into(),
                )
            }
            "connect_loopback" => {
                let gid = if pos(0).is_none() {
                    kw("policy_global_id")
                } else {
                    pos(0)
                };
                let secret = if pos(1).is_none() {
                    kw("secret")
                } else {
                    pos(1)
                };
                Arg::Str(
                    self.connect_to(
                        &alloc::format!("loopback://{}", gid.__str__()),
                        &secret.__str__(),
                    )?
                    .into(),
                )
            }
            "disconnect" => {
                let peer = if pos(0).is_none() {
                    kw("peer_id")
                } else {
                    pos(0)
                };
                self.disconnect(&peer.__str__())?;
                Arg::None
            }
            "ping" => {
                let peer = if pos(0).is_none() {
                    kw("peer_id")
                } else {
                    pos(0)
                };
                let timeout = if pos(1).is_none() {
                    kw("timeout")
                } else {
                    pos(1)
                };
                self.ping(&peer.__str__(), timeout.as_f64()).into_arg()
            }
            "has_peer" => {
                let peer = if pos(0).is_none() {
                    kw("peer_id")
                } else {
                    pos(0)
                };
                self.has_peer(&peer.__str__()).into_arg()
            }
            "send_rpc" => {
                let peer = if pos(0).is_none() {
                    kw("peer_id")
                } else {
                    pos(0)
                };
                let path = if pos(1).is_none() { kw("path") } else { pos(1) };
                let a = if pos(2).is_none() { kw("args") } else { pos(2) };
                let k = if pos(3).is_none() {
                    kw("kwargs")
                } else {
                    pos(3)
                };
                let path: Vec<String> = path.__iter__()?.iter().map(|s| s.__str__()).collect();
                let a: Vec<Arg> = if a.is_none() {
                    Vec::new()
                } else {
                    a.__iter__()?
                };
                let k = if k.is_none() {
                    PyDictBuilder::new().finish()
                } else {
                    k
                };
                self.send_rpc(peer.__str__(), path, a, k)?
            }
            "open_channel" => {
                let peer = if pos(0).is_none() {
                    kw("peer_id")
                } else {
                    pos(0)
                };
                let cname = if pos(1).is_none() { kw("name") } else { pos(1) };
                self.open_channel(&peer.__str__(), &cname.__str__())?
                    .into_arg()
            }
            "channel_names" => {
                let peer = if pos(0).is_none() {
                    kw("peer_id")
                } else {
                    pos(0)
                };
                self.channel_names(&peer.__str__()).into_arg()
            }
            "_peer_has_lanes" => {
                let peer = if pos(0).is_none() {
                    kw("peer_id")
                } else {
                    pos(0)
                };
                self.0.carrier.peer_has_lanes(&peer.__str__()).into_arg()
            }
            "matches_token" => self.matches_token(&pos(0).__str__()).into_arg(),
            "can_handle_uri" => self.can_handle_uri(&pos(0).__str__()).into_arg(),
            "_build_response" => {
                let msg = pos(0);
                let peer = if pos(1).is_none() {
                    kw("peer_id")
                } else {
                    pos(1)
                };
                let peer = if peer.is_none() {
                    None
                } else {
                    Some(peer.__str__())
                };
                super::_carriers::base::_build_response(self, &msg, peer.as_deref())
            }
            "_handle_request_frame" => {
                let msg = pos(0);
                let reply = if pos(1).is_none() {
                    kw("reply")
                } else {
                    pos(1)
                };
                let peer = if pos(2).is_none() {
                    kw("peer_id")
                } else {
                    pos(2)
                };
                let peer = if peer.is_none() {
                    None
                } else {
                    Some(peer.__str__())
                };
                super::_carriers::base::_handle_request_frame(
                    self,
                    msg,
                    Arc::new(move |resp: Arg| {
                        let _ = reply.__call__(Args::from(resp));
                    }),
                    peer.as_deref(),
                );
                Arg::None
            }
            "_register_peer" => {
                let peer = pos(0).__str__();
                super::_carriers::base::_register_peer(
                    self,
                    &peer,
                    super::_carriers::base::PeerHandle::Virtual(pos(1)),
                );
                Arg::None
            }
            "_unregister_peer" => {
                super::_carriers::base::_unregister_peer(self, &pos(0).__str__());
                Arg::None
            }
            "_set_peer_caps" => {
                self.0.carrier.set_peer_caps(&pos(0).__str__(), pos(1));
                Arg::None
            }
            "_local_caps" => super::_carriers::base::_local_caps(self),
            "_fail_pending" => self._fail_pending(args.clone())?,
            "_register_pending" => self._register_pending(args.clone())?,
            "_inbound_executor" => self._inbound_executor(),
            "_complete_pending" => {
                self.0.carrier.complete_pending(&pos(0));
                Arg::None
            }
            "_on_stream_frame" => {
                let peer = pos(0);
                let peer = if peer.is_none() {
                    None
                } else {
                    Some(peer.__str__())
                };
                let raw = pos(1).as_bytes_like().unwrap_or_default();
                super::_carriers::base::_on_stream_frame(self, peer.as_deref(), &raw);
                Arg::None
            }
            "_on_stream_payload" => {
                let lane = pos(1).as_i64().unwrap_or(0) as u8;
                let raw = pos(2).as_bytes_like().unwrap_or_default();
                super::_carriers::base::_on_stream_payload(self, &pos(0).__str__(), lane, &raw);
                Arg::None
            }
            "_on_peer_goodbye" => {
                let peer = pos(0);
                if !peer.is_none() {
                    super::_carriers::base::_on_peer_goodbye(self, &peer.__str__());
                }
                Arg::None
            }
            "_encode" => super::_carriers::codec::encode(&pos(0), &self.codec())?.into_arg(),
            "_decode" => {
                let raw = pos(0).as_bytes_like().unwrap_or_default();
                super::_carriers::codec::decode(&raw, &self.codec())?
            }
            "_make_rpc_request" => {
                let path: Vec<String> = pos(0).__iter__()?.iter().map(|s| s.__str__()).collect();
                let a: Vec<Arg> = if pos(1).is_none() {
                    Vec::new()
                } else {
                    pos(1).__iter__()?
                };
                let k = if pos(2).is_none() {
                    PyDictBuilder::new().finish()
                } else {
                    pos(2)
                };
                super::_carriers::base::_make_rpc_request(&path, &a, &k, &pos(3).__str__())
            }
            "_require_drivers" => {
                let mods: Vec<String> = pos(0).__iter__()?.iter().map(|s| s.__str__()).collect();
                super::_carriers::base::_require_drivers(self, &mods, &pos(1).__str__())?
            }
            "_max_message_bytes" => super::_carriers::base::_max_message_bytes(self).into_arg(),
            "_try_admit" => super::_carriers::base::_try_admit(self).into_arg(),
            "_ensure_executor" | "_shutdown_executor" | "_shutdown_lanes" => {
                if name == "_shutdown_lanes" {
                    let reason = if pos(0).is_none() {
                        String::from("protocol stopped")
                    } else {
                        pos(0).__str__()
                    };
                    super::_carriers::base::_shutdown_lanes(self, &reason);
                }
                Arg::None
            }
            "model_dump" | "as_dict" => self.model_dump(),
            other => {
                // A callable attribute set on the instance (a test stub).
                if self.has_field(other) {
                    return self.field(other).__call__(args);
                }
                return Err(LailaError::attribute_error(alloc::format!(
                    "'{}' object has no attribute '{other}'",
                    self.0.spec.class_name
                )));
            }
        })
    }

    /// `proto.model_dump()`: the public fields.
    pub fn model_dump(&self) -> Arg {
        let mut d = PyDictBuilder::new();
        for (k, v) in self.0.fields.read().iter() {
            if !k.starts_with('_') {
                d.insert(k.as_str(), v.clone());
            }
        }
        d.finish()
    }

    /// The transport inside an `Arg`, if it is one.
    pub fn of(x: &Arg) -> Option<Protocol> {
        match x {
            Arg::Object(o) => o.as_any().downcast_ref::<Protocol>().cloned(),
            _ => None,
        }
    }
    /// The handle for an inner state.
    pub fn from_inner(inner: &Weak<ProtocolInner>) -> Option<Protocol> {
        inner.upgrade().map(Protocol)
    }
    /// `Weak<dyn ChannelOwner>` for a new [`Channel`].
    pub fn owner_weak(&self) -> Weak<dyn ChannelOwner> {
        let w: Weak<ProtocolInner> = self.0.me.clone();
        w
    }
}

/// Field coercion (pydantic's lax mode for the handful of typed fields;
/// a field the spec declares with a string default -- Modbus RTU's serial
/// `port` -- is a string).
fn coerce_field(name: &str, default: &FieldDefault, value: Arg) -> Result<Arg> {
    if matches!(default, FieldDefault::Str(_)) {
        return Ok(value);
    }
    Ok(match name {
        "port"
        | "mtu"
        | "max_retries"
        | "max_rpc_retries"
        | "channel_queue_size"
        | "channel_queue_bytes"
        | "stream_chunk_bytes"
        | "max_stream_frame_bytes" => match value.as_i64() {
            Some(i) => Arg::Int(i),
            None => match value.as_f64() {
                Some(f) if f.fract() == 0.0 => Arg::Int(f as i64),
                _ => {
                    return Err(LailaError::value_error(alloc::format!(
                        "Input should be a valid integer for field {name}"
                    )))
                }
            },
        },
        "rpc_timeout" | "handshake_timeout" | "ping_timeout" | "rpc_backoff_base"
        | "rpc_backoff_max" | "ack_timeout" => match value.as_f64() {
            Some(f) => Arg::Float(f),
            None => {
                return Err(LailaError::value_error(alloc::format!(
                    "Input should be a valid number for field {name}"
                )))
            }
        },
        "codec" => {
            let c = value.__str__();
            if !super::_carriers::codec::CODECS.contains(&c.as_str()) {
                return Err(LailaError::value_error(alloc::format!(
                    "Unknown codec {}; expected one of ('json', 'msgpack').",
                    crate::python::pyrepr::str_repr(&c)
                )));
            }
            Arg::Str(c.into())
        }
        _ => value,
    })
}

impl ChannelOwner for ProtocolInner {
    fn _stream_enqueue(&self, channel: &Channel, payload: &[u8]) -> Result<()> {
        let Some(p) = Protocol::from_inner(&self.me) else {
            return Err(LailaError::connection_error("transport is gone"));
        };
        super::_carriers::base::_stream_enqueue(&p, channel, payload)
    }
    fn _on_channel_closed_locally(&self, channel: &Channel) {
        if let Some(p) = Protocol::from_inner(&self.me) {
            super::_carriers::base::_on_channel_closed_locally(&p, channel);
        }
    }
    fn rpc_timeout(&self) -> f64 {
        Protocol::from_inner(&self.me)
            .map(|p| p.rpc_timeout())
            .unwrap_or(60.0)
    }
    fn stream_chunk_bytes(&self) -> usize {
        Protocol::from_inner(&self.me)
            .map(|p| p.i64_field("stream_chunk_bytes", 65536).max(1) as usize)
            .unwrap_or(65536)
    }
    fn as_arg(&self) -> Arg {
        match Protocol::from_inner(&self.me) {
            Some(p) => p.into_arg(),
            None => Arg::None,
        }
    }
}

impl core::fmt::Debug for Protocol {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}({})", self.0.spec.class_name, self.global_id())
    }
}
impl PyObject for Protocol {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        self.0.spec.class_name
    }
    fn __repr__(&self) -> String {
        alloc::format!("{self:?}")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Protocol>()
            .is_some_and(|o| o.is(self))
    }
    /// Two handles of one transport are the same Python object.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        self.__eq__(other)
    }
    /// `type(proto)`: the transport's class object.
    fn __class__(&self) -> Option<crate::python::PyClass> {
        Some(self.0.spec.class_object())
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        Protocol::__getattr__(self, name)
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        Protocol::__setattr__(self, name, value)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        Protocol::__call_method__(self, name, args)
    }
    /// The `laila.args.environment` mirror fields: the public config
    /// fields plus identity.
    fn __cli_fields__(&self) -> Option<Vec<(String, Arg)>> {
        let mut out: Vec<(String, Arg)> = Vec::new();
        for (k, v) in self.0.fields.read().iter() {
            if !k.starts_with('_') {
                out.push((k.clone(), v.clone()));
            }
        }
        out.push((String::from("evolution"), self.0.identity.evolution_arg()));
        out.push((String::from("global_id"), Arg::Str(self.global_id().into())));
        out.push((String::from("scopes"), self.0.identity.scopes_arg()));
        out.push((String::from("uuid"), Arg::Str(self.uuid().into())));
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Some(out)
    }
}
impl IntoArg for Protocol {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for Protocol {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl HasIdentity for Protocol {
    fn global_id(&self) -> String {
        Protocol::global_id(self)
    }
    fn uuid(&self) -> String {
        Protocol::uuid(self)
    }
}

/// Declare a transport class marker: a unit struct that is callable as the
/// class (`DefaultTCPProtocol(codec="json")`), answers `isinstance` for
/// the class and its subclasses, and converts to the class object.
#[macro_export]
macro_rules! protocol_class {
    ($ty:ident, $spec:expr) => {
        #[allow(non_camel_case_types)]
        #[derive(Clone, Copy, Debug, Default)]
        pub struct $ty;

        impl $ty {
            /// The static spec of the class.
            pub fn spec(
            ) -> &'static $crate::policy::central::communication::protocols::base::ProtocolSpec
            {
                &$spec
            }
            #[allow(clippy::new_ret_no_self)]
            pub fn new(
                args: impl Into<$crate::python::Args>,
            ) -> $crate::python::Result<
                $crate::policy::central::communication::protocols::base::Protocol,
            > {
                $crate::policy::central::communication::protocols::base::Protocol::new(&$spec, args)
            }
            pub fn class_object() -> $crate::python::PyClass {
                $spec.class_object()
            }
            /// `cls.matches_token(token)`
            pub fn matches_token(token: impl $crate::python::IntoArg) -> bool {
                $spec.matches_token(&$crate::python::IntoArg::into_arg(token).__str__())
            }
            /// `cls.can_handle_uri(uri)`
            pub fn can_handle_uri(uri: impl $crate::python::IntoArg) -> bool {
                $spec.can_handle_uri(&$crate::python::IntoArg::into_arg(uri).__str__())
            }
            /// `cls.protocol_name`
            pub fn protocol_name() -> &'static str {
                $spec.protocol_name
            }
        }
        impl $crate::python::builtins::IsInstance for $ty {
            fn isinstance(x: &$crate::python::Arg) -> bool {
                $crate::policy::central::communication::protocols::base::Protocol::of(x)
                    .is_some_and(|p| p.spec().is_subclass_of($spec.class_name))
            }
        }
        impl $crate::python::IntoArg for $ty {
            fn into_arg(self) -> $crate::python::Arg {
                $crate::python::IntoArg::into_arg(Self::class_object())
            }
        }
        impl $crate::python::PyArg for $ty {
            fn to_arg(&self) -> $crate::python::Arg {
                $crate::python::IntoArg::into_arg(Self::class_object())
            }
        }
        impl $crate::python::builtins::CallValue for $ty {
            fn call_value(
                &self,
                args: $crate::python::Args,
            ) -> $crate::python::Result<$crate::python::Arg> {
                Self::new(args).map($crate::python::IntoArg::into_arg)
            }
        }
        impl $crate::python::builtins::ClassValue for $ty {
            fn class_value() -> $crate::python::Arg {
                $crate::python::IntoArg::into_arg(Self::class_object())
            }
        }
    };
}

protocol_class!(_LAILA_IDENTIFIABLE_COMM_PROTOCOL, BASE_SPEC);

impl _LAILA_IDENTIFIABLE_COMM_PROTOCOL {
    /// `cls.persistent`
    pub const persistent: bool = true;
    /// `cls.supports_ping`
    pub const supports_ping: bool = true;
    /// `cls.supports_channels`
    pub const supports_channels: bool = false;
}

/// `str(x)` for a dynamic arg that may be a protocol: its `protocol_name`.
pub fn protocol_name_of(x: &Arg) -> String {
    Protocol::of(x)
        .map(|p| p.protocol_name().to_string())
        .unwrap_or_else(|| x.__str__())
}
