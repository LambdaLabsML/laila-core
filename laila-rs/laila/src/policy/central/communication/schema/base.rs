//! Core communication sub-system for inter-policy peer-to-peer RPC.
//!
//! [`Communication`] (`_LAILA_IDENTIFIABLE_COMMUNICATION`) is the *central
//! communication* subsystem of every laila policy. Its job is to mediate
//! calls between local and remote policies without any of the upstream
//! code having to know which transport is in play. Concretely it owns
//! three things:
//!
//! - A *protocol registry* (`connections`): one or more transport-level
//!   drivers. Each protocol implements its own listener loop,
//!   peer-handshake, and wire encoding. The communication object only
//!   ever asks "do you handle this URI?" or "do you have this peer
//!   connected?" -- everything else is delegated.
//! - A *peer registry* (`peers`): a [`PeerRegistry`] from remote-policy
//!   `global_id` to a [`PeerProxy`]. Proxies look like local policies to
//!   the rest of the codebase but route every method call through
//!   [`Communication::_send_rpc`]; indexing one with a channel name
//!   (`peers[gid]["video"]`) yields a stream `Channel`.
//! - An *inbound dispatcher* ([`Communication::_execute_rpc`]): when a
//!   protocol finishes deserializing an incoming RPC frame it hands the
//!   dotted attribute path + args/kwargs back to the communication
//!   object, which walks the path on the local policy and invokes the
//!   resolved method.
//!
//! The class is also responsible for *future virtualisation*: when a
//! remote call returns a future-shaped envelope (as marked by the
//! `__laila_future__` key) it transparently wraps the envelope in a
//! `RemoteFuture` that proxies status / wait / result calls back to the
//! originating peer (see [`Communication::_maybe_wrap_remote_future`]).

use alloc::string::{String, ToString};
use alloc::sync::{Arc, Weak};
use alloc::vec::Vec;

use crate::atomic::definitions::HasIdentity;
use crate::basics::definitions::identifiable_object::Identity;
use crate::macros::strings::_CENTRAL_COMMUNICATION_SCOPE;
use crate::policy::central::command::schema::future::future::RemoteFuture;
use crate::policy::{Policy, PolicyState};
use crate::python::object::PyObject;
use crate::python::sync::{Mutex, RwLock};
use crate::python::{pysig, Arg, Args, ExcKind, IntoArg, LailaError, PyArg, Result};

use super::super::protocols::base::Protocol;
use super::super::registry::{PeerProxy, PeerRegistry};

/// `log = logging.getLogger(__name__)` -- the module logger.
pub fn log() -> crate::python::stdlib::logging::Logger {
    crate::python::stdlib::logging::Logger::get("laila.policy.central.communication.schema.base")
}

/// The background liveness loop: its stop flag and the thread.
struct Liveness {
    stop: crate::python::stdlib::Event,
    thread: Option<crate::python::stdlib::Thread>,
}

/// The inbound-RPC admission gate (`threading.BoundedSemaphore`): the
/// cap is fixed when the gate is first used, as in Python.
struct RpcGate {
    cap: i64,
    inflight: i64,
}

pub struct CommInner {
    pub(crate) identity: Mutex<Identity>,
    /// `policy_id`: `global_id` of the owning policy (CLI-exempt).
    pub(crate) policy_id: Mutex<Arg>,
    /// `peers`: remote-policy gid -> [`PeerProxy`] (the live registry).
    pub(crate) peers: PeerRegistry,
    /// `connections`: protocol gid -> protocol, in registration order.
    pub(crate) connections: Mutex<Vec<(String, Protocol)>>,
    /// How often (seconds) the liveness loop pings each peer.
    pub(crate) liveness_interval: Mutex<f64>,
    /// Whether the liveness loop runs at all.
    pub(crate) liveness_enabled: Mutex<bool>,
    /// Max inbound RPCs this policy will run/queue concurrently across all
    /// transports before rejecting new ones with `ERR_BUSY`.
    pub(crate) max_inflight_rpcs: Mutex<i64>,
    /// `_local_policy` (weak: the policy owns the hub).
    pub(crate) local_policy: RwLock<Option<Weak<PolicyState>>>,
    /// `_local_policy = <stand-in>`: a test's stub object answering the
    /// RPC paths in place of a policy.
    pub(crate) local_stub: RwLock<Option<Arg>>,
    liveness: Mutex<Option<Liveness>>,
    rpc_gate: Mutex<Option<RpcGate>>,
    /// Dynamic / private attributes set from the outside (`comm.x = 1`).
    pub(crate) attrs: crate::python::object::PyAttrs,
}

/// `_LAILA_IDENTIFIABLE_COMMUNICATION` (handle; clones alias one hub).
#[derive(Clone)]
pub struct Communication(pub Arc<CommInner>);

impl Communication {
    pub const CLASS_NAME: &'static str = "_LAILA_IDENTIFIABLE_COMMUNICATION";

    /// `DefaultCentralCommunication(policy_id=None, peers=None, connections=None,
    /// liveness_interval=15.0, liveness_enabled=True, max_inflight_rpcs=1000,
    /// uuid=.., nickname=.., scopes=.., evolution=..)`.
    #[pysig(
        "__init__(*, policy_id=None, peers=None, connections=None, liveness_interval=15.0, liveness_enabled=True, max_inflight_rpcs=1000, uuid=None, nickname=None, scopes=None, evolution=None, global_id=None)"
    )]
    pub fn new(args: impl Into<Args>) -> Result<Communication> {
        let mut id = Args::new();
        id.push_keyword(
            "default_scopes",
            alloc::vec![Arg::Str(_CENTRAL_COMMUNICATION_SCOPE.into())],
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
        let registry = match PeerRegistry::of(&peers) {
            Some(r) => r,
            None if peers.is_none() => PeerRegistry::new(),
            None => PeerRegistry::from_args(Args::from(peers))?,
        };
        let mut protos: Vec<(String, Protocol)> = Vec::new();
        if !connections.is_none() {
            for v in connections.values()?.__iter__()? {
                let p = Protocol::of(&v).ok_or_else(|| {
                    crate::_compat::pydantic::validation_error(
                        Self::CLASS_NAME,
                        "connections",
                        "Input should be a communication protocol",
                        "is_instance_of",
                        &v,
                    )
                })?;
                protos.push((p.global_id(), p));
            }
        }
        let interval = if liveness_interval.is_none() {
            15.0
        } else {
            liveness_interval.as_f64().ok_or_else(|| {
                crate::_compat::pydantic::validation_error(
                    Self::CLASS_NAME,
                    "liveness_interval",
                    "Input should be a valid number",
                    "float_type",
                    &liveness_interval,
                )
            })?
        };
        let enabled = if liveness_enabled.is_none() {
            true
        } else {
            liveness_enabled.__bool__()
        };
        let max_inflight = if max_inflight_rpcs.is_none() {
            1000
        } else {
            max_inflight_rpcs.as_i64().ok_or_else(|| {
                crate::_compat::pydantic::validation_error(
                    Self::CLASS_NAME,
                    "max_inflight_rpcs",
                    "Input should be a valid integer",
                    "int_type",
                    &max_inflight_rpcs,
                )
            })?
        };
        let comm = Communication(Arc::new(CommInner {
            identity: Mutex::new(Identity::from_args(id)?),
            policy_id: Mutex::new(policy_id),
            peers: registry,
            connections: Mutex::new(protos),
            liveness_interval: Mutex::new(interval),
            liveness_enabled: Mutex::new(enabled),
            max_inflight_rpcs: Mutex::new(max_inflight),
            local_policy: RwLock::new(None),
            local_stub: RwLock::new(None),
            liveness: Mutex::new(None),
            rpc_gate: Mutex::new(None),
            attrs: crate::python::object::PyAttrs::new(),
        }));
        for (_, p) in comm.0.connections.lock().iter() {
            p.set_communication(Some(&comm));
        }
        Ok(comm)
    }

    // ---------- identity / fields ----------

    pub fn global_id(&self) -> String {
        self.0.identity.lock().global_id()
    }
    pub fn uuid(&self) -> String {
        self.0.identity.lock().uuid.clone()
    }
    /// `comm.policy_id` (`None` when unset).
    pub fn policy_id(&self) -> Option<String> {
        let p = self.0.policy_id.lock();
        if p.is_none() {
            None
        } else {
            Some(p.__str__())
        }
    }
    pub fn policy_id_arg(&self) -> Arg {
        self.0.policy_id.lock().clone()
    }
    pub fn set_policy_id(&self, policy_id: Arg) {
        *self.0.policy_id.lock() = policy_id;
    }
    /// `comm.peers` -- the live registry.
    pub fn peers(&self) -> PeerRegistry {
        self.0.peers.clone()
    }
    /// `comm.connections` -- a live `{gid: protocol}` view.
    pub fn connections(&self) -> ConnectionRegistry {
        ConnectionRegistry(self.clone())
    }
    /// `list(comm.connections.values())`
    pub fn connections_vec(&self) -> Vec<Protocol> {
        self.0
            .connections
            .lock()
            .iter()
            .map(|(_, p)| p.clone())
            .collect()
    }
    /// `comm.connections.get(gid)`
    pub fn connection(&self, gid: &str) -> Option<Protocol> {
        self.0
            .connections
            .lock()
            .iter()
            .find(|(g, _)| g == gid)
            .map(|(_, p)| p.clone())
    }
    pub fn liveness_interval(&self) -> f64 {
        *self.0.liveness_interval.lock()
    }
    pub fn set_liveness_interval(&self, v: f64) {
        *self.0.liveness_interval.lock() = v;
    }
    pub fn liveness_enabled(&self) -> bool {
        *self.0.liveness_enabled.lock()
    }
    pub fn set_liveness_enabled(&self, v: bool) {
        *self.0.liveness_enabled.lock() = v;
    }
    pub fn max_inflight_rpcs(&self) -> i64 {
        *self.0.max_inflight_rpcs.lock()
    }
    pub fn set_max_inflight_rpcs(&self, v: i64) {
        *self.0.max_inflight_rpcs.lock() = v;
    }
    /// `comm._local_policy`
    pub fn local_policy(&self) -> Option<Policy> {
        self.0
            .local_policy
            .read()
            .as_ref()
            .and_then(Weak::upgrade)
            .map(Policy)
    }
    /// `comm._local_policy = policy`
    pub fn set_local_policy(&self, policy: Option<&Policy>) {
        *self.0.local_stub.write() = None;
        *self.0.local_policy.write() = policy.map(|p| Arc::downgrade(&p.0));
    }
    /// Python `is`.
    pub fn is(&self, other: &Communication) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    // ------------------------------------------------------------------
    // Protocol management
    // ------------------------------------------------------------------

    /// Register and start a transport protocol: sets the protocol's
    /// back-reference, adds it to the registry, and calls
    /// `protocol.start(Args::new())` so the connection is live when this returns.
    pub fn add_connection(&self, protocol: impl IntoArg) -> Result<()> {
        let protocol = &Self::proto_of(protocol.into_arg())?;
        protocol.set_communication(Some(self));
        let gid = protocol.global_id();
        {
            let mut c = self.0.connections.lock();
            match c.iter_mut().find(|(g, _)| *g == gid) {
                Some(slot) => slot.1 = protocol.clone(),
                None => c.push((gid, protocol.clone())),
            }
        }
        protocol.start(Args::new())
    }
    /// Stop a transport protocol and remove it from this hub. The protocol
    /// is responsible for all its own cleanup.
    pub fn remove_connection(&self, protocol: impl IntoArg) -> Result<()> {
        let protocol = &Self::proto_of(protocol.into_arg())?;
        let gid = protocol.global_id();
        if self.connection(&gid).is_none() {
            return Ok(());
        }
        protocol.stop(Args::new())?;
        self.0.connections.lock().retain(|(g, _)| *g != gid);
        protocol.set_communication(None);
        Ok(())
    }
    /// A protocol argument (`add_connection(proto)`), typed or dynamic.
    fn proto_of(a: Arg) -> Result<Protocol> {
        Protocol::of(&a).ok_or_else(|| {
            LailaError::type_error(alloc::format!(
                "expected a communication protocol, got {}",
                a.type_name()
            ))
        })
    }
    /// `path` of an RPC as `list[str]`.
    fn path_of(path: Arg) -> Result<Vec<String>> {
        Ok(path.__iter__()?.iter().map(|s| s.__str__()).collect())
    }
    fn no_protocols() -> LailaError {
        LailaError::connection_error(
            "No communication protocols configured. Call add_connection() first.",
        )
    }
    /// Find a registered protocol that can handle *uri*: the first that
    /// claims it, else the first registered one as a best-effort fallback.
    pub fn _resolve_protocol_for_uri(&self, uri: &str) -> Result<Protocol> {
        let protos = self.connections_vec();
        if protos.is_empty() {
            return Err(Self::no_protocols());
        }
        for p in &protos {
            if p.can_handle_uri(uri) {
                return Ok(p.clone());
            }
        }
        Ok(protos[0].clone())
    }
    /// Find a registered protocol matching a transport *token* (`"tcpip"`,
    /// `"lora"`, ..); `None` is the first registered protocol.
    pub fn _resolve_protocol_for_token(&self, token: Option<&str>) -> Result<Protocol> {
        let protos = self.connections_vec();
        if protos.is_empty() {
            return Err(Self::no_protocols());
        }
        let Some(token) = token else {
            return Ok(protos[0].clone());
        };
        for p in &protos {
            if p.matches_token(token) {
                return Ok(p.clone());
            }
        }
        Err(LailaError::connection_error(alloc::format!(
            "No registered communication protocol matches {}. Register one with add_connection().",
            crate::python::pyrepr::str_repr(token)
        )))
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    /// Start every registered protocol's listener loop (idempotent at the
    /// protocol level) and the liveness loop.
    pub fn start(&self, _args: impl Into<Args>) -> Result<()> {
        for p in self.connections_vec() {
            p.start(Args::new())?;
        }
        self._start_liveness();
        let _ = log().info(
            "Communication started for policy %s",
            alloc::vec![self.policy_id_arg()],
        );
        Ok(())
    }
    /// Stop every registered protocol and drop all peer proxies.
    /// Best-effort: a protocol whose `stop()` raises is skipped and the
    /// remaining ones still stop.
    pub fn stop(&self, _args: impl Into<Args>) -> Result<()> {
        self._stop_liveness();
        for p in self.connections_vec() {
            if p.stop(Args::new()).is_err() {
                let _ = log().warning(
                    "Protocol %s failed to stop cleanly for policy %s",
                    alloc::vec![Arg::Str(p.class_name().into()), self.policy_id_arg()],
                );
            }
        }
        for peer_id in self.0.peers.keys_vec() {
            self._unregister_peer(&peer_id);
        }
        let _ = log().info(
            "Communication stopped for policy %s",
            alloc::vec![self.policy_id_arg()],
        );
        Ok(())
    }

    // ------------------------------------------------------------------
    // Peer management
    // ------------------------------------------------------------------

    /// Initiate a peering connection to a remote policy at *uri*; returns
    /// the `global_id` of the newly peered remote policy.
    pub fn add_peer(&self, uri: impl IntoArg, secret: impl IntoArg) -> Result<String> {
        let uri = uri.into_arg().__str__();
        let secret = secret.into_arg().__str__();
        let proto = self._resolve_protocol_for_uri(&uri)?;
        proto.connect_to(&uri, &secret)
    }
    /// Peer with a remote policy over TCP/IP (WebSocket): `ws://host:port`.
    pub fn add_tcpip_peer(
        &self,
        host: impl IntoArg,
        port: impl IntoArg,
        secret: impl IntoArg,
    ) -> Result<String> {
        self.add_peer(
            alloc::format!(
                "ws://{}:{}",
                host.into_arg().__str__(),
                port.into_arg().__str__()
            ),
            secret.into_arg().__str__(),
        )
    }
    /// Disconnect *peer_id* via whichever protocol holds it. Idempotent.
    pub fn remove_peer(&self, peer_id: impl IntoArg) {
        let peer_id = &peer_id.into_arg().__str__();
        for p in self.connections_vec() {
            if p.has_peer(peer_id) {
                let _ = p.disconnect(peer_id);
                return;
            }
        }
        self._unregister_peer(peer_id);
    }
    /// The registered protocol currently holding *peer_id*, if any.
    pub fn _holder_protocol(&self, peer_id: &str) -> Option<Protocol> {
        self.connections_vec()
            .into_iter()
            .find(|p| p.has_peer(peer_id))
    }

    // ------------------------------------------------------------------
    // Liveness (per-peer ping loop)
    // ------------------------------------------------------------------

    /// Start the background liveness loop (idempotent).
    pub fn _start_liveness(&self) {
        if !self.liveness_enabled() {
            return;
        }
        let mut slot = self.0.liveness.lock();
        if let Some(l) = slot.as_ref() {
            if l.thread.as_ref().is_some_and(|t| t.is_alive(Args::new())) {
                return;
            }
        }
        let stop = crate::python::stdlib::Event::new(Args::new());
        let weak = Arc::downgrade(&self.0);
        let stop2 = stop.clone();
        let body: crate::python::object::Callable = Arc::new(move |_a: Args| {
            Communication::_liveness_loop(&weak, &stop2);
            Ok(Arg::None)
        });
        let mut a = Args::new();
        a.push_keyword("target", Arg::Callable(body));
        a.push_keyword("daemon", true);
        a.push_keyword(
            "name",
            alloc::format!(
                "comm-liveness-{}",
                self.policy_id().unwrap_or_else(|| String::from("None"))
            ),
        );
        let thread = crate::python::stdlib::Thread::new(a)
            .and_then(|t| t.start(Args::new()).map(|()| t))
            .ok();
        *slot = Some(Liveness { stop, thread });
    }
    /// Stop the background liveness loop (idempotent).
    pub fn _stop_liveness(&self) {
        let taken = self.0.liveness.lock().take();
        if let Some(l) = taken {
            l.stop.set(Args::new());
            if let Some(t) = l.thread {
                let mut a = Args::new();
                a.push_keyword("timeout", 2.0);
                let _ = t.join(a);
            }
        }
    }
    /// `comm._liveness_thread is not None and is_alive()`
    pub fn _liveness_running(&self) -> bool {
        self.0
            .liveness
            .lock()
            .as_ref()
            .and_then(|l| l.thread.as_ref())
            .is_some_and(|t| t.is_alive(Args::new()))
    }
    /// Ping each peer every `liveness_interval` and drop dead ones. Runs
    /// entirely off the data path on its own thread. Only protocols that
    /// are `persistent` and `supports_ping` are probed.
    fn _liveness_loop(weak: &Weak<CommInner>, stop: &crate::python::stdlib::Event) {
        loop {
            let interval = match weak.upgrade() {
                Some(c) => *c.liveness_interval.lock(),
                None => return,
            };
            let mut a = Args::new();
            a.push_keyword("timeout", interval);
            if stop.wait(a).unwrap_or(true) {
                return;
            }
            let Some(inner) = weak.upgrade() else {
                return;
            };
            let comm = Communication(inner);
            for peer_id in comm.0.peers.keys_vec() {
                if stop.is_set(Args::new()) {
                    return;
                }
                let Some(proto) = comm._holder_protocol(&peer_id) else {
                    continue;
                };
                let spec = proto.spec();
                if !(spec.persistent && spec.supports_ping) {
                    continue;
                }
                let deadline = proto.ping_timeout();
                let alive = proto.ping(&peer_id, Some(deadline));
                if !alive {
                    let _ = proto.disconnect(&peer_id);
                    comm._unregister_peer(&peer_id);
                }
            }
        }
    }
    /// Create a proxy for a newly connected peer. Called by protocol
    /// instances after a successful handshake. One [`PeerProxy`] is built
    /// and stored in *both* `self.peers` and `laila._remote_policies`.
    pub fn _register_peer(&self, peer_id: &str) {
        if !self.0.peers.contains(peer_id) {
            let proxy = PeerProxy::build(peer_id, self, Arg::None);
            self.0.peers.insert(peer_id, proxy.clone());
            super::super::_register_remote_policy(peer_id, proxy.into_arg());
            // ensure liveness monitoring is running once we have a peer
            self._start_liveness();
        }
    }
    /// Remove a peer proxy after the transport connection closes (also
    /// from `laila._remote_policies`).
    pub fn _unregister_peer(&self, peer_id: &str) {
        self.0.peers.remove(peer_id);
        super::super::_unregister_remote_policy(peer_id);
    }

    // ------------------------------------------------------------------
    // Inbound admission control (backpressure)
    // ------------------------------------------------------------------

    /// Claim an inbound-RPC slot without blocking. `True` if admitted.
    pub fn _acquire_rpc_slot(&self) -> bool {
        let mut g = self.0.rpc_gate.lock();
        let gate = g.get_or_insert_with(|| RpcGate {
            cap: self.max_inflight_rpcs().max(1),
            inflight: 0,
        });
        if gate.inflight < gate.cap {
            gate.inflight += 1;
            true
        } else {
            false
        }
    }
    /// Release a previously-claimed inbound-RPC slot. Idempotent-safe.
    pub fn _release_rpc_slot(&self) {
        let mut g = self.0.rpc_gate.lock();
        if let Some(gate) = g.as_mut() {
            if gate.inflight > 0 {
                gate.inflight -= 1;
            }
        }
    }
    /// The number of admitted inbound RPCs right now.
    pub fn _inflight_rpcs(&self) -> i64 {
        self.0.rpc_gate.lock().as_ref().map_or(0, |g| g.inflight)
    }

    // ------------------------------------------------------------------
    // RPC dispatch (inbound)
    // ------------------------------------------------------------------

    /// Execute a dotted-path method call on the local policy: walk *path*
    /// from the policy object (`AttributeError` for a missing segment),
    /// rebuild entry envelopes shipped by a morphed caller, and invoke.
    pub fn _execute_rpc(
        &self,
        path: impl IntoArg,
        args: impl IntoArg,
        kwargs: impl IntoArg,
    ) -> Result<Arg> {
        let path = &Self::path_of(path.into_arg())?;
        let (args, kwargs) = (&args.into_arg(), &kwargs.into_arg());
        let target = match self.local_policy() {
            Some(p) => p.to_arg(),
            None => match self.0.local_stub.read().clone() {
                Some(stub) => stub,
                None => {
                    return Err(LailaError::runtime_error(
                        "Communication has no reference to the local policy.",
                    ))
                }
            },
        };
        let (args, kwargs) = super::super::protocol::rehydrate_entry_args(args, kwargs)?;
        let mut call = Args::new();
        for a in args.__iter__()? {
            call.push_positional(a);
        }
        for kv in kwargs.items()?.__iter__()? {
            let pair = kv.__iter__()?;
            if let [k, v] = pair.as_slice() {
                call.push_keyword(&k.__str__(), v.clone());
            }
        }
        let mut obj = target;
        let Some((last, parents)) = path.split_last() else {
            return obj.__call__(call);
        };
        for segment in parents {
            obj = obj.__getattr__(segment)?;
        }
        // ``getattr(obj, last)(*args, **kwargs)``: a value attribute
        // (a lambda stuck on the policy) is called; otherwise the method.
        match obj.__getattr__(last) {
            Ok(v) if crate::python::builtins::is_callable(&v) => v.__call__(call),
            Ok(v) if !matches!(v, Arg::Object(_)) => Err(LailaError::type_error(alloc::format!(
                "'{}' object is not callable",
                v.type_name()
            ))),
            Ok(v) => match obj.__call_method__(last, call.clone()) {
                Ok(r) => Ok(r),
                Err(e) if e.kind == ExcKind::AttributeError => v.__call__(call),
                Err(e) => Err(e),
            },
            Err(e) if e.kind == ExcKind::AttributeError => obj.__call_method__(last, call),
            Err(e) => Err(e),
        }
    }

    // ------------------------------------------------------------------
    // RPC dispatch (outbound)
    // ------------------------------------------------------------------

    /// Pick the transport that carries the call to *peer_id*. *comm* is a
    /// *communication id* -- a registered connection's `global_id` or a
    /// protocol token; when given that channel must already hold the peer,
    /// when `None` the first registered protocol holding *peer_id* is used.
    pub fn _select_protocol_for_peer(
        &self,
        peer_id: impl IntoArg,
        comm: impl IntoArg,
    ) -> Result<Protocol> {
        let (peer_id, comm) = (&peer_id.into_arg().__str__(), &comm.into_arg());
        if !comm.is_none() {
            let key = comm.__str__();
            let proto = match self.connection(&key) {
                Some(p) => Some(p),
                None => self._resolve_protocol_for_token(Some(&key)).ok(),
            };
            let Some(proto) = proto else {
                return Err(LailaError::connection_error(alloc::format!(
                    "Unknown communication channel {}: not a connection id or a registered protocol token.",
                    comm.__repr__()
                )));
            };
            if !proto.has_peer(peer_id) {
                return Err(LailaError::connection_error(alloc::format!(
                    "Communication channel {} has no connection to peer {peer_id}.",
                    comm.__repr__()
                )));
            }
            return Ok(proto);
        }
        for p in self.connections_vec() {
            if p.has_peer(peer_id) {
                return Ok(p);
            }
        }
        Err(LailaError::connection_error(alloc::format!(
            "No connection to peer {peer_id}"
        )))
    }
    /// Send an RPC call to a peer over the selected transport. A
    /// `__laila_future__` envelope in the response is wrapped in a
    /// `RemoteFuture` bound to this hub (and *comm*).
    pub fn _send_rpc(
        &self,
        peer_id: impl IntoArg,
        path: impl IntoArg,
        args: impl IntoArg,
        kwargs: impl IntoArg,
    ) -> Result<Arg> {
        self._send_rpc_via(peer_id, path, args, kwargs, Arg::None)
    }
    /// `_send_rpc(peer_id, path, args, kwargs, comm=..)`: *comm* selects
    /// the channel (a connection id / protocol token).
    pub fn _send_rpc_via(
        &self,
        peer_id: impl IntoArg,
        path: impl IntoArg,
        args: impl IntoArg,
        kwargs: impl IntoArg,
        comm: impl IntoArg,
    ) -> Result<Arg> {
        let (peer_id, comm) = (peer_id.into_arg().__str__(), comm.into_arg());
        let proto = self._select_protocol_for_peer(&peer_id, &comm)?;
        let result = proto.send_rpc(&peer_id, path, args, kwargs)?;
        self._maybe_wrap_remote_future(result, &peer_id, comm)
    }
    /// Promote a future-shaped envelope into a real `RemoteFuture`.
    pub fn _maybe_wrap_remote_future(&self, result: Arg, peer_id: &str, comm: Arg) -> Result<Arg> {
        if !matches!(result, Arg::Dict(_)) {
            return Ok(result);
        }
        if !result.get("__laila_future__")?.__bool__() {
            return Ok(result);
        }
        RemoteFuture::from_envelope(&result, peer_id, self, comm).map(IntoArg::into_arg)
    }

    // ---------- dynamic layer ----------

    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "global_id" => Arg::Str(self.global_id().into()),
            "uuid" | "_uuid" => Arg::Str(self.uuid().into()),
            "scopes" | "_scopes" => self.0.identity.lock().scopes_arg(),
            "evolution" | "_evolution" => self.0.identity.lock().evolution_arg(),
            "policy_id" => self.policy_id_arg(),
            "peers" => self.peers().into_arg(),
            "connections" => self.connections().into_arg(),
            "liveness_interval" => Arg::Float(self.liveness_interval()),
            "liveness_enabled" => Arg::Bool(self.liveness_enabled()),
            "max_inflight_rpcs" => Arg::Int(self.max_inflight_rpcs()),
            "_local_policy" => match self.local_policy() {
                Some(p) => p.into_arg(),
                None => self.0.local_stub.read().clone().unwrap_or(Arg::None),
            },
            "_liveness_thread" => match self
                .0
                .liveness
                .lock()
                .as_ref()
                .and_then(|l| l.thread.clone())
            {
                Some(t) => t.into_arg(),
                None => Arg::None,
            },
            "_liveness_stop" => match self.0.liveness.lock().as_ref() {
                Some(l) => l.stop.clone().into_arg(),
                None => Arg::None,
            },
            "__class__" => Self::class_object().into_arg(),
            other => {
                if let Some(v) = self.0.attrs.get(other) {
                    return Ok(v);
                }
                return Err(LailaError::attribute_error(alloc::format!(
                    "'{}' object has no attribute '{other}'",
                    Self::CLASS_NAME
                )));
            }
        })
    }
    pub fn __setattr__(&self, name: &str, value: impl IntoArg) -> Result<()> {
        let value = value.into_arg();
        match name {
            "policy_id" => self.set_policy_id(value),
            "liveness_interval" => self.set_liveness_interval(
                value
                    .as_f64()
                    .ok_or_else(|| LailaError::type_error("liveness_interval must be a number"))?,
            ),
            "liveness_enabled" => self.set_liveness_enabled(value.__bool__()),
            "max_inflight_rpcs" => self.set_max_inflight_rpcs(
                value
                    .as_i64()
                    .ok_or_else(|| LailaError::type_error("max_inflight_rpcs must be an int"))?,
            ),
            "_local_policy" => match Policy::of(&value) {
                Some(p) => self.set_local_policy(Some(&p)),
                None if value.is_none() => self.set_local_policy(None),
                None => {
                    *self.0.local_policy.write() = None;
                    *self.0.local_stub.write() = Some(value);
                }
            },
            "peers" => {
                let reg = match PeerRegistry::of(&value) {
                    Some(r) => r,
                    None => PeerRegistry::from_args(Args::from(value))?,
                };
                self.0.peers.clear();
                for (g, p) in reg.0.lock().iter() {
                    self.0.peers.insert(g, p.clone());
                }
            }
            other => self.0.attrs.__setattr__(other, value)?,
        }
        Ok(())
    }
    pub fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let pos = |i: usize, key: &str| {
            args.positional
                .get(i)
                .cloned()
                .unwrap_or_else(|| args.get(key))
        };
        Ok(match name {
            "add_connection" => {
                self.add_connection(pos(0, "protocol"))?;
                Arg::None
            }
            "remove_connection" => {
                self.remove_connection(pos(0, "protocol"))?;
                Arg::None
            }
            "_resolve_protocol_for_uri" => self
                ._resolve_protocol_for_uri(&pos(0, "uri").__str__())?
                .into_arg(),
            "_resolve_protocol_for_token" => {
                let t = pos(0, "token");
                let t = if t.is_none() { None } else { Some(t.__str__()) };
                self._resolve_protocol_for_token(t.as_deref())?.into_arg()
            }
            "start" => {
                self.start(Args::new())?;
                Arg::None
            }
            "stop" => {
                self.stop(Args::new())?;
                Arg::None
            }
            "add_peer" => Arg::Str(
                self.add_peer(pos(0, "uri").__str__(), pos(1, "secret").__str__())?
                    .into(),
            ),
            "add_tcpip_peer" => {
                let port = pos(1, "port");
                let port = port.as_i64().ok_or_else(|| {
                    LailaError::type_error(alloc::format!(
                        "port must be an int, got {}",
                        port.type_name()
                    ))
                })?;
                Arg::Str(
                    self.add_tcpip_peer(
                        pos(0, "host").__str__(),
                        port,
                        pos(2, "secret").__str__(),
                    )?
                    .into(),
                )
            }
            "remove_peer" => {
                self.remove_peer(pos(0, "peer_id").__str__());
                Arg::None
            }
            "_holder_protocol" => match self._holder_protocol(&pos(0, "peer_id").__str__()) {
                Some(p) => p.into_arg(),
                None => Arg::None,
            },
            "_start_liveness" => {
                self._start_liveness();
                Arg::None
            }
            "_stop_liveness" => {
                self._stop_liveness();
                Arg::None
            }
            "_register_peer" => {
                self._register_peer(&pos(0, "peer_id").__str__());
                Arg::None
            }
            "_unregister_peer" => {
                self._unregister_peer(&pos(0, "peer_id").__str__());
                Arg::None
            }
            "_acquire_rpc_slot" => Arg::Bool(self._acquire_rpc_slot()),
            "_release_rpc_slot" => {
                self._release_rpc_slot();
                Arg::None
            }
            "_execute_rpc" => {
                self._execute_rpc(pos(0, "path"), pos(1, "args"), pos(2, "kwargs"))?
            }
            "_select_protocol_for_peer" => self
                ._select_protocol_for_peer(pos(0, "peer_id").__str__(), pos(1, "comm"))?
                .into_arg(),
            "_send_rpc" => self._send_rpc_via(
                pos(0, "peer_id"),
                pos(1, "path"),
                pos(2, "args"),
                pos(3, "kwargs"),
                pos(4, "comm"),
            )?,
            "_maybe_wrap_remote_future" => self._maybe_wrap_remote_future(
                pos(0, "result"),
                &pos(1, "peer_id").__str__(),
                pos(2, "comm"),
            )?,
            "model_dump" => self.model_dump(),
            other => {
                if let Some(v) = self.0.attrs.get(other) {
                    return v.__call__(args);
                }
                return Err(LailaError::attribute_error(alloc::format!(
                    "'{}' object has no attribute '{other}'",
                    Self::CLASS_NAME
                )));
            }
        })
    }
    /// `comm.model_dump()` -- the public fields.
    pub fn model_dump(&self) -> Arg {
        let mut d = crate::python::PyDictBuilder::new();
        let identity = self.0.identity.lock();
        d.insert("uuid", identity.uuid.as_str());
        d.insert("scopes", identity.scopes_arg());
        d.insert("evolution", identity.evolution_arg());
        d.insert("policy_id", self.policy_id_arg());
        d.insert("peers", self.0.peers.snapshot());
        d.insert("connections", self.connections().snapshot());
        d.insert("liveness_interval", self.liveness_interval());
        d.insert("liveness_enabled", self.liveness_enabled());
        d.insert("max_inflight_rpcs", self.max_inflight_rpcs());
        d.finish()
    }
    pub fn class_object() -> crate::python::PyClass {
        crate::python::PyClass::with_ctor(
            Self::CLASS_NAME,
            "laila.policy.central.communication.schema.base",
            Arc::new(|a: Args| Communication::new(a).map(IntoArg::into_arg)),
        )
        .with_hooks(Arc::new(CommClassHooks))
    }
    /// The hub inside an `Arg`, if it is one.
    pub fn of(x: &Arg) -> Option<Communication> {
        match x {
            Arg::Object(o) => o.as_any().downcast_ref::<Communication>().cloned(),
            _ => None,
        }
    }
}

/// The hub's class-level attributes: its methods as *unbound* functions
/// (`orig = type(comm)._execute_rpc; orig(comm, path, args, kwargs)`).
struct CommClassHooks;

impl crate::python::pyclass::ClassHooks for CommClassHooks {
    fn getattr(&self, name: &str) -> Option<Arg> {
        match name {
            "_execute_rpc" | "_acquire_rpc_slot" | "_release_rpc_slot" | "add_peer"
            | "add_connection" | "remove_connection" | "stop" | "ping" => {
                let method = String::from(name);
                Some(crate::python::callable(move |a: Args| -> Result<Arg> {
                    let mut it = a.positional.into_iter();
                    let me = it.next().ok_or_else(|| {
                        LailaError::type_error(alloc::format!(
                            "{method}() missing 1 required positional argument: 'self'"
                        ))
                    })?;
                    let rest = Args {
                        positional: it.collect(),
                        keywords: a.keywords,
                    };
                    me.__call_method__(&method, rest)
                }))
            }
            _ => None,
        }
    }
}

impl core::fmt::Debug for Communication {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}({})", Self::CLASS_NAME, self.global_id())
    }
}
impl PyObject for Communication {
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
            .downcast_ref::<Communication>()
            .is_some_and(|o| o.is(self))
    }
    fn __is__(&self, other: &dyn PyObject) -> bool {
        self.__eq__(other)
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        Communication::__getattr__(self, name)
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        Communication::__setattr__(self, name, value)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        Communication::__call_method__(self, name, args)
    }
    fn __class__(&self) -> Option<crate::python::PyClass> {
        Some(Self::class_object())
    }
    /// The `laila.args.environment` mirror fields (`connections` is walked
    /// by `build_environment` itself; `policy_id` / `peers` are CLI-exempt).
    fn __cli_fields__(&self) -> Option<Vec<(String, Arg)>> {
        let identity = self.0.identity.lock();
        Some(alloc::vec![
            (String::from("evolution"), identity.evolution_arg()),
            (
                String::from("global_id"),
                Arg::Str(identity.global_id().into())
            ),
            (
                String::from("liveness_enabled"),
                Arg::Bool(self.liveness_enabled())
            ),
            (
                String::from("liveness_interval"),
                Arg::Float(self.liveness_interval())
            ),
            (
                String::from("max_inflight_rpcs"),
                Arg::Int(self.max_inflight_rpcs())
            ),
            (String::from("scopes"), identity.scopes_arg()),
            (
                String::from("uuid"),
                Arg::Str(identity.uuid.as_str().into())
            ),
        ])
    }
}
impl IntoArg for Communication {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for Communication {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl HasIdentity for Communication {
    fn global_id(&self) -> String {
        Communication::global_id(self)
    }
    fn uuid(&self) -> String {
        Communication::uuid(self)
    }
}
impl crate::python::builtins::IsInstance for Communication {
    fn isinstance(x: &Arg) -> bool {
        Communication::of(x).is_some()
    }
}

/// `_LAILA_IDENTIFIABLE_COMMUNICATION` as a class marker.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct _LAILA_IDENTIFIABLE_COMMUNICATION;
impl _LAILA_IDENTIFIABLE_COMMUNICATION {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<Communication> {
        Communication::new(args)
    }
    pub fn class_object() -> crate::python::PyClass {
        Communication::class_object()
    }
}
impl crate::python::builtins::IsInstance for _LAILA_IDENTIFIABLE_COMMUNICATION {
    fn isinstance(x: &Arg) -> bool {
        Communication::of(x).is_some()
    }
}
impl IntoArg for _LAILA_IDENTIFIABLE_COMMUNICATION {
    fn into_arg(self) -> Arg {
        Communication::class_object().into_arg()
    }
}
impl PyArg for _LAILA_IDENTIFIABLE_COMMUNICATION {
    fn to_arg(&self) -> Arg {
        Communication::class_object().into_arg()
    }
}
impl crate::python::builtins::CallValue for _LAILA_IDENTIFIABLE_COMMUNICATION {
    fn call_value(&self, args: Args) -> Result<Arg> {
        Communication::new(args).map(IntoArg::into_arg)
    }
}

/// `comm.connections`: a live `{gid: protocol}` view of the registry with
/// the `dict` protocol.
#[derive(Clone)]
pub struct ConnectionRegistry(pub Communication);

impl ConnectionRegistry {
    fn key(k: &Arg) -> Result<String> {
        match k {
            Arg::Str(s) => Ok(s.to_string()),
            other => Err(LailaError::key_error(other.__repr__())),
        }
    }
    pub fn snapshot(&self) -> Arg {
        let mut d = crate::python::PyDictBuilder::new();
        for (g, p) in self.0 .0.connections.lock().iter() {
            d.insert(g.clone(), p.to_arg());
        }
        d.finish()
    }
    fn remove(&self, gid: &str) -> Option<Protocol> {
        let mut c = self.0 .0.connections.lock();
        let i = c.iter().position(|(g, _)| g == gid)?;
        Some(c.remove(i).1)
    }
    /// `connections[gid]` on the typed registry (any key spelling).
    pub fn __getitem__(&self, key: impl IntoArg) -> Result<Arg> {
        PyObject::__getitem__(self, &key.into_arg())
    }
    /// `len(connections)`
    pub fn __len__(&self) -> Result<usize> {
        Ok(self.0 .0.connections.lock().len())
    }
    /// `gid in connections`
    pub fn __contains__(&self, item: impl IntoArg) -> Result<bool> {
        PyObject::__contains__(self, &item.into_arg())
    }
}

impl crate::python::builtins::PyLen for ConnectionRegistry {
    fn __len__(&self) -> Result<usize> {
        ConnectionRegistry::__len__(self)
    }
}

impl core::fmt::Debug for ConnectionRegistry {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.snapshot().__repr__())
    }
}
impl PyObject for ConnectionRegistry {
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
            .downcast_ref::<ConnectionRegistry>()
            .is_some_and(|o| self.snapshot() == o.snapshot())
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
            .downcast_ref::<ConnectionRegistry>()
            .is_some_and(|o| o.0.is(&self.0))
    }
    fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error("unhashable type: 'dict'"))
    }
    fn __len__(&self) -> Result<usize> {
        ConnectionRegistry::__len__(self)
    }
    fn __contains__(&self, item: &Arg) -> Result<bool> {
        Ok(match item {
            Arg::Str(s) => self.0.connection(s).is_some(),
            _ => false,
        })
    }
    fn __iter__(&self) -> Result<Vec<Arg>> {
        Ok(self
            .0
             .0
            .connections
            .lock()
            .iter()
            .map(|(g, _)| Arg::Str(g.as_str().into()))
            .collect())
    }
    fn __getitem__(&self, key: &Arg) -> Result<Arg> {
        let k = Self::key(key)?;
        self.0
            .connection(&k)
            .map(IntoArg::into_arg)
            .ok_or_else(|| LailaError::key_error(key.__repr__()))
    }
    fn __setitem__(&self, key: &Arg, value: Arg) -> Result<()> {
        let k = Self::key(key)?;
        let p = Protocol::of(&value).ok_or_else(|| {
            LailaError::type_error(alloc::format!(
                "connections values must be communication protocols, got {}",
                value.type_name()
            ))
        })?;
        let mut c = self.0 .0.connections.lock();
        match c.iter_mut().find(|(g, _)| *g == k) {
            Some(slot) => slot.1 = p,
            None => c.push((k, p)),
        }
        Ok(())
    }
    fn __delitem__(&self, key: &Arg) -> Result<()> {
        let k = Self::key(key)?;
        self.remove(&k)
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
                match self.remove(&k) {
                    Some(p) => Ok(p.into_arg()),
                    None => match args.positional.get(1) {
                        Some(default) => Ok(default.clone()),
                        None => Err(LailaError::key_error(key.__repr__())),
                    },
                }
            }
            "clear" => {
                self.0 .0.connections.lock().clear();
                Ok(Arg::None)
            }
            other => Err(LailaError::attribute_error(alloc::format!(
                "'dict' object has no attribute '{other}'"
            ))),
        }
    }
}
impl IntoArg for ConnectionRegistry {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for ConnectionRegistry {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
