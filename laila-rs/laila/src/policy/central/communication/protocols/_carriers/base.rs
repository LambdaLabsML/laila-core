//! `_CarrierRPCProtocol` -- the wire-agnostic RPC base every real
//! transport shares.
//!
//! One place for: the codec (`json` / `msgpack`), the inbound dispatcher
//! (`_build_response` / `_handle_request_frame`) with its admission gate
//! and the ping fast path, the outbound correlation table
//! (`_register_pending` / `_complete_pending` / `_await_pending`) with
//! backpressure retry, peer bookkeeping (`_register_peer` /
//! `_unregister_peer` / goodbye), and the stream-lane multiplexer
//! (`open_channel`, lane allocation, the `__comm_channel_open__` /
//! `__comm_channel_close__` control RPCs, inbound frame routing).
//!
//! The concrete wire (stream / datagram / loopback / websocket) only
//! provides `_send_once`, `_send_oneway`, `_stream_enqueue`,
//! `_send_goodbye` and the lifecycle verbs; see the sibling modules.

use alloc::string::String;
use alloc::sync::{Arc, Weak};
use alloc::vec::Vec;
use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::python::object::PyObject;
use crate::python::sync::{wait_until, Condvar, Mutex};
use crate::python::{Arg, Args, IntoArg, LailaError, PyArg, PyDictBuilder, Result};

use super::super::super::channel::Channel;
use super::super::super::protocol as rpc_protocol;
use super::super::base::{CarrierKind, Protocol, ProtocolInner};
use super::codec as _codec;

/// `_COMM_PING_PATH`
pub const _COMM_PING_PATH: &[&str] = &["__comm_ping__"];
/// `_COMM_CHANNEL_OPEN_PATH`
pub const _COMM_CHANNEL_OPEN_PATH: &[&str] = &["__comm_channel_open__"];
/// `_COMM_CHANNEL_CLOSE_PATH`
pub const _COMM_CHANNEL_CLOSE_PATH: &[&str] = &["__comm_channel_close__"];
/// `_MAX_LANE = 255`
pub const _MAX_LANE: u8 = 255;
/// `_PEER_CONNECT = "peer.connect"`
pub const _PEER_CONNECT: &str = "peer.connect";
/// `_PEER_DISCONNECT = "peer.disconnect"`
pub const _PEER_DISCONNECT: &str = "peer.disconnect";
/// `_GOODBYE_TIMEOUT = 1.0`
pub const _GOODBYE_TIMEOUT: f64 = 1.0;

/// `BackpressureError` -- the peer answered `ERR_BUSY` (the exception
/// class of `laila.policy.central.communication.wire`).
pub use crate::python::errors::BackpressureError;

/// `log = logging.getLogger(__name__)` -- the module logger.
pub fn log() -> crate::python::stdlib::logging::Logger {
    crate::python::stdlib::logging::Logger::get(
        "laila.policy.central.communication.protocols._carriers.base",
    )
}

fn is_backpressure(e: &LailaError) -> bool {
    e.kind == crate::python::ExcKind::BackpressureError
}

/// `_CarrierRPCProtocol` as a class marker (for `isinstance` / subclass
/// checks in tests and the parity table).
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct _CarrierRPCProtocol;
impl crate::python::builtins::IsInstance for _CarrierRPCProtocol {
    fn isinstance(x: &Arg) -> bool {
        Protocol::of(x).is_some_and(|p| p.spec().is_carrier())
    }
}
impl _CarrierRPCProtocol {
    pub fn class_object() -> crate::python::PyClass {
        crate::python::PyClass::named(
            "_CarrierRPCProtocol",
            "laila.policy.central.communication.protocols._carriers.base",
        )
    }
}
impl IntoArg for _CarrierRPCProtocol {
    fn into_arg(self) -> Arg {
        Self::class_object().into_arg()
    }
}

// ---------------------------------------------------------------------------
// state

/// What `_connections[peer_id]` holds for a peer.
#[derive(Clone)]
pub enum PeerHandle {
    /// Loopback: the peer carrier itself.
    Loopback(Weak<ProtocolInner>),
    /// Stream / WebSocket: the link (writer queue + socket).
    Link(Arc<super::stream::Link>),
    /// Datagram: the peer's address key (`"host:port"`).
    Addr(String),
    /// Anything a test stub registered.
    Virtual(Arg),
}

impl PeerHandle {
    pub fn as_arg(&self) -> Arg {
        match self {
            PeerHandle::Loopback(w) => match Protocol::from_inner(w) {
                Some(p) => p.into_arg(),
                None => Arg::None,
            },
            PeerHandle::Link(l) => Arg::Str(alloc::format!("<link {:p}>", Arc::as_ptr(l)).into()),
            PeerHandle::Addr(a) => Arg::Str(a.as_str().into()),
            PeerHandle::Virtual(v) => v.clone(),
        }
    }
}

/// One outbound RPC awaiting its response.
pub struct Pending {
    pub peer_id: Option<String>,
    pub state: Mutex<PendingState>,
    pub cv: Condvar,
}
#[derive(Default)]
pub struct PendingState {
    pub done: bool,
    pub result: Option<Arg>,
    pub error: Option<Arg>,
    pub exception: Option<LailaError>,
}
/// A pending slot as the Python test suite sees it: the dict
/// `{"event": Event, "exception": exc | None, "peer_id": ..}` of
/// `proto._pending_rpcs[request_id]`.
#[derive(Clone)]
pub struct PendingSlot(pub Arc<Pending>);

impl PyObject for PendingSlot {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "dict"
    }
    fn __repr__(&self) -> String {
        let s = self.0.state.lock();
        alloc::format!(
            "{{'event': <Event set={}>, 'exception': {}, 'peer_id': {}}}",
            s.done,
            s.exception
                .as_ref()
                .map(|e| alloc::format!("{e:?}"))
                .unwrap_or_else(|| String::from("None")),
            self.0
                .peer_id
                .as_deref()
                .map(|p| alloc::format!("{p:?}"))
                .unwrap_or_else(|| String::from("None"))
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<PendingSlot>()
            .is_some_and(|o| Arc::ptr_eq(&self.0, &o.0))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __getitem__(&self, key: &Arg) -> Result<Arg> {
        match key.__str__().as_str() {
            "event" => Ok(Arg::Object(Arc::new(PendingEvent(self.0.clone())))),
            "exception" => Ok(self
                .0
                .state
                .lock()
                .exception
                .clone()
                .map(IntoArg::into_arg)
                .unwrap_or(Arg::None)),
            "peer_id" => Ok(self.0.peer_id.clone().into_arg()),
            "result" => Ok(self.0.state.lock().result.clone().unwrap_or(Arg::None)),
            "error" => Ok(self.0.state.lock().error.clone().unwrap_or(Arg::None)),
            other => Err(LailaError::key_error(alloc::format!("{other:?}"))),
        }
    }
}

/// The `threading.Event` of a pending slot (`slot["event"].is_set()`).
#[derive(Clone)]
pub struct PendingEvent(pub Arc<Pending>);

impl PyObject for PendingEvent {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "Event"
    }
    fn __repr__(&self) -> String {
        alloc::format!("<Event set={}>", self.0.state.lock().done)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<PendingEvent>()
            .is_some_and(|o| Arc::ptr_eq(&self.0, &o.0))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __call_method__(&self, name: &str, _args: Args) -> Result<Arg> {
        match name {
            "is_set" => Ok(Arg::Bool(self.0.state.lock().done)),
            "wait" => {
                let mut s = self.0.state.lock();
                while !s.done {
                    self.0.cv.wait(&mut s);
                }
                Ok(Arg::Bool(true))
            }
            "set" => {
                self.0.settle(|_| {});
                Ok(Arg::None)
            }
            other => Err(LailaError::attribute_error(alloc::format!(
                "'Event' object has no attribute '{other}'"
            ))),
        }
    }
}

impl Pending {
    fn settle(&self, f: impl FnOnce(&mut PendingState)) {
        let mut s = self.state.lock();
        f(&mut s);
        s.done = true;
        self.cv.notify_all();
    }
    /// `slot.setdefault("exception", exc); slot["event"].set()`
    pub fn fail(&self, exc: LailaError) {
        self.settle(|s| {
            if s.exception.is_none() {
                s.exception = Some(exc);
            }
        });
    }
}

#[derive(Default)]
struct LaneTable {
    /// `_lanes: {peer: {lane: Channel}}`
    by_lane: Vec<(String, Vec<(u8, Channel)>)>,
    /// `_lane_names: {peer: {name: Channel}}`
    by_name: Vec<(String, Vec<(String, Channel)>)>,
    /// `_lane_cursor: {peer: lane}`
    cursor: Vec<(String, u8)>,
}

impl LaneTable {
    fn lanes_of(&self, peer: &str) -> Option<&Vec<(u8, Channel)>> {
        self.by_lane.iter().find(|(p, _)| p == peer).map(|(_, v)| v)
    }
    fn lanes_mut(&mut self, peer: &str) -> &mut Vec<(u8, Channel)> {
        if let Some(i) = self.by_lane.iter().position(|(p, _)| p == peer) {
            return &mut self.by_lane[i].1;
        }
        self.by_lane.push((String::from(peer), Vec::new()));
        &mut self.by_lane.last_mut().unwrap().1
    }
    fn names_of(&self, peer: &str) -> Option<&Vec<(String, Channel)>> {
        self.by_name.iter().find(|(p, _)| p == peer).map(|(_, v)| v)
    }
    fn names_mut(&mut self, peer: &str) -> &mut Vec<(String, Channel)> {
        if let Some(i) = self.by_name.iter().position(|(p, _)| p == peer) {
            return &mut self.by_name[i].1;
        }
        self.by_name.push((String::from(peer), Vec::new()));
        &mut self.by_name.last_mut().unwrap().1
    }
    fn lane(&self, peer: &str, lane: u8) -> Option<Channel> {
        self.lanes_of(peer)?
            .iter()
            .find(|(l, _)| *l == lane)
            .map(|(_, c)| c.clone())
    }
    fn named(&self, peer: &str, name: &str) -> Option<Channel> {
        self.names_of(peer)?
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, c)| c.clone())
    }
    /// `_drop_channel(ch)`
    fn drop_channel(&mut self, ch: &Channel) {
        if let Some(i) = self.by_lane.iter().position(|(p, _)| p == ch.peer_id()) {
            self.by_lane[i]
                .1
                .retain(|(l, c)| !(*l == ch.lane_id() && c.is(ch)));
        }
        if let Some(i) = self.by_name.iter().position(|(p, _)| p == ch.peer_id()) {
            self.by_name[i]
                .1
                .retain(|(n, c)| !(n == ch.name() && c.is(ch)));
        }
    }
    /// `_alloc_lane(peer_id, preferred=None)`
    fn alloc_lane(&mut self, peer: &str, preferred: Option<u8>) -> Result<u8> {
        let used: Vec<u8> = self
            .lanes_of(peer)
            .map(|v| v.iter().map(|(l, _)| *l).collect())
            .unwrap_or_default();
        if let Some(p) = preferred {
            if p >= 1 && !used.contains(&p) {
                return Ok(p);
            }
        }
        let mut cursor = self
            .cursor
            .iter()
            .find(|(p, _)| p == peer)
            .map(|(_, c)| *c)
            .unwrap_or(0);
        for _ in 0.._MAX_LANE {
            cursor = (cursor % _MAX_LANE) + 1;
            if !used.contains(&cursor) {
                match self.cursor.iter_mut().find(|(p, _)| p == peer) {
                    Some(slot) => slot.1 = cursor,
                    None => self.cursor.push((String::from(peer), cursor)),
                }
                return Ok(cursor);
            }
        }
        Err(LailaError::connection_error(alloc::format!(
            "All {_MAX_LANE} stream lanes to peer {peer} are in use."
        )))
    }
}

/// `_CarrierRPCProtocol`'s private attributes.
pub struct CarrierState {
    started: AtomicBool,
    connections: Mutex<Vec<(String, PeerHandle)>>,
    pending: Mutex<Vec<(String, Arc<Pending>)>>,
    lanes: Mutex<LaneTable>,
    peer_caps: Mutex<Vec<(String, Arg)>>,
    unknown_lane_drops: AtomicU64,
    reserved_marker_drops: AtomicU64,
    malformed_stream_drops: AtomicU64,
    /// `_event_loop` / `_loop_thread` (loopthread parity; the Rust wires
    /// do not need them).
    pub event_loop: Mutex<Arg>,
    pub loop_thread: Mutex<Arg>,
}

impl Default for CarrierState {
    fn default() -> Self {
        CarrierState {
            started: AtomicBool::new(false),
            connections: Mutex::new(Vec::new()),
            pending: Mutex::new(Vec::new()),
            lanes: Mutex::new(LaneTable::default()),
            peer_caps: Mutex::new(Vec::new()),
            unknown_lane_drops: AtomicU64::new(0),
            reserved_marker_drops: AtomicU64::new(0),
            malformed_stream_drops: AtomicU64::new(0),
            event_loop: Mutex::new(Arg::None),
            loop_thread: Mutex::new(Arg::None),
        }
    }
}

impl CarrierState {
    pub fn started(&self) -> bool {
        self.started.load(Ordering::Acquire)
    }
    pub fn set_started(&self, v: bool) {
        self.started.store(v, Ordering::Release)
    }
    /// `peer_id in self._connections`
    pub fn has_peer(&self, peer_id: &str) -> bool {
        self.connections.lock().iter().any(|(p, _)| p == peer_id)
    }
    /// `self._connections.get(peer_id)`
    pub fn connection(&self, peer_id: &str) -> Option<PeerHandle> {
        self.connections
            .lock()
            .iter()
            .find(|(p, _)| p == peer_id)
            .map(|(_, h)| h.clone())
    }
    /// `list(self._connections)`
    pub fn peer_ids(&self) -> Vec<String> {
        self.connections
            .lock()
            .iter()
            .map(|(p, _)| p.clone())
            .collect()
    }
    /// `list(self._connections.items())`
    pub fn connections(&self) -> Vec<(String, PeerHandle)> {
        self.connections.lock().clone()
    }
    pub fn insert_connection(&self, peer_id: &str, handle: PeerHandle) {
        let mut g = self.connections.lock();
        match g.iter_mut().find(|(p, _)| p == peer_id) {
            Some(slot) => slot.1 = handle,
            None => g.push((String::from(peer_id), handle)),
        }
    }
    pub fn remove_connection(&self, peer_id: &str) -> Option<PeerHandle> {
        let mut g = self.connections.lock();
        let i = g.iter().position(|(p, _)| p == peer_id)?;
        Some(g.remove(i).1)
    }
    pub fn clear_connections(&self) {
        self.connections.lock().clear();
    }
    pub fn connections_dict(&self) -> Arg {
        let mut d = PyDictBuilder::new();
        for (p, h) in self.connections.lock().iter() {
            d.insert(p.as_str(), h.as_arg());
        }
        d.finish()
    }
    pub fn pending_dict(&self) -> Arg {
        let mut d = PyDictBuilder::new();
        for (rid, slot) in self.pending.lock().iter() {
            d.insert(
                rid.as_str(),
                Arg::Object(Arc::new(PendingSlot(slot.clone()))),
            );
        }
        d.finish()
    }
    pub fn pending_count(&self) -> usize {
        self.pending.lock().len()
    }
    pub fn peer_caps_dict(&self) -> Arg {
        let mut d = PyDictBuilder::new();
        for (p, c) in self.peer_caps.lock().iter() {
            d.insert(p.as_str(), c.clone());
        }
        d.finish()
    }
    pub fn lanes_dict(&self) -> Arg {
        let mut d = PyDictBuilder::new();
        for (p, lanes) in self.lanes.lock().by_lane.iter() {
            let mut inner = PyDictBuilder::new();
            for (l, c) in lanes {
                inner.insert(*l as i64, c.clone());
            }
            d.insert(p.as_str(), inner.finish());
        }
        d.finish()
    }
    pub fn lane_names_dict(&self) -> Arg {
        let mut d = PyDictBuilder::new();
        for (p, names) in self.lanes.lock().by_name.iter() {
            let mut inner = PyDictBuilder::new();
            for (n, c) in names {
                inner.insert(n.as_str(), c.clone());
            }
            d.insert(p.as_str(), inner.finish());
        }
        d.finish()
    }
    /// `self._unknown_lane_drops += 1`
    pub fn note_unknown_lane_drop(&self) {
        self.unknown_lane_drops.fetch_add(1, Ordering::Relaxed);
    }
    pub fn unknown_lane_drops(&self) -> i64 {
        self.unknown_lane_drops.load(Ordering::Relaxed) as i64
    }
    pub fn reserved_marker_drops(&self) -> i64 {
        self.reserved_marker_drops.load(Ordering::Relaxed) as i64
    }
    pub fn malformed_stream_drops(&self) -> i64 {
        self.malformed_stream_drops.load(Ordering::Relaxed) as i64
    }
    /// `_set_peer_caps(peer_id, caps)`
    pub fn set_peer_caps(&self, peer_id: &str, caps: Arg) {
        let caps = match caps {
            Arg::Dict(_) => caps,
            _ => PyDictBuilder::new().finish(),
        };
        let mut g = self.peer_caps.lock();
        match g.iter_mut().find(|(p, _)| p == peer_id) {
            Some(slot) => slot.1 = caps,
            None => g.push((String::from(peer_id), caps)),
        }
    }
    /// `_peer_has_lanes(peer_id)`
    pub fn peer_has_lanes(&self, peer_id: &str) -> bool {
        self.peer_caps
            .lock()
            .iter()
            .find(|(p, _)| p == peer_id)
            .and_then(|(_, c)| c.get("lanes").ok())
            .is_some_and(|v| v.__bool__())
    }
    /// `channel_names(peer_id)`
    pub fn channel_names(&self, peer_id: &str) -> Vec<String> {
        self.lanes
            .lock()
            .names_of(peer_id)
            .map(|v| {
                v.iter()
                    .filter(|(_, c)| !c.closed())
                    .map(|(n, _)| n.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    // ---- pending table ----

    /// `_register_pending(peer_id)`
    pub fn register_pending(&self, peer_id: Option<&str>) -> (String, Arc<Pending>) {
        let request_id = crate::python::stdlib::uuid.uuid4().__str__();
        let slot = Arc::new(Pending {
            peer_id: peer_id.map(String::from),
            state: Mutex::new(PendingState::default()),
            cv: Condvar::new(),
        });
        self.pending.lock().push((request_id.clone(), slot.clone()));
        (request_id, slot)
    }
    pub fn pop_pending(&self, request_id: &str) -> Option<Arc<Pending>> {
        let mut g = self.pending.lock();
        let i = g.iter().position(|(r, _)| r == request_id)?;
        Some(g.remove(i).1)
    }
    /// `_fail_pending(reason, peer_id=None)`
    pub fn fail_pending(&self, reason: &str, peer_id: Option<&str>) {
        let victims: Vec<(String, Arc<Pending>)> = {
            let mut g = self.pending.lock();
            let (victims, keep): (Vec<_>, Vec<_>) = g
                .drain(..)
                .partition(|(_, s)| peer_id.is_none() || s.peer_id.as_deref() == peer_id);
            *g = keep;
            victims
        };
        for (_, slot) in victims {
            slot.fail(LailaError::connection_error(reason));
        }
    }
    /// `_fail_request(request_id, exc)`
    pub fn fail_request(&self, request_id: &str, exc: LailaError) {
        if let Some(slot) = self.pop_pending(request_id) {
            slot.fail(exc);
        }
    }
    /// `_complete_pending(msg)`
    pub fn complete_pending(&self, msg: &Arg) {
        let rid = rpc_protocol::msg_get(msg, "id");
        if rid.is_none() {
            return;
        }
        let rid = rid.__str__();
        let slot = self
            .pending
            .lock()
            .iter()
            .find(|(r, _)| *r == rid)
            .map(|(_, s)| s.clone());
        let Some(slot) = slot else {
            return;
        };
        let error = rpc_protocol::msg_get(msg, "error");
        let result = rpc_protocol::msg_get(msg, "result");
        let has_error = msg.__contains__("error").unwrap_or(false);
        slot.settle(|s| {
            if has_error {
                s.error = Some(error);
            } else {
                s.result = Some(result);
            }
        });
    }
    pub fn clear_pending(&self) {
        self.pending.lock().clear();
    }
}

// ---------------------------------------------------------------------------
// per-thread RPC wait override (``ping`` shortens the wait)

#[cfg(feature = "std")]
std::thread_local! {
    static RPC_WAIT_OVERRIDE: core::cell::Cell<Option<f64>> = const { core::cell::Cell::new(None) };
}
fn rpc_wait_override() -> Option<f64> {
    #[cfg(feature = "std")]
    {
        RPC_WAIT_OVERRIDE.with(|c| c.get())
    }
    #[cfg(not(feature = "std"))]
    {
        None
    }
}
fn set_rpc_wait_override(v: Option<f64>) {
    #[cfg(feature = "std")]
    RPC_WAIT_OVERRIDE.with(|c| c.set(v));
    #[cfg(not(feature = "std"))]
    let _ = v;
}

// ---------------------------------------------------------------------------
// helpers

fn path_of(msg: &Arg) -> Vec<String> {
    let params = rpc_protocol::msg_get(msg, "params");
    let path = if params.is_none() {
        Arg::None
    } else {
        params.get("path").unwrap_or(Arg::None)
    };
    match path.__iter__() {
        Ok(items) => items.iter().map(|s| s.__str__()).collect(),
        Err(_) => Vec::new(),
    }
}
fn path_eq(path: &[String], target: &[&str]) -> bool {
    path.len() == target.len() && path.iter().zip(target).all(|(a, b)| a == b)
}
fn strs(path: &[&str]) -> Vec<String> {
    path.iter().map(|s| String::from(*s)).collect()
}
pub(crate) fn list_arg(path: &[String]) -> Arg {
    path.iter()
        .map(|s| Arg::Str(s.as_str().into()))
        .collect::<Vec<_>>()
        .into_arg()
}
fn exc_text(e: &LailaError) -> String {
    alloc::format!("{}: {}", e.kind.name(), e.__str__())
}

/// `_make_rpc_request(path, args, kwargs, request_id)`
pub fn _make_rpc_request(path: &[String], args: &[Arg], kwargs: &Arg, request_id: &str) -> Arg {
    let mut params = PyDictBuilder::new();
    params.insert("path", list_arg(path));
    params.insert("args", args.to_vec().into_arg());
    params.insert(
        "kwargs",
        match kwargs {
            Arg::Dict(_) => kwargs.clone(),
            _ => PyDictBuilder::new().finish(),
        },
    );
    rpc_protocol::make_request("rpc.call", params.finish(), Some(request_id))
}

/// `_local_caps()`: `{"lanes": 1}` when the transport has stream lanes.
pub fn _local_caps(proto: &Protocol) -> Arg {
    let mut d = PyDictBuilder::new();
    if proto.supports_channels() {
        d.insert("lanes", 1);
    }
    d.finish()
}

/// `_goodbye_message()`
pub fn _goodbye_message(proto: &Protocol) -> Arg {
    let mut p = PyDictBuilder::new();
    p.insert("from_id", proto.policy_id());
    rpc_protocol::make_notification(_PEER_DISCONNECT, p.finish())
}

/// `_is_goodbye(msg)`
pub fn _is_goodbye(msg: &Arg) -> bool {
    rpc_protocol::msg_get(msg, "method").__str__() == _PEER_DISCONNECT
}

/// `_is_ping_frame(msg)`
pub fn _is_ping_frame(msg: &Arg) -> bool {
    rpc_protocol::msg_get(msg, "method").__str__() == "rpc.call"
        && path_eq(&path_of(msg), _COMM_PING_PATH)
}

/// `_is_control_path(path)`
pub fn _is_control_path(path: &[String]) -> bool {
    path_eq(path, _COMM_CHANNEL_OPEN_PATH) || path_eq(path, _COMM_CHANNEL_CLOSE_PATH)
}

/// `_max_message_bytes()`: the largest stream message a lane accepts.
pub fn _max_message_bytes(proto: &Protocol) -> i64 {
    let cap = proto.i64_field("max_stream_frame_bytes", 64 * 1024 * 1024);
    if matches!(
        proto.spec().carrier,
        CarrierKind::Datagram | CarrierKind::LazyDatagram
    ) {
        let mtu = proto.i64_field("mtu", 1200);
        return cap.min(mtu - _codec::STREAM_HEADER_LEN as i64).max(0);
    }
    cap
}

/// `_require_drivers(modules, extra)`: the Rust build has no host-side
/// Python drivers, so any declared module is "not installed".
pub fn _require_drivers(proto: &Protocol, modules: &[String], extra: &str) -> Result<Arg> {
    if let Some(name) = modules.first() {
        return Err(LailaError::runtime_error(alloc::format!(
            "The {} transport requires {}, which is not installed. Install it with `pip install laila-core[{extra}]`.",
            crate::python::pyrepr::str_repr(proto.protocol_name()),
            crate::python::pyrepr::str_repr(name)
        )));
    }
    Ok(PyDictBuilder::new().finish())
}

// ---------------------------------------------------------------------------
// inbound

/// `self._build_response(msg, peer_id)` as the carrier calls it: an
/// instance override (`proto._build_response = spy`) wins over the
/// built-in [`_build_response`].
pub fn build_response_dispatch(proto: &Protocol, msg: &Arg, peer_id: Option<&str>) -> Arg {
    if proto.has_field("_build_response") {
        let hook = proto.field("_build_response");
        let mut a = Args::from(msg.clone());
        a.push_keyword(
            "peer_id",
            peer_id.map(|p| Arg::Str(p.into())).unwrap_or(Arg::None),
        );
        return match hook.__call__(a) {
            Ok(r) => r,
            Err(e) => rpc_protocol::make_error(
                rpc_protocol::msg_get(msg, "id"),
                rpc_protocol::ERR_EXECUTION,
                &e.__str__(),
                Arg::None,
            ),
        };
    }
    _build_response(proto, msg, peer_id)
}

/// `_build_response(msg, peer_id=None)`: run one inbound frame to a
/// JSON-RPC response (ping fast path, lane control, or the policy call).
pub fn _build_response(proto: &Protocol, msg: &Arg, peer_id: Option<&str>) -> Arg {
    let request_id = rpc_protocol::msg_get(msg, "id");
    let method = rpc_protocol::msg_get(msg, "method");
    if method.__str__() != "rpc.call" || method.is_none() {
        return rpc_protocol::make_error(
            request_id,
            rpc_protocol::ERR_METHOD_NOT_FOUND,
            &alloc::format!("Unknown method: {}", method.__str__()),
            Arg::None,
        );
    }
    let params = rpc_protocol::msg_get(msg, "params");
    let path = path_of(msg);
    if path_eq(&path, _COMM_PING_PATH) {
        return rpc_protocol::make_result(request_id, Arg::Str("pong".into()));
    }
    if _is_control_path(&path) {
        return _control_response(proto, msg, peer_id);
    }
    let args = if params.is_none() {
        Arg::None
    } else {
        params.get("args").unwrap_or(Arg::None)
    };
    let kwargs = if params.is_none() {
        Arg::None
    } else {
        params.get("kwargs").unwrap_or(Arg::None)
    };
    let outcome = match proto.communication() {
        Some(comm) => {
            // `mock.patch.object(type(comm), "_execute_rpc", spy)`
            let mut a = Args::new();
            a.push_positional(path.clone());
            a.push_positional(args.clone());
            a.push_positional(kwargs.clone());
            match crate::python::mock::call_patched_method(
                super::super::super::schema::base::Communication::CLASS_NAME,
                "_execute_rpc",
                comm.to_arg(),
                a,
            ) {
                Some(r) => r,
                None => comm._execute_rpc(&path, &args, &kwargs),
            }
        }
        None => Err(LailaError::runtime_error(
            "Communication has no reference to the local policy.",
        )),
    };
    match outcome {
        Ok(result) => rpc_protocol::make_result(request_id, result),
        Err(e) => rpc_protocol::make_error(
            request_id,
            rpc_protocol::ERR_EXECUTION,
            &exc_text(&e),
            Arg::None,
        ),
    }
}

/// `_busy_response(msg)`
pub fn _busy_response(proto: &Protocol, msg: &Arg) -> Arg {
    rpc_protocol::make_error(
        rpc_protocol::msg_get(msg, "id"),
        rpc_protocol::ERR_BUSY,
        &alloc::format!(
            "{} peer is busy: inbound RPC queue at capacity.",
            crate::python::pyrepr::str_repr(proto.protocol_name())
        ),
        Arg::None,
    )
}

/// The reply callback a wire hands to the dispatcher.
pub type Reply = Arc<dyn Fn(Arg) + Send + Sync>;

/// `_handle_request_frame(msg, reply, peer_id=None)`: answer pings and
/// lane control inline, gate everything else through the hub's admission
/// counter and run it on a worker thread.
pub fn _handle_request_frame(proto: &Protocol, msg: Arg, reply: Reply, peer_id: Option<&str>) {
    // An instance override (`object.__setattr__(proto, "_handle_request_frame",
    // f)`): the instance `__dict__` wins over the class method.
    if proto.has_field("_handle_request_frame") {
        let hook = proto.field("_handle_request_frame");
        let reply_fn = crate::python::callable(move |a: Args| -> Result<Arg> {
            reply(a.positional.first().cloned().unwrap_or(Arg::None));
            Ok(Arg::None)
        });
        let mut a = Args::from(msg);
        a.push_positional(reply_fn);
        a.push_keyword(
            "peer_id",
            peer_id.map(|p| Arg::Str(p.into())).unwrap_or(Arg::None),
        );
        let _ = hook.__call__(a);
        return;
    }
    if _is_ping_frame(&msg) {
        reply(rpc_protocol::make_result(
            rpc_protocol::msg_get(&msg, "id"),
            Arg::Str("pong".into()),
        ));
        return;
    }
    if rpc_protocol::msg_get(&msg, "method").__str__() == "rpc.call"
        && _is_control_path(&path_of(&msg))
    {
        reply(_control_response(proto, &msg, peer_id));
        return;
    }
    let comm = proto.communication();
    let admitted = comm.as_ref().map(|c| c._acquire_rpc_slot()).unwrap_or(true);
    if !admitted {
        reply(_busy_response(proto, &msg));
        return;
    }
    let p = proto.clone();
    let peer = peer_id.map(String::from);
    let name = alloc::format!("{}-inbound", proto.class_name());
    let spawned = crate::python::sync::spawn_named(name, move || {
        let resp = build_response_dispatch(&p, &msg, peer.as_deref());
        if let Some(c) = p.communication() {
            c._release_rpc_slot();
        }
        reply(resp);
    });
    if spawned.is_err() {
        if let Some(c) = comm {
            c._release_rpc_slot();
        }
    }
}

// ---------------------------------------------------------------------------
// peers

/// `_register_peer(peer_id, handle)`
pub fn _register_peer(proto: &Protocol, peer_id: &str, handle: PeerHandle) {
    proto.0.carrier.insert_connection(peer_id, handle);
    if let Some(c) = proto.communication() {
        c._register_peer(peer_id);
    }
    _bind_static_lanes(proto, peer_id);
}

/// `_unregister_peer(peer_id)`
pub fn _unregister_peer(proto: &Protocol, peer_id: &str) {
    let handle = proto.0.carrier.connection(peer_id);
    _close_peer_lanes(proto, peer_id, "peer disconnected");
    proto.0.carrier.fail_pending(
        &alloc::format!("peer {peer_id} disconnected"),
        Some(peer_id),
    );
    proto
        .0
        .carrier
        .peer_caps
        .lock()
        .retain(|(p, _)| p != peer_id);
    proto
        .0
        .carrier
        .lanes
        .lock()
        .cursor
        .retain(|(p, _)| p != peer_id);
    proto.0.carrier.remove_connection(peer_id);
    if let Some(c) = proto.communication() {
        c._unregister_peer(peer_id);
    }
    // The stream carrier owns one link per peer and closes it here
    // (`_StreamRPCProtocol._unregister_peer`); a point-to-point link *is*
    // the wire and stays open so a new handshake can follow.
    if proto.spec().carrier != CarrierKind::P2P {
        if let Some(PeerHandle::Link(link)) = handle {
            link.close();
        }
    }
}

/// `_on_peer_goodbye(peer_id)`
pub fn _on_peer_goodbye(proto: &Protocol, peer_id: &str) {
    if proto.0.carrier.has_peer(peer_id) {
        _unregister_peer(proto, peer_id);
    }
}

/// `disconnect(peer_id)` for the wire carriers: goodbye, then unregister.
pub fn disconnect(proto: &Protocol, peer_id: &str) -> Result<()> {
    if !proto.0.carrier.has_peer(peer_id) {
        return Ok(());
    }
    let _ = _send_goodbye(proto, peer_id);
    _unregister_peer(proto, peer_id);
    Ok(())
}

/// `_send_goodbye(peer_id)` by wire.
pub fn _send_goodbye(proto: &Protocol, peer_id: &str) -> Result<()> {
    match proto.spec().carrier {
        CarrierKind::Stream => super::stream::_send_goodbye(proto, peer_id),
        CarrierKind::Datagram | CarrierKind::LazyDatagram => {
            super::datagram::_send_goodbye(proto, peer_id)
        }
        CarrierKind::WebSocket => super::super::tcpip::_send_goodbye(proto, peer_id),
        CarrierKind::P2P => super::p2p::_send_goodbye(proto, peer_id),
        CarrierKind::Broker => super::broker::_send_goodbye(proto, peer_id),
        CarrierKind::Register => super::register::_send_goodbye(proto, peer_id),
        _ => Ok(()),
    }
}

/// `_goodbye_all()`
pub fn _goodbye_all(proto: &Protocol) {
    for peer_id in proto.0.carrier.peer_ids() {
        let _ = _send_goodbye(proto, &peer_id);
    }
}

// ---------------------------------------------------------------------------
// outbound

/// `ping(peer_id, timeout=None)`
pub fn ping(proto: &Protocol, peer_id: &str, timeout: Option<f64>) -> bool {
    if !proto.has_peer(peer_id) {
        return false;
    }
    set_rpc_wait_override(Some(timeout.unwrap_or_else(|| proto.ping_timeout())));
    let out = proto.send_rpc(
        peer_id,
        strs(_COMM_PING_PATH),
        Vec::<Arg>::new(),
        PyDictBuilder::new().finish(),
    );
    set_rpc_wait_override(None);
    matches!(out, Ok(Arg::Str(s)) if &*s == "pong")
}

/// `_await_pending(request_id, slot)`
pub fn _await_pending(proto: &Protocol, request_id: &str, slot: &Pending) -> Result<Arg> {
    let timeout = rpc_wait_override()
        .filter(|t| *t > 0.0)
        .unwrap_or_else(|| proto.rpc_timeout());
    let completed = {
        let mut g = slot.state.lock();
        wait_until(&slot.cv, &mut g, Some(timeout), |s| s.done)
    };
    proto.0.carrier.pop_pending(request_id);
    if !completed {
        return Err(LailaError::timeout_error(alloc::format!(
            "RPC to peer timed out after {timeout}s on {}.",
            proto.class_name()
        )));
    }
    let mut s = slot.state.lock();
    if let Some(e) = s.exception.take() {
        return Err(e);
    }
    if let Some(err) = s.error.take() {
        let code = err.get("code").unwrap_or(Arg::None).as_i64();
        let message = rpc_protocol::error_message(&err);
        if code == Some(rpc_protocol::ERR_BUSY) {
            return Err(LailaError::backpressure_error(message));
        }
        return Err(LailaError::runtime_error(alloc::format!(
            "Remote RPC error: {message}"
        )));
    }
    Ok(s.result.take().unwrap_or(Arg::None))
}

/// `_send_once(peer_id, path, args, kwargs)` by wire.
pub fn _send_once(
    proto: &Protocol,
    peer_id: &str,
    path: &[String],
    args: &[Arg],
    kwargs: &Arg,
) -> Result<Arg> {
    if let Some(r) = proto
        .hooks()
        .and_then(|h| h.send_once(proto, peer_id, path, args, kwargs))
    {
        return r;
    }
    // `c._send_once = fn` (a test stub assigned on the instance).
    if proto.has_field("_send_once") {
        let f = proto.field("_send_once");
        if !f.is_none() {
            let mut a = Args::new();
            a.push_positional(peer_id);
            a.push_positional(path.to_vec());
            a.push_positional(args.to_vec());
            a.push_positional(kwargs.clone());
            return f.__call__(a);
        }
    }
    match proto.spec().carrier {
        CarrierKind::Stream => super::stream::_send_once(proto, peer_id, path, args, kwargs),
        CarrierKind::Datagram | CarrierKind::LazyDatagram => {
            super::datagram::_send_once(proto, peer_id, path, args, kwargs)
        }
        CarrierKind::WebSocket => {
            super::super::tcpip::_send_once(proto, peer_id, path, args, kwargs)
        }
        CarrierKind::P2P => super::p2p::_send_once(proto, peer_id, path, args, kwargs),
        CarrierKind::Broker => super::broker::_send_once(proto, peer_id, path, args, kwargs),
        CarrierKind::Register => super::register::_send_once(proto, peer_id, path, args, kwargs),
        CarrierKind::Loopback => super::super::local::loopback::send_rpc(
            proto,
            peer_id,
            path.to_vec(),
            args.to_vec(),
            kwargs.clone(),
        ),
        CarrierKind::Base => Err(LailaError::not_implemented("send_rpc")),
    }
}

/// `_try_admit()`: a slot from the hub's bounded admission; always admitted
/// without a hub.
pub fn _try_admit(proto: &Protocol) -> bool {
    proto
        .communication()
        .map(|c| c._acquire_rpc_slot())
        .unwrap_or(true)
}

/// `send_rpc(peer_id, path, args, kwargs)`: one call with exponential
/// backoff on `BackpressureError`.
pub fn send_rpc(
    proto: &Protocol,
    peer_id: &str,
    path: Vec<String>,
    args: Vec<Arg>,
    kwargs: Arg,
) -> Result<Arg> {
    let max_retries = proto.i64_field("max_rpc_retries", 5).max(0);
    let base = proto.f64_field("rpc_backoff_base", 0.05);
    let cap = proto.f64_field("rpc_backoff_max", 5.0);
    let mut attempt: i64 = 0;
    loop {
        match _send_once(proto, peer_id, &path, &args, &kwargs) {
            Err(e) if is_backpressure(&e) => {
                if attempt >= max_retries {
                    return Err(e);
                }
                let delay = cap.min(base * libm::pow(2.0, attempt as f64));
                let jitter = {
                    let mut b = [0u8; 8];
                    crate::_compat::random::fill(&mut b);
                    (u64::from_le_bytes(b) as f64 / u64::MAX as f64) * delay
                };
                let _ = crate::python::stdlib::time.sleep(delay + jitter);
                attempt += 1;
            }
            other => return other,
        }
    }
}

/// `_send_oneway(peer_id, path, kwargs)`: fire-and-forget on a helper
/// thread (the wire carriers queue directly).
pub fn _send_oneway(proto: &Protocol, peer_id: &str, path: &[String], kwargs: &Arg) -> Result<()> {
    match proto.spec().carrier {
        CarrierKind::Stream => super::stream::_send_oneway(proto, peer_id, path, kwargs),
        CarrierKind::Datagram | CarrierKind::LazyDatagram => {
            super::datagram::_send_oneway(proto, peer_id, path, kwargs)
        }
        CarrierKind::Loopback => {
            super::super::local::loopback::_send_oneway(proto, peer_id, path, kwargs)
        }
        CarrierKind::P2P => super::p2p::_send_oneway(proto, peer_id, path, kwargs),
        CarrierKind::Broker => super::broker::_send_oneway(proto, peer_id, path, kwargs),
        CarrierKind::Register => super::register::_send_oneway(proto, peer_id, path, kwargs),
        _ => {
            let p = proto.clone();
            let peer = String::from(peer_id);
            let path = path.to_vec();
            let kwargs = kwargs.clone();
            crate::python::sync::spawn_named(String::from("comm-channel-close"), move || {
                let _ = p.send_rpc(&peer, path, Vec::<Arg>::new(), kwargs);
            })
        }
    }
}

// ---------------------------------------------------------------------------
// stream lanes

/// `_new_channel(peer_id, name, lane, tx_lane=None)`
fn _new_channel(
    proto: &Protocol,
    table: &mut LaneTable,
    peer_id: &str,
    name: &str,
    lane: u8,
    tx_lane: Option<u8>,
) -> Channel {
    let ch = Channel::build(
        proto.owner_weak(),
        peer_id,
        name,
        lane,
        proto.i64_field("channel_queue_size", 256).max(1) as usize,
        proto
            .i64_field("channel_queue_bytes", 64 * 1024 * 1024)
            .max(1) as usize,
        _max_message_bytes(proto).max(0) as usize,
        tx_lane,
    );
    table.lanes_mut(peer_id).push((lane, ch.clone()));
    table
        .names_mut(peer_id)
        .push((String::from(name), ch.clone()));
    ch
}

fn static_lane(proto: &Protocol, name: &str) -> Option<i64> {
    let lanes = proto.field("static_lanes");
    if !matches!(lanes, Arg::Dict(_)) {
        return None;
    }
    let v = lanes.get(name).ok()?;
    if v.is_none() {
        None
    } else {
        v.as_i64()
    }
}

/// `open_channel(peer_id, name)`
pub fn open_channel(proto: &Protocol, peer_id: &str, name: &str) -> Result<Channel> {
    if name == "default" {
        return Err(LailaError::value_error(
            "'default' is the RPC proxy, not a stream channel.",
        ));
    }
    if !proto.supports_channels() {
        return Err(LailaError::connection_error(alloc::format!(
            "{} has no stream lanes.",
            crate::python::pyrepr::str_repr(proto.protocol_name())
        )));
    }
    if !proto.has_peer(peer_id) {
        return Err(LailaError::connection_error(alloc::format!(
            "No connection to peer {peer_id}"
        )));
    }
    let mut opener = false;
    let ch = {
        let mut table = proto.0.carrier.lanes.lock();
        let mut ch = table.named(peer_id, name);
        if let Some(c) = &ch {
            if c.closed() {
                table.drop_channel(c);
                ch = None;
            }
        }
        match ch {
            Some(c) => c,
            None => {
                if let Some(lane) = static_lane(proto, name) {
                    if !(1..=_MAX_LANE as i64).contains(&lane) {
                        return Err(LailaError::connection_error(alloc::format!(
                            "static_lanes[{}]={lane} is outside 1..{_MAX_LANE}.",
                            crate::python::pyrepr::str_repr(name)
                        )));
                    }
                    let lane = lane as u8;
                    if let Some(other) = table.lane(peer_id, lane) {
                        if !other.closed() {
                            return Err(LailaError::connection_error(alloc::format!(
                                "Lane {lane} to peer {peer_id} is already bound to channel {}.",
                                crate::python::pyrepr::str_repr(other.name())
                            )));
                        }
                    }
                    return Ok(_new_channel(
                        proto,
                        &mut table,
                        peer_id,
                        name,
                        lane,
                        Some(lane),
                    ));
                }
                if !proto.0.carrier.peer_has_lanes(peer_id) {
                    return Err(LailaError::connection_error(alloc::format!(
                        "Peer {peer_id} did not advertise stream lanes during the handshake (an older laila or an RPC-only firmware). If it emits lane frames on a fixed lane, configure static_lanes={{{}: <lane>}} on this connection.",
                        crate::python::pyrepr::str_repr(name)
                    )));
                }
                let lane = table.alloc_lane(peer_id, None)?;
                opener = true;
                _new_channel(proto, &mut table, peer_id, name, lane, None)
            }
        }
    };
    if ch.tx_lane_id().is_some() {
        return Ok(ch);
    }
    if !opener {
        if ch.wait_opened(Some(proto.rpc_timeout())) && !ch.closed() {
            return Ok(ch);
        }
        return Err(LailaError::connection_error(alloc::format!(
            "Channel {} to peer {peer_id} did not finish opening ({}).",
            crate::python::pyrepr::str_repr(name),
            ch.closed_reason()
                .unwrap_or_else(|| String::from("timeout"))
        )));
    }
    let mut kwargs = PyDictBuilder::new();
    kwargs.insert("name", name);
    kwargs.insert("lane", ch.lane_id() as i64);
    kwargs.insert("options", PyDictBuilder::new().finish());
    let outcome = proto
        .send_rpc(
            peer_id,
            strs(_COMM_CHANNEL_OPEN_PATH),
            Vec::<Arg>::new(),
            kwargs.finish(),
        )
        .and_then(|result| {
            let tx = result.get("lane")?.as_i64().unwrap_or(0);
            if !(1..=_MAX_LANE as i64).contains(&tx) {
                return Err(LailaError::value_error(alloc::format!(
                    "peer returned lane {tx}"
                )));
            }
            Ok(tx as u8)
        });
    let tx_lane = match outcome {
        Ok(l) => l,
        Err(e) => {
            {
                let mut table = proto.0.carrier.lanes.lock();
                if ch.tx_lane_id().is_some() && !ch.closed() {
                    return Ok(ch);
                }
                table.drop_channel(&ch);
            }
            ch._close_local(&alloc::format!("open failed: {}", e.__str__()));
            return Err(LailaError::connection_error(alloc::format!(
                "Could not open channel {} to peer {peer_id}: {}",
                crate::python::pyrepr::str_repr(name),
                e.__str__()
            )));
        }
    };
    {
        let _table = proto.0.carrier.lanes.lock();
        if ch.closed() {
            return Err(LailaError::connection_error(alloc::format!(
                "Channel {} to peer {peer_id} closed while opening ({}).",
                crate::python::pyrepr::str_repr(name),
                ch.closed_reason().unwrap_or_default()
            )));
        }
        if ch.tx_lane_id().is_none() {
            ch._mark_opened(tx_lane);
        }
    }
    Ok(ch)
}

/// `_bind_static_lanes(peer_id)`
pub fn _bind_static_lanes(proto: &Protocol, peer_id: &str) {
    if !proto.supports_channels() {
        return;
    }
    let lanes = proto.field("static_lanes");
    let Arg::Dict(d) = &lanes else {
        return;
    };
    let pairs: Vec<(String, i64)> = d
        .read()
        .iter()
        .filter_map(|(k, v)| Some((k.__str__(), v.as_i64()?)))
        .collect();
    let mut table = proto.0.carrier.lanes.lock();
    for (name, lane) in pairs {
        if !(1..=_MAX_LANE as i64).contains(&lane) || name == "default" {
            continue;
        }
        let lane = lane as u8;
        if table.lane(peer_id, lane).is_some() || table.named(peer_id, &name).is_some() {
            continue;
        }
        _new_channel(proto, &mut table, peer_id, &name, lane, Some(lane));
    }
}

/// `_close_peer_lanes(peer_id, reason)`
pub fn _close_peer_lanes(proto: &Protocol, peer_id: &str, reason: &str) {
    let channels: Vec<Channel> = {
        let mut table = proto.0.carrier.lanes.lock();
        let mut out: Vec<Channel> = Vec::new();
        if let Some(i) = table.by_lane.iter().position(|(p, _)| p == peer_id) {
            for (_, c) in table.by_lane.remove(i).1 {
                if !out.iter().any(|o| o.is(&c)) {
                    out.push(c);
                }
            }
        }
        if let Some(i) = table.by_name.iter().position(|(p, _)| p == peer_id) {
            for (_, c) in table.by_name.remove(i).1 {
                if !out.iter().any(|o| o.is(&c)) {
                    out.push(c);
                }
            }
        }
        out
    };
    for ch in channels {
        ch._close_local(reason);
    }
}

/// `_shutdown_lanes(reason="protocol stopped")`
pub fn _shutdown_lanes(proto: &Protocol, reason: &str) {
    let peers: Vec<String> = {
        let table = proto.0.carrier.lanes.lock();
        let mut p: Vec<String> = table.by_lane.iter().map(|(p, _)| p.clone()).collect();
        for (q, _) in table.by_name.iter() {
            if !p.contains(q) {
                p.push(q.clone());
            }
        }
        p
    };
    for peer in peers {
        _close_peer_lanes(proto, &peer, reason);
    }
}

/// `_control_response(msg, peer_id)`
pub fn _control_response(proto: &Protocol, msg: &Arg, peer_id: Option<&str>) -> Arg {
    let params = rpc_protocol::msg_get(msg, "params");
    let kwargs = if params.is_none() {
        Arg::None
    } else {
        params.get("kwargs").unwrap_or(Arg::None)
    };
    let kwargs = if kwargs.is_none() {
        PyDictBuilder::new().finish()
    } else {
        kwargs
    };
    match _handle_control(proto, &path_of(msg), &kwargs, peer_id) {
        Ok(result) => rpc_protocol::make_result(rpc_protocol::msg_get(msg, "id"), result),
        Err(e) => rpc_protocol::make_error(
            rpc_protocol::msg_get(msg, "id"),
            rpc_protocol::ERR_EXECUTION,
            &exc_text(&e),
            Arg::None,
        ),
    }
}

/// `_handle_control(path, kwargs, peer_id)`
pub fn _handle_control(
    proto: &Protocol,
    path: &[String],
    kwargs: &Arg,
    peer_id: Option<&str>,
) -> Result<Arg> {
    let Some(peer_id) = peer_id else {
        return Err(LailaError::runtime_error(
            "Stream-lane control requires an authenticated peer.",
        ));
    };
    if !proto.supports_channels() {
        return Err(LailaError::connection_error(alloc::format!(
            "{} has no stream lanes.",
            crate::python::pyrepr::str_repr(proto.protocol_name())
        )));
    }
    if path_eq(path, _COMM_CHANNEL_OPEN_PATH) {
        let name = kwargs.get("name")?;
        let name_s = match &name {
            Arg::Str(s) if !s.is_empty() && &**s != "default" => s.to_string(),
            _ => {
                return Err(LailaError::value_error(alloc::format!(
                    "Invalid channel name {}.",
                    name.__repr__()
                )))
            }
        };
        let remote_lane = kwargs.get("lane")?.as_i64().unwrap_or(0);
        if !(1..=_MAX_LANE as i64).contains(&remote_lane) {
            return Err(LailaError::value_error(alloc::format!(
                "Invalid lane {remote_lane}."
            )));
        }
        let remote_lane = remote_lane as u8;
        let mut table = proto.0.carrier.lanes.lock();
        let mut ch = table.named(peer_id, &name_s);
        if let Some(c) = &ch {
            if c.closed() {
                table.drop_channel(c);
                ch = None;
            }
        }
        if let Some(c) = ch {
            if c.tx_lane_id().is_none() {
                c._mark_opened(remote_lane);
            } else {
                c.set_tx_lane_id(Some(remote_lane));
            }
            let mut d = PyDictBuilder::new();
            d.insert("lane", c.lane_id() as i64);
            return Ok(d.finish());
        }
        let lane = table.alloc_lane(peer_id, Some(remote_lane))?;
        _new_channel(proto, &mut table, peer_id, &name_s, lane, Some(remote_lane));
        let mut d = PyDictBuilder::new();
        d.insert("lane", lane as i64);
        return Ok(d.finish());
    }
    if path_eq(path, _COMM_CHANNEL_CLOSE_PATH) {
        let lane = kwargs.get("lane")?.as_i64().unwrap_or(0);
        let ch = {
            let mut table = proto.0.carrier.lanes.lock();
            let ch = if (0..=255).contains(&lane) {
                table.lane(peer_id, lane as u8)
            } else {
                None
            };
            if let Some(c) = &ch {
                table.drop_channel(c);
            }
            ch
        };
        if let Some(c) = ch {
            c._close_local("closed by peer");
        }
        return Ok(Arg::Bool(true));
    }
    Err(LailaError::value_error(alloc::format!(
        "Unknown control path {}.",
        list_arg(path).__repr__()
    )))
}

/// `_on_channel_closed_locally(ch)`
pub fn _on_channel_closed_locally(proto: &Protocol, ch: &Channel) {
    proto.0.carrier.lanes.lock().drop_channel(ch);
    let Some(tx) = ch.tx_lane_id() else {
        return;
    };
    if !proto.has_peer(ch.peer_id()) {
        return;
    }
    let mut kwargs = PyDictBuilder::new();
    kwargs.insert("lane", tx as i64);
    let _ = _send_oneway(
        proto,
        ch.peer_id(),
        &strs(_COMM_CHANNEL_CLOSE_PATH),
        &kwargs.finish(),
    );
}

/// `_stream_enqueue(channel, payload)` by wire.
pub fn _stream_enqueue(proto: &Protocol, channel: &Channel, payload: &[u8]) -> Result<()> {
    match proto.spec().carrier {
        CarrierKind::Stream => super::stream::_stream_enqueue(proto, channel, payload),
        CarrierKind::Datagram | CarrierKind::LazyDatagram => {
            super::datagram::_stream_enqueue(proto, channel, payload)
        }
        CarrierKind::Loopback => {
            super::super::local::loopback::_stream_enqueue(proto, channel, payload)
        }
        CarrierKind::P2P => super::p2p::_stream_enqueue(proto, channel, payload),
        _ => Err(LailaError::connection_error(alloc::format!(
            "{} cannot carry stream lanes.",
            crate::python::pyrepr::str_repr(proto.protocol_name())
        ))),
    }
}

/// `_lookup_lane(peer_id, lane)`
pub fn _lookup_lane(proto: &Protocol, peer_id: Option<&str>, lane: u8) -> Option<Channel> {
    proto.0.carrier.lanes.lock().lane(peer_id?, lane)
}

/// `_on_stream_frame(peer_id, raw)`: route one inbound lane frame.
pub fn _on_stream_frame(proto: &Protocol, peer_id: Option<&str>, raw: &[u8]) {
    if !_codec::is_stream_frame(raw) {
        proto
            .0
            .carrier
            .reserved_marker_drops
            .fetch_add(1, Ordering::Relaxed);
        return;
    }
    let (lane, seq, flags) = match _codec::unpack_stream_header(raw) {
        Ok(h) => h,
        Err(_) => {
            proto
                .0
                .carrier
                .malformed_stream_drops
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    let ch = _lookup_lane(proto, peer_id, lane);
    match ch {
        Some(c) if !c.closed() => c._on_chunk(
            seq,
            flags,
            &raw[_codec::STREAM_HEADER_LEN..],
            crate::python::stdlib::time.monotonic(),
        ),
        _ => {
            proto
                .0
                .carrier
                .unknown_lane_drops
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// `_on_stream_payload(peer_id, lane, payload)` (loopback: whole messages).
pub fn _on_stream_payload(proto: &Protocol, peer_id: &str, lane: u8, payload: &[u8]) {
    match _lookup_lane(proto, Some(peer_id), lane) {
        Some(c) if !c.closed() => c._enqueue_inbound(payload),
        _ => {
            proto
                .0
                .carrier
                .unknown_lane_drops
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// `_stream_chunks(channel, payload)`: the lane frames for one message.
pub fn _stream_chunks(proto: &Protocol, channel: &Channel, payload: &[u8]) -> Vec<Vec<u8>> {
    let size = proto.i64_field("stream_chunk_bytes", 65536).max(1) as usize;
    let n = payload.len();
    let count = core::cmp::max(1, n.div_ceil(size));
    let first = channel._next_chunk_seqs(count as u64);
    let lane = channel.tx_lane_id().unwrap_or(0);
    if count == 1 {
        return alloc::vec![_codec::pack_stream_frame(
            lane,
            first,
            _codec::FLAG_START | _codec::FLAG_END,
            payload
        )];
    }
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let mut flags = 0u8;
        if i == 0 {
            flags |= _codec::FLAG_START;
        }
        if i == count - 1 {
            flags |= _codec::FLAG_END;
        }
        let end = core::cmp::min(n, (i + 1) * size);
        out.push(_codec::pack_stream_frame(
            lane,
            (first + i as u64) % _codec::SEQ_MODULUS,
            flags,
            &payload[i * size..end],
        ));
    }
    out
}

/// `self._pending_rpcs` after `stop()`.
pub fn _reset_after_stop(proto: &Protocol) {
    _shutdown_lanes(proto, "protocol stopped");
    proto.0.carrier.fail_pending("transport stopped", None);
    proto.0.carrier.clear_pending();
    proto.0.carrier.set_started(false);
}

/// The common `start()` prologue: `False` when already started.
pub fn _begin_start(proto: &Protocol) -> bool {
    !proto.0.carrier.started()
}

/// Build the `peer.connect` request.
pub fn _handshake_request(proto: &Protocol, secret: &str) -> Arg {
    let mut p = PyDictBuilder::new();
    p.insert("from_id", proto.policy_id());
    p.insert("secret", secret);
    p.insert("caps", _local_caps(proto));
    rpc_protocol::make_request(_PEER_CONNECT, p.finish(), None)
}

/// Validate the `peer.connect` response: `(peer_id, caps)`.
pub fn _handshake_result(msg: &Arg) -> Result<(String, Arg)> {
    if msg.__contains__("error").unwrap_or(false) {
        let err = rpc_protocol::msg_get(msg, "error");
        return Err(LailaError::connection_error(alloc::format!(
            "Peer rejected connection: {}",
            rpc_protocol::error_message(&err)
        )));
    }
    let result = rpc_protocol::msg_get(msg, "result");
    let peer_id = if result.is_none() {
        Arg::None
    } else {
        result.get("peer_id").unwrap_or(Arg::None)
    };
    if peer_id.is_none() {
        return Err(LailaError::connection_error(
            "Peer response missing peer_id.",
        ));
    }
    let caps = if result.is_none() {
        Arg::None
    } else {
        result.get("caps").unwrap_or(Arg::None)
    };
    Ok((peer_id.__str__(), caps))
}

/// Answer an inbound `peer.connect`: `Ok((peer_id, caps, response))` or
/// `Err(response)` for a rejected handshake.
pub fn _handshake_accept(
    proto: &Protocol,
    msg: &Arg,
) -> core::result::Result<(String, Arg, Arg), Arg> {
    let request_id = rpc_protocol::msg_get(msg, "id");
    if !rpc_protocol::is_request(msg)
        || rpc_protocol::msg_get(msg, "method").__str__() != _PEER_CONNECT
    {
        return Err(rpc_protocol::make_error(
            request_id,
            rpc_protocol::ERR_INVALID_REQUEST,
            "First message must be a peer.connect request.",
            Arg::None,
        ));
    }
    let params = rpc_protocol::msg_get(msg, "params");
    let get = |k: &str| {
        if params.is_none() {
            Arg::None
        } else {
            params.get(k).unwrap_or(Arg::None)
        }
    };
    let peer_id = get("from_id");
    if get("secret").__str__() != proto.peer_secret_key() || get("secret").is_none() {
        return Err(rpc_protocol::make_error(
            request_id,
            rpc_protocol::ERR_AUTH_FAILED,
            "Invalid peer secret key.",
            Arg::None,
        ));
    }
    let mut r = PyDictBuilder::new();
    r.insert("peer_id", proto.policy_id());
    if proto.spec().carrier != CarrierKind::WebSocket {
        r.insert("caps", _local_caps(proto));
    }
    Ok((
        peer_id.__str__(),
        get("caps"),
        rpc_protocol::make_result(request_id, r.finish()),
    ))
}
