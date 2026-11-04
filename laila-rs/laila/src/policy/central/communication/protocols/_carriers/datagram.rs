//! `_DatagramRPCProtocol` -- reliable JSON-RPC over lossy packet links.
//!
//! Each codec payload is split into `mtu`-sized fragments, every fragment
//! carries the 8-byte header `>3sBHH` (`b"LDG"`, type, index, count) and a
//! 16-byte message id; the receiver acks every fragment, reassembles, and
//! de-duplicates by message id (a 4096-entry window). A retransmit loop
//! resends unacked fragments every `ack_timeout` seconds up to
//! `max_retries` times, then fails the owning RPC with `ConnectionError`.
//! Stream lanes ride the same socket as single-datagram frames (no
//! fragmentation: a lane message must fit `mtu - 7`).
//!
//! Rust shape: a receive thread and a retransmit thread per endpoint.

use alloc::collections::VecDeque;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicBool, Ordering};
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};

use crate::python::object::PyObject;
use crate::python::sync::{wait_until, Condvar, Mutex};
use crate::python::{Arg, Args, IntoArg, LailaError, PyArg, Result};

use super::super::super::channel::Channel;
use super::super::super::protocol as rpc_protocol;
use super::super::base::{CarrierKind, Protocol};
use super::base::{self as carrier, PeerHandle};
use super::codec as _codec;

/// `log = logging.getLogger(__name__)` -- the module logger.
pub fn log() -> crate::python::stdlib::logging::Logger {
    crate::python::stdlib::logging::Logger::get(
        "laila.policy.central.communication.protocols._carriers.datagram",
    )
}

/// `_MAGIC = b"LDG"`
pub const _MAGIC: &[u8; 3] = b"LDG";
/// `_TYPE_DATA = 0`
pub const _TYPE_DATA: i64 = 0;
/// `_TYPE_ACK = 1`
pub const _TYPE_ACK: i64 = 1;
/// `_HEADER.size` (`>3sBHH`)
pub const HEADER_LEN: usize = 8;
/// `_MSG_ID_LEN = 16`
pub const _MSG_ID_LEN: usize = 16;
/// `_PREFIX_LEN = _HEADER.size + _MSG_ID_LEN`
pub const _PREFIX_LEN: usize = HEADER_LEN + _MSG_ID_LEN;

/// `_HEADER = struct.Struct(">3sBHH")` as a value (`.size`, `.pack`, `.unpack`).
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct HeaderStruct;
pub static _HEADER: HeaderStruct = HeaderStruct;

impl HeaderStruct {
    pub fn size(&self) -> i64 {
        HEADER_LEN as i64
    }
    pub fn pack_bytes(ptype: u8, index: u16, count: u16) -> [u8; HEADER_LEN] {
        let mut h = [0u8; HEADER_LEN];
        h[..3].copy_from_slice(_MAGIC);
        h[3] = ptype;
        h[4..6].copy_from_slice(&index.to_be_bytes());
        h[6..8].copy_from_slice(&count.to_be_bytes());
        h
    }
    pub fn unpack_bytes(raw: &[u8]) -> Option<([u8; 3], u8, u16, u16)> {
        if raw.len() < HEADER_LEN {
            return None;
        }
        Some((
            [raw[0], raw[1], raw[2]],
            raw[3],
            u16::from_be_bytes([raw[4], raw[5]]),
            u16::from_be_bytes([raw[6], raw[7]]),
        ))
    }
    /// `_HEADER.pack(magic, ptype, index, count)`
    pub fn pack(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let p = |i: usize| a.positional.get(i).cloned().unwrap_or(Arg::None);
        let ptype = p(1).as_i64().unwrap_or(0) as u8;
        let index = p(2).as_i64().unwrap_or(0) as u16;
        let count = p(3).as_i64().unwrap_or(0) as u16;
        Ok(Self::pack_bytes(ptype, index, count).to_vec().into_arg())
    }
    /// `_HEADER.unpack(raw)` -> `(magic, ptype, index, count)`
    pub fn unpack(&self, raw: impl IntoArg) -> Result<Arg> {
        let raw = raw.into_arg().as_bytes_like().unwrap_or_default();
        let Some((magic, ptype, index, count)) = Self::unpack_bytes(&raw) else {
            return Err(LailaError::new(
                crate::python::ExcKind::ValueError,
                alloc::format!("unpack requires a buffer of {HEADER_LEN} bytes"),
            ));
        };
        Ok(Arg::Tuple(Arc::from(alloc::vec![
            magic.to_vec().into_arg(),
            Arg::Int(ptype as i64),
            Arg::Int(index as i64),
            Arg::Int(count as i64),
        ])))
    }
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "size" => Ok(Arg::Int(HEADER_LEN as i64)),
            "format" => Ok(Arg::Str(">3sBHH".into())),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'Struct' object has no attribute '{other}'"
            ))),
        }
    }
}
impl PyObject for HeaderStruct {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "Struct"
    }
    fn __repr__(&self) -> String {
        String::from("<Struct '>3sBHH'>")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other.as_any().downcast_ref::<HeaderStruct>().is_some()
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(0x4c4447)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        HeaderStruct::__getattr__(self, name)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "pack" => self.pack(args),
            "unpack" => self.unpack(args.positional.first().cloned().unwrap_or(Arg::None)),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'Struct' object has no attribute '{other}'"
            ))),
        }
    }
}
impl IntoArg for HeaderStruct {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for HeaderStruct {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(*self))
    }
}

/// `_DatagramRPCProtocol` as a class marker.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct _DatagramRPCProtocol;
impl crate::python::builtins::IsInstance for _DatagramRPCProtocol {
    fn isinstance(x: &Arg) -> bool {
        Protocol::of(x).is_some_and(|p| {
            matches!(
                p.spec().carrier,
                CarrierKind::Datagram | CarrierKind::LazyDatagram
            )
        })
    }
}

// ---------------------------------------------------------------------------
// state

type MsgId = [u8; 16];
/// `(peer_id, msg_id, fragment index)`: the key of an unacknowledged fragment.
type UnackedKey = (String, MsgId, u16);

struct Reasm {
    count: u16,
    frags: Vec<(u16, Vec<u8>)>,
}

struct Unacked {
    packet: Vec<u8>,
    tries: i64,
    request_id: Option<String>,
}

pub struct HandshakeSlot {
    state: Mutex<Option<Arg>>,
    cv: Condvar,
}

/// The packet link under the datagram carrier (`_create_datagram_endpoint`
/// / `_sendto` / `_feed_packet` in Python). The default is a UDP socket
/// ([`_DatagramEndpoint`]); a driver or a test harness installs its own through
/// [`set_link`] before `start()` (the in-memory lossy "ether" of the carrier
/// tests, a radio modem, ...).
pub trait DatagramLink: Send + Sync {
    /// Bring the endpoint up (`start()`); a link may be reused across
    /// lifecycles, so `close()` / `open()` pair up.
    fn open(&self) -> Result<()> {
        Ok(())
    }
    /// `transport.get_extra_info("sockname")`: the local address.
    fn local_addr(&self) -> String;
    /// The bound port, when the link is IP-based.
    fn local_port(&self) -> Option<u16> {
        None
    }
    /// `_resolve_peer_addr(uri)`: the peer address a URI designates.
    fn resolve_peer_addr(&self, uri: &str) -> Result<String>;
    /// `_sendto(addr, packet)`.
    fn send_to(&self, addr: &str, packet: &[u8]) -> Result<()>;
    /// Block (bounded, ~100ms) for the next inbound packet; `Ok(None)` on
    /// timeout.
    fn recv_from(&self) -> Result<Option<(String, Vec<u8>)>>;
    /// `transport.close()`.
    fn close(&self) {}
}

/// The default [`DatagramLink`]: a bound UDP socket.
#[allow(non_camel_case_types)]
pub struct _DatagramEndpoint(pub UdpSocket);

impl DatagramLink for _DatagramEndpoint {
    fn local_addr(&self) -> String {
        self.0
            .local_addr()
            .map(|a| a.to_string())
            .unwrap_or_default()
    }
    fn local_port(&self) -> Option<u16> {
        self.0.local_addr().ok().map(|a| a.port())
    }
    fn resolve_peer_addr(&self, uri: &str) -> Result<String> {
        let (host, port) = super::uri::split_host_port(uri, "127.0.0.1", 0);
        let sa = (host.as_str(), port)
            .to_socket_addrs()
            .ok()
            .and_then(|mut a| a.next())
            .ok_or_else(|| {
                LailaError::connection_error(alloc::format!("could not resolve {host}:{port}"))
            })?;
        Ok(sa.to_string())
    }
    fn send_to(&self, addr: &str, packet: &[u8]) -> Result<()> {
        let Some(sa) = parse_addr(addr) else {
            return Err(LailaError::connection_error(alloc::format!(
                "bad peer address {addr}"
            )));
        };
        self.0
            .send_to(packet, sa)
            .map(|_| ())
            .map_err(|e| io_err("datagram send failed", e))
    }
    fn recv_from(&self) -> Result<Option<(String, Vec<u8>)>> {
        let mut buf = alloc::vec![0u8; 65536];
        match self.0.recv_from(&mut buf) {
            Ok((n, from)) => Ok(Some((from.to_string(), buf[..n].to_vec()))),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                Ok(None)
            }
            Err(e) => Err(io_err("datagram receive failed", e)),
        }
    }
}

/// Install the packet link `start()` will use instead of binding a UDP
/// socket (a driver endpoint or a test harness's in-memory ether).
pub fn set_link(proto: &Protocol, link: Arc<dyn DatagramLink>) {
    proto.0.transport.lock().datagram_link = Some(link);
}

/// The endpoint: link + reliability tables (`_transport`, `_reasm`,
/// `_seen`, `_unacked`, `_handshake_pending`).
pub struct DatagramState {
    pub link: Arc<dyn DatagramLink>,
    pub stop: Arc<AtomicBool>,
    /// The protocol's class name: the key `mock.patch.object(proto,
    /// "_sendto", ..)` installs under.
    class_name: &'static str,
    reasm: Mutex<Vec<((String, MsgId), Reasm)>>,
    seen: Mutex<(Vec<MsgId>, VecDeque<MsgId>)>,
    unacked: Mutex<Vec<(UnackedKey, Unacked)>>,
    handshake_pending: Mutex<Vec<(String, Arc<HandshakeSlot>)>>,
}

impl DatagramState {
    /// `proto._unacked` as a dict `{(peer, msg_id, frag): {"tries", "request_id"}}`.
    pub fn unacked_dict(&self) -> Arg {
        let mut d = crate::python::PyDictBuilder::new();
        for ((peer, mid, frag), u) in self.unacked.lock().iter() {
            let key = Arg::Tuple(
                alloc::vec![
                    Arg::Str(peer.as_str().into()),
                    crate::python::object::bytes(mid),
                    (*frag as i64).into_arg(),
                ]
                .into(),
            );
            let mut v = crate::python::PyDictBuilder::new();
            v.insert("tries", u.tries);
            v.insert("request_id", u.request_id.clone());
            d.insert(key, v.finish());
        }
        d.finish()
    }
    /// `proto._reasm`'s keys as a list of `(peer, msg_id)` tuples.
    pub fn reasm_keys(&self) -> Arg {
        self.reasm
            .lock()
            .iter()
            .map(|((peer, mid), _)| {
                Arg::Tuple(
                    alloc::vec![
                        Arg::Str(peer.as_str().into()),
                        crate::python::object::bytes(mid)
                    ]
                    .into(),
                )
            })
            .collect::<Vec<_>>()
            .into_arg()
    }
}

/// `proto._unacked` (`{}` when the transport is not running).
pub fn unacked_dict(proto: &Protocol) -> Arg {
    match state_of(proto) {
        Ok(s) => s.unacked_dict(),
        Err(_) => crate::python::PyDictBuilder::new().finish(),
    }
}

fn io_err(prefix: &str, e: std::io::Error) -> LailaError {
    LailaError::connection_error(alloc::format!("{prefix}: {e}"))
}

fn state_of(proto: &Protocol) -> Result<Arc<DatagramState>> {
    proto.0.transport.lock().datagram.clone().ok_or_else(|| {
        LailaError::connection_error(alloc::format!(
            "{} transport is shut down.",
            crate::python::pyrepr::str_repr(proto.protocol_name())
        ))
    })
}

fn parse_addr(addr: &str) -> Option<SocketAddr> {
    addr.to_socket_addrs().ok()?.next()
}

/// `_sendto(addr, packet)`
fn sendto(state: &DatagramState, addr: &str, packet: &[u8]) -> Result<()> {
    // `mock.patch.object(proto, "_sendto", fn)`: the stand-in takes the
    // packet instead of the link (a test's lossy "ether").
    if let Some(m) = crate::python::mock::patched(state.class_name, "_sendto") {
        let mut a = Args::new();
        a.push_positional(addr);
        a.push_positional(Arg::Bytes(Arc::from(packet)));
        return m.__call__(a).map(|_| ());
    }
    state.link.send_to(addr, packet)
}

/// `_send_message(addr, payload, request_id=None)`: fragment + track.
fn send_message(
    proto: &Protocol,
    state: &DatagramState,
    addr: &str,
    payload: &[u8],
    request_id: Option<&str>,
) -> Result<()> {
    let msg_id: MsgId = crate::python::stdlib::uuid.uuid4().bytes;
    let mtu = proto.i64_field("mtu", 1200).max(1) as usize;
    let mut fragments: Vec<&[u8]> = payload.chunks(mtu).collect();
    if fragments.is_empty() {
        fragments.push(&[]);
    }
    let count = fragments.len() as u16;
    for (index, frag) in fragments.iter().enumerate() {
        let mut packet = Vec::with_capacity(_PREFIX_LEN + frag.len());
        packet.extend_from_slice(&HeaderStruct::pack_bytes(
            _TYPE_DATA as u8,
            index as u16,
            count,
        ));
        packet.extend_from_slice(&msg_id);
        packet.extend_from_slice(frag);
        state.unacked.lock().push((
            (String::from(addr), msg_id, index as u16),
            Unacked {
                packet: packet.clone(),
                tries: 0,
                request_id: request_id.map(String::from),
            },
        ));
        sendto(state, addr, &packet)?;
    }
    Ok(())
}

/// `_give_up(key, slot, reason)`
fn give_up(
    proto: &Protocol,
    state: &DatagramState,
    addr: &str,
    msg_id: &MsgId,
    request_id: Option<String>,
    reason: &str,
) {
    state
        .unacked
        .lock()
        .retain(|((a, m, _), _)| !(a == addr && m == msg_id));
    if let Some(rid) = request_id {
        proto.0.carrier.fail_request(
            &rid,
            LailaError::connection_error(alloc::format!(
                "datagram delivery to {addr} gave up ({reason}) after {} retransmits on {}.",
                proto.i64_field("max_retries", 5),
                proto.class_name()
            )),
        );
    }
}

/// `_retransmit_loop()`
fn retransmit_loop(proto: Protocol, state: Arc<DatagramState>) {
    let max_retries = proto.i64_field("max_retries", 5);
    loop {
        // Sleep ``ack_timeout`` in small slices so stop is prompt.
        let ack_timeout = proto.f64_field("ack_timeout", 0.5).max(0.001);
        let deadline = std::time::Instant::now() + core::time::Duration::from_secs_f64(ack_timeout);
        while std::time::Instant::now() < deadline {
            if state.stop.load(Ordering::Acquire) {
                return;
            }
            std::thread::sleep(core::time::Duration::from_millis(10));
        }
        if state.stop.load(Ordering::Acquire) {
            return;
        }
        let snapshot: Vec<(UnackedKey, i64, Vec<u8>, Option<String>)> = state
            .unacked
            .lock()
            .iter()
            .map(|(k, u)| (k.clone(), u.tries, u.packet.clone(), u.request_id.clone()))
            .collect();
        for (key, tries, packet, rid) in snapshot {
            let still = state.unacked.lock().iter().any(|(k, _)| *k == key);
            if !still {
                continue;
            }
            if tries >= max_retries {
                give_up(&proto, &state, &key.0, &key.1, rid, "no ack");
                continue;
            }
            {
                let mut g = state.unacked.lock();
                if let Some((_, u)) = g.iter_mut().find(|(k, _)| *k == key) {
                    u.tries += 1;
                }
            }
            if let Err(e) = sendto(&state, &key.0, &packet) {
                give_up(
                    &proto,
                    &state,
                    &key.0,
                    &key.1,
                    rid,
                    &alloc::format!("send failed: {}", e.__str__()),
                );
            }
        }
    }
}

/// `_peer_for_addr(addr)`
fn peer_for_addr(proto: &Protocol, addr: &str) -> Option<String> {
    proto
        .0
        .carrier
        .connections()
        .into_iter()
        .find(|(_, h)| matches!(h, PeerHandle::Addr(a) if a == addr))
        .map(|(p, _)| p)
}

/// `_mark_seen(msg_id)`
fn mark_seen(state: &DatagramState, msg_id: MsgId) {
    let mut g = state.seen.lock();
    g.0.push(msg_id);
    g.1.push_back(msg_id);
    if g.1.len() > 4096 {
        if let Some(old) = g.1.pop_front() {
            if let Some(i) = g.0.iter().position(|m| *m == old) {
                g.0.swap_remove(i);
            }
        }
    }
}

/// `_feed_packet(addr, data)`
pub fn feed_packet(proto: &Protocol, state: &Arc<DatagramState>, addr: &str, data: &[u8]) {
    if _codec::is_reserved_frame(data) {
        match peer_for_addr(proto, addr) {
            Some(peer_id) => carrier::_on_stream_frame(proto, Some(&peer_id), data),
            None => carrier::_on_stream_frame(proto, None, data),
        }
        return;
    }
    if data.len() < _PREFIX_LEN || &data[..3] != _MAGIC {
        return;
    }
    let Some((_magic, ptype, frag_index, frag_count)) = HeaderStruct::unpack_bytes(data) else {
        return;
    };
    let mut msg_id = [0u8; 16];
    msg_id.copy_from_slice(&data[HEADER_LEN.._PREFIX_LEN]);
    let payload = &data[_PREFIX_LEN..];
    if ptype as i64 == _TYPE_ACK {
        state
            .unacked
            .lock()
            .retain(|((a, m, i), _)| !(a == addr && *m == msg_id && *i == frag_index));
        return;
    }
    let mut ack = Vec::with_capacity(_PREFIX_LEN);
    ack.extend_from_slice(&HeaderStruct::pack_bytes(
        _TYPE_ACK as u8,
        frag_index,
        frag_count,
    ));
    ack.extend_from_slice(&msg_id);
    let _ = sendto(state, addr, &ack);
    if state.seen.lock().0.contains(&msg_id) {
        return;
    }
    let full = {
        let mut g = state.reasm.lock();
        let key = (String::from(addr), msg_id);
        let i = match g.iter().position(|(k, _)| *k == key) {
            Some(i) => i,
            None => {
                g.push((
                    key,
                    Reasm {
                        count: frag_count,
                        frags: Vec::new(),
                    },
                ));
                g.len() - 1
            }
        };
        let entry = &mut g[i].1;
        if !entry.frags.iter().any(|(fi, _)| *fi == frag_index) {
            entry.frags.push((frag_index, payload.to_vec()));
        }
        if (entry.frags.len() as u16) < entry.count {
            return;
        }
        let (_, mut entry) = g.remove(i);
        entry.frags.sort_by_key(|(i, _)| *i);
        let mut full = Vec::new();
        for (_, f) in entry.frags {
            full.extend_from_slice(&f);
        }
        full
    };
    mark_seen(state, msg_id);
    on_message(proto, state, addr, &full);
}

/// `_on_message(addr, payload)`
fn on_message(proto: &Protocol, state: &Arc<DatagramState>, addr: &str, payload: &[u8]) {
    let Ok(msg) = _codec::decode(payload, &proto.codec()) else {
        return;
    };
    if rpc_protocol::is_request(&msg) {
        let method = rpc_protocol::msg_get(&msg, "method").__str__();
        if method == carrier::_PEER_CONNECT {
            handle_handshake(proto, state, addr, &msg);
        } else if carrier::_is_goodbye(&msg) {
            if let Some(peer_id) = peer_for_addr(proto, addr) {
                let params = rpc_protocol::msg_get(&msg, "params");
                let from = if params.is_none() {
                    Arg::None
                } else {
                    params.get("from_id").unwrap_or(Arg::None)
                };
                if from.__str__() == peer_id {
                    carrier::_on_peer_goodbye(proto, &peer_id);
                }
            }
        } else {
            dispatch_rpc(proto, state, addr, msg);
        }
    } else if rpc_protocol::is_response(&msg) {
        let rid = rpc_protocol::msg_get(&msg, "id").__str__();
        let slot = state
            .handshake_pending
            .lock()
            .iter()
            .find(|(r, _)| *r == rid)
            .map(|(_, s)| s.clone());
        match slot {
            Some(slot) => {
                let mut d = crate::python::PyDictBuilder::new();
                d.insert("msg", msg);
                d.insert("addr", addr);
                *slot.state.lock() = Some(d.finish());
                slot.cv.notify_all();
            }
            None => proto.0.carrier.complete_pending(&msg),
        }
    }
}

/// `_handle_handshake(addr, msg)`
fn handle_handshake(proto: &Protocol, state: &Arc<DatagramState>, addr: &str, msg: &Arg) {
    let codec = proto.codec();
    match carrier::_handshake_accept(proto, msg) {
        Err(resp) => {
            if let Ok(b) = _codec::encode(&resp, &codec) {
                let _ = send_message(proto, state, addr, &b, None);
            }
        }
        Ok((peer_id, caps, resp)) => {
            proto.0.carrier.set_peer_caps(&peer_id, caps);
            carrier::_register_peer(proto, &peer_id, PeerHandle::Addr(String::from(addr)));
            if let Ok(b) = _codec::encode(&resp, &codec) {
                let _ = send_message(proto, state, addr, &b, None);
            }
        }
    }
}

/// `_dispatch_rpc(addr, msg)`
fn dispatch_rpc(proto: &Protocol, state: &Arc<DatagramState>, addr: &str, msg: Arg) {
    let p = proto.clone();
    let st = state.clone();
    let a = String::from(addr);
    let codec = proto.codec();
    let reply: carrier::Reply = Arc::new(move |resp: Arg| {
        if let Ok(b) = _codec::encode(&resp, &codec) {
            let _ = send_message(&p, &st, &a, &b, None);
        }
    });
    let peer = peer_for_addr(proto, addr);
    carrier::_handle_request_frame(proto, msg, reply, peer.as_deref());
}

fn recv_loop(proto: Protocol, state: Arc<DatagramState>) {
    loop {
        if state.stop.load(Ordering::Acquire) {
            break;
        }
        match state.link.recv_from() {
            Ok(Some((from, data))) => feed_packet(&proto, &state, &from, &data),
            Ok(None) => continue,
            Err(_) => {
                if state.stop.load(Ordering::Acquire) {
                    break;
                }
                // ICMP unreachable etc. -- keep serving.
                std::thread::sleep(core::time::Duration::from_millis(5));
            }
        }
    }
}

/// `_create_datagram_endpoint()`: the installed link, else bind a UDP socket.
fn create_endpoint(proto: &Protocol) -> Result<Arc<dyn DatagramLink>> {
    if let Some(link) = proto.0.transport.lock().datagram_link.clone() {
        return Ok(link);
    }
    if proto.spec().carrier == CarrierKind::LazyDatagram {
        let mods: Vec<String> = proto
            .spec()
            .driver_modules
            .iter()
            .map(|m| m.to_string())
            .collect();
        carrier::_require_drivers(proto, &mods, proto.spec().driver_extra)?;
        return Err(LailaError::runtime_error(alloc::format!(
            "The {} transport requires dedicated radio hardware/firmware and an out-of-band interface configuration; no host-side driver endpoint is available to bring up automatically.",
            crate::python::pyrepr::str_repr(proto.protocol_name())
        )));
    }
    let host = proto.str_field("host");
    let host = if host.is_empty() {
        String::from("0.0.0.0")
    } else {
        host
    };
    let port = proto.i64_field("port", 0).clamp(0, 65535) as u16;
    let sock = UdpSocket::bind((host.as_str(), port))
        .map_err(|e| io_err("could not bind UDP socket", e))?;
    sock.set_read_timeout(Some(core::time::Duration::from_millis(100)))
        .map_err(|e| io_err("socket timeout", e))?;
    Ok(Arc::new(_DatagramEndpoint(sock)))
}

/// `start()`
pub fn start(proto: &Protocol) -> Result<()> {
    if !carrier::_begin_start(proto) {
        return Ok(());
    }
    let link = create_endpoint(proto)?;
    link.open()?;
    let port = link.local_port();
    let stop = Arc::new(AtomicBool::new(false));
    let state = Arc::new(DatagramState {
        link,
        stop: stop.clone(),
        class_name: proto.0.spec.class_name,
        reasm: Mutex::new(Vec::new()),
        seen: Mutex::new((Vec::new(), VecDeque::new())),
        unacked: Mutex::new(Vec::new()),
        handshake_pending: Mutex::new(Vec::new()),
    });
    {
        let mut t = proto.0.transport.lock();
        t.bound_port = port;
        t.stop = Some(stop);
        t.datagram = Some(state.clone());
    }
    let (p1, s1) = (proto.clone(), state.clone());
    super::transport::spawn_loop(
        proto,
        alloc::format!("{}-recv", proto.class_name()),
        move || recv_loop(p1, s1),
    )?;
    let (p2, s2) = (proto.clone(), state);
    super::transport::spawn_loop(
        proto,
        alloc::format!("{}-retransmit", proto.class_name()),
        move || retransmit_loop(p2, s2),
    )?;
    proto.0.carrier.set_started(true);
    Ok(())
}

/// `stop()`
pub fn stop(proto: &Protocol) -> Result<()> {
    if !proto.0.carrier.started() {
        return Ok(());
    }
    carrier::_goodbye_all(proto);
    for peer_id in proto.0.carrier.peer_ids() {
        carrier::_unregister_peer(proto, &peer_id);
    }
    let state = {
        let mut t = proto.0.transport.lock();
        t.raise_stop();
        let s = t.datagram.take();
        t.clear();
        s
    };
    super::transport::join_loops(proto);
    if let Some(s) = state {
        s.reasm.lock().clear();
        s.unacked.lock().clear();
        s.handshake_pending.lock().clear();
        s.link.close();
    }
    carrier::_reset_after_stop(proto);
    Ok(())
}

/// `connect(uri, secret) -> peer gid`
pub fn connect(proto: &Protocol, uri: &str, secret: &str) -> Result<String> {
    proto.start(Args::new())?;
    let state = state_of(proto)?;
    let addr = state.link.resolve_peer_addr(uri)?;
    let req = carrier::_handshake_request(proto, secret);
    let rid = rpc_protocol::msg_get(&req, "id").__str__();
    let slot = Arc::new(HandshakeSlot {
        state: Mutex::new(None),
        cv: Condvar::new(),
    });
    state
        .handshake_pending
        .lock()
        .push((rid.clone(), slot.clone()));
    let b = _codec::encode(&req, &proto.codec())?;
    send_message(proto, &state, &addr, &b, None)?;
    let completed = {
        let mut g = slot.state.lock();
        wait_until(&slot.cv, &mut g, Some(proto.handshake_timeout()), |s| {
            s.is_some()
        })
    };
    state.handshake_pending.lock().retain(|(r, _)| *r != rid);
    if !completed {
        return Err(LailaError::connection_error("Peer handshake timed out."));
    }
    let reply = slot.state.lock().take().unwrap_or(Arg::None);
    let msg = reply.get("msg")?;
    let (peer_id, caps) = carrier::_handshake_result(&msg)?;
    proto.0.carrier.set_peer_caps(&peer_id, caps);
    carrier::_register_peer(proto, &peer_id, PeerHandle::Addr(addr));
    Ok(peer_id)
}

fn addr_of(proto: &Protocol, peer_id: &str) -> Result<String> {
    match proto.0.carrier.connection(peer_id) {
        Some(PeerHandle::Addr(a)) => Ok(a),
        _ => Err(LailaError::connection_error(alloc::format!(
            "No connection to peer {peer_id}"
        ))),
    }
}

/// `_send_once(peer_id, path, args, kwargs)`
pub fn _send_once(
    proto: &Protocol,
    peer_id: &str,
    path: &[String],
    args: &[Arg],
    kwargs: &Arg,
) -> Result<Arg> {
    let addr = addr_of(proto, peer_id)?;
    let state = state_of(proto)?;
    let (request_id, slot) = proto.0.carrier.register_pending(Some(peer_id));
    let req = carrier::_make_rpc_request(path, args, kwargs, &request_id);
    let b = _codec::encode(&req, &proto.codec())?;
    if let Err(e) = send_message(proto, &state, &addr, &b, Some(&request_id)) {
        proto.0.carrier.pop_pending(&request_id);
        return Err(e);
    }
    carrier::_await_pending(proto, &request_id, &slot)
}

/// `_send_oneway(peer_id, path, kwargs)`
pub fn _send_oneway(proto: &Protocol, peer_id: &str, path: &[String], kwargs: &Arg) -> Result<()> {
    let addr = addr_of(proto, peer_id)?;
    let state = state_of(proto)?;
    let rid = crate::python::stdlib::uuid.uuid4().__str__();
    let req = carrier::_make_rpc_request(path, &[], kwargs, &rid);
    let b = _codec::encode(&req, &proto.codec())?;
    send_message(proto, &state, &addr, &b, None)
}

/// `_send_goodbye(peer_id)`
pub fn _send_goodbye(proto: &Protocol, peer_id: &str) -> Result<()> {
    let Ok(addr) = addr_of(proto, peer_id) else {
        return Ok(());
    };
    let Ok(state) = state_of(proto) else {
        return Ok(());
    };
    let b = _codec::encode(&carrier::_goodbye_message(proto), &proto.codec())?;
    send_message(proto, &state, &addr, &b, None)
}

/// `_stream_enqueue(channel, payload)`: one datagram per lane message.
pub fn _stream_enqueue(proto: &Protocol, channel: &Channel, payload: &[u8]) -> Result<()> {
    let addr = addr_of(proto, channel.peer_id())?;
    let state = state_of(proto)?;
    let limit = carrier::_max_message_bytes(proto).max(0) as usize;
    if payload.len() > limit {
        return Err(LailaError::value_error(alloc::format!(
            "Stream message of {} bytes does not fit one datagram (mtu={} - {} header = {limit}). Datagram lanes do not fragment.",
            payload.len(),
            proto.i64_field("mtu", 1200),
            _codec::STREAM_HEADER_LEN
        )));
    }
    let seq = channel._next_chunk_seqs(1);
    let packet = _codec::pack_stream_frame(
        channel.tx_lane_id().unwrap_or(0),
        seq,
        _codec::FLAG_START | _codec::FLAG_END,
        payload,
    );
    let _ = sendto(&state, &addr, &packet);
    Ok(())
}
