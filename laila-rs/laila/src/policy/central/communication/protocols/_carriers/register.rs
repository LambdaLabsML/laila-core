//! `_RegisterRPCProtocol` -- JSON-RPC over a register / mailbox bus with
//! no asynchronous push (Modbus, I2C, SPI, 1-Wire, EtherNet/IP).
//!
//! A message is exchanged by writing it into a *mailbox* region on the
//! peer and polling our own mailbox for replies. The carrier provides the
//! reliable JSON-RPC layer (handshake, correlation, off-thread dispatch)
//! and the polling loop; a concrete transport supplies `_deliver(data)`
//! and `_poll_inbound()`. These buses are point-to-point, so addressing
//! is implicit; the single peer is still tracked by `global_id`.
//!
//! Rust shape: the bus is a [`RegisterBus`] -- the real transports need a
//! host-side driver the Rust build does not ship (`start()` raises the
//! `laila-core[<extra>]` install hint); a harness installs an in-process
//! mailbox pair with [`set_bus`].

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicBool, Ordering};

use crate::python::sync::{wait_until, Condvar, Mutex};
use crate::python::{Arg, LailaError, Result};

use super::super::super::protocol as rpc_protocol;
use super::super::base::{CarrierKind, Protocol};
use super::base::{self as carrier, PeerHandle};
use super::codec as _codec;

/// `log = logging.getLogger(__name__)` -- the module logger.
pub fn log() -> crate::python::stdlib::logging::Logger {
    crate::python::stdlib::logging::Logger::get(
        "laila.policy.central.communication.protocols._carriers.register",
    )
}

/// `_RegisterRPCProtocol` as a class marker.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct _RegisterRPCProtocol;
impl crate::python::builtins::IsInstance for _RegisterRPCProtocol {
    fn isinstance(x: &Arg) -> bool {
        Protocol::of(x).is_some_and(|p| p.spec().carrier == CarrierKind::Register)
    }
}

/// The bus plumbing a concrete transport supplies (`_open_bus`,
/// `_deliver`, `_poll_inbound`, `_close_bus`).
pub trait RegisterBus: Send + Sync {
    fn open(&self) -> Result<()> {
        Ok(())
    }
    /// Write one frame into the peer's inbox region.
    fn deliver(&self, data: &[u8]) -> Result<()>;
    /// The next frame from our inbox region, `None` when nothing is pending.
    fn poll_inbound(&self) -> Result<Option<Vec<u8>>>;
    fn close(&self) {}
}

pub struct HandshakeSlot {
    msg: Mutex<Option<Arg>>,
    cv: Condvar,
}

/// The bus + the poller's stop flag + `_handshake_pending`.
pub struct RegisterState {
    pub bus: Arc<dyn RegisterBus>,
    pub stop: Arc<AtomicBool>,
    handshake_pending: Mutex<Vec<(String, Arc<HandshakeSlot>)>>,
}

/// Install the mailbox bus (before `start()`).
pub fn set_bus(proto: &Protocol, bus: Arc<dyn RegisterBus>) {
    proto.0.transport.lock().register_bus = Some(bus);
}

fn state_of(proto: &Protocol) -> Result<Arc<RegisterState>> {
    proto.0.transport.lock().register.clone().ok_or_else(|| {
        LailaError::connection_error(alloc::format!(
            "{} transport is shut down.",
            crate::python::pyrepr::str_repr(proto.protocol_name())
        ))
    })
}

/// `start()`: open the bus and start the polling loop (idempotent).
pub fn start(proto: &Protocol) -> Result<()> {
    if !carrier::_begin_start(proto) {
        return Ok(());
    }
    let bus = match proto.0.transport.lock().register_bus.clone() {
        Some(b) => b,
        None => {
            let mods: Vec<String> = proto
                .spec()
                .driver_modules
                .iter()
                .map(|m| String::from(*m))
                .collect();
            carrier::_require_drivers(proto, &mods, proto.spec().driver_extra)?;
            return Err(LailaError::runtime_error(if proto.spec().note.is_empty() {
                alloc::format!(
                    "The {} transport requires a configured bus device.",
                    crate::python::pyrepr::str_repr(proto.protocol_name())
                )
            } else {
                String::from(proto.spec().note)
            }));
        }
    };
    bus.open()?;
    let stop = Arc::new(AtomicBool::new(false));
    let state = Arc::new(RegisterState {
        bus,
        stop: stop.clone(),
        handshake_pending: Mutex::new(Vec::new()),
    });
    {
        let mut t = proto.0.transport.lock();
        t.stop = Some(stop);
        t.register = Some(state.clone());
    }
    let p = proto.clone();
    super::transport::spawn_loop(
        proto,
        alloc::format!("{}-poll", proto.class_name()),
        move || poll_loop(p, state),
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
        let s = t.register.take();
        t.clear();
        s
    };
    if let Some(s) = &state {
        s.bus.close();
    }
    super::transport::join_loops(proto);
    if let Some(s) = state {
        s.handshake_pending.lock().clear();
    }
    carrier::_reset_after_stop(proto);
    Ok(())
}

/// `_poll_loop()`
fn poll_loop(proto: Protocol, state: Arc<RegisterState>) {
    loop {
        if state.stop.load(Ordering::Acquire) {
            break;
        }
        match state.bus.poll_inbound() {
            Ok(Some(data)) if !data.is_empty() => _feed_message(&proto, &state, &data),
            Ok(_) => {
                let interval = proto.f64_field("poll_interval", 0.01).max(0.0005);
                std::thread::sleep(core::time::Duration::from_secs_f64(interval));
            }
            Err(_) => break,
        }
    }
}

/// `_deliver_async(obj)`
fn deliver(proto: &Protocol, state: &RegisterState, obj: &Arg) -> Result<()> {
    let data = _codec::encode(obj, &proto.codec())?;
    state.bus.deliver(&data)
}

/// `_feed_message(data)`
pub fn _feed_message(proto: &Protocol, state: &Arc<RegisterState>, data: &[u8]) {
    let Ok(msg) = _codec::decode(data, &proto.codec()) else {
        return;
    };
    if rpc_protocol::is_request(&msg) {
        let method = rpc_protocol::msg_get(&msg, "method").__str__();
        if method == carrier::_PEER_CONNECT {
            handle_handshake(proto, state, &msg);
        } else if carrier::_is_goodbye(&msg) {
            let params = rpc_protocol::msg_get(&msg, "params");
            let from = if params.is_none() {
                Arg::None
            } else {
                params.get("from_id").unwrap_or(Arg::None)
            };
            if !from.is_none() {
                carrier::_on_peer_goodbye(proto, &from.__str__());
            }
        } else {
            let p = proto.clone();
            let st = state.clone();
            let reply: carrier::Reply = Arc::new(move |resp: Arg| {
                let _ = deliver(&p, &st, &resp);
            });
            let peer = proto.0.carrier.peer_ids().into_iter().next();
            carrier::_handle_request_frame(proto, msg, reply, peer.as_deref());
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
                *slot.msg.lock() = Some(msg);
                slot.cv.notify_all();
            }
            None => proto.0.carrier.complete_pending(&msg),
        }
    }
}

/// `_handle_handshake(msg)`
fn handle_handshake(proto: &Protocol, state: &Arc<RegisterState>, msg: &Arg) {
    match carrier::_handshake_accept(proto, msg) {
        Err(resp) => {
            let _ = deliver(proto, state, &resp);
        }
        Ok((peer_id, caps, resp)) => {
            proto.0.carrier.set_peer_caps(&peer_id, caps);
            carrier::_register_peer(proto, &peer_id, PeerHandle::Virtual(Arg::Bool(true)));
            let _ = deliver(proto, state, &resp);
        }
    }
}

/// `connect(uri, secret)`: the handshake over the bus (one peer per link).
pub fn connect(proto: &Protocol, _uri: &str, secret: &str) -> Result<String> {
    proto.start(crate::python::Args::new())?;
    let state = state_of(proto)?;
    let req = carrier::_handshake_request(proto, secret);
    let rid = rpc_protocol::msg_get(&req, "id").__str__();
    let slot = Arc::new(HandshakeSlot {
        msg: Mutex::new(None),
        cv: Condvar::new(),
    });
    state
        .handshake_pending
        .lock()
        .push((rid.clone(), slot.clone()));
    deliver(proto, &state, &req)?;
    let completed = {
        let mut g = slot.msg.lock();
        wait_until(&slot.cv, &mut g, Some(proto.handshake_timeout()), |m| {
            m.is_some()
        })
    };
    state.handshake_pending.lock().retain(|(r, _)| *r != rid);
    if !completed {
        return Err(LailaError::connection_error("Peer handshake timed out."));
    }
    let reply = slot.msg.lock().take().unwrap_or(Arg::None);
    let (peer_id, caps) = carrier::_handshake_result(&reply)?;
    proto.0.carrier.set_peer_caps(&peer_id, caps);
    carrier::_register_peer(proto, &peer_id, PeerHandle::Virtual(Arg::Bool(true)));
    Ok(peer_id)
}

fn require_peer(proto: &Protocol, peer_id: &str) -> Result<()> {
    if proto.0.carrier.has_peer(peer_id) {
        Ok(())
    } else {
        Err(LailaError::connection_error(alloc::format!(
            "No connection to peer {peer_id}"
        )))
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
    require_peer(proto, peer_id)?;
    let state = state_of(proto)?;
    let (request_id, slot) = proto.0.carrier.register_pending(Some(peer_id));
    let req = carrier::_make_rpc_request(path, args, kwargs, &request_id);
    if let Err(e) = deliver(proto, &state, &req) {
        proto.0.carrier.pop_pending(&request_id);
        return Err(e);
    }
    carrier::_await_pending(proto, &request_id, &slot)
}

/// `_send_oneway(peer_id, path, kwargs)`
pub fn _send_oneway(proto: &Protocol, peer_id: &str, path: &[String], kwargs: &Arg) -> Result<()> {
    require_peer(proto, peer_id)?;
    let state = state_of(proto)?;
    let rid = crate::python::stdlib::uuid.uuid4().__str__();
    deliver(
        proto,
        &state,
        &carrier::_make_rpc_request(path, &[], kwargs, &rid),
    )
}

/// `_send_goodbye(peer_id)`
pub fn _send_goodbye(proto: &Protocol, peer_id: &str) -> Result<()> {
    if !proto.0.carrier.has_peer(peer_id) {
        return Ok(());
    }
    let Ok(state) = state_of(proto) else {
        return Ok(());
    };
    deliver(proto, &state, &carrier::_goodbye_message(proto))
}
