//! `_BrokerRPCProtocol` -- JSON-RPC over a message broker / pub-sub fabric
//! (MQTT, AMQP, XMPP, DDS, ZeroMQ).
//!
//! There is no point-to-point socket: every endpoint subscribes to its own
//! *inbox* topic `laila/inbox/<policy_global_id>` and addresses a peer by
//! publishing to the peer's inbox. A request carries `reply_to` (the
//! sender's inbox); request / response correlate by the JSON-RPC `id`
//! through the inherited pending table. `peer.connect` is published to
//! the target inbox; the acceptor records `peer_id -> reply_to`.
//!
//! Rust shape: the broker client is a [`BrokerBus`] -- the real transports
//! need a host-side driver the Rust build does not ship (`start()` raises
//! the `laila-core[<extra>]` install hint); a harness installs an
//! in-process bus with [`set_bus`].

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::python::sync::{wait_until, Condvar, Mutex};
use crate::python::{Arg, LailaError, Result};

use super::super::super::protocol as rpc_protocol;
use super::super::base::{CarrierKind, Protocol};
use super::base::{self as carrier, PeerHandle};
use super::codec as _codec;

/// `log = logging.getLogger(__name__)` -- the module logger.
pub fn log() -> crate::python::stdlib::logging::Logger {
    crate::python::stdlib::logging::Logger::get(
        "laila.policy.central.communication.protocols._carriers.broker",
    )
}

/// `_BrokerRPCProtocol` as a class marker.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct _BrokerRPCProtocol;
impl crate::python::builtins::IsInstance for _BrokerRPCProtocol {
    fn isinstance(x: &Arg) -> bool {
        Protocol::of(x).is_some_and(|p| p.spec().carrier == CarrierKind::Broker)
    }
}

/// Inbound payloads of a subscription land here (`_feed_message`).
pub type Sink = Arc<dyn Fn(Vec<u8>) + Send + Sync>;

/// The broker plumbing a concrete transport supplies (`_broker_connect`,
/// `_broker_subscribe`, `_broker_publish`, `_broker_close`).
pub trait BrokerBus: Send + Sync {
    fn connect(&self) -> Result<()> {
        Ok(())
    }
    fn subscribe(&self, topic: &str, sink: Sink) -> Result<()>;
    fn publish(&self, topic: &str, data: &[u8]) -> Result<()>;
    fn close(&self) {}
}

pub struct HandshakeSlot {
    msg: Mutex<Option<Arg>>,
    cv: Condvar,
}

/// `_inbox` + the bus + `_handshake_pending`.
pub struct BrokerState {
    pub inbox: String,
    pub bus: Arc<dyn BrokerBus>,
    handshake_pending: Mutex<Vec<(String, Arc<HandshakeSlot>)>>,
}

/// Install the pub/sub bus (before `start()`).
pub fn set_bus(proto: &Protocol, bus: Arc<dyn BrokerBus>) {
    proto.0.transport.lock().broker_bus = Some(bus);
}

/// `_inbox_topic()`
pub fn _inbox_topic(proto: &Protocol) -> String {
    alloc::format!(
        "laila/inbox/{}",
        proto.policy_id().unwrap_or_else(|| String::from("None"))
    )
}

/// `_peer_inbox(peer_policy_id)`
pub fn _peer_inbox(peer_policy_id: &str) -> String {
    alloc::format!("laila/inbox/{peer_policy_id}")
}

fn state_of(proto: &Protocol) -> Result<Arc<BrokerState>> {
    proto.0.transport.lock().broker.clone().ok_or_else(|| {
        LailaError::connection_error(alloc::format!(
            "{} transport is shut down.",
            crate::python::pyrepr::str_repr(proto.protocol_name())
        ))
    })
}

/// `start()`: connect to the broker and subscribe to the inbox.
pub fn start(proto: &Protocol) -> Result<()> {
    if !carrier::_begin_start(proto) {
        return Ok(());
    }
    let bus = match proto.0.transport.lock().broker_bus.clone() {
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
                    "The {} transport requires a configured broker client.",
                    crate::python::pyrepr::str_repr(proto.protocol_name())
                )
            } else {
                String::from(proto.spec().note)
            }));
        }
    };
    bus.connect()?;
    let state = Arc::new(BrokerState {
        inbox: _inbox_topic(proto),
        bus: bus.clone(),
        handshake_pending: Mutex::new(Vec::new()),
    });
    let p = proto.clone();
    let st = state.clone();
    bus.subscribe(
        &state.inbox,
        Arc::new(move |data: Vec<u8>| _feed_message(&p, &st, &data)),
    )?;
    proto.0.transport.lock().broker = Some(state);
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
        let s = t.broker.take();
        t.clear();
        s
    };
    if let Some(s) = state {
        s.bus.close();
        s.handshake_pending.lock().clear();
    }
    carrier::_reset_after_stop(proto);
    Ok(())
}

/// `_publish_async(topic, obj)`
fn publish(proto: &Protocol, state: &BrokerState, topic: &str, obj: &Arg) -> Result<()> {
    let data = _codec::encode(obj, &proto.codec())?;
    state.bus.publish(topic, &data)
}

/// `_feed_message(data)`: decode a frame and route it.
pub fn _feed_message(proto: &Protocol, state: &Arc<BrokerState>, data: &[u8]) {
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
            let reply_to = rpc_protocol::msg_get(&msg, "reply_to");
            let p = proto.clone();
            let st = state.clone();
            let reply: carrier::Reply = Arc::new(move |resp: Arg| {
                if !reply_to.is_none() {
                    let _ = publish(&p, &st, &reply_to.__str__(), &resp);
                }
            });
            let peer = peer_for_reply_to(proto, &rpc_protocol::msg_get(&msg, "reply_to"));
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

fn peer_for_reply_to(proto: &Protocol, reply_to: &Arg) -> Option<String> {
    if reply_to.is_none() {
        return None;
    }
    let inbox = reply_to.__str__();
    proto
        .0
        .carrier
        .connections()
        .into_iter()
        .find(|(_, h)| matches!(h, PeerHandle::Addr(a) if *a == inbox))
        .map(|(p, _)| p)
}

/// `_handle_handshake(msg)`
fn handle_handshake(proto: &Protocol, state: &Arc<BrokerState>, msg: &Arg) {
    let params = rpc_protocol::msg_get(msg, "params");
    let reply_to = if params.is_none() {
        Arg::None
    } else {
        params.get("reply_to").unwrap_or(Arg::None)
    };
    if reply_to.is_none() {
        return;
    }
    let reply_to = reply_to.__str__();
    match carrier::_handshake_accept(proto, msg) {
        Err(resp) => {
            let _ = publish(proto, state, &reply_to, &resp);
        }
        Ok((peer_id, caps, resp)) => {
            proto.0.carrier.set_peer_caps(&peer_id, caps);
            carrier::_register_peer(proto, &peer_id, PeerHandle::Addr(reply_to.clone()));
            let _ = publish(proto, state, &reply_to, &resp);
        }
    }
}

/// `connect(uri, secret)`: handshake with the peer whose policy id is the
/// URI authority.
pub fn connect(proto: &Protocol, uri: &str, secret: &str) -> Result<String> {
    proto.start(crate::python::Args::new())?;
    let state = state_of(proto)?;
    let peer_inbox = _peer_inbox(&super::uri::uri_authority(uri));
    let req = carrier::_handshake_request(proto, secret);
    let params = rpc_protocol::msg_get(&req, "params");
    params.__setitem__("reply_to", Arg::Str(state.inbox.as_str().into()))?;
    req.__setitem__("params", params)?;
    let rid = rpc_protocol::msg_get(&req, "id").__str__();
    let slot = Arc::new(HandshakeSlot {
        msg: Mutex::new(None),
        cv: Condvar::new(),
    });
    state
        .handshake_pending
        .lock()
        .push((rid.clone(), slot.clone()));
    publish(proto, &state, &peer_inbox, &req)?;
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
    carrier::_register_peer(proto, &peer_id, PeerHandle::Addr(peer_inbox));
    Ok(peer_id)
}

fn inbox_of(proto: &Protocol, peer_id: &str) -> Result<String> {
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
    let peer_inbox = inbox_of(proto, peer_id)?;
    let state = state_of(proto)?;
    let (request_id, slot) = proto.0.carrier.register_pending(Some(peer_id));
    let req = carrier::_make_rpc_request(path, args, kwargs, &request_id);
    let _ = req.__setitem__("reply_to", Arg::Str(state.inbox.as_str().into()));
    if let Err(e) = publish(proto, &state, &peer_inbox, &req) {
        proto.0.carrier.pop_pending(&request_id);
        return Err(e);
    }
    carrier::_await_pending(proto, &request_id, &slot)
}

/// `_send_oneway(peer_id, path, kwargs)`
pub fn _send_oneway(proto: &Protocol, peer_id: &str, path: &[String], kwargs: &Arg) -> Result<()> {
    let peer_inbox = inbox_of(proto, peer_id)?;
    let state = state_of(proto)?;
    let rid = crate::python::stdlib::uuid.uuid4().__str__();
    let req = carrier::_make_rpc_request(path, &[], kwargs, &rid);
    let _ = req.__setitem__("reply_to", Arg::Str(state.inbox.as_str().into()));
    publish(proto, &state, &peer_inbox, &req)
}

/// `_send_goodbye(peer_id)`
pub fn _send_goodbye(proto: &Protocol, peer_id: &str) -> Result<()> {
    let Ok(peer_inbox) = inbox_of(proto, peer_id) else {
        return Ok(());
    };
    let Ok(state) = state_of(proto) else {
        return Ok(());
    };
    publish(
        proto,
        &state,
        &peer_inbox,
        &carrier::_goodbye_message(proto),
    )
}
