//! `tcpip` -- TCP/IP (WebSocket) communication protocol implementation.
//!
//! Concrete `_LAILA_IDENTIFIABLE_COMM_PROTOCOL` exposing a long-lived
//! bidirectional WebSocket channel between two policies (`ws://` /
//! `wss://`). The on-the-wire RPC envelope is `protocol`'s JSON-RPC, the
//! socket plumbing lives in `connection`.
//!
//! The protocol owns a listener (`_server`), the live sockets per peer
//! (`_connections`, keyed by remote-policy `global_id`) and the
//! pending-RPC table (`_pending_rpcs`): each outbound `send_rpc`
//! registers a slot, blocks on it, and is woken by the inbound dispatcher
//! when the matching response arrives.
//!
//! Python: `laila.policy.central.communication.protocols.tcpip`.

use alloc::string::String;

use crate::python::{Arg, IntoArg, LailaError, Result};

use super::super::connection;
use super::super::protocol as rpc_protocol;
use super::_carriers::base::{self as carrier, PeerHandle};
use super::base::{CarrierKind, Protocol, ProtocolSpec, Wire, SPEC_DEFAULTS};

pub use super::super::connection::ws_frame;

/// `log = logging.getLogger(__name__)` -- the module logger.
pub fn log() -> crate::python::stdlib::logging::Logger {
    crate::python::stdlib::logging::Logger::get(
        "laila.policy.central.communication.protocols.tcpip",
    )
}

/// `_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.tcpip",
    protocol_name: "tcpip",
    tokens: &["tcpip", "websocket", "ws", "wss"],
    schemes: &["ws://", "wss://"],
    carrier: CarrierKind::WebSocket,
    wire: Wire::WebSocket,
    supports_channels: false,
    bases: &["_LAILA_IDENTIFIABLE_COMM_PROTOCOL"],
    tier: "works",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL, SPEC);

/// `start()`: bind the WebSocket server (idempotent).
pub fn start(proto: &Protocol) -> Result<()> {
    if proto.0.carrier.started() {
        return Ok(());
    }
    connection::start_server(proto)?;
    proto.0.carrier.set_started(true);
    let _ = log().info(
        "TCP/IP protocol started for policy %s on port %s",
        alloc::vec![proto.policy_id().into_arg(), proto.bound_port().into_arg()],
    );
    Ok(())
}

/// `stop()`: close every peer socket, the server, and fail pending RPCs.
pub fn stop(proto: &Protocol) -> Result<()> {
    if !proto.0.carrier.started() {
        return Ok(());
    }
    for (peer_id, handle) in proto.0.carrier.connections() {
        if let PeerHandle::Link(l) = handle {
            connection::close_link(&l);
        }
        _unregister_peer(proto, &peer_id);
    }
    {
        let mut t = proto.0.transport.lock();
        t.raise_stop();
    }
    connection::wake_listener(proto);
    proto.0.transport.lock().clear();
    proto.0.carrier.fail_pending("transport stopped", None);
    proto.0.carrier.clear_pending();
    proto.0.carrier.set_started(false);
    let _ = log().info(
        "TCP/IP protocol stopped for policy %s",
        alloc::vec![proto.policy_id().into_arg()],
    );
    Ok(())
}

/// `connect(uri, secret)`: outbound WebSocket + `peer.connect` handshake.
pub fn connect(proto: &Protocol, uri: &str, secret: &str) -> Result<String> {
    proto.start(crate::python::Args::new())?;
    connection::connect_outbound(proto, uri, secret)
}

/// `_unregister_peer(peer_id)`: drop the socket, fail its RPCs, notify the hub.
pub fn _unregister_peer(proto: &Protocol, peer_id: &str) {
    carrier::_unregister_peer(proto, peer_id);
}

/// `disconnect(peer_id)`: close the WebSocket (the graceful goodbye) and drop it.
pub fn disconnect(proto: &Protocol, peer_id: &str) -> Result<()> {
    let Some(handle) = proto.0.carrier.connection(peer_id) else {
        return Ok(());
    };
    if let PeerHandle::Link(l) = handle {
        connection::close_link(&l);
    }
    _unregister_peer(proto, peer_id);
    Ok(())
}

/// `_send_goodbye(peer_id)`: the close frame *is* the goodbye.
pub fn _send_goodbye(proto: &Protocol, peer_id: &str) -> Result<()> {
    if let Some(PeerHandle::Link(l)) = proto.0.carrier.connection(peer_id) {
        connection::close_link(&l);
    }
    Ok(())
}

/// `_send(peer_id, path, args, kwargs, timeout)`: one correlated call.
pub fn _send_once(
    proto: &Protocol,
    peer_id: &str,
    path: &[String],
    args: &[Arg],
    kwargs: &Arg,
) -> Result<Arg> {
    let link = match proto.0.carrier.connection(peer_id) {
        Some(PeerHandle::Link(l)) if !l.closed() => l,
        _ => {
            return Err(LailaError::connection_error(alloc::format!(
                "No connection to peer {peer_id}"
            )))
        }
    };
    let (request_id, slot) = proto.0.carrier.register_pending(Some(peer_id));
    let req = carrier::_make_rpc_request(path, args, kwargs, &request_id);
    let frame = match rpc_protocol::encode(&req) {
        Ok(f) => f,
        Err(e) => {
            proto.0.carrier.pop_pending(&request_id);
            return Err(e);
        }
    };
    let cap = connection::max_frame_bytes();
    if frame.len() > cap {
        // The peer would close the socket (1009) on receipt; refuse up
        // front so the link survives and the caller learns why.
        proto.0.carrier.pop_pending(&request_id);
        return Err(LailaError::connection_error(alloc::format!(
            "RPC frame of {} bytes exceeds the {cap} byte WebSocket frame limit for peer {peer_id}.",
            frame.len(),
        )));
    }
    link.put_rpc(link.framing().wrap_rpc(frame.as_bytes()));
    carrier::_await_pending(proto, &request_id, &slot)
}

/// `_send_oneway` is not needed: the WebSocket transport has no lanes.
pub fn _send_oneway(
    _proto: &Protocol,
    _peer_id: &str,
    _path: &[String],
    _kwargs: &Arg,
) -> Result<()> {
    Ok(())
}

/// `connect_tcpip(host, port, secret)` / `connect_ws(...)`
pub fn connect_ws(proto: &Protocol, host: &str, port: i64, secret: &str) -> Result<String> {
    connect(proto, &alloc::format!("ws://{host}:{port}"), secret)
}
