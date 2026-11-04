//! `loopback` -- in-process loopback communication transport.
//!
//! The degenerate transport: both policies live in the *same* process, so
//! there is no socket at all -- an RPC is dispatched by directly invoking
//! the target policy's inbound handler (`_build_response`). Future-shaped
//! results are still virtualised into `RemoteFuture` proxies exactly as
//! over a real wire, which makes loopback a faithful, dependency-free way
//! to exercise the full inter-policy path in one process.
//!
//! - `protocol_name` `"loopback"` (aliases `local` / `inproc`)
//! - URI scheme `loopback://<policy_global_id>`
//!
//! Peers are matched through a process-wide registry keyed by the owning
//! policy's `global_id`. Stream lanes hand the payload straight to the
//! peer carrier's inbound lane (zero copy).
//!
//! Python: `laila.policy.central.communication.protocols.local.loopback`.

use alloc::string::String;
use alloc::sync::{Arc, Weak};
use alloc::vec::Vec;

use crate::python::sync::Mutex;
use crate::python::{Arg, LailaError, Result};

use super::super::super::channel::Channel;
use super::super::super::protocol as rpc_protocol;
use super::super::_carriers::base::{self as carrier, PeerHandle};
use super::super::base::{CarrierKind, Protocol, ProtocolInner, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.local.loopback",
    protocol_name: "loopback",
    tokens: &["inproc", "local", "loopback"],
    schemes: &["loopback://"],
    carrier: CarrierKind::Loopback,
    wire: Wire::Loopback,
    supports_channels: true,
    bases: &["_CarrierRPCProtocol", "_LAILA_IDENTIFIABLE_COMM_PROTOCOL"],
    tier: "works",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL, SPEC);

/// `_LOOPBACK_REGISTRY`: policy global_id -> loopback protocol instance.
static _LOOPBACK_REGISTRY: Mutex<Vec<(String, Weak<ProtocolInner>)>> = Mutex::new(Vec::new());

fn registry_get(pid: &str) -> Option<Protocol> {
    _LOOPBACK_REGISTRY
        .lock()
        .iter()
        .find(|(k, _)| k == pid)
        .and_then(|(_, w)| Protocol::from_inner(w))
}

fn registry_put(pid: &str, proto: &Protocol) {
    let mut g = _LOOPBACK_REGISTRY.lock();
    g.retain(|(_, w)| w.strong_count() > 0);
    match g.iter_mut().find(|(k, _)| k == pid) {
        Some(slot) => slot.1 = proto.0.me.clone(),
        None => g.push((String::from(pid), proto.0.me.clone())),
    }
}

fn registry_pop(pid: &str) {
    _LOOPBACK_REGISTRY.lock().retain(|(k, _)| k != pid);
}

/// `_LOOPBACK_REGISTRY` as a dict snapshot (`{gid: protocol}`).
pub fn _loopback_registry() -> Arg {
    let mut d = crate::python::PyDictBuilder::new();
    for (k, w) in _LOOPBACK_REGISTRY.lock().iter() {
        if let Some(p) = Protocol::from_inner(w) {
            d.insert(k.as_str(), crate::python::IntoArg::into_arg(p));
        }
    }
    d.finish()
}

/// `start()`: register this endpoint under its policy's `global_id`.
pub fn start(proto: &Protocol) -> Result<()> {
    if !carrier::_begin_start(proto) {
        return Ok(());
    }
    if let Some(pid) = proto.policy_id() {
        registry_put(&pid, proto);
    }
    proto.0.carrier.set_started(true);
    Ok(())
}

/// `stop()`: deregister, tell every peer we are gone, drop them.
pub fn stop(proto: &Protocol) -> Result<()> {
    if !proto.0.carrier.started() {
        return Ok(());
    }
    if let Some(pid) = proto.policy_id() {
        registry_pop(&pid);
    }
    for peer_id in proto.0.carrier.peer_ids() {
        disconnect(proto, &peer_id)?;
    }
    carrier::_shutdown_lanes(proto, "protocol stopped");
    proto.0.carrier.clear_pending();
    proto.0.carrier.set_started(false);
    Ok(())
}

fn target_of(proto: &Protocol, peer_id: &str) -> Option<Protocol> {
    match proto.0.carrier.connection(peer_id) {
        Some(PeerHandle::Loopback(w)) => Protocol::from_inner(&w),
        _ => None,
    }
}

/// `disconnect(peer_id)`: drop the peer on both ends (idempotent). The
/// goodbye is a direct call into the peer carrier.
pub fn disconnect(proto: &Protocol, peer_id: &str) -> Result<()> {
    let Some(target) = target_of(proto, peer_id) else {
        return Ok(());
    };
    carrier::_unregister_peer(proto, peer_id);
    if let Some(my_pid) = proto.policy_id() {
        carrier::_on_peer_goodbye(&target, &my_pid);
    }
    Ok(())
}

/// `connect(uri, secret)`: peer with another in-process policy.
pub fn connect(proto: &Protocol, uri: &str, secret: &str) -> Result<String> {
    proto.start(crate::python::Args::new())?;
    let target_pid = super::super::_carriers::uri::uri_authority(uri);
    let Some(target) = registry_get(&target_pid) else {
        return Err(LailaError::connection_error(alloc::format!(
            "No in-process loopback endpoint for policy {}. Start the target policy's loopback connection first.",
            crate::python::pyrepr::str_repr(&target_pid)
        )));
    };
    if secret != target.peer_secret_key() {
        return Err(LailaError::connection_error("Invalid peer secret key."));
    }
    // Register both directions so either side can call the other.
    proto
        .0
        .carrier
        .set_peer_caps(&target_pid, carrier::_local_caps(&target));
    carrier::_register_peer(
        proto,
        &target_pid,
        PeerHandle::Loopback(target.0.me.clone()),
    );
    if let Some(my_pid) = proto.policy_id() {
        target
            .0
            .carrier
            .set_peer_caps(&my_pid, carrier::_local_caps(proto));
        carrier::_register_peer(&target, &my_pid, PeerHandle::Loopback(proto.0.me.clone()));
    }
    Ok(target_pid)
}

/// `send_rpc(peer_id, path, args, kwargs)`: dispatch directly into the
/// peer policy (zero-copy; no codec, no socket).
pub fn send_rpc(
    proto: &Protocol,
    peer_id: &str,
    path: Vec<String>,
    args: Vec<Arg>,
    kwargs: Arg,
) -> Result<Arg> {
    let Some(target) = target_of(proto, peer_id) else {
        return Err(LailaError::connection_error(alloc::format!(
            "No connection to peer {peer_id}"
        )));
    };
    let rid = crate::python::stdlib::uuid.uuid4().__str__();
    let req = carrier::_make_rpc_request(&path, &args, &kwargs, &rid);
    // Run on the TARGET so its _execute_rpc walks the target policy; our
    // own policy id identifies us as the authenticated caller.
    let my_pid = proto.policy_id();
    let resp = carrier::_build_response(&target, &req, my_pid.as_deref());
    if resp.__contains__("error")? {
        let err = rpc_protocol::msg_get(&resp, "error");
        let message = rpc_protocol::error_message(&err);
        if err.get("code").unwrap_or(Arg::None).as_i64() == Some(rpc_protocol::ERR_BUSY) {
            return Err(LailaError::backpressure_error(message));
        }
        return Err(LailaError::runtime_error(alloc::format!(
            "Remote RPC error: {message}"
        )));
    }
    Ok(rpc_protocol::msg_get(&resp, "result"))
}

/// `_send_oneway(peer_id, path, kwargs)`: same direct dispatch, result ignored.
pub fn _send_oneway(proto: &Protocol, peer_id: &str, path: &[String], kwargs: &Arg) -> Result<()> {
    let Some(target) = target_of(proto, peer_id) else {
        return Ok(());
    };
    let rid = crate::python::stdlib::uuid.uuid4().__str__();
    let req = carrier::_make_rpc_request(path, &[], kwargs, &rid);
    let my_pid = proto.policy_id();
    let _ = carrier::_build_response(&target, &req, my_pid.as_deref());
    Ok(())
}

/// `_stream_enqueue(channel, payload)`: deliver straight into the peer's
/// matching inbound lane.
pub fn _stream_enqueue(proto: &Protocol, channel: &Channel, payload: &[u8]) -> Result<()> {
    let Some(target) = target_of(proto, channel.peer_id()) else {
        return Err(LailaError::connection_error(alloc::format!(
            "No connection to peer {}",
            channel.peer_id()
        )));
    };
    let Some(my_pid) = proto.policy_id() else {
        return Err(LailaError::connection_error(
            "Loopback carrier has no owning policy id.",
        ));
    };
    let Some(lane) = channel.tx_lane_id() else {
        return Err(LailaError::connection_error(
            "Channel has no transmit lane.",
        ));
    };
    carrier::_on_stream_payload(&target, &my_pid, lane, payload);
    Ok(())
}

/// `connect_loopback(policy_global_id, secret)`
pub fn connect_loopback(proto: &Protocol, policy_global_id: &str, secret: &str) -> Result<String> {
    connect(
        proto,
        &alloc::format!("loopback://{policy_global_id}"),
        secret,
    )
}

#[allow(dead_code)]
fn _assert_handles(_: Arc<ProtocolInner>) {}
