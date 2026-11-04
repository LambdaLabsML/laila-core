//! WebSocket connection management for the TCP/IP protocol
//! (`laila.policy.central.communication.connection`).
//!
//! Three phases, as in Python:
//!
//! 1. **Server bring-up** ([`start_server`]) -- bind a listener to
//!    `proto.host:proto.port`; every accepted socket runs
//!    [`_handle_inbound`].
//! 2. **Handshake** ([`_handle_inbound`] / [`connect_outbound`]) -- after
//!    the HTTP upgrade, the first frame must be a `peer.connect` JSON-RPC
//!    request carrying the originating policy's `global_id` and the shared
//!    secret. On success both sides register the peer and enter the
//!    receive loop.
//! 3. **Receive loop** ([`_receive_loop`]) -- decodes inbound frames and
//!    routes requests to the carrier dispatcher and responses to the
//!    pending table.
//!
//! Rust shape: RFC 6455 is implemented in-tree (the Python side uses the
//! `websockets` package): text frames carry the JSON envelope, clients
//! mask, servers do not, `ping` is answered with `pong`, `close` ends the
//! link. Frames are capped at [`MAX_FRAME_BYTES`] symmetrically.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicBool, Ordering};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};

use crate::python::{Arg, LailaError, Result};

use super::protocol;
use super::protocols::_carriers::base::{self as carrier, PeerHandle};
use super::protocols::_carriers::stream::{Framing, Link, StreamSock};
use super::protocols::base::Protocol;

/// `log = logging.getLogger(__name__)` -- the module logger.
pub fn log() -> crate::python::stdlib::logging::Logger {
    crate::python::stdlib::logging::Logger::get("laila.policy.central.communication.connection")
}

/// `_COMM_PING_PATH = ["__comm_ping__"]`
pub const _COMM_PING_PATH: &[&str] = &["__comm_ping__"];

/// Largest WebSocket frame accepted or sent on a peer link (64 MiB, the
/// same as the stream carriers' `max_stream_frame_bytes`).
pub const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

/// The module path `mock.patch.object(connection, "MAX_FRAME_BYTES", n)`
/// patches under.
pub const MODULE_PATH: &str = "laila.policy.central.communication.connection";

/// `connection.MAX_FRAME_BYTES` as read at run time: the patched value
/// while a `mock.patch.object(connection, "MAX_FRAME_BYTES", n)` is active,
/// [`MAX_FRAME_BYTES`] otherwise.
pub fn max_frame_bytes() -> usize {
    match crate::python::mock::patched(MODULE_PATH, "MAX_FRAME_BYTES") {
        Some(v) => v
            .as_i64()
            .map(|n| n.max(0) as usize)
            .unwrap_or(MAX_FRAME_BYTES),
        None => MAX_FRAME_BYTES,
    }
}

/// The `connection` module as a value (`patch.object(connection, ..)`).
#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)]
pub struct connection_mod;
impl crate::python::PyArg for connection_mod {
    fn to_arg(&self) -> crate::python::Arg {
        crate::python::mock::module_target(MODULE_PATH)
    }
}
impl crate::python::builtins::ModuleValue for &connection_mod {
    fn module_value(self) -> crate::python::Arg {
        crate::python::mock::module_target(MODULE_PATH)
    }
}

const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

fn io_err(prefix: &str, e: std::io::Error) -> LailaError {
    LailaError::connection_error(alloc::format!("{prefix}: {e}"))
}

// ---------------------------------------------------------------------------
// RFC 6455 framing

/// Build one WebSocket frame (`FIN` set) with *opcode* and *payload*;
/// masked with a random key when *mask* (clients mask, servers do not).
pub fn ws_frame(opcode: u8, payload: &[u8], mask: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 14);
    out.push(0x80 | (opcode & 0x0F));
    let mask_bit = if mask { 0x80 } else { 0 };
    let n = payload.len();
    if n < 126 {
        out.push(mask_bit | n as u8);
    } else if n <= 0xFFFF {
        out.push(mask_bit | 126);
        out.extend_from_slice(&(n as u16).to_be_bytes());
    } else {
        out.push(mask_bit | 127);
        out.extend_from_slice(&(n as u64).to_be_bytes());
    }
    if mask {
        let key = crate::_compat::random::urandom(4);
        out.extend_from_slice(&key);
        out.extend(payload.iter().enumerate().map(|(i, b)| b ^ key[i % 4]));
    } else {
        out.extend_from_slice(payload);
    }
    out
}

/// One parsed frame.
struct Frame {
    fin: bool,
    opcode: u8,
    payload: Vec<u8>,
}

fn read_exact(reader: &mut TcpStream, buf: &mut [u8]) -> std::io::Result<bool> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) => return Ok(false),
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}

/// Read one frame; `Ok(None)` at EOF, `Err` on a protocol violation / cap.
fn read_ws_frame(reader: &mut TcpStream) -> std::io::Result<Option<Frame>> {
    let mut head = [0u8; 2];
    if !read_exact(reader, &mut head)? {
        return Ok(None);
    }
    let fin = head[0] & 0x80 != 0;
    let opcode = head[0] & 0x0F;
    let masked = head[1] & 0x80 != 0;
    let mut len = (head[1] & 0x7F) as u64;
    if len == 126 {
        let mut b = [0u8; 2];
        if !read_exact(reader, &mut b)? {
            return Ok(None);
        }
        len = u16::from_be_bytes(b) as u64;
    } else if len == 127 {
        let mut b = [0u8; 8];
        if !read_exact(reader, &mut b)? {
            return Ok(None);
        }
        len = u64::from_be_bytes(b);
    }
    if len > MAX_FRAME_BYTES as u64 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "frame exceeds MAX_FRAME_BYTES (1009)",
        ));
    }
    let mut key = [0u8; 4];
    if masked && !read_exact(reader, &mut key)? {
        return Ok(None);
    }
    let mut payload = alloc::vec![0u8; len as usize];
    if len > 0 && !read_exact(reader, &mut payload)? {
        return Ok(None);
    }
    if masked {
        for (i, b) in payload.iter_mut().enumerate() {
            *b ^= key[i % 4];
        }
    }
    Ok(Some(Frame {
        fin,
        opcode,
        payload,
    }))
}

/// Read one complete *message* (reassembling continuation frames and
/// answering pings inline). `None` at EOF or after a close frame.
fn read_ws_message(reader: &mut TcpStream, link: &Link) -> Option<Vec<u8>> {
    let mut message: Vec<u8> = Vec::new();
    let mut in_message = false;
    loop {
        let frame = match read_ws_frame(reader) {
            Ok(Some(f)) => f,
            _ => return None,
        };
        match frame.opcode {
            0x8 => {
                // close: echo the close frame (best effort) and end
                let masked = matches!(link.framing(), Framing::WebSocket { mask: true });
                link.put_rpc(ws_frame(0x8, &frame.payload, masked));
                link.flush(0.5);
                return None;
            }
            0x9 => {
                let masked = matches!(link.framing(), Framing::WebSocket { mask: true });
                link.put_rpc(ws_frame(0xA, &frame.payload, masked));
            }
            0xA => {}
            0x0 => {
                if !in_message {
                    return None;
                }
                message.extend_from_slice(&frame.payload);
                if message.len() > MAX_FRAME_BYTES {
                    return None;
                }
                if frame.fin {
                    return Some(message);
                }
            }
            0x1 | 0x2 => {
                if frame.fin {
                    return Some(frame.payload);
                }
                in_message = true;
                message = frame.payload;
            }
            _ => return None,
        }
    }
}

// ---------------------------------------------------------------------------
// HTTP upgrade

fn accept_key(client_key: &str) -> String {
    use base64::Engine;
    use sha1::{Digest, Sha1};
    let mut h = Sha1::new();
    h.update(client_key.as_bytes());
    h.update(WS_GUID.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(h.finalize())
}

/// Read an HTTP/1.1 head (up to the blank line).
fn read_http_head(sock: &mut TcpStream) -> std::io::Result<Option<String>> {
    let mut buf: Vec<u8> = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        match sock.read(&mut byte) {
            Ok(0) => return Ok(None),
            Ok(_) => {
                buf.push(byte[0]);
                if buf.ends_with(b"\r\n\r\n") {
                    break;
                }
                if buf.len() > 16 * 1024 {
                    return Ok(None);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(Some(String::from_utf8_lossy(&buf).into_owned()))
}

fn header_value<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        if k.trim().eq_ignore_ascii_case(name) {
            Some(v.trim())
        } else {
            None
        }
    })
}

/// The server side of the upgrade: `101 Switching Protocols` or `false`.
fn server_upgrade(sock: &mut TcpStream) -> bool {
    let Ok(Some(head)) = read_http_head(sock) else {
        return false;
    };
    let is_get = head.lines().next().is_some_and(|l| l.starts_with("GET "));
    let key = header_value(&head, "Sec-WebSocket-Key");
    let upgrade_ok =
        header_value(&head, "Upgrade").is_some_and(|u| u.eq_ignore_ascii_case("websocket"));
    let (Some(key), true, true) = (key, is_get, upgrade_ok) else {
        let _ = sock.write_all(
            b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
        );
        return false;
    };
    let resp = alloc::format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {}\r\n\r\n",
        accept_key(key)
    );
    sock.write_all(resp.as_bytes()).is_ok()
}

/// The client side of the upgrade.
fn client_upgrade(sock: &mut TcpStream, host: &str, port: u16, path: &str) -> Result<()> {
    use base64::Engine;
    let key = base64::engine::general_purpose::STANDARD.encode(crate::_compat::random::urandom(16));
    let req = alloc::format!(
        "GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
    );
    sock.write_all(req.as_bytes())
        .map_err(|e| io_err("WebSocket upgrade failed", e))?;
    let head = read_http_head(sock)
        .map_err(|e| io_err("WebSocket upgrade failed", e))?
        .ok_or_else(|| {
            LailaError::connection_error("WebSocket upgrade failed: connection closed.")
        })?;
    let status_101 = head.lines().next().is_some_and(|l| l.contains(" 101"));
    let accept_ok = header_value(&head, "Sec-WebSocket-Accept") == Some(accept_key(&key).as_str());
    if !status_101 || !accept_ok {
        return Err(LailaError::connection_error(alloc::format!(
            "WebSocket upgrade rejected: {}",
            head.lines().next().unwrap_or("")
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// server

/// `start_server(proto)`: bind and start accepting on a thread.
pub fn start_server(proto: &Protocol) -> Result<()> {
    let host = proto.str_field("host");
    let host = if host.is_empty() {
        String::from("0.0.0.0")
    } else {
        host
    };
    let port = proto.i64_field("port", 0).clamp(0, 65535) as u16;
    let listener = TcpListener::bind((host.as_str(), port))
        .map_err(|e| io_err("could not bind WebSocket server", e))?;
    let bound = listener
        .local_addr()
        .map_err(|e| io_err("listener address", e))?
        .port();
    let stop = {
        let mut t = proto.0.transport.lock();
        t.bound_port = Some(bound);
        let connect_host = if host == "0.0.0.0" {
            "127.0.0.1"
        } else {
            host.as_str()
        };
        t.listener_addr = Some(alloc::format!("{connect_host}:{bound}"));
        t.fresh_stop()
    };
    let p = proto.clone();
    crate::python::sync::spawn_named(alloc::format!("{}-accept", proto.class_name()), move || {
        accept_loop(p, listener, stop)
    })?;
    let _ = log().info(
        "Communication server listening on %s:%s",
        alloc::vec![Arg::Str(host.into()), Arg::Int(i64::from(bound))],
    );
    Ok(())
}

fn accept_loop(proto: Protocol, listener: TcpListener, stop: Arc<AtomicBool>) {
    loop {
        if stop.load(Ordering::Acquire) {
            break;
        }
        match listener.accept() {
            Ok((sock, _)) => {
                if stop.load(Ordering::Acquire) {
                    break;
                }
                let p = proto.clone();
                let _ = crate::python::sync::spawn_named(
                    alloc::format!("{}-inbound", proto.class_name()),
                    move || _handle_inbound(p, sock),
                );
            }
            Err(_) => {
                if stop.load(Ordering::Acquire) {
                    break;
                }
                std::thread::sleep(core::time::Duration::from_millis(20));
            }
        }
    }
}

/// Poke the listener so a blocking `accept()` returns and sees the flag.
pub fn wake_listener(proto: &Protocol) {
    let addr = proto.0.transport.lock().listener_addr.clone();
    if let Some(addr) = addr {
        if let Ok(mut addrs) = addr.to_socket_addrs() {
            if let Some(a) = addrs.next() {
                let _ = TcpStream::connect_timeout(&a, core::time::Duration::from_millis(200));
            }
        }
    }
}

/// `_handle_inbound(proto, ws)`: the acceptor side of the handshake.
fn _handle_inbound(proto: Protocol, mut sock: TcpStream) {
    let _ = sock.set_nodelay(true);
    let _ = sock.set_read_timeout(Some(core::time::Duration::from_secs(10)));
    if !server_upgrade(&mut sock) {
        return;
    }
    let Ok(mut reader) = sock.try_clone() else {
        return;
    };
    let link = Link::new(
        &proto,
        StreamSock::Tcp(sock),
        Framing::WebSocket { mask: false },
    );
    let Some(raw) = read_ws_message(&mut reader, &link) else {
        link.close();
        return;
    };
    let _ = reader.set_read_timeout(None);
    let Ok(msg) = protocol::decode(&String::from_utf8_lossy(&raw)) else {
        link.close();
        return;
    };
    match carrier::_handshake_accept(&proto, &msg) {
        Err(resp) => {
            send_json(&link, &resp);
            link.flush(2.0);
            link.put_rpc(ws_frame(0x8, &1000u16.to_be_bytes(), false));
            link.flush(0.5);
            link.close();
        }
        Ok((peer_id, _caps, resp)) => {
            send_json(&link, &resp);
            carrier::_register_peer(&proto, &peer_id, PeerHandle::Link(link.clone()));
            let _ = log().info(
                "Accepted inbound peer %s",
                alloc::vec![Arg::Str(peer_id.as_str().into())],
            );
            _receive_loop(proto, reader, link, peer_id);
        }
    }
}

fn send_json(link: &Link, obj: &Arg) {
    if let Ok(text) = protocol::encode(obj) {
        link.put_rpc(link.framing().wrap_rpc(text.as_bytes()));
    }
}

/// `connect_outbound(proto, uri, secret)`: the initiator side.
pub fn connect_outbound(proto: &Protocol, uri: &str, secret: &str) -> Result<String> {
    let (host, port) = super::protocols::_carriers::uri::split_host_port(uri, "127.0.0.1", 0);
    let path = {
        let rest = uri.split_once("://").map(|(_, r)| r).unwrap_or(uri);
        match rest.find('/') {
            Some(i) => String::from(&rest[i..]),
            None => String::from("/"),
        }
    };
    let timeout = core::time::Duration::from_secs(10);
    let addrs = (host.as_str(), port)
        .to_socket_addrs()
        .map_err(|e| io_err(&alloc::format!("could not resolve {host}:{port}"), e))?;
    let mut sock = None;
    let mut last = None;
    for a in addrs {
        match TcpStream::connect_timeout(&a, timeout) {
            Ok(s) => {
                sock = Some(s);
                break;
            }
            Err(e) => last = Some(e),
        }
    }
    let Some(mut sock) = sock else {
        return Err(match last {
            Some(e) => io_err(&alloc::format!("could not connect to {host}:{port}"), e),
            None => LailaError::connection_error(alloc::format!("could not resolve {host}:{port}")),
        });
    };
    let _ = sock.set_nodelay(true);
    let _ = sock.set_read_timeout(Some(timeout));
    client_upgrade(&mut sock, &host, port, &path)?;
    let mut reader = sock.try_clone().map_err(|e| io_err("socket clone", e))?;
    let link = Link::new(
        proto,
        StreamSock::Tcp(sock),
        Framing::WebSocket { mask: true },
    );
    let mut params = crate::python::PyDictBuilder::new();
    params.insert("from_id", proto.policy_id());
    params.insert("secret", secret);
    let req = protocol::make_request(carrier::_PEER_CONNECT, params.finish(), None);
    send_json(&link, &req);
    let Some(raw) = read_ws_message(&mut reader, &link) else {
        link.close();
        return Err(LailaError::connection_error(
            "Peer handshake timed out or connection lost.",
        ));
    };
    let _ = reader.set_read_timeout(None);
    let msg = match protocol::decode(&String::from_utf8_lossy(&raw)) {
        Ok(m) => m,
        Err(e) => {
            link.close();
            return Err(e);
        }
    };
    let (peer_id, _caps) = match carrier::_handshake_result(&msg) {
        Ok(x) => x,
        Err(e) => {
            link.close();
            return Err(e);
        }
    };
    carrier::_register_peer(proto, &peer_id, PeerHandle::Link(link.clone()));
    let _ = log().info(
        "Connected to outbound peer %s at %s",
        alloc::vec![Arg::Str(peer_id.as_str().into()), Arg::Str(uri.into())],
    );
    let p = proto.clone();
    let pid = peer_id.clone();
    crate::python::sync::spawn_named(alloc::format!("{}-reader", proto.class_name()), move || {
        _receive_loop(p, reader, link, pid)
    })?;
    Ok(peer_id)
}

/// `_receive_loop(proto, ws, peer_id)`: identical on both sides.
fn _receive_loop(proto: Protocol, mut reader: TcpStream, link: Arc<Link>, peer_id: String) {
    let reply: carrier::Reply = {
        let l = link.clone();
        Arc::new(move |resp: Arg| send_json(&l, &resp))
    };
    while let Some(raw) = read_ws_message(&mut reader, &link) {
        let Ok(msg) = protocol::decode(&String::from_utf8_lossy(&raw)) else {
            continue;
        };
        if protocol::is_request(&msg) {
            if carrier::_is_goodbye(&msg) {
                break;
            }
            carrier::_handle_request_frame(&proto, msg, reply.clone(), Some(&peer_id));
        } else if protocol::is_response(&msg) {
            proto.0.carrier.complete_pending(&msg);
        }
    }
    let still_ours = matches!(
        proto.0.carrier.connection(&peer_id),
        Some(PeerHandle::Link(l)) if Arc::ptr_eq(&l, &link)
    );
    if still_ours {
        carrier::_unregister_peer(&proto, &peer_id);
    }
    link.close();
    let _ = log().info(
        "Connection to peer %s closed.",
        alloc::vec![Arg::Str(peer_id.as_str().into())],
    );
}

/// `ws.close()`: send a close frame and shut the link.
pub fn close_link(link: &Link) {
    let masked = matches!(link.framing(), Framing::WebSocket { mask: true });
    link.put_rpc(ws_frame(0x8, &1000u16.to_be_bytes(), masked));
    link.flush(0.5);
    link.close();
}
