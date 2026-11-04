//! `_StreamRPCProtocol` -- reliable duplex byte streams (TCP, TLS, unix
//! socket) carrying length-prefixed codec frames and stream lanes.
//!
//! Rust shape: an accept thread per listener, and per link one *reader*
//! thread (frames in -> dispatcher) and one *writer* thread draining a
//! two-tier queue -- control/RPC frames first, then **one** chunk of the
//! next stream message, then RPC frames again -- so a bulk stream never
//! starves pings and RPC replies. The chunk queue is bounded
//! (`channel_queue_size`); when full the *oldest* message is dropped and
//! counted on its channel (`tx_dropped`).

use alloc::collections::VecDeque;
use alloc::string::String;
use alloc::sync::{Arc, Weak};
use alloc::vec::Vec;
use core::sync::atomic::{AtomicBool, Ordering};
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream, ToSocketAddrs};
use std::os::unix::net::{UnixListener, UnixStream};

use crate::python::sync::{wait_until, Condvar, Mutex};
use crate::python::{Arg, LailaError, Result};

use super::super::super::channel::Channel;
use super::super::super::protocol as rpc_protocol;
use super::super::base::{Protocol, ProtocolInner, Wire};
use super::base::{self as carrier, PeerHandle};
use super::codec as _codec;

/// `log = logging.getLogger(__name__)` -- the module logger.
pub fn log() -> crate::python::stdlib::logging::Logger {
    crate::python::stdlib::logging::Logger::get(
        "laila.policy.central.communication.protocols._carriers.stream",
    )
}

/// `_StreamRPCProtocol` as a class marker.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct _StreamRPCProtocol;
impl crate::python::builtins::IsInstance for _StreamRPCProtocol {
    fn isinstance(x: &Arg) -> bool {
        Protocol::of(x).is_some_and(|p| p.spec().carrier == super::super::base::CarrierKind::Stream)
    }
}

// ---------------------------------------------------------------------------
// sockets

/// A user-supplied duplex byte link (a test harness or a driver): the
/// point-to-point carrier's `_open_stream` result.
pub trait Duplex: Send + Sync {
    /// Bring the link up (`_open_stream`); a harness link installed once
    /// is reused across `start()` / `stop()` lifecycles, so `shutdown()` /
    /// `open()` pair up.
    fn open(&self) -> std::io::Result<()> {
        Ok(())
    }
    /// Blocking read; `Ok(0)` means "nothing yet" when `polls()` is true,
    /// EOF otherwise.
    fn read(&self, buf: &mut [u8]) -> std::io::Result<usize>;
    fn write(&self, buf: &[u8]) -> std::io::Result<usize>;
    /// Close the link; a blocked `read` must return afterwards.
    fn shutdown(&self);
    /// Does `read` return `Ok(0)` on a timeout rather than only at EOF?
    fn polls(&self) -> bool {
        false
    }
}

/// A connected duplex socket.
pub enum StreamSock {
    Tcp(TcpStream),
    Unix(UnixStream),
    /// A raw serial device (`/dev/ttyUSB0`) opened read/write; reads time
    /// out (`VTIME`) so the reader can poll the stop flag.
    Serial(std::fs::File),
    /// A driver / harness link.
    Virtual(Arc<dyn Duplex>),
    /// A TLS session over TCP (`tls://`).
    #[cfg(feature = "tls")]
    Tls(Arc<super::tls::TlsSock>),
}

impl StreamSock {
    pub fn try_clone(&self) -> std::io::Result<StreamSock> {
        Ok(match self {
            StreamSock::Tcp(s) => StreamSock::Tcp(s.try_clone()?),
            StreamSock::Unix(s) => StreamSock::Unix(s.try_clone()?),
            StreamSock::Serial(f) => StreamSock::Serial(f.try_clone()?),
            StreamSock::Virtual(d) => StreamSock::Virtual(d.clone()),
            #[cfg(feature = "tls")]
            StreamSock::Tls(t) => StreamSock::Tls(t.clone()),
        })
    }
    pub fn shutdown(&self) {
        match self {
            StreamSock::Tcp(s) => {
                let _ = s.shutdown(Shutdown::Both);
            }
            StreamSock::Unix(s) => {
                let _ = s.shutdown(Shutdown::Both);
            }
            StreamSock::Serial(_) => {}
            StreamSock::Virtual(d) => d.shutdown(),
            #[cfg(feature = "tls")]
            StreamSock::Tls(t) => t.shutdown(),
        }
    }
    pub fn set_read_timeout(&self, t_: Option<core::time::Duration>) {
        let t = t_;
        let _ = match self {
            StreamSock::Tcp(s) => s.set_read_timeout(t),
            StreamSock::Unix(s) => s.set_read_timeout(t),
            StreamSock::Serial(_) | StreamSock::Virtual(_) => Ok(()),
            #[cfg(feature = "tls")]
            StreamSock::Tls(t) => {
                t.set_read_timeout(t_);
                Ok(())
            }
        };
    }
    /// Does a zero-length read mean "no data yet" (poll again)?
    pub fn polls(&self) -> bool {
        match self {
            StreamSock::Serial(_) => true,
            StreamSock::Virtual(d) => d.polls(),
            _ => false,
        }
    }
    /// `_on_stream_ready`: TCP_NODELAY so small RPC frames go out at once.
    pub fn set_nodelay(&self) {
        if let StreamSock::Tcp(s) = self {
            let _ = s.set_nodelay(true);
        }
        #[cfg(feature = "tls")]
        if let StreamSock::Tls(t) = self {
            t.set_nodelay();
        }
    }
}
impl Read for StreamSock {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            StreamSock::Tcp(s) => s.read(buf),
            StreamSock::Unix(s) => s.read(buf),
            StreamSock::Serial(f) => f.read(buf),
            StreamSock::Virtual(d) => d.read(buf),
            #[cfg(feature = "tls")]
            StreamSock::Tls(t) => t.read(buf),
        }
    }
}
impl Write for StreamSock {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            StreamSock::Tcp(s) => s.write(buf),
            StreamSock::Unix(s) => s.write(buf),
            StreamSock::Serial(f) => f.write(buf),
            StreamSock::Virtual(d) => d.write(buf),
            #[cfg(feature = "tls")]
            StreamSock::Tls(t) => t.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            StreamSock::Tcp(s) => s.flush(),
            StreamSock::Unix(s) => s.flush(),
            StreamSock::Serial(f) => f.flush(),
            StreamSock::Virtual(_) => Ok(()),
            #[cfg(feature = "tls")]
            StreamSock::Tls(t) => t.flush(),
        }
    }
}

/// How payloads are framed on a link.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Framing {
    /// 4-byte big-endian length prefix (the stream carriers).
    LengthPrefix,
    /// RFC 6455 frames (`tcpip`); clients mask.
    WebSocket { mask: bool },
}

impl Framing {
    /// Wrap one RPC payload for the wire.
    pub fn wrap_rpc(&self, payload: &[u8]) -> Vec<u8> {
        match self {
            Framing::LengthPrefix => _codec::frame(payload),
            Framing::WebSocket { mask } => super::super::tcpip::ws_frame(0x1, payload, *mask),
        }
    }
    /// Wrap one stream-lane chunk.
    pub fn wrap_stream(&self, chunk: &[u8]) -> Vec<u8> {
        match self {
            Framing::LengthPrefix => _codec::frame(chunk),
            Framing::WebSocket { mask } => super::super::tcpip::ws_frame(0x2, chunk, *mask),
        }
    }
}

// ---------------------------------------------------------------------------
// link

struct LinkQueues {
    rpc: VecDeque<Vec<u8>>,
    stream: VecDeque<(Channel, Vec<u8>)>,
}

/// `_Link`: one peer connection's writer with its two-tier queue.
pub struct Link {
    proto: Weak<ProtocolInner>,
    writer: Mutex<Option<StreamSock>>,
    queues: Mutex<LinkQueues>,
    cv: Condvar,
    stream_q_max: usize,
    closed: AtomicBool,
    framing: Framing,
    /// Set once the writer thread has drained its queue and exited.
    idle: Mutex<bool>,
    idle_cv: Condvar,
}

impl Link {
    /// `_Link(proto, writer, loop)` + `start()`.
    pub fn new(proto: &Protocol, writer: StreamSock, framing: Framing) -> Arc<Link> {
        let link = Arc::new(Link {
            proto: proto.0.me.clone(),
            writer: Mutex::new(Some(writer)),
            queues: Mutex::new(LinkQueues {
                rpc: VecDeque::new(),
                stream: VecDeque::new(),
            }),
            cv: Condvar::new(),
            stream_q_max: proto.i64_field("channel_queue_size", 256).max(1) as usize,
            closed: AtomicBool::new(false),
            framing,
            idle: Mutex::new(true),
            idle_cv: Condvar::new(),
        });
        let l = link.clone();
        let _ = crate::python::sync::spawn_named(
            alloc::format!("{}-writer", proto.class_name()),
            move || l.run(),
        );
        link
    }
    pub fn closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
    pub fn framing(&self) -> Framing {
        self.framing
    }
    /// `put_rpc(data)` -- *data* is already framed.
    pub fn put_rpc(&self, data: Vec<u8>) {
        if self.closed() {
            return;
        }
        self.queues.lock().rpc.push_back(data);
        self.cv.notify_all();
    }
    /// `put_stream(channel, payload)`
    pub fn put_stream(&self, channel: &Channel, payload: Vec<u8>) {
        if self.closed() {
            return;
        }
        let mut q = self.queues.lock();
        if q.stream.len() >= self.stream_q_max {
            if let Some((old, _)) = q.stream.pop_front() {
                old.note_tx_dropped();
            }
        }
        q.stream.push_back((channel.clone(), payload));
        self.cv.notify_all();
    }
    fn write(&self, data: &[u8]) -> bool {
        let mut w = self.writer.lock();
        let Some(sock) = w.as_mut() else {
            return false;
        };
        sock.write_all(data).and_then(|()| sock.flush()).is_ok()
    }
    fn drain_rpc(&self) -> bool {
        loop {
            let next = self.queues.lock().rpc.pop_front();
            match next {
                Some(data) => {
                    if !self.write(&data) {
                        return false;
                    }
                }
                None => return true,
            }
        }
    }
    fn run(&self) {
        *self.idle.lock() = false;
        loop {
            if self.closed() {
                break;
            }
            {
                let mut q = self.queues.lock();
                while q.rpc.is_empty() && q.stream.is_empty() && !self.closed() {
                    self.cv.wait(&mut q);
                }
            }
            if self.closed() {
                break;
            }
            if !self.drain_rpc() {
                break;
            }
            let next = self.queues.lock().stream.pop_front();
            if let Some((channel, payload)) = next {
                if channel.closed() || channel.tx_lane_id().is_none() {
                    continue;
                }
                let Some(proto) = Protocol::from_inner(&self.proto) else {
                    break;
                };
                let mut ok = true;
                for chunk in carrier::_stream_chunks(&proto, &channel, &payload) {
                    if !self.write(&self.framing.wrap_stream(&chunk)) || !self.drain_rpc() {
                        ok = false;
                        break;
                    }
                    if self.closed() {
                        break;
                    }
                }
                if !ok {
                    break;
                }
            }
        }
        self.closed.store(true, Ordering::Release);
        *self.idle.lock() = true;
        self.idle_cv.notify_all();
        self.cv.notify_all();
    }
    /// `len(link.stream_q)` / `len(link.rpc_q)`: the queued message counts.
    pub fn queue_lens(&self) -> (usize, usize) {
        let q = self.queues.lock();
        (q.rpc.len(), q.stream.len())
    }
    /// `flush(timeout)`: wait until the RPC queue drained (or *timeout*).
    pub fn flush(&self, timeout: f64) {
        let mut q = self.queues.lock();
        let closed = &self.closed;
        wait_until(&self.cv, &mut q, Some(timeout), |q| {
            q.rpc.is_empty() || closed.load(Ordering::Acquire)
        });
    }
    /// `close_threadsafe()`: `close()` from any thread (no loop to hop to
    /// in the Rust port -- `close` is already thread-safe).
    pub fn close_threadsafe(&self) {
        self.close();
    }
    /// `close()`: stop the writer and shut the socket (unblocks the reader).
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.cv.notify_all();
        if let Some(sock) = self.writer.lock().as_ref() {
            sock.shutdown();
        }
        // Release the socket once the writer thread has exited (bounded
        // wait: a writer wedged in a blocking write keeps its fd until it
        // returns), so the descriptor is really closed when `stop()` returns.
        {
            let mut idle = self.idle.lock();
            wait_until(&self.idle_cv, &mut idle, Some(2.0), |i| *i);
        }
        if let Some(mut w) = self.writer.try_lock() {
            *w = None;
        }
    }
    /// The writer socket's raw descriptor (serial links; `None` once closed).
    #[cfg(unix)]
    pub fn raw_fd(&self) -> Option<i32> {
        use std::os::fd::AsRawFd;
        match self.writer.lock().as_ref() {
            Some(StreamSock::Serial(f)) => Some(f.as_raw_fd()),
            Some(StreamSock::Tcp(s)) => Some(s.as_raw_fd()),
            Some(StreamSock::Unix(s)) => Some(s.as_raw_fd()),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// listener

enum Listener {
    Tcp(TcpListener),
    Unix(UnixListener),
    /// A TCP listener whose accepted connections are TLS-wrapped (on the
    /// inbound link's thread, so the accept loop never blocks on a
    /// handshake).
    #[cfg(feature = "tls")]
    Tls(TcpListener, Arc<rustls::ServerConfig>),
}

/// An accepted connection before any TLS handshake.
enum Accepted {
    Ready(StreamSock),
    #[cfg(feature = "tls")]
    Tls(TcpStream, Arc<rustls::ServerConfig>),
}

impl Accepted {
    fn shutdown(&self) {
        match self {
            Accepted::Ready(s) => s.shutdown(),
            #[cfg(feature = "tls")]
            Accepted::Tls(s, _) => {
                let _ = s.shutdown(Shutdown::Both);
            }
        }
    }
    /// The TLS handshake (server side) for a `tls://` listener.
    fn ready(self, proto: &Protocol) -> Result<StreamSock> {
        match self {
            Accepted::Ready(s) => Ok(s),
            #[cfg(feature = "tls")]
            Accepted::Tls(tcp, cfg) => {
                let _ = tcp.set_read_timeout(Some(core::time::Duration::from_secs_f64(
                    proto.handshake_timeout().max(0.05),
                )));
                let sock = super::tls::accept(cfg, tcp)?;
                sock.set_read_timeout(None);
                Ok(StreamSock::Tls(sock))
            }
        }
    }
}

impl Listener {
    fn accept(&self) -> std::io::Result<Accepted> {
        Ok(match self {
            Listener::Tcp(l) => Accepted::Ready(StreamSock::Tcp(l.accept()?.0)),
            Listener::Unix(l) => Accepted::Ready(StreamSock::Unix(l.accept()?.0)),
            #[cfg(feature = "tls")]
            Listener::Tls(l, cfg) => Accepted::Tls(l.accept()?.0, cfg.clone()),
        })
    }
}

/// Bind the TCP listener (`host`, `port`) and record the bound port.
fn bind_tcp(proto: &Protocol) -> Result<TcpListener> {
    let host = proto.str_field("host");
    let host = if host.is_empty() {
        String::from("0.0.0.0")
    } else {
        host
    };
    let port = proto.i64_field("port", 0).clamp(0, 65535) as u16;
    let l = TcpListener::bind((host.as_str(), port))
        .map_err(|e| io_err("could not bind TCP listener", e))?;
    let bound = l
        .local_addr()
        .map_err(|e| io_err("listener address", e))?
        .port();
    let mut t = proto.0.transport.lock();
    t.bound_port = Some(bound);
    let connect_host = if host == "0.0.0.0" {
        "127.0.0.1"
    } else {
        host.as_str()
    };
    t.listener_addr = Some(alloc::format!("{connect_host}:{bound}"));
    Ok(l)
}

#[cfg(not(feature = "tls"))]
const NO_TLS_BACKEND: &str =
    "TLS transport: this build has no TLS backend; enable the `tls` feature of laila-core or use tcp://.";

fn io_err(prefix: &str, e: std::io::Error) -> LailaError {
    LailaError::connection_error(alloc::format!("{prefix}: {e}"))
}

/// `start()`: bind the listener and run the accept loop on a thread.
pub fn start(proto: &Protocol) -> Result<()> {
    if !carrier::_begin_start(proto) {
        return Ok(());
    }
    let listener = match proto.spec().wire {
        Wire::Tcp => Some(Listener::Tcp(bind_tcp(proto)?)),
        Wire::Unix => {
            let path = super::super::local::unixsocket::effective_path(proto);
            if std::path::Path::new(&path).exists() {
                let _ = std::fs::remove_file(&path);
            }
            let l =
                UnixListener::bind(&path).map_err(|e| io_err("could not bind unix socket", e))?;
            let mut t = proto.0.transport.lock();
            t.bound_path = Some(path.clone());
            t.listener_addr = Some(path);
            Some(Listener::Unix(l))
        }
        Wire::Tls => {
            // No certificate -> client-only endpoint (can peer out, cannot
            // accept inbound). Avoids forcing a cert on pure clients.
            if proto.str_field("certfile").is_empty() {
                None
            } else {
                #[cfg(feature = "tls")]
                {
                    let cfg = super::tls::server_config(proto)?;
                    Some(Listener::Tls(bind_tcp(proto)?, cfg))
                }
                #[cfg(not(feature = "tls"))]
                {
                    return Err(LailaError::runtime_error(NO_TLS_BACKEND));
                }
            }
        }
        _ => None,
    };
    let stop = proto.0.transport.lock().fresh_stop();
    if let Some(listener) = listener {
        let p = proto.clone();
        let name = alloc::format!("{}-accept", proto.class_name());
        crate::python::sync::spawn_named(name, move || accept_loop(p, listener, stop))?;
    }
    proto.0.carrier.set_started(true);
    Ok(())
}

fn accept_loop(proto: Protocol, listener: Listener, stop: Arc<AtomicBool>) {
    loop {
        if stop.load(Ordering::Acquire) {
            break;
        }
        match listener.accept() {
            Ok(sock) => {
                if stop.load(Ordering::Acquire) {
                    sock.shutdown();
                    break;
                }
                let p = proto.clone();
                let _ = crate::python::sync::spawn_named(
                    alloc::format!("{}-inbound-link", proto.class_name()),
                    move || {
                        if let Ok(sock) = sock.ready(&p) {
                            handle_inbound_stream(p, sock)
                        }
                    },
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
    if let Listener::Unix(_) = listener {
        if let Some(path) = proto.0.transport.lock().bound_path.clone() {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Poke the listener so a blocking `accept()` returns and sees the flag.
fn wake_listener(proto: &Protocol) {
    let addr = proto.0.transport.lock().listener_addr.clone();
    let Some(addr) = addr else {
        return;
    };
    match proto.spec().wire {
        Wire::Tcp | Wire::Tls => {
            if let Ok(mut addrs) = addr.to_socket_addrs() {
                if let Some(a) = addrs.next() {
                    let _ = TcpStream::connect_timeout(&a, core::time::Duration::from_millis(200));
                }
            }
        }
        Wire::Unix => {
            let _ = UnixStream::connect(addr);
        }
        _ => {}
    }
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
    proto.0.carrier.clear_connections();
    {
        let mut t = proto.0.transport.lock();
        t.raise_stop();
    }
    wake_listener(proto);
    {
        let mut t = proto.0.transport.lock();
        if let Some(path) = t.bound_path.take() {
            let _ = std::fs::remove_file(path);
        }
        t.clear();
    }
    carrier::_reset_after_stop(proto);
    Ok(())
}

/// `_open_connection(uri)` by wire.
fn open_connection(proto: &Protocol, uri: &str) -> Result<StreamSock> {
    match proto.spec().wire {
        Wire::Unix => {
            let path = super::uri::uri_path(uri);
            UnixStream::connect(&path)
                .map(StreamSock::Unix)
                .map_err(|e| io_err(&alloc::format!("could not connect to {path}"), e))
        }
        #[cfg(not(feature = "tls"))]
        Wire::Tls => Err(LailaError::runtime_error(NO_TLS_BACKEND)),
        _ => {
            let (host, port) = super::uri::split_host_port(uri, "127.0.0.1", 0);
            let timeout = core::time::Duration::from_secs_f64(proto.handshake_timeout().max(0.1));
            let addrs = (host.as_str(), port)
                .to_socket_addrs()
                .map_err(|e| io_err(&alloc::format!("could not resolve {host}:{port}"), e))?;
            let mut last = None;
            for a in addrs {
                match TcpStream::connect_timeout(&a, timeout) {
                    #[cfg(feature = "tls")]
                    Ok(s) if proto.spec().wire == Wire::Tls => {
                        let _ = s.set_read_timeout(Some(timeout));
                        let sock = super::tls::connect(proto, s, &host)?;
                        sock.set_read_timeout(None);
                        return Ok(StreamSock::Tls(sock));
                    }
                    Ok(s) => return Ok(StreamSock::Tcp(s)),
                    Err(e) => last = Some(e),
                }
            }
            Err(match last {
                Some(e) => io_err(&alloc::format!("could not connect to {host}:{port}"), e),
                None => {
                    LailaError::connection_error(alloc::format!("could not resolve {host}:{port}"))
                }
            })
        }
    }
}

/// `connect(uri, secret) -> peer gid`
pub fn connect(proto: &Protocol, uri: &str, secret: &str) -> Result<String> {
    proto.start(crate::python::Args::new())?;
    let sock = open_connection(proto, uri)?;
    sock.set_nodelay();
    let mut reader = sock.try_clone().map_err(|e| io_err("socket clone", e))?;
    let link = Link::new(proto, sock, Framing::LengthPrefix);
    let req = carrier::_handshake_request(proto, secret);
    let codec = proto.codec();
    link.put_rpc(_codec::frame(&_codec::encode(&req, &codec)?));
    reader.set_read_timeout(Some(core::time::Duration::from_secs_f64(
        proto.handshake_timeout().max(0.05),
    )));
    let raw = match _codec::read_frame(&mut reader) {
        Ok(Some(raw)) => raw,
        Ok(None) => {
            link.close();
            return Err(LailaError::connection_error(
                "Peer closed during handshake.",
            ));
        }
        Err(_) => {
            link.close();
            return Err(LailaError::connection_error("Peer handshake timed out."));
        }
    };
    reader.set_read_timeout(None);
    if _codec::is_reserved_frame(&raw) {
        link.close();
        return Err(LailaError::connection_error(
            "Peer sent a stream frame before completing the handshake.",
        ));
    }
    let msg = _codec::decode(&raw, &codec)?;
    let (peer_id, caps) = match carrier::_handshake_result(&msg) {
        Ok(x) => x,
        Err(e) => {
            link.close();
            return Err(e);
        }
    };
    proto.0.carrier.set_peer_caps(&peer_id, caps);
    carrier::_register_peer(proto, &peer_id, PeerHandle::Link(link.clone()));
    let p = proto.clone();
    let pid = peer_id.clone();
    crate::python::sync::spawn_named(alloc::format!("{}-reader", proto.class_name()), move || {
        receive_loop(p, reader, link, pid)
    })?;
    Ok(peer_id)
}

/// `_handle_inbound_stream(reader, writer)`: the server side of the handshake.
fn handle_inbound_stream(proto: Protocol, sock: StreamSock) {
    let Ok(mut reader) = sock.try_clone() else {
        return;
    };
    reader.set_read_timeout(Some(core::time::Duration::from_secs_f64(
        proto.handshake_timeout().max(0.05),
    )));
    let raw = match _codec::read_frame(&mut reader) {
        Ok(Some(raw)) => raw,
        _ => {
            sock.shutdown();
            return;
        }
    };
    reader.set_read_timeout(None);
    if _codec::is_reserved_frame(&raw) {
        sock.shutdown();
        return;
    }
    sock.set_nodelay();
    let codec = proto.codec();
    let link = Link::new(&proto, sock, Framing::LengthPrefix);
    let msg = match _codec::decode(&raw, &codec) {
        Ok(m) => m,
        Err(_) => {
            link.close();
            return;
        }
    };
    let (peer_id, caps, resp) = match carrier::_handshake_accept(&proto, &msg) {
        Ok(x) => x,
        Err(resp) => {
            if let Ok(b) = _codec::encode(&resp, &codec) {
                link.put_rpc(_codec::frame(&b));
            }
            link.flush(2.0);
            link.close();
            return;
        }
    };
    if let Ok(b) = _codec::encode(&resp, &codec) {
        link.put_rpc(_codec::frame(&b));
    }
    proto.0.carrier.set_peer_caps(&peer_id, caps);
    carrier::_register_peer(&proto, &peer_id, PeerHandle::Link(link.clone()));
    receive_loop(proto, reader, link, peer_id);
}

/// `_receive_loop(reader, link, peer_id)`
fn receive_loop(proto: Protocol, mut reader: StreamSock, link: Arc<Link>, peer_id: String) {
    let codec = proto.codec();
    let reply = make_reply(&proto, &link);
    while let Ok(Some(raw)) = _codec::read_frame(&mut reader) {
        if _codec::is_reserved_frame(&raw) {
            carrier::_on_stream_frame(&proto, Some(&peer_id), &raw);
            continue;
        }
        let Ok(msg) = _codec::decode(&raw, &codec) else {
            continue;
        };
        if rpc_protocol::is_request(&msg) {
            if carrier::_is_goodbye(&msg) {
                break;
            }
            carrier::_handle_request_frame(&proto, msg, reply.clone(), Some(&peer_id));
        } else if rpc_protocol::is_response(&msg) {
            proto.0.carrier.complete_pending(&msg);
        }
    }
    // Only drop the peer if this link is still the registered one (a
    // reconnect may have replaced it).
    let still_ours = matches!(
        proto.0.carrier.connection(&peer_id),
        Some(PeerHandle::Link(l)) if Arc::ptr_eq(&l, &link)
    );
    if still_ours {
        carrier::_unregister_peer(&proto, &peer_id);
    }
    link.close();
}

/// `_make_reply(link)`
fn make_reply(proto: &Protocol, link: &Arc<Link>) -> carrier::Reply {
    let codec = proto.codec();
    let link = link.clone();
    Arc::new(move |resp: Arg| {
        if let Ok(b) = _codec::encode(&resp, &codec) {
            link.put_rpc(link.framing().wrap_rpc(&b));
        }
    })
}

fn link_of(proto: &Protocol, peer_id: &str) -> Result<Arc<Link>> {
    match proto.0.carrier.connection(peer_id) {
        Some(PeerHandle::Link(l)) if !l.closed() => Ok(l),
        _ => Err(LailaError::connection_error(alloc::format!(
            "No connection to peer {peer_id}"
        ))),
    }
}

/// `_queue_rpc(peer_id, msg)`
pub fn _queue_rpc(proto: &Protocol, peer_id: &str, msg: &Arg) -> Result<()> {
    let link = link_of(proto, peer_id)?;
    let b = _codec::encode(msg, &proto.codec())?;
    link.put_rpc(link.framing().wrap_rpc(&b));
    Ok(())
}

/// `_send_once(peer_id, path, args, kwargs)`
pub fn _send_once(
    proto: &Protocol,
    peer_id: &str,
    path: &[String],
    args: &[Arg],
    kwargs: &Arg,
) -> Result<Arg> {
    if !proto.0.carrier.has_peer(peer_id) {
        return Err(LailaError::connection_error(alloc::format!(
            "No connection to peer {peer_id}"
        )));
    }
    let (request_id, slot) = proto.0.carrier.register_pending(Some(peer_id));
    let req = carrier::_make_rpc_request(path, args, kwargs, &request_id);
    if let Err(e) = _queue_rpc(proto, peer_id, &req) {
        proto.0.carrier.pop_pending(&request_id);
        return Err(e);
    }
    carrier::_await_pending(proto, &request_id, &slot)
}

/// `_send_oneway(peer_id, path, kwargs)`
pub fn _send_oneway(proto: &Protocol, peer_id: &str, path: &[String], kwargs: &Arg) -> Result<()> {
    let rid = crate::python::stdlib::uuid.uuid4().__str__();
    _queue_rpc(
        proto,
        peer_id,
        &carrier::_make_rpc_request(path, &[], kwargs, &rid),
    )
}

/// `_send_goodbye(peer_id)`
pub fn _send_goodbye(proto: &Protocol, peer_id: &str) -> Result<()> {
    let Ok(link) = link_of(proto, peer_id) else {
        return Ok(());
    };
    let b = _codec::encode(&carrier::_goodbye_message(proto), &proto.codec())?;
    link.put_rpc(link.framing().wrap_rpc(&b));
    link.flush(0.5);
    Ok(())
}

/// `_stream_enqueue(channel, payload)`
pub fn _stream_enqueue(proto: &Protocol, channel: &Channel, payload: &[u8]) -> Result<()> {
    let link = link_of(proto, channel.peer_id())?;
    link.put_stream(channel, payload.to_vec());
    Ok(())
}

/// `proto._link` read dynamically: the link's queue state as a Python-shaped
/// object (`link.stream_q` / `link.rpc_q` are sized sequences, `link.closed`
/// a bool).
#[derive(Clone)]
pub struct LinkView(pub Arc<Link>);

impl core::fmt::Debug for LinkView {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let (rpc, stream) = self.0.queue_lens();
        write!(
            f,
            "_Link(rpc_q={rpc}, stream_q={stream}, closed={})",
            self.0.closed()
        )
    }
}

impl crate::python::PyObject for LinkView {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "_Link"
    }
    fn __repr__(&self) -> String {
        alloc::format!("{self:?}")
    }
    fn __eq__(&self, other: &dyn crate::python::PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<LinkView>()
            .is_some_and(|o| Arc::ptr_eq(&o.0, &self.0))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        use crate::python::IntoArg;
        let (rpc, stream) = self.0.queue_lens();
        Ok(match name {
            // the queues' *lengths* are what callers read (`len(link.stream_q)`)
            "stream_q" => alloc::vec![Arg::None; stream].into_arg(),
            "rpc_q" => alloc::vec![Arg::None; rpc].into_arg(),
            "stream_q_max" => (self.0.stream_q_max as i64).into_arg(),
            "closed" => self.0.closed().into_arg(),
            other => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'_Link' object has no attribute '{other}'"
                )))
            }
        })
    }
}
impl crate::python::IntoArg for LinkView {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
