//! `_P2PStreamRPCProtocol` -- the stream carrier for a *single,
//! already-connected* duplex byte link with no listen / accept step:
//! serial lines (UART / RS-232 / RS-485), USB-CDC, Bluetooth RFCOMM, CAN
//! ISO-TP, ... Both endpoints open the link; one initiates the
//! `peer.connect` handshake and the other answers on the same stream.
//!
//! The link is one [`Link`] (priority RPC queue + chunked stream queue);
//! `stream_chunk_bytes` defaults to `4096` here because serial links are
//! slow and a liveness ping must never wait behind a 30 KB frame.
//!
//! Rust shape: `_open_stream` is resolved from the spec's wire --
//! [`Wire::Serial`] opens the configured `port` as a raw tty (no
//! `pyserial`), [`Wire::Driver`] stops at the driver check
//! (`_require_drivers`) and then the hardware note; a harness may also
//! install a [`Duplex`] with [`set_link`] before `start()`.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicBool, Ordering};
use std::io::Read;

use crate::python::sync::{wait_until, Condvar, Mutex};
use crate::python::{Arg, IntoArg, LailaError, Result};

use super::super::super::channel::Channel;
use super::super::super::protocol as rpc_protocol;
use super::super::base::{CarrierKind, Protocol, Wire};
use super::base::{self as carrier, PeerHandle};
use super::codec as _codec;
use super::stream::{Duplex, Framing, Link, StreamSock};

/// `log = logging.getLogger(__name__)` -- the module logger.
pub fn log() -> crate::python::stdlib::logging::Logger {
    crate::python::stdlib::logging::Logger::get(
        "laila.policy.central.communication.protocols._carriers.p2p",
    )
}

/// `_P2PStreamRPCProtocol` as a class marker.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct _P2PStreamRPCProtocol;
impl crate::python::builtins::IsInstance for _P2PStreamRPCProtocol {
    fn isinstance(x: &Arg) -> bool {
        Protocol::of(x).is_some_and(|p| p.spec().carrier == CarrierKind::P2P)
    }
}

/// The point-to-point link state (`_link`, `_reader`, `_handshake_pending`).
pub struct P2PState {
    pub link: Arc<Link>,
    pub stop: Arc<AtomicBool>,
    handshake_pending: Mutex<Vec<(String, Arc<HandshakeSlot>)>>,
    /// The serial device's descriptors (writer, reader clone) while the
    /// link is open -- `proto._serial.fileno()`.
    pub serial_fds: Vec<i32>,
}

/// `proto._serial` of a serial carrier: the open device as a Python-shaped
/// object (`fileno()`), `None` before `start()` / after `stop()`.
#[derive(Clone, Debug)]
pub struct SerialView {
    pub port: String,
    pub fds: Vec<i32>,
}

impl crate::python::PyObject for SerialView {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "Serial"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "Serial<id=0x{:x}, open=True>(port={:?})",
            self.fds.first().copied().unwrap_or(-1),
            self.port
        )
    }
    fn __eq__(&self, other: &dyn crate::python::PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<SerialView>()
            .is_some_and(|o| o.fds == self.fds)
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(self.fds.first().copied().unwrap_or(-1) as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "port" => Arg::Str(self.port.as_str().into()),
            "is_open" => Arg::Bool(!self.fds.is_empty()),
            "fds" => Arg::List(Arc::new(crate::python::sync::RwLock::new(
                self.fds.iter().map(|f| Arg::Int(*f as i64)).collect(),
            ))),
            other => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'Serial' object has no attribute '{other}'"
                )))
            }
        })
    }
    fn __call_method__(&self, name: &str, _args: crate::python::Args) -> Result<Arg> {
        match name {
            "fileno" => match self.fds.first() {
                Some(fd) => Ok(Arg::Int(*fd as i64)),
                None => Err(LailaError::new(
                    crate::python::ExcKind::OSError,
                    "serial port is closed",
                )),
            },
            other => Err(LailaError::attribute_error(alloc::format!(
                "'Serial' object has no attribute '{other}'"
            ))),
        }
    }
}
impl crate::python::IntoArg for SerialView {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

/// `proto._serial` (see [`SerialView`]).
pub fn serial_view(proto: &Protocol) -> Arg {
    match proto.0.transport.lock().p2p.clone() {
        Some(st) if !st.serial_fds.is_empty() => SerialView {
            port: proto.str_field("port"),
            fds: st.serial_fds.clone(),
        }
        .into_arg(),
        _ => Arg::None,
    }
}

pub struct HandshakeSlot {
    msg: Mutex<Option<Arg>>,
    cv: Condvar,
}

/// Install the duplex link a harness / driver opened (before `start()`):
/// the carrier's `_open_stream` result.
pub fn set_link(proto: &Protocol, link: Arc<dyn Duplex>) {
    proto.0.transport.lock().p2p_link = Some(link);
}

fn io_err(prefix: &str, e: std::io::Error) -> LailaError {
    LailaError::connection_error(alloc::format!("{prefix}: {e}"))
}

/// `_open_stream()`: the link by wire.
fn open_stream(proto: &Protocol) -> Result<StreamSock> {
    if let Some(d) = proto.0.transport.lock().p2p_link.clone() {
        d.open().map_err(|e| io_err("link open", e))?;
        return Ok(StreamSock::Virtual(d));
    }
    let mods: Vec<String> = proto
        .spec()
        .driver_modules
        .iter()
        .map(|m| String::from(*m))
        .collect();
    match proto.spec().wire {
        Wire::Serial => {
            // `_make_serial()`: pyserial is not needed on the Rust side,
            // the device is opened as a raw tty.
            let port = proto.str_field("port");
            if port.is_empty() {
                return Err(LailaError::runtime_error(alloc::format!(
                    "{} requires a `port` (e.g. '/dev/ttyUSB0').",
                    proto.class_name()
                )));
            }
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&port)
                .map_err(|e| {
                    LailaError::runtime_error(alloc::format!(
                        "{} could not open serial port {}: {e}",
                        proto.class_name(),
                        crate::python::pyrepr::str_repr(&port)
                    ))
                })?;
            configure_serial(proto, &file)?;
            Ok(StreamSock::Serial(file))
        }
        _ => {
            carrier::_require_drivers(proto, &mods, proto.spec().driver_extra)?;
            let note = proto.spec().note;
            Err(LailaError::runtime_error(if note.is_empty() {
                alloc::format!(
                    "The {} transport requires an established point-to-point link; configure the endpoint before peering.",
                    crate::python::pyrepr::str_repr(proto.protocol_name())
                )
            } else {
                String::from(note)
            }))
        }
    }
}

/// Raw mode + line speed + a 100 ms read timeout (`VMIN=0`, `VTIME=1`),
/// the `pyserial` `Serial(port, baudrate, timeout=0)` equivalent.
#[cfg(all(unix, feature = "std"))]
fn configure_serial(proto: &Protocol, file: &std::fs::File) -> Result<()> {
    use std::os::unix::io::AsRawFd;
    let fd = file.as_raw_fd();
    // SAFETY: `termios` is a plain C struct filled by `tcgetattr` on a
    // valid open fd; every call below only reads / writes that struct.
    unsafe {
        let mut tio: libc::termios = core::mem::zeroed();
        if libc::tcgetattr(fd, &mut tio) != 0 {
            // Not a tty (a pipe or a pty-less file): leave it alone.
            return Ok(());
        }
        libc::cfmakeraw(&mut tio);
        tio.c_cflag |= libc::CLOCAL | libc::CREAD;
        tio.c_cc[libc::VMIN] = 0;
        tio.c_cc[libc::VTIME] = 1;
        let speed = match proto.i64_field("baudrate", 115200) {
            9600 => libc::B9600,
            19200 => libc::B19200,
            38400 => libc::B38400,
            57600 => libc::B57600,
            230400 => libc::B230400,
            _ => libc::B115200,
        };
        let _ = libc::cfsetispeed(&mut tio, speed);
        let _ = libc::cfsetospeed(&mut tio, speed);
        if libc::tcsetattr(fd, libc::TCSANOW, &tio) != 0 {
            return Err(io_err(
                "could not configure serial port",
                std::io::Error::last_os_error(),
            ));
        }
    }
    Ok(())
}
#[cfg(not(all(unix, feature = "std")))]
fn configure_serial(_proto: &Protocol, _file: &std::fs::File) -> Result<()> {
    Ok(())
}

fn state_of(proto: &Protocol) -> Result<Arc<P2PState>> {
    proto
        .0
        .transport
        .lock()
        .p2p
        .clone()
        .ok_or_else(|| LailaError::connection_error("Point-to-point link is not open."))
}

/// `start()`: open the stream, start the link writer and the receive loop.
pub fn start(proto: &Protocol) -> Result<()> {
    if !carrier::_begin_start(proto) {
        return Ok(());
    }
    let sock = open_stream(proto)?;
    let reader = sock.try_clone().map_err(|e| io_err("link clone", e))?;
    #[cfg(unix)]
    let serial_fds: Vec<i32> = {
        use std::os::fd::AsRawFd;
        match (&sock, &reader) {
            (StreamSock::Serial(a), StreamSock::Serial(b)) => {
                alloc::vec![a.as_raw_fd(), b.as_raw_fd()]
            }
            _ => Vec::new(),
        }
    };
    #[cfg(not(unix))]
    let serial_fds: Vec<i32> = Vec::new();
    let link = Link::new(proto, sock, Framing::LengthPrefix);
    let stop = Arc::new(AtomicBool::new(false));
    let state = Arc::new(P2PState {
        link,
        stop: stop.clone(),
        handshake_pending: Mutex::new(Vec::new()),
        serial_fds,
    });
    {
        let mut t = proto.0.transport.lock();
        t.stop = Some(stop);
        t.p2p = Some(state.clone());
    }
    let p = proto.clone();
    super::transport::spawn_loop(
        proto,
        alloc::format!("{}-reader", proto.class_name()),
        move || receive_loop(p, reader, state),
    )?;
    proto.0.carrier.set_started(true);
    Ok(())
}

/// `stop()`: goodbye, unregister, close the link (idempotent).
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
        let s = t.p2p.take();
        t.clear();
        s
    };
    if let Some(s) = &state {
        s.link.close();
    }
    super::transport::join_loops(proto);
    if let Some(s) = state {
        s.handshake_pending.lock().clear();
    }
    carrier::_reset_after_stop(proto);
    Ok(())
}

/// `_sole_peer()`
fn sole_peer(proto: &Protocol) -> Option<String> {
    proto.0.carrier.peer_ids().into_iter().next()
}

/// Read one length-prefixed frame, polling the stop flag on links whose
/// reads time out (serial / harness). `None` at EOF or stop.
fn read_frame_polling(
    reader: &mut StreamSock,
    buf: &mut Vec<u8>,
    stop: &AtomicBool,
) -> Option<Vec<u8>> {
    let polls = reader.polls();
    let mut chunk = [0u8; 4096];
    loop {
        if let Ok(Some((payload, used))) = _codec::split_frame(buf) {
            buf.drain(..used);
            return Some(payload);
        }
        if stop.load(Ordering::Acquire) {
            return None;
        }
        match reader.read(&mut chunk) {
            Ok(0) if polls => continue,
            Ok(0) => return None,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::Interrupted
                        | std::io::ErrorKind::WouldBlock
                        | std::io::ErrorKind::TimedOut
                ) =>
            {
                continue
            }
            Err(_) => return None,
        }
    }
}

/// `_receive_loop()`
fn receive_loop(proto: Protocol, mut reader: StreamSock, state: Arc<P2PState>) {
    let codec = proto.codec();
    let link = state.link.clone();
    let reply: carrier::Reply = {
        let l = link.clone();
        let c = codec.clone();
        Arc::new(move |resp: Arg| {
            if let Ok(b) = _codec::encode(&resp, &c) {
                l.put_rpc(_codec::frame(&b));
            }
        })
    };
    let mut buf: Vec<u8> = Vec::new();
    while let Some(raw) = read_frame_polling(&mut reader, &mut buf, &state.stop) {
        if _codec::is_reserved_frame(&raw) {
            match sole_peer(&proto) {
                Some(peer_id) => carrier::_on_stream_frame(&proto, Some(&peer_id), &raw),
                // stream frame before any handshake: not ours yet
                None => proto.0.carrier.note_unknown_lane_drop(),
            }
            continue;
        }
        let Ok(msg) = _codec::decode(&raw, &codec) else {
            continue;
        };
        if rpc_protocol::is_request(&msg) {
            let method = rpc_protocol::msg_get(&msg, "method").__str__();
            if method == carrier::_PEER_CONNECT {
                handle_handshake(&proto, &link, &msg);
            } else if carrier::_is_goodbye(&msg) {
                // The link stays open (it is the wire itself); only the
                // peering is dropped, so a new handshake can follow.
                let params = rpc_protocol::msg_get(&msg, "params");
                let from = if params.is_none() {
                    Arg::None
                } else {
                    params.get("from_id").unwrap_or(Arg::None)
                };
                if let Some(p) = sole_peer(&proto) {
                    if from.__str__() == p {
                        carrier::_on_peer_goodbye(&proto, &p);
                    }
                }
            } else {
                let peer = sole_peer(&proto);
                carrier::_handle_request_frame(&proto, msg, reply.clone(), peer.as_deref());
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
    for peer_id in proto.0.carrier.peer_ids() {
        carrier::_unregister_peer(&proto, &peer_id);
    }
}

/// `_handle_handshake(msg)`
fn handle_handshake(proto: &Protocol, link: &Arc<Link>, msg: &Arg) {
    let codec = proto.codec();
    match carrier::_handshake_accept(proto, msg) {
        Err(resp) => {
            if let Ok(b) = _codec::encode(&resp, &codec) {
                link.put_rpc(_codec::frame(&b));
            }
        }
        Ok((peer_id, caps, resp)) => {
            proto.0.carrier.set_peer_caps(&peer_id, caps);
            carrier::_register_peer(proto, &peer_id, PeerHandle::Link(link.clone()));
            if let Ok(b) = _codec::encode(&resp, &codec) {
                link.put_rpc(_codec::frame(&b));
            }
        }
    }
}

/// `connect(uri, secret)`: the handshake over the already-open link.
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
    state
        .link
        .put_rpc(_codec::frame(&_codec::encode(&req, &proto.codec())?));
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
    carrier::_register_peer(proto, &peer_id, PeerHandle::Link(state.link.clone()));
    Ok(peer_id)
}

/// `_queue_rpc_threadsafe(msg)`
fn queue_rpc(proto: &Protocol, peer_id: &str, msg: &Arg) -> Result<()> {
    if !proto.0.carrier.has_peer(peer_id) {
        return Err(LailaError::connection_error(alloc::format!(
            "No connection to peer {peer_id}"
        )));
    }
    let state = state_of(proto)?;
    if state.link.closed() {
        return Err(LailaError::connection_error(
            "Point-to-point link is not open.",
        ));
    }
    state
        .link
        .put_rpc(_codec::frame(&_codec::encode(msg, &proto.codec())?));
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
    if let Err(e) = queue_rpc(proto, peer_id, &req) {
        proto.0.carrier.pop_pending(&request_id);
        return Err(e);
    }
    carrier::_await_pending(proto, &request_id, &slot)
}

/// `_send_oneway(peer_id, path, kwargs)`
pub fn _send_oneway(proto: &Protocol, peer_id: &str, path: &[String], kwargs: &Arg) -> Result<()> {
    let rid = crate::python::stdlib::uuid.uuid4().__str__();
    queue_rpc(
        proto,
        peer_id,
        &carrier::_make_rpc_request(path, &[], kwargs, &rid),
    )
}

/// `_send_goodbye(peer_id)`
pub fn _send_goodbye(proto: &Protocol, peer_id: &str) -> Result<()> {
    let Ok(state) = state_of(proto) else {
        return Ok(());
    };
    if state.link.closed() || !proto.0.carrier.has_peer(peer_id) {
        return Ok(());
    }
    state.link.put_rpc(_codec::frame(&_codec::encode(
        &carrier::_goodbye_message(proto),
        &proto.codec(),
    )?));
    state.link.flush(0.5);
    Ok(())
}

/// `_stream_enqueue(channel, payload)`
pub fn _stream_enqueue(proto: &Protocol, channel: &Channel, payload: &[u8]) -> Result<()> {
    if !proto.0.carrier.has_peer(channel.peer_id()) {
        return Err(LailaError::connection_error(alloc::format!(
            "No connection to peer {}",
            channel.peer_id()
        )));
    }
    let state = state_of(proto)?;
    if state.link.closed() {
        return Err(LailaError::connection_error(
            "Point-to-point link is not open.",
        ));
    }
    state.link.put_stream(channel, payload.to_vec());
    Ok(())
}
