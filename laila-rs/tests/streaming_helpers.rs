//! Shared, hardware-free fixtures of the stream-lane tests -- the Rust
//! twin of `tests/functional/policy/communication/streaming/unit_tests/_helpers.py`
//! (rule P18c: a sibling helper module with a hand-written twin).
//!
//! - [`TwoPolicies`] -- two `DefaultPolicy` instances peered over the
//!   loopback transport, raw TCP on 127.0.0.1, or UDP on 127.0.0.1.
//! - [`UartPtyPair`] -- `openpty()`; laila's `DefaultUARTProtocol` opens
//!   the *slave* as its serial port, a fake peer drives the *master* fd.
//! - [`FakeRpcOnlyPeer`] / [`FakeLanePeer`] -- the fake peers. The Python
//!   originals are asyncio peers; here one reader thread per peer drives
//!   the master fd (non-blocking, polled) and writes go through a lock,
//!   paced by the same token-bucket throttle. `asyncio.run_coroutine_
//!   threadsafe(peer.coro(..), peer.loop).result(..)` of the Python tests
//!   is simply `peer.coro(..)` here (every verb is synchronous).
//! - [`UartNullModem`] -- two policies on two ptys bridged master-to-master
//!   by a copier thread (`poll`), with an optional byte-rate throttle.
#![allow(non_snake_case, non_camel_case_types, dead_code)]

use std::collections::VecDeque;
use std::ffi::CStr;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

use laila::macros::defaults::{
    DefaultLoopbackProtocol, DefaultPolicy, DefaultTCPProtocol, DefaultUARTProtocol,
    DefaultUDPProtocol,
};
use laila::policy::central::communication::protocols::_carriers::codec;
use laila::policy::central::communication::wire;
use laila::prelude::*;
use laila::python::object::bytes as bytes_arg;
use laila::python::stdlib::time;
use laila::python::sync::Mutex;
use laila::python::{PyClass, PyObject};

// ----------------------------------------------------------------------
// small helpers
// ----------------------------------------------------------------------

fn now() -> f64 {
    time.monotonic()
}

fn sleep(secs: f64) {
    if secs > 0.0 {
        std::thread::sleep(std::time::Duration::from_secs_f64(secs));
    }
}

fn io_error(what: &str) -> LailaError {
    LailaError::new(
        laila::python::ExcKind::OSError,
        format!("{what}: {}", std::io::Error::last_os_error()),
    )
}

fn kw<'a>(args: &'a Args, name: &str) -> Option<&'a Arg> {
    args.keywords
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v)
}

fn kw_f64(args: &Args, name: &str) -> Option<f64> {
    kw(args, name).and_then(|v| v.as_f64())
}

fn kw_i64(args: &Args, name: &str, default: i64) -> i64 {
    kw(args, name).and_then(|v| v.as_i64()).unwrap_or(default)
}

fn kw_str(args: &Args, name: &str, default: &str) -> String {
    kw(args, name)
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_else(|| String::from(default))
}

/// `(key, value)` pairs of a dict argument (`{}` when `None`).
fn dict_items(v: &Arg) -> Vec<(String, Arg)> {
    match v {
        Arg::Dict(d) => d
            .read()
            .iter()
            .map(|(k, v)| (k.__str__(), v.clone()))
            .collect(),
        _ => Vec::new(),
    }
}

fn dict(pairs: Vec<(&str, Arg)>) -> Arg {
    Arg::Dict(laila::python::methods::dict_from_pairs(
        pairs.into_iter().map(|(k, v)| (Arg::Str(k.into()), v)),
    ))
}

fn comm_of(policy: &Arg) -> Result<Arg> {
    policy.__getattr__("central")?.__getattr__("communication")
}

/// `make_policy(proto)`: `DefaultPolicy` with *proto* registered and started.
pub fn make_policy(args: Args) -> Result<Arg> {
    let proto = args.positional.first().cloned().unwrap_or(Arg::None);
    let p = DefaultPolicy::new(Args::new())?.into_arg();
    comm_of(&p)?.__call_method__("add_connection", Args::from(proto))?;
    Ok(p)
}

/// `wait_until(pred, timeout=5.0, step=0.01)`: poll *pred* until true or
/// *timeout*; return the final value.
pub fn wait_until(args: Args) -> Result<Arg> {
    let pred = args.positional.first().cloned().unwrap_or(Arg::None);
    let timeout = args
        .positional
        .get(1)
        .and_then(|v| v.as_f64())
        .or_else(|| kw_f64(&args, "timeout"))
        .unwrap_or(5.0);
    let step = args
        .positional
        .get(2)
        .and_then(|v| v.as_f64())
        .or_else(|| kw_f64(&args, "step"))
        .unwrap_or(0.01);
    let deadline = now() + timeout;
    while now() < deadline {
        if pred.__call__(Args::new())?.__bool__() {
            return Ok(Arg::Bool(true));
        }
        sleep(step);
    }
    Ok(Arg::Bool(pred.__call__(Args::new())?.__bool__()))
}

// ----------------------------------------------------------------------
// TwoPolicies
// ----------------------------------------------------------------------

struct TwoPoliciesState {
    kind: String,
    proto_kwargs: Vec<(String, Arg)>,
    liveness_interval: Option<f64>,
    a: Mutex<Arg>,
    b: Mutex<Arg>,
    pa: Mutex<Arg>,
    pb: Mutex<Arg>,
    aid: Mutex<Arg>,
    bid: Mutex<Arg>,
    entered: AtomicBool,
}

/// Two in-process policies peered over one transport kind
/// (`TwoPolicies(kind="loopback", *, liveness_interval=None, **proto_kwargs)`).
#[derive(Clone)]
pub struct TwoPolicies(Arc<TwoPoliciesState>);

impl TwoPolicies {
    pub fn new(args: impl Into<Args>) -> Result<TwoPolicies> {
        let args: Args = args.into();
        let kind = args
            .positional
            .first()
            .and_then(|v| v.as_str().map(String::from))
            .or_else(|| kw(&args, "kind").and_then(|v| v.as_str().map(String::from)))
            .unwrap_or_else(|| String::from("loopback"));
        let liveness_interval = kw_f64(&args, "liveness_interval");
        let proto_kwargs: Vec<(String, Arg)> = args
            .keywords
            .iter()
            .filter(|(k, _)| k != "kind" && k != "liveness_interval")
            .cloned()
            .collect();
        Ok(TwoPolicies(Arc::new(TwoPoliciesState {
            kind,
            proto_kwargs,
            liveness_interval,
            a: Mutex::new(Arg::None),
            b: Mutex::new(Arg::None),
            pa: Mutex::new(Arg::None),
            pb: Mutex::new(Arg::None),
            aid: Mutex::new(Arg::None),
            bid: Mutex::new(Arg::None),
            entered: AtomicBool::new(false),
        })))
    }

    fn factory(&self, kw: Args) -> Result<Arg> {
        Ok(match self.0.kind.as_str() {
            "loopback" => DefaultLoopbackProtocol::new(kw)?.into_arg(),
            "tcp" => DefaultTCPProtocol::new(kw)?.into_arg(),
            "udp" => DefaultUDPProtocol::new(kw)?.into_arg(),
            other => return Err(LailaError::value_error(other)),
        })
    }

    fn uri(&self) -> Result<String> {
        Ok(match self.0.kind.as_str() {
            "loopback" => format!("loopback://{}", self.bid().__str__()),
            "tcp" => format!(
                "tcp://127.0.0.1:{}",
                self.pb().__getattr__("bound_port")?.__str__()
            ),
            "udp" => format!(
                "udp://127.0.0.1:{}",
                self.pb().__getattr__("bound_port")?.__str__()
            ),
            other => return Err(LailaError::value_error(other)),
        })
    }

    pub fn __enter__(&self) -> Result<TwoPolicies> {
        let mut kw = Args::new();
        for (k, v) in &self.0.proto_kwargs {
            kw.push_keyword(k, v.clone());
        }
        if matches!(self.0.kind.as_str(), "tcp" | "udp") && kw.get("host").is_none() {
            kw.push_keyword("host", "127.0.0.1");
        }
        *self.0.pa.lock() = self.factory(kw.clone())?;
        *self.0.pb.lock() = self.factory(kw)?;
        *self.0.a.lock() = DefaultPolicy::new(Args::new())?.into_arg();
        *self.0.b.lock() = DefaultPolicy::new(Args::new())?.into_arg();
        if let Some(li) = self.0.liveness_interval {
            self.ca()?.__setattr__("liveness_interval", li)?;
            self.cb()?.__setattr__("liveness_interval", li)?;
        }
        self.ca()?
            .__call_method__("add_connection", Args::from(self.pa()))?;
        self.cb()?
            .__call_method__("add_connection", Args::from(self.pb()))?;
        *self.0.aid.lock() = self.a().__getattr__("global_id")?;
        *self.0.bid.lock() = self.b().__getattr__("global_id")?;
        let uri = self.uri()?;
        let mut a = Args::from(Arg::Str(uri.into()));
        a.push_positional(self.pb().__getattr__("peer_secret_key")?);
        let bid = self.ca()?.__call_method__("add_peer", a)?;
        if bid.__str__() != self.bid().__str__() {
            return Err(LailaError::new(
                laila::python::ExcKind::AssertionError,
                "add_peer returned another gid",
            ));
        }
        self.0.entered.store(true, Ordering::Release);
        let this = self.clone();
        let registered = wait_until(Args::from(laila::python::callable(move |_a: Args| {
            Ok::<Arg, LailaError>(Arg::Bool(
                this.cb()?.__getattr__("peers")?.__contains__(this.aid())?,
            ))
        })))?;
        if !registered.__bool__() {
            return Err(LailaError::runtime_error("peer B never registered A"));
        }
        Ok(self.clone())
    }

    pub fn __exit__(&self, _args: impl Into<Args>) -> Result<()> {
        if !self.0.entered.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        for pol in [self.a(), self.b()] {
            if let Ok(c) = comm_of(&pol) {
                let _ = c.__call_method__("stop", Args::new());
            }
        }
        Ok(())
    }

    pub fn a(&self) -> Arg {
        self.0.a.lock().clone()
    }
    pub fn b(&self) -> Arg {
        self.0.b.lock().clone()
    }
    pub fn pa(&self) -> Arg {
        self.0.pa.lock().clone()
    }
    pub fn pb(&self) -> Arg {
        self.0.pb.lock().clone()
    }
    pub fn aid(&self) -> Arg {
        self.0.aid.lock().clone()
    }
    pub fn bid(&self) -> Arg {
        self.0.bid.lock().clone()
    }
    pub fn kind(&self) -> Arg {
        Arg::Str(self.0.kind.clone().into())
    }
    pub fn ca(&self) -> Result<Arg> {
        comm_of(&self.a())
    }
    pub fn cb(&self) -> Result<Arg> {
        comm_of(&self.b())
    }
}

impl Drop for TwoPoliciesState {
    fn drop(&mut self) {
        if self.entered.load(Ordering::Acquire) {
            for pol in [self.a.lock().clone(), self.b.lock().clone()] {
                if let Ok(c) = comm_of(&pol) {
                    let _ = c.__call_method__("stop", Args::new());
                }
            }
        }
    }
}

impl laila::python::context::RaisingExit for TwoPolicies {
    fn __laila_exit(&self) -> Result<()> {
        self.__exit__(Args::new())
    }
}

impl PyObject for TwoPolicies {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "TwoPolicies"
    }
    fn __repr__(&self) -> String {
        format!("<TwoPolicies {}>", self.0.kind)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<TwoPolicies>()
            .is_some_and(|o| Arc::ptr_eq(&self.0, &o.0))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "a" => self.a(),
            "b" => self.b(),
            "pa" => self.pa(),
            "pb" => self.pb(),
            "aid" => self.aid(),
            "bid" => self.bid(),
            "kind" => self.kind(),
            "ca" => self.ca()?,
            "cb" => self.cb()?,
            other => {
                return Err(LailaError::attribute_error(format!(
                    "'TwoPolicies' object has no attribute '{other}'"
                )))
            }
        })
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "__enter__" => Ok(self.__enter__()?.into_arg()),
            "__exit__" => self.__exit__(args).map(|()| Arg::Bool(false)),
            "ca" => self.ca(),
            "cb" => self.cb(),
            other => self.__getattr__(other),
        }
    }
}

impl IntoArg for TwoPolicies {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

// ----------------------------------------------------------------------
// pty + fake peers
// ----------------------------------------------------------------------

/// `os.openpty()` -> `(master, slave)`.
pub fn openpty() -> Result<(i32, i32)> {
    let (mut m, mut s) = (0i32, 0i32);
    let rc = unsafe {
        libc::openpty(
            &mut m,
            &mut s,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            core::ptr::null_mut(),
        )
    };
    if rc != 0 {
        return Err(io_error("openpty"));
    }
    Ok((m, s))
}

/// `os.ttyname(fd)`.
pub fn ttyname(fd: i32) -> Result<String> {
    let mut buf = [0u8; 256];
    let rc = unsafe { libc::ttyname_r(fd, buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
    if rc != 0 {
        return Err(io_error("ttyname"));
    }
    let s = unsafe { CStr::from_ptr(buf.as_ptr() as *const libc::c_char) };
    Ok(s.to_string_lossy().into_owned())
}

/// `os.close(fd)` (errors ignored, like the tests' `except OSError: pass`).
pub fn close_fd(fd: i32) {
    if fd >= 0 {
        unsafe {
            libc::close(fd);
        }
    }
}

fn set_blocking(fd: i32, blocking: bool) {
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        if flags < 0 {
            return;
        }
        let flags = if blocking {
            flags & !libc::O_NONBLOCK
        } else {
            flags | libc::O_NONBLOCK
        };
        libc::fcntl(fd, libc::F_SETFL, flags);
    }
}

/// `poll([fd], events, timeout_ms)` -> ready?
fn poll_fd(fd: i32, events: i16, timeout_ms: i32) -> bool {
    let mut p = libc::pollfd {
        fd,
        events,
        revents: 0,
    };
    let rc = unsafe { libc::poll(&mut p, 1, timeout_ms) };
    rc > 0 && (p.revents & (events | libc::POLLHUP | libc::POLLERR)) != 0
}

fn read_fd(fd: i32, buf: &mut [u8]) -> std::io::Result<usize> {
    let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
    if n < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(n as usize)
}

fn write_fd(fd: i32, buf: &[u8]) -> std::io::Result<usize> {
    let n = unsafe { libc::write(fd, buf.as_ptr() as *const libc::c_void, buf.len()) };
    if n < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(n as usize)
}

fn would_block(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
    )
}

/// Token-bucket pacing for a byte rate (`None` = unlimited).
struct Throttle {
    rate: Option<f64>,
    next: Mutex<f64>,
}

impl Throttle {
    fn new(rate: Option<f64>) -> Throttle {
        Throttle {
            rate: rate.filter(|r| *r > 0.0),
            next: Mutex::new(now()),
        }
    }
    fn pace(&self, nbytes: usize) {
        let Some(rate) = self.rate else { return };
        let t = now();
        let delay = {
            let mut next = self.next.lock();
            *next = next.max(t) + nbytes as f64 / rate;
            *next - t
        };
        sleep(delay);
    }
}

/// A tiny deterministic PRNG (`random.Random(seed).random()` stand-in --
/// the tests only need a seeded, repeatable drop pattern).
struct Rng(u64);

impl Rng {
    fn new(seed: i64) -> Rng {
        Rng((seed as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn random(&mut self) -> f64 {
        // xorshift64*
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        let r = x.wrapping_mul(0x2545_F491_4F6C_DD1D);
        (r >> 11) as f64 / (1u64 << 53) as f64
    }
}

struct LaneState {
    lane: u8,
    chunk_size: usize,
    drop_probability: Mutex<f64>,
    rng: Mutex<Rng>,
    seq: AtomicU64,
    chunks_dropped: AtomicI64,
    messages_emitted: AtomicI64,
    messages_with_drop: AtomicI64,
}

struct FakePeerState {
    fd: i32,
    rfd: AtomicI64,
    wfd: AtomicI64,
    gid: String,
    throttle: Throttle,
    remember_blob: Arg,
    codec_name: String,
    read_chunk: usize,
    stop: AtomicBool,
    ready: AtomicBool,
    thread: Mutex<Option<JoinHandle<()>>>,
    wlock: Mutex<()>,
    // observability
    handshakes: AtomicI64,
    handshake_params: Mutex<Vec<Arg>>,
    pings_answered: AtomicI64,
    rpc_paths: Mutex<Vec<Arg>>,
    stream_frames_seen: AtomicI64,
    stream_bytes_seen: AtomicI64,
    responses_seen: Mutex<Vec<Arg>>,
    errors: Mutex<Vec<String>>,
    lane: Option<LaneState>,
}

/// A fake peer instance (`FakeRpcOnlyPeer(fd, ..)` / `FakeLanePeer(fd, ..)`).
#[derive(Clone)]
pub struct FakePeer(Arc<FakePeerState>);

impl FakePeer {
    fn build(args: Args, lane: bool) -> Result<FakePeer> {
        let fd = args
            .positional
            .first()
            .and_then(|v| v.as_i64())
            .or_else(|| kw(&args, "fd").and_then(|v| v.as_i64()))
            .ok_or_else(|| LailaError::type_error("FakeRpcOnlyPeer() needs the pty master fd"))?
            as i32;
        let gid = match kw(&args, "gid") {
            Some(g) if !g.is_none() => g.__str__(),
            _ => format!("LAILA:POLICY:{}", laila::python::stdlib::uuid.uuid4()),
        };
        let lane = lane.then(|| LaneState {
            lane: kw_i64(&args, "lane", 1) as u8,
            chunk_size: kw_i64(&args, "chunk_size", 4096).max(1) as usize,
            drop_probability: Mutex::new(kw_f64(&args, "drop_probability").unwrap_or(0.0)),
            rng: Mutex::new(Rng::new(kw_i64(&args, "seed", 7))),
            seq: AtomicU64::new(0),
            chunks_dropped: AtomicI64::new(0),
            messages_emitted: AtomicI64::new(0),
            messages_with_drop: AtomicI64::new(0),
        });
        Ok(FakePeer(Arc::new(FakePeerState {
            fd,
            rfd: AtomicI64::new(-1),
            wfd: AtomicI64::new(-1),
            gid,
            throttle: Throttle::new(kw_f64(&args, "throttle_bytes_per_sec")),
            remember_blob: kw(&args, "remember_blob").cloned().unwrap_or(Arg::None),
            codec_name: kw_str(&args, "codec_name", "json"),
            read_chunk: kw_i64(&args, "read_chunk", 4096).max(1) as usize,
            stop: AtomicBool::new(false),
            ready: AtomicBool::new(false),
            thread: Mutex::new(None),
            wlock: Mutex::new(()),
            handshakes: AtomicI64::new(0),
            handshake_params: Mutex::new(Vec::new()),
            pings_answered: AtomicI64::new(0),
            rpc_paths: Mutex::new(Vec::new()),
            stream_frames_seen: AtomicI64::new(0),
            stream_bytes_seen: AtomicI64::new(0),
            responses_seen: Mutex::new(Vec::new()),
            errors: Mutex::new(Vec::new()),
            lane,
        })))
    }

    // -- lifecycle --------------------------------------------------------

    pub fn start(&self) -> Result<FakePeer> {
        let rfd = unsafe { libc::dup(self.0.fd) };
        let wfd = unsafe { libc::dup(self.0.fd) };
        if rfd < 0 || wfd < 0 {
            return Err(io_error("dup"));
        }
        set_blocking(rfd, false);
        set_blocking(wfd, false);
        self.0.rfd.store(rfd as i64, Ordering::Release);
        self.0.wfd.store(wfd as i64, Ordering::Release);
        self.0.stop.store(false, Ordering::Release);
        let this = self.clone();
        let handle = std::thread::Builder::new()
            .name("fake-peer".into())
            .spawn(move || this.read_loop())
            .map_err(|e| LailaError::runtime_error(format!("fake peer did not start: {e}")))?;
        *self.0.thread.lock() = Some(handle);
        self.0.ready.store(true, Ordering::Release);
        Ok(self.clone())
    }

    pub fn stop(&self) {
        self.0.stop.store(true, Ordering::Release);
        let handle = self.0.thread.lock().take();
        if let Some(h) = handle {
            if h.thread().id() != std::thread::current().id() {
                let _ = h.join();
            }
        }
        close_fd(self.0.rfd.swap(-1, Ordering::AcqRel) as i32);
        close_fd(self.0.wfd.swap(-1, Ordering::AcqRel) as i32);
    }

    fn stopped(&self) -> bool {
        self.0.stop.load(Ordering::Acquire)
    }

    // -- inbound -----------------------------------------------------------

    /// Throttled `readexactly`: read in small pieces, pacing each. `None`
    /// at EOF / stop.
    fn read_exact(&self, n: usize) -> Option<Vec<u8>> {
        let rfd = self.0.rfd.load(Ordering::Acquire) as i32;
        let mut buf = Vec::with_capacity(n);
        let mut chunk = vec![0u8; self.0.read_chunk];
        while buf.len() < n {
            if self.stopped() {
                return None;
            }
            let want = (n - buf.len()).min(self.0.read_chunk);
            if !poll_fd(rfd, libc::POLLIN, 50) {
                continue;
            }
            match read_fd(rfd, &mut chunk[..want]) {
                Ok(0) => return None,
                Ok(got) => {
                    self.0.throttle.pace(got);
                    buf.extend_from_slice(&chunk[..got]);
                }
                Err(e) if would_block(&e) => continue,
                Err(_) => return None,
            }
        }
        Some(buf)
    }

    fn read_loop(&self) {
        while !self.stopped() {
            let Some(header) = self.read_exact(4) else {
                return;
            };
            let length = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
            let raw = if length > 0 {
                match self.read_exact(length) {
                    Some(r) => r,
                    None => return,
                }
            } else {
                Vec::new()
            };
            self.on_frame(&raw);
        }
    }

    fn on_frame(&self, raw: &[u8]) {
        if !raw.is_empty() && raw[0] <= wire::RESERVED_MARKER_MAX {
            self.0.stream_frames_seen.fetch_add(1, Ordering::AcqRel);
            self.0
                .stream_bytes_seen
                .fetch_add(raw.len() as i64, Ordering::AcqRel);
            return;
        }
        let msg = match codec::decode(raw, &self.0.codec_name) {
            Ok(m) => m,
            Err(e) => {
                self.0.errors.lock().push(format!("decode: {}", e));
                return;
            }
        };
        if msg.__contains__("method").unwrap_or(false) {
            self.on_request(&msg);
        } else {
            self.0.responses_seen.lock().push(msg);
        }
    }

    fn on_request(&self, msg: &Arg) {
        let rid = msg.get("id").unwrap_or(Arg::None);
        let method = msg.get("method").unwrap_or(Arg::None).__str__();
        let params = match msg.get("params") {
            Ok(p) if !p.is_none() => p,
            _ => dict(Vec::new()),
        };
        let res = (|| -> Result<()> {
            if method == "peer.connect" {
                self.0.handshakes.fetch_add(1, Ordering::AcqRel);
                self.0.handshake_params.lock().push(params.clone());
                // Older peers: no "caps" key in the result.
                return self.send_obj(&dict(vec![
                    ("jsonrpc", "2.0".into_arg()),
                    ("id", rid),
                    (
                        "result",
                        dict(vec![("peer_id", self.0.gid.clone().into_arg())]),
                    ),
                ]));
            }
            if method != "rpc.call" {
                return self.send_obj(&Self::error(
                    rid,
                    -32601,
                    &format!("Unknown method: {method}"),
                ));
            }
            let path = params.get("path").unwrap_or(Arg::None);
            let path: Vec<String> = if path.is_none() {
                Vec::new()
            } else {
                path.__iter__()?.iter().map(|p| p.__str__()).collect()
            };
            self.0.rpc_paths.lock().push(
                path.iter()
                    .map(|s| Arg::Str(s.clone().into()))
                    .collect::<Vec<_>>()
                    .into_arg(),
            );
            if path == ["__comm_ping__"] {
                self.0.pings_answered.fetch_add(1, Ordering::AcqRel);
                return self.send_obj(&dict(vec![
                    ("jsonrpc", "2.0".into_arg()),
                    ("id", rid),
                    ("result", "pong".into_arg()),
                ]));
            }
            if path == ["central", "memory", "_remote_remember"] && !self.0.remember_blob.is_none()
            {
                return self.send_obj(&dict(vec![
                    ("jsonrpc", "2.0".into_arg()),
                    ("id", rid),
                    ("result", vec![self.0.remember_blob.clone()].into_arg()),
                ]));
            }
            self.send_obj(&Self::error(
                rid,
                -32601,
                &format!("Unknown method: {}", path.join(".")),
            ))
        })();
        if let Err(e) = res {
            self.0.errors.lock().push(format!("request: {}", e));
        }
    }

    fn error(rid: Arg, code: i64, message: &str) -> Arg {
        dict(vec![
            ("jsonrpc", "2.0".into_arg()),
            ("id", rid),
            (
                "error",
                dict(vec![
                    ("code", code.into_arg()),
                    ("message", message.into_arg()),
                ]),
            ),
        ])
    }

    // -- outbound ----------------------------------------------------------

    /// Write *data* in `read_chunk` pieces so RPC replies interleave
    /// realistically; a `deadline` (seconds) bounds a write parked on a
    /// full pty buffer (`TimeoutError`, like the asyncio future's).
    fn write_raw_within(&self, data: &[u8], deadline: Option<f64>) -> Result<()> {
        let _g = self.0.wlock.lock();
        let wfd = self.0.wfd.load(Ordering::Acquire) as i32;
        if wfd < 0 {
            return Err(LailaError::connection_error("fake peer is not started"));
        }
        let until = deadline.map(|d| now() + d);
        for piece in data.chunks(self.0.read_chunk) {
            let mut off = 0;
            while off < piece.len() {
                if self.stopped() {
                    return Err(LailaError::connection_error("fake peer stopped"));
                }
                if let Some(t) = until {
                    if now() > t {
                        return Err(LailaError::new(
                            laila::python::ExcKind::TimeoutError,
                            "fake peer write timed out",
                        ));
                    }
                }
                match write_fd(wfd, &piece[off..]) {
                    Ok(n) => off += n,
                    Err(e) if would_block(&e) => {
                        poll_fd(wfd, libc::POLLOUT, 50);
                    }
                    Err(e) => return Err(LailaError::connection_error(format!("write: {e}"))),
                }
            }
            self.0.throttle.pace(piece.len());
        }
        Ok(())
    }

    pub fn _write_raw(&self, data: &[u8]) -> Result<()> {
        self.write_raw_within(data, None)
    }

    pub fn send_obj(&self, obj: &Arg) -> Result<()> {
        let payload = codec::encode(obj, &self.0.codec_name)?;
        self._write_raw(&codec::frame(&payload))
    }

    /// Write one length-prefixed frame with an arbitrary payload.
    pub fn send_raw_frame(&self, payload: &[u8]) -> Result<()> {
        self._write_raw(&codec::frame(payload))
    }

    // -- lane peer ---------------------------------------------------------

    fn lane(&self) -> Result<&LaneState> {
        self.0.lane.as_ref().ok_or_else(|| {
            LailaError::attribute_error("'FakeRpcOnlyPeer' object has no attribute 'emit'")
        })
    }

    /// Chunk *payload* into lane frames; `None` marks a dropped chunk.
    pub fn frames_for(&self, payload: &[u8]) -> Result<Vec<Option<Vec<u8>>>> {
        let lane = self.lane()?;
        let size = lane.chunk_size;
        let count = payload.len().div_ceil(size).max(1);
        let mut out = Vec::with_capacity(count);
        let mut had_drop = false;
        let drop_p = *lane.drop_probability.lock();
        for i in 0..count {
            let mut flags = 0u8;
            if i == 0 {
                flags |= wire::FLAG_START;
            }
            if i == count - 1 {
                flags |= wire::FLAG_END;
            }
            let lo = (i * size).min(payload.len());
            let hi = ((i + 1) * size).min(payload.len());
            let seq = lane.seq.load(Ordering::Acquire);
            let frame = wire::pack_stream_frame(lane.lane, seq, flags, &payload[lo..hi]);
            lane.seq
                .store((seq + 1) % wire::SEQ_MODULUS, Ordering::Release);
            if drop_p > 0.0 && lane.rng.lock().random() < drop_p {
                lane.chunks_dropped.fetch_add(1, Ordering::AcqRel);
                had_drop = true;
                out.push(None);
            } else {
                out.push(Some(frame));
            }
        }
        lane.messages_emitted.fetch_add(1, Ordering::AcqRel);
        if had_drop {
            lane.messages_with_drop.fetch_add(1, Ordering::AcqRel);
        }
        Ok(out)
    }

    /// Emit one message; `false` if any chunk was dropped.
    pub fn emit_within(&self, payload: &[u8], deadline: Option<f64>) -> Result<bool> {
        let frames = self.frames_for(payload)?;
        for f in frames.iter().flatten() {
            self.write_raw_within(&codec::frame(f), deadline)?;
        }
        Ok(frames.iter().all(|f| f.is_some()))
    }

    pub fn emit(&self, payload: &[u8]) -> Result<bool> {
        self.emit_within(payload, None)
    }

    fn payload_of(args: &Args) -> Result<Vec<u8>> {
        args.positional
            .first()
            .and_then(|v| v.as_bytes_like())
            .ok_or_else(|| LailaError::type_error("a bytes-like payload is required"))
    }

    fn list_arg(v: &Mutex<Vec<Arg>>) -> Arg {
        v.lock().clone().into_arg()
    }
}

impl PyObject for FakePeer {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        if self.0.lane.is_some() {
            "FakeLanePeer"
        } else {
            "FakeRpcOnlyPeer"
        }
    }
    fn __repr__(&self) -> String {
        format!("<{} gid={}>", self.type_name(), self.0.gid)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<FakePeer>()
            .is_some_and(|o| Arc::ptr_eq(&self.0, &o.0))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        let s = &self.0;
        Ok(match name {
            "fd" => Arg::Int(s.fd as i64),
            "gid" => Arg::Str(s.gid.clone().into()),
            "codec_name" => Arg::Str(s.codec_name.clone().into()),
            "read_chunk" => Arg::Int(s.read_chunk as i64),
            "remember_blob" => s.remember_blob.clone(),
            // no event loop on the Rust side: the verbs are synchronous
            "loop" => Arg::None,
            "handshakes" => Arg::Int(s.handshakes.load(Ordering::Acquire)),
            "handshake_params" => Self::list_arg(&s.handshake_params),
            "pings_answered" => Arg::Int(s.pings_answered.load(Ordering::Acquire)),
            "rpc_paths" => Self::list_arg(&s.rpc_paths),
            "stream_frames_seen" => Arg::Int(s.stream_frames_seen.load(Ordering::Acquire)),
            "stream_bytes_seen" => Arg::Int(s.stream_bytes_seen.load(Ordering::Acquire)),
            "responses_seen" => Self::list_arg(&s.responses_seen),
            "errors" => s
                .errors
                .lock()
                .iter()
                .map(|e| Arg::Str(e.clone().into()))
                .collect::<Vec<_>>()
                .into_arg(),
            "lane" => Arg::Int(self.lane()?.lane as i64),
            "chunk_size" => Arg::Int(self.lane()?.chunk_size as i64),
            "drop_probability" => Arg::Float(*self.lane()?.drop_probability.lock()),
            "seq" => Arg::Int(self.lane()?.seq.load(Ordering::Acquire) as i64),
            "chunks_dropped" => Arg::Int(self.lane()?.chunks_dropped.load(Ordering::Acquire)),
            "messages_emitted" => Arg::Int(self.lane()?.messages_emitted.load(Ordering::Acquire)),
            "messages_with_drop" => {
                Arg::Int(self.lane()?.messages_with_drop.load(Ordering::Acquire))
            }
            other if FAKE_PEER_METHODS.contains(&other) => {
                let this = self.clone();
                let n = String::from(other);
                laila::python::callable(move |a: Args| this.__call_method__(&n, a))
            }
            other => {
                return Err(LailaError::attribute_error(format!(
                    "'{}' object has no attribute '{other}'",
                    self.type_name()
                )))
            }
        })
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        match name {
            "drop_probability" => {
                *self.lane()?.drop_probability.lock() = value.as_f64().unwrap_or(0.0);
                Ok(())
            }
            other => Err(LailaError::attribute_error(format!(
                "'{}' object attribute '{other}' is read-only",
                self.type_name()
            ))),
        }
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let timeout = || {
            args.positional
                .get(1)
                .and_then(|v| v.as_f64())
                .or_else(|| kw_f64(&args, "timeout"))
        };
        Ok(match name {
            "start" => self.start()?.into_arg(),
            "stop" => {
                self.stop();
                Arg::None
            }
            "send_obj" | "send_obj_threadsafe" => {
                let obj = args.positional.first().cloned().unwrap_or(Arg::None);
                self.send_obj(&obj)?;
                Arg::None
            }
            "send_raw_frame_threadsafe" | "send_raw_frame" => {
                self.send_raw_frame(&Self::payload_of(&args)?)?;
                Arg::None
            }
            "_write_raw" => {
                self._write_raw(&Self::payload_of(&args)?)?;
                Arg::None
            }
            "frames_for" => self
                .frames_for(&Self::payload_of(&args)?)?
                .into_iter()
                .map(|f| f.map(bytes_arg).unwrap_or(Arg::None))
                .collect::<Vec<_>>()
                .into_arg(),
            "emit" => Arg::Bool(self.emit(&Self::payload_of(&args)?)?),
            "emit_threadsafe" => {
                Arg::Bool(self.emit_within(&Self::payload_of(&args)?, timeout().or(Some(120.0)))?)
            }
            other => {
                return Err(LailaError::attribute_error(format!(
                    "'{}' object has no attribute '{other}'",
                    self.type_name()
                )))
            }
        })
    }
}

impl IntoArg for FakePeer {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

const FAKE_PEER_METHODS: &[&str] = &[
    "start",
    "stop",
    "send_obj",
    "send_obj_threadsafe",
    "send_raw_frame_threadsafe",
    "send_raw_frame",
    "_write_raw",
    "frames_for",
    "emit",
    "emit_threadsafe",
];

/// `FakeRpcOnlyPeer`: RPC-only peer driving a pty master fd. Speaks only
/// the *current* wire format (4-byte BE length prefix + JSON-RPC 2.0):
/// answers `peer.connect` **without** `caps`, `__comm_ping__` with
/// `"pong"`, `central.memory._remote_remember` with a canned serialized
/// Entry, and returns a JSON-RPC error for any unknown method. It never
/// emits `0x01` frames. Stands in for older laila peers and MCU firmware.
#[derive(Clone, Copy, Debug, Default)]
pub struct FakeRpcOnlyPeer;

impl FakeRpcOnlyPeer {
    /// `FakeRpcOnlyPeer(master_fd, **kw)`: the class is a constructor
    /// marker, the instance a [`FakePeer`].
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<FakePeer> {
        FakePeer::build(args.into(), false)
    }
    pub fn class_object() -> PyClass {
        PyClass::with_ctor(
            "FakeRpcOnlyPeer",
            "_helpers",
            Arc::new(|a: Args| Self::new(a).map(IntoArg::into_arg)),
        )
    }
}

/// `FakeLanePeer`: [`FakeRpcOnlyPeer`] that can emit lane frames on a
/// static lane (never answers `__comm_channel_open__`, so laila must use
/// `static_lanes`); supports chunking and random chunk loss.
#[derive(Clone, Copy, Debug, Default)]
pub struct FakeLanePeer;

impl FakeLanePeer {
    /// `FakeLanePeer(master_fd, **kw)`: the class is a constructor
    /// marker, the instance a [`FakePeer`].
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<FakePeer> {
        FakePeer::build(args.into(), true)
    }
    pub fn class_object() -> PyClass {
        PyClass::with_ctor(
            "FakeLanePeer",
            "_helpers",
            Arc::new(|a: Args| Self::new(a).map(IntoArg::into_arg)),
        )
    }
}

macro_rules! class_value {
    ($t:ty) => {
        impl IntoArg for $t {
            fn into_arg(self) -> Arg {
                <$t>::class_object().into_arg()
            }
        }
        impl laila::python::builtins::ClassValue for $t {
            fn class_value() -> Arg {
                <$t>::class_object().into_arg()
            }
        }
        impl laila::python::PyArg for $t {
            fn to_arg(&self) -> Arg {
                <$t>::class_object().into_arg()
            }
        }
    };
}
class_value!(FakeRpcOnlyPeer);
class_value!(FakeLanePeer);

// ----------------------------------------------------------------------
// UartNullModem
// ----------------------------------------------------------------------

struct NullModemState {
    throttle: Option<f64>,
    liveness_interval: Option<f64>,
    uart_kwargs: Vec<(String, Arg)>,
    fds: Mutex<[i32; 4]>, // m1, s1, m2, s2
    stop: Arc<AtomicBool>,
    thread: Mutex<Option<JoinHandle<()>>>,
    a: Mutex<Arg>,
    b: Mutex<Arg>,
    pa: Mutex<Arg>,
    pb: Mutex<Arg>,
    aid: Mutex<Arg>,
    bid: Mutex<Arg>,
    bytes_copied: Arc<AtomicI64>,
    entered: AtomicBool,
}

/// Two laila policies on two ptys bridged master-to-master (a null-modem).
///
/// A copier thread moves bytes `master1 -> master2` and `master2 ->
/// master1` with `poll`; an optional byte-rate throttle emulates a slow
/// serial link. Policy A initiates the handshake over its port.
#[derive(Clone)]
pub struct UartNullModem(Arc<NullModemState>);

impl UartNullModem {
    pub fn new(args: impl Into<Args>) -> Result<UartNullModem> {
        let args: Args = args.into();
        Ok(UartNullModem(Arc::new(NullModemState {
            throttle: kw_f64(&args, "throttle_bytes_per_sec").filter(|r| *r > 0.0),
            liveness_interval: kw_f64(&args, "liveness_interval"),
            uart_kwargs: args
                .keywords
                .iter()
                .filter(|(k, _)| k != "throttle_bytes_per_sec" && k != "liveness_interval")
                .cloned()
                .collect(),
            fds: Mutex::new([-1; 4]),
            stop: Arc::new(AtomicBool::new(false)),
            thread: Mutex::new(None),
            a: Mutex::new(Arg::None),
            b: Mutex::new(Arg::None),
            pa: Mutex::new(Arg::None),
            pb: Mutex::new(Arg::None),
            aid: Mutex::new(Arg::None),
            bid: Mutex::new(Arg::None),
            bytes_copied: Arc::new(AtomicI64::new(0)),
            entered: AtomicBool::new(false),
        })))
    }

    fn bridge(
        m1: i32,
        m2: i32,
        throttle: Option<f64>,
        stop: Arc<AtomicBool>,
        copied: Arc<AtomicI64>,
    ) {
        set_blocking(m1, false);
        set_blocking(m2, false);
        let mut budget_t = now();
        let mut buf = [0u8; 4096];
        let mut fds = [
            libc::pollfd {
                fd: m1,
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: m2,
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        while !stop.load(Ordering::Acquire) {
            for p in fds.iter_mut() {
                p.revents = 0;
            }
            let rc = unsafe { libc::poll(fds.as_mut_ptr(), 2, 50) };
            if rc <= 0 {
                continue;
            }
            for (i, pfd) in fds.iter().enumerate() {
                if pfd.revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) == 0 {
                    continue;
                }
                let (src, dst) = if i == 0 { (m1, m2) } else { (m2, m1) };
                let data = match read_fd(src, &mut buf) {
                    Ok(0) => return,
                    Ok(n) => &buf[..n],
                    Err(e) if would_block(&e) => continue,
                    Err(_) => return,
                };
                if let Some(rate) = throttle {
                    let t = now();
                    budget_t = budget_t.max(t) + data.len() as f64 / rate;
                    sleep(budget_t - t);
                }
                let mut off = 0;
                while off < data.len() && !stop.load(Ordering::Acquire) {
                    match write_fd(dst, &data[off..]) {
                        Ok(n) => {
                            off += n;
                            copied.fetch_add(n as i64, Ordering::AcqRel);
                        }
                        Err(e) if would_block(&e) => {
                            poll_fd(dst, libc::POLLOUT, 50);
                        }
                        Err(_) => return,
                    }
                }
            }
        }
    }

    pub fn __enter__(&self) -> Result<UartNullModem> {
        let (m1, s1) = openpty()?;
        let (m2, s2) = openpty()?;
        *self.0.fds.lock() = [m1, s1, m2, s2];
        let (stop, copied, throttle) = (
            self.0.stop.clone(),
            self.0.bytes_copied.clone(),
            self.0.throttle,
        );
        stop.store(false, Ordering::Release);
        let handle = std::thread::Builder::new()
            .name("null-modem".into())
            .spawn(move || Self::bridge(m1, m2, throttle, stop, copied))
            .map_err(|e| LailaError::runtime_error(format!("null-modem thread: {e}")))?;
        *self.0.thread.lock() = Some(handle);
        let mut ka = Args::new();
        ka.push_keyword("port", ttyname(s1)?);
        let mut kb = Args::new();
        kb.push_keyword("port", ttyname(s2)?);
        for (k, v) in &self.0.uart_kwargs {
            ka.push_keyword(k, v.clone());
            kb.push_keyword(k, v.clone());
        }
        *self.0.pa.lock() = DefaultUARTProtocol::new(ka)?.into_arg();
        *self.0.pb.lock() = DefaultUARTProtocol::new(kb)?.into_arg();
        *self.0.a.lock() = DefaultPolicy::new(Args::new())?.into_arg();
        *self.0.b.lock() = DefaultPolicy::new(Args::new())?.into_arg();
        if let Some(li) = self.0.liveness_interval {
            self.ca()?.__setattr__("liveness_interval", li)?;
            self.cb()?.__setattr__("liveness_interval", li)?;
        }
        self.ca()?
            .__call_method__("add_connection", Args::from(self.pa()))?;
        self.cb()?
            .__call_method__("add_connection", Args::from(self.pb()))?;
        *self.0.aid.lock() = self.a().__getattr__("global_id")?;
        self.0.entered.store(true, Ordering::Release);
        let mut a = Args::from(Arg::Str(format!("serial://{}", ttyname(s1)?).into()));
        a.push_positional(self.pb().__getattr__("peer_secret_key")?);
        let bid = self.ca()?.__call_method__("add_peer", a)?;
        *self.0.bid.lock() = bid.clone();
        if bid.__str__() != self.b().__getattr__("global_id")?.__str__() {
            return Err(LailaError::new(
                laila::python::ExcKind::AssertionError,
                "add_peer returned another gid",
            ));
        }
        let this = self.clone();
        let registered = wait_until(Args::from(laila::python::callable(move |_a: Args| {
            Ok::<Arg, LailaError>(Arg::Bool(
                this.cb()?.__getattr__("peers")?.__contains__(this.aid())?,
            ))
        })))?;
        if !registered.__bool__() {
            return Err(LailaError::runtime_error(
                "peer B never registered A over the null-modem",
            ));
        }
        Ok(self.clone())
    }

    pub fn __exit__(&self, _args: impl Into<Args>) -> Result<()> {
        if !self.0.entered.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        for pol in [self.a(), self.b()] {
            if let Ok(c) = comm_of(&pol) {
                let _ = c.__call_method__("stop", Args::new());
            }
        }
        self.0.stop.store(true, Ordering::Release);
        let handle = self.0.thread.lock().take();
        if let Some(h) = handle {
            let _ = h.join();
        }
        let fds = core::mem::replace(&mut *self.0.fds.lock(), [-1; 4]);
        for fd in fds {
            close_fd(fd);
        }
        Ok(())
    }

    pub fn a(&self) -> Arg {
        self.0.a.lock().clone()
    }
    pub fn b(&self) -> Arg {
        self.0.b.lock().clone()
    }
    pub fn pa(&self) -> Arg {
        self.0.pa.lock().clone()
    }
    pub fn pb(&self) -> Arg {
        self.0.pb.lock().clone()
    }
    pub fn aid(&self) -> Arg {
        self.0.aid.lock().clone()
    }
    pub fn bid(&self) -> Arg {
        self.0.bid.lock().clone()
    }
    pub fn ca(&self) -> Result<Arg> {
        comm_of(&self.a())
    }
    pub fn cb(&self) -> Result<Arg> {
        comm_of(&self.b())
    }
    pub fn bytes_copied(&self) -> Arg {
        Arg::Int(self.0.bytes_copied.load(Ordering::Acquire))
    }
}

impl Drop for NullModemState {
    fn drop(&mut self) {
        if self.entered.load(Ordering::Acquire) {
            for pol in [self.a.lock().clone(), self.b.lock().clone()] {
                if let Ok(c) = comm_of(&pol) {
                    let _ = c.__call_method__("stop", Args::new());
                }
            }
        }
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.thread.lock().take() {
            let _ = h.join();
        }
        for fd in core::mem::replace(&mut *self.fds.lock(), [-1; 4]) {
            close_fd(fd);
        }
    }
}

impl laila::python::context::RaisingExit for UartNullModem {
    fn __laila_exit(&self) -> Result<()> {
        self.__exit__(Args::new())
    }
}

impl PyObject for UartNullModem {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "UartNullModem"
    }
    fn __repr__(&self) -> String {
        String::from("<UartNullModem>")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<UartNullModem>()
            .is_some_and(|o| Arc::ptr_eq(&self.0, &o.0))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "a" => self.a(),
            "b" => self.b(),
            "pa" => self.pa(),
            "pb" => self.pb(),
            "aid" => self.aid(),
            "bid" => self.bid(),
            "ca" => self.ca()?,
            "cb" => self.cb()?,
            "bytes_copied" => self.bytes_copied(),
            other => {
                return Err(LailaError::attribute_error(format!(
                    "'UartNullModem' object has no attribute '{other}'"
                )))
            }
        })
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "__enter__" => Ok(self.__enter__()?.into_arg()),
            "__exit__" => self.__exit__(args).map(|()| Arg::Bool(false)),
            "ca" => self.ca(),
            "cb" => self.cb(),
            other => self.__getattr__(other),
        }
    }
}

impl IntoArg for UartNullModem {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

// ----------------------------------------------------------------------
// UartPtyPair
// ----------------------------------------------------------------------

struct PtyPairState {
    peer_cls: Arg,
    throttle: Option<f64>,
    uart_kwargs: Vec<(String, Arg)>,
    peer_kwargs: Vec<(String, Arg)>,
    liveness_interval: Option<f64>,
    fds: Mutex<[i32; 2]>, // master, slave
    proto: Mutex<Arg>,
    policy: Mutex<Arg>,
    peer: Mutex<Arg>,
    peer_gid: Mutex<Arg>,
    entered: AtomicBool,
}

/// laila UART protocol on a pty slave + a fake peer on the master
/// (`UartPtyPair(peer_cls=FakeRpcOnlyPeer, *, throttle_bytes_per_sec=None,
/// uart_kwargs=None, peer_kwargs=None, liveness_interval=None)`).
#[derive(Clone)]
pub struct UartPtyPair(Arc<PtyPairState>);

impl UartPtyPair {
    pub fn new(args: impl Into<Args>) -> Result<UartPtyPair> {
        let args: Args = args.into();
        let peer_cls = args
            .positional
            .first()
            .cloned()
            .or_else(|| kw(&args, "peer_cls").cloned())
            .unwrap_or_else(|| FakeRpcOnlyPeer.into_arg());
        Ok(UartPtyPair(Arc::new(PtyPairState {
            peer_cls,
            throttle: kw_f64(&args, "throttle_bytes_per_sec").filter(|r| *r > 0.0),
            uart_kwargs: kw(&args, "uart_kwargs").map(dict_items).unwrap_or_default(),
            peer_kwargs: kw(&args, "peer_kwargs").map(dict_items).unwrap_or_default(),
            liveness_interval: kw_f64(&args, "liveness_interval"),
            fds: Mutex::new([-1; 2]),
            proto: Mutex::new(Arg::None),
            policy: Mutex::new(Arg::None),
            peer: Mutex::new(Arg::None),
            peer_gid: Mutex::new(Arg::None),
            entered: AtomicBool::new(false),
        })))
    }

    pub fn __enter__(&self) -> Result<UartPtyPair> {
        let (master, slave) = openpty()?;
        *self.0.fds.lock() = [master, slave];
        let codec_name = self
            .0
            .uart_kwargs
            .iter()
            .find(|(k, _)| k == "codec")
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| "json".into_arg());
        let mut pk = Args::from(Arg::Int(master as i64));
        pk.push_keyword(
            "throttle_bytes_per_sec",
            self.0.throttle.map(Arg::Float).unwrap_or(Arg::None),
        );
        pk.push_keyword("codec_name", codec_name);
        for (k, v) in &self.0.peer_kwargs {
            pk.push_keyword(k, v.clone());
        }
        let peer = self
            .0
            .peer_cls
            .__call__(pk)?
            .__call_method__("start", Args::new())?;
        *self.0.peer.lock() = peer;
        self.0.entered.store(true, Ordering::Release);
        let mut uk = Args::new();
        uk.push_keyword("port", ttyname(slave)?);
        for (k, v) in &self.0.uart_kwargs {
            uk.push_keyword(k, v.clone());
        }
        *self.0.proto.lock() = DefaultUARTProtocol::new(uk)?.into_arg();
        *self.0.policy.lock() = DefaultPolicy::new(Args::new())?.into_arg();
        if let Some(li) = self.0.liveness_interval {
            self.comm()?.__setattr__("liveness_interval", li)?;
        }
        self.comm()?
            .__call_method__("add_connection", Args::from(self.proto()))?;
        let mut a = Args::from(Arg::Str(format!("serial://{}", ttyname(slave)?).into()));
        a.push_positional(self.proto().__getattr__("peer_secret_key")?);
        *self.0.peer_gid.lock() = self.comm()?.__call_method__("add_peer", a)?;
        Ok(self.clone())
    }

    pub fn __exit__(&self, _args: impl Into<Args>) -> Result<()> {
        if !self.0.entered.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        let r = match self.comm() {
            Ok(c) => c.__call_method__("stop", Args::new()).map(|_| ()),
            Err(_) => Ok(()),
        };
        let peer = self.peer();
        if !peer.is_none() {
            let _ = peer.__call_method__("stop", Args::new());
        }
        for fd in core::mem::replace(&mut *self.0.fds.lock(), [-1; 2]) {
            close_fd(fd);
        }
        r
    }

    pub fn proto(&self) -> Arg {
        self.0.proto.lock().clone()
    }
    pub fn policy(&self) -> Arg {
        self.0.policy.lock().clone()
    }
    pub fn peer(&self) -> Arg {
        self.0.peer.lock().clone()
    }
    pub fn peer_gid(&self) -> Arg {
        self.0.peer_gid.lock().clone()
    }
    pub fn master(&self) -> Arg {
        Arg::Int(self.0.fds.lock()[0] as i64)
    }
    pub fn slave(&self) -> Arg {
        Arg::Int(self.0.fds.lock()[1] as i64)
    }
    pub fn comm(&self) -> Result<Arg> {
        comm_of(&self.policy())
    }
}

impl Drop for PtyPairState {
    fn drop(&mut self) {
        if self.entered.load(Ordering::Acquire) {
            if let Ok(c) = comm_of(&self.policy.lock().clone()) {
                let _ = c.__call_method__("stop", Args::new());
            }
            let peer = self.peer.lock().clone();
            if !peer.is_none() {
                let _ = peer.__call_method__("stop", Args::new());
            }
        }
        for fd in core::mem::replace(&mut *self.fds.lock(), [-1; 2]) {
            close_fd(fd);
        }
    }
}

impl laila::python::context::RaisingExit for UartPtyPair {
    fn __laila_exit(&self) -> Result<()> {
        self.__exit__(Args::new())
    }
}

impl PyObject for UartPtyPair {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "UartPtyPair"
    }
    fn __repr__(&self) -> String {
        String::from("<UartPtyPair>")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<UartPtyPair>()
            .is_some_and(|o| Arc::ptr_eq(&self.0, &o.0))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "proto" => self.proto(),
            "policy" => self.policy(),
            "peer" => self.peer(),
            "peer_gid" => self.peer_gid(),
            "master" => self.master(),
            "slave" => self.slave(),
            "comm" => self.comm()?,
            other => {
                return Err(LailaError::attribute_error(format!(
                    "'UartPtyPair' object has no attribute '{other}'"
                )))
            }
        })
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "__enter__" => Ok(self.__enter__()?.into_arg()),
            "__exit__" => self.__exit__(args).map(|()| Arg::Bool(false)),
            "comm" => self.comm(),
            other => self.__getattr__(other),
        }
    }
}

impl IntoArg for UartPtyPair {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

impl laila::python::object::PyArg for UartPtyPair {
    fn to_arg(&self) -> Arg {
        self.clone().into_arg()
    }
}

/// Unused-import guard for the byte queue type some helpers share.
type _Bytes = VecDeque<u8>;
