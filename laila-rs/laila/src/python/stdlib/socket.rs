//! `import socket` -- the slice the test suites use: an ephemeral-port
//! probe (`socket.socket(AF_INET, SOCK_STREAM)`, `bind(("127.0.0.1", 0))`,
//! `getsockname()[1]`, `close()`), spelled as in Python. The laila
//! transports themselves live on `std::net` / `mio` behind the protocol
//! classes; this module is only the stdlib surface a test body reaches.

use alloc::string::String;
use alloc::sync::Arc;

use crate::python::args::Args;
use crate::python::errors::{LailaError, Result};
use crate::python::object::{Arg, IntoArg, PyArg, PyObject};
use crate::python::sync::Mutex;

/// `socket.AF_INET`
pub const AF_INET: i64 = 2;
/// `socket.AF_INET6`
pub const AF_INET6: i64 = 10;
/// `socket.SOCK_STREAM`
pub const SOCK_STREAM: i64 = 1;
/// `socket.SOCK_DGRAM`
pub const SOCK_DGRAM: i64 = 2;
/// `socket.SOL_SOCKET`
pub const SOL_SOCKET: i64 = 1;
/// `socket.SO_REUSEADDR`
pub const SO_REUSEADDR: i64 = 2;

enum Bound {
    Unbound,
    Tcp(std::net::TcpListener),
    Udp(std::net::UdpSocket),
    Closed,
}

/// `socket.socket(family, type)`: a handle; clones share one socket.
#[derive(Clone)]
pub struct Socket(Arc<Mutex<Bound>>, i64, i64);

impl Socket {
    pub fn new(family: i64, kind: i64) -> Socket {
        Socket(Arc::new(Mutex::new(Bound::Unbound)), family, kind)
    }
    /// `sock.bind((host, port))`
    pub fn bind(&self, args: impl Into<Args>) -> Result<()> {
        let a: Args = args.into();
        let addr = a.positional.first().cloned().unwrap_or(Arg::None);
        let host = addr.__getitem__(Arg::Int(0))?.__str__();
        let port = addr.__getitem__(Arg::Int(1))?.as_i64().unwrap_or(0);
        let target = alloc::format!("{host}:{port}");
        let bound = if self.2 == SOCK_DGRAM {
            std::net::UdpSocket::bind(&target).map(Bound::Udp)
        } else {
            std::net::TcpListener::bind(&target).map(Bound::Tcp)
        }
        .map_err(|e| {
            LailaError::new(
                crate::python::errors::ExcKind::OSError,
                alloc::format!("{e}"),
            )
        })?;
        *self.0.lock() = bound;
        Ok(())
    }
    /// `sock.listen(backlog=..)` (a bound `TcpListener` already listens).
    pub fn listen(&self, _args: impl Into<Args>) -> Result<()> {
        Ok(())
    }
    /// `sock.setsockopt(level, opt, value)` -- accepted, no-op.
    pub fn setsockopt(&self, _args: impl Into<Args>) -> Result<()> {
        Ok(())
    }
    /// `sock.settimeout(t)` -- accepted, no-op.
    pub fn settimeout(&self, _args: impl Into<Args>) -> Result<()> {
        Ok(())
    }
    /// `sock.getsockname()` -> `(host, port)`
    pub fn getsockname(&self) -> Result<Arg> {
        let addr = match &*self.0.lock() {
            Bound::Tcp(l) => l.local_addr(),
            Bound::Udp(u) => u.local_addr(),
            Bound::Unbound => return Ok(("0.0.0.0", 0).into_arg()),
            Bound::Closed => {
                return Err(LailaError::new(
                    crate::python::errors::ExcKind::OSError,
                    "[Errno 9] Bad file descriptor",
                ));
            }
        }
        .map_err(|e| {
            LailaError::new(
                crate::python::errors::ExcKind::OSError,
                alloc::format!("{e}"),
            )
        })?;
        Ok((addr.ip().to_string(), addr.port() as i64).into_arg())
    }
    /// `sock.close()`
    pub fn close(&self, _args: impl Into<Args>) {
        *self.0.lock() = Bound::Closed;
    }
    /// `sock.fileno()` -- `-1` once closed, as in Python.
    pub fn fileno(&self) -> i64 {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            match &*self.0.lock() {
                Bound::Tcp(l) => l.as_raw_fd() as i64,
                Bound::Udp(u) => u.as_raw_fd() as i64,
                _ => -1,
            }
        }
        #[cfg(not(unix))]
        {
            -1
        }
    }
    /// `with socket.socket(..) as s:`
    pub fn __enter__(&self) -> Socket {
        self.clone()
    }
    pub fn __exit__(&self, _args: impl Into<Args>) -> bool {
        self.close(Args::new());
        false
    }
}

impl core::fmt::Debug for Socket {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "<socket.socket family={} type={}>", self.1, self.2)
    }
}

impl PyObject for Socket {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "socket"
    }
    fn __repr__(&self) -> String {
        alloc::format!("{self:?}")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Socket>()
            .is_some_and(|o| Arc::ptr_eq(&o.0, &self.0))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "family" => Ok(Arg::Int(self.1)),
            "type" => Ok(Arg::Int(self.2)),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'socket' object has no attribute '{other}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "bind" => self.bind(args).map(|()| Arg::None),
            "listen" => self.listen(args).map(|()| Arg::None),
            "setsockopt" => self.setsockopt(args).map(|()| Arg::None),
            "settimeout" => self.settimeout(args).map(|()| Arg::None),
            "getsockname" => self.getsockname(),
            "close" => {
                self.close(args);
                Ok(Arg::None)
            }
            "fileno" => Ok(Arg::Int(self.fileno())),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'socket' object has no attribute '{other}'"
            ))),
        }
    }
    fn __bool__(&self) -> bool {
        true
    }
}

impl IntoArg for Socket {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for Socket {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}

/// `import socket`
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct socket_mod;
/// `socket` module object.
#[allow(non_upper_case_globals)]
pub static socket: socket_mod = socket_mod;
#[allow(non_snake_case)]
impl socket_mod {
    pub fn AF_INET(&self) -> i64 {
        AF_INET
    }
    pub fn AF_INET6(&self) -> i64 {
        AF_INET6
    }
    pub fn SOCK_STREAM(&self) -> i64 {
        SOCK_STREAM
    }
    pub fn SOCK_DGRAM(&self) -> i64 {
        SOCK_DGRAM
    }
    pub fn SOL_SOCKET(&self) -> i64 {
        SOL_SOCKET
    }
    pub fn SO_REUSEADDR(&self) -> i64 {
        SO_REUSEADDR
    }
    /// `socket.socket(family=AF_INET, type=SOCK_STREAM, ..)`
    pub fn socket(&self, args: impl Into<Args>) -> Socket {
        let a: Args = args.into();
        let family = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("family"))
            .as_i64()
            .unwrap_or(AF_INET);
        let kind = a
            .positional
            .get(1)
            .cloned()
            .unwrap_or_else(|| a.get("type"))
            .as_i64()
            .unwrap_or(SOCK_STREAM);
        Socket::new(family, kind)
    }
    /// `socket.gethostname()`
    pub fn gethostname(&self) -> String {
        std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".into())
    }
}
