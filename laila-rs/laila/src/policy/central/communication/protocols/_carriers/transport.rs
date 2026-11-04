//! Wire-level state shared by the carriers: the bound endpoint, the stop
//! flag the background threads poll, and the per-wire handles
//! (`UdpSocket`, listener, datagram reliability tables).
//!
//! Rust shape of the Python private attrs `_server` / `_transport` /
//! `_endpoint` / `_bound_port` / `_bound_path` / `_event_loop`.

use alloc::string::String;
use alloc::sync::Arc;
use core::sync::atomic::AtomicBool;

use super::broker::{BrokerBus, BrokerState};
use super::datagram::{DatagramLink, DatagramState};
use super::p2p::P2PState;
use super::register::{RegisterBus, RegisterState};
use super::stream::Duplex;

/// A background thread's stop switch (`loop.stop()`).
pub type StopFlag = Arc<AtomicBool>;

/// The per-transport wire state behind `Protocol.transport`.
#[derive(Default)]
pub struct Transport {
    /// OS-assigned port after `start()` (IP transports).
    pub bound_port: Option<u16>,
    /// The listening socket path after `start()` (unix socket).
    pub bound_path: Option<String>,
    /// Set by `stop()`; the accept / receive / retransmit threads exit on
    /// their next poll.
    pub stop: Option<StopFlag>,
    /// The datagram endpoint (socket + reliability tables).
    pub datagram: Option<Arc<DatagramState>>,
    /// A harness / driver packet link installed before `start()` (the
    /// `_create_datagram_endpoint` result).
    pub datagram_link: Option<Arc<dyn DatagramLink>>,
    /// The listener's local address for the self-connect that unblocks a
    /// blocking accept (`"host:port"` or a unix path).
    pub listener_addr: Option<String>,
    /// The point-to-point link (`_link` / `_reader`) once started.
    pub p2p: Option<Arc<P2PState>>,
    /// A harness / driver duplex installed before `start()` (the
    /// `_open_stream` result).
    pub p2p_link: Option<Arc<dyn Duplex>>,
    /// The broker client (`_inbox` + bus) once started.
    pub broker: Option<Arc<BrokerState>>,
    /// A harness / driver pub-sub bus installed before `start()`.
    pub broker_bus: Option<Arc<dyn BrokerBus>>,
    /// The register-bus poller once started.
    pub register: Option<Arc<RegisterState>>,
    /// A harness / driver mailbox bus installed before `start()`.
    pub register_bus: Option<Arc<dyn RegisterBus>>,
    /// The receive / retransmit / poll threads of the current lifecycle
    /// (the Python loop thread `stop()` joins): a restart must not race a
    /// reader of the previous lifecycle still draining the endpoint.
    #[cfg(feature = "std")]
    pub threads: alloc::vec::Vec<std::thread::JoinHandle<()>>,
}

/// Run *body* on a named background thread owned by *proto*'s current
/// lifecycle (joined by [`join_loops`] on `stop()`).
pub fn spawn_loop(
    proto: &super::super::base::Protocol,
    name: String,
    body: impl FnOnce() + Send + 'static,
) -> crate::python::Result<()> {
    #[cfg(feature = "std")]
    {
        let handle = crate::python::stdlib::spawn_native_thread(name, body).map_err(|e| {
            crate::python::LailaError::runtime_error(alloc::format!("can't start new thread: {e}"))
        })?;
        proto.0.transport.lock().threads.push(handle);
        Ok(())
    }
    #[cfg(not(feature = "std"))]
    {
        let _ = proto;
        crate::python::sync::spawn_named(name, body)
    }
}

/// Join the lifecycle's threads after the stop flag is raised (a thread
/// stopping its own transport skips itself).
pub fn join_loops(proto: &super::super::base::Protocol) {
    #[cfg(feature = "std")]
    {
        let handles: alloc::vec::Vec<std::thread::JoinHandle<()>> =
            core::mem::take(&mut proto.0.transport.lock().threads);
        let me = std::thread::current().id();
        for h in handles {
            if h.thread().id() == me {
                continue;
            }
            let _ = h.join();
        }
    }
    #[cfg(not(feature = "std"))]
    {
        let _ = proto;
    }
}

impl Transport {
    pub fn bound_port(&self) -> Option<u16> {
        self.bound_port
    }
    pub fn bound_path(&self) -> Option<String> {
        self.bound_path.clone()
    }
    pub fn set_bound_port(&mut self, port: Option<u16>) {
        self.bound_port = port;
    }
    /// A fresh stop flag for a new lifecycle.
    pub fn fresh_stop(&mut self) -> StopFlag {
        let flag = Arc::new(AtomicBool::new(false));
        self.stop = Some(flag.clone());
        flag
    }
    /// Raise the stop flag (idempotent) and return it.
    pub fn raise_stop(&mut self) -> Option<StopFlag> {
        if let Some(f) = &self.stop {
            f.store(true, core::sync::atomic::Ordering::Release);
        }
        self.stop.take()
    }
    /// Reset every wire handle (after `stop()`).
    pub fn clear(&mut self) {
        self.bound_port = None;
        self.bound_path = None;
        self.stop = None;
        self.datagram = None;
        self.listener_addr = None;
        self.p2p = None;
        self.broker = None;
        self.register = None;
    }
}
