//! Hand-written twin of `tests/functional/policy/communication/unit_tests/test_p2p_and_dep_transports.py`
//! (override `handwritten:`): the in-memory connected duplex the Python
//! test builds by subclassing `_P2PStreamRPCProtocol` is a [`Duplex`] link
//! installed on a plain point-to-point transport here. The
//! dependency-bound transport checks are the generated ones.
#![allow(non_snake_case, non_upper_case_globals, dead_code, unused_variables, unused_imports, unused_mut, unused_assignments, clippy::all)]

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use laila::prelude::*;
use laila::macros::defaults::{DefaultAMQPProtocol, DefaultCANProtocol, DefaultMQTTProtocol, DefaultPolicy, DefaultRS232Protocol, DefaultRS485Protocol, DefaultUARTProtocol, DefaultZeroMQProtocol};
use laila::macros::defaults::{DefaultENIPProtocol, DefaultI2CProtocol, DefaultModbusTCPProtocol, DefaultSPIProtocol};
use laila::macros::defaults::{DefaultSatelliteProtocol, DefaultSigfoxProtocol, DefaultUSBProtocol};
use laila::policy::central::communication::protocols::_carriers::p2p::{self, _P2PStreamRPCProtocol};
use laila::policy::central::communication::protocols::_carriers::stream::Duplex;
use laila::policy::central::communication::protocols::base::{
    CarrierKind, FieldDefault, Protocol, ProtocolSpec, Wire, SPEC_DEFAULTS,
};
use laila::python::sync::{wait_until, Condvar, Mutex};
use laila::python::{Args, LailaError};

// ------------------------------------------------------------------ #
// P2P carrier over an in-memory connected duplex
// ------------------------------------------------------------------ #

/// One side's inbound byte queue (the Python `asyncio.StreamReader`).
pub struct Pipe {
    inbox: Mutex<VecDeque<u8>>,
    cv: Condvar,
}

/// `_P2P_LINKS: dict = {}` -- link id -> side -> that side's reader.
static _P2P_LINKS: std::sync::LazyLock<Mutex<HashMap<String, HashMap<String, Arc<Pipe>>>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

/// `_P2P_LINKS.clear()`
pub fn p2p_links_clear() {
    _P2P_LINKS.lock().clear();
}

/// `_FakeWriter` + the reader: `_open_stream` registers this side's reader
/// under the link id; `write` feeds the other side's reader.
pub struct FakeDuplex {
    link_id: String,
    side: String,
    reader: Arc<Pipe>,
    closed: AtomicBool,
}

impl Duplex for FakeDuplex {
    fn open(&self) -> std::io::Result<()> {
        self.closed.store(false, Ordering::Release);
        self.reader.inbox.lock().clear();
        _P2P_LINKS
            .lock()
            .entry(self.link_id.clone())
            .or_default()
            .insert(self.side.clone(), self.reader.clone());
        Ok(())
    }
    fn read(&self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut g = self.reader.inbox.lock();
        wait_until(&self.reader.cv, &mut g, Some(0.05), |q| !q.is_empty());
        let mut n = 0;
        while n < buf.len() {
            match g.pop_front() {
                Some(b) => {
                    buf[n] = b;
                    n += 1;
                }
                None => break,
            }
        }
        Ok(n)
    }
    fn write(&self, data: &[u8]) -> std::io::Result<usize> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(data.len());
        }
        let other = if self.side == "a" { "b" } else { "a" };
        let entry = _P2P_LINKS
            .lock()
            .get(&self.link_id)
            .and_then(|m| m.get(other).cloned());
        if let Some(reader) = entry {
            reader.inbox.lock().extend(data.iter().copied());
            reader.cv.notify_all();
        }
        Ok(data.len())
    }
    fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        self.reader.cv.notify_all();
    }
    fn polls(&self) -> bool {
        true
    }
}

pub static FAKE_P2P_SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_FakeP2PProtocol",
    module: "test_p2p_and_dep_transports",
    protocol_name: "fakep2p",
    tokens: &["fakep2p"],
    schemes: &["fakep2p://"],
    carrier: CarrierKind::P2P,
    wire: Wire::None,
    supports_channels: true,
    fields: &[("stream_chunk_bytes", FieldDefault::Int(4096)), ("link_id", FieldDefault::Str("")), ("side", FieldDefault::Str("a"))],
    bases: &["_P2PStreamRPCProtocol", "_CarrierRPCProtocol", "_LAILA_IDENTIFIABLE_COMM_PROTOCOL"],
    ..SPEC_DEFAULTS
};

pub struct _FakeP2PProtocol;

impl _FakeP2PProtocol {
    /// The class object (`factory=_FakeP2PProtocol` in the teardown pairs).
    pub fn class_object() -> laila::python::PyClass {
        laila::python::PyClass::with_ctor(
            FAKE_P2P_SPEC.class_name,
            FAKE_P2P_SPEC.module,
            Arc::new(|a: Args| Self::new(a).map(laila::python::IntoArg::into_arg)),
        )
    }
    pub fn new(args: impl Into<Args>) -> Result<Protocol> {
        let proto = Protocol::new(&FAKE_P2P_SPEC, args)?;
        p2p::set_link(
            &proto,
            Arc::new(FakeDuplex {
                link_id: proto.str_field("link_id"),
                side: proto.str_field("side"),
                reader: Arc::new(Pipe {
                    inbox: Mutex::new(VecDeque::new()),
                    cv: Condvar::new(),
                }),
                closed: AtomicBool::new(false),
            }),
        );
        Ok(proto)
    }
}

impl laila::python::IntoArg for _FakeP2PProtocol {
    fn into_arg(self) -> Arg {
        laila::python::IntoArg::into_arg(_FakeP2PProtocol::class_object())
    }
}
impl laila::python::builtins::ClassValue for _FakeP2PProtocol {
    fn class_value() -> Arg {
        laila::python::IntoArg::into_arg(_FakeP2PProtocol::class_object())
    }
}
impl laila::python::PyArg for _FakeP2PProtocol {
    fn to_arg(&self) -> Arg {
        laila::python::IntoArg::into_arg(_FakeP2PProtocol::class_object())
    }
}

laila::python! {
    #![unittest]
    #![functions(p2p_links_clear)]

    fn _mk(proto: Arg) -> Arg {
        let p = DefaultPolicy();
        p.central.communication.add_connection(proto);
        return p;
    }

    py_class!(TestP2PCarrier);
    impl TestP2PCarrier {
        fn setUp(&self) {
            p2p_links_clear();
        }

        fn test_p2p_echo_round_trip(&self) {
            let a = _mk(_FakeP2PProtocol(link_id="L", side="a"));
            let b = _mk(_FakeP2PProtocol(link_id="L", side="b"));
            try {
                let pb = next(iter(b.central.communication.connections.values()));
                let rid = a.central.communication.add_peer("fakep2p://bus", pb.peer_secret_key);
                self.assertEqual(rid, b.global_id);
                object.__setattr__(b, "echo", lambda m: f "p2p:{m}");
                self.assertEqual(a.central.communication.peers[b.global_id].echo("hi"), "p2p:hi");
            } finally {
                a.central.communication.stop();
                b.central.communication.stop();
            }
        }
    }
    // ------------------------------------------------------------------ #
    // Dependency-bound transports: routing + capability errors
    // ------------------------------------------------------------------ #

    py_class!(TestSerialTransportsRouting);
    impl TestSerialTransportsRouting {
        fn test_tokens_and_uris_disjoint(&self) {
            self.assertTrue(DefaultUARTProtocol.matches_token("serial"));
            self.assertTrue(DefaultUARTProtocol.can_handle_uri("serial:///dev/ttyUSB0"));
            self.assertTrue(DefaultRS232Protocol.matches_token("rs-232"));
            self.assertTrue(DefaultRS232Protocol.can_handle_uri("rs232:///dev/ttyS0"));
            self.assertTrue(DefaultRS485Protocol.matches_token("rs485"));
            self.assertFalse(DefaultUARTProtocol.matches_token("rs485"));
        }

        fn test_uart_without_port_raises_clear_error(&self) {
            let proto = DefaultUARTProtocol();  // no port set
            with self.assertRaises(RuntimeError) as ctx {
                proto.start();
            }
            self.assertIn("port", str(ctx.exception).lower());
            proto.stop();
        }

        fn test_serial_modem_transports_route(&self) {
            // from laila.macros.defaults import DefaultSatelliteProtocol, DefaultSigfoxProtocol, DefaultUSBProtocol

            self.assertTrue(DefaultSigfoxProtocol.can_handle_uri("sigfox:///dev/ttyUSB0"));
            self.assertTrue(DefaultSatelliteProtocol.matches_token("iridium"));
            self.assertTrue(DefaultUSBProtocol.can_handle_uri("usb:///dev/ttyACM0"));
            // serial-backed; without a port they raise the same clear error
            // py2rs: `for factory in (DefaultSigfoxProtocol, DefaultSatelliteProtocol, DefaultUSBProtocol)` unrolled, one block per class [P19d]
            {
                let proto = DefaultSigfoxProtocol();
                with self.assertRaises(RuntimeError) {
                    proto.start();
                }
                proto.stop();
            }
            {
                let proto = DefaultSatelliteProtocol();
                with self.assertRaises(RuntimeError) {
                    proto.start();
                }
                proto.stop();
            }
            {
                let proto = DefaultUSBProtocol();
                with self.assertRaises(RuntimeError) {
                    proto.start();
                }
                proto.stop();
            }
        }
    }

    py_class!(TestCapabilityErrors);
    impl TestCapabilityErrors {
        fn test_can_missing_driver_raises_install_hint(&self) {
            let proto = DefaultCANProtocol(channel="vcan0");
            try {
                with self.assertRaises(RuntimeError) as ctx {
                    proto.start();
                }
                self.assertIn("laila-core[can]", str(ctx.exception));
            } finally {
                proto.stop();
            }
        }

        fn test_mqtt_missing_driver_raises_install_hint(&self) {
            let proto = DefaultMQTTProtocol();
            try {
                with self.assertRaises(RuntimeError) as ctx {
                    proto.start();
                }
                self.assertIn("laila-core[mqtt]", str(ctx.exception));
            } finally {
                proto.stop();
            }
        }

        fn test_amqp_missing_driver_raises_install_hint(&self) {
            let proto = DefaultAMQPProtocol();
            try {
                with self.assertRaises(RuntimeError) as ctx {
                    proto.start();
                }
                self.assertIn("laila-core[amqp]", str(ctx.exception));
            } finally {
                proto.stop();
            }
        }

        fn test_zeromq_missing_driver_raises_install_hint(&self) {
            let proto = DefaultZeroMQProtocol();
            try {
                with self.assertRaises(RuntimeError) as ctx {
                    proto.start();
                }
                self.assertIn("laila-core[zmq]", str(ctx.exception));
            } finally {
                proto.stop();
            }
        }

        fn test_register_bus_missing_driver_raises_install_hint(&self) {
            // from laila.macros.defaults import DefaultENIPProtocol, DefaultI2CProtocol, DefaultModbusTCPProtocol, DefaultSPIProtocol

            let cases = [
                (DefaultModbusTCPProtocol, "laila-core[modbus]"),
                (DefaultI2CProtocol, "laila-core[i2c]"),
                (DefaultSPIProtocol, "laila-core[spi]"),
                (DefaultENIPProtocol, "laila-core[enip]"),
            ];
            for (factory, hint) in cases {
                let proto = factory();
                try {
                    with self.assertRaises(RuntimeError) as ctx {
                        proto.start();
                    }
                    self.assertIn(hint, str(ctx.exception));
                } finally {
                    proto.stop();
                }
            }
        }
    }
}
