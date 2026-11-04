//! Hand-written twin of `tests/functional/policy/communication/unit_tests/test_carriers_broker_register.py`
//! (override `handwritten:`): the tiny pub/sub bus and the shared mailbox
//! pair the Python test builds by subclassing the carriers are a
//! [`BrokerBus`] and a [`RegisterBus`] installed on plain carrier
//! transports here.
#![allow(non_snake_case, non_upper_case_globals, dead_code, unused_variables, unused_imports, unused_mut, unused_assignments, clippy::all)]

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use laila::prelude::*;
use laila::macros::defaults::DefaultPolicy;
use laila::policy::central::communication::protocols::_carriers::broker::{
    self, BrokerBus, Sink, _BrokerRPCProtocol,
};
use laila::policy::central::communication::protocols::_carriers::register::{
    self, RegisterBus, _RegisterRPCProtocol,
};
use laila::policy::central::communication::protocols::base::{
    CarrierKind, FieldDefault, Protocol, ProtocolSpec, Wire, SPEC_DEFAULTS,
};
use laila::python::sync::{wait_until, Condvar, Mutex};
use laila::python::{Args, LailaError};

// ------------------------------------------------------------------ #
// Broker carrier over an in-process pub/sub bus
// ------------------------------------------------------------------ #

/// A subscription's delivery queue: messages are handed to the sink in
/// order, off the publisher's thread (Python: `loop.call_soon_threadsafe`).
struct Subscriber {
    id: usize,
    queue: Mutex<VecDeque<Vec<u8>>>,
    cv: Condvar,
    stop: AtomicBool,
}

impl Subscriber {
    fn start(self: &Arc<Self>, sink: Sink) {
        let me = self.clone();
        std::thread::spawn(move || loop {
            let next = {
                let mut g = me.queue.lock();
                wait_until(&me.cv, &mut g, Some(0.1), |q| !q.is_empty());
                g.pop_front()
            };
            match next {
                Some(data) => sink(data),
                None if me.stop.load(Ordering::Acquire) => break,
                None => {}
            }
        });
    }
}

/// `_BUS: dict = {}` -- topic -> subscribers.
static _BUS: std::sync::LazyLock<Mutex<HashMap<String, Vec<Arc<Subscriber>>>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));
static NEXT_ID: AtomicUsize = AtomicUsize::new(1);

/// `_BUS.clear()`
pub fn bus_clear() {
    for subs in _BUS.lock().drain() {
        for s in subs.1 {
            s.stop.store(true, Ordering::Release);
        }
    }
}

/// `_FakeBrokerProtocol`'s bus: `_broker_subscribe` / `_broker_publish` /
/// `_broker_close` over `_BUS`.
pub struct FakeBus {
    mine: Mutex<Vec<Arc<Subscriber>>>,
}

impl BrokerBus for FakeBus {
    fn subscribe(&self, topic: &str, sink: Sink) -> Result<()> {
        let sub = Arc::new(Subscriber {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            queue: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            stop: AtomicBool::new(false),
        });
        sub.start(sink);
        _BUS.lock().entry(topic.to_string()).or_default().push(sub.clone());
        self.mine.lock().push(sub);
        Ok(())
    }
    fn publish(&self, topic: &str, data: &[u8]) -> Result<()> {
        let subs: Vec<Arc<Subscriber>> = _BUS.lock().get(topic).cloned().unwrap_or_default();
        for sub in subs {
            sub.queue.lock().push_back(data.to_vec());
            sub.cv.notify_all();
        }
        Ok(())
    }
    fn close(&self) {
        let mine: Vec<Arc<Subscriber>> = self.mine.lock().drain(..).collect();
        let mut bus = _BUS.lock();
        for sub in mine {
            sub.stop.store(true, Ordering::Release);
            for subs in bus.values_mut() {
                subs.retain(|s| s.id != sub.id);
            }
        }
    }
}

pub static FAKE_BROKER_SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_FakeBrokerProtocol",
    module: "test_carriers_broker_register",
    protocol_name: "fakebroker",
    tokens: &["fakebroker"],
    schemes: &["fakebroker://"],
    carrier: CarrierKind::Broker,
    wire: Wire::None,
    bases: &["_BrokerRPCProtocol", "_CarrierRPCProtocol", "_LAILA_IDENTIFIABLE_COMM_PROTOCOL"],
    ..SPEC_DEFAULTS
};

pub struct _FakeBrokerProtocol;

impl _FakeBrokerProtocol {
    /// The class object (`factory=_FakeBrokerProtocol` in the teardown pairs).
    pub fn class_object() -> laila::python::PyClass {
        laila::python::PyClass::with_ctor(
            FAKE_BROKER_SPEC.class_name,
            FAKE_BROKER_SPEC.module,
            Arc::new(|a: Args| Self::new(a).map(laila::python::IntoArg::into_arg)),
        )
    }
    pub fn new(args: impl Into<Args>) -> Result<Protocol> {
        let proto = Protocol::new(&FAKE_BROKER_SPEC, args)?;
        broker::set_bus(&proto, Arc::new(FakeBus { mine: Mutex::new(Vec::new()) }));
        Ok(proto)
    }
}

impl laila::python::IntoArg for _FakeBrokerProtocol {
    fn into_arg(self) -> Arg {
        laila::python::IntoArg::into_arg(_FakeBrokerProtocol::class_object())
    }
}
impl laila::python::builtins::ClassValue for _FakeBrokerProtocol {
    fn class_value() -> Arg {
        laila::python::IntoArg::into_arg(_FakeBrokerProtocol::class_object())
    }
}
impl laila::python::PyArg for _FakeBrokerProtocol {
    fn to_arg(&self) -> Arg {
        laila::python::IntoArg::into_arg(_FakeBrokerProtocol::class_object())
    }
}

// ------------------------------------------------------------------ #
// Register carrier over a shared mailbox pair
// ------------------------------------------------------------------ #

/// `_LINKS: dict = {}` -- link id -> (`"a"` mailbox, `"b"` mailbox).
static _LINKS: std::sync::LazyLock<Mutex<HashMap<String, (VecDeque<Vec<u8>>, VecDeque<Vec<u8>>)>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

/// `_LINKS.clear()`
pub fn links_clear() {
    _LINKS.lock().clear();
}

/// `_FakeRegisterProtocol`'s bus: `_open_bus` / `_deliver` / `_poll_inbound`
/// over the shared mailbox pair.
pub struct FakeMailbox {
    link_id: String,
    side: String,
}

impl RegisterBus for FakeMailbox {
    fn open(&self) -> Result<()> {
        _LINKS.lock().entry(self.link_id.clone()).or_default();
        Ok(())
    }
    fn deliver(&self, data: &[u8]) -> Result<()> {
        let mut links = _LINKS.lock();
        let pair = links.entry(self.link_id.clone()).or_default();
        let other = if self.side == "a" { &mut pair.1 } else { &mut pair.0 };
        other.push_back(data.to_vec());
        Ok(())
    }
    fn poll_inbound(&self) -> Result<Option<Vec<u8>>> {
        let mut links = _LINKS.lock();
        let pair = links.entry(self.link_id.clone()).or_default();
        let mine = if self.side == "a" { &mut pair.0 } else { &mut pair.1 };
        Ok(mine.pop_front())
    }
}

pub static FAKE_REGISTER_SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_FakeRegisterProtocol",
    module: "test_carriers_broker_register",
    protocol_name: "fakereg",
    tokens: &["fakereg"],
    schemes: &["fakereg://"],
    carrier: CarrierKind::Register,
    wire: Wire::None,
    fields: &[("link_id", FieldDefault::Str("")), ("side", FieldDefault::Str("a"))],
    bases: &["_RegisterRPCProtocol", "_CarrierRPCProtocol", "_LAILA_IDENTIFIABLE_COMM_PROTOCOL"],
    ..SPEC_DEFAULTS
};

pub struct _FakeRegisterProtocol;

impl _FakeRegisterProtocol {
    /// The class object (`factory=_FakeRegisterProtocol` in the teardown pairs).
    pub fn class_object() -> laila::python::PyClass {
        laila::python::PyClass::with_ctor(
            FAKE_REGISTER_SPEC.class_name,
            FAKE_REGISTER_SPEC.module,
            Arc::new(|a: Args| Self::new(a).map(laila::python::IntoArg::into_arg)),
        )
    }
    pub fn new(args: impl Into<Args>) -> Result<Protocol> {
        let proto = Protocol::new(&FAKE_REGISTER_SPEC, args)?;
        register::set_bus(
            &proto,
            Arc::new(FakeMailbox {
                link_id: proto.str_field("link_id"),
                side: proto.str_field("side"),
            }),
        );
        Ok(proto)
    }
}

impl laila::python::IntoArg for _FakeRegisterProtocol {
    fn into_arg(self) -> Arg {
        laila::python::IntoArg::into_arg(_FakeRegisterProtocol::class_object())
    }
}
impl laila::python::builtins::ClassValue for _FakeRegisterProtocol {
    fn class_value() -> Arg {
        laila::python::IntoArg::into_arg(_FakeRegisterProtocol::class_object())
    }
}
impl laila::python::PyArg for _FakeRegisterProtocol {
    fn to_arg(&self) -> Arg {
        laila::python::IntoArg::into_arg(_FakeRegisterProtocol::class_object())
    }
}

laila::python! {
    #![unittest]
    #![functions(bus_clear, links_clear)]

    fn _mk(proto: Arg) -> Arg {
        let p = DefaultPolicy();
        p.central.communication.add_connection(proto);
        return p;
    }

    py_class!(TestBrokerCarrier);
    impl TestBrokerCarrier {
        fn setUp(&self) {
            bus_clear();
        }

        fn test_broker_echo_round_trip(&self) {
            let a = _mk(_FakeBrokerProtocol());
            let b = _mk(_FakeBrokerProtocol());
            try {
                let pb = next(iter(b.central.communication.connections.values()));
                let rid = a.central.communication.add_peer(
                    f "fakebroker://{b.global_id}", pb.peer_secret_key
                );
                self.assertEqual(rid, b.global_id);
                object.__setattr__(b, "echo", lambda m: f "b:{m}");
                self.assertEqual(a.central.communication.peers[b.global_id].echo("hi"), "b:hi");
            } finally {
                a.central.communication.stop();
                b.central.communication.stop();
            }
        }

        fn test_broker_wrong_secret_rejected(&self) {
            let a = _mk(_FakeBrokerProtocol(handshake_timeout=2.0));
            let b = _mk(_FakeBrokerProtocol());
            try {
                with self.assertRaises(ConnectionError) {
                    a.central.communication.add_peer(f "fakebroker://{b.global_id}", "nope");
                }
            } finally {
                a.central.communication.stop();
                b.central.communication.stop();
            }
        }
    }

    py_class!(TestRegisterCarrier);
    impl TestRegisterCarrier {
        fn setUp(&self) {
            links_clear();
        }

        fn test_register_echo_round_trip(&self) {
            let a = _mk(_FakeRegisterProtocol(link_id="L1", side="a", poll_interval=0.01));
            let b = _mk(_FakeRegisterProtocol(link_id="L1", side="b", poll_interval=0.01));
            try {
                let pb = next(iter(b.central.communication.connections.values()));
                let rid = a.central.communication.add_peer("fakereg://bus", pb.peer_secret_key);
                self.assertEqual(rid, b.global_id);
                object.__setattr__(b, "echo", lambda m: f "r:{m}");
                self.assertEqual(a.central.communication.peers[b.global_id].echo("hi"), "r:hi");
            } finally {
                a.central.communication.stop();
                b.central.communication.stop();
            }
        }
    }
}
