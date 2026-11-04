//! Hand-written twin of `tests/functional/policy/communication/unit_tests/test_graceful_teardown.py`
//! (override `handwritten:`). The wire transports run for real on
//! localhost; the broker / register / datagram / point-to-point carriers
//! run over the Rust fakes of the sibling modules (`test_carriers`,
//! `test_carriers_broker_register`, `test_p2p_and_dep_transports`).
//!
//! Differences from the Python module:
//! - `_AsyncioErrors` has nothing to capture (the Rust carriers run on
//!   plain threads, there is no asyncio loop to install an exception
//!   handler on); the probe is kept as a no-op so the test bodies read the
//!   same.
//! - `TestUartGoodbye` (a pty + the streaming suite's fake JSON-RPC peer)
//!   lives with the streaming suite's `_helpers` twin.
#![allow(non_snake_case, non_upper_case_globals, dead_code, unused_variables, unused_imports, unused_mut, unused_assignments, clippy::all)]

use laila::prelude::*;
use laila::macros::defaults::DefaultUARTProtocol;
use laila::macros::defaults::{DefaultLoopbackProtocol, DefaultPolicy, DefaultTCPIPProtocol, DefaultTCPProtocol, DefaultUDPProtocol, DefaultUnixSocketProtocol};
use laila::policy::central::communication::protocols::_carriers::loopthread;

use super::test_carriers::_LossyDatagramProtocol;
use super::test_carriers_broker_register::{_FakeBrokerProtocol, _FakeRegisterProtocol};
use super::test_p2p_and_dep_transports::_FakeP2PProtocol;

/// `_reset_fakes()`
fn _reset_fakes() {
    super::test_carriers_broker_register::bus_clear();
    super::test_carriers_broker_register::links_clear();
    super::test_carriers::ether_clear();
    super::test_p2p_and_dep_transports::p2p_links_clear();
}

laila::python! {
    #![unittest]
    #![modules(loopthread)]
    #![functions(_reset_fakes)]

    //: liveness must never be what drops a peer in these tests

    static _NO_LIVENESS: ::std::sync::LazyLock<Arg> = ::std::sync::LazyLock::new(|| __laila_const__NO_LIVENESS().expect("module constant _NO_LIVENESS"));
    fn __laila_const__NO_LIVENESS() -> Arg {
        return 600.0;
    }

    // `_helpers.wait_until`: poll *pred* until it holds or *timeout* elapses.
    #[pysig("wait_until(pred, timeout=5.0, step=0.01)")]
    fn wait_until(args: Args) -> bool {
        let deadline = time.monotonic() + timeout;
        while time.monotonic() < deadline {
            if pred() {
                return True;
            }
            time.sleep(step);
        }
        return pred();
    }

    // ----------------------------------------------------------------------
    // probes
    // ----------------------------------------------------------------------

    py_class!(_AsyncioErrors; init);
    impl _AsyncioErrors {
        // Capture loop-level exceptions and ``asyncio``/``websockets`` ERROR logs.
        // (No asyncio loop in the Rust carriers: nothing is ever captured.)

        #[pysig("__init__(self)")]
        fn __init__(&self, args: Args) -> () {
            self.contexts = [];
            self.records = [];
        }

        #[pysig("watch(self, *protos)")]
        fn watch(&self, args: Args) -> () {
            return;
        }

        fn __enter__(&self) -> Arg {
            return self;
        }

        #[pysig("__exit__(self, *exc)")]
        fn __exit__(&self, args: Args) -> () {
            return;
        }

        fn report(&self) -> Arg {
            // (`[f"{c.get('message')}: {c.get('exception')!r}" for c in
            // self.contexts]` -- always empty in Rust, see the module doc.)
            let out: Arg = [];
            return out;
        }
    }

    fn _open_fds() -> Arg {
        gc.collect();
        try {
            return set(os.listdir("/proc/self/fd"));
        } except OSError {
            return set();
        }
    }
    // ----------------------------------------------------------------------
    // transport harnesses: build (pa, pb) and the URI A uses to reach B
    // ----------------------------------------------------------------------

    py_class!(_Pair; init);
    impl _Pair {
        // Two policies, two protocol instances, A peered to B.

        #[pysig("__init__(self, name, factory, uri_fn, kw_a, kw_b, reset=None)")]
        fn __init__(&self, args: Args) -> () {
            self.name = name;
            self.factory = factory;
            self.uri_fn = uri_fn;
            (self.kw_a, self.kw_b) = (kw_a, kw_b);
            self.reset = reset;
            self.a = None;
            self.b = None;
            self.pa = None;
            self.pb = None;
        }

        fn build(&self) -> Arg {
            if self.reset is not None {
                self.reset();
            }
            self.pa = self.factory(**self.kw_a);
            self.pb = self.factory(**self.kw_b);
            (self.a, self.b) = (DefaultPolicy(), DefaultPolicy());
            for pol in (self.a, self.b) {
                pol.central.communication.liveness_interval = _NO_LIVENESS;
            }
            self.a.central.communication.add_connection(self.pa);
            self.b.central.communication.add_connection(self.pb);
            return self;
        }

        #[property]
        fn ca(&self) -> Arg {
            return self.a.central.communication;
        }

        #[property]
        fn cb(&self) -> Arg {
            return self.b.central.communication;
        }

        fn uri(&self) -> String {
            return self.uri_fn(self.pb, self.b);
        }

        fn peer(&self) -> () {
            let bid = self.ca.add_peer(self.uri(), self.pb.peer_secret_key);
            assert(bid == self.b.global_id);
            assert(wait_until(lambda: self.a.global_id in self.cb.peers, timeout=5), f "{self.name}: B never registered A");
            assert(self.pa.ping(self.b.global_id), f "{self.name}: ping A->B failed");
        }

        fn stop_all(&self) -> () {
            for comm in (self.ca, self.cb) {
                try {
                    comm.stop();
                } except Exception {
                }
            }
        }
    }

    static PAIRS: ::std::sync::LazyLock<Arg> = ::std::sync::LazyLock::new(|| __laila_const_PAIRS().expect("module constant PAIRS"));
    fn __laila_const_PAIRS() -> Arg {
        return {
            "loopback": dict(
                factory=DefaultLoopbackProtocol,
                uri_fn=lambda pb, b: f "loopback://{b.global_id}",
                kw_a={},
                kw_b={},
            ),
            "tcp": dict(
                factory=DefaultTCPProtocol,
                uri_fn=lambda pb, b: f "tcp://127.0.0.1:{pb.bound_port}",
                kw_a={"host": "127.0.0.1"},
                kw_b={"host": "127.0.0.1"},
            ),
            "unix": dict(
                factory=DefaultUnixSocketProtocol,
                uri_fn=lambda pb, b: f "unix://{pb.bound_path}",
                kw_a={},
                kw_b={},
            ),
            "udp": dict(
                factory=DefaultUDPProtocol,
                uri_fn=lambda pb, b: f "udp://127.0.0.1:{pb.bound_port}",
                kw_a={"host": "127.0.0.1"},
                kw_b={"host": "127.0.0.1"},
            ),
            "tcpip-ws": dict(
                factory=DefaultTCPIPProtocol,
                uri_fn=lambda pb, b: f "ws://127.0.0.1:{pb.bound_port}",
                kw_a={"host": "127.0.0.1"},
                kw_b={"host": "127.0.0.1"},
            ),
            "broker(fake)": dict(
                factory=_FakeBrokerProtocol,
                uri_fn=lambda pb, b: f "fakebroker://{b.global_id}",
                kw_a={},
                kw_b={},
                reset=_reset_fakes,
            ),
            "register(fake)": dict(
                factory=_FakeRegisterProtocol,
                uri_fn=lambda pb, b: "fakereg://bus",
                kw_a={"link_id": "GT", "side": "a", "poll_interval": 0.005},
                kw_b={"link_id": "GT", "side": "b", "poll_interval": 0.005},
                reset=_reset_fakes,
            ),
            "datagram(fake)": dict(
                factory=_LossyDatagramProtocol,
                uri_fn=lambda pb, b: "fakedg://B",
                kw_a={"addr": "A", "mtu": 64},
                kw_b={"addr": "B", "mtu": 64},
                reset=_reset_fakes,
            ),
            "p2p(fake)": dict(
                factory=_FakeP2PProtocol,
                uri_fn=lambda pb, b: "fakep2p://bus",
                kw_a={"link_id": "GT", "side": "a"},
                kw_b={"link_id": "GT", "side": "b"},
                reset=_reset_fakes,
            ),
        };
    }

    fn _pair(name: String) -> Arg {
        return _Pair(name, **PAIRS[name]).build();
    }

    fn _assert_proto_down(tc: Arg, proto: Arg, label: String) -> () {
        tc.assertFalse(proto._started, f "{label}: still started");
        tc.assertEqual(getattr(proto, "_connections", {}), {}, f "{label}: connections left");
        let r#loop = getattr(proto, "_event_loop", None);
        tc.assertIsNone(r#loop, f "{label}: event loop not released");
        tc.assertIsNone(getattr(proto, "_loop_thread", None), f "{label}: loop thread not joined");
    }
    // ----------------------------------------------------------------------
    // tests
    // ----------------------------------------------------------------------

    py_class!(TestGracefulTeardownAllTransports);
    impl TestGracefulTeardownAllTransports {
        fn setUp(&self) {
            _reset_fakes();
            laila.terminate();
        }

        fn tearDown(&self) {
            laila.terminate();
            _reset_fakes();
        }
        // 1 -----------------------------------------------------------------

        fn test_stop_is_clean_and_leaks_nothing(&self) {
            for name in PAIRS {
                with self.subTest(transport=name) {
                    let pr = _pair(name);
                    try {
                        // cycle 1: peer + stop (absorbs any lazily created resources)
                        pr.peer();
                        pr.stop_all();
                        let baseline = _open_fds();
                        // cycle 2: restart the very same protocol instances
                        pr.pa.start();
                        pr.pb.start();
                        pr.peer();
                        let (aid, bid) = (pr.a.global_id, pr.b.global_id);
                        self.assertIn(bid, laila._remote_policies);
                        with _AsyncioErrors() as probe {
                            probe.watch(pr.pa, pr.pb);
                            pr.stop_all();
                            time.sleep(0.2);
                        }
                        self.assertEqual(probe.report(), [], f "{name}: asyncio errors on stop");
                        for proto in (pr.pa, pr.pb) {
                            _assert_proto_down(self, proto, name);
                        }
                        self.assertEqual(dict(pr.ca.peers), {});
                        self.assertEqual(dict(pr.cb.peers), {});
                        self.assertNotIn(aid, laila._remote_policies);
                        self.assertNotIn(bid, laila._remote_policies);
                        self.assertIsNone(pr.ca._liveness_thread);
                        let leaked = _open_fds() - baseline;
                        self.assertEqual(leaked, set(), f "{name}: fds leaked per lifecycle");
                    } finally {
                        pr.stop_all();
                    }
                }
            }
        }
        // 2 -----------------------------------------------------------------

        fn test_remove_peer_is_noticed_by_remote(&self) {
            for name in PAIRS {
                with self.subTest(transport=name) {
                    let pr = _pair(name);
                    try {
                        pr.peer();
                        let (aid, bid) = (pr.a.global_id, pr.b.global_id);
                        with _AsyncioErrors() as probe {
                            probe.watch(pr.pa, pr.pb);
                            let t0 = time.monotonic();
                            pr.ca.remove_peer(bid);
                            self.assertNotIn(bid, pr.ca.peers);
                            self.assertFalse(pr.pa.has_peer(bid));
                            let noticed = wait_until(lambda: aid not in pr.cb.peers, timeout=3.0);
                            let dt = time.monotonic() - t0;
                        }
                        self.assertTrue(noticed, f "{name}: remote kept us after remove_peer");
                        self.assertLess(dt, 3.0);
                        self.assertFalse(pr.pb.has_peer(aid));
                        self.assertEqual(probe.report(), []);
                        // both transports are still up; re-peer works
                        self.assertTrue(pr.pa._started and pr.pb._started);
                        pr.peer();
                        self.assertIn(aid, pr.cb.peers);
                    } finally {
                        pr.stop_all();
                    }
                }
            }
        }
        // 3 -----------------------------------------------------------------

        fn test_one_sided_stop_is_noticed_by_remote(&self) {
            for name in PAIRS {
                with self.subTest(transport=name) {
                    let pr = _pair(name);
                    try {
                        pr.peer();
                        let (aid, bid) = (pr.a.global_id, pr.b.global_id);
                        with _AsyncioErrors() as probe {
                            probe.watch(pr.pa, pr.pb);
                            pr.ca.stop();
                            let noticed = wait_until(lambda: aid not in pr.cb.peers, timeout=3.0);
                            time.sleep(0.1);
                        }
                        self.assertTrue(noticed, f "{name}: remote kept us after one-sided stop");
                        self.assertFalse(pr.pb.has_peer(aid));
                        self.assertEqual(probe.report(), []);
                        _assert_proto_down(self, pr.pa, name);
                        // B is untouched and still serves
                        self.assertTrue(pr.pb._started);
                        self.assertNotIn(bid, pr.ca.peers);
                    } finally {
                        pr.stop_all();
                    }
                }
            }
        }
        // 4 -----------------------------------------------------------------

        fn test_send_after_stop_raises_connection_error(&self) {
            for name in PAIRS {
                with self.subTest(transport=name) {
                    let pr = _pair(name);
                    try {
                        pr.peer();
                        let bid = pr.b.global_id;
                        pr.ca.stop();
                        with self.assertRaises(ConnectionError) {
                            pr.pa.send_rpc(bid, ["__comm_ping__"], (), {});
                        }
                        self.assertFalse(pr.pa.ping(bid));
                        with self.assertRaises(ConnectionError) {
                            pr.ca._send_rpc(bid, ["echo"], ("x",), {});
                        }
                        // stop is idempotent
                        pr.ca.stop();
                        pr.pa.stop();
                    } finally {
                        pr.stop_all();
                    }
                }
            }
        }
        // 5 -----------------------------------------------------------------

        fn test_liveness_keeps_healthy_peers(&self) {
            // The liveness sweep must only drop peers that stop answering.
            //
            //         Guards the WebSocket regression where ``ping()`` was the base
            //         ``return False`` and every ws peer was dropped on the first sweep.
            //
            for name in PAIRS {
                with self.subTest(transport=name) {
                    let pr = _pair(name);
                    try {
                        pr.peer();
                        let (aid, bid) = (pr.a.global_id, pr.b.global_id);
                        for pol in (pr.a, pr.b) {
                            pol.central.communication.liveness_interval = 0.2;
                            pol.central.communication._stop_liveness();
                            pol.central.communication._start_liveness();
                        }
                        time.sleep(1.2);  // ~6 sweeps on each side
                        self.assertIn(bid, pr.ca.peers, f "{name}: A dropped healthy B");
                        self.assertIn(aid, pr.cb.peers, f "{name}: B dropped healthy A");
                        self.assertTrue(pr.pa.ping(bid));
                        self.assertTrue(pr.pb.ping(aid));
                    } finally {
                        pr.stop_all();
                    }
                }
            }
        }
        // 6 -----------------------------------------------------------------

        fn test_laila_terminate_tears_everything_down(&self) {
            for name in PAIRS {
                with self.subTest(transport=name) {
                    let pr = _pair(name);
                    try {
                        pr.peer();
                        laila.activate_policy(pr.a);
                        laila.activate_policy(pr.b);
                        with _AsyncioErrors() as probe {
                            probe.watch(pr.pa, pr.pb);
                            let errors = laila.terminate();
                            time.sleep(0.1);
                        }
                        self.assertEqual(errors, [], f "{name}: terminate reported errors");
                        self.assertEqual(probe.report(), []);
                        for proto in (pr.pa, pr.pb) {
                            _assert_proto_down(self, proto, name);
                        }
                        self.assertEqual(laila._remote_policies, {});
                        self.assertEqual(laila._local_policies, {});
                    } finally {
                        pr.stop_all();
                    }
                }
            }
        }
    }

    py_class!(TestFailedStartLeaksNothing);
    impl TestFailedStartLeaksNothing {
        // A ``start()`` whose boot raises must leave no loop / thread behind.

        fn test_failed_boot_closes_loop(&self) {
            // from laila.macros.defaults import DefaultUARTProtocol

            let proto = DefaultUARTProtocol();  // no port -> _open_stream raises
            let baseline = _open_fds();
            with self.assertRaises(RuntimeError) {
                proto.start();
            }
            _assert_proto_down(self, proto, "uart-no-port");
            self.assertIsNone(proto._inbound_executor);
            self.assertEqual(_open_fds() - baseline, set());
            proto.stop();  // idempotent on a never-started protocol
        }
    }
}

// ----------------------------------------------------------------------
// `TestLoopThreadHelpers`: unit checks on the shared loop-thread
// lifecycle (plain Rust: the bodies create `async def`s and a `py_model!`
// probe, which `laila::python!` does not lower inside a method).
// ----------------------------------------------------------------------

/// `class _P: _event_loop = None; _loop_thread = None` -- a bag of
/// attributes `loopthread` writes to.
#[derive(Debug, Default)]
struct _P {
    attrs: laila::python::sync::Mutex<alloc_btree::BTreeMap<String, Arg>>,
}
mod alloc_btree {
    pub use std::collections::BTreeMap;
}
impl laila::python::object::PyObject for _P {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "_P"
    }
    fn __repr__(&self) -> String {
        "<_P>".into()
    }
    fn __eq__(&self, other: &dyn laila::python::object::PyObject) -> bool {
        std::ptr::eq(self as *const _ as *const u8, other.as_any() as *const _ as *const u8)
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(self as *const _ as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(self.attrs.lock().get(name).cloned().unwrap_or(Arg::None))
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        self.attrs.lock().insert(name.to_string(), value);
        Ok(())
    }
}

fn _probe() -> Arg {
    Arg::Object(std::sync::Arc::new(_P::default()))
}

fn _loop_of(p: &Arg) -> loopthread::LoopHandle {
    loopthread::LoopHandle::of(&p.__getattr__("_event_loop").unwrap()).expect("_event_loop is a loop")
}

#[test]
fn test_loop_thread_helpers__test_stop_closes_loop_and_cancels_tasks() {
    use laila::python::stdlib::asyncio;
    let p = _probe();
    let started = laila::python::stdlib::Event::new(Args::new());
    let hung: Arg = Vec::<Arg>::new().into_arg();

    let (started2, hung2) = (started.clone(), hung.clone());
    let _async_start = asyncio::coroutine_function("_async_start", move |a: Args| {
        let ready = a.positional.first().cloned().unwrap_or(Arg::None);
        let (started3, hung3) = (started2.clone(), hung2.clone());
        async move {
            // `_forever`: parks on the loop until it is cancelled (the shim
            // drops pending tasks when the loop closes; the `CancelledError`
            // branch has no twin).
            hung3.append(Arg::from("task"))?;
            started3.set(Args::new());
            ready.__call_method__("set", Args::new())?;
            Ok(Arg::None)
        }
    });

    loopthread::start_loop_thread(&p, _async_start, 5.0).unwrap();
    let mut w = Args::new();
    w.push_positional(2.0);
    assert!(started.wait(w).unwrap());
    let r#loop = _loop_of(&p);
    assert!(r#loop.0.is_running());
    loopthread::stop_loop_thread(&p, loopthread::cancel_pending_tasks(), 5.0).unwrap();
    assert!(p.__getattr__("_event_loop").unwrap().is_none());
    assert!(p.__getattr__("_loop_thread").unwrap().is_none());
    assert!(r#loop.0.is_closed());
    assert_eq!(hung.__len__().unwrap(), 1);
    // idempotent
    loopthread::stop_loop_thread(&p, Arg::None, 5.0).unwrap();
}

#[test]
fn test_loop_thread_helpers__test_failed_start_raises_and_closes_loop() {
    use laila::python::stdlib::asyncio;
    let p = _probe();
    let _boom = asyncio::coroutine_function("_boom", |_a: Args| async {
        Err::<Arg, _>(laila::python::LailaError::runtime_error("no device"))
    });
    let err = loopthread::start_loop_thread(&p, _boom, 5.0).unwrap_err();
    assert_eq!(err.kind, laila::python::ExcKind::RuntimeError);
    assert!(p.__getattr__("_event_loop").unwrap().is_none());
    assert!(p.__getattr__("_loop_thread").unwrap().is_none());
}
