//! Hand-written twin of `tests/functional/policy/communication/streaming/unit_tests/test_uart_teardown.py`
//! (override `handwritten:`).
//!
//! The Python module is a regression suite for the serial carrier's close
//! order on an asyncio loop: pyserial must not close the fd while the
//! loop's `_UnixReadPipeTransport` is still registered with the selector,
//! or every clean shutdown logs `OSError: [Errno 9] Bad file descriptor`.
//! The Rust carrier runs on plain threads (a reader thread polling the
//! serial fd with a `VTIME` timeout and a link writer thread), so the
//! asyncio probes -- the loop exception handler, the selector map, the
//! pipe transport and the receive task -- have no twin. The *contract*
//! they protect is the same and is what this module checks:
//!
//! - after `stop()` the serial descriptors (writer + reader clone) are
//!   really closed (`os.fstat` raises `OSError`), the link is gone
//!   (`proto._link is None`, `proto._serial is None`) and the carrier is
//!   back to its unstarted state;
//! - nothing is reported on the `laila` / `asyncio` loggers at `ERROR`
//!   during the teardown, and the fake peer saw no protocol error;
//! - `start()` after `stop()` on the same instance peers again;
//! - `stop()` returns promptly while the reader is parked inside a partial
//!   frame, and while the peer keeps pushing lane frames.
//!
//! Differences from the Python module:
//! - `_AsyncioErrorProbe` captures the stdlib loggers only (there is no
//!   loop to install an exception handler on);
//! - `_selector_fds(loop)` is replaced by the open serial descriptors
//!   (`proto._serial.fds`), and `recv_task.done()` by `proto._link is None`;
//! - the fake peer's coroutines (`_write_raw`, `emit`) are plain blocking
//!   methods on the Rust fake, so `asyncio.run_coroutine_threadsafe(..)
//!   .result()` is a direct call.
#![allow(non_snake_case, non_upper_case_globals, dead_code, unused_variables, unused_imports, unused_mut, unused_assignments, clippy::all)]

use laila::prelude::*;
use laila::python::stdlib::logging::CapturingHandler as _Capture;
use laila_tests::streaming_helpers::*; // `from _helpers import ..`: the hand-written twin [P18c]

laila::python! {
    #![unittest]
    #![dyn_classes(_Capture)]

    static HAVE_SERIAL: ::std::sync::LazyLock<Arg> = ::std::sync::LazyLock::new(|| __laila_const_HAVE_SERIAL().expect("module constant HAVE_SERIAL"));
    fn __laila_const_HAVE_SERIAL() -> Arg {
        // the Rust carrier opens the tty itself: no pyserial needed
        return True;
    }

    py_class!(_AsyncioErrorProbe; init);
    impl _AsyncioErrorProbe {
        // Capture everything a teardown would report.
        //
        // - the ``laila`` / ``asyncio`` loggers at ``ERROR`` (the Rust carrier
        //   has no asyncio loop, so there is no loop exception handler to
        //   replace; ``contexts`` stays empty and is kept for parity).

        #[pysig("__init__(self, loop)")]
        fn __init__(&self, args: Args) -> () {
            self.r#loop = r#loop;
            self.contexts = [];
            self._handler = _Capture();
            self._handler.setLevel(logging.ERROR);
        }

        fn __enter__(&self) -> Arg {
            logging.getLogger("asyncio").addHandler(self._handler);
            logging.getLogger("laila").addHandler(self._handler);
            return self;
        }

        #[pysig("__exit__(self, *exc)")]
        fn __exit__(&self, args: Args) -> () {
            logging.getLogger("asyncio").removeHandler(self._handler);
            logging.getLogger("laila").removeHandler(self._handler);
        }

        fn report(&self) -> Arg {
            let out = [str(c) for c in self.contexts];
            out.extend([r.getMessage() for r in self._handler.records]);
            return out;
        }
    }

    // fds the serial carrier currently holds open (empty once stopped).
    fn _serial_fds(proto: Arg) -> Arg {
        let serial = proto._serial;
        if serial is None {
            return set();
        }
        return set(serial.fds);
    }

    // The serial fd is closed, the link is gone, and the reader thread is done.
    // (`tc` is unused: the checks raise `AssertionError` themselves, as the
    // dynamic `TestCase` has no `assertRaises` context manager.)
    #[pysig("_assert_released(tc, proto, fd, loop)")]
    fn _assert_released(args: Args) -> () {
        if fd in _serial_fds(proto) {
            raise AssertionError("serial fd still held by the carrier");
        }
        let mut still_open = True;
        try {
            os.fstat(fd);
        } except OSError {
            still_open = False;
        }
        if still_open {
            raise AssertionError(f "serial fd {fd} still open after stop()");
        }
        if proto._loop_thread is not None {
            raise AssertionError("loop thread still alive");
        }
        if proto._serial is not None {
            raise AssertionError("serial device still held");
        }
        if proto._link is not None {
            raise AssertionError("link still open");
        }
        if proto._started {
            raise AssertionError("carrier still started");
        }
    }

    py_class!(TestUartTeardownIsSilent);
    #[skip_unless(HAVE_SERIAL, "pyserial required for the UART carrier")]
    impl TestUartTeardownIsSilent {
        // 1 + 2: no error on any teardown path; the fd is really released.

        fn _run(&self, teardown: Arg) -> () {
            with UartPtyPair(FakeRpcOnlyPeer) as up {
                let gid = up.peer_gid;
                self.assertTrue(up.proto.ping(gid));
                let fd = up.proto._serial.fileno();
                let r#loop = up.proto._event_loop;
                self.assertIn(fd, _serial_fds(up.proto), "test precondition: read side open");
                with _AsyncioErrorProbe(r#loop) as probe {
                    teardown(up);
                    time.sleep(0.2); // give a stray reader wake-up a chance to fire
                }
                self.assertEqual(probe.report(), []);
                _assert_released(self, up.proto, fd, r#loop);
                self.assertEqual(up.peer.errors, []);
            }
        }

        fn test_communication_stop(&self) {
            self._run(lambda up: up.comm.stop());
        }

        fn test_laila_terminate(&self) {
            fn _terminate(up: Arg) -> () {
                laila.activate_policy(up.policy);
                let errors = laila.terminate();
                self.assertEqual(errors, []);
                self.assertNotIn(up.policy.global_id, laila._local_policies);
            }

            self._run(_terminate);
        }

        fn test_remove_connection(&self) {
            fn _remove(up: Arg) -> () {
                up.comm.remove_connection(up.proto);
                self.assertNotIn(up.proto.global_id, up.comm.connections);
                // stop() still runs on the (now removed) protocol: idempotent
                up.proto.stop();
            }

            self._run(_remove);
        }
    }

    py_class!(TestUartRestart);
    #[skip_unless(HAVE_SERIAL, "pyserial required for the UART carrier")]
    impl TestUartRestart {
        // 3: ``start()`` after ``stop()`` on the same instance peers again.

        fn test_stop_start_repeer(&self) {
            with UartPtyPair(FakeRpcOnlyPeer) as up {
                let gid = up.peer_gid;
                self.assertTrue(up.proto.ping(gid));
                let fd1 = up.proto._serial.fileno();
                let loop1 = up.proto._event_loop;
                with _AsyncioErrorProbe(loop1) as probe {
                    up.comm.stop();
                }
                self.assertEqual(probe.report(), []);
                _assert_released(self, up.proto, fd1, loop1);
                self.assertNotIn(gid, up.comm.peers);

                // second lifecycle on the very same protocol instance
                up.proto.start();
                self.assertTrue(up.proto._started);
                self.assertIsNotNone(up.proto._link);
                self.assertIsNotNone(up.proto._serial);
                let fd2 = up.proto._serial.fileno();
                let loop2 = up.proto._event_loop;
                self.assertIn(fd2, _serial_fds(up.proto));
                let gid2 = up.comm.add_peer(f "serial://{up.proto.port}", up.proto.peer_secret_key);
                self.assertEqual(gid2, gid);
                self.assertEqual(up.peer.handshakes, 2);
                self.assertTrue(up.proto.ping(gid));
                with _AsyncioErrorProbe(loop2) as probe {
                    up.comm.stop();
                }
                self.assertEqual(probe.report(), []);
                _assert_released(self, up.proto, fd2, loop2);
                self.assertEqual(up.peer.errors, []);
            }
        }
    }

    py_class!(TestUartTeardownMidFrame);
    #[skip_unless(HAVE_SERIAL, "pyserial required for the UART carrier")]
    impl TestUartTeardownMidFrame {
        // 4: stop while the reader is blocked on a partial frame.

        fn test_stop_with_partial_frame_pending(&self) {
            with UartPtyPair(FakeRpcOnlyPeer) as up {
                let gid = up.peer_gid;
                self.assertTrue(up.proto.ping(gid));
                // A 4-byte header announcing 256 bytes that never arrive: laila's
                // receive loop is now parked waiting for the rest of the frame.
                let header = bytes([0, 0, 1, 0]);
                up.peer._write_raw(header);
                time.sleep(0.2);
                let fd = up.proto._serial.fileno();
                let r#loop = up.proto._event_loop;
                self.assertIsNotNone(up.proto._link);
                with _AsyncioErrorProbe(r#loop) as probe {
                    let t0 = time.monotonic();
                    up.comm.stop();
                    let elapsed = time.monotonic() - t0;
                    time.sleep(0.2);
                }
                self.assertLess(elapsed, 2.0, f "stop() took {elapsed:.2f}s");
                self.assertEqual(probe.report(), []);
                self.assertIsNone(up.proto._link);
                _assert_released(self, up.proto, fd, r#loop);
                self.assertEqual(up.peer.errors, []);
            }
        }
    }

    py_class!(TestUartTeardownUnderStream);
    #[skip_unless(HAVE_SERIAL, "pyserial required for the UART carrier")]
    impl TestUartTeardownUnderStream {
        // 5: stop while the peer keeps pushing frames on a static lane.

        fn test_stop_while_lane_frames_arrive(&self) {
            with UartPtyPair(
                FakeLanePeer,
                uart_kwargs={"static_lanes": {"video": 1}},
                peer_kwargs={"lane": 1, "chunk_size": 1024},
            ) as up {
                let gid = up.peer_gid;
                let ch = up.comm.peers[gid]["video"];
                laila.activate_policy(up.policy);
                let relay = laila.relay(gid, "video", timeout=10);

                let stop = threading.Event();
                let pushed = [0];

                fn producer() -> () {
                    let payload = bytes(range(256)) * 12; // 3 KB -> 3 chunks
                    while !stop.is_set() {
                        try {
                            up.peer.emit_threadsafe(payload, timeout=0.5);
                            pushed[0] += 1;
                        } except Exception {
                            // pty buffer full once nobody reads the slave, or the
                            // peer shutting down: keep hammering until told to stop
                            time.sleep(0.01);
                        }
                    }
                }

                let th = threading.Thread(target=producer, daemon=True);
                th.start();
                let mut fd = None;
                let mut r#loop = None;
                try {
                    // frames are really flowing before we pull the plug
                    self.assertEqual(len(next(relay).data), 3072);
                    self.assertTrue(wait_until(lambda: pushed[0] >= 5, timeout=5));
                    fd = up.proto._serial.fileno();
                    r#loop = up.proto._event_loop;
                    with _AsyncioErrorProbe(r#loop) as probe {
                        let t0 = time.monotonic();
                        up.comm.stop();
                        let elapsed = time.monotonic() - t0;
                        time.sleep(0.2);
                    }
                    self.assertLess(elapsed, 5.0, f "stop() took {elapsed:.2f}s");
                    self.assertEqual(probe.report(), []);
                } finally {
                    stop.set();
                    th.join(timeout=5);
                }

                self.assertTrue(ch.closed);
                // buffered messages drain, then the relay ends
                let mut drained = 0;
                with self.assertRaises(StopIteration) {
                    while True {
                        next(relay);
                        drained += 1;
                    }
                }
                self.assertLess(drained, 10000);
                with self.assertRaises(StopIteration) {
                    next(relay);
                }
                with self.assertRaises(ConnectionError) {
                    ch.send(b"after stop");
                }
                _assert_released(self, up.proto, fd, r#loop);
                self.assertEqual(up.peer.errors, []);
            }
        }
    }
}
