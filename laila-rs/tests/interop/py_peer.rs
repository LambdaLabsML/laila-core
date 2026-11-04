//! Rust <-> CPython interop: the scenarios of
//! `laila-js/tests/interop/interop.test.js` against the same
//! `py_peer.py`, in both directions over `tcpip` (WebSocket) and `tcp`.
//!
//! - **Rust client -> Python server**: peer to a CPython-hosted policy,
//!   RPC (`echo`, `peer_count`, `probe_entry`), `remember` / `memorize` /
//!   `forget` across the wire (dicts, numpy arrays, a 256 KB blob), a
//!   stream lane echoed reversed by CPython (`tcp` only: the WebSocket
//!   transport carries RPC only), remote errors, `remove_peer` + re-peer.
//! - **Python client -> Rust server**: the CPython client runs the same
//!   round-trips against a Rust-hosted policy and reports one JSON check
//!   per step; the Rust side verifies what landed in its pool.
//!
//! Skipped when `import laila, websockets, numpy` fails under
//! `$LAILA_PYTHON` with `$LAILA_PYTHONPATH` (see `py_harness`).
#![allow(
    non_snake_case,
    non_upper_case_globals,
    dead_code,
    unused_variables,
    unused_imports,
    unused_mut,
    unused_assignments,
    clippy::all
)]

mod py_harness;

use laila::entry::constitution::build_maps::build_by_scope;
use laila::entry::entry::Entry;
use laila::macros::defaults::{
    DefaultPolicy, DefaultPool, DefaultTCPIPProtocol, DefaultTCPProtocol,
};
use laila::policy::central::communication::channel::{Channel, Relay, StreamEntry};
use laila::prelude::*;
use laila_tests::streaming_helpers::wait_until;
use py_harness::*;

laila::python! {
    #![unittest]

    static HAVE_PYTHON: ::std::sync::LazyLock<Arg> = ::std::sync::LazyLock::new(|| Arg::Bool(have_python()));

    static STREAM_SIZES: ::std::sync::LazyLock<Arg> = ::std::sync::LazyLock::new(|| __laila_const_STREAM_SIZES().expect("module constant STREAM_SIZES"));
    fn __laila_const_STREAM_SIZES() -> Arg {
        return [1, 100, 4096, 65536 + 7];
    }

    // The CPython reference payload (`py_peer._reference_payload`).
    fn _reference_payload() -> Arg {
        return {
            "message": "hello-from-python",
            "int": 7,
            "float": 2.5,
            "one": 1.0,
            "bool": True,
            "none": None,
            "list": [1, "two", 3.0, False, None],
            "nested": {"a": {"b": [1, 2, {"c": "d"}]}},
            "unicode": "ключ 键 🔑",
        };
    }

    // `n` bytes of `i % 256` (the JS harness's `_blob`).
    fn _blob(n: Arg) -> Arg {
        return bytes([i % 256 for i in range(n)]);
    }

    fn _proto_cls(transport: Arg) -> Arg {
        return DefaultTCPIPProtocol if transport == "tcpip" else DefaultTCPProtocol;
    }

    fn _sha1(data: Arg) -> Arg {
        return hashlib.sha1(data).hexdigest();
    }

    fn _pool_of(policy: Arg, nickname: Arg) -> Arg {
        let router = policy.central.memory.pool_router;
        return router.pools[router.pools_nicknames[nickname]];
    }

    // The live `Entry` behind a memory-pool record (`{entry, recorder, ..}`).
    fn _entry_of(pool: Arg, gid: Arg) -> Arg {
        let rec = pool[gid];
        let entry = rec["entry"] if isinstance(rec, dict) and "entry" in rec else rec;
        if isinstance(entry, Entry) {
            return entry;
        }
        return build_by_scope(entry);
    }

    // Describe what *pool* holds under *gid* (mirror of `py_peer._probe_entry`).
    fn _probe_entry(pool: Arg, gid: Arg) -> Arg {
        if gid not in pool {
            return {"present": False};
        }
        let data = _entry_of(pool, gid).data;
        let out = {"present": True};
        if isinstance(data, np.ndarray) {
            out["type"] = "ndarray";
            out["dtype"] = str(data.dtype);
            out["shape"] = list(data.shape);
            out["sum"] = float(data.sum());
        } else {
            out["type"] = r#type(data).__name__;
            out["data"] = data;
        }
        return out;
    }

    // Echo every message on stream lane *name* back reversed (`data[::-1]`),
    // for the first peer that opened it (mirror of `py_peer._echo_lane`).
    fn _echo_lane(policy: Arg, name: Arg) {
        let comm = policy.central.communication;
        let peer_id = None;
        while peer_id is None {
            for pid in list(comm.peers.keys()) {
                try {
                    if name in comm.peers[pid].channels() {
                        peer_id = pid;
                        break;
                    }
                } except Exception {
                    pass;
                }
            }
            if peer_id is None {
                time.sleep(0.02);
            }
        }
        let ch = comm.peers[peer_id][name];
        try {
            for e in laila.relay(ch) {
                ch.send(bytes(e.data)[::-1]);
            }
        } except Exception {
            pass;
        }
    }

    // ------------------------------------------------------------------
    // Rust client -> Python server
    // ------------------------------------------------------------------

    static TRANSPORTS: ::std::sync::LazyLock<Arg> = ::std::sync::LazyLock::new(|| __laila_const_TRANSPORTS().expect("module constant TRANSPORTS"));
    fn __laila_const_TRANSPORTS() -> Arg {
        return ["tcpip", "tcp"];
    }

    // Rust has no inheritance between `py_class!` structs: each scenario
    // runs for both transports under `subTest`, with the per-transport
    // fixture built and torn down around it (`before` / `after` of the
    // JS harness).
    py_class!(TestRustClientPythonServer);
    #[skip_unless(HAVE_PYTHON, "no CPython laila (LAILA_PYTHON / LAILA_PYTHONPATH)")]
    impl TestRustClientPythonServer {
        fn setUp(&self) {
            self._original = laila.get_active_policy();
        }

        fn tearDown(&self) {
            laila.activate_policy(self._original);
        }

        fn _start(&self, transport: Arg) {
            self.transport = transport;
            self.py = spawn_py_server(transport);
            self.local = DefaultPolicy();
            laila.activate_policy(self.local);
            self.local.central.memory.extend(pool=DefaultPool(), pool_nickname="rs-local");
            let proto_cls = _proto_cls(transport);
            self.pa = proto_cls(host="127.0.0.1", port=0);
            laila.communication.add_connection(self.pa);
            self.remote_id = laila.add_peer(self.py.uri(transport), self.py.SECRET);
        }

        fn _stop(&self) {
            try {
                self.local.central.communication.stop();
            } finally {
                self.py.kill();
                laila.activate_policy(self._original);
            }
        }

        fn _each(&self, body: Arg) {
            for transport in TRANSPORTS {
                with self.subTest(transport=transport) {
                    self._start(transport);
                    try {
                        body(transport);
                    } finally {
                        self._stop();
                    }
                }
            }
        }

        fn _assert_reference(&self, data: Arg) {
            self.assertEqual(data["message"], "hello-from-python");
            self.assertEqual(data["int"], 7);
            self.assertEqual(data["float"], 2.5);
            self.assertEqual(data["one"], 1.0);
            self.assertEqual(data["bool"], True);
            self.assertIsNone(data["none"]);
            self.assertEqual(data["list"], [1, "two", 3.0, False, None]);
            self.assertEqual(data["nested"], {"a": {"b": [1, 2, {"c": "d"}]}});
            self.assertEqual(data["unicode"], "ключ 键 🔑");
        }

        fn test_handshake_yields_the_python_policy_id(&self) {
            fn body(transport: Arg) {
                self.assertEqual(self.remote_id, self.py.POLICY_ID);
                self.assertIn(self.remote_id, laila.peers);
            }
            self._each(body);
        }

        fn test_ping_and_rpc_echo_of_mixed_json_types(&self) {
            fn body(transport: Arg) {
                self.assertTrue(self.pa.ping(self.remote_id));
                let proxy = laila.peers[self.remote_id];
                let payload = {"k": [1, 2.5, "s", None, True], "u": "ключ 键 🔑", "nested": {"a": {}}};
                self.assertEqual(proxy.echo(payload), payload);
                self.assertEqual(proxy.echo("plain"), "plain");
                self.assertEqual(proxy.peer_count(), 1);
            }
            self._each(body);
        }

        fn test_remember_rebuilds_the_cpython_entry(&self) {
            fn body(transport: Arg) {
                let res = laila.remember(entry_ids=self.py.ENTRY_ID, pool_nickname="remote-store", policy_id=self.remote_id, persist=False);
                self.assertEqual(laila.get_active_policy().global_id, self.local.global_id);
                self._assert_reference(res.data);
                let entry = res.wait();
                self.assertIsInstance(entry, Entry);
                self.assertEqual(entry.global_id, self.py.ENTRY_ID);
            }
            self._each(body);
        }

        fn test_remember_of_a_numpy_array_yields_an_ndarray(&self) {
            fn body(transport: Arg) {
                let entry = laila.remember(entry_ids=self.py.ARRAY_ENTRY_ID, pool_nickname="remote-store", policy_id=self.remote_id, persist=False).wait();
                let arr = entry.data;
                self.assertIsInstance(arr, np.ndarray);
                self.assertEqual(str(arr.dtype), "float32");
                self.assertEqual(list(arr.shape), [3, 4]);
                self.assertEqual(arr.tolist(), [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11]]);
            }
            self._each(body);
        }

        fn test_remember_with_default_persist_resolves_to_the_rebuilt_entry(&self) {
            fn body(transport: Arg) {
                let fut = laila.remember(entry_ids=self.py.ENTRY_ID, pool_nickname="remote-store", policy_id=self.remote_id);
                let entry = fut.wait(10);
                self.assertIsInstance(entry, Entry);
                self.assertEqual(entry.global_id, self.py.ENTRY_ID);
                self.assertEqual(laila.get_active_policy().global_id, self.local.global_id);
            }
            self._each(body);
        }

        fn test_memorize_stores_a_rust_entry_in_the_cpython_pool(&self) {
            fn body(transport: Arg) {
                let e1 = laila.constant(data={"from": "rust", "n": [1, 2, 3], "f": 0.5, "t": True, "z": None}, nickname="rs-push");
                let e2 = laila.constant(data=np.ones((2, 3), dtype=np.int64), nickname="rs-push-arr");
                let f1 = laila.memorize(e1, policy_id=self.remote_id, pool_nickname="remote-store");
                let f2 = laila.memorize(e2, policy_id=self.remote_id, pool_nickname="remote-store");
                self.assertEqual(f1.data, e1.global_id);
                self.assertEqual(f2.data, e2.global_id);
                let proxy = laila.peers[self.remote_id];
                self.assertEqual(
                    proxy.probe_entry(e1.global_id),
                    {"present": True, "type": "dict", "data": {"from": "rust", "n": [1, 2, 3], "f": 0.5, "t": True, "z": None}},
                );
                self.assertEqual(
                    proxy.probe_entry(e2.global_id),
                    {"present": True, "type": "ndarray", "dtype": "int64", "shape": [2, 3], "sum": 6.0},
                );
                self.assertEqual(proxy.probe_entry("LAILA:ENTRY:absent"), {"present": False});
                // and it round-trips back through CPython's pickle
                let back = laila.remember(e2.global_id, policy_id=self.remote_id, pool_nickname="remote-store", persist=False).wait();
                self.assertIsInstance(back.data, np.ndarray);
                self.assertEqual(back.data.tolist(), [[1, 1, 1], [1, 1, 1]]);
            }
            self._each(body);
        }

        fn test_256_kb_payload_crosses_the_wire_in_both_directions(&self) {
            fn body(transport: Arg) {
                let big = _blob(256 * 1024);
                let e = laila.constant(data=big, nickname="rs-big");
                laila.memorize(e, policy_id=self.remote_id, pool_nickname="remote-store").wait(30);
                let back = laila.remember(e.global_id, policy_id=self.remote_id, pool_nickname="remote-store", persist=False).wait(30);
                self.assertIsInstance(back.data, bytes);
                self.assertEqual(len(back.data), len(big));
                self.assertEqual(_sha1(back.data), _sha1(big));
            }
            self._each(body);
        }

        fn test_forget_deletes_on_the_cpython_side(&self) {
            fn body(transport: Arg) {
                let e = laila.constant(data={"bye": 1}, nickname="rs-forget");
                laila.memorize(e, policy_id=self.remote_id, pool_nickname="remote-store").wait(10);
                let proxy = laila.peers[self.remote_id];
                self.assertTrue(proxy.probe_entry(e.global_id)["present"]);
                let f = laila.forget(e.global_id, policy=self.remote_id, pool="remote-store");
                if f is not None and hasattr(f, "wait") {
                    f.wait(10);
                }
                self.assertTrue(wait_until(lambda: proxy.probe_entry(e.global_id)["present"] == False, 10));
            }
            self._each(body);
        }

        fn test_stream_lane_rust_sends_cpython_echoes_reversed(&self) {
            // `tcp` only: the WebSocket transport carries RPC only.
            self._start("tcp");
            try {
                let ch = laila.peers[self.remote_id]["video"];
                self.assertTrue(ch.opened);
                let payloads = [_blob(n) for n in STREAM_SIZES];
                for p in payloads {
                    ch.send(p);
                }
                let relay = ch.relay(timeout=20);
                let got = [next(relay) for _ in payloads];
                for g in got {
                    self.assertIsInstance(g, StreamEntry);
                }
                self.assertEqual([bytes(g.data) for g in got], [p[::-1] for p in payloads]);
                self.assertEqual([g.stream.seq for g in got], list(range(len(payloads))));
                for g in got {
                    self.assertEqual(g.stream.peer_id, self.remote_id);
                    self.assertEqual(g.stream.channel, "video");
                }
                self.assertEqual(ch.dropped, 0);
                self.assertEqual(ch.tx_seq, len(payloads));
                ch.close();
            } finally {
                self._stop();
            }
        }

        fn test_remote_error_surfaces_as_an_exception(&self) {
            fn body(transport: Arg) {
                let proxy = laila.peers[self.remote_id];
                with self.assertRaises(Exception) as cm {
                    proxy.no_such_method();
                }
                let text = str(cm.exception);
                self.assertTrue(
                    "AttributeError" in text or "no attribute" in text or "Remote RPC error" in text,
                    text,
                );
            }
            self._each(body);
        }

        fn test_remove_peer_is_noticed_by_cpython_and_re_peer_works(&self) {
            fn body(transport: Arg) {
                let proxy = laila.peers[self.remote_id];
                self.assertEqual(proxy.peer_count(), 1);
                self.local.central.communication.remove_peer(self.remote_id);
                self.assertNotIn(self.remote_id, laila.peers);
                time.sleep(0.3);
                let again = laila.add_peer(self.py.uri(transport), self.py.SECRET);
                self.assertEqual(again, self.remote_id);
                fn _peered_again() -> Arg {
                    let proxy = laila.peers[self.remote_id];
                    return proxy.peer_count() == 1;
                }
                self.assertTrue(wait_until(_peered_again, 5));
            }
            self._each(body);
        }
    }

    // ------------------------------------------------------------------
    // Python client -> Rust server
    // ------------------------------------------------------------------

    py_class!(TestPythonClientRustServer);
    #[skip_unless(HAVE_PYTHON, "no CPython laila (LAILA_PYTHON / LAILA_PYTHONPATH)")]
    impl TestPythonClientRustServer {
        fn setUp(&self) {
            self._original = laila.get_active_policy();
        }

        fn tearDown(&self) {
            laila.activate_policy(self._original);
        }

        // Host a Rust policy on *transport* and run the CPython client
        // against it; `self.checks` are the JSON lines it reported.
        fn _start(&self, transport: Arg) {
            self.transport = transport;
            self.policy = DefaultPolicy();
            laila.activate_policy(self.policy);
            self.pool = DefaultPool();
            laila.memory.extend(pool=self.pool, pool_nickname="remote-store");
            let payload = _reference_payload();
            payload["message"] = "hello-from-rust";
            self.ref_entry = laila.constant(data=payload, nickname="rs-ref");
            laila.memorize(self.ref_entry, pool_nickname="remote-store").wait(10);
            let pool = self.pool;
            object.__setattr__(self.policy, "echo", lambda x: x);
            object.__setattr__(self.policy, "probe_entry", lambda gid: _probe_entry(pool, gid));
            let proto_cls = _proto_cls(transport);
            self.proto = proto_cls(host="127.0.0.1", port=0);
            laila.communication.add_connection(self.proto);
            let scheme = "ws" if transport == "tcpip" else "tcp";
            let uri = f "{scheme}://127.0.0.1:{self.proto.bound_port}";
            // Echo lane "video" back reversed while the CPython client runs
            // (mirror of `py_peer._echo_lane`); ends when the lane closes.
            if r#type(self.proto).supports_channels {
                threading.Thread(target=_echo_lane, args=(self.policy, "video"), daemon=True).start();
            }
            self.checks = run_py_client(uri, self.proto.peer_secret_key, self.policy.global_id, self.ref_entry.global_id);
        }

        fn _stop(&self) {
            try {
                self.policy.central.communication.stop();
            } finally {
                laila.activate_policy(self._original);
            }
        }

        fn _each(&self, body: Arg) {
            for transport in TRANSPORTS {
                with self.subTest(transport=transport) {
                    self._start(transport);
                    try {
                        body(transport);
                    } finally {
                        self._stop();
                    }
                }
            }
        }

        fn test_cpython_peered_to_the_rust_policy(&self) {
            fn body(transport: Arg) {
                self.assertTrue(self.checks["peered"]["ok"], str(self.checks["peered"]));
                self.assertTrue(self.checks["ping"]["ok"]);
            }
            self._each(body);
        }

        fn test_rpc_echo_answered_by_rust(&self) {
            fn body(transport: Arg) {
                self.assertEqual(self.checks["echo"]["value"], {"k": [1, 2.5, "s", None, True], "u": "ключ"});
            }
            self._each(body);
        }

        fn test_cpython_remember_rebuilt_the_rust_entry(&self) {
            fn body(transport: Arg) {
                let r = self.checks["remember"];
                self.assertEqual(r["gid"], self.ref_entry.global_id);
                self.assertTrue(r["active_restored"]);
                let data = r["data"];
                self.assertEqual(data["message"], "hello-from-rust");
                self.assertEqual(data["int"], 7);
                self.assertEqual(data["float"], 2.5);
                self.assertEqual(data["bool"], True);
                self.assertIsNone(data["none"]);
                self.assertEqual(data["list"], [1, "two", 3.0, False, None]);
                self.assertEqual(data["nested"], {"a": {"b": [1, 2, {"c": "d"}]}});
                self.assertEqual(data["unicode"], "ключ 键 🔑");
            }
            self._each(body);
        }

        fn test_cpython_memorize_landed_in_the_rust_pool(&self) {
            fn body(transport: Arg) {
                let m = self.checks["memorize"];
                self.assertEqual(m["gids"], m["expected"]);
                let (gid_dict, gid_arr) = m["expected"];
                self.assertIn(gid_dict, self.pool);
                self.assertIn(gid_arr, self.pool);
                self.assertEqual(_entry_of(self.pool, gid_dict).data, {"from": "python", "n": [1, 2, 3]});
                let arr = _entry_of(self.pool, gid_arr).data;
                self.assertIsInstance(arr, np.ndarray);
                self.assertEqual(str(arr.dtype), "int64");
                self.assertEqual(list(arr.shape), [2, 3]);
                self.assertEqual(arr.tolist(), [[4, 4, 4], [4, 4, 4]]);
                // what CPython saw through the probe RPC matches
                self.assertEqual(self.checks["probe"]["pushed"], {"present": True, "type": "dict", "data": {"from": "python", "n": [1, 2, 3]}});
                self.assertEqual(self.checks["probe"]["pushed_arr"], {"present": True, "type": "ndarray", "dtype": "int64", "shape": [2, 3], "sum": 24.0});
            }
            self._each(body);
        }

        fn test_cpython_remembered_its_numpy_array_back_from_rust_intact(&self) {
            fn body(transport: Arg) {
                self.assertEqual(
                    self.checks["remember_array"],
                    {"check": "remember_array", "is_ndarray": True, "dtype": "int64", "shape": [2, 3], "sum": 24.0},
                );
            }
            self._each(body);
        }

        fn test_stream_lane_cpython_sends_rust_echoes_reversed(&self) {
            // `tcp` only: the WebSocket transport carries RPC only.
            self._start("tcp");
            try {
                let st = self.checks.get("stream");
                self.assertIsNotNone(st, "python client reported no stream check");
                self.assertEqual(st["sent"], [_sha1(_blob(n)) for n in STREAM_SIZES]);
                self.assertEqual(st["got"], st["sent"]);
                self.assertEqual(st["seqs"], list(range(len(STREAM_SIZES))));
                self.assertEqual(st["channel"], ["video"]);
                self.assertEqual(st["peer"], [self.policy.global_id]);
                self.assertEqual(st["dropped"], 0);
            } finally {
                self._stop();
            }
        }

        fn test_cpython_remove_peer_was_honoured(&self) {
            fn body(transport: Arg) {
                self.assertFalse(self.checks["removed"]["has_peer"]);
                self.assertTrue(wait_until(lambda: len(self.policy.central.communication.peers) == 0, 5));
            }
            self._each(body);
        }
    }
}
