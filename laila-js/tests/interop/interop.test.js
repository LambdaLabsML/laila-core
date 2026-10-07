/**
 * Live Python <-> JavaScript interop over the real wire.
 *
 * Both directions, over the WebSocket (``tcpip``) and raw TCP transports:
 *
 * - **JS client -> Python server**: the JS port peers to a CPython ``laila``
 *   process (``py_peer.py server``), exchanges RPCs, pulls entries with
 *   ``laila.remember(..., policy_id=py)`` (CPython pickle blobs rebuilt in
 *   JS), pushes entries with ``laila.memorize(..., policy_id=py)`` (JS pickle
 *   blobs rebuilt in CPython) and checks the Python side via probe RPCs.
 * - **Python client -> JS server**: a CPython process (``py_peer.py client``)
 *   peers to a JS-hosted policy and runs the mirror-image checks, reporting
 *   one JSON line per check.
 *
 * Requires a CPython with ``laila`` importable. Resolution order:
 * ``$LAILA_PYTHON`` (interpreter, default ``python3``) and
 * ``$LAILA_PYTHONPATH`` (default: the directory containing the ``laila``
 * package, i.e. the parent of the repository root). When unavailable the suite is
 * skipped, not failed.
 */
import { test, describe, before, after } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import path from "node:path";
import readline from "node:readline";
import { spawn, spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const S = new URL("../../src/", import.meta.url).href;
const laila = (await import(S + "index.js")).default;
const defaults = await import(S + "macros/defaults.js");
const time = await import(S + "_compat/time.js");
const { NDArray } = await import(S + "_compat/ndarray.js");
const { PyFloat, dict_get, dict_has } = await import(S + "_compat/pytypes.js");
const { object_setattr } = await import(S + "_compat/pydantic.js");
const { Entry, build_by_scope } = await import(S + "entry/index.js");
const { StreamEntry } = await import(S + "policy/central/communication/channel.js");

const { DefaultPolicy, DefaultPool, DefaultTCPIPProtocol, DefaultTCPProtocol } = defaults;

const sha1 = (b) => crypto.createHash("sha1").update(Buffer.from(b)).digest("hex");
const _blob = (n) => Buffer.from(Array.from({ length: n }, (_, i) => i % 256));
const STREAM_SIZES = [1, 100, 4096, 65536 + 7];

const PY = process.env.LAILA_PYTHON ?? "python3";
// tests/interop -> laila-js -> laila (the package) -> its parent, which must be on sys.path
const PYTHONPATH = process.env.LAILA_PYTHONPATH ?? path.resolve(HERE, "../../..", "..");
const PY_ENV = { ...process.env, PYTHONPATH: PYTHONPATH + (process.env.PYTHONPATH ? path.delimiter + process.env.PYTHONPATH : "") };
const PY_PEER = path.join(HERE, "py_peer.py");

const have_python = (() => {
  const r = spawnSync(PY, ["-c", "import laila, websockets, numpy"], { env: PY_ENV, encoding: "utf8" });
  return r.status === 0;
})();

function macrotask(fn) {
  return new Promise((resolve, reject) =>
    setImmediate(() => {
      try {
        resolve(fn());
      } catch (e) {
        reject(e);
      }
    }),
  );
}

function wait_until(pred, timeout = 5.0) {
  const deadline = time.monotonic() + timeout;
  while (time.monotonic() < deadline) {
    if (pred()) return true;
    time.sleep(0.01);
  }
  return pred();
}

/** Spawn ``py_peer.py server <transport>`` and wait for ``READY``. */
function spawn_py_server(transport) {
  const proc = spawn(PY, [PY_PEER, "server", transport], { stdio: ["ignore", "pipe", "inherit"], env: PY_ENV });
  const info = {};
  const ready = new Promise((resolve, reject) => {
    readline.createInterface({ input: proc.stdout }).on("line", (line) => {
      line = line.trim();
      if (line === "READY") return resolve();
      const i = line.indexOf("=");
      if (i > 0) info[line.slice(0, i)] = line.slice(i + 1);
    });
    proc.on("exit", (code) => reject(new Error(`python peer exited early (code ${code})`)));
    setTimeout(() => reject(new Error("python peer did not become ready")), 60_000).unref();
  });
  return { proc, info, ready };
}

/** Run ``py_peer.py client ...`` to completion; returns ``{check: payload}``. */
function run_py_client(uri, secret, remote_id, entry_id) {
  const proc = spawn(PY, [PY_PEER, "client", uri, secret, remote_id, entry_id], { stdio: ["ignore", "pipe", "inherit"], env: PY_ENV });
  const checks = {};
  return new Promise((resolve, reject) => {
    let done = false;
    readline.createInterface({ input: proc.stdout }).on("line", (line) => {
      line = line.trim();
      if (line === "DONE") {
        done = true;
        return;
      }
      if (line.startsWith("{")) {
        const obj = JSON.parse(line);
        checks[obj.check] = obj;
      }
    });
    proc.on("exit", (code) => (done && code === 0 ? resolve(checks) : reject(new Error(`python client failed (code ${code}); checks so far: ${JSON.stringify(checks)}`))));
    setTimeout(() => {
      proc.kill("SIGKILL");
      reject(new Error("python client timed out"));
    }, 120_000).unref();
  });
}

const REFERENCE_PAYLOAD = {
  message: "hello-from-python",
  int: 7,
  float: 2.5,
  one: new PyFloat(1),
  bool: true,
  none: null,
  list: [1, "two", new PyFloat(3), false, null],
  nested: { a: { b: [1, 2, { c: "d" }] } },
  unicode: "ключ 键 🔑",
};

/** Compare a value that crossed the wire with the Python reference payload. */
function assert_reference(data) {
  assert.equal(data.message, "hello-from-python");
  assert.equal(data.int, 7);
  assert.equal(Number(data.float), 2.5);
  assert.equal(Number(data.one), 1);
  assert.equal(data.bool, true);
  assert.equal(data.none, null);
  assert.deepEqual(data.list.map((x) => (x instanceof PyFloat ? Number(x) : x)), [1, "two", 3, false, null]);
  assert.deepEqual(data.nested, { a: { b: [1, 2, { c: "d" }] } });
  assert.equal(data.unicode, "ключ 键 🔑");
}

const _pool_of = (policy, nickname) => {
  const router = policy.central.memory.pool_router;
  return dict_get(router.pools, dict_get(router.pools_nicknames, nickname));
};

/** The live ``Entry`` behind a memory-pool ``Record`` (``{entry, recorder, ...}``). */
function _entry_of(pool, gid) {
  const rec = pool.__getitem__(gid);
  const entry = dict_has(rec, "entry") ? dict_get(rec, "entry") : rec;
  return entry instanceof Entry ? entry : build_by_scope(entry);
}

/** Describe what *pool* holds under *gid* (mirror of ``py_peer._probe_entry``). */
function _probe_entry(pool, gid) {
  if (!pool.__contains__(gid)) return { present: false };
  const data = _entry_of(pool, gid).data;
  const out = { present: true };
  if (data instanceof NDArray) {
    out.type = "ndarray";
    out.dtype = data.dtype;
    out.shape = [...data.shape];
    out.sum = data.tolist().flat(Infinity).reduce((a, b) => a + Number(b), 0);
  } else {
    out.type = Array.isArray(data) ? "list" : typeof data === "object" && data !== null ? "dict" : typeof data;
    out.data = data;
  }
  return out;
}

// ---------------------------------------------------------------------------
// JS client -> Python server
// ---------------------------------------------------------------------------
for (const transport of ["tcpip", "tcp"]) {
  describe(`JS client -> Python server [${transport}]`, { skip: have_python ? false : `no CPython laila (${PY}, PYTHONPATH=${PYTHONPATH})` }, () => {
    let original, py, local, remote_id, pa;
    const uri = () => (transport === "tcpip" ? `ws://127.0.0.1:${py.info.PORT}` : `tcp://127.0.0.1:${py.info.PORT}`);

    before(async () => {
      original = laila.get_active_policy();
      py = spawn_py_server(transport);
      await py.ready;
      await macrotask(() => {
        local = new DefaultPolicy();
        laila.activate_policy(local);
        local.central.memory.extend(new DefaultPool(), { pool_nickname: "js-local" });
        const Proto = transport === "tcpip" ? DefaultTCPIPProtocol : DefaultTCPProtocol;
        pa = new Proto({ host: "127.0.0.1", port: 0 });
        laila.communication.add_connection(pa);
        remote_id = laila.add_peer(uri(), py.info.SECRET);
      });
    });
    after(async () => {
      await macrotask(() => {
        try {
          local.central.communication.stop();
        } finally {
          py.proc.kill("SIGTERM");
          laila.activate_policy(original);
        }
      });
    });

    test("handshake yields the Python policy id", () => {
      assert.equal(remote_id, py.info.POLICY_ID);
      assert.ok(remote_id in laila.peers);
    });

    test("ping + RPC echo of mixed JSON types", () =>
      macrotask(() => {
        assert.equal(pa.ping(remote_id), true);
        const proxy = laila.peers[remote_id];
        const payload = { k: [1, 2.5, "s", null, true], u: "ключ 键 🔑", nested: { a: {} } };
        // A trailing plain object is ``**kwargs`` in the JS calling convention;
        // ``call_with`` passes the dict positionally like Python's ``echo(payload)``.
        assert.deepEqual(proxy.echo.call_with([payload]), payload);
        assert.equal(proxy.echo("plain"), "plain");
        assert.equal(proxy.peer_count(), 1);
      }));

    test("remember(policy_id=py) rebuilds the CPython entry in JS", () =>
      macrotask(() => {
        const res = laila.remember({ entry_ids: py.info.ENTRY_ID, pool_nickname: "remote-store", policy_id: remote_id, persist: false });
        assert.equal(laila.get_active_policy().global_id, local.global_id);
        assert_reference(res.data);
        const entry = res.wait();
        assert.ok(entry instanceof Entry);
        assert.equal(entry.global_id, py.info.ENTRY_ID);
      }));

    test("remember(policy_id=py) of a numpy array yields an NDArray", () =>
      macrotask(() => {
        const entry = laila.remember({ entry_ids: py.info.ARRAY_ENTRY_ID, pool_nickname: "remote-store", policy_id: remote_id, persist: false }).wait();
        const arr = entry.data;
        assert.ok(arr instanceof NDArray);
        assert.equal(arr.dtype, "<f4");
        assert.deepEqual([...arr.shape], [3, 4]);
        assert.deepEqual(arr.tolist(), [
          [0, 1, 2, 3],
          [4, 5, 6, 7],
          [8, 9, 10, 11],
        ]);
      }));

    test("remember(policy_id=py) with default persist resolves to the rebuilt entry", () =>
      macrotask(() => {
        // The peer path ignores ``persist`` (as in Python: only ``pool`` is
        // forwarded); the local future resolves to the rebuilt Entry.
        const fut = laila.remember({ entry_ids: py.info.ENTRY_ID, pool_nickname: "remote-store", policy_id: remote_id });
        const entry = fut.wait(10);
        assert.ok(entry instanceof Entry);
        assert.equal(entry.global_id, py.info.ENTRY_ID);
        assert.equal(laila.get_active_policy().global_id, local.global_id);
      }));

    test("memorize(policy_id=py) stores a JS entry in the CPython pool", () =>
      macrotask(() => {
        const e1 = laila.constant({ from: "javascript", n: [1, 2, 3], f: 0.5, t: true, z: null }, { nickname: "js-push" });
        const e2 = laila.constant(NDArray.ones([2, 3], "<i8"), { nickname: "js-push-arr" });
        const f1 = laila.memorize(e1, { policy_id: remote_id, pool_nickname: "remote-store" });
        const f2 = laila.memorize(e2, { policy_id: remote_id, pool_nickname: "remote-store" });
        assert.equal(f1.data, e1.global_id);
        assert.equal(f2.data, e2.global_id);
        const proxy = laila.peers[remote_id];
        assert.deepEqual(proxy.probe_entry(e1.global_id), { present: true, type: "dict", data: { from: "javascript", n: [1, 2, 3], f: 0.5, t: true, z: null } });
        const probe_arr = proxy.probe_entry(e2.global_id);
        assert.equal(Number(probe_arr.sum), 6); // CPython ``6.0`` decodes as a PyFloat
        delete probe_arr.sum;
        assert.deepEqual(probe_arr, { present: true, type: "ndarray", dtype: "int64", shape: [2, 3] });
        assert.deepEqual(proxy.probe_entry("LAILA:ENTRY:absent"), { present: false });
        // and it round-trips back through CPython's pickle
        const back = laila.remember(e2.global_id, { policy_id: remote_id, pool_nickname: "remote-store", persist: false }).wait();
        assert.ok(back.data instanceof NDArray);
        assert.deepEqual(back.data.tolist(), [
          [1, 1, 1],
          [1, 1, 1],
        ]);
      }));

    test("forget(policy=py) deletes on the CPython side", () =>
      macrotask(() => {
        const e = laila.constant({ bye: 1 }, { nickname: "js-forget" });
        laila.memorize(e, { policy_id: remote_id, pool_nickname: "remote-store" }).wait(10);
        const proxy = laila.peers[remote_id];
        assert.equal(proxy.probe_entry(e.global_id).present, true);
        const f = laila.forget(e.global_id, { policy: remote_id, pool: "remote-store" });
        if (f && typeof f.wait === "function") f.wait(10);
        assert.ok(wait_until(() => proxy.probe_entry(e.global_id).present === false, 10));
      }));

    test("stream lane: JS sends, CPython echoes reversed", { skip: transport === "tcpip" ? "WebSocket transport carries RPC only" : false }, () =>
      macrotask(() => {
        const ch = laila.peers[remote_id].channel("video");
        assert.ok(ch.opened);
        const payloads = STREAM_SIZES.map(_blob);
        for (const p of payloads) ch.send(p);
        const relay = ch.relay(20);
        const got = payloads.map(() => relay.__next__());
        for (const g of got) assert.ok(g instanceof StreamEntry);
        assert.deepEqual(
          got.map((g) => Buffer.from(g.data)),
          payloads.map((p) => Buffer.from([...p].reverse())),
        );
        assert.deepEqual(got.map((g) => g.stream.seq), payloads.map((_, i) => i));
        for (const g of got) {
          assert.equal(g.stream.peer_id, remote_id);
          assert.equal(g.stream.channel, "video");
        }
        assert.equal(ch.dropped, 0);
        assert.equal(ch.tx_seq, payloads.length);
        ch.close();
      }));

    test("remote error surfaces as a JS exception", () =>
      macrotask(() => {
        assert.throws(() => laila.peers[remote_id].no_such_method(), /AttributeError|no attribute|Remote RPC error/);
      }));

    test("remove_peer is noticed by CPython; re-peer works", () =>
      macrotask(() => {
        const proxy = laila.peers[remote_id];
        assert.equal(proxy.peer_count(), 1);
        local.central.communication.remove_peer(remote_id);
        assert.ok(!(remote_id in laila.peers));
        time.sleep(0.3);
        const again = laila.add_peer(uri(), py.info.SECRET);
        assert.equal(again, remote_id);
        assert.ok(wait_until(() => laila.peers[remote_id].peer_count() === 1, 5));
      }));
  });
}

// ---------------------------------------------------------------------------
// Python client -> JS server
// ---------------------------------------------------------------------------
for (const transport of ["tcpip", "tcp"]) {
  describe(`Python client -> JS server [${transport}]`, { skip: have_python ? false : `no CPython laila (${PY}, PYTHONPATH=${PYTHONPATH})` }, () => {
    let original, policy, pool, proto, ref_entry, checks;

    before(async () => {
      original = laila.get_active_policy();
      await macrotask(() => {
        policy = new DefaultPolicy();
        laila.activate_policy(policy);
        pool = new DefaultPool();
        laila.memory.extend(pool, { pool_nickname: "remote-store" });
        ref_entry = laila.constant({ ...REFERENCE_PAYLOAD, message: "hello-from-javascript" }, { nickname: "js-ref" });
        laila.memorize(ref_entry, { pool_nickname: "remote-store" }).wait(10);
        object_setattr(policy, "echo", (x) => x);
        object_setattr(policy, "probe_entry", (gid) => _probe_entry(pool, gid));
        const Proto = transport === "tcpip" ? DefaultTCPIPProtocol : DefaultTCPProtocol;
        proto = new Proto({ host: "127.0.0.1", port: 0 });
        laila.communication.add_connection(proto);
      });
      const uri = transport === "tcpip" ? `ws://127.0.0.1:${proto.bound_port}` : `tcp://127.0.0.1:${proto.bound_port}`;
      // Echo lane "video" back reversed while the CPython client runs
      // (mirror of ``py_peer._echo_lane``); ends when the lane closes.
      const echo_lane = (async () => {
        if (!proto.constructor.supports_channels) return;
        const comm = policy.central.communication;
        let peer_id = null;
        while (peer_id === null) {
          for (const pid of comm.peers.keys()) if (comm.peers[pid].channels().includes("video")) peer_id = pid;
          if (peer_id === null) await new Promise((r) => setTimeout(r, 20));
        }
        const ch = comm.peers[peer_id].channel("video");
        try {
          for await (const e of ch.relay()) ch.send(Buffer.from([...Buffer.from(e.data)].reverse()));
        } catch {
          /* lane closed by the peer */
        }
      })();
      checks = await run_py_client(uri, proto.peer_secret_key, policy.global_id, ref_entry.global_id);
      await Promise.race([echo_lane, new Promise((r) => setTimeout(r, 2000))]);
    });
    after(async () => {
      await macrotask(() => {
        try {
          policy.central.communication.stop();
        } finally {
          laila.activate_policy(original);
        }
      });
    });

    test("CPython peered to the JS policy", () => {
      assert.equal(checks.peered.ok, true, JSON.stringify(checks.peered));
      assert.equal(checks.ping.ok, true);
    });
    test("RPC echo answered by JS", () => {
      assert.deepEqual(checks.echo.value, { k: [1, 2.5, "s", null, true], u: "ключ" });
    });
    test("CPython remember(policy_id=js) rebuilt the JS entry", () => {
      const r = checks.remember;
      assert.equal(r.gid, ref_entry.global_id);
      assert.equal(r.active_restored, true);
      assert.equal(r.data.message, "hello-from-javascript");
      assert.equal(r.data.int, 7);
      assert.equal(r.data.float, 2.5);
      assert.equal(r.data.bool, true);
      assert.equal(r.data.none, null);
      assert.deepEqual(r.data.list, [1, "two", 3.0, false, null]);
      assert.deepEqual(r.data.nested, { a: { b: [1, 2, { c: "d" }] } });
      assert.equal(r.data.unicode, "ключ 键 🔑");
    });
    test("CPython memorize(policy_id=js) landed in the JS pool", () =>
      macrotask(() => {
        const m = checks.memorize;
        assert.deepEqual(m.gids, m.expected);
        const [gid_dict, gid_arr] = m.expected;
        assert.ok(pool.__contains__(gid_dict));
        assert.ok(pool.__contains__(gid_arr));
        assert.deepEqual(_entry_of(pool, gid_dict).data, { from: "python", n: [1, 2, 3] });
        const arr = _entry_of(pool, gid_arr).data;
        assert.ok(arr instanceof NDArray);
        assert.equal(arr.dtype, "<i8");
        assert.deepEqual([...arr.shape], [2, 3]);
        assert.deepEqual(
          arr.tolist().map((row) => row.map(Number)),
          [
            [4, 4, 4],
            [4, 4, 4],
          ],
        );
        // what CPython saw through the probe RPC matches
        assert.deepEqual(checks.probe.pushed, { present: true, type: "dict", data: { from: "python", n: [1, 2, 3] } });
        assert.deepEqual(checks.probe.pushed_arr, { present: true, type: "ndarray", dtype: "<i8", shape: [2, 3], sum: 24 });
      }));
    test("CPython remembered its numpy array back from JS intact", () => {
      assert.deepEqual(checks.remember_array, { check: "remember_array", is_ndarray: true, dtype: "int64", shape: [2, 3], sum: 24 });
    });
    test("stream lane: CPython sends, JS echoes reversed", { skip: transport === "tcpip" ? "WebSocket transport carries RPC only" : false }, () => {
      const st = checks.stream;
      assert.ok(st, "python client reported no stream check");
      assert.deepEqual(st.sent, STREAM_SIZES.map((n) => sha1(_blob(n))));
      assert.deepEqual(st.got, st.sent);
      assert.deepEqual(st.seqs, STREAM_SIZES.map((_, i) => i));
      assert.deepEqual(st.channel, ["video"]);
      assert.deepEqual(st.peer, [policy.global_id]);
      assert.equal(st.dropped, 0);
    });

    test("CPython remove_peer was honoured", () =>
      macrotask(() => {
        assert.equal(checks.removed.has_peer, false);
        assert.ok(wait_until(() => policy.central.communication.peers.__len__() === 0, 5));
      }));
  });
}
