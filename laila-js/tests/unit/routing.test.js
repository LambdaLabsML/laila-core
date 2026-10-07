/**
 * Root-level memory routing over peers + transport catalogue conformance:
 *   tests/functional/policy/communication/unit_tests/test_peer_routing.py
 *   tests/functional/policy/communication/unit_tests/test_src_dst_transfer.py
 *   tests/functional/policy/communication/unit_tests/test_emulated_peers_memory.py
 *   tests/functional/policy/communication/unit_tests/test_transport_memory_roundtrip.py
 *   tests/functional/policy/communication/unit_tests/test_comm_id_routing.py
 *   tests/functional/policy/communication/unit_tests/test_remote_future_materialize.py
 *   tests/functional/policy/communication/unit_tests/test_liveness.py
 *   tests/functional/policy/communication/unit_tests/test_transport_environment.py
 *   tests/functional/policy/communication/unit_tests/test_p2p_and_dep_transports.py
 *   tests/functional/policy/communication/unit_tests/test_graceful_teardown.py
 *
 * The subprocess peer of ``test_peer_routing.py`` is a Node child here; the
 * same scenario against a *Python* child lives in ``tests/interop``.
 */
import { test, describe, before, after, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import readline from "node:readline";
import { spawn } from "node:child_process";

const S = new URL("../../src/", import.meta.url).href;
const laila = (await import(S + "index.js")).default;
const defaults = await import(S + "macros/defaults.js");
const E = await import(S + "_compat/errors.js");
const time = await import(S + "_compat/time.js");
const asyncio = await import(S + "_compat/asyncio.js");
const TH = await import(S + "_compat/threading.js");
const { Field, PrivateAttr, define_fields, define_private, object_setattr } = await import(S + "_compat/pydantic.js");
const { repr } = await import(S + "_compat/pyrepr.js");
const { dict_get, dict_has, dict_items } = await import(S + "_compat/pytypes.js");
const { _LAILA_IDENTIFIABLE_OBJECT: _IO } = await import(S + "basics/index.js");
const { transformation_base64 } = await import(S + "entry/index.js");
const { _FUTURE_SCOPE, _GROUP_FUTURE_SCOPE } = await import(S + "macros/strings.js");
const { RemoteFuture } = await import(S + "policy/central/command/schema/future/future/remote_future.js");
const { _LAILA_IDENTIFIABLE_COMMUNICATION } = await import(S + "policy/central/communication/schema/base.js");
const PROTOCOLS = await import(S + "policy/central/communication/protocols/index.js");
const { _P2PStreamRPCProtocol } = await import(S + "policy/central/communication/protocols/_carriers/p2p.js");
const loopthread = await import(S + "policy/central/communication/protocols/_carriers/loopthread.js");
const { _LAILA_IDENTIFIABLE_COMM_PROTOCOL, register_comm_protocol } = PROTOCOLS;

const {
  DefaultPolicy,
  DefaultPool,
  DefaultTCPIPProtocol,
  DefaultTCPProtocol,
  DefaultUDPProtocol,
  DefaultUnixSocketProtocol,
  DefaultLoopbackProtocol,
  DefaultLoRaProtocol,
  DefaultBluetoothProtocol,
} = defaults;

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

const _attach = (policy, name, fn) => object_setattr(policy, name, fn);
const _conn = (policy) => Object.values(policy.central.communication.connections)[0];
const _mk = (proto) => {
  const p = new DefaultPolicy();
  p.central.communication.add_connection(proto);
  return p;
};
const _stop = (...policies) => {
  for (const p of policies) {
    try {
      p.central.communication.stop();
    } catch {
      /* ignore */
    }
  }
};
const _wait = (fut) => {
  if (fut !== null && fut !== undefined && typeof fut.wait === "function") fut.wait(10);
};
const _pool_of = (policy, nickname) => {
  const router = policy.central.memory.pool_router;
  return dict_get(router.pools, dict_get(router.pools_nicknames, nickname));
};
const _alpha_pool = (policy) => dict_get(policy.central.memory.pool_router.pools, policy.central.memory.alpha_pool);
const _in = (pool, gid) => pool.__contains__(gid);

function wait_until(pred, timeout = 5.0) {
  const deadline = time.monotonic() + timeout;
  while (time.monotonic() < deadline) {
    if (pred()) return true;
    time.sleep(0.01);
  }
  return pred();
}

function _uri(name, policy_b, proto_b) {
  if (name === "unix") return `unix://${proto_b.bound_path}`;
  if (name === "loopback") return `loopback://${policy_b.global_id}`;
  return `${name}://127.0.0.1:${proto_b.bound_port}`;
}

/** Every concrete transport behind a ``Default*Protocol`` alias (unique by class). */
function _all_concrete_transports() {
  const seen = new Map();
  for (const name of Object.keys(defaults)) {
    if (name.startsWith("Default") && name.endsWith("Protocol")) {
      const cls = defaults[name];
      seen.set(cls.name, cls);
    }
  }
  return [...seen.values()];
}

// ---------------------------------------------------------------------------
// test_peer_routing.py :: 1. tokens / scaffolds / _resolve_protocol_for_token
// ---------------------------------------------------------------------------
describe("TestProtocolTokens", () => {
  test("tcpip protocol name", () => assert.equal(DefaultTCPIPProtocol.protocol_name, "tcpip"));
  test("tcpip matches tokens and aliases", () => {
    for (const token of ["tcpip", "TCPIP", "ws", "wss", "websocket"]) assert.ok(DefaultTCPIPProtocol.matches_token(token), token);
  });
  test("tcpip no longer claims raw tcp token", () => assert.equal(DefaultTCPIPProtocol.matches_token("tcp"), false));
  test("tcpip rejects other tokens", () => {
    assert.equal(DefaultTCPIPProtocol.matches_token("lora"), false);
    assert.equal(DefaultTCPIPProtocol.matches_token("bluetooth"), false);
  });
  test("lora scaffold", () => {
    assert.equal(DefaultLoRaProtocol.protocol_name, "lora");
    assert.ok(DefaultLoRaProtocol.matches_token("lora"));
    assert.ok(DefaultLoRaProtocol.can_handle_uri("lora://node-7"));
    assert.equal(DefaultLoRaProtocol.can_handle_uri("ws://host:1"), false);
    const proto = new DefaultLoRaProtocol();
    assert.equal(proto.has_peer("anyone"), false);
    assert.throws(() => proto.start(), E.NotImplementedError);
    assert.throws(() => proto.connect("lora://x", "secret"), E.NotImplementedError);
    assert.throws(() => proto.send_rpc("x", ["a"], [], {}), E.NotImplementedError);
  });
  test("bluetooth scaffold", () => {
    assert.equal(DefaultBluetoothProtocol.protocol_name, "bluetooth");
    for (const token of ["bluetooth", "bt", "ble", "BT"]) assert.ok(DefaultBluetoothProtocol.matches_token(token), token);
    assert.ok(DefaultBluetoothProtocol.can_handle_uri("bt://AA:BB:CC"));
    assert.equal(DefaultBluetoothProtocol.can_handle_uri("lora://x"), false);
    const proto = new DefaultBluetoothProtocol();
    assert.equal(proto.has_peer("anyone"), false);
    assert.throws(() => proto.start(), E.NotImplementedError);
  });
});

describe("TestResolveProtocolForToken", () => {
  test("no connections raises", () => {
    const comm = new _LAILA_IDENTIFIABLE_COMMUNICATION({ policy_id: "test" });
    assert.throws(() => comm._resolve_protocol_for_token("tcpip"), E.ConnectionError);
  });
  test("None defaults to tcpip", () =>
    macrotask(() => {
      const comm = new _LAILA_IDENTIFIABLE_COMMUNICATION({ policy_id: "test" });
      const tcp = new DefaultTCPIPProtocol();
      comm.add_connection(tcp);
      try {
        assert.equal(comm._resolve_protocol_for_token(null), tcp);
        assert.equal(comm._resolve_protocol_for_token("websocket"), tcp);
      } finally {
        comm.stop();
      }
    }));
  test("unregistered token raises", () =>
    macrotask(() => {
      const comm = new _LAILA_IDENTIFIABLE_COMMUNICATION({ policy_id: "test" });
      const tcp = new DefaultTCPIPProtocol();
      comm.add_connection(tcp);
      try {
        assert.throws(() => comm._resolve_protocol_for_token("lora"), E.ConnectionError);
      } finally {
        comm.stop();
      }
    }));
});

// ---------------------------------------------------------------------------
// test_peer_routing.py :: 2. policy_id routing against a local target
// ---------------------------------------------------------------------------
describe("TestPolicyIdLocalRouting", () => {
  let original, A, L, pool;
  beforeEach(() => {
    original = laila.get_active_policy();
    A = new DefaultPolicy();
    L = new DefaultPolicy();
    laila.activate_policy(L);
    pool = new DefaultPool();
    L.central.memory.extend(pool, { pool_nickname: "l-store" });
    laila.activate_policy(A);
  });
  afterEach(() => laila.activate_policy(original));

  test("unknown policy_id raises", () => {
    assert.throws(() => laila.memorize({ entries: laila.constant(1), policy_id: "LAILA:POLICY:nope" }), E.ConnectionError);
  });
  test("memorize into local target and active restored", () =>
    macrotask(() => {
      const entry = laila.constant({ x: 1 }, { nickname: "le" });
      const gid = entry.global_id;
      _wait(laila.memorize({ entries: entry, policy_id: L.global_id, pool_nickname: "l-store" }));
      assert.equal(laila.get_active_policy().global_id, A.global_id);
      assert.ok(_in(pool, gid));
    }));
  test("remember from local target round trips", () =>
    macrotask(() => {
      const entry = laila.constant({ msg: "round-trip" }, { nickname: "le2" });
      const gid = entry.global_id;
      _wait(laila.memorize({ entries: entry, policy_id: L.global_id, pool_nickname: "l-store" }));
      const res = laila.remember({ entry_ids: gid, policy_id: L.global_id, pool_nickname: "l-store", persist: false });
      assert.equal(laila.get_active_policy().global_id, A.global_id);
      assert.deepEqual(res.data, { msg: "round-trip" });
    }));
});

// ---------------------------------------------------------------------------
// test_peer_routing.py :: 3. remember(policy_id=...) against a subprocess peer
// ---------------------------------------------------------------------------
const REMOTE_PEER = new URL("./fixtures/remote_peer.js", import.meta.url);

/** Spawn the fixture peer and wait for its ``READY`` banner. */
export function spawn_peer(cmd, args, env = {}) {
  const proc = spawn(cmd, args, { stdio: ["ignore", "pipe", "inherit"], env: { ...process.env, ...env } });
  const info = {};
  const ready = new Promise((resolve, reject) => {
    const rl = readline.createInterface({ input: proc.stdout });
    rl.on("line", (line) => {
      line = line.trim();
      if (line === "READY") {
        resolve();
        return;
      }
      const i = line.indexOf("=");
      if (i > 0) info[line.slice(0, i)] = line.slice(i + 1);
    });
    proc.on("exit", (code) => reject(new Error(`peer exited early (code ${code})`)));
    setTimeout(() => reject(new Error("peer did not become ready")), 30_000).unref();
  });
  return { proc, info, ready };
}

describe("TestPeerRoutingSubprocess", () => {
  let original, peer, local, remote_id;
  before(async () => {
    original = laila.get_active_policy();
    peer = spawn_peer(process.execPath, [REMOTE_PEER.pathname]);
    await peer.ready;
    await macrotask(() => {
      local = new DefaultPolicy();
      laila.activate_policy(local);
      laila.communication.add_connection(new DefaultTCPIPProtocol({ host: "127.0.0.1", port: 0, peer_secret_key: peer.info.SECRET }));
      remote_id = laila.communication.add_tcpip_peer("127.0.0.1", Number(peer.info.PORT), peer.info.SECRET);
      time.sleep(0.3);
    });
  });
  after(async () => {
    await macrotask(() => {
      try {
        local.central.communication.stop();
      } finally {
        peer.proc.kill("SIGTERM");
        laila.activate_policy(original);
      }
    });
  });

  test("peered with subprocess policy", () => assert.equal(remote_id, peer.info.POLICY_ID));
  test("remember with policy_id fetches from peer", () =>
    macrotask(() => {
      const res = laila.remember({
        entry_ids: peer.info.ENTRY_ID,
        pool_nickname: "remote-store",
        policy_id: peer.info.POLICY_ID,
        persist: false,
      });
      assert.equal(laila.get_active_policy().global_id, local.global_id);
      assert.deepEqual(res.data, { message: "hello-from-remote" });
      assert.equal(res.wait().global_id, peer.info.ENTRY_ID);
    }));
});

// ---------------------------------------------------------------------------
// test_src_dst_transfer.py
// ---------------------------------------------------------------------------
describe("TestResolvePolicyRef", () => {
  test("None passes through", () => assert.equal(laila._resolve_policy_ref(null), null));
  test("gid passes through", () => {
    const p = new DefaultPolicy();
    assert.equal(laila._resolve_policy_ref(p.global_id), p.global_id);
  });
  test("object yields its gid", () => {
    const p = new DefaultPolicy();
    assert.equal(laila._resolve_policy_ref(p), p.global_id);
  });
  test("nickname is deterministic", () => {
    const first = laila._resolve_policy_ref("edge-sensor");
    const second = laila._resolve_policy_ref("edge-sensor");
    assert.equal(first, second);
    assert.ok(_IO.is_laila_resource(first));
    const p = new DefaultPolicy();
    p.uuid = _IO.generate_uuid_from_nickname("edge-sensor");
    assert.equal(p.global_id, first);
  });
});

describe("TestDstRoutingLocalTarget", () => {
  let original, A, L;
  beforeEach(() => {
    original = laila.get_active_policy();
    A = new DefaultPolicy();
    L = new DefaultPolicy();
    laila.activate_policy(L);
    L.central.memory.extend(new DefaultPool(), { pool_nickname: "l-store" });
    laila.activate_policy(A);
  });
  afterEach(() => laila.activate_policy(original));

  test("memorize into dst_policy", () =>
    macrotask(() => {
      const entry = laila.constant({ x: 1 }, { nickname: "sd-e1" });
      _wait(laila.memorize(entry, { dst_policy: L.global_id, dst_pool: "l-store" }));
      assert.equal(laila.get_active_policy().global_id, A.global_id);
      assert.ok(_in(_pool_of(L, "l-store"), entry.global_id));
    }));
  test("remember from dst_policy", () =>
    macrotask(() => {
      const entry = laila.constant({ msg: "rt" }, { nickname: "sd-e2" });
      _wait(laila.memorize(entry, { dst_policy: L.global_id, dst_pool: "l-store" }));
      const res = laila.remember(entry.global_id, { dst_policy: L.global_id, dst_pool: "l-store", persist: false });
      assert.equal(laila.get_active_policy().global_id, A.global_id);
      assert.deepEqual(res.data, { msg: "rt" });
    }));
  test("forget on dst_policy", () =>
    macrotask(() => {
      const entry = laila.constant({ d: 9 }, { nickname: "sd-e3" });
      _wait(laila.memorize(entry, { dst_policy: L.global_id, dst_pool: "l-store" }));
      assert.ok(_in(_pool_of(L, "l-store"), entry.global_id));
      _wait(laila.forget(entry.global_id, { policy: L.global_id, pool: "l-store" }));
      assert.equal(laila.get_active_policy().global_id, A.global_id);
      assert.equal(_in(_pool_of(L, "l-store"), entry.global_id), false);
    }));
  test("dst_policy by nickname", () =>
    macrotask(() => {
      L.uuid = _IO.generate_uuid_from_nickname("sd-edge");
      laila.activate_policy(L);
      laila.activate_policy(A);
      const entry = laila.constant({ via: "nickname" }, { nickname: "sd-e4" });
      _wait(laila.memorize(entry, { dst_policy: "sd-edge", dst_pool: "l-store" }));
      assert.ok(_in(_pool_of(L, "l-store"), entry.global_id));
    }));
  test("back-compat policy_id alias", () =>
    macrotask(() => {
      const entry = laila.constant({ legacy: true }, { nickname: "sd-e5" });
      _wait(laila.memorize({ entries: entry, policy_id: L.global_id, pool_nickname: "l-store" }));
      assert.ok(_in(_pool_of(L, "l-store"), entry.global_id));
    }));
});

describe("TestThreePartyRelayLocal", () => {
  let original, A, B, C;
  beforeEach(() => {
    original = laila.get_active_policy();
    A = new DefaultPolicy();
    B = new DefaultPolicy();
    C = new DefaultPolicy();
    laila.activate_policy(B);
    B.central.memory.extend(new DefaultPool(), { pool_nickname: "b-store" });
    laila.activate_policy(C);
    C.central.memory.extend(new DefaultPool(), { pool_nickname: "c-store" });
    laila.activate_policy(A);
  });
  afterEach(() => laila.activate_policy(original));

  const _seed = (policy, nickname, entry) => {
    laila.activate_policy(policy);
    try {
      _wait(laila.memorize(entry, { dst_pool: nickname }));
    } finally {
      laila.activate_policy(A);
    }
  };

  test("push relay src to dst", () =>
    macrotask(() => {
      const entry = laila.constant({ relay: "push" }, { nickname: "sd-r1" });
      _seed(B, "b-store", entry);
      laila.memorize(entry.global_id, { src_policy: B.global_id, src_pool: "b-store", dst_policy: C.global_id, dst_pool: "c-store" });
      assert.equal(laila.get_active_policy().global_id, A.global_id);
      assert.ok(_in(_pool_of(C, "c-store"), entry.global_id));
    }));
  test("pull relay dst into src", () =>
    macrotask(() => {
      const entry = laila.constant({ relay: "pull" }, { nickname: "sd-r2" });
      _seed(C, "c-store", entry);
      laila.remember(entry.global_id, { src_policy: B.global_id, src_pool: "b-store", dst_policy: C.global_id, dst_pool: "c-store" });
      assert.equal(laila.get_active_policy().global_id, A.global_id);
      assert.ok(_in(_pool_of(B, "b-store"), entry.global_id));
    }));
  test("unknown src policy raises", () => {
    assert.throws(
      () =>
        laila.memorize("LAILA:POLICY:00000000-0000-0000-0000-000000000000", {
          src_policy: "LAILA:POLICY:11111111-1111-1111-1111-111111111111",
          dst_policy: C.global_id,
        }),
      E.ConnectionError,
    );
  });
});

describe("TestStandalonePool", () => {
  let original, A, standalone;
  beforeEach(() => {
    original = laila.get_active_policy();
    A = new DefaultPolicy();
    laila.activate_policy(A);
    standalone = new DefaultPool();
  });
  afterEach(() => laila.activate_policy(original));

  test("memorize into standalone pool object", () =>
    macrotask(() => {
      const entry = laila.constant({ s: "andalone" }, { nickname: "sd-s1" });
      _wait(laila.memorize(entry, { dst_pool: standalone }));
      assert.ok(_in(standalone, entry.global_id));
    }));
  test("remember from standalone pool object", () =>
    macrotask(() => {
      const entry = laila.constant({ hello: "pool" }, { nickname: "sd-s2" });
      _wait(laila.memorize(entry, { dst_pool: standalone }));
      const res = laila.remember(entry.global_id, { dst_pool: standalone, persist: false });
      assert.deepEqual(res.data, { hello: "pool" });
    }));
});

// ---------------------------------------------------------------------------
// test_emulated_peers_memory.py
// ---------------------------------------------------------------------------
const _EMULATED_CASES = {
  loopback: DefaultLoopbackProtocol,
  tcp: DefaultTCPProtocol,
  udp: DefaultUDPProtocol,
  unix: DefaultUnixSocketProtocol,
};

describe("TestCrossPeerMemory", () => {
  let original;
  beforeEach(() => (original = laila.get_active_policy()));
  afterEach(() => laila.activate_policy(original));

  const _peer = (name, factory) => {
    const a = _mk(new factory());
    const b = _mk(new factory());
    const pb = _conn(b);
    const bid = a.central.communication.add_peer(_uri(name, b, pb), pb.peer_secret_key);
    return [a, b, bid];
  };

  for (const [name, factory] of Object.entries(_EMULATED_CASES)) {
    test(`memorize then remember across ${name}`, () =>
      macrotask(() => {
        const [a, b, bid] = _peer(name, factory);
        try {
          laila.activate_policy(a);
          const payload = { transport: name, vals: [1, 2, 3], ok: true };
          const entry = laila.constant(payload);
          const gid = entry.global_id;
          const mfut = laila.memorize(entry, { policy_id: bid });
          assert.equal(mfut.data, gid, name);
          const got = laila.remember(gid, { policy_id: bid });
          assert.deepEqual(got.data, payload, name);
          assert.equal(got.wait().global_id, gid, name);
        } finally {
          _stop(a, b);
        }
      }));
  }

  test("localhost round trip latency is small", () =>
    macrotask(() => {
      const [a, b, bid] = _peer("tcp", DefaultTCPProtocol);
      try {
        laila.activate_policy(a);
        const entry = laila.constant({ x: 1 });
        laila.memorize(entry, { policy_id: bid }).wait();
        const t0 = time.perf_counter();
        void laila.remember(entry.global_id, { policy_id: bid }).data;
        const elapsed = time.perf_counter() - t0;
        assert.ok(elapsed < 1.0, `localhost round-trip too slow: ${elapsed.toFixed(3)}s`);
      } finally {
        _stop(a, b);
      }
    }));
});

// ---------------------------------------------------------------------------
// test_transport_memory_roundtrip.py
// ---------------------------------------------------------------------------
describe("TestMemoryOverTransport", () => {
  let original;
  beforeEach(() => (original = laila.get_active_policy()));
  afterEach(() => laila.activate_policy(original));

  const cases = {
    tcp: DefaultTCPProtocol,
    udp: DefaultUDPProtocol,
    unix: DefaultUnixSocketProtocol,
    loopback: DefaultLoopbackProtocol,
    matter: defaults.DefaultMatterProtocol,
    sixlowpan: defaults.DefaultSixLoWPANProtocol,
  };
  for (const [name, factory] of Object.entries(cases)) {
    test(`memorize round trips over ${name}`, () =>
      macrotask(() => {
        const a = _mk(new factory());
        const b = _mk(new factory());
        try {
          const pb = _conn(b);
          a.central.communication.add_peer(_uri(name, b, pb), pb.peer_secret_key);

          laila.activate_policy(b);
          const entry = laila.constant({ transport: name, n: 7 });
          const gid = entry.global_id;
          b.central.memory.memorize(entry);
          const pool_b = _alpha_pool(b);
          assert.ok(wait_until(() => dict_has(pool_b.resource, gid), 8.0), `${name}: B never stored the entry`);

          _attach(b, "pool_size", () => Object.keys(pool_b.resource).length);
          _attach(b, "has_entry", (g) => dict_has(pool_b.resource, g));
          laila.activate_policy(a);
          const proxy = a.central.communication.peers[b.global_id];
          assert.equal(proxy.pool_size(), Object.keys(pool_b.resource).length, name);
          assert.equal(proxy.has_entry(gid), true, `${name}: remote read missed entry`);
          assert.equal(proxy.has_entry("LAILA:ENTRY:absent"), false);
        } finally {
          _stop(a, b);
        }
      }));
  }
});

// ---------------------------------------------------------------------------
// test_comm_id_routing.py
// ---------------------------------------------------------------------------
describe("TestRegistryConsistency", () => {
  test("class names are unique", () => {
    const names = [...PROTOCOLS.iter_comm_protocols()].map((c) => c.name);
    assert.equal(names.length, new Set(names).size);
  });
  test("no token claimed by two transports", () => {
    const concrete = _all_concrete_transports();
    assert.ok(concrete.length >= 50);
    const tokens = new Set();
    for (const cls of concrete) {
      for (const t of cls._TOKEN_ALIASES ?? []) tokens.add(t);
      tokens.add(cls.protocol_name);
    }
    for (const token of tokens) {
      const claimers = concrete.filter((c) => c.matches_token(token)).map((c) => c.name);
      assert.ok(claimers.length <= 1, `token ${repr(token)} claimed by ${claimers}`);
    }
  });
  test("no uri scheme claimed by two transports", () => {
    const concrete = _all_concrete_transports();
    const uris = [
      "tcp://h:1", "udp://h:1", "tls://h:1", "tcps://h:1", "unix:///x", "loopback://gid", "ws://h:1", "wss://h:1",
      "lora://n", "bt://m", "ethernet://h:1", "wifi://h:1", "wifidirect://h:1", "mqtt://p", "mqtts://p", "amqp://p",
      "amqps://p", "zmq://p", "zeromq://p", "cellular://h:1", "gsm://h:1", "lte://h:1", "nr5g://h:1", "nbiot://h:1",
      "ltem://h:1", "matter://h:1", "serial:///dev/x", "uart:///dev/x", "rs232:///dev/x", "rs485:///dev/x", "can://vcan0",
      "sigfox:///dev/x", "satellite:///dev/x", "usb:///dev/x", "modbustcp://h:1", "modbusrtu:///dev/x", "i2c://1/0x20",
      "spi://0.0", "enip://h", "sixlowpan://h:1", "coap://h:1", "coaps://h:1", "http2://h:1", "http3://h:1", "quic://h:1",
      "dtls://h:1", "xmpp://p", "dds://p", "opc.tcp://h:1", "grpc://h:1", "lorawan://n", "zigbee://n", "thread://n",
      "zwave://n", "nfc://n", "ant://n", "espnow://n", "uwb://n", "irda://n", "rfid:///dev/x", "lin:///dev/x",
      "onewire://n", "i2s://c", "sdio://f", "profinet://s", "ethercat://s",
    ];
    for (const uri of uris) {
      const claimers = concrete.filter((c) => c.can_handle_uri(uri)).map((c) => c.name);
      assert.ok(claimers.length <= 1, `uri ${repr(uri)} claimed by ${claimers}`);
    }
  });
});

describe("TestChannelSelection", () => {
  test("select by token and by id", () =>
    macrotask(() => {
      const a = _mk(new DefaultTCPProtocol());
      const b = _mk(new DefaultTCPProtocol());
      try {
        const pb = _conn(b);
        const rid = a.central.communication.add_peer(`tcp://127.0.0.1:${pb.bound_port}`, pb.peer_secret_key);
        const comm = a.central.communication;
        const pa = _conn(a);
        assert.equal(comm._select_protocol_for_peer(rid, "tcp"), pa);
        assert.equal(comm._select_protocol_for_peer(rid, pa.global_id), pa);
        assert.equal(comm._select_protocol_for_peer(rid, null), pa);
        assert.throws(() => comm._select_protocol_for_peer(rid, "nonsense-channel"), E.ConnectionError);
      } finally {
        _stop(a, b);
      }
    }));
  test("via routes over named channel", () =>
    macrotask(() => {
      const a = _mk(new DefaultUDPProtocol());
      const b = _mk(new DefaultUDPProtocol());
      try {
        const pb = _conn(b);
        a.central.communication.add_peer(`udp://127.0.0.1:${pb.bound_port}`, pb.peer_secret_key);
        _attach(b, "echo", (m) => `e:${m}`);
        const proxy = a.central.communication.peers[b.global_id];
        assert.equal(proxy.via("udp").echo("z"), "e:z");
        assert.ok(repr(proxy.via("udp")).includes("via"));
      } finally {
        _stop(a, b);
      }
    }));
});

describe("TestRequestApi", () => {
  let original;
  beforeEach(() => (original = laila.get_active_policy()));
  afterEach(() => laila.activate_policy(original));

  test("request unknown peer raises", () => assert.throws(() => laila.request("LAILA:POLICY:nope"), E.ConnectionError));
  test("request returns bound proxy", () =>
    macrotask(() => {
      const a = _mk(new DefaultTCPProtocol());
      const b = _mk(new DefaultTCPProtocol());
      laila.activate_policy(a);
      try {
        const pb = _conn(b);
        const rid = a.central.communication.add_peer(`tcp://127.0.0.1:${pb.bound_port}`, pb.peer_secret_key);
        _attach(b, "echo", (m) => `r:${m}`);
        const bound = laila.request(rid, "tcp");
        assert.equal(bound.echo("hey"), "r:hey");
      } finally {
        _stop(a, b);
      }
    }));
});

// ---------------------------------------------------------------------------
// test_remote_future_materialize.py
// ---------------------------------------------------------------------------
class _FakeComm {
  constructor(result_payload) {
    this._result_payload = result_payload;
    this.calls = [];
  }
  _send_rpc(_policy_id, path, _args, _kwargs, _opts = {}) {
    this.calls.push(path[0]);
    if (path[0] === "_get_future_result_entry" || path[0] === "_wait_future_entry") return this._result_payload;
    if (path[0] === "_get_future_status") return "FINISHED";
    if (path[0] === "_get_future_exception") return null;
    return null;
  }
}

function _mk_remote(scope, comm, { is_group = false } = {}) {
  const rf = new RemoteFuture({
    uuid: crypto.randomUUID().replace(/-/g, ""),
    scopes: [scope],
    policy_id: "LAILA:POLICY:peer",
    taskforce_id: "LAILA:TASK_FORCE:tf",
  });
  rf.bind(comm, { is_group });
  return rf;
}

describe("TestRemoteFutureMaterialize", () => {
  test("result is rebuilt entry and cached", () =>
    macrotask(() => {
      const entry = laila.constant({ k: 99, s: "hi" });
      const blob = entry.serialize(transformation_base64);
      const fake = new _FakeComm(blob);
      const rf = _mk_remote(_FUTURE_SCOPE, fake);
      const out = rf.result;
      assert.deepEqual(out.data, { k: 99, s: "hi" });
      assert.deepEqual(rf.data, { k: 99, s: "hi" });
      void rf.result;
      assert.equal(fake.calls.filter((c) => c === "_get_future_result_entry").length, 1);
    }));
  test("status is lightweight proxy", () =>
    macrotask(() => {
      const entry = laila.constant(1);
      const fake = new _FakeComm(entry.serialize(transformation_base64));
      const rf = _mk_remote(_FUTURE_SCOPE, fake);
      assert.ok(String(rf.status).endsWith("FINISHED"));
      assert.ok(fake.calls.includes("_get_future_status"));
      assert.ok(!fake.calls.includes("_get_future_result_entry"));
    }));
  test("wait returns entry", () =>
    macrotask(() => {
      const entry = laila.constant({ v: [1, 2] });
      const fake = new _FakeComm(entry.serialize(transformation_base64));
      const rf = _mk_remote(_FUTURE_SCOPE, fake);
      assert.deepEqual(rf.wait().data, { v: [1, 2] });
    }));
  test("group future returns lists", () =>
    macrotask(() => {
      const e1 = laila.constant("a");
      const e2 = laila.constant("b");
      const fake = new _FakeComm([e1.serialize(transformation_base64), e2.serialize(transformation_base64)]);
      const rf = _mk_remote(_GROUP_FUTURE_SCOPE, fake, { is_group: true });
      const entries = rf.result;
      assert.deepEqual(entries.map((e) => e.data), ["a", "b"]);
      assert.deepEqual(rf.data, ["a", "b"]);
    }));
});

// ---------------------------------------------------------------------------
// test_liveness.py
// ---------------------------------------------------------------------------
describe("TestPing", () => {
  test("ping live and unknown peer", () =>
    macrotask(() => {
      const a = _mk(new DefaultTCPProtocol());
      const b = _mk(new DefaultTCPProtocol());
      try {
        const pb = _conn(b);
        const bid = a.central.communication.add_peer(`tcp://127.0.0.1:${pb.bound_port}`, pb.peer_secret_key);
        const pa = _conn(a);
        assert.equal(pa.ping(bid), true, "live peer should ping True");
        assert.equal(pa.ping("nobody"), false);
        a.central.communication.remove_peer(bid);
        assert.equal(pa.has_peer(bid), false);
        assert.equal(pa.ping(bid), false);
      } finally {
        _stop(a, b);
      }
    }));
});

class _DeadProtocol extends _LAILA_IDENTIFIABLE_COMM_PROTOCOL {
  static protocol_name = "deadstub";
  static {
    define_private(this, { _held: PrivateAttr({ default_factory: () => new Set() }) });
  }
  start() {
    return null;
  }
  stop() {
    return null;
  }
  connect(_uri, _secret) {
    throw new E.NotImplementedError();
  }
  has_peer(peer_id) {
    return this._held.has(peer_id);
  }
  ping(_peer_id, _timeout = null) {
    return false;
  }
  disconnect(peer_id) {
    this._held.delete(peer_id);
  }
  send_rpc(_peer_id, _path, _args, _kwargs) {
    throw new E.ConnectionError("dead");
  }
}

describe("TestLivenessLoop", () => {
  test("dead peer is unregistered", () =>
    macrotask(() => {
      const comm = new _LAILA_IDENTIFIABLE_COMMUNICATION({ policy_id: "liveness-test" });
      comm.liveness_interval = 0.2;
      const proto = new _DeadProtocol();
      comm.add_connection(proto);
      try {
        proto._held.add("PEER1");
        comm._register_peer("PEER1");
        assert.ok(comm.peers.__contains__("PEER1"));
        assert.ok(wait_until(() => !comm.peers.__contains__("PEER1"), 5.0), "liveness loop did not drop dead peer");
      } finally {
        comm.stop();
      }
    }));
});

// ---------------------------------------------------------------------------
// test_transport_environment.py
// ---------------------------------------------------------------------------
describe("TestTransportEnvironment", () => {
  const classes = _all_concrete_transports();
  test("catalogue size", () => assert.ok(classes.length >= 50));
  test("config is json serializable", () => {
    for (const cls of classes) {
      const proto = new cls();
      const dump = proto.model_dump({ mode: "json" });
      JSON.stringify(dump);
    }
  });
  test("reconstructable from config", () => {
    for (const cls of classes) {
      const proto = new cls();
      const dump = proto.model_dump({ mode: "json" });
      const cfg = Object.fromEntries(Object.entries(dump).filter(([k]) => k !== "class_token"));
      const rebuilt = new cls(cfg);
      assert.equal(rebuilt.protocol_name, proto.protocol_name, cls.name);
    }
  });
  test("no runtime handle fields", () => {
    const allowed = (v) => v === null || ["boolean", "number", "string"].includes(typeof v) || Array.isArray(v) || (typeof v === "object" && (Object.getPrototypeOf(v) === Object.prototype || Object.getPrototypeOf(v) === null || typeof v.valueOf?.() === "number"));
    for (const cls of classes) {
      const proto = new cls();
      for (const fname of Object.keys(proto.constructor.model_fields)) {
        const val = proto[fname];
        assert.ok(allowed(val), `${cls.name}.${fname} is a non-config handle`);
      }
    }
  });
});

// ---------------------------------------------------------------------------
// test_p2p_and_dep_transports.py
// ---------------------------------------------------------------------------
const _P2P_LINKS = new Map();

class _FakeWriter {
  constructor(link_id, side) {
    this.link_id = link_id;
    this.side = side;
    this.closed = false;
  }
  write(data) {
    if (this.closed) return;
    const other = this.side === "a" ? "b" : "a";
    const entry = _P2P_LINKS.get(this.link_id)?.get(other);
    if (entry) {
      const [reader, loop] = entry;
      try {
        loop.call_soon_threadsafe(() => reader.feed_data(Buffer.from(data)));
      } catch (e) {
        if (!(e instanceof E.RuntimeError)) throw e;
      }
    }
  }
  async drain() {
    return null;
  }
  close() {
    this.closed = true;
  }
}

class _FakeP2PProtocol extends _P2PStreamRPCProtocol {
  static protocol_name = "fakep2p";
  static {
    define_fields(this, {
      link_id: ["str", Field({ default: "" })],
      side: ["str", Field({ default: "a" })],
    });
  }
  static matches_token(token) {
    return token.toLowerCase() === "fakep2p";
  }
  static can_handle_uri(uri) {
    return uri.startsWith("fakep2p://");
  }
  async _open_stream() {
    const reader = new asyncio.StreamReader();
    const loop = asyncio.get_running_loop();
    if (!_P2P_LINKS.has(this.link_id)) _P2P_LINKS.set(this.link_id, new Map());
    _P2P_LINKS.get(this.link_id).set(this.side, [reader, loop]);
    return [reader, new _FakeWriter(this.link_id, this.side)];
  }
}
register_comm_protocol(_FakeP2PProtocol);

describe("TestP2PCarrier", () => {
  beforeEach(() => _P2P_LINKS.clear());
  test("p2p echo round trip", () =>
    macrotask(() => {
      const a = _mk(new _FakeP2PProtocol({ link_id: "L", side: "a" }));
      const b = _mk(new _FakeP2PProtocol({ link_id: "L", side: "b" }));
      try {
        const pb = _conn(b);
        const rid = a.central.communication.add_peer("fakep2p://bus", pb.peer_secret_key);
        assert.equal(rid, b.global_id);
        _attach(b, "echo", (m) => `p2p:${m}`);
        assert.equal(a.central.communication.peers[b.global_id].echo("hi"), "p2p:hi");
      } finally {
        _stop(a, b);
      }
    }));
});

describe("TestSerialTransportsRouting", () => {
  const { DefaultUARTProtocol, DefaultRS232Protocol, DefaultRS485Protocol } = defaults;
  test("tokens and uris disjoint", () => {
    assert.ok(DefaultUARTProtocol.matches_token("serial"));
    assert.ok(DefaultUARTProtocol.can_handle_uri("serial:///dev/ttyUSB0"));
    assert.ok(DefaultRS232Protocol.matches_token("rs-232"));
    assert.ok(DefaultRS232Protocol.can_handle_uri("rs232:///dev/ttyS0"));
    assert.ok(DefaultRS485Protocol.matches_token("rs485"));
    assert.equal(DefaultUARTProtocol.matches_token("rs485"), false);
  });
  test("uart without port raises clear error", () =>
    macrotask(() => {
      const proto = new DefaultUARTProtocol();
      assert.throws(() => proto.start(), (e) => e instanceof E.RuntimeError && /port/.test(String(e.message).toLowerCase()));
      proto.stop();
    }));
  test("serial modem transports route", () =>
    macrotask(() => {
      const { DefaultSatelliteProtocol, DefaultSigfoxProtocol, DefaultUSBProtocol } = defaults;
      assert.ok(DefaultSigfoxProtocol.can_handle_uri("sigfox:///dev/ttyUSB0"));
      assert.ok(DefaultSatelliteProtocol.matches_token("iridium"));
      assert.ok(DefaultUSBProtocol.can_handle_uri("usb:///dev/ttyACM0"));
      for (const factory of [DefaultSigfoxProtocol, DefaultSatelliteProtocol, DefaultUSBProtocol]) {
        const proto = new factory();
        assert.throws(() => proto.start(), E.RuntimeError);
        proto.stop();
      }
    }));
});

describe("TestCapabilityErrors", () => {
  const cases = [
    ["can", () => new defaults.DefaultCANProtocol({ channel: "vcan0" }), "laila-core[can]"],
    ["mqtt", () => new defaults.DefaultMQTTProtocol(), "laila-core[mqtt]"],
    ["amqp", () => new defaults.DefaultAMQPProtocol(), "laila-core[amqp]"],
    ["zeromq", () => new defaults.DefaultZeroMQProtocol(), "laila-core[zmq]"],
    ["modbus", () => new defaults.DefaultModbusTCPProtocol(), "laila-core[modbus]"],
    ["i2c", () => new defaults.DefaultI2CProtocol(), "laila-core[i2c]"],
    ["spi", () => new defaults.DefaultSPIProtocol(), "laila-core[spi]"],
    ["enip", () => new defaults.DefaultENIPProtocol(), "laila-core[enip]"],
  ];
  for (const [name, factory, hint] of cases) {
    test(`${name} missing driver raises install hint`, () =>
      macrotask(() => {
        const proto = factory();
        try {
          assert.throws(() => proto.start(), (e) => e instanceof E.RuntimeError && String(e.message).includes(hint));
        } finally {
          proto.stop();
        }
      }));
  }
});

// ---------------------------------------------------------------------------
// test_graceful_teardown.py (wire transports + the in-memory p2p fake)
// ---------------------------------------------------------------------------
const _NO_LIVENESS = 600.0;

function _open_fds() {
  try {
    return new Set(fs.readdirSync("/proc/self/fd"));
  } catch {
    return new Set();
  }
}

class _Pair {
  constructor(name, { factory, uri_fn, kw_a, kw_b, reset = null }) {
    Object.assign(this, { name, factory, uri_fn, kw_a, kw_b, reset });
  }
  build() {
    if (this.reset !== null) this.reset();
    this.pa = new this.factory(this.kw_a);
    this.pb = new this.factory(this.kw_b);
    this.a = new DefaultPolicy();
    this.b = new DefaultPolicy();
    for (const pol of [this.a, this.b]) pol.central.communication.liveness_interval = _NO_LIVENESS;
    this.a.central.communication.add_connection(this.pa);
    this.b.central.communication.add_connection(this.pb);
    return this;
  }
  get ca() {
    return this.a.central.communication;
  }
  get cb() {
    return this.b.central.communication;
  }
  uri() {
    return this.uri_fn(this.pb, this.b);
  }
  peer() {
    const bid = this.ca.add_peer(this.uri(), this.pb.peer_secret_key);
    assert.equal(bid, this.b.global_id);
    assert.ok(wait_until(() => this.cb.peers.__contains__(this.a.global_id), 5), `${this.name}: B never registered A`);
    assert.ok(this.pa.ping(this.b.global_id), `${this.name}: ping A->B failed`);
  }
  stop_all() {
    for (const comm of [this.ca, this.cb]) {
      try {
        comm.stop();
      } catch {
        /* ignore */
      }
    }
  }
}

const PAIRS = {
  loopback: { factory: DefaultLoopbackProtocol, uri_fn: (_pb, b) => `loopback://${b.global_id}`, kw_a: {}, kw_b: {} },
  tcp: { factory: DefaultTCPProtocol, uri_fn: (pb) => `tcp://127.0.0.1:${pb.bound_port}`, kw_a: { host: "127.0.0.1" }, kw_b: { host: "127.0.0.1" } },
  unix: { factory: DefaultUnixSocketProtocol, uri_fn: (pb) => `unix://${pb.bound_path}`, kw_a: {}, kw_b: {} },
  udp: { factory: DefaultUDPProtocol, uri_fn: (pb) => `udp://127.0.0.1:${pb.bound_port}`, kw_a: { host: "127.0.0.1" }, kw_b: { host: "127.0.0.1" } },
  "tcpip-ws": { factory: DefaultTCPIPProtocol, uri_fn: (pb) => `ws://127.0.0.1:${pb.bound_port}`, kw_a: { host: "127.0.0.1" }, kw_b: { host: "127.0.0.1" } },
  "p2p(fake)": { factory: _FakeP2PProtocol, uri_fn: () => "fakep2p://bus", kw_a: { link_id: "GT", side: "a" }, kw_b: { link_id: "GT", side: "b" }, reset: () => _P2P_LINKS.clear() },
};
const _pair = (name) => new _Pair(name, PAIRS[name]).build();

function _assert_proto_down(proto, label) {
  assert.equal(proto._started, false, `${label}: still started`);
  const conns = proto._connections ?? {};
  assert.equal(conns instanceof Map ? conns.size : Object.keys(conns).length, 0, `${label}: connections left`);
  assert.equal(proto._event_loop ?? null, null, `${label}: event loop not released`);
  assert.equal(proto._loop_thread ?? null, null, `${label}: loop thread not joined`);
}

describe("TestGracefulTeardownAllTransports", () => {
  beforeEach(() => macrotask(() => laila.terminate()));
  afterEach(() => macrotask(() => laila.terminate()));

  for (const name of Object.keys(PAIRS)) {
    test(`[${name}] stop is clean and leaks nothing`, () =>
      macrotask(() => {
        const pr = _pair(name);
        try {
          pr.peer();
          pr.stop_all();
          const baseline = _open_fds();
          pr.pa.start();
          pr.pb.start();
          pr.peer();
          const [aid, bid] = [pr.a.global_id, pr.b.global_id];
          assert.ok(bid in laila._remote_policies);
          pr.stop_all();
          time.sleep(0.2);
          for (const proto of [pr.pa, pr.pb]) _assert_proto_down(proto, name);
          assert.equal(pr.ca.peers.__len__(), 0);
          assert.equal(pr.cb.peers.__len__(), 0);
          assert.ok(!(aid in laila._remote_policies));
          assert.ok(!(bid in laila._remote_policies));
          assert.equal(pr.ca._liveness_thread, null);
          const leaked = [..._open_fds()].filter((fd) => !baseline.has(fd));
          assert.deepEqual(leaked, [], `${name}: fds leaked per lifecycle`);
        } finally {
          pr.stop_all();
        }
      }));

    test(`[${name}] remove_peer is noticed by remote`, () =>
      macrotask(() => {
        const pr = _pair(name);
        try {
          pr.peer();
          const [aid, bid] = [pr.a.global_id, pr.b.global_id];
          const t0 = time.monotonic();
          pr.ca.remove_peer(bid);
          assert.ok(!pr.ca.peers.__contains__(bid));
          assert.equal(pr.pa.has_peer(bid), false);
          const noticed = wait_until(() => !pr.cb.peers.__contains__(aid), 3.0);
          const dt = time.monotonic() - t0;
          assert.ok(noticed, `${name}: remote kept us after remove_peer`);
          assert.ok(dt < 3.0);
          assert.equal(pr.pb.has_peer(aid), false);
          assert.ok(pr.pa._started && pr.pb._started);
          pr.peer();
          assert.ok(pr.cb.peers.__contains__(aid));
        } finally {
          pr.stop_all();
        }
      }));

    test(`[${name}] one-sided stop is noticed by remote`, () =>
      macrotask(() => {
        const pr = _pair(name);
        try {
          pr.peer();
          const [aid, bid] = [pr.a.global_id, pr.b.global_id];
          pr.ca.stop();
          const noticed = wait_until(() => !pr.cb.peers.__contains__(aid), 3.0);
          time.sleep(0.1);
          assert.ok(noticed, `${name}: remote kept us after one-sided stop`);
          assert.equal(pr.pb.has_peer(aid), false);
          _assert_proto_down(pr.pa, name);
          assert.ok(pr.pb._started);
          assert.ok(!pr.ca.peers.__contains__(bid));
        } finally {
          pr.stop_all();
        }
      }));

    test(`[${name}] send after stop raises ConnectionError`, () =>
      macrotask(() => {
        const pr = _pair(name);
        try {
          pr.peer();
          const bid = pr.b.global_id;
          pr.ca.stop();
          assert.throws(() => pr.pa.send_rpc(bid, ["__comm_ping__"], [], {}), E.ConnectionError);
          assert.equal(pr.pa.ping(bid), false);
          assert.throws(() => pr.ca._send_rpc(bid, ["echo"], ["x"], {}), E.ConnectionError);
          pr.ca.stop();
          pr.pa.stop();
        } finally {
          pr.stop_all();
        }
      }));

    test(`[${name}] liveness keeps healthy peers`, () =>
      macrotask(() => {
        const pr = _pair(name);
        try {
          pr.peer();
          const [aid, bid] = [pr.a.global_id, pr.b.global_id];
          for (const pol of [pr.a, pr.b]) {
            pol.central.communication.liveness_interval = 0.2;
            pol.central.communication._stop_liveness();
            pol.central.communication._start_liveness();
          }
          time.sleep(1.2);
          assert.ok(pr.ca.peers.__contains__(bid), `${name}: A dropped healthy B`);
          assert.ok(pr.cb.peers.__contains__(aid), `${name}: B dropped healthy A`);
          assert.ok(pr.pa.ping(bid));
          assert.ok(pr.pb.ping(aid));
        } finally {
          pr.stop_all();
        }
      }));

    test(`[${name}] laila.terminate tears everything down`, () =>
      macrotask(() => {
        const pr = _pair(name);
        try {
          pr.peer();
          laila.activate_policy(pr.a);
          laila.activate_policy(pr.b);
          const errors = laila.terminate();
          time.sleep(0.1);
          assert.deepEqual(errors, [], `${name}: terminate reported errors`);
          for (const proto of [pr.pa, pr.pb]) _assert_proto_down(proto, name);
          assert.deepEqual(laila._remote_policies, {});
          assert.deepEqual(laila._local_policies, {});
        } finally {
          pr.stop_all();
        }
      }));
  }
});

describe("TestFailedStartLeaksNothing", () => {
  test("failed boot closes loop", () =>
    macrotask(() => {
      const proto = new defaults.DefaultUARTProtocol();
      const baseline = _open_fds();
      assert.throws(() => proto.start(), E.RuntimeError);
      _assert_proto_down(proto, "uart-no-port");
      assert.equal(proto._inbound_executor, null);
      assert.deepEqual([..._open_fds()].filter((fd) => !baseline.has(fd)), []);
      proto.stop();
    }));
});

describe("TestLoopThreadHelpers", () => {
  test("stop closes loop and cancels tasks", () =>
    macrotask(() => {
      const p = { _event_loop: null, _loop_thread: null };
      const started = new TH.Event();
      const hung = [];
      const _async_start = async (ready) => {
        const _forever = async () => {
          try {
            await asyncio.sleep(3600);
          } catch (e) {
            if (e instanceof E.CancelledError) hung.push("cancelled");
            throw e;
          }
        };
        // ``ensure_future(_forever())``: the coroutine body runs inside the Task.
        hung.push(asyncio.ensure_future(_forever));
        started.set();
        ready.set();
      };
      loopthread.start_loop_thread(p, _async_start, { ready_timeout: 5.0 });
      assert.ok(started.wait(2.0));
      const loop = p._event_loop;
      assert.ok(loop.is_running());
      loopthread.stop_loop_thread(p, loopthread.cancel_pending_tasks);
      assert.equal(p._event_loop, null);
      assert.equal(p._loop_thread, null);
      assert.ok(loop.is_closed());
      assert.ok(hung.includes("cancelled"));
      loopthread.stop_loop_thread(p, null);
    }));
  test("failed start raises and closes loop", () =>
    macrotask(() => {
      const p = { _event_loop: null, _loop_thread: null };
      const _boom = async (_ready) => {
        throw new E.RuntimeError("no device");
      };
      assert.throws(() => loopthread.start_loop_thread(p, _boom, { ready_timeout: 5.0 }), E.RuntimeError);
      assert.equal(p._event_loop, null);
      assert.equal(p._loop_thread, null);
    }));
});
