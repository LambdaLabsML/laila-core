/**
 * Central communication sub-package: ports of
 *   tests/functional/policy/communication/unit_tests/test_communication.py
 *   tests/functional/policy/communication/unit_tests/test_rpc.py
 *   tests/functional/policy/communication/unit_tests/test_carriers.py
 *   tests/functional/policy/communication/unit_tests/test_carriers_broker_register.py
 *   tests/functional/policy/communication/unit_tests/test_backpressure.py
 *   tests/functional/policy/communication/unit_tests/test_transport_conformance.py
 *   tests/functional/policy/communication/unit_tests/test_tier1_transports.py
 *   tests/functional/policy/communication/streaming/unit_tests/test_peer_registry.py
 *   tests/functional/policy/communication/streaming/unit_tests/test_channels_basic.py (5, 7, 12, 16, UDP)
 *   tests/functional/policy/communication/streaming/unit_tests/test_stream_entry.py (contract)
 *
 * plus a catalogue check (token resolution / class metadata) whose expected
 * values were produced by the Python package. Tests that need the ``laila``
 * root surface (``laila.peers`` / ``laila.relay`` / ``laila.request`` /
 * subprocess peers) live in the functional tree once p8/p9 land.
 */
import { test, describe, before, after, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";

const S = new URL("../../src/", import.meta.url).href;
// Provisional ``laila`` root (registers ``laila`` for ``lazy("laila")``).
const LAILA = (await import("./fixtures/laila_root.js")).default;
const defaults = await import(S + "macros/defaults.js");
const E = await import(S + "_compat/errors.js");
const time = await import(S + "_compat/time.js");
const asyncio = await import(S + "_compat/asyncio.js");
const { Field, PrivateAttr, define_fields, define_private, object_setattr } = await import(S + "_compat/pydantic.js");
const { repr } = await import(S + "_compat/pyrepr.js");
const { Struct } = await import(S + "_compat/struct.js");
const { Entry } = await import(S + "entry/index.js");
const { EntryState } = await import(S + "entry/entry_state.js");
const COMM = await import(S + "policy/central/communication/index.js");
const protocol = await import(S + "policy/central/communication/protocol.js");
const wire = await import(S + "policy/central/communication/wire.js");
const { RemotePolicyProxy, _RemoteAttrChain } = await import(S + "policy/central/communication/proxy.js");
const { PeerProxy, PeerRegistry } = await import(S + "policy/central/communication/registry.js");
const { Channel, Relay, StreamEntry } = await import(S + "policy/central/communication/channel.js");
const { _LAILA_IDENTIFIABLE_COMMUNICATION } = await import(S + "policy/central/communication/schema/base.js");
const PROTOCOLS = await import(S + "policy/central/communication/protocols/index.js");
const codec = await import(S + "policy/central/communication/protocols/_carriers/codec.js");
const { uri_authority } = await import(S + "policy/central/communication/protocols/_carriers/uri.js");
const { _HEADER, _TYPE_DATA, _DatagramRPCProtocol } = await import(S + "policy/central/communication/protocols/_carriers/datagram.js");
const { _BrokerRPCProtocol } = await import(S + "policy/central/communication/protocols/_carriers/broker.js");
const { _RegisterRPCProtocol } = await import(S + "policy/central/communication/protocols/_carriers/register.js");
const { BackpressureError, _CarrierRPCProtocol } = await import(S + "policy/central/communication/protocols/_carriers/base.js");
const { _LAILA_IDENTIFIABLE_COMM_PROTOCOL, register_comm_protocol, iter_comm_protocols, comm_protocol_for_token } = PROTOCOLS;

const { DefaultPolicy, DefaultTCPIPProtocol } = defaults;

/**
 * Run a body on a fresh macrotask: ``node:test`` invokes test bodies from a
 * microtask, where a blocking wait is impossible by construction.
 */
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

function _make_policy() {
  return _mk(new DefaultTCPIPProtocol());
}

function _peer(a, b) {
  b.central.communication.start();
  const tcp_b = _conn(b);
  return a.central.communication.add_tcpip_peer("127.0.0.1", tcp_b.bound_port, tcp_b.peer_secret_key);
}

function wait_until(pred, timeout = 5.0) {
  const deadline = time.monotonic() + timeout;
  while (time.monotonic() < deadline) {
    if (pred()) return true;
    time.sleep(0.01);
  }
  return pred();
}

const _stop = (...policies) => {
  for (const p of policies) p.central.communication.stop();
};

// ---------------------------------------------------------------------------
// test_communication.py :: TestProtocolHelpers
// ---------------------------------------------------------------------------
describe("TestProtocolHelpers", () => {
  test("make_request has jsonrpc version", () => {
    const req = protocol.make_request("rpc.call", { path: ["x"] });
    assert.equal(req.jsonrpc, "2.0");
    assert.equal(req.method, "rpc.call");
    assert.ok("id" in req);
  });
  test("make_request custom id", () => {
    const req = protocol.make_request("peer.connect", {}, "abc");
    assert.equal(req.id, "abc");
  });
  test("make_result", () => {
    const res = protocol.make_result("req-1", { ok: true });
    assert.equal(res.id, "req-1");
    assert.deepEqual(res.result, { ok: true });
  });
  test("make_error without data", () => {
    const err = protocol.make_error("req-1", -32600, "bad request");
    assert.equal(err.error.code, -32600);
    assert.equal(err.error.message, "bad request");
    assert.ok(!("data" in err.error));
  });
  test("make_error with data", () => {
    const err = protocol.make_error("req-1", -32002, "boom", { detail: 1 });
    assert.deepEqual(err.error.data, { detail: 1 });
  });
  test("encode/decode roundtrip", () => {
    const obj = { key: [1, 2, 3], nested: { a: true } };
    assert.deepEqual(protocol.decode(protocol.encode(obj)), obj);
  });
  test("is_request / is_response", () => {
    const req = protocol.make_request("rpc.call", {});
    assert.ok(protocol.is_request(req));
    assert.ok(!protocol.is_response(req));
    const res = protocol.make_result("r1", null);
    assert.ok(!protocol.is_request(res));
    assert.ok(protocol.is_response(res));
  });
  test("LailaJSONEncoder handles model_dump", () => {
    const entry = LAILA.constant(42);
    const decoded = protocol.decode(protocol.encode({ entry }));
    assert.ok("entry" in decoded);
  });
});

// ---------------------------------------------------------------------------
// test_carriers.py :: TestCodec
// ---------------------------------------------------------------------------
describe("TestCodec", () => {
  test("json roundtrip", () => {
    const msg = { jsonrpc: "2.0", method: "rpc.call", params: { path: ["a", "b"] }, id: "1" };
    assert.deepEqual(codec.decode(codec.encode(msg, "json"), "json"), msg);
  });
  test("msgpack roundtrip", () => {
    const msg = { jsonrpc: "2.0", result: [1, 2, { x: true }], id: "2" };
    assert.deepEqual(codec.decode(codec.encode(msg, "msgpack"), "msgpack"), msg);
  });
  test("unknown codec raises", () => {
    assert.throws(() => codec.encode({}, "yaml"), E.ValueError);
    assert.throws(() => codec.decode(Buffer.alloc(0), "yaml"), E.ValueError);
  });
  test("encodes laila entry under both codecs", () => {
    const entry = LAILA.constant({ k: [1, 2, 3] });
    const msg = protocol.make_result("rid-1", entry);
    for (const c of ["json", "msgpack"]) {
      const decoded = codec.decode(codec.encode(msg, c), c);
      assert.equal(decoded.id, "rid-1");
      assert.equal(typeof decoded.result, "object");
      assert.deepEqual(decoded.result, entry.model_dump({ mode: "json" }));
    }
  });
  test("frame prefix is length", () => {
    const payload = Buffer.from("hello world");
    const framed = codec.frame(payload);
    const [length] = new Struct(">I").unpack(framed.subarray(0, 4));
    assert.equal(length, payload.length);
    assert.deepEqual(framed.subarray(4), payload);
  });
  test("read_frame roundtrip", async () => {
    const reader = new asyncio.StreamReader();
    reader.feed_data(Buffer.concat([codec.frame(Buffer.from("abc")), codec.frame(Buffer.from("defg"))]));
    reader.feed_eof();
    assert.deepEqual(await codec.read_frame(reader), Buffer.from("abc"));
    assert.deepEqual(await codec.read_frame(reader), Buffer.from("defg"));
    assert.equal(await codec.read_frame(reader), null);
  });
});

// ---------------------------------------------------------------------------
// wire.py lane framing
// ---------------------------------------------------------------------------
describe("TestWire", () => {
  test("pack/unpack stream frame", () => {
    const raw = wire.pack_stream_frame(3, 7, wire.FLAG_START | wire.FLAG_END, Buffer.from("abc"));
    assert.equal(raw[0], wire.STREAM_MARKER);
    assert.equal(raw.length, wire.STREAM_HEADER_LEN + 3);
    const [lane, seq, flags] = wire.unpack_stream_header(raw);
    assert.deepEqual([lane, seq, flags], [3, 7, wire.FLAG_START | wire.FLAG_END]);
    assert.deepEqual(raw.subarray(wire.STREAM_HEADER_LEN), Buffer.from("abc"));
  });
  test("is_stream_frame / is_reserved_frame", () => {
    assert.ok(wire.is_stream_frame(wire.pack_stream_frame(1, 0, 0, Buffer.alloc(0))));
    assert.ok(!wire.is_stream_frame(Buffer.from('{"jsonrpc":"2.0"}')));
    assert.ok(wire.is_reserved_frame(Buffer.from([0x05])));
    assert.ok(!wire.is_reserved_frame(Buffer.from([0x7b])));
    assert.ok(!wire.is_reserved_frame(Buffer.alloc(0)));
  });
  test("seq wraps modulo 2**32", () => {
    const raw = wire.pack_stream_frame(1, wire.SEQ_MODULUS - 1, 0, Buffer.alloc(0));
    assert.equal(wire.unpack_stream_header(raw)[1], wire.SEQ_MODULUS - 1);
  });
});

// ---------------------------------------------------------------------------
// Catalogue: token resolution + metadata (expected values from Python)
// ---------------------------------------------------------------------------
describe("TestTransportCatalogue", () => {
  const PY_TOKENS = {
    tcpip: "TCPIP", ws: "TCPIP", wss: "TCPIP", tcp: "TCP", "tcp-ip": null, udp: "UDP", tls: "TLS", ethernet: "ETHERNET", eth: "ETHERNET",
    loopback: "LOOPBACK", local: "LOOPBACK", unix: "UNIXSOCKET", unixsocket: "UNIXSOCKET", grpc: "GRPC", h2: "HTTP2", quic: "HTTP3",
    modbus: "MODBUS_TCP", opcua: "OPCUA", mqtt: "MQTT", amqp: "AMQP", zmq: "ZEROMQ", xmpp: "XMPP", dds: "DDS", coap: "COAP", dtls: "DTLS",
    uart: "UART", serial: "UART", rs232: "RS232", rs485: "RS485", usb: "USB", can: "CAN", i2c: "I2C", spi: "SPI", enip: "ENIP",
    "modbus-rtu": "MODBUS_RTU", lin: "LIN", w1: "ONEWIRE", i2s: "I2S", sdio: "SDIO", ethercat: "ETHERCAT", profinet: "PROFINET",
    lora: "LORA", lorawan: "LORAWAN", ltem: "LTEM", nbiot: "NBIOT", satellite: "SATELLITE", sigfox: "SIGFOX", cellular: "CELLULAR",
    gsm: "GSM", lte: "LTE", "5g": "NR5G", bluetooth: "BLUETOOTH", ble: "BLUETOOTH", matter: "MATTER", wifi: "WIFI", p2p: "WIFIDIRECT",
    "6lowpan": "SIXLOWPAN", rfid: "RFID", "ant+": "ANT", espnow: "ESPNOW", ir: "IRDA", nfc: "NFC", thread: "THREAD", uwb: "UWB",
    zigbee: "ZIGBEE", "z-wave": "ZWAVE", ip: null,
  };
  test("58 concrete transports registered (Python: 58)", () => {
    // The fakes registered further down this file (``fakedg``, ``fakebroker``,
    // ``fakereg``, ``rpconly``) live in the same catalog; exclude them.
    const TEST_FAKES = new Set(["fakedg", "fakebroker", "fakereg", "fake", "rpconly"]);
    const all = iter_comm_protocols().filter((c) => !TEST_FAKES.has(c.protocol_name));
    assert.equal(all.length, 58);
    assert.equal(new Set(all.map((c) => c.protocol_name)).size, 58);
  });
  test("comm_protocol_for_token matches Python", () => {
    for (const [tok, short] of Object.entries(PY_TOKENS)) {
      const cls = comm_protocol_for_token(tok);
      if (short === null) assert.equal(cls, null, tok);
      else assert.equal(cls?.name, `_LAILA_IDENTIFIABLE_${short}_COMM_PROTOCOL`, tok);
    }
  });
  test("every Default*Protocol alias resolves to a registered class", () => {
    const names = Object.keys(defaults).filter((n) => n.startsWith("Default") && n.endsWith("Protocol"));
    assert.ok(names.length >= 30);
    const registered = new Set(iter_comm_protocols());
    for (const n of names) assert.ok(registered.has(defaults[n]), n);
    assert.equal(defaults.DefaultWebSocketProtocol, defaults.DefaultTCPIPProtocol);
    assert.equal(defaults.DefaultCentralCommunication, _LAILA_IDENTIFIABLE_COMMUNICATION);
  });
  test("module surface", () => {
    for (const n of COMM.__all__) assert.ok(n in COMM, n);
    for (const n of PROTOCOLS.__all__) assert.ok(n in PROTOCOLS, n);
    assert.equal(COMM.protocols, PROTOCOLS);
  });
});

// ---------------------------------------------------------------------------
// test_transport_conformance.py
// ---------------------------------------------------------------------------
describe("TestTransportConformance", () => {
  const _MUST_WORK = new Set(["tcp", "tcpip", "udp", "tls", "ethernet", "wifi", "wifi-direct", "cellular", "gsm", "lte", "nr5g", "nbiot", "ltem", "matter", "unix", "loopback", "6lowpan"]);
  const _CLEAN_ERRORS = [E.RuntimeError, E.NotImplementedError, E.ConnectionError, E.TimeoutError];
  const classes = (() => {
    const seen = new Map();
    for (const name of Object.keys(defaults)) {
      if (name.startsWith("Default") && name.endsWith("Protocol")) seen.set(defaults[name].name, defaults[name]);
    }
    return [...seen.values()];
  })();

  test("catalog discoverable", () => assert.ok(classes.length >= 30));

  test("every transport metadata", () => {
    for (const cls of classes) {
      const name = cls.protocol_name;
      assert.ok(name && name !== "base" && name !== "carrier", name);
      assert.ok(cls.matches_token(name), `${name} !match self`);
      assert.equal(typeof cls.can_handle_uri("nope://x"), "boolean");
      assert.equal(cls.can_handle_uri("nope://x"), false);
    }
  });

  test("every transport has uniform connection surface", () => {
    for (const cls of classes) {
      for (const m of ["connect", "disconnect", "ping", "send_rpc", "has_peer", "start", "stop"]) {
        assert.equal(typeof cls.prototype[m], "function", `${cls.name}.${m}`);
      }
      assert.equal(typeof cls.persistent, "boolean");
      assert.equal(typeof cls.supports_ping, "boolean");
      const proto = new cls();
      assert.equal(proto.ping("nobody"), false);
      proto.disconnect("nobody");
    }
  });

  test("every transport instantiates with identity", () => {
    for (const cls of classes) {
      const a = new cls();
      const b = new cls();
      assert.ok(a.global_id);
      assert.notEqual(a.global_id, b.global_id);
      assert.equal(a.has_peer("anyone"), false);
    }
  });

  test("every transport start is clean", () =>
    macrotask(() => {
      for (const cls of classes) {
        const proto = new cls();
        const name = proto.protocol_name;
        if (_MUST_WORK.has(name)) {
          try {
            proto.start();
            proto.start();
            assert.equal(proto.has_peer("x"), false);
          } finally {
            proto.stop();
            proto.stop();
          }
        } else {
          let raised = null;
          try {
            proto.start();
          } catch (e) {
            raised = e;
          }
          assert.ok(raised !== null, `${name} start() did not raise`);
          assert.ok(
            _CLEAN_ERRORS.some((k) => raised instanceof k),
            `${name}: ${raised?.constructor?.name}: ${raised?.message}`,
          );
          proto.stop();
        }
      }
    }));

  test("unique class names for environment round trip", () => {
    const names = classes.map((c) => c.name);
    assert.equal(names.length, new Set(names).size);
  });
});

// ---------------------------------------------------------------------------
// test_communication.py :: lifecycle / connections / peering / auth / proxy / rpc
// ---------------------------------------------------------------------------
describe("TestCommunicationLifecycle", () => {
  test("start binds a port", () =>
    macrotask(() => {
      const p = _make_policy();
      const tcp = _conn(p);
      p.central.communication.start();
      try {
        assert.ok(tcp._started);
        assert.ok(tcp.bound_port > 0);
      } finally {
        _stop(p);
      }
    }));
  test("start is idempotent", () =>
    macrotask(() => {
      const p = _make_policy();
      const tcp = _conn(p);
      p.central.communication.start();
      const first = tcp.bound_port;
      p.central.communication.start();
      try {
        assert.equal(tcp.bound_port, first);
      } finally {
        _stop(p);
      }
    }));
  test("stop clears state", () =>
    macrotask(() => {
      const p = _make_policy();
      const tcp = _conn(p);
      p.central.communication.start();
      p.central.communication.stop();
      assert.equal(tcp._started, false);
      assert.equal(p.central.communication.peers.size, 0);
    }));
  test("stop without start is noop", () => macrotask(() => _stop(_make_policy())));
});

describe("TestConnectionManagement", () => {
  test("add_connection stores protocol", () =>
    macrotask(() => {
      const comm = new _LAILA_IDENTIFIABLE_COMMUNICATION({ policy_id: "test" });
      const tcp = new DefaultTCPIPProtocol();
      comm.add_connection(tcp);
      try {
        assert.ok(tcp.global_id in comm.connections);
        assert.equal(tcp._communication, comm);
      } finally {
        comm.stop();
      }
    }));
  test("add_peer with no protocols raises", () =>
    macrotask(() => {
      const comm = new _LAILA_IDENTIFIABLE_COMMUNICATION({ policy_id: "test" });
      assert.throws(() => comm.add_peer("ws://127.0.0.1:9999", "secret"), E.ConnectionError);
    }));
  test("can_handle_uri for tcpip", () => {
    assert.ok(DefaultTCPIPProtocol.can_handle_uri("ws://host:1234"));
    assert.ok(DefaultTCPIPProtocol.can_handle_uri("wss://host:1234"));
    assert.ok(!DefaultTCPIPProtocol.can_handle_uri("shm://region"));
  });
});

describe("TestPeering", () => {
  let A, B;
  beforeEach(() =>
    macrotask(() => {
      A = _make_policy();
      B = _make_policy();
    }));
  afterEach(() => macrotask(() => _stop(A, B)));

  test("bidirectional peering", () =>
    macrotask(() => {
      _peer(A, B);
      assert.ok(wait_until(() => A.global_id in B.central.communication.peers));
      assert.ok(B.global_id in A.central.communication.peers);
      assert.ok(A.global_id in B.central.communication.peers);
    }));
  test("peer returns remote global id", () => macrotask(() => assert.equal(_peer(A, B), B.global_id)));
  test("peers are remote policy proxies", () =>
    macrotask(() => {
      _peer(A, B);
      const proxy = A.central.communication.peers[B.global_id];
      assert.ok(proxy instanceof RemotePolicyProxy);
      assert.equal(proxy.global_id, B.global_id);
    }));
});

describe("TestSecretKeyAuth", () => {
  let A, B;
  beforeEach(() =>
    macrotask(() => {
      A = _make_policy();
      B = _make_policy();
      B.central.communication.start();
    }));
  afterEach(() => macrotask(() => _stop(A, B)));

  test("wrong secret raises ConnectionError", () =>
    macrotask(() => {
      const tcp_b = _conn(B);
      assert.throws(() => A.central.communication.add_tcpip_peer("127.0.0.1", tcp_b.bound_port, "definitely-wrong-secret"), E.ConnectionError);
    }));
  test("correct secret succeeds", () =>
    macrotask(() => {
      const tcp_b = _conn(B);
      assert.equal(A.central.communication.add_tcpip_peer("127.0.0.1", tcp_b.bound_port, tcp_b.peer_secret_key), B.global_id);
    }));
});

describe("TestRemoteProxy", () => {
  const comm = () => new _LAILA_IDENTIFIABLE_COMMUNICATION({ policy_id: "sender" });
  test("proxy global_id", () => assert.equal(new RemotePolicyProxy("LAILA:POLICY:abcd-1234", comm()).global_id, "LAILA:POLICY:abcd-1234"));
  test("proxy repr", () => assert.ok(repr(new RemotePolicyProxy("P1", comm())).includes("P1")));
  test("getattr returns remote attr chain", () => assert.ok(new RemotePolicyProxy("P1", comm()).central instanceof _RemoteAttrChain));
  test("attr chain extends on dotted access", () => {
    const r = repr(new RemotePolicyProxy("P1", comm()).central.memory.memorize);
    for (const seg of ["central", "memory", "memorize"]) assert.ok(r.includes(seg), r);
  });
});

describe("TestRemoteRPC", () => {
  let A, B;
  beforeEach(() =>
    macrotask(() => {
      A = _make_policy();
      B = _make_policy();
      _peer(A, B);
      wait_until(() => A.global_id in B.central.communication.peers);
    }));
  afterEach(() => macrotask(() => _stop(A, B)));

  test("rpc call extend on remote", () =>
    macrotask(() => {
      const pool = new defaults.DefaultPool();
      A.central.memory.extend(pool, { affinity: 0.5, pool_nickname: "rpc-pool" });
      const remote_a = B.central.communication.peers[A.global_id];
      const routed = remote_a.central.memory.pool_router.route({ entries: ["x"], pool_nickname: "rpc-pool" });
      assert.notEqual(routed, null);
    }));
  test("rpc round trip returns result", () =>
    macrotask(() => {
      _attach(A, "echo", (msg) => `echo: ${msg}`);
      assert.equal(B.central.communication.peers[A.global_id].echo("hello"), "echo: hello");
    }));
  test("rpc error propagates as RuntimeError", () =>
    macrotask(() => {
      _attach(A, "boom", () => {
        throw new E.ValueError("test-explosion");
      });
      assert.throws(() => B.central.communication.peers[A.global_id].boom(), (e) => e instanceof E.RuntimeError && e.message.includes("test-explosion"));
    }));
  test("rpc bidirectional", () =>
    macrotask(() => {
      _attach(A, "ping", () => "pong-from-a");
      _attach(B, "ping", () => "pong-from-b");
      assert.equal(A.central.communication.peers[B.global_id].ping(), "pong-from-b");
      assert.equal(B.central.communication.peers[A.global_id].ping(), "pong-from-a");
    }));
  test("send_rpc to disconnected peer raises", () =>
    macrotask(() => {
      assert.throws(() => A.central.communication._send_rpc("nonexistent-peer-id", ["foo"], [], {}), E.ConnectionError);
    }));
});

describe("TestMultiplePeers", () => {
  test("one policy can peer with multiple", () =>
    macrotask(() => {
      const A = _make_policy(), B = _make_policy(), C = _make_policy();
      try {
        _peer(A, B);
        _peer(A, C);
        const peers_a = A.central.communication.peers;
        assert.equal(peers_a.size, 2);
        assert.ok(B.global_id in peers_a);
        assert.ok(C.global_id in peers_a);
      } finally {
        _stop(A, B, C);
      }
    }));
});

describe("TestExecuteRPC", () => {
  test("execute_rpc without local policy raises", () => {
    const comm = new _LAILA_IDENTIFIABLE_COMMUNICATION({ policy_id: "orphan" });
    assert.throws(() => comm._execute_rpc(["central", "memory"], [], {}), E.RuntimeError);
  });
  test("execute_rpc bad path raises AttributeError", () =>
    macrotask(() => {
      const policy = _make_policy();
      try {
        assert.throws(() => policy.central.communication._execute_rpc(["nonexistent", "path"], [], {}), E.AttributeError);
      } finally {
        _stop(policy);
      }
    }));
});

// ---------------------------------------------------------------------------
// test_rpc.py
// ---------------------------------------------------------------------------
describe("test_rpc.py", () => {
  let A, B, proxy_a, proxy_b;
  before(() =>
    macrotask(() => {
      A = _make_policy();
      B = _make_policy();
      _peer(A, B);
      wait_until(() => A.global_id in B.central.communication.peers);
      proxy_b = A.central.communication.peers[B.global_id];
      proxy_a = B.central.communication.peers[A.global_id];
    }));
  after(() => macrotask(() => _stop(A, B)));

  const rpc = (name, fn, body) =>
    test(name, () =>
      macrotask(() => {
        if (fn) _attach(B, name.replace(/\W/g, "_"), fn);
        body(name.replace(/\W/g, "_"));
      }));

  describe("TestReturnTypes", () => {
    rpc("return_none", () => null, (n) => assert.equal(proxy_b[n](), null));
    rpc("return_int", () => 42, (n) => assert.equal(proxy_b[n](), 42));
    rpc("return_negative_int", () => -99, (n) => assert.equal(proxy_b[n](), -99));
    rpc("return_zero", () => 0, (n) => assert.equal(proxy_b[n](), 0));
    rpc("return_large_int", () => 10 ** 15, (n) => assert.equal(proxy_b[n](), 10 ** 15));
    rpc("return_float", () => 3.14159, (n) => assert.ok(Math.abs(proxy_b[n]() - 3.14159) < 1e-7));
    rpc("return_bool_true", () => true, (n) => assert.equal(proxy_b[n](), true));
    rpc("return_bool_false", () => false, (n) => assert.equal(proxy_b[n](), false));
    rpc("return_string", () => "hello world", (n) => assert.equal(proxy_b[n](), "hello world"));
    rpc("return_empty_string", () => "", (n) => assert.equal(proxy_b[n](), ""));
    rpc("return_unicode_string", () => "日本語テスト 🚀", (n) => assert.equal(proxy_b[n](), "日本語テスト 🚀"));
    rpc("return_list", () => [1, 2, 3], (n) => assert.deepEqual(proxy_b[n](), [1, 2, 3]));
    rpc("return_empty_list", () => [], (n) => assert.deepEqual(proxy_b[n](), []));
    rpc("return_nested_list", () => [[1, 2], [3, [4, 5]]], (n) => assert.deepEqual(proxy_b[n](), [[1, 2], [3, [4, 5]]]));
    rpc("return_dict", () => ({ a: 1, b: "two" }), (n) => assert.deepEqual(proxy_b[n](), { a: 1, b: "two" }));
    rpc("return_empty_dict", () => ({}), (n) => assert.deepEqual(proxy_b[n](), {}));
    const deep = { level1: { level2: { level3: "deep" } } };
    rpc("return_nested_dict", () => deep, (n) => assert.deepEqual(proxy_b[n](), deep));
    const mixed = { nums: [1, 2], nested: { ok: true }, list_of_dicts: [{ x: 1 }] };
    rpc("return_mixed_collection", () => mixed, (n) => assert.deepEqual(proxy_b[n](), mixed));
  });

  describe("TestArgPassing", () => {
    rpc("single_positional_arg", (x) => x + 1, (n) => assert.equal(proxy_b[n](10), 11));
    rpc("multiple_positional_args", (a, b) => a + b, (n) => assert.equal(proxy_b[n](3, 7), 10));
    rpc("string_arg", (s) => s.toUpperCase(), (n) => assert.equal(proxy_b[n]("hello"), "HELLO"));
    rpc("list_arg", (lst) => lst.length, (n) => assert.equal(proxy_b[n]([1, 2, 3, 4]), 4));
    rpc("dict_arg", (d) => Object.keys(d).sort(), (n) => assert.deepEqual(proxy_b[n]({ b: 2, a: 1 }), ["a", "b"]));
    rpc("kwarg_only", (kw = {}) => kw.name ?? "anon", (n) => assert.equal(proxy_b[n]({ name: "Alice" }), "Alice"));
    rpc("mixed_args_and_kwargs", (prefix, msg, kw = {}) => `${prefix}: ${msg}${kw.suffix ?? "!"}`, (n) => assert.equal(proxy_b[n]("INFO", "started", { suffix: "." }), "INFO: started."));
    rpc("no_args", () => 99, (n) => assert.equal(proxy_b[n](), 99));
    rpc("none_arg", (x) => x === null, (n) => assert.equal(proxy_b[n](null), true));
    rpc("bool_args", (a, b) => a && b, (n) => {
      assert.equal(proxy_b[n](true, true), true);
      assert.equal(proxy_b[n](true, false), false);
    });
    rpc("float_precision", (x) => x / 2.0, (n) => assert.ok(Math.abs(proxy_b[n](0.1) - 0.05) < 1e-9));
    const big = "x".repeat(100_000);
    rpc("large_string_arg", (s) => s, (n) => assert.equal(proxy_b[n](big), big));
    rpc("nested_structure_arg", (d) => d.users.length, (n) => assert.equal(proxy_b[n]({ users: [{ name: "a" }, { name: "b" }] }), 2));
    rpc("many_positional_args", (...args) => args.reduce((s, x) => s + x, 0), (n) => assert.equal(proxy_b[n](1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 55));
    rpc("many_kwargs", (kw = {}) => Object.keys(kw).length, (n) => assert.equal(proxy_b[n]({ a: 1, b: 2, c: 3, d: 4, e: 5 }), 5));
  });

  describe("TestErrorPropagation", () => {
    const raises = (n, text) => assert.throws(() => proxy_b[n](), (e) => e instanceof E.RuntimeError && e.message.includes(text));
    rpc("value_error", () => { throw new E.ValueError("bad value"); }, (n) => raises(n, "bad value"));
    rpc("type_error", () => { throw new E.TypeError("wrong type"); }, (n) => raises(n, "TypeError"));
    rpc("key_error", () => { throw new E.KeyError("missing"); }, (n) => raises(n, "KeyError"));
    rpc("runtime_error", () => { throw new E.RuntimeError("custom runtime"); }, (n) => raises(n, "custom runtime"));
    test("attribute_error_for_missing_path", () => macrotask(() => assert.throws(() => proxy_b.nonexistent_method(), (e) => e instanceof E.RuntimeError && e.message.includes("AttributeError"))));
    test("deeply_nested_missing_path", () => macrotask(() => assert.throws(() => proxy_b.central.nonexistent.deep.path(), (e) => e instanceof E.RuntimeError && e.message.includes("AttributeError"))));
    rpc("zero_division", () => { throw new E.ZeroDivisionError("division by zero"); }, (n) => raises(n, "ZeroDivisionError"));
    rpc("index_error", () => { throw new E.IndexError("list index out of range"); }, (n) => raises(n, "IndexError"));
    rpc("assertion_error", () => { throw new E.AssertionError("assertion failed"); }, (n) => raises(n, "assertion failed"));
  });

  describe("TestBidirectional", () => {
    test("a calls b and b calls a", () =>
      macrotask(() => {
        _attach(A, "name_", () => "A");
        _attach(B, "name_", () => "B");
        assert.equal(proxy_b.name_(), "B");
        assert.equal(proxy_a.name_(), "A");
      }));
    test("a calls b which reads a state", () =>
      macrotask(() => {
        _attach(B, "my_id", () => B.global_id);
        assert.equal(proxy_b.my_id(), B.global_id);
      }));
    test("interleaved calls", () =>
      macrotask(() => {
        _attach(A, "echo_a", (x) => `a:${x}`);
        _attach(B, "echo_b", (x) => `b:${x}`);
        for (let i = 0; i < 10; i++) {
          assert.equal(proxy_b.echo_b(i), `b:${i}`);
          assert.equal(proxy_a.echo_a(i), `a:${i}`);
        }
      }));
  });

  describe("TestDeepChains", () => {
    test("two level chain (pool_router.route)", () =>
      macrotask(() => {
        const result = proxy_b.central.memory.pool_router.route({ entries: ["anything"] });
        assert.notEqual(result, null);
      }));
    test("rpc chain is lazy", () => assert.ok(proxy_b.central.memory.pool_router.route instanceof _RemoteAttrChain));
    test("chain repr includes path", () => {
      const r = repr(proxy_b.central.memory.pool_router.route);
      for (const seg of ["central", "memory", "pool_router", "route"]) assert.ok(r.includes(seg), r);
    });
  });

  describe("TestStatefulRPC", () => {
    test("mutate and read counter", () =>
      macrotask(() => {
        const counter = { val: 0 };
        _attach(B, "increment", () => { counter.val += 1; return counter.val; });
        _attach(B, "get_counter", () => counter.val);
        proxy_b.increment();
        proxy_b.increment();
        proxy_b.increment();
        assert.equal(proxy_b.get_counter(), 3);
      }));
    test("append to remote list", () =>
      macrotask(() => {
        const items = [];
        _attach(B, "push", (x) => items.push(x));
        _attach(B, "get_items", () => [...items]);
        proxy_b.push("alpha");
        proxy_b.push("beta");
        proxy_b.push("gamma");
        assert.deepEqual(proxy_b.get_items(), ["alpha", "beta", "gamma"]);
      }));
    test("set and get dict entry", () =>
      macrotask(() => {
        const store = {};
        _attach(B, "set_val", (k, v) => { store[k] = v; return null; });
        _attach(B, "get_val", (k) => (k in store ? store[k] : null));
        proxy_b.set_val("name", "laila");
        proxy_b.set_val("version", 2);
        assert.equal(proxy_b.get_val("name"), "laila");
        assert.equal(proxy_b.get_val("version"), 2);
        assert.equal(proxy_b.get_val("missing"), null);
      }));
  });

  describe("TestConcurrentRPC", () => {
    test("rapid sequential calls", () =>
      macrotask(() => {
        _attach(B, "identity", (x) => x);
        for (let i = 0; i < 50; i++) assert.equal(proxy_b.identity(i), i);
      }));
    test("parallel calls (async twins)", async () => {
      _attach(B, "square", (x) => x * x);
      const comm = A.central.communication;
      const results = await Promise.all(Array.from({ length: 20 }, (_, i) => comm._send_rpc_async(B.global_id, ["square"], [i], {})));
      assert.deepEqual(results, Array.from({ length: 20 }, (_, i) => i * i));
    });
    test("concurrent bidirectional (async twins)", async () => {
      _attach(A, "from_a", (x) => `a:${x}`);
      _attach(B, "from_b", (x) => `b:${x}`);
      const ca = A.central.communication, cb = B.central.communication;
      const [rb, ra] = await Promise.all([
        Promise.all(Array.from({ length: 10 }, (_, i) => ca._send_rpc_async(B.global_id, ["from_b"], [i], {}))),
        Promise.all(Array.from({ length: 10 }, (_, i) => cb._send_rpc_async(A.global_id, ["from_a"], [i], {}))),
      ]);
      for (let i = 0; i < 10; i++) {
        assert.equal(rb[i], `b:${i}`);
        assert.equal(ra[i], `a:${i}`);
      }
    });
  });

  describe("TestSerializationEdgeCases", () => {
    const echo = (v) => macrotask(() => {
      _attach(B, "echo", (x) => x);
      assert.deepEqual(proxy_b.echo(v), v);
    });
    test("special json chars in string", () => echo('quote" backslash\\ newline\n tab\t slash/ unicode\u00e9'));
    test("null bytes in string", () => echo("a\x00b"));
    test("very nested structure", () => echo({ a: { b: { c: { d: { e: { f: { g: { h: [1, { i: "deep" }] } } } } } } } }));
    test("list of mixed types", () => echo([1, "two", 3.0, true, null, [4], { five: 5 }]));
    test("large payload", () =>
      macrotask(() => {
        // Python names this remote ``length``; in JS ``length`` is reserved
        // on proxies (array-likeness probes), so the remote is ``count``.
        _attach(B, "count", (x) => x.length);
        assert.equal(proxy_b.count(Array.from({ length: 5000 }, (_, i) => i)), 5000);
      }));
    test("return large payload", () =>
      macrotask(() => {
        const big_list = Array.from({ length: 5000 }, (_, i) => i);
        _attach(B, "big", () => big_list);
        assert.deepEqual(proxy_b.big(), big_list);
      }));
    test("dict with numeric string keys", () =>
      macrotask(() => {
        // Rule D1: dicts with index-like keys decode to a Map so Python's
        // insertion order survives the JS integer-key reordering.
        _attach(B, "echo", (x) => x);
        const res = proxy_b.echo({ 1: "one", 2: "two", 10: "ten" });
        assert.ok(res instanceof Map);
        assert.deepEqual([...res.entries()], [["1", "one"], ["2", "two"], ["10", "ten"]]);
      }));
    test("unicode keys and values", () => echo({ ключ: "значение", 键: "值", "🔑": "🎯" }));
    test("empty nested structures", () => echo({ a: [], b: {}, c: [[], {}], d: { e: [], f: {} } }));
  });

  describe("TestRealPolicyTreeRPC", () => {
    test("read global_id via proxy", () => assert.equal(proxy_b.global_id, B.global_id));
    test("read remote command alpha taskforce", () =>
      macrotask(() => {
        _attach(B, "get_alpha_tf", () => B.central.command.alpha_taskforce);
        const tf_id = proxy_b.get_alpha_tf();
        assert.notEqual(tf_id, null);
        assert.equal(typeof tf_id, "string");
      }));
    test("count remote taskforces", () =>
      macrotask(() => {
        _attach(B, "tf_count", () => Object.keys(B.central.command.taskforces).length);
        assert.ok(proxy_b.tf_count() >= 1);
      }));
  });

  describe("TestComputationRPC", () => {
    test("fibonacci", () =>
      macrotask(() => {
        const fib = (n) => (n < 2 ? n : fib(n - 1) + fib(n - 2));
        _attach(B, "fib", fib);
        assert.equal(proxy_b.fib(10), 55);
        assert.equal(proxy_b.fib(0), 0);
        assert.equal(proxy_b.fib(1), 1);
      }));
    test("sorting", () =>
      macrotask(() => {
        _attach(B, "sort", (lst) => [...lst].sort((a, b) => a - b));
        assert.deepEqual(proxy_b.sort([3, 1, 4, 1, 5, 9, 2, 6]), [1, 1, 2, 3, 4, 5, 6, 9]);
      }));
  });
});

describe("TestThreePolicyMesh", () => {
  let A, B, C;
  before(() =>
    macrotask(() => {
      A = _make_policy();
      B = _make_policy();
      C = _make_policy();
      _peer(A, B);
      _peer(A, C);
      _peer(B, C);
      wait_until(() => A.global_id in B.central.communication.peers && A.global_id in C.central.communication.peers && B.global_id in C.central.communication.peers);
      _attach(A, "who", () => "A");
      _attach(B, "who", () => "B");
      _attach(C, "who", () => "C");
    }));
  after(() => macrotask(() => _stop(A, B, C)));

  test("a calls b and c", () =>
    macrotask(() => {
      assert.equal(A.central.communication.peers[B.global_id].who(), "B");
      assert.equal(A.central.communication.peers[C.global_id].who(), "C");
    }));
  test("b calls a and c", () =>
    macrotask(() => {
      assert.equal(B.central.communication.peers[A.global_id].who(), "A");
      assert.equal(B.central.communication.peers[C.global_id].who(), "C");
    }));
  test("c calls a and b", () =>
    macrotask(() => {
      assert.equal(C.central.communication.peers[A.global_id].who(), "A");
      assert.equal(C.central.communication.peers[B.global_id].who(), "B");
    }));
  test("aggregated results from b and c", () =>
    macrotask(() => {
      _attach(B, "value", () => 10);
      _attach(C, "value", () => 20);
      assert.equal(A.central.communication.peers[B.global_id].value() + A.central.communication.peers[C.global_id].value(), 30);
    }));
});

describe("TestConnectionErrors", () => {
  test("send_rpc to unknown peer", () =>
    macrotask(() => {
      const A = _make_policy();
      A.central.communication.start();
      try {
        assert.throws(() => A.central.communication._send_rpc("LAILA:POLICY:does-not-exist", ["x"], [], {}), E.ConnectionError);
      } finally {
        _stop(A);
      }
    }));
  test("add_peer bad uri raises", () =>
    macrotask(() => {
      const A = _make_policy();
      A.central.communication.start();
      try {
        assert.throws(() => A.central.communication.add_peer("ws://256.256.256.256:1", "s"));
      } finally {
        _stop(A);
      }
    }));
  test("proxy after disconnect", () =>
    macrotask(() => {
      const A = _make_policy(), B = _make_policy();
      try {
        _peer(A, B);
        _attach(B, "ping", () => "pong");
        const proxy_b = A.central.communication.peers[B.global_id];
        assert.equal(proxy_b.ping(), "pong");
        B.central.communication.stop();
        assert.ok(wait_until(() => !(B.global_id in A.central.communication.peers) || !_conn(A).has_peer(B.global_id)));
        assert.throws(() => proxy_b.ping(), (e) => e instanceof E.ConnectionError || e instanceof E.RuntimeError);
      } finally {
        _stop(A, B);
      }
    }));
});

// ---------------------------------------------------------------------------
// test_tier1_transports.py
// ---------------------------------------------------------------------------
describe("TestTier1Transports", () => {
  const _IP_SCHEMES = new Set(["tcp", "tls", "udp", "ethernet", "wifi", "wifidirect", "cellular", "gsm", "lte", "nr5g", "nbiot", "ltem", "matter"]);
  const _uri_for = (name, policy_b, proto_b) => {
    if (_IP_SCHEMES.has(name)) return `${name}://127.0.0.1:${proto_b.bound_port}`;
    if (name === "unix") return `unix://${proto_b.bound_path}`;
    if (name === "loopback") return `loopback://${policy_b.global_id}`;
    throw new E.ValueError(name);
  };
  const _TRANSPORTS = {
    tcp: defaults.DefaultTCPProtocol,
    udp: defaults.DefaultUDPProtocol,
    unix: defaults.DefaultUnixSocketProtocol,
    loopback: defaults.DefaultLoopbackProtocol,
    ethernet: defaults.DefaultEthernetProtocol,
    wifi: defaults.DefaultWiFiProtocol,
    wifidirect: defaults.DefaultWiFiDirectProtocol,
    cellular: defaults.DefaultCellularProtocol,
    gsm: defaults.DefaultGSMProtocol,
    lte: defaults.DefaultLTEProtocol,
    nr5g: defaults.DefaultNR5GProtocol,
    nbiot: defaults.DefaultNBIoTProtocol,
    ltem: defaults.DefaultLTEMProtocol,
    matter: defaults.DefaultMatterProtocol,
  };
  const _run_case = (name, cls, c = "json") => {
    const a = _mk(new cls({ codec: c }));
    const b = _mk(new cls({ codec: c }));
    try {
      const pb = _conn(b);
      const remote_id = a.central.communication.add_peer(_uri_for(name, b, pb), pb.peer_secret_key);
      assert.equal(remote_id, b.global_id, name);
      _attach(b, "echo", (m) => `echo:${m}`);
      assert.equal(a.central.communication.peers[b.global_id].echo("hi"), "echo:hi", name);
      assert.ok(wait_until(() => a.global_id in b.central.communication.peers), name);
      _attach(a, "ping", () => `pong:${name}`);
      assert.equal(b.central.communication.peers[a.global_id].ping(), `pong:${name}`, name);
    } finally {
      _stop(a, b);
    }
  };
  for (const c of ["json", "msgpack"]) {
    for (const [name, cls] of Object.entries(_TRANSPORTS)) {
      test(`${name} carries rpc (${c})`, () => macrotask(() => _run_case(name, cls, c)));
    }
  }
  test("wrong secret rejected", () =>
    macrotask(() => {
      for (const [name, cls] of Object.entries(_TRANSPORTS)) {
        const a = _mk(new cls({ handshake_timeout: 3.0 }));
        const b = _mk(new cls());
        try {
          const pb = _conn(b);
          assert.throws(() => a.central.communication.add_peer(_uri_for(name, b, pb), "wrong-secret"), E.ConnectionError, name);
        } finally {
          _stop(a, b);
        }
      }
    }));
  test("disconnected peer send raises", () =>
    macrotask(() => {
      const a = _mk(new defaults.DefaultTCPProtocol());
      try {
        assert.throws(() => a.central.communication._send_rpc("nobody", ["x"], [], {}), E.ConnectionError);
      } finally {
        _stop(a);
      }
    }));
});

describe("TestTLSTransport", { skip: (() => { try { execFileSync("openssl", ["version"], { stdio: "ignore" }); return false; } catch { return "openssl required to generate a test cert"; } })() }, () => {
  let dir, cert, key;
  before(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), "laila_tls_"));
    cert = path.join(dir, "cert.pem");
    key = path.join(dir, "key.pem");
    execFileSync("openssl", ["req", "-x509", "-newkey", "rsa:2048", "-keyout", key, "-out", cert, "-days", "1", "-nodes", "-subj", "/CN=localhost"], { stdio: "ignore" });
  });
  after(() => fs.rmSync(dir, { recursive: true, force: true }));
  test("tls round trip", () =>
    macrotask(() => {
      const a = _mk(new defaults.DefaultTLSProtocol());
      const b = _mk(new defaults.DefaultTLSProtocol({ certfile: cert, keyfile: key }));
      try {
        const pb = _conn(b);
        assert.equal(a.central.communication.add_peer(`tls://127.0.0.1:${pb.bound_port}`, pb.peer_secret_key), b.global_id);
        _attach(b, "echo", (m) => `tls:${m}`);
        assert.equal(a.central.communication.peers[b.global_id].echo("x"), "tls:x");
      } finally {
        _stop(a, b);
      }
    }));
});

// ---------------------------------------------------------------------------
// test_carriers.py :: datagram reliability over a lossy in-memory ether
// ---------------------------------------------------------------------------
const _ETHER = new Map();
class _LossyDatagramProtocol extends _DatagramRPCProtocol {
  static protocol_name = "fakedg";
  static {
    define_fields(this, {
      addr: ["str", Field({ default: "" })],
      drop_first_data: ["int", Field({ default: 0 })],
    });
    define_private(this, { _dropped: PrivateAttr({ default: 0 }) });
  }
  static matches_token(token) {
    return token.toLowerCase() === "fakedg";
  }
  static can_handle_uri(uri) {
    return uri.startsWith("fakedg://");
  }
  async _create_datagram_endpoint() {
    _ETHER.set(this.addr, this);
    const addr = this.addr;
    const transport = {
      close() {
        _ETHER.delete(addr);
      },
      get_extra_info() {
        return [addr, 0];
      },
    };
    return [transport, null];
  }
  async _resolve_peer_addr(uri) {
    return uri_authority(uri);
  }
  _sendto(addr, packet) {
    const [, ptype] = _HEADER.unpack(packet.subarray(0, _HEADER.size));
    if (ptype === _TYPE_DATA && this._dropped < this.drop_first_data) {
      this._dropped += 1;
      return;
    }
    const target = _ETHER.get(addr);
    if (target === undefined || target._event_loop === null) return;
    target._event_loop.call_soon_threadsafe(() => target._feed_packet(this.addr, packet));
  }
}
register_comm_protocol(_LossyDatagramProtocol);

describe("TestDatagramReliability", () => {
  beforeEach(() => _ETHER.clear());
  const _run_echo = (drop_first_data, payload) => {
    const a = _mk(new _LossyDatagramProtocol({ addr: "A", mtu: 64, ack_timeout: 0.05 }));
    const b = _mk(new _LossyDatagramProtocol({ addr: "B", mtu: 64, ack_timeout: 0.05, drop_first_data }));
    try {
      const pb = _conn(b);
      assert.equal(a.central.communication.add_peer("fakedg://B", pb.peer_secret_key), b.global_id);
      _attach(b, "echo", (m) => `len=${m.length}`);
      return a.central.communication.peers[b.global_id].echo(payload);
    } finally {
      _stop(a, b);
    }
  };
  test("fragmented echo no loss", () => macrotask(() => assert.equal(_run_echo(0, "z".repeat(4000)), "len=4000")));
  test("fragmented echo with loss recovers", () => macrotask(() => assert.equal(_run_echo(3, "q".repeat(2000)), "len=2000")));
});

// ---------------------------------------------------------------------------
// test_carriers_broker_register.py
// ---------------------------------------------------------------------------
const _BUS = new Map();
class _FakeBrokerProtocol extends _BrokerRPCProtocol {
  static protocol_name = "fakebroker";
  static matches_token(token) {
    return token.toLowerCase() === "fakebroker";
  }
  static can_handle_uri(uri) {
    return uri.startsWith("fakebroker://");
  }
  async _broker_connect() {}
  async _broker_subscribe(topic) {
    if (!_BUS.has(topic)) _BUS.set(topic, []);
    _BUS.get(topic).push(this);
  }
  async _broker_publish(topic, data) {
    for (const sub of [...(_BUS.get(topic) ?? [])]) {
      if (sub._event_loop !== null) sub._event_loop.call_soon_threadsafe(() => sub._feed_message(data));
    }
  }
  async _broker_close() {
    for (const subs of _BUS.values()) {
      const i = subs.indexOf(this);
      if (i >= 0) subs.splice(i, 1);
    }
  }
}
register_comm_protocol(_FakeBrokerProtocol);

describe("TestBrokerCarrier", () => {
  beforeEach(() => _BUS.clear());
  test("broker echo round trip", () =>
    macrotask(() => {
      const a = _mk(new _FakeBrokerProtocol());
      const b = _mk(new _FakeBrokerProtocol());
      try {
        const pb = _conn(b);
        assert.equal(a.central.communication.add_peer(`fakebroker://${b.global_id}`, pb.peer_secret_key), b.global_id);
        _attach(b, "echo", (m) => `b:${m}`);
        assert.equal(a.central.communication.peers[b.global_id].echo("hi"), "b:hi");
      } finally {
        _stop(a, b);
      }
    }));
  test("broker wrong secret rejected", () =>
    macrotask(() => {
      const a = _mk(new _FakeBrokerProtocol({ handshake_timeout: 2.0 }));
      const b = _mk(new _FakeBrokerProtocol());
      try {
        assert.throws(() => a.central.communication.add_peer(`fakebroker://${b.global_id}`, "nope"), E.ConnectionError);
      } finally {
        _stop(a, b);
      }
    }));
});

const _LINKS = new Map();
class _FakeRegisterProtocol extends _RegisterRPCProtocol {
  static protocol_name = "fakereg";
  static {
    define_fields(this, {
      link_id: ["str", Field({ default: "" })],
      side: ["str", Field({ default: "a" })],
    });
  }
  static matches_token(token) {
    return token.toLowerCase() === "fakereg";
  }
  static can_handle_uri(uri) {
    return uri.startsWith("fakereg://");
  }
  async _open_bus() {
    if (!_LINKS.has(this.link_id)) _LINKS.set(this.link_id, { a: [], b: [] });
  }
  async _deliver(data) {
    _LINKS.get(this.link_id)[this.side === "a" ? "b" : "a"].push(data);
  }
  async _poll_inbound() {
    const q = _LINKS.get(this.link_id)[this.side];
    return q.length ? q.shift() : null;
  }
}
register_comm_protocol(_FakeRegisterProtocol);

describe("TestRegisterCarrier", () => {
  beforeEach(() => _LINKS.clear());
  test("register echo round trip", () =>
    macrotask(() => {
      const a = _mk(new _FakeRegisterProtocol({ link_id: "L1", side: "a", poll_interval: 0.01 }));
      const b = _mk(new _FakeRegisterProtocol({ link_id: "L1", side: "b", poll_interval: 0.01 }));
      try {
        const pb = _conn(b);
        assert.equal(a.central.communication.add_peer("fakereg://bus", pb.peer_secret_key), b.global_id);
        _attach(b, "echo", (m) => `r:${m}`);
        assert.equal(a.central.communication.peers[b.global_id].echo("hi"), "r:hi");
      } finally {
        _stop(a, b);
      }
    }));
});

// ---------------------------------------------------------------------------
// test_backpressure.py
// ---------------------------------------------------------------------------
class _FakeCarrier extends _CarrierRPCProtocol {
  static protocol_name = "fake";
}
const _rpc = (p, args = null, kwargs = null) => protocol.make_request("rpc.call", { path: p, args: args ?? [], kwargs: kwargs ?? {} });

describe("TestHubAdmission", () => {
  test("cap admits then rejects then releases", () => {
    const comm = new _LAILA_IDENTIFIABLE_COMMUNICATION({ policy_id: "t" });
    comm.max_inflight_rpcs = 2;
    assert.equal(comm._acquire_rpc_slot(), true);
    assert.equal(comm._acquire_rpc_slot(), true);
    assert.equal(comm._acquire_rpc_slot(), false);
    comm._release_rpc_slot();
    assert.equal(comm._acquire_rpc_slot(), true);
  });
  test("no hub always admits", () => assert.equal(new _FakeCarrier()._try_admit(), true));
});

describe("TestHandleRequestFrame", () => {
  const _carrier = (cap) => {
    const comm = new _LAILA_IDENTIFIABLE_COMMUNICATION({ policy_id: "t" });
    comm.max_inflight_rpcs = cap;
    const c = new _FakeCarrier();
    c._communication = comm;
    return [c, comm];
  };
  test("ping bypasses admission", () => {
    const [c, comm] = _carrier(1);
    comm._acquire_rpc_slot();
    const got = {};
    c._handle_request_frame(_rpc(["__comm_ping__"]), (r) => Object.assign(got, r));
    assert.equal(got.result, "pong");
  });
  test("over capacity replies busy without queuing", () => {
    const [c, comm] = _carrier(1);
    comm._acquire_rpc_slot();
    const got = {};
    c._handle_request_frame(_rpc(["central", "memory", "x"]), (r) => Object.assign(got, r));
    assert.equal(got.error?.code, protocol.ERR_BUSY);
    assert.equal(c._inbound_executor, null);
  });
  test("admitted request runs and releases slot", () =>
    macrotask(() => {
      const [c, comm] = _carrier(2);
      comm._local_policy = { echo: (v) => v };
      const box = {};
      c._handle_request_frame(_rpc(["echo"], ["hi"]), (r) => Object.assign(box, r));
      assert.ok(wait_until(() => "result" in box || "error" in box));
      assert.equal(box.result, "hi");
      assert.equal(comm._acquire_rpc_slot(), true);
      assert.equal(comm._acquire_rpc_slot(), true);
      c._shutdown_executor();
    }));
});

describe("TestSenderBackoff", () => {
  test("retries then succeeds", () =>
    macrotask(() => {
      const c = new _FakeCarrier({ rpc_backoff_base: 0.001, rpc_backoff_max: 0.005, max_rpc_retries: 5 });
      let n = 0;
      c._send_once = () => {
        n += 1;
        if (n < 3) throw new BackpressureError("busy");
        return "result";
      };
      assert.equal(c.send_rpc("p", ["x"], [], {}), "result");
      assert.equal(n, 3);
    }));
  test("gives up after max retries", () =>
    macrotask(() => {
      const c = new _FakeCarrier({ rpc_backoff_base: 0.001, rpc_backoff_max: 0.005, max_rpc_retries: 2 });
      let n = 0;
      c._send_once = () => {
        n += 1;
        throw new BackpressureError("still busy");
      };
      assert.throws(() => c.send_rpc("p", ["x"], [], {}), BackpressureError);
      assert.equal(n, 3);
    }));
  test("non busy error is not retried", () =>
    macrotask(() => {
      const c = new _FakeCarrier({ rpc_backoff_base: 0.001, max_rpc_retries: 5 });
      let n = 0;
      c._send_once = () => {
        n += 1;
        throw new E.RuntimeError("hard failure");
      };
      assert.throws(() => c.send_rpc("p", ["x"], [], {}), E.RuntimeError);
      assert.equal(n, 1);
    }));
});

// ---------------------------------------------------------------------------
// streaming :: TwoPolicies helper (_helpers.py)
// ---------------------------------------------------------------------------
function TwoPolicies(kind, proto_kwargs = {}) {
  const cls = { loopback: defaults.DefaultLoopbackProtocol, tcp: defaults.DefaultTCPProtocol, udp: defaults.DefaultUDPProtocol }[kind];
  const a = _mk(new cls(proto_kwargs));
  const b = _mk(new cls(proto_kwargs));
  const pa = _conn(a), pb = _conn(b);
  const uri = kind === "loopback" ? `loopback://${b.global_id}` : `${kind}://127.0.0.1:${pb.bound_port}`;
  const bid = a.central.communication.add_peer(uri, pb.peer_secret_key);
  assert.equal(bid, b.global_id);
  assert.ok(wait_until(() => a.global_id in b.central.communication.peers));
  return { a, b, pa, pb, aid: a.global_id, bid, ca: a.central.communication, cb: b.central.communication, close: () => _stop(a, b) };
}

const _blob = (n, tag) => {
  const seed = crypto.createHash("sha256").update(`${n}:${tag}`).digest();
  if (!n) return Buffer.alloc(0);
  const reps = Math.ceil(n / seed.length);
  return Buffer.concat(Array(reps).fill(seed)).subarray(0, n);
};
const SIZES = [0, 1, 1024, 4096, 65536, 300_000];

describe("TestOpenAndTransfer (channels 5)", () => {
  const _roundtrip = (kind) => {
    const tp = TwoPolicies(kind);
    try {
      const ch_a = tp.ca.peers[tp.bid].channel("video");
      assert.ok(ch_a instanceof Channel);
      assert.ok(ch_a.opened);
      assert.equal(tp.ca.peers[tp.bid].channel("video"), ch_a);
      assert.ok(wait_until(() => tp.cb.peers[tp.aid].channels().includes("video")));
      const ch_b = tp.cb.peers[tp.aid].channel("video");
      assert.equal(ch_a.tx_lane_id, ch_b.lane_id);
      assert.equal(ch_b.tx_lane_id, ch_a.lane_id);
      const payloads = SIZES.map((n, i) => _blob(n, i));
      const t_send = time.monotonic();
      for (const p of payloads) ch_a.send(p);
      const relay = ch_b.relay(10);
      assert.ok(relay instanceof Relay);
      const got = payloads.map(() => relay.__next__());
      assert.deepEqual(got.map((g) => Buffer.from(g.data)), payloads);
      assert.deepEqual(got.map((g) => g.stream.seq), payloads.map((_, i) => i));
      for (const g of got) {
        assert.equal(g.stream.peer_id, tp.aid);
        assert.equal(g.stream.channel, "video");
        assert.equal(g.stream.lane, ch_b.lane_id);
        assert.ok(g.stream.arrived_at >= t_send);
        assert.equal(g.stream.dropped_before, 0);
      }
      assert.equal(ch_b.dropped, 0);
      assert.equal(ch_b.discarded, 0);
      assert.equal(ch_a.tx_seq, payloads.length);
      for (const p of [...payloads].reverse()) ch_b.send(p);
      const r_a = ch_a.relay(10);
      assert.deepEqual(payloads.map(() => Buffer.from(r_a.__next__().data)), [...payloads].reverse());
      assert.ok(tp.pa.ping(tp.bid));
    } finally {
      tp.close();
    }
  };
  test("loopback", () => macrotask(() => _roundtrip("loopback")));
  test("tcp", () => macrotask(() => _roundtrip("tcp")));
  test("udp single datagram", () =>
    macrotask(() => {
      const tp = TwoPolicies("udp");
      try {
        const ch_a = tp.ca.peers[tp.bid].channel("telemetry");
        const limit = tp.pa.mtu - 7;
        assert.equal(ch_a.max_message_bytes, limit);
        const payloads = [0, 1, 100, limit].map((n) => _blob(n, n));
        for (const p of payloads) ch_a.send(p);
        const ch_b = tp.cb.peers[tp.aid].channel("telemetry");
        const got = payloads.map(() => Buffer.from(ch_b.relay(10).__next__().data));
        assert.deepEqual(got, payloads);
        assert.throws(() => ch_a.send(Buffer.alloc(limit + 1, 0x78)), E.ValueError);
        assert.throws(() => ch_a.send(Buffer.alloc(5000, 0x78)), E.ValueError);
        assert.ok(tp.pa.ping(tp.bid));
      } finally {
        tp.close();
      }
    }));
});

describe("TestStreamEntryOverTransport (channels 7)", () => {
  for (const kind of ["loopback", "tcp"]) {
    test(`entries are constant entries (${kind})`, () =>
      macrotask(() => {
        const tp = TwoPolicies(kind);
        try {
          const ch_a = tp.ca.peers[tp.bid].channel("video");
          const ch_b = tp.cb.peers[tp.aid].channel("video");
          const payloads = [3, 300, 30_000].map((n) => _blob(n, 99));
          for (const p of payloads) ch_a.send(p);
          const es = payloads.map(() => ch_b.relay(10).__next__());
          es.forEach((e, i) => {
            assert.ok(e instanceof StreamEntry);
            assert.ok(e instanceof Entry);
            assert.deepEqual(Buffer.from(e.data), payloads[i]);
            assert.equal(e.state, EntryState.READY);
            assert.equal(e.evolution, null);
            assert.deepEqual(e.scopes, Entry.constant(Buffer.from("x")).scopes);
          });
          assert.equal(new Set(es.map((e) => e.global_id)).size, 3);
          const arr = es.map((e) => e.stream.arrived_at);
          assert.deepEqual(arr, [...arr].sort((x, y) => x - y));
          // a stream entry can be memorized / remembered like any constant
          tp.b.central.memory.memorize(es[1]);
          const back = tp.b.central.memory.remember(es[1].global_id);
          const one = Array.isArray(back) ? back[0] : back;
          assert.deepEqual(Buffer.from(one.data), payloads[1]);
        } finally {
          tp.close();
        }
      }));
  }
});

describe("TestChannelIsolationAndPeerLoss (channels 12, 16)", () => {
  test("several channels are isolated; closing one ends only that relay", () =>
    macrotask(() => {
      const tp = TwoPolicies("loopback");
      try {
        const a1 = tp.ca.peers[tp.bid].channel("one"), a2 = tp.ca.peers[tp.bid].channel("two");
        assert.notEqual(a1.lane_id, a2.lane_id);
        a1.send(Buffer.from("1"));
        a2.send(Buffer.from("2"));
        const b1 = tp.cb.peers[tp.aid].channel("one"), b2 = tp.cb.peers[tp.aid].channel("two");
        assert.equal(Buffer.from(b1.relay(5).__next__().data).toString(), "1");
        assert.equal(Buffer.from(b2.relay(5).__next__().data).toString(), "2");
        a1.close();
        assert.ok(a1.closed);
        assert.ok(wait_until(() => b1.closed));
        assert.throws(() => b1.relay(1).__next__(), E.StopIteration);
        assert.throws(() => a1.send(Buffer.from("x")), E.ConnectionError);
        a2.send(Buffer.from("still"));
        assert.equal(Buffer.from(b2.relay(5).__next__().data).toString(), "still");
        assert.deepEqual(tp.ca.peers[tp.bid].channels(), ["two"]);
      } finally {
        tp.close();
      }
    }));
  test("peer loss ends relay and send raises; reconnect gives a fresh channel", () =>
    macrotask(() => {
      const tp = TwoPolicies("tcp");
      try {
        const ch_a = tp.ca.peers[tp.bid].channel("video");
        const ch_b = tp.cb.peers[tp.aid].channel("video");
        ch_a.send(Buffer.from("before"));
        assert.equal(Buffer.from(ch_b.relay(5).__next__().data).toString(), "before");
        tp.ca.remove_peer(tp.bid);
        assert.ok(ch_a.closed);
        assert.throws(() => ch_a.send(Buffer.from("x")), E.ConnectionError);
        assert.ok(wait_until(() => ch_b.closed));
        assert.throws(() => ch_b.relay(1).__next__(), E.StopIteration);
        // reconnect -> re-index gives a fresh channel
        tp.ca.add_peer(`tcp://127.0.0.1:${tp.pb.bound_port}`, tp.pb.peer_secret_key);
        const fresh = tp.ca.peers[tp.bid].channel("video");
        assert.notEqual(fresh, ch_a);
        assert.ok(!fresh.closed);
        fresh.send(Buffer.from("after"));
        assert.ok(wait_until(() => tp.aid in tp.cb.peers && tp.cb.peers[tp.aid].channels().includes("video")));
        assert.equal(Buffer.from(tp.cb.peers[tp.aid].channel("video").relay(5).__next__().data).toString(), "after");
      } finally {
        tp.close();
      }
    }));
});

// ---------------------------------------------------------------------------
// streaming :: test_peer_registry.py
// ---------------------------------------------------------------------------
describe("TestPeerRegistry", () => {
  for (const kind of ["loopback", "tcp"]) {
    test(`registry and proxy semantics (${kind})`, () =>
      macrotask(() => {
        const tp = TwoPolicies(kind);
        try {
          const peers = tp.ca.peers;
          assert.ok(peers instanceof PeerRegistry);
          assert.equal(peers.size, 1);
          assert.equal(peers.__len__(), 1);
          assert.ok(tp.bid in peers);
          assert.deepEqual([...peers.keys()], [tp.bid]);
          const proxy = peers[tp.bid];
          assert.ok(proxy instanceof PeerProxy);
          assert.ok(proxy instanceof RemotePolicyProxy);
          assert.equal(peers.get(tp.bid), proxy);
          assert.equal(LAILA._remote_policies[tp.bid], proxy);
          assert.equal(proxy.channel("default"), proxy);
          assert.throws(() => proxy.__getitem__(0), E.TypeError);
          assert.throws(() => peers.channel(tp.ca, tp.bid, "default"), E.ValueError);
          assert.ok(proxy.central.memory.remember instanceof _RemoteAttrChain);
          assert.ok(tp.pa.ping(tp.bid));
          const ch = proxy.channel("video");
          assert.ok(ch instanceof Channel);
          assert.equal(proxy.channel("video"), ch);
          assert.deepEqual(proxy.channels(), ["video"]);
          assert.equal(repr(proxy), `PeerProxy(${repr(tp.bid)})`);
        } finally {
          tp.close();
        }
      }));
  }
  test("registry dict ops and unregister", () =>
    macrotask(() => {
      const tp = TwoPolicies("loopback");
      try {
        const peers = tp.ca.peers;
        assert.deepEqual(Object.fromEntries(peers.items()), { [tp.bid]: peers[tp.bid] });
        tp.ca.remove_peer(tp.bid);
        assert.equal(peers.size, 0);
        assert.ok(!(tp.bid in LAILA._remote_policies));
      } finally {
        tp.close();
      }
    }));
  test("peers field coerces plain dict", () => {
    const comm = new DefaultPolicy().central.communication;
    assert.ok(comm.peers instanceof PeerRegistry);
    const comm2 = new _LAILA_IDENTIFIABLE_COMMUNICATION({ peers: {} });
    assert.ok(comm2.peers instanceof PeerRegistry);
  });
});

class _RpcOnlyLoopback extends defaults.DefaultLoopbackProtocol {
  static protocol_name = "rpconly";
  static supports_channels = false;
  _stream_enqueue() {
    throw new E.AssertionError("stream path reached on an RPC-only transport");
  }
}
register_comm_protocol(_RpcOnlyLoopback);

describe("TestRpcOnlyTransport", () => {
  test("channel access raises without rpc", () =>
    macrotask(() => {
      const a = new DefaultPolicy(), b = new DefaultPolicy();
      const pa = new _RpcOnlyLoopback(), pb = new _RpcOnlyLoopback();
      a.central.communication.add_connection(pa);
      b.central.communication.add_connection(pb);
      try {
        const bid = a.central.communication.add_peer(`loopback://${b.global_id}`, pb.peer_secret_key);
        assert.ok(wait_until(() => a.global_id in b.central.communication.peers));
        const proxy = a.central.communication.peers[bid];
        assert.ok(proxy instanceof PeerProxy);
        assert.ok(pa.ping(bid));
        assert.equal(proxy.channel("default"), proxy);
        assert.equal(_RpcOnlyLoopback.supports_channels, false);
        assert.deepEqual(pa._local_caps(), {});
        const calls = [];
        const orig = pb._build_response.bind(pb);
        pb._build_response = (msg, peer_id = null) => {
          calls.push(msg);
          return orig(msg, peer_id);
        };
        const t0 = time.monotonic();
        assert.throws(() => proxy.channel("video"), (e) => e instanceof E.ConnectionError && e.message.includes("stream lanes"));
        assert.ok(time.monotonic() - t0 < 1.0);
        assert.deepEqual(calls, []);
        assert.deepEqual(proxy.channels(), []);
        assert.deepEqual(pa.channel_names(bid), []);
      } finally {
        _stop(a, b);
      }
    }));
  test("websocket tcpip is rpc only", () => assert.equal(DefaultTCPIPProtocol.supports_channels, false));
});

// ---------------------------------------------------------------------------
// Scaffold transports (lora / bluetooth) keep the documented contract
// ---------------------------------------------------------------------------
describe("TestScaffoldTransports", () => {
  for (const [cls, scheme] of [
    [defaults.DefaultLoRaProtocol, "lora://node"],
    [defaults.DefaultBluetoothProtocol, "bt://aa:bb"],
  ]) {
    test(`${cls.protocol_name} scaffold`, () => {
      assert.ok(cls.can_handle_uri(scheme));
      assert.ok(cls.prototype instanceof _LAILA_IDENTIFIABLE_COMM_PROTOCOL);
      const p = new cls();
      assert.throws(() => p.start(), E.NotImplementedError);
      assert.throws(() => p.connect(scheme, "s"), E.NotImplementedError);
      assert.throws(() => p.send_rpc("x", ["a"], [], {}), E.NotImplementedError);
      assert.equal(p.stop(), null);
      assert.equal(p.has_peer("x"), false);
    });
  }
  test("hub routes lora / bluetooth tokens to the scaffold", () =>
    macrotask(() => {
      const comm = new _LAILA_IDENTIFIABLE_COMMUNICATION({ policy_id: "t" });
      const lora = new defaults.DefaultLoRaProtocol();
      // add_connection starts the protocol -> scaffold raises NotImplementedError
      assert.throws(() => comm.add_connection(lora), E.NotImplementedError);
      assert.equal(comm._resolve_protocol_for_token("lora"), lora);
      assert.throws(() => comm.add_peer("lora://node-1", "s"), E.NotImplementedError);
    }));
});
