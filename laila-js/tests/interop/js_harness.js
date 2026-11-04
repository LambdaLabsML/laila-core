#!/usr/bin/env node
/**
 * JavaScript ``laila`` peer harness for the N-language interop matrix.
 *
 * Implements the harness CLI contract documented in
 * ``tests/interop/README.md`` at the repository root (the same contract
 * ``tests/interop/harness/py_harness.py`` and
 * ``laila-c/tests/interop/c_harness.cpp`` implement), so the pytest
 * orchestrator can drive the Node port exactly like any other language.
 *
 * Modes
 * -----
 *   serve <scheme> <port> <secret> <value> [peer_uri ...]
 *   remember <uri> <secret> <gid> [pool_nickname]      -> VALUE <string>
 *   memorize <uri> <secret> <value> [pool_nickname]    -> MEMO <gid>
 *   memorize_typed <uri> <secret> <kind> [pool_nickname] -> MEMO <gid>
 *   remember_typed <uri> <secret> <gid> <kind>         -> TYPED_OK <kind> | TYPED_MISMATCH <kind> <repr>
 *   memorize_json <uri> <secret> <json> [pool_nickname] -> MEMO <gid>
 *   remember_json <uri> <secret> <gid> [pool_nickname] -> JSON <json>
 *   memorize_bytes <uri> <secret> <n> <seed> [pool_nickname] -> MEMO <gid> <fnv1a64>
 *   remember_hash <uri> <secret> <gid> [pool_nickname] -> HASH <fnv1a64> <len>
 *   relay <via_uri> <secret> <from_policy_gid> <gid> [pool_nickname] -> VALUE <string>
 *   forget <uri> <secret> <gid> [pool_nickname]        -> FORGOT <gid>
 *
 * Any failure prints ``ERROR <message>`` and exits 1.
 */
import { NDArray } from "../../src/_compat/ndarray.js";
import { PyFloat } from "../../src/_compat/pytypes.js";
import laila from "../../src/index.js";
import {
  DefaultPolicy,
  DefaultPool,
  DefaultTCPIPProtocol,
  DefaultTCPProtocol,
} from "../../src/macros/defaults.js";

const STORE = "remote-store";

// Fixed typed payloads; keep in sync with TYPED in py_harness.py and
// build_typed() in c_harness.cpp.
const TYPED = {
  int: () => 1234567,
  double: () => 3.5,
  bool: () => true,
  bytes: () => Buffer.from([0, 1, 254, 255]),
  dict: () => ({ a: 1, b: [1, 2, 3] }),
  numpy: () =>
    NDArray.array(
      [
        [1, 2],
        [3, 4],
      ],
      "<f4",
    ),
};

const say = (line) => process.stdout.write(line + "\n");

function wait(fut, timeout = 60) {
  if (fut && typeof fut.wait === "function") return fut.wait(timeout);
  return fut;
}

function activate() {
  const policy = new DefaultPolicy();
  laila.activate_policy(policy);
  return policy;
}

/** Register ``tcp://`` and ``ws://`` protocols; the one matching *serve_scheme* binds *port*. */
function add_transports(secret, serve_scheme = null, port = 0) {
  const protos = {};
  for (const [scheme, Proto] of [
    ["tcp", DefaultTCPProtocol],
    ["ws", DefaultTCPIPProtocol],
  ]) {
    const p = new Proto({
      host: "127.0.0.1",
      port: scheme === serve_scheme ? port : 0,
      peer_secret_key: secret,
    });
    laila.communication.add_connection(p);
    protos[scheme] = p;
  }
  return protos;
}

const to_plain = (v) =>
  v instanceof Map
    ? Object.fromEntries([...v].map(([k, x]) => [k, to_plain(x)]))
    : Array.isArray(v)
      ? v.map(to_plain)
      : v instanceof Number
        ? Number(v)
        : typeof v === "bigint"
          ? Number(v)
          : v instanceof NDArray
            ? v.tolist()
            : v &&
                typeof v === "object" &&
                !(v instanceof Uint8Array) &&
                Object.getPrototypeOf(v) === Object.prototype
              ? Object.fromEntries(
                  Object.entries(v).map(([k, x]) => [k, to_plain(x)]),
                )
              : v;

/** Deterministic byte stream shared by every harness (xorshift32, low byte per step). */
function xorshift_bytes(n, seed) {
  let x = seed >>> 0 || 1;
  const out = Buffer.alloc(n);
  for (let i = 0; i < n; i++) {
    x ^= x << 13;
    x >>>= 0;
    x ^= x >>> 17;
    x ^= x << 5;
    x >>>= 0;
    out[i] = x & 0xff;
  }
  return out;
}

/** FNV-1a 64 as 16 lowercase hex digits (two 32-bit limbs; no BigInt in the hot loop). */
function fnv1a64(buf) {
  // h = hi:lo (uint64); prime 0x100000001b3 = 2^40 + 0x1b3.
  let hi = 0xcbf29ce4 >>> 0;
  let lo = 0x84222325 >>> 0;
  for (let i = 0; i < buf.length; i++) {
    lo = (lo ^ buf[i]) >>> 0;
    // (hi:lo) * 0x1b3 + (hi:lo) << 40, mod 2^64.
    const lo_lo = (lo & 0xffff) * 0x1b3;
    const lo_hi = (lo >>> 16) * 0x1b3 + (lo_lo >>> 16);
    const new_lo = (((lo_hi & 0xffff) << 16) | (lo_lo & 0xffff)) >>> 0;
    let new_hi = (Math.imul(hi, 0x1b3) + (lo_hi >>> 16)) >>> 0;
    // + lo << 40  => contributes (lo << 8) to hi only.
    new_hi = (new_hi + ((lo << 8) >>> 0)) >>> 0;
    hi = new_hi;
    lo = new_lo;
  }
  return hi.toString(16).padStart(8, "0") + lo.toString(16).padStart(8, "0");
}

function repr(v) {
  if (v instanceof NDArray)
    return `ndarray(${v.dtype},[${[...v.shape]}]):${JSON.stringify(v.tolist())}`;
  if (v instanceof Uint8Array) return `bytes:${Buffer.from(v).toString("hex")}`;
  if (v instanceof PyFloat) return `float:${Number(v)}`;
  try {
    return `${typeof v}:${JSON.stringify(to_plain(v))}`;
  } catch {
    return String(v);
  }
}

function typed_matches(kind, got) {
  switch (kind) {
    case "int":
      return (
        typeof got === "number" && Number.isInteger(got) && got === 1234567
      );
    case "double":
      return (
        (typeof got === "number" || got instanceof Number) &&
        typeof got !== "boolean" &&
        Number(got) === 3.5
      );
    case "bool":
      return got === true;
    case "bytes":
      return (
        got instanceof Uint8Array && Buffer.from(got).equals(TYPED.bytes())
      );
    case "dict":
      return JSON.stringify(to_plain(got)) === JSON.stringify(TYPED.dict());
    case "numpy": {
      if (!(got instanceof NDArray)) return false;
      const want = TYPED.numpy();
      return (
        got.dtype === want.dtype &&
        JSON.stringify([...got.shape]) === JSON.stringify([...want.shape]) &&
        JSON.stringify(got.tolist()) === JSON.stringify(want.tolist())
      );
    }
    default:
      throw new Error(`unknown typed kind: ${kind}`);
  }
}

// ---------------------------------------------------------------------------
// modes
// ---------------------------------------------------------------------------

function do_serve(scheme, port, secret, value, peers) {
  const policy = activate();
  const alpha = laila.constant(value);
  wait(laila.memorize(alpha));

  laila.memory.extend(new DefaultPool(), { pool_nickname: STORE });
  const store = laila.constant(value);
  wait(laila.memorize(store, { pool_nickname: STORE }));

  const protos = add_transports(secret, scheme, port);

  say(`POLICY ${policy.global_id}`);
  say(`ALPHA_GID ${alpha.global_id}`);
  say(`STORE_GID ${store.global_id}`);
  for (const uri of peers) say(`PEER ${uri} ${laila.add_peer(uri, secret)}`);
  say(`READY ${protos[scheme].bound_port}`);

  const stop = () => {
    try {
      policy.central.communication.stop();
    } finally {
      process.exit(0);
    }
  };
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
  // Keep the event loop alive; the listeners run in the background.
  setInterval(() => {}, 1 << 30);
  return null; // stays up
}

function do_remember(uri, secret, gid, nick) {
  activate();
  add_transports(secret);
  const pid = laila.add_peer(uri, secret);
  const entry = wait(
    laila.remember(gid, {
      dst_policy: pid,
      dst_pool: nick ?? null,
      persist: false,
    }),
  );
  say(`VALUE ${entry.data}`);
  return 0;
}

/** Peer with *uri* and memorize a new constant holding *data* there (into pool *nick* when given). */
function push(uri, secret, data, nick) {
  activate();
  add_transports(secret);
  const pid = laila.add_peer(uri, secret);
  const e = laila.constant(data);
  wait(laila.memorize(e, { dst_policy: pid, dst_pool: nick ?? null }));
  return e;
}

/** Peer with *uri* and read *gid* (from pool *nick* when given). */
function pull(uri, secret, gid, nick) {
  activate();
  add_transports(secret);
  const pid = laila.add_peer(uri, secret);
  return wait(
    laila.remember(gid, {
      dst_policy: pid,
      dst_pool: nick ?? null,
      persist: false,
    }),
  );
}

function do_memorize(uri, secret, value, nick) {
  const e = push(uri, secret, value, nick);
  say(`MEMO ${e.global_id}`);
  return 0;
}

function do_memorize_typed(uri, secret, kind, nick) {
  if (!(kind in TYPED)) throw new Error(`unknown typed kind: ${kind}`);
  const e = push(uri, secret, TYPED[kind](), nick);
  say(`MEMO ${e.global_id}`);
  return 0;
}

function do_memorize_json(uri, secret, text, nick) {
  const e = push(uri, secret, JSON.parse(text), nick);
  say(`MEMO ${e.global_id}`);
  return 0;
}

function do_remember_json(uri, secret, gid, nick) {
  const got = pull(uri, secret, gid, nick).data;
  say(`JSON ${JSON.stringify(to_plain(got))}`);
  return 0;
}

function do_memorize_bytes(uri, secret, n, seed, nick) {
  const data = xorshift_bytes(n, seed);
  const e = push(uri, secret, data, nick);
  say(`MEMO ${e.global_id} ${fnv1a64(data)}`);
  return 0;
}

function do_remember_hash(uri, secret, gid, nick) {
  const got = pull(uri, secret, gid, nick).data;
  if (!(got instanceof Uint8Array)) {
    say(
      `HASH_TYPE ${got === null ? "null" : (got?.constructor?.name ?? typeof got)}`,
    );
    return 0;
  }
  say(`HASH ${fnv1a64(got)} ${got.length}`);
  return 0;
}

function do_remember_typed(uri, secret, gid, kind) {
  if (!(kind in TYPED)) throw new Error(`unknown typed kind: ${kind}`);
  activate();
  add_transports(secret);
  const pid = laila.add_peer(uri, secret);
  const entry = wait(laila.remember(gid, { dst_policy: pid, persist: false }));
  const got = entry.data;
  if (typed_matches(kind, got)) say(`TYPED_OK ${kind}`);
  else say(`TYPED_MISMATCH ${kind} ${repr(got)}`);
  return 0;
}

/** 3-party: ask the policy at *via_uri* (B) to pull *gid* from *from_gid* (C, optionally its pool *nick*) into B, then read it back from B. */
function do_relay(via_uri, secret, from_gid, gid, nick) {
  activate();
  add_transports(secret);
  const via = laila.add_peer(via_uri, secret);
  wait(
    laila.remember(gid, {
      src_policy: via,
      dst_policy: from_gid,
      dst_pool: nick ?? null,
    }),
  );
  const entry = wait(laila.remember(gid, { dst_policy: via, persist: false }));
  say(`VALUE ${entry.data}`);
  return 0;
}

function do_forget(uri, secret, gid, nick) {
  activate();
  add_transports(secret);
  const pid = laila.add_peer(uri, secret);
  wait(laila.forget(gid, { policy: pid, pool: nick ?? null }));
  say(`FORGOT ${gid}`);
  return 0;
}

function usage() {
  process.stderr.write(
    "usage: js_harness.js serve <scheme> <port> <secret> <value> [peer_uri ...]\n" +
      "       js_harness.js remember <uri> <secret> <gid> [pool_nickname]\n" +
      "       js_harness.js memorize <uri> <secret> <value> [pool_nickname]\n" +
      "       js_harness.js memorize_typed <uri> <secret> <kind> [pool_nickname]\n" +
      "       js_harness.js remember_typed <uri> <secret> <gid> <kind>\n" +
      "       js_harness.js memorize_json <uri> <secret> <json> [pool_nickname]\n" +
      "       js_harness.js remember_json <uri> <secret> <gid> [pool_nickname]\n" +
      "       js_harness.js memorize_bytes <uri> <secret> <n> <seed> [pool_nickname]\n" +
      "       js_harness.js remember_hash <uri> <secret> <gid> [pool_nickname]\n" +
      "       js_harness.js relay <via_uri> <secret> <from_policy_gid> <gid> [pool_nickname]\n" +
      "       js_harness.js forget <uri> <secret> <gid> [pool_nickname]\n",
  );
  return 2;
}

function finish(code) {
  try {
    laila.get_active_policy().central.communication.stop();
  } catch {
    /* no active communication */
  }
  // Policies own background work; don't let a straggler keep the harness alive.
  process.stdout.write("", () => process.exit(code));
}

function main(argv) {
  const [mode, ...a] = argv;
  if (!mode) return finish(usage());
  let code;
  try {
    if (mode === "serve" && a.length >= 4)
      code = do_serve(a[0], Number(a[1]), a[2], a[3], a.slice(4));
    else if (mode === "remember" && a.length >= 3)
      code = do_remember(a[0], a[1], a[2], a[3]);
    else if (mode === "memorize" && a.length >= 3)
      code = do_memorize(a[0], a[1], a[2], a[3]);
    else if (mode === "memorize_typed" && a.length >= 3)
      code = do_memorize_typed(a[0], a[1], a[2], a[3]);
    else if (mode === "remember_typed" && a.length >= 4)
      code = do_remember_typed(a[0], a[1], a[2], a[3]);
    else if (mode === "memorize_json" && a.length >= 3)
      code = do_memorize_json(a[0], a[1], a[2], a[3]);
    else if (mode === "remember_json" && a.length >= 3)
      code = do_remember_json(a[0], a[1], a[2], a[3]);
    else if (mode === "memorize_bytes" && a.length >= 4)
      code = do_memorize_bytes(a[0], a[1], Number(a[2]), Number(a[3]), a[4]);
    else if (mode === "remember_hash" && a.length >= 3)
      code = do_remember_hash(a[0], a[1], a[2], a[3]);
    else if (mode === "relay" && a.length >= 4)
      code = do_relay(a[0], a[1], a[2], a[3], a[4]);
    else if (mode === "forget" && a.length >= 3)
      code = do_forget(a[0], a[1], a[2], a[3]);
    else code = usage();
  } catch (e) {
    say(
      `ERROR ${e && e.constructor ? e.constructor.name : "Error"}: ${e && e.message ? e.message : e}`,
    );
    code = 1;
  }
  if (code !== null) finish(code);
}

// Run as a macrotask so the loop pump and listeners start from a clean tick
// (same convention as tests/interop/interop.test.js).
setImmediate(() => main(process.argv.slice(2)));
