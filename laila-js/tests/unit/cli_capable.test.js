/**
 * CLI-capable base: parameter resolution, the ``laila.args.environment``
 * mirror and the 100-case dump -> args -> rebuild -> re-dump round-trip.
 *   tests/functional/basics/cli_capable/unit_tests/test_cli_capable.py
 *   tests/functional/basics/cli_capable/unit_tests/test_roundtrip.py
 */
import { test, describe, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import crypto from "node:crypto";

const S = new URL("../../src/", import.meta.url).href;
const laila = (await import(S + "index.js")).default;
const defaults = await import(S + "macros/defaults.js");
const E = await import(S + "_compat/errors.js");
const { DotMap } = await import(S + "_compat/dotmap.js");
const { Field, PrivateAttr, define_fields, define_private } = await import(S + "_compat/pydantic.js");
const { dumps: json_dumps } = await import(S + "_compat/pyjson.js");
const { deepcopy } = await import(S + "_compat/copy.js");
const { dict_get, dict_set, dict_items, dict_keys, dict_values, dict_has, isdict } = await import(S + "_compat/pytypes.js");
const CLI = await import(S + "basics/definitions/cli_capable.js");
const { _LAILA_IDENTIFIABLE_OBJECT, GLOBAL_ID_REGEX_PATTERN } = await import(S + "basics/definitions/identifiable_object.js");
const { _CENTRAL_COMMUNICATION_SCOPE, _POOL_SCOPE, _TASK_FORCE_SCOPE, _COMM_PROTOCOL_SCOPE } = await import(S + "macros/strings.js");
const { CLICapable, CLIExempt, _eligible_model_fields, _is_cli_exempt, build_environment } = CLI;

const { DefaultPolicy, DefaultPool, DefaultTaskForce, DefaultTCPIPProtocol } = defaults;

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

/** ``with patch("laila.args", args): body()`` */
function with_args(args, body) {
  const orig = laila.args;
  laila.args = args;
  try {
    return body();
  } finally {
    laila.args = orig;
  }
}

// ---------------------------------------------------------------------------
// test_cli_capable.py
// ---------------------------------------------------------------------------
class DummyCommunication extends CLICapable(_LAILA_IDENTIFIABLE_OBJECT) {
  static {
    define_fields(this, {
      host: ["str", Field({ default: "0.0.0.0" })],
      port: ["int", Field({ default: 0 })],
      secret: ["str", Field({ default: "default_secret" })],
      peers: ["dict", CLIExempt({ default_factory: () => ({}) })],
      policy_id: ["str", CLIExempt({ default: null })],
    });
    define_private(this, { _scopes: PrivateAttr({ default_factory: () => [_CENTRAL_COMMUNICATION_SCOPE] }) });
  }
}

class DummyPool extends CLICapable(_LAILA_IDENTIFIABLE_OBJECT) {
  static {
    define_fields(this, {
      batch_accelerated: ["bool", Field({ default: false })],
      resource: ["dict", CLIExempt({ default_factory: () => ({}) })],
    });
    define_private(this, { _scopes: PrivateAttr({ default_factory: () => [_POOL_SCOPE] }) });
  }
}

describe("TestCLIExemptMarker", () => {
  test("cli exempt marks field", () => assert.ok(_is_cli_exempt(DummyCommunication.model_fields.peers)));
  test("regular field not exempt", () => assert.equal(_is_cli_exempt(DummyCommunication.model_fields.host), false));
  test("eligible fields excludes exempt", () => {
    const eligible = _eligible_model_fields(DummyCommunication);
    assert.ok(eligible.includes("host"));
    assert.ok(eligible.includes("port"));
    assert.ok(eligible.includes("secret"));
    assert.ok(!eligible.includes("peers"));
    assert.ok(!eligible.includes("policy_id"));
  });
});

describe("TestResolutionPriority", () => {
  test("explicit params highest priority", () => {
    const args = new DotMap({ policy: { central: { communication: { host: "from_args", port: 9999 } } } });
    const comm = with_args(args, () => new DummyCommunication({ host: "explicit_host", port: 1234 }));
    assert.equal(comm.host, "explicit_host");
    assert.equal(comm.port, 1234);
  });
  test("args override defaults", () => {
    const args = new DotMap({ policy: { central: { communication: { host: "from_args", port: 8080, secret: "args_secret" } } } });
    const comm = with_args(args, () => new DummyCommunication());
    assert.equal(comm.host, "from_args");
    assert.equal(comm.port, 8080);
    assert.equal(comm.secret, "args_secret");
  });
  test("defaults when no args", () => {
    const comm = with_args(new DotMap(), () => new DummyCommunication());
    assert.equal(comm.host, "0.0.0.0");
    assert.equal(comm.port, 0);
    assert.equal(comm.secret, "default_secret");
  });
  test("mixed resolution", () => {
    const args = new DotMap({ policy: { central: { communication: { port: 5555 } } } });
    const comm = with_args(args, () => new DummyCommunication({ host: "explicit" }));
    assert.equal(comm.host, "explicit");
    assert.equal(comm.port, 5555);
    assert.equal(comm.secret, "default_secret");
  });
  test("exempt fields not injected from args", () => {
    const args = new DotMap({ policy: { central: { communication: { peers: { fake: "peer" }, policy_id: "should_not_inject" } } } });
    const comm = with_args(args, () => new DummyCommunication());
    assert.deepEqual(comm.peers, {});
    assert.equal(comm.policy_id, null);
  });
});

describe("TestDynamicPathResolution", () => {
  test("pool with global_id in args", () => {
    const test_uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    const expected_gid = _LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: test_uuid, scopes: [_POOL_SCOPE] });
    const args = new DotMap({ policy: { central: { memory: { pools: { [expected_gid]: { batch_accelerated: true } } } } } });
    const pool = with_args(args, () => new DummyPool({ uuid: test_uuid }));
    assert.equal(pool.batch_accelerated, true);
  });
  test("pool without uuid uses defaults", () => {
    const pool = with_args(new DotMap(), () => new DummyPool());
    assert.equal(pool.batch_accelerated, false);
  });
});

describe("TestRuntimeError", () => {
  test("required field raises RuntimeError", () => {
    class StrictComm extends CLICapable(_LAILA_IDENTIFIABLE_OBJECT) {
      static _cli_required_fields = new Set(["mandatory_host"]);
      static {
        define_fields(this, { mandatory_host: ["str", Field({ default: null })] });
        define_private(this, { _scopes: PrivateAttr({ default_factory: () => [_CENTRAL_COMMUNICATION_SCOPE] }) });
      }
    }
    assert.throws(
      () => with_args(new DotMap(), () => new StrictComm()),
      (e) => e instanceof E.RuntimeError && /mandatory_host/.test(e.message),
    );
  });
});

const _active_environment = () => dict_get(laila.args.environment.policies, String(laila.active_policy.global_id));

describe("TestLailaEnvironment", () => {
  test("environment returns dict", () => {
    // The Python original relies on an earlier test having built a policy
    // (``policies`` is only mirrored once one exists); make that explicit.
    void laila.active_policy;
    const env = laila.args.environment;
    assert.ok(env instanceof DotMap); // ``DotMap`` is a ``dict`` subclass in Python
    assert.ok(dict_has(env, "policies"));
    assert.ok(dict_has(env.policies, String(laila.active_policy.global_id)));
  });
  test("environment has communication", () =>
    macrotask(() => {
      const old = laila.get_active_policy();
      const p = new DefaultPolicy();
      const tcp = new DefaultTCPIPProtocol();
      p.central.communication.add_connection(tcp);
      laila.active_policy = p;
      try {
        const policy_dump = dict_get(laila.args.environment.policies, String(p.global_id));
        assert.ok(dict_has(policy_dump, "central"));
        assert.ok(dict_has(policy_dump.central, "communication"));
        const comm = policy_dump.central.communication;
        assert.ok(dict_has(comm, "connections"));
        const proto_data = dict_values(comm.connections)[0];
        for (const k of ["host", "port", "peer_secret_key"]) assert.ok(dict_has(proto_data, k), k);
      } finally {
        p.central.communication.stop();
        laila.active_policy = old;
      }
    }));
  test("environment excludes exempt fields", () => {
    const policy_dump = _active_environment();
    assert.ok(!dict_has(policy_dump, "future_bank"));
    for (const tf_data of dict_values(policy_dump.central.command.taskforces)) assert.ok(!dict_has(tf_data, "policy_id"));
  });
  test("environment excludes private attrs", () => {
    const policy_dump = _active_environment();
    for (const k of ["_uuid", "_scopes", "_evolution"]) assert.ok(!dict_has(policy_dump, k), k);
  });
  test("environment has pool info", () => {
    const router = _active_environment().central.memory.pool_router;
    assert.ok(dict_has(router, "pools"));
    assert.ok(dict_has(router, "pools_nicknames"));
  });
  test("environment pool excludes resource", () => {
    for (const pool_data of dict_values(_active_environment().central.memory.pool_router.pools)) {
      assert.ok(!dict_has(pool_data, "resource"));
      assert.ok(!dict_has(pool_data, "transformations"));
    }
  });
  test("environment taskforce excludes status", () => {
    for (const tf_data of dict_values(_active_environment().central.command.taskforces)) {
      assert.ok(!dict_has(tf_data, "status"));
      assert.ok(!dict_has(tf_data, "policy_id"));
      assert.ok(dict_has(tf_data, "backend"));
      assert.ok(dict_has(tf_data, "num_workers"));
    }
  });
  test("environment has identity properties", () => {
    const policy_dump = _active_environment();
    for (const k of ["global_id", "uuid", "scopes"]) assert.ok(dict_has(policy_dump, k), k);
  });
});

// ---------------------------------------------------------------------------
// test_roundtrip.py
// ---------------------------------------------------------------------------
const IDENTITY_KEYS = new Set(["uuid", "global_id", "scopes", "evolution"]);
const _GID_PREFIX = "LAILA:";
const _is_global_id = (s) => typeof s === "string" && s.startsWith(_GID_PREFIX);

function _strip_identity(d) {
  if (isdict(d)) {
    const cleaned = {};
    for (const [k, v] of dict_items(d)) {
      if (IDENTITY_KEYS.has(k)) continue;
      if (_is_global_id(k)) {
        cleaned[`__item_${Object.keys(cleaned).length}__`] = _strip_identity(v);
      } else {
        const stripped = _strip_identity(v);
        if (_is_global_id(stripped)) continue;
        cleaned[k] = stripped;
      }
    }
    return cleaned;
  }
  if (Array.isArray(d)) return d.map(_strip_identity);
  return d;
}

const _sorted_json = (x) => json_dumps(x, { sort_keys: true, default: String });

function _normalize_collections(d) {
  if (isdict(d)) {
    const items = dict_items(d);
    const gid_keyed = items.filter(([k]) => _is_global_id(k));
    if (gid_keyed.length) {
      const sorted_vals = gid_keyed.map(([, v]) => _normalize_collections(v)).sort((a, b) => (_sorted_json(a) < _sorted_json(b) ? -1 : _sorted_json(a) > _sorted_json(b) ? 1 : 0));
      const result = Object.fromEntries(items.filter(([k]) => !_is_global_id(k)).map(([k, v]) => [k, _normalize_collections(v)]));
      sorted_vals.forEach((val, i) => (result[`_item_${i}`] = val));
      return result;
    }
    return Object.fromEntries(items.map(([k, v]) => [k, _normalize_collections(v)]));
  }
  if (Array.isArray(d)) return d.map(_normalize_collections);
  return d;
}

const _get = (d, k, dflt = {}) => (d && dict_has(d, k) ? dict_get(d, k) : dflt);
const _extract_comm = (env) => _get(_get(_get(env, "policy"), "central"), "communication");
const _extract_proto = (env) => {
  const conns = _get(_extract_comm(env), "connections");
  const vals = dict_values(conns);
  return vals.length ? vals[0] : {};
};
const _extract_command = (env) => _get(_get(_get(env, "policy"), "central"), "command");
const _extract_memory = (env) => _get(_get(_get(env, "policy"), "central"), "memory");

function _fresh_policy(kwargs = {}) {
  laila._active_policy_gid = null;
  return new DefaultPolicy(kwargs);
}

/** Register a TCP/IP protocol with *policy* **without** starting it. */
function _attach_tcp(policy, tcp) {
  tcp._communication = policy.central.communication;
  dict_set(policy.central.communication.connections, tcp.global_id, tcp);
}

function _fresh_policy_with_tcp(tcp_kwargs = {}) {
  const p = _fresh_policy();
  const tcp = new DefaultTCPIPProtocol(tcp_kwargs);
  _attach_tcp(p, tcp);
  return [p, tcp];
}

function _stop_started_protocols() {
  for (const policy of Object.values(laila._local_policies)) {
    const comm = policy?.central?.communication;
    if (!comm) continue;
    for (const proto of dict_values(comm.connections ?? {})) {
      if (proto._started) {
        try {
          proto.stop();
        } catch {
          /* ignore */
        }
      }
    }
  }
}

const _roundtrip_config_fields = (env1, env2) => _sorted_json(_strip_identity(env1)) === _sorted_json(_strip_identity(env2));
const _dump = (policy) => build_environment(policy);
const _cpu = () => Math.max(1, os.cpus().length || 1);

describe("TestRoundTrip", () => {
  let _orig_args;
  beforeEach(() => {
    laila._active_policy_gid = null;
    _orig_args = laila.args;
  });
  afterEach(() => {
    _stop_started_protocols();
    laila._active_policy_gid = null;
    laila.args = _orig_args;
  });

  const _roundtrip = (policy1, check_identity = false) => {
    const env1 = _dump(policy1);
    laila.args = new DotMap(env1);
    laila._active_policy_gid = null;
    const policy2 = new DefaultPolicy();
    const env2 = _dump(policy2);
    if (check_identity) assert.equal(_sorted_json(env1), _sorted_json(env2));
    else assert.ok(_roundtrip_config_fields(env1, env2), `Config mismatch.\nENV1: ${json_dumps(_strip_identity(env1), { indent: 2, default: String })}\nENV2: ${json_dumps(_strip_identity(env2), { indent: 2, default: String })}`);
    return [env1, env2, policy2];
  };

  // 1-10: vanilla
  test("01 vanilla default policy", () => _roundtrip(_fresh_policy()));
  test("02 default with tcp dumps contain host", () => {
    const [p] = _fresh_policy_with_tcp();
    const env = _dump(p);
    assert.ok(dict_has(_extract_comm(env), "connections"));
    assert.ok(dict_has(_extract_proto(env), "host"));
  });
  test("03 default with tcp dumps contain port", () => assert.ok(dict_has(_extract_proto(_dump(_fresh_policy_with_tcp()[0])), "port")));
  test("04 default with tcp dumps contain peer_secret_key", () => assert.ok(dict_has(_extract_proto(_dump(_fresh_policy_with_tcp()[0])), "peer_secret_key")));
  test("05 default dumps contain alpha_taskforce", () => assert.ok(dict_has(_extract_command(_dump(_fresh_policy())), "alpha_taskforce")));
  test("06 default dumps contain pool_router", () => assert.ok(dict_has(_extract_memory(_dump(_fresh_policy())), "pool_router")));
  test("07 default dumps no resource in pools", () => {
    for (const pd of dict_values(_get(_get(_extract_memory(_dump(_fresh_policy())), "pool_router"), "pools"))) assert.ok(!dict_has(pd, "resource"));
  });
  test("08 default dumps no transformations in pools", () => {
    for (const pd of dict_values(_get(_get(_extract_memory(_dump(_fresh_policy())), "pool_router"), "pools"))) assert.ok(!dict_has(pd, "transformations"));
  });
  test("09 default dumps no status in taskforces", () => {
    for (const tf of dict_values(_get(_extract_command(_dump(_fresh_policy())), "taskforces"))) assert.ok(!dict_has(tf, "status"));
  });
  test("10 default dumps no future_bank", () => assert.ok(!dict_has(_get(_dump(_fresh_policy()), "policy"), "future_bank")));

  // 11-20: protocol params
  const proto_case = (name, kwargs, key, expected) =>
    test(name, () => {
      const [p] = _fresh_policy_with_tcp(kwargs);
      assert.equal(_extract_proto(_dump(p))[key], expected);
    });
  proto_case("11 custom host in dump", { host: "192.168.1.100" }, "host", "192.168.1.100");
  proto_case("12 custom port in dump", { port: 8443 }, "port", 8443);
  proto_case("13 custom secret in dump", { peer_secret_key: "supersecret123" }, "peer_secret_key", "supersecret123");
  test("14 all tcp params custom in dump", () => {
    const [p] = _fresh_policy_with_tcp({ host: "10.0.0.1", port: 9090, peer_secret_key: "abc" });
    const proto = _extract_proto(_dump(p));
    assert.equal(proto.host, "10.0.0.1");
    assert.equal(proto.port, 9090);
    assert.equal(proto.peer_secret_key, "abc");
  });
  proto_case("15 localhost binding in dump", { host: "127.0.0.1", port: 0 }, "host", "127.0.0.1");
  proto_case("16 ipv6 host in dump", { host: "::1" }, "host", "::1");
  proto_case("17 high port number in dump", { port: 65535 }, "port", 65535);
  proto_case("18 empty string secret in dump", { peer_secret_key: "" }, "peer_secret_key", "");
  proto_case("19 long secret key in dump", { peer_secret_key: "x".repeat(1024) }, "peer_secret_key", "x".repeat(1024));
  proto_case("20 unicode host in dump", { host: "ホスト.example.com" }, "host", "ホスト.example.com");

  // 21-30: taskforce params
  test("21 taskforce num_workers in dump", () => {
    const p = _fresh_policy();
    const tf_id = dict_keys(p.central.command.taskforces)[0];
    dict_get(p.central.command.taskforces, tf_id).num_workers = 16;
    assert.equal(dict_get(_extract_command(_dump(p)).taskforces, tf_id).num_workers, 16);
  });
  test("22 taskforce backend roundtrip", () => {
    const p = _fresh_policy();
    dict_get(p.central.command.taskforces, p.central.command.alpha_taskforce).backend = "async_threads";
    _roundtrip(p);
  });
  test("23 taskforce num_workers falls back after rebuild", () => {
    const p = _fresh_policy();
    dict_get(p.central.command.taskforces, p.central.command.alpha_taskforce).num_workers = 32;
    laila.args = new DotMap(_dump(p));
    laila._active_policy_gid = null;
    const p2 = new DefaultPolicy();
    assert.equal(dict_get(p2.central.command.taskforces, p2.central.command.alpha_taskforce).num_workers, _cpu());
  });
  test("24 taskforce uuid preserved roundtrip", () => {
    const test_uuid = "aaaaaaaa-1111-2222-3333-444444444444";
    const gid = _LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: test_uuid, scopes: [_TASK_FORCE_SCOPE] });
    const p = _fresh_policy();
    const tf = new DefaultTaskForce({ uuid: test_uuid, policy_id: p.global_id, num_workers: 7 });
    dict_set(p.central.command.taskforces, gid, tf);
    laila.args = new DotMap(_dump(p));
    laila._active_policy_gid = null;
    const p2 = new DefaultPolicy();
    const tf2 = new DefaultTaskForce({ uuid: test_uuid, policy_id: p2.global_id });
    dict_set(p2.central.command.taskforces, tf2.global_id, tf2);
    assert.equal(tf2.num_workers, 7);
  });
  test("25 multiple taskforces in dump", () => {
    const p = _fresh_policy();
    const before = dict_keys(p.central.command.taskforces).length;
    p.central.command.add_taskforce(new DefaultTaskForce({ policy_id: p.global_id }));
    assert.equal(dict_keys(_get(_extract_command(_dump(p)), "taskforces")).length, before + 1);
  });
  test("26 new taskforce not affected by stale args", () => {
    const p = _fresh_policy();
    laila.args = new DotMap(_dump(p));
    laila._active_policy_gid = null;
    const p2 = new DefaultPolicy();
    assert.equal(new DefaultTaskForce({ policy_id: p2.global_id }).num_workers, _cpu());
  });
  test("27 taskforce env has backend", () => {
    for (const tf of dict_values(_get(_extract_command(_dump(_fresh_policy())), "taskforces"))) assert.ok(dict_has(tf, "backend"));
  });
  test("28 taskforce env has num_workers", () => {
    for (const tf of dict_values(_get(_extract_command(_dump(_fresh_policy())), "taskforces"))) assert.ok(dict_has(tf, "num_workers"));
  });
  test("29 taskforce config strip identity works", () => {
    const cmd = _get(_get(_get(_strip_identity(_dump(_fresh_policy())), "policy"), "central"), "command");
    assert.ok(!dict_has(cmd, "uuid"));
  });
  test("30 taskforce default backend is async_threads", () => {
    const backends = dict_values(_get(_extract_command(_dump(_fresh_policy())), "taskforces")).map((tf) => tf.backend);
    assert.ok(backends.includes("async_threads"));
  });

  // 31-40: pool / memory
  const pools_of = (env) => dict_values(_get(_get(_extract_memory(env), "pool_router"), "pools"));
  test("31 pool batch_accelerated in dump", () => {
    for (const pd of pools_of(_dump(_fresh_policy()))) assert.ok(dict_has(pd, "batch_accelerated"));
  });
  test("32 pool batch_accelerated default false", () => {
    for (const pd of pools_of(_dump(_fresh_policy()))) assert.equal(pd.batch_accelerated, false);
  });
  test("33 pool nicknames in dump", () => assert.ok(dict_has(_get(_extract_memory(_dump(_fresh_policy())), "pool_router"), "pools_nicknames")));
  test("34 pool nicknames has _memory", () => assert.ok(dict_has(_get(_get(_extract_memory(_dump(_fresh_policy())), "pool_router"), "pools_nicknames"), "_memory")));
  test("35 multiple pools in dump", () => {
    const p = _fresh_policy();
    p.central.memory.extend(new DefaultPool(), { pool_nickname: "extra" });
    assert.equal(pools_of(_dump(p)).length, 2);
  });
  test("36 pool uuid preserved roundtrip", () => {
    const test_uuid = "bbbbbbbb-1111-2222-3333-444444444444";
    const gid = _LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: test_uuid, scopes: [_POOL_SCOPE] });
    const p = _fresh_policy();
    dict_set(p.central.memory.pool_router.pools, gid, new DefaultPool({ uuid: test_uuid }));
    laila.args = new DotMap(_dump(p));
    laila._active_policy_gid = null;
    void new DefaultPolicy();
    assert.equal(new DefaultPool({ uuid: test_uuid }).batch_accelerated, false);
  });
  test("37 pool config normalized comparison", () => {
    const n = _normalize_collections(_strip_identity(_dump(_fresh_policy())));
    const router = _get(_get(_get(_get(n, "policy"), "central"), "memory"), "pool_router");
    assert.notEqual(_get(router, "pools", router), null);
  });
  test("38 pool env no resource", () => {
    for (const pd of pools_of(_dump(_fresh_policy()))) assert.ok(!dict_has(pd, "resource"));
  });
  test("39 pool env no transformations", () => {
    for (const pd of pools_of(_dump(_fresh_policy()))) assert.ok(!dict_has(pd, "transformations"));
  });
  test("40 pool env no pools_pq", () => assert.ok(!dict_has(_get(_extract_memory(_dump(_fresh_policy())), "pool_router"), "pools_pq")));

  // 41-50: structural
  test("41 env is dict", () => assert.ok(isdict(_dump(_fresh_policy()))));
  test("42 env has policy key", () => assert.ok(dict_has(_dump(_fresh_policy()), "policy")));
  test("43 env policy has global_id", () => assert.ok(dict_has(_dump(_fresh_policy()).policy, "global_id")));
  test("44 env top-level values not None", () => {
    for (const [k, v] of dict_items(_dump(_fresh_policy()).policy)) {
      if (k === "evolution") continue;
      assert.notEqual(v, null, `Top-level key '${k}' is None`);
    }
  });
  test("45 env central has three sections", () => {
    const central = _dump(_fresh_policy()).policy.central;
    for (const k of ["command", "memory", "communication"]) assert.ok(dict_has(central, k), k);
  });
  test("46 env no model_config leaked", () => assert.ok(!dict_has(_dump(_fresh_policy()).policy, "model_config")));
  test("47 env no classvar leaked", () => assert.ok(!dict_has(_dump(_fresh_policy()).policy, "_cli_required_fields")));
  test("48 env policy uuid is string", () => assert.equal(typeof _dump(_fresh_policy()).policy.uuid, "string"));
  test("49 env scopes is list", () => assert.ok(Array.isArray(_dump(_fresh_policy()).policy.scopes)));
  test("50 env global_id matches format", () => assert.match(_dump(_fresh_policy()).policy.global_id, GLOBAL_ID_REGEX_PATTERN));

  // 51-60: host/port combos
  test("51 tcp all zeros host", () => {
    const proto = _extract_proto(_dump(_fresh_policy_with_tcp({ host: "0.0.0.0", port: 0 })[0]));
    assert.equal(proto.host, "0.0.0.0");
    assert.equal(proto.port, 0);
  });
  proto_case("52 tcp wildcard and high port", { host: "0.0.0.0", port: 49152 }, "port", 49152);
  proto_case("53 tcp special chars in secret", { peer_secret_key: "p@$$w0rd!#%^&*()" }, "peer_secret_key", "p@$$w0rd!#%^&*()");
  test("54 tcp uuid as secret", () => assert.ok(dict_has(_extract_proto(_dump(_fresh_policy_with_tcp({ peer_secret_key: crypto.randomUUID() })[0])), "peer_secret_key")));
  proto_case("55 tcp numeric string host", { host: "999.999.999.999" }, "host", "999.999.999.999");
  proto_case("56 tcp fqdn host", { host: "node-42.cluster.internal.example.com" }, "host", "node-42.cluster.internal.example.com");
  proto_case("57 tcp port 1", { port: 1 }, "port", 1);
  proto_case("58 tcp port 443", { port: 443 }, "port", 443);
  proto_case("59 tcp port 8080", { port: 8080 }, "port", 8080);
  proto_case("60 tcp empty host", { host: "" }, "host", "");

  // 61-70: mutations / multiple roundtrips
  test("61 mutate proto host reflected in dump", () => {
    const [p, tcp] = _fresh_policy_with_tcp({ host: "host-A" });
    assert.equal(_extract_proto(_dump(p)).host, "host-A");
    tcp.host = "host-B";
    assert.equal(_extract_proto(_dump(p)).host, "host-B");
  });
  test("62 mutate proto port reflected in dump", () => {
    const [p, tcp] = _fresh_policy_with_tcp({ port: 1111 });
    tcp.port = 2222;
    assert.equal(_extract_proto(_dump(p)).port, 2222);
  });
  test("63 mutate workers appears in fresh dump", () => {
    const p = _fresh_policy();
    const tf_id = dict_keys(p.central.command.taskforces)[0];
    dict_get(p.central.command.taskforces, tf_id).num_workers = 4;
    assert.equal(dict_get(_extract_command(_dump(p)).taskforces, tf_id).num_workers, 4);
    dict_get(p.central.command.taskforces, tf_id).num_workers = 32;
    assert.equal(dict_get(_extract_command(_dump(p)).taskforces, tf_id).num_workers, 32);
  });
  test("64 protocol roundtrip falls back to defaults", () => {
    const [p] = _fresh_policy_with_tcp({ host: "stable.host", port: 7777 });
    laila.args = new DotMap(_dump(p));
    laila._active_policy_gid = null;
    const env2 = _dump(new DefaultPolicy());
    assert.deepEqual(_get(_extract_comm(env2), "connections"), {});
  });
  test("65 vanilla roundtrip is stable", () => {
    let p = _fresh_policy();
    for (let i = 0; i < 5; i++) {
      laila.args = new DotMap(_dump(p));
      laila._active_policy_gid = null;
      p = new DefaultPolicy();
    }
    assert.ok(_roundtrip_config_fields(_dump(p), _dump(_fresh_policy())));
  });
  test("66 protocol uuid preserved roundtrip", () => {
    const test_uuid = "cccccccc-1111-2222-3333-444444444444";
    const p = _fresh_policy();
    _attach_tcp(p, new DefaultTCPIPProtocol({ uuid: test_uuid, host: "preserved.host", port: 7777, peer_secret_key: "preserved-key" }));
    laila.args = new DotMap(_dump(p));
    laila._active_policy_gid = null;
    const p2 = new DefaultPolicy();
    const tcp2 = new DefaultTCPIPProtocol({ uuid: test_uuid });
    _attach_tcp(p2, tcp2);
    assert.equal(tcp2.host, "preserved.host");
    assert.equal(tcp2.port, 7777);
    assert.equal(tcp2.peer_secret_key, "preserved-key");
  });
  test("67 connections in normalized comparison", () => {
    const [p1] = _fresh_policy_with_tcp({ host: "test", port: 123, peer_secret_key: "same" });
    const [p2] = _fresh_policy_with_tcp({ host: "test", port: 123, peer_secret_key: "same" });
    assert.deepEqual(_normalize_collections(_strip_identity(_dump(p1))), _normalize_collections(_strip_identity(_dump(p2))));
  });
  test("68 idempotent dump", () => {
    const [p] = _fresh_policy_with_tcp();
    assert.equal(_sorted_json(_dump(p)), _sorted_json(_dump(p)));
  });
  test("69 dump is deep copy safe", () => {
    const env = _dump(_fresh_policy());
    assert.equal(_sorted_json(env), _sorted_json(deepcopy(env)));
  });
  test("70 env survives json serialize/deserialize", () => {
    const env = _dump(_fresh_policy());
    laila.args = new DotMap(JSON.parse(json_dumps(env, { default: String })));
    laila._active_policy_gid = null;
    assert.ok(_roundtrip_config_fields(env, _dump(new DefaultPolicy())));
  });

  // 71-80: exempt field boundaries
  test("71 no peers in dump", () => assert.ok(!dict_has(_extract_comm(_dump(_fresh_policy())), "peers")));
  test("72 no policy_id in comm dump", () => assert.ok(!dict_has(_extract_comm(_dump(_fresh_policy())), "policy_id")));
  test("73 no policy_id in command dump", () => assert.ok(!dict_has(_extract_command(_dump(_fresh_policy())), "policy_id")));
  test("74 taskforces present but exempt fields excluded", () => {
    const cmd = _extract_command(_dump(_fresh_policy()));
    assert.ok(dict_has(cmd, "taskforces"));
    for (const tf of dict_values(cmd.taskforces)) {
      assert.ok(!dict_has(tf, "status"));
      assert.ok(!dict_has(tf, "policy_id"));
    }
  });
  test("75 central present as config key", () => assert.ok(dict_has(_dump(_fresh_policy()).policy, "central")));
  test("76 no future_bank in dump", () => assert.ok(!dict_has(_dump(_fresh_policy()).policy, "future_bank")));
  test("77 no pools_pq in router dump", () => assert.ok(!dict_has(_get(_extract_memory(_dump(_fresh_policy())), "pool_router"), "pools_pq")));
  test("78 no logic in central dump", () => assert.ok(!dict_has(_get(_dump(_fresh_policy()).policy, "central"), "logic")));
  test("79 pools present without resource/transformations", () => {
    const router = _get(_extract_memory(_dump(_fresh_policy())), "pool_router");
    assert.ok(dict_has(router, "pools"));
    for (const pd of dict_values(router.pools)) {
      assert.ok(!dict_has(pd, "resource"));
      assert.ok(!dict_has(pd, "transformations"));
    }
  });
  test("80 no private attrs anywhere", () => {
    const [p] = _fresh_policy_with_tcp();
    const NICKNAME_PATHS = new Set([".policy.central.memory.pool_router.pools_nicknames"]);
    const check = (d, path_ = "") => {
      if (isdict(d)) {
        for (const [k, v] of dict_items(d)) {
          const full = `${path_}.${k}`;
          assert.ok(!k.startsWith("__"), `Private attr '${k}' leaked at ${full}`);
          if (k.startsWith("_") && !NICKNAME_PATHS.has(path_)) assert.ok(_is_global_id(k), `Unexpected underscore key '${k}' at ${full}`);
          check(v, full);
        }
      } else if (Array.isArray(d)) d.forEach((v, i) => check(v, `${path_}[${i}]`));
    };
    check(_dump(p));
  });

  // 81-90: partial args
  const with_fresh_args = (obj) => {
    laila.args = new DotMap(obj);
    laila._active_policy_gid = null;
    return new DefaultPolicy();
  };
  test("81 empty args uses all defaults", () => {
    const p = with_fresh_args({});
    assert.notEqual(p.central.communication, null);
    assert.deepEqual(p.central.communication.connections, {});
  });
  test("82 partially filled central args", () => {
    const p = with_fresh_args({ policy: { central: {} } });
    assert.notEqual(p.central.communication, null);
    assert.notEqual(p.central.command, null);
    assert.notEqual(p.central.memory, null);
  });
  test("83 args with extra unknown keys ignored", () => {
    const p = with_fresh_args({ policy: { central: { communication: { nonexistent_field: "should_be_ignored" } } } });
    assert.notEqual(p.central.communication, null);
  });
  test("84 args for command dont affect comm", () => {
    const p = with_fresh_args({ policy: { central: { command: { alpha_taskforce: "test-id" } } } });
    assert.notEqual(p.central.communication, null);
  });
  test("85 args nested deep but empty", () => assert.notEqual(with_fresh_args({ policy: { central: { communication: {} } } }).central.communication, null));
  test("86 args with None values uses defaults", () => assert.notEqual(with_fresh_args({ policy: { central: { communication: { connections: null } } } }).central.communication, null));
  test("87 protocol args inject with known uuid", () => {
    const test_uuid = "dddddddd-1111-2222-3333-444444444444";
    const gid = _LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: test_uuid, scopes: [_COMM_PROTOCOL_SCOPE] });
    laila.args = new DotMap({ policy: { central: { communication: { connections: { [gid]: { host: "injected-host", port: 5555 } } } } } });
    const tcp = new DefaultTCPIPProtocol({ uuid: test_uuid });
    assert.equal(tcp.host, "injected-host");
    assert.equal(tcp.port, 5555);
  });
  test("88 protocol args no match uses defaults", () => {
    laila.args = new DotMap({ policy: { central: { communication: { connections: { "LAILA:COMM_PROTOCOL:aaaa-bbbb": { host: "should-not-match" } } } } } });
    assert.equal(new DefaultTCPIPProtocol().host, "0.0.0.0");
  });
  test("89 roundtrip policy without protocol is clean", () => _roundtrip(_fresh_policy()));
  test("90 roundtrip policy with protocol normalizes", () => {
    const [p] = _fresh_policy_with_tcp({ host: "round", port: 999 });
    const env1 = _dump(p);
    laila.args = new DotMap(env1);
    laila._active_policy_gid = null;
    const env2 = _dump(new DefaultPolicy());
    const comm1 = _get(_get(_get(_normalize_collections(_strip_identity(env1)), "policy"), "central"), "communication");
    const comm2 = _get(_get(_get(_normalize_collections(_strip_identity(env2)), "policy"), "central"), "communication");
    delete comm1.connections;
    delete comm2.connections;
    assert.deepEqual(comm1, comm2);
  });

  // 91-100: stress / exotic
  proto_case("91 very long host string", { host: "a".repeat(10000) }, "host", "a".repeat(10000));
  proto_case("92 secret with newlines and tabs", { peer_secret_key: "line1\nline2\ttab" }, "peer_secret_key", "line1\nline2\ttab");
  test("93 secret with null bytes", () => assert.ok(dict_has(_extract_proto(_dump(_fresh_policy_with_tcp({ peer_secret_key: "before\x00after" })[0])), "peer_secret_key")));
  proto_case("94 port zero roundtrip", { port: 0 }, "port", 0);
  test("95 all configurable fields present in proto env", () => {
    const eligible = _eligible_model_fields(new DefaultTCPIPProtocol().constructor);
    const proto = _extract_proto(_dump(_fresh_policy_with_tcp()[0]));
    for (const field of eligible) assert.ok(dict_has(proto, field), `Eligible field '${field}' missing from proto dump`);
  });
  test("96 dump json roundtrip preserves types", () => {
    const loaded = JSON.parse(json_dumps(_dump(_fresh_policy_with_tcp()[0]), { default: String }));
    const proto = Object.values(loaded.policy.central.communication.connections)[0];
    assert.ok(Number.isInteger(proto.port));
    assert.equal(typeof proto.host, "string");
    assert.equal(typeof proto.peer_secret_key, "string");
  });
  test("97 multiple protocols in dump", () => {
    const p = _fresh_policy();
    _attach_tcp(p, new DefaultTCPIPProtocol({ host: "host-1", port: 1111 }));
    _attach_tcp(p, new DefaultTCPIPProtocol({ host: "host-2", port: 2222 }));
    const conns = _get(_extract_comm(_dump(p)), "connections");
    assert.equal(dict_keys(conns).length, 2);
    assert.deepEqual(new Set(dict_values(conns).map((v) => v.host)), new Set(["host-1", "host-2"]));
  });
  test("98 simultaneous custom on all subsystems", () => {
    const [p] = _fresh_policy_with_tcp({ host: "multi", port: 1234, peer_secret_key: "multi-key" });
    const tf_id = dict_keys(p.central.command.taskforces)[0];
    dict_get(p.central.command.taskforces, tf_id).num_workers = 8;
    const env1 = _dump(p);
    assert.equal(dict_get(_extract_command(env1).taskforces, tf_id).num_workers, 8);
    const proto = _extract_proto(env1);
    assert.equal(proto.host, "multi");
    assert.equal(proto.port, 1234);
    assert.equal(proto.peer_secret_key, "multi-key");
  });
  test("99 env dump to file and back", () => {
    const [p] = _fresh_policy_with_tcp({ host: "file-test", port: 5678 });
    const tmp = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "laila-env-")), "env.json");
    fs.writeFileSync(tmp, json_dumps(_dump(p), { default: String }));
    try {
      const loaded = JSON.parse(fs.readFileSync(tmp, "utf8"));
      assert.ok("connections" in loaded.policy.central.communication);
      const proto = Object.values(loaded.policy.central.communication.connections)[0];
      assert.equal(proto.host, "file-test");
      assert.equal(proto.port, 5678);
    } finally {
      fs.rmSync(path.dirname(tmp), { recursive: true, force: true });
    }
  });
  test("100 comprehensive all fields", () => {
    const [p] = _fresh_policy_with_tcp({ host: "comprehensive.test", port: 31415, peer_secret_key: "ultimate-key-2024" });
    const tf_id = p.central.command.alpha_taskforce;
    dict_get(p.central.command.taskforces, tf_id).num_workers = 13;
    const env1 = _dump(p);
    const ctf = dict_get(_extract_command(env1).taskforces, tf_id);
    assert.equal(ctf.num_workers, 13);
    assert.equal(ctf.backend, "async_threads");
    const proto = _extract_proto(env1);
    assert.equal(proto.host, "comprehensive.test");
    assert.equal(proto.port, 31415);
    assert.equal(proto.peer_secret_key, "ultimate-key-2024");
    for (const pd of pools_of(env1)) {
      assert.ok(!dict_has(pd, "resource"));
      assert.ok(!dict_has(pd, "transformations"));
      assert.equal(_get(pd, "batch_accelerated", true), false);
    }
    assert.ok(!dict_has(env1.policy, "future_bank"));
    assert.ok(!dict_has(_extract_comm(env1), "peers"));
    assert.ok(!dict_has(_extract_comm(env1), "policy_id"));
  });
});
