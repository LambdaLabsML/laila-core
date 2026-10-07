/**
 * Top-level ``laila`` module (``src/index.js``): ports of
 *   tests/functional/macros/unit_tests/test_macros.py
 *   tests/functional/utils/unit_tests/test_remember_persist.py
 *   tests/functional/basics/runtime/unit_tests/test_terminate.py
 *   tests/functional/basics/runtime/unit_tests/test_environment_load.py
 *   (+ ``_runtime_helpers.py``)
 *
 * The resource probes of ``_runtime_helpers.py`` are rebuilt on Linux
 * ``/proc`` (listening TCP ports owned by this process, child pids). Python
 * thread-count probes have no JS counterpart (the port runs on one thread);
 * the cases that only assert thread exit (terminate 07 / 12, env-load 091)
 * are skipped with that reason, every other case keeps its assertions.
 */
import { test, describe, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";

const { default: laila, S } = await import("./fixtures/laila_root.js");
const ROOT = await import("../../src/index.js");

const E = await import(S + "_compat/errors.js");
const time = await import(S + "_compat/time.js");
const { DotMap } = await import(S + "_compat/dotmap.js");
const { UUID, uuid5, NAMESPACE_DNS } = await import(S + "_compat/uuid.js");
const { deepcopy } = await import(S + "_compat/copy.js");
const { object_setattr } = await import(S + "_compat/pydantic.js");
const { dict_get, dict_has, len } = await import(S + "_compat/pytypes.js");
const aliases = await import(S + "macros/aliases.js");
const defaults = await import(S + "macros/defaults.js");
const strings = await import(S + "macros/strings.js");
const { Entry } = await import(S + "entry/index.js");
const { _LAILA_IDENTIFIABLE_OBJECT } = await import(S + "basics/definitions/identifiable_object.js");
const { _refresh_args_environment } = await import(S + "basics/definitions/cli_capable.js");
const { _LAILA_IDENTIFIABLE_POOL } = await import(S + "data/schema/base.js");
const { SQLitePool } = await import(S + "data/sqlite/sqlite.js");
const { DuckDBPool } = await import(S + "data/duckdb/duckdb.js");
const { HDF5Pool } = await import(S + "data/hdf5/hdf5.js");
const { RemotePolicyProxy } = await import(S + "policy/central/communication/proxy.js");
const { _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL } = await import(S + "policy/central/communication/protocols/tcpip.js");
const { PythonAsyncThreadPoolTaskForce } = await import(S + "policy/central/command/taskforce/async_thread_pool_executor/index.js");
const { PythonProcessPoolTaskForce } = await import(S + "policy/central/command/taskforce/process_pool_executor/index.js");
const { FutureStatus } = await import(S + "policy/central/command/schema/future/future/future_status.js");
const PP = await import("./fixtures/process_pool_tasks.js");

const TMP_ROOT = fs.mkdtempSync(path.join(os.tmpdir(), "laila_root_test_"));
laila.set_default_directory(TMP_ROOT);

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
const t = (name, fn) => test(name, () => macrotask(fn));
const sorted = (xs) => [...xs].sort();
const count_equal = (a, b) => assert.deepEqual(sorted(a), sorted(b));
const keys = (d) => (d instanceof Map ? [...d.keys()] : Object.keys(d));
const values = (d) => (d instanceof Map ? [...d.values()] : Object.values(d));

/** ``tempfile.TemporaryDirectory()`` */
function with_tmpdir(body) {
  const td = fs.mkdtempSync(path.join(os.tmpdir(), "laila_rt_"));
  try {
    return body(td);
  } finally {
    fs.rmSync(td, { recursive: true, force: true });
  }
}

// ---------------------------------------------------------------------------
// _runtime_helpers.py
// ---------------------------------------------------------------------------
const _IS_LINUX = process.platform === "linux";

/** Socket inodes owned by this process (``/proc/self/fd``). */
function _socket_inodes() {
  const out = new Set();
  let fds;
  try {
    fds = fs.readdirSync("/proc/self/fd");
  } catch {
    return out;
  }
  for (const fd of fds) {
    try {
      const m = /^socket:\[(\d+)\]$/.exec(fs.readlinkSync(`/proc/self/fd/${fd}`));
      if (m) out.add(m[1]);
    } catch {
      /* fd closed meanwhile */
    }
  }
  return out;
}

/** Sorted TCP ports this process is listening on (loopback / wildcard). */
function _listening_ports() {
  if (!_IS_LINUX) return [];
  const inodes = _socket_inodes();
  const out = [];
  for (const table of ["/proc/net/tcp", "/proc/net/tcp6"]) {
    let text;
    try {
      text = fs.readFileSync(table, "utf8");
    } catch {
      continue;
    }
    for (const line of text.split("\n").slice(1)) {
      const cols = line.trim().split(/\s+/);
      if (cols.length < 10) continue;
      const [, local, , st] = cols;
      const inode = cols[9];
      if (st !== "0A" || !inodes.has(inode)) continue;
      const [ip_hex, port_hex] = local.split(":");
      const port = parseInt(port_hex, 16);
      const loop_or_wild =
        ip_hex === "0100007F" || ip_hex === "00000000" || /^0*$/.test(ip_hex) || /^0{24}0100007F$/.test(ip_hex) || /^0{20}FFFF0100007F$/i.test(ip_hex) || /^0{31}1$/.test(ip_hex);
      if (loop_or_wild) out.push(port);
    }
  }
  return sorted(out).map(Number).sort((a, b) => a - b);
}

/** Recursive child pids of this process (``psutil.Process.children(recursive=True)``). */
function _child_pids() {
  if (!_IS_LINUX) return [];
  const parent_of = new Map();
  let entries;
  try {
    entries = fs.readdirSync("/proc");
  } catch {
    return [];
  }
  for (const name of entries) {
    if (!/^\d+$/.test(name)) continue;
    try {
      const stat = fs.readFileSync(`/proc/${name}/stat`, "utf8");
      const rest = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
      parent_of.set(Number(name), Number(rest[1]));
    } catch {
      /* raced */
    }
  }
  const out = [];
  const stack = [process.pid];
  while (stack.length) {
    const p = stack.pop();
    for (const [pid, ppid] of parent_of) {
      if (ppid === p) {
        out.push(pid);
        stack.push(pid);
      }
    }
  }
  return out.sort((a, b) => a - b);
}

/** Current process FD count, or ``null`` if not retrievable. */
function _open_fd_count() {
  try {
    return fs.readdirSync("/proc/self/fd").length;
  } catch {
    return null;
  }
}

/** Poll *predicate* until truthy or *timeout* expires (blocking). */
function _wait_until(predicate, timeout = 5.0, interval = 0.05) {
  const deadline = time.time() + timeout;
  while (time.time() < deadline) {
    try {
      if (predicate()) return true;
    } catch {
      /* keep polling */
    }
    time.sleep(interval);
  }
  try {
    return Boolean(predicate());
  } catch {
    return false;
  }
}

/** Bind a localhost ephemeral port, then close it so the caller can reclaim it. */
function _bind_ephemeral() {
  const srv = net.createServer();
  srv.listen(0, "127.0.0.1");
  // ``listen`` completes asynchronously; pump until bound.
  _wait_until(() => srv.address() !== null, 2.0, 0.005);
  const port = srv.address().port;
  srv.close();
  return port;
}

/** ``RuntimeBaselineMixin`` */
function runtime_baseline(ctx) {
  beforeEach(() =>
    macrotask(() => {
      laila.terminate({ wait: true, cancel_pending: true });
      ctx._orig_args = laila.args;
      laila.args = new ROOT._LailaArgs();
      laila.arg_reader._target = laila.args;
      ctx._baseline_ports = _listening_ports();
      ctx._baseline_children = _child_pids();
    }),
  );
  afterEach(() =>
    macrotask(() => {
      try {
        laila.terminate({ wait: true, cancel_pending: true });
      } catch {
        /* ignore */
      }
      try {
        laila.args = ctx._orig_args;
        laila.arg_reader._target = laila.args;
      } catch {
        /* ignore */
      }
    }),
  );
  ctx.assert_no_leaks = ({ timeout = 5.0 } = {}) => {
    const ok = _wait_until(
      () =>
        JSON.stringify(_listening_ports()) === JSON.stringify(ctx._baseline_ports) &&
        JSON.stringify(_child_pids()) === JSON.stringify(ctx._baseline_children),
      timeout,
    );
    if (ok) return;
    assert.deepEqual(_listening_ports(), ctx._baseline_ports);
    assert.deepEqual(_child_pids(), ctx._baseline_children);
  };
}

// ---------------------------------------------------------------------------
// test_macros.py
// ---------------------------------------------------------------------------
describe("TestMacrosStrings", () => {
  const scope_names = Object.keys(strings).filter((n) => n.endsWith("_SCOPE"));
  test("scope constants are nonempty strings", () => {
    assert.ok(scope_names.length > 0);
    for (const name of scope_names) {
      assert.equal(typeof strings[name], "string", name);
      assert.ok(strings[name], name);
    }
  });
  test("default pool nickname", () => assert.equal(strings._DEFAULT_POOL_NICKNAME, "_memory"));
  test("topmost scope", () => assert.equal(strings._TOPMOST_SCOPE, "LAILA"));
  test("string constant values unique except laila alias pair", () => {
    const str_pairs = Object.keys(strings)
      .filter((n) => !n.startsWith("__") && typeof strings[n] === "string")
      .map((n) => [n, strings[n]]);
    const counts = new Map();
    for (const [, v] of str_pairs) counts.set(v, (counts.get(v) ?? 0) + 1);
    const duplicated = new Set([...counts].filter(([, c]) => c > 1).map(([v]) => v));
    assert.deepEqual(duplicated, new Set(["LAILA"]), "only _LAILA_SCOPE and _TOPMOST_SCOPE may share the same value");
    assert.equal(counts.get("LAILA"), 2);
    const laila_names = new Set(str_pairs.filter(([, v]) => v === "LAILA").map(([n]) => n));
    assert.deepEqual(laila_names, new Set(["_LAILA_SCOPE", "_TOPMOST_SCOPE"]));
  });
  test("scope values are uppercase", () => {
    for (const name of scope_names) assert.equal(strings[name], strings[name].toUpperCase(), `${name} should be uppercase`);
  });
  test("scope values contain no whitespace", () => {
    for (const name of scope_names) assert.ok(!/[ \t]/.test(strings[name]), `${name} contains whitespace`);
  });
  test("default pool nickname starts with underscore", () => assert.ok(strings._DEFAULT_POOL_NICKNAME.startsWith("_")));
  test("entry scope exists", () => assert.equal(strings._ENTRY_SCOPE, "ENTRY"));
  test("pool scope exists", () => assert.equal(strings._POOL_SCOPE, "POOL"));
  test("future scope exists", () => assert.equal(strings._FUTURE_SCOPE, "FUTURE"));
  test("all scope names follow naming convention", () => {
    for (const name of scope_names) {
      assert.ok(name.startsWith("_"), `${name} should start with _`);
      assert.equal(name, name.toUpperCase(), `${name} should be all-uppercase`);
    }
  });
});

describe("TestMacrosAliases", () => {
  test("callables", () => {
    assert.equal(typeof aliases.constant, "function");
    assert.equal(typeof aliases.variable, "function");
    assert.equal(typeof aliases.contingent, "function");
  });
  test("future is class", () => {
    assert.equal(typeof aliases.future, "function");
    assert.ok(aliases.future.prototype !== undefined);
  });
  test("constant produces entry", () => {
    const e = aliases.constant("macro-test");
    assert.ok(e instanceof Entry);
    assert.equal(e.data, "macro-test");
  });
  test("variable produces entry", () => {
    const e = aliases.variable("macro-test");
    assert.ok(e instanceof Entry);
    assert.equal(e.evolution, 0);
  });
  test("contingent produces entry", () => {
    const e = aliases.contingent({ data: "macro-test" });
    assert.ok(e instanceof Entry);
  });
  test("constant creates entry", () => assert.ok(aliases.constant("test") instanceof Entry));
  test("variable creates entry", () => assert.ok(aliases.variable("test") instanceof Entry));
  test("root re-exports the aliases", () => {
    assert.equal(laila.constant, aliases.constant);
    assert.equal(laila.variable, aliases.variable);
    assert.equal(laila.contingent, aliases.contingent);
    assert.equal(laila.future, aliases.future);
    assert.equal(ROOT.constant, aliases.constant);
  });
});

describe("TestMacrosDefaults", () => {
  const is_class = (x) => typeof x === "function" && x.prototype !== undefined;
  test("default policy and pool are classes", () => {
    assert.ok(is_class(defaults.DefaultPolicy));
    assert.ok(is_class(defaults.DefaultPool));
  });
  test("laila universal namespace is uuid", () => assert.ok(defaults.LAILA_UNIVERSAL_NAMESPACE instanceof UUID));
  test("auto initialize policy", () => assert.equal(defaults.AUTO_INITIALIZE_POLICY, true));
  test("namespace is deterministic", () => {
    const expected = uuid5(NAMESPACE_DNS, "laila");
    assert.equal(String(defaults.LAILA_UNIVERSAL_NAMESPACE), String(expected));
    assert.equal(String(laila.get_active_namespace()), String(expected));
  });
  test("default taskforce is class", () => assert.ok(is_class(defaults.DefaultTaskForce)));
  test("default central command is class", () => assert.ok(is_class(defaults.DefaultCentralCommand)));
  test("default central memory is class", () => assert.ok(is_class(defaults.DefaultCentralMemory)));
  test("default pool router is class", () => assert.ok(is_class(defaults.DefaultPoolRouter)));
  t("default pool is instantiable", () => {
    const pool = new defaults.DefaultPool();
    assert.notEqual(pool.global_id, null);
  });
  t("default policy is instantiable", () => {
    const policy = new defaults.DefaultPolicy();
    assert.notEqual(policy.global_id, null);
    policy.central.command.shutdown({ wait: true, cancel_pending: true });
  });
  test("root exposes every Default* alias", () => {
    for (const name of Object.keys(defaults)) assert.equal(laila[name], defaults[name], name);
    assert.equal(ROOT.DefaultPolicy, defaults.DefaultPolicy);
  });
});

// ---------------------------------------------------------------------------
// laila/__init__.py :: module surface (not covered by a Python test file)
// ---------------------------------------------------------------------------
describe("TestRootSurface", () => {
  test("__version__ matches package.json", () => {
    const pkg = JSON.parse(fs.readFileSync(new URL("../../package.json", import.meta.url), "utf8"));
    assert.equal(laila.__version__, pkg.version);
  });
  test("default export is registered as lazy('laila')", async () => {
    const { lazy } = await import(S + "_compat/lazy.js");
    assert.equal(lazy("laila").memorize, laila.memorize);
    assert.equal(lazy("laila").args, laila.args);
  });
  test("named exports mirror the default object", () => {
    for (const name of ["memorize", "remember", "forget", "build", "terminate", "read_args", "activate_policy", "get_active_policy", "add_peer", "request", "relay", "status", "wait", "set_default_directory", "resolve_global_id", "get_active_namespace", "set_active_namespace"]) {
      assert.equal(ROOT[name], laila[name], name);
    }
    assert.equal(ROOT.Entry, Entry);
    assert.equal(ROOT.manifest, ROOT.Manifest);
    assert.equal(laila.manifest, ROOT.Manifest);
    assert.equal(ROOT.TaskForce.PythonAsyncThreadPoolTaskForce, PythonAsyncThreadPoolTaskForce);
    assert.equal(laila._ENTRY_SCOPE, "ENTRY");
  });
  t("subsystem shortcuts resolve through the active policy", () => {
    const p = laila.active_policy;
    assert.equal(laila.memory, p.central.memory);
    assert.equal(laila.command, p.central.command);
    assert.equal(laila.communication, p.central.communication);
    assert.equal(laila.peers, p.central.communication.peers);
    assert.equal(laila.alpha_pool, dict_get(p.central.memory.pool_router.pools, p.central.memory.alpha_pool));
    assert.equal(laila.local_policies, laila._local_policies);
    assert.equal(laila.remote_policies, laila._remote_policies);
    assert.deepEqual(laila.universe, { ...laila._remote_policies, ...laila._local_policies });
    assert.equal(laila.runtime.status, ROOT.runtime.status);
    laila.runtime = null; // no-op setter
    assert.equal(laila.runtime, ROOT.runtime);
    laila.terminate();
  });
  t("active_policy setter routes through activate_policy", () => {
    const p1 = laila.active_policy;
    const p2 = new defaults.DefaultPolicy();
    laila.active_policy = p2;
    assert.equal(laila.active_policy, p2);
    assert.equal(laila._active_policy_gid, p2.global_id);
    assert.ok(p1.global_id in laila._local_policies);
    assert.ok(p2.global_id in laila._local_policies);
    laila.terminate();
  });
  t("set_active_namespace / get_active_namespace", () => {
    const before = laila.get_active_namespace();
    try {
      laila.set_active_namespace("acme.research.experiments");
      assert.equal(String(laila.get_active_namespace()), String(uuid5(NAMESPACE_DNS, "acme.research.experiments")));
      const a = laila.constant(1, { nickname: "n" }).global_id;
      laila.set_active_namespace("other");
      const b = laila.constant(1, { nickname: "n" }).global_id;
      assert.notEqual(a, b);
    } finally {
      laila.set_active_namespace("laila");
      assert.equal(String(laila.get_active_namespace()), String(before));
    }
  });
  test("encryption_key accessor aliases laila.args.encryption.key", () => {
    const orig = laila.args;
    laila.args = new ROOT._LailaArgs();
    try {
      assert.equal(laila.encryption_key, null);
      laila.args.encryption; // autovivified empty DotMap -> still None
      assert.equal(laila.encryption_key, null);
      laila.encryption_key = "k";
      assert.equal(laila.encryption_key, "k");
      assert.equal(laila.args.encryption.key, "k");
      laila.encryption_key = "";
      assert.equal(laila.encryption_key, null);
    } finally {
      laila.args = orig;
    }
  });
  test("logger accessor is the singleton; setter replaces it", () => {
    const { Logger, get_logger } = ROOT;
    const l1 = laila.logger;
    assert.equal(l1, get_logger());
    const fresh = new Logger();
    laila.logger = fresh;
    assert.equal(laila.logger, fresh);
    Logger.reset_singleton();
  });
  test("_LailaArgs coerces plain dicts to DotMap and ignores inert environment writes", () => {
    const a = new ROOT._LailaArgs();
    a.foo = { bar: 1 };
    assert.ok(a.foo instanceof DotMap);
    assert.equal(a.foo.bar, 1);
    a.environment = {};
    assert.ok(a.environment instanceof DotMap);
    assert.equal(len(a.environment), 0);
    assert.equal(ROOT._is_env_load_trigger({}), false);
    assert.equal(ROOT._is_env_load_trigger(null), false);
    assert.equal(ROOT._is_env_load_trigger("x"), false);
    assert.equal(ROOT._is_env_load_trigger({ policies: {} }), true);
    assert.equal(ROOT._is_env_load_trigger(new DotMap({ policies: {} })), false);
    assert.equal(ROOT._is_env_load_trigger(new DotMap({ active_gid: "x" })), true);
    assert.equal(ROOT._is_env_load_trigger(new DotMap({ policies: { a: {} } })), true);
  });
  t("read_args merges a JSON file into laila.args", () => {
    const orig = laila.args;
    laila.args = new ROOT._LailaArgs();
    laila.arg_reader._target = laila.args;
    try {
      with_tmpdir((td) => {
        const p = path.join(td, "cfg.json");
        fs.writeFileSync(p, JSON.stringify({ policy: { central: { memory: { x: 1 } } }, flag: true }));
        laila.read_args(p);
        // ArgReader flattens one level (``policy.central`` -> ``policy_central``).
        assert.equal(laila.args.policy_central.memory.x, 1);
        assert.equal(laila.args.flag, true);
      });
    } finally {
      laila.args = orig;
      laila.arg_reader._target = laila.args;
    }
  });
  test("request / relay guard unknown peers", () => {
    assert.throws(() => laila.request("LAILA:POLICY:00000000-0000-0000-0000-000000000001"), E.ConnectionError);
    assert.throws(() => laila.relay("LAILA:POLICY:00000000-0000-0000-0000-000000000001"), E.TypeError);
  });
  t("_resolve_future resolves identities, strings and raises for junk", () => {
    const p = laila.active_policy;
    const fid = laila.command.submit([() => 1]);
    const fut = laila._resolve_future(fid);
    assert.equal(laila._resolve_future(fut), fut);
    assert.equal(laila._resolve_future(fid.global_id), fut);
    assert.equal(fut.wait().data, 1); // raw results are wrapped as Entry on read
    assert.throws(() => laila._resolve_future("LAILA:FUTURE:" + crypto.randomUUID()), E.KeyError);
    assert.throws(() => laila._resolve_future(42), E.TypeError);
    assert.equal(laila.wait(fid).data, 1);
    assert.equal(laila.status(fid), ROOT.runtime.status(fid));
    void p;
    laila.terminate();
  });
  test("set_default_directory expands ~ and rewires every subdirectory", () => {
    const saved = { ...defaults.LAILA_DEFAULT_DIRECTORIES };
    try {
      laila.set_default_directory("~/laila-root-test");
      const root = path.join(os.homedir(), "laila-root-test");
      assert.equal(defaults.LAILA_DEFAULT_DIRECTORIES.root, root);
      for (const sub of ["pools", "logs", "secrets", "indices"]) assert.equal(defaults.LAILA_DEFAULT_DIRECTORIES[sub], path.join(root, sub));
    } finally {
      Object.assign(defaults.LAILA_DEFAULT_DIRECTORIES, saved);
    }
  });
});

// ---------------------------------------------------------------------------
// test_remember_persist.py
// ---------------------------------------------------------------------------
describe("TestRememberPersist", () => {
  const ctx = {};
  beforeEach(() =>
    macrotask(() => {
      ctx._policy = laila.get_active_policy();
      ctx._memory = ctx._policy.central.memory;
      const suffix = crypto.randomUUID().replaceAll("-", "").slice(0, 12);
      ctx._source_pool = new _LAILA_IDENTIFIABLE_POOL();
      ctx._memory.extend(ctx._source_pool, { pool_nickname: `persist-src-${suffix}` });
      ctx._nick_prefix = `persist-${suffix}`;
    }),
  );
  const _alpha_pool = () => dict_get(ctx._memory.pool_router.pools, ctx._memory.alpha_pool);
  const _resolve = (future_ref) => dict_get(ctx._policy.future_bank, future_ref.global_id);

  t("remember persist true writes entry into alpha pool", () => {
    const entry = laila.constant({ n: 1 }, { nickname: `${ctx._nick_prefix}-single` });
    _resolve(laila.memorize({ entries: entry, pool_id: ctx._source_pool.global_id })).wait();
    assert.ok(!dict_has(_alpha_pool().resource, entry.global_id));

    let future_ref;
    laila.guarantee.__enter__();
    try {
      future_ref = laila.remember({ entry_ids: entry.global_id, pool_id: ctx._source_pool.global_id, persist: true });
    } finally {
      laila.guarantee.__exit__(null, null, null);
    }

    assert.ok(dict_has(_alpha_pool().resource, entry.global_id));
    assert.deepEqual(_resolve(future_ref).result.data, { n: 1 });
  });

  t("remember persist true writes batch into alpha pool", () => {
    const entries = [0, 1, 2].map((i) => laila.constant(`batch-${i}`, { nickname: `${ctx._nick_prefix}-batch-${i}` }));
    _resolve(laila.memorize({ entries, pool_id: ctx._source_pool.global_id })).wait();
    for (const entry of entries) assert.ok(!dict_has(_alpha_pool().resource, entry.global_id));

    let future_ref;
    laila.guarantee.__enter__();
    try {
      future_ref = laila.remember({ entry_ids: entries.map((e) => e.global_id), pool_id: ctx._source_pool.global_id, persist: true });
    } finally {
      laila.guarantee.__exit__(null, null, null);
    }

    for (const entry of entries) assert.ok(dict_has(_alpha_pool().resource, entry.global_id));
    const recalled = _resolve(future_ref).result;
    count_equal(
      recalled.map((r) => r.data),
      entries.map((e) => e.data),
    );
  });

  t("remember persist false does not write to alpha pool", () => {
    const entry = laila.constant("no-persist", { nickname: `${ctx._nick_prefix}-off` });
    _resolve(laila.memorize({ entries: entry, pool_id: ctx._source_pool.global_id })).wait();

    let future_ref;
    laila.guarantee.__enter__();
    try {
      future_ref = laila.remember({ entry_ids: entry.global_id, pool_id: ctx._source_pool.global_id, persist: false });
    } finally {
      laila.guarantee.__exit__(null, null, null);
    }

    assert.ok(!dict_has(_alpha_pool().resource, entry.global_id));
    assert.equal(_resolve(future_ref).result.data, "no-persist");
  });

  t("remember persist true short circuits when source is alpha", () => {
    const entry = laila.constant("alpha-source", { nickname: `${ctx._nick_prefix}-alpha-src` });
    _resolve(laila.memorize({ entries: entry })).wait();
    assert.ok(dict_has(_alpha_pool().resource, entry.global_id));

    let future_ref;
    laila.guarantee.__enter__();
    try {
      future_ref = laila.remember({ entry_ids: entry.global_id, pool_id: _alpha_pool().global_id, persist: true });
    } finally {
      laila.guarantee.__exit__(null, null, null);
    }

    assert.equal(_resolve(future_ref).result.data, "alpha-source");
  });

  t("remember persist true wait completes after alpha write", () => {
    const entry = laila.constant("wait-order", { nickname: `${ctx._nick_prefix}-wait-order` });
    _resolve(laila.memorize({ entries: entry, pool_id: ctx._source_pool.global_id })).wait();

    const future_ref = laila.remember({ entry_ids: entry.global_id, pool_id: ctx._source_pool.global_id, persist: true });
    const recalled = _resolve(future_ref).wait();
    assert.equal(recalled.data, "wait-order");
    assert.ok(dict_has(_alpha_pool().resource, entry.global_id));
  });
});

// ---------------------------------------------------------------------------
// test_terminate.py
// ---------------------------------------------------------------------------
describe("TestTerminate", () => {
  const ctx = {};
  runtime_baseline(ctx);

  t("01 terminate empty state returns empty", () => {
    const errs = laila.terminate();
    assert.deepEqual(errs, []);
    assert.deepEqual(laila._local_policies, {});
  });

  t("02 terminate idempotent", () => {
    void laila.active_policy;
    laila.terminate();
    assert.deepEqual(laila.terminate(), []);
  });

  t("03 terminate clears local policies", () => {
    void laila.active_policy;
    assert.equal(Object.keys(laila._local_policies).length, 1);
    laila.terminate();
    assert.deepEqual(laila._local_policies, {});
  });

  t("04 terminate clears remote policies", () => {
    void laila.active_policy;
    const comm = laila.active_policy.central.communication;
    const proxy = new RemotePolicyProxy("LAILA:POLICY:00000000-0000-0000-0000-000000000001", comm);
    laila._remote_policies[proxy.global_id] = proxy;
    assert.ok(proxy.global_id in laila._remote_policies);
    laila.terminate();
    assert.deepEqual(laila._remote_policies, {});
  });

  t("05 terminate resets active policy gid", () => {
    void laila.active_policy;
    assert.notEqual(laila._active_policy_gid, null);
    laila.terminate();
    assert.equal(laila._active_policy_gid, null);
  });

  t("06 terminate clears env mirror policies", () => {
    void laila.active_policy;
    const gid = laila.active_policy.global_id;
    assert.ok(gid in laila.args.environment.policies.toDict());
    laila.terminate();
    const env = laila.args.get("environment");
    if (env !== null && typeof env.get === "function") {
      const policies = env.get("policies");
      if (policies !== null && policies !== undefined) assert.equal(len(policies), 0);
    }
  });

  test("07 terminate default policy joins threadpool dispatcher", { skip: "Node runtime has no dispatcher thread; see 08/09 for the shutdown semantics" }, () => {});

  t("08 terminate wait false returns promptly", () => {
    void laila.active_policy;
    laila.command.submit([() => time.sleep(0.5)]);
    const t0 = time.time();
    laila.terminate({ wait: false });
    assert.ok(time.time() - t0 < 0.6);
  });

  t("09 terminate cancel pending marks pending cancelled", () => {
    void laila.active_policy;
    const cmd = laila.command;
    const tf = dict_get(cmd.taskforces, cmd.alpha_taskforce);
    const _slow = async () => {
      await new Promise((r) => setTimeout(r, 50));
    };
    const capacity = tf.num_workers * tf.max_async_per_thread;
    const flooded = Array.from({ length: capacity * 2 + 16 }, () => tf._queue_submit(_slow));
    laila.terminate({ wait: false, cancel_pending: true });
    const cancelled = flooded.filter((f) => f.status === FutureStatus.CANCELLED);
    assert.ok(cancelled.length > 0, "terminate(cancel_pending=True) cancelled no queued futures");
  });

  t("10 terminate with multiple policies", () => {
    const p1 = new defaults.DefaultPolicy();
    const p2 = new defaults.DefaultPolicy();
    const p3 = new defaults.DefaultPolicy();
    laila.activate_policy(p1);
    laila._local_policies[p2.global_id] = p2;
    laila._local_policies[p3.global_id] = p3;
    assert.equal(Object.keys(laila._local_policies).length, 3);
    laila.terminate();
    assert.deepEqual(laila._local_policies, {});
  });

  t("11 terminate releases tcp port", () => {
    void laila.active_policy;
    const port = _bind_ephemeral();
    const proto = new defaults.DefaultTCPIPProtocol({ host: "127.0.0.1", port });
    laila.communication.add_connection(proto);
    assert.ok(_wait_until(() => _listening_ports().includes(port), 3.0), `Port ${port} never became listening`);
    laila.terminate();
    assert.ok(_wait_until(() => !_listening_ports().includes(port), 5.0), `Port ${port} still listed after terminate`);
  });

  test("12 terminate joins tcpip event loop thread", { skip: "Node runtime has no per-protocol loop thread; 11 covers the socket release" }, () => {});

  t("13 terminate continues when pool close raises", () =>
    with_tmpdir((td) => {
      void laila.active_policy;
      const good = new SQLitePool({ file_path: path.join(td, "good.sqlite") });
      const bad = new SQLitePool({ file_path: path.join(td, "bad.sqlite") });
      object_setattr(bad, "close", () => {
        throw new E.RuntimeError("boom-close");
      });
      laila.memory.extend(good);
      laila.memory.extend(bad);
      const errs = laila.terminate();
      assert.ok(
        errs.some((e) => e.includes("boom-close")),
        `missing boom-close in errors: ${errs}`,
      );
      assert.deepEqual(laila._local_policies, {});
    }));

  t("14 terminate continues when command shutdown raises", () => {
    void laila.active_policy;
    object_setattr(laila.active_policy.central.command, "shutdown", () => {
      throw new E.RuntimeError("boom-cmd");
    });
    const errs = laila.terminate();
    assert.ok(
      errs.some((e) => e.includes("boom-cmd")),
      `missing boom-cmd in errors: ${errs}`,
    );
  });

  t("15 terminate continues when communication stop raises", () => {
    void laila.active_policy;
    object_setattr(laila.active_policy.central.communication, "stop", () => {
      throw new E.RuntimeError("boom-comm");
    });
    const errs = laila.terminate();
    assert.ok(
      errs.some((e) => e.includes("boom-comm")),
      `missing boom-comm in errors: ${errs}`,
    );
    assert.deepEqual(laila._local_policies, {});
  });

  t("16 terminate closes hdf5 file handle", () =>
    with_tmpdir((td) => {
      void laila.active_policy;
      const p = path.join(td, "p.h5");
      const pool = new HDF5Pool({ file_path: p });
      laila.memory.extend(pool);
      laila.terminate();
      // The file must be re-openable for writing by a fresh pool.
      const again = new HDF5Pool({ file_path: p });
      again.close();
    }));

  t("17 terminate closes sqlite connection", () =>
    with_tmpdir((td) => {
      void laila.active_policy;
      const pool = new SQLitePool({ file_path: path.join(td, "p.sqlite") });
      assert.notEqual(pool._conn, null);
      laila.memory.extend(pool);
      laila.terminate();
      assert.equal(pool._conn, null);
    }));

  t("18 terminate closes duckdb connection", () =>
    with_tmpdir((td) => {
      void laila.active_policy;
      const pool = new DuckDBPool({ file_path: path.join(td, "p.duckdb") });
      assert.notEqual(pool._conn, null);
      laila.memory.extend(pool);
      laila.terminate();
      assert.equal(pool._conn, null);
    }));

  t("19 terminate processpool workers exit", () => {
    void laila.active_policy;
    const baseline = _child_pids();
    const tf = new PythonProcessPoolTaskForce({ policy_id: laila.active_policy.global_id, num_workers: 4 });
    laila.command.add_taskforce(tf);
    for (let i = 0; i < 4; i++) tf._worker_pool.submit(PP._return_1);
    assert.ok(_wait_until(() => _child_pids().length > baseline.length, 15.0), "Process pool workers did not appear");
    laila.terminate();
    assert.ok(_wait_until(() => JSON.stringify(_child_pids()) === JSON.stringify(baseline), 20.0), `Worker pids leaked: ${_child_pids().filter((p) => !baseline.includes(p))}`);
  });

  test("20 terminate redis subprocess terminated", { skip: "redis-server backend not exercised in the hermetic JS suite" }, () => {});
  test("21 terminate postgres subprocess terminated", { skip: "postgres backend not exercised in the hermetic JS suite" }, () => {});
  test("22 terminate mongo subprocess terminated", { skip: "mongod backend not exercised in the hermetic JS suite" }, () => {});

  t("23 terminate then active policy creates fresh", () => {
    const gid1 = laila.active_policy.global_id;
    laila.terminate();
    assert.deepEqual(laila._local_policies, {});
    const p2 = laila.active_policy;
    assert.notEqual(p2.global_id, gid1);
    assert.ok(p2.global_id in laila._local_policies);
  });

  t("24 terminate with proxy chain closes both pools", () =>
    with_tmpdir((td) => {
      void laila.active_policy;
      const a = new SQLitePool({ file_path: path.join(td, "a.sqlite") });
      const b = new SQLitePool({ file_path: path.join(td, "b.sqlite") });
      a.__lshift__(b);
      laila.memory.extend(a);
      laila.memory.extend(b);

      const closed = { a: false, b: false };
      object_setattr(a, "close", () => {
        closed.a = true;
        if (a._conn !== null) {
          a._conn.close();
          a._conn = null;
        }
      });
      object_setattr(b, "close", () => {
        closed.b = true;
        if (b._conn !== null) {
          b._conn.close();
          b._conn = null;
        }
      });
      laila.terminate();
      assert.ok(closed.a);
      assert.ok(closed.b);
    }));

  t("25 terminate does not crash with none communication", () => {
    void laila.active_policy;
    laila.active_policy.central.communication = null;
    const errs = laila.terminate();
    assert.deepEqual(laila._local_policies, {});
    assert.ok(!errs.join("").includes("AttributeError"), `unexpected attribute error in errors: ${errs}`);
    assert.ok(!errs.join("").includes("TypeError"), `unexpected type error in errors: ${errs}`);
  });
});

// ---------------------------------------------------------------------------
// test_environment_load.py
// ---------------------------------------------------------------------------
function _seed_default_policy_env() {
  laila.terminate({ wait: true, cancel_pending: true });
  void laila.active_policy;
  const env = { policies: laila.args.environment.policies.toDict() };
  return deepcopy(env);
}

function _first_policy_gid(env) {
  return Object.keys(env.policies)[0];
}

function _setdefault(obj, key, dflt) {
  if (!(key in obj) || obj[key] === null || obj[key] === undefined) obj[key] = dflt;
  return obj[key];
}

function _add_protocol_to_env(env, gid, { host = "127.0.0.1", port = 0, peer_secret_key = "secret" } = {}) {
  const proto_gid = _LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: gid, scopes: ["COMM_PROTOCOL"] });
  const pol_gid = _first_policy_gid(env);
  const conns = _setdefault(_setdefault(_setdefault(env.policies[pol_gid], "central", {}), "communication", {}), "connections", {});
  conns[proto_gid] = { class_token: "_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL", host, port, peer_secret_key };
  return proto_gid;
}

function _add_pool_to_env(env, { class_token, pool_uuid, extra = null, nickname = null }) {
  const pool_gid = _LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: pool_uuid, scopes: ["POOL"] });
  const pol_gid = _first_policy_gid(env);
  const pdata = env.policies[pol_gid];
  const router = _setdefault(_setdefault(_setdefault(pdata, "central", {}), "memory", {}), "pool_router", {});
  const pools = _setdefault(router, "pools", {});
  const pool_entry = { class_token, batch_accelerated: false };
  if (extra) Object.assign(pool_entry, extra);
  pools[pool_gid] = pool_entry;
  if (nickname !== null) _setdefault(router, "pools_nicknames", {})[nickname] = pool_gid;
  return pool_gid;
}

function _add_taskforce_to_env(env, { class_token, tf_uuid, extra = null }) {
  const tf_gid = _LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: tf_uuid, scopes: ["TASK_FORCE"] });
  const pol_gid = _first_policy_gid(env);
  const pdata = env.policies[pol_gid];
  const tfs = _setdefault(_setdefault(_setdefault(pdata, "central", {}), "command", {}), "taskforces", {});
  const tf_entry = { class_token };
  if (extra) Object.assign(tf_entry, extra);
  tfs[tf_gid] = tf_entry;
  return tf_gid;
}

function _make_two_policy_env() {
  laila.terminate({ wait: true, cancel_pending: true });
  const p1 = new defaults.DefaultPolicy();
  const p2 = new defaults.DefaultPolicy();
  laila.activate_policy(p2);
  laila._local_policies[p1.global_id] = p1;
  _refresh_args_environment(p1);
  _refresh_args_environment(p2);
  const env = { policies: laila.args.environment.policies.toDict() };
  return deepcopy(env);
}

const _pad12 = (i) => String(i).padStart(12, "0");

describe("TestEnvironmentLoad", () => {
  const ctx = {};
  runtime_baseline(ctx);
  const assert_no_leaks = (opts) => ctx.assert_no_leaks(opts);

  // ==================== Block A: smoke (1-10) ======================
  t("001 load empty policies creates fresh default", () => {
    assert.equal(Object.keys(laila._local_policies).length, 0);
    laila.args.environment = { policies: {} };
    assert.equal(Object.keys(laila._local_policies).length, 1);
    assert.notEqual(laila.args.get("environment"), null);
    laila.terminate();
    assert_no_leaks();
  });

  t("002 load single policy no subinstances roundtrip", () => {
    const env = _seed_default_policy_env();
    const gid = _first_policy_gid(env);
    laila.args.environment = env;
    assert.equal(laila.active_policy.global_id, gid);
    laila.terminate();
    assert_no_leaks();
  });

  t("003 load returns active gid matching 4 rule single", () => {
    const env = _seed_default_policy_env();
    const gid = _first_policy_gid(env);
    laila.args.environment = env;
    assert.equal(laila._active_policy_gid, gid);
    laila.terminate();
    assert_no_leaks();
  });

  t("004 load with active gid explicit match", () => {
    const env = _make_two_policy_env();
    const chosen = Object.keys(env.policies)[1];
    env.active_gid = chosen;
    laila.args.environment = env;
    assert.equal(laila._active_policy_gid, chosen);
    laila.terminate();
    assert_no_leaks();
  });

  t("005 load with invalid active gid raises", () => {
    const env = _seed_default_policy_env();
    env.active_gid = "LAILA:POLICY:00000000-0000-0000-0000-000000000099";
    assert.throws(() => {
      laila.args.environment = env;
    }, E.ValueError);
    laila.terminate();
    assert_no_leaks();
  });

  t("006 load two policies without active gid raises", () => {
    const env = _make_two_policy_env();
    assert.throws(() => {
      laila.args.environment = env;
    }, E.ValueError);
    laila.terminate();
    assert_no_leaks();
  });

  t("007 load two policies with active gid succeeds", () => {
    const env = _make_two_policy_env();
    const chosen = Object.keys(env.policies)[0];
    env.active_gid = chosen;
    laila.args.environment = env;
    assert.equal(laila._active_policy_gid, chosen);
    assert.equal(Object.keys(laila._local_policies).length, 2);
    laila.terminate();
    assert_no_leaks();
  });

  t("008 load via setitem syntax", () => {
    const env = _seed_default_policy_env();
    const gid = _first_policy_gid(env);
    laila.args.__setitem__("environment", env);
    assert.equal(laila.active_policy.global_id, gid);
    laila.terminate();
    assert_no_leaks();
  });

  t("009 load via setattr syntax", () => {
    const env = _seed_default_policy_env();
    const gid = _first_policy_gid(env);
    laila.args.environment = env;
    assert.equal(laila.active_policy.global_id, gid);
    laila.terminate();
    assert_no_leaks();
  });

  t("010 assigning empty dict does not trigger load", () => {
    void laila.active_policy;
    const gid = laila.active_policy.global_id;
    laila.args.environment = {};
    assert.ok(gid in laila._local_policies);
    laila.terminate();
    assert_no_leaks();
  });

  // ==================== Block B: pool round-trips (11-35) ===========
  function _roundtrip_pool({ class_token, extra = null, nickname = null }) {
    const env = _seed_default_policy_env();
    const pool_uuid = crypto.randomUUID();
    const pool_gid = _add_pool_to_env(env, { class_token, pool_uuid, extra, nickname });
    laila.args.environment = env;
    assert.ok(dict_has(laila.memory.pool_router.pools, pool_gid));
    const loaded = dict_get(laila.memory.pool_router.pools, pool_gid);
    assert.equal(loaded.constructor.name, class_token);
    laila.terminate();
    assert_no_leaks();
  }

  t("011 roundtrip default pool no nickname", () => _roundtrip_pool({ class_token: "_LAILA_IDENTIFIABLE_POOL" }));
  t("012 roundtrip default pool with nickname", () => _roundtrip_pool({ class_token: "_LAILA_IDENTIFIABLE_POOL", nickname: "my-default" }));
  t("013 roundtrip hdf5 pool default path", () => with_tmpdir((td) => _roundtrip_pool({ class_token: "HDF5Pool", extra: { file_path: path.join(td, "p13.h5") } })));
  t("014 roundtrip hdf5 pool custom path", () => with_tmpdir((td) => _roundtrip_pool({ class_token: "HDF5Pool", extra: { file_path: path.join(td, "deep", "nested", "p14.h5") } })));
  t("015 roundtrip sqlite pool on disk", () => with_tmpdir((td) => _roundtrip_pool({ class_token: "SQLitePool", extra: { file_path: path.join(td, "p15.sqlite") } })));
  t("016 roundtrip sqlite pool with nickname", () => with_tmpdir((td) => _roundtrip_pool({ class_token: "SQLitePool", extra: { file_path: path.join(td, "p16.sqlite") }, nickname: "cache" })));
  t("017 roundtrip duckdb pool default path", () => with_tmpdir((td) => _roundtrip_pool({ class_token: "DuckDBPool", extra: { file_path: path.join(td, "p17.duckdb") } })));
  t("018 roundtrip duckdb pool with nickname", () => with_tmpdir((td) => _roundtrip_pool({ class_token: "DuckDBPool", extra: { file_path: path.join(td, "p18.duckdb") }, nickname: "duck-cache" })));
  t("019 roundtrip default pool nickname disk", () => _roundtrip_pool({ class_token: "_LAILA_IDENTIFIABLE_POOL", nickname: "alpha" }));

  t("020 roundtrip two default pools", () => {
    const env = _seed_default_policy_env();
    const gid_a = _add_pool_to_env(env, { class_token: "_LAILA_IDENTIFIABLE_POOL", pool_uuid: "aaaaaaaa-0000-0000-0000-000000000020", nickname: "aa" });
    const gid_b = _add_pool_to_env(env, { class_token: "_LAILA_IDENTIFIABLE_POOL", pool_uuid: "bbbbbbbb-0000-0000-0000-000000000020", nickname: "bb" });
    laila.args.environment = env;
    const pools = laila.memory.pool_router.pools;
    assert.ok(dict_has(pools, gid_a));
    assert.ok(dict_has(pools, gid_b));
    laila.terminate();
    assert_no_leaks();
  });

  test("021 roundtrip redis pool default", { skip: "redis-server backend not exercised in the hermetic JS suite" }, () => {});
  test("022 roundtrip redis pool custom prefix", { skip: "redis-server backend not exercised in the hermetic JS suite" }, () => {});
  test("023 roundtrip postgres pool managed", { skip: "PostgresPool requires server; skipped to keep suite hermetic" }, () => {});
  test("024 roundtrip postgres pool external", { skip: "PostgresPool requires server; skipped to keep suite hermetic" }, () => {});
  test("025 roundtrip mongo pool managed", { skip: "MongoPool requires mongod; skipped to keep suite hermetic" }, () => {});
  test("026 roundtrip mongo pool external", { skip: "MongoPool requires mongod; skipped to keep suite hermetic" }, () => {});

  t("027 roundtrip s3 pool basic", () =>
    _roundtrip_pool({ class_token: "S3Pool", extra: { bucket_name: "fake-bucket", access_key_id: "AKIATEST", secret_access_key: "fakesecret", region_name: "us-east-1" } }));
  t("028 roundtrip s3 pool custom region", () =>
    _roundtrip_pool({ class_token: "S3Pool", extra: { bucket_name: "fake-bucket-2", access_key_id: "AKIATEST", secret_access_key: "fakesecret", region_name: "eu-west-1" } }));
  t("029 roundtrip s3 pool no region", () =>
    _roundtrip_pool({ class_token: "S3Pool", extra: { bucket_name: "fake-bucket-3", access_key_id: "AKIATEST", secret_access_key: "fakesecret" } }));
  t("030 roundtrip backblaze pool", () =>
    _roundtrip_pool({ class_token: "BackblazePool", extra: { bucket_name: "b2-fake", application_key_id: "fake-id", application_key: "fake-key" } }));
  t("031 roundtrip backblaze pool custom endpoint", () =>
    _roundtrip_pool({
      class_token: "BackblazePool",
      extra: { bucket_name: "b2-fake-2", application_key_id: "fake-id", application_key: "fake-key", endpoint_url: "https://s3.eu-central-003.backblazeb2.com" },
    }));
  t("032 roundtrip cloudflare pool", () =>
    _roundtrip_pool({ class_token: "CloudflarePool", extra: { bucket_name: "r2-fake", account_id: "fake-acct", access_key_id: "fake-id", secret_access_key: "fake-key" } }));
  t("033 roundtrip cloudflare pool other acct", () =>
    _roundtrip_pool({ class_token: "CloudflarePool", extra: { bucket_name: "r2-fake-2", account_id: "other-acct", access_key_id: "fake-id", secret_access_key: "fake-key" } }));
  t("034 roundtrip gcs pool", () => _roundtrip_pool({ class_token: "GCSPool", extra: { bucket_name: "gcs-fake", project_id: "fake-proj" } }));
  t("035 roundtrip azure pool", () =>
    _roundtrip_pool({
      class_token: "AzurePool",
      extra: {
        connection_string: "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=ZmFrZQ==;EndpointSuffix=core.windows.net",
        container_name: "fake-container",
      },
    }));

  // ==================== Block C: multi-pool combos (36-45) ==========
  t("036 two default pools in one policy", () => {
    const env = _seed_default_policy_env();
    _add_pool_to_env(env, { class_token: "_LAILA_IDENTIFIABLE_POOL", pool_uuid: "cccccccc-0000-0000-0000-000000000036", nickname: "A" });
    _add_pool_to_env(env, { class_token: "_LAILA_IDENTIFIABLE_POOL", pool_uuid: "dddddddd-0000-0000-0000-000000000036", nickname: "B" });
    laila.args.environment = env;
    assert.equal(len(laila.memory.pool_router.pools), 3);
    laila.terminate();
    assert_no_leaks();
  });

  t("037 three mixed pools", () =>
    with_tmpdir((td) => {
      const env = _seed_default_policy_env();
      _add_pool_to_env(env, { class_token: "_LAILA_IDENTIFIABLE_POOL", pool_uuid: "aaaa0000-0000-0000-0000-000000000037" });
      _add_pool_to_env(env, { class_token: "SQLitePool", pool_uuid: "bbbb0000-0000-0000-0000-000000000037", extra: { file_path: path.join(td, "37.sqlite") } });
      laila.args.environment = env;
      assert.equal(len(laila.memory.pool_router.pools), 3);
      laila.terminate();
      assert_no_leaks();
    }));

  t("038 five default pools", () => {
    const env = _seed_default_policy_env();
    for (let i = 0; i < 5; i++) _add_pool_to_env(env, { class_token: "_LAILA_IDENTIFIABLE_POOL", pool_uuid: `abcd0000-0000-0000-0000-${_pad12(i)}` });
    laila.args.environment = env;
    assert.equal(len(laila.memory.pool_router.pools), 6);
    laila.terminate();
    assert_no_leaks();
  });

  t("039 pool proxy chain not restored", () => {
    const env = _seed_default_policy_env();
    _add_pool_to_env(env, { class_token: "_LAILA_IDENTIFIABLE_POOL", pool_uuid: "aaaa0000-0000-0000-0000-000000000039" });
    laila.args.environment = env;
    for (const pool of values(laila.memory.pool_router.pools)) assert.equal(pool.proxy_to, null);
    laila.terminate();
    assert_no_leaks();
  });

  t("040 class token resolves default pool", () => {
    const env = _seed_default_policy_env();
    const gid = _add_pool_to_env(env, { class_token: "_LAILA_IDENTIFIABLE_POOL", pool_uuid: "aaaa0000-0000-0000-0000-000000000040" });
    laila.args.environment = env;
    const pool = dict_get(laila.memory.pool_router.pools, gid);
    assert.equal(pool.constructor, _LAILA_IDENTIFIABLE_POOL);
    laila.terminate();
    assert_no_leaks();
  });

  t("041 class token resolves sqlite", () =>
    with_tmpdir((td) => {
      const env = _seed_default_policy_env();
      const gid = _add_pool_to_env(env, { class_token: "SQLitePool", pool_uuid: "aaaa0000-0000-0000-0000-000000000041", extra: { file_path: path.join(td, "41.sqlite") } });
      laila.args.environment = env;
      assert.equal(dict_get(laila.memory.pool_router.pools, gid).constructor, SQLitePool);
      laila.terminate();
      assert_no_leaks();
    }));

  t("042 class token resolves duckdb", () =>
    with_tmpdir((td) => {
      const env = _seed_default_policy_env();
      const gid = _add_pool_to_env(env, { class_token: "DuckDBPool", pool_uuid: "aaaa0000-0000-0000-0000-000000000042", extra: { file_path: path.join(td, "42.duckdb") } });
      laila.args.environment = env;
      assert.equal(dict_get(laila.memory.pool_router.pools, gid).constructor, DuckDBPool);
      laila.terminate();
      assert_no_leaks();
    }));

  t("043 class token resolves hdf5", () =>
    with_tmpdir((td) => {
      const env = _seed_default_policy_env();
      const gid = _add_pool_to_env(env, { class_token: "HDF5Pool", pool_uuid: "aaaa0000-0000-0000-0000-000000000043", extra: { file_path: path.join(td, "43.h5") } });
      laila.args.environment = env;
      assert.equal(dict_get(laila.memory.pool_router.pools, gid).constructor, HDF5Pool);
      laila.terminate();
      assert_no_leaks();
    }));

  t("044 pool nicknames roundtrip", () => {
    const env = _seed_default_policy_env();
    const gid = _add_pool_to_env(env, { class_token: "_LAILA_IDENTIFIABLE_POOL", pool_uuid: "aaaa0000-0000-0000-0000-000000000044", nickname: "cache-44" });
    laila.args.environment = env;
    const nicks = laila.memory.pool_router.pools_nicknames;
    assert.ok(dict_has(nicks, "cache-44"));
    assert.equal(dict_get(nicks, "cache-44"), gid);
    laila.terminate();
    assert_no_leaks();
  });

  t("045 pool with batch accelerated field", () => {
    const env = _seed_default_policy_env();
    const gid = _add_pool_to_env(env, { class_token: "_LAILA_IDENTIFIABLE_POOL", pool_uuid: "aaaa0000-0000-0000-0000-000000000045", extra: { batch_accelerated: true } });
    laila.args.environment = env;
    assert.equal(dict_get(laila.memory.pool_router.pools, gid).batch_accelerated, true);
    laila.terminate();
    assert_no_leaks();
  });

  // ==================== Block D: taskforces (46-55) =================
  t("046 threadpool default num workers", () => {
    const env = _seed_default_policy_env();
    const gid = _add_taskforce_to_env(env, { class_token: "PythonAsyncThreadPoolTaskForce", tf_uuid: "aaaa0000-0000-0000-0000-000000000046" });
    laila.args.environment = env;
    assert.equal(dict_get(laila.command.taskforces, gid).constructor, PythonAsyncThreadPoolTaskForce);
    laila.terminate();
    assert_no_leaks();
  });

  t("047 threadpool num workers 4", () => {
    const env = _seed_default_policy_env();
    const gid = _add_taskforce_to_env(env, { class_token: "PythonAsyncThreadPoolTaskForce", tf_uuid: "aaaa0000-0000-0000-0000-000000000047", extra: { num_workers: 4 } });
    laila.args.environment = env;
    assert.equal(dict_get(laila.command.taskforces, gid).num_workers, 4);
    laila.terminate();
    assert_no_leaks();
  });

  t("048 threadpool num workers 16", () => {
    const env = _seed_default_policy_env();
    const gid = _add_taskforce_to_env(env, { class_token: "PythonAsyncThreadPoolTaskForce", tf_uuid: "aaaa0000-0000-0000-0000-000000000048", extra: { num_workers: 16 } });
    laila.args.environment = env;
    assert.equal(dict_get(laila.command.taskforces, gid).num_workers, 16);
    laila.terminate();
    assert_no_leaks();
  });

  t("049 processpool default", () => {
    const env = _seed_default_policy_env();
    const gid = _add_taskforce_to_env(env, { class_token: "PythonProcessPoolTaskForce", tf_uuid: "aaaa0000-0000-0000-0000-000000000049" });
    laila.args.environment = env;
    assert.equal(dict_get(laila.command.taskforces, gid).constructor, PythonProcessPoolTaskForce);
    laila.terminate();
    assert_no_leaks();
  });

  t("050 processpool num workers 4", () => {
    const env = _seed_default_policy_env();
    const gid = _add_taskforce_to_env(env, { class_token: "PythonProcessPoolTaskForce", tf_uuid: "aaaa0000-0000-0000-0000-000000000050", extra: { num_workers: 4 } });
    laila.args.environment = env;
    assert.equal(dict_get(laila.command.taskforces, gid).num_workers, 4);
    laila.terminate();
    assert_no_leaks();
  });

  t("051 two taskforces in one policy", () => {
    const env = _seed_default_policy_env();
    const gid_a = _add_taskforce_to_env(env, { class_token: "PythonAsyncThreadPoolTaskForce", tf_uuid: "aaaa0000-0000-0000-0000-000000000051" });
    const gid_b = _add_taskforce_to_env(env, { class_token: "PythonAsyncThreadPoolTaskForce", tf_uuid: "bbbb0000-0000-0000-0000-000000000051" });
    laila.args.environment = env;
    assert.ok(dict_has(laila.command.taskforces, gid_a));
    assert.ok(dict_has(laila.command.taskforces, gid_b));
    laila.terminate();
    assert_no_leaks();
  });

  t("052 alpha taskforce explicit", () => {
    const env = _seed_default_policy_env();
    _add_taskforce_to_env(env, { class_token: "PythonAsyncThreadPoolTaskForce", tf_uuid: "aaaa0000-0000-0000-0000-000000000052" });
    const gid_b = _add_taskforce_to_env(env, { class_token: "PythonAsyncThreadPoolTaskForce", tf_uuid: "bbbb0000-0000-0000-0000-000000000052" });
    const pol_gid = _first_policy_gid(env);
    env.policies[pol_gid].central.command.alpha_taskforce = gid_b;
    laila.args.environment = env;
    assert.equal(laila.command.alpha_taskforce, gid_b);
    laila.terminate();
    assert_no_leaks();
  });

  t("053 processpool replaces threadpool", () => {
    const env = _seed_default_policy_env();
    _add_taskforce_to_env(env, { class_token: "PythonProcessPoolTaskForce", tf_uuid: "aaaa0000-0000-0000-0000-000000000053", extra: { num_workers: 4 } });
    laila.args.environment = env;
    assert.ok(values(laila.command.taskforces).some((tf) => tf instanceof PythonProcessPoolTaskForce));
    laila.terminate();
    assert_no_leaks();
  });

  t("054 thread to process transition", () => {
    const env_a = _seed_default_policy_env();
    _add_taskforce_to_env(env_a, { class_token: "PythonAsyncThreadPoolTaskForce", tf_uuid: "aaaa0000-0000-0000-0000-000000000054" });
    laila.args.environment = env_a;

    const env_b = _seed_default_policy_env();
    _add_taskforce_to_env(env_b, { class_token: "PythonProcessPoolTaskForce", tf_uuid: "bbbb0000-0000-0000-0000-000000000054", extra: { num_workers: 4 } });
    laila.args.environment = env_b;
    laila.terminate();
    assert_no_leaks();
  });

  t("055 process to thread transition", () => {
    const env_a = _seed_default_policy_env();
    _add_taskforce_to_env(env_a, { class_token: "PythonProcessPoolTaskForce", tf_uuid: "aaaa0000-0000-0000-0000-000000000055", extra: { num_workers: 4 } });
    laila.args.environment = env_a;

    const env_b = _seed_default_policy_env();
    _add_taskforce_to_env(env_b, { class_token: "PythonAsyncThreadPoolTaskForce", tf_uuid: "bbbb0000-0000-0000-0000-000000000055" });
    laila.args.environment = env_b;
    laila.terminate();
    assert_no_leaks();
  });

  // ==================== Block E: protocols (56-65) ==================
  t("056 one tcp protocol loaded not started", () => {
    const env = _seed_default_policy_env();
    const gid = _add_protocol_to_env(env, "aaaa0000-0000-0000-0000-000000000056", { host: "127.0.0.1", port: 0 });
    laila.args.environment = env;
    const proto = dict_get(laila.communication.connections, gid);
    assert.equal(proto._started, false);
    laila.terminate();
    assert_no_leaks();
  });

  t("057 tcp protocol custom secret", () => {
    const env = _seed_default_policy_env();
    const gid = _add_protocol_to_env(env, "aaaa0000-0000-0000-0000-000000000057", { peer_secret_key: "my-secret" });
    laila.args.environment = env;
    assert.equal(dict_get(laila.communication.connections, gid).peer_secret_key, "my-secret");
    laila.terminate();
    assert_no_leaks();
  });

  t("058 two tcp protocols different ports", () => {
    const env = _seed_default_policy_env();
    const gid_a = _add_protocol_to_env(env, "aaaa0000-0000-0000-0000-000000000058");
    const gid_b = _add_protocol_to_env(env, "bbbb0000-0000-0000-0000-000000000058");
    laila.args.environment = env;
    assert.ok(dict_has(laila.communication.connections, gid_a));
    assert.ok(dict_has(laila.communication.connections, gid_b));
    laila.terminate();
    assert_no_leaks();
  });

  t("059 three tcp protocols", () => {
    const env = _seed_default_policy_env();
    for (let i = 0; i < 3; i++) _add_protocol_to_env(env, `aaaa0000-0000-0000-0000-${_pad12(i)}`);
    laila.args.environment = env;
    assert.equal(len(laila.communication.connections), 3);
    laila.terminate();
    assert_no_leaks();
  });

  t("060 tcp protocol loopback port zero", () => {
    const env = _seed_default_policy_env();
    _add_protocol_to_env(env, "aaaa0000-0000-0000-0000-000000000060", { host: "127.0.0.1", port: 0 });
    laila.args.environment = env;
    laila.terminate();
    assert_no_leaks();
  });

  t("061 tcp protocol explicit start releases port", () => {
    const env = _seed_default_policy_env();
    const port = _bind_ephemeral();
    const gid = _add_protocol_to_env(env, "aaaa0000-0000-0000-0000-000000000061", { host: "127.0.0.1", port });
    laila.args.environment = env;
    const proto = dict_get(laila.communication.connections, gid);
    proto.start();
    assert.ok(_wait_until(() => _listening_ports().includes(port), 3.0));
    laila.terminate();
    assert.ok(_wait_until(() => !_listening_ports().includes(port), 5.0));
    assert_no_leaks();
  });

  t("062 tcp protocol default secret is random", () => {
    const env = _seed_default_policy_env();
    const gid = _add_protocol_to_env(env, "aaaa0000-0000-0000-0000-000000000062", { peer_secret_key: "initial-secret" });
    laila.args.environment = env;
    assert.equal(dict_get(laila.communication.connections, gid).peer_secret_key, "initial-secret");
    laila.terminate();
    assert_no_leaks();
  });

  t("063 tcp protocol host specified", () => {
    const env = _seed_default_policy_env();
    const gid = _add_protocol_to_env(env, "aaaa0000-0000-0000-0000-000000000063", { host: "0.0.0.0" });
    laila.args.environment = env;
    assert.equal(dict_get(laila.communication.connections, gid).host, "0.0.0.0");
    laila.terminate();
    assert_no_leaks();
  });

  t("064 tcp protocol port reuse after reload", () => {
    const port = _bind_ephemeral();

    const env_a = _seed_default_policy_env();
    const gid_a = _add_protocol_to_env(env_a, "aaaa0000-0000-0000-0000-000000000064", { host: "127.0.0.1", port });
    laila.args.environment = env_a;
    dict_get(laila.communication.connections, gid_a).start();
    assert.ok(_wait_until(() => _listening_ports().includes(port), 3.0));

    const env_b = _seed_default_policy_env();
    const gid_b = _add_protocol_to_env(env_b, "bbbb0000-0000-0000-0000-000000000064", { host: "127.0.0.1", port });
    laila.args.environment = env_b;
    dict_get(laila.communication.connections, gid_b).start();
    assert.ok(_wait_until(() => _listening_ports().includes(port), 5.0));
    laila.terminate();
    assert_no_leaks();
  });

  t("065 tcp protocol class token round trip", () => {
    const env = _seed_default_policy_env();
    const gid = _add_protocol_to_env(env, "aaaa0000-0000-0000-0000-000000000065");
    laila.args.environment = env;
    assert.equal(dict_get(laila.communication.connections, gid).constructor, _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL);
    laila.terminate();
    assert_no_leaks();
  });

  // ==================== Block F: sequential reloads (66-75) =========
  t("066 load same env twice", () => {
    const env = _seed_default_policy_env();
    const gid = _first_policy_gid(env);
    laila.args.environment = env;
    laila.args.environment = env;
    assert.equal(laila._active_policy_gid, gid);
    laila.terminate();
    assert_no_leaks();
  });

  t("067 load a then b drops a", () => {
    const env_a = _seed_default_policy_env();
    const gid_a = _first_policy_gid(env_a);
    laila.args.environment = env_a;
    const env_b = _seed_default_policy_env();
    const gid_b = _first_policy_gid(env_b);
    laila.args.environment = env_b;
    assert.ok(!(gid_a in laila._local_policies));
    assert.ok(gid_b in laila._local_policies);
    laila.terminate();
    assert_no_leaks();
  });

  t("068 load a terminate load b", () => {
    const env_a = _seed_default_policy_env();
    laila.args.environment = env_a;
    laila.terminate();
    const env_b = _seed_default_policy_env();
    const gid_b = _first_policy_gid(env_b);
    laila.args.environment = env_b;
    assert.equal(laila._active_policy_gid, gid_b);
    laila.terminate();
    assert_no_leaks();
  });

  t("069 five sequential same reloads", () => {
    const env = _seed_default_policy_env();
    for (let i = 0; i < 5; i++) laila.args.environment = env;
    laila.terminate();
    assert_no_leaks();
  });

  t("070 reload add pool", () => {
    const env = _seed_default_policy_env();
    laila.args.environment = env;
    const env2 = _seed_default_policy_env();
    _add_pool_to_env(env2, { class_token: "_LAILA_IDENTIFIABLE_POOL", pool_uuid: "aaaa0000-0000-0000-0000-000000000070" });
    laila.args.environment = env2;
    laila.terminate();
    assert_no_leaks();
  });

  t("071 reload drop pool", () => {
    const env = _seed_default_policy_env();
    _add_pool_to_env(env, { class_token: "_LAILA_IDENTIFIABLE_POOL", pool_uuid: "aaaa0000-0000-0000-0000-000000000071" });
    laila.args.environment = env;
    const env2 = _seed_default_policy_env();
    laila.args.environment = env2;
    laila.terminate();
    assert_no_leaks();
  });

  t("072 reload swap taskforce", () => {
    const env_a = _seed_default_policy_env();
    _add_taskforce_to_env(env_a, { class_token: "PythonAsyncThreadPoolTaskForce", tf_uuid: "aaaa0000-0000-0000-0000-000000000072" });
    laila.args.environment = env_a;
    const env_b = _seed_default_policy_env();
    _add_taskforce_to_env(env_b, { class_token: "PythonAsyncThreadPoolTaskForce", tf_uuid: "bbbb0000-0000-0000-0000-000000000072" });
    laila.args.environment = env_b;
    laila.terminate();
    assert_no_leaks();
  });

  t("073 reload swap protocol", () => {
    const env_a = _seed_default_policy_env();
    _add_protocol_to_env(env_a, "aaaa0000-0000-0000-0000-000000000073");
    laila.args.environment = env_a;
    const env_b = _seed_default_policy_env();
    _add_protocol_to_env(env_b, "bbbb0000-0000-0000-0000-000000000073");
    laila.args.environment = env_b;
    laila.terminate();
    assert_no_leaks();
  });

  t("074 reload change nickname", () => {
    const env_a = _seed_default_policy_env();
    _add_pool_to_env(env_a, { class_token: "_LAILA_IDENTIFIABLE_POOL", pool_uuid: "aaaa0000-0000-0000-0000-000000000074", nickname: "A" });
    laila.args.environment = env_a;
    const env_b = _seed_default_policy_env();
    _add_pool_to_env(env_b, { class_token: "_LAILA_IDENTIFIABLE_POOL", pool_uuid: "aaaa0000-0000-0000-0000-000000000074", nickname: "B" });
    laila.args.environment = env_b;
    assert.ok(dict_has(laila.memory.pool_router.pools_nicknames, "B"));
    laila.terminate();
    assert_no_leaks();
  });

  t("075 reload change active gid", () => {
    const env_a = _make_two_policy_env();
    env_a.active_gid = Object.keys(env_a.policies)[0];
    laila.args.environment = env_a;

    const env_b = _make_two_policy_env();
    env_b.active_gid = Object.keys(env_b.policies)[1];
    laila.args.environment = env_b;
    assert.equal(laila._active_policy_gid, env_b.active_gid);
    laila.terminate();
    assert_no_leaks();
  });

  // ==================== Block G: error injection (76-90) ============
  t("076 no policies key raises", () => {
    assert.throws(() => {
      laila.args.environment = { policies_misnamed: { x: {} } };
    }, E.ValueError);
    laila.terminate();
    assert_no_leaks();
  });

  t("077 policy value not a dict raises", () => {
    const env = _seed_default_policy_env();
    env.policies[_first_policy_gid(env)] = "not-a-dict";
    assert.throws(() => {
      laila.args.environment = env;
    }, E.ValueError);
    laila.terminate();
    assert_no_leaks();
  });

  t("078 pool missing class token raises", () => {
    const env = _seed_default_policy_env();
    const pol_gid = _first_policy_gid(env);
    const bad_gid = _LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: "ffff0000-0000-0000-0000-000000000078", scopes: ["POOL"] });
    const pools = _setdefault(_setdefault(_setdefault(_setdefault(env.policies[pol_gid], "central", {}), "memory", {}), "pool_router", {}), "pools", {});
    pools[bad_gid] = { batch_accelerated: false };
    assert.throws(() => {
      laila.args.environment = env;
    }, E.ValueError);
    laila.terminate();
    assert_no_leaks();
  });

  t("079 unknown class token raises with known list", () => {
    const env = _seed_default_policy_env();
    _add_pool_to_env(env, { class_token: "DoesNotExistPool", pool_uuid: "ffff0000-0000-0000-0000-000000000079" });
    assert.throws(
      () => {
        laila.args.environment = env;
      },
      (e) => e instanceof E.ValueError && String(e.message).includes("DoesNotExistPool"),
    );
    laila.terminate();
    assert_no_leaks();
  });

  t("080 invalid policy gid raises", () => {
    const env = _seed_default_policy_env();
    const gid = _first_policy_gid(env);
    env.policies["not-a-valid-gid"] = env.policies[gid];
    delete env.policies[gid];
    assert.throws(() => {
      laila.args.environment = env;
    }, E.ValueError);
    laila.terminate();
    assert_no_leaks();
  });

  t("081 active gid with empty policies raises", () => {
    assert.throws(() => {
      laila.args.environment = { policies: {}, active_gid: "LAILA:POLICY:00000000-0000-0000-0000-000000000081" };
    }, E.ValueError);
    laila.terminate();
    assert_no_leaks();
  });

  t("082 pool kwargs missing required field raises", () => {
    const env = _seed_default_policy_env();
    _add_pool_to_env(env, { class_token: "S3Pool", pool_uuid: "ffff0000-0000-0000-0000-000000000082", extra: {} });
    assert.throws(() => {
      laila.args.environment = env;
    });
    assert.deepEqual(laila._local_policies, {});
    laila.terminate();
    assert_no_leaks();
  });

  t("083 pool kwargs wrong type raises", () => {
    const env = _seed_default_policy_env();
    _add_pool_to_env(env, { class_token: "_LAILA_IDENTIFIABLE_POOL", pool_uuid: "ffff0000-0000-0000-0000-000000000083", extra: { batch_accelerated: "not-a-bool" } });
    try {
      laila.args.environment = env;
    } catch {
      /* either outcome acceptable, as in Python */
    }
    laila.terminate();
    assert_no_leaks();
  });

  t("084 cloudflare missing account id raises", () => {
    const env = _seed_default_policy_env();
    _add_pool_to_env(env, { class_token: "CloudflarePool", pool_uuid: "ffff0000-0000-0000-0000-000000000084", extra: { bucket_name: "x", access_key_id: "x", secret_access_key: "x" } });
    assert.throws(() => {
      laila.args.environment = env;
    });
    laila.terminate();
    assert_no_leaks();
  });

  t("085 taskforce missing class token raises", () => {
    const env = _seed_default_policy_env();
    const bad_gid = _LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: "ffff0000-0000-0000-0000-000000000085", scopes: ["TASK_FORCE"] });
    const pol_gid = _first_policy_gid(env);
    const tfs = _setdefault(_setdefault(_setdefault(env.policies[pol_gid], "central", {}), "command", {}), "taskforces", {});
    tfs[bad_gid] = { backend: "threads" };
    assert.throws(() => {
      laila.args.environment = env;
    }, E.ValueError);
    laila.terminate();
    assert_no_leaks();
  });

  t("086 terminate failure does not corrupt state", () => {
    void laila.active_policy;
    const env = _seed_default_policy_env();
    try {
      laila.args.environment = env;
    } catch {
      /* ignore */
    }
    assert.equal(Object.keys(laila._local_policies).length, 1);
    laila.terminate();
    assert_no_leaks();
  });

  t("087 pool constructor failure rolls back", () => {
    const env = _seed_default_policy_env();
    _add_pool_to_env(env, { class_token: "UnknownClassToken", pool_uuid: "ffff0000-0000-0000-0000-000000000087" });
    assert.throws(() => {
      laila.args.environment = env;
    }, E.ValueError);
    assert.deepEqual(laila._local_policies, {});
    laila.terminate();
    assert_no_leaks();
  });

  t("088 nested dict input is accepted", () => {
    const env = _seed_default_policy_env();
    laila.args.environment = new DotMap(env);
    laila.terminate();
    assert_no_leaks();
  });

  t("089 duplicate policy uuid raises", () => {
    const env = _seed_default_policy_env();
    const gid = _first_policy_gid(env);
    env.policies[gid + "@evolution=1"] = env.policies[gid];
    assert.throws(() => {
      laila.args.environment = env;
    }, E.ValueError);
    laila.terminate();
    assert_no_leaks();
  });

  t("090 duplicate protocol gid across policies raises", () => {
    const env = _make_two_policy_env();
    env.active_gid = Object.keys(env.policies)[0];
    const proto_gid = _LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: "cccc0000-0000-0000-0000-000000000090", scopes: ["COMM_PROTOCOL"] });
    for (const pol_gid of Object.keys(env.policies)) {
      const conns = _setdefault(_setdefault(_setdefault(env.policies[pol_gid], "central", {}), "communication", {}), "connections", {});
      conns[proto_gid] = { class_token: "_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL", host: "127.0.0.1", port: 0, peer_secret_key: "x" };
    }
    assert.throws(() => {
      laila.args.environment = env;
    }, E.ValueError);
    laila.terminate();
    assert_no_leaks();
  });

  // ==================== Block H: resource realism (91-100) ==========
  t("091 ten sequential envs leave no ports or children behind", () => {
    // Python asserts the thread count returns to baseline; the Node port is
    // single-threaded, so the equivalent observable is the handle baseline.
    for (let i = 0; i < 10; i++) laila.args.environment = _seed_default_policy_env();
    laila.terminate();
    assert_no_leaks();
  });

  t("092 fd count returns to baseline", () => {
    const baseline_fds = _open_fd_count();
    with_tmpdir((td) => {
      const env = _seed_default_policy_env();
      _add_pool_to_env(env, { class_token: "SQLitePool", pool_uuid: "aaaa0000-0000-0000-0000-000000000092", extra: { file_path: path.join(td, "92.sqlite") } });
      laila.args.environment = env;
      laila.terminate();
    });
    assert.ok(_wait_until(() => Math.abs((_open_fd_count() ?? 0) - (baseline_fds ?? 0)) <= 4, 5.0));
    assert_no_leaks();
  });

  t("093 open files after load then terminate match baseline", () => {
    with_tmpdir((td) => {
      const env = _seed_default_policy_env();
      _add_pool_to_env(env, { class_token: "SQLitePool", pool_uuid: "aaaa0000-0000-0000-0000-000000000093", extra: { file_path: path.join(td, "93.sqlite") } });
      laila.args.environment = env;
      laila.terminate();
    });
    assert_no_leaks();
  });

  t("094 listening ports returns to baseline", () => {
    const baseline_ports = _listening_ports();
    const env = _seed_default_policy_env();
    const gid = _add_protocol_to_env(env, "aaaa0000-0000-0000-0000-000000000094", { host: "127.0.0.1", port: 0 });
    laila.args.environment = env;
    dict_get(laila.communication.connections, gid).start();
    laila.terminate();
    assert.ok(_wait_until(() => JSON.stringify(_listening_ports()) === JSON.stringify(baseline_ports), 5.0));
    assert_no_leaks();
  });

  t("095 processpool workers fully exit", () => {
    const env = _seed_default_policy_env();
    _add_taskforce_to_env(env, { class_token: "PythonProcessPoolTaskForce", tf_uuid: "aaaa0000-0000-0000-0000-000000000095", extra: { num_workers: 4 } });
    laila.args.environment = env;
    laila.terminate();
    assert_no_leaks({ timeout: 10.0 });
  });

  test("096 redis subprocess fully exits", { skip: "redis-server backend not exercised in the hermetic JS suite" }, () => {});
  test("097 postgres subprocess fully exits", { skip: "Postgres tier-c is gated on server binary; skip in hermetic CI" }, () => {});
  test("098 mongo subprocess fully exits", { skip: "Mongo tier-c is gated on server binary; skip in hermetic CI" }, () => {});

  t("099 hdf5 file unlocked after reload", () =>
    with_tmpdir((td) => {
      const env = _seed_default_policy_env();
      _add_pool_to_env(env, { class_token: "HDF5Pool", pool_uuid: "aaaa0000-0000-0000-0000-000000000099", extra: { file_path: path.join(td, "99.h5") } });
      laila.args.environment = env;
      const env2 = _seed_default_policy_env();
      _add_pool_to_env(env2, { class_token: "HDF5Pool", pool_uuid: "bbbb0000-0000-0000-0000-000000000099", extra: { file_path: path.join(td, "99.h5") } });
      laila.args.environment = env2;
      laila.terminate();
      const again = new HDF5Pool({ file_path: path.join(td, "99.h5") });
      again.close();
      assert_no_leaks();
    }));

  t("100 sqlite file unlocked after reload", () =>
    with_tmpdir((td) => {
      const env = _seed_default_policy_env();
      _add_pool_to_env(env, { class_token: "SQLitePool", pool_uuid: "aaaa0000-0000-0000-0000-000000000100", extra: { file_path: path.join(td, "100.sqlite") } });
      laila.args.environment = env;
      const env2 = _seed_default_policy_env();
      _add_pool_to_env(env2, { class_token: "SQLitePool", pool_uuid: "bbbb0000-0000-0000-0000-000000000100", extra: { file_path: path.join(td, "100.sqlite") } });
      laila.args.environment = env2;
      laila.terminate();
      const again = new SQLitePool({ file_path: path.join(td, "100.sqlite") });
      again.close();
      assert_no_leaks();
    }));
});
