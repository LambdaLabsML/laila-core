/**
 * Utils: ports of
 *   tests/functional/utils/args/unit_tests/test_arg_reader.py
 *   tests/functional/utils/decorators/unit_tests/test_decorators.py
 *   tests/functional/utils/unit_tests/test_guarantee_multiprocess.py
 *
 * The two child *processes* of ``test_guarantee_multiprocess.py`` are Node
 * children running ``fixtures/guarantee_peer.js`` (the ``_child_main`` of the
 * Python module); the parent policy peers with both over the WebSocket
 * (``tcpip``) transport exactly like the Python ``setUpModule``.
 *
 * Node mapping of the coroutine bodies: a sync RPC (``proxy.fn(...)``) blocks
 * by pumping the loop, which is impossible after an ``await`` (microtask), so
 * inside ``async`` bodies the proxy is called through its awaitable form
 * ``await proxy.fn.call_async(...)`` -- same RPC, same ``RemoteFuture``.
 */
import { test, describe, before, after, beforeEach } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import readline from "node:readline";
import { spawn } from "node:child_process";

const { default: laila, S } = await import("./fixtures/laila_root.js");

const E = await import(S + "_compat/errors.js");
const T = await import(S + "_compat/pytypes.js");
const asyncio = await import(S + "_compat/asyncio.js");
const time = await import(S + "_compat/time.js");
const { with_, with_async } = await import(S + "_compat/contextlib.js");
const { repr } = await import(S + "_compat/pyrepr.js");
const { dict_get, dict_has, isdict } = T;
const { DefaultPolicy, DefaultTCPIPProtocol } = await import(S + "macros/defaults.js");
const { ArgReader } = await import(S + "utils/args/index.js");
const { synchronized } = await import(S + "utils/decorators/synchronized.js");
const { ensure_list } = await import(S + "utils/decorators/typecheck.js");
const { _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT } = await import(S + "atomic/definitions/locally_atomic_identifiable_object.js");
const { RemoteFuture } = await import(S + "policy/central/command/schema/future/future/remote_future.js");
const { _LAILA_IDENTIFIABLE_FUTURE } = await import(S + "policy/central/command/schema/future/future/future_identity.js");

/**
 * Run a synchronous body on a fresh macrotask: ``node:test`` invokes test
 * bodies from a microtask, where a blocking wait is impossible by
 * construction (nothing can settle until the job returns).
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
const t = (name, fn, opts) => (opts ? test(name, opts, () => macrotask(fn)) : test(name, () => macrotask(fn)));

// ---------------------------------------------------------------------------
// test_arg_reader.py
// ---------------------------------------------------------------------------
describe("TestArgReader", () => {
  let reader;
  beforeEach(() => {
    reader = new ArgReader(laila.args);
    reader.clear();
  });

  /** ``tempfile.mkstemp(suffix)`` + write *content*. */
  const _tmp_file = (suffix, content) => {
    const p = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "laila-args-")), `args${suffix}`);
    fs.writeFileSync(p, content, "utf8");
    return p;
  };
  const _remove = (p) => fs.rmSync(path.dirname(p), { recursive: true, force: true });

  // ── basic loaders ───────────────────────────────────────

  test("test_load_json_flat_and_one_level_nested", () => {
    const p = _tmp_file(".json", '{"s3_bucket_region":"us-east-1","s3":{"bucket_name":"laila-test-1"}}');
    try {
      assert.equal(reader.load(p), undefined);
      assert.equal(laila.args.s3_bucket_region, "us-east-1");
      assert.equal(laila.args.s3_bucket_name, "laila-test-1");
    } finally {
      _remove(p);
    }
  });

  test("test_load_toml", () => {
    const p = _tmp_file(".toml", 's3_bucket_region = "us-west-2"\n[cloudflare]\naccount_id = "abc123"\n');
    try {
      assert.equal(reader.load(p), undefined);
      assert.equal(laila.args.s3_bucket_region, "us-west-2");
      assert.equal(laila.args.cloudflare_account_id, "abc123");
    } finally {
      _remove(p);
    }
  });

  test("test_load_env", () => {
    const p = _tmp_file(".env", "S3_BUCKET_REGION=eu-central-1\nRETRIES=3\nUSE_SSL=true\n");
    try {
      assert.equal(reader.load(p), undefined);
      assert.equal(laila.args.S3_BUCKET_REGION, "eu-central-1");
      assert.equal(laila.args.RETRIES, 3);
      assert.equal(laila.args.USE_SSL, true);
    } finally {
      _remove(p);
    }
  });

  test("test_load_xml", () => {
    const p = _tmp_file(".xml", "<args><s3_bucket_region>ap-south-1</s3_bucket_region><s3><bucket_name>laila-test-2</bucket_name></s3></args>");
    try {
      assert.equal(reader.load(p), undefined);
      assert.equal(laila.args.s3_bucket_region, "ap-south-1");
      assert.equal(laila.args.s3_bucket_name, "laila-test-2");
    } finally {
      _remove(p);
    }
  });

  test("test_load_terminal", () => {
    assert.equal(reader.load("terminal", { terminal_args: ["s3_bucket_region=us-east-2", "max_retries=5", "debug=false"] }), undefined);
    assert.equal(laila.args.s3_bucket_region, "us-east-2");
    assert.equal(laila.args.max_retries, 5);
    assert.equal(laila.args.debug, false);
  });

  test("test_read_args_returns_none", () => {
    const p = _tmp_file(".json", '{"foo":"bar"}');
    try {
      assert.equal(laila.read_args(p), undefined);
      assert.equal(laila.args.foo, "bar");
    } finally {
      _remove(p);
    }
  });

  // ── edge cases: file not found / malformed ──────────────

  test("test_load_missing_file_raises", () => {
    assert.throws(() => reader.load("/nonexistent/path/file.json"), E.FileNotFoundError);
  });

  test("test_load_empty_json_raises", () => {
    const p = _tmp_file(".json", "");
    try {
      assert.throws(() => reader.load(p));
    } finally {
      _remove(p);
    }
  });

  test("test_load_malformed_json_raises", () => {
    const p = _tmp_file(".json", "{bad json");
    try {
      assert.throws(() => reader.load(p));
    } finally {
      _remove(p);
    }
  });

  test("test_load_json_array_raises", () => {
    const p = _tmp_file(".json", "[1, 2, 3]");
    try {
      assert.throws(() => reader.load(p), E.ValueError);
    } finally {
      _remove(p);
    }
  });

  test("test_load_malformed_toml_raises", () => {
    const p = _tmp_file(".toml", "[[[invalid");
    try {
      assert.throws(() => reader.load(p));
    } finally {
      _remove(p);
    }
  });

  test("test_unsupported_suffix_raises", () => {
    const p = _tmp_file(".yaml", "key: value");
    try {
      assert.throws(() => reader.load(p), E.ValueError);
    } finally {
      _remove(p);
    }
  });

  test("test_load_no_suffix_raises", () => {
    const p = _tmp_file("", "");
    try {
      assert.throws(() => reader.load(p), E.ValueError);
    } finally {
      _remove(p);
    }
  });

  // ── coercion ────────────────────────────────────────────

  test("test_coerce_true_false", () => {
    const p = _tmp_file(".env", "A=true\nB=false\nC=TRUE\nD=False\n");
    try {
      reader.load(p);
      assert.equal(laila.args.A, true);
      assert.equal(laila.args.B, false);
      assert.equal(laila.args.C, true);
      assert.equal(laila.args.D, false);
    } finally {
      _remove(p);
    }
  });

  test("test_coerce_none_null", () => {
    const p = _tmp_file(".env", "A=none\nB=null\nC=None\n");
    try {
      reader.load(p);
      assert.equal(laila.args.A, null);
      assert.equal(laila.args.B, null);
      assert.equal(laila.args.C, null);
    } finally {
      _remove(p);
    }
  });

  test("test_coerce_int_and_float", () => {
    const p = _tmp_file(".env", "I=42\nF=3.14\n");
    try {
      reader.load(p);
      assert.equal(laila.args.I, 42);
      assert.ok(T.is_int(laila.args.I));
      assert.ok(Math.abs(laila.args.F - 3.14) < 1e-7);
      assert.ok(T.is_float(laila.args.F));
    } finally {
      _remove(p);
    }
  });

  test("test_coerce_json_object_in_value_gets_flattened", () => {
    const p = _tmp_file(".env", 'DATA={"a": 1}\n');
    try {
      reader.load(p);
      assert.equal(laila.args.DATA_a, 1);
    } finally {
      _remove(p);
    }
  });

  test("test_coerce_json_array_in_value", () => {
    const p = _tmp_file(".env", "DATA=[1,2,3]\n");
    try {
      reader.load(p);
      assert.deepEqual(laila.args.DATA, [1, 2, 3]);
    } finally {
      _remove(p);
    }
  });

  test("test_coerce_quoted_string", () => {
    const p = _tmp_file(".env", 'NAME="hello world"\n');
    try {
      reader.load(p);
      assert.equal(laila.args.NAME, "hello world");
    } finally {
      _remove(p);
    }
  });

  // ── env file edge cases ─────────────────────────────────

  test("test_env_blank_lines_and_comments_skipped", () => {
    const p = _tmp_file(".env", "\n# comment\n\nKEY=val\n");
    try {
      reader.load(p);
      assert.equal(laila.args.KEY, "val");
    } finally {
      _remove(p);
    }
  });

  test("test_env_line_without_equals_skipped", () => {
    const p = _tmp_file(".env", "no-equals\nKEY=val\n");
    try {
      reader.load(p);
      assert.equal(laila.args.KEY, "val");
    } finally {
      _remove(p);
    }
  });

  test("test_env_value_with_equals", () => {
    const p = _tmp_file(".env", "URL=http://host?a=1&b=2\n");
    try {
      reader.load(p);
      assert.equal(laila.args.URL, "http://host?a=1&b=2");
    } finally {
      _remove(p);
    }
  });

  // ── clear ───────────────────────────────────────────────

  test("test_clear_removes_loaded_args", () => {
    const p = _tmp_file(".json", '{"x": 1, "y": 2}');
    try {
      reader.load(p);
      assert.equal(laila.args.x, 1);
      reader.clear();
      assert.equal("x" in laila.args && laila.args.x === 1, false);
    } finally {
      _remove(p);
    }
  });

  test("test_clear_on_empty_args_is_safe", () => {
    reader.clear();
  });

  // ── multiple loads overwrite ────────────────────────────

  test("test_second_load_overwrites_first", () => {
    const p1 = _tmp_file(".json", '{"k": "first"}');
    const p2 = _tmp_file(".json", '{"k": "second"}');
    try {
      reader.load(p1);
      assert.equal(laila.args.k, "first");
      reader.load(p2);
      assert.equal(laila.args.k, "second");
    } finally {
      _remove(p1);
      _remove(p2);
    }
  });

  // ── terminal edge cases ─────────────────────────────────

  test("test_terminal_empty_args", () => {
    reader.load("terminal", { terminal_args: [] });
  });

  test("test_terminal_no_equals_skipped", () => {
    reader.load("terminal", { terminal_args: ["noequals", "k=v"] });
    assert.equal(laila.args.k, "v");
  });

  test("test_terminal_empty_key_skipped", () => {
    reader.load("terminal", { terminal_args: ["=value", "k=v"] });
    assert.equal(laila.args.k, "v");
  });
});

// ---------------------------------------------------------------------------
// test_decorators.py :: ensure_list
// ---------------------------------------------------------------------------
describe("TestEnsureList", () => {
  test("test_single_string_becomes_list", () => {
    const f = ensure_list("items")(function f(items) {
      return items;
    });
    assert.deepEqual(f("a"), ["a"]);
  });

  test("test_list_unchanged", () => {
    const f = ensure_list("items")(function f(items) {
      return items;
    });
    const original = ["x", "y"];
    assert.equal(f(original), original);
  });

  test("test_set_unchanged", () => {
    const f = ensure_list("items")(function f(items) {
      return items;
    });
    const original = new Set([1, 2]);
    assert.equal(f(original), original);
  });

  test("test_frozenset_unchanged", () => {
    const f = ensure_list("items")(function f(items) {
      return items;
    });
    const original = new T.PyFrozenSet([3, 4]);
    assert.equal(f(original), original);
  });

  test("test_missing_named_arg_raises_type_error", () => {
    const f = ensure_list("items")(function f() {
      return null;
    });
    assert.throws(
      () => f(),
      (e) => e instanceof E.TypeError && String(e.message).includes("items"),
    );
  });

  test("test_keyword_argument", () => {
    // ``f(items="k")``: the keyword form is the trailing options object.
    const f = ensure_list("items")(function f({ items } = {}) {
      return items;
    });
    assert.deepEqual(f({ items: "k" }), ["k"]);
  });

  test("test_preserves_wrapped_function_name", () => {
    function original(items) {
      return items;
    }
    const wrapped = ensure_list("items")(original);
    assert.equal(wrapped.name, "original");
  });

  test("test_integer_becomes_list", () => {
    const f = ensure_list("x")(function f(x) {
      return x;
    });
    assert.deepEqual(f(42), [42]);
  });

  test("test_none_becomes_list", () => {
    const f = ensure_list("x")(function f(x) {
      return x;
    });
    assert.deepEqual(f(null), [null]);
  });

  test("test_tuple_becomes_list", () => {
    const f = ensure_list("x")(function f(x) {
      return x;
    });
    assert.deepEqual(f(T.tuple([1, 2])), [T.tuple([1, 2])]);
  });

  test("test_dict_becomes_list", () => {
    const f = ensure_list("x")(function f(x) {
      return x;
    });
    const d = { a: 1 };
    assert.deepEqual(f(d), [d]);
  });

  test("test_empty_list_unchanged", () => {
    const f = ensure_list("x")(function f(x) {
      return x;
    });
    assert.deepEqual(f([]), []);
  });

  test("test_empty_set_unchanged", () => {
    const f = ensure_list("x")(function f(x) {
      return x;
    });
    assert.deepEqual(f(new Set()), new Set());
  });

  test("test_with_default_value", () => {
    const f = ensure_list("x")(function f(x = "default") {
      return x;
    });
    assert.deepEqual(f(), ["default"]);
  });

  test("test_multiple_args_only_named_wrapped", () => {
    const f = ensure_list("b")(function f(a, b, c = 10) {
      return [a, b, c];
    });
    const [a, b, c] = f(1, "hello", 20);
    assert.equal(a, 1);
    assert.deepEqual(b, ["hello"]);
    assert.equal(c, 20);
  });

  test("test_wrong_arg_name_raises", () => {
    const f = ensure_list("nonexistent")(function f(x) {
      return x;
    });
    assert.throws(() => f(42), E.TypeError);
  });

  test("test_boolean_becomes_list", () => {
    const f = ensure_list("x")(function f(x) {
      return x;
    });
    assert.deepEqual(f(true), [true]);
  });
});

// ---------------------------------------------------------------------------
// test_decorators.py :: synchronized
// ---------------------------------------------------------------------------
class _LockableStub extends _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT {}

/**
 * ``_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT.atomic = spy_atomic`` ... ``finally: = orig``.
 * *record* receives ``self`` for every ``atomic()`` entry; restored on exit.
 */
function _with_spy_atomic(record, fn) {
  const proto = _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT.prototype;
  const orig = proto.atomic;
  proto.atomic = function spy_atomic(...args) {
    record(this);
    return orig.apply(this, args);
  };
  try {
    return fn();
  } finally {
    proto.atomic = orig;
  }
}

describe("TestSynchronized", () => {
  test("test_non_lockable_runs_without_lock", () => {
    class C {
      work(x) {
        return x + 1;
      }
    }
    C.prototype.work = synchronized(C.prototype.work);
    assert.equal(new C().work(2), 3);
  });

  test("test_lockable_runs_under_atomic", () => {
    const entered = [];
    _with_spy_atomic(
      (self) => entered.push(self),
      () => {
        class C {
          work(_target) {
            return entered.length;
          }
        }
        C.prototype.work = synchronized(C.prototype.work);

        const lockable = new _LockableStub();
        assert.equal(new C().work(lockable), 1);
        assert.equal(entered[0], lockable);
      },
    );
  });

  test("test_multiple_lockable_args_sorted_by_id", () => {
    const lock_order = [];
    _with_spy_atomic(
      (self) => lock_order.push(T.id(self)),
      () => {
        class C {
          work(_a, _b) {
            return true;
          }
        }
        C.prototype.work = synchronized(C.prototype.work);

        const a = new _LockableStub();
        const b = new _LockableStub();
        new C().work(a, b);
        assert.deepEqual(lock_order, T.sorted([T.id(a), T.id(b)]));
      },
    );
  });

  test("test_lockable_in_kwargs", () => {
    const entered = [];
    _with_spy_atomic(
      (self) => entered.push(self),
      () => {
        class C {
          work({ target = null } = {}) {
            return entered.length;
          }
        }
        C.prototype.work = synchronized(C.prototype.work);

        const lockable = new _LockableStub();
        new C().work({ target: lockable });
        assert.equal(entered.length, 1);
        assert.equal(entered[0], lockable);
      },
    );
  });

  test("test_return_value_preserved", () => {
    class C {
      work(x) {
        return { key: x };
      }
    }
    C.prototype.work = synchronized(C.prototype.work);
    assert.deepEqual(new C().work(99), { key: 99 });
  });

  test("test_exception_propagates", () => {
    class C {
      work() {
        throw new E.ValueError("sync-boom");
      }
    }
    C.prototype.work = synchronized(C.prototype.work);
    assert.throws(
      () => new C().work(),
      (e) => e instanceof E.ValueError && /sync-boom/.test(e.message),
    );
  });

  test("test_mixed_lockable_and_plain_args", () => {
    const entered = [];
    _with_spy_atomic(
      (self) => entered.push(self),
      () => {
        class C {
          work(a, b, c) {
            return [a, b, c];
          }
        }
        C.prototype.work = synchronized(C.prototype.work);

        const lockable = new _LockableStub();
        void new C().work(lockable, 42, "plain");
        assert.equal(entered.length, 1);
        assert.equal(entered[0], lockable);
      },
    );
  });

  test("test_same_lockable_passed_twice", () => {
    const entered = [];
    _with_spy_atomic(
      (self) => entered.push(T.id(self)),
      () => {
        class C {
          work(_a, _b) {
            return true;
          }
        }
        C.prototype.work = synchronized(C.prototype.work);

        const lockable = new _LockableStub();
        new C().work(lockable, lockable);
        const unique_ids = new Set(entered);
        assert.equal(unique_ids.size, 1);
      },
    );
  });
});

// ---------------------------------------------------------------------------
// test_guarantee_multiprocess.py
// ---------------------------------------------------------------------------
const GUARANTEE_PEER = new URL("./fixtures/guarantee_peer.js", import.meta.url);

/**
 * ``ctx.Process(target=_child_main, ...)`` + ``ready_q.get(timeout=30)``:
 * spawn the fixture peer and resolve with its banner once ``READY`` arrives.
 */
function _spawn_peer(label) {
  const proc = spawn(process.execPath, [GUARANTEE_PEER.pathname], { stdio: ["pipe", "pipe", "inherit"] });
  const info = {};
  const ready = new Promise((resolve, reject) => {
    const rl = readline.createInterface({ input: proc.stdout });
    rl.on("line", (line) => {
      line = line.trim();
      if (line === "READY") {
        resolve(info);
        return;
      }
      const i = line.indexOf("=");
      if (i > 0) info[line.slice(0, i)] = line.slice(i + 1);
    });
    proc.on("exit", (code) => reject(new Error(`peer ${label} exited early (code ${code})`)));
    setTimeout(() => reject(new Error(`peer ${label} did not become ready`)), 30_000).unref();
  });
  return { proc, info, ready };
}

/** ``rec["proc"].join(timeout)`` */
function _join(proc, timeout) {
  return new Promise((resolve) => {
    if (proc.exitCode !== null || proc.signalCode !== null) return resolve(true);
    const timer = setTimeout(() => resolve(false), timeout * 1000);
    proc.once("exit", () => {
      clearTimeout(timer);
      resolve(true);
    });
  });
}

/**
 * ``_HARNESS``: the parent policy, child processes, and their proxies.
 * Populated by the module-level ``before`` (``setUpModule``).
 */
const _HARNESS = { children: {}, policy_a: null, proxy_b: null, proxy_c: null, peer_b_id: "", peer_c_id: "" };


describe("test_guarantee_multiprocess.py", () => {
  // setUpModule
  before(async () => {
    const B = _spawn_peer("B");
    const C = _spawn_peer("C");
    _HARNESS.children.B = { proc: B.proc, info: B.info };
    _HARNESS.children.C = { proc: C.proc, info: C.info };
    const info_b = await B.ready;
    const info_c = await C.ready;

    await macrotask(() => {
      laila._active_policy_gid = null;
      _HARNESS.policy_a = new DefaultPolicy();
      laila.activate_policy(_HARNESS.policy_a);
      const tcp_a = new DefaultTCPIPProtocol({ host: "127.0.0.1", port: 0 });
      _HARNESS.policy_a.central.communication.add_connection(tcp_a);

      _HARNESS.peer_b_id = _HARNESS.policy_a.central.communication.add_tcpip_peer(info_b.HOST, Number(info_b.PORT), info_b.SECRET);
      _HARNESS.peer_c_id = _HARNESS.policy_a.central.communication.add_tcpip_peer(info_c.HOST, Number(info_c.PORT), info_c.SECRET);

      for (let i = 0; i < 60; i++) {
        const peers = _HARNESS.policy_a.central.communication.peers;
        if (_HARNESS.peer_b_id in peers && _HARNESS.peer_c_id in peers) break;
        time.sleep(0.05);
      }

      _HARNESS.proxy_b = _HARNESS.policy_a.central.communication.peers[_HARNESS.peer_b_id];
      _HARNESS.proxy_c = _HARNESS.policy_a.central.communication.peers[_HARNESS.peer_c_id];
    });
  });

  // tearDownModule
  after(async () => {
    for (const rec of Object.values(_HARNESS.children)) {
      try {
        rec.proc.stdin.write("stop\n");
      } catch {
        /* pass */
      }
      if (!(await _join(rec.proc, 5.0))) {
        rec.proc.kill("SIGTERM");
        if (!(await _join(rec.proc, 2.0))) rec.proc.kill("SIGKILL");
      }
    }
    await macrotask(() => {
      try {
        _HARNESS.policy_a.central.communication.stop();
      } catch {
        /* pass */
      }
      // Release the parent's taskforce threads so the test process can exit.
      try {
        _HARNESS.policy_a.central.command.shutdown({ wait: true, cancel_pending: true });
      } catch {
        /* pass */
      }
    });
  });

  // _TwoPeerTest helpers
  const proxy_b = () => _HARNESS.proxy_b;
  const proxy_c = () => _HARNESS.proxy_c;
  const peer_b_id = () => _HARNESS.peer_b_id;
  const peer_c_id = () => _HARNESS.peer_c_id;
  const policy_a = () => _HARNESS.policy_a;
  /** ``asyncio.run(coro)`` */
  const _run_async = (coro) => asyncio.run(coro);
  /** Awaitable proxy call for use inside coroutine bodies (see header). */
  // Python calls the proxy synchronously inside coroutines and gets the
  // ``RemoteFuture`` back; the boxed awaitable form is the JS equivalent.
  // Any promise that resolves with a (thenable) ``RemoteFuture`` adopts it,
  // so the future travels in a ``{ result }`` box: ``(await rpc(...)).result``.
  const rpc = (chain, ...args) => chain.call_async_boxed(...args);
  const str = (x) => T.str(x);

  // -------------------------------------------------------------------------
  // TestSyncGuarantee
  // -------------------------------------------------------------------------
  describe("TestSyncGuarantee", () => {
    t("test_single_remote_future_fast", () => {
      let rf;
      with_(laila.guarantee, () => {
        rf = proxy_b().sleep_and_return(0.0, "a");
      });
      assert.equal(str(rf.status), str(rf.status));
    });

    t("test_single_remote_future_short", () => {
      const start = time.monotonic();
      let rf;
      with_(laila.guarantee, () => {
        rf = proxy_b().sleep_and_return(0.05, "a");
      });
      assert.ok(time.monotonic() - start >= 0.05);
      assert.notEqual(rf.result, null);
    });

    t("test_single_remote_future_medium", () => {
      const start = time.monotonic();
      with_(laila.guarantee, () => {
        proxy_b().sleep_and_return(0.1, "a");
      });
      assert.ok(time.monotonic() - start >= 0.1);
    });

    t("test_single_remote_future_longer", () => {
      const start = time.monotonic();
      with_(laila.guarantee, () => {
        proxy_b().sleep_and_return(0.2, "a");
      });
      assert.ok(time.monotonic() - start >= 0.2);
    });

    t(
      "test_single_remote_future_result_is_entry_gid",
      () => {
        let rf;
        with_(laila.guarantee, () => {
          rf = proxy_b().sleep_and_return(0.05, "payload");
        });
        // RemoteFuture now fully mirrors a local Future: .result is the
        // rebuilt Entry (value via .data, gid via .global_id).
        assert.equal(rf.result.data, "payload");
        assert.ok(rf.result.global_id.startsWith("LAILA:"));
      },
      );

    t(
      "test_two_remote_futures_from_peer_b",
      () => {
        let rf1, rf2;
        with_(laila.guarantee, () => {
          rf1 = proxy_b().sleep_and_return(0.05, "a");
          rf2 = proxy_b().sleep_and_return(0.05, "b");
        });
        assert.notEqual(rf1.global_id, rf2.global_id);
      },
      );

    t("test_three_remote_futures_from_peer_b", () => {
      let handles;
      with_(laila.guarantee, () => {
        handles = T.range(3).map((i) => proxy_b().sleep_and_return(0.05, i));
      });
      assert.equal(handles.length, 3);
    });

    t("test_five_remote_futures_from_peer_b", () => {
      with_(laila.guarantee, () => {
        for (const i of T.range(5)) proxy_b().sleep_and_return(0.02, i);
      });
    });

    t("test_many_remote_futures_from_peer_b", () => {
      with_(laila.guarantee, () => {
        for (const i of T.range(12)) proxy_b().sleep_and_return(0.02, i);
      });
    });

    t(
      "test_remote_futures_from_peer_c",
      () => {
        let rf;
        with_(laila.guarantee, () => {
          rf = proxy_c().sleep_and_return(0.05, "from-c");
        });
        assert.equal(str(rf.policy_id), peer_c_id());
      },
      );

    t(
      "test_mixed_peers_single_each",
      () => {
        let rb, rc;
        with_(laila.guarantee, () => {
          rb = proxy_b().sleep_and_return(0.05, "b");
          rc = proxy_c().sleep_and_return(0.05, "c");
        });
        assert.equal(str(rb.policy_id), peer_b_id());
        assert.equal(str(rc.policy_id), peer_c_id());
      },
      );

    t("test_mixed_peers_many", () => {
      with_(laila.guarantee, () => {
        for (const i of T.range(4)) {
          proxy_b().sleep_and_return(0.02, i);
          proxy_c().sleep_and_return(0.02, i);
        }
      });
    });

    t(
      "test_pre_completed_remote_future_inside_guarantee",
      () => {
        let rf;
        with_(laila.guarantee, () => {
          rf = proxy_b().finished_future("done");
        });
        assert.ok(str(rf.status).endsWith("FINISHED"));
      },
      );

    t("test_pre_completed_remote_future_does_not_block", () => {
      const rf = proxy_b().finished_future("done");
      const start = time.monotonic();
      with_(laila.guarantee, () => {
        // no new futures; still enters/exits fast
      });
      assert.ok(time.monotonic() - start < 1.0);
      assert.notEqual(rf.result, null);
    });

    t("test_empty_guarantee_no_futures", () => {
      const start = time.monotonic();
      with_(laila.guarantee, () => {});
      assert.ok(time.monotonic() - start < 1.0);
    });

    t("test_empty_guarantee_returns_cleanly", () => {
      const out = [];
      with_(laila.guarantee, () => {
        out.push("in");
      });
      out.push("out");
      assert.deepEqual(out, ["in", "out"]);
    });

    t("test_nested_guarantee_inner_waits", () => {
      const start = time.monotonic();
      with_(laila.guarantee, () =>
        with_(laila.guarantee, () => {
          proxy_b().sleep_and_return(0.1, "x");
        }),
      );
      assert.ok(time.monotonic() - start >= 0.1);
    });

    t("test_nested_guarantee_outer_waits_for_outer_future", () => {
      const start = time.monotonic();
      with_(laila.guarantee, () => {
        proxy_b().sleep_and_return(0.1, "outer");
        with_(laila.guarantee, () => {
          proxy_b().sleep_and_return(0.05, "inner");
        });
      });
      assert.ok(time.monotonic() - start >= 0.1);
    });

    t("test_guarantee_raises_remote_exception", () => {
      assert.throws(
        () =>
          with_(laila.guarantee, () => {
            proxy_b().raise_after(0.02, "boom");
          }),
        (e) => e instanceof E.RuntimeError && str(e).includes("boom"),
      );
    });

    t("test_guarantee_mix_success_and_error", () => {
      assert.throws(
        () =>
          with_(laila.guarantee, () => {
            proxy_b().sleep_and_return(0.05, "ok");
            proxy_b().raise_after(0.02, "kaboom");
          }),
        E.RuntimeError,
      );
    });
  });

  // -------------------------------------------------------------------------
  // TestAsyncGuarantee
  // -------------------------------------------------------------------------
  describe("TestAsyncGuarantee", () => {
    test("test_single_remote_future", async () => {
      const _run = async () => {
        let rf;
        await with_async(laila.guarantee_async, async () => {
          rf = (await rpc(proxy_b().sleep_and_return, 0.05, "a")).result;
        });
        return { rf }; // boxed: an async fn returning a thenable future would adopt it
      };
      const { rf } = await _run_async(_run);
      await macrotask(() => {
        assert.notEqual(rf.result, null);
      });
    });

    test("test_multiple_remote_futures", async () => {
      const _run = async () => {
        await with_async(laila.guarantee_async, async () => {
          for (const i of T.range(4)) (await rpc(proxy_b().sleep_and_return, 0.05, i)).result;
        });
      };
      await _run_async(_run);
    });

    test("test_async_guarantee_duration_single", async () => {
      const _run = async () => {
        const t0 = time.monotonic();
        await with_async(laila.guarantee_async, async () => {
          (await rpc(proxy_b().sleep_and_return, 0.1, "x")).result;
        });
        return time.monotonic() - t0;
      };
      assert.ok((await _run_async(_run)) >= 0.1);
    });

    test("test_async_guarantee_duration_many", async () => {
      const _run = async () => {
        const t0 = time.monotonic();
        await with_async(laila.guarantee_async, async () => {
          for (const _ of T.range(3)) (await rpc(proxy_b().sleep_and_return, 0.1, "x")).result;
        });
        return time.monotonic() - t0;
      };
      assert.ok((await _run_async(_run)) >= 0.1);
    });

    test("test_async_mixed_peers", async () => {
      const _run = async () => {
        await with_async(laila.guarantee_async, async () => {
          (await rpc(proxy_b().sleep_and_return, 0.05, "b")).result;
          (await rpc(proxy_c().sleep_and_return, 0.05, "c")).result;
        });
      };
      await _run_async(_run);
    });

    test("test_async_mixed_peers_many", async () => {
      const _run = async () => {
        await with_async(laila.guarantee_async, async () => {
          for (const i of T.range(3)) {
            (await rpc(proxy_b().sleep_and_return, 0.02, i)).result;
            (await rpc(proxy_c().sleep_and_return, 0.02, i)).result;
          }
        });
      };
      await _run_async(_run);
    });

    test("test_async_with_asyncio_sleep_interleaved", async () => {
      const _run = async () => {
        await with_async(laila.guarantee_async, async () => {
          (await rpc(proxy_b().sleep_and_return, 0.1, "x")).result;
          await asyncio.sleep(0.05);
        });
      };
      await _run_async(_run);
    });

    test("test_async_event_loop_not_blocked", async () => {
      const _run = async () => {
        const t0 = time.monotonic();
        await with_async(laila.guarantee_async, async () => {
          (await rpc(proxy_b().sleep_and_return, 0.2, "slow")).result;
          await asyncio.sleep(0.05);
        });
        return time.monotonic() - t0;
      };
      const elapsed = await _run_async(_run);
      assert.ok(elapsed >= 0.2);
      assert.ok(elapsed < 0.6);
    });

    test("test_async_nested_guarantee", async () => {
      const _run = async () => {
        await with_async(laila.guarantee_async, async () => {
          await with_async(laila.guarantee_async, async () => {
            (await rpc(proxy_b().sleep_and_return, 0.05, "inner")).result;
          });
          (await rpc(proxy_b().sleep_and_return, 0.05, "outer")).result;
        });
      };
      await _run_async(_run);
    });

    test("test_async_nested_outer_sees_inner_error", async () => {
      const _run = async () => {
        await with_async(laila.guarantee_async, () =>
          with_async(laila.guarantee_async, async () => {
            (await rpc(proxy_b().raise_after, 0.02, "inner-error")).result;
          }),
        );
      };
      await assert.rejects(_run_async(_run));
    });

    test("test_async_pre_completed_future", async () => {
      const _run = async () => {
        const rf = (await rpc(proxy_b().finished_future, "done")).result;
        await with_async(laila.guarantee_async, async () => {});
        return { rf }; // boxed: an async fn returning a thenable future would adopt it
      };
      const { rf } = await _run_async(_run);
      await macrotask(() => {
        assert.ok(str(rf.status).endsWith("FINISHED"));
      });
    });

    test("test_async_empty_guarantee", async () => {
      const _run = async () => {
        await with_async(laila.guarantee_async, async () => {});
      };
      await _run_async(_run);
    });

    test("test_async_remote_group_future", async () => {
      const _run = async () => {
        await with_async(laila.guarantee_async, async () => {
          (await rpc(proxy_b().spawn_group, 3, 0.05, "g")).result;
        });
      };
      await _run_async(_run);
    });

    test("test_async_remote_group_then_scalar", async () => {
      const _run = async () => {
        await with_async(laila.guarantee_async, async () => {
          (await rpc(proxy_b().spawn_group, 2, 0.05, "a")).result;
          (await rpc(proxy_b().sleep_and_return, 0.05, "scalar")).result;
        });
      };
      await _run_async(_run);
    });

    test("test_async_exception_propagates", async () => {
      const _run = async () => {
        await with_async(laila.guarantee_async, async () => {
          (await rpc(proxy_b().raise_after, 0.02, "async-boom")).result;
        });
      };
      await assert.rejects(_run_async(_run), (e) => str(e).includes("async-boom"));
    });

    test("test_async_mixed_error_peers", async () => {
      const _run = async () => {
        await with_async(laila.guarantee_async, async () => {
          (await rpc(proxy_b().sleep_and_return, 0.05, "ok")).result;
          (await rpc(proxy_c().raise_after, 0.02, "c-boom")).result;
        });
      };
      await assert.rejects(_run_async(_run));
    });

    test("test_async_many_peers_concurrent", async () => {
      const _run = async () => {
        const t0 = time.monotonic();
        await with_async(laila.guarantee_async, async () => {
          for (const _ of T.range(3)) {
            (await rpc(proxy_b().sleep_and_return, 0.1, "b")).result;
            (await rpc(proxy_c().sleep_and_return, 0.1, "c")).result;
          }
        });
        return time.monotonic() - t0;
      };
      assert.ok((await _run_async(_run)) >= 0.1);
    });

    test("test_async_sequential_guarantee_blocks", async () => {
      const _run = async () => {
        await with_async(laila.guarantee_async, async () => {
          (await rpc(proxy_b().sleep_and_return, 0.05, "first")).result;
        });
        await with_async(laila.guarantee_async, async () => {
          (await rpc(proxy_c().sleep_and_return, 0.05, "second")).result;
        });
      };
      await _run_async(_run);
    });

    test("test_async_returns_control_after_exit", async () => {
      const after_ = [];
      const _run = async () => {
        await with_async(laila.guarantee_async, async () => {
          (await rpc(proxy_b().sleep_and_return, 0.05, "x")).result;
        });
        after_.push(1);
      };
      await _run_async(_run);
      assert.deepEqual(after_, [1]);
    });

    test("test_async_guarantee_with_gather_inside", async () => {
      const _run = async () => {
        await with_async(laila.guarantee_async, async () => {
          const rfs = [];
          for (const i of T.range(3)) rfs.push((await rpc(proxy_b().sleep_and_return, 0.05, i)).result);
          await asyncio.gather(...rfs);
        });
      };
      await _run_async(_run);
    });
  });

  // -------------------------------------------------------------------------
  // TestAwaitDirect
  // -------------------------------------------------------------------------
  describe("TestAwaitDirect", () => {
    test("test_await_single_remote_future", async () => {
      const _run = async () => {
        const rf = (await rpc(proxy_b().sleep_and_return, 0.05, "x")).result;
        await rf;
        return { rf }; // boxed: an async fn returning a thenable future would adopt it
      };
      const { rf } = await _run_async(_run);
      await macrotask(() => {
        assert.notEqual(rf.result, null);
      });
    });

    test("test_await_returns_result_id", async () => {
      const _run = async () => {
        const rf = (await rpc(proxy_b().sleep_and_return, 0.05, "payload")).result;
        const out = await rf;
        return out;
      };
      const out = await _run_async(_run);
      assert.equal(out.data, "payload");
    });

    test("test_await_zero_sleep", async () => {
      const _run = async () => {
        const rf = (await rpc(proxy_b().sleep_and_return, 0.0, "z")).result;
        return await rf;
      };
      await _run_async(_run);
    });

    test("test_await_pre_finished_future", async () => {
      const _run = async () => {
        const rf = (await rpc(proxy_b().finished_future, "done")).result;
        const t0 = time.monotonic();
        await rf;
        return time.monotonic() - t0;
      };
      assert.ok((await _run_async(_run)) < 1.0);
    });

    test("test_await_pre_finished_future_status", async () => {
      const _run = async () => {
        const rf = (await rpc(proxy_b().finished_future, "done")).result;
        await rf;
        return { rf }; // boxed: an async fn returning a thenable future would adopt it
      };
      const { rf } = await _run_async(_run);
      await macrotask(() => {
        assert.ok(str(rf.status).endsWith("FINISHED"));
      });
    });

    test("test_await_errored_future_raises", async () => {
      const _run = async () => {
        const rf = (await rpc(proxy_b().raise_after, 0.02, "awaited-boom")).result;
        await rf;
      };
      await assert.rejects(_run_async(_run), (e) => str(e).includes("awaited-boom"));
    });

    test("test_await_errored_future_message", async () => {
      const _run = async () => {
        const rf = (await rpc(proxy_c().raise_after, 0.02, "c-error")).result;
        await rf;
      };
      await assert.rejects(_run_async(_run), (e) => str(e).includes("c-error"));
    });

    test("test_await_same_future_twice", async () => {
      const _run = async () => {
        const rf = (await rpc(proxy_b().sleep_and_return, 0.05, "twice")).result;
        const a = await rf;
        const b = await rf;
        return [a, b];
      };
      const [a, b] = await _run_async(_run);
      assert.equal(a, b);
    });

    test("test_await_sequentially", async () => {
      const _run = async () => {
        const rf1 = (await rpc(proxy_b().sleep_and_return, 0.05, 1)).result;
        await rf1;
        const rf2 = (await rpc(proxy_b().sleep_and_return, 0.05, 2)).result;
        await rf2;
      };
      await _run_async(_run);
    });

    test("test_gather_three_from_peer_b", async () => {
      const _run = async () => {
        const rfs = [];
        for (const i of T.range(3)) rfs.push((await rpc(proxy_b().sleep_and_return, 0.05, i)).result);
        return await asyncio.gather(...rfs);
      };
      const res = await _run_async(_run);
      assert.equal(res.length, 3);
    });

    test("test_gather_five_from_peer_b", async () => {
      const _run = async () => {
        const rfs = [];
        for (const i of T.range(5)) rfs.push((await rpc(proxy_b().sleep_and_return, 0.05, i)).result);
        return await asyncio.gather(...rfs);
      };
      const res = await _run_async(_run);
      assert.equal(res.length, 5);
    });

    test("test_gather_ten_from_peer_b", async () => {
      const _run = async () => {
        const rfs = [];
        for (const i of T.range(10)) rfs.push((await rpc(proxy_b().sleep_and_return, 0.02, i)).result);
        return await asyncio.gather(...rfs);
      };
      const res = await _run_async(_run);
      assert.equal(res.length, 10);
    });

    test("test_gather_across_peers", async () => {
      const _run = async () => {
        const rfs = [
          (await rpc(proxy_b().sleep_and_return, 0.05, "b1")).result,
          (await rpc(proxy_c().sleep_and_return, 0.05, "c1")).result,
          (await rpc(proxy_b().sleep_and_return, 0.05, "b2")).result,
          (await rpc(proxy_c().sleep_and_return, 0.05, "c2")).result,
        ];
        return await asyncio.gather(...rfs);
      };
      const res = await _run_async(_run);
      assert.equal(res.length, 4);
    });

    test("test_gather_both_peers_many", async () => {
      const _run = async () => {
        const rfs = [];
        for (const i of T.range(4)) {
          rfs.push((await rpc(proxy_b().sleep_and_return, 0.02, i)).result);
          rfs.push((await rpc(proxy_c().sleep_and_return, 0.02, i)).result);
        }
        return await asyncio.gather(...rfs);
      };
      const res = await _run_async(_run);
      assert.equal(res.length, 8);
    });

    test("test_wait_for_bounded_success", async () => {
      const _run = async () => {
        const rf = (await rpc(proxy_b().sleep_and_return, 0.05, "wf")).result;
        return await asyncio.wait_for(rf, 5.0);
      };
      const out = await _run_async(_run);
      assert.notEqual(out, null);
    });

    test("test_wait_for_timeout_on_slow_future", async () => {
      const _run = async () => {
        const rf = (await rpc(proxy_b().long_sleep, 0.5)).result;
        await asyncio.wait_for(rf, 0.1);
      };
      await assert.rejects(_run_async(_run), asyncio.TimeoutError);
    });

    test("test_await_then_status", async () => {
      const _run = async () => {
        const rf = (await rpc(proxy_b().sleep_and_return, 0.05, "s")).result;
        await rf;
        return { rf }; // boxed: an async fn returning a thenable future would adopt it
      };
      const { rf } = await _run_async(_run);
      await macrotask(() => {
        assert.ok(str(rf.status).endsWith("FINISHED"));
      });
    });

    test("test_await_then_repeated_status", async () => {
      const _run = async () => {
        const rf = (await rpc(proxy_b().sleep_and_return, 0.05, "s")).result;
        await rf;
        return { rf };
      };
      const { rf } = await _run_async(_run);
      // ``rf.status`` is a blocking RPC (Python blocks the loop thread here).
      const [a, b] = await macrotask(() => [str(rf.status), str(rf.status)]);
      assert.equal(a, b);
    });
  });

  // -------------------------------------------------------------------------
  // TestWaitDirect
  // -------------------------------------------------------------------------
  describe("TestWaitDirect", () => {
    t(
      "test_wait_returns_result_id",
      () => {
        const rf = proxy_b().sleep_and_return(0.05, "x");
        const out = rf.wait();
        assert.equal(out.data, "x");
      },
      );

    t(
      "test_wait_blocks_until_completion",
      () => {
        const rf = proxy_b().sleep_and_return(0.1, "x");
        const t0 = time.monotonic();
        rf.wait();
        assert.ok(time.monotonic() - t0 >= 0.1);
      },
      );

    t(
      "test_wait_zero_sleep",
      () => {
        const rf = proxy_b().sleep_and_return(0.0, "z");
        assert.notEqual(rf.wait(), null);
      },
      );

    t(
      "test_wait_then_status_finished",
      () => {
        const rf = proxy_b().sleep_and_return(0.05, "x");
        rf.wait();
        assert.ok(str(rf.status).endsWith("FINISHED"));
      },
      );

    t(
      "test_wait_on_pre_finished_future_returns_fast",
      () => {
        const rf = proxy_b().finished_future("done");
        const t0 = time.monotonic();
        rf.wait();
        assert.ok(time.monotonic() - t0 < 1.0);
      },
      );

    t(
      "test_wait_idempotent",
      () => {
        const rf = proxy_b().sleep_and_return(0.05, "x");
        const a = rf.wait();
        const b = rf.wait();
        assert.equal(a, b);
      },
      );

    t(
      "test_wait_raises_remote_error",
      () => {
        const rf = proxy_b().raise_after(0.02, "err-wait");
        assert.throws(
          () => rf.wait(),
          (e) => e instanceof E.RuntimeError && str(e).includes("err-wait"),
        );
      },
      );

    t(
      "test_wait_raises_remote_error_peer_c",
      () => {
        const rf = proxy_c().raise_after(0.02, "err-wait-c");
        assert.throws(
          () => rf.wait(),
          (e) => e instanceof E.RuntimeError && str(e).includes("err-wait-c"),
        );
      },
      );

    t(
      "test_wait_sequential_b_then_c",
      () => {
        const rf_b = proxy_b().sleep_and_return(0.05, "b");
        const rf_c = proxy_c().sleep_and_return(0.05, "c");
        rf_b.wait();
        rf_c.wait();
      },
      );

    t(
      "test_wait_many_sequential",
      () => {
        const rfs = T.range(4).map((i) => proxy_b().sleep_and_return(0.02, i));
        for (const rf of rfs) rf.wait();
      },
      );

    t(
      "test_wait_result_property_after_wait",
      () => {
        const rf = proxy_b().sleep_and_return(0.05, "x");
        rf.wait();
        assert.equal(rf.result.data, "x");
      },
      );

    t(
      "test_wait_then_exception_none_on_success",
      () => {
        const rf = proxy_b().sleep_and_return(0.05, "x");
        rf.wait();
        assert.equal(rf.exception, null);
      },
      );
  });

  // -------------------------------------------------------------------------
  // TestRemoteGroupFuture
  // -------------------------------------------------------------------------
  describe("TestRemoteGroupFuture", () => {
    t(
      "test_group_size_one",
      () => {
        // Group of size 1 gets collapsed into a single future remotely via submit()
        const rf = proxy_b().spawn_group(1, 0.05, "g1");
        rf.wait();
      },
      );

    t(
      "test_group_size_two",
      () => {
        const rf = proxy_b().spawn_group(2, 0.05, "g2");
        rf.wait();
      },
      );

    t(
      "test_group_size_five",
      () => {
        const rf = proxy_b().spawn_group(5, 0.05, "g5");
        rf.wait();
      },
      );

    t(
      "test_group_size_twenty",
      () => {
        const rf = proxy_b().spawn_group(20, 0.02, "g20");
        rf.wait();
      },
      );

    t(
      "test_group_is_remote_future_instance",
      () => {
        const rf = proxy_b().spawn_group(3, 0.05, "g");
        assert.ok(rf instanceof RemoteFuture);
      },
      );

    t(
      "test_group_is_group_flag",
      () => {
        const rf = proxy_b().spawn_group(3, 0.05, "g");
        assert.ok(rf.is_group);
      },
      );

    t(
      "test_group_status_after_wait",
      () => {
        const rf = proxy_b().spawn_group(3, 0.05, "g");
        rf.wait();
        const status = rf.status;
        assert.ok(isdict(status));
        assert.equal(Number(dict_get(dict_get(status, "percentages"), "finished")), 100.0);
      },
      );

    test("test_group_await_via_direct_await", async () => {
      const _run = async () => {
        const rf = (await rpc(proxy_b().spawn_group, 3, 0.05, "g")).result;
        await rf;
        return { rf }; // boxed: an async fn returning a thenable future would adopt it
      };
      const { rf } = await _run_async(_run);
      await macrotask(() => {
        const status = rf.status;
        assert.ok(isdict(status));
        assert.equal(Number(dict_get(dict_get(status, "percentages"), "finished")), 100.0);
      });
    });

    t(
      "test_group_error_raises_on_wait",
      () => {
        const rf = proxy_b().spawn_error_group(3, 1, 0.02);
        assert.throws(() => rf.wait(), E.RuntimeError);
      },
      );

    t(
      "test_group_error_from_peer_c",
      () => {
        const rf = proxy_c().spawn_error_group(4, 2, 0.02);
        assert.throws(() => rf.wait(), E.RuntimeError);
      },
      );

    t("test_group_inside_sync_guarantee", () => {
      const t0 = time.monotonic();
      with_(laila.guarantee, () => {
        proxy_b().spawn_group(3, 0.05, "g");
      });
      assert.ok(time.monotonic() - t0 >= 0.05);
    });

    test("test_group_inside_async_guarantee", async () => {
      const _run = async () => {
        await with_async(laila.guarantee_async, async () => {
          (await rpc(proxy_c().spawn_group, 3, 0.05, "g")).result;
        });
      };
      await _run_async(_run);
    });
  });

  // -------------------------------------------------------------------------
  // TestRouting
  // -------------------------------------------------------------------------
  describe("TestRouting", () => {
    t(
      "test_is_laila_identifiable_future",
      () => {
        const rf = proxy_b().sleep_and_return(0.05, "x");
        assert.ok(rf instanceof _LAILA_IDENTIFIABLE_FUTURE);
      },
      );

    t(
      "test_policy_id_is_peer_b",
      () => {
        const rf = proxy_b().sleep_and_return(0.05, "x");
        assert.equal(str(rf.policy_id), peer_b_id());
      },
      );

    t(
      "test_policy_id_is_peer_c",
      () => {
        const rf = proxy_c().sleep_and_return(0.05, "x");
        assert.equal(str(rf.policy_id), peer_c_id());
      },
      );

    t(
      "test_registered_in_local_future_bank",
      () => {
        const rf = proxy_b().sleep_and_return(0.05, "x");
        assert.ok(dict_has(policy_a().future_bank, rf.global_id));
      },
      );

    t(
      "test_runtime_status_matches",
      () => {
        const rf = proxy_b().sleep_and_return(0.05, "x");
        rf.wait();
        assert.equal(str(laila.runtime.status(rf)), str(rf.status));
      },
      );

    t(
      "test_runtime_wait_routes_remote",
      () => {
        const rf = proxy_b().sleep_and_return(0.05, "x");
        const out = laila.runtime.wait(rf);
        assert.equal(out.data, "x");
      },
      );

    t(
      "test_runtime_result_routes_remote",
      () => {
        const rf = proxy_b().sleep_and_return(0.05, "x");
        rf.wait();
        assert.equal(laila.runtime.result(rf), rf.result);
      },
      );

    t(
      "test_two_peers_distinguishable",
      () => {
        const rb = proxy_b().sleep_and_return(0.05, "b");
        const rc = proxy_c().sleep_and_return(0.05, "c");
        assert.notEqual(str(rb.policy_id), str(rc.policy_id));
      },
      );

    t(
      "test_await_dunder_defined",
      () => {
        const rf = proxy_b().sleep_and_return(0.0, "x");
        assert.equal(typeof rf.__await__, "function");
      },
      );

    t(
      "test_repr_contains_policy_and_future_ids",
      () => {
        const rf = proxy_b().sleep_and_return(0.0, "x");
        const r = repr(rf);
        assert.ok(r.includes(rf.global_id));
        assert.ok(r.includes(str(rf.policy_id)));
      },
      );
  });
});
