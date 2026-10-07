/**
 * Foundations: atomic types, identifiable objects, CLI-capable base,
 * decorators, ArgReader, logger records, runtime facade, asyncio compat.
 * Ports of ``tests/functional/{atomics,utils,logger}`` unit tests that do not
 * need a running policy.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const S = new URL("../../src/", import.meta.url).href;
const { repr } = await import(S + "_compat/pyrepr.js");
const T = await import(S + "_compat/pytypes.js");
const E = await import(S + "_compat/errors.js");
const TH = await import(S + "_compat/threading.js");
const CL = await import(S + "_compat/contextlib.js");
const asyncio = await import(S + "_compat/asyncio.js");
const lazy_mod = await import(S + "_compat/lazy.js");
const { with_, with_async } = CL;
const { AtomicDict, AtomicDotMap, AtomicFlag, AtomicInt, AtomicList, AtomicStr } = await import(S + "atomic/index.js");
const { _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT, _LAILA_LOCALLY_ATOMIC_OBJECT } = await import(S + "atomic/definitions/index.js");
const { _LAILA_IDENTIFIABLE_OBJECT, _LAILA_CLI_CAPABLE_CLASS, _LAILA_OBJECT } = await import(S + "basics/index.js");
const { synchronized, ensure_list } = await import(S + "utils/decorators/index.js");
const { _signature } = await import(S + "utils/decorators/typecheck.js");
const { ArgReader } = await import(S + "utils/args/index.js");
const { guarantee, guarantee_async, _Guarantee, _AsyncGuarantee } = await import(S + "utils/index.js");
const LG = await import(S + "logger/index.js");
const logging = await import(S + "_compat/logging.js");
const runtime = await import(S + "runtime/index.js");
const { LAILA_UNIVERSAL_NAMESPACE } = await import(S + "macros/defaults.js");

// The ``laila`` package root is not built yet (p8); the foundations only need
// ``get_active_namespace`` from it (nickname -> uuid5). Tests that need more
// register richer stubs and restore this one.
const LAILA_STUB = { get_active_namespace: () => LAILA_UNIVERSAL_NAMESPACE };
lazy_mod.register("laila", LAILA_STUB);

function run_threads(n, target) {
  const threads = T.range(n).map((i) => new TH.Thread({ target, args: [i] }));
  for (const t of threads) t.start();
  for (const t of threads) t.join();
  for (const t of threads) if (t.exception) throw t.exception;
}

/**
 * Run a synchronous body on a fresh macrotask: ``node:test`` invokes test
 * bodies from a microtask, where a blocking wait (``Thread.join``) is
 * impossible by construction (nothing can settle until the job returns).
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

// ── AtomicDotMap ─────────────────────────────────────────────────────────

test("AtomicDotMap: set/get/delete, snapshots, repr", () => {
  const dm = new AtomicDotMap();
  dm.s3_bucket_region = "us-east-1";
  assert.equal(dm.s3_bucket_region, "us-east-1");
  dm.token = "abc";
  delete dm.token;
  assert.equal(dm.token, null);
  assert.equal(dm.missing, null);
  const dm2 = new AtomicDotMap();
  dm2.a = 1;
  dm2.b = 2;
  assert.deepEqual(dm2.to_dict(), { a: 1, b: 2 });
  assert.deepEqual(dm2.keys().sort(), ["a", "b"]);
  assert.deepEqual(dm2.items().map((t) => [...t]), [["a", 1], ["b", 2]]);
  assert.ok(repr(dm2).includes("AtomicDotMap"));
  assert.equal(repr(dm2), "AtomicDotMap({'a': 1, 'b': 2})");
  assert.ok("a" in dm2);
  assert.ok(!("zzz" in dm2));
  assert.ok(dm instanceof _LAILA_LOCALLY_ATOMIC_OBJECT);
  assert.ok(dm instanceof _LAILA_OBJECT);
  // ``datetime.now(UTC).isoformat(timespec="milliseconds")``
  assert.match(dm.creation_timestamp, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}\+00:00$/);
  assert.throws(() => dm.__delattr__("nope"), E.KeyError);
});

test("AtomicDotMap: concurrent sets and atomic()", () =>
  macrotask(() => {
  const dm = new AtomicDotMap();
  run_threads(10, (i) => dm.__setattr__(`k${i}`, i));
  const data = dm.to_dict();
  assert.equal(Object.keys(data).length, 10);
  for (let i = 0; i < 10; i++) assert.equal(data[`k${i}`], i);
  with_(dm.atomic(), () => {
    assert.ok(dm.locked());
    dm.inside = true;
  });
  assert.equal(dm.inside, true);
  }));

// ── AtomicInt / AtomicFlag / AtomicStr / AtomicList ───────────────────────

test("AtomicInt", () =>
  macrotask(() => {
  const i = new AtomicInt();
  assert.equal(i.get(), 0);
  i.set_to(5);
  assert.equal(i.get(), 5);
  assert.equal(i.add(3), 8);
  assert.equal(i.increment(), 9);
  assert.equal(i.decrement(), 8);
  i.set_to(99);
  i.reset();
  assert.equal(i.get(), 0);
  with_(i.atomic(), (locked) => {
    locked.value += 10;
  });
  assert.equal(i.get(), 10);
  assert.equal(+i, 10);
  const c = new AtomicInt();
  run_threads(8, () => {
    for (let k = 0; k < 500; k++) c.increment();
  });
  assert.equal(c.get(), 4000);
  }));

test("AtomicFlag", () =>
  macrotask(() => {
  const f = new AtomicFlag();
  assert.equal(f.is_set(), false);
  f.set();
  assert.equal(f.is_set(), true);
  f.toggle();
  assert.equal(f.is_set(), false);
  f.clear();
  assert.equal(f.is_set(), false);
  f.set_to(true);
  assert.equal(f.is_set(), true);
  f.set_to(false);
  assert.equal(f.is_set(), false);
  with_(f.atomic(), (locked) => {
    locked.value = true;
  });
  assert.equal(f.is_set(), true);
  assert.equal(f.__bool__(), true);
  run_threads(10, () => {
    for (let k = 0; k < 1000; k++) f.toggle();
  });
  assert.ok([true, false].includes(f.is_set()));
  }));

test("AtomicStr", () =>
  macrotask(() => {
  const s = new AtomicStr();
  assert.equal(s.get(), "");
  assert.equal(s.length(), 0);
  s.set("ab");
  assert.equal(s.get(), "ab");
  assert.equal(s.append("cd"), "abcd");
  assert.equal(s.length(), 4);
  s.clear();
  assert.equal(s.get(), "");
  with_(s.atomic(), (locked) => {
    locked.value = "locked";
  });
  assert.equal(s.get(), "locked");
  s.set("hello");
  assert.equal(T.str(s), "hello");
  const w = new AtomicStr();
  run_threads(5, (i) => {
    for (let k = 0; k < 100; k++) w.append(String.fromCharCode(65 + i));
  });
  assert.equal(w.length(), 500);
  }));

test("AtomicList", () =>
  macrotask(() => {
  const lst = new AtomicList();
  assert.equal(T.len(lst), 0);
  assert.deepEqual(lst.to_list(), []);
  lst.append(1);
  lst.extend([2, 3]);
  lst.insert(1, 9);
  assert.deepEqual(lst.to_list(), [1, 9, 2, 3]);
  assert.equal(lst.pop(), 3);
  assert.deepEqual(lst.to_list(), [1, 9, 2]);
  assert.equal(lst[0], 1);
  assert.equal(lst[-1], 2);
  assert.ok(9 in lst);

  const l2 = new AtomicList({ value: [0, 1, 2, 3, 4] });
  l2.set_at(2, 99);
  assert.equal(l2.get_at(2), 99);
  assert.deepEqual(l2.slice(1, 4), [1, 99, 3]);
  const l3 = new AtomicList({ value: [0, 1, 2, 3, 4] });
  l3.trim(1, 4);
  assert.deepEqual(l3.to_list(), [1, 2, 3]);

  const l4 = new AtomicList();
  with_(l4.atomic(), (raw) => {
    raw.push(1, 2, 3); // ``raw`` is the underlying list (JS Array)
  });
  assert.deepEqual(l4.to_list(), [1, 2, 3]);

  const l5 = new AtomicList();
  run_threads(6, (idx) => {
    for (let k = 0; k < 200; k++) l5.append(idx);
  });
  assert.equal(T.len(l5), 1200);
  }));

// ── AtomicDict ───────────────────────────────────────────────────────────

test("AtomicDict: mapping protocol, ordering helpers, atomic view", () => {
  const d = new AtomicDict();
  d["a"] = 1;
  d["b"] = 2;
  d["c"] = 3;
  assert.equal(d["a"], 1);
  assert.equal(T.len(d), 3);
  assert.ok("b" in d);
  assert.equal(d.get("zzz"), null);
  assert.equal(d.get("zzz", 7), 7);
  assert.throws(() => d.__getitem__("zzz"), E.KeyError);
  assert.deepEqual([...d.keys()], ["a", "b", "c"]);
  assert.deepEqual([...d.item_at(0)], ["a", 1]);
  assert.equal(d.key_at(-1), "c");
  assert.equal(d.value_at(1), 2);
  const [k, v] = d.pop_next();
  assert.deepEqual([k, v], ["a", 1]);
  delete d["b"];
  assert.deepEqual([...d.keys()], ["c"]);
  assert.equal(d.increment("n"), 1);
  assert.equal(d.increment("n", 5), 6);
  assert.equal(d.compute("n", (x) => x * 2), 12);
  d.setdefault("s", "x");
  assert.equal(d.setdefault("s", "y"), "x");
  assert.equal(repr(d), "AtomicDict({'c': 3, 'n': 12, 's': 'x'})");
  with_(d.atomic(), (view) => {
    assert.ok(d._lock._is_owned());
    view["z"] = 26;
    assert.equal(view["z"], 26);
    assert.equal(AtomicDict.current(), d);
  });
  assert.throws(() => AtomicDict.current(), E.RuntimeError);
  assert.equal(d["z"], 26);
  d.trim(0, 2);
  assert.equal(T.len(d), 2);
  d.clear();
  assert.equal(T.len(d), 0);
  const d2 = new AtomicDict({ x: 1 });
  assert.equal(d2["x"], 1);
});

test("AtomicDict: concurrent increments are consistent", () =>
  macrotask(() => {
  const d = new AtomicDict();
  run_threads(8, () => {
    for (let k = 0; k < 300; k++) d.increment("hits");
  });
  assert.equal(d["hits"], 2400);
  }));

// ── identifiable object ───────────────────────────────────────────────────

test("_LAILA_IDENTIFIABLE_OBJECT: identity and global_id round-trip", () => {
  class Foo extends _LAILA_IDENTIFIABLE_OBJECT {
    static _DEFAULT_SCOPES = ["OBJECT", "FOO"];
  }
  const f = new Foo({ nickname: "alpha" });
  assert.equal(f.uuid, _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("alpha"));
  assert.equal(f.global_id, `LAILA:OBJECT:FOO:${f.uuid}`);
  assert.deepEqual(f.scopes, ["OBJECT", "FOO"]);
  const g = Foo.from_global_id(f.global_id);
  assert.equal(g.global_id, f.global_id);
  assert.equal(new Foo({ uuid: f.uuid, evolution: 3 }).global_id, `LAILA:OBJECT:FOO:${f.uuid}@evolution=3`);
  assert.equal(_LAILA_IDENTIFIABLE_OBJECT.get_uuid_from_global_id(f.global_id), f.uuid);
  assert.deepEqual(_LAILA_IDENTIFIABLE_OBJECT.get_scopes_from_global_id(f.global_id), ["OBJECT", "FOO"]);
  assert.ok(_LAILA_IDENTIFIABLE_OBJECT.is_laila_resource(f.global_id));
  assert.ok(!_LAILA_IDENTIFIABLE_OBJECT.is_laila_resource("nope"));
  assert.deepEqual(f.identity(), { uuid: f.uuid, scopes: ["OBJECT", "FOO"] });
  assert.equal(f.identity_as_json(), `{"uuid": "${f.uuid}", "scopes": ["OBJECT", "FOO"]}`);
  assert.deepEqual(new Foo({ uuid: f.uuid, evolution: 2 }).identity(), { uuid: f.uuid, scopes: ["OBJECT", "FOO"], evolution: 2 });
});

test("_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT: locking surface", () => {
  class Bar extends _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT {}
  const b = new Bar();
  assert.equal(b.locked(), false);
  assert.ok(b.lock());
  assert.ok(b.locked());
  b.unlock();
  assert.equal(b.locked(), false);
  const seen = with_(b.atomic(), (self) => {
    assert.ok(b.locked());
    return self;
  });
  assert.equal(seen, b);
  assert.equal(b.locked(), false);
  assert.ok(b instanceof _LAILA_LOCALLY_ATOMIC_OBJECT);
  assert.ok(b instanceof _LAILA_IDENTIFIABLE_OBJECT);
  assert.ok(!(b instanceof _LAILA_CLI_CAPABLE_CLASS));
});

// ── ensure_list ───────────────────────────────────────────────────────────

test("ensure_list", () => {
  const f = ensure_list("items")(function f(items) {
    return items;
  });
  assert.deepEqual(f("a"), ["a"]);
  const original = ["x", "y"];
  assert.equal(f(original), original);
  const s = new Set([1, 2]);
  assert.equal(f(s), s);
  const fs_ = new T.PyFrozenSet([3, 4]);
  assert.equal(f(fs_), fs_);
  assert.deepEqual(f({ items: "k" }), [{ items: "k" }]); // a positional dict is wrapped (f(items="k") is f("k") in JS)
  assert.equal(f.name, "f");

  const g = ensure_list("x")(function g(x) {
    return x;
  });
  assert.deepEqual(g(42), [42]);
  assert.deepEqual(g(null), [null]);
  assert.deepEqual(g(T.tuple([1, 2])), [T.tuple([1, 2])]);
  const d = { a: 1 };
  assert.deepEqual(g(d), [d]);
  assert.deepEqual(g([]), []);
  assert.deepEqual(g(new Set()), new Set());
  assert.deepEqual(g(true), [true]);

  const noarg = ensure_list("items")(function noarg() {
    return null;
  });
  assert.throws(() => noarg(), (e) => e instanceof TypeError && /items/.test(e.message));

  const dflt = ensure_list("x")(function dflt(x = "default") {
    return x;
  });
  assert.deepEqual(dflt(), ["default"]);

  const multi = ensure_list("b")(function multi(a, b, c = 10) {
    return [a, b, c];
  });
  assert.deepEqual(multi(1, "hello", 20), [1, ["hello"], 20]);

  const wrong = ensure_list("nonexistent")(function wrong(x) {
    return x;
  });
  assert.throws(() => wrong(42), TypeError);

  class K {
    memorize(entries, { pool = null } = {}) {
      return [entries, pool];
    }
  }
  K.prototype.memorize = ensure_list("entries")(K.prototype.memorize);
  assert.deepEqual(new K().memorize("e", { pool: "p" }), [["e"], "p"]);
  assert.deepEqual(_signature((a, b = 1, ...rest) => 0).map((p) => p.name), ["a", "b", "...rest"]);
});

// ── synchronized ──────────────────────────────────────────────────────────

class _LockableStub extends _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT {}

function spy_atomic(fn) {
  const proto = _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT.prototype;
  const orig = proto.atomic;
  const entered = [];
  proto.atomic = function (...args) {
    entered.push(this);
    return orig.apply(this, args);
  };
  try {
    fn(entered);
  } finally {
    proto.atomic = orig;
  }
}

test("synchronized", () => {
  class C {
    work(x) {
      return x + 1;
    }
  }
  C.prototype.work = synchronized(C.prototype.work);
  assert.equal(new C().work(2), 3);

  spy_atomic((entered) => {
    class D {
      work(_target) {
        return entered.length;
      }
    }
    D.prototype.work = synchronized(D.prototype.work);
    const lockable = new _LockableStub();
    assert.equal(new D().work(lockable), 1);
    assert.equal(entered[0], lockable);
  });

  spy_atomic((entered) => {
    class D {
      work(_a, _b) {
        return true;
      }
    }
    D.prototype.work = synchronized(D.prototype.work);
    const a = new _LockableStub();
    const b = new _LockableStub();
    new D().work(a, b);
    assert.deepEqual(entered.map(T.id), T.sorted([T.id(a), T.id(b)]));
  });

  spy_atomic((entered) => {
    class D {
      work({ target = null } = {}) {
        return entered.length;
      }
    }
    D.prototype.work = synchronized(D.prototype.work);
    const lockable = new _LockableStub();
    new D().work({ target: lockable });
    assert.equal(entered.length, 1);
    assert.equal(entered[0], lockable);
  });

  class R {
    work(x) {
      return { key: x };
    }
  }
  R.prototype.work = synchronized(R.prototype.work);
  assert.deepEqual(new R().work(99), { key: 99 });

  class X {
    work() {
      throw new E.ValueError("sync-boom");
    }
  }
  X.prototype.work = synchronized(X.prototype.work);
  assert.throws(() => new X().work(), /sync-boom/);

  spy_atomic((entered) => {
    class D {
      work(a, b, c) {
        return [a, b, c];
      }
    }
    D.prototype.work = synchronized(D.prototype.work);
    const lockable = new _LockableStub();
    new D().work(lockable, 42, "plain");
    assert.equal(entered.length, 1);
    assert.equal(entered[0], lockable);
  });

  spy_atomic((entered) => {
    class D {
      work(_a, _b) {
        return true;
      }
    }
    D.prototype.work = synchronized(D.prototype.work);
    const lockable = new _LockableStub();
    new D().work(lockable, lockable);
    assert.equal(new Set(entered.map(T.id)).size, 1);
    assert.equal(lockable.locked(), false);
  });

  class G {
    work() {}
  }
  G.prototype.work = synchronized(G.prototype.work, { scope: "global" });
  assert.throws(() => new G().work(), E.NotImplementedError);
});

// ── ArgReader ─────────────────────────────────────────────────────────────

function tmp_file(suffix, content) {
  const p = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "laila-args-")), `args${suffix}`);
  fs.writeFileSync(p, content, "utf8");
  return p;
}

test("ArgReader: loaders", () => {
  const args = new AtomicDotMap();
  const reader = new ArgReader(args);
  reader.clear();

  let p = tmp_file(".json", '{"s3_bucket_region":"us-east-1","s3":{"bucket_name":"laila-test-1"}}');
  assert.equal(reader.load(p), undefined);
  assert.equal(args.s3_bucket_region, "us-east-1");
  assert.equal(args.s3_bucket_name, "laila-test-1");

  p = tmp_file(".toml", 's3_bucket_region = "us-west-2"\n[cloudflare]\naccount_id = "abc123"\n');
  reader.load(p);
  assert.equal(args.s3_bucket_region, "us-west-2");
  assert.equal(args.cloudflare_account_id, "abc123");

  p = tmp_file(".env", "S3_BUCKET_REGION=eu-central-1\nRETRIES=3\nUSE_SSL=true\n");
  reader.load(p);
  assert.equal(args.S3_BUCKET_REGION, "eu-central-1");
  assert.equal(args.RETRIES, 3);
  assert.equal(args.USE_SSL, true);

  p = tmp_file(".xml", "<args><s3_bucket_region>ap-south-1</s3_bucket_region><s3><bucket_name>laila-test-2</bucket_name></s3></args>");
  reader.load(p);
  assert.equal(args.s3_bucket_region, "ap-south-1");
  assert.equal(args.s3_bucket_name, "laila-test-2");

  reader.load("terminal", { terminal_args: ["s3_bucket_region=us-east-2", "max_retries=5", "debug=false"] });
  assert.equal(args.s3_bucket_region, "us-east-2");
  assert.equal(args.max_retries, 5);
  assert.equal(args.debug, false);

  reader.load("terminal", { terminal_args: [] });
  reader.load("terminal", { terminal_args: ["noequals", "k=v"] });
  assert.equal(args.k, "v");
  reader.load("terminal", { terminal_args: ["=value", "k=v2"] });
  assert.equal(args.k, "v2");
});

test("ArgReader: errors", () => {
  const reader = new ArgReader(new AtomicDotMap());
  assert.throws(() => reader.load("/nonexistent/path/file.json"), E.FileNotFoundError);
  assert.throws(() => reader.load(tmp_file(".json", "")));
  assert.throws(() => reader.load(tmp_file(".json", "{bad json")));
  assert.throws(() => reader.load(tmp_file(".json", "[1, 2, 3]")), E.ValueError);
  assert.throws(() => reader.load(tmp_file(".toml", "[[[invalid")));
  assert.throws(() => reader.load(tmp_file(".yaml", "key: value")), E.ValueError);
  assert.throws(() => reader.load(tmp_file("", "")), E.ValueError);
});

test("ArgReader: coercion / env edge cases / clear / overwrite", () => {
  const args = new AtomicDotMap();
  const reader = new ArgReader(args);
  reader.load(tmp_file(".env", "A=true\nB=false\nC=TRUE\nD=False\n"));
  assert.equal(args.A, true);
  assert.equal(args.B, false);
  assert.equal(args.C, true);
  assert.equal(args.D, false);
  reader.load(tmp_file(".env", "A=none\nB=null\nC=None\n"));
  assert.equal(args.A, null);
  assert.equal(args.B, null);
  assert.equal(args.C, null);
  reader.load(tmp_file(".env", "I=42\nF=3.14\nG=2.0\n"));
  assert.equal(args.I, 42);
  assert.ok(T.is_int(args.I));
  assert.ok(Math.abs(args.F - 3.14) < 1e-9);
  assert.ok(T.is_float(args.F));
  assert.ok(args.G instanceof T.PyFloat);
  assert.equal(repr(args.G), "2.0");
  reader.load(tmp_file(".env", 'DATA={"a": 1}\n'));
  assert.equal(args.DATA_a, 1);
  reader.load(tmp_file(".env", "DATA=[1,2,3]\n"));
  assert.deepEqual(args.DATA, [1, 2, 3]);
  reader.load(tmp_file(".env", 'NAME="hello world"\n'));
  assert.equal(args.NAME, "hello world");
  reader.load(tmp_file(".env", "\n# comment\n\nKEY=val\n"));
  assert.equal(args.KEY, "val");
  reader.load(tmp_file(".env", "no-equals\nKEY=val2\n"));
  assert.equal(args.KEY, "val2");
  reader.load(tmp_file(".env", "URL=http://host?a=1&b=2\n"));
  assert.equal(args.URL, "http://host?a=1&b=2");

  reader.load(tmp_file(".json", '{"x": 1, "y": 2}'));
  assert.equal(args.x, 1);
  reader.clear();
  assert.equal(args.x, null);
  assert.deepEqual(args.keys(), []);
  reader.clear();

  reader.load(tmp_file(".json", '{"k": "first"}'));
  assert.equal(args.k, "first");
  reader.load(tmp_file(".json", '{"k": "second"}'));
  assert.equal(args.k, "second");

  assert.equal(ArgReader._coerce_scalar(5), 5);
  assert.equal(ArgReader._coerce_scalar("'q'"), "q");
  assert.equal(ArgReader._coerce_scalar("{not json"), "{not json");
  assert.equal(ArgReader._coerce_scalar(" 1_000 "), 1000);
});

// ── logger ────────────────────────────────────────────────────────────────

test("logger.record: normalize_level / numeric_level / build_record", () => {
  assert.equal(LG.normalize_level("debug"), "DEBUG");
  assert.equal(LG.normalize_level(40), "ERROR");
  assert.equal(LG.normalize_level(42), "INFO");
  assert.equal(LG.normalize_level("bogus"), "INFO");
  assert.equal(LG.normalize_level(null), "INFO");
  assert.equal(LG.numeric_level("critical"), 50);
  assert.equal(LG.numeric_level(10), 10);

  class Ident extends _LAILA_IDENTIFIABLE_OBJECT {}
  const pol = new Ident({ nickname: "p" });
  const rec = LG.build_record("memory.memorize", {
    level: "info",
    message: "hi",
    policy_id: pol,
    pool_id: "LAILA:POOL:x",
    pool_nickname: "s3",
    status: "done",
    child_future_ids: [pol, "f2"],
    extra: { k: 1 },
    precedence: 3,
  });
  assert.deepEqual(Object.keys(rec), ["ts", "ts_unix", "level", "event", "extra", "message", "policy_id", "pool_id", "pool_nickname", "precedence", "status", "child_future_ids"]);
  assert.match(rec.ts, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{6})?Z$/);
  assert.ok(typeof rec.ts_unix === "number" || rec.ts_unix instanceof T.PyFloat);
  assert.equal(rec.level, "INFO");
  assert.equal(rec.policy_id, pol.global_id);
  assert.equal(rec.precedence, "3");
  assert.deepEqual(rec.child_future_ids, [pol.global_id, "f2"]);
  assert.deepEqual(rec.extra, { k: 1 });
  const bare = LG.build_record("x");
  assert.deepEqual(Object.keys(bare), ["ts", "ts_unix", "level", "event", "extra"]);
  assert.ok(!("message" in bare));
});

test("Logger: singleton, start/stop, emit to stdlib sink, levels", () => {
  LG.Logger.reset_singleton();
  const a = LG.get_logger();
  const b = new LG.Logger({ level: "WARNING" });
  assert.equal(a, b);
  assert.equal(a.level, "WARNING");
  assert.equal(LG.get_logger(), a);
  assert.ok(a instanceof _LAILA_CLI_CAPABLE_CLASS);
  assert.ok(a instanceof _LAILA_IDENTIFIABLE_OBJECT);
  assert.deepEqual(a.scopes, ["LOGGER"]);
  assert.match(a.global_id, /^LAILA:LOGGER:[0-9a-f-]{36}$/);
  assert.equal(a.enabled, false);
  assert.equal(a.pool_nickname, null);

  const root = logging.getLogger(LG._LAILA_LOGGER_NAME);
  assert.ok(root.handlers.some((h) => h instanceof logging.NullHandler));
  const mem = new logging.MemoryHandler();
  root.addHandler(mem);
  try {
    a.info("dropped-before-enable");
    assert.equal(mem.records.length, 0);

    const stderr_write = process.stderr.write;
    const captured = [];
    process.stderr.write = (chunk) => {
      captured.push(String(chunk));
      return true;
    };
    try {
      LG.enable_logging("INFO");
      assert.equal(a.enabled, true);
      assert.equal(a.display, true); // forced: no pool sink
      assert.equal(a._installed_handlers.length, 1);
      a.debug("too-low");
      a.info("hello", { policy_id: "LAILA:POLICY:abc" });
      a.error("bad");
      LG.enable_logging("INFO"); // idempotent re-start
      assert.equal(a._installed_handlers.length, 1);
    } finally {
      process.stderr.write = stderr_write;
    }
    assert.equal(mem.records.length, 2);
    assert.equal(mem.records[0].levelno, 20);
    assert.match(mem.records[0].getMessage(), /^hello \| \{'ts': '.*'logger_id': '/);
    assert.ok(mem.records[0].getMessage().includes("'policy_id': 'LAILA:POLICY:abc'"));
    assert.ok(captured.some((l) => /\[INFO\] laila: hello \| /.test(l)));
    assert.ok(captured.some((l) => /\[ERROR\] laila: bad \| /.test(l)));

    LG.set_log_level("error");
    assert.equal(a.level, "ERROR");
    assert.equal(root.level, 40);
    a.warning("skip");
    assert.equal(mem.records.length, 2);

    a.record_future_created({ global_id: "LAILA:FUTURE:f1", policy_id: "LAILA:POLICY:p", purpose: "memorize", status: "not_started" });
    assert.equal(mem.records.length, 2); // INFO < ERROR
    LG.set_log_level("DEBUG");
    a.record_future_transition({ global_id: "LAILA:FUTURE:f1", _exception: new E.ValueError("boom") }, "error", "running");
    const last = mem.records.at(-1);
    assert.equal(last.levelno, 40);
    assert.ok(last.getMessage().includes("'exc_repr': \"ValueError('boom')\""));
    assert.ok(last.getMessage().includes("'prev_status': 'running'"));
    a.record_group_future_created({ global_id: "LAILA:FUTURE:g", future_ids: ["a", "b"] });
    assert.ok(mem.records.at(-1).getMessage().includes("group future created with 2 children"));
    a.record_memorize({ entries: { global_id: "LAILA:ENTRY:e" }, pool: { global_id: "LAILA:POOL:p" }, policy: null });
    assert.ok(mem.records.at(-1).getMessage().includes("memorize LAILA:ENTRY:e -> LAILA:POOL:p"));
    a.record_remember({ entry_ids: ["LAILA:ENTRY:e"], pool: null, policy: null });
    assert.ok(mem.records.at(-1).getMessage().includes("remember LAILA:ENTRY:e <- None"));
    a.record_forget({ entry_ids: "LAILA:ENTRY:e", pool: null, policy: null });
    assert.ok(mem.records.at(-1).getMessage().includes("forget LAILA:ENTRY:e -- None"));

    LG.disable_logging();
    assert.equal(a.enabled, false);
    assert.equal(a._installed_handlers.length, 0);
    const n = mem.records.length;
    a.info("after-disable");
    assert.equal(mem.records.length, n);
  } finally {
    root.removeHandler(mem);
    LG.Logger.reset_singleton();
  }
  assert.equal(LG.Logger._singleton, null);
  LG.disable_logging(); // no-op without singleton
});

test("Logger: pool sink failure is recorded, not raised", () => {
  LG.Logger.reset_singleton();
  const stderr_write = process.stderr.write;
  process.stderr.write = () => true;
  try {
    const lg = LG.enable_logging("DEBUG", { pool_nickname: "nope" });
    assert.equal(lg.display, false);
    lg.info("x");
    assert.match(lg._last_pool_sink_error, /ImportError|module 'laila' is not loaded/);
  } finally {
    process.stderr.write = stderr_write;
    LG.Logger.reset_singleton();
  }
});

// ── runtime facade + guarantee (with a stub policy registered as ``laila``) ──

test("runtime: _resolve_future / status / result / exception / wait", () => {
  class Future {
    constructor(gid, { status = "done", result = 1, exception = null } = {}) {
      this.global_id = gid;
      this.status = status;
      this.result = result;
      this.exception = exception;
    }
    wait(timeout) {
      this.waited = timeout;
      return this.result;
    }
  }
  class GroupFuture extends Future {}
  class RemoteFuture extends Future {}
  class _LAILA_IDENTIFIABLE_FUTURE {
    constructor(gid) {
      this.global_id = gid;
    }
  }
  const f1 = new Future("LAILA:FUTURE:1", { result: "r1", exception: null });
  const bank = { "LAILA:FUTURE:1": f1 };
  const stub_policy = { future_bank: bank };
  const prev = Object.fromEntries(lazy_mod.registered_modules().map((n) => [n, lazy_mod.resolve(n)]));
  lazy_mod.register("laila.policy.central.command.schema.future.future.future", { Future });
  lazy_mod.register("laila.policy.central.command.schema.future.future.future_identity", { _LAILA_IDENTIFIABLE_FUTURE });
  lazy_mod.register("laila.policy.central.command.schema.future.future.group_future", { GroupFuture });
  lazy_mod.register("laila.policy.central.command.schema.future.future.remote_future", { RemoteFuture });
  lazy_mod.register("laila", { ...LAILA_STUB, _local_policies: new Map([["p", stub_policy]]) });
  try {
    assert.equal(runtime._resolve_future(f1), f1);
    assert.equal(runtime._resolve_future("LAILA:FUTURE:1"), f1);
    assert.equal(runtime._resolve_future(new _LAILA_IDENTIFIABLE_FUTURE("LAILA:FUTURE:1")), f1);
    assert.throws(() => runtime._resolve_future("LAILA:FUTURE:zzz"), E.KeyError);
    assert.throws(() => runtime._resolve_future(new _LAILA_IDENTIFIABLE_FUTURE("zzz")), E.KeyError);
    assert.throws(() => runtime._resolve_future(42), (e) => e instanceof TypeError && /Cannot resolve future for <class 'int'>/.test(e.message));
    assert.equal(runtime.status("LAILA:FUTURE:1"), "done");
    assert.equal(runtime.result(f1), "r1");
    assert.equal(runtime.exception(f1), null);
    assert.equal(runtime.wait("LAILA:FUTURE:1", 2.5), "r1");
    assert.equal(f1.waited, 2.5);
    runtime.wait(f1);
    assert.equal(f1.waited, null);
  } finally {
    for (const n of lazy_mod.registered_modules()) if (!(n in prev)) lazy_mod.unregister(n);
    for (const [n, ns] of Object.entries(prev)) lazy_mod.register(n, ns);
  }
});

test("guarantee / guarantee_async against a stub command", async () => {
  // A stub command with the ``_guarantee_*`` protocol used by the real one.
  const frames = [];
  const command = {
    _guarantee_enter() {
      frames.push(new Map());
    },
    _guarantee_stack() {
      // scopes are plain dicts (``dict[str, Future]``), as in the real command
      return frames.map((m) => Object.fromEntries(m));
    },
    _guarantee_exit() {
      return [...frames.pop().values()];
    },
    _register(fut) {
      frames[frames.length - 1].set(fut.global_id, fut);
    },
  };
  const stub_laila = { ...LAILA_STUB, get_active_policy: () => ({ central: { command } }) };
  lazy_mod.register("laila", stub_laila);
  try {
    // sync: waits for every registered future, re-raises the first error
    const waited = [];
    const mk = (gid, err = null) => ({
      global_id: gid,
      wait(t) {
        waited.push([gid, t]);
        if (err) throw err;
        return gid;
      },
    });
    assert.ok(guarantee instanceof _Guarantee);
    const r = with_(guarantee, (g) => {
      assert.equal(g, guarantee);
      command._register(mk("a"));
      command._register(mk("b"));
      return 7;
    });
    assert.equal(r, 7);
    assert.deepEqual(waited, [["a", null], ["b", null]]);
    assert.equal(frames.length, 0);

    assert.throws(
      () =>
        with_(guarantee, () => {
          command._register(mk("c", new E.ValueError("c failed")));
          command._register(mk("d", new E.ValueError("d failed")));
        }),
      /c failed/,
    );
    // body exception wins over wait errors
    assert.throws(
      () =>
        with_(guarantee, () => {
          command._register(mk("e", new E.ValueError("e failed")));
          throw new E.RuntimeError("body");
        }),
      /body/,
    );
    assert.equal(frames.length, 0);

    // async: awaitable futures are awaited; errors surface at exit
    assert.ok(guarantee_async instanceof _AsyncGuarantee);
    const settled = [];
    const mk_async = (gid, err = null) => {
      const p = new Promise((res, rej) => setTimeout(() => (err ? rej(err) : res(gid)), 5));
      p.catch(() => {});
      return {
        global_id: gid,
        then: (a, b) => p.then(a, b),
        wait() {
          throw new Error("should not be called for awaitables");
        },
      };
    };
    const out = await with_async(guarantee_async, async () => {
      command._register(mk_async("x"));
      settled.push("body");
      return 9;
    });
    assert.equal(out, 9);
    assert.equal(frames.length, 0);
    assert.equal(guarantee_async._task_stacks.size, 0);

    await assert.rejects(
      with_async(guarantee_async, async () => {
        command._register(mk_async("y", new E.ValueError("y failed")));
        await asyncio.sleep(0.05); // give the watcher time to observe the failure
      }),
      /y failed/,
    );
    assert.equal(frames.length, 0);

    // sync-only futures are adapted through to_thread
    const sync_waits = [];
    await with_async(guarantee_async, async () => {
      command._register({
        global_id: "z",
        wait(t) {
          sync_waits.push(t);
          return "z";
        },
      });
    });
    assert.deepEqual(sync_waits, [null]);

    // nested async scopes compose: inner only waits for its own futures
    const order = [];
    await with_async(guarantee_async, async () => {
      command._register(mk_async("outer"));
      await with_async(guarantee_async, async () => {
        command._register(mk_async("inner"));
        order.push("inner-body");
      });
      order.push("after-inner");
    });
    assert.deepEqual(order, ["inner-body", "after-inner"]);
    assert.equal(frames.length, 0);
  } finally {
    lazy_mod.register("laila", LAILA_STUB);
  }
});

// ── asyncio compat ────────────────────────────────────────────────────────

test("asyncio: Task / wait / wait_for / gather / current_task / primitives", async () => {
  assert.equal(asyncio.current_task(), null);
  const t = asyncio.create_task(async () => {
    const me = asyncio.current_task();
    assert.ok(me instanceof asyncio.Task);
    await asyncio.sleep(0.001);
    return 5;
  }, { name: "t1" });
  assert.equal(t.get_name(), "t1");
  assert.equal(t.done(), false);
  assert.equal(await t, 5);
  assert.equal(t.done(), true);
  assert.equal(t.result(), 5);
  assert.equal(t.exception(), null);

  const failing = asyncio.create_task(async () => {
    throw new E.ValueError("nope");
  });
  await assert.rejects(failing, E.ValueError);
  assert.ok(failing.exception() instanceof E.ValueError);
  assert.throws(() => failing.result(), E.ValueError);

  const slow = asyncio.create_task(() => asyncio.sleep(0.05));
  assert.ok(slow.cancel());
  // the CancelledError is thrown into the body at its ``sleep``; the task
  // only *is* cancelled once the body has let it propagate (CPython)
  assert.ok(!slow.cancelled());
  assert.equal(slow.cancelling(), 1);
  await assert.rejects(slow, E.CancelledError);
  assert.ok(slow.cancelled());
  assert.throws(() => slow.result(), E.CancelledError);
  assert.equal(slow.uncancel(), 0);

  const a = asyncio.create_task(() => asyncio.sleep(0.001, "a"));
  const b = asyncio.create_task(() => asyncio.sleep(0.2, "b"));
  const [done, pending] = await asyncio.wait([a, b], { return_when: asyncio.FIRST_COMPLETED });
  assert.ok(done.has(a) && pending.has(b));
  const [d2, p2] = await asyncio.wait([b], { timeout: 0.01 });
  assert.equal(d2.size, 0);
  assert.equal(p2.size, 1);
  await assert.rejects(asyncio.wait_for(asyncio.sleep(0.1), 0.01), E.TimeoutError);
  assert.equal(await asyncio.wait_for(asyncio.sleep(0.001, "ok"), 1), "ok");
  assert.deepEqual(await asyncio.gather(asyncio.sleep(0.001, 1), asyncio.sleep(0.001, 2)), [1, 2]);
  const g = await asyncio.gather(
    asyncio.sleep(0.001, 1),
    (async () => {
      throw new E.KeyError("k");
    })(),
    { return_exceptions: true },
  );
  assert.equal(g[0], 1);
  assert.ok(g[1] instanceof E.KeyError);
  b.cancel();

  const ev = new asyncio.Event();
  setTimeout(() => ev.set(), 2);
  assert.equal(await ev.wait(), true);
  const lock = new asyncio.Lock();
  await with_async(lock, async () => assert.ok(lock.locked()));
  assert.equal(lock.locked(), false);
  const q = new asyncio.Queue(1);
  q.put_nowait(1);
  assert.throws(() => q.put_nowait(2), asyncio.QueueFull);
  assert.equal(await q.get(), 1);
  assert.throws(() => q.get_nowait(), asyncio.QueueEmpty);
  const fut = new asyncio.Future();
  fut.set_result(3);
  assert.equal(await fut, 3);
  assert.throws(() => fut.set_result(4), E.InvalidStateError);
  const sh = asyncio.shield(asyncio.sleep(0.001, "s"));
  assert.equal(await sh, "s");
});
