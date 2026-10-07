/**
 * Smoke tests for the Python-compat layer (``src/_compat``): repr/json/uuid5/
 * struct/urlquote/datetime/errors/enum/DotMap/pydantic/threading/executor/
 * logging/ndarray/subprocess, each compared against CPython behaviour.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
const C = new URL("../../src/_compat/", import.meta.url).href;
const LAILA_C_VECTORS = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../../laila-c/tests/vectors");
const { repr } = await import(C + "pyrepr.js");
const T = await import(C + "pytypes.js");
const J = await import(C + "pyjson.js");
const U = await import(C + "uuid.js");
const S = await import(C + "struct.js");
const Q = await import(C + "urlquote.js");
const D = await import(C + "datetime.js");
const E = await import(C + "errors.js");
const { Enum } = await import(C + "enum.js");
const { DotMap } = await import(C + "dotmap.js");
const P = await import(C + "pydantic.js");
const TH = await import(C + "threading.js");
const X = await import(C + "executor.js");
const L = await import(C + "logging.js");
const { NDArray } = await import(C + "ndarray.js");
const SP = await import(C + "subprocess.js");
const { deepcopy } = await import(C + "copy.js");


test('repr / float_repr (compared to CPython outputs)', async () => {
  const cases = [
    [3.5, "3.5"], [new T.PyFloat(1e16), "1e+16"], [new T.PyFloat(1e15), "1000000000000000.0"], [0.1, "0.1"], [1e-5, "1e-05"], [0.0001, "0.0001"],
    [-2.5e-10, "-2.5e-10"], [new T.PyFloat(123456789012345680000), "1.2345678901234568e+20"], [1.0, "1"], [1e16, "10000000000000000"], // integral JS number -> int
    [new T.PyFloat(1), "1.0"], [NaN, "nan"], [-Infinity, "-inf"], [true, "True"], [null, "None"],
    ["it's", '"it\'s"'], ['say "hi"', "'say \"hi\"'"], ["a\nb\\", "'a\\nb\\\\'"], ["ünïcödé 😀", "'ünïcödé 😀'"], ["\x01", "'\\x01'"],
    [[1, "a", null], "[1, 'a', None]"], [T.tuple([1]), "(1,)"], [T.tuple([1, 2]), "(1, 2)"], [{ b: 1, a: [2] }, "{'b': 1, 'a': [2]}"],
    [new Map([[1, "x"]]), "{1: 'x'}"], [new Set(), "set()"], [new Uint8Array([0, 1, 254, 255, 39]), "b\"\\x00\\x01\\xfe\\xff'\""],
    [10n ** 30n, "1000000000000000000000000000000"],
  ];
  for (const [v, want] of cases) assert.equal(repr(v), want, `repr(${String(v)})`);
});

test('json', async () => {
  assert.equal(J.dumps({ a: [1, 2.5, "ü", null, true], b: { c: new T.PyFloat(2) } }), '{"a": [1, 2.5, "\\u00fc", null, true], "b": {"c": 2.0}}');
  assert.equal(J.dumps([1, 2], { separators: [",", ":"] }), "[1,2]");
  assert.equal(J.dumps({ a: 1 }, { indent: 2 }), '{\n  "a": 1\n}');
  assert.equal(J.dumps({ k: "😀" }), '{"k": "\\ud83d\\ude00"}');
  assert.equal(J.dumps({ k: "😀" }, { ensure_ascii: false }), '{"k": "😀"}');
  assert.equal(J.dumps(NaN), "NaN");
  const parsed = J.loads('{"a": 1.0, "b": 2, "c": 1e400, "d": 123456789012345678901234567890, "e": [NaN, "\\ud83d\\ude00"]}');
  assert.ok(parsed.a instanceof T.PyFloat && parsed.a.valueOf() === 1);
  assert.equal(parsed.b, 2);
  assert.equal(parsed.c, Infinity);
  assert.equal(parsed.d, 123456789012345678901234567890n);
  assert.ok(Number.isNaN(parsed.e[0]));
  assert.equal(parsed.e[1], "😀");
  assert.ok(J.loads('{"0": 1, "x": 2}') instanceof Map, "index-like keys -> Map");
  assert.throws(() => J.loads("{bad"), E.JSONDecodeError);
  assert.equal(J.dumps(J.loads('{"b": 1, "a": 2}')), '{"b": 1, "a": 2}', "order preserved");
});

test('uuid5 vs python vectors', { skip: fs.existsSync(path.join(LAILA_C_VECTORS, "uuid5.json")) ? false : "laila-c vectors missing" }, async () => {
  const uv = JSON.parse(fs.readFileSync(path.join(LAILA_C_VECTORS, "uuid5.json"), "utf8"));
  for (const c of uv.cases) assert.equal(U.uuid5(uv.namespace, c.nickname).toString(), c.uuid, `uuid5 ${c.nickname}`);
  assert.equal(U.uuid4().toString().length, 36);
});

test('struct', async () => {
  assert.deepEqual([...S.pack(">I", 0xdeadbeef)], [0xde, 0xad, 0xbe, 0xef]);
  assert.deepEqual([...S.pack(">BBIB", 1, 2, 3, 4)], [1, 2, 0, 0, 0, 3, 4]);
  assert.deepEqual([...S.pack(">3sBHH", "LAI", 7, 258, 65535)], [76, 65, 73, 7, 1, 2, 255, 255]);
  assert.deepEqual(S.unpack(">3sBHH", S.pack(">3sBHH", Buffer.from("LAI"), 7, 258, 65535)).slice(1), [7, 258, 65535]);
  assert.deepEqual(S.unpack("<q", S.pack("<q", -5)), [-5]);
  assert.equal(S.calcsize(">BBIB"), 7);
});

test('quote', async () => {
  assert.equal(Q.quote("a b/c?ü", { safe: "" }), "a%20b%2Fc%3F%C3%BC");
  assert.equal(Q.quote("a b/c"), "a%20b/c");
  assert.equal(Q.unquote("a%20b%2Fc%3F%C3%BC"), "a b/c?ü");
});

test('datetime', async () => {
  assert.match(D.now_iso_ms(), /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}\+00:00$/);
  assert.match(D.now_iso_us(), /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}\+00:00$/);
  assert.equal(D.fromisoformat("2026-09-29T17:39:18.890+00:00").toISOString(), "2026-09-29T17:39:18.890Z");
  assert.equal(D.ts_to_iso_z(1700000000.5), "2023-11-14T22:13:20.500000Z");
  assert.equal(D.ts_to_iso_z(1700000000), "2023-11-14T22:13:20Z");
});

test('errors', async () => {
  try { throw new E.KeyError("x"); } catch (e) { assert.equal(e.message, "'x'"); assert.ok(e instanceof E.LookupError); assert.ok(e instanceof Error); }
  assert.ok(new E.TypeError("t") instanceof globalThis.TypeError);
  assert.ok(new E.TypeError("t") instanceof E.TypeError);
  assert.equal(String(new E.ValueError("bad")), "ValueError: bad");
});

const FS = Enum("FutureStatus", { NOT_STARTED: "not_started", FINISHED: "finished" });

test('enum', async () => {
  assert.ok(FS.FINISHED == "finished");
  assert.equal(`${FS.FINISHED}`, "FutureStatus.FINISHED");
  assert.equal(FS.FINISHED.value, "finished");
  assert.equal(FS("finished"), FS.FINISHED);
  assert.ok(FS.FINISHED instanceof FS);
  assert.equal(repr(FS.FINISHED), "<FutureStatus.FINISHED: 'finished'>");
  assert.throws(() => FS("zzz"), E.ValueError);
  assert.equal(J.dumps({ s: FS.FINISHED }), '{"s": "finished"}');
  assert.ok(T.eq(FS.FINISHED, "finished"));
  const m = new Map([[FS.FINISHED, 1]]); assert.equal(m.get(FS("finished")), 1);
});

test('DotMap', async () => {
  const d = new DotMap();
  d.a.b = 1;
  d.c = { x: { y: 2 } };
  assert.equal(d.a.b, 1);
  assert.ok(d.a instanceof DotMap);
  assert.equal(T.is_plain_object(d.c), true, "plain dict stored as-is on set");
  assert.deepEqual(d.toDict(), { a: { b: 1 }, c: { x: { y: 2 } } });
  assert.equal(T.len(d), 2);
  assert.equal("zz" in d, false);
  assert.equal(d.get("zz"), null);
  assert.equal("zz" in d, false, "get does not autovivify");
  assert.equal(String(d), "DotMap(a=DotMap(b=1), c={'x': {'y': 2}})");
  const e2 = new DotMap({ p: { q: 1 } });
  assert.ok(e2.p instanceof DotMap);
  assert.ok(e2.__eq__(new DotMap({ p: { q: 1 } })) && e2.__eq__({ p: { q: 1 } }) && new DotMap().__eq__({}));
  assert.ok(T.eq(e2, { p: { q: 1 } }));
  assert.equal(T.bool(new DotMap()), false);
  assert.equal(d.then, undefined);
  const awaited = await d; assert.equal(awaited, d);
  assert.equal(JSON.stringify(e2), '{"p":{"q":1}}');
  class Args extends DotMap { __setitem__(k, v) { if (k === "env") this.hooked = v; super.__setitem__(k, v); } }
  const a = new Args(); a.env = 5; assert.equal(a.get("env"), 5); assert.equal(a.hooked, 5);
  assert.ok(a.nested instanceof Args, "autovivified child uses subclass");
  assert.deepEqual(Object.keys(d), ["a", "c"]);
  delete d.c; assert.equal("c" in d, false);
});

test('pydantic', async () => {
  const { BaseModel, Field, PrivateAttr, ConfigDict, define_fields, define_private, model_validator, field_validator, ValidationError, finalize_model, Annotated, BeforeValidator } = P;
  class M extends BaseModel {
    static model_config = ConfigDict({ arbitrary_types_allowed: true });
    static {
      define_fields(this, {
        i: ["int", 0], f: ["float", 0.0], s: ["str", ""], b: ["bool", false],
        ls: ["list[str]", Field({ default_factory: () => [] })], d: ["dict[str, Any]", Field({ default_factory: () => ({}) })],
        o: ["str | None", null], e: [FS, FS.NOT_STARTED], g: ["int", Field({ default: 1, ge: 1 })], st: ["set[str] | None", null], a: ["Any", null],
      });
      define_private(this, { _p: PrivateAttr({ default: null }), _lock: PrivateAttr({ default_factory: () => new TH.RLock() }) });
    }
  }
  const ok = (kw) => new M(kw);
  assert.equal(ok({ i: true }).i, 1);
  assert.equal(ok({ i: 3.0 }).i, 3);
  assert.throws(() => ok({ i: 3.5 }), (e) => e instanceof ValidationError && e.errors()[0].type === "int_from_float");
  assert.equal(ok({ i: "7" }).i, 7);
  assert.throws(() => ok({ i: "x" }), (e) => e.errors()[0].type === "int_parsing");
  assert.throws(() => ok({ i: null }), (e) => e.errors()[0].type === "int_type");
  assert.equal(ok({ f: "2.5" }).f, 2.5);
  assert.ok(Number.isNaN(ok({ f: "nan" }).f));
  assert.throws(() => ok({ s: 3 }), (e) => e.errors()[0].type === "string_type");
  assert.equal(ok({ s: Buffer.from("x") }).s, "x");
  assert.equal(ok({ b: "yes" }).b, true);
  assert.equal(ok({ b: "off" }).b, false);
  assert.throws(() => ok({ b: 2 }), (e) => e.errors()[0].type === "bool_parsing");
  assert.deepEqual(ok({ ls: T.tuple(["a", "b"]) }).ls, ["a", "b"]);
  assert.throws(() => ok({ ls: [1] }), (e) => e.errors()[0].type === "string_type" && e.errors()[0].loc[1] === 0);
  assert.throws(() => ok({ ls: "ab" }), (e) => e.errors()[0].type === "list_type");
  assert.throws(() => ok({ d: [["a", 1]] }), (e) => e.errors()[0].type === "dict_type");
  assert.equal(ok({ o: null }).o, null);
  assert.throws(() => ok({ o: 5 }), (e) => e.errors()[0].type === "string_type");
  assert.equal(ok({ e: "finished" }).e, FS.FINISHED);
  assert.throws(() => ok({ e: "z" }), (e) => e.errors()[0].msg === "Input should be 'not_started' or 'finished'");
  assert.throws(() => ok({ g: 0 }), (e) => e.errors()[0].type === "greater_than_equal");
  assert.ok(ok({ st: ["a", "a"] }).st instanceof Set && ok({ st: ["a", "a"] }).st.size === 1);
  const m1 = ok({ i: 5, zzz: 1 });
  assert.deepEqual([...m1.model_fields_set], ["i"]);
  assert.deepEqual(Object.keys(M.model_fields), ["i", "f", "s", "b", "ls", "d", "o", "e", "g", "st", "a"]);
  assert.equal(repr(m1), "M(i=5, f=0.0, s='', b=False, ls=[], d={}, o=None, e=<FutureStatus.NOT_STARTED: 'not_started'>, g=1, st=None, a=None)".replace("f=0.0", "f=0"));
  assert.ok(!T.eq(m1, ok({ i: 5 })), "distinct RLock privates -> unequal (pydantic semantics)");
  class Eq extends BaseModel { static { define_fields(this, { i: ["int", 0] }); } }
  assert.ok(T.eq(new Eq({ i: 5 }), new Eq({ i: 5 })) && !T.eq(new Eq({ i: 5 }), new Eq({ i: 6 })));
  assert.equal(m1._p, null); m1._p = 3; assert.equal(m1._p, 3); assert.equal(m1.__pydantic_private__._p, 3);
  assert.ok(m1._lock instanceof TH.RLock);
  assert.equal(M.__private_attributes__._lock.default_factory !== null, true);
  try { ok({ i: "x", g: "y" }); } catch (e) { assert.equal(e.error_count(), 2); assert.ok(e.message.startsWith("2 validation errors for M\ni\n  Input should be a valid integer, unable to parse string as an integer [type=int_parsing, input_value='x', input_type=str]\ng\n")); }
  m1.i = "9"; assert.equal(m1.i, "9", "no validate_assignment");
  class R extends BaseModel { static { define_fields(this, { r: ["int"] }); } }
  assert.throws(() => new R(), (e) => e.errors()[0].type === "missing" && e.errors()[0].msg === "Field required");
  class F extends BaseModel { static model_config = ConfigDict({ extra: "forbid" }); static { define_fields(this, { x: ["int", 1] }); } }
  assert.throws(() => new F({ y: 2 }), (e) => e.errors()[0].type === "extra_forbidden");
  class Uem extends BaseModel { static model_config = ConfigDict({ use_enum_values: true }); static { define_fields(this, { e: [FS, FS.NOT_STARTED] }); } }
  assert.equal(new Uem({ e: FS.FINISHED }).e, "finished"); assert.equal(new Uem().e, FS.NOT_STARTED, "default not coerced");
  // model_construct / model_copy / model_dump
  const c = M.model_construct({ i: "raw" }); assert.equal(c.i, "raw"); assert.deepEqual([...c.model_fields_set], ["i"]); assert.equal(c.g, 1);
  assert.equal(m1.model_copy({ update: { i: 11 } }).i, 11);
  const dump = ok({ i: 5 }).model_dump(); assert.deepEqual(Object.keys(dump), Object.keys(M.model_fields)); assert.equal(dump.e, FS.NOT_STARTED);
  class Xm extends BaseModel { static { define_fields(this, { k: ["str | None", Field({ default: null, exclude: true, repr: false })], z: ["int", 1] }); } }
  assert.equal(repr(new Xm({ k: "s" })), "Xm(z=1)"); assert.deepEqual(new Xm({ k: "s" }).model_dump(), { z: 1 });
  // inheritance + post_init chain + validators
  const order = [];
  class Base extends BaseModel {
    static { define_fields(this, { a: ["int", 1] }); define_private(this, { _ts: PrivateAttr({ default: null }) }); model_validator(this, "before", (cls, data) => { order.push("before:" + cls.name); if (!("a" in data)) data.a = 42; return data; }); model_validator(this, "after", (self) => { order.push("after"); }); }
    constructor(data = {}) { super(data); this._ts = "stamped"; order.push("ctor-tail"); }
    model_post_init(ctx) { super.model_post_init(ctx); order.push("post_init:Base"); }
  }
  class Child extends Base {
    static { define_fields(this, { b: ["str", "x"], a: ["int", 2] }); field_validator(this, "b", (cls, v) => v.toUpperCase()); }
    model_post_init(ctx) { super.model_post_init(ctx); order.push("post_init:Child"); this.seen_ts = this._ts; }
  }
  const ch = new Child({ b: "hi" });
  assert.deepEqual(order, ["before:Child", "post_init:Base", "post_init:Child", "after", "ctor-tail"]);
  assert.equal(ch.b, "HI"); assert.equal(ch.a, 42); assert.equal(ch._ts, "stamped"); assert.equal(ch.seen_ts, null);
  assert.deepEqual(Object.keys(Child.model_fields), ["a", "b"], "overridden field keeps parent position");
  assert.deepEqual(Base.__subclasses__(), [Child]);
  class Nested extends BaseModel { static { define_fields(this, { inner: [Child, Field({ default_factory: () => new Child() })], maybe: ["Child | None", null] }); } }
  assert.ok(new Nested({ inner: { b: "q" } }).inner instanceof Child && new Nested({ inner: { b: "q" } }).inner.b === "Q");
  assert.ok(new Nested({ maybe: { b: "z" } }).maybe instanceof Child, "string annotation resolves registered class");
  const ann = Annotated("int", BeforeValidator((v) => (v === "seven" ? 7 : v)));
  class An extends BaseModel { static { define_fields(this, { n: [ann, 0] }); } }
  assert.equal(new An({ n: "seven" }).n, 7);
  // deepcopy of a model
  const dc = deepcopy(ch); assert.ok(dc instanceof Child && dc !== ch && dc.b === "HI" && dc._ts === "stamped");
  assert.ok(T.eq(dc, ch));
});

test('threading / executor (run from a macrotask so blocking waits are legal)', async () => {
  await new Promise((resolve, reject) => setImmediate(() => { try { sync_part(); resolve(); } catch (e) { reject(e); } }));
  function sync_part() {
    const lock = new TH.RLock();
    lock.acquire(); lock.acquire(); assert.ok(lock.locked()); lock.release(); lock.release(); assert.ok(!lock.locked());
    const ev = new TH.Event();
    setTimeout(() => ev.set(), 10);
    assert.equal(ev.wait(1), true);
    const ev2 = new TH.Event(); assert.equal(ev2.wait(0.02), false);
    // lock contention across contexts: a Thread holds the lock, main blocks on acquire
    const l2 = new TH.RLock(); let order2 = [];
    const th = new TH.Thread({ target: () => { l2.acquire(); order2.push("t-acq"); return new Promise((r) => setTimeout(() => { l2.release(); order2.push("t-rel"); r(); }, 15)); } });
    th.start();
    TH.blocking_wait(() => order2.length >= 1, 1);
    // holding a lock across a blocking wait is rejected (rule L1)
    const th_bad = new TH.Thread({ target: () => { const lb = new TH.RLock(); lb.acquire(); try { new TH.Event().wait(0.1); } finally { lb.release(); } } });
    th_bad.start(); th_bad.join(1); assert.ok(th_bad.exception instanceof TH.BlockingNotPossibleError);
    assert.equal(l2.acquire({ blocking: false }), false, "held by another context");
    assert.equal(l2.acquire({ timeout: 1 }), true); order2.push("m-acq"); l2.release();
    assert.deepEqual(order2, ["t-acq", "t-rel", "m-acq"]);
    th.join(1); assert.equal(th.is_alive(), false);
    // L1 guard
    const l3 = new TH.RLock(); l3.acquire();
    assert.throws(() => new TH.Event().wait(0.1), TH.BlockingNotPossibleError);
    l3.release();
    // executor
    const ex = new X.ThreadPoolExecutor({ max_workers: 2 });
    const futs = [1, 2, 3].map((n) => ex.submit((x) => x * 2, n));
    assert.deepEqual(futs.map((f) => f.result(1)), [2, 4, 6]);
    const bad = ex.submit(() => { throw new E.ValueError("boom"); });
    assert.throws(() => bad.result(1), E.ValueError);
    const af = ex.submit(async (x) => { await new Promise((r) => setTimeout(r, 5)); return x + 1; }, 1);
    assert.equal(af.result(1), 2);
    ex.shutdown({ wait: true });
    // Condition
    const cond = new TH.Condition(); let ready = false;
    setTimeout(() => { cond.acquire(); ready = true; cond.notify_all(); cond.release(); }, 10);
    cond.acquire(); assert.equal(cond.wait_for(() => ready, 1), true); cond.release();
    // Semaphore
    const sem = new TH.Semaphore(1); assert.ok(sem.acquire()); assert.equal(sem.acquire({ blocking: false }), false); sem.release();
    // threading.local per context
    const loc = TH.local(); loc.x = 1;
    let seen; const th2 = new TH.Thread({ target: () => { seen = loc.x; loc.x = 2; } }); th2.start(); th2.join(1);
    assert.equal(seen, undefined); assert.equal(loc.x, 1);
  }
});

test('pump / hop re-entrancy (nested uv_run inside Node immediates and hops)', async () => {
  const PU = await import(C + "pump.js");
  const A = await import(C + "asyncio.js");
  if (!PU.pump_available()) return;
  // hops: FIFO, delivered on the next macrotask, never keep the process alive on their own
  const seen = [];
  PU.hop(() => seen.push(1)); PU.hop(() => seen.push(2));
  await new Promise((r) => setTimeout(r, 50));
  assert.deepEqual(seen, [1, 2]);
  // a pump started from the 2nd immediate of a batch must not abort the process
  // (Node's processImmediate is not re-entrant); hops keep running inside it,
  // Node's own immediates are deferred until it unwinds
  const log = [];
  await new Promise((resolve, reject) => {
    setImmediate(() => log.push("imm1"));
    setImmediate(() => {
      try {
        let done = false;
        setTimeout(() => { done = true; }, 10);
        setImmediate(() => log.push("node-immediate"));
        PU.hop(() => log.push("hop"));
        assert.equal(PU.pump_until(() => done, 2), true);
        log.push("pumped");
        setImmediate(resolve);
      } catch (e) { reject(e); }
    });
  });
  assert.deepEqual(log, ["imm1", "hop", "pumped", "node-immediate"]);
  // nested pump inside a hop inside an immediate (suspension bookkeeping)
  await new Promise((resolve, reject) => {
    setImmediate(() => {});
    setImmediate(() => {
      try {
        let a = false, b = false;
        PU.hop(() => { setTimeout(() => { b = true; }, 5); assert.equal(PU.pump_until(() => b, 2), true); a = true; });
        assert.equal(PU.pump_until(() => a, 2), true);
        resolve();
      } catch (e) { reject(e); }
    });
  });
  // a throwing hop and a throwing nextTick inside a pump surface as uncaughtException,
  // do not leak into the waiting frame and do not corrupt the async-hooks stack
  // (node:test captures uncaught exceptions itself, so this runs in a child process)
  const { execFileSync } = await import("node:child_process");
  const child = `
    const PU = await import(${JSON.stringify(C + "pump.js")});
    const caught = [];
    process.on("uncaughtException", (e) => caught.push(e.message));
    const seen = [];
    PU.hop(() => { throw new Error("hop-boom"); });
    PU.hop(() => seen.push(3));
    await new Promise((r) => setTimeout(r, 5));
    await new Promise((resolve, reject) => setTimeout(() => {
      let done = false;
      setTimeout(() => { done = true; }, 20);
      setTimeout(() => process.nextTick(() => { throw new Error("tick-boom"); }), 2);
      const ok = PU.pump_until(() => done, 2);
      ok ? resolve() : reject(new Error("pump timed out"));
    }, 0));
    await new Promise((r) => setTimeout(r, 5));
    console.log(JSON.stringify({ seen, caught }));
  `;
  const out = execFileSync(process.execPath, ["--input-type=module", "-e", child], { encoding: "utf8", timeout: 20000 });
  assert.deepEqual(JSON.parse(out.trim().split("\n").pop()), { seen: [3], caught: ["hop-boom", "tick-boom"] });
  // asyncio.sleep is interrupted by task cancellation (timer cleared, CancelledError at the await)
  let outcome = null;
  const t0 = performance.now();
  const task = A.create_task(async () => { try { await A.sleep(30); outcome = "slept"; } catch (e) { outcome = e instanceof A.CancelledError ? "cancelled" : e; } });
  await new Promise((r) => setTimeout(r, 5));
  task.cancel();
  await new Promise((r) => setTimeout(r, 5));
  assert.equal(outcome, "cancelled");
  assert.ok(performance.now() - t0 < 1000);
});

test('logging', async () => {
  const lg = L.getLogger("laila.test"); const mh = new L.MemoryHandler(); L.getLogger("laila").addHandler(mh); L.getLogger("laila").setLevel(L.DEBUG);
  lg.info("hello %s", "world"); lg.debug("dbg");
  assert.deepEqual(mh.output, ["INFO:laila.test:hello world", "DEBUG:laila.test:dbg"]);
  assert.equal(L.getLogger("laila.test.sub").parent, lg);
});

test('ndarray', async () => {
  const arr = NDArray.array([[1.5, 2.5], [3.5, 4.5]], "<f8");
  assert.deepEqual(arr.shape, [2, 2]); assert.deepEqual(arr.tolist(), [[1.5, 2.5], [3.5, 4.5]]);
  assert.equal(arr.tobytes().toString("base64"), Buffer.from(new Float64Array([1.5, 2.5, 3.5, 4.5]).buffer).toString("base64"));
  assert.deepEqual(NDArray.arange(6, "|u1").reshape([2, 3]).tolist(), [[0, 1, 2], [3, 4, 5]]);
  assert.ok(T.eq(NDArray.array([1, 2, 3], "<i4"), NDArray.array([1, 2, 3], "<i4")));
});

test('subprocess', async () => {
  const cp = SP.run(["echo", "hi"], { capture_output: true, text: true });
  assert.equal(cp.returncode, 0); assert.equal(cp.stdout, "hi\n");
  assert.ok(SP.which("echo"));
  assert.throws(() => SP.run(["false"], { check: true }), SP.CalledProcessError);
});
