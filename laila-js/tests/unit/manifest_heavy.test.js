/**
 * Heavy manifest workloads on starved taskforces: port of
 *   tests/functional/policy/memory/manifest/unit_tests/test_manifest_heavy_workloads.py
 *
 * These tests exercise the real recursion paths in laila -- ``laila.build``
 * -> ``ComplexConstitution`` -> ``manifest.async_realized`` ->
 * ``laila.remember`` -> per-entry fetch coroutines, plus constitution bodies
 * that themselves call ``manifest.realized`` / ``laila.build`` -- while the
 * alpha and internal taskforces are shrunk to one loop thread with one or
 * two slots each. Without slot parking every one of these would deadlock;
 * with it they must complete *and* produce the right values.
 *
 * All waits carry a timeout so a scheduler regression fails loudly instead
 * of hanging the suite.
 *
 * Constitution bodies are JavaScript here (laila-js executes constitution
 * source with ``new Function``); Python's ``builtins`` registry becomes a
 * ``globalThis`` slot and ``import laila`` inside the body becomes a second
 * ``globalThis`` slot holding the laila root.
 */
import { test, describe, beforeEach } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const { default: laila, S } = await import("./fixtures/laila_root.js");

const TH = await import(S + "_compat/threading.js");
const { with_ } = await import(S + "_compat/contextlib.js");
const { dict_set, dict_has, dict_del } = await import(S + "_compat/pytypes.js");
const { Entry, EntryState } = await import(S + "entry/index.js");
const { CyclicDependencyError } = await import(S + "policy/central/command/schema/parking.js");
const { PythonAsyncThreadPoolTaskForce } = await import(S + "policy/central/command/taskforce/async_thread_pool_executor/index.js");
const { Manifest } = await import(S + "policy/central/memory/schema/manifest.js");

const TMP_ROOT = fs.mkdtempSync(path.join(os.tmpdir(), "laila_manifest_heavy_"));
laila.set_default_directory(TMP_ROOT);

const _T = 120.0;
const _REGISTRY_NAME = "_laila_heavy_manifest_test_registry";
// ``import laila`` inside a constitution body: the root is published on
// ``globalThis`` for the duration of the tests that need it.
const _LAILA_NAME = "_laila_heavy_manifest_test_laila";

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

const range = (n) => Array.from({ length: n }, (_, i) => i);
const sum = (xs) => xs.reduce((a, b) => a + b, 0);

/** ``while hasattr(v, "data"): v = v.data`` */
function _unwrap(v) {
  while (v !== null && v !== undefined && typeof v === "object" && "data" in v) v = v.data;
  return v;
}

/** Swap the command's alpha/internal taskforces for 1-thread pools. */
class _TinyTaskforces {
  constructor(opts = {}) {
    const { alpha_slots = 1, internal_slots = 1, sync_workers = 2 } = opts;
    this.alpha_slots = alpha_slots;
    this.internal_slots = internal_slots;
    this.sync_workers = sync_workers;
  }

  __enter__() {
    const cmd = laila.command;
    this._saved = [cmd.alpha_taskforce, cmd.internal_taskforce];
    const pid = laila.active_policy.global_id;
    this.alpha = new PythonAsyncThreadPoolTaskForce({
      policy_id: pid,
      num_workers: 1,
      max_async_per_thread: this.alpha_slots,
      sync_workers: this.sync_workers,
      rank: 2,
    });
    this.internal = new PythonAsyncThreadPoolTaskForce({
      policy_id: pid,
      num_workers: 1,
      max_async_per_thread: this.internal_slots,
      sync_workers: this.sync_workers,
      rank: 1,
    });
    dict_set(cmd.taskforces, this.alpha.global_id, this.alpha);
    dict_set(cmd.taskforces, this.internal.global_id, this.internal);
    cmd.alpha_taskforce = this.alpha.global_id;
    cmd.internal_taskforce = this.internal.global_id;
    return this;
  }

  __exit__(..._exc) {
    const cmd = laila.command;
    [cmd.alpha_taskforce, cmd.internal_taskforce] = this._saved;
    for (const tf of [this.alpha, this.internal]) {
      try {
        tf.shutdown({ wait: true, cancel_pending: true });
      } finally {
        if (dict_has(cmd.taskforces, tf.global_id)) dict_del(cmd.taskforces, tf.global_id);
      }
    }
    return false;
  }

  assert_quiescent() {
    for (const tf of [this.alpha, this.internal]) {
      assert.equal(tf.inflight, 0, `${tf.global_id} still has slots occupied`);
      assert.equal(tf.parked, 0, `${tf.global_id} still has parked waiters`);
    }
  }
}

const _SUM_ALL =
  "function combine(manifest) {\n" + //
  "  const d = manifest.realized;\n" +
  "  return Object.values(d).reduce((s, e) => s + e.data, 0);\n" +
  "}\n";

// Walks a chain of nested manifests: every level has an 'x' leaf and an
// optional 'inner' manifest. Each ``.realized`` on an inner manifest is a
// fresh nested remember issued from the sync body (executor thread).
const _WALK_NESTED =
  "function walk(manifest) {\n" +
  "  let total = 0;\n" +
  "  let cur = manifest;\n" +
  "  let depth = 0;\n" +
  "  while (cur !== null && cur !== undefined) {\n" +
  "    const d = cur.realized;\n" +
  "    total += d['x'].data;\n" +
  "    cur = d['inner'] ?? null;\n" +
  "    depth += 1;\n" +
  "  }\n" +
  "  return [total, depth];\n" +
  "}\n";

/** Constitution that builds another STAGED entry (from a registry) first. */
function _build_body_for(key) {
  return (
    "function dep(manifest) {\n" +
    `  const laila = globalThis[${JSON.stringify(_LAILA_NAME)}];\n` +
    `  const reg = globalThis[${JSON.stringify(_REGISTRY_NAME)}];\n` +
    `  const prev = reg[${JSON.stringify(key)}];\n` +
    "  if (prev.state.name !== 'READY') {\n" +
    "    laila.build(prev).wait(120);\n" +
    "  }\n" +
    "  const d = manifest.realized;\n" +
    "  return prev.data + d['x'].data;\n" +
    "}\n"
  );
}

/** ``setattr(builtins, _REGISTRY_NAME, registry)`` ... ``delattr`` */
function _install_registry(registry) {
  globalThis[_REGISTRY_NAME] = registry;
  globalThis[_LAILA_NAME] = laila;
}
function _remove_registry() {
  delete globalThis[_REGISTRY_NAME];
  delete globalThis[_LAILA_NAME];
}

describe("TestHeavyManifestWorkloads", () => {
  beforeEach(() => macrotask(() => void laila.get_active_policy()));

  // ------------------------------------------------------------------
  // W1: wide fan-out -- many concurrent builds over one wide manifest
  // ------------------------------------------------------------------
  t("001_wide_manifest_many_concurrent_builds_on_1x1", () => {
    // 20 parents x (150 async_realized + 150 body-realized) fetches, all
    // funnelled through a single internal slot.
    const [n_leaves, n_builds] = [150, 20];
    const leaves = range(n_leaves).map((i) => Entry.constant(i));
    const manifest = new Manifest({ data: Object.fromEntries(leaves.map((e, i) => [`k${i}`, e])) });
    with_(new _TinyTaskforces({ alpha_slots: 1, internal_slots: 1 }), (tfs) => {
      manifest.memorize().wait(_T);
      const entries = range(n_builds).map(() => Entry.variable(null, { constitution: _SUM_ALL, manifest }));
      const futs = entries.map((e) => laila.build(e));
      for (const f of futs) f.wait(_T);
      const expected = sum(range(n_leaves));
      for (const e of entries) {
        assert.equal(e.state, EntryState.READY);
        assert.equal(e.data, expected);
      }
      tfs.assert_quiescent();
    });
  });

  // ------------------------------------------------------------------
  // W2: deep nesting -- bodies recursively realize nested manifests
  // ------------------------------------------------------------------
  t("002_nested_manifest_chain_walked_from_sync_bodies", () => {
    const depth = 8;
    with_(new _TinyTaskforces({ alpha_slots: 1, internal_slots: 1 }), (tfs) => {
      let inner = null;
      for (const level of range(depth)) {
        const payload = { x: Entry.constant(level) };
        if (inner !== null) payload.inner = inner;
        const m = new Manifest({ data: payload });
        m.memorize().wait(_T);
        inner = m;
      }
      const outer = inner;

      const n_builds = 12;
      const entries = range(n_builds).map(() => Entry.variable(null, { constitution: _WALK_NESTED, manifest: outer }));
      const futs = entries.map((e) => laila.build(e));
      for (const f of futs) f.wait(_T);
      for (const e of entries) {
        const [total, seen_depth] = e.data;
        assert.equal(seen_depth, depth);
        assert.equal(total, sum(range(depth)));
      }
      tfs.assert_quiescent();
    });
  });

  // ------------------------------------------------------------------
  // W3: shared leaves -- many manifests over the same leaf set
  // ------------------------------------------------------------------
  t("003_many_manifests_sharing_leaves_built_concurrently", () => {
    const leaves = range(60).map((i) => Entry.constant(i));
    with_(new _TinyTaskforces({ alpha_slots: 2, internal_slots: 2 }), (tfs) => {
      laila.memorize(leaves).wait(_T);
      const manifests = [];
      for (const j of range(25)) {
        // rotating window of 20 leaves each
        const window = range(20).map((k) => leaves[(j + k) % leaves.length]);
        const m = new Manifest({ data: Object.fromEntries(window.map((e, k) => [`k${k}`, e])) });
        manifests.push([m, sum(window.map((e) => e.data))]);
      }
      for (const [m] of manifests) m.memorize().wait(_T);
      const entries = manifests.map(([m, expected]) => [Entry.variable(null, { constitution: _SUM_ALL, manifest: m }), expected]);
      const futs = entries.map(([e]) => laila.build(e));
      for (const f of futs) f.wait(_T);
      for (const [e, expected] of entries) assert.equal(e.data, expected);
      tfs.assert_quiescent();
    });
  });

  // ------------------------------------------------------------------
  // W4: build chain -- each body builds the previous STAGED entry first
  // ------------------------------------------------------------------
  t("004_build_chain_through_constitution_bodies_on_1x1", () => {
    const depth = 15;
    const registry = {};
    _install_registry(registry);
    try {
      with_(new _TinyTaskforces({ alpha_slots: 1, internal_slots: 1 }), (tfs) => {
        const base_manifest = new Manifest({ data: { x: Entry.constant(1) } });
        base_manifest.memorize().wait(_T);
        // Level 0 has no dependency: plain sum.
        let prev = Entry.variable(null, { constitution: _SUM_ALL, manifest: base_manifest });
        registry["e0"] = prev;
        for (let i = 1; i <= depth; i++) {
          const m = new Manifest({ data: { x: Entry.constant(1) } });
          m.memorize().wait(_T);
          const e = Entry.variable(null, { constitution: _build_body_for(`e${i - 1}`), manifest: m });
          registry[`e${i}`] = e;
          prev = e;
        }
        // Build only the top; every level below is built recursively
        // from inside the constitution bodies.
        laila.build(registry[`e${depth}`]).wait(_T);
        for (let i = 0; i <= depth; i++) {
          assert.equal(registry[`e${i}`].state, EntryState.READY);
          assert.equal(registry[`e${i}`].data, i + 1);
        }
        tfs.assert_quiescent();
      });
    } finally {
      _remove_registry();
    }
  });

  // ------------------------------------------------------------------
  // W5: cyclic dependency between two constitutions is detected
  // ------------------------------------------------------------------
  t("005_cyclic_build_dependency_raises_instead_of_hanging", () => {
    const registry = {};
    _install_registry(registry);
    try {
      with_(new _TinyTaskforces({ alpha_slots: 2, internal_slots: 2 }), () => {
        const ma = new Manifest({ data: { x: Entry.constant(1) } });
        const mb = new Manifest({ data: { x: Entry.constant(2) } });
        ma.memorize().wait(_T);
        mb.memorize().wait(_T);
        const a = Entry.variable(null, { constitution: _build_body_for("b"), manifest: ma });
        const b = Entry.variable(null, { constitution: _build_body_for("a"), manifest: mb });
        [registry["a"], registry["b"]] = [a, b];
        assert.throws(() => laila.build(a).wait(_T), CyclicDependencyError);
        assert.equal(a.state, EntryState.STAGED);
        assert.equal(b.state, EntryState.STAGED);
      });
    } finally {
      _remove_registry();
    }
  });

  // ------------------------------------------------------------------
  // W6: concurrent sync realize from many user threads on 1 slot
  // ------------------------------------------------------------------
  t("006_concurrent_realized_from_threads_on_1x1", () => {
    const leaves = range(50).map((i) => Entry.constant(i));
    const manifest = new Manifest({ data: Object.fromEntries(leaves.map((e, i) => [`k${i}`, e])) });
    with_(new _TinyTaskforces({ alpha_slots: 1, internal_slots: 1 }), (tfs) => {
      manifest.memorize().wait(_T);
      const results = [];
      const errors = [];
      const lock = new TH.Lock();

      const worker = () => {
        try {
          const d = manifest.realized;
          with_(lock, () => {
            results.push(sum(Object.values(d).map((e) => e.data)));
          });
        } catch (exc) {
          with_(lock, () => {
            errors.push(exc);
          });
        }
      };

      const threads = range(24).map(() => new TH.Thread({ target: worker }));
      for (const th of threads) th.start();
      for (const th of threads) th.join(_T);
      assert.equal(
        threads.some((th) => th.is_alive()),
        false,
        "realized() hung",
      );
      assert.deepEqual(errors, []);
      assert.deepEqual(
        results,
        range(24).map(() => sum(range(50))),
      );
      tfs.assert_quiescent();
    });
  });

  // ------------------------------------------------------------------
  // W7: wide remember/memorize group futures on 1 slot
  // ------------------------------------------------------------------
  t("007_wide_memorize_and_remember_on_1x1", () => {
    const n = 400;
    const entries = range(n).map((i) => Entry.constant(i));
    with_(new _TinyTaskforces({ alpha_slots: 1, internal_slots: 1 }), (tfs) => {
      laila.memorize(entries).wait(_T);
      const got = laila.remember(entries.map((e) => e.global_id)).wait(_T);
      assert.deepEqual(
        [...got].map((g) => g.data),
        range(n),
      );
      tfs.assert_quiescent();
    });
  });

  // ------------------------------------------------------------------
  // W8: user job mixing await-remember with nested builds
  // ------------------------------------------------------------------
  t("008_user_jobs_awaiting_remember_and_build_concurrently", () => {
    const leaves = range(30).map((i) => Entry.constant(i));
    const manifest = new Manifest({ data: Object.fromEntries(leaves.map((e, i) => [`k${i}`, e])) });
    with_(new _TinyTaskforces({ alpha_slots: 1, internal_slots: 1 }), (tfs) => {
      manifest.memorize().wait(_T);

      const job = async (gid) => {
        const leaf = await laila.remember(gid);
        const staged = Entry.variable(null, { constitution: _SUM_ALL, manifest });
        const built = await laila.build(staged);
        return leaf.data + _unwrap(built);
      };

      const gf = laila.command.submit(leaves.map((e) => () => job(e.global_id)));
      const outs = gf.wait(_T).map((r) => _unwrap(r));
      assert.deepEqual(
        outs,
        range(30).map((i) => i + sum(range(30))),
      );
      tfs.assert_quiescent();
    });
  });
});

// Tear down the lazily-activated default policy so the process exits.
test("zz teardown", () =>
  macrotask(() => {
    laila.terminate();
    fs.rmSync(TMP_ROOT, { recursive: true, force: true });
  }));
