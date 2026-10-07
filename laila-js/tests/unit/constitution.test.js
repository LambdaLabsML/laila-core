/**
 * Constitution-driven Entry building and persistence: port of
 *   tests/functional/entry/constitution/unit_tests/test_constitution_build.py
 *
 * Covers:
 *   * ``Entry.variable(constitution=..., manifest=...)`` construction.
 *   * ``laila.build(entry)`` submits and materialises the payload, clearing
 *     the attached constitution on success.
 *   * ``Entry.data`` raises ``EntryNotBuiltError`` until the entry has been
 *     built (no implicit lazy build any more).
 *   * Memorize on a non-READY (STAGED) complex entry is rejected; building
 *     first then memorizing the materialized result succeeds.
 *
 * The constitution body is the JavaScript spelling of the Python
 * ``def combine(manifest)`` (``_exec_one_fn`` evaluates JS source).
 */
import { test, describe, beforeEach } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const { default: laila, S } = await import("./fixtures/laila_root.js");

const E = await import(S + "_compat/errors.js");
const { Entry, EntryState } = await import(S + "entry/index.js");
const { TransformationSequence } = await import(S + "entry/compdata/transformation/base.js");
const { Base64 } = await import(S + "entry/compdata/transformation/base64/base64.js");
const { ComplexConstitution } = await import(S + "entry/constitution/index.js");
const { EntryNotBuiltError } = await import(S + "entry/exceptions.js");
const { Manifest } = await import(S + "policy/central/memory/schema/manifest.js");

const TMP_ROOT = fs.mkdtempSync(path.join(os.tmpdir(), "laila_constitution_test_"));
laila.set_default_directory(TMP_ROOT);

/**
 * Run a synchronous body on a fresh macrotask: ``node:test`` invokes test
 * bodies from a microtask, where a blocking wait (``Future.wait``) is
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
const t = (name, fn) => test(name, () => macrotask(fn));

const _CONSTITUTION_SRC = "function combine(manifest) {\n  const d = manifest.realized;\n  return d.a.data + d.b.data;\n}\n";

/** ``setUp`` shared by both suites: two constants + a memorized manifest. */
function _setup_manifest(ctx) {
  laila.get_active_policy();

  ctx.e1 = Entry.constant(10);
  ctx.e2 = Entry.constant(32);
  ctx.manifest = new Manifest({ data: { a: ctx.e1, b: ctx.e2 } });
  ctx.manifest.memorize().wait();
}

// ---------------------------------------------------------------------------
// TestConstitutionBuild
// ---------------------------------------------------------------------------
describe("TestConstitutionBuild", () => {
  const ctx = {};
  beforeEach(() => macrotask(() => _setup_manifest(ctx)));

  t("test_variable_with_constitution_is_staged_and_carries_constitution", () => {
    const entry = Entry.variable(null, { constitution: _CONSTITUTION_SRC, manifest: ctx.manifest });
    assert.equal(entry.state, EntryState.STAGED);
    assert.notEqual(entry.constitution, null);
    assert.ok(entry.constitution instanceof ComplexConstitution);
    assert.equal(entry.constitution.code, _CONSTITUTION_SRC);
    assert.equal(entry.constitution.manifest_global_id, ctx.manifest.global_id);
  });

  t("test_build_returns_future_and_materializes_payload", () => {
    const entry = Entry.variable(null, { constitution: _CONSTITUTION_SRC, manifest: ctx.manifest });
    const future = laila.build(entry);
    assert.notEqual(future, null);
    future.wait(null);

    assert.equal(entry.state, EntryState.READY);
    assert.equal(entry.data, 42);
    assert.equal(entry.constitution, null);
  });

  t("test_build_via_taskforce_id_string", () => {
    const entry = Entry.variable(null, { constitution: _CONSTITUTION_SRC, manifest: ctx.manifest });
    const policy = laila.get_active_policy();
    laila.build(entry, { taskforce_id: policy.central.command.alpha_taskforce }).wait(null);
    assert.equal(entry.data, 42);
  });

  t("test_build_twice_raises", () => {
    const entry = Entry.variable(null, { constitution: _CONSTITUTION_SRC, manifest: ctx.manifest });
    laila.build(entry).wait(null);
    assert.throws(() => laila.build(entry).wait(null), E.RuntimeError);
  });

  t("test_build_without_constitution_raises", () => {
    const entry = Entry.constant("already-built");
    assert.throws(() => laila.build(entry).wait(null), E.RuntimeError);
  });

  t("test_data_raises_until_built", () => {
    const entry = Entry.variable(null, { constitution: _CONSTITUTION_SRC, manifest: ctx.manifest });
    assert.throws(() => entry.data, EntryNotBuiltError);
    laila.build(entry).wait(null);
    assert.equal(entry.data, 42);
    assert.equal(entry.state, EntryState.READY);
    assert.equal(entry.constitution, null);
  });
});

// ---------------------------------------------------------------------------
// TestConstitutionSerializeReadyOnly
// ---------------------------------------------------------------------------
describe("TestConstitutionSerializeReadyOnly", () => {
  const ctx = {};
  beforeEach(() =>
    macrotask(() => {
      _setup_manifest(ctx);
      ctx.transformations = new TransformationSequence({ transformations: [new Base64()] });
    }),
  );

  t("test_serialize_on_staged_complex_entry_raises", () => {
    const entry = Entry.variable(null, { constitution: _CONSTITUTION_SRC, manifest: ctx.manifest });
    assert.throws(() => entry.serialize(ctx.transformations), E.RuntimeError);
  });

  t("test_memorize_on_staged_complex_entry_raises", () => {
    const entry = Entry.variable(null, { constitution: _CONSTITUTION_SRC, manifest: ctx.manifest });
    const ref = laila.memorize(entry);
    assert.throws(() => ref.wait(null));
  });

  t("test_build_then_memorize_then_remember_succeeds", () => {
    const entry = Entry.variable(null, { constitution: _CONSTITUTION_SRC, manifest: ctx.manifest });
    laila.build(entry).wait(null);
    assert.equal(entry.state, EntryState.READY);
    assert.equal(entry.data, 42);

    laila.memorize(entry).wait();

    const fut = laila.remember(entry.global_id, { persist: false });
    let recovered = fut.wait(null);
    if (Array.isArray(recovered)) recovered = recovered[0];
    assert.equal(recovered.data, 42);
    assert.equal(recovered.state, EntryState.READY);
  });
});

// Tear down the lazily-activated default policy so the process exits.
test("zz teardown", () =>
  macrotask(() => {
    laila.terminate({ wait: true, cancel_pending: true });
    fs.rmSync(TMP_ROOT, { recursive: true, force: true });
  }));
