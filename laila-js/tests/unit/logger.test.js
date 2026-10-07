/**
 * Logger: port of
 *   tests/functional/logger/unit_tests/test_logger.py
 *
 * Comprehensive test suite for the LAILA Logger singleton: record schema and
 * id normalization, singleton enforcement and reset semantics, stdlib handler
 * installation / levels / idempotent start-stop, structured emission for
 * memory verbs and the future lifecycle, pool-sink memorize roundtrip on the
 * in-memory, HDF5 and SQLite backends, the pool-sink recursion guard, the
 * ``laila.args.environment.logger`` mirror and the ``laila.terminate``
 * interaction.
 */
import { test, describe, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const { default: laila, S } = await import("./fixtures/laila_root.js");

const E = await import(S + "_compat/errors.js");
const T = await import(S + "_compat/pytypes.js");
const time = await import(S + "_compat/time.js");
const json = await import(S + "_compat/pyjson.js");
const logging = await import(S + "_compat/logging.js");
const { _LAILA_IDENTIFIABLE_POOL } = await import(S + "data/schema/base.js");
const LG = await import(S + "logger/index.js");
const { _LAILA_LOGGER_NAME, Logger, _install_null_handler, build_record, disable_logging, enable_logging, get_logger, normalize_level, numeric_level, set_log_level } = LG;
const { _LOGGER_SCOPE } = await import(S + "macros/strings.js");
const { FutureStatus } = await import(S + "policy/central/command/schema/future/future/future_status.js");
const { GroupFuture } = await import(S + "policy/central/command/schema/future/future/group_future.js");
const { ConcurrentPackageFuture } = await import(S + "policy/central/command/taskforce/thread_pool_executor/future.js");
const { _LAILA_IDENTIFIABLE_POLICY } = await import(S + "policy/schema/base.js");
const { SQLitePool } = await import(S + "data/sqlite/sqlite.js");
const { HDF5Pool } = await import(S + "data/hdf5/hdf5.js");
const { optional_import } = await import(S + "_compat/optional.js");

const { str, isdict } = T;

// ``try: from laila.data.hdf5.hdf5 import HDF5Pool`` -- the JS module always
// imports; the optional ``h5wasm`` dependency (``import h5py``) is what may
// be missing, resolved exactly like ``hdf5.js`` does.
const HAVE_HDF5POOL = optional_import("h5wasm", { file: "dist/node/hdf5_hl.js" }) !== null;


const TMP_ROOT = fs.mkdtempSync(path.join(os.tmpdir(), "laila_logger_test_root_"));
laila.set_default_directory(TMP_ROOT);

/**
 * Run a synchronous body on a fresh macrotask: ``node:test`` invokes test
 * bodies from a microtask, where a blocking wait (``Future.wait``,
 * ``time.sleep``) is impossible by construction.
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
const mkdtemp = (prefix) => fs.mkdtempSync(path.join(os.tmpdir(), prefix));
const rmtree = (p) => fs.rmSync(p, { recursive: true, force: true });

/** A stdlib handler that stores every record it receives. */
class _CapturingHandler extends logging.Handler {
  constructor() {
    super();
    /** @type {logging.LogRecord[]} */
    this.records = [];
  }
  emit(record) {
    this.records.push(record);
  }
}

/** Attach a capturing handler to the ``"laila"`` root logger. */
function _capture_logger_records() {
  const root = logging.getLogger(_LAILA_LOGGER_NAME);
  const handler = new _CapturingHandler();
  handler.setLevel(logging.DEBUG);
  root.addHandler(handler);
  return [root, handler];
}

/** A trivial pool that captures every write for assertion purposes. */
class _RecordingPool extends _LAILA_IDENTIFIABLE_POOL {
  model_post_init(_context) {
    super.model_post_init(_context);
    if (!this.__dict__) this.__dict__ = {};
    this.__dict__._writes ??= [];
  }
  _write(key, value) {
    super._write(key, value);
    (this.__dict__._writes ??= []).push([key, value]);
  }
}
void _RecordingPool;

/** Tear down any singleton from a prior test. */
function _reset_logger_state() {
  Logger.reset_singleton();
  const root = logging.getLogger(_LAILA_LOGGER_NAME);
  for (const handler of [...root.handlers]) {
    if (!(handler instanceof logging.NullHandler)) root.removeHandler(handler);
  }
}

/**
 * The console sink writes every record to stderr; keep the runner output
 * readable by swallowing it for the duration of a suite (assertions never
 * look at stderr -- they go through ``_CapturingHandler``).
 */
function quiet_stderr() {
  let saved;
  beforeEach(() => {
    saved = process.stderr.write;
    process.stderr.write = () => true;
  });
  afterEach(() => {
    process.stderr.write = saved;
  });
}

const _type_name = (h) => h.constructor.name;

// ---------------------------------------------------------------------------
// TestRecordBuilder
// ---------------------------------------------------------------------------
describe("TestRecordBuilder", () => {
  test("test_01_normalize_level_uppercase_string", () => {
    assert.equal(normalize_level("info"), "INFO");
    assert.equal(normalize_level("DEBUG"), "DEBUG");
    assert.equal(normalize_level("warning"), "WARNING");
  });

  test("test_02_normalize_level_numeric", () => {
    assert.equal(normalize_level(logging.INFO), "INFO");
    assert.equal(normalize_level(logging.ERROR), "ERROR");
  });

  test("test_03_normalize_level_unknown_falls_back_to_info", () => {
    assert.equal(normalize_level("not-a-level"), "INFO");
    assert.equal(normalize_level(99999), "INFO");
  });

  test("test_04_numeric_level_matches_stdlib", () => {
    assert.equal(numeric_level("INFO"), logging.INFO);
    assert.equal(numeric_level("ERROR"), logging.ERROR);
    assert.equal(numeric_level("DEBUG"), logging.DEBUG);
  });

  test("test_05_build_record_minimal_has_required_keys", () => {
    const rec = build_record("test.event");
    for (const key of ["ts", "ts_unix", "level", "event", "extra"]) assert.ok(key in rec, key);
    assert.equal(rec.event, "test.event");
    assert.equal(rec.level, "INFO");
    assert.deepEqual(rec.extra, {});
  });

  test("test_06_build_record_omits_unset_optional_ids", () => {
    const rec = build_record("test.event");
    for (const key of ["policy_id", "pool_id", "entry_id", "future_id", "taskforce_id"]) assert.ok(!(key in rec), key);
  });

  test("test_07_build_record_normalizes_id_objects_to_strings", () => {
    class _Stub {
      static global_id = "ENTRY:abc:0";
      global_id = "ENTRY:abc:0";
    }
    const rec = build_record("memory.memorize", {
      policy_id: new _Stub(),
      pool_id: "POOL:def:0",
      entry_id: new _Stub(),
      taskforce_id: new _Stub(),
    });
    assert.equal(rec.policy_id, "ENTRY:abc:0");
    assert.equal(rec.pool_id, "POOL:def:0");
    assert.equal(rec.entry_id, "ENTRY:abc:0");
    assert.equal(rec.taskforce_id, "ENTRY:abc:0");
  });

  test("test_08_build_record_ts_is_iso_z_suffixed", () => {
    const rec = build_record("e");
    assert.ok(rec.ts.endsWith("Z"));
    assert.ok(T.is_float(rec.ts_unix));
  });

  test("test_09_build_record_status_and_prev_status_are_strings", () => {
    const rec = build_record("future.status", { status: FutureStatus.RUNNING, prev_status: FutureStatus.NOT_STARTED });
    assert.equal(rec.status, str(FutureStatus.RUNNING));
    assert.equal(rec.prev_status, str(FutureStatus.NOT_STARTED));
  });

  test("test_10_build_record_child_lists_normalize_ids", () => {
    class _Stub {
      global_id = "FUTURE:x:0";
    }
    const rec = build_record("future.group_finished", {
      child_future_ids: [new _Stub(), "FUTURE:y:0"],
      child_results: ["ENTRY:r1:0"],
    });
    assert.deepEqual(rec.child_future_ids, ["FUTURE:x:0", "FUTURE:y:0"]);
    assert.deepEqual(rec.child_results, ["ENTRY:r1:0"]);
  });

  test("test_11_build_record_is_json_serializable", () => {
    const rec = build_record("memory.memorize", {
      policy_id: "POLICY:p:0",
      pool_id: "POOL:p:0",
      entry_id: "ENTRY:e:0",
      extra: { shape: [1, 2, 3], ok: true },
    });
    const encoded = json.dumps(rec);
    const decoded = json.loads(encoded);
    assert.equal(decoded.event, "memory.memorize");
    assert.deepEqual(decoded.extra.shape, [1, 2, 3]);
  });

  test("test_12_build_record_preserves_extra", () => {
    const rec = build_record("e", { extra: { a: 1 } });
    assert.deepEqual(rec.extra, { a: 1 });
    rec.extra.b = 2;
    const rec2 = build_record("e", { extra: { a: 1 } });
    assert.deepEqual(rec2.extra, { a: 1 });
  });
});

// ---------------------------------------------------------------------------
// TestSingletonAndScope
// ---------------------------------------------------------------------------
describe("TestSingletonAndScope", () => {
  quiet_stderr();
  beforeEach(() => _reset_logger_state());
  afterEach(() => _reset_logger_state());

  test("test_13_logger_scope_is_logger", () => {
    const logger = new Logger();
    assert.deepEqual(logger.scopes, [_LOGGER_SCOPE]);
  });

  test("test_14_logger_global_id_includes_logger_scope", () => {
    const logger = new Logger();
    assert.ok(logger.global_id.includes("LOGGER"));
  });

  test("test_15_singleton_returned_by_call", () => {
    const a = new Logger();
    const b = new Logger();
    assert.equal(a, b);
  });

  test("test_16_get_logger_lazy_creates", () => {
    Logger.reset_singleton();
    const a = get_logger();
    const b = get_logger();
    assert.equal(a, b);
    assert.ok(a instanceof Logger);
  });

  test("test_17_laila_logger_property_returns_singleton", () => {
    const a = get_logger();
    assert.equal(laila.logger, a);
  });

  test("test_18_singleton_call_with_kwargs_updates_existing", () => {
    const a = new Logger({ level: "WARNING" });
    new Logger({ level: "DEBUG" });
    assert.equal(a.level, "DEBUG");
  });

  test("test_19_reset_singleton_releases_handle", () => {
    const a = new Logger();
    Logger.reset_singleton();
    const b = new Logger();
    assert.notEqual(a, b);
  });

  test("test_20_reset_singleton_stops_handlers", () => {
    const logger = new Logger();
    logger.start();
    assert.equal(logger.enabled, true);
    Logger.reset_singleton();
    const new_logger = new Logger();
    assert.equal(new_logger.enabled, false);
  });
});

// ---------------------------------------------------------------------------
// TestStdlibIntegration
// ---------------------------------------------------------------------------
describe("TestStdlibIntegration", () => {
  quiet_stderr();
  let tmp;
  beforeEach(() => {
    _reset_logger_state();
    tmp = mkdtemp("laila_logger_test_");
  });
  afterEach(() => {
    _reset_logger_state();
    rmtree(tmp);
  });

  test("test_21_install_null_handler_idempotent", () => {
    const root1 = _install_null_handler();
    const before = root1.handlers.filter((h) => h instanceof logging.NullHandler).length;
    const root2 = _install_null_handler();
    const after = root2.handlers.filter((h) => h instanceof logging.NullHandler).length;
    assert.equal(before, after);
    assert.ok(before >= 1);
  });

  test("test_22_start_attaches_console_handler", () => {
    const logger = new Logger();
    logger.start();
    const root = logging.getLogger(_LAILA_LOGGER_NAME);
    const kinds = root.handlers.map(_type_name);
    assert.ok(kinds.includes("StreamHandler"));
    // No pool means display gets forced on.
    assert.equal(logger.display, true);
  });

  test("test_23_pool_sink_suppresses_console_handler", () => {
    const logger = new Logger({ pool_nickname: "some-pool" });
    logger.start();
    const root = logging.getLogger(_LAILA_LOGGER_NAME);
    const non_null = root.handlers.filter((h) => !(h instanceof logging.NullHandler));
    // display defaults to False; with a pool configured the console
    // handler is not attached.
    const kinds = non_null.map(_type_name);
    assert.ok(!kinds.includes("StreamHandler"));
    assert.equal(logger.display, false);
  });

  test("test_23b_pool_sink_with_display_true_attaches_console", () => {
    const logger = new Logger({ pool_nickname: "some-pool", display: true });
    logger.start();
    const root = logging.getLogger(_LAILA_LOGGER_NAME);
    const kinds = root.handlers.map(_type_name);
    // Both sinks are active.
    assert.ok(kinds.includes("StreamHandler"));
    assert.equal(logger.display, true);
  });

  test("test_23c_no_pool_forces_display_true_even_if_user_set_false", () => {
    const logger = new Logger({ display: false });
    // No pool configured; start() must override display to True so
    // records always have at least one sink.
    logger.start();
    assert.equal(logger.display, true);
    const root = logging.getLogger(_LAILA_LOGGER_NAME);
    const kinds = root.handlers.map(_type_name);
    assert.ok(kinds.includes("StreamHandler"));
  });

  test("test_24_start_is_idempotent_no_duplicate_handlers", () => {
    const logger = new Logger();
    logger.start();
    const first = [...logging.getLogger(_LAILA_LOGGER_NAME).handlers];
    logger.start();
    const second = [...logging.getLogger(_LAILA_LOGGER_NAME).handlers];
    // Handlers replaced — count of non-null should be the same
    assert.equal(first.filter((h) => !(h instanceof logging.NullHandler)).length, second.filter((h) => !(h instanceof logging.NullHandler)).length);
  });

  test("test_25_stop_removes_installed_handlers", () => {
    const logger = new Logger();
    logger.start();
    logger.stop();
    const root = logging.getLogger(_LAILA_LOGGER_NAME);
    for (const handler of root.handlers) assert.ok(handler instanceof logging.NullHandler || handler instanceof _CapturingHandler);
  });

  test("test_26_set_level_updates_handlers_and_root", () => {
    const logger = new Logger();
    logger.start();
    logger.set_level("DEBUG");
    const root = logging.getLogger(_LAILA_LOGGER_NAME);
    assert.equal(root.level, logging.DEBUG);
    for (const handler of logger._installed_handlers) assert.equal(handler.level, logging.DEBUG);
  });

  test("test_27_emit_below_threshold_dropped", () => {
    const logger = new Logger({ level: "ERROR" });
    logger.start();
    const [root, capture] = _capture_logger_records();
    try {
      logger.info("info-msg");
      assert.ok(!capture.records.some((r) => r.getMessage().includes("info-msg")));
    } finally {
      root.removeHandler(capture);
    }
  });

  test("test_28_emit_above_threshold_passes", () => {
    const logger = new Logger({ level: "DEBUG" });
    logger.start();
    const [root, capture] = _capture_logger_records();
    try {
      logger.error("err-msg");
      assert.ok(capture.records.some((r) => r.getMessage().includes("err-msg")));
    } finally {
      root.removeHandler(capture);
    }
  });

  test("test_29_disabled_logger_emits_nothing", () => {
    const logger = new Logger();
    // never .start()
    const [root, capture] = _capture_logger_records();
    try {
      logger.info("should-not-appear");
      assert.ok(!capture.records.some((r) => r.getMessage().includes("should-not-appear")));
    } finally {
      root.removeHandler(capture);
    }
  });

  test("test_30_enable_logging_helper_sets_level_and_starts", () => {
    const logger = enable_logging("DEBUG");
    assert.equal(logger.enabled, true);
    assert.equal(logger.level, "DEBUG");
  });

  test("test_31_disable_logging_helper_stops", () => {
    enable_logging("INFO");
    disable_logging();
    assert.equal(get_logger().enabled, false);
  });

  test("test_32_set_log_level_helper_changes_level", () => {
    enable_logging("INFO");
    set_log_level("WARNING");
    assert.equal(get_logger().level, "WARNING");
  });
});

// ---------------------------------------------------------------------------
// _ActivePolicyHarness
// ---------------------------------------------------------------------------
/** Mixin that activates a fresh policy and restores the original on tearDown. */
function _activate_fresh_policy(ctx) {
  ctx._original_policy = laila.get_active_policy();
  ctx._policy = new _LAILA_IDENTIFIABLE_POLICY();
  laila.activate_policy(ctx._policy);
}

function _restore_original_policy(ctx) {
  try {
    laila.get_active_policy().central.command.shutdown({ wait: true, cancel_pending: true });
  } catch {
    /* pass */
  }
  laila.activate_policy(ctx._original_policy);
}

/** Pull the structured record dict out of each captured LogRecord. */
function _extract_records_from_capture(capture, event_name = null) {
  const out = [];
  for (const r of capture.records) {
    if (!r.args || r.args.length < 2) continue;
    const payload = r.args[1];
    if (!isdict(payload)) continue;
    if (event_name !== null && payload.event !== event_name) continue;
    out.push(payload);
  }
  return out;
}

// ---------------------------------------------------------------------------
// TestStructuredEmission
// ---------------------------------------------------------------------------
describe("TestStructuredEmission", () => {
  quiet_stderr();
  const ctx = {};
  beforeEach(() =>
    macrotask(() => {
      _reset_logger_state();
      _activate_fresh_policy(ctx);
    }),
  );
  afterEach(() =>
    macrotask(() => {
      _reset_logger_state();
      _restore_original_policy(ctx);
    }),
  );

  t("test_34_record_memorize_emits_one_record_per_entry", () => {
    const logger = new Logger({ level: "DEBUG" });
    logger.start();
    const [root, capture] = _capture_logger_records();
    try {
      const entries = T.range(3).map((i) => laila.constant(i));
      const pool = new _LAILA_IDENTIFIABLE_POOL();
      ctx._policy.central.memory.extend(pool, { pool_nickname: "rec-mem" });
      logger.record_memorize({ entries, pool, policy: ctx._policy });
      const memorize_records = capture.records.filter((r) => r.getMessage().includes("memory.memorize"));
      assert.equal(memorize_records.length, 3);
    } finally {
      root.removeHandler(capture);
    }
  });

  t("test_35_record_remember_uses_correct_event_name", () => {
    const logger = new Logger({ level: "DEBUG" });
    logger.start();
    const [root, capture] = _capture_logger_records();
    try {
      const pool = new _LAILA_IDENTIFIABLE_POOL();
      ctx._policy.central.memory.extend(pool, { pool_nickname: "rec-rem" });
      logger.record_remember({ entry_ids: ["ENTRY:abc:0"], pool, policy: ctx._policy });
      const messages = capture.records.map((r) => r.getMessage());
      assert.ok(messages.some((m) => m.includes("memory.remember")));
    } finally {
      root.removeHandler(capture);
    }
  });

  t("test_36_record_forget_uses_correct_event_name", () => {
    const logger = new Logger({ level: "DEBUG" });
    logger.start();
    const [root, capture] = _capture_logger_records();
    try {
      const pool = new _LAILA_IDENTIFIABLE_POOL();
      ctx._policy.central.memory.extend(pool, { pool_nickname: "rec-for" });
      logger.record_forget({ entry_ids: ["ENTRY:abc:0"], pool, policy: ctx._policy });
      assert.ok(capture.records.some((r) => r.getMessage().includes("memory.forget")));
    } finally {
      root.removeHandler(capture);
    }
  });

  t(
    "test_37_record_carries_all_involved_global_ids",
    () => {
      const logger = new Logger({ level: "DEBUG" });
      logger.start();
      const [root, capture] = _capture_logger_records();
      try {
        const entry = laila.constant("x");
        const pool = new _LAILA_IDENTIFIABLE_POOL();
        ctx._policy.central.memory.extend(pool, { pool_nickname: "ids" });
        logger.record_memorize({ entries: [entry], pool, policy: ctx._policy });
        const records = _extract_records_from_capture(capture, "memory.memorize");
        assert.ok(records.length >= 1);
        const rec = records[records.length - 1];
        assert.equal(rec.policy_id, ctx._policy.global_id);
        assert.equal(rec.pool_id, pool.global_id);
        assert.equal(rec.entry_id, entry.global_id);
        assert.equal(rec.pool_nickname, "ids");
      } finally {
        root.removeHandler(capture);
      }
    },
    );

  t("test_38_logger_id_attached_automatically", () => {
    const logger = new Logger({ level: "DEBUG" });
    logger.start();
    const [root, capture] = _capture_logger_records();
    try {
      logger.info("hi");
      const records = _extract_records_from_capture(capture, "log");
      assert.ok(records.length >= 1);
      assert.equal(records[records.length - 1].logger_id, logger.global_id);
    } finally {
      root.removeHandler(capture);
    }
  });

  t("test_39_future_creation_emits_record_via_callback", () => {
    const logger = new Logger({ level: "DEBUG" });
    logger.start();
    const [root, capture] = _capture_logger_records();
    try {
      new ConcurrentPackageFuture({
        taskforce_id: ctx._policy.central.command.alpha_taskforce,
        policy_id: ctx._policy.global_id,
        purpose: "test-purpose",
      });
      const messages = capture.records.map((r) => r.getMessage());
      assert.ok(messages.some((m) => m.includes("future.created")));
    } finally {
      root.removeHandler(capture);
    }
  });

  t("test_40_future_status_transition_emits_record", () => {
    const logger = new Logger({ level: "DEBUG" });
    logger.start();
    const [root, capture] = _capture_logger_records();
    try {
      const f = new ConcurrentPackageFuture({
        taskforce_id: ctx._policy.central.command.alpha_taskforce,
        policy_id: ctx._policy.global_id,
        purpose: "trans",
      });
      f.status = FutureStatus.RUNNING;
      f.status = FutureStatus.FINISHED;
      const messages = capture.records.map((r) => r.getMessage());
      assert.ok(messages.some((m) => m.includes("future.status")));
    } finally {
      root.removeHandler(capture);
    }
  });

  t("test_41_group_future_creation_emits_record", () => {
    const logger = new Logger({ level: "DEBUG" });
    logger.start();
    const [root, capture] = _capture_logger_records();
    try {
      const f1 = new ConcurrentPackageFuture({
        taskforce_id: ctx._policy.central.command.alpha_taskforce,
        policy_id: ctx._policy.global_id,
        purpose: "child-1",
      });
      const f2 = new ConcurrentPackageFuture({
        taskforce_id: ctx._policy.central.command.alpha_taskforce,
        policy_id: ctx._policy.global_id,
        purpose: "child-2",
      });
      new GroupFuture({
        taskforce_id: ctx._policy.central.command.alpha_taskforce,
        policy_id: ctx._policy.global_id,
        future_ids: [f1.global_id, f2.global_id],
      });
      const messages = capture.records.map((r) => r.getMessage());
      assert.ok(messages.some((m) => m.includes("future.group_created")));
    } finally {
      root.removeHandler(capture);
    }
  });

  t("test_42_future_error_transition_recorded_at_error_level", () => {
    const logger = new Logger({ level: "DEBUG" });
    logger.start();
    const [root, capture] = _capture_logger_records();
    try {
      const f = new ConcurrentPackageFuture({
        taskforce_id: ctx._policy.central.command.alpha_taskforce,
        policy_id: ctx._policy.global_id,
        purpose: "err-future",
      });
      f.exception = new E.RuntimeError("boom");
      f.status = FutureStatus.ERROR;
      const records = _extract_records_from_capture(capture, "future.status");
      const error_records = records.filter((r) => r.status === "error");
      assert.ok(error_records.length >= 1);
      const rec = error_records[error_records.length - 1];
      assert.equal(rec.level, "ERROR");
      assert.ok("exc_type" in (rec.extra ?? {}));
    } finally {
      root.removeHandler(capture);
    }
  });
});

// ---------------------------------------------------------------------------
// TestPoolSink
// ---------------------------------------------------------------------------
describe("TestPoolSink", () => {
  quiet_stderr();
  const ctx = {};
  beforeEach(() =>
    macrotask(() => {
      _reset_logger_state();
      _activate_fresh_policy(ctx);
    }),
  );
  afterEach(() =>
    macrotask(() => {
      _reset_logger_state();
      _restore_original_policy(ctx);
    }),
  );

  const _wait_for_writes = (pool, count, timeout = 5.0) => {
    const start = time.time();
    while (time.time() - start < timeout) {
      if ([...pool.keys()].length >= count) return true;
      time.sleep(0.02);
    }
    return false;
  };

  t(
    "test_43_pool_sink_writes_record_via_memorize",
    () => {
      const sink_pool = new _LAILA_IDENTIFIABLE_POOL();
      ctx._policy.central.memory.extend(sink_pool, { pool_nickname: "sink-mem" });
      const logger = new Logger({ level: "DEBUG", pool_nickname: "sink-mem" });
      logger.start();
      logger.info("captured-into-pool");
      assert.ok(_wait_for_writes(sink_pool, 1), `expected at least 1 write, got ${JSON.stringify([...sink_pool.keys()])}`);
    },
    );

  t(
    "test_44_pool_sink_record_is_recoverable_as_dict",
    () => {
      const sink_pool = new _LAILA_IDENTIFIABLE_POOL();
      ctx._policy.central.memory.extend(sink_pool, { pool_nickname: "sink-rec" });
      const logger = new Logger({ level: "DEBUG", pool_nickname: "sink-rec" });
      logger.start();
      logger.info("recoverable-msg", { extra: { "unique-key": "unique-value" } });
      assert.ok(_wait_for_writes(sink_pool, 1));
      const keys = [...sink_pool.keys()];
      const recovered = laila.remember({ entry_ids: [keys[0]], pool_id: sink_pool.global_id });
      recovered.wait();
      const results = "result" in recovered ? recovered.result : recovered;
      const entry = Array.isArray(results) ? results[0] : results;
      // entry.data should round-trip the dict
      assert.ok(isdict(entry.data));
      assert.equal(entry.data.event, "log");
      assert.equal(entry.data.extra["unique-key"], "unique-value");
    },
    );

  t(
    "test_45_pool_sink_recursion_guard_does_not_double_log",
    () => {
      const sink_pool = new _LAILA_IDENTIFIABLE_POOL();
      ctx._policy.central.memory.extend(sink_pool, { pool_nickname: "sink-rg" });
      const logger = new Logger({ level: "DEBUG", pool_nickname: "sink-rg" });
      logger.start();
      logger.info("just-once");
      // wait briefly for any potential recursive writes to settle
      time.sleep(0.4);
      const keys = [...sink_pool.keys()];
      // Exactly one entry — no recursion produced extra entries
      assert.equal(keys.length, 1);
    },
    );

  t("test_46_pool_sink_failure_does_not_raise", () => {
    const logger = new Logger({ level: "DEBUG", pool_nickname: "does-not-exist" });
    logger.start();
    try {
      logger.info("should-fail-quietly");
    } catch (exc) {
      assert.fail(`pool sink raised ${exc}`);
    }
    assert.notEqual(logger._last_pool_sink_error, null);
  });

  t("test_47_pool_sink_disabled_when_no_pool_configured", () => {
    const sink_pool = new _LAILA_IDENTIFIABLE_POOL();
    ctx._policy.central.memory.extend(sink_pool, { pool_nickname: "not-used" });
    const logger = new Logger({ level: "DEBUG" });
    logger.start();
    logger.info("not-going-to-pool");
    time.sleep(0.2);
    assert.equal([...sink_pool.keys()].length, 0);
  });

  t(
    "test_48_pool_sink_records_multiple_levels",
    () => {
      const sink_pool = new _LAILA_IDENTIFIABLE_POOL();
      ctx._policy.central.memory.extend(sink_pool, { pool_nickname: "sink-multi" });
      const logger = new Logger({ level: "DEBUG", pool_nickname: "sink-multi" });
      logger.start();
      logger.debug("d");
      logger.info("i");
      logger.warning("w");
      logger.error("e");
      assert.ok(_wait_for_writes(sink_pool, 4));
    },
    );
});

// ---------------------------------------------------------------------------
// TestHDF5PoolSink
// ---------------------------------------------------------------------------
describe("TestHDF5PoolSink", { skip: HAVE_HDF5POOL ? false : "HDF5Pool not available (h5py not installed)" }, () => {
  quiet_stderr();
  const ctx = {};
  let tmp, pool;
  beforeEach(() =>
    macrotask(() => {
      _reset_logger_state();
      _activate_fresh_policy(ctx);
      tmp = mkdtemp("laila_logger_hdf5_");
      pool = new HDF5Pool({ file_path: path.join(tmp, "logs.h5py") });
      ctx._policy.central.memory.extend(pool, { pool_nickname: "hdf5-sink" });
    }),
  );
  afterEach(() =>
    macrotask(() => {
      try {
        pool.close();
      } catch {
        /* pass */
      }
      _reset_logger_state();
      _restore_original_policy(ctx);
      rmtree(tmp);
    }),
  );

  const _wait_for_writes = (count, timeout = 5.0) => {
    const start = time.time();
    while (time.time() - start < timeout) {
      if ([...pool.keys()].length >= count) return true;
      time.sleep(0.05);
    }
    return false;
  };

  t(
    "test_49_hdf5_sink_writes_log_record",
    () => {
      const logger = new Logger({ level: "DEBUG", pool_nickname: "hdf5-sink" });
      logger.start();
      logger.info("hdf5-roundtrip", { extra: { k: "v" } });
      assert.ok(_wait_for_writes(1));
    },
    );

  t(
    "test_50_hdf5_sink_record_roundtrips_via_remember",
    () => {
      const logger = new Logger({ level: "DEBUG", pool_nickname: "hdf5-sink" });
      logger.start();
      logger.info("hdf5-readback", { extra: { echo: 42 } });
      assert.ok(_wait_for_writes(1));
      const keys = [...pool.keys()];
      const gf = laila.remember({ entry_ids: keys, pool_id: pool.global_id });
      gf.wait();
      let results = gf.result;
      if (!Array.isArray(results)) results = [results];
      const records = results.map((r) => r.data);
      assert.ok(records.some((rec) => (rec.extra ?? {}).echo === 42));
    },
    );

  t(
    "test_51_hdf5_sink_preserves_structured_id_fields",
    () => {
      const logger = new Logger({ level: "DEBUG", pool_nickname: "hdf5-sink" });
      logger.start();
      const entry = laila.constant("payload");
      logger.record_memorize({ entries: [entry], pool, policy: ctx._policy });
      assert.ok(_wait_for_writes(1));
      const keys = [...pool.keys()];
      const gf = laila.remember({ entry_ids: keys, pool_id: pool.global_id });
      gf.wait();
      let results = gf.result;
      if (!Array.isArray(results)) results = [results];
      const rec = results[0].data;
      assert.equal(rec.event, "memory.memorize");
      assert.equal(rec.pool_id, pool.global_id);
      assert.equal(rec.policy_id, ctx._policy.global_id);
      assert.equal(rec.entry_id, entry.global_id);
    },
    );

  t(
    "test_52_hdf5_sink_serializes_many_records_independently",
    () => {
      const logger = new Logger({ level: "DEBUG", pool_nickname: "hdf5-sink" });
      logger.start();
      for (const i of T.range(8)) logger.info(`msg-${i}`, { extra: { i } });
      assert.ok(_wait_for_writes(8, 10.0));
      const keys = [...pool.keys()];
      assert.equal(keys.length, 8);
      const gf = laila.remember({ entry_ids: keys, pool_id: pool.global_id });
      gf.wait();
      let results = gf.result;
      if (!Array.isArray(results)) results = [results];
      const seen = results.map((r) => Number(r.data.extra.i)).sort((a, b) => a - b);
      assert.deepEqual(seen, T.range(8));
    },
    );
});

// ---------------------------------------------------------------------------
// TestEnvironmentMirror
// ---------------------------------------------------------------------------
describe("TestEnvironmentMirror", () => {
  quiet_stderr();
  let _prev_env;
  beforeEach(() => {
    _reset_logger_state();
    // Stash current env so subsequent tests aren't polluted
    try {
      _prev_env = laila.args.get("environment");
    } catch {
      _prev_env = null;
    }
    void _prev_env;
  });
  afterEach(() => _reset_logger_state());

  test("test_53_logger_construction_writes_env_mirror", () => {
    new Logger({ level: "DEBUG" });
    const env = laila.args.environment;
    const logger_dump = env.get("logger");
    assert.notEqual(logger_dump, null);
    // DotMap supports .get
    assert.equal(logger_dump.get("level"), "DEBUG");
  });

  test("test_54_args_path_for_logger_is_top_level", async () => {
    const { _SCOPE_TO_ARGS_PATH } = await import(S + "basics/definitions/cli_capable.js");
    assert.equal(_SCOPE_TO_ARGS_PATH["LOGGER"], "logger");
  });

  test("test_55_args_input_injection_pulls_logger_fields", () => {
    Logger.reset_singleton();
    laila.args.logger = { level: "ERROR", capture_traceback: true };
    try {
      const logger = new Logger();
      assert.equal(logger.level, "ERROR");
      assert.equal(logger.capture_traceback, true);
    } finally {
      try {
        laila.args.logger = {};
      } catch {
        /* pass */
      }
    }
  });
});

// ---------------------------------------------------------------------------
// TestTerminateInteraction
// ---------------------------------------------------------------------------
describe("TestTerminateInteraction", () => {
  quiet_stderr();
  beforeEach(() => _reset_logger_state());
  afterEach(() => _reset_logger_state());

  t("test_56_terminate_resets_logger_singleton", () => {
    const old = new Logger();
    old.start();
    const old_id = T.id(old);
    laila.terminate({ wait: true, cancel_pending: false });
    const new_ = new Logger();
    assert.notEqual(T.id(new_), old_id);
    assert.equal(new_.enabled, false);
  });
});

// ---------------------------------------------------------------------------
// TestSQLitePoolSink
// ---------------------------------------------------------------------------
describe("TestSQLitePoolSink", () => {
  quiet_stderr();
  const ctx = {};
  let tmp, pool;
  beforeEach(() =>
    macrotask(() => {
      _reset_logger_state();
      _activate_fresh_policy(ctx);
      tmp = mkdtemp("laila_logger_sqlite_");
      pool = new SQLitePool({ file_path: path.join(tmp, "pool.sqlite") });
      ctx._policy.central.memory.extend(pool, { pool_nickname: "sqlite-sink" });
    }),
  );
  afterEach(() =>
    macrotask(() => {
      try {
        pool.close();
      } catch {
        /* pass */
      }
      _reset_logger_state();
      _restore_original_policy(ctx);
      rmtree(tmp);
    }),
  );

  t(
    "test_59_sqlite_sink_writes_log_record",
    () => {
      const logger = new Logger({ level: "DEBUG", pool_nickname: "sqlite-sink" });
      logger.start();
      logger.info("sqlite-roundtrip", { extra: { answer: 42 } });
      time.sleep(0.2);
      const keys = [...pool.keys()];
      assert.ok(keys.length >= 1);
    },
    );

  t(
    "test_60_sqlite_sink_record_roundtrips_via_remember",
    () => {
      const logger = new Logger({ level: "DEBUG", pool_nickname: "sqlite-sink" });
      logger.start();
      logger.info("sqlite-readback", { extra: { echo: "yes" } });
      time.sleep(0.2);
      const keys = [...pool.keys()];
      const gf = laila.remember({ entry_ids: keys, pool_id: pool.global_id });
      gf.wait();
      const results = Array.isArray(gf.result) ? gf.result : [gf.result];
      const recs = results.map((r) => r.data);
      assert.ok(recs.some((r) => (r.extra ?? {}).echo === "yes"));
    },
    );
});

// ---------------------------------------------------------------------------
// TestEdgeCases
// ---------------------------------------------------------------------------
describe("TestEdgeCases", () => {
  quiet_stderr();
  const ctx = {};
  beforeEach(() =>
    macrotask(() => {
      _reset_logger_state();
      _activate_fresh_policy(ctx);
    }),
  );
  afterEach(() =>
    macrotask(() => {
      _reset_logger_state();
      _restore_original_policy(ctx);
    }),
  );

  t("test_61_emit_unset_logger_id_is_filled_in_emit", () => {
    const logger = new Logger({ level: "DEBUG" });
    logger.start();
    const [root, capture] = _capture_logger_records();
    try {
      const rec = build_record("custom.event", { level: "INFO" });
      assert.ok(!("logger_id" in rec));
      logger.emit(rec);
      assert.equal(rec.logger_id, logger.global_id);
    } finally {
      root.removeHandler(capture);
    }
  });

  t("test_62_set_level_helper_changes_threshold", () => {
    const logger = new Logger({ level: "ERROR" });
    logger.start();
    const [root, capture] = _capture_logger_records();
    try {
      logger.info("dropped");
      logger.set_level("DEBUG");
      logger.info("kept");
      const messages = capture.records.map((r) => r.getMessage());
      assert.ok(!messages.some((m) => m.includes("dropped")));
      assert.ok(messages.some((m) => m.includes("kept")));
    } finally {
      root.removeHandler(capture);
    }
  });

  t("test_63_capture_traceback_attaches_when_enabled", () => {
    const logger = new Logger({ level: "DEBUG", capture_traceback: true });
    logger.start();
    const [root, capture] = _capture_logger_records();
    try {
      const f = new ConcurrentPackageFuture({
        taskforce_id: ctx._policy.central.command.alpha_taskforce,
        policy_id: ctx._policy.global_id,
        purpose: "trace",
      });
      try {
        throw new E.ValueError("traced-boom");
      } catch (exc) {
        if (!(exc instanceof E.ValueError)) throw exc;
        f.exception = exc;
      }
      f.status = FutureStatus.ERROR;
      for (const r of capture.records) {
        if (!r.args || r.args.length < 2) continue;
        const payload = r.args[1];
        if (isdict(payload) && payload.status === "error") {
          if ("traceback" in (payload.extra ?? {})) return;
        }
      }
      assert.fail("expected a traceback to be captured");
    } finally {
      root.removeHandler(capture);
    }
  });

  t(
    "test_64_recursion_guard_holds_under_thread_dispatch",
    () => {
      // Pool sink writes do not recurse even when memorize spawns workers.
      const sink_pool = new _LAILA_IDENTIFIABLE_POOL();
      ctx._policy.central.memory.extend(sink_pool, { pool_nickname: "rg-thread" });
      const logger = new Logger({ level: "DEBUG", pool_nickname: "rg-thread" });
      logger.start();
      for (const i of T.range(5)) logger.info(`line-${i}`);
      time.sleep(0.5);
      const keys = [...sink_pool.keys()];
      // Exactly 5 entries — no recursion blew it up
      assert.equal(keys.length, 5);
    },
    );

  t("test_65_singleton_survives_args_round_trip", () => {
    Logger.reset_singleton();
    laila.args.logger = { level: "DEBUG" };
    const a = new Logger();
    assert.equal(a.level, "DEBUG");
    const b = new Logger();
    assert.equal(a, b);
  });

  t("test_66_recording_missing_pool_does_not_break_emission", () => {
    const logger = new Logger({ level: "DEBUG", pool_nickname: "ghost-pool-not-registered" });
    logger.start();
    const [root, capture] = _capture_logger_records();
    try {
      logger.info("ghost-msg");
      const messages = capture.records.map((r) => r.getMessage());
      assert.ok(messages.some((m) => m.includes("ghost-msg")));
      assert.notEqual(logger._last_pool_sink_error, null);
    } finally {
      root.removeHandler(capture);
    }
  });
});

// ---------------------------------------------------------------------------
// TestIntegrationWithMemory
// ---------------------------------------------------------------------------
describe("TestIntegrationWithMemory", () => {
  quiet_stderr();
  const ctx = {};
  beforeEach(() =>
    macrotask(() => {
      _reset_logger_state();
      _activate_fresh_policy(ctx);
    }),
  );
  afterEach(() =>
    macrotask(() => {
      _reset_logger_state();
      _restore_original_policy(ctx);
    }),
  );

  t("test_57_memorize_emits_memorize_event", () => {
    const logger = new Logger({ level: "DEBUG" });
    logger.start();
    const [root, capture] = _capture_logger_records();
    try {
      const primary = new _LAILA_IDENTIFIABLE_POOL();
      ctx._policy.central.memory.extend(primary, { pool_nickname: "primary57" });
      const entry = laila.constant({ k: 1 });
      const future = laila.memorize(entry, { pool_nickname: "primary57" });
      if (future !== null && future !== undefined) future.wait();
      const messages = capture.records.map((r) => r.getMessage());
      assert.ok(messages.some((m) => m.includes("memory.memorize")));
    } finally {
      root.removeHandler(capture);
    }
  });

  t("test_58_forget_emits_forget_event", () => {
    const logger = new Logger({ level: "DEBUG" });
    logger.start();
    const [root, capture] = _capture_logger_records();
    try {
      const primary = new _LAILA_IDENTIFIABLE_POOL();
      ctx._policy.central.memory.extend(primary, { pool_nickname: "primary58" });
      const entry = laila.constant("x");
      const f1 = laila.memorize(entry, { pool_nickname: "primary58" });
      if (f1 !== null && f1 !== undefined) f1.wait();
      const f2 = laila.forget(entry.global_id, { pool_nickname: "primary58" });
      if (f2 !== null && f2 !== undefined) f2.wait();
      const messages = capture.records.map((r) => r.getMessage());
      assert.ok(messages.some((m) => m.includes("memory.forget")));
    } finally {
      root.removeHandler(capture);
    }
  });
});

// Tear down the lazily-activated default policy so the process exits.
test("zz teardown", () =>
  macrotask(() => {
    laila.terminate({ wait: true, cancel_pending: true });
    rmtree(TMP_ROOT);
  }));
