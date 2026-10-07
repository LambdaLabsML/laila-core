/**
 * Cloud / server-backed pool backends: ports of
 *   tests/functional/pools/azure/unit_tests/test_azure_pool.py
 *   tests/functional/pools/backblaze/unit_tests/test_backblaze_pool.py
 *   tests/functional/pools/cloudflare/unit_tests/test_cloudflare_pool.py
 *   tests/functional/pools/gcs/unit_tests/test_gcs_pool.py
 *   tests/functional/pools/huggingface/unit_tests/test_huggingface_pool.py
 *   tests/functional/pools/mongo/unit_tests/test_mongo_pool.py
 *   tests/functional/pools/postgres/unit_tests/test_postgres_pool.py
 *   tests/functional/pools/redispool/unit_tests/test_redis_pool.py
 *   tests/functional/pools/s3/unit_tests/test_s3_pool.py
 *
 * One ``describe`` per Python TestCase, named ``<Backend>: <ClassName>``.
 * Every suite keeps the Python gating: the cloud backends read their
 * credentials from ``~/dev_secrets.toml`` (S3: ``~/.laila/secrets/s3_test.toml``)
 * through ``laila.read_args`` at module load and the whole describe is
 * skipped -- with the Python reason string -- when the file / keys are
 * absent; the server backends gate on an env URI / DSN or the server
 * binary being on ``PATH`` (``shutil.which``). Python's outer
 * ``skipUnless(HAVE_<X>POOL, "<X>Pool import failed")`` maps onto the npm
 * client library being installed (``optional_import``), see ``gate_options``.
 *
 * The per-test bodies are shared factories (``C.<name>``) because the nine
 * Python files are near copies of one another; each describe lists its
 * tests in the Python order with the Python constants (thread counts,
 * key counts) so the asserted behaviour is identical.
 */
import { test, describe, before, after, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const { default: laila, S } = await import("./fixtures/laila_root.js");

const E = await import(S + "_compat/errors.js");
const TH = await import(S + "_compat/threading.js");
const time = await import(S + "_compat/time.js");
const json = await import(S + "_compat/pyjson.js");
const { with_ } = await import(S + "_compat/contextlib.js");
const { SKIP_VALIDATION } = await import(S + "_compat/pydantic.js");
const { optional_import } = await import(S + "_compat/optional.js");
const { uuid4 } = await import(S + "_compat/uuid.js");
const { LAILA_DEFAULT_DIRECTORIES } = await import(S + "macros/defaults.js");
const { S3Pool, AzurePool, BackblazePool, CloudflarePool, GCSPool, HuggingFacePool, MongoPool, PostgresPool, RedisPool } = await import(S + "data/index.js");

const TMP_ROOT = fs.mkdtempSync(path.join(os.tmpdir(), "laila_pools_cloud_"));
laila.set_default_directory(TMP_ROOT);

// botocore resolves the signing region from the env / ``~/.aws/config`` only
// and falls back to ``us-east-1``; the JS SDK chain behind
// ``_default_region_provider`` (src/data/boto/boto.js) additionally asks the
// EC2 instance metadata service, so on an EC2 box an R2 / B2 client signs
// with the *instance* region (R2 rejects it: ``InvalidRegionName``). Pin the
// chain to botocore's behaviour for the suite; the Python tests never see
// IMDS. (Honoured by the SDK; a no-op off EC2.)
process.env.AWS_EC2_METADATA_DISABLED ??= "true";

// A timer macrotask rather than ``setImmediate``: the sync pool hooks
// ``block_on`` promise-based network clients, and a pump started inside a
// Node ``Immediate`` callback never completes socket I/O (requests sit until
// the SDK's socket timeout). Timers are a plain macrotask for the pump.
function macrotask(fn) {
  return new Promise((resolve, reject) =>
    setTimeout(() => {
      try {
        resolve(fn());
      } catch (e) {
        reject(e);
      }
    }, 0),
  );
}
const t = (name, fn) => test(name, () => macrotask(fn));

/**
 * Tests whose Python behaviour the cooperative-thread runtime cannot
 * reproduce (see ``_NESTED_EVENT_WAIT`` below). They stay ``todo`` so the
 * port keeps asserting the Python behaviour, but the body only runs when
 * ``LAILA_RUN_DEADLOCKING=1`` is set: a hung synchronous pump cannot be
 * interrupted by node:test's timeout, so running it by default would hang
 * the whole suite instead of reporting a TODO.
 */
const RUN_DEADLOCKING = process.env.LAILA_RUN_DEADLOCKING === "1";
const t_todo = (name, reason, fn) =>
  test(name, { todo: reason }, () =>
    macrotask(() => {
      if (!RUN_DEADLOCKING) assert.fail(`${reason} -- body skipped (set LAILA_RUN_DEADLOCKING=1 to run it; it hangs the process)`);
      return fn();
    }),
  );
/** ``t`` or ``t_todo`` depending on an optional ``todo`` reason. */
const t_maybe = (name, todo, fn) => (todo ? t_todo(name, todo, fn) : t(name, fn));

// ``holder`` blocks in ``release.wait(1.0)`` while holding the lock; with
// cooperative threads that wait runs *inside* the main thread's
// ``ready.wait`` pump, so main only resumes after the holder's timeout has
// released the lock and ``atomic(timeout_s=0.05)`` then succeeds instead of
// raising ``TimeoutError`` (Python: the holder still owns the lock).
const _NESTED_EVENT_WAIT =
  "SRC-BUG: src/_compat/threading.js cooperative Thread/Event -- the lock holder's Event.wait nests under the main thread's " +
  "wait, so the lock is already released when main contends and no TimeoutError is raised (Python raises)";
const sorted = (xs) => [...xs].sort();
const count_equal = (a, b) => assert.deepEqual(sorted(a), sorted(b));
const range = (n) => Array.from({ length: n }, (_, i) => i);

/** ``shutil.which`` */
function which(cmd) {
  for (const dir of (process.env.PATH ?? "").split(path.delimiter)) {
    if (!dir) continue;
    const p = path.join(dir, cmd);
    try {
      fs.accessSync(p, fs.constants.X_OK);
      if (fs.statSync(p).isFile()) return p;
    } catch {
      /* not here */
    }
  }
  return null;
}

/** ``unittest.mock.Mock()`` -- a callable recording its calls. */
function Mock(impl = () => undefined) {
  const m = function (...args) {
    m.calls.push(args);
    return impl.apply(this, args);
  };
  m.calls = [];
  m.assert_called_once = () => assert.equal(m.calls.length, 1, `expected exactly one call, got ${m.calls.length}`);
  m.assert_not_called = () => assert.equal(m.calls.length, 0, `expected no calls, got ${m.calls.length}`);
  return m;
}

/** ``patch.object(Cls, name, autospec=True, ...)`` -- returns ``{ mock, stop }``. */
function patch_object(Cls, name, impl) {
  const P = Cls.prototype;
  const original = P[name];
  const mock = Mock(impl);
  P[name] = mock;
  return { mock, stop: () => void (P[name] = original) };
}

/** ``with (p1, p2, ...): body`` for a list of patchers. */
function with_patches(patches, body) {
  try {
    return body();
  } finally {
    for (const p of [...patches].reverse()) p.stop();
  }
}

// ---------------------------------------------------------------------------
// Secrets (module load, exactly like the Python module bodies)
// ---------------------------------------------------------------------------
const DEV_SECRETS_PATH = path.join(os.homedir(), "dev_secrets.toml");
const S3_SECRETS_PATH = path.join(os.homedir(), ".laila", "secrets", "s3_test.toml");

/**
 * ``laila.read_args(path); _secrets = laila.args; HAVE = all(k in _secrets ...)``
 * @returns {{have: boolean, secrets: any}}
 */
function load_secrets(secrets_path, required_keys = []) {
  try {
    if (fs.existsSync(secrets_path)) {
      laila.read_args(secrets_path);
      const secrets = laila.args;
      // A shared dev_secrets.toml may hold credentials for other
      // providers only; skip (rather than error) when ours are absent.
      return { have: required_keys.every((k) => k in secrets), secrets };
    }
    return { have: false, secrets: null };
  } catch {
    return { have: false, secrets: null };
  }
}

/**
 * ``@unittest.skipUnless(HAVE_<X>POOL, "<X>Pool import failed")`` (outer) +
 * ``@unittest.skipUnless(<gate>, <reason>)`` (inner). The JS classes always
 * import; what can be missing is the npm client library (Python's boto3 /
 * azure-storage-blob / ...), which makes construction or the first call
 * raise ``ImportError`` -- the same outcome the Python suite has without
 * the pip package. The outer decorator's reason wins, as in unittest.
 * @returns {{}|{skip: string}}
 */
function gate_options(pool_name, pkg, gate, reason) {
  if (optional_import(pkg) === null) return { skip: `${pool_name} import failed (${pkg} not installed)` };
  return gate ? {} : { skip: reason };
}

// ---------------------------------------------------------------------------
// Shared test bodies (``get`` returns the suite's current pool)
// ---------------------------------------------------------------------------
const C = {
  pool_id_format: (get) => t("pool_id_format", () => assert.ok(get().pool_id.startsWith("LAILA:POOL:"))),
  get_missing_returns_none: (get) =>
    t("get_missing_returns_none", () => {
      assert.equal(get()["missing"], null);
      assert.equal(get().exists("missing"), false);
    }),
  set_and_get_roundtrip_string: (get) =>
    t("set_and_get_roundtrip_string", () => {
      get()["a"] = json.dumps("123");
      assert.equal(get()["a"], "123");
    }),
  set_non_string_raises_type_error: (get) =>
    t("set_non_string_raises_type_error", () => {
      assert.throws(() => {
        get()["bad"] = 123;
      }, E.TypeError);
    }),
  set_none_raises_type_error: (get) =>
    t("set_none_raises_type_error", () => {
      assert.throws(() => {
        get()["none"] = null;
      }, E.TypeError);
    }),
  delitem_silent_if_missing: (get) =>
    t("delitem_silent_if_missing", () => {
      get().__delitem__("ghost");
      assert.equal(get().exists("ghost"), false);
    }),
  delitem_removes_existing: (get) =>
    t("delitem_removes_existing", () => {
      get()["x"] = json.dumps("val");
      assert.equal(get().exists("x"), true);
      delete get()["x"];
      assert.equal(get().exists("x"), false);
      assert.equal(get()["x"], null);
    }),
  exists_true_when_key_present: (get) =>
    t("exists_true_when_key_present", () => {
      get()["present"] = json.dumps(7);
      assert.equal(get().exists("present"), true);
      assert.equal("present" in get(), true);
    }),
  keys_snapshot_returns_list: (get) =>
    t("keys_snapshot_returns_list", () => {
      get()["a"] = json.dumps(1);
      get()["b"] = json.dumps(2);
      const ks = get().keys({ as_generator: false });
      assert.ok(Array.isArray(ks));
      count_equal(ks, ["a", "b"]);
    }),
  /** ``n`` keys; ``check_iter`` mirrors ``assertTrue(hasattr(it, "__iter__"))``. */
  keys_generator_yields_all_current_keys: (get, { n = 10, check_iter = true } = {}) =>
    t("keys_generator_yields_all_current_keys", () => {
      const items = Object.fromEntries(range(n).map((i) => [`k${i}`, json.dumps(i)]));
      for (const [k, v] of Object.entries(items)) get()[k] = v;
      const it = get().keys({ as_generator: true });
      if (check_iter) assert.equal(typeof it[Symbol.iterator], "function");
      const got = [...it];
      count_equal(got, Object.keys(items));
    }),
  keys_snapshot_is_not_affected_by_later_mutations: (get) =>
    t("keys_snapshot_is_not_affected_by_later_mutations", () => {
      get()["a"] = json.dumps(1);
      const snap = get().keys();
      get()["b"] = json.dumps(2);
      count_equal(snap, ["a"]);
    }),
  overwrite_value: (get) =>
    t("overwrite_value", () => {
      get()["k"] = json.dumps("v1");
      get()["k"] = json.dumps("v2");
      assert.equal(get()["k"], "v2");
    }),
  empty_removes_all: (get) =>
    t("empty_removes_all", () => {
      get()["a"] = json.dumps(1);
      get()["b"] = json.dumps(2);
      get().empty();
      assert.equal(get()["a"], null);
      assert.equal(get()["b"], null);
      assert.deepEqual([...get().keys()], []);
    }),
  concurrent_writes_no_loss: (get, { n = 10, todo = null } = {}) =>
    t_maybe("concurrent_writes_no_loss", todo, () => {
      const threads = [];
      const writer = (i) => {
        get()[String(i)] = json.dumps(i);
      };
      for (const i of range(n)) {
        const th = new TH.Thread({ target: writer, args: [i] });
        th.start();
        threads.push(th);
      }
      for (const th of threads) th.join();
      for (const i of range(n)) assert.equal(get()[String(i)], i);
    }),
  store_entry_payload_as_object: (get) =>
    t("store_entry_payload_as_object", () => {
      const e = laila.constant({ x: 1 });
      const payload = { global_id: e.global_id, data: e.data };
      get()[e.global_id] = payload;
      assert.deepEqual(get()[e.global_id], payload);
    }),
  /** ``self.pool.close(); self.pool.close()`` -- or on a fresh pool from ``make``. */
  close_is_idempotent: (get, { make = null } = {}) =>
    t("close_is_idempotent", () => {
      const p = make !== null ? make() : get();
      p.close();
      p.close();
    }),
  delete_then_recreate: (get) =>
    t("delete_then_recreate", () => {
      get()["z"] = json.dumps(9);
      delete get()["z"];
      assert.equal(get().exists("z"), false);
      get()["z"] = json.dumps(10);
      assert.equal(get()["z"], 10);
    }),
  atomic_reentrant_same_thread: (get) =>
    t("atomic_reentrant_same_thread", () => {
      with_(get().atomic(), () => {
        get()["r"] = json.dumps(1);
        with_(get().atomic(), () => {
          get()["r"] = json.dumps(2);
        });
      });
      assert.equal(get()["r"], 2);
    }),
  atomic_thread_safety_for_read_modify_write: (get, { n_threads = 3, n_steps = 5 } = {}) =>
    t("atomic_thread_safety_for_read_modify_write", () => {
      get()["counter"] = json.dumps(0);
      const worker = () => {
        for (let s = 0; s < n_steps; s++) {
          with_(get().atomic(), () => {
            const current = Number(get()["counter"]);
            get()["counter"] = json.dumps(current + 1);
          });
        }
      };
      const threads = range(n_threads).map(() => new TH.Thread({ target: worker }));
      for (const th of threads) th.start();
      for (const th of threads) th.join();
      assert.equal(Number(get()["counter"]), n_threads * n_steps);
    }),
  /** Write through ``self.pool``, close it, re-open the *same* namespace via ``reopen()``. */
  persistence_across_pool_instances: (get, reopen) =>
    t("persistence_across_pool_instances", () => {
      get()["persistent"] = json.dumps({ a: 1 });
      get().close();
      const p2 = reopen();
      try {
        assert.deepEqual(p2["persistent"], { a: 1 });
      } finally {
        p2.close();
      }
    }),
  key_with_special_chars_roundtrip: (get) =>
    t("key_with_special_chars_roundtrip", () => {
      const key = "key/with:special=chars";
      get()[key] = json.dumps("value");
      assert.equal(get().exists(key), true);
      assert.equal(get()[key], "value");
      delete get()[key];
      assert.equal(get().exists(key), false);
    }),
};

/** ``setUp: self.pool = make(); tearDown: empty (suppressed) + close``. */
function per_test_pool(ctx, make) {
  beforeEach(() =>
    macrotask(() => {
      ctx.pool = make();
    }),
  );
  afterEach(() =>
    macrotask(() => {
      if (ctx.pool !== null && ctx.pool !== undefined) {
        try {
          ctx.pool.empty();
        } catch {
          /* pass */
        }
        ctx.pool.close();
      }
    }),
  );
}

/** ``setUpClass: cls._shared_pool = make(); setUp: empty(); tearDownClass: empty + close``. */
function shared_pool(ctx, make, { suppress_empty = true } = {}) {
  before(() =>
    macrotask(() => {
      ctx.shared = make();
    }),
  );
  after(() =>
    macrotask(() => {
      if (ctx.shared !== null && ctx.shared !== undefined) {
        if (suppress_empty) {
          try {
            ctx.shared.empty();
          } catch {
            /* pass */
          }
        }
        ctx.shared.close();
      }
    }),
  );
  beforeEach(() =>
    macrotask(() => {
      ctx.pool = ctx.shared;
      if (suppress_empty) {
        try {
          ctx.pool.empty();
        } catch {
          /* pass */
        }
      } else ctx.pool.empty();
    }),
  );
}

// ===========================================================================
// test_s3_pool.py
// ===========================================================================
{
  const { have: HAVE_DEV_SECRETS, secrets: _secrets } = load_secrets(S3_SECRETS_PATH);

  /** Create S3Pool. Uses ``~/.laila/secrets/s3_test.toml`` for credentials. */
  const _make_pool = (bucket_name = "laila-test-1", kwargs = {}) => {
    kwargs = { ...kwargs };
    if (HAVE_DEV_SECRETS && _secrets) {
      const bucket = kwargs.bucket_name ?? bucket_name;
      delete kwargs.bucket_name;
      return new S3Pool({
        bucket_name: bucket,
        access_key_id: _secrets.AWS_ACCESS_KEY,
        secret_access_key: _secrets.AWS_SECRET_ACCESS_KEY,
        region_name: _secrets.AWS_REGION,
        ...kwargs,
      });
    }
    if (!("bucket_name" in kwargs)) kwargs.bucket_name = bucket_name;
    return new S3Pool(kwargs);
  };

  /** S3 pool tests. Requires existing bucket laila-test-1. */
  describe("S3: TestS3Pool", gate_options("S3Pool", "@aws-sdk/client-s3", HAVE_DEV_SECRETS, "dev_secrets.toml required for S3 tests"), () => {
    const ctx = { pool: null };
    const get = () => ctx.pool;
    per_test_pool(ctx, () => _make_pool());

    C.pool_id_format(get);
    C.get_missing_returns_none(get);
    C.set_and_get_roundtrip_string(get);
    C.set_non_string_raises_type_error(get);
    C.set_none_raises_type_error(get);
    C.delitem_silent_if_missing(get);
    C.delitem_removes_existing(get);
    C.exists_true_when_key_present(get);
    C.keys_snapshot_returns_list(get);
    C.keys_generator_yields_all_current_keys(get);
    C.keys_snapshot_is_not_affected_by_later_mutations(get);
    C.overwrite_value(get);
    C.empty_removes_all(get);
    t("multiple_instances_share_bucket_namespace", () => {
      const p1 = _make_pool("laila-test-1");
      const p2 = _make_pool("laila-test-1");
      assert.notEqual(p1.pool_id, p2.pool_id);
      try {
        p1["shared_key"] = json.dumps(42);
        assert.equal(p2["shared_key"], 42);
      } finally {
        p1.empty();
        p1.close();
        p2.close();
      }
    });
    C.concurrent_writes_no_loss(get, { n: 10 });
    C.store_entry_payload_as_object(get);
    C.close_is_idempotent(get);
    C.delete_then_recreate(get);
    C.atomic_reentrant_same_thread(get);
    C.atomic_thread_safety_for_read_modify_write(get, { n_threads: 3, n_steps: 5 });
    C.persistence_across_pool_instances(get, () => _make_pool(ctx.pool.bucket_name));
    C.key_with_special_chars_roundtrip(get);
  });
}

// ===========================================================================
// test_azure_pool.py
// ===========================================================================
{
  const { have: HAVE_DEV_SECRETS, secrets: _secrets } = load_secrets(DEV_SECRETS_PATH, ["AZURE_CONNECTION_STRING", "AZURE_CONTAINER_NAME"]);

  const _make_pool = (container_name = "lailatestmarch", kwargs = {}) => {
    kwargs = { ...kwargs };
    if (HAVE_DEV_SECRETS && _secrets) {
      const container = kwargs.container_name ?? container_name;
      delete kwargs.container_name;
      return new AzurePool({ container_name: container, connection_string: _secrets.AZURE_CONNECTION_STRING, ...kwargs });
    }
    if (!("container_name" in kwargs)) kwargs.container_name = container_name;
    const connection_string = kwargs.connection_string ?? "dummy";
    delete kwargs.connection_string;
    return new AzurePool({ connection_string, ...kwargs });
  };

  describe("Azure: TestAzurePool", gate_options("AzurePool", "@azure/storage-blob", HAVE_DEV_SECRETS, "dev_secrets.toml required for Azure tests"), () => {
    const ctx = { pool: null };
    const get = () => ctx.pool;
    per_test_pool(ctx, () => _make_pool(_secrets.AZURE_CONTAINER_NAME));

    C.pool_id_format(get);
    C.get_missing_returns_none(get);
    C.set_and_get_roundtrip_string(get);
    C.set_non_string_raises_type_error(get);
    C.set_none_raises_type_error(get);
    C.delitem_silent_if_missing(get);
    C.delitem_removes_existing(get);
    C.exists_true_when_key_present(get);
    C.keys_snapshot_returns_list(get);
    C.keys_generator_yields_all_current_keys(get);
    C.keys_snapshot_is_not_affected_by_later_mutations(get);
    C.overwrite_value(get);
    C.empty_removes_all(get);
    C.concurrent_writes_no_loss(get, { n: 10 });
    C.store_entry_payload_as_object(get);
    C.close_is_idempotent(get);
    C.delete_then_recreate(get);
    C.atomic_reentrant_same_thread(get);
    C.atomic_thread_safety_for_read_modify_write(get, { n_threads: 3, n_steps: 5 });
    C.persistence_across_pool_instances(get, () => _make_pool(ctx.pool.container_name));
    C.key_with_special_chars_roundtrip(get);
  });
}

// ===========================================================================
// test_backblaze_pool.py
// ===========================================================================
{
  const { have: HAVE_DEV_SECRETS, secrets: _secrets } = load_secrets(DEV_SECRETS_PATH, ["BACKBLAZE_APPLICATION_KEY"]);

  let _key_id = null;
  let _endpoint = null;
  if (HAVE_DEV_SECRETS && _secrets !== null) {
    _key_id = _secrets.get("BACKBLAZE_APPLICATION_KEY_ID", null);
    if (_key_id === null) _key_id = _secrets.get("BACKBALZE_APPLICATION_KEY_ID", null);
    if (_key_id === null) _key_id = _secrets.get("BACKBLAZE_KEY_ID", null);
    if (_key_id === null) _key_id = _secrets.get("BACKBALZE_KEY_ID", null);
    _endpoint = _secrets.get("BACKBLAZE_ENDPOINT_URL", null);
    if (_endpoint === null) _endpoint = _secrets.get("BACKBALZE_ENDPOINT_URL", null);
  }

  const _make_pool = (bucket_name = "laila-test", kwargs = {}) => {
    kwargs = { ...kwargs };
    if (HAVE_DEV_SECRETS && _secrets && _key_id && _endpoint) {
      const bucket = kwargs.bucket_name ?? bucket_name;
      delete kwargs.bucket_name;
      return new BackblazePool({
        bucket_name: bucket,
        application_key_id: _key_id,
        application_key: _secrets.BACKBLAZE_APPLICATION_KEY,
        endpoint_url: _endpoint,
        ...kwargs,
      });
    }
    if (!("bucket_name" in kwargs)) kwargs.bucket_name = bucket_name;
    const application_key_id = kwargs.application_key_id ?? "dummy";
    const application_key = kwargs.application_key ?? "dummy";
    const endpoint_url = kwargs.endpoint_url ?? "https://s3.us-west-004.backblazeb2.com";
    delete kwargs.application_key_id;
    delete kwargs.application_key;
    delete kwargs.endpoint_url;
    return new BackblazePool({ application_key_id, application_key, endpoint_url, ...kwargs });
  };

  const gate = Boolean(HAVE_DEV_SECRETS && _key_id && _endpoint);
  describe("Backblaze: TestBackblazePool", gate_options("BackblazePool", "@aws-sdk/client-s3", gate, "Backblaze key id and endpoint required in dev_secrets.toml"), () => {
    const ctx = { pool: null };
    const get = () => ctx.pool;
    per_test_pool(ctx, () => _make_pool(_secrets.BACKBLAZE_BUCKET_NAME));

    C.pool_id_format(get);
    C.get_missing_returns_none(get);
    C.set_and_get_roundtrip_string(get);
    C.set_non_string_raises_type_error(get);
    C.set_none_raises_type_error(get);
    C.delitem_silent_if_missing(get);
    C.delitem_removes_existing(get);
    C.exists_true_when_key_present(get);
    C.keys_snapshot_returns_list(get);
    C.keys_generator_yields_all_current_keys(get);
    C.keys_snapshot_is_not_affected_by_later_mutations(get);
    C.overwrite_value(get);
    C.empty_removes_all(get);
    C.concurrent_writes_no_loss(get, { n: 10 });
    C.store_entry_payload_as_object(get);
    C.close_is_idempotent(get);
    C.delete_then_recreate(get);
    C.atomic_reentrant_same_thread(get);
    C.atomic_thread_safety_for_read_modify_write(get, { n_threads: 3, n_steps: 5 });
    C.persistence_across_pool_instances(get, () => _make_pool(ctx.pool.bucket_name));
    C.key_with_special_chars_roundtrip(get);
  });
}

// ===========================================================================
// test_cloudflare_pool.py
// ===========================================================================
{
  const { have: HAVE_DEV_SECRETS, secrets: _secrets } = load_secrets(DEV_SECRETS_PATH, [
    "CLOUDFLARE_ACCOUNT_ID",
    "CLOUDFLARE_ACCESS_KEY_ID",
    "CLOUDFLARE_SECRET_ACCESS_KEY",
    "CLOUDFLARE_BUCKET_NAME",
  ]);

  /** Create CloudflarePool. Uses `dev_secrets.toml` if available. */
  const _make_pool = (bucket_name = "test", kwargs = {}) => {
    kwargs = { ...kwargs };
    if (HAVE_DEV_SECRETS && _secrets) {
      return new CloudflarePool({
        account_id: _secrets.CLOUDFLARE_ACCOUNT_ID,
        access_key_id: _secrets.CLOUDFLARE_ACCESS_KEY_ID,
        secret_access_key: _secrets.CLOUDFLARE_SECRET_ACCESS_KEY,
        bucket_name: _secrets.CLOUDFLARE_BUCKET_NAME,
        ...kwargs,
      });
    }
    if (!("bucket_name" in kwargs)) kwargs.bucket_name = bucket_name;
    const account_id = kwargs.account_id ?? "dummy";
    const access_key_id = kwargs.access_key_id ?? "dummy";
    const secret_access_key = kwargs.secret_access_key ?? "dummy";
    delete kwargs.account_id;
    delete kwargs.access_key_id;
    delete kwargs.secret_access_key;
    return new CloudflarePool({ account_id, access_key_id, secret_access_key, ...kwargs });
  };

  /** Cloudflare pool tests. Requires `dev_secrets.toml` with Cloudflare credentials. */
  describe("Cloudflare: TestCloudflarePool", gate_options("CloudflarePool", "@aws-sdk/client-s3", HAVE_DEV_SECRETS, "dev_secrets.toml required for Cloudflare tests"), () => {
    const ctx = { pool: null };
    const get = () => ctx.pool;
    per_test_pool(ctx, () => _make_pool());

    C.pool_id_format(get);
    C.get_missing_returns_none(get);
    C.set_and_get_roundtrip_string(get);
    C.set_non_string_raises_type_error(get);
    C.set_none_raises_type_error(get);
    C.delitem_silent_if_missing(get);
    C.delitem_removes_existing(get);
    C.exists_true_when_key_present(get);
    C.keys_snapshot_returns_list(get);
    C.keys_generator_yields_all_current_keys(get);
    C.keys_snapshot_is_not_affected_by_later_mutations(get);
    C.overwrite_value(get);
    C.empty_removes_all(get);
    C.concurrent_writes_no_loss(get, { n: 10 });
    C.store_entry_payload_as_object(get);
    C.close_is_idempotent(get);
    C.delete_then_recreate(get);
    C.atomic_reentrant_same_thread(get);
    C.atomic_thread_safety_for_read_modify_write(get, { n_threads: 3, n_steps: 5 });
    C.persistence_across_pool_instances(get, () => _make_pool());
    C.key_with_special_chars_roundtrip(get);
  });
}

// ===========================================================================
// test_gcs_pool.py
// ===========================================================================
{
  const { have: HAVE_DEV_SECRETS, secrets: _secrets } = load_secrets(DEV_SECRETS_PATH, ["GCP_SERVICE_ACCOUNT", "GCP_BUCKET_NAME"]);

  const _make_pool = (bucket_name = "laila-test", kwargs = {}) => {
    kwargs = { ...kwargs };
    if (HAVE_DEV_SECRETS && _secrets) {
      const bucket = kwargs.bucket_name ?? bucket_name;
      delete kwargs.bucket_name;
      return new GCSPool({
        bucket_name: bucket,
        service_account_info: _secrets.GCP_SERVICE_ACCOUNT,
        project_id: _secrets.GCP_SERVICE_ACCOUNT.get("project_id"),
        ...kwargs,
      });
    }
    if (!("bucket_name" in kwargs)) kwargs.bucket_name = bucket_name;
    return new GCSPool(kwargs);
  };

  describe("GCS: TestGCSPool", gate_options("GCSPool", "@google-cloud/storage", HAVE_DEV_SECRETS, "dev_secrets.toml required for GCS tests"), () => {
    const ctx = { pool: null };
    const get = () => ctx.pool;
    per_test_pool(ctx, () => _make_pool(_secrets.GCP_BUCKET_NAME));

    C.pool_id_format(get);
    C.get_missing_returns_none(get);
    C.set_and_get_roundtrip_string(get);
    C.set_non_string_raises_type_error(get);
    C.set_none_raises_type_error(get);
    C.delitem_silent_if_missing(get);
    C.delitem_removes_existing(get);
    C.exists_true_when_key_present(get);
    C.keys_snapshot_returns_list(get);
    C.keys_generator_yields_all_current_keys(get);
    C.keys_snapshot_is_not_affected_by_later_mutations(get);
    C.overwrite_value(get);
    C.empty_removes_all(get);
    C.concurrent_writes_no_loss(get, { n: 10 });
    C.store_entry_payload_as_object(get);
    C.close_is_idempotent(get);
    C.delete_then_recreate(get);
    C.atomic_reentrant_same_thread(get);
    C.atomic_thread_safety_for_read_modify_write(get, { n_threads: 3, n_steps: 5 });
    C.persistence_across_pool_instances(get, () => _make_pool(ctx.pool.bucket_name));
    C.key_with_special_chars_roundtrip(get);
  });
}

// ===========================================================================
// test_huggingface_pool.py
// ===========================================================================
{
  const { have: HAVE_DEV_SECRETS, secrets: _secrets } = load_secrets(DEV_SECRETS_PATH, ["HF_REPO_ID", "HF_TOKEN"]);

  const _have_hf_secrets = () => {
    if (!(HAVE_DEV_SECRETS && _secrets)) return false;
    const required = ["HF_REPO_ID", "HF_TOKEN"];
    return required.every((k) => k in _secrets);
  };

  const _make_pool = (path_prefix, kwargs = {}) => {
    kwargs = { ...kwargs };
    if (!_have_hf_secrets()) throw new E.RuntimeError("HF_* secrets missing in dev_secrets.toml");
    const repo_id = kwargs.repo_id ?? _secrets.HF_REPO_ID;
    const repo_type = kwargs.repo_type ?? _secrets.get("HF_REPO_TYPE", "dataset");
    const revision = kwargs.revision ?? _secrets.get("HF_REVISION", "main");
    const token = kwargs.token ?? _secrets.HF_TOKEN;
    delete kwargs.repo_id;
    delete kwargs.repo_type;
    delete kwargs.revision;
    delete kwargs.token;
    return new HuggingFacePool({ repo_id, repo_type, revision, token, path_prefix, ...kwargs });
  };

  /** Hugging Face Hub pool tests. Requires HF_* secrets. */
  describe("HuggingFace: TestHuggingFacePool", gate_options("HuggingFacePool", "@huggingface/hub", _have_hf_secrets(), "HF_REPO_ID/HF_TOKEN required in dev_secrets.toml"), () => {
    const ctx = { pool: null, _prefix: null };
    const get = () => ctx.pool;
    // setUpClass
    before(() => {
      ctx._prefix = `laila_hf_tests/${uuid4()}`;
    });
    per_test_pool(ctx, () => _make_pool(ctx._prefix));

    C.pool_id_format(get);
    C.get_missing_returns_none(get);
    C.set_and_get_roundtrip_string(get);
    C.set_non_string_raises_type_error(get);
    C.set_none_raises_type_error(get);
    C.delitem_silent_if_missing(get);
    C.delitem_removes_existing(get);
    C.exists_true_when_key_present(get);
    C.keys_snapshot_returns_list(get);
    C.keys_generator_yields_all_current_keys(get, { n: 5, check_iter: false });
    C.overwrite_value(get);
    C.empty_removes_all(get);
    C.concurrent_writes_no_loss(get, { n: 5 });
    C.store_entry_payload_as_object(get);
    C.close_is_idempotent(get);
    C.delete_then_recreate(get);
    C.atomic_reentrant_same_thread(get);
    C.atomic_thread_safety_for_read_modify_write(get, { n_threads: 3, n_steps: 3 });
    C.persistence_across_pool_instances(get, () => _make_pool(ctx._prefix));
    C.key_with_special_chars_roundtrip(get);
  });
}

// ===========================================================================
// test_mongo_pool.py
// ===========================================================================
{
  const MONGO_URI = process.env.LAILA_MONGO_URI || process.env.MONGO_URI || null;
  const HAVE_MONGOD = which("mongod") !== null;

  const _make_pool = (kwargs = {}) => {
    kwargs = { ...kwargs };
    if (MONGO_URI !== null && !("uri" in kwargs)) kwargs.uri = MONGO_URI;
    return new MongoPool(kwargs);
  };

  describe("Mongo: TestMongoPool", gate_options("MongoPool", "mongodb", Boolean(MONGO_URI || HAVE_MONGOD), "Mongo URI or mongod binary required for MongoPool tests"), () => {
    const ctx = { pool: null, shared: null };
    const get = () => ctx.pool;
    shared_pool(ctx, () => _make_pool({ nickname: "mongo-test-shared" }));

    C.pool_id_format(get);
    C.get_missing_returns_none(get);
    C.set_and_get_roundtrip_string(get);
    C.set_non_string_raises_type_error(get);
    C.set_none_raises_type_error(get);
    C.delitem_silent_if_missing(get);
    C.delitem_removes_existing(get);
    C.exists_true_when_key_present(get);
    C.keys_snapshot_returns_list(get);
    C.keys_generator_yields_all_current_keys(get);
    C.keys_snapshot_is_not_affected_by_later_mutations(get);
    C.overwrite_value(get);
    C.empty_removes_all(get);
    t("multiple_instances_have_independent_servers", () => {
      const p1 = _make_pool({ nickname: "mongo-indep-one" });
      const p2 = _make_pool({ nickname: "mongo-indep-two" });
      try {
        p1["x"] = json.dumps(1);
        p2["x"] = json.dumps(2);
        assert.equal(p1["x"], 1);
        assert.equal(p2["x"], 2);
      } finally {
        p1.empty();
        p2.empty();
        p1.close();
        p2.close();
      }
    });
    // ``_write`` itself takes ``atomic()`` around ``block_on`` (same shape as
    // the verified R2 deadlock; not reproducible here without a server).
    C.concurrent_writes_no_loss(get, { n: 20 });
    C.store_entry_payload_as_object(get);
    C.close_is_idempotent(get, { make: () => _make_pool() });
    C.delete_then_recreate(get);
    C.atomic_reentrant_same_thread(get);
    C.atomic_thread_safety_for_read_modify_write(get, { n_threads: 3, n_steps: 5 });
    t("persistence_across_pool_instances", () => {
      const shared_nick = "persistence-test-mongo";
      const p1 = _make_pool({ nickname: shared_nick });
      p1["persistent"] = json.dumps({ a: 1 });
      p1.close();

      const p2 = _make_pool({ nickname: shared_nick });
      try {
        assert.deepEqual(p2["persistent"], { a: 1 });
      } finally {
        p2.empty();
        p2.close();
      }
    });
    C.key_with_special_chars_roundtrip(get);
  });

  describe("Mongo: TestMongoPoolLocalBootstrap", () => {
    const _fake_client = () => {
      const collection = { createIndex: Mock(async () => undefined) };
      const db = { collection: Mock(() => collection) };
      const client = { db: Mock(() => db), close: Mock(async () => undefined) };
      return client;
    };

    // ``patch("laila.data.mongo.mongo.MongoClient", return_value=fake_client)``
    // rebinds a module global; the JS module keeps ``MongoClient`` in a
    // module-private ``const`` (src/data/mongo/mongo.js:23) that cannot be
    // patched from outside, and without the swap ``_connect`` dials a real
    // server (or raises ImportError when ``mongodb`` is not installed).
    const _NO_MODULE_PATCH = "unittest.mock.patch('laila.data.mongo.mongo.MongoClient') has no JS analogue: module-level const binding (src/data/mongo/mongo.js:23)";

    test("local_server_reused_when_available", { skip: _NO_MODULE_PATCH }, () =>
      macrotask(() => {
        const fake_client = _fake_client();
        void fake_client;
        const patches = [patch_object(MongoPool, "_local_server_available", () => true)];
        let pool;
        with_patches(patches, () => {
          pool = new MongoPool();
        });
        try {
          assert.ok(pool._mongo_dir.startsWith(LAILA_DEFAULT_DIRECTORIES.pools));
          assert.equal(pool.host, "127.0.0.1");
        } finally {
          pool.close();
        }
      }),
    );

    test("local_server_started_when_missing", { skip: _NO_MODULE_PATCH }, () =>
      macrotask(() => {
        const fake_client = _fake_client();
        void fake_client;
        const patches = [patch_object(MongoPool, "_local_server_available", () => false), patch_object(MongoPool, "_start_local_server", () => undefined)];
        const start_mock = patches[1].mock;
        let pool;
        with_patches(patches, () => {
          pool = new MongoPool();
        });
        try {
          start_mock.assert_called_once();
        } finally {
          pool.close();
        }
      }),
    );

    t("close_stops_owned_local_server", () => {
      const pool = new MongoPool(SKIP_VALIDATION);
      const client = { close: Mock(async () => undefined) };
      const proc = { terminate: Mock(() => undefined), wait: Mock(() => undefined), kill: Mock(() => undefined) };
      pool._client = client;
      pool._mongo_proc = proc;
      pool._owns_local_server = true;
      MongoPool.prototype.close.call(pool);
      client.close.assert_called_once();
      proc.terminate.assert_called_once();
    });
  });
}

// ===========================================================================
// test_postgres_pool.py
// ===========================================================================
{
  const POSTGRES_DSN = process.env.LAILA_POSTGRES_DSN || process.env.POSTGRES_DSN || null;
  const HAVE_INITDB = which("initdb") !== null;
  // Python's ``psycopg`` is the npm ``pg`` package: constructing a pool
  // raises ``ImportError`` from ``_connect`` when it is missing, in both
  // runtimes, so the bootstrap tests that build a pool need it installed.
  const HAVE_PG = optional_import("pg") !== null;

  const _make_pool = (kwargs = {}) => {
    kwargs = { ...kwargs };
    if (POSTGRES_DSN !== null && !("dsn" in kwargs)) kwargs.dsn = POSTGRES_DSN;
    return new PostgresPool(kwargs);
  };

  describe("Postgres: TestPostgresPool", gate_options("PostgresPool", "pg", Boolean(POSTGRES_DSN || HAVE_INITDB), "Postgres DSN or initdb/postgres binary required"), () => {
    const ctx = { pool: null, shared: null };
    const get = () => ctx.pool;
    shared_pool(ctx, () => _make_pool({ nickname: "postgres-test-shared" }));

    C.pool_id_format(get);
    C.get_missing_returns_none(get);
    C.set_and_get_roundtrip_string(get);
    C.set_non_string_raises_type_error(get);
    C.set_none_raises_type_error(get);
    C.delitem_silent_if_missing(get);
    C.delitem_removes_existing(get);
    C.exists_true_when_key_present(get);
    C.keys_snapshot_returns_list(get);
    C.keys_generator_yields_all_current_keys(get);
    C.keys_snapshot_is_not_affected_by_later_mutations(get);
    C.overwrite_value(get);
    C.empty_removes_all(get);
    t("multiple_instances_have_independent_servers", () => {
      const p1 = _make_pool({ nickname: "postgres-pool-one" });
      const p2 = _make_pool({ nickname: "postgres-pool-two" });
      try {
        p1["x"] = json.dumps(1);
        p2["x"] = json.dumps(2);
        assert.equal(p1["x"], 1);
        assert.equal(p2["x"], 2);
      } finally {
        p1.empty();
        p2.empty();
        p1.close();
        p2.close();
      }
    });
    // ``_write`` itself takes ``atomic()`` around ``block_on`` (same shape as
    // the verified R2 deadlock; not reproducible here without a server).
    C.concurrent_writes_no_loss(get, { n: 20 });
    C.store_entry_payload_as_object(get);
    C.close_is_idempotent(get, { make: () => _make_pool() });
    C.delete_then_recreate(get);
    C.atomic_reentrant_same_thread(get);
    C.atomic_thread_safety_for_read_modify_write(get, { n_threads: 3, n_steps: 5 });
    t("persistence_across_pool_instances", () => {
      const shared_nick = "persistence-test-postgres";
      const p1 = _make_pool({ nickname: shared_nick });
      p1["persistent"] = json.dumps({ a: 1 });
      p1.close();

      const p2 = _make_pool({ nickname: shared_nick });
      try {
        assert.deepEqual(p2["persistent"], { a: 1 });
      } finally {
        p2.empty();
        p2.close();
      }
    });
    C.key_with_special_chars_roundtrip(get);
  });

  describe("Postgres: TestPostgresPoolLocalBootstrap", () => {
    const _fake_connection = () => {
      const cursor = {
        execute: Mock(() => cursor),
        fetchone: Mock(() => null),
        fetchall: Mock(() => []),
        __enter__: Mock(() => cursor),
        __exit__: Mock(() => false),
      };
      const conn = { cursor: Mock(() => cursor), commit: Mock(() => undefined), close: Mock(() => undefined) };
      return conn;
    };
    const needs_pg = HAVE_PG ? {} : { skip: "pg (psycopg) not installed: PostgresPool._connect raises ImportError before the patched bootstrap hooks run" };

    test("local_server_reused_when_available", needs_pg, () =>
      macrotask(() => {
        const fake_conn = _fake_connection();
        const patches = [
          patch_object(PostgresPool, "_local_server_available", () => true),
          patch_object(PostgresPool, "_initdb_if_needed", () => undefined),
          patch_object(PostgresPool, "_start_local_server", () => undefined),
          patch_object(PostgresPool, "_connect_local", () => fake_conn),
        ];
        const [, initdb_mock, start_mock] = patches.map((p) => p.mock);
        let pool;
        with_patches(patches, () => {
          pool = new PostgresPool();
        });
        try {
          initdb_mock.assert_not_called();
          start_mock.assert_not_called();
          const expected_pool_dir = path.join(LAILA_DEFAULT_DIRECTORIES.pools, pool.uuid);
          assert.equal(pool._socket_dir, path.join(expected_pool_dir, "socket"));
          assert.equal(pool._postgres_dir, path.join(expected_pool_dir, "data"));
        } finally {
          pool.close();
        }
      }),
    );

    test("local_server_started_when_missing", needs_pg, () =>
      macrotask(() => {
        const fake_conn = _fake_connection();
        const patches = [
          patch_object(PostgresPool, "_local_server_available", () => false),
          patch_object(PostgresPool, "_initdb_if_needed", () => undefined),
          patch_object(PostgresPool, "_start_local_server", () => undefined),
          patch_object(PostgresPool, "_connect_local", () => fake_conn),
        ];
        const [, initdb_mock, start_mock] = patches.map((p) => p.mock);
        let pool;
        with_patches(patches, () => {
          pool = new PostgresPool();
        });
        try {
          initdb_mock.assert_called_once();
          start_mock.assert_called_once();
        } finally {
          pool.close();
        }
      }),
    );

    /** ``patch("os.path.exists", return_value=...)`` -- the JS hook is ``fs.existsSync``. */
    const patch_exists = (value) => {
      const original = fs.existsSync;
      fs.existsSync = () => value;
      return { stop: () => void (fs.existsSync = original) };
    };

    t("initdb_skipped_when_cluster_exists", () => {
      const pool = new PostgresPool(SKIP_VALIDATION);
      pool._postgres_dir = "/tmp/test-postgres-dir";
      const patches = [patch_exists(true), patch_object(PostgresPool, "_run_command", () => undefined)];
      const run_mock = patches[1].mock;
      with_patches(patches, () => {
        PostgresPool.prototype._initdb_if_needed.call(pool);
      });
      run_mock.assert_not_called();
    });

    t("initdb_runs_when_cluster_missing", () => {
      const pool = new PostgresPool(SKIP_VALIDATION);
      pool._postgres_dir = "/tmp/test-postgres-dir";
      pool._local_user = "laila";
      const patches = [patch_exists(false), patch_object(PostgresPool, "_run_command", () => undefined)];
      const run_mock = patches[1].mock;
      with_patches(patches, () => {
        PostgresPool.prototype._initdb_if_needed.call(pool);
      });
      run_mock.assert_called_once();
    });

    t("close_stops_owned_local_server", () => {
      const pool = new PostgresPool(SKIP_VALIDATION);
      const conn = { close: Mock(() => undefined) };
      const proc = { terminate: Mock(() => undefined), wait: Mock(() => undefined), kill: Mock(() => undefined) };
      pool._conn = conn;
      pool._postgres_proc = proc;
      pool._owns_local_server = true;
      PostgresPool.prototype.close.call(pool);
      conn.close.assert_called_once();
      proc.terminate.assert_called_once();
    });
  });
}

// ===========================================================================
// test_redis_pool.py
// ===========================================================================
{
  const HAVE_REDIS_SERVER = which("redis-server") !== null;

  describe("Redis: TestRedisPool", gate_options("RedisPool", "redis", HAVE_REDIS_SERVER, "redis-server not found"), () => {
    const ctx = { pool: null, shared: null };
    const get = () => ctx.pool;
    // setUpClass / tearDownClass / setUp (``self.pool.empty()`` unsuppressed)
    before(() =>
      macrotask(() => {
        ctx.shared = new RedisPool({ nickname: "redis-test-shared" });
      }),
    );
    after(() =>
      macrotask(() => {
        if (ctx.shared !== null && ctx.shared !== undefined) ctx.shared.close();
      }),
    );
    beforeEach(() =>
      macrotask(() => {
        ctx.pool = ctx.shared;
        ctx.pool.empty();
      }),
    );

    // 1
    C.pool_id_format(get);
    // 2
    t("client_is_initialized", () => assert.notEqual(get()._client, null));
    // 3
    C.get_missing_returns_none(get);
    // 4
    C.set_and_get_roundtrip_string(get);
    // 5
    C.set_non_string_raises_type_error(get);
    // 6
    C.set_none_raises_type_error(get);
    // 7
    C.delitem_silent_if_missing(get);
    // 8
    C.delitem_removes_existing(get);
    // 9
    C.exists_true_when_key_present(get);
    // 10
    C.keys_snapshot_returns_list(get);
    // 11
    C.keys_generator_yields_all_current_keys(get);
    // 12
    C.keys_snapshot_is_not_affected_by_later_mutations(get);
    // 13
    C.overwrite_value(get);
    // 14
    t("multiple_instances_have_independent_servers", () => {
      const p1 = new RedisPool();
      const p2 = new RedisPool();
      try {
        p1["x"] = json.dumps(1);
        p2["x"] = json.dumps(2);
        assert.equal(p1["x"], 1);
        assert.equal(p2["x"], 2);
      } finally {
        p1.close();
        p2.close();
      }
    });
    // 15
    C.concurrent_writes_no_loss(get, { n: 50 });
    // 16
    t("concurrent_writes_and_reads", () => {
      const stop = new TH.Event();
      const read_errors = [];

      const writer = () => {
        let i = 0;
        while (!stop.is_set()) {
          get()[String(i)] = json.dumps(i);
          i = (i + 1) % 10;
        }
      };

      const reader = () => {
        while (!stop.is_set()) {
          for (const k of [...get().keys()]) {
            const v = get()[k];
            if (v !== null && !Number.isInteger(v)) read_errors.push(`bad value ${JSON.stringify(v)}`);
          }
        }
      };

      const tw = new TH.Thread({ target: writer });
      const tr = new TH.Thread({ target: reader });
      tw.start();
      tr.start();
      time.sleep(0.2);
      stop.set();
      tw.join();
      tr.join();

      assert.deepEqual(read_errors, []);
    });
    // 17
    C.store_entry_payload_as_object(get);
    // 18
    C.close_is_idempotent(get, { make: () => new RedisPool() });
    // 19
    C.delete_then_recreate(get);
    // 20
    t("custom_key_prefix_affects_redis_hash_key", () => {
      const p = new RedisPool({ key_prefix: "custom_pool" });
      try {
        assert.equal(p.redis_hash_key, "custom_pool");
      } finally {
        p.close();
      }
    });
    // 21
    t("custom_lock_prefix_affects_redis_lock_key", () => {
      const p = new RedisPool({ lock_prefix: "custom_lock" });
      try {
        assert.equal(p.redis_lock_key, "custom_lock");
      } finally {
        p.close();
      }
    });
    // 22
    t("redis_password_parameter_roundtrip", () => {
      const p = new RedisPool({ redis_password: "test-pass" });
      try {
        assert.equal(p.redis_password, "test-pass");
        // If initialization succeeded, auth + ping path worked.
        assert.notEqual(p._client, null);
      } finally {
        p.close();
      }
    });
    // 23
    C.atomic_reentrant_same_thread(get);
    // 24
    C.atomic_thread_safety_for_read_modify_write(get, { n_threads: 10, n_steps: 50 });
    // 25
    t_todo("atomic_timeout_when_lock_held_by_other_thread", _NESTED_EVENT_WAIT, () => {
      const ready = new TH.Event();
      const release = new TH.Event();
      const errors = [];

      const holder = () => {
        try {
          with_(get().atomic(), () => {
            ready.set();
            release.wait(1.0);
          });
        } catch (e) {
          errors.push(e);
        }
      };

      const th = new TH.Thread({ target: holder });
      th.start();
      ready.wait(1.0);

      assert.throws(() => with_(get().atomic({ timeout_s: 0.05 }), () => {}), E.TimeoutError);

      release.set();
      th.join(1.0);
      assert.deepEqual(errors, []);
    });
  });
}

test("zz teardown", () =>
  macrotask(() => {
    laila.terminate();
    fs.rmSync(TMP_ROOT, { recursive: true, force: true });
  }));
