/**
 * Local pool backends: ports of
 *   tests/functional/pools/sqlite/unit_tests/test_sqlite_pool.py
 *   tests/functional/pools/duckdb/unit_tests/test_duckdb_pool.py
 *   tests/functional/pools/hdf5/unit_tests/test_hdf5_pool.py
 *   tests/functional/pools/filesystem/unit_tests/test_filesystem_pool.py
 *   tests/functional/pools/{sqlite,duckdb,hdf5,filesystem}/unit_tests/test_*_compdata_roundtrip.py
 *
 * The three file-backed stores share one test body (the Python files are
 * byte-for-byte copies modulo class name / suffix); the filesystem pool
 * stubs out the loop-mount machinery exactly like the Python suite.
 */
import { test, describe, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const { default: laila, S } = await import("./fixtures/laila_root.js");
const RT = await import("./fixtures/compdata_roundtrip.js");

const E = await import(S + "_compat/errors.js");
const TH = await import(S + "_compat/threading.js");
const json = await import(S + "_compat/pyjson.js");
const { with_ } = await import(S + "_compat/contextlib.js");
const { SQLitePool } = await import(S + "data/sqlite/sqlite.js");
const { DuckDBPool } = await import(S + "data/duckdb/duckdb.js");
const { HDF5Pool } = await import(S + "data/hdf5/hdf5.js");
const { FilesystemPool } = await import(S + "data/filesystem/filesystem.js");

const TMP_ROOT = fs.mkdtempSync(path.join(os.tmpdir(), "laila_pools_local_"));
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
const mkdtemp = (prefix) => fs.mkdtempSync(path.join(os.tmpdir(), prefix));
const rm = (p) => fs.rmSync(p, { recursive: true, force: true });

// ---------------------------------------------------------------------------
// Shared body: test_sqlite_pool.py / test_duckdb_pool.py / test_hdf5_pool.py
// ---------------------------------------------------------------------------
function file_backed_pool_suite(name, Pool, { prefix, suffix, has_empty = true, identity_handoff = false }) {
  describe(name, () => {
    const dirs = [];
    const _make_pool = (kwargs = {}) => {
      if (!("file_path" in kwargs)) {
        const tmp = mkdtemp(prefix);
        dirs.push(tmp);
        kwargs.file_path = path.join(tmp, `pool${suffix}`);
      }
      return new Pool(kwargs);
    };
    let pool;
    beforeEach(() =>
      macrotask(() => {
        pool = _make_pool();
      }),
    );
    afterEach(() =>
      macrotask(() => {
        try {
          pool?.close();
        } catch {
          // ignore
        }
        while (dirs.length) rm(dirs.pop());
      }),
    );

    t("pool id format", () => assert.ok(pool.pool_id.startsWith("LAILA:POOL:")));
    t("file path is set", () => {
      assert.notEqual(pool.file_path, null);
      assert.ok(pool.file_path.endsWith(suffix));
    });
    t("get missing returns none", () => {
      assert.equal(pool["missing"], null);
      assert.equal(pool.exists("missing"), false);
    });
    t("set and get roundtrip string", () => {
      pool["a"] = json.dumps("123");
      assert.equal(pool["a"], "123");
    });
    t("set non string raises type error", () => {
      assert.throws(() => {
        pool["bad"] = 123;
      }, E.TypeError);
    });
    t("set none raises type error", () => {
      assert.throws(() => {
        pool["none"] = null;
      }, E.TypeError);
    });
    t("delitem silent if missing", () => {
      pool.__delitem__("ghost");
      assert.equal(pool.exists("ghost"), false);
    });
    t("delitem removes existing", () => {
      pool["x"] = json.dumps("val");
      assert.equal(pool.exists("x"), true);
      delete pool["x"];
      assert.equal(pool.exists("x"), false);
      assert.equal(pool["x"], null);
    });
    t("exists true when key present", () => {
      pool["present"] = json.dumps(7);
      assert.equal(pool.exists("present"), true);
      assert.equal("present" in pool, true);
    });
    t("keys snapshot returns list", () => {
      pool["a"] = json.dumps(1);
      pool["b"] = json.dumps(2);
      const ks = pool.keys({ as_generator: false });
      assert.ok(Array.isArray(ks));
      count_equal(ks, ["a", "b"]);
    });
    t("keys generator yields all current keys", () => {
      const items = Object.fromEntries(Array.from({ length: 10 }, (_, i) => [`k${i}`, json.dumps(i)]));
      for (const [k, v] of Object.entries(items)) pool[k] = v;
      const it = pool.keys({ as_generator: true });
      assert.equal(typeof it[Symbol.iterator], "function");
      count_equal([...it], Object.keys(items));
    });
    t("keys snapshot is not affected by later mutations", () => {
      pool["a"] = json.dumps(1);
      const snap = pool.keys();
      pool["b"] = json.dumps(2);
      count_equal(snap, ["a"]);
    });
    t("overwrite value", () => {
      pool["k"] = json.dumps("v1");
      pool["k"] = json.dumps("v2");
      assert.equal(pool["k"], "v2");
    });
    if (has_empty) {
      t("empty removes all", () => {
        pool["a"] = json.dumps(1);
        pool["b"] = json.dumps(2);
        pool.empty();
        assert.equal(pool["a"], null);
        assert.equal(pool["b"], null);
        assert.deepEqual([...pool.keys()], []);
      });
    }
    t("multiple instances have independent files", () => {
      const tmp1 = mkdtemp(prefix);
      const tmp2 = mkdtemp(prefix);
      const p1 = new Pool({ file_path: path.join(tmp1, `p1${suffix}`) });
      const p2 = new Pool({ file_path: path.join(tmp2, `p2${suffix}`) });
      try {
        p1["x"] = json.dumps(1);
        p2["x"] = json.dumps(2);
        assert.equal(p1["x"], 1);
        assert.equal(p2["x"], 2);
      } finally {
        p1.close();
        p2.close();
        rm(tmp1);
        rm(tmp2);
      }
    });
    t("concurrent writes no loss", () => {
      const n = 50;
      const threads = [];
      for (let i = 0; i < n; i++) {
        const th = new TH.Thread({ target: (j) => void (pool[String(j)] = json.dumps(j)), args: [i] });
        th.start();
        threads.push(th);
      }
      for (const th of threads) th.join();
      for (let i = 0; i < n; i++) assert.equal(pool[String(i)], i);
    });
    t("concurrent writes and reads", () => {
      // Bounded version of the 0.2s writer/reader race (cooperative threads).
      const read_errors = [];
      const writer = () => {
        for (let i = 0; i < 200; i++) pool[String(i % 10)] = json.dumps(i);
      };
      const reader = () => {
        for (let r = 0; r < 20; r++) {
          for (const k of [...pool.keys()]) {
            const v = pool[k];
            if (v !== null && !Number.isInteger(v)) read_errors.push(`bad value ${v}`);
          }
        }
      };
      const tw = new TH.Thread({ target: writer });
      const tr = new TH.Thread({ target: reader });
      tw.start();
      tr.start();
      tw.join();
      tr.join();
      assert.deepEqual(read_errors, []);
    });
    t("store entry payload as object", () => {
      const e = laila.constant({ x: 1 });
      const payload = { global_id: e.global_id, data: e.data };
      pool[e.global_id] = payload;
      assert.deepEqual(pool[e.global_id], payload);
    });
    t("close is idempotent", () => {
      pool.close();
      pool.close();
    });
    t("delete then recreate", () => {
      pool["z"] = json.dumps(9);
      delete pool["z"];
      assert.equal(pool.exists("z"), false);
      pool["z"] = json.dumps(10);
      assert.equal(pool["z"], 10);
    });
    if (Pool === HDF5Pool) {
      t("custom file path used", () => {
        const tmp = mkdtemp(prefix);
        const p = path.join(tmp, `custom${suffix}`);
        const custom = new Pool({ file_path: p });
        try {
          assert.equal(custom.file_path, p);
          custom["k"] = json.dumps(1);
          assert.ok(fs.existsSync(p));
          assert.equal(custom["k"], 1);
        } finally {
          custom.close();
          rm(tmp);
        }
      });
    }
    t("atomic reentrant same thread", () => {
      with_(pool.atomic(), () => {
        pool["r"] = json.dumps(1);
        with_(pool.atomic(), () => {
          pool["r"] = json.dumps(2);
        });
      });
      assert.equal(pool["r"], 2);
    });
    t("atomic thread safety for read modify write", () => {
      pool["counter"] = json.dumps(0);
      const n_threads = 10;
      const n_steps = 50;
      const worker = () => {
        for (let s = 0; s < n_steps; s++) {
          with_(pool.atomic(), () => {
            const current = Number(pool["counter"]);
            pool["counter"] = json.dumps(current + 1);
          });
        }
      };
      const threads = Array.from({ length: n_threads }, () => new TH.Thread({ target: worker }));
      for (const th of threads) th.start();
      for (const th of threads) th.join();
      assert.equal(Number(pool["counter"]), n_threads * n_steps);
    });
    t("persistence across pool instances", () => {
      const tmp = mkdtemp(prefix);
      const p = path.join(tmp, `persist${suffix}`);
      const p1 = new Pool({ file_path: p });
      const persisted_uuid = p1.uuid;
      const persisted_scopes = [...p1.scopes];
      const persisted_evolution = p1.evolution;
      p1["persistent"] = json.dumps({ a: 1 });
      p1.close();

      const p2 = new Pool({ file_path: p });
      if (identity_handoff) {
        p2.uuid = persisted_uuid;
        p2.scopes = persisted_scopes;
        p2.evolution = persisted_evolution;
      }
      try {
        assert.deepEqual(p2["persistent"], { a: 1 });
      } finally {
        p2.close();
        rm(tmp);
      }
    });
    t("key with special chars roundtrip", () => {
      const key = "key/with:special=chars";
      pool[key] = json.dumps("value");
      assert.equal(pool.exists(key), true);
      assert.equal(pool[key], "value");
      delete pool[key];
      assert.equal(pool.exists(key), false);
    });
  });
}

file_backed_pool_suite("TestSQLitePool", SQLitePool, { prefix: "laila_sqlite_test_", suffix: ".laila_sqlitedb", identity_handoff: true });
file_backed_pool_suite("TestDuckDBPool", DuckDBPool, { prefix: "laila_duckdb_test_", suffix: ".duckdb" });
file_backed_pool_suite("TestHDF5Pool", HDF5Pool, { prefix: "laila_hdf5_test_", suffix: ".h5py", has_empty: false });

// ---------------------------------------------------------------------------
// test_filesystem_pool.py
// ---------------------------------------------------------------------------
describe("TestFilesystemPool", () => {
  const P = FilesystemPool.prototype;
  const ctx = {};

  /** ``patch.object(FilesystemPool, name, autospec=True, ...)`` */
  function patch(name, impl) {
    const original = P[name];
    const spy = function (...args) {
      spy.calls.push(args);
      return spy.impl.call(this, ...args);
    };
    spy.calls = [];
    spy.impl = impl;
    spy.reset_mock = () => {
      spy.calls = [];
    };
    P[name] = spy;
    return { spy, stop: () => void (P[name] = original) };
  }

  const _fake_mount = function () {
    fs.mkdirSync(this.mount_dir, { recursive: true });
  };
  const _fake_create = function () {
    fs.mkdirSync(path.dirname(this.image_path), { recursive: true });
    fs.closeSync(fs.openSync(this.image_path, "a"));
  };

  beforeEach(() =>
    macrotask(() => {
      ctx.mount_root = mkdtemp("laila_filesystem_mount_");
      ctx.patchers = [
        patch("_resolve_pool_dir", function () {
          return path.join(ctx.mount_root, this.pool_id);
        }),
        patch("_is_mounted", () => false),
        patch("_mount_image", _fake_mount),
        patch("_create_image_file", _fake_create),
      ];
      [ctx.mock_resolve_pool_dir, ctx.mock_is_mounted, ctx.mock_mount_image, ctx.mock_create_image_file] = ctx.patchers.map((p) => p.spy);
      ctx.pool = new FilesystemPool();
    }),
  );
  afterEach(() =>
    macrotask(() => {
      ctx.pool?.close();
      for (const p of [...ctx.patchers].reverse()) p.stop();
      rm(ctx.mount_root);
    }),
  );

  t("pool id format", () => assert.ok(ctx.pool.pool_id.startsWith("LAILA:POOL:")));

  t("image and mount paths are created", () => {
    const pool = ctx.pool;
    assert.ok(fs.existsSync(pool.image_path));
    assert.ok(fs.statSync(pool.mount_dir).isDirectory());
    assert.ok(pool.image_path.endsWith(".img"));
    assert.equal(pool.pool_dir, path.join(ctx.mount_root, pool.pool_id));
    assert.equal(pool.image_path, path.join(pool.pool_dir, `${pool.pool_id}.img`));
    assert.equal(pool.mount_dir, path.join(pool.pool_dir, "mnt"));
  });

  t("get missing returns none without creating file", () => {
    assert.equal(ctx.pool["missing"], null);
    assert.equal(ctx.pool.exists("missing"), false);
    assert.deepEqual(fs.readdirSync(ctx.pool.mount_dir), []);
  });

  t("set and get roundtrip string", () => {
    ctx.pool["a"] = json.dumps("123");
    assert.equal(ctx.pool["a"], "123");
  });
  t("set non string raises type error", () => {
    assert.throws(() => {
      ctx.pool["bad"] = 123;
    }, E.TypeError);
  });
  t("set none raises type error", () => {
    assert.throws(() => {
      ctx.pool["none"] = null;
    }, E.TypeError);
  });
  t("delitem silent if missing", () => {
    ctx.pool.__delitem__("ghost");
    assert.equal(ctx.pool.exists("ghost"), false);
  });
  t("delitem removes existing", () => {
    ctx.pool["x"] = json.dumps("val");
    assert.equal(ctx.pool.exists("x"), true);
    delete ctx.pool["x"];
    assert.equal(ctx.pool.exists("x"), false);
    assert.equal(ctx.pool["x"], null);
  });
  t("exists true when key present", () => {
    ctx.pool["present"] = json.dumps(7);
    assert.equal(ctx.pool.exists("present"), true);
    assert.equal("present" in ctx.pool, true);
  });
  t("keys snapshot returns list", () => {
    ctx.pool["a"] = json.dumps(1);
    ctx.pool["b"] = json.dumps(2);
    const ks = ctx.pool.keys({ as_generator: false });
    assert.ok(Array.isArray(ks));
    count_equal(ks, ["a", "b"]);
  });
  t("keys generator yields all current keys", () => {
    const items = Object.fromEntries(Array.from({ length: 10 }, (_, i) => [`k${i}`, json.dumps(i)]));
    for (const [k, v] of Object.entries(items)) ctx.pool[k] = v;
    const it = ctx.pool.keys({ as_generator: true });
    assert.equal(typeof it[Symbol.iterator], "function");
    count_equal([...it], Object.keys(items));
  });
  t("keys snapshot is not affected by later mutations", () => {
    ctx.pool["a"] = json.dumps(1);
    const snap = ctx.pool.keys();
    ctx.pool["b"] = json.dumps(2);
    count_equal(snap, ["a"]);
  });
  t("overwrite value", () => {
    ctx.pool["k"] = json.dumps("v1");
    ctx.pool["k"] = json.dumps("v2");
    assert.equal(ctx.pool["k"], "v2");
  });
  t("multiple instances have independent mounts", () => {
    const p1 = new FilesystemPool();
    const p2 = new FilesystemPool();
    try {
      p1["x"] = json.dumps(1);
      p2["x"] = json.dumps(2);
      assert.equal(p1["x"], 1);
      assert.equal(p2["x"], 2);
      assert.notEqual(p1.mount_dir, p2.mount_dir);
    } finally {
      p1.close();
      p2.close();
    }
  });
  t("concurrent writes no loss", () => {
    const n = 50;
    const threads = [];
    for (let i = 0; i < n; i++) {
      const th = new TH.Thread({ target: (j) => void (ctx.pool[String(j)] = json.dumps(j)), args: [i] });
      th.start();
      threads.push(th);
    }
    for (const th of threads) th.join();
    for (let i = 0; i < n; i++) assert.equal(ctx.pool[String(i)], i);
  });
  t("concurrent writes and reads", () => {
    const read_errors = [];
    const writer = () => {
      for (let i = 0; i < 200; i++) ctx.pool[String(i % 10)] = json.dumps(i);
    };
    const reader = () => {
      for (let r = 0; r < 20; r++) {
        for (const k of [...ctx.pool.keys()]) {
          const v = ctx.pool[k];
          if (v !== null && !Number.isInteger(v)) read_errors.push(`bad value ${v}`);
        }
      }
    };
    const tw = new TH.Thread({ target: writer });
    const tr = new TH.Thread({ target: reader });
    tw.start();
    tr.start();
    tw.join();
    tr.join();
    assert.deepEqual(read_errors, []);
  });
  t("store entry payload as object", () => {
    const e = laila.constant({ x: 1 });
    const payload = { global_id: e.global_id, data: e.data };
    ctx.pool[e.global_id] = payload;
    assert.deepEqual(ctx.pool[e.global_id], payload);
  });
  t("close is idempotent", () => {
    ctx.pool.close();
    ctx.pool.close();
  });
  t("delete then recreate", () => {
    ctx.pool["z"] = json.dumps(9);
    delete ctx.pool["z"];
    assert.equal(ctx.pool.exists("z"), false);
    ctx.pool["z"] = json.dumps(10);
    assert.equal(ctx.pool["z"], 10);
  });
  t("paths follow fixed pool layout", () => {
    const pool = new FilesystemPool({ nickname: "filesystem-fixed-paths" });
    try {
      assert.equal(pool.pool_dir, path.join(ctx.mount_root, pool.pool_id));
      assert.equal(pool.image_path, path.join(pool.pool_dir, `${pool.pool_id}.img`));
      assert.equal(pool.mount_dir, path.join(pool.pool_dir, "mnt"));
      assert.ok(fs.existsSync(pool.image_path));
    } finally {
      pool.close();
    }
  });
  t("mount_dir argument is rejected", () => {
    assert.throws(() => new FilesystemPool({ mount_dir: "/tmp/not-allowed" }), E.ValueError);
  });
  t("image_dir argument is rejected", () => {
    assert.throws(() => new FilesystemPool({ image_dir: "/tmp/not-allowed" }), E.ValueError);
  });
  t("image_path argument is rejected", () => {
    assert.throws(() => new FilesystemPool({ image_path: path.join("/tmp", "bad.img") }), E.ValueError);
  });
  t("persistence across pool instances", () => {
    const pool_dir = path.join(ctx.mount_root, "persistent-pool");
    const image_path = path.join(pool_dir, "persistent.img");
    const mount_dir = path.join(pool_dir, "mnt");
    fs.mkdirSync(mount_dir, { recursive: true });

    const original_impl = ctx.mock_resolve_pool_dir.impl;
    ctx.mock_resolve_pool_dir.impl = () => pool_dir;
    const img = patch("_resolve_image_path", () => image_path);
    let pool2;
    try {
      const pool1 = new FilesystemPool();
      pool1["persistent"] = json.dumps({ a: 1 });
      pool1.close();
      pool2 = new FilesystemPool();
    } finally {
      ctx.mock_resolve_pool_dir.impl = original_impl;
      img.stop();
    }
    try {
      assert.deepEqual(pool2["persistent"], { a: 1 });
    } finally {
      pool2.close();
    }
  });
  t("key with special chars roundtrip", () => {
    const key = "key/with:special=chars";
    ctx.pool[key] = json.dumps("value");
    assert.equal(ctx.pool.exists(key), true);
    assert.equal(ctx.pool[key], "value");
    delete ctx.pool[key];
    assert.equal(ctx.pool.exists(key), false);
  });
  t("reuses existing mount without mounting image", () => {
    const pool_dir = path.join(ctx.mount_root, "reused-pool");
    const image_path = path.join(pool_dir, "reused.img");
    const mount_dir = path.join(pool_dir, "mnt");
    fs.mkdirSync(mount_dir, { recursive: true });
    fs.closeSync(fs.openSync(image_path, "a"));

    ctx.mock_mount_image.reset_mock();
    ctx.mock_create_image_file.reset_mock();

    const original_impl = ctx.mock_resolve_pool_dir.impl;
    const original_is_mounted = ctx.mock_is_mounted.impl;
    ctx.mock_resolve_pool_dir.impl = () => pool_dir;
    const img = patch("_resolve_image_path", () => image_path);
    let pool;
    try {
      ctx.mock_is_mounted.impl = () => true;
      pool = new FilesystemPool();
      assert.equal(pool.mount_dir, mount_dir);
      assert.equal(ctx.mock_mount_image.calls.length, 0);
      assert.equal(ctx.mock_create_image_file.calls.length, 0);
    } finally {
      ctx.mock_resolve_pool_dir.impl = original_impl;
      ctx.mock_is_mounted.impl = original_is_mounted;
      img.stop();
      pool?.close();
    }
    try {
      assert.ok(fs.statSync(mount_dir).isDirectory());
    } finally {
      rm(pool_dir);
    }
  });
  t("existing image is mounted without recreating image", () => {
    const pool_dir = path.join(ctx.mount_root, "existing-pool");
    const image_path = path.join(pool_dir, "existing.img");
    fs.mkdirSync(pool_dir, { recursive: true });
    fs.writeFileSync(image_path, Buffer.from("existing-image"));

    ctx.mock_mount_image.reset_mock();
    ctx.mock_create_image_file.reset_mock();

    const original_impl = ctx.mock_resolve_pool_dir.impl;
    ctx.mock_resolve_pool_dir.impl = () => pool_dir;
    const img = patch("_resolve_image_path", () => image_path);
    let pool;
    try {
      pool = new FilesystemPool();
    } finally {
      ctx.mock_resolve_pool_dir.impl = original_impl;
      img.stop();
    }
    try {
      assert.equal(ctx.mock_mount_image.calls.length, 1);
      assert.equal(ctx.mock_create_image_file.calls.length, 0);
      assert.equal(pool.image_path, image_path);
    } finally {
      pool.close();
      rm(pool_dir);
    }
  });
});

// ---------------------------------------------------------------------------
// test_*_compdata_roundtrip.py
// ---------------------------------------------------------------------------
describe("TestSQLiteCompDataRoundtrip", () => {
  RT.register_dtype_matrix(() => new SQLitePool({ file_path: path.join(RT.make_tmpdir("laila_sqlite_cd_rt_"), "pool.laila_sqlitedb") }));
});
describe("TestDuckDBCompDataRoundtrip", () => {
  RT.register_dtype_matrix(() => new DuckDBPool({ file_path: path.join(RT.make_tmpdir("laila_duckdb_cd_rt_"), "pool.duckdb") }));
});
describe("TestHDF5CompDataRoundtrip", () => {
  RT.register_dtype_matrix(() => new HDF5Pool({ file_path: path.join(RT.make_tmpdir("laila_hdf5_cd_rt_"), "pool.h5py") }));
});
describe("TestFilesystemCompDataRoundtrip", () => {
  // Same loop-mount stubs as TestFilesystemPool: the matrix exercises the
  // key/value layer, not ``mount(8)``.
  const P = FilesystemPool.prototype;
  const saved = {};
  let mount_root;
  const stubs = {
    _resolve_pool_dir() {
      return path.join(mount_root, this.pool_id);
    },
    _is_mounted: () => false,
    _mount_image() {
      fs.mkdirSync(this.mount_dir, { recursive: true });
    },
    _create_image_file() {
      fs.mkdirSync(path.dirname(this.image_path), { recursive: true });
      fs.closeSync(fs.openSync(this.image_path, "a"));
    },
  };
  beforeEach(() => {
    mount_root = mkdtemp("laila_filesystem_cd_rt_");
    for (const [k, v] of Object.entries(stubs)) {
      saved[k] = P[k];
      P[k] = v;
    }
  });
  afterEach(() => {
    for (const [k, v] of Object.entries(saved)) P[k] = v;
    rm(mount_root);
  });
  RT.register_dtype_matrix(() => new FilesystemPool());
});

test("zz teardown", () =>
  macrotask(() => {
    laila.terminate();
    rm(TMP_ROOT);
  }));
