/** DuckDB pool implementation. */
import fs from "node:fs";
import path from "node:path";

import { block_on } from "../../_compat/blocking.js";
import { with_ } from "../../_compat/contextlib.js";
import { ImportError, RuntimeError, TypeError as PyTypeError } from "../../_compat/errors.js";
import { lazy, register } from "../../_compat/lazy.js";
import { optional_import } from "../../_compat/optional.js";
import { ConfigDict, Field, PrivateAttr, define_fields, define_private } from "../../_compat/pydantic.js";
import * as json from "../../_compat/pyjson.js";
import { is_str, isdict } from "../../_compat/pytypes.js";
import { transformation_base64 } from "../../entry/index.js";
import { TransformationSequence } from "../../entry/compdata/transformation/base.js";
import { _LAILA_IDENTIFIABLE_POOL } from "../schema/base.js";
import { _expanduser } from "../sqlite/sqlite.js";

// ``try: import duckdb / except ImportError: duckdb = None``
const duckdb = optional_import("duckdb");

/**
 * Minimal cursor-style wrapper over the callback-based ``duckdb`` client so
 * the pool body reads like its Python counterpart
 * (``conn.execute(sql, params).fetchone()``). Every statement is awaited to
 * completion through the loop pump before returning.
 */
class _DuckDBConnection {
  constructor(db) {
    this._db = db;
  }

  /**
   * @param {string} sql
   * @param {any[]} [params]
   * @returns {{fetchone: () => any[]|null, fetchall: () => any[][]}}
   */
  execute(sql, params = []) {
    return _DuckDBConnection._cursor(block_on(this._all(sql, params)));
  }

  /** Native-async counterpart of ``execute`` for the ``_*_async`` hooks. */
  async execute_async(sql, params = []) {
    return _DuckDBConnection._cursor(await this._all(sql, params));
  }

  _all(sql, params) {
    return new Promise((resolve, reject) => {
      this._db.all(sql, ...params, (err, res) => (err ? reject(err) : resolve(res ?? [])));
    });
  }

  static _cursor(rows) {
    const tuples = rows.map((r) => Object.values(r));
    return {
      fetchone: () => (tuples.length ? tuples[0] : null),
      fetchall: () => tuples,
    };
  }

  close() {
    block_on(new Promise((resolve, reject) => this._db.close((err) => (err ? reject(err) : resolve()))));
  }
}

/** DuckDB-backed pool storing entries in a local DuckDB database file. */
export class DuckDBPool extends _LAILA_IDENTIFIABLE_POOL {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_fields(this, {
      file_path: ["str | None", Field({ default: null })],
      transformations: [[TransformationSequence, "None"], Field({ default: transformation_base64 })],
    });
    define_private(this, {
      _conn: PrivateAttr({ default: null }),
    });
  }

  /** Initialise the DuckDB connection and create the entries table. */
  model_post_init(_context) {
    super.model_post_init(_context);
    if (duckdb === null) throw new ImportError("duckdb is required for DuckDBPool");

    if (this.file_path === null || this.file_path === undefined) {
      const { LAILA_DEFAULT_DIRECTORIES } = lazy("laila.macros.defaults");

      const pool_dir = path.join(LAILA_DEFAULT_DIRECTORIES.pools, this.uuid);
      fs.mkdirSync(pool_dir, { recursive: true });
      this.file_path = path.join(pool_dir, "pool.duckdb");
    } else {
      this.file_path = _expanduser(this.file_path);
      const parent = path.dirname(this.file_path);
      if (parent) fs.mkdirSync(parent, { recursive: true });
    }

    const db = block_on(
      new Promise((resolve, reject) => {
        const d = new duckdb.Database(this.file_path, (err) => (err ? reject(err) : resolve(d)));
      }),
    );
    this._conn = new _DuckDBConnection(db);
    const row = this._conn
      .execute("SELECT COUNT(*) FROM information_schema.columns " + "WHERE table_name = 'laila_pool_entries' AND column_name = 'pool_id'")
      .fetchone();
    if (row && Number(row[0]) > 0) this._conn.execute("DROP TABLE laila_pool_entries");
    this._conn.execute(
      `
            CREATE TABLE IF NOT EXISTS laila_pool_entries (
                key VARCHAR NOT NULL PRIMARY KEY,
                value TEXT NOT NULL
            )
            `,
    );
  }

  /** Return the active DuckDB connection. */
  _connection() {
    if (this._conn === null || this._conn === undefined) throw new RuntimeError("DuckDBPool is closed.");
    return this._conn;
  }

  /** Close the DuckDB connection. */
  close() {
    if (this._conn !== null && this._conn !== undefined) {
      this._conn.close();
      this._conn = null;
    }
  }

  /** Retrieve the JSON value for *key*, or ``null`` if absent. */
  _read(key) {
    const row = with_(this.atomic(), () => this._connection().execute("SELECT value FROM laila_pool_entries WHERE key = ?", [key]).fetchone());
    return row !== null ? json.loads(row[0]) : null;
  }

  /** Insert or update *entry* under *key*. */
  _write(key, entry) {
    let value = entry;
    if (isdict(value)) value = json.dumps(value);
    if (!is_str(value)) throw new PyTypeError("DuckDBPool expects a serialized JSON string.");

    with_(this.atomic(), () => {
      this._connection().execute(
        `
                INSERT INTO laila_pool_entries(key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                `,
        [key, value],
      );
    });
  }

  /** Delete the row for *key*. */
  _delete(key) {
    with_(this.atomic(), () => {
      this._connection().execute("DELETE FROM laila_pool_entries WHERE key = ?", [key]);
    });
  }

  /** Remove all entries from the pool. */
  _empty() {
    with_(this.atomic(), () => {
      this._connection().execute("DELETE FROM laila_pool_entries");
    });
  }

  /** Return ``true`` if *key* is present. */
  _exists(key) {
    return with_(this.atomic(), () => {
      const row = this._connection().execute("SELECT 1 FROM laila_pool_entries WHERE key = ?", [key]).fetchone();
      return row !== null;
    });
  }

  /** Check membership, delegates to ``_exists``. */
  __contains__(key) {
    return this._exists(key);
  }

  // Python's inherited ``_*_async`` defaults run the sync calls inline on
  // the loop; here the sync calls ``block_on`` the callback client, which a
  // coroutine (microtask) cannot do, so the hooks await the client directly.
  // The pool lock only guards the connection hand-out: the duckdb client
  // serialises statements itself.

  async _read_async(key) {
    const conn = with_(this.atomic(), () => this._connection());
    const row = (await conn.execute_async("SELECT value FROM laila_pool_entries WHERE key = ?", [key])).fetchone();
    return row !== null ? json.loads(row[0]) : null;
  }

  async _write_async(key, entry) {
    let value = entry;
    if (isdict(value)) value = json.dumps(value);
    if (!is_str(value)) throw new PyTypeError("DuckDBPool expects a serialized JSON string.");
    const conn = with_(this.atomic(), () => this._connection());
    await conn.execute_async(
      `
                INSERT INTO laila_pool_entries(key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                `,
      [key, value],
    );
  }

  async _delete_async(key) {
    const conn = with_(this.atomic(), () => this._connection());
    await conn.execute_async("DELETE FROM laila_pool_entries WHERE key = ?", [key]);
  }

  async _exists_async(key) {
    const conn = with_(this.atomic(), () => this._connection());
    const row = (await conn.execute_async("SELECT 1 FROM laila_pool_entries WHERE key = ?", [key])).fetchone();
    return row !== null;
  }

  /**
   * Return all keys in the pool.
   *
   * @param {boolean} [as_generator=false] If ``true``, return a lazy iterator
   *   instead of a list.
   * @returns {Iterable<string>} Pool keys.
   */
  _keys(as_generator = false) {
    if (!as_generator) {
      return with_(this.atomic(), () => {
        const rows = this._connection().execute("SELECT key FROM laila_pool_entries ORDER BY key").fetchall();
        return rows.map((row) => row[0]);
      });
    }

    const self = this;
    function* _gen() {
      const cm = self.atomic();
      cm.__enter__();
      try {
        const rows = self._connection().execute("SELECT key FROM laila_pool_entries ORDER BY key").fetchall();
        for (const row of rows) yield row[0];
      } finally {
        cm.__exit__(null, null, null);
      }
    }

    return _gen();
  }
}

register("laila.data.duckdb.duckdb", { DuckDBPool });
