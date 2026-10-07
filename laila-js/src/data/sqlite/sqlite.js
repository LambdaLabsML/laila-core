/** SQLite pool implementation. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

import { with_ } from "../../_compat/contextlib.js";
import { RuntimeError, TypeError as PyTypeError } from "../../_compat/errors.js";
import { lazy, register } from "../../_compat/lazy.js";
import { ConfigDict, Field, PrivateAttr, define_fields, define_private } from "../../_compat/pydantic.js";
import * as json from "../../_compat/pyjson.js";
import { is_str, isdict } from "../../_compat/pytypes.js";
import { transformation_base64 } from "../../entry/index.js";
import { TransformationSequence } from "../../entry/compdata/transformation/base.js";
import { _LAILA_IDENTIFIABLE_POOL } from "../schema/base.js";

/** ``os.path.expanduser`` */
export function _expanduser(p) {
  if (p === "~") return os.homedir();
  if (p.startsWith("~/")) return path.join(os.homedir(), p.slice(2));
  return p;
}

/** SQLite-backed pool storing entries in a local database file. */
export class SQLitePool extends _LAILA_IDENTIFIABLE_POOL {
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

  /** Open the SQLite database and create the entries table. */
  model_post_init(_context) {
    super.model_post_init(_context);
    if (this.file_path === null || this.file_path === undefined) {
      const { LAILA_DEFAULT_DIRECTORIES } = lazy("laila.macros.defaults");

      const pool_dir = path.join(LAILA_DEFAULT_DIRECTORIES.pools, this.uuid);
      fs.mkdirSync(pool_dir, { recursive: true });
      this.file_path = path.join(pool_dir, "pool.laila_sqlitedb");
    } else {
      this.file_path = _expanduser(this.file_path);
      const parent = path.dirname(this.file_path);
      if (parent) fs.mkdirSync(parent, { recursive: true });
    }

    this._conn = new DatabaseSync(this.file_path);
    const row = this._conn.prepare("SELECT COUNT(*) AS n FROM pragma_table_info('laila_pool_entries') WHERE name='pool_id'").get();
    if (row && Number(row.n) > 0) this._conn.exec("DROP TABLE laila_pool_entries");
    this._conn.exec(
      `
            CREATE TABLE IF NOT EXISTS laila_pool_entries (
                key TEXT NOT NULL PRIMARY KEY,
                value TEXT NOT NULL
            )
            `,
    );
  }

  /** Return the active SQLite connection. */
  _connection() {
    if (this._conn === null || this._conn === undefined) throw new RuntimeError("SQLitePool is closed.");
    return this._conn;
  }

  /** Close the SQLite connection. */
  close() {
    if (this._conn !== null && this._conn !== undefined) {
      this._conn.close();
      this._conn = null;
    }
  }

  /** Retrieve the JSON value for *key*, or ``null`` if absent. */
  _read(key) {
    const row = with_(this.atomic(), () => this._connection().prepare("SELECT value FROM laila_pool_entries WHERE key = ?").get(key));
    return row !== undefined && row !== null ? json.loads(row.value) : null;
  }

  /** Insert or update *entry* under *key*. */
  _write(key, entry) {
    let value = entry;
    if (isdict(value)) value = json.dumps(value);
    if (!is_str(value)) throw new PyTypeError("SQLitePool expects a serialized JSON string.");

    with_(this.atomic(), () => {
      this._connection()
        .prepare(
          `
                INSERT INTO laila_pool_entries(key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                `,
        )
        .run(key, value);
    });
  }

  /** Delete the row for *key*. */
  _delete(key) {
    with_(this.atomic(), () => {
      this._connection().prepare("DELETE FROM laila_pool_entries WHERE key = ?").run(key);
    });
  }

  /** Remove all entries from the pool. */
  _empty() {
    with_(this.atomic(), () => {
      this._connection().exec("DELETE FROM laila_pool_entries");
    });
  }

  /** Return ``true`` if *key* is present in the table. */
  _exists(key) {
    return with_(this.atomic(), () => {
      const row = this._connection().prepare("SELECT 1 FROM laila_pool_entries WHERE key = ?").get(key);
      return row !== undefined && row !== null;
    });
  }

  /** Check membership, delegates to ``_exists``. */
  __contains__(key) {
    return this._exists(key);
  }

  /**
   * Return all keys in the table.
   *
   * @param {boolean} [as_generator=false] If ``true``, return a lazy iterator
   *   instead of a list.
   * @returns {Iterable<string>} Pool keys.
   */
  _keys(as_generator = false) {
    if (!as_generator) {
      return with_(this.atomic(), () => {
        const rows = this._connection().prepare("SELECT key FROM laila_pool_entries ORDER BY key").all();
        return rows.map((row) => row.key);
      });
    }

    const self = this;
    function* _gen() {
      const cm = self.atomic();
      cm.__enter__();
      try {
        const rows = self._connection().prepare("SELECT key FROM laila_pool_entries ORDER BY key").all();
        for (const row of rows) yield row.key;
      } finally {
        cm.__exit__(null, null, null);
      }
    }

    return _gen();
  }
}

register("laila.data.sqlite.sqlite", { SQLitePool });
