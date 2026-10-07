/** PostgreSQL pool implementation with optional managed local server. */
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

import * as atexit from "../../_compat/atexit.js";
import { block_on } from "../../_compat/blocking.js";
import { with_ } from "../../_compat/contextlib.js";
import { FileNotFoundError, ImportError, RuntimeError, TypeError as PyTypeError, ValueError } from "../../_compat/errors.js";
import { lazy, register } from "../../_compat/lazy.js";
import { optional_import } from "../../_compat/optional.js";
import { ConfigDict, Field, PrivateAttr, define_fields, define_private } from "../../_compat/pydantic.js";
import * as json from "../../_compat/pyjson.js";
import { is_str, isdict } from "../../_compat/pytypes.js";
import * as subprocess from "../../_compat/subprocess.js";
import * as time from "../../_compat/time.js";
import { transformation_base64 } from "../../entry/index.js";
import { TransformationSequence } from "../../entry/compdata/transformation/base.js";
import { _LAILA_IDENTIFIABLE_POOL } from "../schema/base.js";

// ``try: import psycopg / except ImportError: psycopg = None`` -- ``pg`` on npm.
const psycopg = optional_import("pg");

function _read_pipe(stream) {
  if (!stream) return "";
  try {
    const chunk = stream.read();
    return chunk === null || chunk === undefined ? "" : chunk.toString("utf-8");
  } catch {
    return "";
  }
}

function _suppress(fn) {
  try {
    return fn();
  } catch {
    return undefined;
  }
}

/**
 * Cursor-flavoured wrapper over a connected ``pg.Client`` so the pool body
 * mirrors psycopg usage (``conn.cursor()`` / ``cur.execute`` / ``fetchone`` /
 * ``conn.commit``). ``%s`` placeholders are rewritten to ``$n``.
 *
 * ``pg`` runs each statement in its own implicit transaction, so the
 * explicit ``commit()`` psycopg needs (``autocommit=False``) is a no-op here.
 */
class _PgConnection {
  constructor(client) {
    this._client = client;
    this._closed = false;
  }

  static _placeholders(sql) {
    let i = 0;
    return sql.replace(/%s/g, () => `$${(i += 1)}`);
  }

  cursor() {
    const client = this._client;
    let rows = [];
    const cur = {
      execute(sql, params = []) {
        const res = block_on(client.query({ text: _PgConnection._placeholders(sql), values: [...params], rowMode: "array" }));
        rows = res.rows ?? [];
        return cur;
      },
      fetchone() {
        return rows.length ? rows.shift() : null;
      },
      fetchall() {
        const out = rows;
        rows = [];
        return out;
      },
      __enter__() {
        return cur;
      },
      __exit__() {
        return false;
      },
    };
    return cur;
  }

  /** Native-async statement for the pool's ``_async`` hooks. */
  async execute_async(sql, params = []) {
    const res = await this._client.query({ text: _PgConnection._placeholders(sql), values: [...params], rowMode: "array" });
    return res.rows ?? [];
  }

  commit() {}

  close() {
    if (this._closed) return;
    this._closed = true;
    block_on(this._client.end(), 2.0);
  }
}

/**
 * ``psycopg.connect(...)``: build and connect a ``pg.Client``.
 * @param {object} kwargs
 */
function _pg_connect(kwargs) {
  if (psycopg === null) throw new ImportError("psycopg is required for PostgresPool");
  const { dsn = null, host = null, port = null, dbname = null, user = null, password = null, connect_timeout = null } = kwargs;
  const cfg = {};
  if (dsn !== null) cfg.connectionString = dsn;
  if (host !== null) cfg.host = host;
  if (port !== null) cfg.port = port;
  if (dbname !== null) cfg.database = dbname;
  if (user !== null) cfg.user = user;
  if (password !== null) cfg.password = password;
  if (connect_timeout !== null) cfg.connectionTimeoutMillis = Math.round(connect_timeout * 1000);
  const client = new psycopg.Client(cfg);
  client.on("error", () => {});
  block_on(client.connect());
  return new _PgConnection(client);
}

/**
 * PostgreSQL-backed pool.
 *
 * Can connect to an existing PostgreSQL via ``dsn`` or explicit
 * ``host``/``port``/``dbname``/``user`` parameters, or automatically start
 * and manage a local ``postgres`` process.
 */
export class PostgresPool extends _LAILA_IDENTIFIABLE_POOL {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_fields(this, {
      dsn: ["str | None", Field({ default: null })],
      host: ["str | None", Field({ default: null })],
      port: ["int", Field({ default: 5432 })],
      dbname: ["str | None", Field({ default: null })],
      user: ["str | None", Field({ default: null })],
      password: ["str | None", Field({ default: null })],
      server_start_timeout_s: ["float", Field({ default: 5.0 })],
      transformations: [[TransformationSequence, "None"], Field({ default: transformation_base64 })],
    });
    define_private(this, {
      _conn: PrivateAttr({ default: null }),
      _postgres_proc: PrivateAttr({ default: null }),
      _owns_local_server: PrivateAttr({ default: false }),
      _postgres_dir: PrivateAttr({ default: null }),
      _socket_dir: PrivateAttr({ default: null }),
      _local_dbname: PrivateAttr({ default: "postgres" }),
      _local_user: PrivateAttr({ default: "laila" }),
    });
  }

  /** Connect to PostgreSQL and create the entries table. */
  model_post_init(_context) {
    super.model_post_init(_context);
    this._conn = this._connect();
    const cur = this._conn.cursor();
    cur.execute("SELECT column_name FROM information_schema.columns " + "WHERE table_name = 'laila_pool_entries' AND column_name = 'pool_id'");
    if (cur.fetchone() !== null) cur.execute("DROP TABLE laila_pool_entries");
    cur.execute(
      `
                CREATE TABLE IF NOT EXISTS laila_pool_entries (
                    key TEXT NOT NULL PRIMARY KEY,
                    value TEXT NOT NULL
                )
                `,
    );
    this._conn.commit();
    atexit.register(this.close.bind(this));
  }

  /** Establish a ``psycopg`` connection. */
  _connect() {
    if (psycopg === null) throw new ImportError("psycopg is required for PostgresPool");
    if (this.dsn !== null && this.dsn !== undefined) return _pg_connect({ dsn: this.dsn });
    if ((this.host ?? null) === null && (this.dbname ?? null) === null && (this.user ?? null) === null && (this.password ?? null) === null) {
      this._configure_local_server();
      this._ensure_local_server();
      return this._connect_local();
    }
    if ((this.host ?? null) === null || (this.dbname ?? null) === null || (this.user ?? null) === null) {
      throw new ValueError(
        "PostgresPool requires either dsn, explicit host/dbname/user parameters, or no connection parameters for a managed local server.",
      );
    }
    return _pg_connect({
      host: this.host,
      port: this.port,
      dbname: this.dbname,
      user: this.user,
      password: this.password,
    });
  }

  /** Set up data and socket directories for a managed local postgres. */
  _configure_local_server() {
    if ((this._postgres_dir ?? null) !== null && (this._socket_dir ?? null) !== null) return;
    const { LAILA_DEFAULT_DIRECTORIES } = lazy("laila.macros.defaults");

    const pool_dir = path.join(LAILA_DEFAULT_DIRECTORIES.pools, this.uuid);
    this._postgres_dir = path.join(pool_dir, "data");
    this._socket_dir = path.join(pool_dir, "socket");
    fs.mkdirSync(this._postgres_dir, { recursive: true });
    fs.mkdirSync(this._socket_dir, { recursive: true });
    this.port = 20000 + (parseInt(createHash("sha1").update(Buffer.from(this.pool_id, "utf-8")).digest("hex").slice(0, 8), 16) % 20000);
  }

  /**
   * Connect to the managed local postgres via UNIX socket.
   * @param {{connect_timeout?: number}} [opts]
   */
  _connect_local(opts = {}) {
    const { connect_timeout = 1 } = opts;
    if ((this._socket_dir ?? null) === null) throw new RuntimeError("Local Postgres socket directory is not configured.");
    return _pg_connect({
      host: this._socket_dir,
      port: this.port,
      dbname: this._local_dbname,
      user: this._local_user,
      connect_timeout,
    });
  }

  /** Start a local postgres if one is not already reachable. */
  _ensure_local_server() {
    if (this._local_server_available()) {
      this._owns_local_server = false;
      return;
    }
    this._initdb_if_needed();
    this._start_local_server();
  }

  /** Return ``true`` if the local postgres accepts connections. */
  _local_server_available() {
    try {
      const conn = this._connect_local({ connect_timeout: 1 });
      conn.close();
      return true;
    } catch {
      return false;
    }
  }

  /** Run ``initdb`` if the data directory is uninitialised. */
  _initdb_if_needed() {
    if ((this._postgres_dir ?? null) === null) throw new RuntimeError("Local Postgres data directory is not configured.");
    if (fs.existsSync(path.join(this._postgres_dir, "PG_VERSION"))) return;
    this._run_command(["initdb", "-D", this._postgres_dir, "-U", this._local_user, "-A", "trust"], {
      action: `initialize local Postgres data directory ${this._postgres_dir}`,
    });
  }

  /**
   * Execute a subprocess command, raising on failure.
   * @param {string[]} command
   * @param {{action: string}} opts
   */
  _run_command(command, opts) {
    const { action } = opts;
    try {
      subprocess.run(command, { check: true, capture_output: true, text: true });
    } catch (exc) {
      if (exc instanceof FileNotFoundError) {
        const err = new RuntimeError(`Unable to ${action}: command not found: ${command[0]}`);
        err.__cause__ = exc;
        throw err;
      }
      if (exc instanceof subprocess.CalledProcessError) {
        const err = new RuntimeError(`Unable to ${action}. Stdout: ${String(exc.stdout ?? "").trim()} Stderr: ${String(exc.stderr ?? "").trim()}`);
        err.__cause__ = exc;
        throw err;
      }
      throw exc;
    }
  }

  /** Launch a ``postgres`` subprocess and wait for readiness. */
  _start_local_server() {
    if ((this._postgres_dir ?? null) === null || (this._socket_dir ?? null) === null) {
      throw new RuntimeError("Local Postgres server is not fully configured.");
    }
    const cmd = ["postgres", "-D", this._postgres_dir, "-k", this._socket_dir, "-p", String(this.port), "-h", ""];
    this._postgres_proc = new subprocess.Popen(cmd, { stdout: subprocess.PIPE, stderr: subprocess.PIPE, text: true });
    this._owns_local_server = true;

    const deadline = time.time() + this.server_start_timeout_s;
    let last_err = null;
    while (time.time() < deadline) {
      if (this._postgres_proc.poll() !== null) {
        const exit_code = this._postgres_proc.poll();
        const stdout = _read_pipe(this._postgres_proc.stdout);
        const stderr = _read_pipe(this._postgres_proc.stderr);
        this.close();
        throw new RuntimeError(`postgres exited early (code=${exit_code}). Stdout: ${stdout.trim()} Stderr: ${stderr.trim()}`);
      }
      try {
        const conn = this._connect_local({ connect_timeout: 1 });
        conn.close();
        return;
      } catch (exc) {
        last_err = exc;
        time.sleep(0.1);
      }
    }

    const stdout = this._postgres_proc ? _read_pipe(this._postgres_proc.stdout) : "";
    const stderr = this._postgres_proc ? _read_pipe(this._postgres_proc.stderr) : "";
    this.close();
    throw new RuntimeError(
      `postgres failed to start for ${this.pool_id}. Last error: ${last_err && last_err.message !== undefined ? last_err.message : last_err}. Stdout: ${stdout.trim()} Stderr: ${stderr.trim()}`,
    );
  }

  /** Return the active psycopg connection. */
  _connection() {
    if (this._conn === null || this._conn === undefined) throw new RuntimeError("PostgresPool is closed.");
    return this._conn;
  }

  /** Close the connection and terminate any managed postgres process. */
  close() {
    if (this._conn !== null && this._conn !== undefined) {
      _suppress(() => this._conn.close());
      this._conn = null;
    }
    const proc = this._postgres_proc;
    this._postgres_proc = null;
    if (proc !== null && proc !== undefined && this._owns_local_server) {
      _suppress(() => proc.terminate());
      try {
        proc.wait(2.0);
      } catch {
        _suppress(() => proc.kill());
        _suppress(() => proc.wait(2.0));
      }
    }
    this._owns_local_server = false;
  }

  /** Retrieve the JSON value for *key*, or ``null`` if absent. */
  _read(key) {
    const row = with_(this.atomic(), () => {
      const cur = this._connection().cursor();
      cur.execute("SELECT value FROM laila_pool_entries WHERE key = %s", [key]);
      return cur.fetchone();
    });
    return row !== null ? json.loads(row[0]) : null;
  }

  /** Insert or update *entry* under *key*. */
  _write(key, entry) {
    let value = entry;
    if (isdict(value)) value = json.dumps(value);
    if (!is_str(value)) throw new PyTypeError("PostgresPool expects a serialized JSON string.");

    with_(this.atomic(), () => {
      const cur = this._connection().cursor();
      cur.execute(
        `
                    INSERT INTO laila_pool_entries(key, value)
                    VALUES (%s, %s)
                    ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value
                    `,
        [key, value],
      );
      this._connection().commit();
    });
  }

  /** Delete the row for *key*. */
  _delete(key) {
    with_(this.atomic(), () => {
      const cur = this._connection().cursor();
      cur.execute("DELETE FROM laila_pool_entries WHERE key = %s", [key]);
      this._connection().commit();
    });
  }

  /** Remove all entries from the pool. */
  _empty() {
    with_(this.atomic(), () => {
      const cur = this._connection().cursor();
      cur.execute("DELETE FROM laila_pool_entries");
      this._connection().commit();
    });
  }

  /** Return ``true`` if *key* is present in the table. */
  _exists(key) {
    return with_(this.atomic(), () => {
      const cur = this._connection().cursor();
      cur.execute("SELECT 1 FROM laila_pool_entries WHERE key = %s", [key]);
      return cur.fetchone() !== null;
    });
  }

  /** Check membership, delegates to ``_exists``. */
  __contains__(key) {
    return this._exists(key);
  }

  // -------- Async hooks: the ``pg`` client is natively async --------

  async _read_async(key) {
    const rows = await this._connection().execute_async("SELECT value FROM laila_pool_entries WHERE key = %s", [key]);
    return rows.length ? json.loads(rows[0][0]) : null;
  }

  async _write_async(key, entry) {
    let value = entry;
    if (isdict(value)) value = json.dumps(value);
    if (!is_str(value)) throw new PyTypeError("PostgresPool expects a serialized JSON string.");
    await this._connection().execute_async(
      `
                    INSERT INTO laila_pool_entries(key, value)
                    VALUES (%s, %s)
                    ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value
                    `,
      [key, value],
    );
  }

  async _delete_async(key) {
    await this._connection().execute_async("DELETE FROM laila_pool_entries WHERE key = %s", [key]);
  }

  async _exists_async(key) {
    const rows = await this._connection().execute_async("SELECT 1 FROM laila_pool_entries WHERE key = %s", [key]);
    return rows.length > 0;
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
        const cur = this._connection().cursor();
        cur.execute("SELECT key FROM laila_pool_entries ORDER BY key");
        return cur.fetchall().map((row) => row[0]);
      });
    }

    const self = this;
    function* _gen() {
      const cm = self.atomic();
      cm.__enter__();
      try {
        const cur = self._connection().cursor();
        cur.execute("SELECT key FROM laila_pool_entries ORDER BY key");
        for (const row of cur.fetchall()) yield row[0];
      } finally {
        cm.__exit__(null, null, null);
      }
    }

    return _gen();
  }
}

register("laila.data.postgres.postgres", { PostgresPool });
