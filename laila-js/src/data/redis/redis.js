/** Redis pool implementation with a managed private redis-server. */
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

import * as atexit from "../../_compat/atexit.js";
import { block_on } from "../../_compat/blocking.js";
import { ImportError, RuntimeError, TypeError as PyTypeError } from "../../_compat/errors.js";
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

// ``import redis`` -- node-redis (``redis`` on npm) is the client library.
const redis = optional_import("redis");

/** Drain whatever a child's pipe has buffered (``proc.stdout.read()``). */
function _read_pipe(stream) {
  if (!stream) return "";
  try {
    const chunk = stream.read();
    return chunk === null || chunk === undefined ? "" : chunk.toString("utf-8");
  } catch {
    return "";
  }
}

/** ``with suppress(Exception): ...`` */
function _suppress(fn) {
  try {
    return fn();
  } catch {
    return undefined;
  }
}

/**
 * Build and connect a node-redis client (``redis.Redis(...)``).
 *
 * @param {{unix_socket_path: string, password?: string|null, socket_connect_timeout?: number|null}} opts
 */
function _connect_client(opts) {
  if (redis === null) throw new ImportError("redis is required for RedisPool");
  const { unix_socket_path, password = null, socket_connect_timeout = null } = opts;
  const socket = { path: unix_socket_path, reconnectStrategy: false };
  if (socket_connect_timeout !== null) socket.connectTimeout = Math.max(1, Math.round(socket_connect_timeout * 1000));
  const client = redis.createClient({ socket, password: password ?? undefined });
  client.on("error", () => {});
  block_on(client.connect());
  return client;
}

/** Best-effort teardown used by ``close`` and by the GC finalizer. */
function _teardown(box) {
  const proc = box.proc;
  box.proc = null;

  if (proc === null || proc === undefined) return;

  if (box.client !== null && box.client !== undefined) {
    // ``client.shutdown(save=True)``
    _suppress(() => block_on(box.client.sendCommand(["SHUTDOWN", "SAVE"]), 1.5));
    _suppress(() => block_on(box.client.disconnect(), 0.5));
  }

  _suppress(() => proc.terminate());

  try {
    proc.wait(1.5);
  } catch {
    _suppress(() => proc.kill());
    _suppress(() => proc.wait(1.5));
  }

  _suppress(() => fs.rmSync(box.socket_path));
}

// ``__del__``: best-effort cleanup when the pool is garbage-collected.
const _finalizers = new FinalizationRegistry((box) => _suppress(() => _teardown(box)));

/**
 * Redis-backed Pool using a private redis-server over a UNIX socket.
 *
 * Persistence:
 *   - Data directory: ``<LAILA_DEFAULT_DIRECTORIES["pools"]>/<pool.uuid>/``
 *   - Dump file (private): ``pool.rdb``
 *   - If ``pool.rdb`` exists, Redis loads it automatically.
 *   - Dumps are never deleted.
 *
 * Notes:
 *   - No TCP ports
 *   - No orphan-process handling
 *   - Values stored are serialized JSON strings
 */
export class RedisPool extends _LAILA_IDENTIFIABLE_POOL {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_fields(this, {
      // Redis key namespaces
      key_prefix: ["str", Field({ default: "pool" })],
      lock_prefix: ["str", Field({ default: "pool_lock" })],

      // Optional auth
      redis_password: ["str | None", Field({ default: null })],

      // Behavior
      server_start_timeout_s: ["float", Field({ default: 3.0 })],
      lock_timeout: ["int", Field({ default: 30 })],

      redis_dir: ["str | None", Field({ default: null })],

      transformations: [[TransformationSequence, "None"], Field({ default: transformation_base64 })],
    });
    define_private(this, {
      _client: PrivateAttr({ default: null }),
      _redis_proc: PrivateAttr({ default: null }),
      _db_dump_name: PrivateAttr({ default: "laila_pool.rdb" }),
      _redis_socket_name: PrivateAttr({ default: "laila_redis.sock" }),
      // Shared teardown state for ``close`` / ``__del__`` (GC finalizer).
      _box: PrivateAttr({ default_factory: () => ({ proc: null, client: null, socket_path: null }) }),
    });
  }

  // ---------------- lifecycle ----------------
  /**
   * Always:
   *   1) establish redis_dir from pool_id
   *   2) start redis-server (unix socket only)
   *   3) connect client
   */
  model_post_init(_context) {
    super.model_post_init(_context);
    // Python: ``import redis`` at module top makes ``RedisPool`` unimportable
    // without the client library; here the class loads and construction fails.
    if (redis === null) throw new ImportError("redis is required for RedisPool");
    // Python: ``import redis`` at module top makes ``RedisPool`` unimportable
    // without the client library; here the class loads and construction fails.
    if (redis === null) throw new ImportError("redis is required for RedisPool");
    const { LAILA_DEFAULT_DIRECTORIES } = lazy("laila.macros.defaults");

    const pool_dir = path.join(LAILA_DEFAULT_DIRECTORIES.pools, this.uuid);
    this.redis_dir = pool_dir;
    fs.mkdirSync(this.redis_dir, { recursive: true });
    this._box.socket_path = this._redis_socket_path;

    // Remove stale socket file (do NOT handle orphan processes yet)
    _suppress(() => fs.rmSync(this._redis_socket_path));

    this._start_redis_server();

    this._client = _connect_client({
      unix_socket_path: this._redis_socket_path,
      password: this.redis_password,
    });
    this._box.client = this._client;

    _finalizers.register(this, this._box);
    atexit.register(this.close.bind(this));
  }

  /** Best-effort cleanup on garbage collection. */
  __del__() {
    _suppress(() => this.close());
  }

  /**
   * Clean shutdown.
   * Ask Redis to save and exit; never delete dumps.
   */
  close() {
    const proc = this._redis_proc;
    this._redis_proc = null;

    if (proc === null || proc === undefined) return;

    this._box.proc = proc;
    this._box.client = this._client;
    _teardown(this._box);
  }

  // ---------------- redis-server ----------------
  /** Launch a ``redis-server`` subprocess on a UNIX socket. */
  _start_redis_server() {
    if (!this.redis_dir) throw new RuntimeError("Redis server not fully configured (missing redis_dir)");

    const cmd = [
      "redis-server",
      "--port",
      "0", // disable TCP
      "--unixsocket",
      this._redis_socket_path,
      "--unixsocketperm",
      "700",
      "--dir",
      this.redis_dir,
      "--dbfilename",
      this._db_dump_name,
      "--save",
      "900 1",
      "--save",
      "300 10",
      "--save",
      "60 10000",
      "--appendonly",
      "no",
      "--protected-mode",
      "yes",
    ];

    if (this.redis_password) cmd.push("--requirepass", this.redis_password);

    this._redis_proc = new subprocess.Popen(cmd, { stdout: subprocess.PIPE, stderr: subprocess.PIPE, text: true });
    this._box.proc = this._redis_proc;

    const deadline = time.time() + this.server_start_timeout_s;
    let last_err = null;

    while (time.time() < deadline) {
      if (this._redis_proc.poll() !== null) {
        const exit_code = this._redis_proc.poll();
        const stdout = _read_pipe(this._redis_proc.stdout);
        const stderr = _read_pipe(this._redis_proc.stderr);
        this.close();
        throw new RuntimeError(`redis-server exited early (code=${exit_code}). ` + `Stdout: ${stdout.trim()} Stderr: ${stderr.trim()}`);
      }

      try {
        const r = _connect_client({
          unix_socket_path: this._redis_socket_path,
          password: this.redis_password,
          socket_connect_timeout: 0.25,
        });
        try {
          block_on(r.ping());
        } finally {
          _suppress(() => block_on(r.disconnect(), 0.5));
        }
        return;
      } catch (e) {
        last_err = e;
        time.sleep(0.05);
      }
    }

    const stdout = this._redis_proc ? _read_pipe(this._redis_proc.stdout) : "";
    const stderr = this._redis_proc ? _read_pipe(this._redis_proc.stderr) : "";
    this.close();
    throw new RuntimeError(
      `redis-server failed to start on unix socket ${this._redis_socket_path}. ` +
        `Last error: ${last_err && last_err.message !== undefined ? last_err.message : last_err}. Stdout: ${stdout.trim()} Stderr: ${stderr.trim()}`,
    );
  }

  // ---------------- private helpers ----------------
  /** Deterministic UNIX socket path derived from pool UUID. */
  get _redis_socket_path() {
    const digest = createHash("sha1").update(Buffer.from(this.uuid, "utf-8")).digest("hex").slice(0, 16);
    return `/tmp/laila_redis_${digest}.sock`;
  }

  // ---------------- key helpers ----------------
  /** Redis hash key used to store all pool entries. */
  get redis_hash_key() {
    return this.key_prefix;
  }

  /** Redis key used for distributed locking. */
  get redis_lock_key() {
    return this.lock_prefix;
  }

  _require_client() {
    if (this._client === null || this._client === undefined) throw new RuntimeError("Redis client not initialized.");
    return this._client;
  }

  /** Retrieve the JSON value for *key*, or ``null`` if absent. */
  _read(key) {
    const client = this._require_client();
    const value = block_on(client.hGet(this.redis_hash_key, key));

    return value !== null && value !== undefined ? json.loads(value) : null;
  }

  /** Store *entry* as a JSON string in the Redis hash. */
  _write(key, entry) {
    let value = entry;
    if (isdict(value)) value = json.dumps(value);
    const client = this._require_client();
    if (!is_str(value)) throw new PyTypeError("RedisPool expects a serialized JSON string.");
    block_on(client.hSet(this.redis_hash_key, key, value));
  }

  /** Delete *key* from the Redis hash. */
  _delete(key) {
    const client = this._require_client();
    block_on(client.hDel(this.redis_hash_key, key));
  }

  /** Remove all entries from the pool (deletes the Redis hash). */
  _empty() {
    const client = this._require_client();
    block_on(client.del(this.redis_hash_key));
  }

  /** Return ``true`` if *key* exists in the Redis hash. */
  _exists(key) {
    const client = this._require_client();
    return Boolean(block_on(client.hExists(this.redis_hash_key, key)));
  }

  /** Check membership, delegates to ``_exists``. */
  __contains__(key) {
    return this._exists(key);
  }

  // -------- Async hooks: the node-redis client is natively async --------

  async _read_async(key) {
    const client = this._require_client();
    const value = await client.hGet(this.redis_hash_key, key);
    return value !== null && value !== undefined ? json.loads(value) : null;
  }

  async _write_async(key, entry) {
    let value = entry;
    if (isdict(value)) value = json.dumps(value);
    const client = this._require_client();
    if (!is_str(value)) throw new PyTypeError("RedisPool expects a serialized JSON string.");
    await client.hSet(this.redis_hash_key, key, value);
  }

  async _delete_async(key) {
    const client = this._require_client();
    await client.hDel(this.redis_hash_key, key);
  }

  async _exists_async(key) {
    const client = this._require_client();
    return Boolean(await client.hExists(this.redis_hash_key, key));
  }

  /**
   * Return all keys in the Redis hash.
   *
   * @param {boolean} [as_generator=false] If ``true``, return a lazy iterator
   *   using ``HSCAN``.
   * @returns {Iterable<string>} Pool keys.
   */
  _keys(as_generator = false) {
    const client = this._require_client();

    if (!as_generator) return [...block_on(client.hKeys(this.redis_hash_key))];

    const self = this;
    function* _gen() {
      let cursor = 0;
      while (true) {
        const res = block_on(client.hScan(self.redis_hash_key, cursor));
        cursor = Number(res.cursor);
        const batch = res.tuples ?? res.entries ?? [];
        for (const item of batch) yield item.field;
        if (cursor === 0) break;
      }
    }

    return _gen();
  }
}

register("laila.data.redis.redis", { RedisPool });
