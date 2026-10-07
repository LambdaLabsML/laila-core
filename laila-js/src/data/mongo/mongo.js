/** MongoDB-backed pool implementation with optional managed local server. */
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

import * as atexit from "../../_compat/atexit.js";
import { block_on } from "../../_compat/blocking.js";
import { with_ } from "../../_compat/contextlib.js";
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

// ``try: from pymongo import MongoClient / except ImportError: MongoClient = None``
const _mongodb = optional_import("mongodb");
const MongoClient = _mongodb ? _mongodb.MongoClient : null;
const ServerSelectionTimeoutError = _mongodb ? _mongodb.MongoServerSelectionError : Error;

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
 * ``MongoClient(uri_or_host, port=..., serverSelectionTimeoutMS=...)``: build
 * and connect a ``mongodb`` client synchronously.
 */
function _mongo_client(uri, opts) {
  const client = new MongoClient(uri, opts);
  block_on(client.connect());
  return client;
}

/**
 * MongoDB-backed pool.
 *
 * Can connect to an existing MongoDB via ``uri`` or ``host``/``port``, or
 * automatically start and manage a local ``mongod`` process.
 */
export class MongoPool extends _LAILA_IDENTIFIABLE_POOL {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_fields(this, {
      uri: ["str | None", Field({ default: null })],
      host: ["str | None", Field({ default: null })],
      port: ["int", Field({ default: 27017 })],
      dbname: ["str", Field({ default: "laila" })],
      server_start_timeout_s: ["float", Field({ default: 5.0 })],
      transformations: [[TransformationSequence, "None"], Field({ default: transformation_base64 })],
    });
    define_private(this, {
      _client: PrivateAttr({ default: null }),
      _mongo_proc: PrivateAttr({ default: null }),
      _owns_local_server: PrivateAttr({ default: false }),
      _mongo_dir: PrivateAttr({ default: null }),
    });
  }

  /** Connect to MongoDB and ensure the entries collection is indexed. */
  model_post_init(_context) {
    super.model_post_init(_context);
    this._client = this._connect();
    block_on(this._collection().createIndex({ key: 1 }, { unique: true }));
    atexit.register(this.close.bind(this));
  }

  /** Establish a ``MongoClient`` connection. */
  _connect() {
    if (MongoClient === null) throw new ImportError("pymongo is required for MongoPool");
    if (this.uri !== null && this.uri !== undefined) return _mongo_client(this.uri, { serverSelectionTimeoutMS: 1000 });
    if (this.host === null || this.host === undefined) {
      this._configure_local_server();
      this._ensure_local_server();
      return _mongo_client(this._local_uri, { serverSelectionTimeoutMS: 1000 });
    }
    return _mongo_client(`mongodb://${this.host}:${this.port}/`, { serverSelectionTimeoutMS: 1000 });
  }

  /** Set up directory and port for a managed local mongod. */
  _configure_local_server() {
    if (this._mongo_dir !== null && this._mongo_dir !== undefined) return;
    const { LAILA_DEFAULT_DIRECTORIES } = lazy("laila.macros.defaults");

    const pool_dir = path.join(LAILA_DEFAULT_DIRECTORIES.pools, this.uuid);
    this._mongo_dir = pool_dir;
    fs.mkdirSync(this._mongo_dir, { recursive: true });
    this.port = 30000 + (parseInt(createHash("sha1").update(Buffer.from(this.pool_id, "utf-8")).digest("hex").slice(0, 8), 16) % 20000);
    this.host = "127.0.0.1";
  }

  /** MongoDB connection URI for the managed local server. */
  get _local_uri() {
    if (this.host === null || this.host === undefined) throw new RuntimeError("Local Mongo host is not configured.");
    return `mongodb://${this.host}:${this.port}/`;
  }

  /** Start a local mongod if one is not already reachable. */
  _ensure_local_server() {
    if (this._local_server_available()) {
      this._owns_local_server = false;
      return;
    }
    for (let attempt = 0; attempt < 5; attempt += 1) {
      try {
        this._start_local_server();
        return;
      } catch (exc) {
        if (!(exc instanceof RuntimeError)) throw exc;
        if (!String(exc.message).includes("code=48")) throw exc;
        time.sleep(1);
        if (this._local_server_available()) {
          this._owns_local_server = false;
          return;
        }
        this.port += 1;
      }
    }
  }

  /** Return ``true`` if the local mongod responds to a ping. */
  _local_server_available() {
    if (MongoClient === null) return false;
    try {
      const client = _mongo_client(this._local_uri, { serverSelectionTimeoutMS: 500 });
      block_on(client.db("admin").command({ ping: 1 }));
      block_on(client.close());
      return true;
    } catch {
      return false;
    }
  }

  /** Launch a ``mongod`` subprocess and wait for readiness. */
  _start_local_server() {
    if (this._mongo_dir === null || this._mongo_dir === undefined || this.host === null || this.host === undefined) {
      throw new RuntimeError("Local Mongo server is not fully configured.");
    }
    const cmd = ["mongod", "--dbpath", this._mongo_dir, "--port", String(this.port), "--bind_ip", this.host, "--quiet"];
    this._mongo_proc = new subprocess.Popen(cmd, { stdout: subprocess.PIPE, stderr: subprocess.PIPE, text: true });
    this._owns_local_server = true;

    const deadline = time.time() + this.server_start_timeout_s;
    let last_err = null;
    while (time.time() < deadline) {
      if (this._mongo_proc.poll() !== null) {
        const exit_code = this._mongo_proc.poll();
        const stdout = _read_pipe(this._mongo_proc.stdout);
        const stderr = _read_pipe(this._mongo_proc.stderr);
        this.close();
        throw new RuntimeError(`mongod exited early (code=${exit_code}). Stdout: ${stdout.trim()} Stderr: ${stderr.trim()}`);
      }
      try {
        const client = _mongo_client(this._local_uri, { serverSelectionTimeoutMS: 500 });
        block_on(client.db("admin").command({ ping: 1 }));
        block_on(client.close());
        return;
      } catch (exc) {
        last_err = exc;
        time.sleep(0.1);
      }
    }

    const stdout = this._mongo_proc ? _read_pipe(this._mongo_proc.stdout) : "";
    const stderr = this._mongo_proc ? _read_pipe(this._mongo_proc.stderr) : "";
    this.close();
    throw new RuntimeError(
      `mongod failed to start for ${this.pool_id}. Last error: ${last_err && last_err.message !== undefined ? last_err.message : last_err}. Stdout: ${stdout.trim()} Stderr: ${stderr.trim()}`,
    );
  }

  /** Return the active MongoDB database handle. */
  _db() {
    if (this._client === null || this._client === undefined) throw new RuntimeError("MongoPool is closed.");
    return this._client.db(this.dbname);
  }

  /** Return the ``laila_pool_entries`` collection. */
  _collection() {
    return this._db().collection("laila_pool_entries");
  }

  /** Close the client and terminate any managed mongod process. */
  close() {
    if (this._client !== null && this._client !== undefined) {
      _suppress(() => block_on(this._client.close(), 2.0));
      this._client = null;
    }
    const proc = this._mongo_proc;
    this._mongo_proc = null;
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
    const doc = with_(this.atomic(), () => block_on(this._collection().findOne({ key }, { projection: { _id: 0, value: 1 } })));
    return doc !== null && doc !== undefined ? json.loads(doc.value) : null;
  }

  /** Upsert *entry* under *key*. */
  _write(key, entry) {
    let value = entry;
    if (isdict(value)) value = json.dumps(value);
    if (!is_str(value)) throw new PyTypeError("MongoPool expects a serialized JSON string.");

    with_(this.atomic(), () => {
      block_on(this._collection().updateOne({ key }, { $set: { value } }, { upsert: true }));
    });
  }

  /** Delete the document for *key*. */
  _delete(key) {
    with_(this.atomic(), () => {
      block_on(this._collection().deleteOne({ key }));
    });
  }

  /** Remove all documents from the entries collection. */
  _empty() {
    with_(this.atomic(), () => {
      block_on(this._collection().deleteMany({}));
    });
  }

  /** Return ``true`` if a document for *key* exists. */
  _exists(key) {
    return with_(this.atomic(), () => block_on(this._collection().countDocuments({ key }, { limit: 1 })) > 0);
  }

  /** Check membership, delegates to ``_exists``. */
  __contains__(key) {
    return this._exists(key);
  }

  // -------- Async hooks: the ``mongodb`` driver is natively async --------

  async _read_async(key) {
    const doc = await this._collection().findOne({ key }, { projection: { _id: 0, value: 1 } });
    return doc !== null && doc !== undefined ? json.loads(doc.value) : null;
  }

  async _write_async(key, entry) {
    let value = entry;
    if (isdict(value)) value = json.dumps(value);
    if (!is_str(value)) throw new PyTypeError("MongoPool expects a serialized JSON string.");
    await this._collection().updateOne({ key }, { $set: { value } }, { upsert: true });
  }

  async _delete_async(key) {
    await this._collection().deleteOne({ key });
  }

  async _exists_async(key) {
    return (await this._collection().countDocuments({ key }, { limit: 1 })) > 0;
  }

  /**
   * Return all keys in the collection.
   *
   * @param {boolean} [as_generator=false] If ``true``, return a lazy iterator
   *   instead of a list.
   * @returns {Iterable<string>} Pool keys.
   */
  _keys(as_generator = false) {
    const self = this;
    function* _iter_keys() {
      const docs = block_on(self._collection().find({}, { projection: { _id: 0, key: 1 } }).sort({ key: 1 }).toArray());
      for (const doc of docs) yield doc.key;
    }

    if (!as_generator) return with_(this.atomic(), () => [..._iter_keys()]);
    return _iter_keys();
  }
}

export { ServerSelectionTimeoutError };

register("laila.data.mongo.mongo", { MongoPool });
