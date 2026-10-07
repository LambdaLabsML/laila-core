/**
 * HDF5 file pool implementation.
 *
 * Backed by ``h5wasm`` (libhdf5 compiled to WebAssembly, running directly on
 * the host filesystem under Node). Datasets are written exactly as
 * ``h5py.string_dtype(encoding="utf-8")`` scalars (variable-length UTF-8
 * strings), so a pool file is interchangeable between the Python and the
 * JavaScript implementation.
 *
 * ``h5wasm`` exposes no ``H5Ldelete``: removing a dataset (``_delete``, or
 * the delete-then-recreate an overwrite needs) is implemented by rewriting
 * the file without the dropped links. That keeps the on-disk layout identical
 * to what ``h5py`` produces at the cost of O(entries) per delete / overwrite.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { block_on } from "../../_compat/blocking.js";
import { with_ } from "../../_compat/contextlib.js";
import { ImportError, RuntimeError, TypeError as PyTypeError } from "../../_compat/errors.js";
import { lazy, register } from "../../_compat/lazy.js";
import { optional_import } from "../../_compat/optional.js";
import { ConfigDict, Field, PrivateAttr, define_fields, define_private } from "../../_compat/pydantic.js";
import * as json from "../../_compat/pyjson.js";
import { is_bytes, is_str, isdict } from "../../_compat/pytypes.js";
import { quote, unquote } from "../../_compat/urlquote.js";
import { transformation_base64 } from "../../entry/index.js";
import { TransformationSequence } from "../../entry/compdata/transformation/base.js";
import { _LAILA_IDENTIFIABLE_POOL } from "../schema/base.js";

// ``import h5py`` -- h5wasm's Node build (ESM-only ``h5wasm/node`` entry).
const h5py = optional_import("h5wasm", { file: "dist/node/hdf5_hl.js" });
let _h5_ready = false;

function _ensure_ready() {
  if (h5py === null) throw new ImportError("h5py is required for HDF5Pool");
  if (!_h5_ready) {
    block_on(h5py.ready);
    _h5_ready = true;
  }
}

/** ``os.path.expanduser`` */
function _expanduser(p) {
  if (p === "~") return os.homedir();
  if (p.startsWith("~/")) return path.join(os.homedir(), p.slice(2));
  return p;
}

/**
 * ``h5py.File(path, "a")``: read/write, create if missing.
 *
 * h5wasm's ``"a"`` only opens an existing file, so a missing file is created
 * with ``"w"`` first.
 */
function _open_file(file_path) {
  _ensure_ready();
  if (!fs.existsSync(file_path)) {
    const f = new h5py.File(file_path, "w");
    f.close();
  }
  return new h5py.File(file_path, "a");
}

/** ``h5py.string_dtype(encoding="utf-8")`` -- h5wasm dtype code for vlen UTF-8. */
const _STRING_DTYPE = "S";

/**
 * HDF5 file-backed pool.
 *
 * Each entry is stored as a UTF-8 string dataset inside a single HDF5 file
 * that remains open for the lifetime of the pool.
 */
export class HDF5Pool extends _LAILA_IDENTIFIABLE_POOL {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_fields(this, {
      file_path: ["str | None", Field({ default: null })],
      transformations: [[TransformationSequence, "None"], Field({ default: transformation_base64 })],
    });
    define_private(this, {
      _file: PrivateAttr({ default: null }),
    });
  }

  /** Return the active policy's memory global ID. */
  _resolve_memory_global_id() {
    const { active_policy } = lazy("laila");

    return active_policy.central.memory.global_id;
  }

  /** Open (or create) the HDF5 file. */
  model_post_init(_context) {
    super.model_post_init(_context);
    if (this.file_path === null || this.file_path === undefined) {
      const { LAILA_DEFAULT_DIRECTORIES } = lazy("laila.macros.defaults");

      const pool_dir = path.join(LAILA_DEFAULT_DIRECTORIES.pools, this.uuid);
      fs.mkdirSync(pool_dir, { recursive: true });
      this.file_path = path.join(pool_dir, "pool.h5py");
    } else {
      fs.mkdirSync(path.dirname(_expanduser(this.file_path)), { recursive: true });
      this.file_path = _expanduser(this.file_path);
    }

    // Open file and keep it open for the lifetime of the pool.
    this._file = _open_file(this.file_path);
  }

  /** Return the root HDF5 group. */
  _root() {
    if (this._file === null || this._file === undefined) throw new RuntimeError("HDF5Pool is closed.");
    return this._file;
  }

  // ---------------- internal helpers ----------------
  /** URL-encode *key* to avoid ``/`` collisions in HDF5 paths. */
  _storage_key(key) {
    return quote(key, { safe: "" });
  }

  /** Decode a storage key back to the logical key. */
  _logical_key(key) {
    return unquote(key);
  }

  /** ``skey in root`` */
  _has(skey) {
    return this._root().keys().includes(skey);
  }

  /** Read the raw UTF-8 string for *key*, or ``null``. */
  _read_raw(key) {
    const skey = this._storage_key(key);
    const root = this._root();
    if (!root.keys().includes(skey)) return null;
    const raw = root.get(skey).value;
    if (is_bytes(raw) || Buffer.isBuffer(raw)) return Buffer.from(raw).toString("utf-8");
    return String(raw);
  }

  /**
   * ``del root[skey]`` for every name in *skeys*: rewrite the file without
   * those links (h5wasm has no link deletion). Datasets are copied by value
   * so the result is byte-for-byte what h5py would hold after the deletes.
   */
  _drop_datasets(skeys) {
    const drop = new Set(skeys);
    const root = this._root();
    const keep = root.keys().filter((k) => !drop.has(k));
    if (keep.length === root.keys().length) return;
    const values = keep.map((k) => [k, root.get(k).value]);
    root.close();
    const tmp_path = `${this.file_path}.rebuild-${process.pid}`;
    const out = new h5py.File(tmp_path, "w");
    for (const [k, v] of values) out.create_dataset({ name: k, data: v, dtype: _STRING_DTYPE });
    out.close();
    fs.renameSync(tmp_path, this.file_path);
    this._file = new h5py.File(this.file_path, "a");
  }

  /** Close the HDF5 file handle. */
  close() {
    if (this._file !== null && this._file !== undefined) {
      this._file.close();
      this._file = null;
    }
  }

  // ---------------- mapping API ----------------
  /** Retrieve the JSON value for *key*, or ``null`` if absent. */
  _read(key) {
    const raw = with_(this.atomic(), () => this._read_raw(key));
    return raw !== null ? json.loads(raw) : null;
  }

  /** Store *entry* as a UTF-8 HDF5 dataset under *key*. */
  _write(key, entry) {
    let value = entry;
    if (isdict(value)) value = json.dumps(value);
    if (!is_str(value)) throw new PyTypeError("HDF5Pool expects a serialized JSON string.");

    const skey = this._storage_key(key);
    with_(this.atomic(), () => {
      const root = this._root();
      if (root.keys().includes(skey)) this._drop_datasets([skey]);
      this._root().create_dataset({ name: skey, data: value, dtype: _STRING_DTYPE });
      this._root().flush?.();
    });
  }

  /** Delete the dataset for *key* if it exists. */
  _delete(key) {
    const skey = this._storage_key(key);
    with_(this.atomic(), () => {
      const root = this._root();
      if (root.keys().includes(skey)) this._drop_datasets([skey]);
    });
  }

  /** Remove all entries from the pool. */
  _empty() {
    if (this._file === null || this._file === undefined) throw new RuntimeError("HDF5Pool is closed.");
    with_(this.atomic(), () => {
      this._drop_datasets([...this._file.keys()]);
    });
  }

  /** Return ``true`` if a dataset for *key* exists. */
  _exists(key) {
    const skey = this._storage_key(key);
    return with_(this.atomic(), () => this._root().keys().includes(skey));
  }

  /** Check membership, delegates to ``_exists``. */
  __contains__(key) {
    return this._exists(key);
  }

  /**
   * Return all keys in the HDF5 file.
   *
   * @param {boolean} [as_generator=false] If ``true``, return a lazy iterator
   *   instead of a list.
   * @returns {Iterable<string>} Pool keys.
   */
  _keys(as_generator = false) {
    if (!as_generator) {
      return with_(this.atomic(), () => {
        const skeys = [...this._root().keys()];
        return skeys.map((k) => this._logical_key(k));
      });
    }

    const self = this;
    function* _gen() {
      const cm = self.atomic();
      cm.__enter__();
      try {
        for (const k of self._root().keys()) yield self._logical_key(k);
      } finally {
        cm.__exit__(null, null, null);
      }
    }

    return _gen();
  }
}

register("laila.data.hdf5.hdf5", { HDF5Pool });
