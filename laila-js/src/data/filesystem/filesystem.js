/**
 * Loopback-mounted ext4 filesystem pool implementation.
 *
 * A ``FilesystemPool`` writes each entry as a single JSON file inside a
 * private, loopback-mounted ext4 image. The image is created on first use,
 * mounted via ``mount -o loop``, and not unmounted by laila (so subsequent
 * process runs can reuse it).
 *
 * Why an image-backed mount instead of a plain directory?
 *
 * - It bounds the pool's footprint to a fixed size (``_image_size_bytes``,
 *   64 MiB by default) so a runaway producer can't fill the host disk.
 * - It provides POSIX-level isolation -- the pool's contents share an
 *   independent inode table from the host filesystem and can be
 *   detached/relocated as a single ``.img`` file.
 *
 * The class is Linux-only (relies on ``mkfs.ext4`` and ``mount``) and needs
 * root privileges or appropriate capabilities to mount.
 */
import fs from "node:fs";
import path from "node:path";

import { suppress, with_ } from "../../_compat/contextlib.js";
import { FileNotFoundError, RuntimeError, TypeError as PyTypeError, ValueError } from "../../_compat/errors.js";
import { lazy, register } from "../../_compat/lazy.js";
import { ConfigDict, Field, PrivateAttr, define_fields, define_private } from "../../_compat/pydantic.js";
import * as json from "../../_compat/pyjson.js";
import { is_str, isdict } from "../../_compat/pytypes.js";
import * as subprocess from "../../_compat/subprocess.js";
import { quote, unquote } from "../../_compat/urlquote.js";
import { transformation_base64 } from "../../entry/index.js";
import { TransformationSequence } from "../../entry/compdata/transformation/index.js";
import { _LAILA_IDENTIFIABLE_POOL } from "../schema/base.js";

/** ``os.path.ismount`` */
function _ismount(p) {
  let st;
  try {
    st = fs.lstatSync(p);
  } catch {
    return false;
  }
  if (st.isSymbolicLink()) return false;
  const parent = path.join(p, "..");
  let pst;
  try {
    pst = fs.lstatSync(parent);
  } catch {
    return false;
  }
  if (st.dev !== pst.dev) return true;
  if (st.ino === pst.ino) return true;
  return false;
}

/**
 * Pool backed by JSON files on a loopback-mounted ext4 image.
 *
 * Each entry lives in a single ``<urlencoded-key>.json`` file inside the
 * pool's mount directory. Keys are URL-encoded so any character is safe for
 * use in a filename, and decoded on enumeration via ``_logical_key``.
 *
 * The image is created with ``_create_image_file`` (``truncate`` +
 * ``mkfs.ext4``) and mounted via ``_mount_image`` on first construction.
 * Subsequent constructions detect the mount via ``/proc/self/mountinfo``
 * and skip both steps.
 *
 * The constructor refuses any explicit ``image_dir`` / ``image_path`` /
 * ``mount_dir`` overrides because the pool computes those itself from
 * ``LAILA_DEFAULT_DIRECTORIES`` and its own UUID -- letting callers override
 * them would silently break the "one image file per pool, name derived from
 * pool UUID" invariant.
 *
 * Notes
 * -----
 * The default ``transformations`` is ``transformation_base64``, which gives
 * the JSON serialiser pure-ASCII payloads (avoiding encoding surprises
 * across mount platforms).
 */
export class FilesystemPool extends _LAILA_IDENTIFIABLE_POOL {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_fields(this, {
      transformations: [[TransformationSequence, "None"], Field({ default: transformation_base64 })],
    });
    define_private(this, {
      _pool_dir: PrivateAttr(),
      _mount_dir: PrivateAttr(),
      _image_path: PrivateAttr(),
      _image_size_bytes: PrivateAttr({ default: 64 * 1024 * 1024 }),
    });
  }

  /** Validate that reserved path fields are not overridden. */
  constructor(data = {}) {
    if (data !== null && typeof data === "object" && typeof data !== "symbol") {
      const has = (k) => (data instanceof Map ? data.has(k) : Object.prototype.hasOwnProperty.call(data, k));
      if (has("image_dir")) throw new ValueError("FilesystemPool storage path is fixed and cannot be overridden.");
      if (has("image_path")) throw new ValueError("FilesystemPool image_path is fixed and cannot be overridden.");
      if (has("mount_dir")) throw new ValueError("FilesystemPool mount_dir is fixed and cannot be overridden.");
    }
    super(data);
  }

  /** Root directory for this pool's artefacts. */
  get pool_dir() {
    return this._pool_dir;
  }

  /** Mount-point directory where entries are stored. */
  get mount_dir() {
    return this._mount_dir;
  }

  /** Path to the ext4 image file. */
  get image_path() {
    return this._image_path;
  }

  /** Create or mount the filesystem image. */
  model_post_init(_context) {
    super.model_post_init(_context);
    this._pool_dir = this._resolve_pool_dir();
    fs.mkdirSync(this.pool_dir, { recursive: true });
    this._image_path = this._resolve_image_path();
    this._mount_dir = this._resolve_mount_dir();
    fs.mkdirSync(this.mount_dir, { recursive: true });

    if (this._is_mounted(this.mount_dir)) return;

    if (fs.existsSync(this.image_path)) {
      this._mount_image();
      return;
    }

    this._create_image_file();
    this._mount_image();
  }

  /** No-op; the mount persists beyond pool lifetime. */
  close() {
    return null;
  }

  /** Derive the pool directory from default directories. */
  _resolve_pool_dir() {
    const { LAILA_DEFAULT_DIRECTORIES } = lazy("laila.macros.defaults");

    return path.join(LAILA_DEFAULT_DIRECTORIES.pools, this.uuid);
  }

  /** Resolve the ``.img`` or ``.iso`` image path for this pool. */
  _resolve_image_path() {
    const img_path = path.join(this.pool_dir, `${this.pool_id}.img`);
    const iso_path = path.join(this.pool_dir, `${this.pool_id}.iso`);

    if (fs.existsSync(img_path) && fs.existsSync(iso_path)) {
      throw new RuntimeError(`Ambiguous filesystem image for ${this.pool_id}: both ${img_path} and ${iso_path} exist.`);
    }
    if (fs.existsSync(img_path)) return img_path;
    if (fs.existsSync(iso_path)) return iso_path;
    return img_path;
  }

  /** Return the mount sub-directory path. */
  _resolve_mount_dir() {
    return path.join(this.pool_dir, "mnt");
  }

  /** Check whether *p* is currently a mount point. */
  _is_mounted(p) {
    if (_ismount(p)) return true;

    const normalized_path = fs.realpathSync(p);
    try {
      const text = fs.readFileSync("/proc/self/mountinfo", "utf-8");
      for (const line of text.split("\n")) {
        const parts = line.split(/\s+/).filter(Boolean);
        if (parts.length > 4) {
          let real;
          try {
            real = fs.realpathSync(parts[4]);
          } catch {
            real = parts[4];
          }
          if (real === normalized_path) return true;
        }
      }
    } catch {
      // OSError
    }

    return false;
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

  /** Allocate and format a new ext4 image file. */
  _create_image_file() {
    const fd = fs.openSync(this.image_path, "w");
    try {
      fs.ftruncateSync(fd, this._image_size_bytes);
    } finally {
      fs.closeSync(fd);
    }
    this._run_command(["mkfs.ext4", "-F", this.image_path], { action: `format filesystem image ${this.image_path}` });
  }

  /** Loop-mount the image file at ``mount_dir``. */
  _mount_image() {
    this._run_command(["mount", "-o", "loop", this.image_path, this.mount_dir], {
      action: `mount filesystem image ${this.image_path} at ${this.mount_dir}`,
    });
  }

  /** URL-encode *key* and append ``.json``. */
  _storage_key(key) {
    return `${quote(key, { safe: "" })}.json`;
  }

  /** Strip the ``.json`` suffix and URL-decode. */
  _logical_key(storage_key) {
    if (storage_key.endsWith(".json")) storage_key = storage_key.slice(0, -5);
    return unquote(storage_key);
  }

  /** Full filesystem path for the given entry key. */
  _entry_path(key) {
    return path.join(this.mount_dir, this._storage_key(key));
  }

  /** Read and parse the JSON file for *key*, or return ``null``. */
  _read(key) {
    const p = this._entry_path(key);
    if (!fs.existsSync(p)) return null;

    const raw = with_(this.atomic(), () => {
      // ``open(path, "a+")``: create-if-missing, then read from the start.
      const fd = fs.openSync(p, "a+");
      try {
        return fs.readFileSync(fd, "utf-8");
      } finally {
        fs.closeSync(fd);
      }
    });

    if (raw.trim() === "") return null;
    return json.loads(raw);
  }

  /** Write *entry* as a JSON file under *key*. */
  _write(key, entry) {
    let value = entry;
    if (isdict(value)) value = json.dumps(value);
    if (!is_str(value)) throw new PyTypeError("FilesystemPool expects a serialized JSON string.");

    const p = this._entry_path(key);
    with_(this.atomic(), () => {
      fs.writeFileSync(p, value, "utf-8");
    });
  }

  /** Remove the JSON file for *key*; no-op if absent. */
  _delete(key) {
    const p = this._entry_path(key);
    with_(this.atomic(), () => {
      with_(suppress(FileNotFoundError), () => {
        try {
          fs.rmSync(p);
        } catch (e) {
          if (e && e.code === "ENOENT") throw new FileNotFoundError(e.message);
          throw e;
        }
      });
    });
  }

  /** Remove all ``.json`` files from the mount directory. */
  _empty() {
    with_(this.atomic(), () => {
      for (const name of fs.readdirSync(this.mount_dir)) {
        if (!name.endsWith(".json")) continue;
        try {
          fs.rmSync(path.join(this.mount_dir, name));
        } catch (e) {
          if (!(e && e.code === "ENOENT")) throw e;
        }
      }
    });
  }

  /** Return ``true`` if a file for *key* exists on disk. */
  _exists(key) {
    return fs.existsSync(this._entry_path(key));
  }

  /** Check membership, delegates to ``_exists``. */
  __contains__(key) {
    return this._exists(key);
  }

  /**
   * Return all keys in the mount directory.
   *
   * @param {boolean} [as_generator=false] If ``true``, return a lazy iterator
   *   instead of a list.
   * @returns {Iterable<string>} Pool keys.
   */
  _keys(as_generator = false) {
    if (!as_generator) {
      return with_(this.atomic(), () => {
        const names = fs.readdirSync(this.mount_dir).filter((name) => name.endsWith(".json"));
        return names.map((name) => this._logical_key(name));
      });
    }

    const self = this;
    function* _gen() {
      const cm = self.atomic();
      cm.__enter__();
      try {
        for (const name of fs.readdirSync(self.mount_dir)) {
          if (name.endsWith(".json")) yield self._logical_key(name);
        }
      } finally {
        cm.__exit__(null, null, null);
      }
    }

    return _gen();
  }
}

register("laila.data.filesystem.filesystem", { FilesystemPool });
