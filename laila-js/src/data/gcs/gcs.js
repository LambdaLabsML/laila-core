/** Google Cloud Storage pool implementation. */
import { block_on } from "../../_compat/blocking.js";
import { ImportError, TypeError as PyTypeError } from "../../_compat/errors.js";
import { register } from "../../_compat/lazy.js";
import { optional_import } from "../../_compat/optional.js";
import { ConfigDict, Field, PrivateAttr, define_fields, define_private } from "../../_compat/pydantic.js";
import * as json from "../../_compat/pyjson.js";
import { is_str, isdict } from "../../_compat/pytypes.js";
import { quote, unquote } from "../../_compat/urlquote.js";
import { transformation_base64 } from "../../entry/index.js";
import { TransformationSequence } from "../../entry/compdata/transformation/base.js";
import { _LAILA_IDENTIFIABLE_POOL } from "../schema/base.js";

// ``try: from google.cloud import storage / except ImportError: storage = None``
const _gcs = optional_import("@google-cloud/storage");
const storage = _gcs ? { Client: _gcs.Storage } : null;
// Service-account JSON is passed straight to the client as ``credentials``.
const service_account = _gcs ? { Credentials: { from_service_account_info: (info) => info } } : null;

/** ``google.api_core.exceptions.NotFound``: the client surfaces it as an HTTP 404. */
export function NotFound(e) {
  return Boolean(e) && (e.code === 404 || e.code === "404");
}

/**
 * Google Cloud Storage-backed pool.
 *
 * Entries are stored as JSON objects in a GCS bucket.  Authentication
 * can use explicit service-account info or Application Default Credentials.
 */
export class GCSPool extends _LAILA_IDENTIFIABLE_POOL {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_fields(this, {
      service_account_info: ["dict[str, Any] | None", Field({ default: null })],
      project_id: ["str | None", Field({ default: null })],
      bucket_name: ["str"],
      transformations: [[TransformationSequence, "None"], Field({ default: transformation_base64 })],
    });
    define_private(this, {
      _client: PrivateAttr({ default: null }),
      _bucket: PrivateAttr({ default: null }),
    });
  }

  /** Return the GCS ``storage.Client``, creating it on first call. */
  _get_client() {
    if (this._client !== null && this._client !== undefined) return this._client;
    if (storage === null || service_account === null) throw new ImportError("google-cloud-storage is required for GCSPool");

    const kwargs = {};
    if (this.service_account_info !== null && this.service_account_info !== undefined) {
      const credentials = service_account.Credentials.from_service_account_info(this.service_account_info);
      kwargs.credentials = credentials;
      if (this.project_id === null || this.project_id === undefined) {
        this.project_id = this.service_account_info.project_id ?? null;
      }
    }

    if (this.project_id !== null && this.project_id !== undefined) kwargs.projectId = this.project_id;

    this._client = new storage.Client(kwargs);
    return this._client;
  }

  /** Return the GCS bucket handle. */
  _get_bucket() {
    if (this._bucket === null || this._bucket === undefined) {
      this._bucket = this._get_client().bucket(this.bucket_name);
    }
    return this._bucket;
  }

  /** URL-encode a logical key for GCS. */
  _object_key(key) {
    return quote(key, { safe: "" });
  }

  /** Decode a GCS object name back to the logical key. */
  _logical_key(object_key) {
    return unquote(object_key);
  }

  /** Release client references. */
  close() {
    this._client = null;
    this._bucket = null;
  }

  /** Retrieve the JSON value for *key*, or ``null`` if absent. */
  _read(key) {
    const blob = this._get_bucket().file(this._object_key(key));
    let raw;
    try {
      const [contents] = block_on(blob.download());
      raw = contents.toString("utf-8");
    } catch (e) {
      if (NotFound(e)) return null;
      throw e;
    }
    return json.loads(raw);
  }

  /** Store *entry* as a JSON blob under *key*. */
  _write(key, entry) {
    let value = entry;
    if (isdict(value)) value = json.dumps(value);
    if (!is_str(value)) throw new PyTypeError("GCSPool expects a serialized JSON string.");

    const blob = this._get_bucket().file(this._object_key(key));
    block_on(blob.save(value, { contentType: "application/json", resumable: false }));
  }

  /** Delete the blob for *key*; no-op if absent. */
  _delete(key) {
    const blob = this._get_bucket().file(this._object_key(key));
    try {
      block_on(blob.delete());
    } catch (e) {
      if (NotFound(e)) return;
      throw e;
    }
  }

  /** Remove all blobs from the bucket. */
  _empty() {
    const [blobs] = block_on(this._get_client().bucket(this.bucket_name).getFiles());
    for (const blob of blobs) {
      block_on(blob.delete());
    }
  }

  /** Return ``true`` if a blob for *key* exists. */
  _exists(key) {
    const blob = this._get_bucket().file(this._object_key(key));
    const [exists] = block_on(blob.exists());
    return Boolean(exists);
  }

  /** Check membership, delegates to ``_exists``. */
  __contains__(key) {
    return this._exists(key);
  }

  /**
   * Return all keys in the bucket.
   *
   * @param {boolean} [as_generator=false] If ``true``, return a lazy iterator
   *   instead of a list.
   * @returns {Iterable<string>} Pool keys.
   */
  _keys(as_generator = false) {
    const self = this;

    function* _iter_keys() {
      const [blobs] = block_on(self._get_client().bucket(self.bucket_name).getFiles());
      for (const blob of blobs) yield self._logical_key(blob.name);
    }

    if (!as_generator) return [..._iter_keys()];
    return _iter_keys();
  }
}

register("laila.data.gcs.gcs", { GCSPool });
