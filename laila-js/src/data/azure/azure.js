/** Azure Blob Storage pool implementation. */
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

// ``try: from azure.storage.blob import BlobServiceClient / except ImportError``
const _azure_blob = optional_import("@azure/storage-blob");
const BlobServiceClient = _azure_blob ? _azure_blob.BlobServiceClient : null;

/** ``azure.core.exceptions.ResourceNotFoundError`` (HTTP 404). */
export function ResourceNotFoundError(e) {
  return Boolean(e) && (e.statusCode === 404 || (e.details && e.details.errorCode === "BlobNotFound"));
}

/** ``azure.core.exceptions.ResourceExistsError`` (HTTP 409). */
export function ResourceExistsError(e) {
  return Boolean(e) && (e.statusCode === 409 || (e.details && e.details.errorCode === "ContainerAlreadyExists"));
}

/** Collect an async iterable into an array, synchronously. */
function _collect(async_iterable) {
  return block_on(
    (async () => {
      const out = [];
      for await (const item of async_iterable) out.push(item);
      return out;
    })(),
  );
}

/**
 * Azure Blob Storage-backed pool.
 *
 * Each entry is stored as a JSON blob in the configured container.
 * Keys are URL-encoded to avoid path-separator issues.
 */
export class AzurePool extends _LAILA_IDENTIFIABLE_POOL {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_fields(this, {
      connection_string: ["str"],
      container_name: ["str"],
      transformations: [[TransformationSequence, "None"], Field({ default: transformation_base64 })],
    });
    define_private(this, {
      _client: PrivateAttr({ default: null }),
      _container: PrivateAttr({ default: null }),
    });
  }

  /** Return the ``BlobServiceClient``, creating it on first call. */
  _get_client() {
    if (this._client !== null && this._client !== undefined) return this._client;
    if (BlobServiceClient === null) throw new ImportError("azure-storage-blob is required for AzurePool");
    this._client = BlobServiceClient.fromConnectionString(this.connection_string);
    return this._client;
  }

  /** Return the container client, creating the container if needed. */
  _get_container() {
    if (this._container === null || this._container === undefined) {
      this._container = this._get_client().getContainerClient(this.container_name);
      try {
        block_on(this._container.create());
      } catch (e) {
        if (!ResourceExistsError(e)) throw e;
      }
    }
    return this._container;
  }

  /** URL-encode a logical key for blob storage. */
  _object_key(key) {
    return quote(key, { safe: "" });
  }

  /** Decode a blob name back to the logical key. */
  _logical_key(object_key) {
    return unquote(object_key);
  }

  /** Release client references. */
  close() {
    this._client = null;
    this._container = null;
  }

  /** Retrieve the JSON value for *key*, or ``null`` if absent. */
  _read(key) {
    const blob = this._get_container().getBlobClient(this._object_key(key));
    let raw;
    try {
      raw = block_on(blob.downloadToBuffer()).toString("utf-8");
    } catch (e) {
      if (ResourceNotFoundError(e)) return null;
      throw e;
    }
    return json.loads(raw);
  }

  /** Store *entry* as a JSON blob under *key*. */
  _write(key, entry) {
    let value = entry;
    if (isdict(value)) value = json.dumps(value);
    if (!is_str(value)) throw new PyTypeError("AzurePool expects a serialized JSON string.");

    const blob = this._get_container().getBlockBlobClient(this._object_key(key));
    const data = Buffer.from(value, "utf-8");
    block_on(blob.upload(data, data.length, { blobHTTPHeaders: { blobContentType: "application/json" } }));
  }

  /** Delete the blob for *key*; no-op if absent. */
  _delete(key) {
    const blob = this._get_container().getBlobClient(this._object_key(key));
    try {
      block_on(blob.delete());
    } catch (e) {
      if (ResourceNotFoundError(e)) return;
      throw e;
    }
  }

  /** Remove all blobs from the container. */
  _empty() {
    const blobs = _collect(this._get_container().listBlobsFlat());
    for (const blob of blobs) {
      block_on(this._get_container().deleteBlob(blob.name));
    }
  }

  /** Return ``true`` if a blob for *key* exists. */
  _exists(key) {
    const blob = this._get_container().getBlobClient(this._object_key(key));
    return Boolean(block_on(blob.exists()));
  }

  /** Check membership, delegates to ``_exists``. */
  __contains__(key) {
    return this._exists(key);
  }

  /**
   * Return all keys in the container.
   *
   * @param {boolean} [as_generator=false] If ``true``, return a lazy iterator
   *   instead of a list.
   * @returns {Iterable<string>} Pool keys.
   */
  _keys(as_generator = false) {
    const self = this;

    function* _iter_keys() {
      for (const blob of _collect(self._get_container().listBlobsFlat())) yield self._logical_key(blob.name);
    }

    if (!as_generator) return [..._iter_keys()];
    return _iter_keys();
  }
}

register("laila.data.azure.azure", { AzurePool });
