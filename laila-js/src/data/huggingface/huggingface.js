/** Hugging Face Hub pool implementation. */
import { block_on } from "../../_compat/blocking.js";
import { with_ } from "../../_compat/contextlib.js";
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

// ``try: from huggingface_hub import HfApi, hf_hub_download / except ImportError``
const _hub = optional_import("@huggingface/hub");

/** ``huggingface_hub.utils.EntryNotFoundError``: a 404 from the Hub API. */
export function EntryNotFoundError(e) {
  return Boolean(e) && (e.statusCode === 404 || e.name === "EntryNotFoundError");
}

/** ``{"type": repo_type, "name": repo_id}`` -- the Hub's repo designation. */
function _repo(repo_id, repo_type) {
  return { type: repo_type, name: repo_id };
}

/**
 * ``huggingface_hub.hf_hub_download(...)``: fetch a file; raises
 * ``EntryNotFoundError`` when the file does not exist. Returns the decoded
 * text (the Python call returns a cached path the caller then reads).
 */
export const hf_hub_download =
  _hub === null
    ? null
    : function hf_hub_download({ repo_id, filename, repo_type = "model", revision = "main", token = null }) {
        const blob = block_on(
          _hub.downloadFile({
            repo: _repo(repo_id, repo_type),
            path: filename,
            revision,
            accessToken: token ?? undefined,
          }),
        );
        if (blob === null || blob === undefined) {
          const err = new Error(`404 Client Error. Entry Not Found for url: ${repo_id}/${filename}`);
          err.name = "EntryNotFoundError";
          err.statusCode = 404;
          throw err;
        }
        return block_on(blob.text());
      };

/** ``huggingface_hub.HfApi(token=...)`` */
export class HfApi {
  constructor({ token = null } = {}) {
    if (_hub === null) throw new ImportError("huggingface_hub is required for HuggingFacePool");
    this.token = token;
  }

  _access_token(token) {
    const t = token === undefined ? this.token : token;
    return t === null || t === undefined ? undefined : t;
  }

  /** ``api.upload_file(path_or_fileobj=bytes, path_in_repo=..., ...)`` */
  upload_file({ path_or_fileobj, path_in_repo, repo_id, repo_type = "model", revision = "main", commit_message = undefined, token = undefined }) {
    return block_on(
      _hub.uploadFile({
        repo: _repo(repo_id, repo_type),
        file: { path: path_in_repo, content: new Blob([path_or_fileobj]) },
        branch: revision,
        commitTitle: commit_message,
        accessToken: this._access_token(token),
      }),
    );
  }

  /** ``api.delete_file(path_in_repo=..., ...)`` */
  delete_file({ path_in_repo, repo_id, repo_type = "model", revision = "main", commit_message = undefined, token = undefined }) {
    return block_on(
      _hub.deleteFile({
        repo: _repo(repo_id, repo_type),
        path: path_in_repo,
        branch: revision,
        commitTitle: commit_message,
        accessToken: this._access_token(token),
      }),
    );
  }

  /** ``api.list_repo_files(repo_id=..., ...)`` -> list of in-repo paths. */
  list_repo_files({ repo_id, repo_type = "model", revision = "main", token = undefined }) {
    const access_token = this._access_token(token);
    return block_on(
      (async () => {
        const paths = [];
        for await (const entry of _hub.listFiles({ repo: _repo(repo_id, repo_type), revision, recursive: true, accessToken: access_token })) {
          if (entry.type === "file") paths.push(entry.path);
        }
        return paths;
      })(),
    );
  }
}

/**
 * Hugging Face Hub-backed pool.
 *
 * Stores each entry as one JSON object file in a Hub repo (public or private).
 * Objects are keyed by entry global_id directly (URL-encoded).
 */
export class HuggingFacePool extends _LAILA_IDENTIFIABLE_POOL {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_fields(this, {
      repo_id: ["str"],
      repo_type: ["str", Field({ default: "model" })],
      revision: ["str", Field({ default: "main" })],
      token: ["str | None", Field({ default: null })],
      path_prefix: ["str", Field({ default: "laila_pool" })],
      transformations: [[TransformationSequence, "None"], Field({ default: transformation_base64 })],
    });
    define_private(this, {
      _api: PrivateAttr({ default: null }),
    });
  }

  /** Return the ``HfApi`` instance, creating it on first call. */
  _get_api() {
    if (this._api !== null && this._api !== undefined) return this._api;
    if (_hub === null || hf_hub_download === null) throw new ImportError("huggingface_hub is required for HuggingFacePool");
    this._api = new HfApi({ token: this.token });
    return this._api;
  }

  /** Return the normalised path prefix (no leading/trailing slashes). */
  _prefix() {
    return this.path_prefix.replace(/^\/+|\/+$/g, "");
  }

  /** Build the in-repo file path for *key*. */
  _object_key(key) {
    const encoded = quote(key, { safe: "" });
    const prefix = this._prefix();
    if (prefix) return `${prefix}/${encoded}.json`;
    return `${encoded}.json`;
  }

  /** Extract the logical key from an in-repo file path. */
  _logical_key(object_key) {
    const prefix = this._prefix();
    let cleaned = object_key;
    if (prefix && cleaned.startsWith(prefix + "/")) cleaned = cleaned.slice(prefix.length + 1);
    if (cleaned.endsWith(".json")) cleaned = cleaned.slice(0, -5);
    return unquote(cleaned);
  }

  /** Release the API handle. */
  close() {
    this._api = null;
  }

  /** Download and parse the JSON file for *key*, or return ``null``. */
  _read(key) {
    return with_(this.atomic(), () => {
      try {
        if (hf_hub_download === null) throw new ImportError("huggingface_hub is required for HuggingFacePool");
        const raw = hf_hub_download({
          repo_id: this.repo_id,
          filename: this._object_key(key),
          repo_type: this.repo_type,
          revision: this.revision,
          token: this.token,
        });
        return json.loads(raw);
      } catch (e) {
        if (EntryNotFoundError(e)) return null;
        throw e;
      }
    });
  }

  /** Upload *entry* as a JSON file to the Hub repo. */
  _write(key, entry) {
    let value = entry;
    if (isdict(value)) value = json.dumps(value);
    if (!is_str(value)) throw new PyTypeError("HuggingFacePool expects a serialized JSON string.");

    with_(this.atomic(), () => {
      this._get_api().upload_file({
        path_or_fileobj: Buffer.from(value, "utf-8"),
        path_in_repo: this._object_key(key),
        repo_id: this.repo_id,
        repo_type: this.repo_type,
        revision: this.revision,
        commit_message: `laila: set ${key}`,
      });
    });
  }

  /** Delete the file for *key* from the Hub repo; no-op if absent. */
  _delete(key) {
    with_(this.atomic(), () => {
      try {
        this._get_api().delete_file({
          path_in_repo: this._object_key(key),
          repo_id: this.repo_id,
          repo_type: this.repo_type,
          revision: this.revision,
          commit_message: `laila: delete ${key}`,
        });
      } catch (e) {
        if (!EntryNotFoundError(e)) throw e;
      }
    });
  }

  /** Remove all entries from the pool. */
  _empty() {
    with_(this.atomic(), () => {
      for (const key of [...this._keys(false)]) this._delete(key);
    });
  }

  /** Return ``true`` if *key* exists in the Hub repo. */
  _exists(key) {
    return with_(this.atomic(), () => {
      try {
        if (hf_hub_download === null) throw new ImportError("huggingface_hub is required for HuggingFacePool");
        hf_hub_download({
          repo_id: this.repo_id,
          filename: this._object_key(key),
          repo_type: this.repo_type,
          revision: this.revision,
          token: this.token,
        });
        return true;
      } catch (e) {
        if (EntryNotFoundError(e)) return false;
        throw e;
      }
    });
  }

  /** Check membership, delegates to ``_exists``. */
  __contains__(key) {
    return this._exists(key);
  }

  /**
   * Return all keys in the Hub repo under the configured prefix.
   *
   * @param {boolean} [as_generator=false] If ``true``, return a lazy iterator
   *   instead of a list.
   * @returns {Iterable<string>} Pool keys.
   */
  _keys(as_generator = false) {
    const prefix = this._prefix();
    const all_files = this._get_api().list_repo_files({
      repo_id: this.repo_id,
      repo_type: this.repo_type,
      revision: this.revision,
      token: this.token,
    });
    const self = this;

    function* _iter_keys() {
      for (const path of all_files) {
        if (prefix && !path.startsWith(prefix + "/")) continue;
        if (!path.endsWith(".json")) continue;
        yield self._logical_key(path);
      }
    }

    if (!as_generator) return [..._iter_keys()];
    return _iter_keys();
  }
}

register("laila.data.huggingface.huggingface", { HuggingFacePool });
