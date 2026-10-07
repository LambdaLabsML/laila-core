/**
 * Fernet symmetric encryption / decryption data transformation.
 *
 * The ``FernetEncryption`` transformation slots into a transformation pipeline
 * as the encryption step. It wraps a UTF-8 string in a Fernet token
 * (AES-128-CBC + HMAC-SHA256, time-stamped, URL-safe base64 encoded) using the
 * ``_codecs/fernet.js`` port of the ``cryptography`` library.
 *
 * A pipeline that compresses then encrypts (e.g.
 * ``[Json, Zlib, FernetEncryption, Base64]``) gives you compact,
 * ciphertext-only blobs that can sit safely in any text-only pool backend.
 *
 * Optional TTL
 * ------------
 * ``backward_kwargs: {ttl: seconds}`` instructs ``backward`` to reject tokens
 * older than ``seconds`` (raising ``ValueError``). This is useful for
 * ephemeral payloads that should not be readable after a deadline -- the same
 * machinery is exposed in the standalone recovery snippet emitted into
 * ``backward_code``.
 *
 * Key handling
 * ------------
 * The key is **never** written into ``backward_code`` or into any serialized
 * form of the transformation (it is an ``exclude: true`` field). Like every
 * other secret in laila it lives in ``laila.args``:
 *
 * - canonical location: ``laila.args.encryption.key``
 * - convenience alias:  ``laila.encryption_key`` (getter / setter)
 *
 * Both the writer and the reader are expected to configure the *same* key in
 * their own process (directly, or through ``laila.read_args(<secrets file>)``).
 * Construct the transformation with no ``key`` to pick it up from there; an
 * explicit ``key`` still wins and is useful for ad-hoc / test pipelines.
 *
 * The recovery snippet carries only a short SHA-256 fingerprint of the
 * write-time key, so a reader configured with a *different* key fails with a
 * descriptive ``ValueError`` instead of a bare ``InvalidToken``. A
 * 12-hex-character prefix of a hash of a random 32-byte key discloses nothing
 * useful about the key itself.
 */
import { createHash } from "node:crypto";
import { Field, define_fields, field_validator } from "../../../../_compat/pydantic.js";
import { RuntimeError, TypeError as PyTypeError, ValueError } from "../../../../_compat/errors.js";
import { lazy } from "../../../../_compat/lazy.js";
import { isdict, dict_len } from "../../../../_compat/pytypes.js";
import { Fernet, InvalidToken } from "../../../../_codecs/fernet.js";
import { emit_fernet, register_backend } from "../../../../_codecs/recovery_codes.js";
import { _data_transformation } from "../base.js"; // renamed base
import { _kwargs } from "../base64/base64.js";

export const _FINGERPRINT_HEX_LEN = 12;

/**
 * @param {any} v
 * @returns {Buffer}
 */
export function _coerce_key_bytes(v) {
  if (typeof v === "string") v = Buffer.from(v, "utf8");
  if (!(v instanceof Uint8Array)) throw new PyTypeError("Encryption.key must be bytes or str");
  return Buffer.from(v);
}

/**
 * Return the short SHA-256 fingerprint laila embeds in recovery code.
 * @param {string|Uint8Array} key
 * @returns {string}
 */
export function key_fingerprint(key) {
  return createHash("sha256").update(_coerce_key_bytes(key)).digest("hex").slice(0, _FINGERPRINT_HEX_LEN);
}

/**
 * Return the process-wide encryption key configured in ``laila.args``.
 *
 * Looks up ``laila.args.encryption.key`` (also reachable as
 * ``laila.encryption_key``) without auto-creating DotMap nodes.
 *
 * @param {string|null} [expected_fingerprint] Fingerprint recorded at write
 *   time (see ``key_fingerprint``). When given, the configured key must
 *   produce the same fingerprint.
 * @returns {Buffer}
 * @throws {RuntimeError} No key is configured in this process.
 * @throws {ValueError} A key is configured but does not match
 *   *expected_fingerprint*.
 */
export function resolve_encryption_key(expected_fingerprint = null) {
  const laila = lazy("laila");

  const section = laila.args.get("encryption");
  const key = section !== null && section !== undefined && typeof section.get === "function" ? section.get("key") : null;
  if (key === null || key === undefined || key === "" || (isdict(key) && dict_len(key) === 0) || (key && typeof key.toDict === "function" && typeof key.items === "function" && key.items().length === 0)) {
    throw new RuntimeError(
      "no encryption key configured in this process; set " +
        "`laila.encryption_key = <fernet key>` (i.e. laila.args.encryption.key) " +
        "on both the writing and the reading side",
    );
  }
  const key_bytes = _coerce_key_bytes(key);
  if (expected_fingerprint !== null && expected_fingerprint !== undefined && key_fingerprint(key_bytes) !== expected_fingerprint) {
    throw new ValueError(
      "configured encryption key (laila.encryption_key) does not match the key used " +
        `at write time (fingerprint ${expected_fingerprint}); set the same key as the writer`,
    );
  }
  return key_bytes;
}

/**
 * Reversible Fernet symmetric encryption transformation.
 *
 * Constructor options:
 * - ``key`` (string | bytes, optional): Fernet-compatible encryption key.
 *   When omitted the key is read from ``laila.args.encryption.key`` /
 *   ``laila.encryption_key``. Never serialized or shown in ``repr``.
 */
export class FernetEncryption extends _data_transformation {
  static {
    define_fields(this, {
      name: ["str", Field({ default: "fernet" })],
      key: ["str | bytes | None", Field({ default: null, exclude: true, repr: false })],
    });
    _data_transformation.__init_subclass__(this);
    field_validator(this, "key", FernetEncryption._coerce_key);
  }

  /** Coerce string keys to bytes; ``null`` defers to ``laila.args``. */
  static _coerce_key(_cls, v) {
    if (v === null || v === undefined) return null;
    return _coerce_key_bytes(v);
  }

  /** Initialise the Fernet encryptor and build backward recovery code. */
  model_post_init(_context) {
    super.model_post_init(_context);

    if (this.key === null || this.key === undefined) this.key = resolve_encryption_key();

    this._fernet = new Fernet(this.key);

    // Standalone recovery code: no key material, only its fingerprint.
    // The reader resolves the key from its own ``laila.args``.
    this.backward_code = emit_fernet(key_fingerprint(this.key), this.backward_kwargs);
  }

  /**
   * Encrypt a UTF-8 string and return the Fernet token as a string.
   * @param {string} data Plain-text string to encrypt.
   * @returns {string} Fernet token.
   * @throws {TypeError} If *data* is not a string.
   */
  forward(data) {
    if (typeof data !== "string") throw new PyTypeError("Encryption.forward expects a Base64 string (str)");
    return this._fernet.encrypt(Buffer.from(data, "utf8")).toString("utf8");
  }

  /**
   * Decrypt a Fernet token back to the original string.
   * @param {string} data Fernet token string.
   * @returns {string} Original plain-text string.
   * @throws {TypeError} If *data* is not a string.
   * @throws {ValueError} If the token is invalid or TTL has expired.
   */
  backward(data) {
    if (typeof data !== "string") throw new PyTypeError("Encryption.backward expects a Fernet token string (str)");
    const ttl = _kwargs(this.backward_kwargs).ttl ?? null;

    try {
      return Buffer.from(this._fernet.decrypt(Buffer.from(data, "utf8"), ttl)).toString("utf8");
    } catch (e) {
      if (e instanceof InvalidToken) {
        const err = new ValueError("Invalid Fernet token or TTL expired");
        err.__cause__ = e;
        throw err;
      }
      throw e;
    }
  }
}

// ``_fernet`` is a plain (non-pydantic) instance attribute in Python
// (``_fernet: Any = None`` on a model becomes a private attr); mirror that
// with a prototype default so unbuilt instances read ``null``.
Object.defineProperty(FernetEncryption.prototype, "_fernet", { value: null, writable: true, configurable: true });

// JS backend for the ``fernet`` recovery snippet: resolve the key from
// ``laila.args`` (checking the fingerprint) exactly like the Python snippet.
register_backend("fernet", ({ fingerprint, kwargs }) => {
  return (inp) => {
    if (typeof inp !== "string") throw new PyTypeError("Encryption.backward expects a Fernet token string (str)");
    const f = new Fernet(resolve_encryption_key(fingerprint));
    const token = Buffer.from(inp, "utf8");
    const ttl = _kwargs(kwargs).ttl ?? null;
    let out;
    try {
      out = f.decrypt(token, ttl);
    } catch (e) {
      if (e instanceof InvalidToken) {
        const err = new ValueError("Invalid Fernet token or TTL expired");
        err.__cause__ = e;
        throw err;
      }
      throw e;
    }
    return Buffer.from(out).toString("utf8");
  };
});
