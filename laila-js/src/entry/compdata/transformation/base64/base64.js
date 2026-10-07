/**
 * Base64 encode / decode data transformation.
 *
 * The ``Base64`` transformation makes binary blobs JSON-safe by mapping bytes
 * <-> ASCII strings via ``base64.b64encode`` / ``base64.b64decode``. It is the
 * standard "tail" step in pool transformation pipelines whose backing store
 * wants only printable text (filesystem JSON, S3 JSON objects, postgres TEXT
 * columns, ...).
 *
 * The forward direction takes bytes (``Uint8Array`` / ``Buffer`` /
 * ``PyByteArray`` / ``DataView``) and returns a UTF-8 string; the backward
 * direction accepts either flavour and returns bytes. ``forward_kwargs`` and
 * ``backward_kwargs`` are forwarded to the underlying base64 functions so
 * callers can, for example, configure a URL-safe alphabet via
 * ``altchars: Buffer.from("-_")``.
 */
import { Field, define_fields } from "../../../../_compat/pydantic.js";
import { TypeError as PyTypeError, ValueError } from "../../../../_compat/errors.js";
import { dict_items } from "../../../../_compat/pytypes.js";
import * as base64 from "../../../../_codecs/base64.js";
import { emit_base64 } from "../../../../_codecs/recovery_codes.js";
import { _data_transformation } from "../base.js";

/** Plain-object view of a kwargs mapping (Object / Map). */
export function _kwargs(kw) {
  const o = {};
  for (const [k, v] of dict_items(kw ?? {})) o[String(k)] = v;
  return o;
}

/**
 * Reversible Base64 encoding transformation.
 *
 * Forward encodes binary data to a Base64 UTF-8 string; backward decodes it
 * back to raw bytes.
 */
export class Base64 extends _data_transformation {
  static {
    define_fields(this, { name: ["str", Field({ default: "base64" })] });
    _data_transformation.__init_subclass__(this);
  }

  /** Build standalone backward recovery code. */
  model_post_init(_context) {
    super.model_post_init(_context);
    // Standalone recovery code with embedded kwargs (e.g., altchars, validate)
    this.backward_code = emit_base64(this.backward_kwargs);
  }

  /**
   * Encode binary data -> Base64 UTF-8 string.
   * @param {Uint8Array|DataView} data
   * @returns {string}
   */
  forward(data) {
    if (data instanceof DataView) data = new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    if (!(data instanceof Uint8Array)) throw new PyTypeError("Base64.forward expects bytes/bytearray/memoryview");
    // b64encode supports altchars=...
    const out = base64.b64encode(Buffer.from(data), _kwargs(this.forward_kwargs));
    return out.toString("utf8");
  }

  /**
   * Decode Base64 (str/bytes/bytearray/memoryview) -> raw bytes.
   * @param {string|Uint8Array|DataView} payload
   * @returns {Buffer}
   */
  backward(payload) {
    if (payload instanceof DataView) payload = new Uint8Array(payload.buffer, payload.byteOffset, payload.byteLength);
    // base64.b64decode accepts str or bytes-like
    try {
      return base64.b64decode(payload, _kwargs(this.backward_kwargs));
    } catch (e) {
      const err = new ValueError("Invalid Base64 payload");
      err.__cause__ = e;
      throw err;
    }
  }
}
