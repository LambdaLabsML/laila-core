/**
 * Zlib compression / decompression data transformation.
 *
 * The ``Zlib`` transformation slots into a transformation pipeline as the
 * compression step. It accepts a UTF-8 string, compresses it with
 * ``zlib.compress``, and re-encodes the compressed bytes with base64 so the
 * result is still a plain string (matching the contract assumed by downstream
 * encoding / serialisation steps and by JSON-only pools).
 *
 * Two reasons for the integrated base64 wrap inside this single
 * transformation:
 *
 * 1. It keeps the output of every transformation in the pipeline a string, so
 *    users can chain ``[Json, Zlib, Base64]`` -- or just ``[Json, Zlib]`` --
 *    without thinking about bytes/str mismatches.
 * 2. The recovery code that the transformation emits is therefore
 *    self-contained: it only needs to import ``zlib`` and ``base64``, with no
 *    implicit dependency on the upstream pipe step's output type.
 *
 * ``forward_kwargs`` / ``backward_kwargs`` are forwarded to ``zlib.compress``
 * / ``zlib.decompress`` so callers can tune compression level, window bits,
 * etc.
 */
import { Field, define_fields } from "../../../../_compat/pydantic.js";
import { TypeError as PyTypeError } from "../../../../_compat/errors.js";
import * as base64 from "../../../../_codecs/base64.js";
import * as zlib from "../../../../_codecs/zlib.js";
import { emit_zlib } from "../../../../_codecs/recovery_codes.js";
import { _data_transformation } from "../base.js";
import { _kwargs } from "../base64/base64.js";

const _utf8_strict = new TextDecoder("utf-8", { fatal: true });

/**
 * Reversible zlib compression transformation.
 *
 * Forward compresses a UTF-8 string to a Base64-encoded compressed string;
 * backward decompresses it.
 */
export class Zlib extends _data_transformation {
  static {
    define_fields(this, { name: ["str", Field({ default: "zlib" })] });
    _data_transformation.__init_subclass__(this);
  }

  /** Build standalone backward recovery code. */
  model_post_init(_context) {
    super.model_post_init(_context);
    // Standalone recovery code mirroring `backward()` with embedded kwargs
    this.backward_code = emit_zlib(this.backward_kwargs);
  }

  /**
   * Compress a UTF-8 string and return a Base64-encoded result.
   * @param {string} data Plain-text UTF-8 string.
   * @returns {string} Base64-encoded compressed bytes.
   * @throws {TypeError} If *data* is not a string.
   */
  forward(data) {
    if (typeof data !== "string") throw new PyTypeError("Zlib.forward expects a UTF-8 string (str)");
    const compressed = zlib.compress(Buffer.from(data, "utf8"), _kwargs(this.forward_kwargs));
    return base64.b64encode(compressed).toString("utf8");
  }

  /**
   * Decompress a Base64-encoded compressed string.
   * @param {string} data Base64-encoded compressed payload.
   * @returns {string} Original UTF-8 string.
   * @throws {TypeError} If *data* is not a string.
   */
  backward(data) {
    if (typeof data !== "string") throw new PyTypeError("Zlib.backward expects a Base64 string (str)");
    const compressed = base64.b64decode(data, { validate: true });
    return _utf8_strict.decode(zlib.decompress(compressed, _kwargs(this.backward_kwargs)));
  }
}
