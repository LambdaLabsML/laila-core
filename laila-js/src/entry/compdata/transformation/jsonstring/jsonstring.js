/**
 * JSON string serialisation / deserialisation data transformation.
 *
 * The ``JsonString`` transformation is the canonical "round-trip through JSON
 * text" step. ``forward`` accepts any JSON-serialisable value and emits a
 * compact JSON string (no whitespace, unicode-preserving); ``backward`` parses
 * the string back to the equivalent value.
 *
 * This is most useful as the final step of a pipeline whose pool backend
 * stores plain strings (filesystem JSON files, S3 JSON objects, postgres TEXT
 * columns, ...). For binary serialisation of arbitrary objects, prefer
 * ``PickleSerializer``; for compact binary representations of JSON-shaped
 * data, prefer ``MsgpackSerializer``.
 */
import { Field, define_fields } from "../../../../_compat/pydantic.js";
import { JSONDecodeError, TypeError as PyTypeError, ValueError } from "../../../../_compat/errors.js";
import * as json from "../../../../_compat/pyjson.js";
import { emit_json_string } from "../../../../_codecs/recovery_codes.js";
import { _data_transformation } from "../base.js";
import { _kwargs } from "../base64/base64.js";

/**
 * Reversible JSON string transformation.
 *
 * Forward serialises a value to a compact JSON string; backward parses it
 * back.
 */
export class JsonString extends _data_transformation {
  static {
    define_fields(this, { name: ["str", Field({ default: "json_string" })] });
    _data_transformation.__init_subclass__(this);
  }

  /** Build standalone backward recovery code. */
  model_post_init(_context) {
    super.model_post_init(_context);
    // Standalone recovery code mirroring `backward()` with embedded kwargs
    this.backward_code = emit_json_string();
  }

  /**
   * Serialize *data* to a compact JSON string.
   * @param {any} data JSON-serialisable value.
   * @returns {string} Compact JSON string.
   * @throws {TypeError} If *data* is not JSON-serialisable.
   */
  forward(data) {
    try {
      return json.dumps(data, { separators: [",", ":"], ensure_ascii: false, ..._kwargs(this.forward_kwargs) });
    } catch (e) {
      if (e instanceof PyTypeError || e instanceof ValueError) throw new PyTypeError(`JsonString.forward: object not JSON serializable: ${e.message}`);
      throw e;
    }
  }

  /**
   * Deserialize a JSON string back to a value.
   * @param {string} data JSON string.
   * @returns {any} Parsed value.
   * @throws {TypeError} If *data* is not a string or contains invalid JSON.
   */
  backward(data) {
    if (typeof data !== "string") throw new PyTypeError("JsonString.backward expects a JSON string (str)");
    try {
      return json.loads(data, _kwargs(this.backward_kwargs));
    } catch (e) {
      if (e instanceof JSONDecodeError) throw new PyTypeError(`JsonString.backward: invalid JSON string: ${e.msg}`);
      throw e;
    }
  }
}
