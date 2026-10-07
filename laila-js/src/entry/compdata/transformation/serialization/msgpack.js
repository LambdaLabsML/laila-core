/**
 * Msgpack serialisation / deserialisation transformation.
 *
 * ``backward`` (and the emitted ``backward_code``) unpack with
 * ``strict_map_key: false`` so that dict payloads keyed by ``int`` /
 * ``float`` / ``bool`` -- which msgpack packs without complaint -- round-trip
 * instead of raising ``ValueError`` on the way back.
 */
import { Field, define_fields } from "../../../../_compat/pydantic.js";
import * as msgpack from "../../../../_codecs/msgpack.js";
import { emit_msgpack } from "../../../../_codecs/recovery_codes.js";
import { _data_transformation } from "../base.js";
import { _kwargs } from "../base64/base64.js";

/** Reversible msgpack serialiser for objects. */
export class MsgpackSerializer extends _data_transformation {
  static {
    define_fields(this, { name: ["str", Field({ default: "msgpack" })] });
    _data_transformation.__init_subclass__(this);
  }

  /** Build backward_code dynamically after model creation. */
  model_post_init(_context) {
    super.model_post_init(_context);
    this.backward_code = emit_msgpack(this.backward_kwargs);
  }

  /**
   * Serialize a value into bytes using msgpack.
   * @param {any} inp Object to serialize.
   * @returns {Buffer} Msgpack-encoded bytes.
   */
  forward(inp) {
    const kwargs = { use_bin_type: true, ..._kwargs(this.forward_kwargs) };
    return msgpack.packb(inp, kwargs);
  }

  /**
   * Deserialize bytes back into a value using msgpack.
   * @param {Uint8Array} inp Msgpack-encoded bytes.
   * @returns {any} Deserialized object.
   */
  backward(inp) {
    const kwargs = { raw: false, strict_map_key: false, ..._kwargs(this.backward_kwargs) };
    return msgpack.unpackb(inp, kwargs);
  }
}
