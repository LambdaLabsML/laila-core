/** Pickle serialisation / deserialisation transformation. */
import { Field, define_fields } from "../../../../_compat/pydantic.js";
import * as pickle from "../../../../_codecs/pickle.js";
import { emit_pickle } from "../../../../_codecs/recovery_codes.js";
import { _data_transformation } from "../base.js";
import { _kwargs } from "../base64/base64.js";

/** Reversible pickle serialiser for arbitrary objects. */
export class PickleSerializer extends _data_transformation {
  static {
    define_fields(this, { name: ["str", Field({ default: "pickle" })] });
    _data_transformation.__init_subclass__(this);
  }

  /** Build backward_code dynamically after model creation. */
  model_post_init(_context) {
    super.model_post_init(_context);
    this.backward_code = emit_pickle(this.backward_kwargs);
  }

  /**
   * Serialize a value into bytes using pickle.
   * @param {any} inp Object to serialize.
   * @returns {Buffer} Pickled bytes.
   */
  forward(inp) {
    const kwargs = { ..._kwargs(this.forward_kwargs) };
    return pickle.dumps(inp, kwargs);
  }

  /**
   * Deserialize bytes back into a value using pickle.
   * @param {Uint8Array} inp Pickled bytes.
   * @returns {any} Unpickled object.
   */
  backward(inp) {
    const kwargs = { ..._kwargs(this.backward_kwargs) };
    return pickle.loads(inp, kwargs);
  }
}
