/** NumPy array serialisation / deserialisation transformation. */
import { Field, define_fields } from "../../../../_compat/pydantic.js";
import * as npy from "../../../../_codecs/npy.js";
import { emit_numpy } from "../../../../_codecs/recovery_codes.js";
import { _data_transformation } from "../base.js";
import { _kwargs } from "../base64/base64.js";

/** Reversible NumPy serialiser using ``np.save`` / ``np.load`` (``.npy`` format). */
export class NumpySerializer extends _data_transformation {
  static {
    define_fields(this, { name: ["str", Field({ default: "numpy" })] });
    _data_transformation.__init_subclass__(this);
  }

  /** Build backward_code dynamically after model creation. */
  model_post_init(_context) {
    super.model_post_init(_context);
    this.backward_code = emit_numpy(this.backward_kwargs);
  }

  /**
   * Serialize an ``NDArray`` into bytes.
   * @param {import("../../../../_compat/ndarray.js").NDArray} inp Array to serialize.
   * @returns {Buffer} ``np.save``-encoded bytes.
   */
  forward(inp) {
    const kwargs = { allow_pickle: false, ..._kwargs(this.forward_kwargs) };
    return npy.save(inp, kwargs);
  }

  /**
   * Deserialize bytes back into an ``NDArray``.
   * @param {Uint8Array} inp Bytes produced by ``forward``.
   * @returns {import("../../../../_compat/ndarray.js").NDArray} Reconstructed array.
   */
  backward(inp) {
    const kwargs = { allow_pickle: false, ..._kwargs(this.backward_kwargs) };
    return npy.load(inp, kwargs);
  }
}
