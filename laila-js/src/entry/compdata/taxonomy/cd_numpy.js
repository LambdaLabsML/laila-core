/**
 * ``ComputationalData`` subclass for ``numpy.ndarray`` (``NDArray``) payloads.
 *
 * Uses ``NumpySerializer`` (the ``.npy`` format under the hood) so arrays are
 * persisted with their dtype and shape in a portable, language-neutral binary
 * format -- much more compact than pickle for numeric data and readable from
 * any process with NumPy installed.
 */
import { define_fields, finalize_model } from "../../../_compat/pydantic.js";
import { TypeError as PyTypeError } from "../../../_compat/errors.js";
import { tuple, type_name } from "../../../_compat/pytypes.js";
import { NDArray, dtype_name } from "../../../_compat/ndarray.js";
import { NumpySerializer } from "../transformation/serialization/index.js";
import { ComputationalData, _scalar_len, register_cdtype } from "./compdata.js";

/**
 * Computational-data wrapper for ``NDArray`` payloads.
 *
 * Defaults to ``NumpySerializer`` (``.npy`` format). ``len()`` follows NumPy
 * semantics (size of the first axis) and raises for 0-D arrays. ``shape``
 * returns the array's native shape tuple. Both shallow and deep copy emit a
 * contiguous copy of the underlying buffer.
 */
export class CD_numpyarray extends ComputationalData {
  static _SERIALIZER_CLS = NumpySerializer;

  static {
    define_fields(this, { data: [NDArray] });
    finalize_model(this);
  }

  // --- Serializer getter/setter ---
  /** Return the serializer instance (public accessor). */
  get serializer() {
    return this._ensure_serializer();
  }

  /** Set a new serializer instance. */
  set serializer(value) {
    if (!(value instanceof NumpySerializer)) throw new PyTypeError(`serializer must be a NumpySerializer, got ${type_name(value)}`);
    this._serializer = value;
  }

  /** Return the size of the first dimension. */
  __len__() {
    return this.data.ndim ? this.data.shape[0] : _scalar_len();
  }

  /** Return the array shape tuple. */
  get shape() {
    return tuple(this.data.shape);
  }

  /** Return a shallow (contiguous) copy of the array. */
  __copy__() {
    return new this.constructor(this.data.copy());
  }

  /** Return a deep copy of the array. */
  __deepcopy__(_memo = null) {
    return new this.constructor(this.data.copy());
  }

  /** Return a developer-friendly representation. */
  __repr__() {
    return `CD_numpyarray(shape=(${this.data.shape.join(", ")}${this.data.shape.length === 1 ? "," : ""}), dtype=${dtype_name(this.data.dtype)})`;
  }
}

register_cdtype(NDArray)(CD_numpyarray);
