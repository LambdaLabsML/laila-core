/**
 * ``ComputationalData`` catch-all subclass for arbitrary objects.
 *
 * Registered against ``object`` so the MRO walk in the ``ComputationalData``
 * dispatcher always finds at least this class for otherwise-unknown payload
 * types. Uses ``PickleSerializer`` -- the broadest-compatibility option, at
 * the cost of being Python-specific on the wire.
 */
import { finalize_model } from "../../../_compat/pydantic.js";
import { AttributeError, TypeError as PyTypeError } from "../../../_compat/errors.js";
import { len, type_name } from "../../../_compat/pytypes.js";
import { deepcopy } from "../../../_compat/copy.js";
import { repr } from "../../../_compat/pyrepr.js";
import { PickleSerializer } from "../transformation/serialization/index.js";
import { ComputationalData, _scalar_len, _set_fallback_wrapper, register_cdtype } from "./compdata.js";

/** ``hasattr(x, "__len__")`` for JS values. */
function _has_len(x) {
  if (x === null || x === undefined) return false;
  if (typeof x === "string" || Array.isArray(x) || x instanceof Uint8Array || x instanceof Map || x instanceof Set) return true;
  if (typeof x === "object") return typeof x.__len__ === "function" || Object.getPrototypeOf(x) === Object.prototype;
  return false;
}

/**
 * Catch-all computational-data wrapper for arbitrary objects.
 *
 * Selected by the ``ComputationalData`` dispatcher when no more specific
 * wrapper is registered for the payload's type. Pickles the value to bytes
 * for serialization. ``len()`` defers to the payload's own ``__len__`` when
 * present and otherwise raises (per ``_scalar_len``).
 */
export class CD_generic extends ComputationalData {
  static _SERIALIZER_CLS = PickleSerializer;

  static {
    finalize_model(this);
  }

  // --- Serializer getter/setter ---
  /** Return the serializer instance (public accessor). */
  get serializer() {
    return this._ensure_serializer();
  }

  /** Set a new serializer instance. */
  set serializer(value) {
    if (!(value instanceof PickleSerializer)) throw new PyTypeError(`serializer must be a PickleSerializer, got ${type_name(value)}`);
    this._serializer = value;
  }

  /** Return the length if the payload supports ``__len__``. */
  __len__() {
    if (_has_len(this.data)) return len(this.data);
    return _scalar_len();
  }

  /**
   * Not applicable for generic objects.
   * @throws {AttributeError} Always.
   */
  get shape() {
    throw new AttributeError(`${type_name(this.data)} has no 'shape' attribute`);
  }

  /** Return a shallow copy. */
  __copy__() {
    return new this.constructor(this.data);
  }

  /** Return a deep copy. */
  __deepcopy__(memo = null) {
    return new this.constructor(deepcopy(this.data, memo));
  }

  /** Return a developer-friendly representation. */
  __repr__() {
    return `CD_generic(${repr(this.data)})`;
  }
}

register_cdtype("object")(CD_generic); // final catch-all
_set_fallback_wrapper(CD_generic);
