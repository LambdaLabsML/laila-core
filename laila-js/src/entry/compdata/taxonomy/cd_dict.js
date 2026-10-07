/**
 * ``ComputationalData`` subclass for ``dict`` payloads.
 *
 * Dicts are serialized with msgpack rather than pickle so the on-disk
 * representation is language-neutral (other msgpack readers can decode
 * laila-pooled dicts) and substantially smaller for the common
 * shallow-string-keyed cases.
 */
import { define_fields, finalize_model } from "../../../_compat/pydantic.js";
import { AttributeError, TypeError as PyTypeError } from "../../../_compat/errors.js";
import { dict_copy, dict_len, type_name } from "../../../_compat/pytypes.js";
import { deepcopy } from "../../../_compat/copy.js";
import { repr } from "../../../_compat/pyrepr.js";
import { MsgpackSerializer } from "../transformation/serialization/index.js";
import { ComputationalData, register_cdtype } from "./compdata.js";

/**
 * Computational-data wrapper for ``dict`` payloads.
 *
 * Defaults to ``MsgpackSerializer``. ``len(self)`` reports the number of
 * keys; ``shape`` is intentionally undefined (dicts have no canonical shape
 * and silently returning a fake value would mask bugs).
 */
export class CD_dict extends ComputationalData {
  static _SERIALIZER_CLS = MsgpackSerializer;

  static {
    define_fields(this, { data: ["dict[Any, Any]"] });
    finalize_model(this);
  }

  // --- Serializer getter/setter ---
  /** Return the serializer instance (public accessor). */
  get serializer() {
    return this._ensure_serializer();
  }

  /** Set a new serializer instance. */
  set serializer(value) {
    if (!(value instanceof MsgpackSerializer)) throw new PyTypeError(`serializer must be a MsgpackSerializer, got ${type_name(value)}`);
    this._serializer = value;
  }

  /** Return the number of keys in the dict. */
  __len__() {
    return dict_len(this.data);
  }

  /**
   * Not applicable for dicts.
   * @throws {AttributeError} Always.
   */
  get shape() {
    throw new AttributeError("dict has no 'shape' attribute");
  }

  /** Return a shallow copy. */
  __copy__() {
    return new this.constructor(dict_copy(this.data));
  }

  /** Return a deep copy. */
  __deepcopy__(memo = null) {
    return new this.constructor(deepcopy(this.data, memo));
  }

  /** Return a developer-friendly representation. */
  __repr__() {
    return `CD_dict(${repr(this.data)})`;
  }
}

register_cdtype("dict")(CD_dict);
