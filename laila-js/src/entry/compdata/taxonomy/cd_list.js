/**
 * ``ComputationalData`` subclass for ``list`` and ``tuple`` payloads.
 *
 * Both sequence types share one wrapper. Lists serialize as msgpack arrays;
 * tuples default to pickle because msgpack has no tuple type, which is what
 * keeps the original type (list vs tuple) intact on round-trip. Copy
 * semantics respect the source type (tuples are immutable so shallow-copy
 * returns the same object).
 *
 * Known limitation: a tuple *nested inside a list* still goes through msgpack
 * and comes back as a list.
 */
import { define_fields, finalize_model } from "../../../_compat/pydantic.js";
import { TypeError as PyTypeError } from "../../../_compat/errors.js";
import { PyTuple, tuple, type_name } from "../../../_compat/pytypes.js";
import { deepcopy } from "../../../_compat/copy.js";
import { MsgpackSerializer, PickleSerializer } from "../transformation/serialization/index.js";
import { ComputationalData, register_cdtype } from "./compdata.js";

/**
 * Computational-data wrapper for ``list`` and ``tuple`` payloads.
 *
 * Defaults to ``MsgpackSerializer``. ``shape`` returns a 1-D shape
 * ``(len,)`` so callers that bridge between sequence-like and array-like
 * compdata can use a uniform interface.
 */
export class CD_list extends ComputationalData {
  static _SERIALIZER_CLS = MsgpackSerializer;

  static {
    define_fields(this, { data: ["list[Any] | tuple[Any, ...]"] });
    finalize_model(this);
  }

  /**
   * Msgpack for lists; pickle for tuples.
   *
   * msgpack has no tuple type -- a tuple comes back as a list -- so a tuple
   * payload defaults to ``PickleSerializer``, which is the only way to honour
   * the "list vs tuple is preserved on round-trip" promise in the module
   * docstring. Lists keep the compact msgpack encoding.
   */
  _ensure_serializer() {
    let serializer = this._serializer;
    if (serializer === null || serializer === undefined) {
      if (this.data instanceof PyTuple) serializer = new PickleSerializer();
      else serializer = new this.constructor._SERIALIZER_CLS();
      this._serializer = serializer;
    }
    return serializer;
  }

  // --- Serializer getter/setter ---
  /** Return the serializer instance (public accessor). */
  get serializer() {
    return this._ensure_serializer();
  }

  /** Set a new serializer instance. */
  set serializer(value) {
    if (!(value instanceof MsgpackSerializer || value instanceof PickleSerializer)) {
      throw new PyTypeError(`serializer must be a MsgpackSerializer or PickleSerializer, got ${type_name(value)}`);
    }
    this._serializer = value;
  }

  /** Return the number of elements. */
  __len__() {
    return this.data.length;
  }

  /** Return a 1-D shape tuple ``(len,)``. */
  get shape() {
    return tuple([this.data.length]);
  }

  /** Return a shallow copy. */
  __copy__() {
    let copied_data;
    if (this.data instanceof PyTuple) copied_data = this.data; // tuples are immutable
    else copied_data = this.data.slice();
    return new this.constructor(copied_data);
  }

  /** Return a deep copy. */
  __deepcopy__(memo = null) {
    memo = memo ?? new Map();
    const items = [];
    for (const e of this.data) items.push(deepcopy(e, memo));
    const copied = this.data instanceof PyTuple ? PyTuple.from_iterable(items) : items;
    return new this.constructor(copied);
  }

  /** Return a developer-friendly representation. */
  __repr__() {
    const tname = this.data instanceof PyTuple ? "tuple" : "list";
    return `CD_list(type=${tname}, len=${this.data.length})`;
  }
}

register_cdtype("list", "tuple")(CD_list);
