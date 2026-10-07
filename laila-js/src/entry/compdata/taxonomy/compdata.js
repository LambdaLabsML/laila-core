/**
 * Base ``ComputationalData`` class -- type-dispatched payload wrapper.
 *
 * Every ``Entry`` payload is wrapped in a ``ComputationalData`` (or one of its
 * registered subclasses) so the system has a uniform handle on the data
 * regardless of underlying type. The wrapper provides:
 *
 * - a ``data`` attribute exposing the unwrapped value,
 * - a swappable ``serializer`` (``PickleSerializer`` by default) used by
 *   pool-side ``serialize()`` to produce bytes plus an inverse-decode source
 *   string,
 * - a stub ``__len__`` / ``shape`` / ``__copy__`` / ``__deepcopy__`` API that
 *   subclasses fill in for their concrete payload type.
 *
 * Type dispatch
 * -------------
 * Subclasses decorate themselves with ``register_cdtype(...types)(cls)`` to
 * claim ownership of one or more payload types. ``new ComputationalData(data)``
 * then looks up ``type(data)`` in ``TYPE_TO_WRAPPER`` (and walks the MRO if
 * no exact match is found) and instantiates the matching subclass. A generic
 * ``CD_generic`` fallback wraps anything unknown.
 *
 * Python *types* are modelled as tokens: either the Python type name as
 * returned by ``type_name`` (``"dict"``, ``"list"``, ``"tuple"``, ``"str"``,
 * ``"bytes"``, ... and the catch-all ``"object"``) or a JS constructor
 * (``NDArray``, any class) matched along the prototype chain. The MRO of a
 * value is ``[type_name(x), ...constructors up the prototype chain, "object"]``.
 *
 * This means user code can simply write ``Entry.constant(my_array)`` and the
 * right wrapper (e.g. ``CD_numpyarray``) is selected automatically --
 * subclasses with bespoke serialization are picked transparently.
 */
import { BaseModel, ConfigDict, PrivateAttr, SKIP_VALIDATION, define_fields, define_private } from "../../../_compat/pydantic.js";
import { NotImplementedError, OverflowError, TypeError as PyTypeError, ValueError } from "../../../_compat/errors.js";
import { getitem, is_plain_object, type_name } from "../../../_compat/pytypes.js";
import { repr } from "../../../_compat/pyrepr.js";
import { lazy } from "../../../_compat/lazy.js";
import { indexable, sequence_key } from "../../../_compat/proxy.js";
import { PickleSerializer } from "../transformation/serialization/index.js";

// ---------------------------------------------------------------------
// Mapping: python type ➜ wrapper subclass
// ---------------------------------------------------------------------
/** @type {Map<string|Function, typeof ComputationalData>} */
export const TYPE_TO_WRAPPER = new Map();

/**
 * Decorator that registers *cls* as the wrapper for one or more payload
 * types.
 *
 * Each call records ``TYPE_TO_WRAPPER[t] = cls`` for every ``t`` in
 * *payload_types*. The dispatch in ``ComputationalData``'s constructor first
 * checks for an exact type match and then walks the MRO, so registering
 * against an abstract base (e.g. ``NDArray``) is enough to claim every
 * subclass that doesn't have its own explicit registration.
 *
 * @param {...(string|Function)} payload_types Type tokens that should
 *   resolve to this wrapper subclass.
 */
export function register_cdtype(...payload_types) {
  return function deco(cls) {
    for (const t of payload_types) TYPE_TO_WRAPPER.set(t, cls);
    return cls;
  };
}

/**
 * Raise ``TypeError`` for payloads that have no meaningful length.
 *
 * Used by scalar wrapper subclasses (single int, single float, etc.) to keep
 * ``__len__`` semantically honest -- ``len(scalar)`` should fail loudly
 * rather than silently return 1.
 */
export function _scalar_len() {
  throw new PyTypeError("Length undefined for scalars / objects without __len__");
}

/** @type {typeof ComputationalData|null} */
let _fallback_wrapper = null;

/**
 * Record the catch-all wrapper (``CD_generic`` registers itself here at
 * import time, standing in for Python's in-function
 * ``from .cd_object import CD_generic``).
 * @param {typeof ComputationalData} cls
 */
export function _set_fallback_wrapper(cls) {
  _fallback_wrapper = cls;
}

/**
 * ``type(data).__mro__`` as dispatch tokens: the Python type name first,
 * then every constructor up the prototype chain, then ``"object"``.
 * @param {any} data
 * @returns {Array<string|Function>}
 */
export function _type_mro(data) {
  const out = [type_name(data)];
  if (data !== null && typeof data === "object") {
    // DotMap and other mapping objects are dict subclasses in Python
    if (!is_plain_object(data) && typeof data.toDict === "function" && typeof data.items === "function") out.push("dict");
    let proto = Object.getPrototypeOf(data);
    while (proto !== null && proto !== Object.prototype) {
      if (proto.constructor && !out.includes(proto.constructor)) out.push(proto.constructor);
      proto = Object.getPrototypeOf(proto);
    }
  }
  out.push("object");
  return out;
}

// ---------------------------------------------------------------------
// Factory / Superclass
// ---------------------------------------------------------------------
/**
 * Generic computational-data wrapper with dynamic subclass dispatch and
 * serializer-based transformation.
 *
 * Direct instantiation (``new ComputationalData(value)``) routes to the
 * registered subclass for ``type(value)``, falling back to a generic object
 * wrapper. Subclass instantiation (``new CD_numpyarray(value)``) bypasses
 * dispatch -- the chosen subclass is used directly.
 *
 * Construction mirrors ``ComputationalData(value)`` /
 * ``ComputationalData(data=value)``: the payload is the single positional
 * argument; an optional trailing options object carries other keyword
 * arguments (``new CD_dict(value, {})``). Supplying ``data`` in both places
 * raises ``TypeError``.
 */
export class ComputationalData extends BaseModel {
  static model_config = ConfigDict({ arbitrary_types_allowed: true, validate_assignment: true, repr: false });

  // The serializer is created lazily on first access rather than via
  // ``PrivateAttr(default_factory=...)``: pydantic re-inspects a private
  // factory's signature on every instantiation and ComputationalData is
  // built for every Entry. Subclasses override ``_SERIALIZER_CLS``.
  static _SERIALIZER_CLS = PickleSerializer;

  static {
    define_fields(this, { data: ["object"] });
    define_private(this, { _serializer: PrivateAttr({ default: null }) });
  }

  /**
   * Construct from either a positional payload or a ``data`` option.
   *
   * Dispatches to the registered subclass matching the payload type when
   * called on ``ComputationalData`` itself (Python's ``__new__``):
   *
   * 1. If a concrete subclass is being instantiated directly
   *    (``new CD_numpyarray(value)``), bypass dispatch and use that subclass.
   * 2. Otherwise extract the payload, look it up in ``TYPE_TO_WRAPPER`` by
   *    exact type.
   * 3. If no exact match, walk the payload's MRO and use the first ancestor
   *    with a registration.
   * 4. Fall back to ``CD_generic`` (which uses pickle for everything).
   *
   * @param {...any} args
   * @throws {TypeError} If no payload was provided.
   */
  constructor(...args) {
    if (args[0] === SKIP_VALIDATION) {
      super(SKIP_VALIDATION);
      return;
    }
    const kwargs = ComputationalData._parse_args(args);

    if (new.target === ComputationalData) {
      // Extract payload
      const data = kwargs.data;
      if (data === null || data === undefined) throw new PyTypeError("Missing required argument 'data'");

      // Exact type match, then walk MRO for ancestor matches
      let chosen = null;
      for (const token of _type_mro(data)) {
        chosen = TYPE_TO_WRAPPER.get(token) ?? null;
        if (chosen) break;
      }

      // Fallback (``from .cd_object import CD_generic`` -- resolved lazily
      // because cd_object imports this module)
      if (chosen === null) chosen = _fallback_wrapper ?? lazy("laila.entry.compdata.taxonomy.cd_object").CD_generic;

      return new chosen(...args);
    }

    super(kwargs);
    // ``cd[0]`` / ``cd["key"]`` -> ``__getitem__`` (Python item access)
    return indexable(this, { index_key: sequence_key });
  }

  /**
   * ``(*args, **kwargs)`` -> ``{data, ...}``: one positional payload plus an
   * optional trailing options object.
   * @param {any[]} args
   */
  static _parse_args(args) {
    if (args.length === 0) return {};
    if (args.length === 1) return { data: args[0] };
    if (args.length === 2 && is_plain_object(args[1])) {
      if ("data" in args[1]) throw new PyTypeError("Payload given both positionally and as 'data='");
      return { ...args[1], data: args[0] };
    }
    throw new PyTypeError("At most one positional argument (the payload)");
  }

  /** Return the instance serializer, constructing the class default on first use. */
  _ensure_serializer() {
    let serializer = this._serializer;
    if (serializer === null || serializer === undefined) {
      serializer = new this.constructor._SERIALIZER_CLS();
      this._serializer = serializer;
    }
    return serializer;
  }

  // --- Serializer getter/setter ---
  /**
   * The serializer used by ``serialize``.
   *
   * Defaults to ``PickleSerializer``. Subclasses with bespoke serializers
   * (e.g. ``CD_numpyarray`` -> NumPy npy serializer) override
   * ``_SERIALIZER_CLS``.
   */
  get serializer() {
    return this._ensure_serializer();
  }

  /**
   * Replace the serializer.
   *
   * The new value must duck-type as a ``PickleSerializer`` (i.e. provide
   * ``.forward(data)`` and ``.backward_code``).
   */
  set serializer(value) {
    if (!(value instanceof PickleSerializer)) {
      throw new PyTypeError(`serializer must be a PickleSerializer or compatible object, got ${type_name(value)}`);
    }
    this._serializer = value;
  }

  /**
   * Serialize ``data`` to bytes with the current serializer.
   *
   * @returns {[Uint8Array, string]} ``[serialized_bytes, backward_code]``.
   *   The backward-code string defines a one-argument Python callable that,
   *   applied to ``serialized_bytes``, recovers the original ``data`` value.
   *   The pool's ``Entry.serialize`` then bundles this code into the
   *   inverse-transformation chain so the read-side can rebuild without
   *   knowing the specific serializer that was used at write time.
   *
   * Notes
   * -----
   * Format-specific serializers (msgpack for dicts/lists, ...) reject values
   * their wire format cannot express -- arrays nested in a dict, dates,
   * integers beyond 64 bits. Rather than failing the memorize, such payloads
   * fall back to the universal ``PickleSerializer``. Because the inverse code
   * travels with the entry, readers are oblivious to which path was taken.
   */
  serialize() {
    const serializer = this.serializer;
    try {
      return [serializer.forward(this.data), serializer.backward_code];
    } catch (e) {
      if (!(e instanceof PyTypeError || e instanceof OverflowError || e instanceof ValueError)) throw e;
      if (serializer instanceof PickleSerializer) throw e;
      const fallback = new PickleSerializer();
      return [fallback.forward(this.data), fallback.backward_code];
    }
  }

  // --- Descriptor behavior -------------------------------------------
  /**
   * Descriptor protocol: class-level access yields the wrapper,
   * instance-level access yields the unwrapped payload.
   *
   * JavaScript has no descriptor protocol; the method is kept for API parity
   * (``cd.__get__(null)`` -> wrapper, ``cd.__get__(obj)`` -> payload).
   */
  __get__(obj, _objtype = null) {
    return obj === null || obj === undefined ? this : this.data;
  }

  /** Index into the underlying payload. */
  __getitem__(index) {
    return getitem(this.data, index);
  }

  /** Return a developer-friendly representation. */
  __repr__() {
    return `${this.constructor.name}(data=${repr(this.data)})`;
  }

  __str__() {
    return this.__repr__();
  }

  toString() {
    return this.__repr__();
  }

  /** Return the length of the payload (subclasses must override). */
  __len__() {
    throw new NotImplementedError();
  }

  /** Shape of the payload (subclasses must override). */
  get shape() {
    throw new NotImplementedError();
  }

  /** Shallow copy (subclasses must override). */
  __copy__() {
    throw new NotImplementedError();
  }

  /** Deep copy (subclasses must override). */
  __deepcopy__(_memo = null) {
    throw new NotImplementedError();
  }
}
