/**
 * Abstract base classes for data transformations and the pipeline that chains them.
 *
 * A *transformation* is a reversible function on opaque data: an encoding
 * (base64), a compression (zlib), an encryption (AES), a serialisation step
 * (msgpack, pickle, numpy, torch), or a wrapper into a JSON-friendly string.
 * They are the building blocks that ``SimpleConstitution`` strings together
 * to recover an entry's payload from its serialised form.
 *
 * Two layers live in this module:
 *
 * - ``_data_transformation`` -- the base class every concrete transformation
 *   inherits from. Subclasses register themselves in
 *   ``_data_transformation.REGISTRY`` keyed by their ``name`` field, so the
 *   constitution machinery can look up a transformation by name when
 *   reconstructing it from a serialised dict.
 * - ``TransformationSequence`` -- an ordered pipeline of transformations.
 *   ``forward`` applies them in order on the way out (when serialising into a
 *   pool); ``backward`` is invoked *implicitly* by the constitution by
 *   replaying the inverse code snippets that ``forward`` collects.
 *
 * An empty pipeline is an explicit identity, which lets pools default their
 * ``transformations`` field to ``null`` / ``[]`` without any special-casing on
 * the call sites.
 */
import { BaseModel, ConfigDict, Field, define_fields, finalize_model } from "../../../_compat/pydantic.js";
import { check_abstract } from "../../../_compat/abc.js";
import { TypeError as PyTypeError } from "../../../_compat/errors.js";
import { is_list } from "../../../_compat/pytypes.js";

/**
 * Abstract base for a single reversible data transformation.
 *
 * Concrete subclasses must:
 *
 * - Set a stable, unique ``name`` field. The registry uses this name as its
 *   key, so renaming a transformation is a wire-format breaking change.
 * - Implement ``forward`` (apply on the way out) and ``backward`` (apply on
 *   the way back in). The two must be mathematical inverses on the data shape
 *   they're declared for.
 * - Optionally provide ``backward_code``: a string snippet that can be
 *   evaluated by the constitution to reconstruct the backward step at recovery
 *   time without needing to import the original transformation class. Used by
 *   ``SimpleConstitution``.
 *
 * Subclasses with a non-empty ``name`` are auto-registered in ``REGISTRY``
 * via ``__init_subclass__`` so look-up by name Just Works without any
 * explicit registration call. (JavaScript has no ``__init_subclass__`` hook;
 * subclasses call ``_data_transformation.__init_subclass__(this)`` from their
 * static initialiser right after declaring their ``name`` default.)
 */
export class _data_transformation extends BaseModel {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  /** @type {Object<string, typeof _data_transformation>} */
  static REGISTRY = {};

  static {
    define_fields(this, {
      name: ["str", Field({ default_factory: () => "" })],
      forward_kwargs: ["dict[str, Any]", Field({ default_factory: () => ({}) })],
      backward_kwargs: ["dict[str, Any]", Field({ default_factory: () => ({}) })],
      backward_code: ["str", Field({ default_factory: () => "" })],
    });
  }

  /** ``ABC``: refuse to instantiate while ``forward`` / ``backward`` are abstract. */
  constructor(data = {}) {
    check_abstract(new.target, _data_transformation, ["forward", "backward"]);
    super(data);
  }

  /**
   * Register the subclass in ``REGISTRY`` when *name* is set.
   * @param {typeof _data_transformation} cls
   */
  static __init_subclass__(cls) {
    finalize_model(cls);
    const info = cls.model_fields.name;
    const n = info && !info.is_required ? info.get_default() : null;
    if (typeof n === "string" && n) _data_transformation.REGISTRY[n] = cls;
  }

  /** @abstract */
  forward(_data) {
    throw new PyTypeError(`Can't instantiate abstract class ${this.constructor.name} with abstract method forward`);
  }

  /** @abstract */
  backward(_data) {
    throw new PyTypeError(`Can't instantiate abstract class ${this.constructor.name} with abstract method backward`);
  }
}

/**
 * Sequential transformation pipeline.
 *
 * Holds an ordered list of ``_data_transformation`` instances.
 *
 * - ``forward`` -- applies the transformations left-to-right and returns
 *   ``[transformed_data, inverse_codes_in_reverse]``. The inverse-code list
 *   is what the constitution will replay at recovery time, so it is built in
 *   reverse order on the spot.
 * - ``append`` -- mutate the pipeline (append one or many), returning
 *   ``this`` for chaining.
 * - Iteration / repr -- standard niceties.
 * - An empty pipeline is the identity: ``forward(x)`` returns ``[x, []]`` and
 *   the constitution simply returns ``x`` on the way back.
 *
 * Pre-built sequences are exported from ``laila/entry``
 * (``transformation_base64``, ``transformation_base64_compression``,
 * ``transformation_base64_compression_encryption``,
 * ``transformation_encryption``) for the common pool defaults.
 */
export class TransformationSequence extends BaseModel {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_fields(this, {
      transformations: ["list[_data_transformation]", Field({ default_factory: () => [] })],
    });
  }

  /**
   * Apply every contained transformation in order and collect their inverses.
   *
   * For each transformation ``t`` in ``transformations`` (in declaration
   * order):
   *
   * 1. Replace ``current`` with ``t.forward(current)``.
   * 2. Append ``t.backward_code`` to the inverse-code list.
   *
   * At the end, the inverse-code list is reversed so that recovery at the
   * constitution layer can simply replay the codes top-to-bottom.
   *
   * @param {any} data Input value -- whatever shape the first transformation
   *   in the pipeline expects.
   * @returns {[any, string[]]} ``[transformed_value, inverse_code_snippets]``
   *   where the second element is already reversed for replay.
   */
  forward(data) {
    let current = data;
    /** @type {string[]} */
    const inverse_codes = [];

    for (const transformation of this.transformations) {
      current = transformation.forward(current);
      inverse_codes.push(transformation.backward_code);
    }

    return [current, inverse_codes.slice().reverse()];
  }

  /** Iterate over the contained transformations. */
  [Symbol.iterator]() {
    return this.transformations[Symbol.iterator]();
  }

  __iter__() {
    return this[Symbol.iterator]();
  }

  /**
   * Append one or more transformations to the pipeline.
   *
   * @param {_data_transformation|_data_transformation[]} t Transformation(s) to append.
   * @returns {TransformationSequence} ``this``, for chaining.
   * @throws {TypeError} If *t* is not a valid transformation or list thereof.
   */
  append(t) {
    if (t instanceof _data_transformation) {
      this.transformations.push(t);
      return this;
    }

    if (is_list(t)) {
      for (const _t of t) {
        if (!(_t instanceof _data_transformation)) throw new PyTypeError("All elements must be _data_transformation instances.");
        this.transformations.push(_t);
      }
      return this;
    }

    throw new PyTypeError("append expects a _data_transformation or list of them.");
  }

  /** Return a human-readable pipeline summary. */
  __repr__() {
    if (!this.transformations.length) return `${this.constructor.name}(identity)`;
    const names = this.transformations.map((t) => t.name ?? t.constructor.name).join(" -> ");
    return `${this.constructor.name}(${names})`;
  }
}
