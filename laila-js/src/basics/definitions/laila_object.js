/**
 * Root base class for every laila object.
 *
 * ``_LAILA_OBJECT`` sits at the very bottom of the laila class hierarchy --
 * both ``_LAILA_IDENTIFIABLE_OBJECT`` (identity / global ids) and
 * ``_LAILA_LOCALLY_ATOMIC_OBJECT`` (per-instance locking) derive from it, so
 * entries, policies, pools, futures, the logger and the ``Atomic*`` wrappers
 * all share whatever lives here.
 *
 * Today that is a single concern: a **creation timestamp** recording when the
 * object was constructed, as an ISO-8601 UTC string with millisecond
 * precision (the same shape as ``Record.record_timestamp``). It answers "when
 * was this object created?" for any laila object, independent of when (or
 * whether) it is memorized into a pool.
 *
 * Implementation note: the stamp is applied in the constructor *after*
 * ``super()`` returns rather than in ``model_post_init``. Pydantic v2's
 * ``validate_python`` wipes private attributes set before the base
 * initialiser runs, and not every subclass chains ``model_post_init`` back to
 * this class; stamping after the super call is robust to both.
 * Deserialization paths that bypass the constructor (e.g. ``Entry.from_dict``,
 * which uses ``Object.create``) restore or re-stamp the value by assigning
 * ``_creation_timestamp`` directly; the public ``creation_timestamp`` property
 * is read-only.
 */
import { BaseModel, PrivateAttr, define_private, SKIP_VALIDATION } from "../../_compat/pydantic.js";
import { now_iso_ms } from "../../_compat/datetime.js";

/**
 * Return the current UTC time as an ISO-8601 string (millisecond precision).
 *
 * Shared by ``_LAILA_OBJECT`` and by deserializers that need to stamp objects
 * constructed outside the constructor.
 * @returns {string}
 */
export function _now_creation_timestamp() {
  return now_iso_ms();
}

/**
 * Pydantic base model shared by every laila object.
 *
 * Provides ``creation_timestamp`` -- the ISO-8601 UTC creation time of the
 * instance, captured once at construction.
 *
 * Notes
 * -----
 * The private attribute uses a plain ``PrivateAttr({default: null})`` rather
 * than a ``default_factory`` for the same reason identity fields do in
 * ``_LAILA_IDENTIFIABLE_OBJECT``: Pydantic re-inspects private factories on
 * every instantiation, and this class is on the hot path of every entry and
 * task. The value is assigned in the constructor instead.
 */
export class _LAILA_OBJECT extends BaseModel {
  static {
    define_private(this, { _creation_timestamp: PrivateAttr({ default: null }) });
  }

  /** Delegate to Pydantic, then stamp the creation timestamp. */
  constructor(data = {}) {
    super(data);
    if (data === SKIP_VALIDATION) return;
    this._creation_timestamp = _now_creation_timestamp();
  }

  /**
   * Cooperative no-op hook.
   *
   * Declared explicitly so that every class below this root has a
   * well-defined ``model_post_init`` to chain to via ``super``. Pydantic wraps
   * it to initialise ``__pydantic_private__`` first. Subclasses in mixin
   * diamonds (e.g. ``_LAILA_LOCALLY_ATOMIC_OBJECT`` +
   * ``_LAILA_IDENTIFIABLE_OBJECT``) must define their own hook and call
   * ``super`` -- Pydantic's auto-injected hook does not chain and would break
   * the diamond.
   */
  model_post_init(_context) {
    super.model_post_init(_context);
  }

  /**
   * ISO-8601 UTC timestamp of when this object was constructed.
   *
   * Read-only on purpose: it is runtime provenance, not configuration. (A
   * public setter would also make the CLI-capable machinery treat it as a
   * ``laila.args``-configurable field and mirror it into
   * ``laila.args.environment``.) Deserializers that need to restore a
   * persisted value assign the private ``_creation_timestamp`` attribute
   * directly.
   *
   * ``null`` only for instances built through paths that bypass the
   * constructor and have not yet restored the value.
   * @returns {string|null}
   */
  get creation_timestamp() {
    return this._creation_timestamp ?? null;
  }
}
