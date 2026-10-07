/**
 * Virtual base for every laila data container.
 *
 * A *data container* is any identifiable object that holds records and
 * hands entries back to whoever reads from it. The base class fixes the
 * shape of that contract without choosing a key type -- that is left to
 * the subclasses, which is exactly where the two concrete families
 * differ:
 *
 * - ``_LAILA_IDENTIFIABLE_POOL`` (``data/schema/base.js``) is a **map**:
 *   keys are entry ``global_id`` strings and the container is the
 *   persistence tier that ``memorize`` / ``remember`` / ``forget`` route to.
 * - ``MultiBuffer`` (``data/multibuffer/multibuffer.js``) is a **list**:
 *   keys are integer slot indices and the container is a fixed-capacity
 *   ring with independent read/write heads -- the proxy a microcontroller
 *   uses to stand in front of a camera's frame buffer.
 *
 * Both agree on the *value* contract: what goes in is a ``Record`` (an
 * entry plus provenance), and what comes out is the bare ``Entry``.
 *
 * Virtual in the laila sense
 * --------------------------
 * The class is instantiable as a type (Pydantic needs that to build the
 * schema, and tests use it to check identity plumbing), but the three
 * item-access methods raise ``NotImplementedError`` until a subclass
 * supplies them -- the same convention ``BotoPool._get_client`` and the
 * pool storage hooks follow.
 *
 * Item access (``container[key]`` / ``container[key] = v`` / ``key in
 * container`` / ``delete container[key]``) is routed to the Python protocol
 * methods through the ``indexable`` proxy returned by the constructor.
 */
import { NotImplementedError } from "../../_compat/errors.js";
import { register } from "../../_compat/lazy.js";
import { indexable } from "../../_compat/proxy.js";
import { ConfigDict, PrivateAttr, define_private } from "../../_compat/pydantic.js";
import { _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT } from "../../atomic/definitions/locally_atomic_identifiable_object.js";
import { CLICapable } from "../../basics/definitions/cli_capable.js";
import { _DATA_CONTAINER_SCOPE } from "../../macros/strings.js";

/**
 * Virtual base class for record-holding containers.
 *
 * Carries the identity contract (``global_id`` / uuid / nickname / scopes),
 * per-instance atomic locking, and the CLI parameter resolution tiers.
 * Declares -- but does not implement -- the item protocol every container
 * exposes:
 *
 * - ``container[key]`` returns the entry stored at *key* (or ``null`` when
 *   the slot is empty).
 * - ``container[key] = value`` stores *value* at *key*; subclasses decide
 *   how a bare entry becomes a record on the way in.
 * - ``container.empty()`` discards every stored record.
 *
 * The meaning of *key* is entirely up to the subclass.
 */
export class _LAILA_IDENTIFIABLE_DATA_CONTAINER extends CLICapable(_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT) {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_private(this, {
      _scopes: PrivateAttr({ default_factory: () => [_DATA_CONTAINER_SCOPE] }),
    });
  }

  /**
   * Property-key normaliser for ``container[key]``. Containers keyed by
   * strings (pools) keep the key as-is -- ``pool["0"]`` is the string ``"0"``
   * exactly as in Python. Integer-indexed containers (``MultiBuffer``)
   * override with ``sequence_key`` so ``mb[4]`` / ``mb[-1]`` arrive as ints.
   */
  static _index_key = (prop) => prop;

  constructor(data = {}) {
    super(data);
    // ``container[key]`` -> ``__getitem__`` (Python item access).
    return indexable(this, { index_key: new.target._index_key });
  }

  /** Return the entry stored at *key*. Subclasses define key semantics. */
  __getitem__(_key) {
    throw new NotImplementedError(
      `${this.constructor.name} does not implement __getitem__; ` + "_LAILA_IDENTIFIABLE_DATA_CONTAINER is a virtual base.",
    );
  }

  /** Store *value* at *key*. Subclasses define key and record semantics. */
  __setitem__(_key, _value) {
    throw new NotImplementedError(
      `${this.constructor.name} does not implement __setitem__; ` + "_LAILA_IDENTIFIABLE_DATA_CONTAINER is a virtual base.",
    );
  }

  /** Discard every record held by this container. */
  empty() {
    throw new NotImplementedError(`${this.constructor.name} does not implement empty; ` + "_LAILA_IDENTIFIABLE_DATA_CONTAINER is a virtual base.");
  }
}

register("laila.data.schema.data_container", { _LAILA_IDENTIFIABLE_DATA_CONTAINER });
