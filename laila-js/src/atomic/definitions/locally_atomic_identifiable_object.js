/**
 * Identifiable + locally-atomic mixin.
 *
 * Single-purpose convenience module: glues ``_LAILA_IDENTIFIABLE_OBJECT``
 * (identity, global ids, serialisation hooks) and
 * ``_LAILA_LOCALLY_ATOMIC_OBJECT`` (per-instance reentrant lock,
 * ``with_(self.atomic(), ...)``) into one base class so subclasses can pick up
 * both concerns by inheriting from a single name. This is the most-used base
 * class in laila -- pools, taskforces, comm protocols, futures and many more
 * inherit from it.
 */
import { finalize_model } from "../../_compat/pydantic.js";
import { _LAILA_IDENTIFIABLE_OBJECT } from "../../basics/definitions/identifiable_object.js";
import { LocallyAtomic } from "./locally_atomic_object.js";

/**
 * Identifiable object with a per-instance reentrant lock.
 *
 * Pure mixin: defines no extra fields or methods of its own. Inherits identity
 * machinery (uuid / scopes / evolution / global_id) from
 * ``_LAILA_IDENTIFIABLE_OBJECT`` and locking machinery (``lock`` / ``unlock``
 * / ``atomic``) from ``_LAILA_LOCALLY_ATOMIC_OBJECT``.
 *
 * The MRO is intentional: ``_LAILA_LOCALLY_ATOMIC_OBJECT`` first so its
 * ``model_config = ConfigDict({arbitrary_types_allowed: true})`` wins (allowing
 * private RLock storage), then ``_LAILA_IDENTIFIABLE_OBJECT`` for the identity
 * hooks.
 */
export class _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT extends LocallyAtomic(_LAILA_IDENTIFIABLE_OBJECT) {
  static {
    finalize_model(this);
  }
}
