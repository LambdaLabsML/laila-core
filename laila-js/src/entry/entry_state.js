/** Enumeration of lifecycle states for an Entry. */
import { Enum } from "../_compat/enum.js";
import { register } from "../_compat/lazy.js";

/**
 * Lifecycle state of an ``Entry``.
 *
 * Members
 * -------
 * READY
 *     Data is available and fully materialised.
 * POOLED
 *     Entry is stored in a pool.
 * POOLING
 *     Entry is being transferred to a pool.
 * STAGED
 *     Entry is staged but data may not yet be materialised.
 * STALE
 *     Entry data is out of date.
 * NA
 *     Not applicable. Used by Entry subclasses whose lifecycle states are
 *     meaningless (e.g. ``Manifest``). Only ``Entry`` may hold non-``NA``
 *     states.
 *
 * Values follow ``enum.auto()`` (1-based, declaration order).
 */
export const EntryState = Enum("EntryState", {
  READY: 1,
  POOLED: 2,
  POOLING: 3,
  STAGED: 4,
  STALE: 5,
  NA: 6,
});

register("laila.entry.entry_state", { EntryState });
