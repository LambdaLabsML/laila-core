/**
 * Memory sub-package -- the routing brain behind ``memorize`` / ``remember`` / ``forget``.
 *
 * Layout:
 *
 * - ``schema`` -- the central memory class itself
 *   (``_LAILA_IDENTIFIABLE_CENTRAL_MEMORY``) plus ``Manifest``, the
 *   structured-references payload type.
 * - ``router`` -- the ``PoolRouter`` that resolves a request (by ``pool_id``,
 *   ``pool_nickname``, or affinity) to a concrete ``Pool``.
 * - ``record`` -- the ``Record`` envelope that decorates an ``Entry`` with
 *   recorder/borrower metadata before it is serialized to the pool.
 * - ``hint`` -- the ``MemoryHint`` knob that lets callers nudge routing
 *   towards a particular pool, purpose, or affinity.
 */

export { _LAILA_IDENTIFIABLE_CENTRAL_MEMORY } from "./schema/base.js";
export { Manifest } from "./schema/manifest.js";
export { _LAILA_IDENTIFIABLE_POOL_ROUTER } from "./router/pool_router.js";
export { Record } from "./record/record.js";
export { MemoryHint } from "./hint/hint.js";
