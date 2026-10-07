/**
 * Pool router -- resolves a memory request to a concrete ``Pool``.
 *
 * The router maintains three coupled data structures:
 *
 * - ``pools`` -- ``{global_id: Pool}`` -- the authoritative pool registry.
 * - ``pools_pq`` -- a ``(-affinity, global_id)`` priority queue used as the
 *   fallback ordering when nothing more specific is requested.
 * - ``pools_nicknames`` -- ``{nickname: global_id}`` -- a friendly-name
 *   index so callers can pass ``pool_nickname="cache"`` instead of a full
 *   UUID-bearing gid.
 *
 * Routing precedence used by ``PoolRouter.route``:
 *
 * 1. Explicit ``pool_id`` -- always wins.
 * 2. ``pool_nickname`` -- resolved through ``pools_nicknames``.
 * 3. (Future) ``affinity`` -- not yet implemented; falls through to (4).
 * 4. The ``DEFAULT`` nickname's pool, which is auto-registered if missing.
 */
import { lazy, register } from "../../../../_compat/lazy.js";
import { ConfigDict, Field, PrivateAttr, define_fields, define_private } from "../../../../_compat/pydantic.js";
import { dict_len, dict_set, getitem, tuple } from "../../../../_compat/pytypes.js";
import { PriorityQueue } from "../../../../_compat/queue.js";
import { CLICapable, CLIExempt } from "../../../../basics/definitions/cli_capable.js";
import { _LAILA_IDENTIFIABLE_OBJECT } from "../../../../basics/definitions/identifiable_object.js";
import { _DEFAULT_POOL_NICKNAME, _POOL_ROUTER_SCOPE } from "../../../../macros/strings.js";

/**
 * Selects the destination pool for ``memorize`` / ``remember`` / ``forget``
 * calls.
 *
 * Constructed automatically by every ``_LAILA_IDENTIFIABLE_CENTRAL_MEMORY``
 * and exposed as ``policy.central.memory.pool_router``. Users typically
 * interact with it indirectly through ``Policy.extend`` and the ``pool_id``
 * / ``pool_nickname`` kwargs on the high-level memory API, but it is also
 * usable directly for advanced multi-pool setups.
 */
export class _LAILA_IDENTIFIABLE_POOL_ROUTER extends CLICapable(_LAILA_IDENTIFIABLE_OBJECT) {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_private(this, {
      _scopes: PrivateAttr({ default_factory: () => [_POOL_ROUTER_SCOPE] }),
    });
    define_fields(this, {
      // ``_LAILA_IDENTIFIABLE_POOL`` is resolved by name at first validation
      // (``data/schema/base.js`` registers it), avoiding a cyclic import.
      pools: ["dict[str, _LAILA_IDENTIFIABLE_POOL] | None", CLIExempt({ default_factory: () => ({}) })],
      pools_pq: [[PriorityQueue, "None"], CLIExempt({ default_factory: () => new PriorityQueue() })],
      pools_nicknames: ["dict[str, str] | None", Field({ default_factory: () => ({}) })],
    });
  }

  /**
   * Auto-register a ``DefaultPool`` (in-memory) when no pools were supplied.
   *
   * This is what makes a fresh ``DefaultPolicy()`` immediately usable without
   * any pool wiring -- the in-memory default is good enough for tests and
   * quickstart, and users override it by calling ``Policy.extend`` with a
   * "real" pool (filesystem, S3, postgres, ...).
   */
  model_post_init(_context) {
    super.model_post_init(_context);
    if (dict_len(this.pools) === 0) {
      const { DefaultPool } = lazy("laila.macros.defaults");

      this.extend(new DefaultPool(), { affinity: 1, pool_nickname: _DEFAULT_POOL_NICKNAME });
    }
  }

  /**
   * Register a pool with optional affinity priority and nickname.
   *
   * Stores *pool* under its ``global_id`` in ``pools``, pushes a
   * ``(-affinity, gid)`` entry into the affinity priority queue (negated so
   * higher affinity sorts first), and -- if a nickname was given -- adds a
   * ``nickname -> gid`` entry to ``pools_nicknames``.
   *
   * Re-registering an existing nickname overwrites the previous binding;
   * re-registering an existing gid overwrites the pool instance. That makes
   * hot-swapping a pool implementation possible at the cost of users needing
   * to be careful about accidentally shadowing a gid.
   *
   * @param {any} pool The pool instance to register.
   * @param {{affinity?: number|null, pool_nickname?: string|null}} [opts]
   *   ``affinity``: routing priority (higher = preferred). Defaults to ``0``,
   *   which sorts last in the priority queue. ``pool_nickname``:
   *   human-readable alias for this pool, usable as the ``pool_nickname``
   *   kwarg on memory operations.
   */
  extend(pool, opts = {}) {
    let { affinity = null, pool_nickname = null } = opts;
    if (affinity === null) affinity = 0; // farthest away

    this.pools_pq.put(tuple([-affinity, pool.global_id]));
    dict_set(this.pools, pool.global_id, pool);
    if (pool_nickname !== null) dict_set(this.pools_nicknames, pool_nickname, pool.global_id);
  }

  /**
   * Resolve the destination pool for *entries*.
   *
   * Today the routing decision does not actually depend on *entries* -- it
   * is purely based on the explicit ``pool_id`` / ``pool_nickname`` arguments
   * and the default-pool fallback. The *entries* parameter is retained for
   * forward compatibility with future strategies (e.g. content-based or
   * affinity-based routing that picks per-entry destinations).
   *
   * @param {any[]} entries The entries (or entry ids) being routed. Currently unused.
   * @param {{pool_id?: string|null, pool_nickname?: string|null, affinity?: number|null}} [opts]
   *   ``pool_id``: explicit pool ``global_id`` -- highest precedence.
   *   ``pool_nickname``: nickname resolved via ``_route_by_nickname``.
   *   ``affinity``: reserved for future affinity-based routing.
   * @returns {any} The selected pool instance.
   * @throws {KeyError} If ``pool_id`` is given but not registered, or if
   *   ``pool_nickname`` does not resolve.
   */
  route(entries, opts = {}) {
    const { pool_id = null, pool_nickname = null } = opts;
    if (pool_id !== null) return getitem(this.pools, pool_id);
    return this._route_by_nickname({ pool_nickname });
  }

  /**
   * Resolve a pool by nickname, falling back to the default pool.
   *
   * When *pool_nickname* is ``null`` the fallback is the pool registered
   * under ``_DEFAULT_POOL_NICKNAME`` (always present because
   * ``model_post_init`` auto-registers one if missing).
   *
   * @throws {KeyError} If *pool_nickname* is set but not registered, or if
   *   the default nickname has been removed and no fallback exists.
   */
  _route_by_nickname(opts = {}) {
    const { pool_nickname = null } = opts;
    if (pool_nickname !== null) return getitem(this.pools, getitem(this.pools_nicknames, pool_nickname));
    return getitem(this.pools, getitem(this.pools_nicknames, _DEFAULT_POOL_NICKNAME));
  }
}

register("laila.policy.central.memory.router.pool_router", { _LAILA_IDENTIFIABLE_POOL_ROUTER });
