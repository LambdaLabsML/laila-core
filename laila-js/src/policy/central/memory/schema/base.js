/**
 * Central memory system -- memorize, remember, forget, and pool duplication.
 *
 * This module hosts ``_LAILA_IDENTIFIABLE_CENTRAL_MEMORY``, the backbone of
 * every policy's storage layer. It coordinates four moving parts:
 *
 * - the ``PoolRouter`` (one per central memory) that picks destinations,
 * - the *alpha pool* -- a privileged pool that acts as the default
 *   destination and the cache target for ``remember(..., persist=True)``,
 * - the per-pool ``TransformationSequence`` that defines the serialization
 *   pipeline (e.g. ``base64 -> zlib -> msgpack``),
 * - the internal task-force (``command.internal_taskforce``), where every
 *   per-entry coroutine is submitted for concurrent I/O, separate from the
 *   alpha task-force that runs user-submitted work.
 *
 * Two operation flavors live side-by-side:
 *
 * - *parallel-individual* paths (``_parallel_individual_record`` /
 *   ``_fetch`` / ``_delete``) -- one async coroutine per entry, suitable for
 *   any pool that exposes per-key I/O. The default for everything.
 * - *batch-accelerated* paths (``_batch_accelerated_*``) -- placeholders for
 *   pools that can multiplex many keys in a single round-trip (``COPY`` for
 *   postgres, ``mset`` for redis, etc.). Not yet implemented; the
 *   placeholders raise.
 *
 * The ``_remember_with_persist`` / ``_duplicate_pool`` helpers run entire
 * async pipelines on a daemon thread so the work proceeds concurrently
 * without polluting the caller's event loop.
 */
import * as asyncio from "../../../../_compat/asyncio.js";
import { contextmanager, with_async } from "../../../../_compat/contextlib.js";
import { KeyError, NotImplementedError, RuntimeError, TypeError as PyTypeError, ValueError } from "../../../../_compat/errors.js";
import { partial } from "../../../../_compat/functools.js";
import { lazy, register } from "../../../../_compat/lazy.js";
import { ConfigDict, Field, PrivateAttr, define_fields, define_private } from "../../../../_compat/pydantic.js";
import { repr } from "../../../../_compat/pyrepr.js";
import { PyFrozenSet, dict_get, dict_has, getitem, hasattr, sorted, str } from "../../../../_compat/pytypes.js";
import { Thread } from "../../../../_compat/threading.js";
import { CLICapable } from "../../../../basics/definitions/cli_capable.js";
import {
  _LAILA_IDENTIFIABLE_OBJECT,
  EVOLUTION_ATTRIBUTE,
  parse_global_id_attributes,
  split_global_id_attributes,
} from "../../../../basics/definitions/identifiable_object.js";
import { _LAILA_IDENTIFIABLE_POOL } from "../../../../data/schema/base.js";
import { CREATION_TIMESTAMP_ATTRIBUTE, _record_creation_timestamp } from "../../../../data/schema/pool_index.js";
import { Entry } from "../../../../entry/entry.js";
import { EntryState } from "../../../../entry/entry_state.js";
import { get_logger } from "../../../../logger/index.js";
import { _CENTRAL_MEMORY_SCOPE, _DEFAULT_POOL_NICKNAME } from "../../../../macros/strings.js";
import { ensure_list } from "../../../../utils/decorators/typecheck.js";
import { FutureStatus } from "../../command/schema/future/future/future_status.js";
import { GroupFuture } from "../../command/schema/future/future/group_future.js";
import { _RESOLVE_CHAIN, check_resolve_cycle } from "../../command/schema/parking.js";
import { ConcurrentPackageFuture } from "../../command/taskforce/thread_pool_executor/future.js";
import { Record } from "../record/record.js";
import { _LAILA_IDENTIFIABLE_POOL_ROUTER } from "../router/pool_router.js";

const _TERMINAL = [FutureStatus.FINISHED, FutureStatus.ERROR, FutureStatus.CANCELLED];

/** Settle every child future that has not reached a terminal status with *exc*. */
function _fail_pending(child_futures, exc) {
  for (const child_future of child_futures) {
    if (_TERMINAL.includes(child_future.status)) continue;
    child_future.exception = exc;
    child_future.result = null;
    child_future.status = FutureStatus.ERROR;
  }
}

/**
 * Central memory controller for storing, retrieving, and deleting entries.
 *
 * Owns a ``PoolRouter`` that decides which pool a given operation lands on,
 * plus an *alpha pool* selection that doubles as the default destination and
 * the cache target for cache-back reads.
 *
 * Three top-level public methods drive the system:
 *
 * - ``memorize`` -- write entries to the routed pool.
 * - ``remember`` -- read entries (with optional cache-back into the alpha
 *   pool).
 * - ``forget``   -- delete entries from the routed pool.
 *
 * All three are decorated with ``ensure_list`` so callers can pass either a
 * single entry / id or a list and get uniform list-shaped behavior
 * internally.
 */
export class _LAILA_IDENTIFIABLE_CENTRAL_MEMORY extends CLICapable(_LAILA_IDENTIFIABLE_OBJECT) {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_private(this, {
      _scopes: PrivateAttr({ default_factory: () => [_CENTRAL_MEMORY_SCOPE] }),
    });
    define_fields(this, {
      pool_router: [[_LAILA_IDENTIFIABLE_POOL_ROUTER, "None"], Field({ default: null })],
      alpha_pool: ["str | None", Field({ default: null })],
    });
  }

  /**
   * Wire a default ``PoolRouter`` and pick an alpha pool.
   *
   * Run order:
   *
   * 1. If no router was supplied, instantiate a ``DefaultPoolRouter`` (which
   *    itself auto-registers an in-memory ``DefaultPool`` under the
   *    ``DEFAULT`` nickname).
   * 2. If no ``alpha_pool`` was set, prefer the pool registered under
   *    ``DEFAULT``. If for any reason that nickname is missing, create a
   *    fresh ``DefaultPool`` and adopt it.
   *
   * After this hook returns, ``this.alpha_pool`` is guaranteed to resolve
   * through ``this.pool_router.pools[this.alpha_pool]``.
   */
  model_post_init(_context) {
    super.model_post_init(_context);
    if (this.pool_router === null || this.pool_router === undefined) {
      const { DefaultPoolRouter } = lazy("laila.macros.defaults");

      this.pool_router = new DefaultPoolRouter();
    }

    if (this.alpha_pool === null || this.alpha_pool === undefined) {
      if (dict_has(this.pool_router.pools_nicknames, _DEFAULT_POOL_NICKNAME)) {
        this.alpha_pool = getitem(this.pool_router.pools_nicknames, _DEFAULT_POOL_NICKNAME);
      } else {
        const { DefaultPool } = lazy("laila.macros.defaults");

        const alpha = new DefaultPool();
        this.pool_router.extend(alpha, { affinity: 1, pool_nickname: _DEFAULT_POOL_NICKNAME });
        this.alpha_pool = alpha.global_id;
      }
    }
  }

  /**
   * Forward pool registration to ``PoolRouter.extend``.
   *
   * Convenience pass-through so user code can write
   * ``policy.central.memory.extend(my_pool)`` without reaching into the
   * router.
   * @param {any} pool
   * @param {{affinity?: number|null, pool_nickname?: string|null}} [opts]
   */
  extend(pool, opts = {}) {
    const { affinity = null, pool_nickname = null } = opts;
    this.pool_router.extend(pool, { affinity, pool_nickname });
  }

  /**
   * Resolve a pool object, gid string, or nickname to a live pool instance.
   *
   * Lookup order
   * ------------
   * 1. *pool_ref* is already a ``_LAILA_IDENTIFIABLE_POOL`` -- returned as-is.
   * 2. *pool_ref* is a string that matches a registered pool gid -- returned
   *    from ``PoolRouter.pools``.
   * 3. *pool_ref* is a string that matches a registered nickname -- resolved
   *    via ``PoolRouter.pools_nicknames`` and then looked up in
   *    ``PoolRouter.pools``.
   *
   * @throws {KeyError} If the string matches neither a gid nor a nickname.
   * @throws {TypeError} If *pool_ref* is neither a pool nor a string.
   */
  _resolve_pool_ref(pool_ref) {
    if (pool_ref instanceof _LAILA_IDENTIFIABLE_POOL) return pool_ref;

    if (typeof pool_ref === "string") {
      if (dict_has(this.pool_router.pools, pool_ref)) return getitem(this.pool_router.pools, pool_ref);
      if (dict_has(this.pool_router.pools_nicknames, pool_ref)) {
        return getitem(this.pool_router.pools, getitem(this.pool_router.pools_nicknames, pool_ref));
      }
      throw new KeyError(`Pool '${pool_ref}' was not found.`);
    }

    throw new PyTypeError("pool_ref must be a pool object, pool id, or pool nickname.");
  }

  /**
   * Resolve the target pool for a memory op.
   *
   * Unifies the three ways a caller can name a pool:
   *
   * - ``pool`` -- a live pool *object*, a gid string, or a nickname,
   *   resolved directly through ``_resolve_pool_ref``. This is the path that
   *   supports *standalone* pools (e.g. an S3 / Redis pool instance the
   *   caller configured itself and handed in).
   * - ``pool_id`` / ``pool_nickname`` -- the classic router inputs, resolved
   *   through the ``PoolRouter`` (``pool_id`` > ``pool_nickname`` > default
   *   alpha).
   *
   * ``pool`` wins when supplied; otherwise the router decides.
   * @param {any} entries
   * @param {{pool?: any, pool_id?: string|null, pool_nickname?: string|null, affinity?: number|null}} [opts]
   */
  _route_pool(entries, opts = {}) {
    const { pool = null, pool_id = null, pool_nickname = null, affinity = null } = opts;
    if (pool !== null && pool !== undefined) return this._resolve_pool_ref(pool);
    return this.pool_router.route(entries, { pool_id, pool_nickname, affinity });
  }

  // TODO: need to make sure cross-borrowing does not lead to stall
  /**
   * Context manager that lends entries to the caller for the scope of a
   * ``with`` block. Not yet implemented.
   *
   * Planned semantics: the listed *keys* are pinned in the alpha pool for the
   * duration of the block and released on exit. With ``global_borrow=true``
   * the borrow is announced to peer policies so they avoid double-fetching
   * the same artefacts during the same window.
   */
  borrow(keys = null, global_borrow = false) {
    return _borrow_cm.call(this, keys, global_borrow);
  }

  /**
   * Copy every entry from *pool_src* into *pool_dest* asynchronously.
   *
   * Implements the ``cache <= origin`` operator on ``_LAILA_IDENTIFIABLE_POOL``
   * and is also the building block behind any "promote this snapshot to that
   * storage" workflow. Each entry is read from the source pool, then written
   * to the destination pool; both legs proceed concurrently up to
   * *inflight_max_entries* at a time.
   *
   * The return value is a ``GroupFuture`` whose ``future_ids`` reference one
   * ``ConcurrentPackageFuture`` per entry. Each per-entry future flips to
   * ``FINISHED`` when *that* entry has landed in *pool_dest*. Use
   * ``with_(laila.guarantee, ...)`` around the call to block until the entire
   * duplication is done.
   *
   * @param {any} pool_src Source pool, by instance, gid, or nickname.
   * @param {any} pool_dest Destination pool, same shapes accepted.
   * @param {{inflight_max_entries?: number}} [opts] Maximum number of
   *   concurrent (read, write) pairs in flight at any moment (default 4).
   *   Bounded via an ``asyncio.Semaphore``.
   * @returns {GroupFuture} A group future you can ``await`` or ``.wait()`` on.
   * @throws {ValueError} If *inflight_max_entries* is below 1.
   * @throws {RuntimeError} If the source pool would have returned ``null``
   *   from a ``remember`` (typical for the in-memory default pool which
   *   doesn't expose futures).
   */
  _duplicate_pool(pool_src, pool_dest, opts = {}) {
    const { inflight_max_entries = 4 } = opts;
    const { active_policy } = lazy("laila");

    if (inflight_max_entries < 1) throw new ValueError("duplicate_pool requires inflight_max_entries >= 1.");

    const src_pool = this._resolve_pool_ref(pool_src);
    const dest_pool = this._resolve_pool_ref(pool_dest);
    // Both legs are routed through central memory by pool gid, so an
    // unregistered pool would otherwise fail per entry with an opaque
    // KeyError on the gid. Check up front and say what to do instead.
    for (const [role, pool] of [
      ["source", src_pool],
      ["destination", dest_pool],
    ]) {
      if (!dict_has(this.pool_router.pools, pool.global_id)) {
        throw new KeyError(`${role} pool ${pool.global_id} is not registered with central memory; ` + "call laila.memory.extend(pool) first");
      }
    }
    const entry_ids = [...src_pool.keys()];

    const duplicate_futures = new Map();
    for (const entry_id of entry_ids) {
      duplicate_futures.set(
        entry_id,
        new ConcurrentPackageFuture({
          taskforce_id: active_policy.central.command.internal_taskforce,
          policy_id: active_policy.global_id,
          purpose: `duplicate_pool:${entry_id}`,
        }),
      );
    }

    const group_future = new GroupFuture({
      taskforce_id: active_policy.central.command.internal_taskforce,
      policy_id: active_policy.global_id,
      future_ids: [...duplicate_futures.values()].map((f) => f.global_id),
    });

    for (const child_future of duplicate_futures.values()) child_future.future_group_id = group_future.global_id;

    const semaphore = new asyncio.Semaphore(inflight_max_entries);

    const _duplicate_one = async (entry_id, child_future) => {
      try {
        await with_async(semaphore, async () => {
          child_future.status = FutureStatus.RUNNING;

          const remember_ref = this.remember(entry_id, { pool_id: src_pool.global_id });
          if (remember_ref === null || remember_ref === undefined) throw new RuntimeError("duplicate_pool requires a non-default source pool.");

          const remember_fut = getitem(active_policy.future_bank, remember_ref.global_id);
          let entry = await remember_fut;

          const memorize_ref = this.memorize(entry, { pool_id: dest_pool.global_id });
          if (memorize_ref !== null && memorize_ref !== undefined) {
            const memorize_fut = getitem(active_policy.future_bank, memorize_ref.global_id);
            await memorize_fut;
          }

          child_future.exception = null;
          child_future.result = entry_id;
          child_future.status = FutureStatus.FINISHED;
          entry = null;
        });
      } catch (exc) {
        child_future.exception = exc;
        child_future.result = null;
        child_future.status = FutureStatus.ERROR;
      }
    };

    const _duplicate_all = async () => {
      await asyncio.gather(...[...duplicate_futures].map(([entry_id, child_future]) => _duplicate_one(entry_id, child_future)), {
        return_exceptions: true,
      });
    };

    const _run_duplication_event_loop = () => {
      try {
        return asyncio.run(_duplicate_all());
      } catch (exc) {
        _fail_pending(duplicate_futures.values(), exc);
        return undefined;
      }
    };

    new Thread({ target: _run_duplication_event_loop, name: `DuplicatePool-${group_future.global_id}`, daemon: true }).start();

    return group_future;
  }

  /**
   * Persist *entries* to the routed pool.
   *
   * Routes through the ``PoolRouter`` (``pool_id`` > ``pool_nickname`` >
   * default), records a ``memorize`` log line, and dispatches to either the
   * batch-accelerated path (if the target pool supports it) or the per-entry
   * parallel path.
   *
   * @param {any} entries One or more entries to write. The ``ensure_list``
   *   decorator wraps a single entry in a 1-list before this body runs.
   * @param {{pool?: any, pool_id?: string|null, pool_nickname?: string|null, affinity?: number|null}} [opts]
   *   ``pool``: direct pool selector (highest priority) -- a live,
   *   *standalone* pool instance, a gid, or a nickname (see ``_route_pool``).
   *   ``pool_id``: explicit pool gid, classic router input.
   *   ``pool_nickname``: friendly name resolved through the router.
   *   ``affinity``: reserved for future affinity routing.
   * @returns {any} A future (single entry), a group future (many entries), or
   *   ``null`` if the pool path was synchronous and no future handle was
   *   needed.
   */
  memorize(entries, opts = {}) {
    const { pool: pool_ref = null, pool_id = null, pool_nickname = null, affinity = null } = opts;
    const { active_policy } = lazy("laila");

    const pool = this._route_pool(entries, { pool: pool_ref, pool_id, pool_nickname, affinity });

    try {
      get_logger().record_memorize({ entries, pool, policy: active_policy });
    } catch {
      // logging must never break a write
    }

    return this._record(entries, pool);
  }

  /**
   * Pick the right write path based on the pool's ``batch_accelerated`` flag.
   *
   * Pools that can multiplex many writes per round-trip
   * (``batch_accelerated=true``) are routed to ``_batch_accelerated_record``;
   * others use the per-entry parallel path.
   */
  _record(entries, pool) {
    if (pool.batch_accelerated) return this._batch_accelerated_record(entries, pool);
    return this._parallel_individual_record(entries, pool);
  }

  /**
   * Memorize each entry as a single per-entry async coroutine.
   *
   * Each entry's coroutine:
   *
   * 1. wraps the entry in a ``Record`` and runs ``serialize`` inline (pure
   *    CPU, no await needed),
   * 2. ``await``s the pool's ``_write_async`` for the actual storage
   *    round-trip.
   *
   * Submits one coroutine per entry to the internal taskforce. Returns the
   * single future for one entry, or a ``GroupFuture`` for many. Replaces the
   * previous two-stage ``ComplexFuture`` pipeline that double-queued each
   * leg.
   */
  _parallel_individual_record(entries, pool) {
    const { active_policy } = lazy("laila");

    const cmd = active_policy.central.command;
    const internal_id = cmd.internal_taskforce;
    const policy_gid = active_policy.global_id;
    const transformations = pool.transformations;

    const _memorize_one = async (e = null, p = pool, t = transformations, pgid = policy_gid) => {
      // A variable entry whose payload was re-assigned since its last
      // memorize becomes a new evolution (in place); an untouched one
      // is re-written under the same key.
      const bump = typeof e.bump_evolution_if_locally_modified === "function" ? e.bump_evolution_if_locally_modified : null;
      if (bump !== null) e.bump_evolution_if_locally_modified();
      const record = new Record({ entry: e, recorder: pgid });
      let blob = record.serialize(t);
      if (hasattr(blob, "data")) blob = blob.data;
      await p.write_async(e.global_id, blob);
      const mark = typeof e.mark_memorized === "function" ? e.mark_memorized : null;
      if (mark !== null) e.mark_memorized();
      return e.global_id;
    };

    // ``partial`` of a coroutine function is recognised as one by
    // ``ensure_coroutine_function`` and skips the sync-offload hop.
    const factories = entries.map((entry) => partial(_memorize_one, entry));
    return cmd.submit(factories, { taskforce_id: internal_id });
  }

  /** Batch-record entries (not yet implemented). */
  _batch_accelerated_record(_entries, _pool) {
    throw new NotImplementedError();
  }

  /**
   * Fetch entries from the routed pool, optionally caching them into the
   * alpha pool on the way back.
   *
   * When the routed source pool is *also* the alpha pool, or when
   * ``persist=false`` is requested, the fetched entries are simply returned
   * through the regular ``_fetch`` path. Otherwise, ``_remember_with_persist``
   * is used: each entry is fetched from the source pool *and* written to the
   * alpha pool, with the per-entry future only flipping to ``FINISHED`` once
   * both legs are done.
   *
   * That cache-back semantics is what lets ``with laila.guarantee:`` block
   * until the alpha pool has the entries -- subsequent reads from the alpha
   * pool are then guaranteed to be hot.
   *
   * @param {any} entry_ids Identifier(s) to fetch. Lists of live ``Entry``
   *   instances are accepted but the gid is what's actually used.
   * @param {{pool?: any, pool_id?: string|null, pool_nickname?: string|null, affinity?: number|null, persist?: boolean}} [opts]
   *   ``persist`` (default ``true``): whether to cache fetched entries into
   *   the alpha pool.
   * @returns {any} Future-like handle resolving to the entry or list of
   *   entries.
   */
  remember(entry_ids, opts = {}) {
    const { pool: pool_ref = null, pool_id = null, pool_nickname = null, affinity = null, persist = true } = opts;
    const { active_policy } = lazy("laila");

    const pool = this._route_pool(entry_ids, { pool: pool_ref, pool_id, pool_nickname, affinity });

    try {
      get_logger().record_remember({ entry_ids, pool, policy: active_policy });
    } catch {
      // logging must never break a read
    }

    if (!persist || pool.global_id === this.alpha_pool) return this._fetch(entry_ids, { pool });

    return this._remember_with_persist(entry_ids, { pool });
  }

  /**
   * Fetch entries from *pool* and then memorize them into the alpha pool.
   *
   * Returns a single future (for one id) or a ``GroupFuture`` (for many)
   * whose children only reach ``FINISHED`` once both the fetch and the
   * alpha-pool write have completed. Modeled after ``_duplicate_pool``.
   */
  _remember_with_persist(entry_ids, opts = {}) {
    const { pool } = opts;
    const { active_policy } = lazy("laila");

    const child_futures = new Map();
    for (const entry_id of entry_ids) {
      child_futures.set(
        entry_id,
        new ConcurrentPackageFuture({
          taskforce_id: active_policy.central.command.internal_taskforce,
          policy_id: active_policy.global_id,
          purpose: `remember_with_persist:${entry_id}`,
        }),
      );
    }

    let group_future = null;
    if (child_futures.size > 1) {
      group_future = new GroupFuture({
        taskforce_id: active_policy.central.command.internal_taskforce,
        policy_id: active_policy.global_id,
        future_ids: [...child_futures.values()].map((f) => f.global_id),
      });
      for (const child_future of child_futures.values()) child_future.future_group_id = group_future.global_id;
    }

    const alpha_pool_id = this.alpha_pool;

    const _run = async () => {
      try {
        for (const child_future of child_futures.values()) child_future.status = FutureStatus.RUNNING;

        const fetch_ref = this._fetch(entry_ids, { pool });
        if (fetch_ref === null || fetch_ref === undefined) {
          throw new RuntimeError("remember with persist requires a non-default source pool " + "that returns a future.");
        }
        // The fetch / memorize futures below are consumed right here
        // and never handed out, so this coroutine owns their release.
        let fetched;
        try {
          fetched = await fetch_ref;
        } finally {
          fetch_ref.release();
        }

        const entries = Array.isArray(fetched) ? fetched : [fetched];

        const ready_entries = entries.filter((e) => e.state === EntryState.READY);
        if (ready_entries.length) {
          const memorize_ref = this.memorize(ready_entries, { pool_id: alpha_pool_id });
          if (memorize_ref !== null && memorize_ref !== undefined) {
            try {
              await memorize_ref;
            } finally {
              memorize_ref.release();
            }
          }
        }

        const children = [...child_futures.values()];
        for (let i = 0; i < Math.min(entries.length, children.length); i++) {
          const child_future = children[i];
          child_future.exception = null;
          child_future.result = entries[i];
          child_future.status = FutureStatus.FINISHED;
        }
      } catch (exc) {
        _fail_pending(child_futures.values(), exc);
      }
    };

    // Carry only the resolve chain (not the caller's slot context) onto
    // the helper thread so cycle detection keeps working across it.
    const resolve_chain = _RESOLVE_CHAIN.get();

    const _run_event_loop = () => {
      _RESOLVE_CHAIN.set(resolve_chain);
      try {
        return asyncio.run(_run());
      } catch (exc) {
        _fail_pending(child_futures.values(), exc);
        return undefined;
      }
    };

    const thread_name =
      group_future !== null ? `RememberPersist-${group_future.global_id}` : `RememberPersist-${child_futures.values().next().value.global_id}`;
    new Thread({ target: _run_event_loop, name: thread_name, daemon: true }).start();

    if (group_future !== null) return group_future;

    return child_futures.values().next().value;
  }

  // ------------------------------------------------------------------
  // Attribute-based key resolution (``remember("ENTRY:x@...")``)
  // ------------------------------------------------------------------

  /**
   * Attributes ``remember`` knows how to search on. Anything else in the
   * ``@`` suffix of an entry reference is rejected up front.
   */
  static _SEARCH_ATTRIBUTES = new PyFrozenSet([EVOLUTION_ATTRIBUTE, CREATION_TIMESTAMP_ATTRIBUTE]);

  /** Evolution encoded in a storage key; ``-1`` for a constant (no ``@``). */
  static _key_evolution(key) {
    const at = key.indexOf("@");
    const tail = at < 0 ? "" : key.slice(at + 1);
    if (!tail) return -1;
    const raw = parse_global_id_attributes(tail)[EVOLUTION_ATTRIBUTE] ?? null;
    return raw !== null && /^\d+$/.test(raw) ? parseInt(raw, 10) : -1;
  }

  // Kept as a static alias for callers that used to find it here.
  static _record_creation_timestamp = _record_creation_timestamp;

  /** *pool* followed by its ``_proxy_to`` upstream tiers, front to back. */
  static _pool_chain(pool) {
    const chain = [];
    const seen = new Set();
    let cur = pool;
    while (cur !== null && cur !== undefined && !seen.has(cur)) {
      chain.push(cur);
      seen.add(cur);
      cur = cur._proxy_to ?? null;
    }
    return chain;
  }

  /**
   * Turn an entry *reference* into the storage key to read.
   *
   * The reference is a full global id, optionally carrying search attributes
   * after ``@``:
   *
   * - ``LAILA:ENTRY:<uuid>@evolution=N`` (``N >= 0``) -- exact key, no lookup.
   * - ``LAILA:ENTRY:<uuid>`` -- the exact key when it exists locally (a
   *   constant); otherwise the stored evolution with the highest counter
   *   across *pool* and its proxy chain.
   * - ``LAILA:ENTRY:<uuid>@evolution=-k`` -- the k-th evolution from the end
   *   (``-1`` = latest); a constant key counts as the lowest.
   * - ``LAILA:ENTRY:<uuid>@creation_timestamp=<iso>`` (optionally with
   *   ``evolution``) -- the stored evolution whose entry creation_timestamp
   *   equals the given stamp exactly.
   *
   * Resolution order per tier of the proxy chain:
   *
   * 1. the tier's ``_resolve_indexed`` (its ``PoolIndex``); a hit is
   *    **validated** with ``_exists_async`` and, when the key turns out to be
   *    gone, the shard is invalidated and the search continues;
   * 2. only when no tier's index answers: candidate keys from
   *    ``_search_keys`` / ``_candidate_keys``, and for timestamp queries the
   *    candidate records are read on the tier that holds them (no
   *    write-back); the matching raw record is returned so the caller does
   *    not read it twice.
   *
   * @returns {Promise<[string, any|null]>} ``[storage_key, raw_record_or_null]``.
   * @throws {ValueError} On an unsupported search attribute.
   * @throws {KeyError} When no stored evolution satisfies the query.
   */
  async _resolve_entry_key_async(pool, eid) {
    // Shorthands ("ENTRY:nick@evolution=3") are normally expanded by
    // ``laila.remember``; expand here too so direct callers of central
    // memory get the same behaviour.
    eid = Entry.resolve_global_id(str(eid));
    const [base, attrs] = split_global_id_attributes(eid);
    const unknown = Object.keys(attrs).filter((a) => !this.constructor._SEARCH_ATTRIBUTES.has(a));
    if (unknown.length) {
      throw new ValueError(
        `Unsupported search attribute(s) ${repr(sorted(unknown))} in ${repr(eid)}; ` + `supported: ${repr(sorted(this.constructor._SEARCH_ATTRIBUTES))}`,
      );
    }
    const raw_evolution = attrs[EVOLUTION_ATTRIBUTE] ?? null;
    const evolution = raw_evolution !== null ? parseInt(raw_evolution, 10) : null;
    const creation_timestamp = attrs[CREATION_TIMESTAMP_ATTRIBUTE] ?? null;
    const chain = this.constructor._pool_chain(pool);
    const _key_evolution = this.constructor._key_evolution;

    // Fast paths: a non-negative explicit evolution is an exact key; a
    // bare id that exists as-is (a constant) needs no lookup.
    if (creation_timestamp === null) {
      if (evolution !== null && evolution >= 0) return [eid, null];
      if (evolution === null && (await pool._exists_async(base))) return [base, null];
    }

    // 1. Index-first, tier by tier, validating every hit. A hit whose key
    //    is gone (external delete, lost race) is removed from the shard --
    //    which repairs the persisted index -- and the tier is asked again.
    for (const tier of chain) {
      for (let _attempt = 0; _attempt < 64; _attempt++) {
        const key = await asyncio.to_thread(() => tier._resolve_indexed(base, attrs));
        if (key === null || key === undefined) break;
        if (await tier._exists_async(key)) return [key, null];
        await asyncio.to_thread(() => tier.index.remove(key));
      }
    }

    // 2. Scan fallback: candidate (tier, key) pairs across the chain.
    let candidates = [];
    for (const tier of chain) {
      let keys = await asyncio.to_thread(() => tier._search_keys(base, attrs));
      if (keys === null || keys === undefined) keys = await asyncio.to_thread(() => tier._candidate_keys(base));
      for (const key of keys) {
        if (key !== base && !key.startsWith(`${base}@`)) continue;
        if (evolution !== null && evolution >= 0 && _key_evolution(key) !== evolution) continue;
        candidates.push([tier, key]);
      }
    }

    if (evolution !== null && evolution < 0 && candidates.length) {
      // k-th from the end over the sorted, de-duplicated evolutions
      // (constants rank lowest).
      const ranked = sorted(new Set(candidates.map(([, k]) => _key_evolution(k))));
      const idx = ranked.length + evolution;
      if (idx < 0) candidates = [];
      else {
        const wanted = ranked[idx];
        candidates = candidates.filter(([, k]) => _key_evolution(k) === wanted);
      }
    }

    if (creation_timestamp === null) {
      if (!candidates.length) throw new KeyError(`Entry ${eid} not found in pool ${pool.global_id}`);
      if (evolution === null) {
        // Prefer an exact (constant) key; otherwise the highest evolution.
        for (const [, key] of candidates) if (key === base) return [key, null];
      }
      let best = candidates[0];
      for (const tk of candidates) if (_key_evolution(tk[1]) > _key_evolution(best[1])) best = tk;
      return [best[1], null];
    }

    // Time-based search: inspect each candidate record's creation_timestamp.
    const matches = [];
    for (const [tier, key] of candidates) {
      const raw = await tier._read_async(key);
      if (raw === null || raw === undefined) continue;
      if (_record_creation_timestamp(raw) === creation_timestamp) matches.push([_key_evolution(key), key, raw]);
    }
    if (!matches.length) {
      throw new KeyError(`No evolution of ${base} with creation_timestamp=${repr(creation_timestamp)} ` + `in pool ${pool.global_id}`);
    }
    let best = matches[0];
    for (const m of matches) if (m[0] > best[0]) best = m;
    return [best[1], best[2]];
  }

  /**
   * Dispatch fetching to batch or non-batch path based on pool capability.
   * @param {string[]} entry_ids
   * @param {{pool?: any, borrow?: boolean}} [opts]
   */
  _fetch(entry_ids, opts = {}) {
    const { pool = null, borrow = false } = opts;
    if (borrow) throw new NotImplementedError();
    if (pool.batch_accelerated) return this._batch_accelerated_fetch(entry_ids, { pool });
    return this._parallel_individual_fetch(entry_ids, { pool });
  }

  /**
   * Fetch and deserialize each entry via a single per-entry coroutine.
   *
   * Each entry's coroutine:
   *
   * 1. ``await``s the pool's ``_read_async`` for the storage round-trip,
   * 2. ``await``s ``Record._build_async`` to deserialize and recursively
   *    hydrate any nested entries (which themselves may ``await`` further
   *    fetches on the same loop).
   *
   * Submits one coroutine per entry to the internal taskforce and returns
   * the single future (one entry) or a ``GroupFuture`` (many). Replaces the
   * previous two-stage ``ComplexFuture`` pipeline.
   */
  _parallel_individual_fetch(entry_ids, opts = {}) {
    const { pool = null } = opts;
    const { active_policy } = lazy("laila");

    const cmd = active_policy.central.command;
    const internal_id = cmd.internal_taskforce;
    check_resolve_cycle(...entry_ids);

    const _remember_one = async (eid = null, p = pool) => {
      let [key, raw] = await this._resolve_entry_key_async(p, eid);
      // Extend the resolve chain with the *concrete* key so a floating
      // reference (``@evolution=-1``) cannot hide a cycle through a
      // nested manifest / constitution.
      const token = _RESOLVE_CHAIN.set(Object.freeze([..._RESOLVE_CHAIN.get(), key]));
      try {
        if (raw === null || raw === undefined) raw = await p._read_through_async(key);
        if (raw === null || raw === undefined) throw new KeyError(`Entry ${eid} not found in pool ${p.global_id}`);
        const record = await Record._build_async(raw);
        return getitem(record, "entry");
      } finally {
        _RESOLVE_CHAIN.reset(token);
      }
    };

    const factories = entry_ids.map((entry_id) => partial(_remember_one, entry_id));
    return cmd.submit(factories, { taskforce_id: internal_id });
  }

  /** Batch-accelerated fetch path (not yet implemented). */
  _batch_accelerated_fetch(_keys, _opts = {}) {
    throw new NotImplementedError();
  }

  // ------------------------------------------------------------------
  // Direct-await resolver (internal, self-consumed reads)
  // ------------------------------------------------------------------

  /**
   * Concurrency bound for ``_read_entries_async``.
   *
   * Mirrors what the per-entry submit path could have in flight on the
   * internal taskforce (``num_workers * max_async_per_thread``), so switching
   * to the direct resolver does not change the pressure put on a pool
   * backend.
   */
  _direct_read_concurrency() {
    const { active_policy } = lazy("laila");

    const cmd = active_policy.central.command;
    const tf = dict_get(cmd.taskforces, cmd.internal_taskforce, null);
    const workers = (tf !== null && tf !== undefined ? (tf.num_workers ?? 1) : 1) || 1;
    const per_thread = (tf !== null && tf !== undefined ? (tf.max_async_per_thread ?? 64) : 64) || 64;
    return Math.max(1, Math.trunc(workers) * Math.trunc(per_thread));
  }

  /**
   * Fetch *entry_ids* by ``await``ing the pool directly -- no per-entry
   * futures.
   *
   * This is the direct-await counterpart of ``remember`` for callers that
   * consume the entries themselves and never need a per-entry handle
   * (``Manifest.realized`` / ``async_realized``). Semantics match
   * ``remember``:
   *
   * - the pool is routed the same way (``pool`` > ``pool_id`` /
   *   ``pool_nickname`` > router default);
   * - every id is checked against the current resolve chain and each read
   *   extends the chain, so cyclic constitutions still raise
   *   ``CyclicDependencyError``;
   * - with ``persist=true`` and a non-alpha source pool, the READY entries
   *   are cached into the alpha pool before returning (awaiting -- and
   *   releasing -- the single memorize group).
   *
   * Reads run concurrently under an ``asyncio.Semaphore`` (default: the
   * internal taskforce's slot capacity). Nested fetches triggered while
   * building an entry (nested manifests, constitutions) still go through
   * ``command.submit`` and park the surrounding slot exactly as before.
   *
   * Returns the entries in the order of *entry_ids*. Raises ``KeyError`` if
   * any id is missing from the routed pool.
   * @param {string[]} entry_ids
   * @param {{pool?: any, pool_id?: string|null, pool_nickname?: string|null, persist?: boolean, max_concurrency?: number|null}} [opts]
   */
  async _read_entries_async(entry_ids, opts = {}) {
    const { pool = null, pool_id = null, pool_nickname = null, persist = true, max_concurrency = null } = opts;

    entry_ids = [...entry_ids];
    if (!entry_ids.length) return [];

    const routed = this._route_pool(entry_ids, { pool, pool_id, pool_nickname });
    if (routed.batch_accelerated ?? false) {
      // Same limitation as _fetch(): batch-accelerated pools have no
      // implementation yet.
      throw new NotImplementedError();
    }

    try {
      const { active_policy } = lazy("laila");

      get_logger().record_remember({ entry_ids, pool: routed, policy: active_policy });
    } catch {
      // logging must never break a read
    }

    check_resolve_cycle(...entry_ids);
    const limit = max_concurrency !== null ? max_concurrency : this._direct_read_concurrency();
    const sem = new asyncio.Semaphore(Math.max(1, limit));

    const _one = async (eid) =>
      with_async(sem, async () => {
        let [key, raw] = await this._resolve_entry_key_async(routed, eid);
        const token = _RESOLVE_CHAIN.set(Object.freeze([..._RESOLVE_CHAIN.get(), key]));
        try {
          if (raw === null || raw === undefined) raw = await routed._read_through_async(key);
          if (raw === null || raw === undefined) throw new KeyError(`Entry ${eid} not found in pool ${routed.global_id}`);
          const record = await Record._build_async(raw);
          return getitem(record, "entry");
        } finally {
          _RESOLVE_CHAIN.reset(token);
        }
      });

    // Pass factories so each read runs as its own Task with a *copied*
    // context (``asyncio.gather`` wraps coroutines in Tasks): the per-child
    // ``_RESOLVE_CHAIN`` extension must not leak between concurrent reads.
    const entries = [...(await asyncio.gather(...entry_ids.map((eid) => () => _one(eid))))];

    if (persist && routed.global_id !== this.alpha_pool) {
      const ready = entries.filter((e) => e.state === EntryState.READY);
      if (ready.length) {
        const ref = this.memorize(ready, { pool_id: this.alpha_pool });
        if (ref !== null && ref !== undefined) {
          try {
            await ref;
          } finally {
            ref.release();
          }
        }
      }
    }

    return entries;
  }

  /**
   * Run ``_read_entries_async`` as *one* task on the internal taskforce.
   *
   * Returns a single ``Future`` whose ``.data`` is the list of entries (one
   * future for the whole batch instead of one per entry). The caller owns
   * the future: ``wait()`` / ``await`` it, read ``.data`` and ``release()``
   * it.
   * @param {string[]} entry_ids
   * @param {{pool?: any, pool_id?: string|null, pool_nickname?: string|null, persist?: boolean}} [opts]
   */
  _read_entries_direct(entry_ids, opts = {}) {
    const { pool = null, pool_id = null, pool_nickname = null, persist = true } = opts;
    const { active_policy } = lazy("laila");

    const cmd = active_policy.central.command;
    const coro_factory = partial(this._read_entries_async.bind(this), [...entry_ids], { pool, pool_id, pool_nickname, persist });
    coro_factory.__coroutinefunction__ = true;
    return cmd.submit([coro_factory], { taskforce_id: cmd.internal_taskforce });
  }

  // ------------------------------------------------------------------
  // Cross-peer (over-the-wire) memorize / remember
  // ------------------------------------------------------------------

  /**
   * Reconstruct wire-serialized entries and memorize them locally.
   *
   * Invoked over a transport by a peer's ``laila.memorize`` ``dst_policy=``
   * call. Each item in *serialized_entries* is the JSON-safe form produced by
   * ``Entry.serialize(transformation_base64)`` on the sender; here it is
   * rebuilt into a live ``Entry``, written through the normal ``memorize``
   * path, and the call blocks until the write completes so the caller gets a
   * definite list of stored gids back.
   *
   * ``pool`` (a gid or nickname *string* resolvable on this policy) wins over
   * the classic ``pool_id`` / ``pool_nickname``. A standalone pool *object*
   * can never cross the wire, so only string selectors are accepted here.
   * @param {any[]} serialized_entries
   * @param {{pool?: string|null, pool_id?: string|null, pool_nickname?: string|null}} [opts]
   * @returns {string[]}
   */
  _remote_memorize(serialized_entries, opts = {}) {
    const { pool = null, pool_id = null, pool_nickname = null } = opts;
    const { build_by_scope } = lazy("laila.entry.constitution.build_maps");

    const entries = serialized_entries.map((s) => build_by_scope(s, { asynchronous: false }));
    const future = this.memorize(entries, { pool, pool_id, pool_nickname });
    if (future !== null && future !== undefined && hasattr(future, "wait")) future.wait(60);
    return entries.map((e) => e.global_id);
  }

  /**
   * Fetch entries locally and return them wire-serialized.
   *
   * The inverse of ``_remote_memorize``: invoked over a transport by a peer's
   * ``laila.remember`` ``dst_policy=`` call. Resolves the entries from the
   * local pool, then returns each as the JSON-safe
   * ``Entry.serialize(transformation_base64)`` form so the caller can rebuild
   * real ``Entry`` objects on its side.
   * @param {string[]} entry_ids
   * @param {{pool?: string|null, pool_id?: string|null, pool_nickname?: string|null}} [opts]
   * @returns {any[]}
   */
  _remote_remember(entry_ids, opts = {}) {
    const { pool = null, pool_id = null, pool_nickname = null } = opts;
    const { transformation_base64 } = lazy("laila.entry");

    const future = this.remember(entry_ids, { pool, pool_id, pool_nickname, persist: false });
    const result = hasattr(future, "wait") ? future.wait(60) : future;
    const entries = Array.isArray(result) ? result : [result];
    return entries.map((e) => e.serialize(transformation_base64));
  }

  /**
   * Delete *entry_ids* from a local pool on behalf of a peer.
   *
   * Invoked over a transport by a peer's ``laila.forget`` ``policy=`` call.
   * Blocks until the delete completes so the caller gets a definite
   * acknowledgement (the list of gids it asked to delete).
   * @param {any[]} entry_ids
   * @param {{pool?: string|null, pool_id?: string|null, pool_nickname?: string|null}} [opts]
   * @returns {string[]}
   */
  _remote_forget(entry_ids, opts = {}) {
    const { pool = null, pool_id = null, pool_nickname = null } = opts;
    const ids = entry_ids.map((x) => (hasattr(x, "global_id") ? x.global_id : str(x)));
    const future = this.forget(ids, { pool, pool_id, pool_nickname });
    if (future !== null && future !== undefined && hasattr(future, "wait")) future.wait(60);
    return ids;
  }

  // ------------------------------------------------------------------
  // 3-party relays (source-side orchestration)
  //
  // These run on the *source* policy B when an orchestrator A issues a
  // ``src_policy=B, dst_policy=C`` transfer. A reaches B over the A<->B
  // link and asks B to move data over B's own B<->C link -- A never
  // brokers the B<->C connection.
  // ------------------------------------------------------------------

  /**
   * Push entries this policy holds to *dst_policy* (push relay).
   *
   * Runs on the source policy B: read *entry_ids* from B's own ``src_pool``
   * and memorize them into ``dst_policy`` C's ``dst_pool`` over B's existing
   * peer link to C. Blocks until the push completes and returns the stored
   * gids.
   *
   * Raises a clear error if B is not peered to ``dst_policy``.
   * @param {any[]} entry_ids
   * @param {{src_pool?: string|null, dst_policy?: string|null, dst_pool?: string|null, comm?: string|null}} [opts]
   * @returns {string[]}
   */
  _relay_memorize(entry_ids, opts = {}) {
    const { src_pool = null, dst_policy = null, dst_pool = null, comm = null } = opts;
    const laila = lazy("laila");

    const ids = entry_ids.map((x) => (hasattr(x, "global_id") ? x.global_id : str(x)));
    const fetched = laila.remember(ids, { dst_pool: src_pool, persist: false });
    const result = hasattr(fetched, "wait") ? fetched.wait(60) : fetched;
    const entries = Array.isArray(result) ? result : [result];

    const pushed = laila.memorize(entries, { dst_policy, dst_pool, comm });
    if (pushed !== null && pushed !== undefined && hasattr(pushed, "wait")) pushed.wait(60);
    return entries.map((e) => e.global_id);
  }

  /**
   * Pull entries from *dst_policy* into this policy (pull relay).
   *
   * Runs on the source policy B: fetch *entry_ids* from ``dst_policy`` C's
   * ``dst_pool`` over B's own B<->C link, store them into B's ``src_pool``
   * (or B's alpha pool), and return the stored gids.
   *
   * Raises a clear error if B is not peered to ``dst_policy``.
   * @param {any[]} entry_ids
   * @param {{dst_policy?: string|null, dst_pool?: string|null, src_pool?: string|null, comm?: string|null, persist?: boolean}} [opts]
   * @returns {string[]}
   */
  _relay_remember(entry_ids, opts = {}) {
    const { dst_policy = null, dst_pool = null, src_pool = null, comm = null, persist = true } = opts;
    const laila = lazy("laila");

    const ids = entry_ids.map((x) => (hasattr(x, "global_id") ? x.global_id : str(x)));
    const fetched = laila.remember(ids, { dst_policy, dst_pool, comm, persist: false });
    const result = hasattr(fetched, "wait") ? fetched.wait(60) : fetched;
    const entries = Array.isArray(result) ? result : [result];

    const stored = persist ? laila.memorize(entries, { dst_pool: src_pool }) : null;
    if (stored !== null && stored !== undefined && hasattr(stored, "wait")) stored.wait(60);
    return entries.map((e) => e.global_id);
  }

  /**
   * Delete *entry_ids* from the routed pool.
   *
   * Parameters mirror ``memorize`` / ``remember``. The deletion is
   * *pool-local*: it only affects the routed pool, not any other pool that
   * may hold a copy. To remove an entry from every registered pool, iterate
   * over ``this.pool_router.pools`` values and call ``forget`` per pool with
   * an explicit ``pool_id``.
   *
   * @param {any} entry_ids
   * @param {{pool?: any, pool_id?: string|null, pool_nickname?: string|null, affinity?: number|null}} [opts]
   * @returns {any} Future-like handle resolving when deletion finishes.
   */
  forget(entry_ids, opts = {}) {
    const { pool: pool_ref = null, pool_id = null, pool_nickname = null, affinity = null } = opts;
    const pool = this._route_pool(entry_ids, { pool: pool_ref, pool_id, pool_nickname, affinity });

    try {
      const { active_policy } = lazy("laila");

      get_logger().record_forget({ entry_ids, pool, policy: active_policy });
    } catch {
      // logging must never break a delete
    }

    return this._delete(entry_ids, pool);
  }

  /** Dispatch deletion to batch or non-batch path based on pool capability. */
  _delete(entry_ids, pool) {
    if (pool.batch_accelerated) return this._batch_accelerated_delete(entry_ids, pool);
    return this._parallel_individual_delete(entry_ids, pool);
  }

  /** Batch-delete entries (not yet implemented). */
  _batch_accelerated_delete(_entry_ids, _pool) {
    throw new NotImplementedError();
  }

  /** Delete each entry via a single per-entry async coroutine. */
  _parallel_individual_delete(entry_ids, pool) {
    const { active_policy } = lazy("laila");

    const cmd = active_policy.central.command;
    const internal_id = cmd.internal_taskforce;

    const _delete_one = async (eid = null, p = pool) => {
      let key = str(eid);
      // A negative evolution ("@evolution=-1" = latest) must be resolved
      // to the concrete key; an evolution-less reference stays exact.
      const [, attrs] = split_global_id_attributes(key);
      const raw_evolution = attrs[EVOLUTION_ATTRIBUTE] ?? null;
      if (raw_evolution !== null && raw_evolution.startsWith("-")) [key] = await this._resolve_entry_key_async(p, key);
      await p.delete_async(key);
      return key;
    };

    const factories = entry_ids.map((entry_id) => partial(_delete_one, entry_id));
    return cmd.submit(factories, { taskforce_id: internal_id });
  }
}

const _borrow_cm = contextmanager(function* _borrow(keys, _global_borrow) {
  if (keys === null || keys === undefined) keys = [];
  throw new NotImplementedError();
});

// ``@ensure_list("entries")`` / ``@ensure_list("entry_ids")``
{
  const proto = _LAILA_IDENTIFIABLE_CENTRAL_MEMORY.prototype;
  proto.memorize = ensure_list("entries")(proto.memorize);
  proto.remember = ensure_list("entry_ids")(proto.remember);
  proto.forget = ensure_list("entry_ids")(proto.forget);
}

register("laila.policy.central.memory.schema.base", { _LAILA_IDENTIFIABLE_CENTRAL_MEMORY });
