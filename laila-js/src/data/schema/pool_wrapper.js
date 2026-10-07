/**
 * PoolWrapper -- lightweight proxy binding a pool to a manifest for scoped
 * copies.
 *
 * Returned by ``_LAILA_IDENTIFIABLE_POOL.__getitem__`` when the key is a
 * ``Manifest``. The wrapper exists so that the ``<=`` operator can copy
 * *only the entries listed by the manifest* between two pools, instead of
 * copying everything::
 *
 *     my_future = pool_dest[manifest] <= pool_src[manifest]
 *     // JS: pool_dest.__getitem__(manifest).__le__(pool_src.__getitem__(manifest))
 *
 * How the copy works
 * ------------------
 * The right-hand side selects the source: ``pool_src[manifest]`` returns a
 * ``PoolWrapper`` over ``pool_src``. The left-hand side selects the
 * destination the same way. ``__le__`` then walks the manifest's leaf
 * entries, fetching each one from the source pool via ``memory.remember``
 * (which handles serialisation and proxy chains for us), then storing it in
 * the destination pool via ``memory.memorize``. The whole copy runs
 * concurrently with a 4-way semaphore on a daemon thread, returning a
 * ``GroupFuture`` whose children correspond to per-entry copy operations.
 *
 * Concurrency model
 * -----------------
 * ``PoolWrapper`` itself holds no lock -- all I/O is delegated to the wrapped
 * pool's existing public methods, which already take care of their own
 * atomic-lock coverage. The ``<=`` runner builds a ``GroupFuture`` of
 * per-entry ``ConcurrentPackageFuture`` children that the active policy's
 * future bank can observe like any other group future.
 */
import * as asyncio from "../../_compat/asyncio.js";
import { with_async } from "../../_compat/contextlib.js";
import { RuntimeError } from "../../_compat/errors.js";
import { lazy, register } from "../../_compat/lazy.js";
import { NotImplemented, getitem } from "../../_compat/pytypes.js";
import { Thread } from "../../_compat/threading.js";
import { FutureStatus } from "../../policy/central/command/schema/future/future/future_status.js";
import { GroupFuture } from "../../policy/central/command/schema/future/future/group_future.js";
import { ConcurrentPackageFuture } from "../../policy/central/command/taskforce/thread_pool_executor/future.js";

/**
 * Manifest-scoped view over a pool, used as the LHS / RHS of ``<=`` copies.
 *
 * A wrapper is conceptually "this pool, but only the entries listed by
 * *manifest*". The class itself is intentionally minimal: just two slots
 * (``pool`` and ``manifest``) and one operator (``<=``) that performs the
 * actual cross-pool copy.
 *
 * No internal locking; every pool method called from inside ``<=`` already
 * takes the pool's own lock.
 */
export class PoolWrapper {
  /**
   * @param {any} pool The bound pool instance. Must be reachable from the
   *   active local policy's central memory.
   * @param {any} manifest The manifest whose ``global_id`` leaves define the
   *   entries in scope. Both the source and destination wrapper are expected
   *   to use the same manifest in a copy operation.
   */
  constructor(pool, manifest) {
    this.pool = pool;
    this.manifest = manifest;
    Object.seal(this); // ``__slots__ = ("manifest", "pool")``
  }

  /**
   * ``self <= other`` -- copy *other*'s manifest entries into this wrapper's
   * pool.
   *
   * Spawns a daemon thread running an asyncio loop that, for each entry id
   * in the manifest, ``remember``s the entry from the source pool and
   * ``memorize``s it into the destination pool. A four-way semaphore caps
   * the in-flight copy concurrency.
   *
   * Each per-entry copy populates a corresponding ``ConcurrentPackageFuture``
   * in a parent ``GroupFuture`` so callers can wait on the whole batch with
   * the usual future API.
   *
   * @param {PoolWrapper} other Source pool + manifest binding. The manifest
   *   is read from *self* (both wrappers should carry the same manifest in a
   *   normal copy).
   * @returns {any} ``GroupFuture`` -- aggregate future whose children resolve
   *   as each entry finishes copying.
   * @throws {RuntimeError} If the source pool's ``remember`` returns ``null``
   *   (which happens when the active policy's *default* pool is used as the
   *   source -- manifest copies require a non-default source).
   */
  __le__(other) {
    if (!(other instanceof PoolWrapper)) return NotImplemented;

    const laila = lazy("laila");

    const active_policy = laila.get_active_policy();

    const entry_ids = [...this.manifest];

    const duplicate_futures = new Map();
    for (const entry_id of entry_ids) {
      duplicate_futures.set(
        entry_id,
        new ConcurrentPackageFuture({
          taskforce_id: active_policy.central.command.internal_taskforce,
          policy_id: active_policy.global_id,
          purpose: `manifest_copy:${entry_id}`,
        }),
      );
    }

    const group_future = new GroupFuture({
      taskforce_id: active_policy.central.command.internal_taskforce,
      policy_id: active_policy.global_id,
      future_ids: [...duplicate_futures.values()].map((f) => f.global_id),
    });

    for (const child_future of duplicate_futures.values()) child_future.future_group_id = group_future.global_id;

    const src_pool = other.pool;
    const dest_pool = this.pool;
    const memory = active_policy.central.memory;

    const semaphore = new asyncio.Semaphore(4);

    const _copy_one = async (entry_id, child_future) => {
      try {
        await with_async(semaphore, async () => {
          child_future.status = FutureStatus.RUNNING;

          const remember_ref = memory.remember(entry_id, { pool_id: src_pool.global_id });
          if (remember_ref === null || remember_ref === undefined) {
            throw new RuntimeError("Manifest pool copy requires a non-default source pool.");
          }

          const remember_fut = getitem(active_policy.future_bank, remember_ref.global_id);
          let entry = await remember_fut;

          const memorize_ref = memory.memorize(entry, { pool_id: dest_pool.global_id });
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

    const _copy_all = async () => {
      await asyncio.gather(...[...duplicate_futures].map(([eid, cf]) => _copy_one(eid, cf)), { return_exceptions: true });
    };

    const _run_copy_loop = () => {
      try {
        return asyncio.run(_copy_all());
      } catch (exc) {
        for (const child_future of duplicate_futures.values()) {
          if ([FutureStatus.FINISHED, FutureStatus.ERROR, FutureStatus.CANCELLED].includes(child_future.status)) continue;
          child_future.exception = exc;
          child_future.result = null;
          child_future.status = FutureStatus.ERROR;
        }
        return undefined;
      }
    };

    new Thread({ target: _run_copy_loop, name: `ManifestCopy-${group_future.global_id}`, daemon: true }).start();

    return group_future;
  }
}

register("laila.data.schema.pool_wrapper", { PoolWrapper });
