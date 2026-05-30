"""PoolWrapper -- lightweight proxy binding a pool to a manifest for scoped copies.

Returned by :meth:`_LAILA_IDENTIFIABLE_POOL.__getitem__` when the key
is a :class:`Manifest`. The wrapper exists so that the ``<=``
operator can copy *only the entries listed by the manifest* between
two pools, instead of copying everything::

    my_future = pool_dest[manifest] <= pool_src[manifest]

How the copy works
------------------
The right-hand side selects the source: ``pool_src[manifest]``
returns a :class:`PoolWrapper` over ``pool_src``. The left-hand side
selects the destination the same way. ``__le__`` then walks the
manifest's leaf entries, fetching each one from the source pool via
:meth:`memory.remember` (which handles serialisation and proxy chains
for us), then storing it in the destination pool via
:meth:`memory.memorize`. The whole copy runs concurrently with a
4-way semaphore on a daemon thread, returning a
:class:`GroupFuture` whose children correspond to per-entry copy
operations.

Concurrency model
-----------------
:class:`PoolWrapper` itself holds no lock -- all I/O is delegated to
the wrapped pool's existing public methods, which already take care
of their own atomic-lock coverage. The ``<=`` runner builds a
``GroupFuture`` of per-entry ``ConcurrentPackageFuture`` children
that the active policy's future bank can observe like any other
group future.
"""

from __future__ import annotations

import asyncio
import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .base import _LAILA_IDENTIFIABLE_POOL


class PoolWrapper:
    """Manifest-scoped view over a pool, used as the LHS / RHS of ``<=`` copies.

    A wrapper is conceptually "this pool, but only the entries listed
    by *manifest*". The class itself is intentionally minimal: just
    two slots (``pool`` and ``manifest``) and one operator (``<=``)
    that performs the actual cross-pool copy.

    Parameters
    ----------
    pool : _LAILA_IDENTIFIABLE_POOL
        The bound pool instance. Must be reachable from the active
        local policy's central memory.
    manifest : Manifest
        The manifest whose ``global_id`` leaves define the entries
        in scope. Both the source and destination wrapper are
        expected to use the same manifest in a copy operation.

    Notes
    -----
    No internal locking; every pool method called from inside ``<=``
    already takes the pool's own lock.
    """

    __slots__ = ("manifest", "pool")

    def __init__(self, pool: _LAILA_IDENTIFIABLE_POOL, manifest: Any) -> None:
        self.pool = pool
        self.manifest = manifest

    def __le__(self, other: Any) -> Any:
        """``self <= other`` -- copy *other*'s manifest entries into this wrapper's pool.

        Spawns a daemon thread running an asyncio loop that, for each
        entry id in the manifest, ``remember``s the entry from the
        source pool and ``memorize``s it into the destination pool.
        A four-way semaphore caps the in-flight copy concurrency.

        Each per-entry copy populates a corresponding
        :class:`ConcurrentPackageFuture` in a parent
        :class:`GroupFuture` so callers can wait on the whole batch
        with the usual future API.

        Parameters
        ----------
        other : PoolWrapper
            Source pool + manifest binding. The manifest is read from
            *self* (both wrappers should carry the same manifest in a
            normal copy).

        Returns
        -------
        GroupFuture
            Aggregate future whose children resolve as each entry
            finishes copying.

        Raises
        ------
        RuntimeError
            If the source pool's ``remember`` returns ``None`` (which
            happens when the active policy's *default* pool is used as
            the source -- manifest copies require a non-default
            source).
        """
        if not isinstance(other, PoolWrapper):
            return NotImplemented

        import laila

        from ...policy.central.command.schema.future.future.future_status import FutureStatus
        from ...policy.central.command.schema.future.future.group_future import GroupFuture
        from ...policy.central.command.taskforce.thread_pool_executor.future import (
            ConcurrentPackageFuture,
        )

        active_policy = laila.get_active_policy()

        entry_ids = list(self.manifest)

        duplicate_futures = {
            entry_id: ConcurrentPackageFuture(
                taskforce_id=active_policy.central.command.alpha_taskforce,
                policy_id=active_policy.global_id,
                purpose=f"manifest_copy:{entry_id}",
            )
            for entry_id in entry_ids
        }

        group_future = GroupFuture(
            taskforce_id=active_policy.central.command.alpha_taskforce,
            policy_id=active_policy.global_id,
            future_ids=[f.global_id for f in duplicate_futures.values()],
        )

        for child_future in duplicate_futures.values():
            child_future.future_group_id = group_future.global_id

        src_pool = other.pool
        dest_pool = self.pool
        memory = active_policy.central.memory

        semaphore = asyncio.Semaphore(4)

        async def _copy_one(
            entry_id: str,
            child_future: ConcurrentPackageFuture,
        ) -> None:
            try:
                async with semaphore:
                    child_future.status = FutureStatus.RUNNING

                    remember_ref = memory.remember(
                        entry_ids=entry_id,
                        pool_id=src_pool.global_id,
                    )
                    if remember_ref is None:
                        raise RuntimeError("Manifest pool copy requires a non-default source pool.")

                    remember_fut = active_policy.future_bank[remember_ref.global_id]
                    entry = await remember_fut

                    memorize_ref = memory.memorize(
                        entries=entry,
                        pool_id=dest_pool.global_id,
                    )
                    if memorize_ref is not None:
                        memorize_fut = active_policy.future_bank[memorize_ref.global_id]
                        await memorize_fut

                    child_future.exception = None
                    child_future.result = entry_id
                    child_future.status = FutureStatus.FINISHED
                    del entry
            except Exception as exc:
                child_future.exception = exc
                child_future.result = None
                child_future.status = FutureStatus.ERROR

        async def _copy_all() -> None:
            await asyncio.gather(
                *(_copy_one(eid, cf) for eid, cf in duplicate_futures.items()),
                return_exceptions=True,
            )

        def _run_copy_loop() -> None:
            try:
                asyncio.run(_copy_all())
            except Exception as exc:
                for child_future in duplicate_futures.values():
                    if child_future.status in (
                        FutureStatus.FINISHED,
                        FutureStatus.ERROR,
                        FutureStatus.CANCELLED,
                    ):
                        continue
                    child_future.exception = exc
                    child_future.result = None
                    child_future.status = FutureStatus.ERROR

        threading.Thread(
            target=_run_copy_loop,
            name=f"ManifestCopy-{group_future.global_id}",
            daemon=True,
        ).start()

        return group_future
