"""PoolWrapper — lightweight proxy binding a pool to a manifest for scoped copies.

Returned by ``pool[manifest]`` to enable the syntax::

    my_future = pool_dest[manifest] <= pool_src[manifest]

which copies only the manifest's referenced entries from *pool_src* to
*pool_dest*.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .base import _LAILA_IDENTIFIABLE_POOL


class PoolWrapper:
    """Binds a pool to a manifest for manifest-scoped ``<=`` copy operations.

    This is a **plain Python class** with no lock of its own.  All pool I/O
    is delegated through the pool's existing methods which already acquire the
    pool's ``RLock`` internally.

    Parameters
    ----------
    pool : _LAILA_IDENTIFIABLE_POOL
        The bound pool instance.
    manifest : Manifest
        The manifest whose ``global_id`` leaves define the scope.
    """

    __slots__ = ("pool", "manifest")

    def __init__(self, pool: _LAILA_IDENTIFIABLE_POOL, manifest: Any) -> None:
        self.pool = pool
        self.manifest = manifest

    def __le__(self, other: Any) -> Any:
        """Copy manifest entries from *other*'s pool into this wrapper's pool.

        Parameters
        ----------
        other : PoolWrapper
            Source pool+manifest binding.  The manifest on both sides is used
            to determine which ``global_id`` entries to copy.

        Returns
        -------
        GroupFuture
            A future that resolves when all entries have been copied.
        """
        if not isinstance(other, PoolWrapper):
            return NotImplemented

        from ...policy.central.command.schema.future.future.group_future import GroupFuture
        from ...policy.central.command.schema.future.future.future_status import FutureStatus
        from ...policy.central.command.taskforce.thread_pool_executor.future import ConcurrentPackageFuture
        import laila

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
                        raise RuntimeError(
                            "Manifest pool copy requires a non-default source pool."
                        )

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
                *(
                    _copy_one(eid, cf)
                    for eid, cf in duplicate_futures.items()
                ),
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
