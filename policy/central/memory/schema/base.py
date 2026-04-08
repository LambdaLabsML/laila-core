"""Central memory system — memorize, remember, forget, and pool duplication."""

from typing import Dict, Optional, Any, List
import asyncio
import threading
from pydantic import BaseModel, Field, PrivateAttr
from contextlib import contextmanager
from collections.abc import Sequence

from .....pool.schema.base import _LAILA_IDENTIFIABLE_POOL
from .....basics.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from .....basics.definitions.cli_capable import _LAILA_CLI_CAPABLE_CLASS
from ...command.schema.future.future.group_future import GroupFuture
from ...command.schema.future.future.future_status import FutureStatus
from ...command.taskforce.thread_pool_executor.future import ConcurrentPackageFuture
from ..hint.hint import MemoryHint
from ..record.record import Record
from .....utils.decorators.typecheck import ensure_list
from .....macros.strings import _CENTRAL_MEMORY_SCOPE, _DEFAULT_POOL_NICKNAME
from ..router.pool_router import _LAILA_IDENTIFIABLE_POOL_ROUTER
from .....entry import Entry


class _LAILA_IDENTIFIABLE_CENTRAL_MEMORY(_LAILA_CLI_CAPABLE_CLASS, _LAILA_IDENTIFIABLE_OBJECT):
    """Central memory controller for storing, retrieving, and deleting entries across pools."""
    _scopes: list[str] = PrivateAttr(default_factory=lambda: list([_CENTRAL_MEMORY_SCOPE]))
    pool_router: Optional[_LAILA_IDENTIFIABLE_POOL_ROUTER] = Field(default=None)
    alpha_pool: Optional[str] = Field(default=None)

    class Config:
        arbitrary_types_allowed = True

    def model_post_init(self, __context: Any) -> None:
        """Create a default pool router and ensure an alpha pool exists."""
        if self.pool_router is None:
            from .....macros.defaults import DefaultPoolRouter
            self.pool_router = DefaultPoolRouter()

        if self.alpha_pool is None:
            if _DEFAULT_POOL_NICKNAME in self.pool_router.pools_nicknames:
                self.alpha_pool = self.pool_router.pools_nicknames[_DEFAULT_POOL_NICKNAME]
            else:
                from .....macros.defaults import DefaultPool
                alpha = DefaultPool()
                self.pool_router.extend(
                    alpha, affinity=1, pool_nickname=_DEFAULT_POOL_NICKNAME
                )
                self.alpha_pool = alpha.global_id

    
    def extend(self, pool: _LAILA_IDENTIFIABLE_POOL, *, affinity: Optional[float] = None, pool_nickname: Optional[str] = None):
        """Delegate pool registration to the pool router."""
        self.pool_router.extend(pool, affinity=affinity, pool_nickname=pool_nickname)

    def _resolve_pool_ref(self, pool_ref: _LAILA_IDENTIFIABLE_POOL | str) -> _LAILA_IDENTIFIABLE_POOL:
        """Resolve a pool object, ID string, or nickname to a pool instance."""
        if isinstance(pool_ref, _LAILA_IDENTIFIABLE_POOL):
            return pool_ref

        if isinstance(pool_ref, str):
            if pool_ref in self.pool_router.pools:
                return self.pool_router.pools[pool_ref]
            if pool_ref in self.pool_router.pools_nicknames:
                return self.pool_router.pools[self.pool_router.pools_nicknames[pool_ref]]
            raise KeyError(f"Pool '{pool_ref}' was not found.")

        raise TypeError("pool_ref must be a pool object, pool id, or pool nickname.")

    #TODO: need to make sure cross-borrowing does not lead to stall
    @contextmanager
    def borrow(
        self,
        keys = [],
        global_borrow = False,
    ):
        """Context manager for borrowing entries (not yet implemented)."""
        raise NotImplementedError

    def _duplicate_pool(
        self,
        pool_src: _LAILA_IDENTIFIABLE_POOL | str,
        pool_dest: _LAILA_IDENTIFIABLE_POOL | str,
        *,
        inflight_max_entries: int = 4,
    ) -> GroupFuture:
        """Copy all entries from *pool_src* to *pool_dest* asynchronously."""
        from ..... import active_policy

        if inflight_max_entries < 1:
            raise ValueError("duplicate_pool requires inflight_max_entries >= 1.")

        src_pool = self._resolve_pool_ref(pool_src)
        dest_pool = self._resolve_pool_ref(pool_dest)
        entry_ids = list(src_pool.keys())

        duplicate_futures = {
            entry_id: ConcurrentPackageFuture(
                taskforce_id=active_policy.central.command.alpha_taskforce,
                policy_id=active_policy.global_id,
                purpose=f"duplicate_pool:{entry_id}",
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

        semaphore = asyncio.Semaphore(inflight_max_entries)

        async def _duplicate_one(entry_id: str, child_future: ConcurrentPackageFuture) -> None:
            try:
                async with semaphore:
                    child_future.status = FutureStatus.RUNNING

                    remember_ref = self.remember(
                        entry_ids=entry_id,
                        pool_id=src_pool.global_id,
                    )
                    if remember_ref is None:
                        raise RuntimeError("duplicate_pool requires a non-default source pool.")

                    remember_fut = active_policy.future_bank[remember_ref.global_id]
                    entry = await remember_fut

                    memorize_ref = self.memorize(
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

        async def _duplicate_all() -> None:
            await asyncio.gather(
                *(
                    _duplicate_one(entry_id, child_future)
                    for entry_id, child_future in duplicate_futures.items()
                ),
                return_exceptions=True,
            )

        def _run_duplication_event_loop() -> None:
            try:
                asyncio.run(_duplicate_all())
            except Exception as exc:
                for child_future in duplicate_futures.values():
                    if child_future.status in [FutureStatus.FINISHED, FutureStatus.ERROR, FutureStatus.CANCELLED]:
                        continue
                    child_future.exception = exc
                    child_future.result = None
                    child_future.status = FutureStatus.ERROR

        threading.Thread(
            target=_run_duplication_event_loop,
            name=f"DuplicatePool-{group_future.global_id}",
            daemon=True,
        ).start()

        return group_future



    @ensure_list("entries")
    def memorize(
        self, 
        entries: Any,
        *,
        pool_id: Optional[str] = None,
        pool_nickname: Optional[str] = None,
        affinity: Optional[float] = None,
    ):
        """Persist entries to the routed pool and return a ``GroupFuture``."""
        from ..... import active_policy

        pool = self.pool_router.route(
            entries = entries,
            pool_id = pool_id,
            pool_nickname = pool_nickname,
            affinity = affinity,
        )

        return self._record(entries, pool)


    def _record(
        self,
        entries: Entry,
        pool: _LAILA_IDENTIFIABLE_POOL,
    ):
        """Dispatch recording to batch or non-batch path based on pool capability."""
        if pool.batch_accelerated:
            return self._batch_record(entries, pool)
        else:
            return self._non_batch_record(entries, pool)

    def _non_batch_record(
        self,
        entries: Entry,
        pool: _LAILA_IDENTIFIABLE_POOL,
    ):
        """Serialize and write each entry individually via the command taskforce."""
        from ..... import active_policy

        def _individual_record_subprocedure(entry: Entry, pool: _LAILA_IDENTIFIABLE_POOL):
            record=Record(
                    entry = entry,
                    creator = active_policy.global_id,
                    borrower = active_policy.global_id
                )
            transformations = pool.transformations
            #TODO: Add comm encryption here
            #comm_encryption_protocol = active_policy.central_communication.encryption_protocol
            final_transformations = transformations
            pool[entry.global_id] = record.serialize(transformations = final_transformations)

        futures = active_policy.central.command.submit(
            tasks=[lambda entry=entry, pool=pool: _individual_record_subprocedure(entry, pool) for entry in entries]
        )
        return futures


    def _batch_record(
        self,
        entries: List[Entry],
        pool: _LAILA_IDENTIFIABLE_POOL,
    ):
        """Batch-record entries (not yet implemented)."""
        raise NotImplementedError


    @ensure_list("entry_ids")
    def remember(
        self, 
        entry_ids: List[Entry]|List[str],
        *,
        pool_id: Optional[str] = None,
        pool_nickname: Optional[str] = None,
        affinity: Optional[float] = None,
    ):
        """Fetch entries from the routed pool and return a ``GroupFuture``."""
        from ..... import active_policy
        from .....entry import Entry

        pool = self.pool_router.route(
            entries = entry_ids,
            pool_id = pool_id,
            pool_nickname = pool_nickname,
            affinity = affinity,
        )

        return self._fetch(entry_ids, pool=pool)


    def _fetch(
        self, 
        entry_ids: List[str],
        *,
        pool: Optional[Dict[str, _LAILA_IDENTIFIABLE_POOL]] = None,
        borrow: bool = False,
    ):
        """Dispatch fetching to batch or non-batch path based on pool capability."""
        if borrow:
            raise NotImplementedError
        if pool.batch_accelerated:
            return self._batch_fetch(entry_ids, pool=pool)
        else:
            return self._non_batch_fetch(entry_ids, pool=pool)

    
    def _non_batch_fetch(
        self,
        entry_ids: List[str],
        *,
        pool: Optional[Dict[str, _LAILA_IDENTIFIABLE_POOL]] = None,
    ):
        """Fetch each entry individually via the command taskforce."""
        def _individual_fetch_subprocedure(entry_id: str, pool: _LAILA_IDENTIFIABLE_POOL):
            from_pool = pool[entry_id]
            if from_pool is None:
                raise KeyError(f"Entry {entry_id} not found in pool {pool.global_id}")
            return Record.recover(from_pool)["entry"] 
        
        from ..... import active_policy
        futures = active_policy.central.command.submit(
            tasks=[lambda entry_id=entry_id, pool=pool: _individual_fetch_subprocedure(entry_id, pool) for entry_id in entry_ids]
        )
        return futures

    def _batch_fetch(
        self,
        keys: List[str],
        *,
        pool: Optional[Dict[str, _LAILA_IDENTIFIABLE_POOL]] = None,
    ):
        """Batch-accelerated fetch path (not yet implemented)."""
        raise NotImplementedError
            

    @ensure_list("entry_ids")
    def forget(
        self, 
        entry_ids: List[Entry]|List[str],
        *,
        pool_id: Optional[str] = None,
        pool_nickname: Optional[str] = None,
        affinity: Optional[float] = None,
    ):
        """Delete entries from the routed pool and return a ``GroupFuture``."""
        pool = self.pool_router.route(
            entries = entry_ids,
            pool_id = pool_id,
            pool_nickname = pool_nickname,
            affinity = affinity,
        )

        return self._delete(entry_ids, pool=pool)

    def _delete(
        self,
        entry_ids: List[str],
        pool: _LAILA_IDENTIFIABLE_POOL,
    ):
        """Dispatch deletion to batch or non-batch path based on pool capability."""
        if pool.batch_accelerated:
            return self._batch_delete(entry_ids, pool=pool)
        else:
            return self._non_batch_delete(entry_ids, pool=pool)

    def _batch_delete(
        self,
        entry_ids: List[str],
        pool: _LAILA_IDENTIFIABLE_POOL,
    ):
        """Batch-delete entries (not yet implemented)."""
        raise NotImplementedError


    def _non_batch_delete(
        self,
        entry_ids: List[str],
        pool: _LAILA_IDENTIFIABLE_POOL,
    ):
        """Delete each entry individually via the command taskforce."""
        def _individual_delete_subprocedure(entry_id: str, pool: _LAILA_IDENTIFIABLE_POOL):
            del pool[entry_id]
        
        from ..... import active_policy
        futures = active_policy.central.command.submit(
            tasks=[lambda entry_id=entry_id, pool=pool: _individual_delete_subprocedure(entry_id, pool) for entry_id in entry_ids]
        )
        return futures
