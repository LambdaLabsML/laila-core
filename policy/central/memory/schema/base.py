"""Central memory system -- memorize, remember, forget, and pool duplication.

This module hosts :class:`_LAILA_IDENTIFIABLE_CENTRAL_MEMORY`, the
backbone of every policy's storage layer. It coordinates four moving
parts:

- the :class:`PoolRouter` (one per central memory) that picks
  destinations,
- the *alpha pool* -- a privileged pool that acts as the default
  destination and the cache target for ``remember(..., persist=True)``,
- the per-pool :class:`TransformationSequence` that defines the
  serialization pipeline (e.g. ``base64 -> zlib -> msgpack``),
- the alpha task-force, where every per-entry coroutine is submitted
  for concurrent I/O.

Two operation flavors live side-by-side:

- *parallel-individual* paths (``_parallel_individual_record`` /
  ``_fetch`` / ``_delete``) -- one async coroutine per entry, suitable
  for any pool that exposes per-key I/O. The default for everything.
- *batch-accelerated* paths (``_batch_accelerated_*``) -- placeholders
  for pools that can multiplex many keys in a single round-trip
  (``COPY`` for postgres, ``mset`` for redis, etc.). Not yet
  implemented; the placeholders raise.

The ``_remember_with_persist`` / ``_duplicate_pool`` helpers run
entire async pipelines on a daemon thread so the work proceeds
concurrently without polluting the caller's event loop.
"""

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
    """Central memory controller for storing, retrieving, and deleting entries.

    Owns a :class:`PoolRouter` that decides which pool a given
    operation lands on, plus an *alpha pool* selection that doubles as
    the default destination and the cache target for cache-back reads.

    Three top-level public methods drive the system:

    - :meth:`memorize` -- write entries to the routed pool.
    - :meth:`remember` -- read entries (with optional cache-back into
      the alpha pool).
    - :meth:`forget`   -- delete entries from the routed pool.

    All three are decorated with :func:`ensure_list` so callers can
    pass either a single entry / id or a list and get uniform
    list-shaped behavior internally.
    """
    _scopes: list[str] = PrivateAttr(default_factory=lambda: list([_CENTRAL_MEMORY_SCOPE]))
    pool_router: Optional[_LAILA_IDENTIFIABLE_POOL_ROUTER] = Field(default=None)
    alpha_pool: Optional[str] = Field(default=None)

    class Config:
        arbitrary_types_allowed = True

    def model_post_init(self, __context: Any) -> None:
        """Wire a default :class:`PoolRouter` and pick an alpha pool.

        Run order:

        1. If no router was supplied, instantiate a
           :class:`DefaultPoolRouter` (which itself auto-registers an
           in-memory :class:`DefaultPool` under the ``DEFAULT`` nickname).
        2. If no ``alpha_pool`` was set, prefer the pool registered
           under ``DEFAULT``. If for any reason that nickname is missing,
           create a fresh :class:`DefaultPool` and adopt it.

        After this hook returns, ``self.alpha_pool`` is guaranteed to
        resolve through ``self.pool_router.pools[self.alpha_pool]``.
        """
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
        """Forward pool registration to :meth:`PoolRouter.extend`.

        Convenience pass-through so user code can write
        ``policy.central.memory.extend(my_pool)`` without reaching into
        the router.
        """
        self.pool_router.extend(pool, affinity=affinity, pool_nickname=pool_nickname)

    def _resolve_pool_ref(self, pool_ref: _LAILA_IDENTIFIABLE_POOL | str) -> _LAILA_IDENTIFIABLE_POOL:
        """Resolve a pool object, gid string, or nickname to a live pool instance.

        Lookup order
        ------------
        1. *pool_ref* is already a :class:`_LAILA_IDENTIFIABLE_POOL` --
           returned as-is.
        2. *pool_ref* is a string that matches a registered pool gid --
           returned from :attr:`PoolRouter.pools`.
        3. *pool_ref* is a string that matches a registered nickname --
           resolved via :attr:`PoolRouter.pools_nicknames` and then
           looked up in :attr:`PoolRouter.pools`.

        Raises
        ------
        KeyError
            If the string matches neither a gid nor a nickname.
        TypeError
            If *pool_ref* is neither a pool nor a string.
        """
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
        """Context manager that lends entries to the caller for the
        scope of a ``with`` block. Not yet implemented.

        Planned semantics: the listed *keys* are pinned in the alpha
        pool for the duration of the block and released on exit. With
        ``global_borrow=True`` the borrow is announced to peer policies
        so they avoid double-fetching the same artefacts during the
        same window.
        """
        raise NotImplementedError

    def _duplicate_pool(
        self,
        pool_src: _LAILA_IDENTIFIABLE_POOL | str,
        pool_dest: _LAILA_IDENTIFIABLE_POOL | str,
        *,
        inflight_max_entries: int = 4,
    ) -> GroupFuture:
        """Copy every entry from *pool_src* into *pool_dest* asynchronously.

        Implements the ``cache <= origin`` operator on
        :class:`_LAILA_IDENTIFIABLE_POOL` and is also the building
        block behind any "promote this snapshot to that storage"
        workflow. Each entry is read from the source pool, then
        written to the destination pool; both legs proceed
        concurrently up to *inflight_max_entries* at a time.

        The return value is a :class:`GroupFuture` whose ``future_ids``
        reference one :class:`ConcurrentPackageFuture` per entry. Each
        per-entry future flips to ``FINISHED`` when *that* entry has
        landed in *pool_dest*. Use ``with laila.guarantee:`` around the
        call to block until the entire duplication is done.

        Parameters
        ----------
        pool_src : _LAILA_IDENTIFIABLE_POOL or str
            Source pool, by instance, gid, or nickname.
        pool_dest : _LAILA_IDENTIFIABLE_POOL or str
            Destination pool, same shapes accepted.
        inflight_max_entries : int, default 4
            Maximum number of concurrent (read, write) pairs in flight
            at any moment. Bounded via an :class:`asyncio.Semaphore`.

        Returns
        -------
        GroupFuture
            A group future you can ``await`` or ``.wait()`` on.

        Raises
        ------
        ValueError
            If *inflight_max_entries* is below 1.
        RuntimeError
            If the source pool would have returned ``None`` from a
            ``remember`` (typical for the in-memory default pool which
            doesn't expose futures).
        """
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
        """Persist *entries* to the routed pool.

        Routes through the :class:`PoolRouter` (``pool_id`` >
        ``pool_nickname`` > default), records a ``memorize`` log line,
        and dispatches to either the batch-accelerated path (if the
        target pool supports it) or the per-entry parallel path.

        Parameters
        ----------
        entries : Entry or list[Entry]
            One or more entries to write. The :func:`ensure_list`
            decorator wraps a single entry in a 1-list before this body
            runs.
        pool_id : str, optional
            Explicit pool gid, highest-priority routing input.
        pool_nickname : str, optional
            Friendly name resolved through the router.
        affinity : float, optional
            Reserved for future affinity routing.

        Returns
        -------
        Future or GroupFuture or None
            A future identity (single entry), a group future (many
            entries), or ``None`` if the pool path was synchronous and
            no future handle was needed.
        """
        from ..... import active_policy

        pool = self.pool_router.route(
            entries = entries,
            pool_id = pool_id,
            pool_nickname = pool_nickname,
            affinity = affinity,
        )

        try:
            from .....logger import get_logger
            get_logger().record_memorize(
                entries=entries, pool=pool, policy=active_policy,
            )
        except Exception:
            pass

        return self._record(entries, pool)


    def _record(
        self,
        entries: Entry,
        pool: _LAILA_IDENTIFIABLE_POOL,
    ):
        """Pick the right write path based on the pool's ``batch_accelerated`` flag.

        Pools that can multiplex many writes per round-trip
        (``batch_accelerated=True``) are routed to
        :meth:`_batch_accelerated_record`; others use the per-entry
        parallel path.
        """
        if pool.batch_accelerated:
            return self._batch_accelerated_record(entries, pool)
        else:
            return self._parallel_individual_record(entries, pool)

    def _parallel_individual_record(
        self,
        entries: Entry,
        pool: _LAILA_IDENTIFIABLE_POOL,
    ):
        r"""Memorize each entry as a single per-entry async coroutine.

        Each entry's coroutine:

        1. wraps the entry in a :class:`Record` and runs ``serialize`` inline
           (pure CPU, no await needed),
        2. ``await``\ s the pool's ``_write_async`` for the actual storage
           round-trip.

        Submits one coroutine per entry to the alpha taskforce. Returns a
        single future identity for one entry, or a :class:`GroupFuture`
        for many. Replaces the previous two-stage :class:`ComplexFuture`
        pipeline that double-queued each leg.
        """
        from ..... import active_policy

        cmd = active_policy.central.command
        alpha_id = cmd.alpha_taskforce
        policy_gid = active_policy.global_id
        transformations = pool.transformations

        async def _memorize_one(e=None, p=pool, t=transformations, pgid=policy_gid):
            record = Record(entry=e, creator=pgid, borrower=pgid)
            blob = record.serialize(transformations=t)
            if hasattr(blob, "data"):
                blob = blob.data
            await p._write_async(e.global_id, blob)
            return e.global_id

        factories = [
            (lambda e=entry: _memorize_one(e=e))
            for entry in entries
        ]
        return cmd.submit(tasks=factories, taskforce_id=alpha_id)


    def _batch_accelerated_record(
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
        persist: bool = True,
    ):
        """Fetch entries from the routed pool, optionally caching them
        into the alpha pool on the way back.

        When the routed source pool is *also* the alpha pool, or when
        ``persist=False`` is requested, the fetched entries are simply
        returned through the regular :meth:`_fetch` path. Otherwise,
        :meth:`_remember_with_persist` is used: each entry is fetched
        from the source pool *and* written to the alpha pool, with the
        per-entry future only flipping to ``FINISHED`` once both legs
        are done.

        That cache-back semantics is what lets ``with laila.guarantee:``
        block until the alpha pool has the entries -- subsequent reads
        from the alpha pool are then guaranteed to be hot.

        Parameters
        ----------
        entry_ids : str, Entry, or list of either
            Identifier(s) to fetch. Lists of live :class:`Entry`
            instances are accepted but the gid is what's actually used.
        pool_id : str, optional
            Explicit source pool gid.
        pool_nickname : str, optional
            Friendly name for the source pool.
        affinity : float, optional
            Reserved for future affinity routing.
        persist : bool, default True
            Whether to cache fetched entries into the alpha pool.

        Returns
        -------
        Future or GroupFuture
            Future-like handle resolving to the entry or list of
            entries.
        """
        from ..... import active_policy
        from .....entry import Entry

        pool = self.pool_router.route(
            entries = entry_ids,
            pool_id = pool_id,
            pool_nickname = pool_nickname,
            affinity = affinity,
        )

        try:
            from .....logger import get_logger
            get_logger().record_remember(
                entry_ids=entry_ids, pool=pool, policy=active_policy,
            )
        except Exception:
            pass

        if not persist or pool.global_id == self.alpha_pool:
            return self._fetch(entry_ids, pool=pool)

        return self._remember_with_persist(entry_ids, pool=pool)


    def _remember_with_persist(
        self,
        entry_ids: List[str],
        *,
        pool: _LAILA_IDENTIFIABLE_POOL,
    ):
        """Fetch entries from *pool* and then memorize them into the alpha pool.

        Returns a single future (for one id) or a ``GroupFuture`` (for many)
        whose children only reach ``FINISHED`` once both the fetch and the
        alpha-pool write have completed.  Modeled after ``_duplicate_pool``.
        """
        from ..... import active_policy

        child_futures = {
            entry_id: ConcurrentPackageFuture(
                taskforce_id=active_policy.central.command.alpha_taskforce,
                policy_id=active_policy.global_id,
                purpose=f"remember_with_persist:{entry_id}",
            )
            for entry_id in entry_ids
        }

        group_future = None
        if len(child_futures) > 1:
            group_future = GroupFuture(
                taskforce_id=active_policy.central.command.alpha_taskforce,
                policy_id=active_policy.global_id,
                future_ids=[f.global_id for f in child_futures.values()],
            )
            for child_future in child_futures.values():
                child_future.future_group_id = group_future.global_id

        alpha_pool_id = self.alpha_pool

        async def _run() -> None:
            try:
                for child_future in child_futures.values():
                    child_future.status = FutureStatus.RUNNING

                fetch_ref = self._fetch(entry_ids, pool=pool)
                if fetch_ref is None:
                    raise RuntimeError(
                        "remember with persist requires a non-default source pool "
                        "that returns a future."
                    )
                fetch_fut = active_policy.future_bank[fetch_ref.global_id]
                fetched = await fetch_fut

                if isinstance(fetched, list):
                    entries = fetched
                else:
                    entries = [fetched]

                from .....entry.entry_state import EntryState
                ready_entries = [e for e in entries if e.state == EntryState.READY]
                if ready_entries:
                    memorize_ref = self.memorize(
                        entries=ready_entries, pool_id=alpha_pool_id
                    )
                    if memorize_ref is not None:
                        memorize_fut = active_policy.future_bank[memorize_ref.global_id]
                        await memorize_fut

                for entry, (entry_id, child_future) in zip(
                    entries, child_futures.items()
                ):
                    child_future.exception = None
                    child_future.result = entry
                    child_future.status = FutureStatus.FINISHED
            except Exception as exc:
                for child_future in child_futures.values():
                    if child_future.status in [
                        FutureStatus.FINISHED,
                        FutureStatus.ERROR,
                        FutureStatus.CANCELLED,
                    ]:
                        continue
                    child_future.exception = exc
                    child_future.result = None
                    child_future.status = FutureStatus.ERROR

        def _run_event_loop() -> None:
            try:
                asyncio.run(_run())
            except Exception as exc:
                for child_future in child_futures.values():
                    if child_future.status in [
                        FutureStatus.FINISHED,
                        FutureStatus.ERROR,
                        FutureStatus.CANCELLED,
                    ]:
                        continue
                    child_future.exception = exc
                    child_future.result = None
                    child_future.status = FutureStatus.ERROR

        thread_name = (
            f"RememberPersist-{group_future.global_id}"
            if group_future is not None
            else f"RememberPersist-{next(iter(child_futures.values())).global_id}"
        )
        threading.Thread(
            target=_run_event_loop,
            name=thread_name,
            daemon=True,
        ).start()

        if group_future is not None:
            return group_future

        single = next(iter(child_futures.values()))
        return single.future_identity


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
            return self._batch_accelerated_fetch(entry_ids, pool=pool)
        else:
            return self._parallel_individual_fetch(entry_ids, pool=pool)

    
    def _parallel_individual_fetch(
        self,
        entry_ids: List[str],
        *,
        pool: Optional[Dict[str, _LAILA_IDENTIFIABLE_POOL]] = None,
    ):
        r"""Fetch and deserialize each entry via a single per-entry coroutine.

        Each entry's coroutine:

        1. ``await``\ s the pool's ``_read_async`` for the storage round-trip,
        2. ``await``\ s :meth:`Record._build_async` to deserialize and
           recursively hydrate any nested entries (which themselves may
           ``await`` further fetches on the same loop).

        Submits one coroutine per entry to the alpha taskforce and
        returns a single future identity (one entry) or a
        :class:`GroupFuture` (many). Replaces the previous two-stage
        :class:`ComplexFuture` pipeline.
        """
        from ..... import active_policy

        cmd = active_policy.central.command
        alpha_id = cmd.alpha_taskforce

        async def _remember_one(eid=None, p=pool):
            raw = await p._read_async(eid)
            if raw is None:
                raise KeyError(f"Entry {eid} not found in pool {p.global_id}")
            record = await Record._build_async(raw)
            return record["entry"]

        factories = [
            (lambda eid=entry_id: _remember_one(eid=eid))
            for entry_id in entry_ids
        ]
        return cmd.submit(tasks=factories, taskforce_id=alpha_id)

    def _batch_accelerated_fetch(
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
        """Delete *entry_ids* from the routed pool.

        Parameters mirror :meth:`memorize` / :meth:`remember`. The
        deletion is *pool-local*: it only affects the routed pool, not
        any other pool that may hold a copy. To remove an entry from
        every registered pool, iterate over
        ``self.pool_router.pools.values()`` and call ``forget`` per pool
        with an explicit ``pool_id``.

        Returns
        -------
        Future or GroupFuture
            Future-like handle resolving when deletion finishes.
        """
        pool = self.pool_router.route(
            entries = entry_ids,
            pool_id = pool_id,
            pool_nickname = pool_nickname,
            affinity = affinity,
        )

        try:
            from ..... import active_policy
            from .....logger import get_logger
            get_logger().record_forget(
                entry_ids=entry_ids, pool=pool, policy=active_policy,
            )
        except Exception:
            pass

        return self._delete(entry_ids, pool=pool)

    def _delete(
        self,
        entry_ids: List[str],
        pool: _LAILA_IDENTIFIABLE_POOL,
    ):
        """Dispatch deletion to batch or non-batch path based on pool capability."""
        if pool.batch_accelerated:
            return self._batch_accelerated_delete(entry_ids, pool=pool)
        else:
            return self._parallel_individual_delete(entry_ids, pool=pool)

    def _batch_accelerated_delete(
        self,
        entry_ids: List[str],
        pool: _LAILA_IDENTIFIABLE_POOL,
    ):
        """Batch-delete entries (not yet implemented)."""
        raise NotImplementedError


    def _parallel_individual_delete(
        self,
        entry_ids: List[str],
        pool: _LAILA_IDENTIFIABLE_POOL,
    ):
        """Delete each entry via a single per-entry async coroutine."""
        from ..... import active_policy

        cmd = active_policy.central.command
        alpha_id = cmd.alpha_taskforce

        async def _delete_one(eid=None, p=pool):
            await p._delete_async(eid)
            return eid

        factories = [
            (lambda eid=entry_id: _delete_one(eid=eid))
            for entry_id in entry_ids
        ]
        return cmd.submit(tasks=factories, taskforce_id=alpha_id)
