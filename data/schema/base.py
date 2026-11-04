"""Base schema for all LAILA storage-pool implementations.

A *pool* is laila's name for a key-value store that persists
serialized :class:`Entry` blobs. Pools are the leaves of the storage
hierarchy that the central memory subsystem routes
``memorize`` / ``remember`` / ``forget`` calls to. Concrete subclasses
plug into different backends -- in-memory, filesystem, S3, Redis,
Postgres, SQLite, DuckDB, HDF5, GCS, Azure, BackBlaze, Cloudflare,
HuggingFace -- by overriding the small set of "internal storage hooks"
described below.

Public surface (proxy-aware)
----------------------------
- ``pool[key]`` -> stored blob, transparently falling back through the
  proxy chain on miss and caching the result on the way back.
- ``pool[key] = entry`` -> local-only write.
- ``del pool[key]`` -> local-only delete.
- ``pool[manifest]`` -> :class:`PoolWrapper` view scoped by a manifest.
- ``pool.exists(key)`` / ``key in pool`` -> local-only existence check.
- ``pool.keys(as_generator=...)`` -> local-only key enumeration.
- ``pool.empty()`` -> local-only wipe.
- ``pool.sync()`` -> flush in-memory cache (raises if cacheless).
- ``cache <= origin`` / ``origin <= cache`` -> bulk duplicate via
  central memory.

Subclass contract (override these)
----------------------------------
Synchronous: :meth:`_read`, :meth:`_write`, :meth:`_delete`,
:meth:`_exists`, :meth:`_keys`, :meth:`_empty`. Default
implementations operate on the in-memory ``resource`` dict.

Asynchronous: :meth:`_read_async`, :meth:`_write_async`,
:meth:`_delete_async`, :meth:`_exists_async`. Default implementations
just call the sync hook on the calling event loop -- subclasses with a
real async client (e.g. ``aioboto3``) should override these.

Proxy chains
------------
Pools can be wired into proxy chains via ``_proxy_to``. A read that
misses the local store falls through to the upstream pool, and a
successful upstream read is cached locally before being returned.
This lets users compose layered caches like

    in_memory << hdf5 << s3

so that hot reads never leave the in-memory tier.
"""

import asyncio
from collections.abc import Iterable, Iterator
from typing import Any, ClassVar

from pydantic import ConfigDict, Field, PrivateAttr

from ...basics.definitions.cli_capable import CLIExempt
from ...entry.compdata.transformation import TransformationSequence
from ...macros.strings import _POOL_SCOPE
from .data_container import _LAILA_IDENTIFIABLE_DATA_CONTAINER
from .pool_index import PoolIndex, is_index_key


class _LAILA_IDENTIFIABLE_POOL(_LAILA_IDENTIFIABLE_DATA_CONTAINER):
    """Abstract base class for laila storage pools.

    A pool is the *map* flavour of
    :class:`_LAILA_IDENTIFIABLE_DATA_CONTAINER`: keys are entry
    ``global_id`` strings.

    Implements the proxy-aware public read/write/delete/exists/keys
    API and provides default in-memory hook implementations so simple
    pools can be created by just inheriting and overriding nothing.
    Real backends override the ``_read`` / ``_write`` / ``_delete`` /
    ``_keys`` / ``_exists`` / ``_empty`` hooks (and, optionally, their
    ``_async`` counterparts).

    Attributes
    ----------
    resource : dict[str, Any]
        Default in-memory backing store. Concrete subclasses can
        repurpose this (e.g. an in-memory cache fronting a remote
        store) or leave it unused.
    batch_accelerated : bool
        Whether the backend can handle batched writes more efficiently
        than individual ones. Consulted by central memory when
        choosing a write strategy.
    transformations : TransformationSequence | None
        Optional pipeline of :class:`Transformation` objects applied to
        every blob before write and reversed on read. Lets a pool
        opaquely add compression, encoding, or encryption on top of
        the storage backend.
    index_enabled : bool
        Maintain the per-base evolution / creation-timestamp index (see
        :mod:`data.schema.pool_index`) on every write and delete. Turn
        off for pools whose keys are never looked up by attribute (log
        sinks, ring buffers) to save the extra shard write per store.
    index_pool : _LAILA_IDENTIFIABLE_POOL | None
        Where the index shards are stored. ``None`` (default) means this
        pool itself; point it at an in-memory pool when index writes on
        the data backend are too expensive.

    Index bookkeeping keys (``LAILA:POOL_INDEX:...``) are hidden from
    :meth:`keys` unless ``include_index=True`` is passed.
    """

    _scopes: list[str] = PrivateAttr(default_factory=lambda: list([_POOL_SCOPE]))
    _proxy_to: Any | None = PrivateAttr(default=None)
    _index: Any | None = PrivateAttr(default=None)
    resource: dict[str, Any] = CLIExempt(default_factory=dict)
    batch_accelerated: bool = Field(default=False)
    transformations: TransformationSequence | None = CLIExempt(default=None)
    index_enabled: bool = Field(default=True)
    index_pool: Any | None = CLIExempt(default=None)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def model_post_init(self, __context: Any) -> None:
        """Validate the configuration of pools backed by external storage.

        A persistent backend serialises records to JSON / bytes, so it
        needs a transformation sequence to turn an :class:`Entry` into
        a storable blob; ``transformations=None`` there would make every
        ``memorize`` fail later with an opaque serialisation error. It
        is rejected up front instead. In-memory pools may use ``None``.
        """
        super().model_post_init(__context)
        if self._persistent_storage and self.transformations is None:
            raise ValueError(
                f"{type(self).__name__} is backed by persistent storage and requires a "
                "transformation sequence (got transformations=None); use e.g. "
                "laila.transformations.transformation_base64"
            )

    # ``True`` for backends whose storage outlives the Python process (every
    # built-in file / database / object-store pool sets it). It gates the
    # ``transformations=None`` rejection above and the persistence of the
    # pool-index identity (see ``pool_index.py``). Custom backends opt in by
    # setting it on their class; in-process pools leave it ``False``.
    _persistent_storage: ClassVar[bool] = False

    @property
    def _persistent_index_identity(self) -> bool:
        """Whether the :class:`PoolIndex` persists its identity in this pool (see F-03)."""
        return self._persistent_storage

    @property
    def pool_id(self) -> str:
        """Unique identifier for this pool. Alias for :attr:`global_id`."""
        return self.global_id

    # -------- Index --------
    @property
    def index(self) -> PoolIndex:
        """This pool's :class:`PoolIndex` (created on first access)."""
        if self._index is None:
            self._index = PoolIndex(self)
        return self._index

    def _index_record(self, key: str, value: Any) -> None:
        if self.index_enabled and not is_index_key(key):
            self.index.record(key, value)

    def _index_remove(self, key: str) -> None:
        if self.index_enabled and not is_index_key(key):
            self.index.remove(key)

    def _resolve_indexed(self, base_gid: str, attributes: dict[str, str]) -> str | None:
        """Answer an attribute lookup from the index alone, or ``None``.

        Parameters
        ----------
        base_gid : str
            ``LAILA:ENTRY:<uuid>`` (no ``@`` suffix).
        attributes : dict[str, str]
            Search arguments of the reference: ``evolution`` (may be
            negative, ``-1`` = latest) and / or ``creation_timestamp``.

        Returns
        -------
        str or None
            A storage key the index believes exists (central memory
            validates it), or ``None`` when the index cannot answer and
            the caller should fall back to scanning.
        """
        if not self.index_enabled:
            return None
        raw_evolution = attributes.get("evolution")
        evolution = int(raw_evolution) if raw_evolution is not None else None
        timestamp = attributes.get("creation_timestamp")
        if timestamp is not None:
            return self.index.by_creation_timestamp(base_gid, timestamp, evolution)
        return self.index.nth(base_gid, -1 if evolution is None else evolution)

    # -------- Index-maintaining write / delete wrappers --------
    # Every writer (central memory, proxy write-back, ``pool[k] = v``)
    # goes through these so the index sees all mutations. Backends keep
    # overriding only the ``_write`` / ``_delete`` (``_async``) hooks.
    def write(self, key: str, value: Any) -> None:
        """Store *value* under *key* locally and index it."""
        self._write(key, value)
        self._index_record(key, value)

    def delete(self, key: str) -> None:
        """Delete *key* locally and un-index it."""
        self._delete(key)
        self._index_remove(key)

    async def write_async(self, key: str, value: Any) -> None:
        """Async :meth:`write` (index maintenance runs on a worker thread)."""
        await self._write_async(key, value)
        if self.index_enabled and not is_index_key(key):
            await asyncio.to_thread(self.index.record, key, value)

    async def delete_async(self, key: str) -> None:
        """Async :meth:`delete` (index maintenance runs on a worker thread)."""
        await self._delete_async(key)
        if self.index_enabled and not is_index_key(key):
            await asyncio.to_thread(self.index.remove, key)

    # -------- Proxy properties --------
    @property
    def proxy(self):
        """Write-only property used to wire proxy relationships.

        Always returns ``None`` -- the property exists so that the
        natural-looking assignment ``origin.proxy = cache`` can be
        used to express "make ``cache`` a proxy for ``origin``"
        (i.e. set ``cache._proxy_to = origin``). Use
        :attr:`proxy_to` if you want to read the relationship.
        """
        return None

    @proxy.setter
    def proxy(self, pool):
        if pool is not None:
            self._link_proxy(cache=pool, origin=self)

    @property
    def proxy_to(self):
        """The origin pool this one is a cache/proxy for, or ``None``."""
        return self._proxy_to

    @proxy_to.setter
    def proxy_to(self, pool):
        if pool is None:
            self._proxy_to = None
        else:
            self._link_proxy(cache=self, origin=pool)

    @staticmethod
    def _link_proxy(cache, origin) -> None:
        """Set ``cache._proxy_to = origin`` after checking the chain stays acyclic.

        Raises
        ------
        ValueError
            When *cache* is *origin*, or *origin*'s upstream chain
            already leads back to *cache* (``a << b`` then ``b << a``).
        """
        if cache is origin:
            raise ValueError(f"A pool cannot proxy itself ({cache.global_id})")
        seen: set[int] = set()
        cur = origin
        while cur is not None and id(cur) not in seen:
            if cur is cache:
                raise ValueError(
                    f"Proxy cycle: {origin.global_id} already reaches {cache.global_id} "
                    "through its proxy chain"
                )
            seen.add(id(cur))
            cur = getattr(cur, "_proxy_to", None)
        cache._proxy_to = origin

    def _upstream_tiers(self):
        """Upstream pools of this one, nearest first; stops at a repeated pool (defensive)."""
        seen = {id(self)}
        cur = self._proxy_to
        while cur is not None and id(cur) not in seen:
            seen.add(id(cur))
            yield cur
            cur = getattr(cur, "_proxy_to", None)

    def __lshift__(self, other):
        """``cache << origin``: install ``cache`` as a proxy for ``origin``.

        Returns *other* (the origin) so the operator chains
        right-to-left, letting expressions like
        ``mem << hdf5 << s3`` build a multi-tier cache where ``mem``
        fronts ``hdf5`` which fronts ``s3``.
        """
        other.proxy = self
        return other

    def __rshift__(self, other):
        """``origin >> cache``: install ``cache`` as a proxy for ``origin``.

        Returns *other* (the cache) so the operator chains
        left-to-right, letting expressions like
        ``s3 >> hdf5 >> mem`` build a multi-tier cache where ``mem``
        fronts ``hdf5`` which fronts ``s3``.
        """
        self.proxy = other
        return other

    # -------- Internal storage hooks (override in subclasses) --------
    def _read(self, key: str) -> Any | None:
        """Read *key* from this pool's own storage. Override in subclasses.

        The default implementation reads from the in-memory
        ``resource`` dict under the pool's atomic lock and returns
        ``None`` for missing keys (so the proxy-aware
        :meth:`__getitem__` can detect a miss and fall through to the
        upstream pool).
        """
        with self.atomic():
            if key not in self.resource:
                return None
            blob = self.resource[key]
        if blob is None:
            return None
        return blob

    def _write(self, key: str, value: Any) -> None:
        """Write *value* under *key* into this pool's own storage. Override in subclasses.

        Default implementation writes into ``resource`` under the
        atomic lock. Implementations should treat *value* as opaque
        bytes/dict (the central memory layer has already serialised
        the entry).
        """
        with self.atomic():
            self.resource[key] = value

    def _delete(self, key: str) -> None:
        """Delete *key* from this pool's own storage. Override in subclasses.

        Default implementation removes the key from ``resource``
        under the atomic lock. Missing keys are silently tolerated.
        """
        with self.atomic():
            if key in self.resource:
                del self.resource[key]

    def _exists(self, key: str) -> bool:
        """Return ``True`` if *key* is present in this pool's own storage. Override in subclasses.

        Default implementation checks ``resource`` under the atomic
        lock. Should not consult the proxy chain (use the public
        :meth:`exists` if you want a local-only view, which it is).
        """
        with self.atomic():
            return key in self.resource

    def _keys(self, as_generator: bool = False) -> Iterable[str]:
        """Enumerate this pool's own keys. Override in subclasses.

        Parameters
        ----------
        as_generator : bool, default False
            When ``False``, snapshots and returns a list under the
            atomic lock (cheap, O(N) memory). When ``True``, returns
            a generator that holds the lock while iterating -- useful
            when keys are expensive to materialize but the caller
            wants to stream.
        """
        if not as_generator:
            with self.atomic():
                return list(self.resource.keys())
        else:

            def _gen() -> Iterator[str]:
                with self.atomic():
                    for k in self.resource.keys():
                        yield k

            return _gen()

    def _search_keys(self, base_gid: str, attributes: dict[str, str]) -> list[str] | None:
        """Candidate keys for an attribute lookup (``remember("ENTRY:x@...")``).

        Central memory calls this *before* falling back to a full
        :meth:`_keys` scan when it has to find "the evolutions of
        *base_gid*" (``LAILA:ENTRY:<uuid>``, no ``@`` suffix) that
        satisfy *attributes* (``{"evolution": "3"}``,
        ``{"creation_timestamp": "..."}`` ...).

        The default answers from this pool's :attr:`index` when it has
        a shard for *base_gid* and returns ``None`` otherwise ("no
        index, scan"). Returning a list -- even an empty one -- is
        authoritative and skips the scan; central memory still
        validates the key it picks against the store.
        """
        if not self.index_enabled:
            return None
        return self.index.candidates(base_gid)

    def _candidate_keys(self, base_gid: str) -> list[str]:
        """Local-only list of keys that are evolutions of *base_gid*.

        Matches the exact key (a constant) and every ``base_gid@...``
        key (variables). Default implementation filters :meth:`_keys`
        in Python; listing-based backends can push the prefix down.
        """
        prefix = f"{base_gid}@"
        return [k for k in self._keys() if k == base_gid or k.startswith(prefix)]

    def _empty(self) -> None:
        """Wipe this pool's own storage. Override in subclasses.

        Default implementation clears ``resource`` under the atomic
        lock. Should not propagate to the proxy chain.
        """
        with self.atomic():
            self.resource.clear()

    # -------- Default async hooks (override in async-capable pools) --------
    async def _read_async(self, key: str) -> Any | None:
        """Async read; default just delegates to the sync :meth:`_read` inline.

        Subclasses backed by a native-async client (e.g. ``aioboto3``,
        ``asyncpg``) should override this to ``await`` non-blocking
        I/O. The default implementation runs the sync call on the
        calling loop, which blocks every other coroutine on that loop
        for the read's duration -- correct but honest about the
        backend's true (sync) nature.
        """
        return self._read(key)

    async def _write_async(self, key: str, value: Any) -> None:
        """Async write; default delegates to sync :meth:`_write` (see :meth:`_read_async`)."""
        self._write(key, value)

    async def _delete_async(self, key: str) -> None:
        """Async delete; default delegates to sync :meth:`_delete` (see :meth:`_read_async`)."""
        self._delete(key)

    async def _exists_async(self, key: str) -> bool:
        """Async exists; default delegates to sync :meth:`_exists` (see :meth:`_read_async`)."""
        return self._exists(key)

    async def _read_through_async(self, key: str) -> Any | None:
        """Async, proxy-aware read: the coroutine counterpart of :meth:`__getitem__`.

        Reads from this pool's own async storage and, on a miss, falls
        through the ``_proxy_to`` chain. A successful upstream read is
        cached back into this pool (via :meth:`_write_async`) before
        being returned, so later reads bypass the upstream -- exactly
        the layered-cache semantics ``mem << hdf5 << s3`` relies on.

        The synchronous :meth:`__getitem__` already does this walk; this
        method gives the async fetch path (used by ``remember``) the same
        fall-through so a read routed at the front of a proxy chain reaches
        the tier that actually holds the data instead of reporting a miss.
        """
        value = await self._read_async(key)
        if value is not None:
            return value

        # Walk the chain iteratively (cycle-safe) and write the hit back
        # into every tier in front of the one that held it.
        missed = [self]
        for tier in self._upstream_tiers():
            value = await tier._read_async(key)
            if value is not None:
                for front in reversed(missed):
                    await front.write_async(key, value)
                return value
            missed.append(tier)

        return None

    # -------- Proxy-aware public API --------
    def __getitem__(self, key) -> Any | None:
        """Retrieve the blob for *key*, with proxy fall-through and write-back.

        Two special cases:

        - If *key* is a :class:`Manifest`, the call short-circuits and
          returns a :class:`PoolWrapper` view scoped by that manifest.
          That lets users write ``pool[manifest]["nested_key"]`` to
          read keys *as resolved through* the manifest.
        - If the local read misses and ``_proxy_to`` is set, the
          request is forwarded to the upstream pool. On a successful
          upstream read, the value is *cached* into this pool before
          being returned, so subsequent reads bypass the upstream.
        """
        from ...policy.central.memory.schema.manifest import Manifest

        if isinstance(key, Manifest):
            from .pool_wrapper import PoolWrapper

            return PoolWrapper(pool=self, manifest=key)

        value = self._read(key)
        if value is not None:
            return value

        missed = [self]
        for tier in self._upstream_tiers():
            value = tier._read(key)
            if value is not None:
                for front in reversed(missed):
                    front.write(key, value)
                return value
            missed.append(tier)

        return None

    def __setitem__(self, key: str, entry: Any) -> None:
        """Store *entry* under *key*. Local-only; never propagates to a proxy origin."""
        self.write(key, entry)

    def __delitem__(self, key: str) -> None:
        """Delete the entry for *key*. Local-only; never propagates to a proxy origin."""
        self.delete(key)

    def empty(self) -> None:
        """Remove every entry from this pool. Local-only; never propagates to a proxy origin.

        Also drops this pool's index shards, including those held by a
        separate :attr:`index_pool`.
        """
        if self.index_enabled:
            self.index.clear()
        self._empty()

    def exists(self, key: str) -> bool:
        """Return ``True`` if *key* is present in this pool. Local-only check."""
        return self._exists(key)

    def __contains__(self, key: str) -> bool:
        """``key in pool`` -- thin alias for :meth:`exists`."""
        return self.exists(key)

    def keys(self, as_generator: bool = False, include_index: bool = False) -> Iterable[str]:
        """Return the keys stored in this pool. Local-only enumeration.

        Parameters
        ----------
        as_generator : bool, default False
            If ``False``, return a snapshot list (cheap-ish, O(N)
            memory). If ``True``, return an iterator that holds the
            atomic lock for its full lifetime -- prefer this when keys
            are expensive to materialize but the consumer wants to
            stream.
        include_index : bool, default False
            Also yield the pool's own index bookkeeping keys
            (``LAILA:POOL_INDEX:...``). Hidden by default so consumers
            that enumerate a pool (``duplicate_pool``, manifests, user
            code) only see entries.

        Returns
        -------
        Iterable[str]
            Pool keys (snapshot list or generator depending on
            *as_generator*).
        """
        raw = self._keys(as_generator=as_generator)
        if include_index:
            return raw
        if as_generator:
            return (k for k in raw if not is_index_key(k))
        return [k for k in raw if not is_index_key(k)]

    def sync(self) -> None:
        """Flush any in-memory write cache to the backing store.

        The base implementation always raises -- override in subclasses
        that maintain a write-back cache (e.g. some HDF5 / DuckDB
        configurations). Pools that write directly to storage on
        every set should leave the base implementation in place;
        callers can then ``contextlib.suppress(NotImplementedError)``
        the call when treating ``sync`` as a no-op.

        Raises
        ------
        NotImplementedError
            If the pool is cacheless and operates directly on storage.
        """
        raise NotImplementedError(
            "Sync is not implemented for this pool, the pool is cacheless, i.e. operations are immediately executed on the underlying storage."
        )

    def __le__(self, other: Any):
        """``self <= other`` -- bulk-copy ``other`` pool's contents into this one.

        Delegates to ``central.memory._duplicate_pool`` on the active
        local policy, so the copy goes through the same routing /
        manifest machinery as a normal ``memorize`` workflow. *other*
        may be a pool instance or a pool nickname/global-id string.
        """
        from ... import active_policy

        if not isinstance(other, (_LAILA_IDENTIFIABLE_POOL, str)):
            return NotImplemented

        return active_policy.central.memory._duplicate_pool(
            pool_src=other,
            pool_dest=self,
        )
