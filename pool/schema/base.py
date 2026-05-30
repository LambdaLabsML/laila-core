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

from collections.abc import Iterable, Iterator
from typing import Any

from pydantic import ConfigDict, Field, PrivateAttr

from ...atomic.definitions.locally_atomic_identifiable_object import (
    _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT,
)
from ...basics.definitions.cli_capable import _LAILA_CLI_CAPABLE_CLASS, CLIExempt
from ...entry.compdata.transformation import TransformationSequence
from ...macros.strings import _POOL_SCOPE


class _LAILA_IDENTIFIABLE_POOL(_LAILA_CLI_CAPABLE_CLASS, _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT):
    """Abstract base class for laila storage pools.

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
    """

    _scopes: list[str] = PrivateAttr(default_factory=lambda: list([_POOL_SCOPE]))
    _proxy_to: Any | None = PrivateAttr(default=None)
    resource: dict[str, Any] = CLIExempt(default_factory=dict)
    batch_accelerated: bool = Field(default=False)
    transformations: TransformationSequence | None = CLIExempt(default=None)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @property
    def pool_id(self) -> str:
        """Unique identifier for this pool. Alias for :attr:`global_id`."""
        return self.global_id

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
            pool._proxy_to = self

    @property
    def proxy_to(self):
        """The origin pool this one is a cache/proxy for, or ``None``."""
        return self._proxy_to

    @proxy_to.setter
    def proxy_to(self, pool):
        self._proxy_to = pool

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

        if self._proxy_to is not None:
            value = self._proxy_to[key]
            if value is not None:
                self._write(key, value)
                return value

        return None

    def __setitem__(self, key: str, entry: Any) -> None:
        """Store *entry* under *key*. Local-only; never propagates to a proxy origin."""
        self._write(key, entry)

    def __delitem__(self, key: str) -> None:
        """Delete the entry for *key*. Local-only; never propagates to a proxy origin."""
        self._delete(key)

    def empty(self) -> None:
        """Remove every entry from this pool. Local-only; never propagates to a proxy origin."""
        self._empty()

    def exists(self, key: str) -> bool:
        """Return ``True`` if *key* is present in this pool. Local-only check."""
        return self._exists(key)

    def __contains__(self, key: str) -> bool:
        """``key in pool`` -- thin alias for :meth:`exists`."""
        return self.exists(key)

    def keys(self, as_generator: bool = False) -> Iterable[str]:
        """Return the keys stored in this pool. Local-only enumeration.

        Parameters
        ----------
        as_generator : bool, default False
            If ``False``, return a snapshot list (cheap-ish, O(N)
            memory). If ``True``, return an iterator that holds the
            atomic lock for its full lifetime -- prefer this when keys
            are expensive to materialize but the consumer wants to
            stream.

        Returns
        -------
        Iterable[str]
            Pool keys (snapshot list or generator depending on
            *as_generator*).
        """
        return self._keys(as_generator=as_generator)

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
