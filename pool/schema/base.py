"""Base schema for all LAILA storage pool implementations."""
from typing import Optional, Any, Dict, Iterable, Iterator, Mapping
from pydantic import BaseModel, Field, PrivateAttr
from ...basics.definitions.cli_capable import CLIExempt, _LAILA_CLI_CAPABLE_CLASS
from ...entry import Entry
from contextlib import suppress, contextmanager
import threading
from ...atomic.definitions.locally_atomic_identifiable_object import _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT
from ...macros.strings import _POOL_SCOPE
from ...entry.compdata.transformation import TransformationSequence


class _LAILA_IDENTIFIABLE_POOL(_LAILA_CLI_CAPABLE_CLASS, _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT):
    """Abstract base class for all LAILA storage pools.

    A pool is a key-value store that persists serialized ``Entry`` blobs.
    Subclasses implement ``_read``, ``_write``, ``_delete``,
    ``_keys``, ``_exists``, and ``_empty`` against a concrete backend (S3,
    Redis, SQLite, etc.).

    Pools support a proxy relationship via ``_proxy_to``.  When a pool is
    a proxy for another (its *origin*), reads that miss local storage
    automatically fall back to the origin and cache the result locally.
    """

    _scopes: list[str] = PrivateAttr(default_factory=lambda: list([_POOL_SCOPE]))
    _proxy_to: Optional[Any] = PrivateAttr(default=None)
    resource: Dict[str, Any] = CLIExempt(default_factory=dict)
    batch_accelerated: bool = Field(default=False)
    transformations: Optional[TransformationSequence] = CLIExempt(default=None)


    class Config:
        arbitrary_types_allowed = True

    @property
    def pool_id(self) -> str:
        """Unique identifier for this pool, aliased from ``global_id``."""
        return self.global_id

    # -------- Proxy properties --------
    @property
    def proxy(self):
        """Write-only property.  ``origin.proxy = cache`` sets ``cache._proxy_to = origin``."""
        return None

    @proxy.setter
    def proxy(self, pool):
        if pool is not None:
            pool._proxy_to = self

    @property
    def proxy_to(self):
        """The origin pool this pool is a cache/proxy for, or ``None``."""
        return self._proxy_to

    @proxy_to.setter
    def proxy_to(self, pool):
        self._proxy_to = pool

    def __lshift__(self, other):
        """``cache << origin``: cache becomes proxy for origin.

        Returns *other* so that chains like ``mem << hdf5 << s3`` work.
        """
        other.proxy = self
        return other

    def __rshift__(self, other):
        """``origin >> cache``: cache becomes proxy for origin.

        Returns *other* so that chains like ``s3 >> hdf5 >> mem`` work.
        """
        self.proxy = other
        return other

    # -------- Internal storage hooks (override in subclasses) --------
    def _read(self, key: str) -> Optional[Any]:
        """Read from own storage.  Subclasses override this."""
        with self.atomic():
            if key not in self.resource:
                return None
            blob = self.resource[key]
        if blob is None:
            return None
        return blob

    def _write(self, key: str, value: Any) -> None:
        """Write to own storage.  Subclasses override this."""
        with self.atomic():
            self.resource[key] = value

    def _delete(self, key: str) -> None:
        """Delete from own storage.  Subclasses override this."""
        with self.atomic():
            if key in self.resource:
                del self.resource[key]

    def _exists(self, key: str) -> bool:
        """Check own storage.  Subclasses override this."""
        with self.atomic():
            return key in self.resource

    def _keys(self, as_generator: bool = False) -> Iterable[str]:
        """List own keys.  Subclasses override this."""
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
        """Clear own storage.  Subclasses override this."""
        with self.atomic():
            self.resource.clear()

    # -------- Proxy-aware public API --------
    def __getitem__(self, key) -> Optional[Any]:
        """Retrieve the stored blob for *key*, or a ``PoolWrapper`` for a ``Manifest``.

        If the key is not found locally and ``_proxy_to`` is set, the
        request falls back to the origin pool.  A successful fallback
        caches the value in this pool before returning.
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
        """Store *entry* under *key*.  Local only, no propagation."""
        self._write(key, entry)

    def __delitem__(self, key: str) -> None:
        """Delete the entry for *key*.  Local only, no propagation."""
        self._delete(key)

    def empty(self) -> None:
        """Remove all entries from the pool.  Local only, no propagation."""
        self._empty()

    def exists(self, key: str) -> bool:
        """Return ``True`` if *key* is present.  Local only, no propagation."""
        return self._exists(key)

    def __contains__(self, key: str) -> bool:
        """Check membership, delegates to :meth:`exists`."""
        return self.exists(key)

    def keys(self, as_generator: bool = False) -> Iterable[str]:
        """Return the keys stored in this pool.

        Parameters
        ----------
        as_generator : bool, optional
            If ``False`` (default), return a snapshot list.  If ``True``,
            return an iterator that holds the lock for its lifetime.

        Returns
        -------
        Iterable[str]
            Pool keys.
        """
        return self._keys(as_generator=as_generator)

    def sync(self) -> None:
        """Flush any in-memory cache to the backing store.

        Raises
        ------
        NotImplementedError
            If the pool is cacheless and operates directly on storage.
        """
        raise NotImplementedError("Sync is not implemented for this pool, the pool is cacheless, i.e. operations are immediately executed on the underlying storage.")

    def __le__(self, other: Any):
        """Duplicate *other* pool's contents into this pool via the active policy."""
        from ... import active_policy

        if not isinstance(other, (_LAILA_IDENTIFIABLE_POOL, str)):
            return NotImplemented

        return active_policy.central.memory._duplicate_pool(
            pool_src=other,
            pool_dest=self,
        )
