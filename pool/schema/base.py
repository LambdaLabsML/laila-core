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
    Subclasses implement ``__getitem__``, ``__setitem__``, ``__delitem__``,
    ``keys``, ``exists``, and ``empty`` against a concrete backend (S3,
    Redis, SQLite, etc.).
    """

    _scopes: list[str] = PrivateAttr(default_factory=lambda: list([_POOL_SCOPE]))
    resource: Dict[str, Any] = CLIExempt(default_factory=dict)
    batch_accelerated: bool = Field(default=False)
    transformations: Optional[TransformationSequence] = CLIExempt(default=None)


    class Config:
        arbitrary_types_allowed = True

    @property
    def pool_id(self) -> str:
        """Unique identifier for this pool, aliased from ``global_id``."""
        return self.global_id



    # -------- Mapping-like API --------
    def __getitem__(self, key) -> Optional[Any]:
        """Retrieve the stored blob for *key*, or a ``PoolWrapper`` for a ``Manifest``."""
        from ...policy.central.memory.schema.manifest import Manifest
        if isinstance(key, Manifest):
            from .pool_wrapper import PoolWrapper
            return PoolWrapper(pool=self, manifest=key)

        with self.atomic():
            if key not in self.resource:
                return None
            blob = self.resource[key]

        if blob is None:
            return None

        return blob

    def __setitem__(self, key: str, entry: Any) -> None:
        """Store *entry* under *key*."""
        value = entry

        with self.atomic():
            self.resource[key] = value
            
    def __delitem__(self, key: str) -> None:
        """Delete the entry for *key* if present."""
        with self.atomic():
            if key in self.resource:
                del self.resource[key]

    def empty(self) -> None:
        """Remove all entries from the pool."""
        with self.atomic():
            self.resource.clear()

    # -------- Utilities --------
    def exists(self, key: str) -> bool:
        """Return ``True`` if *key* is present in the pool."""
        with self.atomic():
            return key in self.resource

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
            Pool keys as a list or generator.
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

    