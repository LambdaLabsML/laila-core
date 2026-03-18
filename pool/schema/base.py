from typing import Optional, Any, Dict, Iterable, Iterator, Mapping
from pydantic import BaseModel, Field, PrivateAttr
from ...entry import Entry
from contextlib import suppress, contextmanager
import threading
from ...atomic.definitions.locall_atomic_identifiable_object import _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT
from ...macros.strings import _POOL_SCOPE
from ...entry.compdata.transformation import TransformationSequence


class _LAILA_IDENTIFIABLE_POOL(_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT):

    _scopes: list[str] = PrivateAttr(default_factory=lambda: list([_POOL_SCOPE]))
    resource: Dict[str, Any] = Field(default_factory=dict)
    batch_accelerated: bool = Field(default=False)
    transformations: Optional[TransformationSequence] = Field(default=None)


    class Config:
        arbitrary_types_allowed = True

    @property
    def pool_id(self) -> str:
        return self.global_id



    # -------- Mapping-like API --------
    def __getitem__(self, key: str) -> Optional[Any]:
        with self.atomic():
            if key not in self.resource:
                return None
            blob = self.resource[key]

        if blob is None:
            return None

        return blob

    def __setitem__(self, key: str, entry: Any) -> None:
        value = entry

        with self.atomic():
            self.resource[key] = value
            
    def __delitem__(self, key: str) -> None:
        with self.atomic():
            if key in self.resource:
                del self.resource[key]

    def empty(self) -> None:
        """Remove all entries from the pool."""
        with self.atomic():
            self.resource.clear()

    # -------- Utilities --------
    def exists(self, key: str) -> bool:
        with self.atomic():
            return key in self.resource

    def __contains__(self, key: str) -> bool:
        return self.exists(key)
    
    def keys(self, as_generator: bool = False) -> Iterable[str]:
        """
        If as_generator is False: returns a snapshot list of keys.
        If as_generator is True: returns an iterator that holds the lock
        for the duration of the iteration. Be sure to fully consume or
        close the iterator to release the lock promptly.
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
        raise NotImplementedError("Sync is not implemented for this pool, the pool is cacheless, i.e. operations are immediately executed on the underlying storage.")

    def __le__(self, other: Any):
        from ... import active_policy

        if not isinstance(other, (_LAILA_IDENTIFIABLE_POOL, str)):
            return NotImplemented

        return active_policy.central.memory._duplicate_pool(
            pool_src=other,
            pool_dest=self,
        )

    