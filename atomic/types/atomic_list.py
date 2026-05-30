"""Thread-safe generic list with snapshot iterators and atomic batching.

:class:`AtomicList` wraps a Python list with a
:class:`threading.RLock` and provides:

- The standard mutable-sequence API (``__getitem__``, ``__setitem__``,
  ``append``, ``extend``, ``pop``, ``remove``, ``clear``, ...) -- all
  guarded by the lock.
- *Snapshot* iteration: ``__iter__`` returns an iterator over a copy
  of the list taken under the lock, so external code can iterate
  without holding the lock and without seeing the list mutate
  mid-iteration.
- An :meth:`atomic` context manager that yields a "view" object
  capable of doing batch mutations under a single lock acquisition,
  so a sequence of operations appears atomic to other threads.

Use it where multiple threads need to share a mutable list (job
queues, deferred-callback registries, observer lists) without
playing whack-a-mole with manual locking.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from threading import RLock
from typing import Generic, TypeVar, overload

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from ..definitions.locally_atomic_object import _LAILA_LOCALLY_ATOMIC_OBJECT

T = TypeVar("T")


class AtomicList(_LAILA_LOCALLY_ATOMIC_OBJECT, BaseModel, Generic[T]):
    """
    Thread-safe list with common list operations.
    - All mutations are guarded by a re-entrant lock.
    - Reads that return iterators/slices produce *snapshots* to avoid surprises.
    - Use `with lst.atomic():` to batch multiple mutations under one lock.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    value: list[T] = Field(default_factory=list)
    _lock: RLock = PrivateAttr(default_factory=RLock)

    def __len__(self) -> int:
        """Return the number of elements."""
        with self._lock:
            return len(self.value)

    def __repr__(self) -> str:
        """Return a string representation of the list."""
        with self._lock:
            return f"AtomicList({self.value!r})"

    def __iter__(self) -> Iterator[T]:
        """Iterate over a snapshot of the list."""
        with self._lock:
            return iter(list(self.value))

    @overload
    def __getitem__(self, idx: int) -> T: ...
    @overload
    def __getitem__(self, idx: slice) -> list[T]: ...
    def __getitem__(self, idx):  # type: ignore[override]
        """Return the element at *idx*, or a snapshot slice."""
        with self._lock:
            if isinstance(idx, slice):
                return self.value[idx.start : idx.stop : idx.step]  # snapshot slice
            return self.value[idx]

    @overload
    def __setitem__(self, idx: int, item: T) -> None: ...
    @overload
    def __setitem__(self, idx: slice, items: Iterable[T]) -> None: ...
    def __setitem__(self, idx, item) -> None:  # type: ignore[override]
        """Set element(s) at *idx*."""
        with self._lock:
            self.value[idx] = item

    @overload
    def __delitem__(self, idx: int) -> None: ...
    @overload
    def __delitem__(self, idx: slice) -> None: ...
    def __delitem__(self, idx) -> None:  # type: ignore[override]
        """Delete element(s) at *idx*."""
        with self._lock:
            del self.value[idx]

    def append(self, item: T) -> None:
        """Append *item* to the end of the list."""
        with self._lock:
            self.value.append(item)

    def extend(self, items: Iterable[T]) -> None:
        """Extend the list with elements from *items*."""
        with self._lock:
            self.value.extend(items)

    def insert(self, index: int, item: T) -> None:
        """Insert *item* before *index*."""
        with self._lock:
            self.value.insert(index, item)

    def clear(self) -> None:
        """Remove all elements."""
        with self._lock:
            self.value.clear()

    def remove(self, item: T) -> None:
        """Remove the first occurrence of *item*."""
        with self._lock:
            self.value.remove(item)

    def pop(self, index: int = -1) -> T:
        """Remove and return the item at *index* (default last)."""
        with self._lock:
            return self.value.pop(index)

    def count(self, item: T) -> int:
        """Return the number of occurrences of *item*."""
        with self._lock:
            return self.value.count(item)

    def index(self, item: T, start: int = 0, stop: int | None = None) -> int:
        """Return the index of the first occurrence of *item*."""
        with self._lock:
            if stop is None:
                return self.value.index(item, start)
            return self.value.index(item, start, stop)

    def to_list(self) -> list[T]:
        """Return a snapshot (shallow copy) of the list."""
        with self._lock:
            return list(self.value)

    def set_at(self, index: int, item: T) -> None:
        """Set the element at *index* to *item*."""
        with self._lock:
            self.value[index] = item

    def get_at(self, index: int) -> T:
        """Return the element at *index*."""
        with self._lock:
            return self.value[index]

    def slice(self, start: int | None, stop: int | None, step: int | None = None) -> list[T]:
        """Snapshot slice, equivalent to self.value[start:stop:step]."""
        with self._lock:
            return self.value[slice(start, stop, step)]

    def trim(self, start: int | None, stop: int | None) -> None:
        """
        In-place: keep only items in [start:stop], like list slicing, shrink the list.
        Supports negative indices.
        """
        with self._lock:
            n = len(self.value)
            s = 0 if start is None else (start if start >= 0 else n + start)
            e = n if stop is None else (stop if stop >= 0 else n + stop)
            s = max(0, min(s, n))
            e = max(0, min(e, n))
            if s >= e:
                self.value.clear()
            else:
                self.value[:] = self.value[s:e]

    class _Atomic:
        """Context manager that exposes the underlying list while the lock is held."""

        def __init__(self, parent: AtomicList[T]):
            """Initialize with the parent list."""
            self._p = parent

        def __enter__(self) -> list[T]:
            """Acquire the lock and return the raw list."""
            self._p._lock.acquire()
            return self._p.value

        def __exit__(self, exc_type, exc, tb):
            """Release the lock."""
            self._p._lock.release()

    def atomic(self) -> _Atomic:
        """
        Use:
            with lst.atomic() as L:
                L.append(x)
                L.extend([...])
                # Several ops under one lock acquisition
        """
        return AtomicList._Atomic(self)
