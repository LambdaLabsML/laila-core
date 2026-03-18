from __future__ import annotations
from threading import RLock
from typing import TypeVar, Generic, Iterable, Iterator, overload, Any, Optional
from pydantic import BaseModel, Field, PrivateAttr, ConfigDict
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

    # ---------- basic protocol ----------
    def __len__(self) -> int:
        with self._lock:
            return len(self.value)

    def __repr__(self) -> str:
        with self._lock:
            return f"AtomicList({self.value!r})"

    def __iter__(self) -> Iterator[T]:
        with self._lock:
            return iter(list(self.value))  # snapshot

    @overload
    def __getitem__(self, idx: int) -> T: ...
    @overload
    def __getitem__(self, idx: slice) -> list[T]: ...
    def __getitem__(self, idx):  # type: ignore[override]
        with self._lock:
            if isinstance(idx, slice):
                return self.value[idx.start:idx.stop:idx.step]  # snapshot slice
            return self.value[idx]

    @overload
    def __setitem__(self, idx: int, item: T) -> None: ...
    @overload
    def __setitem__(self, idx: slice, items: Iterable[T]) -> None: ...
    def __setitem__(self, idx, item) -> None:  # type: ignore[override]
        with self._lock:
            self.value[idx] = item

    @overload
    def __delitem__(self, idx: int) -> None: ...
    @overload
    def __delitem__(self, idx: slice) -> None: ...
    def __delitem__(self, idx) -> None:  # type: ignore[override]
        with self._lock:
            del self.value[idx]

    # ---------- mutations ----------
    def append(self, item: T) -> None:
        with self._lock:
            self.value.append(item)

    def extend(self, items: Iterable[T]) -> None:
        with self._lock:
            self.value.extend(items)

    def insert(self, index: int, item: T) -> None:
        with self._lock:
            self.value.insert(index, item)

    def clear(self) -> None:
        with self._lock:
            self.value.clear()

    def remove(self, item: T) -> None:
        with self._lock:
            self.value.remove(item)

    def pop(self, index: int = -1) -> T:
        with self._lock:
            return self.value.pop(index)

    # ---------- queries ----------
    def count(self, item: T) -> int:
        with self._lock:
            return self.value.count(item)

    def index(self, item: T, start: int = 0, stop: Optional[int] = None) -> int:
        with self._lock:
            if stop is None:
                return self.value.index(item, start)
            return self.value.index(item, start, stop)

    def to_list(self) -> list[T]:
        """Return a snapshot (shallow copy) of the list."""
        with self._lock:
            return list(self.value)

    # ---------- convenience ----------
    def set_at(self, index: int, item: T) -> None:
        with self._lock:
            self.value[index] = item

    def get_at(self, index: int) -> T:
        with self._lock:
            return self.value[index]

    def slice(self, start: Optional[int], stop: Optional[int], step: Optional[int] = None) -> list[T]:
        """Snapshot slice, equivalent to self.value[start:stop:step]."""
        with self._lock:
            return self.value[slice(start, stop, step)]

    def trim(self, start: Optional[int], stop: Optional[int]) -> None:
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

    # ---------- atomic context for batch ops ----------
    class _Atomic:
        def __init__(self, parent: "AtomicList[T]"):
            self._p = parent
        def __enter__(self) -> list[T]:
            self._p._lock.acquire()
            # Return the underlying list for efficient batch ops while locked.
            return self._p.value
        def __exit__(self, exc_type, exc, tb):
            self._p._lock.release()

    def atomic(self) -> "_Atomic":
        """
        Use:
            with lst.atomic() as L:
                L.append(x)
                L.extend([...])
                # Several ops under one lock acquisition
        """
        return AtomicList._Atomic(self)
