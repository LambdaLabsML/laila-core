"""Thread-safe, insertion-ordered dictionary with atomic context support.

:class:`AtomicDict` is a drop-in :class:`MutableMapping` whose every
mutation is guarded by an internal :class:`threading.RLock`. On top
of the standard mapping API it adds:

- *Insertion-ordered* iteration: the backing ``data`` dict (itself
  insertion-ordered) is the single order authority, so keyed deletes
  are O(1) and iteration never goes stale after out-of-band edits of
  ``data``. A positional index (``_order``) is rebuilt lazily only for
  the positional accessors. Useful when callers need to act on "the
  next item to come in" without scanning.
- Positional accessors (:meth:`item_at`, :meth:`key_at`,
  :meth:`value_at`) and an in-place slice (:meth:`trim`) that work
  against the insertion order.
- An :meth:`atomic` context manager that yields a "view" object
  capable of doing batch mutations *under the lock* (so the whole
  batch appears atomic to other threads). Combined with the
  context-local :meth:`AtomicDict.current` accessor, code inside the
  block can also reach the dict without re-receiving it as a
  parameter.
- Atomic compute-and-set (:meth:`compute`) and increment
  (:meth:`increment`) helpers for common read-modify-write patterns.

Used heavily inside laila for things like the active-policy registry,
future-bank tables, taskforce queues, and the central memory hint /
record indexes -- anywhere a plain dict would be a race-condition
waiting to happen.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, MutableMapping
from contextvars import ContextVar
from threading import RLock
from typing import Any, ClassVar, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from ...basics.definitions.laila_object import _now_creation_timestamp
from ..definitions.locally_atomic_object import _LAILA_LOCALLY_ATOMIC_OBJECT

K = TypeVar("K")
V = TypeVar("V")
T = TypeVar("T")


class AtomicDict(_LAILA_LOCALLY_ATOMIC_OBJECT, BaseModel, MutableMapping[K, V], Generic[K, V]):
    """
    Thread-safe, insertion-ordered dict with index/slice ops and robust order syncing.

    Supports:
      - with d.atomic(hint="..."): mutual exclusion for the instance; 'hint' is a str
      - AtomicDict.current(): access the instance currently under an atomic block (thread/async local)
      - d.run_atomic(fn, hint="..."): run a no-arg function with the lock held
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    data: dict[K, V] = Field(default_factory=dict)
    _lock: RLock = PrivateAttr(default_factory=RLock)
    _order: list[K] | None = PrivateAttr(default=None)

    # context-local "current" AtomicDict (thread- & async-task-safe)
    _current: ClassVar[ContextVar[AtomicDict[Any, Any] | None]] = ContextVar(
        "AtomicDict_current", default=None
    )

    def __init__(self, *args, **kwargs):
        """Create an ``AtomicDict``, optionally from a plain dict.

        Parameters
        ----------
        *args
            At most one positional ``dict`` argument.
        **kwargs
            Passed through; use ``data=`` to supply initial contents.

        Raises
        ------
        TypeError
            On conflicting or unexpected arguments.
        """
        if args and isinstance(args[0], dict):
            if "data" in kwargs:
                raise TypeError("Cannot pass both dict positional arg and data keyword arg")
            kwargs["data"] = args[0]
            args = args[1:]
        if args:
            raise TypeError("AtomicDict accepts at most one positional dict argument")
        data = kwargs.pop("data", {})
        if kwargs:
            unexpected = ", ".join(sorted(kwargs))
            raise TypeError(f"Unexpected keyword arguments: {unexpected}")
        instance = type(self).model_construct(data=data)
        object.__setattr__(self, "__dict__", instance.__dict__)
        object.__setattr__(self, "__pydantic_fields_set__", instance.__pydantic_fields_set__)
        object.__setattr__(self, "__pydantic_extra__", instance.__pydantic_extra__)
        object.__setattr__(self, "__pydantic_private__", instance.__pydantic_private__)
        # ``model_construct`` skips ``_LAILA_OBJECT.__init__``; stamp here.
        self._creation_timestamp = _now_creation_timestamp()
        self._order = None

    def _positional(self) -> list[K]:
        """Return the positional key index, rebuilding it from ``data`` when stale.

        ``data`` is the order authority; ``_order`` is only a cache for
        the positional accessors (:meth:`item_at`, :meth:`key_at`,
        :meth:`value_at`, :meth:`trim`). It is dropped (set to ``None``)
        by every mutation and rebuilt here in O(n) on the next positional
        access, so the mapping hot paths (``__setitem__``, ``pop``,
        ``pop_next``, iteration) never pay for it.
        """
        order = self._order
        if order is None or len(order) != len(self.data):
            order = list(self.data.keys())
            self._order = order
        return order

    def reindex(self) -> None:
        """Rebuild the positional index from the underlying dict.

        Kept for callers that edit ``self.data`` out of band; iteration
        and ``keys()`` read ``data`` directly and never need it.
        """
        with self._lock:
            self._order = list(self.data.keys())

    def _set_nolock(self, key: K, value: V) -> None:
        """Insert or update *key* without acquiring the lock."""
        if key not in self.data:
            order = self._order
            if order is not None and len(order) == len(self.data):
                order.append(key)
            else:
                self._order = None
        self.data[key] = value

    def _del_nolock(self, key: K) -> None:
        """Delete *key* without acquiring the lock (O(1))."""
        del self.data[key]
        self._order = None

    def __getitem__(self, key: K) -> V:
        """Return the value for *key*, raising ``KeyError`` if missing."""
        with self._lock:
            return self.data[key]

    def __setitem__(self, key: K, value: V) -> None:
        """Set *key* to *value* under the lock."""
        with self._lock:
            self._set_nolock(key, value)

    def __delitem__(self, key: K) -> None:
        """Delete *key* under the lock."""
        with self._lock:
            self._del_nolock(key)

    def __len__(self) -> int:
        """Return the number of items."""
        with self._lock:
            return len(self.data)

    def __iter__(self) -> Iterator[K]:
        """Iterate over keys in insertion order (snapshot)."""
        with self._lock:
            return iter(list(self.data.keys()))  # snapshot

    def __contains__(self, key: object) -> bool:  # type: ignore[override]
        """Return ``True`` if *key* is present."""
        with self._lock:
            return key in self.data

    def __repr__(self) -> str:
        """Return an ordered string representation."""
        with self._lock:
            return f"AtomicDict({dict(self.data)!r})"

    def get(self, key: K, default: V | None = None) -> V | None:  # type: ignore[override]
        """Return the value for *key*, or *default* if absent."""
        with self._lock:
            return self.data.get(key, default)

    def setdefault(self, key: K, default: V) -> V:
        """Return the value for *key*, inserting *default* if absent."""
        with self._lock:
            if key in self.data:
                return self.data[key]
            self._set_nolock(key, default)
            return default

    def pop(self, key: K, default: Any = ...):  # type: ignore[override]
        """Remove and return the value for *key*, or *default* if absent."""
        with self._lock:
            if key in self.data:
                val = self.data[key]
                self._del_nolock(key)
                return val
            if default is ...:
                raise KeyError(key)
            return default

    def popitem(self) -> tuple[K, V]:
        """Remove and return the last inserted ``(key, value)`` pair."""
        with self._lock:
            if not self.data:
                raise KeyError("AtomicDict is empty")
            k, v = self.data.popitem()
            order = self._order
            if order is not None and order and order[-1] == k:
                order.pop()
            else:
                self._order = None
            return k, v

    def pop_next(self) -> tuple[K, V]:
        """Remove and return the *first* inserted ``(key, value)`` pair.

        Hot path for queue-style consumers (the taskforce dispatcher pops
        one item per task): the backing ``dict`` is insertion-ordered and
        authoritative, so its first key *is* the oldest entry. O(1).
        """
        with self._lock:
            if not self.data:
                raise KeyError("AtomicDict is empty")
            k = next(iter(self.data))
            v = self.data.pop(k)
            self._order = None
            return k, v

    def clear(self) -> None:
        """Remove all items."""
        with self._lock:
            self.data.clear()
            self._order = None

    def update(
        self, other: Iterable[tuple[K, V]] | Mapping[K, V] | None = None, /, **kwargs: V
    ) -> None:  # type: ignore[override]
        """Merge items from *other* and/or keyword arguments."""
        with self._lock:
            if other is not None:
                if hasattr(other, "keys"):
                    for k in other:  # type: ignore[assignment]
                        self._set_nolock(k, other[k])  # type: ignore[index]
                else:
                    for k, v in other:  # type: ignore[misc]
                        self._set_nolock(k, v)
            for k, v in kwargs.items():
                self._set_nolock(k, v)

    def keys(self) -> list[K]:  # type: ignore[override]
        """Return a snapshot list of keys in insertion order."""
        with self._lock:
            return list(self.data.keys())

    def values(self) -> list[V]:  # type: ignore[override]
        """Return a snapshot list of values in insertion order."""
        with self._lock:
            return list(self.data.values())

    def items(self) -> list[tuple[K, V]]:  # type: ignore[override]
        """Return a snapshot list of ``(key, value)`` pairs in insertion order."""
        with self._lock:
            return list(self.data.items())

    def item_at(self, index: int) -> tuple[K, V]:
        """Return the ``(key, value)`` pair at positional *index*.

        Parameters
        ----------
        index : int
            Position in insertion order (supports negative indexing).

        Returns
        -------
        tuple[K, V]
            The key-value pair.

        Raises
        ------
        IndexError
            If *index* is out of range.
        """
        with self._lock:
            order = self._positional()
            n = len(order)
            if index < 0:
                index += n
            if index < 0 or index >= n:
                raise IndexError("Index out of range")
            k = order[index]
            if k not in self.data:  # same-length out-of-band edit of ``data``
                order = self._order = list(self.data.keys())
                k = order[index]
            return k, self.data[k]

    def key_at(self, index: int) -> K:
        """Return the key at positional *index*."""
        with self._lock:
            return self.item_at(index)[0]

    def value_at(self, index: int) -> V:
        """Return the value at positional *index*."""
        with self._lock:
            return self.item_at(index)[1]

    # --- in-place slicing ---
    def trim(self, start: int | None = None, end: int | None = None) -> None:
        """Keep only items in [start:end) by insertion order (end exclusive)."""
        with self._lock:
            order = self._order = list(self.data.keys())
            n = len(order)

            s = 0 if start is None else start
            e = n if end is None else end
            if s < 0:
                s += n
            if e < 0:
                e += n
            s = max(s, 0)
            e = min(e, n)

            if s >= e:
                self.data.clear()
                self._order = None
                return

            keep_keys = order[s:e]
            keep_set = set(keep_keys)
            for k in order:
                if k not in keep_set:
                    del self.data[k]
            self._order = keep_keys

    def compute(self, key: K, fn: Callable[[V | None], V | None]) -> V | None:
        """Atomically compute a new value for *key*.

        Parameters
        ----------
        key : K
            The target key.
        fn : callable
            Receives the current value (or ``None``) and returns the new
            value.  If ``None`` is returned, the key is removed.

        Returns
        -------
        V or None
            The new value, or ``None`` if the key was removed.
        """
        with self._lock:
            cur = self.data.get(key, None)
            new = fn(cur)
            if new is None:
                if key in self.data:
                    self._del_nolock(key)
                return None
            self._set_nolock(key, new)
            return new

    def increment(self, key: K, delta: Any = 1, default: Any = 0) -> Any:
        """Add *delta* to the value at *key* (starting from *default*)."""
        with self._lock:
            val = self.data.get(key, default)
            val = val + delta
            self._set_nolock(key, val)
            return val

    def pretty(self, indent: int = 2) -> str:
        """Return a human-readable, multi-line string representation."""
        with self._lock:
            lines = ["AtomicDict {"]
            for k, v in self.data.items():
                lines.append(" " * indent + f"{k!r}: {v!r},")
            lines.append("}")
            return "\n".join(lines)

    class _AtomicView:
        """Proxy object yielded by ``AtomicDict.atomic()`` for lock-held mutations."""

        def __init__(self, parent: AtomicDict[K, V]):
            """Initialize with a reference to the parent dict."""
            self._p = parent

        def __setitem__(self, key: K, value: V) -> None:
            self._p._set_nolock(key, value)

        def __delitem__(self, key: K) -> None:
            self._p._del_nolock(key)

        def update(
            self, other: Iterable[tuple[K, V]] | Mapping[K, V] | None = None, /, **kwargs: V
        ) -> None:
            if other is not None:
                if hasattr(other, "keys"):
                    for k in other:  # type: ignore[assignment]
                        self._p._set_nolock(k, other[k])  # type: ignore[index]
                else:
                    for k, v in other:  # type: ignore[misc]
                        self._p._set_nolock(k, v)
            for k, v in kwargs.items():
                self._p._set_nolock(k, v)

    class _Atomic:
        """Context manager that holds the dict lock and exposes an ``_AtomicView``."""

        def __init__(self, parent: AtomicDict[K, V], hint: str = ""):
            """Initialize with parent dict and optional hint."""
            if not isinstance(hint, str):
                raise TypeError("hint must be a str")
            self._p = parent
            self.hint: str = hint
            self._token: Any | None = None

        def __enter__(self) -> AtomicDict._AtomicView:
            self._p._lock.acquire()
            # expose this instance as "current"
            self._token = AtomicDict._current.set(self._p)
            return AtomicDict._AtomicView(self._p)

        def __exit__(self, exc_type, exc, tb):
            if self._token is not None:
                AtomicDict._current.reset(self._token)
                self._token = None
            self._p._lock.release()

    def atomic(self, hint: str = "") -> _Atomic:
        """Return a context manager for batched, lock-held operations.

        Parameters
        ----------
        hint : str
            Descriptive label (reserved for future diagnostics).

        Returns
        -------
        _Atomic
            Context manager yielding an ``_AtomicView``.
        """
        if not isinstance(hint, str):
            raise TypeError("hint must be a str")
        return AtomicDict._Atomic(self, hint)

    @classmethod
    def current(cls) -> AtomicDict[Any, Any]:
        """Return the ``AtomicDict`` currently held in an atomic block.

        Raises
        ------
        RuntimeError
            If called outside an ``atomic`` context.
        """
        d = cls._current.get()
        if d is None:
            raise RuntimeError("AtomicDict.current() called outside an AtomicDict context")
        return d

    def run_atomic(self, fn: Callable[[], T], hint: str = "") -> T:
        """Run *fn* while holding the lock and return its result."""
        if not isinstance(hint, str):
            raise TypeError("hint must be a str")
        with self.atomic(hint=hint):
            return fn()
