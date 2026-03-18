from __future__ import annotations
from collections.abc import MutableMapping, Iterable, Mapping
from threading import RLock
from typing import TypeVar, Generic, Iterator, Callable, Any, Optional, ClassVar
from contextvars import ContextVar
from pydantic import BaseModel, PrivateAttr, Field, ConfigDict
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
    _order: list[K] = PrivateAttr(default_factory=list)

    # context-local "current" AtomicDict (thread- & async-task-safe)
    _current: ClassVar[ContextVar[Optional["AtomicDict[Any, Any]"]]] = ContextVar(
        "AtomicDict_current", default=None
    )

    # --- init ---
    def __init__(self, *args, **kwargs):
        # Allow a dict positional argument for convenience
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
        with self._lock:
            self._order = list(self.data.keys())

    # --- internal: order integrity (LOCK HELD) ---
    def _ensure_order_synced(self) -> None:
        if len(self._order) != len(self.data):
            self._order = list(self.data.keys())
            return
        seen = set()
        for k in self._order:
            if k not in self.data or k in seen:
                self._order = list(self.data.keys())
                return
            seen.add(k)

    def reindex(self) -> None:
        with self._lock:
            self._order = list(self.data.keys())

    # --- internal: mutations (LOCK HELD) ---
    def _set_nolock(self, key: K, value: V) -> None:
        if key not in self.data:
            self._order.append(key)
        self.data[key] = value

    def _del_nolock(self, key: K) -> None:
        del self.data[key]
        self._order.remove(key)

    # --- core mapping ---
    def __getitem__(self, key: K) -> V:
        with self._lock:
            return self.data[key]

    def __setitem__(self, key: K, value: V) -> None:
        with self._lock:
            self._set_nolock(key, value)

    def __delitem__(self, key: K) -> None:
        with self._lock:
            self._del_nolock(key)

    def __len__(self) -> int:
        with self._lock:
            return len(self.data)

    def __iter__(self) -> Iterator[K]:
        with self._lock:
            self._ensure_order_synced()
            return iter(list(self._order))  # snapshot

    def __contains__(self, key: object) -> bool:  # type: ignore[override]
        with self._lock:
            return key in self.data

    def __repr__(self) -> str:
        with self._lock:
            self._ensure_order_synced()
            ordered = {k: self.data[k] for k in self._order}
            return f"AtomicDict({ordered!r})"

    # --- dict-like ---
    def get(self, key: K, default: Optional[V] = None) -> Optional[V]:  # type: ignore[override]
        with self._lock:
            return self.data.get(key, default)

    def setdefault(self, key: K, default: V) -> V:
        with self._lock:
            if key in self.data:
                return self.data[key]
            self._set_nolock(key, default)
            return default

    def pop(self, key: K, default: Any = ...):  # type: ignore[override]
        with self._lock:
            if key in self.data:
                val = self.data[key]
                self._del_nolock(key)
                return val
            if default is ...:
                raise KeyError(key)
            return default

    def popitem(self) -> tuple[K, V]:
        with self._lock:
            self._ensure_order_synced()
            if not self._order:
                raise KeyError("AtomicDict is empty")
            k = self._order.pop()
            v = self.data.pop(k)
            return k, v

    def pop_next(self) -> tuple[K, V]:
        with self._lock:
            self._ensure_order_synced()
            if not self._order:
                raise KeyError("AtomicDict is empty")
            k = self._order.pop(0)
            v = self.data.pop(k)
            return k, v

    def clear(self) -> None:
        with self._lock:
            self.data.clear()
            self._order.clear()

    def update(
        self,
        other: Optional[Iterable[tuple[K, V]] | Mapping[K, V]] = None, /, **kwargs: V
    ) -> None:  # type: ignore[override]
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

    # --- ordered snapshots ---
    def keys(self) -> list[K]:  # type: ignore[override]
        with self._lock:
            self._ensure_order_synced()
            return list(self._order)

    def values(self) -> list[V]:  # type: ignore[override]
        with self._lock:
            self._ensure_order_synced()
            return [self.data[k] for k in self._order]

    def items(self) -> list[tuple[K, V]]:  # type: ignore[override]
        with self._lock:
            self._ensure_order_synced()
            return [(k, self.data[k]) for k in self._order]

    # --- index helpers ---
    def item_at(self, index: int) -> tuple[K, V]:
        with self._lock:
            self._ensure_order_synced()
            n = len(self._order)
            if index < 0:
                index += n
            if index < 0 or index >= n:
                raise IndexError("Index out of range")
            k = self._order[index]
            return k, self.data[k]

    def key_at(self, index: int) -> K:
        with self._lock:
            return self.item_at(index)[0]

    def value_at(self, index: int) -> V:
        with self._lock:
            return self.item_at(index)[1]

    # --- in-place slicing ---
    def trim(self, start: int | None = None, end: int | None = None) -> None:
        """Keep only items in [start:end) by insertion order (end exclusive)."""
        with self._lock:
            self._ensure_order_synced()
            n = len(self._order)

            s = 0 if start is None else start
            e = n if end   is None else end
            if s < 0: s += n
            if e < 0: e += n
            if s < 0: s = 0
            if e > n: e = n

            if s >= e:
                self.data.clear()
                self._order.clear()
                return

            keep_keys = self._order[s:e]
            keep_set = set(keep_keys)
            for k in list(self.data.keys()):
                if k not in keep_set:
                    del self.data[k]
            self._order[:] = keep_keys

    # --- compute/increment helpers ---
    def compute(self, key: K, fn: Callable[[Optional[V]], Optional[V]]) -> Optional[V]:
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
        with self._lock:
            val = self.data.get(key, default)
            val = val + delta
            self._set_nolock(key, val)
            return val

    # --- pretty print ---
    def pretty(self, indent: int = 2) -> str:
        with self._lock:
            self._ensure_order_synced()
            lines = ["AtomicDict {"]
            for k in self._order:
                lines.append(" " * indent + f"{k!r}: {self.data[k]!r},")
            lines.append("}")
            return "\n".join(lines)

    # --- atomic context (proxy) ---
    class _AtomicView:
        def __init__(self, parent: "AtomicDict[K, V]"):
            self._p = parent
        def __setitem__(self, key: K, value: V) -> None:
            self._p._set_nolock(key, value)
        def __delitem__(self, key: K) -> None:
            self._p._del_nolock(key)
        def update(self, other: Optional[Iterable[tuple[K, V]] | Mapping[K, V]] = None, /, **kwargs: V) -> None:
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
        def __init__(self, parent: "AtomicDict[K, V]", hint: str = ""):
            if not isinstance(hint, str):
                raise TypeError("hint must be a str")
            self._p = parent
            self.hint: str = hint           # reserved for future use
            self._token: Optional[Any] = None   # ContextVar token

        def __enter__(self) -> "AtomicDict._AtomicView":
            self._p._lock.acquire()
            self._p._ensure_order_synced()
            # expose this instance as "current"
            self._token = AtomicDict._current.set(self._p)
            return AtomicDict._AtomicView(self._p)

        def __exit__(self, exc_type, exc, tb):
            if self._token is not None:
                AtomicDict._current.reset(self._token)
                self._token = None
            self._p._lock.release()

    def atomic(self, hint: str = "") -> "_Atomic":
        if not isinstance(hint, str):
            raise TypeError("hint must be a str")
        return AtomicDict._Atomic(self, hint)

    # get the current instance inside an atomic context
    @classmethod
    def current(cls) -> "AtomicDict[Any, Any]":
        d = cls._current.get()
        if d is None:
            raise RuntimeError("AtomicDict.current() called outside an AtomicDict context")
        return d

    # convenience runner for arg-less functions
    def run_atomic(self, fn: Callable[[], T], hint: str = "") -> T:
        if not isinstance(hint, str):
            raise TypeError("hint must be a str")
        with self.atomic(hint=hint):
            return fn()
