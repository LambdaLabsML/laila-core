"""Thread-safe mutable string."""
from __future__ import annotations
from threading import RLock
from pydantic import BaseModel, Field, PrivateAttr, ConfigDict
from ..definitions.locally_atomic_object import _LAILA_LOCALLY_ATOMIC_OBJECT


class AtomicStr(_LAILA_LOCALLY_ATOMIC_OBJECT, BaseModel):
    """Thread-safe string with atomic set, append, clear, and get operations."""
    model_config = ConfigDict(arbitrary_types_allowed=True)

    value: str = Field(default="")
    _lock: RLock = PrivateAttr(default_factory=RLock)

    def set(self, new_value: str) -> None:
        """Replace the string with *new_value*."""
        with self._lock:
            self.value = str(new_value)

    def get(self) -> str:
        """Return the current string value."""
        with self._lock:
            return self.value

    def append(self, suffix: str) -> str:
        """Append *suffix* and return the resulting string."""
        with self._lock:
            self.value += str(suffix)
            return self.value

    def clear(self) -> None:
        """Reset the string to empty."""
        with self._lock:
            self.value = ""

    def length(self) -> int:
        """Return the length of the string."""
        with self._lock:
            return len(self.value)

    class _Atomic:
        """Context manager that holds the string's lock."""

        def __init__(self, parent: "AtomicStr"):
            """Initialize with the parent string."""
            self._p = parent
        def __enter__(self) -> "AtomicStr":
            self._p._lock.acquire()
            return self._p
        def __exit__(self, exc_type, exc, tb):
            self._p._lock.release()

    def atomic(self) -> "_Atomic":
        """Return a context manager for batched lock-held operations."""
        return AtomicStr._Atomic(self)

    def __str__(self) -> str:
        """Return the string value."""
        return self.get()

    def __repr__(self) -> str:
        """Return a string representation."""
        return f"AtomicStr({self.get()!r})"
