from __future__ import annotations
from threading import RLock
from pydantic import BaseModel, Field, PrivateAttr, ConfigDict
from ..definitions.locally_atomic_object import _LAILA_LOCALLY_ATOMIC_OBJECT


class AtomicStr(_LAILA_LOCALLY_ATOMIC_OBJECT, BaseModel):
    """
    Thread-safe string with atomic set, append, clear, and get operations.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)

    value: str = Field(default="")
    _lock: RLock = PrivateAttr(default_factory=RLock)

    # Set the string explicitly
    def set(self, new_value: str) -> None:
        with self._lock:
            self.value = str(new_value)

    # Get the current string
    def get(self) -> str:
        with self._lock:
            return self.value

    # Append text to the string
    def append(self, suffix: str) -> str:
        with self._lock:
            self.value += str(suffix)
            return self.value

    # Clear the string
    def clear(self) -> None:
        with self._lock:
            self.value = ""

    # Length of the string
    def length(self) -> int:
        with self._lock:
            return len(self.value)

    # Atomic context manager
    class _Atomic:
        def __init__(self, parent: "AtomicStr"):
            self._p = parent
        def __enter__(self) -> "AtomicStr":
            self._p._lock.acquire()
            return self._p
        def __exit__(self, exc_type, exc, tb):
            self._p._lock.release()

    def atomic(self) -> "_Atomic":
        return AtomicStr._Atomic(self)

    # Make it behave like a string
    def __str__(self) -> str:
        return self.get()

    def __repr__(self) -> str:
        return f"AtomicStr({self.get()!r})"
