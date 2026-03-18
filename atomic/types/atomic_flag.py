from __future__ import annotations
from threading import RLock
from pydantic import BaseModel, Field, PrivateAttr, ConfigDict
from ..definitions.locally_atomic_object import _LAILA_LOCALLY_ATOMIC_OBJECT

class AtomicFlag(_LAILA_LOCALLY_ATOMIC_OBJECT, BaseModel):
    """
    Thread-safe boolean flag with atomic set/clear/toggle/get operations.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)

    value: bool = Field(default=False)
    _lock: RLock = PrivateAttr(default_factory=RLock)

    # Set the flag to True
    def set(self) -> None:
        with self._lock:
            self.value = True

    # Set the flag to False
    def clear(self) -> None:
        with self._lock:
            self.value = False

    # Toggle the flag
    def toggle(self) -> None:
        with self._lock:
            self.value = not self.value

    # Get the current value
    def is_set(self) -> bool:
        with self._lock:
            return self.value

    # Set the value explicitly
    def set_to(self, state: bool) -> None:
        with self._lock:
            self.value = bool(state)

    # Atomic context manager for direct modification
    class _Atomic:
        def __init__(self, parent: "AtomicFlag"):
            self._p = parent
        def __enter__(self) -> "AtomicFlag":
            self._p._lock.acquire()
            return self._p
        def __exit__(self, exc_type, exc, tb):
            self._p._lock.release()

    def atomic(self) -> "_Atomic":
        return AtomicFlag._Atomic(self)

    def __repr__(self) -> str:
        with self._lock:
            return f"AtomicFlag({self.value})"
