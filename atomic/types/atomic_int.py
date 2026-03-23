"""Thread-safe integer counter."""
from __future__ import annotations
from threading import RLock
from pydantic import BaseModel, Field, PrivateAttr, ConfigDict
from typing import Union
from ..definitions.locally_atomic_object import _LAILA_LOCALLY_ATOMIC_OBJECT

class AtomicInt(_LAILA_LOCALLY_ATOMIC_OBJECT, BaseModel):
    """Thread-safe integer with atomic add, increment, decrement, set, and get operations."""
    model_config = ConfigDict(arbitrary_types_allowed=True)

    value: int = Field(default=0)
    _lock: RLock = PrivateAttr(default_factory=RLock)

    def set_to(self, new_value: int) -> None:
        """Set the integer to *new_value*."""
        with self._lock:
            self.value = int(new_value)

    def get(self) -> int:
        """Return the current integer value."""
        with self._lock:
            return self.value

    def add(self, delta: int) -> int:
        """Add *delta* to the value and return the result."""
        with self._lock:
            self.value += delta
            return self.value

    def increment(self) -> int:
        """Increment by 1 and return the new value."""
        return self.add(1)

    def decrement(self) -> int:
        """Decrement by 1 and return the new value."""
        return self.add(-1)

    def reset(self) -> None:
        """Reset the value to zero."""
        with self._lock:
            self.value = 0

    class _Atomic:
        """Context manager that holds the integer's lock."""

        def __init__(self, parent: "AtomicInt"):
            """Initialize with the parent integer."""
            self._p = parent
        def __enter__(self) -> "AtomicInt":
            self._p._lock.acquire()
            return self._p
        def __exit__(self, exc_type, exc, tb):
            self._p._lock.release()

    def atomic(self) -> "_Atomic":
        """Return a context manager for batched lock-held operations."""
        return AtomicInt._Atomic(self)

    def __int__(self) -> int:
        """Allow use in integer expressions."""
        return self.get()

    def __repr__(self) -> str:
        """Return a string representation of the integer."""
        return f"AtomicInt({self.get()})"
