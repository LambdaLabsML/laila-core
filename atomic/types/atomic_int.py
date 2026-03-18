from __future__ import annotations
from threading import RLock
from pydantic import BaseModel, Field, PrivateAttr, ConfigDict
from typing import Union
from ..definitions.locally_atomic_object import _LAILA_LOCALLY_ATOMIC_OBJECT

class AtomicInt(_LAILA_LOCALLY_ATOMIC_OBJECT, BaseModel):
    """
    Thread-safe integer with atomic add, increment, decrement, set, and get operations.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)

    value: int = Field(default=0)
    _lock: RLock = PrivateAttr(default_factory=RLock)

    # Set the integer explicitly
    def set_to(self, new_value: int) -> None:
        with self._lock:
            self.value = int(new_value)

    # Get the current value
    def get(self) -> int:
        with self._lock:
            return self.value

    # Add a delta and return the new value
    def add(self, delta: int) -> int:
        with self._lock:
            self.value += delta
            return self.value

    # Increment by 1 and return the new value
    def increment(self) -> int:
        return self.add(1)

    # Decrement by 1 and return the new value
    def decrement(self) -> int:
        return self.add(-1)

    # Reset to zero
    def reset(self) -> None:
        with self._lock:
            self.value = 0

    # Atomic context manager for batch ops
    class _Atomic:
        def __init__(self, parent: "AtomicInt"):
            self._p = parent
        def __enter__(self) -> "AtomicInt":
            self._p._lock.acquire()
            return self._p
        def __exit__(self, exc_type, exc, tb):
            self._p._lock.release()

    def atomic(self) -> "_Atomic":
        return AtomicInt._Atomic(self)

    # Make it behave like an int in expressions
    def __int__(self) -> int:
        return self.get()

    def __repr__(self) -> str:
        return f"AtomicInt({self.get()})"
