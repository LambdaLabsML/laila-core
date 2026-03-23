"""Thread-safe boolean flag."""
from __future__ import annotations
from threading import RLock
from pydantic import BaseModel, Field, PrivateAttr, ConfigDict
from ..definitions.locally_atomic_object import _LAILA_LOCALLY_ATOMIC_OBJECT

class AtomicFlag(_LAILA_LOCALLY_ATOMIC_OBJECT, BaseModel):
    """Thread-safe boolean flag with atomic set/clear/toggle/get operations."""
    model_config = ConfigDict(arbitrary_types_allowed=True)

    value: bool = Field(default=False)
    _lock: RLock = PrivateAttr(default_factory=RLock)

    def set(self) -> None:
        """Set the flag to ``True``."""
        with self._lock:
            self.value = True

    def clear(self) -> None:
        """Set the flag to ``False``."""
        with self._lock:
            self.value = False

    def toggle(self) -> None:
        """Invert the flag value."""
        with self._lock:
            self.value = not self.value

    def is_set(self) -> bool:
        """Return the current flag value."""
        with self._lock:
            return self.value

    def set_to(self, state: bool) -> None:
        """Set the flag to *state*."""
        with self._lock:
            self.value = bool(state)

    class _Atomic:
        """Context manager that holds the flag's lock."""

        def __init__(self, parent: "AtomicFlag"):
            """Initialize with the parent flag."""
            self._p = parent
        def __enter__(self) -> "AtomicFlag":
            self._p._lock.acquire()
            return self._p
        def __exit__(self, exc_type, exc, tb):
            self._p._lock.release()

    def atomic(self) -> "_Atomic":
        """Return a context manager for batched lock-held operations."""
        return AtomicFlag._Atomic(self)

    def __repr__(self) -> str:
        """Return a string representation of the flag."""
        with self._lock:
            return f"AtomicFlag({self.value})"
