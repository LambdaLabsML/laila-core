"""Locally-atomic base class providing reentrant locking."""
from __future__ import annotations

from contextlib import contextmanager
import threading
from typing import Optional

from pydantic import BaseModel, ConfigDict


class _LAILA_LOCALLY_ATOMIC_OBJECT(BaseModel):
    """Pydantic base model providing thread-safe local locking.

    Subclasses gain ``lock`` / ``unlock`` / ``atomic`` helpers backed by a
    lazily-created ``threading.RLock``.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __init__(self, **data):
        """Forward all keyword arguments to Pydantic."""
        super().__init__(**data)

    def _ensure_local_lock(self) -> threading.RLock:
        """Return the instance's RLock, creating it on first access."""
        lock = getattr(self, "_local_lock", None)
        if lock is None:
            lock = threading.RLock()
            object.__setattr__(self, "_local_lock", lock)
        return lock

    @contextmanager
    def atomic(self, *, scope: str = "local", timeout_s: Optional[float] = None):
        """Context manager that holds the local lock for the block's duration.

        Parameters
        ----------
        scope : str
            Must be ``"local"``.
        timeout_s : float, optional
            Maximum seconds to wait for the lock.

        Yields
        ------
        _LAILA_LOCALLY_ATOMIC_OBJECT
            The locked instance.

        Raises
        ------
        ValueError
            If *scope* is not ``"local"``.
        TimeoutError
            If the lock cannot be acquired within *timeout_s*.
        """
        if scope != "local":
            raise ValueError("Invalid scope for _LAILA_LOCALLY_ATOMIC_OBJECT.atomic() call.")
        if not self.lock(timeout_s=timeout_s):
            raise TimeoutError("Timed out acquiring local lock.")
        try:
            yield self
        finally:
            self.unlock()

    def lock(self, timeout_s: Optional[float] = None) -> bool:
        """Acquire the local lock.

        Parameters
        ----------
        timeout_s : float, optional
            Maximum seconds to wait. Blocks indefinitely when ``None``.

        Returns
        -------
        bool
            ``True`` if the lock was acquired.
        """
        local_lock = self._ensure_local_lock()
        if timeout_s is None:
            local_lock.acquire()
            return True
        return local_lock.acquire(timeout=timeout_s)

    def unlock(self) -> None:
        """Release the local lock."""
        self._ensure_local_lock().release()

    def locked(self) -> bool:
        """Return ``True`` if the local lock is currently held."""
        return self._ensure_local_lock().locked()
