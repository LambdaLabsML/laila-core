from __future__ import annotations

from contextlib import contextmanager
import threading
from typing import Optional

from pydantic import BaseModel, ConfigDict


class _LAILA_LOCALLY_ATOMIC_OBJECT(BaseModel):
    """Local locking mixin without identity concerns."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __init__(self, **data):
        self.__pydantic_validator__.validate_python(data, self_instance=self)

    def _ensure_local_lock(self) -> threading.RLock:
        lock = getattr(self, "_local_lock", None)
        if lock is None:
            lock = threading.RLock()
            object.__setattr__(self, "_local_lock", lock)
        return lock

    @contextmanager
    def atomic(self, *, scope: str = "local", timeout_s: Optional[float] = None):
        if scope != "local":
            raise ValueError("Invalid scope for _LAILA_LOCALLY_ATOMIC_OBJECT.atomic() call.")
        if not self.lock(timeout_s=timeout_s):
            raise TimeoutError("Timed out acquiring local lock.")
        try:
            yield self
        finally:
            self.unlock()

    def lock(self, timeout_s: Optional[float] = None) -> bool:
        local_lock = self._ensure_local_lock()
        if timeout_s is None:
            local_lock.acquire()
            return True
        return local_lock.acquire(timeout=timeout_s)

    def unlock(self) -> None:
        self._ensure_local_lock().release()

    def locked(self) -> bool:
        return self._ensure_local_lock().locked()
