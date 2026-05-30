"""Locally-atomic base class providing reentrant per-instance locking.

The single class here, :class:`_LAILA_LOCALLY_ATOMIC_OBJECT`, mixes a
lazily-created :class:`threading.RLock` into any Pydantic model and
exposes a uniform critical-section API (``lock`` / ``unlock`` /
``atomic``). It is the foundation for every laila object that needs
to coordinate access from multiple threads -- pools, central memory,
the future bank, taskforces, and so on.

"Locally" means *one lock per instance, in the current process*.
For cross-process coordination, see
:class:`_LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT` which adds a
distributed lock alongside the local one.

The lock is a re-entrant ``threading.RLock`` so the same thread can
nest critical sections (e.g. ``with self.atomic(): self.helper()``
where ``helper`` also calls ``with self.atomic(): ...``) without
dead-locking.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager

from pydantic import BaseModel, ConfigDict


class _LAILA_LOCALLY_ATOMIC_OBJECT(BaseModel):
    """Pydantic base model giving subclasses thread-safe per-instance locking.

    Subclasses inherit a lazily-allocated :class:`threading.RLock` and
    three helpers:

    - :meth:`lock` -- acquire the lock, optionally with a timeout.
    - :meth:`unlock` -- release the lock once.
    - :meth:`atomic` -- context manager wrapping :meth:`lock` and
      :meth:`unlock` so a critical section reads naturally as
      ``with self.atomic(): ...``.

    The lock is created on first access (so Pydantic's validation
    pipeline does not need to know about it) and stored via
    ``object.__setattr__`` to bypass Pydantic's attribute machinery.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __init__(self, **data):
        """Forward all keyword arguments to Pydantic.

        Exists only to give subclasses a consistent ``__init__``
        signature; the lock itself is created lazily on first
        access via :meth:`_ensure_local_lock`.
        """
        super().__init__(**data)

    def _ensure_local_lock(self) -> threading.RLock:
        """Return the instance's :class:`threading.RLock`, creating it lazily.

        The lock is stored under the private ``_local_lock`` attribute
        via ``object.__setattr__`` to avoid going through Pydantic's
        attribute validation. Subsequent calls reuse the same lock.
        """
        lock = getattr(self, "_local_lock", None)
        if lock is None:
            lock = threading.RLock()
            object.__setattr__(self, "_local_lock", lock)
        return lock

    @contextmanager
    def atomic(self, *, scope: str = "local", timeout_s: float | None = None):
        """Hold the local lock for the duration of the ``with`` block.

        Subclasses such as :class:`_LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT`
        override the *scope* parameter to also accept ``"global"``;
        this base class only knows about ``"local"`` and rejects
        anything else loudly so a typo cannot silently degrade
        cross-process safety.

        Parameters
        ----------
        scope : str, default ``"local"``
            Must be ``"local"`` here. Accepting the keyword keeps the
            signature compatible with subclasses that add scopes.
        timeout_s : float, optional
            Maximum seconds to wait for the lock. ``None`` (default)
            blocks indefinitely.

        Yields
        ------
        _LAILA_LOCALLY_ATOMIC_OBJECT
            The locked instance, so callers can write
            ``with obj.atomic() as locked: ...``.

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

    def lock(self, timeout_s: float | None = None) -> bool:
        """Acquire the per-instance reentrant lock.

        Parameters
        ----------
        timeout_s : float, optional
            Maximum seconds to wait. Blocks indefinitely when
            ``None`` (default).

        Returns
        -------
        bool
            ``True`` if the lock was acquired, ``False`` if the
            timeout elapsed first.
        """
        local_lock = self._ensure_local_lock()
        if timeout_s is None:
            local_lock.acquire()
            return True
        return local_lock.acquire(timeout=timeout_s)

    def unlock(self) -> None:
        """Release the per-instance reentrant lock once.

        Mirrors :meth:`threading.RLock.release` -- a thread that
        called :meth:`lock` *N* times must call :meth:`unlock` *N*
        times before another thread can acquire the lock.
        """
        self._ensure_local_lock().release()

    def locked(self) -> bool:
        """Return ``True`` if the lock is currently held by some thread.

        Convenience wrapper for :meth:`threading.RLock.locked`. Note
        that a thread observing ``True`` cannot safely conclude the
        lock will still be held by the time it acts -- prefer the
        :meth:`atomic` context manager for synchronisation.
        """
        return self._ensure_local_lock().locked()
