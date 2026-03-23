"""Globally-atomic identifiable object with secret-gated locking."""
from __future__ import annotations

from contextlib import contextmanager
import secrets
from typing import Optional, Any

from pydantic import PrivateAttr

from .locally_atomic_identifiable_object import _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT


class _LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT(_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT):
    """Identifiable object with both local and global locking.

    Global locks are policy-scoped: a caller must present a ``policy_id``
    to acquire the global lock and receives a one-time *secret* token that
    is required for subsequent mutations and for releasing the lock.
    """

    _global_lock: bool = PrivateAttr(default=False)
    _holder_policy_id: Optional[str] = PrivateAttr(default=None)
    _holder_secret: Optional[str] = PrivateAttr(default=None)

    def lock_global(self, policy_id: str, *, timeout_s: Optional[float] = None) -> Optional[str]:
        """Acquire the global lock for a given policy.

        Parameters
        ----------
        policy_id : str
            Identifier of the requesting policy.
        timeout_s : float, optional
            Maximum seconds to wait for the underlying local lock.

        Returns
        -------
        str or None
            A secret token on success (or if already held by the same
            policy), ``None`` if another policy holds the lock.
        """
        with self.atomic(scope="local", timeout_s=timeout_s):
            if self._global_lock:
                if self._holder_policy_id == policy_id and self._holder_secret is not None:
                    return self._holder_secret
                return None
            self._global_lock = True
            self._holder_policy_id = policy_id
            self._holder_secret = secrets.token_hex(8)
            return self._holder_secret

    def unlock_global(self, secret: str) -> None:
        """Release the global lock if *secret* matches the holder token.

        Parameters
        ----------
        secret : str
            The token returned by ``lock_global``.
        """
        with self.atomic(scope="local"):
            if secret != self._holder_secret:
                return
            self._global_lock = False
            self._holder_policy_id = None
            self._holder_secret = None

    def global_locked(self, *, timeout_s: Optional[float] = None) -> bool:
        """Return ``True`` if the global lock is held (or local lock times out)."""
        try:
            with self.atomic(scope="local", timeout_s=timeout_s):
                return self._global_lock
        except TimeoutError:
            return True

    def _verify_secret(self, secret: Optional[str], *, timeout_s: Optional[float] = None) -> bool:
        """Verify that *secret* matches the current holder token."""
        try:
            with self.atomic(scope="local", timeout_s=timeout_s):
                if not self._global_lock:
                    return True
                return secret is not None and secret == self._holder_secret
        except TimeoutError:
            return False

    @contextmanager
    def atomic(
        self,
        *,
        scope: str = "local",
        timeout_s: Optional[float] = None,
        policy_id: Optional[str] = None,
    ):
        """Context manager for local or global atomic sections.

        Parameters
        ----------
        scope : str
            ``"local"`` for a local lock, ``"global"`` for a policy-scoped
            global lock.
        timeout_s : float, optional
            Maximum seconds to wait for lock acquisition.
        policy_id : str, optional
            Required when *scope* is ``"global"``.

        Yields
        ------
        _LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT
            The locked instance.

        Raises
        ------
        ValueError
            If *scope* is invalid or *policy_id* is missing for global scope.
        PermissionError
            If the global lock is already held by another policy.
        """
        if scope == "local":
            with super().atomic(scope="local", timeout_s=timeout_s):
                yield self
            return
        if scope != "global":
            raise ValueError("Invalid scope for _LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT.atomic() call.")
        if policy_id is None:
            raise ValueError("policy_id is required for global atomic scope.")

        lock_global = object.__getattribute__(self, "lock_global")
        unlock_global = object.__getattribute__(self, "unlock_global")
        secret = lock_global(policy_id, timeout_s=timeout_s)
        if secret is None:
            raise PermissionError("Global lock already held by another policy.")
        try:
            yield self
        finally:
            unlock_global(secret=secret)

    def __getattribute__(self, name: str) -> Any:
        """Wrap callable attributes to enforce secret verification on access."""
        attr = super().__getattribute__(name)
        if not callable(attr):
            return attr

        def _wrapped(*args, **kwargs):
            secret = kwargs.pop("secret", None)
            verify_secret = object.__getattribute__(self, "_verify_secret")
            if not verify_secret(secret):
                return PermissionError("Global lock requires correct secret for passing through.")
            return attr(*args, **kwargs)

        return _wrapped
