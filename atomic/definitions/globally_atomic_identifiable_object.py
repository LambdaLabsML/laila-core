from __future__ import annotations

from contextlib import contextmanager
import secrets
from typing import Optional, Any

from pydantic import PrivateAttr

from .locally_atomic_identifiable_object import _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT


class _LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT(_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT):
    _global_lock: bool = PrivateAttr(default=False)
    _holder_policy_id: Optional[str] = PrivateAttr(default=None)
    _holder_secret: Optional[str] = PrivateAttr(default=None)

    def lock_global(self, policy_id: str, *, timeout_s: Optional[float] = None) -> Optional[str]:
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
        with self.atomic(scope="local"):
            if secret != self._holder_secret:
                return
            self._global_lock = False
            self._holder_policy_id = None
            self._holder_secret = None

    def global_locked(self, *, timeout_s: Optional[float] = None) -> bool:
        try:
            with self.atomic(scope="local", timeout_s=timeout_s):
                return self._global_lock
        except TimeoutError:
            return True

    def _verify_secret(self, secret: Optional[str], *, timeout_s: Optional[float] = None) -> bool:
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
