"""Globally-atomic identifiable object with secret-gated locking.

Extends :class:`_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT` with a
*global* (policy-scoped) lock layered on top of the local one. The
global lock is bound to a single ``policy_id`` while it is held; a
secret token returned from :meth:`lock_global` is the only key that
can release the lock or pass through the secret-gated attribute
wrapper installed by :meth:`__getattribute__`.

Use this for resources that need to be coordinated across policies
in the same process (or, in the future, across hosts via the
distributed lock manager). For the much more common per-instance,
single-policy case, prefer
:class:`_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT` -- the global
machinery here adds overhead that is wasted unless multiple policies
genuinely contend for the same object.

Gate semantics
--------------
Only *public* callables defined by a subclass (``bump``, ``write``,
...) are gated. Private / dunder names and everything defined on this
class or its bases (``lock_global``, ``atomic``, ``model_dump``, ...)
are never wrapped, so the lock machinery can call itself and pydantic
can introspect the model freely. While no global lock is held the gate
is a cheap name check and the attribute is returned untouched.

While a global lock is held, a gated call passes when either the
``secret=`` keyword matches the holder token or the call happens
inside the holder's own ``with obj.atomic(scope="global", ...)`` block
(the secret travels in a :class:`contextvars.ContextVar`). Otherwise
:class:`PermissionError` is **raised**.

``lock_global`` is reference-counted per holder policy: a nested
acquire by the holder returns the same token and must be matched by
its own ``unlock_global``; the lock is released when the count
reaches zero.
"""

from __future__ import annotations

import secrets
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, ClassVar

from pydantic import PrivateAttr

from .locally_atomic_identifiable_object import _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT

# Secret of the global lock held by the *current* context (set inside
# ``atomic(scope="global")`` so the holder's body passes the gate).
_HELD_SECRET: ContextVar[str | None] = ContextVar("laila_global_lock_secret", default=None)


class _LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT(_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT):
    """Identifiable object that coordinates access via a policy-scoped global lock.

    Adds three machinery pieces on top of the local-lock base:

    - :meth:`lock_global` / :meth:`unlock_global` -- explicit,
      reference-counted global acquire/release driven by a per-policy
      ``policy_id`` and a secret token returned at acquire-time.
    - :meth:`atomic` (overridden) -- accepts ``scope="global"`` to
      hold the global lock for a ``with`` block, raising
      :class:`PermissionError` when another policy holds it. Inside the
      block the holder's own calls pass the gate without ``secret=``.
    - :meth:`__getattribute__` (overridden) -- while a global lock is
      held, public subclass callables require the holder secret
      (``secret=`` kwarg or the enclosing global block) and raise
      :class:`PermissionError` otherwise. This stops accidental
      mutations by code that doesn't know the lock is in effect.

    Global state (private):

    - ``_global_lock`` -- whether the lock is currently held.
    - ``_holder_policy_id`` -- which policy currently holds it.
    - ``_holder_secret`` -- the token that proves you're the holder.
    - ``_hold_count`` -- nested acquisitions by the holder.
    """

    _global_lock: bool = PrivateAttr(default=False)
    _holder_policy_id: str | None = PrivateAttr(default=None)
    _holder_secret: str | None = PrivateAttr(default=None)
    _hold_count: int = PrivateAttr(default=0)

    # Names that are never gated: everything this class and its bases
    # define (lock machinery, pydantic API, identity helpers). Built
    # lazily per class so subclass additions *are* gated.
    _ungated_names: ClassVar[frozenset[str] | None] = None

    @classmethod
    def _ungated(cls) -> frozenset[str]:
        names = cls.__dict__.get("_ungated_names")
        if names is None:
            collected: set[str] = set()
            for base in _LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT.__mro__:
                collected.update(vars(base).keys())
            names = frozenset(collected)
            cls._ungated_names = names
        return names

    def lock_global(self, policy_id: str, *, timeout_s: float | None = None) -> str | None:
        """Acquire (or re-acquire) the global lock for a given policy.

        Parameters
        ----------
        policy_id : str
            Identifier of the requesting policy.
        timeout_s : float, optional
            Maximum seconds to wait for the underlying local lock.

        Returns
        -------
        str or None
            A secret token on success (the *same* token, with the hold
            count incremented, when the holder re-acquires), ``None`` if
            another policy holds the lock.
        """
        with self.atomic(scope="local", timeout_s=timeout_s):
            if self._global_lock:
                if self._holder_policy_id == policy_id and self._holder_secret is not None:
                    self._hold_count += 1
                    return self._holder_secret
                return None
            self._global_lock = True
            self._holder_policy_id = policy_id
            self._holder_secret = secrets.token_hex(8)
            self._hold_count = 1
            return self._holder_secret

    def unlock_global(self, secret: str) -> None:
        """Release one hold on the global lock if *secret* matches the holder token.

        The lock is released for real when the hold count reaches zero;
        a wrong secret is a silent no-op.

        Parameters
        ----------
        secret : str
            The token returned by ``lock_global``.
        """
        with self.atomic(scope="local"):
            if secret is None or secret != self._holder_secret:
                return
            self._hold_count -= 1
            if self._hold_count > 0:
                return
            self._global_lock = False
            self._holder_policy_id = None
            self._holder_secret = None
            self._hold_count = 0

    def global_locked(self, *, timeout_s: float | None = None) -> bool:
        """Return ``True`` if the global lock is held (or local lock times out)."""
        try:
            with self.atomic(scope="local", timeout_s=timeout_s):
                return self._global_lock
        except TimeoutError:
            return True

    def _verify_secret(self, secret: str | None, *, timeout_s: float | None = None) -> bool:
        """Verify that *secret* (or the context's held secret) matches the holder token."""
        try:
            with self.atomic(scope="local", timeout_s=timeout_s):
                if not self._global_lock:
                    return True
                if secret is not None and secret == self._holder_secret:
                    return True
                held = _HELD_SECRET.get()
                return held is not None and held == self._holder_secret
        except TimeoutError:
            return False

    @contextmanager
    def atomic(
        self,
        *,
        scope: str = "local",
        timeout_s: float | None = None,
        policy_id: str | None = None,
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
            The locked instance. Inside a global block the holder's
            calls on the object pass the gate without ``secret=``.

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
            raise ValueError(
                "Invalid scope for _LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT.atomic() call."
            )
        if policy_id is None:
            raise ValueError("policy_id is required for global atomic scope.")

        secret = self.lock_global(policy_id, timeout_s=timeout_s)
        if secret is None:
            raise PermissionError("Global lock already held by another policy.")
        token = _HELD_SECRET.set(secret)
        try:
            yield self
        finally:
            _HELD_SECRET.reset(token)
            self.unlock_global(secret)

    def __getattribute__(self, name: str) -> Any:
        """Gate public subclass callables behind the holder secret while globally locked."""
        attr = super().__getattribute__(name)
        if name.startswith("_") or not callable(attr):
            return attr
        if name in type(self)._ungated():
            return attr
        # Benign unlocked read: a lock taken between this check and the
        # call is the same race a caller would have with any lock.
        private = object.__getattribute__(self, "__pydantic_private__") or {}
        if not private.get("_global_lock", False):
            return attr

        def _gated(*args, **kwargs):
            secret = kwargs.pop("secret", None)
            verify = object.__getattribute__(self, "_verify_secret")
            if not verify(secret):
                raise PermissionError(
                    f"{type(self).__name__}.{name}() is globally locked by policy "
                    f"{private.get('_holder_policy_id')!r}; pass secret=<token> or call it "
                    "inside the holder's atomic(scope='global') block."
                )
            return attr(*args, **kwargs)

        return _gated
