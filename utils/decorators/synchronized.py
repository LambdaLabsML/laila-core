"""Decorator that acquires atomic locks on identifiable arguments before calling a method.

Goes well beyond a "lock self" decorator: :func:`synchronized`
inspects ``self`` *and* every positional and keyword argument; for
each that is an identifiable atomic object it enters the
corresponding ``with obj.atomic()`` block before calling the wrapped
method, releasing the locks again on exit (or on exception).

Lock-acquisition order is the *id-sorted* order of the involved
objects, so two callers who happen to want overlapping subsets of
the same locks always acquire them in the same order. That trick is
what keeps ``@synchronized`` deadlock-free even when several methods
operate on the same set of objects from different threads.
"""

from contextlib import ExitStack

from ...atomic.definitions.locally_atomic_identifiable_object import (
    _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT,
)


def synchronized(method, *, scope: str = "local"):
    """Wrap *method* so all identifiable arguments are locked before invocation.

    During a call, the wrapper:

    1. Collects ``self`` and every positional/keyword argument that
       is an instance of
       :class:`_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT`.
    2. Sorts the collected objects by ``id()`` and enters each one's
       ``atomic(scope=scope)`` context manager in that order.
       (Sorting is what avoids the classic A/B vs B/A deadlock.)
    3. Invokes the wrapped *method* with the original positional and
       keyword arguments.
    4. Releases the locks in reverse order on exit (handled by
       :class:`ExitStack`).

    Parameters
    ----------
    method : callable
        The method to wrap. Expected to be an instance method, so
        the first positional argument is treated as ``self``.
    scope : str, default ``"local"``
        Forwarded to :meth:`atomic`. Only ``"local"`` is currently
        implemented; ``"global"`` raises :class:`NotImplementedError`.

    Returns
    -------
    callable
        The wrapped method.
    """

    def wrapper(self, *args, **kwargs):
        if scope != "local":
            raise NotImplementedError("Global synchronization is not implemented.")

        lock_cls = _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT
        candidates = (self, *args, *kwargs.values())
        lock_targets = [obj for obj in candidates if isinstance(obj, lock_cls)]

        if not lock_targets:
            return method(self, *args, **kwargs)

        with ExitStack() as stack:
            for obj in sorted(lock_targets, key=id):
                stack.enter_context(obj.atomic(scope=scope))
            return method(self, *args, **kwargs)

    return wrapper
