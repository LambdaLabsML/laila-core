"""Decorator that acquires atomic locks on identifiable arguments before calling a method."""

from contextlib import ExitStack
from ...atomic.definitions.locally_atomic_identifiable_object import _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT


def synchronized(method, *, scope: str = "local"):
    """Wrap *method* so all identifiable arguments are locked before invocation.

    Parameters
    ----------
    method : callable
        The method to wrap.
    scope : str, optional
        Lock scope passed to ``obj.atomic()``.  Only ``"local"`` is
        currently supported.

    Returns
    -------
    callable
        Wrapped function that acquires locks in a deterministic order.
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