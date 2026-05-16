"""Reusable function and method decorators.

Two small, frequently-used decorators live here:

- :func:`synchronized` -- wraps a method so only one thread at a
  time may execute it on a given instance. Uses an internal
  :class:`threading.RLock` per object so the same thread can re-enter
  the protected method (or another synchronized method on the same
  instance) without dead-locking. Use sparingly: prefer the
  fine-grained ``with self.atomic(): ...`` blocks where possible,
  reserve ``@synchronized`` for short methods whose entire body is
  the critical section.
- :func:`ensure_list` -- coerces a parameter to a list before
  calling the wrapped function. Convenient for "accepts a single
  item or an iterable" callers; saves boilerplate at the top of
  every such function.
"""
from .synchronized import synchronized
from .typecheck import ensure_list
