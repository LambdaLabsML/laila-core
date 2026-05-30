"""Thread-safe dot-notation attribute map.

:class:`AtomicDotMap` is the threaded sibling of :class:`dotmap.DotMap`.
It exposes attribute access (``m.a.b.c = 1``) over an underlying
nested-dict structure, but every read and write goes through a
:class:`threading.RLock` so concurrent producers and consumers never
observe a half-written tree.

Used by laila for live, nested configuration that may be mutated at
runtime -- the prototypical example is :data:`laila.args`, which is
read by every CLI-capable class during construction and may be
written by environment-load workflows or by user code rebinding
``laila.args.foo.bar = ...``.
"""

import threading

from ..definitions.locally_atomic_object import _LAILA_LOCALLY_ATOMIC_OBJECT


class AtomicDotMap(_LAILA_LOCALLY_ATOMIC_OBJECT):
    """Thread-safe mapping accessed via attribute syntax.

    All reads and writes to dynamic attributes are guarded by a
    reentrant lock, making the map safe for concurrent use.
    """

    def __init__(self):
        """Initialize with an empty data dict and a reentrant lock."""
        self._data = {}
        self._lock = threading.RLock()

    def __getattr__(self, key):
        """Return the value for *key*, or ``None`` if absent."""
        with self._lock:
            return self._data.get(key)

    def __setattr__(self, key, value):
        """Set *key* to *value*, bypassing the lock for internal attrs."""
        if key in {"_data", "_lock"}:
            super().__setattr__(key, value)
        else:
            with self._lock:
                self._data[key] = value

    def __delattr__(self, key):
        """Delete the attribute *key*."""
        with self._lock:
            del self._data[key]

    def to_dict(self):
        """Return a shallow copy of the internal data as a plain dict."""
        with self._lock:
            return dict(self._data)

    def keys(self):
        """Return a snapshot list of keys."""
        with self._lock:
            return list(self._data.keys())

    def values(self):
        """Return a snapshot list of values."""
        with self._lock:
            return list(self._data.values())

    def items(self):
        """Return a snapshot list of ``(key, value)`` pairs."""
        with self._lock:
            return list(self._data.items())

    def __repr__(self):
        """Return a string representation of the map."""
        with self._lock:
            return f"AtomicDotMap({self._data})"
