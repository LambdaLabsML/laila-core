"""Thread-safe dot-notation attribute map."""
from pydantic import BaseModel, Field, PrivateAttr, model_validator
import threading
import uuid
import time
from typing import Callable, Optional
from abc import ABC, abstractmethod
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
