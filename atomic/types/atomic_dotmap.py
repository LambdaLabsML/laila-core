from pydantic import BaseModel, Field, PrivateAttr, model_validator
import threading
import uuid
import time
from typing import Callable, Optional
from abc import ABC, abstractmethod
from ..definitions.locally_atomic_object import _LAILA_LOCALLY_ATOMIC_OBJECT


# ===== AtomicDotMap (renamed from your ThreadSafeDotMap, semantics unchanged) =====
class AtomicDotMap(_LAILA_LOCALLY_ATOMIC_OBJECT):
    def __init__(self):
        self._data = {}
        self._lock = threading.RLock()

    def __getattr__(self, key):
        with self._lock:
            return self._data.get(key)

    def __setattr__(self, key, value):
        if key in {"_data", "_lock"}:
            super().__setattr__(key, value)
        else:
            with self._lock:
                self._data[key] = value

    def __delattr__(self, key):
        with self._lock:
            del self._data[key]

    def to_dict(self):
        with self._lock:
            return dict(self._data)

    def keys(self):
        with self._lock:
            return list(self._data.keys())

    def values(self):
        with self._lock:
            return list(self._data.values())

    def items(self):
        with self._lock:
            return list(self._data.items())

    def __repr__(self):
        with self._lock:
            return f"AtomicDotMap({self._data})"
