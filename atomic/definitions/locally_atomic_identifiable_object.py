"""Combination of identifiable and locally-atomic base classes."""
from __future__ import annotations

from .identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from .locally_atomic_object import _LAILA_LOCALLY_ATOMIC_OBJECT


class _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT(
    _LAILA_LOCALLY_ATOMIC_OBJECT,
    _LAILA_IDENTIFIABLE_OBJECT,
):
    """Identifiable object that also provides local thread-safe locking.

    Combines UUID-based identity from ``_LAILA_IDENTIFIABLE_OBJECT`` with the
    reentrant-lock primitives of ``_LAILA_LOCALLY_ATOMIC_OBJECT``.
    """

    pass
