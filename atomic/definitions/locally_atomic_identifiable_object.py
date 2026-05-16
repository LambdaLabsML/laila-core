"""Identifiable + locally-atomic mixin.

Single-purpose convenience module: glues
:class:`_LAILA_IDENTIFIABLE_OBJECT` (identity, global ids,
serialisation hooks) and :class:`_LAILA_LOCALLY_ATOMIC_OBJECT`
(per-instance reentrant lock, ``with self.atomic(): ...``) into one
base class so subclasses can pick up both concerns by inheriting from
a single name. This is the most-used base class in laila -- pools,
taskforces, comm protocols, futures and many more inherit from it.
"""
from __future__ import annotations

from ...basics.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from .locally_atomic_object import _LAILA_LOCALLY_ATOMIC_OBJECT


class _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT(
    _LAILA_LOCALLY_ATOMIC_OBJECT,
    _LAILA_IDENTIFIABLE_OBJECT,
):
    """Identifiable object with a per-instance reentrant lock.

    Pure mixin: defines no extra fields or methods of its own.
    Inherits identity machinery (uuid / scopes / evolution /
    global_id) from :class:`_LAILA_IDENTIFIABLE_OBJECT` and
    locking machinery (``lock`` / ``unlock`` / ``atomic``) from
    :class:`_LAILA_LOCALLY_ATOMIC_OBJECT`.

    The MRO is intentional: ``_LAILA_LOCALLY_ATOMIC_OBJECT`` first so
    its ``model_config = ConfigDict(arbitrary_types_allowed=True)``
    wins (allowing private RLock storage), then
    ``_LAILA_IDENTIFIABLE_OBJECT`` for the identity hooks.
    """

    pass
