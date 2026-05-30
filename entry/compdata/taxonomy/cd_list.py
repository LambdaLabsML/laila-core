""":class:`ComputationalData` subclass for ``list`` and ``tuple`` payloads.

Both Python sequence types share one wrapper because their on-disk
representation is identical (msgpack arrays). The original Python type
(list vs tuple) is preserved on round-trip via the wrapper's
``data`` field annotation, and copy semantics respect the source type
(tuples are immutable so shallow-copy returns the same object).
"""

import copy
from typing import Any

from pydantic import PrivateAttr

from ..transformation.serialization import MsgpackSerializer
from .compdata import ComputationalData, register_cdtype


@register_cdtype(list, tuple)
class CD_list(ComputationalData):
    """Computational-data wrapper for ``list`` and ``tuple`` payloads.

    Defaults to :class:`MsgpackSerializer`. :attr:`shape` returns a
    1-D shape ``(len,)`` so callers that bridge between sequence-like
    and array-like compdata can use a uniform interface.
    """

    data: list[Any] | tuple[Any, ...]
    _serializer: MsgpackSerializer = PrivateAttr(default_factory=MsgpackSerializer)

    # --- Serializer getter/setter ---
    @property
    def serializer(self) -> MsgpackSerializer:
        """Return the serializer instance (public accessor)."""
        return self._serializer

    @serializer.setter
    def serializer(self, value: MsgpackSerializer):
        """Set a new serializer instance."""
        if not isinstance(value, MsgpackSerializer):
            raise TypeError(f"serializer must be a MsgpackSerializer, got {type(value).__name__}")
        self._serializer = value

    def __len__(self):
        """Return the number of elements."""
        return len(self.data)

    @property
    def shape(self):
        """Return a 1-D shape tuple ``(len,)``."""
        return (len(self.data),)

    def __copy__(self):
        """Return a shallow copy."""
        if isinstance(self.data, list):
            copied_data = self.data.copy()
        else:
            copied_data = self.data  # tuples are immutable
        return type(self)(copied_data)

    def __deepcopy__(self, memo=None):
        """Return a deep copy."""
        copied = type(self.data)(copy.deepcopy(e, memo) for e in self.data)
        return type(self)(copied)

    def __repr__(self):
        """Return a developer-friendly representation."""
        tname = "list" if isinstance(self.data, list) else "tuple"
        return f"CD_list(type={tname}, len={len(self.data)})"
