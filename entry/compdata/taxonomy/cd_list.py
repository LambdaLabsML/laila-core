""":class:`ComputationalData` subclass for ``list`` and ``tuple`` payloads.

Both Python sequence types share one wrapper. Lists serialize as
msgpack arrays; tuples default to pickle because msgpack has no tuple
type, which is what keeps the original Python type (list vs tuple)
intact on round-trip. Copy semantics respect the source type (tuples
are immutable so shallow-copy returns the same object).

Known limitation: a tuple *nested inside a list* still goes through
msgpack and comes back as a list.
"""

import copy
from typing import Any, ClassVar

from ..transformation.serialization import MsgpackSerializer, PickleSerializer
from .compdata import ComputationalData, register_cdtype


@register_cdtype(list, tuple)
class CD_list(ComputationalData):
    """Computational-data wrapper for ``list`` and ``tuple`` payloads.

    Defaults to :class:`MsgpackSerializer`. :attr:`shape` returns a
    1-D shape ``(len,)`` so callers that bridge between sequence-like
    and array-like compdata can use a uniform interface.
    """

    data: list[Any] | tuple[Any, ...]
    _SERIALIZER_CLS: ClassVar[type] = MsgpackSerializer

    def _ensure_serializer(self):
        """Msgpack for lists; pickle for tuples.

        msgpack has no tuple type -- a tuple comes back as a list -- so a
        tuple payload defaults to :class:`PickleSerializer`, which is the
        only way to honour the "list vs tuple is preserved on round-trip"
        promise in the module docstring. Lists keep the compact msgpack
        encoding.
        """
        serializer = self._serializer
        if serializer is None:
            if isinstance(self.data, tuple):
                serializer = PickleSerializer()
            else:
                serializer = type(self)._SERIALIZER_CLS()
            self._serializer = serializer
        return serializer

    # --- Serializer getter/setter ---
    @property
    def serializer(self) -> MsgpackSerializer | PickleSerializer:
        """Return the serializer instance (public accessor)."""
        return self._ensure_serializer()

    @serializer.setter
    def serializer(self, value: MsgpackSerializer | PickleSerializer):
        """Set a new serializer instance."""
        if not isinstance(value, (MsgpackSerializer, PickleSerializer)):
            raise TypeError(
                f"serializer must be a MsgpackSerializer or PickleSerializer, got {type(value).__name__}"
            )
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
