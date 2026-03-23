"""``ComputationalData`` subclass for ``list`` and ``tuple`` payloads."""

from typing import Any, Union
from pydantic import PrivateAttr
import copy

from .compdata import ComputationalData, register_cdtype, _scalar_len
from ..transformation.serialization import MsgpackSerializer


@register_cdtype(list, tuple)
class CD_list(ComputationalData):
    """Computational data wrapper for ``list`` and ``tuple`` objects.

    Uses ``MsgpackSerializer`` by default.
    """

    data: Union[list[Any], tuple[Any, ...]]
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
