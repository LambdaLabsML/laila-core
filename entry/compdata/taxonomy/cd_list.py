from typing import Any, Union
from pydantic import PrivateAttr
import copy

from .compdata import ComputationalData, register_cdtype, _scalar_len
from ..transformation.serialization import MsgpackSerializer


@register_cdtype(list, tuple)
class CD_list(ComputationalData):
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

    # --- ComputationalData behavior ---
    def __len__(self):
        return len(self.data)

    @property
    def shape(self):
        return (len(self.data),)

    def __copy__(self):
        if isinstance(self.data, list):
            copied_data = self.data.copy()
        else:
            copied_data = self.data  # tuples are immutable
        return type(self)(copied_data)

    def __deepcopy__(self, memo=None):
        copied = type(self.data)(copy.deepcopy(e, memo) for e in self.data)
        return type(self)(copied)

    def __repr__(self):
        tname = "list" if isinstance(self.data, list) else "tuple"
        return f"CD_list(type={tname}, len={len(self.data)})"
