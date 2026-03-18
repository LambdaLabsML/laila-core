from dataclasses import dataclass, field
from typing import Dict, Any
from pydantic import PrivateAttr
import copy

from .compdata import ComputationalData, register_cdtype, _scalar_len
from ..transformation.serialization import MsgpackSerializer


@register_cdtype(dict)
class CD_dict(ComputationalData):
    data: Dict[Any, Any]
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

    # --- Other ComputationalData behaviors ---
    def __len__(self):                
        return len(self.data)

    @property
    def shape(self):
        raise AttributeError("dict has no 'shape' attribute")

    def __copy__(self):
        return type(self)(self.data.copy())

    def __deepcopy__(self, memo=None):
        return type(self)(copy.deepcopy(self.data, memo))
    
    def __repr__(self):
        return f"CD_dict({self.data!r})"
