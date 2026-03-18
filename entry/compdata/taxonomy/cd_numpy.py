from .compdata import ComputationalData, register_cdtype, _scalar_len
import numpy as np
from ..transformation.serialization import NumpySerializer
from pydantic import PrivateAttr
import copy


@register_cdtype(np.ndarray)
class CD_numpyarray(ComputationalData):
    data: np.ndarray
    _serializer: NumpySerializer = PrivateAttr(default_factory=NumpySerializer)

    # --- Serializer getter/setter ---
    @property
    def serializer(self) -> NumpySerializer:
        """Return the serializer instance (public accessor)."""
        return self._serializer

    @serializer.setter
    def serializer(self, value: NumpySerializer):
        """Set a new serializer instance."""
        if not isinstance(value, NumpySerializer):
            raise TypeError(
                f"serializer must be a NumpySerializer, got {type(value).__name__}"
            )
        self._serializer = value

    # --- ComputationalData behavior ---
    def __len__(self):
        return self.data.shape[0] if self.data.ndim else _scalar_len()

    @property
    def shape(self):
        return self.data.shape

    def __copy__(self):
        return type(self)(self.data.copy())

    def __deepcopy__(self, _):
        return type(self)(self.data.copy())

    def __repr__(self):
        return f"CD_numpyarray(shape={self.data.shape}, dtype={self.data.dtype})"
