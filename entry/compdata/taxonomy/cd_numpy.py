""":class:`ComputationalData` subclass for :class:`numpy.ndarray` payloads.

Uses :class:`NumpySerializer` (the ``.npy`` format under the hood) so
arrays are persisted with their dtype and shape in a portable,
language-neutral binary format -- much more compact than pickle for
numeric data and readable from any process with NumPy installed.
"""

from .compdata import ComputationalData, register_cdtype, _scalar_len
import numpy as np
from ..transformation.serialization import NumpySerializer
from pydantic import PrivateAttr
import copy


@register_cdtype(np.ndarray)
class CD_numpyarray(ComputationalData):
    """Computational-data wrapper for :class:`numpy.ndarray` payloads.

    Defaults to :class:`NumpySerializer` (``.npy`` format). ``len()``
    follows NumPy semantics (size of the first axis) and raises for
    0-D arrays. :attr:`shape` returns the array's native shape tuple.
    Both shallow and deep copy emit a contiguous NumPy copy of the
    underlying buffer.
    """

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

    def __len__(self):
        """Return the size of the first dimension."""
        return self.data.shape[0] if self.data.ndim else _scalar_len()

    @property
    def shape(self):
        """Return the array shape tuple."""
        return self.data.shape

    def __copy__(self):
        """Return a shallow (contiguous) copy of the array."""
        return type(self)(self.data.copy())

    def __deepcopy__(self, _):
        """Return a deep copy of the array."""
        return type(self)(self.data.copy())

    def __repr__(self):
        """Return a developer-friendly representation."""
        return f"CD_numpyarray(shape={self.data.shape}, dtype={self.data.dtype})"
