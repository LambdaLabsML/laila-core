from .compdata import ComputationalData, register_cdtype, _scalar_len
from ..transformation.serialization import TorchSerializer
from pydantic import PrivateAttr

try:
    import torch
    _HAVE_TORCH = True
except ModuleNotFoundError:            # pragma: no cover
    torch = None                       # type: ignore
    _HAVE_TORCH = False


if _HAVE_TORCH:
    @register_cdtype(torch.Tensor)
    class CD_torchtensor(ComputationalData):
        """ComputationalData wrapper for torch.Tensor objects."""

        data: "torch.Tensor"  # type: ignore[name-defined]
        _serializer: TorchSerializer = PrivateAttr(default_factory=TorchSerializer)

        # --- Serializer getter/setter ---
        @property
        def serializer(self) -> TorchSerializer:
            """Return the serializer instance (public accessor)."""
            return self._serializer

        @serializer.setter
        def serializer(self, value: TorchSerializer):
            """Set a new serializer instance."""
            if not isinstance(value, TorchSerializer):
                raise TypeError(
                    f"serializer must be a TorchSerializer, got {type(value).__name__}"
                )
            self._serializer = value

        # --- Core tensor behaviors ---
        def __len__(self):
            return self.data.size(0) if self.data.dim() else _scalar_len()

        @property
        def shape(self):
            return tuple(self.data.size())

        def __copy__(self):
            return type(self)(self.data.clone())

        def __deepcopy__(self, _):
            return type(self)(self.data.clone())

        def __repr__(self):
            return (
                f"CD_torchtensor(shape={tuple(self.data.size())}, "
                f"dtype={self.data.dtype}, device={self.data.device})"
            )
