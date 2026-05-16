""":class:`ComputationalData` subclass for PyTorch :class:`torch.Tensor`
payloads.

The wrapper is only registered when :mod:`torch` is importable -- the
``torch`` extras (``pip install laila-core[torch]``) are an optional
dependency. When torch is missing the class definition is skipped
entirely, so the registry has no entry for ``torch.Tensor`` and
attempts to pool a tensor will fall back to :class:`CD_generic`
(pickle), which works but is much slower and not GPU-aware.
"""

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
        """Computational-data wrapper for :class:`torch.Tensor` payloads.

        Defaults to :class:`TorchSerializer` (which uses
        :func:`torch.save` / :func:`torch.load`), preserving dtype,
        device, and storage layout on round-trip. Copy operations use
        :meth:`Tensor.clone`, which is autograd-aware and detaches from
        the computation graph.
        """

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

        def __len__(self):
            """Return the size of the first dimension."""
            return self.data.size(0) if self.data.dim() else _scalar_len()

        @property
        def shape(self):
            """Return the tensor shape as a tuple."""
            return tuple(self.data.size())

        def __copy__(self):
            """Return a cloned tensor wrapper."""
            return type(self)(self.data.clone())

        def __deepcopy__(self, _):
            """Return a deep-cloned tensor wrapper."""
            return type(self)(self.data.clone())

        def __repr__(self):
            """Return a developer-friendly representation."""
            return (
                f"CD_torchtensor(shape={tuple(self.data.size())}, "
                f"dtype={self.data.dtype}, device={self.data.device})"
            )
