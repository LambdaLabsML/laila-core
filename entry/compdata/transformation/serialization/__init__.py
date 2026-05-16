"""Serializer transformations for various Python data formats.

Each serializer is a :class:`_data_transformation` whose ``forward``
turns a Python object into bytes and whose ``backward`` does the
inverse. They are typically the *first* step in a transformation
pipeline (the rest is encoding / compression / encryption on top of
the resulting bytes).

Available serializers
---------------------

============    ===============================================
Class           When to use
============    ===============================================
PickleSerializer  Catch-all; works for arbitrary Python objects
                  but produces opaque, version-coupled blobs.
                  Preferred fall-back when no native serializer
                  fits.
MsgpackSerializer Compact, language-agnostic. Good for dicts /
                  lists of JSON-shaped values; faster and smaller
                  than pickle for those payloads.
NumpySerializer   Optimised for :class:`numpy.ndarray` --
                  preserves dtype, shape, and byte order.
TorchSerializer   Optimised for :class:`torch.Tensor`. Imported
                  lazily; resolves to ``None`` when ``torch`` is
                  not installed.
============    ===============================================
"""

from .pickle import PickleSerializer
from .msgpack import MsgpackSerializer
from .numpy import NumpySerializer

try:
    from .torch import TorchSerializer
except ModuleNotFoundError:
    TorchSerializer = None
