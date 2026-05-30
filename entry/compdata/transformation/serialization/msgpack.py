"""Msgpack serialisation / deserialisation transformation."""

import textwrap
from typing import Any

import msgpack

from ..base import _data_transformation


class MsgpackSerializer(_data_transformation):
    """Reversible msgpack serialiser for Python objects."""

    name: str = "msgpack"

    def model_post_init(self, __context: Any) -> None:
        """Build backward_code dynamically after model creation."""
        self.backward_code = textwrap.dedent(f"""
            def backward(inp):
                import msgpack
                kwargs = {{"raw": False, **{self.backward_kwargs!r}}}
                return msgpack.unpackb(inp, **kwargs)
        """)

    def forward(self, inp: Any) -> bytes:
        """Serialize a Python object into bytes using msgpack.

        Parameters
        ----------
        inp : Any
            Object to serialize.

        Returns
        -------
        bytes
            Msgpack-encoded bytes.
        """
        kwargs = {"use_bin_type": True, **self.forward_kwargs}
        return msgpack.packb(inp, **kwargs)

    def backward(self, inp: bytes) -> Any:
        """Deserialize bytes back into a Python object using msgpack.

        Parameters
        ----------
        inp : bytes
            Msgpack-encoded bytes.

        Returns
        -------
        Any
            Deserialized Python object.
        """
        kwargs = {"raw": False, **self.backward_kwargs}
        return msgpack.unpackb(inp, **kwargs)
