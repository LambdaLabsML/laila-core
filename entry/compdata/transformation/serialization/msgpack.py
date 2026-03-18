import msgpack
import textwrap
from typing import Any
from ..base import _data_transformation


class MsgpackSerializer(_data_transformation):
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
        """Serialize a Python object into bytes using msgpack."""
        kwargs = {"use_bin_type": True, **self.forward_kwargs}
        return msgpack.packb(inp, **kwargs)

    def backward(self, inp: bytes) -> Any:
        """Deserialize bytes back into a Python object using msgpack."""
        kwargs = {"raw": False, **self.backward_kwargs}
        return msgpack.unpackb(inp, **kwargs)
