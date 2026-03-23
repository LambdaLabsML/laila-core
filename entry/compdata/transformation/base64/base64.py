"""Base64 encode / decode data transformation."""

import base64
from typing import Any, Union
from ..base import _data_transformation


class Base64(_data_transformation):
    """Reversible Base64 encoding transformation.

    Forward encodes binary data to a Base64 UTF-8 string;
    backward decodes it back to raw bytes.
    """

    name: str = "base64"

    def model_post_init(self, __context: Any) -> None:
        """Build standalone backward recovery code."""
        # Standalone recovery code with embedded kwargs (e.g., altchars, validate)
        self.backward_code = (
            "def backward(inp):\n"
            "    import base64\n"
            f"    kwargs = {self.backward_kwargs!r}\n"
            "    if isinstance(inp, memoryview):\n"
            "        inp = inp.tobytes()\n"
            "    return base64.b64decode(inp, **kwargs)\n"
        )

    def forward(self, data: Union[bytes, bytearray, memoryview]) -> str:
        """Encode binary data -> Base64 UTF-8 string."""
        if isinstance(data, memoryview):
            data = data.tobytes()
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("Base64.forward expects bytes/bytearray/memoryview")
        # b64encode supports altchars=...
        out = base64.b64encode(bytes(data), **self.forward_kwargs)
        return out.decode("utf-8")

    def backward(self, payload: Union[str, bytes, bytearray, memoryview]) -> bytes:
        """Decode Base64 (str/bytes/bytearray/memoryview) -> raw bytes."""
        if isinstance(payload, memoryview):
            payload = payload.tobytes()
        # base64.b64decode accepts str or bytes-like
        try:
            return base64.b64decode(payload, **self.backward_kwargs)
        except Exception as e:
            raise ValueError("Invalid Base64 payload") from e
