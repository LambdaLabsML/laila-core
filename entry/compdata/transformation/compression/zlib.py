# zlib_op.py
import zlib
import base64
import textwrap
from typing import Any
from ..base import _data_transformation


class Zlib(_data_transformation):
    name: str = "zlib"

    def model_post_init(self, __context: Any) -> None:
        # Standalone recovery code mirroring `backward()` with embedded kwargs
        self.backward_code = textwrap.dedent(f"""
            def backward(data):
                import zlib, base64
                if not isinstance(data, str):
                    raise TypeError("Zlib.backward expects a Base64 string (str)")
                compressed = base64.b64decode(data, validate=True)
                return zlib.decompress(compressed, **{self.backward_kwargs!r}).decode("utf-8")
        """)

    def forward(self, data: str) -> str:
        if not isinstance(data, str):
            raise TypeError("Zlib.forward expects a UTF-8 string (str)")
        compressed = zlib.compress(data.encode("utf-8"), **self.forward_kwargs)
        return base64.b64encode(compressed).decode("utf-8")

    def backward(self, data: str) -> str:
        if not isinstance(data, str):
            raise TypeError("Zlib.backward expects a Base64 string (str)")
        compressed = base64.b64decode(data, validate=True)
        return zlib.decompress(compressed, **self.backward_kwargs).decode("utf-8")
