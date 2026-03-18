import io
import torch
import textwrap
from typing import Any
from ..base import _data_transformation


class TorchSerializer(_data_transformation):
    name: str = "torch"

    def model_post_init(self, __context: Any) -> None:
        """Build backward_code dynamically after model creation."""
        self.backward_code = textwrap.dedent(f"""
            def backward(inp):
                import io
                import torch
                buf = io.BytesIO(inp)
                kwargs = {self.backward_kwargs!r}
                return torch.load(buf, **kwargs)
        """)

    def forward(self, inp: torch.Tensor) -> bytes:
        """Serialize a PyTorch tensor into bytes."""
        buf = io.BytesIO()
        kwargs = {**self.forward_kwargs}
        torch.save(inp, buf, **kwargs)
        return buf.getvalue()

    def backward(self, inp: bytes) -> Any:
        """Deserialize bytes back into a PyTorch tensor."""
        buf = io.BytesIO(inp)
        kwargs = {**self.backward_kwargs}
        return torch.load(buf, **kwargs)
