"""NumPy array serialisation / deserialisation transformation."""

import io
import textwrap
from typing import Any

import numpy as np

from ..base import _data_transformation


class NumpySerializer(_data_transformation):
    """Reversible NumPy serialiser using ``np.save`` / ``np.load``."""

    name: str = "numpy"

    def model_post_init(self, __context: Any) -> None:
        """Build backward_code dynamically after model creation."""
        self.backward_code = textwrap.dedent(f"""
            def backward(inp):
                import io
                import numpy as np
                buf = io.BytesIO(inp)
                kwargs = {{'allow_pickle': False, **{self.backward_kwargs!r}}}
                return np.load(buf, **kwargs)
        """)

    def forward(self, inp: np.ndarray) -> bytes:
        """Serialize a NumPy array into bytes.

        Parameters
        ----------
        inp : np.ndarray
            Array to serialize.

        Returns
        -------
        bytes
            ``np.save``-encoded bytes.
        """
        buf = io.BytesIO()
        kwargs = {"allow_pickle": False, **self.forward_kwargs}
        np.save(buf, inp, **kwargs)
        return buf.getvalue()

    def backward(self, inp: bytes) -> np.ndarray:
        """Deserialize bytes back into a NumPy array.

        Parameters
        ----------
        inp : bytes
            Bytes produced by :meth:`forward`.

        Returns
        -------
        np.ndarray
            Reconstructed array.
        """
        buf = io.BytesIO(inp)
        kwargs = {"allow_pickle": False, **self.backward_kwargs}
        return np.load(buf, **kwargs)
