"""Pickle serialisation / deserialisation transformation."""

import pickle
import textwrap
from typing import Any
from ..base import _data_transformation


class PickleSerializer(_data_transformation):
    """Reversible pickle serialiser for arbitrary Python objects."""

    name: str = "pickle"

    def model_post_init(self, __context: Any) -> None:
        """Build backward_code dynamically after model creation."""
        self.backward_code = textwrap.dedent(f"""
            def backward(inp):
                import pickle
                kwargs = {self.backward_kwargs!r}
                return pickle.loads(inp, **kwargs)
        """)

    def forward(self, inp: Any) -> bytes:
        """Serialize a Python object into bytes using pickle.

        Parameters
        ----------
        inp : Any
            Object to serialize.

        Returns
        -------
        bytes
            Pickled bytes.
        """
        kwargs = {**self.forward_kwargs}
        return pickle.dumps(inp, **kwargs)

    def backward(self, inp: bytes) -> Any:
        """Deserialize bytes back into a Python object using pickle.

        Parameters
        ----------
        inp : bytes
            Pickled bytes.

        Returns
        -------
        Any
            Unpickled Python object.
        """
        kwargs = {**self.backward_kwargs}
        return pickle.loads(inp, **kwargs)
