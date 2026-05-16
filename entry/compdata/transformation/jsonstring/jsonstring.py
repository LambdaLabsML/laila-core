"""JSON string serialisation / deserialisation data transformation.

The :class:`JsonString` transformation is the canonical "round-trip
through JSON text" step. ``forward`` accepts any JSON-serialisable
Python object and emits a compact JSON string (no whitespace,
unicode-preserving); ``backward`` parses the string back to the
equivalent Python value.

This is most useful as the final step of a pipeline whose pool
backend stores plain strings (filesystem JSON files, S3 JSON objects,
postgres TEXT columns, ...). For binary serialisation of arbitrary
Python objects, prefer :class:`PickleSerializer`; for compact
binary representations of JSON-shaped data, prefer
:class:`MsgpackSerializer`.
"""

import json
import textwrap
from typing import Any
from ..base import _data_transformation


class JsonString(_data_transformation):
    """Reversible JSON string transformation.

    Forward serialises a Python object to a compact JSON string;
    backward parses it back.
    """

    name: str = "json_string"

    def model_post_init(self, __context: Any) -> None:
        """Build standalone backward recovery code."""
        # Standalone recovery code mirroring `backward()` with embedded kwargs
        self.backward_code = textwrap.dedent(f"""
            def backward(data):
                import json
                if not isinstance(data, str):
                    raise TypeError("JsonString.backward expects a JSON string (str)")
                return json.loads(data)
        """)

    
    def forward(self, data: Any) -> str:
        """Serialize *data* to a compact JSON string.

        Parameters
        ----------
        data : Any
            JSON-serialisable Python object.

        Returns
        -------
        str
            Compact JSON string.

        Raises
        ------
        TypeError
            If *data* is not JSON-serialisable.
        """
        try:
            return json.dumps(
                data,
                separators=(",", ":"),
                ensure_ascii=False,
                **self.forward_kwargs,
            )
        except (TypeError, ValueError) as e:
            raise TypeError(f"JsonString.forward: object not JSON serializable: {e!s}")

    
    def backward(self, data: str) -> Any:
        """Deserialize a JSON string back to a Python object.

        Parameters
        ----------
        data : str
            JSON string.

        Returns
        -------
        Any
            Parsed Python object.

        Raises
        ------
        TypeError
            If *data* is not a string or contains invalid JSON.
        """
        if not isinstance(data, str):
            raise TypeError("JsonString.backward expects a JSON string (str)")
        try:
            return json.loads(data, **self.backward_kwargs)
        except json.JSONDecodeError as e:
            raise TypeError(f"JsonString.backward: invalid JSON string: {e.msg}")
