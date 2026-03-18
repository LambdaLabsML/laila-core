# json_string_op.py
import json
import textwrap
from typing import Any
from ..base import _data_transformation


class JsonString(_data_transformation):
    name: str = "json_string"

    
    def model_post_init(self, __context: Any) -> None:
        # Standalone recovery code mirroring `backward()` with embedded kwargs
        self.backward_code = textwrap.dedent(f"""
            def backward(data):
                import json
                if not isinstance(data, str):
                    raise TypeError("JsonString.backward expects a JSON string (str)")
                return json.loads(data)
        """)

    
    def forward(self, data: Any) -> str:
        """
        Serialize `data` to a JSON string.
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
        """
        Deserialize the JSON string back to the original Python object.
        """
        if not isinstance(data, str):
            raise TypeError("JsonString.backward expects a JSON string (str)")
        try:
            return json.loads(data, **self.backward_kwargs)
        except json.JSONDecodeError as e:
            raise TypeError(f"JsonString.backward: invalid JSON string: {e.msg}")
