"""Simple constitution: an ordered chain of code strings.

A `SimpleConstitution` carries a list of Python source strings, each
defining exactly one callable.  `build(payload_input)` threads
`payload_input` through every callable in order, mirroring what was
previously called the "recovery sequence" for a serialized payload.
"""

from __future__ import annotations

from typing import Any, List, Optional

from pydantic import PrivateAttr

from .constitution import Constitution, _exec_one_fn, _register_kind


@_register_kind("simple")
class SimpleConstitution(Constitution):
    """An ordered chain of single-callable code strings."""

    _codes: List[str] = PrivateAttr(default_factory=list)

    def __init__(self, **data: Any):
        """Accept `codes` as a list of source strings."""
        super().__init__()
        codes = data.get("codes", [])
        if not isinstance(codes, list) or not all(isinstance(c, str) for c in codes):
            raise TypeError("codes must be a list of Python source strings")
        self._codes = list(codes)

    @property
    def codes(self) -> List[str]:
        """The ordered list of source strings."""
        return list(self._codes)

    def build(self, payload_input: Optional[Any] = None) -> Any:
        """Thread *payload_input* through every code in order."""
        current = payload_input
        for code in self._codes:
            current = _exec_one_fn(code)(current)
        return current

    def as_dict(self) -> dict:
        """Serialize as `{"_kind": "simple", "codes": [...]}`."""
        return {"_kind": "simple", "codes": list(self._codes)}

    @classmethod
    def _from_dict(cls, in_dict: dict) -> "SimpleConstitution":
        """Build a `SimpleConstitution` from its serialized dict."""
        return cls(codes=in_dict.get("codes", []))
