"""Abstract `Constitution` base class.

A `Constitution` describes how to materialize an Entry's payload from
some upstream input.  Two concrete subclasses exist:

* `SimpleConstitution` — an ordered list of code strings; each defines
  exactly one callable that consumes the previous step's output.  Used
  to undo serialization (e.g. base64 → pickle backward).
* `ComplexConstitution` — a single code string that takes a
  `Manifest` and returns the payload value.  Used to compose entries
  from other entries.

Both subclasses implement `build(payload_input=None)` and `as_dict()`.
The dispatcher `Constitution.from_dict(...)` selects the subclass by
the `_kind` tag baked into a serialized constitution.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Optional, Type

from pydantic import BaseModel, ConfigDict


_REGISTRY: Dict[str, Type["Constitution"]] = {}


def _register_kind(kind: str):
    """Decorator to register a `Constitution` subclass under a `_kind` tag."""

    def deco(cls: Type["Constitution"]) -> Type["Constitution"]:
        _REGISTRY[kind] = cls
        cls._kind = kind  # type: ignore[attr-defined]
        return cls

    return deco


def _exec_one_fn(code: str) -> Callable[[Any], Any]:
    """Compile a source string and return its single defined callable.

    Parameters
    ----------
    code : str
        A Python source string defining exactly one top-level callable.

    Returns
    -------
    Callable
        The single callable defined by *code*.

    Raises
    ------
    ValueError
        If *code* defines zero or multiple top-level callables.
    """
    namespace: Dict[str, Any] = {}
    exec(code, namespace)
    fns = [v for k, v in namespace.items() if not k.startswith("__") and callable(v)]
    if len(fns) != 1:
        raise ValueError(
            "constitution code must define exactly one callable function"
        )
    return fns[0]


class Constitution(BaseModel, ABC):
    """Abstract base for entry constitutions.

    A constitution knows how to produce an entry's payload value, given
    optional `payload_input` (used by simple constitutions to receive the
    serialized blob) or no input (complex constitutions read from their
    bound manifest instead).
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    _kind: str = ""

    @abstractmethod
    def build(self, payload_input: Optional[Any] = None) -> Any:
        """Produce and return the entry's payload value."""

    @abstractmethod
    def as_dict(self) -> dict:
        """Return the serialized representation of this constitution."""

    @classmethod
    def from_dict(cls, in_dict: Optional[dict]) -> Optional["Constitution"]:
        """Reconstruct a `Constitution` from its serialized dict.

        Parameters
        ----------
        in_dict : dict or None
            The serialized constitution, including a `_kind` tag.

        Returns
        -------
        Constitution or None
            The reconstructed constitution, or `None` when *in_dict* is
            `None`.

        Raises
        ------
        KeyError
            If *in_dict* is missing the `_kind` tag.
        ValueError
            If the `_kind` is not registered.
        """
        if in_dict is None:
            return None
        if "_kind" not in in_dict:
            raise KeyError("serialized constitution is missing the '_kind' tag")
        kind = in_dict["_kind"]
        sub_cls = _REGISTRY.get(kind)
        if sub_cls is None:
            raise ValueError(
                f"no Constitution subclass registered for kind '{kind}'. "
                f"Registered: {list(_REGISTRY.keys())}"
            )
        return sub_cls._from_dict(in_dict)

    @classmethod
    @abstractmethod
    def _from_dict(cls, in_dict: dict) -> "Constitution":
        """Subclass-specific dict → instance hook used by `from_dict`."""
