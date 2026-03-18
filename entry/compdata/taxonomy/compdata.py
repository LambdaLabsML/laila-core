from __future__ import annotations

import copy
from typing import Any, Dict, Tuple, Type, Optional
import numpy as np
from pydantic import BaseModel, ConfigDict, PrivateAttr

from ..transformation.serialization import PickleSerializer


# ---------------------------------------------------------------------
# Mapping: python type ➜ wrapper subclass
# ---------------------------------------------------------------------
TYPE_TO_WRAPPER: dict[type, Type["ComputationalData"]] = {}


def register_cdtype(*payload_types: type):
    """Decorator to register *cls* for one or more python payload types."""
    def deco(cls: Type["ComputationalData"]):
        for t in payload_types:
            TYPE_TO_WRAPPER[t] = cls
        return cls
    return deco


def _scalar_len():
    raise TypeError("Length undefined for scalars / objects without __len__")


# ---------------------------------------------------------------------
# Factory / Superclass
# ---------------------------------------------------------------------
class ComputationalData(BaseModel):
    """Generic computational data wrapper with dynamic subclass dispatch and serializer-based transformation."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
        repr=False,
    )

    data: object
    _serializer: PickleSerializer = PrivateAttr(default_factory=PickleSerializer)

    # --- Serializer getter/setter ---
    @property
    def serializer(self):
        """Return the current serializer instance."""
        return self._serializer

    @serializer.setter
    def serializer(self, value):
        """Set a new serializer instance."""
        if not isinstance(value, PickleSerializer):
            raise TypeError(
                f"serializer must be a PickleSerializer or compatible object, got {type(value).__name__}"
            )
        self._serializer = value

    def serialize(self) -> str:
        """Serialize data with current serializer."""
        return self.serializer.forward(self.data), self.serializer.backward_code

    # --- Allow single positional argument -------------------------------
    def __init__(self, *args, **kwargs):
        if args:
            if len(args) > 1:
                raise TypeError("At most one positional argument (the payload)")
            if "data" in kwargs:
                raise TypeError("Payload given both positionally and as 'data='")
            kwargs["data"] = args[0]
        super().__init__(**kwargs)

    # --- Factory dispatch -----------------------------------------------
    def __new__(cls, *args, **kwargs):
        if cls is not ComputationalData:  # direct subclass call
            return super().__new__(cls)

        # Extract payload
        data = args[0] if args else kwargs.get("data")
        if data is None:
            raise TypeError("Missing required argument 'data'")

        # Exact type match
        chosen = TYPE_TO_WRAPPER.get(type(data))

        # Walk MRO for ancestor matches
        if chosen is None:
            for parent in type(data).__mro__[1:]:
                chosen = TYPE_TO_WRAPPER.get(parent)
                if chosen:
                    break

        # Fallback
        if chosen is None:
            from .cd_object import CD_generic
            chosen = CD_generic

        return super().__new__(chosen)

    # --- Descriptor behavior -------------------------------------------
    def __get__(self, obj, objtype=None):
        """Instance → payload, Class → wrapper."""
        return self if obj is None else self.data

    # --- Convenience helpers -------------------------------------------
    def __getitem__(self, index):
        return self.data[index]

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(data={self.data!r})"

    __str__ = __repr__

    # --- Abstract stubs each wrapper overrides -------------------------
    def __len__(self):
        raise NotImplementedError

    @property
    def shape(self):
        raise NotImplementedError

    def __copy__(self):
        raise NotImplementedError

    def __deepcopy__(self, memo=None):
        raise NotImplementedError

    # --- Recovery logic -------------------------------------------------
    @classmethod
    def recover(cls, payload_blob, recovery_sequence):
        """Recover serialized data from a payload blob and its recorded recovery sequence."""

        def _recovery_step(data_as_bytes: bytes, passed_code_fn: str):
            """Deserialize data using the provided code snippet defining one function."""
            local_env: Dict[str, Any] = {}
            exec(passed_code_fn, {}, local_env)
            fns = [v for v in local_env.values() if callable(v)]
            if len(fns) != 1:
                raise ValueError("passed_code_fn must define exactly one callable function")
            fn = fns[0]
            return fn(data_as_bytes)

        current_payload = payload_blob
        for fn in recovery_sequence:
            current_payload = _recovery_step(current_payload, fn)

        if current_payload is not None and not isinstance(current_payload, ComputationalData):
            return ComputationalData(data=current_payload)

        return current_payload
