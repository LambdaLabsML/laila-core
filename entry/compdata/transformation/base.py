# ops.py
from __future__ import annotations
from typing import Any, Dict, ClassVar, Type, List
from abc import ABC, abstractmethod
from pydantic import BaseModel, Field, ConfigDict


# -----------------------
# Base transformation model with registry
# -----------------------
class _data_transformation(BaseModel, ABC):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str = Field(default_factory=str)
    forward_kwargs: Dict[str, Any] = Field(default_factory=dict)
    backward_kwargs: Dict[str, Any] = Field(default_factory=dict)
    backward_code: str = Field(default_factory=str)
    # name -> subclass
    REGISTRY: ClassVar[Dict[str, Type["_data_transformation"]]] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        n = getattr(cls, "name", None)
        if isinstance(n, str) and n:
            _data_transformation.REGISTRY[n] = cls

    @abstractmethod
    def forward(self, data: Any) -> Any: ...

    @abstractmethod
    def backward(self, data: Any) -> Any: ...



class TransformationSequence(BaseModel):
    """
    Pydantic v2 sequential transformation pipeline.

    - forward(): applies transformations in order
    - backward(): applies transformations in reverse order
    - empty pipeline = identity (no-op)
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    transformations: List[_data_transformation] = Field(default_factory=list)

    def forward(self, data: Any) -> Any:
        current = data
        inverse_codes: List[str] = []

        for transformation in self.transformations:
            current = transformation.forward(current)
            inverse_codes.append(transformation.backward_code)

        return current, inverse_codes[::-1]


    def __iter__(self):
        return iter(self.transformations)
    

    def append(self, t) -> Any:
        if isinstance (t, _data_transformation):
            self.transformations.append(t)
            return self

        if isinstance (t, list):
            for _t in t:
                if not isinstance(_t, _data_transformation):
                    raise TypeError("All elements must be _data_transformation instances.")
                self.transformations.append(_t)
            return self

        raise TypeError("append expects a _data_transformation or list of them.")

    
    def __repr__(self) -> str:
        if not self.transformations:
            return f"{self.__class__.__name__}(identity)"
        names = " -> ".join(getattr(t, "name", t.__class__.__name__) for t in self.transformations)
        return f"{self.__class__.__name__}({names})"


