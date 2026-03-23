"""Abstract base classes for data transformations and the ``TransformationSequence`` pipeline."""

from __future__ import annotations
from typing import Any, Dict, ClassVar, Type, List
from abc import ABC, abstractmethod
from pydantic import BaseModel, Field, ConfigDict


class _data_transformation(BaseModel, ABC):
    """Abstract base for a single reversible data transformation.

    Subclasses are auto-registered in ``REGISTRY`` keyed by their ``name``
    field.  Every subclass must implement ``forward`` and ``backward``.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str = Field(default_factory=str)
    forward_kwargs: Dict[str, Any] = Field(default_factory=dict)
    backward_kwargs: Dict[str, Any] = Field(default_factory=dict)
    backward_code: str = Field(default_factory=str)
    REGISTRY: ClassVar[Dict[str, Type["_data_transformation"]]] = {}

    def __init_subclass__(cls, **kwargs):
        """Register the subclass in ``REGISTRY`` when *name* is set."""
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
        """Apply all transformations in order and collect inverse codes.

        Parameters
        ----------
        data : Any
            Input data to transform.

        Returns
        -------
        tuple[Any, list[str]]
            Transformed data and the list of inverse code snippets in
            reverse order.
        """
        current = data
        inverse_codes: List[str] = []

        for transformation in self.transformations:
            current = transformation.forward(current)
            inverse_codes.append(transformation.backward_code)

        return current, inverse_codes[::-1]


    def __iter__(self):
        """Iterate over the contained transformations."""
        return iter(self.transformations)
    

    def append(self, t) -> Any:
        """Append one or more transformations to the pipeline.

        Parameters
        ----------
        t : _data_transformation or list[_data_transformation]
            Transformation(s) to append.

        Returns
        -------
        TransformationSequence
            ``self``, for chaining.

        Raises
        ------
        TypeError
            If *t* is not a valid transformation or list thereof.
        """
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
        """Return a human-readable pipeline summary."""
        if not self.transformations:
            return f"{self.__class__.__name__}(identity)"
        names = " -> ".join(getattr(t, "name", t.__class__.__name__) for t in self.transformations)
        return f"{self.__class__.__name__}({names})"


