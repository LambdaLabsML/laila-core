import copy
from .compdata import ComputationalData, register_cdtype, _scalar_len
from ..transformation.serialization import PickleSerializer
from pydantic import PrivateAttr


@register_cdtype(object)  # final catch-all
class CD_generic(ComputationalData):
    """Catch-all computational data type for arbitrary Python objects."""

    _serializer: PickleSerializer = PrivateAttr(default_factory=PickleSerializer)

    # --- Serializer getter/setter ---
    @property
    def serializer(self) -> PickleSerializer:
        """Return the serializer instance (public accessor)."""
        return self._serializer

    @serializer.setter
    def serializer(self, value: PickleSerializer):
        """Set a new serializer instance."""
        if not isinstance(value, PickleSerializer):
            raise TypeError(
                f"serializer must be a PickleSerializer, got {type(value).__name__}"
            )
        self._serializer = value

    # --- ComputationalData behavior ---
    def __len__(self):
        if hasattr(self.data, "__len__"):
            return len(self.data)  # type: ignore[arg-type]
        return _scalar_len()

    @property
    def shape(self):
        raise AttributeError(f"{type(self.data).__name__} has no 'shape' attribute")

    def __copy__(self):
        return type(self)(self.data)

    def __deepcopy__(self, memo=None):
        return type(self)(copy.deepcopy(self.data, memo))

    def __repr__(self):
        return f"CD_generic({self.data!r})"
