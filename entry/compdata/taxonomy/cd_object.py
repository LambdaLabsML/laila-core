""":class:`ComputationalData` catch-all subclass for arbitrary Python objects.

Registered against :class:`object` so the MRO walk in
``ComputationalData.__new__`` always finds at least this class for
otherwise-unknown payload types. Uses :class:`PickleSerializer` -- the
broadest-compatibility option, at the cost of being Python-specific
on the wire.
"""

import copy
from .compdata import ComputationalData, register_cdtype, _scalar_len
from ..transformation.serialization import PickleSerializer
from pydantic import PrivateAttr


@register_cdtype(object)  # final catch-all
class CD_generic(ComputationalData):
    """Catch-all computational-data wrapper for arbitrary Python objects.

    Selected by :meth:`ComputationalData.__new__` when no more specific
    wrapper is registered for the payload's type. Pickles the value to
    bytes for serialization. ``len()`` defers to the payload's own
    ``__len__`` when present and otherwise raises (per
    :func:`_scalar_len`).
    """

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

    def __len__(self):
        """Return the length if the payload supports ``__len__``."""
        if hasattr(self.data, "__len__"):
            return len(self.data)  # type: ignore[arg-type]
        return _scalar_len()

    @property
    def shape(self):
        """Not applicable for generic objects.

        Raises
        ------
        AttributeError
            Always.
        """
        raise AttributeError(f"{type(self.data).__name__} has no 'shape' attribute")

    def __copy__(self):
        """Return a shallow copy."""
        return type(self)(self.data)

    def __deepcopy__(self, memo=None):
        """Return a deep copy."""
        return type(self)(copy.deepcopy(self.data, memo))

    def __repr__(self):
        """Return a developer-friendly representation."""
        return f"CD_generic({self.data!r})"
