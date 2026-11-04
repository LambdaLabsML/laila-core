""":class:`ComputationalData` subclass for Python ``dict`` payloads.

Dicts are serialized with msgpack rather than pickle so the on-disk
representation is language-neutral (other msgpack readers can decode
laila-pooled dicts) and substantially smaller for the common
shallow-string-keyed cases.

msgpack packs in strict mode: a dict holding values it cannot express
faithfully (tuples, tuple keys, ``OrderedDict`` / other ``dict`` or
scalar subclasses, arbitrary objects) raises at pack time and
:meth:`ComputationalData.serialize` falls back to pickle, so the exact
Python types come back on ``remember``. Such payloads give up msgpack's
language-neutrality; plain dicts keep it.
"""

import copy
from typing import ClassVar

from pydantic import InstanceOf

from ..transformation.serialization import MsgpackSerializer
from .compdata import ComputationalData, register_cdtype


@register_cdtype(dict)
class CD_dict(ComputationalData):
    """Computational-data wrapper for ``dict`` payloads.

    Defaults to :class:`MsgpackSerializer`. ``len(self)`` reports the
    number of keys; :attr:`shape` is intentionally undefined (dicts
    have no canonical shape and silently returning a fake value would
    mask bugs).
    """

    # ``InstanceOf`` keeps the caller's object (and its exact type:
    # ``OrderedDict``, ``defaultdict``, ...) instead of coercing to a plain
    # ``dict`` copy, so what you memorize is what you remember.
    data: InstanceOf[dict]
    _SERIALIZER_CLS: ClassVar[type] = MsgpackSerializer

    # --- Serializer getter/setter ---
    @property
    def serializer(self) -> MsgpackSerializer:
        """Return the serializer instance (public accessor)."""
        return self._ensure_serializer()

    @serializer.setter
    def serializer(self, value: MsgpackSerializer):
        """Set a new serializer instance."""
        if not isinstance(value, MsgpackSerializer):
            raise TypeError(f"serializer must be a MsgpackSerializer, got {type(value).__name__}")
        self._serializer = value

    def __len__(self):
        """Return the number of keys in the dict."""
        return len(self.data)

    @property
    def shape(self):
        """Not applicable for dicts.

        Raises
        ------
        AttributeError
            Always.
        """
        raise AttributeError("dict has no 'shape' attribute")

    def __copy__(self):
        """Return a shallow copy."""
        return type(self)(self.data.copy())

    def __deepcopy__(self, memo=None):
        """Return a deep copy."""
        return type(self)(copy.deepcopy(self.data, memo))

    def __repr__(self):
        """Return a developer-friendly representation."""
        return f"CD_dict({self.data!r})"
