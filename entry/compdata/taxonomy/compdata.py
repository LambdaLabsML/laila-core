"""Base :class:`ComputationalData` class -- type-dispatched payload wrapper.

Every :class:`Entry` payload is wrapped in a :class:`ComputationalData`
(or one of its registered subclasses) so the system has a uniform
handle on the data regardless of underlying Python type. The wrapper
provides:

- a ``data`` attribute exposing the unwrapped value,
- a swappable ``serializer`` (``PickleSerializer`` by default) used by
  pool-side ``serialize()`` to produce bytes plus an inverse-decode
  source string,
- a stub ``__len__`` / ``shape`` / ``__copy__`` / ``__deepcopy__`` API
  that subclasses fill in for their concrete payload type.

Type dispatch
-------------
Subclasses decorate themselves with :func:`register_cdtype(*types)` to
claim ownership of one or more Python types. ``ComputationalData(data)``
then looks up ``type(data)`` in :data:`TYPE_TO_WRAPPER` (and walks the
MRO if no exact match is found) and instantiates the matching subclass.
A generic ``CD_generic`` fallback wraps anything unknown.

This means user code can simply write ``Entry.constant(my_array)`` and
the right wrapper (e.g. ``CDNumpy``, ``CDTorch``) is selected
automatically -- subclasses with bespoke serialization
(:mod:`numpy`-aware, :mod:`torch`-aware) are picked transparently.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, PrivateAttr

from ..transformation.serialization import PickleSerializer

# ---------------------------------------------------------------------
# Mapping: python type ➜ wrapper subclass
# ---------------------------------------------------------------------
TYPE_TO_WRAPPER: dict[type, type[ComputationalData]] = {}


def register_cdtype(*payload_types: type):
    """Decorator that registers *cls* as the wrapper for one or more
    payload types.

    Each call records ``TYPE_TO_WRAPPER[t] = cls`` for every ``t`` in
    *payload_types*. The dispatch in :meth:`ComputationalData.__new__`
    first checks for an exact type match and then walks the MRO, so
    registering against an abstract base (e.g. ``np.ndarray``) is
    enough to claim every subclass that doesn't have its own explicit
    registration.

    Parameters
    ----------
    *payload_types : type
        Python types that should resolve to this wrapper subclass.
    """

    def deco(cls: type[ComputationalData]):
        for t in payload_types:
            TYPE_TO_WRAPPER[t] = cls
        return cls

    return deco


def _scalar_len():
    """Raise ``TypeError`` for payloads that have no meaningful length.

    Used by scalar wrapper subclasses (single int, single float, etc.)
    to keep ``__len__`` semantically honest -- ``len(scalar)`` should
    fail loudly rather than silently return 1.
    """
    raise TypeError("Length undefined for scalars / objects without __len__")


# ---------------------------------------------------------------------
# Factory / Superclass
# ---------------------------------------------------------------------
class ComputationalData(BaseModel):
    """Generic computational-data wrapper with dynamic subclass dispatch
    and serializer-based transformation.

    Direct instantiation (``ComputationalData(value)``) routes through
    ``__new__`` to the registered subclass for ``type(value)``,
    falling back to a generic object wrapper. Subclass instantiation
    (``CDNumpy(value)``) bypasses dispatch -- the chosen subclass is
    used directly.
    """

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
        """The serializer used by :meth:`serialize`.

        Defaults to :class:`PickleSerializer`. Subclasses with bespoke
        serializers (e.g. :class:`CDNumpy` -> NumPy npy serializer,
        :class:`CDTorch` -> torch.save serializer) override the
        ``default_factory`` of the underlying private attribute.
        """
        return self._serializer

    @serializer.setter
    def serializer(self, value):
        """Replace the serializer.

        The new value must duck-type as a :class:`PickleSerializer`
        (i.e. provide ``.forward(data)`` and ``.backward_code``).
        """
        if not isinstance(value, PickleSerializer):
            raise TypeError(
                f"serializer must be a PickleSerializer or compatible object, got {type(value).__name__}"
            )
        self._serializer = value

    def serialize(self) -> str:
        """Serialize :attr:`data` to bytes with the current serializer.

        Returns
        -------
        tuple[bytes, str]
            ``(serialized_bytes, backward_code)``. The backward-code
            string defines a one-argument Python callable that, applied
            to ``serialized_bytes``, recovers the original
            :attr:`data` value. The pool's :meth:`Entry.serialize`
            then bundles this code into the inverse-transformation
            chain so the read-side can rebuild without knowing the
            specific serializer that was used at write time.
        """
        return self.serializer.forward(self.data), self.serializer.backward_code

    def __init__(self, *args, **kwargs):
        """Construct from either a positional payload or ``data=`` kwarg.

        ``ComputationalData(value)`` and ``ComputationalData(data=value)``
        are equivalent; supplying both raises ``TypeError``. This
        symmetry lets callers write either form without a thin wrapper.
        """
        if args:
            if len(args) > 1:
                raise TypeError("At most one positional argument (the payload)")
            if "data" in kwargs:
                raise TypeError("Payload given both positionally and as 'data='")
            kwargs["data"] = args[0]
        super().__init__(**kwargs)

    def __new__(cls, *args, **kwargs):
        """Dispatch to the registered subclass matching the payload type.

        Resolution order
        ----------------
        1. If a concrete subclass is being instantiated directly
           (``CDNumpy(value)``), bypass dispatch and use that subclass.
        2. Otherwise extract the payload, look it up in
           :data:`TYPE_TO_WRAPPER` by exact type.
        3. If no exact match, walk the payload's MRO and use the first
           ancestor with a registration.
        4. Fall back to ``CD_generic`` (which uses pickle for
           everything).

        Raises
        ------
        TypeError
            If no payload was provided.
        """
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
        """Descriptor protocol: class-level access yields the wrapper,
        instance-level access yields the unwrapped payload.

        This makes a :class:`ComputationalData` field on a class behave
        like the underlying payload at the instance level (``inst.x``
        returns the raw array) while still allowing introspection at
        the class level (``Klass.x`` returns the wrapper).
        """
        return self if obj is None else self.data

    def __getitem__(self, index):
        """Index into the underlying payload."""
        return self.data[index]

    def __repr__(self) -> str:
        """Return a developer-friendly representation."""
        return f"{self.__class__.__name__}(data={self.data!r})"

    __str__ = __repr__

    def __len__(self):
        """Return the length of the payload (subclasses must override)."""
        raise NotImplementedError

    @property
    def shape(self):
        """Shape of the payload (subclasses must override)."""
        raise NotImplementedError

    def __copy__(self):
        """Shallow copy (subclasses must override)."""
        raise NotImplementedError

    def __deepcopy__(self, memo=None):
        """Deep copy (subclasses must override)."""
        raise NotImplementedError
