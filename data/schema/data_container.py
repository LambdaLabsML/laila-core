"""Virtual base for every laila data container.

A *data container* is any identifiable object that holds records and
hands entries back to whoever reads from it. The base class fixes the
shape of that contract without choosing a key type -- that is left to
the subclasses, which is exactly where the two concrete families
differ:

- :class:`_LAILA_IDENTIFIABLE_POOL` (``data/schema/base.py``) is a
  **map**: keys are entry ``global_id`` strings and the container is
  the persistence tier that ``memorize`` / ``remember`` / ``forget``
  route to.
- :class:`MultiBuffer` (``data/multibuffer/multibuffer.py``) is a
  **list**: keys are integer slot indices and the container is a
  fixed-capacity ring with independent read/write heads -- the proxy a
  microcontroller uses to stand in front of a camera's frame buffer.

Both agree on the *value* contract: what goes in is a
:class:`~laila.policy.central.memory.record.record.Record` (an entry
plus provenance), and what comes out is the bare
:class:`~laila.entry.entry.Entry`.

Virtual in the laila sense
--------------------------
The class is instantiable as a type (Pydantic needs that to build the
schema, and tests use it to check identity plumbing), but the three
item-access methods raise :exc:`NotImplementedError` until a subclass
supplies them -- the same convention ``BotoPool._get_client`` and the
pool storage hooks follow.
"""

from typing import Any

from pydantic import ConfigDict, PrivateAttr

from ...atomic.definitions.locally_atomic_identifiable_object import (
    _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT,
)
from ...basics.definitions.cli_capable import _LAILA_CLI_CAPABLE_CLASS
from ...macros.strings import _DATA_CONTAINER_SCOPE


class _LAILA_IDENTIFIABLE_DATA_CONTAINER(
    _LAILA_CLI_CAPABLE_CLASS, _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT
):
    """Virtual base class for record-holding containers.

    Carries the identity contract (``global_id`` / uuid / nickname /
    scopes), per-instance atomic locking, and the CLI parameter
    resolution tiers. Declares -- but does not implement -- the item
    protocol every container exposes:

    - ``container[key]`` returns the entry stored at *key* (or
      ``None`` when the slot is empty).
    - ``container[key] = value`` stores *value* at *key*; subclasses
      decide how a bare entry becomes a record on the way in.
    - ``container.empty()`` discards every stored record.

    The meaning of *key* is entirely up to the subclass.
    """

    _scopes: list[str] = PrivateAttr(default_factory=lambda: list([_DATA_CONTAINER_SCOPE]))

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __getitem__(self, key: Any) -> Any:
        """Return the entry stored at *key*. Subclasses define key semantics."""
        raise NotImplementedError(
            f"{type(self).__name__} does not implement __getitem__; "
            "_LAILA_IDENTIFIABLE_DATA_CONTAINER is a virtual base."
        )

    def __setitem__(self, key: Any, value: Any) -> None:
        """Store *value* at *key*. Subclasses define key and record semantics."""
        raise NotImplementedError(
            f"{type(self).__name__} does not implement __setitem__; "
            "_LAILA_IDENTIFIABLE_DATA_CONTAINER is a virtual base."
        )

    def empty(self) -> None:
        """Discard every record held by this container."""
        raise NotImplementedError(
            f"{type(self).__name__} does not implement empty; "
            "_LAILA_IDENTIFIABLE_DATA_CONTAINER is a virtual base."
        )
