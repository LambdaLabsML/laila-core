"""Root base class for every laila object.

:class:`_LAILA_OBJECT` sits at the very bottom of the laila class
hierarchy -- both :class:`_LAILA_IDENTIFIABLE_OBJECT` (identity /
global ids) and :class:`_LAILA_LOCALLY_ATOMIC_OBJECT` (per-instance
locking) derive from it, so entries, policies, pools, futures, the
logger and the ``Atomic*`` wrappers all share whatever lives here.

Today that is a single concern: a **creation timestamp** recording
when the Python object was constructed, as an ISO-8601 UTC string with
millisecond precision (the same shape as
``Record.record_timestamp``). It answers "when was this object
created?" for any laila object, independent of when (or whether) it is
memorized into a pool.

Implementation note: the stamp is applied in :meth:`__init__` *after*
``super().__init__`` returns rather than in ``model_post_init``.
Pydantic v2's ``validate_python`` wipes private attributes set before
the base initialiser runs, and not every subclass chains
``model_post_init`` back to this class; stamping after the super call
is robust to both. Deserialization paths that bypass ``__init__``
(e.g. ``Entry.from_dict``, which uses ``cls.__new__``) restore or
re-stamp the value by assigning ``_creation_timestamp`` directly; the
public :attr:`creation_timestamp` property is read-only.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, PrivateAttr


def _now_creation_timestamp() -> str:
    """Return the current UTC time as an ISO-8601 string (millisecond precision).

    Shared by :class:`_LAILA_OBJECT` and by deserializers that need to
    stamp objects constructed outside ``__init__``.
    """
    return datetime.now(UTC).isoformat(timespec="milliseconds")


class _LAILA_OBJECT(BaseModel):
    """Pydantic base model shared by every laila object.

    Provides :attr:`creation_timestamp` -- the ISO-8601 UTC creation
    time of the instance, captured once at construction.

    Notes
    -----
    The private attribute uses a plain ``PrivateAttr(default=None)``
    rather than a ``default_factory`` for the same reason identity
    fields do in :class:`_LAILA_IDENTIFIABLE_OBJECT`: Pydantic
    re-inspects private factories on every instantiation, and this
    class is on the hot path of every entry and task. The value is
    assigned in :meth:`__init__` instead.
    """

    _creation_timestamp: str | None = PrivateAttr(default=None)

    def __init__(self, **data: Any):
        """Delegate to Pydantic, then stamp the creation timestamp."""
        super().__init__(**data)
        self._creation_timestamp = _now_creation_timestamp()

    def model_post_init(self, __context: Any) -> None:
        """Cooperative no-op hook.

        Declared explicitly so that every class below this root has a
        well-defined ``model_post_init`` to chain to via ``super()``.
        Pydantic wraps it to initialise ``__pydantic_private__`` first.
        Subclasses in mixin diamonds (e.g.
        ``_LAILA_LOCALLY_ATOMIC_OBJECT`` + ``_LAILA_IDENTIFIABLE_OBJECT``)
        must define their own hook and call ``super()`` -- Pydantic's
        auto-injected hook does not chain and would break the diamond.
        """
        super().model_post_init(__context)

    @property
    def creation_timestamp(self) -> str | None:
        """ISO-8601 UTC timestamp of when this object was constructed.

        Read-only on purpose: it is runtime provenance, not
        configuration. (A public setter would also make the CLI-capable
        machinery treat it as a ``laila.args``-configurable field and
        mirror it into ``laila.args.environment``.) Deserializers that
        need to restore a persisted value assign the private
        ``_creation_timestamp`` attribute directly.

        ``None`` only for instances built through paths that bypass
        ``__init__`` and have not yet restored the value.
        """
        return self._creation_timestamp
