"""Enumeration of lifecycle states for an Entry."""

from enum import Enum, auto


class EntryState(Enum):
    """Lifecycle state of an ``Entry``.

    Members
    -------
    READY
        Data is available and fully materialised.
    POOLED
        Entry is stored in a pool.
    POOLING
        Entry is being transferred to a pool.
    STAGED
        Entry is staged but data may not yet be materialised.
    STALE
        Entry data is out of date.
    NA
        Not applicable. Used by Entry subclasses whose lifecycle states are
        meaningless (e.g. ``Manifest``). Only ``Entry`` may hold non-``NA``
        states.
    """
    READY = auto()
    POOLED = auto()
    POOLING = auto()
    STAGED = auto()
    STALE = auto()
    NA = auto()
