"""Lightweight read-only views over Entry identity and state."""

from pydantic import PrivateAttr

from ..basics.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from .constitution import Constitution
from .entry_state import EntryState


class EntryIdentityView(_LAILA_IDENTIFIABLE_OBJECT):
    """Read-only projection exposing only the identity and state of an Entry."""

    _scopes: list[str] = PrivateAttr(default_factory=lambda: list(["ENTRY"]))
    _state: EntryState


class EntryHolisticView(_LAILA_IDENTIFIABLE_OBJECT):
    """Read-only projection exposing identity, state, and constitution of an Entry."""

    _scopes: list[str] = PrivateAttr(default_factory=lambda: list(["ENTRY"]))
    _state: EntryState
    _constitution: Constitution
