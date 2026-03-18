from pydantic import BaseModel, ConfigDict, model_validator
from pydantic import BaseModel, Field, PrivateAttr
from typing import Optional, Any, Callable

from ..atomic.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from .entry_state import EntryState
from .entry_constitution import EntryConstitution


class EntryIdentityView(_LAILA_IDENTIFIABLE_OBJECT):
    _scopes: list[str] = PrivateAttr(default_factory=lambda: list(["ENTRY"]))
    _state: EntryState 


class EntryHolisticView(_LAILA_IDENTIFIABLE_OBJECT):
    _scopes: list[str] = PrivateAttr(default_factory=lambda: list(["ENTRY"]))
    _state: EntryState 
    _constitution: EntryConstitution

