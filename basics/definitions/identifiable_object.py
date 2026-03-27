"""Identifiable object base class with UUID and global-ID support."""
from __future__ import annotations
import string
import secrets
import threading

from pydantic import BaseModel, PrivateAttr
from typing import Optional, Any

import re
import uuid
import json

from ...macros.strings import _TOPMOST_SCOPE, _OBJECT_SCOPE, _GLOBAL_ID_SCOPE

GLOBAL_ID_REGEX_PATTERN = re.compile(
    r'^(?P<scopes>(?:[A-Za-z0-9_]+:)+)'
    r'(?P<uuid>[0-9a-fA-F-]{36})'
    r'(?:-(?P<evolution>\d+))?$'
)

_INIT_PENDING = threading.local()


class _LAILA_IDENTIFIABLE_OBJECT(BaseModel):
    """Pydantic base model providing UUID-based identity and global-ID encoding.

    Each instance carries a UUID, a list of hierarchical scopes, and an
    optional evolution counter.  These are combined into a canonical
    *global ID* string used for hashing, comparison, and serialization.

    Parameters
    ----------
    uuid : str, optional
        Explicit UUID to assign. Auto-generated if omitted.
    scopes : list[str], optional
        Hierarchical scope segments. Defaults to ``["OBJECT"]``.
    evolution : int, optional
        Version / evolution counter.
    nickname : str, optional
        Human-readable alias; deterministically converted to a UUID-5.
    """

    _uuid: str = PrivateAttr(default_factory=lambda: str(uuid.uuid4()))
    _scopes: list[str] = PrivateAttr(default_factory=lambda: list(["OBJECT"]))
    _evolution: Optional[int] = PrivateAttr(default=None)

    def __init__(self, **data: Any):
        """Initialize identity fields via thread-local staging."""
        uuid_input = data.pop("uuid", None)
        scopes_input = data.pop("scopes", None)
        evolution_input = data.pop("evolution", None)
        nickname_input = data.pop("nickname", None)

        if nickname_input is not None:
            _INIT_PENDING.uuid = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname(nickname_input)
        elif uuid_input is not None:
            _INIT_PENDING.uuid = str(uuid_input)
        else:
            _INIT_PENDING.uuid = None

        _INIT_PENDING.scopes = list(scopes_input) if scopes_input is not None else None
        _INIT_PENDING.evolution = evolution_input

        super().__init__(**data)

        _INIT_PENDING.uuid = None
        _INIT_PENDING.scopes = None
        _INIT_PENDING.evolution = None

    def model_post_init(self, __context: Any) -> None:
        """Apply staged identity values after Pydantic construction."""
        pending_uuid = getattr(_INIT_PENDING, "uuid", None)
        if pending_uuid is not None:
            self._uuid = pending_uuid
        pending_scopes = getattr(_INIT_PENDING, "scopes", None)
        if pending_scopes is not None:
            self._scopes = pending_scopes
        pending_evolution = getattr(_INIT_PENDING, "evolution", None)
        if pending_evolution is not None:
            self._evolution = pending_evolution


    @classmethod
    def from_global_id(cls, global_id: str) -> _LAILA_IDENTIFIABLE_OBJECT:
        """Construct an instance from an encoded global ID string.

        Parameters
        ----------
        global_id : str
            A valid global ID matching ``GLOBAL_ID_REGEX_PATTERN``.

        Returns
        -------
        _LAILA_IDENTIFIABLE_OBJECT
            New instance with identity fields parsed from *global_id*.

        Raises
        ------
        ValueError
            If *global_id* does not match the expected format.
        """
        if not cls.is_laila_resource(global_id):
            raise ValueError(f"Invalid GID format: {global_id}")
        identity_data = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(global_id)
        return cls(**identity_data)


    @staticmethod
    def to_global_id(
        uuid = None,
        scopes: Optional[list[str]] = None,
        evolution = None,
        nickname: Optional[str] = None
    ) -> str:
        """Build a global ID string from its constituent parts.

        Parameters
        ----------
        uuid : str, optional
            The UUID segment.
        scopes : list[str], optional
            Scope segments inserted between the top-level and GID scopes.
        evolution : int, optional
            If provided, appended as ``-<evolution>`` suffix.
        nickname : str, optional
            Converted to a deterministic UUID-5 before encoding.

        Returns
        -------
        str
            The assembled global ID.
        """
        if nickname is not None:
            uuid = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname(nickname)

        if scopes is None or len(scopes)==0:
            scopes = [_OBJECT_SCOPE]

        scopes = [_TOPMOST_SCOPE, *scopes, _GLOBAL_ID_SCOPE]
        scopes_str = ":".join(scopes) 
        prefix = f"{scopes_str}:"
        base = f"{prefix}{uuid}"
        if evolution is None:
            return base
        return f"{base}-{evolution}"
        

    @staticmethod
    def is_laila_resource(global_id: str) -> bool:
        """Return ``True`` if *global_id* matches the global-ID pattern."""
        return GLOBAL_ID_REGEX_PATTERN.match(global_id) is not None

    @property
    def global_id(self) -> str:
        """Fully-qualified global ID for this instance."""
        return _LAILA_IDENTIFIABLE_OBJECT.to_global_id(
            uuid=self._uuid, 
            scopes=self._scopes, 
            evolution=self._evolution
        )


    @global_id.setter
    def global_id(self, value: str) -> None:
        """Set identity fields by parsing a global ID string."""
        parsed = self.process_global_id(value)
        self._uuid = parsed["uuid"]
        self._scopes = parsed["scopes"]
        self._evolution = parsed["evolution"]

    @property
    def uuid(self) -> str:
        """The object's UUID string."""
        return self._uuid

    @uuid.setter
    def uuid(self, value: str) -> None:
        """Set the object's UUID."""
        self._uuid = value

    @property
    def scopes(self) -> list[str]:
        """The hierarchical scope list."""
        return self._scopes

    @scopes.setter
    def scopes(self, value: list[str]) -> None:
        """Replace the hierarchical scope list."""
        self._scopes = value

    @property
    def evolution(self) -> Optional[int]:
        """The evolution counter, or ``None`` for constants."""
        return self._evolution

    @evolution.setter
    def evolution(self, value: Optional[int]) -> None:
        """Set the evolution counter."""
        self._evolution = value

    @staticmethod
    def get_scopes_from_global_id(global_id: str) -> list[str]:
        """Extract the scopes list from a global ID string."""
        return _LAILA_IDENTIFIABLE_OBJECT.process_global_id(global_id)["scopes"]

    @staticmethod
    def get_uuid_from_global_id(global_id: str) -> str:
        """Extract the UUID from a global ID string."""
        return _LAILA_IDENTIFIABLE_OBJECT.process_global_id(global_id)["uuid"]

    @staticmethod
    def get_evolution_from_global_id(global_id: str) -> Optional[int]:
        """Extract the evolution counter from a global ID string."""
        return _LAILA_IDENTIFIABLE_OBJECT.process_global_id(global_id)["evolution"]

    def has_evolution(self) -> bool:
        """Return ``True`` if an evolution counter is set."""
        return self._evolution is not None
    
    @staticmethod
    def process_global_id(global_id: str) -> dict[str, Any]:
        """Parse a global ID into its ``uuid``, ``scopes``, and ``evolution`` parts.

        Parameters
        ----------
        global_id : str
            A valid global-ID string.

        Returns
        -------
        dict[str, Any]
            Keys: ``"uuid"`` (str), ``"scopes"`` (list[str]),
            ``"evolution"`` (int or None).

        Raises
        ------
        ValueError
            If *global_id* does not match the expected format.
        """
        if not _LAILA_IDENTIFIABLE_OBJECT.is_laila_resource(global_id):
            raise ValueError(f"Invalid GID format: {global_id}")
        match = GLOBAL_ID_REGEX_PATTERN.match(global_id)
        if match is None:
            raise ValueError(f"Invalid GID format: {global_id}")
        return {
            "uuid": match.group("uuid"),
            "scopes": match.group("scopes").split(":")[1:-1],
            "evolution": int(match.group("evolution")) if match.group("evolution") is not None else None,
        }

    @staticmethod
    def type(x:str) -> str:
        """Return ``"variable"`` if *x* has an evolution, otherwise ``"constant"``."""
        processed = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(x)
        if processed["evolution"] is not None:
            return "variable"
        else:
            return "constant"

    def __str__(self) -> str:
        """Return the global ID as the string representation."""
        return self.global_id

    def __repr__(self) -> str:
        """Return the global ID as the repr."""
        return self.global_id

    def __hash__(self) -> int:
        """Hash by global ID so instances are usable in sets and as dict keys."""
        return hash(self.global_id)

    def identity(self) -> dict[str, Any]:
        """Return a dict of non-empty identity fields (uuid, scopes, evolution)."""
        identity: dict[str, Any] = {"uuid": self.uuid}
        if self.scopes:
            identity["scopes"] = self.scopes
        if self.evolution is not None:
            identity["evolution"] = self.evolution
        return identity

    def identity_as_json(self) -> str:
        """Return the identity dict serialized as a JSON string."""
        return json.dumps(self.identity())

    @staticmethod
    def generate_uuid_from_nickname(nickname: str) -> str:
        """Deterministically generate a UUID-5 from a human-readable nickname."""
        from ... import get_active_namespace
        return str(uuid.uuid5(get_active_namespace(), nickname))
