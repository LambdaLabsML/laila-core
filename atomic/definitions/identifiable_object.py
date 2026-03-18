from __future__ import annotations
import string
import secrets

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


class _LAILA_IDENTIFIABLE_OBJECT(BaseModel):

    _uuid: str = PrivateAttr(default_factory=lambda: str(uuid.uuid4()))
    _scopes: list[str] = PrivateAttr(default_factory=lambda: list(["OBJECT"]))
    _evolution: Optional[int] = PrivateAttr(default=None)

    def __init__(self, **data: Any):
        uuid_input = data.pop("uuid", None)
        scopes_input = data.pop("scopes", None)
        evolution_input = data.pop("evolution", None)
        nickname_input = data.pop("nickname", None)

        super().__init__(**data)

        if nickname_input is not None:
            # Nickname has absolute precedence over uuid.
            self._uuid = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname(nickname_input)
        elif uuid_input is not None:
            self._uuid = str(uuid_input)

        if scopes_input is not None:
            self._scopes = list(scopes_input)

        if evolution_input is not None:
            self._evolution = evolution_input


    @classmethod
    def from_global_id(cls, global_id: str) -> _LAILA_IDENTIFIABLE_OBJECT:
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
        return GLOBAL_ID_REGEX_PATTERN.match(global_id) is not None

    @property
    def global_id(self) -> str:
        return _LAILA_IDENTIFIABLE_OBJECT.to_global_id(
            uuid=self._uuid, 
            scopes=self._scopes, 
            evolution=self._evolution
        )


    @global_id.setter
    def global_id(self, value: str) -> None:
        parsed = self.process_global_id(value)
        self._uuid = parsed["uuid"]
        self._scopes = parsed["scopes"]
        self._evolution = parsed["evolution"]

    @property
    def uuid(self) -> str:
        return self._uuid

    @uuid.setter
    def uuid(self, value: str) -> None:
        self._uuid = value

    @property
    def scopes(self) -> list[str]:
        return self._scopes

    @scopes.setter
    def scopes(self, value: list[str]) -> None:
        self._scopes = value

    @property
    def evolution(self) -> Optional[int]:
        return self._evolution

    @evolution.setter
    def evolution(self, value: Optional[int]) -> None:
        self._evolution = value

    @staticmethod
    def get_scopes_from_global_id(global_id: str) -> list[str]:
        return _LAILA_IDENTIFIABLE_OBJECT.process_global_id(global_id)["scopes"]

    @staticmethod
    def get_uuid_from_global_id(global_id: str) -> str:
        return _LAILA_IDENTIFIABLE_OBJECT.process_global_id(global_id)["uuid"]

    @staticmethod
    def get_evolution_from_global_id(global_id: str) -> Optional[int]:
        return _LAILA_IDENTIFIABLE_OBJECT.process_global_id(global_id)["evolution"]

    def has_evolution(self) -> bool:
        return self._evolution is not None
    
    @staticmethod
    def process_global_id(global_id: str) -> dict[str, Any]:
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
        processed = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(x)
        if processed["evolution"] is not None:
            return "variable"
        else:
            return "constant"

    def __str__(self) -> str:
        return self.global_id

    def __repr__(self) -> str:
        return self.global_id

    def __hash__(self) -> int:
        return hash(self.global_id)

    def identity(self) -> dict[str, Any]:
        identity: dict[str, Any] = {"uuid": self.uuid}
        if self.scopes:
            identity["scopes"] = self.scopes
        if self.evolution is not None:
            identity["evolution"] = self.evolution
        return identity

    def identity_as_json(self) -> str:
        return json.dumps(self.identity())

    @staticmethod
    def generate_uuid_from_nickname(nickname: str) -> str:
        from ... import get_active_namespace
        return str(uuid.uuid5(get_active_namespace(), nickname))
