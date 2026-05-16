"""Identifiable-object base class with UUID and global-ID support.

The single class in this module, :class:`_LAILA_IDENTIFIABLE_OBJECT`,
is the foundation everything else in laila inherits from. It pins
down the *identity* contract that the rest of the codebase relies on:

- A UUID (defaulting to a fresh ``uuid4``).
- A list of hierarchical *scopes* (e.g. ``["POLICY"]``, ``["FUTURE"]``).
- An optional *evolution* counter that distinguishes successive
  versions of the same logical object (``None`` for "constant" /
  unversioned objects, an integer for "variable" / mutable ones).

These three components combine into the canonical *global ID*
string produced by :meth:`_LAILA_IDENTIFIABLE_OBJECT.global_id` --
the form is::

    TOP:scope1:...:scopeN:GID:<uuid>[-<evolution>]

The string is used for hashing, equality, dict keys, RPC envelopes,
serialised forms, and pretty-printing alike, so consistency is
critical.

The ``GLOBAL_ID_REGEX_PATTERN`` regex at the top of the module is the
single source of truth for parsing global ids; all helpers funnel
through it via :meth:`_LAILA_IDENTIFIABLE_OBJECT.process_global_id`.

Implementation note: identity fields are passed at construction time
through a thread-local "staging" object (``_INIT_PENDING``). This is
needed because Pydantic v2's ``validate_python`` wipes any private
attributes set before ``super().__init__`` returns -- the staging
trick lets the values be picked up safely in
:meth:`model_post_init`.
"""
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

    Every laila object that needs a stable identifier subclasses this.
    The identity is the triple (uuid, scopes, evolution) -- combined
    into a canonical *global ID* by :attr:`global_id` and used for
    hashing, equality, serialisation, and routing.

    Parameters
    ----------
    uuid : str, optional
        Explicit UUID to assign. Auto-generated as a fresh ``uuid4``
        when omitted. Pass an explicit value for objects whose
        identity must be reproducible (manifests, named pools, ...).
    scopes : list[str], optional
        Hierarchical scope segments slotted between the topmost and
        ``GID`` scopes in the encoded global id. Defaults to
        ``["OBJECT"]``. Subclasses typically override the
        :attr:`_scopes` private attribute to provide their own
        domain-specific scope list (for example
        ``["FUTURE"]`` for futures or ``["POLICY"]`` for policies).
    evolution : int, optional
        Version / evolution counter. ``None`` marks the object as
        *constant* (unversioned, immutable identity); a non-negative
        integer marks it as *variable* (versioned, mutable identity).
    nickname : str, optional
        Human-readable alias. Converted to a deterministic UUID-5
        scoped under the active namespace via
        :meth:`generate_uuid_from_nickname`, so two objects with the
        same nickname under the same namespace share a UUID.

    Notes
    -----
    Identity is intentionally exposed via both private attributes
    (``_uuid``, ``_scopes``, ``_evolution``) and public properties
    (``uuid``, ``scopes``, ``evolution``) plus an aggregate
    :attr:`global_id`. The properties are settable, so identity can
    be mutated post-construction when needed (rare, but supported
    for record-rewriting workflows).
    """

    _uuid: str = PrivateAttr(default_factory=lambda: str(uuid.uuid4()))
    _scopes: list[str] = PrivateAttr(default_factory=lambda: list(["OBJECT"]))
    _evolution: Optional[int] = PrivateAttr(default=None)

    def __init__(self, **data: Any):
        """Stash identity fields in thread-local storage and delegate to Pydantic.

        The values land on ``_INIT_PENDING`` (a thread-local) so they
        survive Pydantic v2's ``validate_python`` boundary, and are
        picked up by :meth:`model_post_init` once construction has
        finished.
        """
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
        """Copy staged identity values from ``_INIT_PENDING`` onto private attrs.

        This is the back half of the construction trick described in
        :meth:`__init__`. Anything that was stashed on the thread-local
        is now safely applied to the instance after Pydantic has
        finished validation.
        """
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
        """Return ``True`` if *global_id* parses as a laila global-ID string.

        Uses :data:`GLOBAL_ID_REGEX_PATTERN`. Useful for input
        validation before passing strings to constructors that expect
        global ids (e.g. :meth:`from_global_id`).
        """
        return GLOBAL_ID_REGEX_PATTERN.match(global_id) is not None

    @property
    def global_id(self) -> str:
        """Fully-qualified global ID string for this instance.

        Composed as ``TOP:scope1:...:scopeN:GID:<uuid>[-<evolution>]``
        from the current ``uuid`` / ``scopes`` / ``evolution`` values.
        Setting this property re-parses the string and re-assigns the
        underlying private attrs (useful for in-place identity
        rebinding, e.g. while restoring from disk).
        """
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
        """Bare UUID string (no scopes, no evolution suffix)."""
        return self._uuid

    @uuid.setter
    def uuid(self, value: str) -> None:
        self._uuid = value

    @property
    def scopes(self) -> list[str]:
        """Hierarchical scope segments, in the order they appear in the global id."""
        return self._scopes

    @scopes.setter
    def scopes(self, value: list[str]) -> None:
        self._scopes = value

    @property
    def evolution(self) -> Optional[int]:
        """Evolution counter, or ``None`` for constant (unversioned) identities.

        See :meth:`Entry.variable` and :meth:`Entry.constant` for the
        most common producers of versioned vs unversioned identities.
        """
        return self._evolution

    @evolution.setter
    def evolution(self, value: Optional[int]) -> None:
        self._evolution = value

    @staticmethod
    def get_scopes_from_global_id(global_id: str) -> list[str]:
        """Extract the scopes list from a global-id string without instantiating."""
        return _LAILA_IDENTIFIABLE_OBJECT.process_global_id(global_id)["scopes"]

    @staticmethod
    def get_uuid_from_global_id(global_id: str) -> str:
        """Extract the bare UUID from a global-id string without instantiating."""
        return _LAILA_IDENTIFIABLE_OBJECT.process_global_id(global_id)["uuid"]

    @staticmethod
    def get_evolution_from_global_id(global_id: str) -> Optional[int]:
        """Extract the evolution counter from a global-id string without instantiating.

        Returns ``None`` for global ids without an evolution suffix.
        """
        return _LAILA_IDENTIFIABLE_OBJECT.process_global_id(global_id)["evolution"]

    def has_evolution(self) -> bool:
        """Return ``True`` if this object has a non-``None`` evolution counter.

        Convenience wrapper around ``self.evolution is not None``;
        equivalent to "is this a *variable* (versioned) identity?"
        """
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
    def type(x: str) -> str:
        """Classify a global id as ``"variable"`` (has evolution) or ``"constant"``.

        Useful when introspecting raw strings (e.g. on the receiving
        side of an RPC) without committing to constructing the
        underlying object.
        """
        processed = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(x)
        if processed["evolution"] is not None:
            return "variable"
        else:
            return "constant"

    def __str__(self) -> str:
        """``str(obj)`` -> :attr:`global_id`. Stable, human-readable, parseable."""
        return self.global_id

    def __repr__(self) -> str:
        """``repr(obj)`` -> :attr:`global_id`. Same as :meth:`__str__`.

        Identical to ``__str__`` because the global id is already
        canonical and unambiguous; no need for additional debug
        framing.
        """
        return self.global_id

    def __hash__(self) -> int:
        """Hash by :attr:`global_id` so instances are usable as set / dict keys.

        Two identities with the same global id (same uuid + scopes +
        evolution) hash equal even if they are distinct Python
        objects -- mirroring the equality contract.
        """
        return hash(self.global_id)

    def identity(self) -> dict[str, Any]:
        """Return a minimal dict describing this object's identity.

        Always includes ``"uuid"``; includes ``"scopes"`` only when
        non-empty and ``"evolution"`` only when non-``None``. The
        result is suitable for passing back into a constructor or
        sending across an RPC envelope.
        """
        identity: dict[str, Any] = {"uuid": self.uuid}
        if self.scopes:
            identity["scopes"] = self.scopes
        if self.evolution is not None:
            identity["evolution"] = self.evolution
        return identity

    def identity_as_json(self) -> str:
        """Return :meth:`identity` serialized to a JSON string."""
        return json.dumps(self.identity())

    @staticmethod
    def generate_uuid_from_nickname(nickname: str) -> str:
        """Deterministically derive a UUID from a human-readable nickname.

        Uses :func:`uuid.uuid5` with the *active namespace* (looked up
        via :func:`laila.get_active_namespace`) so two objects with
        the same nickname under the same namespace share an identity.
        Switching namespaces produces a different UUID for the same
        nickname -- this is how laila keeps independent users from
        accidentally clobbering each other's named resources.
        """
        from ... import get_active_namespace
        return str(uuid.uuid5(get_active_namespace(), nickname))
