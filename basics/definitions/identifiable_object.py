"""Identifiable-object base class with UUID and global-ID support.

The single class in this module, :class:`_LAILA_IDENTIFIABLE_OBJECT`,
is the foundation every *identifiable* laila object inherits from (it
itself derives from :class:`_LAILA_OBJECT`, the shared root that
carries the creation ``creation_timestamp``). It pins down the
*identity* contract that the rest of the codebase relies on:

- A UUID (defaulting to a fresh ``uuid4``).
- A list of hierarchical *scopes* (e.g. ``["POLICY"]``, ``["FUTURE"]``).
- An optional *evolution* counter that distinguishes successive
  versions of the same logical object (``None`` for "constant" /
  unversioned objects, an integer for "variable" / mutable ones).

These three components combine into the canonical *global ID*
string produced by :meth:`_LAILA_IDENTIFIABLE_OBJECT.global_id` --
the form is::

    LAILA:scope1:...:scopeN:<uuid>[@evolution=<n>]

Everything after ``@`` is a comma-separated ``key=value`` list of
*attributes*. ``evolution`` is the only attribute that is part of the
identity and the only one :attr:`global_id` ever emits; any other key
(``creation_timestamp=...``) is accepted by the parser as a *search
argument*, exposed through
:meth:`_LAILA_IDENTIFIABLE_OBJECT.get_attributes_from_global_id`, and
consumed by ``laila.remember`` -- it is never folded into identity.

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

import json
import re
import threading
import uuid
from typing import Any, ClassVar

from pydantic import PrivateAttr

from ...macros.strings import _ENTRY_SCOPE, _OBJECT_SCOPE, _TOPMOST_SCOPE
from .laila_object import _LAILA_OBJECT

# ``key=value`` attribute pair. Keys are identifiers; values may contain
# anything except the pair separator ``,`` and the attribute marker ``@``
# (ISO timestamps with ``:`` / ``+`` / ``.`` are therefore fine).
_ATTRIBUTE_PAIR = r"[A-Za-z_][A-Za-z0-9_]*=[^,@]*"

GLOBAL_ID_REGEX_PATTERN = re.compile(
    r"^(?P<scopes>(?:[A-Za-z0-9_]+:)+)"
    r"(?P<uuid>[0-9a-fA-F-]{36})"
    rf"(?:@(?P<attributes>{_ATTRIBUTE_PAIR}(?:,{_ATTRIBUTE_PAIR})*))?$"
)

_UUID_RE = re.compile(r"^[0-9a-fA-F-]{36}$")
# Hex digits and dashes in the neighbourhood of a uuid's length: what a
# truncated / corrupted uuid (or a uuid with junk appended) looks like.
_UUID_LIKE_RE = re.compile(r"^[0-9a-fA-F-]{30,48}$")
_ATTRIBUTE_RE = re.compile(r"^(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>[^,@]*)$")
# Evolution as written in a *reference*: negatives (``-1`` = latest) are
# search arguments resolved by ``laila.remember``; identity is non-negative.
_REFERENCE_EVOLUTION_RE = re.compile(r"^-?\d+$")

# Name of the single attribute that is part of an object's identity.
EVOLUTION_ATTRIBUTE = "evolution"

# Scopes assumed for a scope-less *reference* (``"run-3"``,
# ``"counter@evolution=3"``) when the resolving class is the generic base.
_DEFAULT_REFERENCE_SCOPES = [_ENTRY_SCOPE]

_INIT_PENDING = threading.local()


def parse_global_id_attributes(attributes: str | None) -> dict[str, str]:
    """Parse the ``@``-suffix of a global id into an ordered ``{key: value}`` dict.

    Parameters
    ----------
    attributes : str or None
        The text after ``@`` (``"evolution=3,creation_timestamp=..."``),
        or ``None`` / ``""`` for no attributes.

    Returns
    -------
    dict[str, str]
        Raw string values keyed by attribute name, in the order written.

    Raises
    ------
    ValueError
        On a malformed pair (missing ``=``, empty key, illegal key
        characters, trailing comma) or a duplicated key.
    """
    if not attributes:
        return {}
    out: dict[str, str] = {}
    for pair in attributes.split(","):
        m = _ATTRIBUTE_RE.match(pair)
        if m is None:
            raise ValueError(f"Invalid global id attribute: {pair!r}")
        key = m.group("key")
        if key in out:
            raise ValueError(f"Duplicate global id attribute: {key!r}")
        out[key] = m.group("value")
    return out


def format_global_id_attributes(attributes: dict[str, Any]) -> str:
    """Inverse of :func:`parse_global_id_attributes` (without the leading ``@``).

    ``None`` values are skipped so callers can pass ``{"evolution": None}``
    for constants; an empty result means "no suffix".
    """
    return ",".join(f"{k}={v}" for k, v in attributes.items() if v is not None)


def split_global_id_attributes(ref: str) -> tuple[str, dict[str, str]]:
    """Split ``"<head>@<attributes>"`` into ``(head, {key: value})``.

    *head* is everything before the first ``@`` (scopes plus uuid or
    nickname). Works on full global ids and on shorthand references
    alike; a reference without ``@`` yields ``(ref, {})``.
    """
    head, sep, tail = ref.partition("@")
    if not sep:
        return ref, {}
    if not tail:
        raise ValueError(f"Invalid GID format: {ref}")
    return head, parse_global_id_attributes(tail)


class _LAILA_IDENTIFIABLE_OBJECT(_LAILA_OBJECT):
    """Pydantic base model providing UUID-based identity and global-ID encoding.

    Every laila object that needs a stable identifier subclasses this.
    Inherits the creation ``creation_timestamp`` from
    :class:`_LAILA_OBJECT`.
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

    Performance note: the private attributes deliberately use plain
    ``PrivateAttr(default=None)`` rather than ``default_factory``.
    Pydantic re-inspects a private ``default_factory``'s signature on
    *every* instantiation, which costs ~25 us per attribute; identity
    objects are constructed on the hot path of every task and entry, so
    defaults are computed in :meth:`__init__` instead. Subclasses set
    their scope list through the :attr:`_DEFAULT_SCOPES` class variable
    (``_DEFAULT_SCOPES = [_FUTURE_SCOPE]``) instead of overriding
    ``_scopes`` with a factory.
    """

    _DEFAULT_SCOPES: ClassVar[list[str]] = [_OBJECT_SCOPE]

    _uuid: str = PrivateAttr(default=None)
    _scopes: list[str] = PrivateAttr(default=None)
    _evolution: int | None = PrivateAttr(default=None)

    def __init__(self, **data: Any):
        """Stash identity fields in thread-local storage and delegate to Pydantic.

        The *effective* identity (explicit values or freshly generated
        defaults) lands on ``_INIT_PENDING`` (a thread-local) so it
        survives Pydantic v2's ``validate_python`` boundary and is
        already valid when any ``model_post_init`` in the hierarchy
        runs (subclasses register themselves by ``global_id`` there).
        :meth:`model_post_init` applies it; a fallback after
        ``super().__init__`` covers subclasses whose ``model_post_init``
        does not chain to this base.
        """
        uuid_input = data.pop("uuid", None)
        scopes_input = data.pop("scopes", None)
        evolution_input = data.pop("evolution", None)
        nickname_input = data.pop("nickname", None)

        if nickname_input is not None:
            pending_uuid = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname(nickname_input)
        elif uuid_input is not None:
            pending_uuid = str(uuid_input)
        else:
            pending_uuid = str(uuid.uuid4())

        scopes_explicit = scopes_input is not None
        if scopes_explicit:
            pending_scopes = list(scopes_input)
        else:
            pending_scopes = list(type(self)._DEFAULT_SCOPES)

        # Save the enclosing construction's staged identity (if any): a
        # nested identifiable object may be built while Pydantic validates
        # our fields, and it must not wipe our values before our own
        # ``model_post_init`` has consumed them.
        prev = (
            getattr(_INIT_PENDING, "uuid", None),
            getattr(_INIT_PENDING, "scopes", None),
            getattr(_INIT_PENDING, "evolution", None),
            getattr(_INIT_PENDING, "scopes_explicit", False),
        )
        _INIT_PENDING.uuid = pending_uuid
        _INIT_PENDING.scopes = pending_scopes
        _INIT_PENDING.evolution = evolution_input
        _INIT_PENDING.scopes_explicit = scopes_explicit

        try:
            super().__init__(**data)
        finally:
            (
                _INIT_PENDING.uuid,
                _INIT_PENDING.scopes,
                _INIT_PENDING.evolution,
                _INIT_PENDING.scopes_explicit,
            ) = prev

        # Fallback for subclasses whose model_post_init does not chain to
        # ours (or that still declare ``_scopes`` with a default_factory).
        if self._uuid is None:
            self._uuid = pending_uuid
        if self._scopes is None or (scopes_explicit and self._scopes != pending_scopes):
            self._scopes = pending_scopes
        if evolution_input is not None and self._evolution is None:
            self._evolution = evolution_input

    def model_post_init(self, __context: Any) -> None:
        """Copy staged identity values from ``_INIT_PENDING`` onto private attrs.

        This is the back half of the construction trick described in
        :meth:`__init__`. Anything that was stashed on the thread-local
        is now safely applied to the instance after Pydantic has
        finished validation. Subclasses that override this method must
        call ``super().model_post_init(__context)`` *first* so that
        ``self.global_id`` is valid for their own registration logic.
        This method itself chains to ``super()`` so the hook stays
        cooperative across mixin diamonds.
        """
        super().model_post_init(__context)
        pending_uuid = getattr(_INIT_PENDING, "uuid", None)
        if pending_uuid is not None:
            self._uuid = pending_uuid
        pending_scopes = getattr(_INIT_PENDING, "scopes", None)
        if pending_scopes is not None:
            # A subclass may still declare ``_scopes`` with its own
            # default_factory; an explicit ``scopes=`` argument always
            # wins, but a generated default must not clobber it.
            if self._scopes is None or getattr(_INIT_PENDING, "scopes_explicit", False):
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
            A full global ID matching ``GLOBAL_ID_REGEX_PATTERN``, or a
            scoped shorthand such as ``"POLICY:my_policy"`` (see
            :meth:`resolve_global_id`).

        Returns
        -------
        _LAILA_IDENTIFIABLE_OBJECT
            New instance with identity fields parsed from *global_id*.

        Raises
        ------
        ValueError
            If *global_id* does not match the expected format.
        """
        identity_data = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(global_id)
        return cls(**identity_data)

    @classmethod
    def resolve_global_id(
        cls,
        ref: str,
        *,
        evolution: int | None = None,
        prefix_scopes: list[str] | None = None,
        default_scopes: list[str] | None = None,
        parse_evolution: bool = True,
    ) -> str:
        """Decode a global-id *reference* into a full global ID.

        This is the single place where the shorthand forms accepted all
        over laila (``laila.remember("MANIFEST:my_dataset")``,
        ``dst_policy="POLICY:trainer"``, ...) are expanded. Accepted
        forms for *ref*:

        - A complete global id (``LAILA:ENTRY:<uuid>[@evolution=<n>]``)
          -- returned unchanged (extra search attributes included).
        - ``SCOPE[:SCOPE...]:<uuid | nickname>[@attrs]`` -- the scopes
          are placed after *prefix_scopes*; the tail is used verbatim
          when it is a UUID and hashed to a UUID-5 under the active
          namespace when it is a nickname. An ``evolution=<n>``
          attribute is read as the evolution counter; other attributes
          are carried over verbatim as search arguments::

              "MANIFEST:my_dataset"          -> LAILA:MANIFEST:<uuid5>
              "POLICY:trainer"               -> LAILA:POLICY:<uuid5>
              "POLICY:3f2a...-c47c"          -> LAILA:POLICY:3f2a...-c47c
              "ENTRY:counter@evolution=3"    -> LAILA:ENTRY:<uuid5>@evolution=3
              "ENTRY:counter@creation_timestamp=2026-..."
                                             -> LAILA:ENTRY:<uuid5>@creation_timestamp=2026-...

        - ``<uuid | nickname>[@attrs]`` with no scope segment -- scoped
          with *default_scopes*, which defaults to the class's
          ``_DEFAULT_SCOPES`` (``Entry`` -> ``ENTRY``, ``Manifest`` ->
          ``MANIFEST``); on the generic base class a scope-less
          reference means an ``ENTRY`` (``"run-3"`` == ``"ENTRY:run-3"``).
          A tail that looks like a truncated / corrupted uuid is
          rejected rather than hashed as a nickname.
        - ``evolution`` may be negative in a *reference*
          (``"ENTRY:counter@evolution=-1"`` = latest stored evolution);
          it is resolved by ``laila.remember`` and never part of an
          identity.

        If the caller spells out the prefix anyway
        (``"LAILA:MANIFEST:my_dataset"``) it is stripped rather than
        doubled.

        Parameters
        ----------
        ref : str
            Global id or shorthand.
        evolution : int, optional
            Explicit evolution counter. Overrides an ``evolution=``
            attribute present in *ref*.
        prefix_scopes : list[str], optional
            Scopes placed before the user-supplied ones. Defaults to
            ``["LAILA"]``.
        default_scopes : list[str], optional
            Scopes used when *ref* has no scope segment. Defaults to
            ``cls._DEFAULT_SCOPES``.
        parse_evolution : bool, default True
            Whether an ``@...`` suffix on the reference is interpreted.
            Pass ``False`` to take the whole tail literally as a
            nickname (``"run@evolution=3"`` becomes the nickname).

        Returns
        -------
        str
            The assembled global id.

        Raises
        ------
        ValueError
            If *ref* is not a string, or has an empty tail / empty scope
            segment (``"POLICY:"``, ``":trainer"``, ``"A::b"``), or a
            malformed attribute list.
        """
        if not isinstance(ref, str):
            raise ValueError(f"global id reference must be a string, got {type(ref).__name__}")

        prefix = list(prefix_scopes) if prefix_scopes is not None else [_TOPMOST_SCOPE]

        if parse_evolution:
            head, attrs = split_global_id_attributes(ref)
        else:
            head, attrs = ref, {}

        *scopes, tail = head.split(":")
        if not tail or not all(scopes):
            raise ValueError(f"Invalid GID format: {ref}")

        # Already a full global id (well-formed *and* framed by the prefix)?
        # ``POLICY:<uuid>`` also matches the regex but lacks the frame, so it
        # falls through and is expanded like any other shorthand.
        if (
            GLOBAL_ID_REGEX_PATTERN.match(ref)
            and scopes[: len(prefix)] == prefix
            and len(scopes) > len(prefix)
            and (evolution is None or attrs.get(EVOLUTION_ATTRIBUTE) == str(evolution))
        ):
            return ref

        if evolution is not None:
            attrs[EVOLUTION_ATTRIBUTE] = str(evolution)
        elif EVOLUTION_ATTRIBUTE in attrs and not _REFERENCE_EVOLUTION_RE.match(
            attrs[EVOLUTION_ATTRIBUTE]
        ):
            raise ValueError(f"Invalid evolution in GID: {ref}")

        if _UUID_RE.match(tail):
            uid = tail
        elif _UUID_LIKE_RE.match(tail):
            # Hex-and-dashes that is not a well-formed uuid: a truncated or
            # corrupted uuid, or the legacy ``<uuid>-<evolution>`` form. A
            # malformed id, not a nickname -- hashing it would silently
            # produce a *different* valid id.
            raise ValueError(f"Invalid GID format: {ref}")
        else:
            uid = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname(tail)

        # Tolerate a spelled-out prefix.
        if prefix and scopes[: len(prefix)] == prefix:
            scopes = scopes[len(prefix) :]
        if not scopes:
            if default_scopes is not None:
                scopes = list(default_scopes)
            elif cls._DEFAULT_SCOPES == [_OBJECT_SCOPE]:
                # The base class has no domain of its own: a scope-less
                # *reference* means an entry (``laila.remember("run-3")``).
                scopes = list(_DEFAULT_REFERENCE_SCOPES)
            else:
                scopes = list(cls._DEFAULT_SCOPES)

        gid = f"{':'.join([*prefix, *scopes])}:{uid}"
        suffix = format_global_id_attributes(attrs)
        return f"{gid}@{suffix}" if suffix else gid

    @staticmethod
    def to_global_id(
        uuid=None,
        scopes: list[str] | None = None,
        evolution=None,
        nickname: str | None = None,
    ) -> str:
        """Build a global ID string from its constituent parts.

        Parameters
        ----------
        uuid : str, optional
            The UUID segment.
        scopes : list[str], optional
            Scope segments inserted after the top-level ``LAILA`` scope.
        evolution : int, optional
            If provided, appended as an ``@evolution=<n>`` attribute.
        nickname : str, optional
            Converted to a deterministic UUID-5 before encoding.

        Returns
        -------
        str
            The assembled global ID.
        """
        if nickname is not None:
            uuid = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname(nickname)

        if scopes is None or len(scopes) == 0:
            scopes = [_OBJECT_SCOPE]

        base = f"{':'.join([_TOPMOST_SCOPE, *scopes])}:{uuid}"
        if evolution is None:
            return base
        return f"{base}@{EVOLUTION_ATTRIBUTE}={evolution}"

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

        Composed as ``LAILA:scope1:...:scopeN:<uuid>[@evolution=<n>]``
        from the current ``uuid`` / ``scopes`` / ``evolution`` values.
        Setting this property re-parses the string and re-assigns the
        underlying private attrs (useful for in-place identity
        rebinding, e.g. while restoring from disk).
        """
        return _LAILA_IDENTIFIABLE_OBJECT.to_global_id(
            uuid=self._uuid, scopes=self._scopes, evolution=self._evolution
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
    def evolution(self) -> int | None:
        """Evolution counter, or ``None`` for constant (unversioned) identities.

        See :meth:`Entry.variable` and :meth:`Entry.constant` for the
        most common producers of versioned vs unversioned identities.
        """
        return self._evolution

    @evolution.setter
    def evolution(self, value: int | None) -> None:
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
    def get_evolution_from_global_id(global_id: str) -> int | None:
        """Extract the evolution counter from a global-id string without instantiating.

        Returns ``None`` for global ids without an ``evolution`` attribute.
        """
        return _LAILA_IDENTIFIABLE_OBJECT.process_global_id(global_id)["evolution"]

    @staticmethod
    def get_attributes_from_global_id(global_id: str) -> dict[str, str]:
        """Return every ``@key=value`` attribute of *global_id* as raw strings.

        Unlike :meth:`process_global_id` this keeps *all* attributes,
        including search arguments such as ``creation_timestamp`` that
        are not part of the identity. ``evolution`` (when present) is
        returned as a string too. Scoped shorthands are expanded first.
        """
        global_id = _LAILA_IDENTIFIABLE_OBJECT.resolve_global_id(global_id)
        match = GLOBAL_ID_REGEX_PATTERN.match(global_id)
        if match is None:
            raise ValueError(f"Invalid GID format: {global_id}")
        return parse_global_id_attributes(match.group("attributes"))

    @staticmethod
    def strip_global_id_attributes(global_id: str) -> str:
        """Return *global_id* without its ``@...`` suffix (``LAILA:scopes:<uuid>``).

        The result is the common prefix shared by every evolution of the
        same object, which is what pool-side searches key on.
        """
        return global_id.partition("@")[0]

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
            A full global-ID string, or a *scoped* shorthand such as
            ``"POLICY:trainer"`` / ``"ENTRY:counter@evolution=3"`` which
            is first expanded through :meth:`resolve_global_id`. A bare
            string with no scope segment (``"counter"``) is an ``ENTRY``
            reference. Attributes other than ``evolution`` are ignored
            (see :meth:`get_attributes_from_global_id`); a *negative*
            evolution is a search argument, not an identity, and is
            rejected here.

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
        if not isinstance(global_id, str) or not global_id:
            raise ValueError(f"Invalid GID format: {global_id}")
        # Framed full gids pass through unchanged; scoped shorthands
        # ("POLICY:trainer", "POOL:<uuid>") are expanded.
        global_id = _LAILA_IDENTIFIABLE_OBJECT.resolve_global_id(global_id)
        match = GLOBAL_ID_REGEX_PATTERN.match(global_id)
        if match is None:
            raise ValueError(f"Invalid GID format: {global_id}")
        attrs = parse_global_id_attributes(match.group("attributes"))
        evolution_raw = attrs.get(EVOLUTION_ATTRIBUTE)
        if evolution_raw is not None and not evolution_raw.isdigit():
            raise ValueError(f"Invalid evolution in GID: {global_id}")
        # The scopes group is "LAILA:<mid...>:"; drop the leading TOPMOST
        # and the empty string after the final ':' so that
        # ``to_global_id(**parsed)`` round-trips exactly.
        return {
            "uuid": match.group("uuid"),
            "scopes": match.group("scopes").split(":")[1:-1],
            "evolution": int(evolution_raw) if evolution_raw is not None else None,
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

    def __eq__(self, other: object) -> bool:
        """Equality follows identity: same :attr:`global_id` means the
        same object, regardless of payload, timestamps or process-local
        state. This is the counterpart of :meth:`__hash__`; without it
        two handles to one identity would hash alike yet compare unequal
        and set / dict de-duplication would be unreliable.
        """
        if not isinstance(other, _LAILA_IDENTIFIABLE_OBJECT):
            return NotImplemented
        return self.global_id == other.global_id

    def __ne__(self, other: object) -> bool:
        eq = self.__eq__(other)
        return eq if eq is NotImplemented else not eq

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
