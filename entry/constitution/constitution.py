"""Abstract :class:`Constitution` base class -- recipe for materializing an
:class:`Entry`'s payload.

A *constitution* tells the build pipeline how to recover (or compute)
an entry's payload from upstream input. Constitutions are first-class,
serializable objects so an entry can be persisted with its build recipe
intact and re-executed in another process / on another machine.

Two concrete subclasses cover the bulk of the design space:

- :class:`SimpleConstitution`
    An ordered list of Python source strings. Each source string
    defines exactly one top-level callable, and the build threads
    ``payload_input`` through the chain in order:
    ``code[0](input) -> code[1](output) -> ...``. The canonical use
    case is the *inverse* transformation chain emitted by
    :meth:`Entry.serialize`: e.g. base64-decode -> zlib-decompress
    -> pickle-loads. Any pure-Python sequence of one-argument
    transformations is fair game.

- :class:`ComplexConstitution`
    A single source string defining ``f(manifest) -> payload`` plus a
    :class:`Manifest` (or its ``global_id``). At build time the
    manifest is resolved from the active policy's memory if necessary,
    its referenced entries are forced to materialize, and the callable
    is applied to the live manifest. This is how derived / composite
    entries are described declaratively (``output = f(input1, input2)``).

Both subclasses implement :meth:`build` and :meth:`as_dict`. The
dispatcher :meth:`Constitution.from_dict` selects the right subclass
by reading the ``_kind`` tag baked into every serialized constitution
(``"simple"`` or ``"complex"``).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

from pydantic import BaseModel, ConfigDict

_REGISTRY: dict[str, type[Constitution]] = {}


def _register_kind(kind: str):
    """Decorator that registers a :class:`Constitution` subclass under a
    string ``_kind`` tag and stamps the same tag onto the class itself.

    The dispatcher :meth:`Constitution.from_dict` uses :data:`_REGISTRY`
    to map serialized ``_kind`` values back to the right subclass.
    Subclasses of :class:`Constitution` should be decorated with
    ``@_register_kind("name")`` exactly once at class definition time.

    Parameters
    ----------
    kind : str
        A short, lowercase, globally-unique tag (``"simple"``,
        ``"complex"``, ...).
    """

    def deco(cls: type[Constitution]) -> type[Constitution]:
        _REGISTRY[kind] = cls
        cls._kind = kind  # type: ignore[attr-defined]
        return cls

    return deco


def _exec_one_fn(code: str) -> Callable[[Any], Any]:
    """Compile a Python source string and return its single defined callable.

    The "exactly one callable" rule keeps constitution code tiny and
    unambiguous: a constitution slot in a serialized entry is meant to
    describe *one* transformation step, so a source string that defines
    two helpers leaves the chain ill-defined. The check tolerates
    private helpers (names starting with ``__``) but treats every other
    callable as a candidate.

    Parameters
    ----------
    code : str
        A Python source string defining exactly one top-level callable
        (function or class).

    Returns
    -------
    Callable
        The single callable defined by *code*.

    Raises
    ------
    ValueError
        If *code* defines zero or multiple top-level callables.

    Notes
    -----
    The execution namespace starts empty -- imports must be performed
    inside the source string itself. This is intentional: it makes the
    code self-contained and survives the round-trip through pool
    storage on machines with different installed packages.
    """
    namespace: dict[str, Any] = {}
    exec(code, namespace)
    fns = [v for k, v in namespace.items() if not k.startswith("__") and callable(v)]
    if len(fns) != 1:
        raise ValueError("constitution code must define exactly one callable function")
    return fns[0]


class Constitution(BaseModel, ABC):
    """Abstract base for entry constitutions.

    A constitution knows how to produce an entry's payload value. The
    ``payload_input`` argument to :meth:`build` is used by simple
    constitutions to receive the serialized-blob bytes that need to be
    decoded; complex constitutions ignore it and read from their bound
    manifest instead.

    Subclasses must implement :meth:`build`, :meth:`as_dict`, and the
    :meth:`_from_dict` hook used by the dispatcher.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    _kind: str = ""

    @abstractmethod
    def build(self, payload_input: Any | None = None) -> Any:
        """Produce and return the entry's payload value.

        Parameters
        ----------
        payload_input : Any, optional
            For :class:`SimpleConstitution`: the serialized blob to be
            threaded through the inverse-transformation chain. For
            :class:`ComplexConstitution`: ignored (input comes from the
            bound manifest).
        """

    @abstractmethod
    def as_dict(self) -> dict:
        """Return the JSON-friendly serialized representation.

        The result must include a ``_kind`` key so :meth:`from_dict`
        can dispatch to the right subclass on rehydration.
        """

    @classmethod
    def from_dict(cls, in_dict: dict | None) -> Constitution | None:
        """Reconstruct a :class:`Constitution` from its serialized dict.

        Reads the ``_kind`` tag, looks up the registered subclass in
        :data:`_REGISTRY`, and delegates to that subclass's
        :meth:`_from_dict` to perform the actual rebuild.

        Parameters
        ----------
        in_dict : dict or None
            The serialized constitution, including a ``_kind`` tag.
            ``None`` is allowed and short-circuits to ``None`` so callers
            can pass through optional fields without a guard.

        Returns
        -------
        Constitution or None
            The reconstructed constitution, or ``None`` when *in_dict*
            is ``None``.

        Raises
        ------
        KeyError
            If *in_dict* is missing the ``_kind`` tag.
        ValueError
            If the ``_kind`` is not in :data:`_REGISTRY` (typically
            because the subclass module has not been imported in this
            process yet).
        """
        if in_dict is None:
            return None
        if "_kind" not in in_dict:
            raise KeyError("serialized constitution is missing the '_kind' tag")
        kind = in_dict["_kind"]
        sub_cls = _REGISTRY.get(kind)
        if sub_cls is None:
            raise ValueError(
                f"no Constitution subclass registered for kind '{kind}'. "
                f"Registered: {list(_REGISTRY.keys())}"
            )
        return sub_cls._from_dict(in_dict)

    @classmethod
    @abstractmethod
    def _from_dict(cls, in_dict: dict) -> Constitution:
        """Subclass-specific dict -> instance hook used by :meth:`from_dict`.

        Implementations should NOT re-check the ``_kind`` tag (the
        dispatcher has already done so) and may freely raise
        ``KeyError`` / ``ValueError`` if subclass-specific fields are
        missing or malformed.
        """
