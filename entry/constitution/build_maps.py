"""Scope-based build dispatch for serialized entries.

Different :class:`Entry` subclasses (the base :class:`Entry`, the
:class:`Manifest`, future user-defined subclasses, ...) need different
deserialization logic. Hard-coding the dispatch in
:class:`Entry.from_dict` would couple the base class to every
subclass; instead each subclass registers a *builder pair*
``(sync_fn, async_fn)`` keyed by its scope string in :data:`BUILDER_MAP`,
and :func:`build_by_scope` reads the ``_scopes`` field of a serialized
dict to look up the right builder.

This is also what makes the deserialization side of pool reads
extensible -- third-party Entry subclasses can be deserialized cleanly
as long as they register their own builders at import time.
"""

from typing import Any, Callable, Dict, Tuple


# Each value is (sync_builder, async_builder).
BUILDER_MAP: Dict[str, Tuple[Callable, Callable]] = {}


def register_builder(scope: str, sync_fn: Callable, async_fn: Callable) -> None:
    """Register a (sync, async) builder pair for the given scope.

    Both builders take the serialized dict and return a hydrated
    :class:`Entry` (sync) or a coroutine that resolves to one (async).
    Subclasses should call this exactly once at module import time.
    Re-registering the same scope silently overwrites the previous
    pair, which is the right behavior for hot-reload during
    development but means production code should not rely on it for
    "namespace" tricks.

    Parameters
    ----------
    scope : str
        The first element of the subclass's ``_scopes`` list (e.g.
        ``"ENTRY"``, ``"MANIFEST"``).
    sync_fn : Callable[[dict, ...], Entry]
        The synchronous builder.
    async_fn : Callable[[dict, ...], Coroutine[Entry]]
        The async builder.
    """
    BUILDER_MAP[scope] = (sync_fn, async_fn)


def build_by_scope(in_dict: Any, *, asynchronous: bool = False, **kwargs) -> Any:
    """Dispatch hydration based on the ``_scopes`` field of *in_dict*.

    Accepted input shapes
    ---------------------
    - ``dict`` -- the standard serialized form. The first scope is
      looked up in :data:`BUILDER_MAP`; defaults to ``"ENTRY"`` when
      the dict has no ``_scopes`` key.
    - ``str`` -- treated as a JSON-encoded dict and parsed before
      dispatch (convenient when reading directly from a JSON-blob pool).
    - live :class:`Entry` -- short-circuited and returned as-is in the
      sync path; the async path wraps it in an already-resolved
      coroutine so the caller can ``await`` it uniformly.

    Parameters
    ----------
    in_dict : dict or str or Entry
        Serialized representation.
    asynchronous : bool, default False
        If ``True``, route to the async builder and return a coroutine
        the caller must ``await``. Otherwise call the sync builder
        inline and return the hydrated entry directly.
    **kwargs
        Forwarded to the resolved builder.

    Returns
    -------
    Entry or Coroutine[Entry]
        The hydrated entry, or a coroutine resolving to it.

    Raises
    ------
    ValueError
        If the dispatched scope has no registered builder.
    RuntimeError
        If *in_dict* is not a dict, str, or live Entry.
    json.JSONDecodeError
        If *in_dict* is a string that fails to parse as JSON.
    """
    import json

    if not isinstance(in_dict, dict):
        if isinstance(in_dict, str):
            in_dict = json.loads(in_dict)
        else:
            from ..entry import Entry
            if isinstance(in_dict, Entry):
                if asynchronous:
                    async def _identity(entry=in_dict):
                        return entry
                    return _identity()
                return in_dict
            raise RuntimeError("Invalid input for entry build.")

    scopes = in_dict.get("_scopes", ["ENTRY"])
    scope = scopes[0] if scopes else "ENTRY"

    builder_pair = BUILDER_MAP.get(scope)
    if builder_pair is None:
        raise ValueError(
            f"No builder registered for scope '{scope}'. "
            f"Registered scopes: {list(BUILDER_MAP.keys())}"
        )

    sync_fn, async_fn = builder_pair
    fn = async_fn if asynchronous else sync_fn
    return fn(in_dict, **kwargs)
