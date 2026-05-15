"""Scope-based build dispatch for serialized entries.

Each `Entry` subclass registers its scope string and a *builder pair*
``(sync_fn, async_fn)`` here. ``build_by_scope`` inspects the
``_scopes`` field in a serialized dict and dispatches to the
appropriate builder based on the ``asynchronous`` flag.
"""

from typing import Any, Callable, Dict, Tuple


# Each value is (sync_builder, async_builder).
BUILDER_MAP: Dict[str, Tuple[Callable, Callable]] = {}


def register_builder(scope: str, sync_fn: Callable, async_fn: Callable) -> None:
    """Register a (sync, async) builder pair for *scope*."""
    BUILDER_MAP[scope] = (sync_fn, async_fn)


def build_by_scope(in_dict: Any, *, asynchronous: bool = False, **kwargs) -> Any:
    """Dispatch hydration based on the ``_scopes`` field in *in_dict*.

    Parameters
    ----------
    in_dict : dict or str or Entry
        Serialized representation (dict, JSON string, or live entry).
    asynchronous : bool, default False
        If True, route to the async builder and return a coroutine.
        Otherwise call the sync builder inline and return the entry.
    **kwargs
        Forwarded to the resolved builder.

    Returns
    -------
    Entry or Coroutine[Entry]
        The hydrated entry (sync) or a coroutine resolving to it (async).
        A live :class:`Entry` passed in is returned as-is in the sync
        path; the async path wraps it in an already-resolved coroutine.

    Raises
    ------
    ValueError
        If the scope has no registered builder.
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
