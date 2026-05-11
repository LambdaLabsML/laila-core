"""Scope-based build dispatch for serialized entries.

Each `Entry` subclass registers its scope string and a `build_from_dict`
classmethod here.  `build_by_scope` inspects the `_scopes` field in a
serialized dict and delegates to the correct builder, returning a
`Future` that resolves to the hydrated entry.
"""

from typing import Any, Callable, Dict


BUILDER_MAP: Dict[str, Callable] = {}


def register_builder(scope: str, builder_fn: Callable) -> None:
    """Register a build function for *scope*."""
    BUILDER_MAP[scope] = builder_fn


def build_by_scope(in_dict: Any, **kwargs) -> Any:
    """Dispatch hydration based on the `_scopes` field in *in_dict*.

    Parameters
    ----------
    in_dict : dict or str or Entry
        Serialized representation (dict, JSON string, or live entry).
    **kwargs
        Forwarded to the resolved builder.

    Returns
    -------
    Future or Entry
        A `Future` resolving to the rebuilt entry, or the entry itself
        if a live `Entry` was passed in.

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
                return in_dict
            raise RuntimeError("Invalid input for entry build.")

    scopes = in_dict.get("_scopes", ["ENTRY"])
    scope = scopes[0] if scopes else "ENTRY"

    builder_fn = BUILDER_MAP.get(scope)
    if builder_fn is None:
        raise ValueError(
            f"No builder registered for scope '{scope}'. "
            f"Registered scopes: {list(BUILDER_MAP.keys())}"
        )
    return builder_fn(in_dict, **kwargs)
