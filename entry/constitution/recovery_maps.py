"""Scope-based recovery dispatch for serialized entries.

Each Entry subclass registers its scope string and ``recover`` classmethod
here.  ``recover_by_scope`` inspects the ``_scopes`` field in a serialized
dict and delegates to the correct recovery function.
"""

from typing import Any, Callable

RECOVERY_MAP: dict[str, Callable] = {}


def register_recovery(scope: str, recover_fn: Callable) -> None:
    """Register a recovery function for *scope*."""
    RECOVERY_MAP[scope] = recover_fn


def recover_by_scope(in_dict: Any, **kwargs) -> Any:
    """Dispatch recovery based on the ``_scopes`` field in *in_dict*.

    Parameters
    ----------
    in_dict : dict or str or Entry
        Serialized representation (dict, JSON string, or live object).
    **kwargs
        Forwarded to the resolved recovery function.

    Returns
    -------
    Entry or subclass
        The recovered instance.

    Raises
    ------
    ValueError
        If the scope has no registered recovery function.
    """
    import json

    if not isinstance(in_dict, dict):
        if isinstance(in_dict, str):
            in_dict = json.loads(in_dict)
        else:
            from ..entry import Entry
            if isinstance(in_dict, Entry):
                return in_dict
            raise RuntimeError("Invalid input for entry recovery.")

    scopes = in_dict.get("_scopes", ["ENTRY"])
    scope = scopes[0] if scopes else "ENTRY"

    recover_fn = RECOVERY_MAP.get(scope)
    if recover_fn is None:
        raise ValueError(
            f"No recovery function registered for scope '{scope}'. "
            f"Registered scopes: {list(RECOVERY_MAP.keys())}"
        )
    return recover_fn(in_dict, **kwargs)
