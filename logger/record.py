"""Structured log record builder for the LAILA Logger singleton.

A laila log record is a JSON-trivial dict with a *fixed* schema of
optional well-known fields (``policy_id``, ``pool_id``, ``future_id``,
``status``, ...) plus a free-form ``extra`` bag for anything not
covered by the schema. Records are always:

- Composed of strings, numbers, lists, and dicts only -- so they
  serialise cleanly with :func:`json.dumps` and round-trip safely
  through any laila pool.
- Time-stamped with both an ISO-8601 string (``ts``, UTC, with
  ``Z`` suffix) and a Unix epoch float (``ts_unix``) -- the former
  for humans, the latter for time-series analytics.
- Equipped with a normalised string ``level`` (one of ``DEBUG``,
  ``INFO``, ``WARNING``, ``ERROR``, ``CRITICAL``).

This module exports two utilities:

- :func:`normalize_level` / :func:`numeric_level` -- bidirectional
  conversion between numeric :mod:`logging` levels and the canonical
  string names used in records.
- :func:`build_record` -- the structured-record factory called by the
  :class:`Logger` itself for every event. Optional fields are omitted
  from the result rather than written as ``None`` so records stay
  compact on the wire.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional


_LEVEL_NUMERIC: Dict[str, int] = {
    "DEBUG": 10,
    "INFO": 20,
    "WARNING": 30,
    "ERROR": 40,
    "CRITICAL": 50,
}


def normalize_level(level: Any) -> str:
    """Coerce a level (str or int) into a canonical upper-case name.

    Parameters
    ----------
    level : str or int
        Either a Python ``logging`` level name or its numeric value.

    Returns
    -------
    str
        One of ``"DEBUG"``, ``"INFO"``, ``"WARNING"``, ``"ERROR"``,
        ``"CRITICAL"``. Unknown values fall back to ``"INFO"``.
    """
    if isinstance(level, int):
        for name, value in _LEVEL_NUMERIC.items():
            if value == level:
                return name
        return "INFO"
    if isinstance(level, str):
        upper = level.upper()
        if upper in _LEVEL_NUMERIC:
            return upper
    return "INFO"


def numeric_level(level: Any) -> int:
    """Return the integer ``logging`` level for a name or numeric value."""
    return _LEVEL_NUMERIC[normalize_level(level)]


def _coerce_id(value: Any) -> Optional[str]:
    """Convert any identifiable-like object to a string global_id."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    gid = getattr(value, "global_id", None)
    if gid is not None:
        return str(gid)
    return str(value)


def build_record(
    event: str,
    *,
    level: str = "INFO",
    message: Optional[str] = None,
    policy_id: Any = None,
    pool_id: Any = None,
    pool_nickname: Optional[str] = None,
    entry_id: Any = None,
    entry_nickname: Optional[str] = None,
    future_id: Any = None,
    future_group_id: Any = None,
    precedence: Any = None,
    purpose: Optional[str] = None,
    taskforce_id: Any = None,
    logger_id: Any = None,
    status: Optional[str] = None,
    prev_status: Optional[str] = None,
    result_id: Any = None,
    child_future_ids: Optional[list] = None,
    child_results: Optional[list] = None,
    peer_id: Any = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a structured LAILA log record dict.

    All ``*_id`` fields accept either a string global_id or any
    :class:`~laila.basics.definitions.identifiable_object._LAILA_IDENTIFIABLE_OBJECT`
    instance and are normalized to strings. Unspecified optional fields
    are omitted (kept out of the dict) so records stay compact.

    Returns
    -------
    dict
        A JSON-trivial record. Always contains ``ts``, ``ts_unix``,
        ``level``, ``event``, and ``extra``.
    """
    now = time.time()
    record: Dict[str, Any] = {
        "ts": datetime.fromtimestamp(now, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
        "ts_unix": now,
        "level": normalize_level(level),
        "event": event,
        "extra": dict(extra) if extra else {},
    }
    if message is not None:
        record["message"] = message

    optional_ids = {
        "policy_id": policy_id,
        "pool_id": pool_id,
        "pool_nickname": pool_nickname,
        "entry_id": entry_id,
        "entry_nickname": entry_nickname,
        "future_id": future_id,
        "future_group_id": future_group_id,
        "precedence": precedence,
        "purpose": purpose,
        "taskforce_id": taskforce_id,
        "logger_id": logger_id,
        "peer_id": peer_id,
        "result_id": result_id,
    }
    for key, value in optional_ids.items():
        if value is None:
            continue
        if key in {"pool_nickname", "entry_nickname", "purpose"}:
            record[key] = str(value)
        else:
            record[key] = _coerce_id(value)

    if status is not None:
        record["status"] = str(status)
    if prev_status is not None:
        record["prev_status"] = str(prev_status)

    if child_future_ids is not None:
        record["child_future_ids"] = [_coerce_id(c) for c in child_future_ids]
    if child_results is not None:
        record["child_results"] = [_coerce_id(c) for c in child_results]

    return record
