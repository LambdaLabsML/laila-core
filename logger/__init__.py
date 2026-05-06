"""LAILA Logger subsystem.

A top-level singleton (sibling to :mod:`laila.policy`, :mod:`laila.pool`,
:mod:`laila.entry`) that emits structured log records both to the
standard library ``logging`` hierarchy and, optionally, into a LAILA
pool through :func:`laila.memorize`.

See :class:`Logger` for the configurable fields and :mod:`record` for
the record schema. The convenience helpers below mirror the ones
re-exported on the ``laila`` package.
"""

from __future__ import annotations

from typing import Any, Optional

from .logger import Logger, _install_null_handler, _LAILA_LOGGER_NAME
from .record import build_record, normalize_level, numeric_level


def get_logger() -> Logger:
    """Return the process-wide :class:`Logger` singleton, creating it lazily."""
    existing = Logger.__dict__.get("_singleton")
    if existing is not None:
        return existing
    return Logger()


def enable_logging(
    level: str = "DEBUG",
    *,
    pool_nickname: Optional[str] = None,
    pool_id: Optional[str] = None,
    display: bool = False,
    capture_traceback: bool = False,
) -> Logger:
    """Configure and start the singleton logger.

    The logger has two independent sinks: a stdout
    :class:`logging.StreamHandler` enabled by ``display=True``, and a
    pool sink enabled by passing ``pool_nickname`` (or ``pool_id``).
    They are not mutually exclusive -- you can have both at once. When
    no pool is configured, ``display`` is forced to ``True`` so records
    are not silently dropped.

    Parameters
    ----------
    level : str, default ``"DEBUG"``
        Stdlib level name. The default captures every record.
    pool_nickname : str, optional
        Pool alias to memorize each record into.
    pool_id : str, optional
        Pool ``global_id`` to memorize each record into.
    display : bool, default False
        When ``True``, also stream records to stderr via stdlib. Forced
        to ``True`` on start if neither ``pool_nickname`` nor ``pool_id``
        is provided.
    capture_traceback : bool, default False
        Include Python tracebacks for errored futures.

    Returns
    -------
    Logger
        The (now-running) singleton.
    """
    logger = get_logger()
    logger.level = level
    logger.display = display
    if pool_nickname is not None:
        logger.pool_nickname = pool_nickname
    if pool_id is not None:
        logger.pool_id = pool_id
    logger.capture_traceback = capture_traceback
    logger.start()
    return logger


def disable_logging() -> None:
    """Stop the singleton logger if one is alive."""
    existing = Logger.__dict__.get("_singleton")
    if existing is not None:
        existing.stop()


def set_log_level(level: str) -> None:
    """Set the singleton logger's level (creating it lazily if needed)."""
    get_logger().set_level(level)


__all__ = [
    "Logger",
    "build_record",
    "normalize_level",
    "numeric_level",
    "get_logger",
    "enable_logging",
    "disable_logging",
    "set_log_level",
    "_install_null_handler",
    "_LAILA_LOGGER_NAME",
]
