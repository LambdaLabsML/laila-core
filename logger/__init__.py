"""LAILA Logger subsystem.

A top-level singleton (sibling to :mod:`laila.policy`,
:mod:`laila.pool`, :mod:`laila.entry`) that fans structured log
records out to two independent sinks:

- The standard-library :mod:`logging` hierarchy under the
  :data:`_LAILA_LOGGER_NAME` channel, with optional stderr
  streaming via the ``display=True`` flag.
- A laila pool, by ``memorize``-ing each record as a JSON-shaped
  entry. The pool can be selected by nickname or global id; missing
  pool configuration triggers an automatic fall-back to ``display=True``
  so records are never silently dropped.

The two sinks are independent and can both be active at once -- a
typical setup is "always display, also persist into Postgres" so an
operator can watch the live stream while history accumulates in a
pool.

This module re-exports:

- :class:`Logger` -- the singleton class itself, with all the
  configurable fields and lifecycle methods (``start``, ``stop``,
  ``set_level``, ...).
- :mod:`record` helpers (:func:`build_record`, :func:`normalize_level`,
  :func:`numeric_level`).
- The convenience top-level functions :func:`get_logger`,
  :func:`enable_logging`, :func:`disable_logging`, :func:`set_log_level`
  -- the same names re-exported on the :mod:`laila` package.
"""

from __future__ import annotations

from typing import Any, Optional

from .logger import Logger, _install_null_handler, _LAILA_LOGGER_NAME
from .record import build_record, normalize_level, numeric_level


def get_logger() -> Logger:
    """Return the process-wide :class:`Logger` singleton, creating it lazily.

    The first call constructs the singleton with default settings
    (silent: no display, no pool). Subsequent calls return the same
    instance. Use :func:`enable_logging` to actually start emitting
    records.
    """
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
    """Stop the singleton logger if one is alive.

    Idempotent: if no logger has been constructed yet (no
    :func:`get_logger` / :func:`enable_logging` calls), this is a
    no-op. Closes both sinks and stops the background drain thread.
    """
    existing = Logger.__dict__.get("_singleton")
    if existing is not None:
        existing.stop()


def set_log_level(level: str) -> None:
    """Set the singleton logger's minimum level.

    Creates the singleton lazily if needed. Accepts any of the
    standard string level names recognised by :mod:`logging`
    (``"DEBUG"``, ``"INFO"``, ``"WARNING"``, ``"ERROR"``,
    ``"CRITICAL"``). Records below the configured level are
    discarded *before* being shipped to either sink.
    """
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
