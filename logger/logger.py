"""LAILA :class:`Logger` singleton.

The Logger is a top-level subsystem (sibling to :mod:`laila.policy`,
:mod:`laila.pool`, :mod:`laila.entry`) that emits structured log records
from anywhere in the package. It has two sinks:

1. The standard library ``logging`` hierarchy rooted at the ``"laila"``
   logger.  ``StreamHandler`` and/or ``FileHandler`` are attached on
   :meth:`Logger.start`; downstream code can route the ``"laila"`` tree
   anywhere with stdlib idioms.
2. An optional **pool sink**: when ``pool_nickname`` (or ``pool_id``) is
   set, every record is wrapped in an :class:`~laila.entry.entry.Entry`
   and routed through :func:`laila.memorize` into the named pool. A
   thread-local recursion guard prevents the sink's own
   :func:`laila.memorize` call from generating another record.

The class enforces a **process-wide singleton**: ``Logger()`` always
returns the same instance, mirroring how :mod:`laila` exposes
``laila.logger`` as a single global handle.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, ClassVar

from pydantic import ConfigDict, Field, PrivateAttr

from ..basics.definitions.cli_capable import _LAILA_CLI_CAPABLE_CLASS
from ..basics.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from ..macros.strings import _LOGGER_SCOPE
from .record import build_record, normalize_level, numeric_level

_LAILA_LOGGER_NAME = "laila"


def _install_null_handler() -> logging.Logger:
    """Attach a :class:`logging.NullHandler` to the ``"laila"`` root once.

    Mirrors the standard library-author idiom: keep the package quiet by
    default so downstream code doesn't see "no handlers could be found"
    warnings, and let the user opt in via :meth:`Logger.start`.
    """
    root = logging.getLogger(_LAILA_LOGGER_NAME)
    has_null = any(isinstance(h, logging.NullHandler) for h in root.handlers)
    if not has_null:
        root.addHandler(logging.NullHandler())
    return root


class Logger(_LAILA_CLI_CAPABLE_CLASS, _LAILA_IDENTIFIABLE_OBJECT):
    """Process-wide LAILA logger singleton.

    The logger has two independent sinks gated by simple flags:

    * **stdout** -- a :class:`logging.StreamHandler` writing to stderr,
      attached on :meth:`start` when ``display`` is ``True``.
    * **pool**   -- when ``pool_nickname`` (or ``pool_id``) is set, every
      record is wrapped in an :class:`~laila.entry.entry.Entry` and
      written into that pool.

    The two sinks are not mutually exclusive: with ``display=True`` and
    a pool configured, records flow into both. When no pool is set,
    ``display`` is forced to ``True`` on :meth:`start` so logs are not
    silently lost.

    Parameters
    ----------
    enabled : bool, default False
        Master switch. When ``False``, :meth:`emit` is a no-op even if a
        record is built.
    level : str, default ``"DEBUG"``
        Stdlib level name. Records below this level are dropped from
        both sinks. The default captures everything.
    format : str
        Stdlib formatter pattern used by the console handler.
    display : bool, default False
        When ``True``, attach a :class:`logging.StreamHandler` to
        stderr. Forced to ``True`` on :meth:`start` if neither
        ``pool_nickname`` nor ``pool_id`` is set, so records always
        have at least one sink.
    pool_nickname : str, optional
        Pool alias to which each record will be memorized as an Entry.
    pool_id : str, optional
        Explicit pool ``global_id`` to memorize records to (alternative
        to ``pool_nickname``).
    capture_traceback : bool, default False
        When ``True``, ``error``/``critical`` records include the full
        Python traceback in ``extra["traceback"]``.
    """

    _scopes: list[str] = PrivateAttr(default_factory=lambda: [_LOGGER_SCOPE])

    model_config = ConfigDict(arbitrary_types_allowed=True)

    enabled: bool = Field(default=False)
    level: str = Field(default="DEBUG")
    format: str = Field(
        default="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    display: bool = Field(default=False)
    pool_nickname: str | None = Field(default=None)
    pool_id: str | None = Field(default=None)
    capture_traceback: bool = Field(default=False)

    _stdlib_root: Any = PrivateAttr(default=None)
    _installed_handlers: list[Any] = PrivateAttr(default_factory=list)
    _in_sink: Any = PrivateAttr(default_factory=threading.local)
    _initialized: bool = PrivateAttr(default=False)
    _last_pool_sink_error: str | None = PrivateAttr(default=None)

    _singleton: ClassVar[Logger | None] = None

    # ------------------------------------------------------------------
    # Singleton enforcement
    # ------------------------------------------------------------------

    def __new__(cls, **data: Any) -> Logger:
        existing = cls.__dict__.get("_singleton")
        if existing is not None:
            return existing
        return super().__new__(cls)

    def __init__(self, **data: Any) -> None:
        cls = type(self)
        existing = cls.__dict__.get("_singleton")
        if existing is self and getattr(self, "_initialized", False):
            for key, value in data.items():
                if key in cls.model_fields:
                    setattr(self, key, value)
            return
        super().__init__(**data)
        cls._singleton = self
        self._initialized = True
        self._stdlib_root = logging.getLogger(_LAILA_LOGGER_NAME)
        _install_null_handler()
        if self.enabled:
            self.start()

    @classmethod
    def reset_singleton(cls) -> None:
        """Tear down handlers and clear the singleton slot.

        Used by :func:`laila.terminate` and the test suite.
        """
        existing = cls.__dict__.get("_singleton")
        if existing is not None:
            try:
                existing.stop()
            except Exception:
                pass
        cls._singleton = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Install the stdout handler on the ``"laila"`` root, if needed.

        The console handler is attached when ``display`` is ``True``.
        If no pool sink is configured (neither ``pool_nickname`` nor
        ``pool_id``), ``display`` is force-set to ``True`` first so
        records always have at least one sink.

        Idempotent: previously installed handlers are removed first so
        repeated ``start()`` calls don't accumulate duplicates. Sets
        ``enabled`` to ``True``.
        """
        self._remove_installed_handlers()

        root = self._stdlib_root or logging.getLogger(_LAILA_LOGGER_NAME)
        self._stdlib_root = root
        root.setLevel(numeric_level(self.level))

        if self.pool_nickname is None and self.pool_id is None:
            self.display = True

        if self.display:
            formatter = logging.Formatter(self.format)
            stream = logging.StreamHandler()
            stream.setFormatter(formatter)
            stream.setLevel(numeric_level(self.level))
            root.addHandler(stream)
            self._installed_handlers.append(stream)

        self.enabled = True

    def stop(self) -> None:
        """Remove installed handlers and disable emission."""
        self._remove_installed_handlers()
        self.enabled = False

    def _remove_installed_handlers(self) -> None:
        """Detach every handler this Logger added to the root."""
        if self._stdlib_root is None:
            self._stdlib_root = logging.getLogger(_LAILA_LOGGER_NAME)
        for handler in list(self._installed_handlers):
            try:
                self._stdlib_root.removeHandler(handler)
                handler.close()
            except Exception:
                pass
        self._installed_handlers.clear()

    def set_level(self, level: Any) -> None:
        """Update both this Logger's stored level and the root level."""
        canonical = normalize_level(level)
        self.level = canonical
        if self._stdlib_root is not None:
            self._stdlib_root.setLevel(numeric_level(canonical))
            for handler in self._installed_handlers:
                handler.setLevel(numeric_level(canonical))

    # ------------------------------------------------------------------
    # Emission
    # ------------------------------------------------------------------

    def emit(self, record: dict[str, Any]) -> None:
        """Send *record* to both sinks, honoring the recursion guard.

        Parameters
        ----------
        record : dict
            A record dict produced by :func:`laila.logger.record.build_record`.
        """
        if not self.enabled:
            return

        record.setdefault("logger_id", self.global_id)

        record_level = numeric_level(record.get("level", "INFO"))
        threshold = numeric_level(self.level)
        if record_level < threshold:
            return

        if getattr(self._in_sink, "active", False):
            return

        if self._stdlib_root is None:
            self._stdlib_root = logging.getLogger(_LAILA_LOGGER_NAME)

        message = record.get("message") or record.get("event", "")
        try:
            self._stdlib_root.log(record_level, "%s | %s", message, record)
        except Exception:
            pass

        if self.pool_nickname is None and self.pool_id is None:
            return

        self._in_sink.active = True
        try:
            self._memorize_record(record)
        except Exception as exc:
            self._last_pool_sink_error = repr(exc)
            try:
                self._stdlib_root.warning("laila.logger pool sink failed: %r", exc)
            except Exception:
                pass
        finally:
            self._in_sink.active = False

    def _memorize_record(self, record: dict[str, Any]) -> None:
        """Wrap *record* in an Entry and persist it into the configured pool.

        The entry is serialized through the same :class:`Record` /
        ``transformations`` pipeline that :func:`laila.memorize` uses, so
        it round-trips through :func:`laila.remember` exactly like any
        other entry. The actual write bypasses :func:`laila.memorize` and
        the taskforce machinery so that the worker futures and routing
        events spawned by a normal memorize call do not themselves
        generate additional log records (which would recurse forever).
        """
        import laila

        nickname = f"log-{record.get('ts_unix', '')}-{record.get('event', '')}-{id(record)}"
        entry = laila.constant(data=record, nickname=nickname)

        policy = laila.get_active_policy()
        router = policy.central.memory.pool_router
        pool = None
        if self.pool_id is not None and self.pool_id in router.pools:
            pool = router.pools[self.pool_id]
        elif self.pool_nickname is not None:
            gid = router.pools_nicknames.get(self.pool_nickname)
            if gid is not None:
                pool = router.pools.get(gid)
        if pool is None:
            raise RuntimeError(
                "logger pool sink not found "
                f"(pool_id={self.pool_id!r}, pool_nickname={self.pool_nickname!r})"
            )

        from ..policy.central.memory.record.record import Record

        record_wrapper = Record(
            entry=entry,
            recorder=policy.global_id,
            borrower=policy.global_id,
        )
        pool[entry.global_id] = record_wrapper.serialize(
            transformations=pool.transformations,
        )

    # ------------------------------------------------------------------
    # Convenience emitters (free-form)
    # ------------------------------------------------------------------

    def debug(self, message: str, **kwargs: Any) -> None:
        """Emit a free-form ``DEBUG`` record."""
        self.emit(build_record("log", level="DEBUG", message=message, **kwargs))

    def info(self, message: str, **kwargs: Any) -> None:
        """Emit a free-form ``INFO`` record."""
        self.emit(build_record("log", level="INFO", message=message, **kwargs))

    def warning(self, message: str, **kwargs: Any) -> None:
        """Emit a free-form ``WARNING`` record."""
        self.emit(build_record("log", level="WARNING", message=message, **kwargs))

    def error(self, message: str, **kwargs: Any) -> None:
        """Emit a free-form ``ERROR`` record."""
        self.emit(build_record("log", level="ERROR", message=message, **kwargs))

    def critical(self, message: str, **kwargs: Any) -> None:
        """Emit a free-form ``CRITICAL`` record."""
        self.emit(build_record("log", level="CRITICAL", message=message, **kwargs))

    # ------------------------------------------------------------------
    # Structured emitters used by the rest of the codebase
    # ------------------------------------------------------------------

    def record_memorize(self, *, entries: Any, pool: Any, policy: Any) -> None:
        """Emit one record per entry being memorized."""
        if not self.enabled:
            return
        if not isinstance(entries, (list, tuple, set, frozenset)):
            entries = [entries]
        pool_nick = self._pool_nickname_of(pool, policy)
        for entry in entries:
            entry_id = getattr(entry, "global_id", None)
            entry_nick = getattr(getattr(entry, "_constitution", None), "nickname", None)
            policy_id = getattr(policy, "global_id", None)
            pool_id = getattr(pool, "global_id", None)
            self.emit(
                build_record(
                    "memory.memorize",
                    level="INFO",
                    policy_id=policy_id,
                    pool_id=pool_id,
                    pool_nickname=pool_nick,
                    entry_id=entry_id,
                    entry_nickname=entry_nick,
                    message=f"memorize {entry_id} -> {pool_id}",
                )
            )

    def record_remember(self, *, entry_ids: Any, pool: Any, policy: Any) -> None:
        """Emit one record per entry id being remembered."""
        if not self.enabled:
            return
        if not isinstance(entry_ids, (list, tuple, set, frozenset)):
            entry_ids = [entry_ids]
        pool_nick = self._pool_nickname_of(pool, policy)
        policy_id = getattr(policy, "global_id", None)
        pool_id = getattr(pool, "global_id", None)
        for entry_id in entry_ids:
            eid = entry_id.global_id if hasattr(entry_id, "global_id") else entry_id
            self.emit(
                build_record(
                    "memory.remember",
                    level="INFO",
                    policy_id=policy_id,
                    pool_id=pool_id,
                    pool_nickname=pool_nick,
                    entry_id=eid,
                    message=f"remember {eid} <- {pool_id}",
                )
            )

    def record_forget(self, *, entry_ids: Any, pool: Any, policy: Any) -> None:
        """Emit one record per entry id being forgotten."""
        if not self.enabled:
            return
        if not isinstance(entry_ids, (list, tuple, set, frozenset)):
            entry_ids = [entry_ids]
        pool_nick = self._pool_nickname_of(pool, policy)
        policy_id = getattr(policy, "global_id", None)
        pool_id = getattr(pool, "global_id", None)
        for entry_id in entry_ids:
            eid = entry_id.global_id if hasattr(entry_id, "global_id") else entry_id
            self.emit(
                build_record(
                    "memory.forget",
                    level="INFO",
                    policy_id=policy_id,
                    pool_id=pool_id,
                    pool_nickname=pool_nick,
                    entry_id=eid,
                    message=f"forget {eid} -- {pool_id}",
                )
            )

    def record_future_created(self, future: Any) -> None:
        """Emit one ``future.created`` record on future construction."""
        if not self.enabled:
            return
        self.emit(
            build_record(
                "future.created",
                level="INFO",
                future_id=getattr(future, "global_id", None),
                policy_id=getattr(future, "policy_id", None),
                taskforce_id=getattr(future, "taskforce_id", None),
                future_group_id=getattr(future, "future_group_id", None),
                precedence=getattr(future, "precedence", None),
                purpose=getattr(future, "purpose", None),
                status=str(getattr(future, "status", "not_started")),
                message=f"future created (purpose={getattr(future, 'purpose', None)})",
            )
        )

    def record_future_transition(
        self,
        future: Any,
        new_status: Any,
        prev_status: Any = None,
    ) -> None:
        """Emit one ``future.status`` record on a state transition."""
        if not self.enabled:
            return
        new_value = getattr(new_status, "value", str(new_status))
        prev_value = (
            getattr(prev_status, "value", str(prev_status)) if prev_status is not None else None
        )
        level = "ERROR" if new_value in ("error", "cancelled") else "INFO"

        extra: dict[str, Any] = {}
        exc = getattr(future, "_exception", None)
        if exc is not None:
            extra["exc_type"] = type(exc).__name__
            extra["exc_repr"] = repr(exc)
            if self.capture_traceback:
                import traceback

                extra["traceback"] = "".join(
                    traceback.format_exception(type(exc), exc, exc.__traceback__)
                )

        result_id = getattr(future, "_result_global_id", None)

        self.emit(
            build_record(
                "future.status",
                level=level,
                future_id=getattr(future, "global_id", None),
                policy_id=getattr(future, "policy_id", None),
                taskforce_id=getattr(future, "taskforce_id", None),
                future_group_id=getattr(future, "future_group_id", None),
                precedence=getattr(future, "precedence", None),
                purpose=getattr(future, "purpose", None),
                status=new_value,
                prev_status=prev_value,
                result_id=result_id,
                message=f"future {getattr(future, 'global_id', None)} -> {new_value}",
                extra=extra,
            )
        )

    def record_group_future_created(self, group: Any) -> None:
        """Emit one ``future.group_created`` record."""
        if not self.enabled:
            return
        children = list(getattr(group, "future_ids", []) or [])
        self.emit(
            build_record(
                "future.group_created",
                level="INFO",
                future_id=getattr(group, "global_id", None),
                policy_id=getattr(group, "policy_id", None),
                taskforce_id=getattr(group, "taskforce_id", None),
                child_future_ids=children,
                message=f"group future created with {len(children)} children",
            )
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _pool_nickname_of(self, pool: Any, policy: Any) -> str | None:
        """Reverse-lookup the nickname registered for *pool* under *policy*."""
        if pool is None or policy is None:
            return None
        try:
            router = policy.central.memory.pool_router
            for nick, gid in router.pools_nicknames.items():
                if gid == getattr(pool, "global_id", None):
                    return nick
        except Exception:
            return None
        return None
