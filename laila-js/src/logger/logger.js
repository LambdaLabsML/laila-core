/**
 * LAILA ``Logger`` singleton.
 *
 * The Logger is a top-level subsystem (sibling to ``laila.policy``,
 * ``laila.data``, ``laila.entry``) that emits structured log records from
 * anywhere in the package. It has two sinks:
 *
 * 1. The standard library ``logging`` hierarchy rooted at the ``"laila"``
 *    logger. ``StreamHandler`` and/or ``FileHandler`` are attached on
 *    ``Logger.start``; downstream code can route the ``"laila"`` tree
 *    anywhere with stdlib idioms.
 * 2. An optional **pool sink**: when ``pool_nickname`` (or ``pool_id``) is
 *    set, every record is wrapped in an ``Entry`` and routed through
 *    ``laila.memorize`` into the named pool. A thread-local recursion guard
 *    prevents the sink's own ``laila.memorize`` call from generating another
 *    record.
 *
 * The class enforces a **process-wide singleton**: ``new Logger()`` always
 * returns the same instance, mirroring how ``laila`` exposes ``laila.logger``
 * as a single global handle.
 */
import * as logging from "../_compat/logging.js";
import { lazy } from "../_compat/lazy.js";
import { RuntimeError } from "../_compat/errors.js";
import { ConfigDict, Field, PrivateAttr, SKIP_VALIDATION, define_fields, define_private, normalize_kwargs } from "../_compat/pydantic.js";
import { dict_get, dict_has, dict_items, getattr, hasattr, id, is_set, len, str } from "../_compat/pytypes.js";
import { repr } from "../_compat/pyrepr.js";
import { local as threading_local } from "../_compat/threading.js";
import { CLICapable } from "../basics/definitions/cli_capable.js";
import { _LAILA_IDENTIFIABLE_OBJECT } from "../basics/definitions/identifiable_object.js";
import { _LOGGER_SCOPE } from "../macros/strings.js";
import { build_record, normalize_level, numeric_level } from "./record.js";

export const _LAILA_LOGGER_NAME = "laila";

/**
 * Attach a ``logging.NullHandler`` to the ``"laila"`` root once.
 *
 * Mirrors the standard library-author idiom: keep the package quiet by
 * default so downstream code doesn't see "no handlers could be found"
 * warnings, and let the user opt in via ``Logger.start``.
 */
export function _install_null_handler() {
  const root = logging.getLogger(_LAILA_LOGGER_NAME);
  const has_null = root.handlers.some((h) => h instanceof logging.NullHandler);
  if (!has_null) root.addHandler(new logging.NullHandler());
  return root;
}

function _is_listish(x) {
  return Array.isArray(x) || is_set(x);
}

/**
 * Process-wide LAILA logger singleton.
 *
 * The logger has two independent sinks gated by simple flags:
 *
 * - **stdout** -- a ``logging.StreamHandler`` writing to stderr, attached on
 *   ``start`` when ``display`` is ``true``.
 * - **pool**   -- when ``pool_nickname`` (or ``pool_id``) is set, every
 *   record is wrapped in an ``Entry`` and written into that pool.
 *
 * The two sinks are not mutually exclusive: with ``display=true`` and a pool
 * configured, records flow into both. When no pool is set, ``display`` is
 * forced to ``true`` on ``start`` so logs are not silently lost.
 *
 * Fields
 * ------
 * enabled : bool, default false
 *     Master switch. When ``false``, ``emit`` is a no-op even if a record is
 *     built.
 * level : str, default ``"DEBUG"``
 *     Stdlib level name. Records below this level are dropped from both
 *     sinks. The default captures everything.
 * format : str
 *     Stdlib formatter pattern used by the console handler.
 * display : bool, default false
 *     When ``true``, attach a ``logging.StreamHandler`` to stderr. Forced to
 *     ``true`` on ``start`` if neither ``pool_nickname`` nor ``pool_id`` is
 *     set, so records always have at least one sink.
 * pool_nickname : str, optional
 *     Pool alias to which each record will be memorized as an Entry.
 * pool_id : str, optional
 *     Explicit pool ``global_id`` to memorize records to (alternative to
 *     ``pool_nickname``).
 * capture_traceback : bool, default false
 *     When ``true``, ``error``/``critical`` records include the full
 *     traceback in ``extra["traceback"]``.
 */
export class Logger extends CLICapable(_LAILA_IDENTIFIABLE_OBJECT) {
  static _DEFAULT_SCOPES = [_LOGGER_SCOPE];

  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  /** @type {Logger|null} */
  static _singleton = null;

  static {
    define_fields(this, {
      enabled: ["bool", Field({ default: false })],
      level: ["str", Field({ default: "DEBUG" })],
      format: ["str", Field({ default: "%(asctime)s [%(levelname)s] %(name)s: %(message)s" })],
      display: ["bool", Field({ default: false })],
      pool_nickname: ["str | None", Field({ default: null })],
      pool_id: ["str | None", Field({ default: null })],
      capture_traceback: ["bool", Field({ default: false })],
    });
    define_private(this, {
      _scopes: PrivateAttr({ default_factory: () => [_LOGGER_SCOPE] }),
      _stdlib_root: PrivateAttr({ default: null }),
      _installed_handlers: PrivateAttr({ default_factory: () => [] }),
      _in_sink: PrivateAttr({ default_factory: threading_local }),
      _initialized: PrivateAttr({ default: false }),
      _last_pool_sink_error: PrivateAttr({ default: null }),
    });
  }

  // ------------------------------------------------------------------
  // Singleton enforcement (``__new__`` + ``__init__``)
  // ------------------------------------------------------------------

  constructor(data = {}) {
    const cls = new.target;
    if (data === SKIP_VALIDATION) {
      super(data);
      return;
    }
    const existing = Object.prototype.hasOwnProperty.call(cls, "_singleton") ? cls._singleton : null;
    if (existing !== null && existing !== undefined && (existing._initialized ?? false)) {
      const fields = cls.model_fields;
      for (const [key, value] of Object.entries(normalize_kwargs(data, cls))) {
        if (key in fields) existing[key] = value;
      }
      return existing;
    }
    super(data);
    cls._singleton = this;
    this._initialized = true;
    this._stdlib_root = logging.getLogger(_LAILA_LOGGER_NAME);
    _install_null_handler();
    if (this.enabled) this.start();
  }

  /**
   * Tear down handlers and clear the singleton slot.
   *
   * Used by ``laila.terminate`` and the test suite.
   */
  static reset_singleton() {
    const existing = Object.prototype.hasOwnProperty.call(this, "_singleton") ? this._singleton : null;
    if (existing !== null && existing !== undefined) {
      try {
        existing.stop();
      } catch {
        /* pass */
      }
    }
    this._singleton = null;
  }

  // ------------------------------------------------------------------
  // Lifecycle
  // ------------------------------------------------------------------

  /**
   * Install the stdout handler on the ``"laila"`` root, if needed.
   *
   * The console handler is attached when ``display`` is ``true``. If no pool
   * sink is configured (neither ``pool_nickname`` nor ``pool_id``),
   * ``display`` is force-set to ``true`` first so records always have at
   * least one sink.
   *
   * Idempotent: previously installed handlers are removed first so repeated
   * ``start()`` calls don't accumulate duplicates. Sets ``enabled`` to
   * ``true``.
   */
  start() {
    this._remove_installed_handlers();

    const root = this._stdlib_root || logging.getLogger(_LAILA_LOGGER_NAME);
    this._stdlib_root = root;
    root.setLevel(numeric_level(this.level));

    if (this.pool_nickname === null && this.pool_id === null) this.display = true;

    if (this.display) {
      const formatter = new logging.Formatter(this.format);
      const stream = new logging.StreamHandler();
      stream.setFormatter(formatter);
      stream.setLevel(numeric_level(this.level));
      root.addHandler(stream);
      this._installed_handlers.push(stream);
    }

    this.enabled = true;
  }

  /** Remove installed handlers and disable emission. */
  stop() {
    this._remove_installed_handlers();
    this.enabled = false;
  }

  /** Detach every handler this Logger added to the root. */
  _remove_installed_handlers() {
    if (this._stdlib_root === null) this._stdlib_root = logging.getLogger(_LAILA_LOGGER_NAME);
    for (const handler of [...this._installed_handlers]) {
      try {
        this._stdlib_root.removeHandler(handler);
        handler.close();
      } catch {
        /* pass */
      }
    }
    this._installed_handlers.length = 0;
  }

  /** Update both this Logger's stored level and the root level. */
  set_level(level) {
    const canonical = normalize_level(level);
    this.level = canonical;
    if (this._stdlib_root !== null) {
      this._stdlib_root.setLevel(numeric_level(canonical));
      for (const handler of this._installed_handlers) handler.setLevel(numeric_level(canonical));
    }
  }

  // ------------------------------------------------------------------
  // Emission
  // ------------------------------------------------------------------

  /**
   * Send *record* to both sinks, honoring the recursion guard.
   * @param {object} record A record dict produced by ``build_record``.
   */
  emit(record) {
    if (!this.enabled) return;

    if (!("logger_id" in record)) record.logger_id = this.global_id;

    const record_level = numeric_level(record.level ?? "INFO");
    const threshold = numeric_level(this.level);
    if (record_level < threshold) return;

    if (getattr(this._in_sink, "active", false)) return;

    if (this._stdlib_root === null) this._stdlib_root = logging.getLogger(_LAILA_LOGGER_NAME);

    const message = record.message || record.event || "";
    try {
      this._stdlib_root.log(record_level, "%s | %s", message, record);
    } catch {
      /* pass */
    }

    if (this.pool_nickname === null && this.pool_id === null) return;

    this._in_sink.active = true;
    try {
      this._memorize_record(record);
    } catch (exc) {
      this._last_pool_sink_error = repr(exc);
      try {
        this._stdlib_root.warning("laila.logger pool sink failed: %r", exc);
      } catch {
        /* pass */
      }
    } finally {
      this._in_sink.active = false;
    }
  }

  /**
   * Wrap *record* in an Entry and persist it into the configured pool.
   *
   * The entry is serialized through the same ``Record`` / ``transformations``
   * pipeline that ``laila.memorize`` uses, so it round-trips through
   * ``laila.remember`` exactly like any other entry. The actual write
   * bypasses ``laila.memorize`` and the taskforce machinery so that the
   * worker futures and routing events spawned by a normal memorize call do
   * not themselves generate additional log records (which would recurse
   * forever).
   */
  _memorize_record(record) {
    const laila = lazy("laila");

    const nickname = `log-${str(record.ts_unix ?? "")}-${record.event ?? ""}-${id(record)}`;
    const entry = laila.constant(record, { nickname });

    const policy = laila.get_active_policy();
    const router = policy.central.memory.pool_router;
    let pool = null;
    if (this.pool_id !== null && dict_has(router.pools, this.pool_id)) {
      pool = dict_get(router.pools, this.pool_id);
    } else if (this.pool_nickname !== null) {
      const gid = dict_get(router.pools_nicknames, this.pool_nickname);
      if (gid !== null && gid !== undefined) pool = dict_get(router.pools, gid);
    }
    if (pool === null || pool === undefined) {
      throw new RuntimeError(`logger pool sink not found (pool_id=${repr(this.pool_id)}, pool_nickname=${repr(this.pool_nickname)})`);
    }

    const { Record } = lazy("laila.policy.central.memory.record.record");

    const record_wrapper = new Record({
      entry,
      recorder: policy.global_id,
      borrower: policy.global_id,
    });
    // Log records are append-only and never looked up by evolution or
    // creation timestamp, so bypass the pool's index-maintaining ``write``
    // wrapper: indexing every log line would cost an extra shard write per
    // record on the sink pool.
    pool._write(entry.global_id, record_wrapper.serialize(pool.transformations));
  }

  // ------------------------------------------------------------------
  // Convenience emitters (free-form)
  // ------------------------------------------------------------------

  /** Emit a free-form ``DEBUG`` record. */
  debug(message, kwargs = {}) {
    this.emit(build_record("log", { ...kwargs, level: "DEBUG", message }));
  }

  /** Emit a free-form ``INFO`` record. */
  info(message, kwargs = {}) {
    this.emit(build_record("log", { ...kwargs, level: "INFO", message }));
  }

  /** Emit a free-form ``WARNING`` record. */
  warning(message, kwargs = {}) {
    this.emit(build_record("log", { ...kwargs, level: "WARNING", message }));
  }

  /** Emit a free-form ``ERROR`` record. */
  error(message, kwargs = {}) {
    this.emit(build_record("log", { ...kwargs, level: "ERROR", message }));
  }

  /** Emit a free-form ``CRITICAL`` record. */
  critical(message, kwargs = {}) {
    this.emit(build_record("log", { ...kwargs, level: "CRITICAL", message }));
  }

  // ------------------------------------------------------------------
  // Structured emitters used by the rest of the codebase
  // ------------------------------------------------------------------

  /** Emit one record per entry being memorized. */
  record_memorize({ entries, pool, policy }) {
    if (!this.enabled) return;
    if (!_is_listish(entries)) entries = [entries];
    const pool_nick = this._pool_nickname_of(pool, policy);
    for (const entry of entries) {
      const entry_id = getattr(entry, "global_id", null);
      const entry_nick = getattr(getattr(entry, "_constitution", null), "nickname", null);
      const policy_id = getattr(policy, "global_id", null);
      const pool_id = getattr(pool, "global_id", null);
      this.emit(
        build_record("memory.memorize", {
          level: "INFO",
          policy_id,
          pool_id,
          pool_nickname: pool_nick,
          entry_id,
          entry_nickname: entry_nick,
          message: `memorize ${str(entry_id)} -> ${str(pool_id)}`,
        }),
      );
    }
  }

  /** Emit one record per entry id being remembered. */
  record_remember({ entry_ids, pool, policy }) {
    if (!this.enabled) return;
    if (!_is_listish(entry_ids)) entry_ids = [entry_ids];
    const pool_nick = this._pool_nickname_of(pool, policy);
    const policy_id = getattr(policy, "global_id", null);
    const pool_id = getattr(pool, "global_id", null);
    for (const entry_id of entry_ids) {
      const eid = hasattr(entry_id, "global_id") ? entry_id.global_id : entry_id;
      this.emit(
        build_record("memory.remember", {
          level: "INFO",
          policy_id,
          pool_id,
          pool_nickname: pool_nick,
          entry_id: eid,
          message: `remember ${str(eid)} <- ${str(pool_id)}`,
        }),
      );
    }
  }

  /** Emit one record per entry id being forgotten. */
  record_forget({ entry_ids, pool, policy }) {
    if (!this.enabled) return;
    if (!_is_listish(entry_ids)) entry_ids = [entry_ids];
    const pool_nick = this._pool_nickname_of(pool, policy);
    const policy_id = getattr(policy, "global_id", null);
    const pool_id = getattr(pool, "global_id", null);
    for (const entry_id of entry_ids) {
      const eid = hasattr(entry_id, "global_id") ? entry_id.global_id : entry_id;
      this.emit(
        build_record("memory.forget", {
          level: "INFO",
          policy_id,
          pool_id,
          pool_nickname: pool_nick,
          entry_id: eid,
          message: `forget ${str(eid)} -- ${str(pool_id)}`,
        }),
      );
    }
  }

  /** Emit one ``future.created`` record on future construction. */
  record_future_created(future) {
    if (!this.enabled) return;
    this.emit(
      build_record("future.created", {
        level: "INFO",
        future_id: getattr(future, "global_id", null),
        policy_id: getattr(future, "policy_id", null),
        taskforce_id: getattr(future, "taskforce_id", null),
        future_group_id: getattr(future, "future_group_id", null),
        precedence: getattr(future, "precedence", null),
        purpose: getattr(future, "purpose", null),
        status: str(getattr(future, "status", "not_started")),
        message: `future created (purpose=${str(getattr(future, "purpose", null))})`,
      }),
    );
  }

  /** Emit one ``future.status`` record on a state transition. */
  record_future_transition(future, new_status, prev_status = null) {
    if (!this.enabled) return;
    const new_value = getattr(new_status, "value", str(new_status));
    const prev_value = prev_status !== null && prev_status !== undefined ? getattr(prev_status, "value", str(prev_status)) : null;
    const level = new_value === "error" || new_value === "cancelled" ? "ERROR" : "INFO";

    const extra = {};
    const exc = getattr(future, "_exception", null);
    if (exc !== null && exc !== undefined) {
      extra.exc_type = exc.constructor?.name ?? typeof exc;
      extra.exc_repr = repr(exc);
      if (this.capture_traceback) {
        extra.traceback = exc instanceof Error ? (exc.stack ?? String(exc)) + "\n" : `${extra.exc_type}: ${str(exc)}\n`;
      }
    }

    // ``result_global_id`` forces the lazy Entry wrap of a raw result;
    // acceptable here because we only get this far when logging is on.
    let result_id;
    try {
      result_id = getattr(future, "result_global_id", null);
    } catch {
      result_id = null;
    }

    this.emit(
      build_record("future.status", {
        level,
        future_id: getattr(future, "global_id", null),
        policy_id: getattr(future, "policy_id", null),
        taskforce_id: getattr(future, "taskforce_id", null),
        future_group_id: getattr(future, "future_group_id", null),
        precedence: getattr(future, "precedence", null),
        purpose: getattr(future, "purpose", null),
        status: new_value,
        prev_status: prev_value,
        result_id,
        message: `future ${str(getattr(future, "global_id", null))} -> ${str(new_value)}`,
        extra,
      }),
    );
  }

  /** Emit one ``future.group_created`` record. */
  record_group_future_created(group) {
    if (!this.enabled) return;
    const children = [...(getattr(group, "future_ids", []) || [])];
    this.emit(
      build_record("future.group_created", {
        level: "INFO",
        future_id: getattr(group, "global_id", null),
        policy_id: getattr(group, "policy_id", null),
        taskforce_id: getattr(group, "taskforce_id", null),
        child_future_ids: children,
        message: `group future created with ${len(children)} children`,
      }),
    );
  }

  // ------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------

  /** Reverse-lookup the nickname registered for *pool* under *policy*. */
  _pool_nickname_of(pool, policy) {
    if (pool === null || pool === undefined || policy === null || policy === undefined) return null;
    try {
      const router = policy.central.memory.pool_router;
      for (const [nick, gid] of dict_items(router.pools_nicknames)) {
        if (gid === getattr(pool, "global_id", null)) return nick;
      }
    } catch {
      return null;
    }
    return null;
  }
}
