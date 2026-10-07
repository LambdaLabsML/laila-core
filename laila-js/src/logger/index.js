/**
 * LAILA Logger subsystem.
 *
 * A top-level singleton (sibling to ``laila.policy``, ``laila.data``,
 * ``laila.entry``) that fans structured log records out to two independent
 * sinks:
 *
 * - The standard-library ``logging`` hierarchy under the
 *   ``_LAILA_LOGGER_NAME`` channel, with optional stderr streaming via the
 *   ``display=true`` flag.
 * - A laila pool, by ``memorize``-ing each record as a JSON-shaped entry. The
 *   pool can be selected by nickname or global id; missing pool
 *   configuration triggers an automatic fall-back to ``display=true`` so
 *   records are never silently dropped.
 *
 * The two sinks are independent and can both be active at once -- a typical
 * setup is "always display, also persist into Postgres" so an operator can
 * watch the live stream while history accumulates in a pool.
 *
 * This module re-exports:
 *
 * - ``Logger`` -- the singleton class itself, with all the configurable
 *   fields and lifecycle methods (``start``, ``stop``, ``set_level``, ...).
 * - ``record`` helpers (``build_record``, ``normalize_level``,
 *   ``numeric_level``).
 * - The convenience top-level functions ``get_logger``, ``enable_logging``,
 *   ``disable_logging``, ``set_log_level`` -- the same names re-exported on
 *   the ``laila`` package.
 */
import { _LAILA_LOGGER_NAME, Logger, _install_null_handler } from "./logger.js";
import { build_record, normalize_level, numeric_level } from "./record.js";

export { _LAILA_LOGGER_NAME, Logger, _install_null_handler, build_record, normalize_level, numeric_level };

function _existing() {
  return Object.prototype.hasOwnProperty.call(Logger, "_singleton") ? Logger._singleton : null;
}

/**
 * Return the process-wide ``Logger`` singleton, creating it lazily.
 *
 * The first call constructs the singleton with default settings (silent: no
 * display, no pool). Subsequent calls return the same instance. Use
 * ``enable_logging`` to actually start emitting records.
 * @returns {Logger}
 */
export function get_logger() {
  const existing = _existing();
  if (existing !== null && existing !== undefined) return existing;
  return new Logger();
}

/**
 * Configure and start the singleton logger.
 *
 * The logger has two independent sinks: a stdout ``logging.StreamHandler``
 * enabled by ``display=true``, and a pool sink enabled by passing
 * ``pool_nickname`` (or ``pool_id``). They are not mutually exclusive -- you
 * can have both at once. When no pool is configured, ``display`` is forced to
 * ``true`` so records are not silently dropped.
 *
 * @param {string} [level="DEBUG"] Stdlib level name. The default captures every record.
 * @param {{pool_nickname?: string|null, pool_id?: string|null, display?: boolean, capture_traceback?: boolean}} [opts]
 *   ``pool_nickname``: pool alias to memorize each record into.
 *   ``pool_id``: pool ``global_id`` to memorize each record into.
 *   ``display``: when ``true``, also stream records to stderr via stdlib.
 *   Forced to ``true`` on start if neither ``pool_nickname`` nor ``pool_id``
 *   is provided. ``capture_traceback``: include tracebacks for errored futures.
 * @returns {Logger} The (now-running) singleton.
 */
export function enable_logging(level = "DEBUG", opts = {}) {
  const { pool_nickname = null, pool_id = null, display = false, capture_traceback = false } = opts;
  const logger = get_logger();
  logger.level = level;
  logger.display = display;
  if (pool_nickname !== null) logger.pool_nickname = pool_nickname;
  if (pool_id !== null) logger.pool_id = pool_id;
  logger.capture_traceback = capture_traceback;
  logger.start();
  return logger;
}

/**
 * Stop the singleton logger if one is alive.
 *
 * Idempotent: if no logger has been constructed yet (no ``get_logger`` /
 * ``enable_logging`` calls), this is a no-op. Closes both sinks and stops the
 * background drain thread.
 */
export function disable_logging() {
  const existing = _existing();
  if (existing !== null && existing !== undefined) existing.stop();
}

/**
 * Set the singleton logger's minimum level.
 *
 * Creates the singleton lazily if needed. Accepts any of the standard string
 * level names recognised by ``logging`` (``"DEBUG"``, ``"INFO"``,
 * ``"WARNING"``, ``"ERROR"``, ``"CRITICAL"``). Records below the configured
 * level are discarded *before* being shipped to either sink.
 */
export function set_log_level(level) {
  get_logger().set_level(level);
}

export const __all__ = [
  "_LAILA_LOGGER_NAME",
  "Logger",
  "_install_null_handler",
  "build_record",
  "disable_logging",
  "enable_logging",
  "get_logger",
  "normalize_level",
  "numeric_level",
  "set_log_level",
];
