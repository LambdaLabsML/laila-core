/**
 * Structured log record builder for the LAILA Logger singleton.
 *
 * A laila log record is a JSON-trivial dict with a *fixed* schema of optional
 * well-known fields (``policy_id``, ``pool_id``, ``future_id``, ``status``,
 * ...) plus a free-form ``extra`` bag for anything not covered by the schema.
 * Records are always:
 *
 * - Composed of strings, numbers, lists, and dicts only -- so they serialise
 *   cleanly with ``json.dumps`` and round-trip safely through any laila pool.
 * - Time-stamped with both an ISO-8601 string (``ts``, UTC, with ``Z``
 *   suffix) and a Unix epoch float (``ts_unix``) -- the former for humans,
 *   the latter for time-series analytics.
 * - Equipped with a normalised string ``level`` (one of ``DEBUG``, ``INFO``,
 *   ``WARNING``, ``ERROR``, ``CRITICAL``).
 *
 * This module exports two utilities:
 *
 * - ``normalize_level`` / ``numeric_level`` -- bidirectional conversion
 *   between numeric ``logging`` levels and the canonical string names used
 *   in records.
 * - ``build_record`` -- the structured-record factory called by the
 *   ``Logger`` itself for every event. Optional fields are omitted from the
 *   result rather than written as ``None`` so records stay compact on the
 *   wire.
 */
import { time } from "../_compat/datetime.js";
import { PyFloat, dict_items, getattr, is_int, str } from "../_compat/pytypes.js";

/** @type {Record<string, number>} */
export const _LEVEL_NUMERIC = {
  DEBUG: 10,
  INFO: 20,
  WARNING: 30,
  ERROR: 40,
  CRITICAL: 50,
};

/**
 * Coerce a level (str or int) into a canonical upper-case name.
 *
 * @param {string|number} level Either a ``logging`` level name or its numeric value.
 * @returns {string} One of ``"DEBUG"``, ``"INFO"``, ``"WARNING"``,
 *   ``"ERROR"``, ``"CRITICAL"``. Unknown values fall back to ``"INFO"``.
 */
export function normalize_level(level) {
  if (is_int(level) && typeof level !== "boolean") {
    for (const [name, value] of Object.entries(_LEVEL_NUMERIC)) if (value === level) return name;
    return "INFO";
  }
  if (typeof level === "string") {
    const upper = level.toUpperCase();
    if (Object.prototype.hasOwnProperty.call(_LEVEL_NUMERIC, upper)) return upper;
  }
  return "INFO";
}

/** Return the integer ``logging`` level for a name or numeric value. */
export function numeric_level(level) {
  return _LEVEL_NUMERIC[normalize_level(level)];
}

/** Convert any identifiable-like object to a string global_id. */
export function _coerce_id(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") return value;
  const gid = getattr(value, "global_id", null);
  if (gid !== null && gid !== undefined) return str(gid);
  return str(value);
}

/**
 * ``datetime.fromtimestamp(now, tz=UTC).isoformat().replace("+00:00", "Z")``:
 * microsecond precision, the fractional part omitted when it is zero.
 */
function _iso_utc(now) {
  const ms_total = Math.round(now * 1e6); // microseconds
  const secs = Math.floor(ms_total / 1e6);
  const micros = ms_total - secs * 1e6;
  const d = new Date(secs * 1000);
  const pad = (n, w) => String(n).padStart(w, "0");
  let s = `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1, 2)}-${pad(d.getUTCDate(), 2)}T${pad(d.getUTCHours(), 2)}:${pad(d.getUTCMinutes(), 2)}:${pad(d.getUTCSeconds(), 2)}`;
  if (micros !== 0) s += `.${pad(micros, 6)}`;
  return s + "Z";
}

/**
 * Build a structured LAILA log record dict.
 *
 * All ``*_id`` fields accept either a string global_id or any
 * ``_LAILA_IDENTIFIABLE_OBJECT`` instance and are normalized to strings.
 * Unspecified optional fields are omitted (kept out of the dict) so records
 * stay compact.
 *
 * @param {string} event
 * @param {object} [opts] keyword arguments: ``level`` (default ``"INFO"``),
 *   ``message``, ``policy_id``, ``pool_id``, ``pool_nickname``, ``entry_id``,
 *   ``entry_nickname``, ``future_id``, ``future_group_id``, ``precedence``,
 *   ``purpose``, ``taskforce_id``, ``logger_id``, ``status``, ``prev_status``,
 *   ``result_id``, ``child_future_ids``, ``child_results``, ``peer_id``,
 *   ``extra``.
 * @returns {object} A JSON-trivial record. Always contains ``ts``,
 *   ``ts_unix``, ``level``, ``event``, and ``extra``.
 */
export function build_record(event, opts = {}) {
  const {
    level = "INFO",
    message = null,
    policy_id = null,
    pool_id = null,
    pool_nickname = null,
    entry_id = null,
    entry_nickname = null,
    future_id = null,
    future_group_id = null,
    precedence = null,
    purpose = null,
    taskforce_id = null,
    logger_id = null,
    status = null,
    prev_status = null,
    result_id = null,
    child_future_ids = null,
    child_results = null,
    peer_id = null,
    extra = null,
  } = opts;

  const now = time();
  const record = {
    ts: _iso_utc(now),
    ts_unix: Number.isInteger(now) ? new PyFloat(now) : now,
    level: normalize_level(level),
    event,
    extra: extra ? Object.fromEntries(dict_items(extra)) : {},
  };
  if (message !== null && message !== undefined) record.message = message;

  const optional_ids = {
    policy_id,
    pool_id,
    pool_nickname,
    entry_id,
    entry_nickname,
    future_id,
    future_group_id,
    precedence,
    purpose,
    taskforce_id,
    logger_id,
    peer_id,
    result_id,
  };
  for (const [key, value] of Object.entries(optional_ids)) {
    if (value === null || value === undefined) continue;
    if (key === "pool_nickname" || key === "entry_nickname" || key === "purpose") record[key] = str(value);
    else record[key] = _coerce_id(value);
  }

  if (status !== null && status !== undefined) record.status = str(status);
  if (prev_status !== null && prev_status !== undefined) record.prev_status = str(prev_status);

  if (child_future_ids !== null && child_future_ids !== undefined) record.child_future_ids = [...child_future_ids].map((c) => _coerce_id(c));
  if (child_results !== null && child_results !== undefined) record.child_results = [...child_results].map((c) => _coerce_id(c));

  return record;
}
