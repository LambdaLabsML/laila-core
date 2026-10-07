/**
 * Python ``datetime`` / ``time`` helpers with CPython-identical formatting.
 *
 *   now_iso_ms()      == datetime.now(UTC).isoformat(timespec="milliseconds")
 *                        -> "2026-10-04T23:51:00.123+00:00"
 *   now_iso_us()      == datetime.now(UTC).isoformat()
 *                        -> "2026-10-04T23:51:00.123456+00:00" (fraction omitted when 0)
 *   fromisoformat(s)  == datetime.fromisoformat(s)  -> Date (UTC instant)
 *   time()            == time.time()  (seconds, float)
 *   monotonic()       == time.monotonic() (seconds, float)
 */
import { ValueError } from "./errors.js";

function _pad(n, w = 2) {
  return String(n).padStart(w, "0");
}

/** ``datetime.now(UTC).isoformat(timespec="milliseconds")`` */
export function now_iso_ms() {
  return isoformat(new Date(), { timespec: "milliseconds" });
}

/** ``datetime.now(UTC).isoformat()`` (microsecond precision, fraction omitted when zero). */
export function now_iso_us() {
  const ms = Date.now();
  // Sub-millisecond digits from the high-resolution clock; wall precision is
  // still ms, but the shape matches CPython's 6-digit fraction.
  const frac_us = Math.floor((performance.now() % 1) * 1000);
  return isoformat(new Date(ms), { timespec: "microseconds", extra_us: frac_us });
}

/**
 * ``datetime.isoformat`` for a UTC instant.
 * @param {Date} d
 * @param {{timespec?: "auto"|"seconds"|"milliseconds"|"microseconds", extra_us?: number, tz?: boolean}} [opts]
 */
export function isoformat(d, opts = {}) {
  const { timespec = "auto", extra_us = 0, tz = true } = opts;
  const base =
    `${_pad(d.getUTCFullYear(), 4)}-${_pad(d.getUTCMonth() + 1)}-${_pad(d.getUTCDate())}` +
    `T${_pad(d.getUTCHours())}:${_pad(d.getUTCMinutes())}:${_pad(d.getUTCSeconds())}`;
  const ms = d.getUTCMilliseconds();
  const us = ms * 1000 + extra_us;
  let frac = "";
  if (timespec === "milliseconds") frac = "." + _pad(ms, 3);
  else if (timespec === "microseconds") frac = "." + _pad(us, 6);
  else if (timespec === "auto" && us !== 0) frac = "." + _pad(us, 6);
  return base + frac + (tz ? "+00:00" : "");
}

/**
 * ``datetime.fromisoformat`` (Python 3.11+ accepts most ISO-8601 forms).
 * Returns a ``Date``; naive strings are read as UTC (laila only emits UTC).
 * @param {string} s
 */
export function fromisoformat(s) {
  const m = /^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2})(?:[.,](\d{1,6}))?)?)?(Z|[+-]\d{2}:?\d{2})?$/.exec(s);
  if (!m) throw new ValueError(`Invalid isoformat string: '${s}'`);
  const [, Y, M, D, h = "0", mi = "0", sec = "0", frac = "", tzs] = m;
  const us = frac ? parseInt(frac.padEnd(6, "0"), 10) : 0;
  let t = Date.UTC(+Y, +M - 1, +D, +h, +mi, +sec, Math.floor(us / 1000));
  if (tzs && tzs !== "Z") {
    const sign = tzs[0] === "-" ? -1 : 1;
    const hh = parseInt(tzs.slice(1, 3), 10);
    const mm = parseInt(tzs.slice(-2), 10);
    t -= sign * (hh * 60 + mm) * 60000;
  }
  return new Date(t);
}

/** ``time.time()`` */
export function time() {
  return Date.now() / 1000;
}

/** ``time.monotonic()`` */
export function monotonic() {
  return performance.now() / 1000;
}

/** ``time.perf_counter()`` */
export const perf_counter = monotonic;

/** ``time.sleep(seconds)`` -- blocking (uses Atomics.wait; never from a microtask). */
export function sleep(seconds) {
  const sab = new Int32Array(new SharedArrayBuffer(4));
  Atomics.wait(sab, 0, 0, Math.max(0, seconds * 1000));
}

/** ``asyncio.sleep(seconds)`` */
export function sleep_async(seconds) {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, seconds * 1000)));
}

/** ``datetime.fromtimestamp(ts, tz=UTC).isoformat().replace("+00:00", "Z")`` as used by the logger. */
export function ts_to_iso_z(ts_seconds) {
  const ms = Math.floor(ts_seconds * 1000);
  const us = Math.round((ts_seconds * 1e6) % 1000);
  return isoformat(new Date(ms), { timespec: "auto", extra_us: Number.isFinite(us) ? Math.max(0, us) : 0 }).replace("+00:00", "Z");
}
