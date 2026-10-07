/**
 * ``time`` -- the subset laila uses.
 *
 * - ``time()``       : seconds since the epoch (float)
 * - ``monotonic()``  : monotonic seconds (float)
 * - ``perf_counter()``: alias of ``monotonic``
 * - ``sleep(s)``     : *blocking* sleep that keeps the event loop pumping
 *   (``threading.blocking_wait`` with an always-false predicate)
 */
import { blocking_wait } from "./threading.js";

export function time() {
  return Date.now() / 1000;
}

export function monotonic() {
  return Number(process.hrtime.bigint()) / 1e9;
}

export function perf_counter() {
  return monotonic();
}

/** Blocking ``time.sleep(seconds)``: the loop keeps running underneath. */
export function sleep(seconds) {
  if (!(seconds > 0)) return;
  blocking_wait(() => false, seconds, { allow_locked: true });
}

export default { time, monotonic, perf_counter, sleep };
