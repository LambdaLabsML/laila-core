/**
 * ``block_on(awaitable)`` -- run a promise to completion synchronously.
 *
 * Python backends talk to their stores through *synchronous* client
 * libraries (``redis-py``, ``psycopg``, ``pymongo``, ``boto3``, ...) and the
 * pool's sync hooks (``_read`` / ``_write`` / ...) call them inline. Node's
 * client libraries are promise-based, so the same hooks drive the native
 * loop pump until the promise settles -- exactly the mechanism behind
 * ``Future.wait`` -- and then return the value (or rethrow the rejection).
 *
 * The wait is allowed while the pool's atomic lock is held (the completing
 * callback is network / disk I/O which never needs that lock), mirroring the
 * sync client call that would block the thread in Python.
 */
import { blocking_wait } from "./threading.js";

/**
 * @template T
 * @param {Promise<T>|{then: Function}} awaitable
 * @param {number|null} [timeout] seconds; ``null`` blocks indefinitely
 * @returns {T}
 */
export function block_on(awaitable, timeout = null) {
  let done = false;
  let failed = false;
  let value;
  let error;
  Promise.resolve(awaitable).then(
    (v) => {
      value = v;
      done = true;
    },
    (e) => {
      error = e;
      failed = true;
      done = true;
    },
  );
  if (!done) {
    const ok = blocking_wait(() => done, timeout, { allow_locked: true });
    if (!ok) {
      const err = new Error(`block_on: timed out after ${timeout}s`);
      err.code = "ERR_LAILA_BLOCK_ON_TIMEOUT";
      throw err;
    }
  }
  if (failed) throw error;
  return value;
}

/** ``time.sleep`` flavour that can be awaited: resolve after *seconds*. */
export function sleep_async(seconds) {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, seconds * 1000)));
}
