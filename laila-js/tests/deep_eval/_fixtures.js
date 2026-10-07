/**
 * Shared fixtures for the deep unit / black-box evaluation suite.
 *
 * Port of ``tests/deep_eval/conftest.py``.
 *
 * Every test module under ``tests/deep_eval`` is import-safe without cloud
 * credentials or optional hardware back-ends. Tests that exercise the active
 * policy use ``with_fresh_policy`` which swaps in an isolated
 * ``_LAILA_IDENTIFIABLE_POLICY`` for the duration of the test and restores
 * the previous one afterwards.
 *
 * Tests that document *suspected defects* are marked ``suspected_bug`` in
 * Python and declared ``xfail(strict=True)``; here they carry a node:test
 * ``todo`` so the suite stays green while the assertions still describe the
 * behaviour the public documentation promises.
 */
export const S = new URL("../../src/", import.meta.url).href;

// Importing the real root module registers ``lazy("laila")`` so laila's own
// internals resolve the active policy, ``laila.args`` and the memory shims
// exactly as user code does.
const { default: laila } = await import(S + "index.js");
const { _LAILA_IDENTIFIABLE_POLICY } = await import(S + "policy/schema/base.js");
const { blocking_wait } = await import(S + "_compat/threading.js");

export { laila };

/**
 * Run a synchronous body on a fresh macrotask: ``node:test`` invokes test
 * bodies from a microtask, where a blocking wait (``Thread.join``,
 * ``Future.wait``, ``laila.remember(...)``) is impossible by construction
 * (nothing can settle until the job returns).
 * @template T
 * @param {() => T} fn
 * @returns {Promise<T>}
 */
export function macrotask(fn) {
  return new Promise((resolve, reject) =>
    setImmediate(() => {
      try {
        resolve(fn());
      } catch (e) {
        reject(e);
      }
    }),
  );
}

/**
 * ``fresh_policy`` fixture: activate an isolated policy for the test and
 * restore the old one after.
 *
 *     original = laila.get_active_policy()
 *     policy = _LAILA_IDENTIFIABLE_POLICY()
 *     laila.activate_policy(policy)
 *     try:
 *         yield policy
 *     finally:
 *         with contextlib.suppress(Exception):
 *             policy.central.command.shutdown(wait=True, cancel_pending=True)
 *         laila.activate_policy(original)
 *
 * Runs synchronously (callers wrap the whole thing in ``macrotask``); ``fn``
 * receives the fresh policy and its return value is passed through.
 * @template T
 * @param {(policy: any) => T} fn
 * @returns {T}
 */
export function with_fresh_policy(fn) {
  const original = laila.get_active_policy();
  const policy = new _LAILA_IDENTIFIABLE_POLICY();
  laila.activate_policy(policy);
  try {
    return fn(policy);
  } finally {
    try {
      policy.central.command.shutdown({ wait: true, cancel_pending: true });
    } catch {
      // contextlib.suppress(Exception)
    }
    laila.activate_policy(original);
  }
}

/**
 * ``run`` fixture: run a coroutine to completion on a private event loop
 * (``asyncio.run(coro)``). Blocks the caller by pumping the Node event loop
 * until the promise settles, so -- like every other blocking wait -- it must
 * run from a macrotask (wrap the test body in ``macrotask``).
 * @template T
 * @param {Promise<T>} coro
 * @returns {T}
 */
export function run(coro) {
  let done = false;
  let failed = false;
  let value;
  let error;
  Promise.resolve(coro).then(
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
  blocking_wait(() => done, null);
  if (failed) throw error;
  return value;
}

/**
 * Async flavour of ``with_fresh_policy`` (used by the ``test_06`` ..
 * ``test_10`` ports): *fn(policy)* runs on a macrotask (so it may block) and
 * an async body is awaited; teardown runs on another macrotask so the
 * blocking ``shutdown(wait=True)`` is legal after an ``await``.
 * @template T
 * @param {(policy: any) => T|Promise<T>} fn
 * @returns {Promise<T>}
 */
export async function with_fresh_policy_async(fn) {
  const { original, policy } = await macrotask(() => {
    const original = laila.get_active_policy();
    const policy = new _LAILA_IDENTIFIABLE_POLICY();
    laila.activate_policy(policy);
    return { original, policy };
  });
  try {
    return await macrotask(() => fn(policy));
  } finally {
    await macrotask(() => {
      try {
        policy.central.command.shutdown({ wait: true, cancel_pending: true });
      } catch {
        // contextlib.suppress(Exception)
      }
      laila.activate_policy(original);
    });
  }
}

/**
 * Async flavour of the ``run`` fixture: run a coroutine (a promise, or a
 * function returning one -- the task then starts it, like
 * ``asyncio.run(main())``) to completion on a private task.
 * @template T
 * @param {Promise<T>|(() => Promise<T>)} coro
 * @returns {Promise<T>}
 */
export async function run_async(coro) {
  const asyncio = await import(S + "_compat/asyncio.js");
  return await asyncio.run(coro);
}
