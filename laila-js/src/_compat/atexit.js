/**
 * ``atexit`` -- run cleanup callbacks when the process exits.
 *
 * Callbacks run in LIFO order (last registered, first run) on the ``exit``
 * event, each isolated from the others' failures, exactly like CPython's
 * ``atexit`` module. ``unregister`` removes every registration of *fn*.
 *
 * Node's ``exit`` handlers must be synchronous; laila's blocking primitives
 * (``Popen.wait``, ``block_on``) drive the native loop pump and therefore
 * remain usable here for short waits (process teardown of managed servers).
 */
const _callbacks = [];
let _hooked = false;

function _run_exitfuncs() {
  while (_callbacks.length) {
    const [fn, args] = _callbacks.pop();
    try {
      fn(...args);
    } catch (e) {
      try {
        process.stderr.write(`Error in atexit._run_exitfuncs:\n${e && e.stack ? e.stack : e}\n`);
      } catch {
        // ignore
      }
    }
  }
}

/**
 * @template {Function} F
 * @param {F} fn
 * @param  {...any} args
 * @returns {F}
 */
export function register(fn, ...args) {
  if (!_hooked) {
    _hooked = true;
    process.on("exit", _run_exitfuncs);
  }
  _callbacks.push([fn, args]);
  return fn;
}

/** Remove every registration of *fn*. */
export function unregister(fn) {
  for (let i = _callbacks.length - 1; i >= 0; i -= 1) {
    if (_callbacks[i][0] === fn) _callbacks.splice(i, 1);
  }
}

/** Run all registered callbacks now (``atexit._run_exitfuncs``). */
export { _run_exitfuncs };

export default { register, unregister, _run_exitfuncs };
