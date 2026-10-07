/**
 * Decorator that acquires atomic locks on identifiable arguments before
 * calling a method.
 *
 * Goes well beyond a "lock self" decorator: ``synchronized`` inspects ``self``
 * *and* every positional and keyword argument; for each that is an
 * identifiable atomic object it enters the corresponding ``obj.atomic()``
 * block before calling the wrapped method, releasing the locks again on exit
 * (or on exception).
 *
 * Lock-acquisition order is the *id-sorted* order of the involved objects, so
 * two callers who happen to want overlapping subsets of the same locks always
 * acquire them in the same order. That trick is what keeps ``synchronized``
 * deadlock-free even when several methods operate on the same set of objects
 * from different threads.
 */
import { ExitStack, with_ } from "../../_compat/contextlib.js";
import { NotImplementedError } from "../../_compat/errors.js";
import { id, is_plain_object, sorted } from "../../_compat/pytypes.js";
import { _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT } from "../../atomic/definitions/locally_atomic_identifiable_object.js";

/**
 * Wrap *method* so all identifiable arguments are locked before invocation.
 *
 * During a call, the wrapper:
 *
 * 1. Collects ``self`` and every positional/keyword argument that is an
 *    instance of ``_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT``. (Keyword
 *    arguments are the values of a trailing plain options object.)
 * 2. Sorts the collected objects by ``id()`` and enters each one's
 *    ``atomic({scope})`` context manager in that order. (Sorting is what
 *    avoids the classic A/B vs B/A deadlock.)
 * 3. Invokes the wrapped *method* with the original arguments.
 * 4. Releases the locks in reverse order on exit (handled by ``ExitStack``).
 *
 * Usage mirrors Python's ``@synchronized`` applied to a method body::
 *
 *   class X extends _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT {
 *     foo(other) { ... }
 *   }
 *   X.prototype.foo = synchronized(X.prototype.foo);
 *
 * @param {Function} method The method to wrap. Expected to be an instance
 *   method, so ``this`` is treated as ``self``.
 * @param {{scope?: string, kwargs?: boolean}} [opts] ``scope`` is forwarded to
 *   ``atomic``. Only ``"local"`` is currently implemented; ``"global"`` raises
 *   ``NotImplementedError``. ``kwargs`` (default ``true``) controls whether a
 *   trailing plain object is inspected as keyword arguments; pass ``false``
 *   for methods whose last positional parameter may itself be a dict value
 *   (e.g. a payload setter), which Python would not inspect.
 * @returns {Function} The wrapped method.
 */
export function synchronized(method, opts = {}) {
  const { scope = "local", kwargs = true } = opts;

  const wrapper = function (...args) {
    if (scope !== "local") throw new NotImplementedError("Global synchronization is not implemented.");

    const lock_cls = _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT;
    const last = args[args.length - 1];
    const kwargs_values = kwargs && is_plain_object(last) ? Object.values(last) : [];
    const candidates = [this, ...args, ...kwargs_values];
    const lock_targets = candidates.filter((obj) => obj instanceof lock_cls);

    if (lock_targets.length === 0) return method.apply(this, args);

    return with_(new ExitStack(), (stack) => {
      for (const obj of sorted(lock_targets, { key: id })) stack.enter_context(obj.atomic({ scope }));
      return method.apply(this, args);
    });
  };
  Object.defineProperty(wrapper, "name", { value: method.name });
  wrapper.__wrapped__ = method;
  return wrapper;
}
