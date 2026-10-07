/**
 * ``functools`` -- the subset laila uses.
 *
 * - ``partial(fn, ...args)``: a callable carrying ``func`` / ``args`` /
 *   ``keywords`` like ``functools.partial``. ``partial_kw(fn, kw, ...args)``
 *   is ``functools.partial(fn, *args, **kw)`` (trailing-options convention:
 *   ``kw`` is appended as the final argument when non-empty).
 * - ``wraps(wrapped)(wrapper)``: copies ``name`` / ``__qualname__`` /
 *   ``__doc__`` and records ``__wrapped__``.
 * - ``qualname(fn)``: Python's ``fn.__qualname__`` best-effort.
 */
import { iscoroutinefunction } from "./asyncio.js";

export function qualname(fn) {
  if (fn == null) return String(fn);
  if (fn.__qualname__) return fn.__qualname__;
  if (fn.func) return qualname(fn.func); // partial
  return fn.name || "<lambda>";
}

/** ``functools.wraps(wrapped)`` -> decorator applied to the wrapper. */
export function wraps(wrapped) {
  return (wrapper) => {
    Object.defineProperty(wrapper, "name", { value: wrapped.name, configurable: true });
    if (wrapped.__qualname__) wrapper.__qualname__ = wrapped.__qualname__;
    if (wrapped.__doc__) wrapper.__doc__ = wrapped.__doc__;
    wrapper.__wrapped__ = wrapped;
    return wrapper;
  };
}

function _make(fn, args, keywords) {
  const p = function (...more) {
    const all = [...args, ...more];
    if (Object.keys(keywords).length) all.push(keywords);
    return fn(...all);
  };
  p.func = fn;
  p.args = Object.freeze([...args]);
  p.keywords = Object.freeze({ ...keywords });
  p.__partial__ = true;
  p.__qualname__ = qualname(fn);
  // ``inspect.iscoroutinefunction(partial(async_fn))`` is True in CPython.
  if (iscoroutinefunction(fn)) p.__coroutinefunction__ = true;
  return p;
}

/** ``functools.partial(fn, *args)`` */
export function partial(fn, ...args) {
  if (fn && fn.__partial__) return _make(fn.func, [...fn.args, ...args], fn.keywords);
  return _make(fn, args, {});
}

/** ``functools.partial(fn, *args, **keywords)`` */
export function partial_kw(fn, keywords, ...args) {
  if (fn && fn.__partial__) return _make(fn.func, [...fn.args, ...args], { ...fn.keywords, ...keywords });
  return _make(fn, args, keywords ?? {});
}

export default { partial, partial_kw, wraps, qualname };
