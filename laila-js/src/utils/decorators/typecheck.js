/**
 * Decorators for lightweight runtime type coercion.
 *
 * Currently exports only ``ensure_list``, which removes the boilerplate at
 * the top of every "accept either a single item or an iterable" function.
 * The decorator inspects the wrapped function's signature, finds the named
 * parameter, and -- if its bound value is not a ``list`` / ``set`` /
 * ``frozenset`` -- wraps it in a single-element list before forwarding the
 * call.
 */
import { TypeError as PyTypeError } from "../../_compat/errors.js";
import { PyTuple, is_plain_object } from "../../_compat/pytypes.js";

/**
 * ``inspect.signature(fn).parameters`` for a JS function, recovered from its
 * source: ``[{name, default}]`` in declaration order. Destructured parameters
 * (the trailing keyword-options object) are reported with name ``"{}"`` and
 * rest parameters as ``"...name"``; ``default`` is the default expression's
 * source text or ``undefined``.
 * @param {Function} fn
 * @returns {{name: string, default: string|undefined}[]}
 */
export function _signature(fn) {
  return _split_parameters(fn).map((p) => {
    if (p.startsWith("{") || p.startsWith("[")) return { name: "{}", default: undefined };
    if (p.startsWith("...")) return { name: `...${p.slice(3).trim().split(/[\s=]/)[0]}`, default: undefined };
    const eq = p.indexOf("=");
    if (eq < 0) return { name: p.split(/\s/)[0], default: undefined };
    return { name: p.slice(0, eq).trim(), default: p.slice(eq + 1).trim() };
  });
}

/** Positional parameter names only (see ``_signature``). */
export function _parameter_names(fn) {
  return _signature(fn).map((p) => p.name);
}

function _split_parameters(fn) {
  let src = Function.prototype.toString.call(fn);
  // strip comments
  src = src.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/.*$/gm, "");
  let params;
  const arrow_single = src.match(/^\s*(?:async\s*)?([A-Za-z_$][\w$]*)\s*=>/);
  if (arrow_single) params = arrow_single[1];
  else {
    const start = src.indexOf("(");
    if (start < 0) return [];
    let depth = 0;
    let end = -1;
    for (let i = start; i < src.length; i++) {
      const ch = src[i];
      if (ch === "(" || ch === "[" || ch === "{") depth++;
      else if (ch === ")" || ch === "]" || ch === "}") {
        depth--;
        if (depth === 0) {
          end = i;
          break;
        }
      }
    }
    if (end < 0) return [];
    params = src.slice(start + 1, end);
  }
  const parts = [];
  let depth = 0;
  let cur = "";
  let quote = null;
  const flush = () => {
    const p = cur.trim();
    cur = "";
    if (p) parts.push(p);
  };
  for (let i = 0; i < params.length; i++) {
    const ch = params[i];
    if (quote) {
      cur += ch;
      if (ch === "\\") cur += params[++i] ?? "";
      else if (ch === quote) quote = null;
      continue;
    }
    if (ch === '"' || ch === "'" || ch === "`") quote = ch;
    else if (ch === "(" || ch === "[" || ch === "{") depth++;
    else if (ch === ")" || ch === "]" || ch === "}") depth--;
    if (ch === "," && depth === 0) flush();
    else cur += ch;
  }
  flush();
  return parts;
}

/** Evaluate a self-contained default expression (literals); ``undefined`` when it cannot be. */
function _eval_default(src) {
  if (src === undefined) return { ok: false };
  try {
    return { ok: true, value: new Function(`"use strict"; return (${src});`)() };
  } catch {
    return { ok: false };
  }
}

function _is_collection(value) {
  if (value instanceof Set) return true; // set / frozenset
  return Array.isArray(value) && !(value instanceof PyTuple); // list (a tuple is wrapped)
}

/**
 * Wrap a single value in a list when *arg_name* is not iterable.
 *
 * Useful for relaxing function signatures so callers can pass either
 * ``f(x)`` or ``f([x, y, z])`` without the function having to special-case
 * the scalar form.
 *
 * Sets and frozensets are *passed through unchanged* (they are already
 * iterable collections); everything else -- including plain strings, which
 * are iterable but rarely intended as a sequence in this context -- is
 * wrapped in a single-element list.
 *
 * Usage mirrors Python's ``@ensure_list("entries")``::
 *
 *   X.prototype.memorize = ensure_list("entries")(X.prototype.memorize);
 *
 * @param {string} arg_name The name of the parameter on the wrapped function
 *   whose value should be coerced. Must be a real parameter of the wrapped
 *   function (raises ``TypeError`` at call time if not).
 * @returns {(fn: Function) => Function} A decorator. Apply to the target function.
 */
export function ensure_list(arg_name) {
  return function decorator(fn) {
    const params = _signature(fn);
    const index = params.findIndex((p) => p.name === arg_name);

    const wrapper = function (...args) {
      const last = args.length ? args[args.length - 1] : undefined;
      const kw = (o) => is_plain_object(o) && Object.prototype.hasOwnProperty.call(o, arg_name);

      if (index >= 0 && index < args.length) {
        if (!_is_collection(args[index])) args[index] = [args[index]];
      } else if (index < 0 && kw(last)) {
        // keyword-only parameter passed in the trailing options object
        if (!_is_collection(last[arg_name])) args[args.length - 1] = { ...last, [arg_name]: [last[arg_name]] };
      } else if (index >= 0) {
        // omitted: ``bind_partial`` + ``apply_defaults`` binds the declared
        // default, which is then coerced like any other value
        const d = _eval_default(params[index].default);
        if (d.ok) {
          while (args.length < index) args.push(undefined);
          args[index] = _is_collection(d.value) ? d.value : [d.value];
        }
      } else {
        throw new PyTypeError(`Argument '${arg_name}' not found in ${fn.name}`);
      }
      return fn.apply(this, args);
    };
    Object.defineProperty(wrapper, "name", { value: fn.name });
    wrapper.__wrapped__ = fn;
    return wrapper;
  };
}
