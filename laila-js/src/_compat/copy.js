/**
 * Python ``copy`` module: ``copy.copy`` and ``copy.deepcopy`` with ``__copy__``
 * / ``__deepcopy__`` hooks and a memo so shared references stay shared.
 */
import { PyTuple, PyFloat, PyByteArray, PyFrozenSet, is_plain_object } from "./pytypes.js";

/** ``copy.copy(x)`` -- shallow. */
export function copy(x) {
  if (x === null || typeof x !== "object") return x;
  if (typeof x.__copy__ === "function") return x.__copy__();
  if (x instanceof PyTuple) return x; // immutable
  if (Array.isArray(x)) return x.slice();
  if (x instanceof Map) return new Map(x);
  if (x instanceof PyFrozenSet) return x;
  if (x instanceof Set) return new Set(x);
  if (x instanceof Uint8Array) return x.slice();
  if (x instanceof Date) return new Date(x.getTime());
  if (is_plain_object(x)) return { ...x };
  if (x instanceof PyFloat || x instanceof Number || x instanceof String) return x;
  if (typeof x.toDict === "function" && typeof x.copy === "function") return x.copy();
  // generic object: same prototype, own props copied (incl. non-enumerable)
  const out = Object.create(Object.getPrototypeOf(x));
  for (const k of Reflect.ownKeys(x)) Object.defineProperty(out, k, Object.getOwnPropertyDescriptor(x, k));
  return out;
}

/** ``copy.deepcopy(x, memo=None)`` */
export function deepcopy(x, memo = null) {
  memo = memo ?? new Map();
  if (x === null || (typeof x !== "object" && typeof x !== "function")) return x;
  if (typeof x === "function") return x;
  if (memo.has(x)) return memo.get(x);
  if (typeof x.__deepcopy__ === "function") {
    const r = x.__deepcopy__(memo);
    memo.set(x, r);
    return r;
  }
  if (x instanceof PyFloat || x instanceof Number || x instanceof String || x instanceof Boolean) return x;
  if (x instanceof Date) return new Date(x.getTime());
  if (x instanceof Uint8Array) {
    const r = x instanceof PyByteArray ? new PyByteArray(x) : x.slice();
    memo.set(x, r);
    return r;
  }
  if (x instanceof PyTuple) {
    const items = [];
    for (const v of x) items.push(deepcopy(v, memo));
    const r = PyTuple.from_iterable(items);
    memo.set(x, r);
    return r;
  }
  if (Array.isArray(x)) {
    const r = [];
    memo.set(x, r);
    for (const v of x) r.push(deepcopy(v, memo));
    return r;
  }
  if (x instanceof Map) {
    const r = new Map();
    memo.set(x, r);
    for (const [k, v] of x) r.set(deepcopy(k, memo), deepcopy(v, memo));
    return r;
  }
  if (x instanceof PyFrozenSet) {
    const r = new PyFrozenSet([...x].map((v) => deepcopy(v, memo)));
    memo.set(x, r);
    return r;
  }
  if (x instanceof Set) {
    const r = new Set();
    memo.set(x, r);
    for (const v of x) r.add(deepcopy(v, memo));
    return r;
  }
  if (is_plain_object(x)) {
    const r = {};
    memo.set(x, r);
    for (const k of Object.keys(x)) r[k] = deepcopy(x[k], memo);
    return r;
  }
  if (x instanceof Error) return x;
  if (x instanceof Promise) return x;
  // DotMap and other mapping-likes with toDict/constructor
  if (typeof x.toDict === "function" && typeof x.items === "function") {
    const r = new x.constructor();
    memo.set(x, r);
    for (const [k, v] of x.items()) r.__setitem__(k, deepcopy(v, memo));
    return r;
  }
  // generic object: same prototype, own props deep-copied
  const out = Object.create(Object.getPrototypeOf(x));
  memo.set(x, out);
  for (const k of Reflect.ownKeys(x)) {
    const d = Object.getOwnPropertyDescriptor(x, k);
    if ("value" in d) d.value = deepcopy(d.value, memo);
    Object.defineProperty(out, k, d);
  }
  return out;
}
