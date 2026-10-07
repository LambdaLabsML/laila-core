/**
 * Python value model on top of JS values.
 *
 * Mapping (rules D1/N1 of the port plan):
 *   None        <-> null            (``undefined`` means "argument omitted")
 *   bool        <-> boolean
 *   int         <-> integral number, or BigInt beyond +/-2**53
 *   float       <-> number; a Python float with an *integral* value decoded
 *                   from Python becomes a ``PyFloat`` so it round-trips as float
 *   str         <-> string
 *   bytes       <-> Uint8Array / Buffer
 *   bytearray   <-> PyByteArray
 *   list        <-> Array
 *   tuple       <-> PyTuple (frozen Array subclass)
 *   dict        <-> plain Object when every key is a string and none is an
 *                   array-index-like string (JS would reorder those); Map otherwise
 *   set         <-> Set;  frozenset <-> PyFrozenSet
 *
 * Plus the Python protocol helpers laila's code relies on: ``bool()``,
 * ``len()``, ``==`` (deep, ``__eq__``-aware), ``str()``, ``isinstance``-style
 * predicates and dict access that raises ``KeyError``.
 */
import { KeyError, TypeError as PyTypeError, IndexError } from "./errors.js";

// --------------------------------------------------------------------------
// Boxed / tagged Python types
// --------------------------------------------------------------------------

/** Python ``tuple``: a frozen Array subclass (``Array.isArray`` is true). */
export class PyTuple extends Array {
  /** @param {Iterable<any>} [iterable] */
  static from_iterable(iterable) {
    const t = new PyTuple();
    if (iterable != null) for (const x of iterable) Array.prototype.push.call(t, x);
    return Object.freeze(t);
  }
  // Array methods such as ``map`` build results with ``new this.constructor``;
  // return plain arrays for derived values (as Python does).
  static get [Symbol.species]() {
    return Array;
  }
  __repr__() {
    return this.length === 1 ? `(${repr_(this[0])},)` : `(${this.map(repr_).join(", ")})`;
  }
}

/** Python ``tuple(iterable)`` */
export function tuple(iterable) {
  return PyTuple.from_iterable(iterable);
}

/**
 * Python ``float`` whose value is integral (``1.0``). Plain JS numbers that are
 * integral serialise as Python ``int``; this box preserves the float type for
 * values decoded from Python so a round trip does not change their type.
 * Arithmetic works through ``valueOf``.
 */
export class PyFloat extends Number {
  __repr__() {
    return float_repr(this.valueOf());
  }
}

/** Python ``bytearray``. */
export class PyByteArray extends Uint8Array {}

/** Python ``frozenset``. */
export class PyFrozenSet extends Set {
  constructor(iterable) {
    super();
    // Set's constructor routes through the (overridden) ``add``; fill via the
    // base implementation instead, then seal.
    if (iterable !== null && iterable !== undefined) for (const x of iterable) Set.prototype.add.call(this, x);
    Object.freeze(this);
  }
  add() {
    throw new PyTypeError("'PyFrozenSet' object does not support item assignment");
  }
  delete() {
    throw new PyTypeError("'PyFrozenSet' object does not support item deletion");
  }
  clear() {
    throw new PyTypeError("'PyFrozenSet' object does not support clear");
  }
}

/** Python's ``NotImplemented`` singleton (returned by rich comparisons). */
export const NotImplemented = Object.freeze({ __NotImplemented__: true, toString: () => "NotImplemented" });

// --------------------------------------------------------------------------
// Type predicates
// --------------------------------------------------------------------------

export function is_none(x) {
  return x === null || x === undefined;
}
export function is_bool(x) {
  return typeof x === "boolean";
}
/**
 * Rule N1: a primitive JS number stands for a Python ``int`` when it is
 * integral, is not ``-0`` (Python ints have no negative zero) and its
 * magnitude is at most 2**64 (larger ints must be BigInt; a larger integral
 * double such as ``1e300`` can only be a float).
 */
export function is_integral(x) {
  return typeof x === "number" && Number.isInteger(x) && !Object.is(x, -0) && Math.abs(x) <= 18446744073709551616;
}
/** Python ``isinstance(x, int)`` (bool excluded, as callers usually intend). */
export function is_int(x) {
  return (is_integral(x) && !(x instanceof PyFloat)) || typeof x === "bigint";
}
/** Python ``isinstance(x, float)``. */
export function is_float(x) {
  return x instanceof PyFloat || (typeof x === "number" && !is_integral(x));
}
/** Python ``isinstance(x, (int, float))`` and not bool. */
export function is_number(x) {
  return typeof x === "number" || typeof x === "bigint" || x instanceof PyFloat;
}
export function is_str(x) {
  return typeof x === "string";
}
export function is_bytes(x) {
  return x instanceof Uint8Array && !(x instanceof PyByteArray);
}
export function is_bytearray(x) {
  return x instanceof PyByteArray;
}
export function is_byteslike(x) {
  return x instanceof Uint8Array;
}
export function is_tuple(x) {
  return x instanceof PyTuple;
}
export function is_list(x) {
  return Array.isArray(x) && !(x instanceof PyTuple);
}
export function is_set(x) {
  return x instanceof Set;
}
/**
 * Python ``isinstance(x, dict)``: a plain object (prototype ``Object.prototype``
 * or ``null``) or a ``Map``. ``DotMap`` *is* a dict subclass in Python but is
 * deliberately excluded here (it has its own mapping protocol); callers that
 * must treat it as a dict combine this with ``is_dotmap``.
 */
export function isdict(x) {
  if (x instanceof Map) return true;
  if (x === null || typeof x !== "object") return false;
  const p = Object.getPrototypeOf(x);
  return p === Object.prototype || p === null;
}
export function is_plain_object(x) {
  if (x === null || typeof x !== "object") return false;
  const p = Object.getPrototypeOf(x);
  return p === Object.prototype || p === null;
}
export function is_callable(x) {
  return typeof x === "function";
}

/** Python ``type(x).__name__`` for error messages. */
export function type_name(x) {
  if (x === null || x === undefined) return "NoneType";
  if (typeof x === "boolean") return "bool";
  if (typeof x === "string") return "str";
  if (typeof x === "bigint") return "int";
  if (typeof x === "number") return is_integral(x) ? "int" : "float";
  if (x instanceof PyFloat) return "float";
  if (x instanceof PyTuple) return "tuple";
  if (Array.isArray(x)) return "list";
  if (x instanceof PyByteArray) return "bytearray";
  if (x instanceof Uint8Array) return "bytes";
  if (x instanceof PyFrozenSet) return "frozenset";
  if (x instanceof Set) return "set";
  if (x instanceof Map || is_plain_object(x)) return "dict";
  if (typeof x === "function") return x.name ? "type" : "function";
  return x.constructor?.name ?? "object";
}

// --------------------------------------------------------------------------
// dict protocol over Object | Map
// --------------------------------------------------------------------------

/** Array-index-like key (``"0"``, ``"42"``) -- JS objects hoist and reorder these. */
export function is_index_like_key(k) {
  return typeof k === "string" && /^(0|[1-9][0-9]*)$/.test(k) && Number(k) < 4294967295;
}

/**
 * Build the JS representation of a Python dict from ``[key, value]`` entries,
 * applying rule D1: plain Object when all keys are safe strings, else Map.
 */
export function dict_from_entries(entries) {
  let needs_map = false;
  for (const [k] of entries) {
    if (typeof k !== "string" || is_index_like_key(k)) {
      needs_map = true;
      break;
    }
  }
  if (needs_map) return new Map(entries);
  const o = {};
  for (const [k, v] of entries) o[k] = v;
  return o;
}

/** Python ``dict()`` (empty). */
export function dict(init) {
  if (init === undefined) return {};
  if (init instanceof Map) return dict_from_entries([...init.entries()]);
  if (Array.isArray(init)) return dict_from_entries(init);
  return { ...init };
}

export function dict_has(d, k) {
  if (d instanceof Map) return d.has(k);
  if (typeof k !== "string" && typeof k !== "symbol") k = String(k);
  return Object.prototype.hasOwnProperty.call(d, k);
}
export function dict_get(d, k, dflt = null) {
  if (d instanceof Map) return d.has(k) ? d.get(k) : dflt;
  if (typeof k !== "string" && typeof k !== "symbol") k = String(k);
  return Object.prototype.hasOwnProperty.call(d, k) ? d[k] : dflt;
}
/** ``d[k]`` raising ``KeyError`` when missing. */
export function getitem(d, k) {
  if (d instanceof Map) {
    if (!d.has(k)) throw new KeyError(k);
    return d.get(k);
  }
  if (Array.isArray(d) || typeof d === "string") {
    let i = Number(k);
    if (i < 0) i += d.length;
    if (!Number.isInteger(i) || i < 0 || i >= d.length)
      throw new IndexError(`${Array.isArray(d) ? "list" : "string"} index out of range`);
    return d[i];
  }
  if (d && typeof d.__getitem__ === "function") return d.__getitem__(k);
  const key = typeof k !== "string" && typeof k !== "symbol" ? String(k) : k;
  if (!Object.prototype.hasOwnProperty.call(d, key)) throw new KeyError(k);
  return d[key];
}
export function dict_set(d, k, v) {
  if (d instanceof Map) d.set(k, v);
  else if (d && typeof d.__setitem__ === "function") d.__setitem__(k, v);
  else d[typeof k === "string" || typeof k === "symbol" ? k : String(k)] = v;
  return d;
}
/** ``del d[k]`` raising ``KeyError`` when missing. */
export function dict_del(d, k) {
  if (d instanceof Map) {
    if (!d.delete(k)) throw new KeyError(k);
    return;
  }
  if (d && typeof d.__delitem__ === "function") return d.__delitem__(k);
  const key = typeof k === "string" || typeof k === "symbol" ? k : String(k);
  if (!Object.prototype.hasOwnProperty.call(d, key)) throw new KeyError(k);
  delete d[key];
}
const _MISSING = Symbol("missing");
/** ``d.pop(k[, default])`` */
export function dict_pop(d, k, dflt = _MISSING) {
  if (dict_has(d, k)) {
    const v = dict_get(d, k);
    dict_del(d, k);
    return v;
  }
  if (dflt === _MISSING) throw new KeyError(k);
  return dflt;
}
export function dict_setdefault(d, k, dflt = null) {
  if (!dict_has(d, k)) dict_set(d, k, dflt);
  return dict_get(d, k);
}
export function dict_keys(d) {
  if (d instanceof Map) return [...d.keys()];
  if (d && typeof d.keys === "function" && !is_plain_object(d)) return [...d.keys()];
  return Object.keys(d);
}
export function dict_values(d) {
  if (d instanceof Map) return [...d.values()];
  if (d && typeof d.values === "function" && !is_plain_object(d)) return [...d.values()];
  return Object.values(d);
}
/** @returns {Array<[any, any]>} */
export function dict_items(d) {
  if (d instanceof Map) return [...d.entries()];
  if (d && typeof d.items === "function" && !is_plain_object(d)) return [...d.items()];
  return Object.entries(d);
}
export function dict_len(d) {
  if (d instanceof Map) return d.size;
  return Object.keys(d).length;
}
export function dict_update(d, other) {
  for (const [k, v] of dict_items(other)) dict_set(d, k, v);
  return d;
}
export function dict_clear(d) {
  if (d instanceof Map) d.clear();
  else for (const k of Object.keys(d)) delete d[k];
}
export function dict_copy(d) {
  if (d instanceof Map) return new Map(d);
  return { ...d };
}

// --------------------------------------------------------------------------
// Protocols: bool(), len(), ==, str(), hash keys
// --------------------------------------------------------------------------

/** Python truthiness. */
export function bool(x) {
  if (x === null || x === undefined || x === false) return false;
  if (x === true) return true;
  if (typeof x === "number") return x !== 0 && !Number.isNaN(x);
  if (typeof x === "bigint") return x !== 0n;
  if (typeof x === "string") return x.length > 0;
  if (typeof x === "object") {
    if (typeof x.__bool__ === "function") return x.__bool__();
    if (typeof x.__len__ === "function") return x.__len__() > 0;
    if (x instanceof PyFloat || x instanceof Number) return x.valueOf() !== 0;
    if (Array.isArray(x)) return x.length > 0;
    if (x instanceof Map || x instanceof Set) return x.size > 0;
    if (x instanceof Uint8Array) return x.length > 0;
    if (is_plain_object(x)) return Object.keys(x).length > 0;
  }
  return true;
}

/** Python ``len()``. */
export function len(x) {
  if (typeof x === "string" || Array.isArray(x) || x instanceof Uint8Array) return x.length;
  if (x instanceof Map || x instanceof Set) return x.size;
  if (x && typeof x.__len__ === "function") return x.__len__();
  if (is_plain_object(x)) return Object.keys(x).length;
  throw new PyTypeError(`object of type '${type_name(x)}' has no len()`);
}

/**
 * Python ``==``: deep structural equality for containers, ``__eq__`` for
 * objects that define it, numeric equality across int/float/BigInt/PyFloat.
 */
export function eq(a, b) {
  if (a === b) return true;
  if (a === null || a === undefined) return b === null || b === undefined;
  if (b === null || b === undefined) return false;
  const ta = typeof a;
  const tb = typeof b;
  // numbers (incl. boxed PyFloat / BigInt). bool is not equal to numbers here
  // except the Python rule True == 1 -- kept, since both are "numbers" in Python.
  const na = _numeric(a);
  const nb = _numeric(b);
  if (na !== undefined || nb !== undefined) {
    if (na === undefined || nb === undefined) return false;
    if (typeof na === "bigint" || typeof nb === "bigint") {
      try {
        return BigInt(na) === BigInt(nb);
      } catch {
        return Number(na) === Number(nb);
      }
    }
    return na === nb;
  }
  if (ta === "object" && typeof a.__eq__ === "function") {
    const r = a.__eq__(b);
    if (r !== NotImplemented) return r;
  }
  if (tb === "object" && typeof b.__eq__ === "function") {
    const r = b.__eq__(a);
    if (r !== NotImplemented) return r;
  }
  if (ta === "string" || tb === "string") return a === b;
  if (ta !== "object" || tb !== "object") return false;
  if (Array.isArray(a) && Array.isArray(b)) {
    if ((a instanceof PyTuple) !== (b instanceof PyTuple)) return false;
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) if (!eq(a[i], b[i])) return false;
    return true;
  }
  if (a instanceof Uint8Array && b instanceof Uint8Array) {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
    return true;
  }
  if (a instanceof Set && b instanceof Set) {
    if (a.size !== b.size) return false;
    for (const x of a) if (!_set_has(b, x)) return false;
    return true;
  }
  if (a instanceof Date && b instanceof Date) return a.getTime() === b.getTime();
  const da = isdict(a) || typeof a.toDict === "function";
  const db = isdict(b) || typeof b.toDict === "function";
  if (da && db) {
    const ia = dict_items(typeof a.toDict === "function" && !isdict(a) ? a : a);
    const ib = dict_items(typeof b.toDict === "function" && !isdict(b) ? b : b);
    if (ia.length !== ib.length) return false;
    for (const [k, v] of ia) {
      let found = false;
      for (const [k2, v2] of ib) {
        if (eq(k, k2)) {
          if (!eq(v, v2)) return false;
          found = true;
          break;
        }
      }
      if (!found) return false;
    }
    return true;
  }
  return false;
}
function _numeric(x) {
  if (typeof x === "number" || typeof x === "bigint") return x;
  if (typeof x === "boolean") return x ? 1 : 0;
  if (x instanceof PyFloat || x instanceof Number) return x.valueOf();
  return undefined;
}
function _set_has(s, x) {
  if (s.has(x)) return true;
  for (const y of s) if (eq(x, y)) return true;
  return false;
}
export function ne(a, b) {
  return !eq(a, b);
}

/** Python ``str(x)``. */
export function str(x) {
  if (x === null || x === undefined) return "None";
  if (x === true) return "True";
  if (x === false) return "False";
  if (typeof x === "string") return x;
  if (typeof x === "number") return is_integral(x) ? String(x) : float_repr(x);
  if (x instanceof PyFloat) return float_repr(x.valueOf());
  if (typeof x === "bigint") return x.toString();
  if (typeof x === "object" && typeof x.__str__ === "function") return x.__str__();
  if (typeof x === "object" && typeof x.__repr__ === "function") return x.__repr__();
  if (Array.isArray(x) || x instanceof Map || x instanceof Set || x instanceof Uint8Array || is_plain_object(x))
    return repr_(x);
  return String(x);
}

/** Python ``float.__repr__`` (shortest round-trip, Python formatting rules). */
export function float_repr(x) {
  if (Number.isNaN(x)) return "nan";
  if (x === Infinity) return "inf";
  if (x === -Infinity) return "-inf";
  if (x === 0) return Object.is(x, -0) ? "-0.0" : "0.0";
  const exp_str = x.toExponential(); // shortest unique digits: "d.ddde+X"
  const m = /^(-?)(\d)(?:\.(\d+))?e([+-]\d+)$/.exec(exp_str);
  const sign = m[1];
  const digits = m[2] + (m[3] ?? "");
  const exp = parseInt(m[4], 10);
  if (exp >= -4 && exp < 16) {
    let s;
    if (exp >= 0) {
      if (digits.length <= exp + 1) s = digits + "0".repeat(exp + 1 - digits.length) + ".0";
      else s = digits.slice(0, exp + 1) + "." + digits.slice(exp + 1);
    } else {
      s = "0." + "0".repeat(-exp - 1) + digits;
    }
    return sign + s;
  }
  const mant = digits.length === 1 ? digits : digits[0] + "." + digits.slice(1);
  const e = Math.abs(exp) < 10 ? "0" + Math.abs(exp) : String(Math.abs(exp));
  return `${sign}${mant}e${exp < 0 ? "-" : "+"}${e}`;
}

// repr is implemented in pyrepr.js (ESM live binding; only used at call time,
// so the import cycle pytypes <-> pyrepr is harmless).
import { repr as repr_ } from "./pyrepr.js";

// --------------------------------------------------------------------------
// Misc Python builtins
// --------------------------------------------------------------------------

/** Python ``sorted(iterable, key=..., reverse=...)`` (stable). */
export function sorted(iterable, { key = null, reverse = false } = {}) {
  const arr = [...iterable];
  const keyed = arr.map((v, i) => [key ? key(v) : v, i, v]);
  keyed.sort((a, b) => {
    const c = compare(a[0], b[0]);
    return c !== 0 ? c : a[1] - b[1];
  });
  if (reverse) {
    // Python's reverse keeps stability (equal keys keep original order).
    const groups = [];
    for (const k of keyed) {
      const last = groups[groups.length - 1];
      if (last && compare(last[0][0], k[0]) === 0) last.push(k);
      else groups.push([k]);
    }
    groups.reverse();
    return groups.flat().map((k) => k[2]);
  }
  return keyed.map((k) => k[2]);
}

/** Python ordering for the comparable builtins (numbers, strings, tuples/lists). */
export function compare(a, b) {
  const na = _numeric(a);
  const nb = _numeric(b);
  if (na !== undefined && nb !== undefined) return na < nb ? -1 : na > nb ? 1 : 0;
  if (typeof a === "string" && typeof b === "string") return a < b ? -1 : a > b ? 1 : 0;
  if (Array.isArray(a) && Array.isArray(b)) {
    const n = Math.min(a.length, b.length);
    for (let i = 0; i < n; i++) {
      const c = compare(a[i], b[i]);
      if (c !== 0) return c;
    }
    return a.length - b.length;
  }
  if (a && typeof a.__lt__ === "function") return a.__lt__(b) ? -1 : b.__lt__?.(a) ? 1 : 0;
  throw new PyTypeError(
    `'<' not supported between instances of '${type_name(a)}' and '${type_name(b)}'`,
  );
}

/** Python ``int(x)`` acceptance rules for str/float/bool. */
export function int(x) {
  if (typeof x === "boolean") return x ? 1 : 0;
  if (typeof x === "bigint") return x;
  if (typeof x === "number" || x instanceof PyFloat) {
    const v = x.valueOf();
    if (!Number.isFinite(v)) throw new (Number.isNaN(v) ? ValueErrorCls() : OverflowErrorCls())(
      Number.isNaN(v) ? "cannot convert float NaN to integer" : "cannot convert float infinity to integer",
    );
    return Math.trunc(v);
  }
  if (typeof x === "string") {
    const s = x.trim().replace(/_/g, "");
    if (!/^[+-]?\d+$/.test(s)) throw new (ValueErrorCls())(`invalid literal for int() with base 10: ${repr_(x)}`);
    const n = Number(s);
    return Number.isSafeInteger(n) ? n : BigInt(s);
  }
  if (x && typeof x.__int__ === "function") return x.__int__();
  throw new PyTypeError(`int() argument must be a string, a bytes-like object or a real number, not '${type_name(x)}'`);
}

/** Python ``float(x)`` acceptance rules. */
export function float(x) {
  if (typeof x === "boolean") return x ? 1 : 0;
  if (typeof x === "number") return x;
  if (typeof x === "bigint") return Number(x);
  if (x instanceof PyFloat) return x.valueOf();
  if (typeof x === "string") {
    const s = x.trim().replace(/_/g, "");
    const l = s.toLowerCase();
    if (l === "inf" || l === "+inf" || l === "infinity" || l === "+infinity") return Infinity;
    if (l === "-inf" || l === "-infinity") return -Infinity;
    if (l === "nan" || l === "+nan" || l === "-nan") return NaN;
    if (!/^[+-]?(\d+\.?\d*(e[+-]?\d+)?|\.\d+(e[+-]?\d+)?)$/i.test(s))
      throw new (ValueErrorCls())(`could not convert string to float: ${repr_(x)}`);
    return Number(s);
  }
  if (x && typeof x.__float__ === "function") return x.__float__();
  throw new PyTypeError(`float() argument must be a string or a real number, not '${type_name(x)}'`);
}

// Late-bound error classes to avoid the errors.js <-> pytypes.js import order
// mattering for ValueError/OverflowError (both modules import each other).
import * as _errors from "./errors.js";
function ValueErrorCls() {
  return _errors.ValueError;
}
function OverflowErrorCls() {
  return _errors.OverflowError;
}

/** Python ``range`` as an array. */
export function range(a, b, step = 1) {
  const [start, stop] = b === undefined ? [0, a] : [a, b];
  const out = [];
  if (step > 0) for (let i = start; i < stop; i += step) out.push(i);
  else for (let i = start; i > stop; i += step) out.push(i);
  return out;
}

/** Python ``zip`` (shortest). */
export function zip(...its) {
  const arrs = its.map((i) => [...i]);
  const n = Math.min(...arrs.map((a) => a.length));
  const out = [];
  for (let i = 0; i < n; i++) out.push(arrs.map((a) => a[i]));
  return out;
}

/** Python ``enumerate``. */
export function enumerate(it, start = 0) {
  return [...it].map((v, i) => [i + start, v]);
}

/** Python ``isinstance(x, cls_or_tuple)`` with support for class arrays. */
export function isinstance(x, cls) {
  if (Array.isArray(cls)) return cls.some((c) => isinstance(x, c));
  if (typeof cls !== "function") return false;
  try {
    return x instanceof cls;
  } catch {
    return false;
  }
}

/** Python ``hasattr`` (own or inherited, including accessor properties). */
export function hasattr(obj, name) {
  if (obj === null || obj === undefined) return false;
  if (typeof obj !== "object" && typeof obj !== "function") return name in Object(obj);
  return name in obj;
}

/** Python ``getattr(obj, name, default)``. */
export function getattr(obj, name, dflt) {
  if (obj === null || obj === undefined) {
    if (dflt !== undefined) return dflt;
    throw new _errors.AttributeError(`'NoneType' object has no attribute '${name}'`);
  }
  if (name in Object(obj)) {
    const v = obj[name];
    return typeof v === "function" && !(v.prototype && v.prototype.constructor === v) ? v.bind(obj) : v;
  }
  if (dflt !== undefined) return dflt;
  throw new _errors.AttributeError(`'${type_name(obj)}' object has no attribute '${name}'`);
}

/** Python ``callable``. */
export const callable = is_callable;

// --------------------------------------------------------------------------
// id()
// --------------------------------------------------------------------------

const _ids = new WeakMap();
let _next_id = 1;

/**
 * Python ``id(obj)``: a process-unique integer per object identity (stable
 * for the object's lifetime). Primitives hash by value.
 */
export function id(obj) {
  if (obj === null || (typeof obj !== "object" && typeof obj !== "function")) {
    const s = typeof obj + ":" + String(obj);
    let h = 0;
    for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
    return h;
  }
  let v = _ids.get(obj);
  if (v === undefined) {
    v = _next_id++;
    _ids.set(obj, v);
  }
  return v;
}
