/**
 * ``dotmap.DotMap`` (1.3.x) emulation.
 *
 * Attribute and item access share one ordered store. Reading a missing key
 * *autovivifies* a nested DotMap (``args.policy.central.memory`` just works),
 * ``.get(k)`` does not. Nested plain objects/Maps passed to the constructor
 * are converted recursively (also inside lists), exactly like dotmap.
 *
 * A Proxy returned from the constructor makes ``d.k`` / ``d[k]`` / ``d.k = v``
 * / ``k in d`` / ``delete d.k`` work literally. Subclasses may override
 * ``__getitem__`` / ``__setitem__`` / ``__setattr__`` (laila's ``_LailaArgs``
 * hooks ``environment`` that way).
 *
 * JS protocol names (``then``, ``toJSON``, ``constructor``, ...) and dunder
 * names are never autovivified, so DotMaps can be awaited, logged and
 * JSON-serialised safely.
 */
import { inspect } from "node:util";
import { KeyError, AttributeError } from "./errors.js";
import { repr } from "./pyrepr.js";
import { isdict, dict_items, is_plain_object, PyTuple } from "./pytypes.js";

const _INTERNAL = new Set(["_map", "_dynamic", "_prevent_method_masking", "_ipython_canary_method_should_not_exist_"]);
const _NO_AUTOVIVIFY = new Set([
  "then",
  "catch",
  "finally",
  "toJSON",
  "constructor",
  "prototype",
  "inspect",
  "nodeType",
  "asymmetricMatch",
  "$$typeof",
  "__proto__",
  "toString",
  "valueOf",
  "length",
  "size",
  "toDict",
  "_map",
]);

const _handler = {
  get(target, prop, receiver) {
    if (typeof prop === "symbol") return Reflect.get(target, prop, receiver);
    if (prop in target) return Reflect.get(target, prop, receiver);
    if (_NO_AUTOVIVIFY.has(prop)) return undefined;
    if (prop.startsWith("__") && prop.endsWith("__")) return undefined;
    return target.__getattr__.call(receiver, prop);
  },
  set(target, prop, value, receiver) {
    if (typeof prop === "symbol" || _INTERNAL.has(prop)) return Reflect.set(target, prop, value);
    target.__setattr__.call(receiver, prop, value);
    return true;
  },
  has(target, prop) {
    if (typeof prop === "symbol") return prop in target;
    return target._map.has(prop);
  },
  deleteProperty(target, prop) {
    if (typeof prop === "symbol" || _INTERNAL.has(prop)) return Reflect.deleteProperty(target, prop);
    if (!target._map.has(prop)) return true;
    target.__delitem__(prop);
    return true;
  },
  ownKeys(target) {
    return [...target._map.keys()].filter((k) => typeof k === "string");
  },
  getOwnPropertyDescriptor(target, prop) {
    if (typeof prop === "string" && target._map.has(prop))
      return { value: target._map.get(prop), writable: true, enumerable: true, configurable: true };
    return Reflect.getOwnPropertyDescriptor(target, prop);
  },
};

export class DotMap {
  /**
   * @param {object|Map|Array<[any,any]>|null} [init]
   * @param {{_dynamic?: boolean, _prevent_method_masking?: boolean}} [opts]
   */
  constructor(init = null, opts = {}) {
    this._map = new Map();
    this._dynamic = opts._dynamic !== false;
    this._prevent_method_masking = opts._prevent_method_masking === true;
    const proxy = new Proxy(this, _handler);
    if (init !== null && init !== undefined) {
      let src;
      if (init instanceof DotMap) src = [...init._map.entries()];
      else if (isdict(init) || typeof init.toDict === "function") src = dict_items(init);
      else src = [...init];
      for (const [k, v] of src) this._map.set(k, this._convert(v));
    }
    return proxy;
  }

  _convert(v) {
    const Ctor = this.constructor;
    if (v instanceof DotMap) return v;
    if (isdict(v)) return new Ctor(v);
    if (Array.isArray(v) && !(v instanceof PyTuple)) return v.map((x) => (isdict(x) ? new Ctor(x) : x));
    return v;
  }

  // -- Python protocol -----------------------------------------------------

  __getitem__(k) {
    if (!this._map.has(k) && this._dynamic && k !== "_ipython_canary_method_should_not_exist_") {
      this.__setitem__(k, new this.constructor());
    }
    if (!this._map.has(k)) throw new KeyError(k);
    return this._map.get(k);
  }
  __setitem__(k, v) {
    this._map.set(k, v);
  }
  __delitem__(k) {
    if (!this._map.has(k)) throw new KeyError(k);
    this._map.delete(k);
  }
  __getattr__(k) {
    try {
      return this.__getitem__(k);
    } catch (e) {
      if (e instanceof KeyError) throw new AttributeError(`'${this.constructor.name}' object has no attribute '${k}'`);
      throw e;
    }
  }
  __setattr__(k, v) {
    this.__setitem__(k, v);
  }
  __contains__(k) {
    return this._map.has(k);
  }
  __len__() {
    return this._map.size;
  }
  __bool__() {
    return this._map.size > 0;
  }
  __iter__() {
    return this._map.keys();
  }
  [Symbol.iterator]() {
    return this._map.keys();
  }
  __eq__(other) {
    if (other instanceof DotMap) other = other.toDict();
    if (!isdict(other)) return false;
    const mine = this.toDict();
    return _deep_eq(mine, other);
  }
  __repr__() {
    return this.__str__();
  }
  __str__() {
    const parts = [];
    for (const [k, v] of this._map) parts.push(`${k}=${v instanceof DotMap ? v.__str__() : repr(v)}`);
    return `${this.constructor.name}(${parts.join(", ")})`;
  }
  toString() {
    return this.__str__();
  }
  [inspect.custom]() {
    return this.__str__();
  }
  toJSON() {
    return this.toDict();
  }

  // -- dict API ------------------------------------------------------------

  get(key, dflt = null) {
    return this._map.has(key) ? this._map.get(key) : dflt;
  }
  setdefault(key, dflt = null) {
    if (!this._map.has(key)) this._map.set(key, dflt);
    return this._map.get(key);
  }
  has(key) {
    return this._map.has(key);
  }
  keys() {
    return [...this._map.keys()];
  }
  values() {
    return [...this._map.values()];
  }
  items() {
    return [...this._map.entries()];
  }
  update(other = null, kwargs = null) {
    if (other) for (const [k, v] of other instanceof DotMap ? other._map.entries() : dict_items(other)) this._map.set(k, v);
    if (kwargs) for (const [k, v] of Object.entries(kwargs)) this._map.set(k, v);
  }
  pop(key, dflt = null) {
    if (!this._map.has(key)) return dflt;
    const v = this._map.get(key);
    this._map.delete(key);
    return v;
  }
  popitem() {
    const last = [...this._map.entries()].pop();
    if (!last) throw new KeyError("popitem(): dictionary is empty");
    this._map.delete(last[0]);
    return PyTuple.from_iterable(last);
  }
  clear() {
    this._map.clear();
  }
  copy() {
    return new this.constructor(this);
  }
  get size() {
    return this._map.size;
  }
  isEmpty() {
    return this._map.size === 0;
  }
  empty() {
    return this._map.size === 0;
  }

  /**
   * Recursively convert to plain objects (``dotmap.toDict``). Keys are kept
   * as-is; a key set that would be reordered by a JS object becomes a Map.
   */
  toDict(seen = null) {
    seen = seen ?? new Map();
    const out = {};
    seen.set(this, out);
    const conv = (v) => {
      if (v instanceof DotMap) return seen.has(v) ? seen.get(v) : v.toDict(seen);
      if (Array.isArray(v) && !(v instanceof PyTuple)) return v.map(conv);
      if (v instanceof PyTuple) return PyTuple.from_iterable(v.map(conv));
      return v;
    };
    for (const [k, v] of this._map) out[k] = conv(v);
    return out;
  }

  /** dotmap ``pprint`` -> JSON-ish dump to stdout. */
  pprint() {
    process.stdout.write(inspect(this.toDict(), { depth: null }) + "\n");
  }

  /** Python ``DotMap.parseOther`` */
  static parseOther(other) {
    if (other instanceof DotMap) return other.toDict();
    return other;
  }

}


function _deep_eq(a, b) {
  if (a === b) return true;
  if (isdict(a) && isdict(b)) {
    const ia = dict_items(a);
    const ib = dict_items(b);
    if (ia.length !== ib.length) return false;
    const mb = new Map(ib);
    for (const [k, v] of ia) {
      if (!mb.has(k)) return false;
      if (!_deep_eq(v, mb.get(k))) return false;
    }
    return true;
  }
  if (Array.isArray(a) && Array.isArray(b)) return a.length === b.length && a.every((x, i) => _deep_eq(x, b[i]));
  if (a && typeof a.__eq__ === "function") return a.__eq__(b) === true;
  if (a instanceof DotMap) return a.__eq__(b);
  if (b instanceof DotMap) return b.__eq__(a);
  return false;
}

/** ``isinstance(x, DotMap)`` without relying on Symbol.hasInstance. */
export function is_dotmap(x) {
  return x instanceof DotMap;
}

export { is_plain_object };
