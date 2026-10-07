/**
 * Thread-safe dot-notation attribute map.
 *
 * ``AtomicDotMap`` is the threaded sibling of ``DotMap``. It exposes attribute
 * access (``m.a = 1``) over an underlying dict, but every read and write goes
 * through an ``RLock`` so concurrent producers and consumers never observe a
 * half-written tree.
 *
 * Used by laila for live, nested configuration that may be mutated at runtime
 * -- the prototypical example is ``laila.args``, which is read by every
 * CLI-capable class during construction and may be written by
 * environment-load workflows or by user code rebinding ``laila.args.foo = ...``.
 *
 * Attribute access is implemented with a Proxy: any name that is not a real
 * attribute of the object (methods, the internal slots) is a dynamic key;
 * reading a missing key yields ``null`` exactly like Python's ``__getattr__``.
 */
import { SKIP_VALIDATION } from "../../_compat/pydantic.js";
import { RLock } from "../../_compat/threading.js";
import { KeyError } from "../../_compat/errors.js";
import { repr } from "../../_compat/pyrepr.js";
import { tuple } from "../../_compat/pytypes.js";
import { _now_creation_timestamp } from "../../basics/definitions/laila_object.js";
import { _LAILA_LOCALLY_ATOMIC_OBJECT } from "../definitions/locally_atomic_object.js";

// Internal attributes stored directly on the instance rather than in the
// user-visible ``_data`` mapping. Module-level (not a class attribute) so
// Pydantic does not mistake the leading underscore for a private attr.
const _INTERNAL_ATTRS = new Set(["_data", "_lock", "_creation_timestamp"]);

const _PROTOCOL = new Set(["then", "catch", "finally", "toJSON", "constructor", "prototype", "inspect", "nodeType", "asymmetricMatch", "$$typeof", "__proto__", "toString", "valueOf", "toDict"]);

const _handler = {
  get(target, prop, receiver) {
    if (typeof prop === "symbol" || prop in target) return Reflect.get(target, prop, receiver);
    if (_PROTOCOL.has(prop) || (prop.startsWith("__") && prop.endsWith("__"))) return undefined;
    return target.__getattr__(prop);
  },
  set(target, prop, value, receiver) {
    if (typeof prop === "symbol") return Reflect.set(target, prop, value, receiver);
    target.__setattr__(prop, value);
    return true;
  },
  has(target, prop) {
    if (typeof prop === "symbol" || prop in target) return true;
    return Object.prototype.hasOwnProperty.call(target._data, prop);
  },
  deleteProperty(target, prop) {
    if (typeof prop === "symbol" || Object.prototype.hasOwnProperty.call(target, prop)) return Reflect.deleteProperty(target, prop);
    target.__delattr__(prop);
    return true;
  },
};

function _locked(self, fn) {
  const lock = self._lock;
  lock.acquire();
  try {
    return fn();
  } finally {
    lock.release();
  }
}

/**
 * Thread-safe mapping accessed via attribute syntax.
 *
 * All reads and writes to dynamic attributes are guarded by a reentrant lock,
 * making the map safe for concurrent use.
 */
export class AtomicDotMap extends _LAILA_LOCALLY_ATOMIC_OBJECT {
  /**
   * Initialize with an empty data dict and a reentrant lock.
   *
   * Deliberately does not chain to the Pydantic initialiser (the map has no
   * declared fields), so the ``_LAILA_OBJECT`` creation_timestamp is stamped
   * here explicitly.
   */
  constructor() {
    super(SKIP_VALIDATION);
    Object.defineProperty(this, "_data", { value: {}, writable: true, enumerable: true, configurable: true });
    Object.defineProperty(this, "_lock", { value: new RLock(), writable: true, enumerable: true, configurable: true });
    this._creation_timestamp = _now_creation_timestamp();
    return new Proxy(this, _handler);
  }

  /** Return the value for *key*, or ``null`` if absent. */
  __getattr__(key) {
    return _locked(this, () => (Object.prototype.hasOwnProperty.call(this._data, key) ? this._data[key] : null));
  }

  /** Set *key* to *value*, bypassing the lock for internal attrs. */
  __setattr__(key, value) {
    if (_INTERNAL_ATTRS.has(key)) {
      if (key === "_creation_timestamp") this._creation_timestamp = value;
      else Object.defineProperty(this, key, { value, writable: true, enumerable: true, configurable: true });
    } else {
      _locked(this, () => {
        this._data[key] = value;
      });
    }
  }

  /** Delete the attribute *key*. */
  __delattr__(key) {
    _locked(this, () => {
      if (!Object.prototype.hasOwnProperty.call(this._data, key)) throw new KeyError(key);
      delete this._data[key];
    });
  }

  /** Return a shallow copy of the internal data as a plain dict. */
  to_dict() {
    return _locked(this, () => ({ ...this._data }));
  }

  /** Return a snapshot list of keys. */
  keys() {
    return _locked(this, () => Object.keys(this._data));
  }

  /** Return a snapshot list of values. */
  values() {
    return _locked(this, () => Object.values(this._data));
  }

  /** Return a snapshot list of ``(key, value)`` pairs. */
  items() {
    return _locked(this, () => Object.entries(this._data).map(([k, v]) => tuple([k, v])));
  }

  /** Return a string representation of the map. */
  __repr__() {
    return _locked(this, () => `AtomicDotMap(${repr(this._data)})`);
  }

  toString() {
    return this.__repr__();
  }

  [Symbol.for("nodejs.util.inspect.custom")]() {
    return this.__repr__();
  }
}
