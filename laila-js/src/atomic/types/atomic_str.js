/**
 * Thread-safe mutable string.
 *
 * ``AtomicStr`` wraps a string with an ``RLock`` so set / append / clear /
 * get operations are race-free between threads. Useful for shared status text
 * (e.g. "what is the worker doing right now") where multiple producers update
 * a human-readable label and consumers read it without locking themselves.
 *
 * Strings are immutable, so every "mutation" is really a re-bind of the
 * underlying ``value`` field; the lock ensures readers always see a
 * consistent snapshot rather than a partially-updated intermediate.
 */
import { ConfigDict, Field, PrivateAttr, define_fields, define_private } from "../../_compat/pydantic.js";
import { RLock } from "../../_compat/threading.js";
import { str as py_str } from "../../_compat/pytypes.js";
import { repr } from "../../_compat/pyrepr.js";
import { _LAILA_LOCALLY_ATOMIC_OBJECT } from "../definitions/locally_atomic_object.js";

function _locked(self, fn) {
  const lock = self._lock;
  lock.acquire();
  try {
    return fn();
  } finally {
    lock.release();
  }
}

/** Thread-safe string with atomic set, append, clear, and get operations. */
export class AtomicStr extends _LAILA_LOCALLY_ATOMIC_OBJECT {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_fields(this, {
      value: ["str", Field({ default: "" })],
    });
    define_private(this, {
      _lock: PrivateAttr({ default_factory: () => new RLock() }),
    });
  }

  /** Replace the string with *new_value*. */
  set(new_value) {
    _locked(this, () => {
      this.value = py_str(new_value);
    });
  }

  /** Return the current string value. */
  get() {
    return _locked(this, () => this.value);
  }

  /** Append *suffix* and return the resulting string. */
  append(suffix) {
    return _locked(this, () => {
      this.value += py_str(suffix);
      return this.value;
    });
  }

  /** Reset the string to empty. */
  clear() {
    _locked(this, () => {
      this.value = "";
    });
  }

  /** Return the length of the string. */
  length() {
    return _locked(this, () => this.value.length);
  }

  /** Context manager that holds the string's lock. */
  static _Atomic = class _Atomic {
    /** Initialize with the parent string. */
    constructor(parent) {
      this._p = parent;
    }

    __enter__() {
      this._p._lock.acquire();
      return this._p;
    }

    __exit__(_exc_type, _exc, _tb) {
      this._p._lock.release();
      return false;
    }

    enter() {
      return this.__enter__();
    }

    exit(...a) {
      return this.__exit__(...a);
    }

    [Symbol.dispose]() {
      this.__exit__(null, null, null);
    }
  };

  /** Return a context manager for batched lock-held operations. */
  atomic() {
    return new AtomicStr._Atomic(this);
  }

  /** Return the string value. */
  __str__() {
    return this.get();
  }

  toString() {
    return this.__str__();
  }

  /** Return a string representation. */
  __repr__() {
    return `AtomicStr(${repr(this.get())})`;
  }

  [Symbol.for("nodejs.util.inspect.custom")]() {
    return this.__repr__();
  }
}
