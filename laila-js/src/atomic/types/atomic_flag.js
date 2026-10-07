/**
 * Thread-safe boolean flag.
 *
 * ``AtomicFlag`` is the simplest of the atomic types: a single boolean
 * guarded by an ``RLock`` so the usual ``set`` / ``clear`` / ``toggle`` /
 * ``is_set`` operations are safe to call from multiple threads. Use it for
 * "has this happened yet?" signals -- shutdown sentinels, init flags, one-shot
 * guards -- where a full ``threading.Event`` is overkill because no thread
 * ever needs to *wait* on the flag.
 *
 * For coordinated wait-for-state semantics, prefer ``threading.Event``. For
 * atomically-incrementable counters, see ``AtomicInt``.
 */
import { ConfigDict, Field, PrivateAttr, define_fields, define_private } from "../../_compat/pydantic.js";
import { RLock } from "../../_compat/threading.js";
import { bool as py_bool } from "../../_compat/pytypes.js";
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

/** Thread-safe boolean flag with atomic set/clear/toggle/get operations. */
export class AtomicFlag extends _LAILA_LOCALLY_ATOMIC_OBJECT {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_fields(this, {
      value: ["bool", Field({ default: false })],
    });
    define_private(this, {
      _lock: PrivateAttr({ default_factory: () => new RLock() }),
    });
  }

  /** Set the flag to ``true``. */
  set() {
    _locked(this, () => {
      this.value = true;
    });
  }

  /** Set the flag to ``false``. */
  clear() {
    _locked(this, () => {
      this.value = false;
    });
  }

  /** Invert the flag value. */
  toggle() {
    _locked(this, () => {
      this.value = !this.value;
    });
  }

  /** Return the current flag value. */
  is_set() {
    return _locked(this, () => this.value);
  }

  /** ``if flag:`` reads the flag value (same as ``is_set``). */
  __bool__() {
    return this.is_set();
  }

  /** Set the flag to *state*. */
  set_to(state) {
    _locked(this, () => {
      this.value = py_bool(state);
    });
  }

  /** Context manager that holds the flag's lock. */
  static _Atomic = class _Atomic {
    /** Initialize with the parent flag. */
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
    return new AtomicFlag._Atomic(this);
  }

  /** Return a string representation of the flag. */
  __repr__() {
    return _locked(this, () => `AtomicFlag(${this.value ? "True" : "False"})`);
  }

  toString() {
    return this.__repr__();
  }

  [Symbol.for("nodejs.util.inspect.custom")]() {
    return this.__repr__();
  }
}
