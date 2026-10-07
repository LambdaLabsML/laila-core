/**
 * Thread-safe integer counter.
 *
 * ``AtomicInt`` wraps an ``int`` with an ``RLock`` so that
 * increment/decrement/add/set/get are race-free even under heavy contention.
 * Use it for shared counters (active workers, in-flight jobs, total processed
 * entries) where the operation reads, modifies, then writes (``i += 1`` is
 * *not* atomic across threads).
 *
 * For a single-bit signal, use ``AtomicFlag``. For a more elaborate
 * compute-and-set pattern, use ``AtomicDict.compute`` on a one-key dict
 * (rare, but possible).
 */
import { ConfigDict, Field, PrivateAttr, define_fields, define_private } from "../../_compat/pydantic.js";
import { RLock } from "../../_compat/threading.js";
import { int as py_int } from "../../_compat/pytypes.js";
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

/** Thread-safe integer with atomic add, increment, decrement, set, and get operations. */
export class AtomicInt extends _LAILA_LOCALLY_ATOMIC_OBJECT {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_fields(this, {
      value: ["int", Field({ default: 0 })],
    });
    define_private(this, {
      _lock: PrivateAttr({ default_factory: () => new RLock() }),
    });
  }

  /** Set the integer to *new_value*. */
  set_to(new_value) {
    _locked(this, () => {
      this.value = py_int(new_value);
    });
  }

  /** Return the current integer value. */
  get() {
    return _locked(this, () => this.value);
  }

  /** Add *delta* to the value and return the result. */
  add(delta) {
    return _locked(this, () => {
      this.value += delta;
      return this.value;
    });
  }

  /** Increment by 1 and return the new value. */
  increment() {
    return this.add(1);
  }

  /** Decrement by 1 and return the new value. */
  decrement() {
    return this.add(-1);
  }

  /** Reset the value to zero. */
  reset() {
    _locked(this, () => {
      this.value = 0;
    });
  }

  /** Context manager that holds the integer's lock. */
  static _Atomic = class _Atomic {
    /** Initialize with the parent integer. */
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
    return new AtomicInt._Atomic(this);
  }

  /** Allow use in integer expressions (``int(x)`` / ``+x``). */
  __int__() {
    return this.get();
  }

  valueOf() {
    return this.get();
  }

  /** Return a string representation of the integer. */
  __repr__() {
    return `AtomicInt(${this.get()})`;
  }

  toString() {
    return this.__repr__();
  }

  [Symbol.for("nodejs.util.inspect.custom")]() {
    return this.__repr__();
  }
}
