/**
 * Thread-safe generic list with snapshot iterators and atomic batching.
 *
 * ``AtomicList`` wraps an array with an ``RLock`` and provides:
 *
 * - The standard mutable-sequence API (``__getitem__``, ``__setitem__``,
 *   ``append``, ``extend``, ``pop``, ``remove``, ``clear``, ...) -- all
 *   guarded by the lock.
 * - *Snapshot* iteration: ``__iter__`` returns an iterator over a copy of the
 *   list taken under the lock, so external code can iterate without holding
 *   the lock and without seeing the list mutate mid-iteration.
 * - An ``atomic`` context manager that yields the raw list while the lock is
 *   held, so a sequence of operations appears atomic to other threads.
 *
 * Use it where multiple threads need to share a mutable list (job queues,
 * deferred-callback registries, observer lists) without playing whack-a-mole
 * with manual locking.
 *
 * Item access: ``lst[i]`` (including negative indices) routes to
 * ``__getitem__`` through the item-access proxy; slices use ``slice()``.
 */
import { ConfigDict, Field, PrivateAttr, define_fields, define_private } from "../../_compat/pydantic.js";
import { RLock } from "../../_compat/threading.js";
import { IndexError, ValueError, TypeError as PyTypeError } from "../../_compat/errors.js";
import { repr } from "../../_compat/pyrepr.js";
import { eq as py_eq } from "../../_compat/pytypes.js";
import { indexable, sequence_key } from "../../_compat/proxy.js";
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

/** Python list index normalisation (raises ``IndexError`` out of range). */
function _norm_index(arr, idx) {
  if (typeof idx !== "number" || !Number.isInteger(idx)) throw new PyTypeError(`list indices must be integers or slices, not ${typeof idx}`);
  const n = arr.length;
  const i = idx < 0 ? idx + n : idx;
  if (i < 0 || i >= n) throw new IndexError("list index out of range");
  return i;
}

/** ``list[start:stop:step]`` snapshot with Python semantics. */
function _py_slice(arr, start, stop, step) {
  const n = arr.length;
  step = step === null || step === undefined ? 1 : step;
  if (step === 0) throw new ValueError("slice step cannot be zero");
  const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
  let s, e;
  if (step > 0) {
    s = start === null || start === undefined ? 0 : start < 0 ? clamp(start + n, 0, n) : clamp(start, 0, n);
    e = stop === null || stop === undefined ? n : stop < 0 ? clamp(stop + n, 0, n) : clamp(stop, 0, n);
  } else {
    s = start === null || start === undefined ? n - 1 : start < 0 ? clamp(start + n, -1, n - 1) : clamp(start, -1, n - 1);
    e = stop === null || stop === undefined ? -1 : stop < 0 ? clamp(stop + n, -1, n - 1) : clamp(stop, -1, n - 1);
  }
  const out = [];
  if (step > 0) for (let i = s; i < e; i += step) out.push(arr[i]);
  else for (let i = s; i > e; i += step) out.push(arr[i]);
  return out;
}

/**
 * Thread-safe list with common list operations.
 * - All mutations are guarded by a re-entrant lock.
 * - Reads that return iterators/slices produce *snapshots* to avoid surprises.
 * - Use ``with_(lst.atomic(), (L) => ...)`` to batch multiple mutations under
 *   one lock.
 */
export class AtomicList extends _LAILA_LOCALLY_ATOMIC_OBJECT {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_fields(this, {
      value: ["list[T]", Field({ default_factory: () => [] })],
    });
    define_private(this, {
      _lock: PrivateAttr({ default_factory: () => new RLock() }),
    });
  }

  constructor(data = {}) {
    super(data);
    return new.target._wrap(this);
  }

  /** Install the item-access proxy around a raw instance. */
  static _wrap(raw) {
    return indexable(raw, { index_key: sequence_key });
  }

  /** Return the number of elements. */
  __len__() {
    return _locked(this, () => this.value.length);
  }

  /** Return a string representation of the list. */
  __repr__() {
    return _locked(this, () => `AtomicList(${repr(this.value)})`);
  }

  toString() {
    return this.__repr__();
  }

  [Symbol.for("nodejs.util.inspect.custom")]() {
    return this.__repr__();
  }

  /** Iterate over a snapshot of the list. */
  __iter__() {
    return _locked(this, () => [...this.value][Symbol.iterator]());
  }

  [Symbol.iterator]() {
    return this.__iter__();
  }

  /** ``item in lst`` -- the sequence protocol's membership test (``==`` semantics). */
  __contains__(item) {
    return _locked(this, () => this.value.some((x) => py_eq(x, item)));
  }

  /**
   * Return the element at *idx*, or a snapshot slice.
   * @param {number|{start?: number|null, stop?: number|null, step?: number|null}} idx
   */
  __getitem__(idx) {
    return _locked(this, () => {
      if (idx !== null && typeof idx === "object") return _py_slice(this.value, idx.start, idx.stop, idx.step); // snapshot slice
      return this.value[_norm_index(this.value, idx)];
    });
  }

  /** Set element(s) at *idx*. */
  __setitem__(idx, item) {
    _locked(this, () => {
      if (idx !== null && typeof idx === "object") {
        const { start = null, stop = null, step = null } = idx;
        if (step !== null && step !== 1) throw new ValueError("extended slice assignment is not supported");
        const n = this.value.length;
        let s = start === null ? 0 : start < 0 ? Math.max(0, start + n) : Math.min(start, n);
        let e = stop === null ? n : stop < 0 ? Math.max(0, stop + n) : Math.min(stop, n);
        if (e < s) e = s;
        this.value.splice(s, e - s, ...item);
        return;
      }
      this.value[_norm_index(this.value, idx)] = item;
    });
  }

  /** Delete element(s) at *idx*. */
  __delitem__(idx) {
    _locked(this, () => {
      if (idx !== null && typeof idx === "object") {
        const keep = new Set(_py_slice(this.value.map((_, i) => i), idx.start, idx.stop, idx.step));
        this.value.splice(0, this.value.length, ...this.value.filter((_, i) => !keep.has(i)));
        return;
      }
      this.value.splice(_norm_index(this.value, idx), 1);
    });
  }

  /** Append *item* to the end of the list. */
  append(item) {
    _locked(this, () => {
      this.value.push(item);
    });
  }

  /** Extend the list with elements from *items*. */
  extend(items) {
    _locked(this, () => {
      for (const x of items) this.value.push(x);
    });
  }

  /** Insert *item* before *index*. */
  insert(index, item) {
    _locked(this, () => {
      const n = this.value.length;
      let i = index < 0 ? Math.max(0, index + n) : Math.min(index, n);
      this.value.splice(i, 0, item);
    });
  }

  /** Remove all elements. */
  clear() {
    _locked(this, () => {
      this.value.length = 0;
    });
  }

  /** Remove the first occurrence of *item*. */
  remove(item) {
    _locked(this, () => {
      const i = this.value.findIndex((x) => py_eq(x, item));
      if (i < 0) throw new ValueError("list.remove(x): x not in list");
      this.value.splice(i, 1);
    });
  }

  /** Remove and return the item at *index* (default last). */
  pop(index = -1) {
    return _locked(this, () => {
      if (!this.value.length) throw new IndexError("pop from empty list");
      const i = _norm_index(this.value, index);
      return this.value.splice(i, 1)[0];
    });
  }

  /** Return the number of occurrences of *item*. */
  count(item) {
    return _locked(this, () => this.value.filter((x) => py_eq(x, item)).length);
  }

  /** Return the index of the first occurrence of *item*. */
  index(item, start = 0, stop = null) {
    return _locked(this, () => {
      const n = this.value.length;
      let s = start < 0 ? Math.max(0, start + n) : start;
      let e = stop === null || stop === undefined ? n : stop < 0 ? Math.max(0, stop + n) : Math.min(stop, n);
      for (let i = s; i < e; i++) if (py_eq(this.value[i], item)) return i;
      throw new ValueError(`${repr(item)} is not in list`);
    });
  }

  /** Return a snapshot (shallow copy) of the list. */
  to_list() {
    return _locked(this, () => [...this.value]);
  }

  /** Set the element at *index* to *item*. */
  set_at(index, item) {
    _locked(this, () => {
      this.value[_norm_index(this.value, index)] = item;
    });
  }

  /** Return the element at *index*. */
  get_at(index) {
    return _locked(this, () => this.value[_norm_index(this.value, index)]);
  }

  /** Snapshot slice, equivalent to ``self.value[start:stop:step]``. */
  slice(start, stop, step = null) {
    return _locked(this, () => _py_slice(this.value, start, stop, step));
  }

  /**
   * In-place: keep only items in [start:stop], like list slicing, shrink the
   * list. Supports negative indices.
   */
  trim(start, stop) {
    _locked(this, () => {
      const n = this.value.length;
      let s = start === null || start === undefined ? 0 : start >= 0 ? start : n + start;
      let e = stop === null || stop === undefined ? n : stop >= 0 ? stop : n + stop;
      s = Math.max(0, Math.min(s, n));
      e = Math.max(0, Math.min(e, n));
      if (s >= e) this.value.length = 0;
      else this.value.splice(0, this.value.length, ...this.value.slice(s, e));
    });
  }

  /** Context manager that exposes the underlying list while the lock is held. */
  static _Atomic = class _Atomic {
    /** Initialize with the parent list. */
    constructor(parent) {
      this._p = parent;
    }

    /** Acquire the lock and return the raw list. */
    __enter__() {
      this._p._lock.acquire();
      return this._p.value;
    }

    /** Release the lock. */
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

  /**
   * Use::
   *
   *   with_(lst.atomic(), (L) => {
   *     L.push(x);
   *     L.push(...[...]);
   *     // Several ops under one lock acquisition
   *   });
   */
  atomic() {
    return new AtomicList._Atomic(this);
  }
}
