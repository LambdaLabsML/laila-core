/**
 * Thread-safe, insertion-ordered dictionary with atomic context support.
 *
 * ``AtomicDict`` is a drop-in ``MutableMapping`` whose every mutation is
 * guarded by an internal ``RLock``. On top of the standard mapping API it
 * adds:
 *
 * - *Insertion-ordered* iteration backed by a separate ``_order`` list that
 *   is auto-synced whenever it diverges from the underlying dict
 *   (``_ensure_order_synced``). Useful when callers need to act on "the next
 *   item to come in" without scanning.
 * - Positional accessors (``item_at``, ``key_at``, ``value_at``) and an
 *   in-place slice (``trim``) that work against the insertion order.
 * - An ``atomic`` context manager that yields a "view" object capable of
 *   doing batch mutations *under the lock* (so the whole batch appears atomic
 *   to other threads). Combined with the context-local ``AtomicDict.current``
 *   accessor, code inside the block can also reach the dict without
 *   re-receiving it as a parameter.
 * - Atomic compute-and-set (``compute``) and increment (``increment``)
 *   helpers for common read-modify-write patterns.
 *
 * Used heavily inside laila for things like the active-policy registry,
 * future-bank tables, taskforce queues, and the central memory hint / record
 * indexes -- anywhere a plain dict would be a race-condition waiting to
 * happen.
 *
 * Item access: instances are returned behind an item-access proxy, so
 * ``d[key]`` / ``d[key] = v`` / ``key in d`` / ``delete d[key]`` behave like
 * Python subscripts for every name that is not a real attribute (see
 * ``_compat/proxy.js``). The explicit ``__getitem__`` / ``__setitem__`` /
 * ``__delitem__`` / ``__contains__`` / ``__len__`` methods are always
 * available.
 */
import { ConfigDict, Field, PrivateAttr, define_fields, define_private, SKIP_VALIDATION, construct_into } from "../../_compat/pydantic.js";
import { RLock } from "../../_compat/threading.js";
import { ContextVar } from "../../_compat/contextvars.js";
import { KeyError, IndexError, RuntimeError, ValueError, TypeError as PyTypeError } from "../../_compat/errors.js";
import { repr } from "../../_compat/pyrepr.js";
import { tuple, isdict, dict_has, dict_get, dict_set, dict_del, dict_keys, dict_items, dict_len, dict_clear, is_plain_object, type_name } from "../../_compat/pytypes.js";
import { indexable } from "../../_compat/proxy.js";
import { _now_creation_timestamp } from "../../basics/definitions/laila_object.js";
import { _LAILA_LOCALLY_ATOMIC_OBJECT } from "../definitions/locally_atomic_object.js";

const _MISSING = Symbol("AtomicDict.missing");

/** ``with self._lock: return fn()`` */
function _locked(self, fn) {
  const lock = self._lock;
  lock.acquire();
  try {
    return fn();
  } finally {
    lock.release();
  }
}

/** ``hasattr(other, "keys")`` -> dict-like */
function _is_mapping(other) {
  return isdict(other) || (other !== null && typeof other === "object" && typeof other.keys === "function" && !Array.isArray(other));
}

/**
 * Thread-safe, insertion-ordered dict with index/slice ops and robust order
 * syncing.
 *
 * Supports:
 *   - ``with_(d.atomic(hint), (view) => ...)``: mutual exclusion for the
 *     instance; 'hint' is a str
 *   - ``AtomicDict.current()``: access the instance currently under an atomic
 *     block (thread/async local)
 *   - ``d.run_atomic(fn, hint)``: run a no-arg function with the lock held
 */
/** Python ``a + b`` for the value kinds an ``AtomicDict`` holds. */
function _py_add(a, b) {
  if (typeof a === "number" && typeof b === "number") return a + b;
  if (typeof a === "bigint" && typeof b === "bigint") return a + b;
  if (typeof a === "string") {
    if (typeof b === "string") return a + b;
    throw new PyTypeError(`can only concatenate str (not "${type_name(b)}") to str`);
  }
  if (Array.isArray(a) && Array.isArray(b)) return [...a, ...b];
  if (a !== null && a !== undefined && typeof a.__add__ === "function") return a.__add__(b);
  throw new PyTypeError(`unsupported operand type(s) for +: '${type_name(a)}' and '${type_name(b)}'`);
}

export class AtomicDict extends _LAILA_LOCALLY_ATOMIC_OBJECT {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_fields(this, {
      data: ["dict[K, V]", Field({ default_factory: () => new Map() })],
    });
    define_private(this, {
      _lock: PrivateAttr({ default_factory: () => new RLock() }),
      _order: PrivateAttr({ default_factory: () => [] }),
    });
  }

  /** context-local "current" AtomicDict (thread- & async-task-safe) */
  static _current = new ContextVar("AtomicDict_current", { default: null });

  /**
   * Create an ``AtomicDict``, optionally from a plain dict.
   *
   * @param {...any} args At most one positional ``dict`` argument (plain
   *   object or ``Map``), optionally followed by a keyword object
   *   (``{data: ...}``).
   * @throws {TypeError} On conflicting or unexpected arguments.
   */
  constructor(...args) {
    super(SKIP_VALIDATION);
    let kwargs = {};
    if (args.length > 1 && is_plain_object(args[args.length - 1])) kwargs = { ...args.pop() };
    if (args.length && (args[0] instanceof Map || is_plain_object(args[0]) || args[0] instanceof AtomicDict)) {
      if ("data" in kwargs) throw new PyTypeError("Cannot pass both dict positional arg and data keyword arg");
      kwargs.data = args[0] instanceof AtomicDict ? args[0].data : args[0];
      args = args.slice(1);
    }
    if (args.length && !(args.length === 1 && (args[0] === undefined || args[0] === null)))
      throw new PyTypeError("AtomicDict accepts at most one positional dict argument");
    let data = "data" in kwargs ? kwargs.data : new Map();
    delete kwargs.data;
    if (Object.keys(kwargs).length) {
      const unexpected = Object.keys(kwargs).sort().join(", ");
      throw new PyTypeError(`Unexpected keyword arguments: ${unexpected}`);
    }
    if (data === null || data === undefined) data = new Map();
    construct_into(this, new.target, { data });
    // ``model_construct`` skips ``_LAILA_OBJECT.__init__``; stamp here.
    this._creation_timestamp = _now_creation_timestamp();
    _locked(this, () => {
      this._order = dict_keys(this.data);
    });
    return new.target._wrap(this);
  }

  /** Install the item-access proxy around a raw instance. */
  static _wrap(raw) {
    return indexable(raw);
  }

  /**
   * Re-sync the order list with the data dict if they diverge.
   *
   * Every mutation path (``_set_nolock``, ``_del_nolock``, ``pop_next``,
   * ``clear``, ...) keeps ``_order`` and ``data`` in step, so divergence can
   * only come from callers mutating ``self.data`` directly. That is detected
   * by the O(1) length comparison. This method runs on *every* ``atomic()``
   * entry and iteration -- the taskforce queue enters it once per submitted
   * task -- so it must not walk the keys: a full O(n) validation here made
   * every submit quadratic in the number of pending tasks. Use ``reindex`` to
   * force a rebuild after out-of-band edits.
   *
   * Accessors that already walk the full order (``items``, ``values``,
   * ``__repr__``, ...) use ``_ensure_order_valid`` instead, which also catches
   * same-length key replacement.
   */
  _ensure_order_synced() {
    if (this._order.length !== dict_len(this.data)) this._order = dict_keys(this.data);
  }

  /**
   * O(n) variant of ``_ensure_order_synced`` for O(n) accessors.
   *
   * A same-length out-of-band edit (``del d.data["a"]; d.data["b"] = 2``)
   * slips past the length comparison and would make ``self.data[k]`` raise
   * ``KeyError`` for a stale ``k``. Callers that are about to walk ``_order``
   * anyway pay nothing extra for the membership scan.
   */
  _ensure_order_valid() {
    if (this._order.length !== dict_len(this.data) || this._order.some((k) => !dict_has(this.data, k))) this._order = dict_keys(this.data);
  }

  /** Rebuild the insertion-order index from the underlying dict. */
  reindex() {
    _locked(this, () => {
      this._order = dict_keys(this.data);
    });
  }

  /** Insert or update *key* without acquiring the lock. */
  _set_nolock(key, value) {
    if (!dict_has(this.data, key)) this._order.push(key);
    dict_set(this.data, key, value);
  }

  /** Delete *key* without acquiring the lock. */
  _del_nolock(key) {
    dict_del(this.data, key);
    const i = this._order.indexOf(key);
    if (i < 0) throw new ValueError("list.remove(x): x not in list");
    this._order.splice(i, 1);
  }

  /** Return the value for *key*, raising ``KeyError`` if missing. */
  __getitem__(key) {
    return _locked(this, () => {
      if (!dict_has(this.data, key)) throw new KeyError(key);
      return dict_get(this.data, key);
    });
  }

  /** Set *key* to *value* under the lock. */
  __setitem__(key, value) {
    _locked(this, () => this._set_nolock(key, value));
  }

  /** Delete *key* under the lock. */
  __delitem__(key) {
    _locked(this, () => this._del_nolock(key));
  }

  /** Return the number of items. */
  __len__() {
    return _locked(this, () => dict_len(this.data));
  }

  /** Iterate over keys in insertion order (snapshot). */
  __iter__() {
    return _locked(this, () => {
      this._ensure_order_synced();
      return [...this._order][Symbol.iterator](); // snapshot
    });
  }

  [Symbol.iterator]() {
    return this.__iter__();
  }

  /** Return ``true`` if *key* is present. */
  __contains__(key) {
    return _locked(this, () => dict_has(this.data, key));
  }

  /** Return an ordered string representation. */
  __repr__() {
    return _locked(this, () => {
      this._ensure_order_valid();
      const ordered = new Map(this._order.map((k) => [k, dict_get(this.data, k)]));
      return `AtomicDict(${repr(ordered)})`;
    });
  }

  toString() {
    return this.__repr__();
  }

  [Symbol.for("nodejs.util.inspect.custom")]() {
    return this.__repr__();
  }

  /** Return the value for *key*, or *default* if absent. */
  get(key, dflt = null) {
    return _locked(this, () => dict_get(this.data, key, dflt));
  }

  /** Return the value for *key*, inserting *default* if absent. */
  setdefault(key, dflt) {
    return _locked(this, () => {
      if (dict_has(this.data, key)) return dict_get(this.data, key);
      this._set_nolock(key, dflt);
      return dflt;
    });
  }

  /** Remove and return the value for *key*, or *default* if absent. */
  pop(key, dflt = _MISSING) {
    return _locked(this, () => {
      if (dict_has(this.data, key)) {
        const val = dict_get(this.data, key);
        this._del_nolock(key);
        return val;
      }
      if (dflt === _MISSING) throw new KeyError(key);
      return dflt;
    });
  }

  /** Remove and return the last inserted ``(key, value)`` pair. */
  popitem() {
    return _locked(this, () => {
      this._ensure_order_synced();
      if (!this._order.length) throw new KeyError("AtomicDict is empty");
      const k = this._order.pop();
      const v = dict_get(this.data, k);
      dict_del(this.data, k);
      return tuple([k, v]);
    });
  }

  /**
   * Remove and return the *first* inserted ``(key, value)`` pair.
   *
   * Hot path for queue-style consumers (the taskforce dispatcher pops one
   * item per task), so this deliberately avoids the O(n) walk in
   * ``_ensure_order_synced``: the backing ``dict`` is itself
   * insertion-ordered and authoritative, so its first key *is* the oldest
   * entry. The ``_order`` index is patched in O(1) when it agrees (the common
   * case) and rebuilt otherwise.
   */
  pop_next() {
    return _locked(this, () => {
      if (dict_len(this.data) === 0) throw new KeyError("AtomicDict is empty");
      const k = dict_keys(this.data)[0];
      const v = dict_get(this.data, k);
      dict_del(this.data, k);
      if (this._order.length && this._order[0] === k) this._order.shift();
      else this._order = dict_keys(this.data);
      return tuple([k, v]);
    });
  }

  /** Remove all items. */
  clear() {
    _locked(this, () => {
      dict_clear(this.data);
      this._order.length = 0;
    });
  }

  /**
   * Merge items from *other* and/or keyword arguments.
   * @param {object|Map|Array<[any,any]>|null} [other]
   * @param {object|null} [kwargs]
   */
  update(other = null, kwargs = null) {
    _locked(this, () => {
      if (other !== null && other !== undefined) {
        if (_is_mapping(other)) {
          for (const [k, v] of dict_items(other)) this._set_nolock(k, v);
        } else {
          for (const [k, v] of other) this._set_nolock(k, v);
        }
      }
      if (kwargs) for (const [k, v] of Object.entries(kwargs)) this._set_nolock(k, v);
    });
  }

  /** Return a snapshot list of keys in insertion order. */
  keys() {
    return _locked(this, () => {
      this._ensure_order_synced();
      return [...this._order];
    });
  }

  /** Return a snapshot list of values in insertion order. */
  values() {
    return _locked(this, () => {
      this._ensure_order_valid();
      return this._order.map((k) => dict_get(this.data, k));
    });
  }

  /** Return a snapshot list of ``(key, value)`` pairs in insertion order. */
  items() {
    return _locked(this, () => {
      this._ensure_order_valid();
      return this._order.map((k) => tuple([k, dict_get(this.data, k)]));
    });
  }

  /**
   * Return the ``(key, value)`` pair at positional *index*.
   *
   * @param {number} index Position in insertion order (supports negative
   *   indexing).
   * @returns {[any, any]} The key-value pair.
   * @throws {IndexError} If *index* is out of range.
   */
  item_at(index) {
    return _locked(this, () => {
      this._ensure_order_synced();
      const n = this._order.length;
      if (index < 0) index += n;
      if (index < 0 || index >= n) throw new IndexError("Index out of range");
      let k = this._order[index];
      if (!dict_has(this.data, k)) {
        // stale order after an out-of-band edit
        this._ensure_order_valid();
        if (index >= this._order.length) throw new IndexError("Index out of range");
        k = this._order[index];
      }
      return tuple([k, dict_get(this.data, k)]);
    });
  }

  /** Return the key at positional *index*. */
  key_at(index) {
    return _locked(this, () => this.item_at(index)[0]);
  }

  /** Return the value at positional *index*. */
  value_at(index) {
    return _locked(this, () => this.item_at(index)[1]);
  }

  // --- in-place slicing ---
  /** Keep only items in [start:end) by insertion order (end exclusive). */
  trim(start = null, end = null) {
    _locked(this, () => {
      this._ensure_order_synced();
      const n = this._order.length;

      let s = start === null || start === undefined ? 0 : start;
      let e = end === null || end === undefined ? n : end;
      if (s < 0) s += n;
      if (e < 0) e += n;
      s = Math.max(s, 0);
      e = Math.min(e, n);

      if (s >= e) {
        dict_clear(this.data);
        this._order.length = 0;
        return;
      }

      const keep_keys = this._order.slice(s, e);
      const keep_set = new Set(keep_keys);
      for (const k of dict_keys(this.data)) {
        if (!keep_set.has(k)) dict_del(this.data, k);
      }
      this._order.splice(0, this._order.length, ...keep_keys);
    });
  }

  /**
   * Atomically compute a new value for *key*.
   *
   * @param {any} key The target key.
   * @param {(cur: any) => any} fn Receives the current value (or ``null``)
   *   and returns the new value. If ``null`` is returned, the key is removed.
   * @returns {any} The new value, or ``null`` if the key was removed.
   */
  compute(key, fn) {
    return _locked(this, () => {
      const cur = dict_get(this.data, key, null);
      const nw = fn(cur);
      if (nw === null || nw === undefined) {
        if (dict_has(this.data, key)) this._del_nolock(key);
        return null;
      }
      this._set_nolock(key, nw);
      return nw;
    });
  }

  /** Add *delta* to the value at *key* (starting from *default*). */
  increment(key, delta = 1, dflt = 0) {
    return _locked(this, () => {
      let val = dict_get(this.data, key, dflt);
      val = _py_add(val, delta);
      this._set_nolock(key, val);
      return val;
    });
  }

  /** Return a human-readable, multi-line string representation. */
  pretty(indent = 2) {
    return _locked(this, () => {
      this._ensure_order_valid();
      const lines = ["AtomicDict {"];
      for (const k of this._order) lines.push(" ".repeat(indent) + `${repr(k)}: ${repr(dict_get(this.data, k))},`);
      lines.push("}");
      return lines.join("\n");
    });
  }

  /** Proxy object yielded by ``AtomicDict.atomic()`` for lock-held mutations. */
  static _AtomicView = class _AtomicView {
    /** Initialize with a reference to the parent dict. */
    constructor(parent) {
      this._p = parent;
      return indexable(this);
    }

    __setitem__(key, value) {
      this._p._set_nolock(key, value);
    }

    __delitem__(key) {
      this._p._del_nolock(key);
    }

    __getitem__(key) {
      if (!dict_has(this._p.data, key)) throw new KeyError(key);
      return dict_get(this._p.data, key);
    }

    __contains__(key) {
      return dict_has(this._p.data, key);
    }

    update(other = null, kwargs = null) {
      if (other !== null && other !== undefined) {
        if (_is_mapping(other)) {
          for (const [k, v] of dict_items(other)) this._p._set_nolock(k, v);
        } else {
          for (const [k, v] of other) this._p._set_nolock(k, v);
        }
      }
      if (kwargs) for (const [k, v] of Object.entries(kwargs)) this._p._set_nolock(k, v);
    }
  };

  /** Context manager that holds the dict lock and exposes an ``_AtomicView``. */
  static _Atomic = class _Atomic {
    /** Initialize with parent dict and optional hint. */
    constructor(parent, hint = "") {
      if (typeof hint !== "string") throw new PyTypeError("hint must be a str");
      this._p = parent;
      this.hint = hint;
      this._token = null;
    }

    __enter__() {
      this._p._lock.acquire();
      this._p._ensure_order_synced();
      // expose this instance as "current"
      this._token = AtomicDict._current.set(this._p);
      return new AtomicDict._AtomicView(this._p);
    }

    __exit__(_exc_type, _exc, _tb) {
      if (this._token !== null) {
        AtomicDict._current.reset(this._token);
        this._token = null;
      }
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
   * Return a context manager for batched, lock-held operations.
   *
   * @param {string} [hint] Descriptive label (reserved for future diagnostics).
   * @returns {InstanceType<typeof AtomicDict._Atomic>} Context manager
   *   yielding an ``_AtomicView``.
   */
  atomic(hint = "") {
    if (typeof hint !== "string") throw new PyTypeError("hint must be a str");
    return new AtomicDict._Atomic(this, hint);
  }

  /**
   * Return the ``AtomicDict`` currently held in an atomic block.
   * @throws {RuntimeError} If called outside an ``atomic`` context.
   */
  static current() {
    const d = this._current.get();
    if (d === null) throw new RuntimeError("AtomicDict.current() called outside an AtomicDict context");
    return d;
  }

  /** Run *fn* while holding the lock and return its result. */
  run_atomic(fn, hint = "") {
    if (typeof hint !== "string") throw new PyTypeError("hint must be a str");
    const cm = this.atomic(hint);
    cm.__enter__();
    try {
      return fn();
    } finally {
      cm.__exit__(null, null, null);
    }
  }
}
