/**
 * Locally-atomic base class providing reentrant per-instance locking.
 *
 * The single class here, ``_LAILA_LOCALLY_ATOMIC_OBJECT``, mixes a
 * lazily-created ``RLock`` into any Pydantic model and exposes a uniform
 * critical-section API (``lock`` / ``unlock`` / ``atomic``). It is the
 * foundation for every laila object that needs to coordinate access from
 * multiple threads -- pools, central memory, the future bank, taskforces, and
 * so on.
 *
 * "Locally" means *one lock per instance, in the current process*. For
 * cross-process coordination, see
 * ``_LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT`` which adds a distributed lock
 * alongside the local one.
 *
 * The lock is a re-entrant ``RLock`` so the same thread can nest critical
 * sections (e.g. ``with_(self.atomic(), () => self.helper())`` where
 * ``helper`` also enters ``self.atomic()``) without dead-locking.
 *
 * Multiple inheritance: Python's
 * ``class _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT(_LAILA_LOCALLY_ATOMIC_OBJECT,
 * _LAILA_IDENTIFIABLE_OBJECT)`` becomes ``extends
 * LocallyAtomic(_LAILA_IDENTIFIABLE_OBJECT)`` here; ``instanceof
 * _LAILA_LOCALLY_ATOMIC_OBJECT`` is true for every mixed-in instance.
 */
import { ConfigDict, finalize_model, PydanticUndefined, define_hidden } from "../../_compat/pydantic.js";
import { RLock, Lock } from "../../_compat/threading.js";
import { contextmanager } from "../../_compat/contextlib.js";
import { ValueError, TimeoutError as PyTimeoutError } from "../../_compat/errors.js";
import { deepcopy, copy } from "../../_compat/copy.js";
import { _LAILA_OBJECT } from "../../basics/definitions/laila_object.js";

const kLocallyAtomic = Symbol("laila.locally_atomic");

const _atomic_cm = contextmanager(function* _atomic(scope, timeout_s) {
  if (scope !== "local") throw new ValueError("Invalid scope for _LAILA_LOCALLY_ATOMIC_OBJECT.atomic() call.");
  if (!this.lock(timeout_s)) throw new PyTimeoutError("Timed out acquiring local lock.");
  try {
    yield this;
  } finally {
    this.unlock();
  }
});

/**
 * Mixin producing the ``_LAILA_LOCALLY_ATOMIC_OBJECT`` behaviour on top of
 * ``Base``: thread-safe per-instance locking.
 *
 * Subclasses inherit a lazily-allocated ``RLock`` and three helpers:
 *
 * - ``lock`` -- acquire the lock, optionally with a timeout.
 * - ``unlock`` -- release the lock once.
 * - ``atomic`` -- context manager wrapping ``lock`` and ``unlock`` so a
 *   critical section reads naturally as ``with_(self.atomic(), () => ...)``.
 *
 * The lock is created on first access (so Pydantic's validation pipeline does
 * not need to know about it) and stored as a plain own property to bypass
 * Pydantic's attribute machinery.
 * @template {typeof _LAILA_OBJECT} B
 * @param {B} Base
 */
export function LocallyAtomic(Base) {
  class _LAILA_LOCALLY_ATOMIC_MIXIN extends Base {
    static model_config = ConfigDict({ arbitrary_types_allowed: true });

    static {
      finalize_model(this);
      Object.defineProperty(this, "name", { value: `_LAILA_LOCALLY_ATOMIC_OBJECT(${Base.name})` });
      Object.defineProperty(this.prototype, kLocallyAtomic, { value: true });
    }

    static [Symbol.hasInstance](x) {
      if (this === _LAILA_LOCALLY_ATOMIC_OBJECT) return !!(x !== null && x !== undefined && typeof x === "object" && x[kLocallyAtomic] === true);
      return Function.prototype[Symbol.hasInstance].call(this, x);
    }

    /**
     * Forward all keyword arguments to Pydantic.
     *
     * Exists only to give subclasses a consistent constructor signature; the
     * lock itself is created lazily on first access via
     * ``_ensure_local_lock``.
     */
    constructor(data = {}) {
      super(data);
    }

    /**
     * Cooperative post-init hook.
     *
     * Pydantic injects a ``model_post_init`` into any class that inherits
     * private attributes (this one inherits ``_creation_timestamp`` from
     * ``_LAILA_OBJECT``), and the injected version does *not* chain via
     * ``super``. In the diamond ``_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT(LAO,
     * IDO)`` that would silently skip
     * ``_LAILA_IDENTIFIABLE_OBJECT.model_post_init`` (which applies the staged
     * uuid / scopes / evolution). Defining the hook explicitly keeps the MRO
     * chain intact.
     */
    model_post_init(_context) {
      super.model_post_init(_context);
    }

    /**
     * Return the instance's ``RLock``, creating it lazily.
     *
     * The lock is stored under the ``_local_lock`` own property to avoid going
     * through Pydantic's attribute validation. Subsequent calls reuse the same
     * lock.
     * @returns {RLock}
     */
    _ensure_local_lock() {
      let lock = this._local_lock ?? null;
      if (lock === null) {
        lock = new RLock();
        Object.defineProperty(this, "_local_lock", { value: lock, writable: true, enumerable: true, configurable: true });
      }
      return lock;
    }

    /**
     * Hold the local lock for the duration of the ``with`` block.
     *
     * Subclasses such as ``_LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT``
     * override the *scope* parameter to also accept ``"global"``; this base
     * class only knows about ``"local"`` and rejects anything else loudly so a
     * typo cannot silently degrade cross-process safety.
     *
     * @param {{scope?: string, timeout_s?: number|null}} [opts]
     *   - ``scope`` (default ``"local"``): must be ``"local"`` here. Accepting
     *     the keyword keeps the signature compatible with subclasses that add
     *     scopes.
     *   - ``timeout_s``: maximum seconds to wait for the lock. ``null``
     *     (default) blocks indefinitely.
     * @returns context manager yielding the locked instance, so callers can
     *   write ``with_(obj.atomic(), (locked) => ...)``.
     * @throws {ValueError} If *scope* is not ``"local"``.
     * @throws {TimeoutError} If the lock cannot be acquired within *timeout_s*.
     */
    atomic(opts = {}) {
      const { scope = "local", timeout_s = null } = opts;
      return _atomic_cm.call(this, scope, timeout_s);
    }

    /**
     * Acquire the per-instance reentrant lock.
     *
     * @param {number|null} [timeout_s] Maximum seconds to wait. Blocks
     *   indefinitely when ``null`` (default).
     * @returns {boolean} ``true`` if the lock was acquired, ``false`` if the
     *   timeout elapsed first.
     */
    lock(timeout_s = null) {
      const local_lock = this._ensure_local_lock();
      if (timeout_s === null || timeout_s === undefined) {
        local_lock.acquire();
        return true;
      }
      return local_lock.acquire({ timeout: timeout_s });
    }

    /**
     * Release the per-instance reentrant lock once.
     *
     * Mirrors ``RLock.release`` -- a thread that called ``lock`` *N* times
     * must call ``unlock`` *N* times before another thread can acquire the
     * lock.
     */
    unlock() {
      this._ensure_local_lock().release();
    }

    /**
     * Return ``true`` if the lock is currently held by some thread.
     *
     * Convenience wrapper for ``RLock.locked``. Note that a thread observing
     * ``true`` cannot safely conclude the lock will still be held by the time
     * it acts -- prefer the ``atomic`` context manager for synchronisation.
     */
    locked() {
      return this._ensure_local_lock().locked();
    }

    // ------------------------------------------------------------------
    // copy / pickle support
    // ------------------------------------------------------------------
    // Locks are process-local primitives: they cannot be pickled or
    // deep-copied, and a copy must never share its lock with the original.
    // ``deepcopy`` / ``pickle`` therefore skip every lock found on the
    // instance (``_local_lock`` own property, ``_lock`` private attrs on the
    // Atomic* types, ...) and the copy gets fresh ones of the same type.

    __deepcopy__(memo = null) {
      memo = memo ?? new Map();
      const cls = this.constructor;
      const m = Object.create(cls.prototype);
      memo.set(this, m);
      const [plain, locks] = _split_locks(_own_dict(this));
      const new_dict = deepcopy(plain, memo);
      for (const [k, name] of Object.entries(locks)) new_dict[k] = _LOCK_FACTORIES[name]();
      Object.assign(m, new_dict);
      const extra = deepcopy(this.__pydantic_extra__ ?? null, memo);
      const fields_set = copy(this.__pydantic_fields_set__ ?? new Set());
      const priv = this.__pydantic_private__ ?? null;
      let new_private = null;
      if (priv !== null) {
        const filtered = {};
        for (const [k, v] of Object.entries(priv)) if (v !== PydanticUndefined) filtered[k] = v;
        const [pplain, plocks] = _split_locks(filtered);
        new_private = deepcopy(pplain, memo);
        for (const [k, name] of Object.entries(plocks)) new_private[k] = _LOCK_FACTORIES[name]();
      }
      define_hidden(m, fields_set, extra, new_private);
      return typeof cls._wrap === "function" ? cls._wrap(m) : m;
    }

    __getstate__() {
      const state = { ...super.__getstate__() };
      const [plain, locks] = _split_locks(state.__dict__ ?? {});
      state.__dict__ = plain;
      state.__laila_dict_locks__ = locks;
      const priv = state.__pydantic_private__;
      if (priv && Object.keys(priv).length) {
        const [pplain, plocks] = _split_locks(priv);
        state.__pydantic_private__ = pplain;
        state.__laila_private_locks__ = plocks;
      }
      return state;
    }

    __setstate__(state) {
      state = { ...state };
      const dict_locks = state.__laila_dict_locks__ ?? {};
      const private_locks = state.__laila_private_locks__ ?? {};
      delete state.__laila_dict_locks__;
      delete state.__laila_private_locks__;
      super.__setstate__(state);
      for (const [k, name] of Object.entries(dict_locks)) {
        Object.defineProperty(this, k, { value: _LOCK_FACTORIES[name](), writable: true, enumerable: true, configurable: true });
      }
      if (Object.keys(private_locks).length) {
        let priv = this.__pydantic_private__;
        if (priv === null || priv === undefined) {
          priv = {};
          Object.defineProperty(this, "__pydantic_private__", { value: priv, writable: true, configurable: true });
        }
        for (const [k, name] of Object.entries(private_locks)) priv[k] = _LOCK_FACTORIES[name]();
      }
    }
  }
  return _LAILA_LOCALLY_ATOMIC_MIXIN;
}

/**
 * Pydantic base model giving subclasses thread-safe per-instance locking.
 *
 * Derives from ``_LAILA_OBJECT``, so every atomic object also carries a
 * creation ``creation_timestamp``. See ``LocallyAtomic`` for the API.
 */
export const _LAILA_LOCALLY_ATOMIC_OBJECT = LocallyAtomic(_LAILA_OBJECT);
Object.defineProperty(_LAILA_LOCALLY_ATOMIC_OBJECT, "name", { value: "_LAILA_LOCALLY_ATOMIC_OBJECT" });

export const _LOCK_FACTORIES = { rlock: () => new RLock(), lock: () => new Lock() };

function _lock_name(v) {
  if (v instanceof RLock) return "rlock";
  if (v instanceof Lock) return "lock";
  return null;
}

/** ``self.__dict__``: own enumerable properties (fields + undeclared attributes). */
function _own_dict(obj) {
  const d = {};
  for (const k of Object.keys(obj)) d[k] = obj[k];
  return d;
}

/**
 * Split *mapping* into ``[copyable entries, {key: lock kind name}]``.
 * @param {Object<string, any>} mapping
 * @returns {[Object<string, any>, Object<string, string>]}
 */
export function _split_locks(mapping) {
  const plain = {};
  const locks = {};
  for (const [k, v] of Object.entries(mapping)) {
    const name = _lock_name(v);
    if (name !== null) locks[k] = name;
    else plain[k] = v;
  }
  return [plain, locks];
}
