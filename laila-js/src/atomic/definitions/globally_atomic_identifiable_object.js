/**
 * Globally-atomic identifiable object with secret-gated locking.
 *
 * Extends ``_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT`` with a *global*
 * (policy-scoped) lock layered on top of the local one. The global lock is
 * bound to a single ``policy_id`` while it is held; a secret token returned
 * from ``lock_global`` is the only key that can release the lock or pass
 * through the secret-gated attribute wrapper installed by the
 * ``__getattribute__`` proxy.
 *
 * Use this for resources that need to be coordinated across policies in the
 * same process (or, in the future, across hosts via the distributed lock
 * manager). For the much more common per-instance, single-policy case, prefer
 * ``_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT`` -- the global machinery here
 * adds overhead that is wasted unless multiple policies genuinely contend for
 * the same object.
 */
import { randomBytes } from "node:crypto";

import { PrivateAttr, define_private } from "../../_compat/pydantic.js";
import { contextmanager, with_ } from "../../_compat/contextlib.js";
import { ValueError, PermissionError, TimeoutError as PyTimeoutError } from "../../_compat/errors.js";
import { is_plain_object } from "../../_compat/pytypes.js";
import { _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT } from "./locally_atomic_identifiable_object.js";

/** ``secrets.token_hex(8)`` */
function _token_hex(nbytes) {
  return randomBytes(nbytes).toString("hex");
}

const _atomic_cm = contextmanager(function* _atomic(scope, timeout_s, policy_id) {
  if (scope === "local") {
    const inner = _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT.prototype.atomic.call(this, { scope: "local", timeout_s });
    inner.__enter__();
    try {
      yield this;
    } finally {
      inner.__exit__(null, null, null);
    }
    return;
  }
  if (scope !== "global") throw new ValueError("Invalid scope for _LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT.atomic() call.");
  if (policy_id === null || policy_id === undefined) throw new ValueError("policy_id is required for global atomic scope.");

  const lock_global = _LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT.prototype.lock_global.bind(this);
  const unlock_global = _LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT.prototype.unlock_global.bind(this);
  const secret = lock_global(policy_id, { timeout_s });
  if (secret === null) throw new PermissionError("Global lock already held by another policy.");
  try {
    yield this;
  } finally {
    unlock_global(secret);
  }
});

/**
 * Identifiable object that coordinates access via a policy-scoped global lock.
 *
 * Adds three machinery pieces on top of the local-lock base:
 *
 * - ``lock_global`` / ``unlock_global`` -- explicit global acquire/release
 *   driven by a per-policy ``policy_id`` and a one-time secret token returned
 *   at acquire-time.
 * - ``atomic`` (overridden) -- accepts ``scope: "global"`` to hold the global
 *   lock for a ``with`` block, raising ``PermissionError`` when another policy
 *   holds it.
 * - the attribute proxy (Python's ``__getattribute__`` override) -- wraps
 *   every callable attribute so calls made while a global lock is held
 *   require a ``secret`` kwarg to pass through. This stops accidental
 *   mutations by code that doesn't know the lock is in effect.
 *
 * Global state (private):
 *
 * - ``_global_lock`` -- whether the lock is currently held.
 * - ``_holder_policy_id`` -- which policy currently holds it.
 * - ``_holder_secret`` -- the token that proves you're the holder.
 */
export class _LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT extends _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT {
  static {
    define_private(this, {
      _global_lock: PrivateAttr({ default: false }),
      _holder_policy_id: PrivateAttr({ default: null }),
      _holder_secret: PrivateAttr({ default: null }),
    });
  }

  constructor(data = {}) {
    super(data);
    return _LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT._wrap(this);
  }

  /** Install the ``__getattribute__`` wrapper around a raw instance. */
  static _wrap(raw) {
    return new Proxy(raw, {
      get(target, name, receiver) {
        const attr = Reflect.get(target, name, receiver);
        if (typeof name === "symbol" || typeof attr !== "function") return attr;
        // Wrap callable attributes to enforce secret verification on access.
        return function _wrapped(...args) {
          let secret = null;
          const last = args[args.length - 1];
          if (is_plain_object(last) && Object.prototype.hasOwnProperty.call(last, "secret")) {
            const { secret: s, ...rest } = last;
            secret = s;
            if (Object.keys(rest).length) args[args.length - 1] = rest;
            else args.pop();
          }
          const verify_secret = _LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT.prototype._verify_secret.bind(receiver);
          if (!verify_secret(secret)) return new PermissionError("Global lock requires correct secret for passing through.");
          return attr.apply(receiver, args);
        };
      },
    });
  }

  /**
   * Acquire the global lock for a given policy.
   *
   * @param {string} policy_id Identifier of the requesting policy.
   * @param {{timeout_s?: number|null}} [opts] Maximum seconds to wait for the
   *   underlying local lock.
   * @returns {string|null} A secret token on success (or if already held by
   *   the same policy), ``null`` if another policy holds the lock.
   */
  lock_global(policy_id, opts = {}) {
    const { timeout_s = null } = opts;
    return with_(this.atomic({ scope: "local", timeout_s }), () => {
      if (this._global_lock) {
        if (this._holder_policy_id === policy_id && this._holder_secret !== null) return this._holder_secret;
        return null;
      }
      this._global_lock = true;
      this._holder_policy_id = policy_id;
      this._holder_secret = _token_hex(8);
      return this._holder_secret;
    });
  }

  /**
   * Release the global lock if *secret* matches the holder token.
   * @param {string} secret The token returned by ``lock_global``.
   */
  unlock_global(secret) {
    with_(this.atomic({ scope: "local" }), () => {
      if (secret !== this._holder_secret) return;
      this._global_lock = false;
      this._holder_policy_id = null;
      this._holder_secret = null;
    });
  }

  /** Return ``true`` if the global lock is held (or local lock times out). */
  global_locked(opts = {}) {
    const { timeout_s = null } = opts;
    try {
      return with_(this.atomic({ scope: "local", timeout_s }), () => this._global_lock);
    } catch (e) {
      if (e instanceof PyTimeoutError) return true;
      throw e;
    }
  }

  /** Verify that *secret* matches the current holder token. */
  _verify_secret(secret, opts = {}) {
    const { timeout_s = null } = opts;
    try {
      return with_(this.atomic({ scope: "local", timeout_s }), () => {
        if (!this._global_lock) return true;
        return secret !== null && secret !== undefined && secret === this._holder_secret;
      });
    } catch (e) {
      if (e instanceof PyTimeoutError) return false;
      throw e;
    }
  }

  /**
   * Context manager for local or global atomic sections.
   *
   * @param {{scope?: string, timeout_s?: number|null, policy_id?: string|null}} [opts]
   *   - ``scope``: ``"local"`` for a local lock, ``"global"`` for a
   *     policy-scoped global lock.
   *   - ``timeout_s``: maximum seconds to wait for lock acquisition.
   *   - ``policy_id``: required when *scope* is ``"global"``.
   * @returns context manager yielding the locked instance.
   * @throws {ValueError} If *scope* is invalid or *policy_id* is missing for
   *   global scope.
   * @throws {PermissionError} If the global lock is already held by another
   *   policy.
   */
  atomic(opts = {}) {
    const { scope = "local", timeout_s = null, policy_id = null } = opts;
    return _atomic_cm.call(this, scope, timeout_s, policy_id);
  }
}
