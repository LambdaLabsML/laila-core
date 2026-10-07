/**
 * Runtime introspection for futures across the policy universe.
 *
 * This module is the *policy-agnostic* facade for asking questions about
 * futures: status, result, exception, or simply blocking until completion.
 * Unlike most of laila, it deliberately does *not* consult
 * ``laila.get_active_policy`` -- callers may pass a future from any local
 * policy and the helpers here will route the request to the right
 * ``future_bank`` automatically.
 *
 * Acceptable references
 * ---------------------
 * Every public helper accepts any of:
 *
 * - ``Future`` / ``GroupFuture`` / ``RemoteFuture`` -- the concrete future
 *   object, used as-is.
 * - ``_LAILA_IDENTIFIABLE_FUTURE`` -- a future *identity* whose ``global_id``
 *   is resolved against the local policy banks.
 * - ``str`` -- a raw ``global_id``, also resolved against the local policy
 *   banks.
 *
 * When resolution is needed, the helpers walk every local policy in
 * ``laila._local_policies`` and return the first hit. Missing ids raise
 * ``KeyError``.
 */
import { KeyError, TypeError as PyTypeError } from "../_compat/errors.js";
import { lazy } from "../_compat/lazy.js";
import { type_name, dict_values, dict_has, dict_get } from "../_compat/pytypes.js";

/**
 * Resolve *future_ref* to a concrete future object.
 *
 * Accepts ``Future``, ``GroupFuture``, ``RemoteFuture``,
 * ``_LAILA_IDENTIFIABLE_FUTURE``, or a raw ``global_id`` string. Concrete
 * futures are returned unchanged; identities and strings are looked up in
 * every local policy's ``future_bank`` (first hit wins).
 *
 * @throws {KeyError} If *future_ref* is an identity or string that does not
 *   match any future in any local policy bank.
 * @throws {TypeError} If *future_ref* is none of the supported reference types.
 */
export function _resolve_future(future_ref) {
  const { Future } = lazy("laila.policy.central.command.schema.future.future.future");
  const { _LAILA_IDENTIFIABLE_FUTURE } = lazy("laila.policy.central.command.schema.future.future.future_identity");
  const { GroupFuture } = lazy("laila.policy.central.command.schema.future.future.group_future");
  const { RemoteFuture } = lazy("laila.policy.central.command.schema.future.future.remote_future");

  if (future_ref instanceof RemoteFuture) return future_ref;
  if (future_ref instanceof Future) return future_ref;
  if (future_ref instanceof GroupFuture) return future_ref;

  if (typeof future_ref === "string") {
    const { _local_policies } = lazy("laila");

    for (const policy of dict_values(_local_policies)) {
      if (dict_has(policy.future_bank, future_ref)) return dict_get(policy.future_bank, future_ref);
    }
    throw new KeyError(`Future ${future_ref} not found in any local policy bank`);
  }

  if (future_ref instanceof _LAILA_IDENTIFIABLE_FUTURE) {
    const { _local_policies } = lazy("laila");

    const gid = future_ref.global_id;
    for (const policy of dict_values(_local_policies)) {
      if (dict_has(policy.future_bank, gid)) return dict_get(policy.future_bank, gid);
    }
    throw new KeyError(`Future ${gid} not found in any local policy bank`);
  }

  throw new PyTypeError(`Cannot resolve future for <class '${type_name(future_ref)}'>`);
}

/**
 * Return the current status of *future_ref* without blocking.
 *
 * For ``Future`` and ``RemoteFuture`` this is a single ``FutureStatus``
 * value; for ``GroupFuture`` it is the aggregate status payload (counts,
 * percentages, top-level progress) defined by ``GroupFuture.status``.
 *
 * @param {any} future_ref Any of the supported reference types (see module docstring).
 */
export function status(future_ref) {
  return _resolve_future(future_ref).status;
}

/**
 * Return the result of *future_ref*, blocking until it is available.
 *
 * The blocking semantics are inherited from the underlying future: a
 * ``LoopBlockingWaitError`` may be raised if called from the same thread that
 * owns an async taskforce loop. Use ``wait`` explicitly if you need to bound
 * the wait.
 *
 * @param {any} future_ref Any of the supported reference types (see module docstring).
 */
export function result(future_ref) {
  return _resolve_future(future_ref).result;
}

/**
 * Return the exception captured by *future_ref*, or ``null``.
 *
 * Returns ``null`` for futures that completed successfully or are still in
 * flight; only futures whose terminal status is ``ERROR`` or ``CANCELLED``
 * carry an exception value.
 *
 * @param {any} future_ref Any of the supported reference types (see module docstring).
 */
export function exception(future_ref) {
  return _resolve_future(future_ref).exception;
}

/**
 * Block until *future_ref* terminates and return its result.
 *
 * The wait honours the same timeout semantics as ``Future.wait``:
 * ``timeout=null`` waits indefinitely; a positive *timeout* raises
 * ``TimeoutError`` on expiry. As with ``result``, calling from inside an
 * async taskforce loop thread raises ``LoopBlockingWaitError`` instead of
 * dead-locking.
 *
 * @param {any} future_ref Any of the supported reference types (see module docstring).
 * @param {number|null} [timeout] Maximum seconds to wait. ``null`` (default) waits forever.
 */
export function wait(future_ref, timeout = null) {
  return _resolve_future(future_ref).wait(timeout);
}
