/**
 * Identity model for a Laila future -- lightweight metadata without result state.
 *
 * A ``_LAILA_IDENTIFIABLE_FUTURE`` carries only the *identity* of a future:
 * the gid of the task-force and policy that own it, optional group /
 * precedence / purpose annotations, and the inherited UUID + scopes. It does
 * NOT carry the result, exception, or status -- those live in the concrete
 * ``Future`` registered in the owning policy's ``future_bank``. The identity
 * object instead resolves all status / result / exception accessors
 * *through* the bank, so any process that holds an identity can introspect
 * the live future as long as the owning policy is reachable in
 * ``laila._local_policies``.
 *
 * The split exists so that a future can be referenced by metadata alone
 * (e.g. across a process boundary) and so that ``RemoteFuture`` can present
 * the same identity-like API on the remote side. Local submission paths
 * return the concrete ``Future`` itself -- it *is* an identity (subclass), so
 * no separate handle is built per task.
 */
import { KeyError } from "../../../../../../_compat/errors.js";
import { lazy, register } from "../../../../../../_compat/lazy.js";
import { dumps as json_dumps } from "../../../../../../_compat/pyjson.js";
import { define_fields } from "../../../../../../_compat/pydantic.js";
import { dict_values, dict_has, dict_get, dict_pop } from "../../../../../../_compat/pytypes.js";
import { _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT } from "../../../../../../atomic/definitions/locally_atomic_identifiable_object.js";
import { _LAILA_IDENTIFIABLE_OBJECT } from "../../../../../../basics/definitions/identifiable_object.js";
import { _FUTURE_SCOPE } from "../../../../../../macros/strings.js";

/** ``getattr(x, "global_id", x)`` */
function _gid(x) {
  return x !== null && x !== undefined && typeof x === "object" && "global_id" in x ? x.global_id : x;
}

/** Python ``str(x)`` for the identity fields (``None`` -> ``"None"``). */
function _s(x) {
  if (x === null || x === undefined) return "None";
  return String(x);
}

/**
 * Lightweight identity record for a Future.
 *
 * Carries only the metadata needed to *find* the live future in some
 * policy's ``future_bank``; the result, exception, and status are
 * properties that resolve through the bank on demand.
 *
 * Fields:
 * - ``taskforce_id``: gid of the task-force that owns this Future.
 * - ``policy_id``: gid of the policy instance that created this Future.
 * - ``future_group_id``: gid of the parent ``GroupFuture`` (optional).
 * - ``precedence``: gid of a future this one depends on (informational).
 * - ``purpose``: human-readable label for logs (e.g. ``"memorize:42"``).
 */
export class _LAILA_IDENTIFIABLE_FUTURE extends _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT {
  static _DEFAULT_SCOPES = [_FUTURE_SCOPE];

  static {
    define_fields(this, {
      taskforce_id: [[_LAILA_IDENTIFIABLE_OBJECT, "str"]],
      policy_id: [[_LAILA_IDENTIFIABLE_OBJECT, "str"]],
      future_group_id: [[_LAILA_IDENTIFIABLE_OBJECT, "str", "None"], null],
      // is this created by another future?
      precedence: [[_LAILA_IDENTIFIABLE_OBJECT, "str", "None"], null],
      purpose: ["str | None", null],
    });
  }

  /** Return a human-readable, line-by-line representation. */
  __str__() {
    const lines = [
      `future_id: ${this.global_id}`,
      `taskforce_id: ${_s(this.taskforce_id)}`,
      `policy_id: ${_s(this.policy_id)}`,
      `future_group_id: ${_s(this.future_group_id)}`,
      `precedence: ${_s(this.precedence)}`,
      `purpose: ${_s(this.purpose)}`,
    ];
    return lines.join("\n");
  }

  /** Return the same representation as ``__str__`` for readability. */
  __repr__() {
    return this.__str__();
  }

  toString() {
    return this.__str__();
  }

  /** Locate the concrete future in any local policy bank (first hit). */
  _bank_lookup() {
    const { _local_policies } = lazy("laila");
    const gid = this.global_id;
    for (const policy of dict_values(_local_policies)) {
      if (dict_has(policy.future_bank, gid)) return dict_get(policy.future_bank, gid);
    }
    throw new KeyError(`Future ${gid} not found in any local policy bank`);
  }

  /** Read-only status resolved from the owning policy's future bank. */
  get status() {
    return this._bank_lookup().status;
  }

  /** Read-only result resolved from the owning policy's future bank. */
  get result() {
    return this._bank_lookup().result;
  }

  /** Read-only unwrapped Entry payload, resolved through the owning policy's future bank. */
  get data() {
    const { _local_policies } = lazy("laila");
    const pid = _gid(this.policy_id);
    const policy = dict_get(_local_policies, pid, null);
    if (policy === null) throw new KeyError(`Owning policy ${pid} for future ${this.global_id} not found locally`);
    if (!dict_has(policy.future_bank, this.global_id)) throw new KeyError(`Future ${this.global_id} not found in policy ${pid}'s future bank`);
    return dict_get(policy.future_bank, this.global_id).data;
  }

  /** Read-only exception resolved from the owning policy's future bank. */
  get exception() {
    return this._bank_lookup().exception;
  }

  /**
   * Block until the future completes, resolved through the future bank.
   *
   * @throws {LoopBlockingWaitError} If called from a thread that owns an
   *   async event loop. Use ``await fut`` from inside coroutines instead.
   */
  wait(timeout = null) {
    const { _check_not_loop_thread } = lazy("laila.policy.central.command.schema.exceptions");
    _check_not_loop_thread();
    return this._bank_lookup().wait(timeout);
  }

  /**
   * Remove the underlying concrete future from its policy's future bank.
   *
   * Delegates to the concrete future's ``Future.release``. A no-op when the
   * future is no longer registered (already released or never created
   * locally). Futures are *never* released automatically -- whoever owns
   * the handle must call this once the result has been consumed, otherwise
   * the future (and its result payload) stays resident for the life of the
   * process.
   */
  release() {
    const { _local_policies } = lazy("laila");
    const gid = this.global_id;
    for (const policy of dict_values(_local_policies)) {
      const fut = dict_get(policy.future_bank, gid, null);
      if (fut !== null) {
        if (fut === this) dict_pop(policy.future_bank, gid, null);
        else fut.release();
        return;
      }
    }
  }

  /**
   * Await the underlying concrete future via the policy's future bank.
   *
   * Delegates to the concrete future's ``__await__`` (each concrete class --
   * ``ConcurrentPackageFuture``, ``ComplexFuture``, ``GroupFuture``,
   * ``RemoteFuture`` -- implements its own non-blocking completion signal).
   */
  __await__() {
    return this._bank_lookup().__await__();
  }

  /** Thenable so ``await identity`` works (``__await__`` protocol). */
  then(onFulfilled, onRejected) {
    let p;
    try {
      p = Promise.resolve(this.__await__());
    } catch (err) {
      p = Promise.reject(err);
    }
    return p.then(onFulfilled, onRejected);
  }

  /** Return a dict with all identity fields. */
  as_dict() {
    return {
      global_id: this.global_id,
      taskforce_id: _gid(this.taskforce_id),
      policy_id: _gid(this.policy_id),
      future_group_id: _gid(this.future_group_id),
      precedence: _gid(this.precedence),
      purpose: this.purpose,
    };
  }

  /** Return a JSON string for the identity fields. */
  __json__() {
    return json_dumps(this.as_dict());
  }
}

register("laila.policy.central.command.schema.future.future.future_identity", { _LAILA_IDENTIFIABLE_FUTURE });
