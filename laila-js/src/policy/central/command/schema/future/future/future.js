/**
 * Abstract ``Future`` base class -- identity, status, callbacks, lifecycle.
 *
 * A ``Future`` is the laila-side analogue of ``concurrent.futures.Future`` /
 * ``asyncio.Future``, but extended with three things the standard library
 * doesn't provide:
 *
 * - **Identity.** A future has a stable ``global_id`` (UUID + scope) so it can
 *   be referenced across processes (see ``RemoteFuture``) and looked up in the
 *   owning policy's ``future_bank`` after the local handle has gone out of
 *   scope.
 * - **Result-as-Entry.** Reading ``future.result`` always yields an ``Entry``:
 *   non-Entry values set by the producer are wrapped in ``Entry.constant``
 *   lazily on first read, so every future ultimately resolves to an
 *   addressable entry that can be ``laila.remember``'d on a peer without the
 *   runner paying for the wrap up front.
 * - **Explicit release.** Every future is registered in the owning policy's
 *   ``future_bank`` and stays there until ``Future.release`` is called --
 *   nothing evicts futures automatically.
 * - **Status callbacks.** Multiple callbacks can be registered against any
 *   ``FutureStatus`` transition via ``add_status_callback``; late
 *   registrations on already-fired statuses fire immediately to close the
 *   obvious race.
 *
 * Concrete subclasses provide the actual ``wait`` / ``__await__`` /
 * producer-side completion logic: ``ConcurrentPackageFuture``,
 * ``ComplexFuture``, ``GroupFuture``, ``RemoteFuture``.
 */
import { NotImplementedError, RuntimeError } from "../../../../../../_compat/errors.js";
import { lazy, register } from "../../../../../../_compat/lazy.js";
import { ConfigDict, Field, PrivateAttr, define_fields, define_private } from "../../../../../../_compat/pydantic.js";
import { with_ } from "../../../../../../_compat/contextlib.js";
import { dict_values, dict_get, dict_pop, dict_set, dict_del, type_name } from "../../../../../../_compat/pytypes.js";
import { get_logger } from "../../../../../../logger/index.js";
import { _LAILA_IDENTIFIABLE_FUTURE } from "./future_identity.js";
import { FutureStatus } from "./future_status.js";

function _make_status_setter(status) {
  const _set = (f) => {
    f.status = status;
  };
  Object.defineProperty(_set, "name", { value: `_set_status_${status.name.toLowerCase()}` });
  return _set;
}

/**
 * Shared "set my status to X" table used by every ``Future``.
 *
 * Stateless, so one map serves all instances -- building seven closures per
 * future was measurable on the per-task hot path.
 * @type {Map<any, (f: any) => void>}
 */
export const _DEFAULT_STATUS_CALLBACKS = new Map([...FutureStatus].map((st) => [st, _make_status_setter(st)]));

/** Resolve the status key used by the callback tables (member or raw value). */
function _st(status) {
  return FutureStatus(status);
}

/**
 * Abstract base class for all in-process laila futures.
 *
 * Inherits identity (``taskforce_id``, ``policy_id``, ``future_group_id``,
 * ``precedence``, ``purpose`` and the usual ``uuid`` / ``scopes``) from
 * ``_LAILA_IDENTIFIABLE_FUTURE``, and adds:
 *
 * - ``status`` (``FutureStatus``) -- the lifecycle marker.
 * - ``result`` -- the (possibly auto-wrapped) ``Entry`` outcome.
 * - ``exception`` -- the failure outcome.
 * - ``_result_global_id`` -- the gid of the result entry.
 * - ``callbacks`` / ``_status_callbacks`` -- per-status hook lists.
 *
 * On construction the future self-registers into the active local policy's
 * ``future_bank`` and into every open guarantee scope on the calling thread.
 * That is what makes ``with laila.guarantee:`` work without any explicit
 * registration on the caller's part.
 *
 * Subclasses must override ``wait`` (and typically ``__await__``) to hook into
 * their concrete completion mechanism.
 */
export class Future extends _LAILA_IDENTIFIABLE_FUTURE {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_private(this, {
      _status: PrivateAttr({ default: FutureStatus.NOT_STARTED }),
      _return_value: PrivateAttr({ default: null }),
      _exception: PrivateAttr({ default: null }),
      _result_global_id: PrivateAttr({ default: null }),
      _result_pending_wrap: PrivateAttr({ default: false }),
      _timeout_ms: PrivateAttr({ default: 100 }),
      // ``_default_callbacks`` is shared and immutable (see
      // ``_DEFAULT_STATUS_CALLBACKS``); ``_status_callbacks`` is created in
      // ``model_post_init``. Neither uses a private factory because pydantic
      // re-inspects a private factory's signature on every instantiation --
      // futures are constructed on the per-task hot path.
      _default_callbacks: PrivateAttr({ default: null }),
      _status_callbacks: PrivateAttr({ default: null }),
    });
    define_fields(this, {
      callbacks: ["dict[FutureStatus, Callable]", Field({ default_factory: () => new Map() })],
    });
  }

  /**
   * Apply staged identity, wire default per-status callbacks and
   * self-register with the active local policy.
   *
   * 1. Chain to ``_LAILA_IDENTIFIABLE_OBJECT.model_post_init`` so
   *    ``this.global_id`` reflects any explicit ``uuid``.
   * 2. Point ``_default_callbacks`` at the shared table.
   * 3. Resolve the active *local* policy (``_get_active_local_policy``
   *    lazily activates a ``DefaultPolicy`` if needed).
   * 4. Register ``this`` in every currently-open guarantee scope on this
   *    thread, then drop a reference into ``policy.future_bank``.
   *
   * Logging the creation event is best-effort.
   */
  model_post_init(_context) {
    super.model_post_init(_context);
    this._setup_default_callbacks();
    const { _get_active_local_policy } = lazy("laila");
    const policy = _get_active_local_policy();
    policy.central.command._register_future_with_active_guarantees(this);
    dict_set(policy.future_bank, this.global_id, this);
    try {
      get_logger().record_future_created(this);
    } catch {
      /* best effort */
    }
  }

  /** Attach the shared default status-transition table and an empty per-instance registry. */
  _setup_default_callbacks() {
    this._default_callbacks = _DEFAULT_STATUS_CALLBACKS;
    this._status_callbacks = new Map();
  }

  /** Return the current status code for this Future. */
  get status() {
    return with_(this.atomic(), () => this._status);
  }

  /**
   * Set the current status code for this Future.
   *
   * After updating the internal state and emitting the standard logger
   * transition, fires every callback registered for *status* via
   * ``add_status_callback``. Callback exceptions are swallowed to avoid
   * disrupting the producer (taskforce runner) thread.
   */
  set status(status) {
    status = _st(status);
    with_(this.atomic(), () => {
      const prev = this._status;
      this._status = status;
      if (prev !== status) {
        try {
          get_logger().record_future_transition(this, status, prev);
        } catch {
          /* best effort */
        }
        const cbs = [...(this._status_callbacks.get(status) ?? [])];
        for (const cb of cbs) {
          try {
            cb(this);
          } catch {
            /* swallowed */
          }
        }
      }
    });
  }

  /**
   * Register *fn* to fire when this future transitions into *status*.
   *
   * Multiple callbacks per status are supported (registered in insertion
   * order, fired in insertion order). The callback runs on whatever thread
   * set the status; it must therefore be cheap and non-blocking. Exceptions
   * raised inside *fn* are swallowed.
   *
   * If the future is *already* in *status* at registration time, *fn* is
   * invoked immediately and synchronously to close the obvious race between
   * registration and a producer that completed first.
   */
  add_status_callback(status, fn) {
    status = _st(status);
    let bucket = this._status_callbacks.get(status);
    if (bucket === undefined) {
      bucket = [];
      this._status_callbacks.set(status, bucket);
    }
    bucket.push(fn);
    if (this._status === status) {
      try {
        fn(this);
      } catch {
        /* swallowed */
      }
    }
  }

  /**
   * The future's result, blocking until completion if necessary.
   *
   * - If the future has already finished successfully, returns the recorded
   *   result without blocking.
   * - If the future ended in error or was cancelled, the recorded exception
   *   is re-raised.
   * - Otherwise, the lock is released and we ``wait(null)`` for completion --
   *   releasing the lock first is critical because the producer thread that
   *   ultimately sets the result will need to re-acquire the same atomic lock.
   */
  get result() {
    const early = with_(this.atomic(), () => {
      if (this._status === FutureStatus.ERROR || this._status === FutureStatus.CANCELLED) throw this._exception;
      if (this._status === FutureStatus.FINISHED) return { value: this._materialize_result() };
      return null;
    });
    if (early !== null) return early.value;
    this.wait(null);
    return with_(this.atomic(), () => {
      if (this._status === FutureStatus.ERROR || this._status === FutureStatus.CANCELLED) throw this._exception;
      return this._materialize_result();
    });
  }

  /**
   * Record the future's result; non-Entry values are wrapped lazily.
   *
   * - ``null`` clears both the value and the result-id slot.
   * - A live ``Entry`` is stored as-is and its ``global_id`` is captured
   *   into ``_result_global_id`` immediately.
   * - Anything else is stored raw and wrapped via ``Entry.constant`` on
   *   first read (``result`` / ``data`` / ``result_global_id``), so the
   *   producer's hot path does not pay for an entry nobody may ever look at.
   */
  set result(result) {
    const { Entry } = lazy("laila.entry");
    with_(this.atomic(), () => {
      if (result === null || result === undefined) {
        this._return_value = null;
        this._result_global_id = null;
        this._result_pending_wrap = false;
      } else if (result instanceof Entry) {
        this._return_value = result;
        this._result_global_id = result.global_id;
        this._result_pending_wrap = false;
      } else {
        this._return_value = result;
        this._result_global_id = null;
        this._result_pending_wrap = true;
      }
    });
  }

  /**
   * Return the result as an ``Entry``, wrapping a raw value on first use.
   *
   * Must be called with the atomic lock held (it is re-entrant, so callers
   * already inside ``with_(this.atomic(), ...)`` are fine).
   */
  _materialize_result() {
    return with_(this.atomic(), () => {
      if (this._result_pending_wrap) {
        const { Entry } = lazy("laila.entry");
        const wrapped = Entry.constant(this._return_value);
        this._return_value = wrapped;
        this._result_global_id = wrapped.global_id;
        this._result_pending_wrap = false;
      }
      return this._return_value;
    });
  }

  /**
   * ``global_id`` of the result entry, or ``null`` if there is no result yet.
   *
   * Forces the lazy ``Entry.constant`` wrap for raw results so the id is
   * stable from the first time anyone asks for it. Never blocks.
   */
  get result_global_id() {
    return with_(this.atomic(), () => {
      if (this._result_pending_wrap) this._materialize_result();
      return this._result_global_id;
    });
  }

  /**
   * Return ``this.result.data`` -- the unwrapped payload value.
   *
   * Blocks until the future finishes (via the underlying ``result`` getter)
   * and then unwraps the entry.
   *
   * @throws {RuntimeError} If the result is not an ``Entry`` instance.
   */
  get data() {
    const { Entry } = lazy("laila.entry");
    const result = this.result;
    if (!(result instanceof Entry)) {
      throw new RuntimeError(`Future result is not an Entry (got ${type_name(result)}); cannot access .data`);
    }
    return result.data;
  }

  /** Return the current exception value. */
  get exception() {
    return this._exception;
  }

  /** Set the exception value. */
  set exception(exception) {
    with_(this.atomic(), () => {
      this._exception = exception ?? null;
    });
  }

  // The public ``callbacks`` field mirrors the *last* callback registered per
  // status (a single-slot view); ``_status_callbacks`` is the registry that
  // actually fires on transitions. Both are kept in step here.

  /** Register a callback for a specific status transition. */
  add_callback(status, fn) {
    status = _st(status);
    dict_set(this.callbacks, status, fn);
    this.add_status_callback(status, fn);
  }

  /** Remove a callback for a specific status. */
  remove_callback(status, fn) {
    status = _st(status);
    const bucket = this._status_callbacks.get(status);
    if (bucket !== undefined) {
      let i;
      while ((i = bucket.indexOf(fn)) >= 0) bucket.splice(i, 1);
      if (bucket.length === 0) this._status_callbacks.delete(status);
    }
    if (dict_get(this.callbacks, status, null) === fn) dict_pop(this.callbacks, status, null);
  }

  /** Clear the callback for a specific status. */
  clear_callbacks(status) {
    status = _st(status);
    this._status_callbacks.delete(status);
    dict_pop(this.callbacks, status, null);
  }

  /** Clear all registered callbacks. */
  clear_all_callbacks() {
    this._status_callbacks.clear();
    if (this.callbacks instanceof Map) this.callbacks.clear();
    else for (const k of Object.keys(this.callbacks)) dict_del(this.callbacks, k);
  }

  // TODO: This needs to go through the central command.
  /**
   * Fire every callback registered for *status* with this future.
   *
   * Callbacks receive the future itself (never the blocking ``.result``),
   * mirroring what the status setter does.
   */
  trigger_callback(status) {
    status = _st(status);
    for (const fn of [...(this._status_callbacks.get(status) ?? [])]) {
      try {
        fn(this);
      } catch {
        /* swallowed */
      }
    }
  }

  /** Return a lightweight identity handle for this future. */
  get future_identity() {
    return new _LAILA_IDENTIFIABLE_FUTURE({
      taskforce_id: this.taskforce_id,
      policy_id: this.policy_id,
      future_group_id: this.future_group_id,
      precedence: this.precedence,
      purpose: this.purpose,
      uuid: this._uuid,
    });
  }

  /**
   * Remove this future from its owning policy's ``future_bank``.
   *
   * The bank holds a strong reference to every future (and therefore to its
   * result payload) until this is called; nothing releases a future
   * automatically. Idempotent and legal in any state: a still-running task
   * keeps its own reference to the future, so releasing early only removes
   * the gid-based lookup -- completion is unaffected. Open guarantee scopes
   * are left untouched; they are transient and drained on exit.
   */
  release() {
    const { _local_policies } = lazy("laila");
    const gid = this.global_id;
    const pid = this.policy_id !== null && typeof this.policy_id === "object" && "global_id" in this.policy_id ? this.policy_id.global_id : this.policy_id;
    const policy = typeof pid === "string" ? dict_get(_local_policies, pid, null) : null;
    if (policy !== null && dict_get(policy.future_bank, gid, null) === this) {
      dict_pop(policy.future_bank, gid, null);
      return;
    }
    for (const p of dict_values(_local_policies)) {
      if (dict_get(p.future_bank, gid, null) === this) {
        dict_pop(p.future_bank, gid, null);
        return;
      }
    }
  }

  /**
   * Block until the future completes. Subclasses must override.
   *
   * Concrete subclasses must implement this and additionally guard against
   * being invoked from an async loop thread (call ``_check_not_loop_thread``
   * first).
   *
   * @throws {NotImplementedError} Always, on the abstract base.
   */
  wait(_timeout = null) {
    throw new NotImplementedError(`${this.constructor.name} does not implement wait(); use a concrete subclass such as ConcurrentPackageFuture.`);
  }

  /** Return true if the Future has finished successfully. */
  finished() {
    return this.status === FutureStatus.FINISHED;
  }

  /** Return true if the Future was cancelled. */
  cancelled() {
    return this.status === FutureStatus.CANCELLED;
  }

  /** Return true if the Future finished with an error. */
  error() {
    return this.status === FutureStatus.ERROR;
  }

  /** Return true if the Future has not started. */
  not_started() {
    return this.status === FutureStatus.NOT_STARTED;
  }

  /** Return true if the Future is currently running. */
  running() {
    return this.status === FutureStatus.RUNNING;
  }
}

register("laila.policy.central.command.schema.future.future.future", { Future, _DEFAULT_STATUS_CALLBACKS });
