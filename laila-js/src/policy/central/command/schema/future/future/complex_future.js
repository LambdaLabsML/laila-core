/**
 * ComplexFuture -- sequential composition of futures (Mode A: declarative pipeline).
 *
 * A ``ComplexFuture`` represents a pipeline of stages, executed one at a time
 * in declared order. The number of stages and the function that produces each
 * stage's ``Future`` (given the prior stage's result) is fixed at construction
 * time. The actual stage ``Future`` instances are built lazily -- stage ``k``
 * is constructed when stage ``k-1`` flips to ``FutureStatus.FINISHED``.
 *
 * This is "Mode A" composition (immutable ``stage_fns``, lazy stage
 * construction). Dynamic-shape pipelines are expressed via composition:
 *
 * - A ``stage_fn`` may return another ``ComplexFuture`` or ``GroupFuture`` --
 *   pipelines nest to arbitrary depth.
 * - A ``stage_fn`` may branch -- pick one of several futures based on the
 *   prior stage's result.
 * - A ``stage_fn`` may return a ``GroupFuture`` -- fan out parallel work whose
 *   aggregated result feeds the next stage.
 *
 * Failure semantics
 * -----------------
 * - If any stage flips to ``ERROR`` or ``CANCELLED``, the parent propagates
 *   that status (and exception) and no further stages are constructed.
 * - Empty ``stage_fns`` is rejected at construction with ``ValueError``.
 * - A ``stage_fn`` that returns a non-future or raises synchronously when
 *   invoked transitions the parent to ``ERROR``.
 *
 * Implementation notes
 * -----------------
 * - Stage callbacks are wired through ``Future.add_status_callback`` for
 *   leaf futures. Group-future stages are polled on a daemon thread (see
 *   ``_watch_group_future``) because ``GroupFuture`` exposes its status as a
 *   percentage breakdown rather than a single ``FutureStatus`` member.
 * - Re-entrant locking via ``_stage_lock`` keeps the stage state-machine
 *   consistent under concurrent callback delivery.
 * - Pydantic v2's ``validate_python`` wipes any private-attribute writes done
 *   before ``super().__init__`` returns, so the raw ``stage_fns`` argument is
 *   shuttled across the boundary via the thread-local ``_PARK`` map and
 *   re-attached in ``model_post_init``.
 */
import { KeyError, RuntimeError, TypeError as PyTypeError, ValueError, FutureTimeoutError } from "../../../../../../_compat/errors.js";
import { lazy, register } from "../../../../../../_compat/lazy.js";
import * as asyncio from "../../../../../../_compat/asyncio.js";
import { get_ident } from "../../../../../../_compat/contextvars.js";
import { RLock, Thread, with_lock } from "../../../../../../_compat/threading.js";
import * as time from "../../../../../../_compat/time.js";
import { ConfigDict, Field, PrivateAttr, SKIP_VALIDATION, define_fields, define_private, normalize_kwargs } from "../../../../../../_compat/pydantic.js";
import { with_ } from "../../../../../../_compat/contextlib.js";
import { PyTuple, dict_values, dict_has, dict_get, type_name } from "../../../../../../_compat/pytypes.js";
import { Future } from "./future.js";
import { _LAILA_IDENTIFIABLE_FUTURE } from "./future_identity.js";
import { FutureStatus } from "./future_status.js";
import { GroupFuture } from "./group_future.js";

/**
 * Thread-local hand-off used by ``ComplexFuture`` to ferry the ``stage_fns``
 * argument from ``__init__`` to ``model_post_init`` across Pydantic v2's
 * ``validate_python`` boundary. Keyed by ``threading.get_ident()``;
 * populated and cleared inside ``ComplexFuture``'s constructor.
 * @type {Map<number, Function[]>}
 */
export const _PARK = new Map();

/**
 * Sequential composition of Future stages with declarative shape.
 *
 * ``stage_fns``: ordered list of callables. Stage 0 is invoked with ``null``;
 * every subsequent stage receives the prior stage's resolved result. Each
 * callable must return a ``Future``, ``GroupFuture``, or
 * ``_LAILA_IDENTIFIABLE_FUTURE`` (resolved against the active local policy's
 * future bank).
 *
 * @throws {ValueError} If ``stage_fns`` is empty at construction.
 */
export class ComplexFuture extends Future {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_fields(this, {
      stage_future_ids: ["list[str]", Field({ default_factory: () => [] })],
    });
    define_private(this, {
      _stage_fns: PrivateAttr({ default_factory: () => [] }),
      _stage_lock: PrivateAttr({ default_factory: () => new RLock() }),
      _current_idx: PrivateAttr({ default: -1 }),
      _terminated: PrivateAttr({ default: false }),
    });
  }

  constructor(data = {}) {
    if (data === SKIP_VALIDATION) {
      super(data);
      return;
    }
    data = normalize_kwargs(data, new.target);
    let stage_fns = data.stage_fns ?? [];
    delete data.stage_fns;
    if (!Array.isArray(stage_fns) && !(stage_fns instanceof PyTuple)) throw new PyTypeError("stage_fns must be a list or tuple of callables");
    if (stage_fns.length === 0) throw new ValueError("ComplexFuture requires at least one stage");
    stage_fns.forEach((fn, idx) => {
      if (typeof fn !== "function") throw new PyTypeError(`stage_fns[${idx}] must be callable, got ${type_name(fn)}`);
    });
    // Pydantic v2 wipes private attributes during validate_python (called
    // inside super()), so we cannot stash stage_fns on `this` before the
    // super call. Hand them off via a thread-local park so model_post_init
    // can pick them up after validation.
    _PARK.set(get_ident(), [...stage_fns]);
    try {
      super(data);
    } finally {
      _PARK.delete(get_ident());
    }
  }

  /**
   * Register with the active policy's future bank, then kick off stage 0.
   *
   * Picks up the parked ``stage_fns`` list from ``_PARK`` (set by the
   * constructor), reattaches it as a private attribute, then synchronously
   * triggers construction of the first stage with ``prior_result=null``.
   * Subsequent stages are constructed lazily inside the per-stage completion
   * callbacks.
   */
  model_post_init(_context) {
    super.model_post_init(_context);
    const parked = _PARK.get(get_ident());
    if (parked !== undefined) this._stage_fns = [...parked];
    this._kick_off_stage(0, { prior_result: null });
  }

  /** 0-based index of the most recently constructed stage (``-1`` before stage 0). */
  get current_stage_index() {
    return this._current_idx;
  }

  /** Total number of stages declared at construction. */
  get num_stages() {
    return this._stage_fns.length;
  }

  /** Resolve and return the list of stage futures constructed so far. */
  stage_futures() {
    const { _local_policies } = lazy("laila");
    const out = [];
    for (const fid of this.stage_future_ids) {
      for (const policy of dict_values(_local_policies)) {
        if (dict_has(policy.future_bank, fid)) {
          out.push(dict_get(policy.future_bank, fid));
          break;
        }
      }
    }
    return out;
  }

  /** Release every stage future constructed so far, then this future. */
  release() {
    for (const stage of this.stage_futures()) stage.release();
    super.release();
  }

  /**
   * Return the underlying Future or GroupFuture for *value*.
   *
   * Accepts a ``Future``, ``GroupFuture``, or a future identity (resolved
   * against the active local policy's future bank).
   */
  _resolve_future(value) {
    if (value instanceof Future) return value;
    if (value instanceof GroupFuture) return value;
    if (value instanceof _LAILA_IDENTIFIABLE_FUTURE) {
      const { _local_policies } = lazy("laila");
      const gid = value.global_id;
      for (const policy of dict_values(_local_policies)) {
        if (dict_has(policy.future_bank, gid)) return dict_get(policy.future_bank, gid);
      }
      throw new KeyError(`Future ${gid} not found in any local policy bank`);
    }
    throw new PyTypeError(`stage_fn must return a Future / GroupFuture / future_identity, got ${type_name(value)}`);
  }

  /** Construct stage *idx* and register chaining callbacks on it. */
  _kick_off_stage(idx, opts) {
    const { prior_result } = opts;
    with_lock(this._stage_lock, () => {
      if (this._terminated) return;
      let stage_fut;
      try {
        const fn = this._stage_fns[idx];
        const produced = fn(prior_result);
        stage_fut = this._resolve_future(produced);
      } catch (exc) {
        this._fail(exc, FutureStatus.ERROR);
        return;
      }

      this.stage_future_ids.push(stage_fut.global_id);
      this._current_idx = idx;

      if (stage_fut instanceof GroupFuture) {
        this._watch_group_future(idx, stage_fut);
      } else {
        stage_fut.add_status_callback(FutureStatus.FINISHED, (f) => this._on_stage_done(idx, f));
        stage_fut.add_status_callback(FutureStatus.ERROR, (f) => this._on_stage_failed(idx, f));
        stage_fut.add_status_callback(FutureStatus.CANCELLED, (f) => this._on_stage_failed(idx, f));
      }

      if (this._status === FutureStatus.NOT_STARTED) this.status = FutureStatus.RUNNING;
    });
  }

  /**
   * Drive a ``GroupFuture`` stage via a daemon-thread poll loop.
   *
   * ``GroupFuture`` exposes its status as a percentage breakdown rather than
   * a single ``FutureStatus``, so it does not support the callback hook used
   * for ``Future`` chaining. We poll until all children are terminal, then
   * synthesize the aggregated outcome and forward to ``_on_stage_done`` /
   * ``_on_stage_failed`` exactly as we would for a leaf future.
   */
  _watch_group_future(idx, gf) {
    const _poll = async () => {
      const poll_interval_s = 0.01;
      for (;;) {
        const pct = gf.status.percentages;
        const terminal_pct = Number(pct.finished) + Number(pct.error) + Number(pct.cancelled);
        if (terminal_pct >= 100.0) break;
        await asyncio.sleep(poll_interval_s);
      }
      let results;
      try {
        results = gf.result;
      } catch (exc) {
        this._on_stage_failed(idx, { exception: exc, status: FutureStatus.ERROR });
        return;
      }
      this._on_stage_done(idx, { result: results });
    };

    new Thread({ target: _poll, name: `ComplexFuture-GF-watch-${idx}`, daemon: true }).start();
  }

  /** Handle a stage finishing successfully -- kick off the next one or finalize. */
  _on_stage_done(idx, fut) {
    const nxt_value = with_lock(this._stage_lock, () => {
      if (this._terminated) return null;
      if (idx !== this._current_idx) return null;
      let value;
      try {
        value = fut.result;
      } catch (exc) {
        this._fail(exc, FutureStatus.ERROR);
        return null;
      }

      const nxt = idx + 1;
      if (nxt >= this._stage_fns.length) {
        this._terminated = true;
        this.exception = null;
        this.result = value;
        this.status = FutureStatus.FINISHED;
        return null;
      }
      return { nxt, value };
    });
    if (nxt_value === null) return;
    this._kick_off_stage(nxt_value.nxt, { prior_result: nxt_value.value });
  }

  /** Propagate a stage's ERROR/CANCELLED to the parent. */
  _on_stage_failed(idx, fut) {
    with_lock(this._stage_lock, () => {
      if (this._terminated) return;
      if (idx !== this._current_idx) return;
      this._fail(fut.exception, fut.status);
    });
  }

  /** Mark the parent as failed/cancelled and seal further progress. */
  _fail(exc, status = FutureStatus.ERROR) {
    this._terminated = true;
    this.exception = exc;
    this.result = null;
    this.status = status;
  }

  /**
   * Block until the pipeline terminates.
   *
   * Polls the parent status (which is driven by stage callbacks). When the
   * parent has reached a terminal status, returns the result or raises the
   * captured exception.
   *
   * @throws {LoopBlockingWaitError} If called from a thread that owns an
   *   async event loop.
   */
  wait(timeout = null) {
    const { _check_not_loop_thread } = lazy("laila.policy.central.command.schema.exceptions");
    const { park_sync } = lazy("laila.policy.central.command.schema.parking");
    _check_not_loop_thread();
    return park_sync(() => this._wait_impl(timeout));
  }

  _wait_impl(timeout) {
    const deadline = timeout === null || timeout === undefined ? null : time.monotonic() + timeout;
    const poll_interval_s = 0.01;
    for (;;) {
      const [status, exc] = with_(this.atomic(), () => [this._status, this._exception]);

      if (status === FutureStatus.FINISHED) return this._materialize_result();
      if (status === FutureStatus.ERROR || status === FutureStatus.CANCELLED) {
        if (exc !== null) throw exc;
        throw new RuntimeError(`ComplexFuture ended with status=${status} and no exception.`);
      }

      if (deadline !== null && time.monotonic() >= deadline) {
        this._default_callbacks.get(FutureStatus.POLL_TIMEOUT)(this);
        throw new FutureTimeoutError();
      }

      time.sleep(poll_interval_s);
    }
  }

  /**
   * Await the pipeline's terminal status by yielding to the event loop.
   *
   * Parks the current taskforce slot (if any) while pending.
   */
  __await__() {
    const { park_async } = lazy("laila.policy.central.command.schema.parking");
    const _await_terminal = async () => {
      const poll_interval_s = 0.01;
      for (;;) {
        const [status, exc] = with_(this.atomic(), () => [this._status, this._exception]);
        if (status === FutureStatus.FINISHED) return this._materialize_result();
        if (status === FutureStatus.ERROR || status === FutureStatus.CANCELLED) {
          if (exc !== null) throw exc;
          throw new RuntimeError(`ComplexFuture ended with status=${status} and no exception.`);
        }
        await asyncio.sleep(poll_interval_s);
      }
    };
    return park_async(_await_terminal());
  }
}

register("laila.policy.central.command.schema.future.future.complex_future", { ComplexFuture, _PARK });
