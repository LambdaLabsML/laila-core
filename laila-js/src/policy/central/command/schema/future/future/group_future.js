/**
 * ``GroupFuture`` -- aggregate future tracking a fixed set of children.
 *
 * A group future is the natural return type of any "submit N tasks together"
 * operation (see ``CentralCommand.submit`` with multiple tasks,
 * ``CentralMemory.memorize`` with multiple entries, the manifest-level
 * helpers, etc.). It owns no execution itself; instead it references the
 * children by ``global_id`` and delegates ``status``, ``result``, ``wait``,
 * and ``__await__`` to them.
 *
 * Children are looked up through the active local policy's future bank on
 * every access; this means the group keeps working even if the local
 * references to the children are dropped, as long as they remain in the bank.
 */
import { RuntimeError } from "../../../../../../_compat/errors.js";
import { lazy, register } from "../../../../../../_compat/lazy.js";
import * as asyncio from "../../../../../../_compat/asyncio.js";
import { ConfigDict, Field, define_fields } from "../../../../../../_compat/pydantic.js";
import { dumps as json_dumps } from "../../../../../../_compat/pyjson.js";
import { repr } from "../../../../../../_compat/pyrepr.js";
import { NotImplemented, PyFloat, dict_values, dict_get, dict_has, dict_pop, dict_items, getitem } from "../../../../../../_compat/pytypes.js";
import { get_logger } from "../../../../../../logger/index.js";
import { _LAILA_IDENTIFIABLE_OBJECT } from "../../../../../../basics/definitions/identifiable_object.js";
import { _GROUP_FUTURE_SCOPE } from "../../../../../../macros/strings.js";
import { FutureStatus } from "./future_status.js";

/** Return the active local policy's future bank mapping. */
function _get_future_bank() {
  const { _get_active_local_policy } = lazy("laila");
  return _get_active_local_policy().future_bank;
}

/** Python ``float(x)``: the status payload is all floats (``100.0``, ``3.0``). */
const _f = (x) => new PyFloat(x);

/**
 * Aggregate future grouping multiple child futures under one handle.
 *
 * The aggregate's status is a *percentage breakdown* of its children (see
 * ``status``) rather than a single ``FutureStatus``, because aggregating
 * heterogeneous outcomes into a single state quickly becomes lossy. Use
 * ``wait`` (or ``await``) to block until every child completes; ``result``
 * then collects the children's results in registration order.
 */
export class GroupFuture extends _LAILA_IDENTIFIABLE_OBJECT {
  static _DEFAULT_SCOPES = [_GROUP_FUTURE_SCOPE];

  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_fields(this, {
      taskforce_id: [[_LAILA_IDENTIFIABLE_OBJECT, "str"]],
      policy_id: [[_LAILA_IDENTIFIABLE_OBJECT, "str"]],
      future_ids: ["list[str]", Field({ default_factory: () => [] })],
    });
  }

  /** Register this group future with the active local policy's future bank. */
  model_post_init(_context) {
    super.model_post_init(_context);
    const { _get_active_local_policy } = lazy("laila");
    const policy = _get_active_local_policy();
    policy.central.command._register_future_with_active_guarantees(this);
    policy.future_bank[this.global_id] = this;
    try {
      get_logger().record_group_future_created(this);
    } catch {
      /* best effort */
    }
  }

  /**
   * Look up child Future objects from the future bank.
   *
   * With ``strict: false`` children that have already been released from
   * the bank are skipped instead of raising ``KeyError`` -- used by the
   * introspection paths (``status``, ``what``, ``repr``) which must stay
   * safe to call after ``release``.
   */
  _resolve_children(opts = {}) {
    const { strict = true } = opts;
    const bank = _get_future_bank();
    if (strict) return this.future_ids.map((fid) => getitem(bank, fid));
    return this.future_ids.filter((fid) => dict_has(bank, fid)).map((fid) => dict_get(bank, fid));
  }

  /**
   * Release this group -- and by default every child -- from the future bank.
   *
   * Children are reachable only through the bank (the group stores
   * ``future_ids``), so the group owns their release. Pass
   * ``{children: false}`` to drop only the group shell. Idempotent.
   */
  release(opts = {}) {
    const { children = true } = opts;
    const { _local_policies } = lazy("laila");
    if (children) {
      for (const fid of [...this.future_ids]) {
        for (const policy of dict_values(_local_policies)) {
          const child = dict_get(policy.future_bank, fid, null);
          if (child !== null) {
            if (typeof child.release === "function") child.release();
            else dict_pop(policy.future_bank, fid, null);
            break;
          }
        }
      }
    }
    const gid = this.global_id;
    for (const policy of dict_values(_local_policies)) {
      if (dict_get(policy.future_bank, gid, null) === this) {
        dict_pop(policy.future_bank, gid, null);
        return;
      }
    }
  }

  // ---------- computed status ----------
  /**
   * Return a percentage breakdown of child statuses::
   *
   *     { total: float, percentages: { finished, running, not_started, error, cancelled } }
   *
   * The percentages always sum to 100 except in the empty-group edge case
   * (everything is reported as 100% ``not_started`` for a group with no
   * children). ``total`` is always the number of child ids in the group;
   * percentages are computed over the children still present in the future
   * bank. Non-terminal transient states (``POLL_TIMEOUT``, ``UNKNOWN``) are
   * counted as ``running``.
   */
  get status() {
    const children = this.future_ids.length ? this._resolve_children({ strict: false }) : [];
    if (children.length === 0) {
      return {
        total: _f(this.future_ids.length),
        percentages: { finished: _f(0.0), running: _f(0.0), not_started: _f(100.0), error: _f(0.0), cancelled: _f(0.0) },
      };
    }
    const live = children.length;
    const statuses = children.map((f) => f.status);
    const count = (pred) => statuses.filter(pred).length;
    const running = count((s) => s === FutureStatus.RUNNING || s === FutureStatus.POLL_TIMEOUT || s === FutureStatus.UNKNOWN);
    const not_started = count((s) => s === FutureStatus.NOT_STARTED);
    const cancelled = count((s) => s === FutureStatus.CANCELLED);
    const finished = count((s) => s === FutureStatus.FINISHED);
    const error = count((s) => s === FutureStatus.ERROR);
    return {
      total: _f(this.future_ids.length),
      percentages: {
        finished: _f((finished / live) * 100.0),
        running: _f((running / live) * 100.0),
        not_started: _f((not_started / live) * 100.0),
        error: _f((error / live) * 100.0),
        cancelled: _f((cancelled / live) * 100.0),
      },
    };
  }

  // ---------- read-only interface (except cancel passthrough) ----------

  /** Merge additional child future IDs into this group. */
  append(future_ids) {
    this.future_ids.push(...future_ids);
  }

  /** Return self with merged child future IDs from another GroupFuture. */
  __add__(other) {
    if (!(other instanceof GroupFuture)) return NotImplemented;
    this.future_ids.push(...other.future_ids);
    return this;
  }

  /**
   * Block until every child future completes; return their results.
   *
   * Children are waited *sequentially*. The same ``timeout`` value is passed
   * to each child individually (in seconds), so the total wall time can be
   * up to ``timeout * len(children)``.
   *
   * @returns {any[]} One entry per child, in the order ``this.future_ids``.
   * @throws {LoopBlockingWaitError} If called from a thread that owns an
   *   async event loop. Use ``await group_future`` from coroutines instead.
   * @throws {RuntimeError} If a child does not expose a ``wait`` method.
   */
  wait(timeout = null) {
    const { _check_not_loop_thread } = lazy("laila.policy.central.command.schema.exceptions");
    const { park_sync } = lazy("laila.policy.central.command.schema.parking");
    _check_not_loop_thread();

    const _wait_all = () => {
      const children = this._resolve_children();
      const return_values = [];
      for (const f of children) {
        if (typeof f.wait === "function") return_values.push(f.wait(timeout));
        else throw new RuntimeError("Future is not associated with a native future.");
      }
      return return_values;
    };

    // Park once for the whole group so nested child waits do not
    // release/re-acquire the slot per child.
    return park_sync(_wait_all);
  }

  /**
   * Collect results from all children without blocking.
   *
   * Assumes every child has already completed (e.g. after a
   * ``laila.guarantee`` block).
   */
  get result() {
    return this._resolve_children().map((f) => f.result);
  }

  /**
   * Return the unwrapped payload data from every child future.
   * @throws {RuntimeError} If any child future's result is not an Entry.
   */
  get data() {
    return this._resolve_children().map((f) => f.data);
  }

  /**
   * Await all children concurrently via ``asyncio.gather``.
   *
   * Parks the current taskforce slot (if any) for the whole group.
   */
  __await__() {
    const { park_async } = lazy("laila.policy.central.command.schema.parking");
    const _await_all = async () => {
      const children = this._resolve_children();
      return await asyncio.gather(...children);
    };
    return park_async(_await_all());
  }

  then(onFulfilled, onRejected) {
    let p;
    try {
      p = Promise.resolve(this.__await__());
    } catch (err) {
      p = Promise.reject(err);
    }
    return p.then(onFulfilled, onRejected);
  }

  /**
   * First child exception, or ``null`` when no child has failed.
   *
   * Mirrors ``Future.exception`` so that group handles work with
   * ``laila.runtime.exception(...)`` and the other status helpers.
   */
  get exception() {
    for (const f of this._resolve_children({ strict: false })) {
      const exc = f.exception ?? null;
      if (exc !== null) return exc;
    }
    return null;
  }

  // ---------- introspection ----------
  /** Nested summary keyed by task_group_id. */
  get what() {
    const group_key = this.global_id;
    const child_details = {};
    const cancelled_ids = [];
    const not_cancelled_ids = [];
    const errors = {};

    const children = this._resolve_children({ strict: false });
    for (const f of children) {
      const fid = f.global_id;
      let det;
      if ("what" in f) det = f.what;
      else if ("details" in f) det = f.details;
      else {
        const st = f.status ?? FutureStatus.UNKNOWN;
        det = {
          [fid]: {
            status: st.value ?? st,
            error: st === FutureStatus.ERROR ? repr(f.exception) : null,
          },
        };
      }
      for (const [child_id, payload] of dict_items(det)) child_details[child_id] = payload;

      try {
        const cancelled = typeof f.cancelled === "function" ? f.cancelled() : false;
        if (cancelled) cancelled_ids.push(fid);
        else not_cancelled_ids.push(fid);
        if (f.status === FutureStatus.ERROR) errors[fid] = repr(f.exception);
      } catch (e) {
        errors[fid] = repr(e);
      }
    }

    return {
      [group_key]: {
        status: this.status,
        taskforce_id: this.taskforce_id,
        policy_id: this.policy_id,
        futures: child_details,
        summary: { cancelled: cancelled_ids, not_cancelled: not_cancelled_ids, errors },
      },
    };
  }

  /** Return JSON representation of the group. */
  __str__() {
    return json_dumps(this.what);
  }

  /** Return JSON representation of the group. */
  __repr__() {
    return json_dumps(this.what);
  }

  toString() {
    return this.__str__();
  }

  /** Iterate over child future IDs. */
  *[Symbol.iterator]() {
    yield* this.future_ids;
  }

  __iter__() {
    return this.future_ids[Symbol.iterator]();
  }

  /** Return the number of child futures. */
  __len__() {
    return this.future_ids.length;
  }

  get length() {
    return this.future_ids.length;
  }
}

register("laila.policy.central.command.schema.future.future.group_future", { GroupFuture });
