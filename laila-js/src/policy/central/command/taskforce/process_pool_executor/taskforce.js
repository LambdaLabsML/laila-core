/** Process-pool backed taskforce using ``concurrent.futures.ProcessPoolExecutor``. */
import os from "node:os";

import { NotImplementedError, RuntimeError, TypeError as PyTypeError, ValueError } from "../../../../../_compat/errors.js";
import { register } from "../../../../../_compat/lazy.js";
import { with_ } from "../../../../../_compat/contextlib.js";
import { ProcessPoolExecutor } from "../../../../../_compat/process_pool.js";
import { ConfigDict, Field, PrivateAttr, define_fields, define_private } from "../../../../../_compat/pydantic.js";
import { PyTuple } from "../../../../../_compat/pytypes.js";
import { Condition, Event, Semaphore, Thread } from "../../../../../_compat/threading.js";
import * as pickle from "../../../../../_codecs/pickle.js";
import { FutureStatus } from "../../schema/future/future/future_status.js";
import { GroupFuture } from "../../schema/future/future/group_future.js";
import { _LAILA_IDENTIFIABLE_TASK_FORCE, _shutdown_args, _submit_args } from "../base.js";
import { TaskForceStatus } from "../status.js";
import { ProcessPackageFuture } from "./future.js";

const _cpu_count = () => os.cpus().length || 1;

/** Top-level picklable function that executes a task in a worker process. */
export function _process_runner(task, args, kwargs) {
  const kw = kwargs ?? {};
  return Object.keys(kw).length ? task(...args, kw) : task(...args);
}
// Picklable by reference (``GLOBAL module.qualname``), like a module-level
// Python function.
_process_runner.__module__ = import.meta.url;
_process_runner.__qualname__ = "_process_runner";

/**
 * Process-pool TaskForce implementation.
 *
 * This backend mirrors the thread-pool task force, but only accepts top-level
 * picklable callables and picklable args/kwargs/results.
 */
export class PythonProcessPoolTaskForce extends _LAILA_IDENTIFIABLE_TASK_FORCE {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_fields(this, {
      backend: ["str", Field({ default: "processes", description: "Execution backend (processes only)." })],
      num_workers: [
        "int",
        Field({
          default_factory: () => Math.max(4, Math.floor(_cpu_count() / 2)),
          ge: 4,
          description: "Number of worker processes (minimum 4).",
        }),
      ],
    });
    define_private(this, {
      _cv: PrivateAttr({ default: null }),
      _worker_pool: PrivateAttr({ default: null }),
      _stop: PrivateAttr({ default: null }),
      _dispatcher: PrivateAttr({ default: null }),
      _submit_slots: PrivateAttr({ default: null }),
      _mp_context: PrivateAttr({ default: null }),
    });
  }

  /** Initialize process pool and dispatcher. */
  _on_start() {
    if (this.backend.toLowerCase() !== "processes") throw new ValueError("PythonProcessPoolTaskForce supports processes only.");

    this._cv = new Condition();
    this._stop = new Event();
    this._mp_context = "spawn";
    this._worker_pool = new ProcessPoolExecutor({ max_workers: this.num_workers, mp_context: this._mp_context });
    this._submit_slots = new Semaphore(Math.max(1, this.num_workers * 2));
    this._dispatcher = new Thread({ target: () => this._loop(), name: "ProcessTaskForce-Dispatcher", daemon: true });
    this._dispatcher.start();
  }

  /** Pause dispatcher loop without destroying the pool (currently a no-op). */
  _on_pause() {
    throw new NotImplementedError();
  }

  /** Tear down dispatcher and process pool. */
  _on_shutdown(wait = true, cancel_pending = true) {
    const opts = _shutdown_args(wait, cancel_pending);
    if (this._stop !== null) this._stop.set();
    if (this._cv !== null) with_(this._cv, () => this._cv.notify_all());

    if (opts.wait && this._dispatcher !== null) this._dispatcher.join();

    if (opts.cancel_pending) {
      with_(this._q.atomic("cancel"), () => {
        for (const [, item] of this._q.items()) {
          const kwargs = item[2];
          const fut = kwargs.fut ?? null;
          if (fut === null) continue;
          fut.exception = new RuntimeError("Task canceled before dispatch.");
          fut.status = FutureStatus.CANCELLED;
          fut.result = null;
        }
        this._q.clear();
      });
    }

    if (this._worker_pool !== null) this._worker_pool.shutdown({ wait: opts.wait, cancel_futures: opts.cancel_pending });
  }

  /** Verify that task, args, and kwargs are picklable. */
  _validate_process_task(task, args, kwargs) {
    try {
      pickle.dumps(PyTuple.from_iterable([task, PyTuple.from_iterable(args), kwargs]));
    } catch (exc) {
      const err = new PyTypeError("Process pool tasks must be top-level picklable callables with picklable args/kwargs.");
      err.__cause__ = exc;
      err.cause = exc;
      throw err;
    }
  }

  /** Internal: enqueue callable into the task queue. */
  _queue_submit(task, ...args) {
    if (this.status !== TaskForceStatus.RUNNING) throw new RuntimeError("TaskForce must be running before submitting tasks.");

    const kwargs = {};
    this._validate_process_task(task, args, kwargs);

    const fut = new ProcessPackageFuture({ taskforce_id: this.global_id, policy_id: this.policy_id });

    with_(this._cv, () => {
      with_(this._q.atomic(), () => {
        kwargs.task = task;
        kwargs.fut = fut;
        this._q.__setitem__(fut.global_id, [_process_runner, args, kwargs]);
      });
      this._cv.notify();
    });

    return fut;
  }

  /** Submit an iterable of zero-arg callables, yielding the futures in submission order. */
  *imap(tasks) {
    for (const f of tasks) yield this._queue_submit(f);
  }

  /**
   * Batch submit zero-arg callables.
   *
   * Returns the future itself (single) or a hollow GroupFuture (multiple)
   * when *wait* is false. When *wait* is true, blocks and returns values.
   * @param {Iterable<Function>} tasks
   * @param {boolean|{wait?: boolean}} [wait=false]
   */
  submit(tasks, wait = false) {
    const opts = _submit_args(wait);
    tasks = [...tasks];

    const futures = [];
    for (const task of tasks) {
      const fut = this._queue_submit(task);
      fut.taskforce_id = this.global_id;
      futures.push(fut);
    }

    if (futures.length === 1) {
      const single = futures[0];
      if (opts.wait) return single.wait(null);
      return single;
    }

    const gf = new GroupFuture({
      taskforce_id: this.global_id,
      policy_id: this.policy_id,
      future_ids: futures.map((f) => f.global_id),
    });

    for (const f of futures) f.future_group_id = gf.global_id;

    if (!opts.wait) return gf;
    return gf.wait(null);
  }

  /** Continuously dispatch tasks from queue to the worker pool. */
  async _loop() {
    const cv = this._cv;
    const stop = this._stop;
    const slots = this._submit_slots;

    while (!stop.is_set()) {
      while (!stop.is_set() && this._q.__len__() === 0) await cv.wait_unlocked_async(0.1, { unref: true });
      if (stop.is_set()) break;
      const item = with_(cv, () => this._q.pop_next()[1]);
      const [runner, args, kwargs] = item;

      while (!stop.is_set() && !(await slots.acquire_async(0.1))) {
        /* keep polling until a submit slot frees up or stop is requested */
      }
      if (stop.is_set()) {
        with_(this._q.atomic(), () => {
          this._q.__setitem__(kwargs.fut.global_id, [runner, args, kwargs]);
        });
        break;
      }

      const fut = kwargs.fut;
      const task = kwargs.task;
      const process_kwargs = {};
      for (const [k, v] of Object.entries(kwargs)) if (k !== "task" && k !== "fut") process_kwargs[k] = v;
      try {
        fut.status = FutureStatus.RUNNING;
        fut.native_future = this._worker_pool.submit(runner, task, PyTuple.from_iterable(args), process_kwargs);
        fut.native_future.add_done_callback((_f) => slots.release());
      } catch (err) {
        slots.release();
        throw err;
      }
    }
  }
}

register("laila.policy.central.command.taskforce.process_pool_executor.taskforce", {
  _process_runner,
  PythonProcessPoolTaskForce,
});
