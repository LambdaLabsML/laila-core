package laila

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/command/taskforce/process_pool_executor/taskforce.py --
// process-pool backed taskforce using ``concurrent.futures.ProcessPoolExecutor``.

var process_tf_log_ = py.Get_logger("laila.policy.central.command.taskforce.process_pool_executor.taskforce")

// Process_runner_ is ``_process_runner(task, args, kwargs)``: the top-level
// picklable function that executes a task in a worker process. In the Go
// port the worker resolves the task through the process-task registry
// (py.Register_process_task) and runs exactly this body.
func Process_runner_(task any, args []any, kwargs py.Kw) (any, error) {
	return py.Call(task, args, kwargs)
}

// Register_process_task registers fn under name so a process-pool
// taskforce can ship it to a worker (the Go analogue of a module-level,
// picklable function). The returned handle is callable in-process too.
// Register before calling Process_worker_main.
func Register_process_task(name string, fn any) *py.ProcessTask {
	return py.Register_process_task(name, fn)
}

// Process_worker_main serves process-pool tasks when this executable was
// spawned as a worker (``LAILA_PROCESS_WORKER=1``); a no-op otherwise.
// Call it first thing in main() / TestMain -- it is the ``__main__``
// guard the spawn start method requires.
func Process_worker_main() { py.Process_worker_main() }

// PythonProcessPoolTaskForce is the process-pool TaskForce implementation.
//
// This backend mirrors the thread-pool task force, but only accepts
// top-level picklable callables and picklable args/kwargs/results -- in
// Go: tasks registered with Register_process_task (or a py.Partial of
// one) with msgpack-serialisable arguments.
//
// # Worker death
//
// When a worker process dies (``os._exit``, segfault, OOM kill) the
// executor is marked *broken*: every in-flight future fails with
// BrokenProcessPool and the executor refuses new work. The dispatcher
// recovers by building a fresh executor with the same spawn context and
// re-submitting the task it was about to place; queued tasks keep
// flowing. Only if that rebuild itself fails does the taskforce flip to
// TaskForceStatus_CRASHED, fail the pending queue and stop accepting
// submissions.
type PythonProcessPoolTaskForce struct {
	LAILA_IDENTIFIABLE_TASK_FORCE_

	Backend     string `py:"backend" default:"processes"`
	Num_workers int    `py:"num_workers,ge=4,factory=Default_num_workers_"`

	Cv_           *py.Condition           `py:"_cv"`
	Worker_pool_  *py.ProcessPoolExecutor `py:"_worker_pool"`
	Stop_flag_    *py.Event               `py:"_stop"`
	Dispatcher_   *py.Thread              `py:"_dispatcher"`
	Submit_slots_ *py.Semaphore           `py:"_submit_slots"`
	Mp_context_   any                     `py:"_mp_context"`
}

// PythonProcessPoolTaskForce_class is the class descriptor. Callables
// cross a process boundary: ``Command.submit`` must hand over the
// original picklable objects, not the coroutine shim (``_wraps_sync =
// False``).
var PythonProcessPoolTaskForce_class = py.NewClass(
	"PythonProcessPoolTaskForce", (*PythonProcessPoolTaskForce)(nil),
	LAILA_IDENTIFIABLE_TASK_FORCE_class,
).Set_classvar("_wraps_sync", false)

// New_PythonProcessPoolTaskForce is ``PythonProcessPoolTaskForce(**data)``.
func New_PythonProcessPoolTaskForce(kw ...py.Kw) (*PythonProcessPoolTaskForce, error) {
	tf := PythonProcessPoolTaskForce_class.New().(*PythonProcessPoolTaskForce)
	if err := tf.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return tf, nil
}

// Default_num_workers_ is ``max(4, (os.cpu_count() or 1) // 2)``.
func (tf *PythonProcessPoolTaskForce) Default_num_workers_() int {
	return max(4, runtime.NumCPU()/2)
}

// Get_stop_ exposes ``_stop`` (Stop_flagged protocol).
func (tf *PythonProcessPoolTaskForce) Get_stop_() *py.Event { return tf.Stop_flag_ }

// status_ reads the lifecycle status under the instance lock.
func (tf *PythonProcessPoolTaskForce) status_() TaskForceStatus {
	tf.Lock()
	defer tf.Unlock()
	return tf.Status
}

// On_start_ initialises the process pool and dispatcher.
func (tf *PythonProcessPoolTaskForce) On_start_() error {
	if strings.ToLower(tf.Backend) != "processes" {
		return py.ValueError.New("PythonProcessPoolTaskForce supports processes only.")
	}

	tf.Cv_ = py.NewCondition(nil)
	tf.Stop_flag_ = py.NewEvent()
	tf.Mp_context_ = "spawn"
	tf.Worker_pool_ = tf.New_pool_()
	tf.Submit_slots_ = py.NewSemaphore(max(1, tf.Num_workers*2))
	tf.Dispatcher_ = py.New_Thread(py.Kw{
		"target": tf.Loop_, "name": "ProcessTaskForce-Dispatcher", "daemon": true,
	})
	return tf.Dispatcher_.Start()
}

// New_pool_ is ``ProcessPoolExecutor(max_workers=self.num_workers,
// mp_context=self._mp_context)``.
func (tf *PythonProcessPoolTaskForce) New_pool_() *py.ProcessPoolExecutor {
	return py.New_ProcessPoolExecutor(tf.Num_workers, py.Kw{"mp_context": tf.Mp_context_})
}

// On_pause_ would pause the dispatcher loop without destroying the pool
// (currently unsupported).
func (tf *PythonProcessPoolTaskForce) On_pause_() error {
	return py.NotImplementedError.New()
}

// On_shutdown_ tears down the dispatcher and process pool
// (``_on_shutdown(*, wait=True, cancel_pending=True)``).
//
// ``wait=True`` without ``cancel_pending`` is a graceful stop: the
// dispatcher keeps placing queued tasks until the queue is empty, then
// the pool runs everything it holds before the workers exit -- the same
// contract as ``ProcessPoolExecutor.shutdown(wait=True)``.
func (tf *PythonProcessPoolTaskForce) On_shutdown_(wait bool, cancel_pending bool) error {
	if wait && !cancel_pending {
		tf.Drain_queue_()
	}

	if tf.Stop_flag_ != nil {
		tf.Stop_flag_.Set()
	}
	if tf.Cv_ != nil {
		tf.Cv_.Acquire(true, -1)
		tf.Cv_.Notify_all()
		_ = tf.Cv_.Release()
	}

	if wait && tf.Dispatcher_ != nil {
		_ = tf.Dispatcher_.Join(-1)
	}

	if cancel_pending {
		tf.Cancel_queued_("Task canceled before dispatch.")
	}

	if tf.Worker_pool_ != nil {
		tf.Worker_pool_.Shutdown(wait, py.Kw{"cancel_futures": cancel_pending})
	}
	return nil
}

// Drain_queue_ blocks until the dispatcher has placed every queued task.
func (tf *PythonProcessPoolTaskForce) Drain_queue_() {
	dispatcher := tf.Dispatcher_
	if dispatcher == nil || !dispatcher.Is_alive() {
		return
	}
	if py.Current_thread() == dispatcher {
		return
	}
	for tf.Q_.Len() > 0 && dispatcher.Is_alive() {
		if tf.Stop_flag_ != nil && tf.Stop_flag_.Is_set() {
			return
		}
		tf.Cv_.Acquire(true, -1)
		tf.Cv_.Notify_all()
		_ = tf.Cv_.Release()
		_ = dispatcher.Join(0.05)
	}
}

// Cancel_queued_ fails every queued-but-undispatched future with reason
// and empties the queue (``_cancel_queued(reason, *,
// status=FutureStatus.CANCELLED)``).
func (tf *PythonProcessPoolTaskForce) Cancel_queued_(reason string, status ...FutureStatus) {
	st := FutureStatus_CANCELLED
	if len(status) > 0 {
		st = status[0]
	}
	_, release := tf.Q_.Atomic("cancel")
	defer release()
	for _, it := range tf.Q_.Items() {
		item := it.Value.(py.Tuple)
		kwargs := item[2].(py.Kw)
		fut, _ := kwargs.Get("fut", nil).(*ProcessPackageFuture)
		if fut == nil {
			continue
		}
		fut.Set_exception(py.RuntimeError.New(reason))
		fut.Set_result(nil)
		fut.Set_status(st)
	}
	tf.Q_.Clear()
}

// Validate_process_task_ verifies that task, args and kwargs are
// picklable: the task must resolve to a registered process task and the
// arguments must be msgpack-serialisable.
func (tf *PythonProcessPoolTaskForce) Validate_process_task_(task any, args []any, kwargs py.Kw) error {
	var cause error
	// ``pickle.dumps(partial)`` pickles the bound ``.args``/``.keywords``
	// too, so an unpicklable bound argument fails at submit time.
	if p, ok := task.(*py.PartialFunc); ok {
		args = append(append([]any(nil), p.Args...), args...)
		if len(p.Keywords) > 0 {
			merged := py.Kw{}
			for k, v := range p.Keywords {
				merged[k] = v
			}
			for k, v := range kwargs {
				merged[k] = v
			}
			kwargs = merged
		}
	}
	if _, ok := py.Lookup_process_task(task); !ok {
		cause = py.PicklingError.Newf("Can't pickle %s: it is not a registered process task", py.Repr(task))
	} else if _, err := py.Msgpack_packb(py.Tuple(args)); err != nil {
		cause = err
	} else if _, err := py.Msgpack_packb(kwargs); err != nil {
		cause = err
	}
	if cause != nil {
		return py.TypeError.New(
			"Process pool tasks must be top-level picklable callables with picklable args/kwargs.",
		).From(cause)
	}
	return nil
}

// Queue_submit_ enqueues the callable into the task queue.
func (tf *PythonProcessPoolTaskForce) Queue_submit_(task any, args ...any) (*ProcessPackageFuture, error) {
	if tf.status_() != TaskForceStatus_RUNNING {
		return nil, py.RuntimeError.New("TaskForce must be running before submitting tasks.")
	}
	pos, kwargs := py.SplitKw(args)
	kwargs = kwargs.Copy()

	if err := tf.Validate_process_task_(task, pos, kwargs); err != nil {
		return nil, err
	}

	fut, err := New_ProcessPackageFuture(py.Kw{
		"taskforce_id": tf.Global_id(),
		"policy_id":    tf.Policy_id,
	})
	if err != nil {
		return nil, err
	}

	tf.Cv_.Acquire(true, -1)
	defer tf.Cv_.Release()
	_, release := tf.Q_.Atomic()
	kwargs["task"] = task
	kwargs["fut"] = fut
	tf.Q_.Setitem(fut.Global_id(), py.Tuple{Process_runner_, pos, kwargs})
	release()
	tf.Cv_.Notify(1)

	return fut, nil
}

// Imap submits an iterable of zero-arg callables, yielding the futures in
// submission order.
func (tf *PythonProcessPoolTaskForce) Imap(tasks any) (py.Iterator, error) {
	items := py.Iter(tasks)
	return func(yield func(any, error) bool) {
		for _, f := range items {
			fut, err := tf.Queue_submit_(f)
			if err != nil {
				yield(nil, err)
				return
			}
			if !yield(fut, nil) {
				return
			}
		}
	}, nil
}

// Submit batch-submits zero-arg callables (``submit(tasks, wait=False)``).
//
// Returns the future itself (single) or a hollow GroupFuture (multiple)
// when wait is False. When wait is True, blocks and returns values.
func (tf *PythonProcessPoolTaskForce) Submit(tasks any, args ...any) (any, error) {
	pos, kw := py.SplitKw(args)
	wait := false
	if len(pos) > 0 {
		wait = py.Truthy(pos[0])
	}
	if kw.Has("wait") {
		wait = py.Truthy(kw["wait"])
	}
	task_list := py.Iter(tasks)

	futures := make([]*ProcessPackageFuture, 0, len(task_list))
	for _, task := range task_list {
		fut, err := tf.Queue_submit_(task)
		if err != nil {
			return nil, err
		}
		fut.Taskforce_id = tf.Global_id()
		futures = append(futures, fut)
	}

	if len(futures) == 1 {
		single := futures[0]
		if wait {
			// ``wait=True`` returns a bare value and no handle, so the
			// future would otherwise stay in the bank forever.
			defer single.Release()
			return single.Wait(nil)
		}
		return single, nil
	}

	ids := make([]string, len(futures))
	for i, f := range futures {
		ids[i] = f.Global_id()
	}
	gf, err := New_GroupFuture(py.Kw{
		"taskforce_id": tf.Global_id(),
		"policy_id":    tf.Policy_id,
		"future_ids":   ids,
	})
	if err != nil {
		return nil, err
	}

	for _, f := range futures {
		f.Future_group_id = gf.Global_id()
	}

	if !wait {
		return gf, nil
	}
	defer gf.Release()
	return gf.Wait(nil)
}

// Loop_ continuously dispatches tasks from the queue to the worker pool.
func (tf *PythonProcessPoolTaskForce) Loop_() {
	cv := tf.Cv_
	stop := tf.Stop_flag_
	slots := tf.Submit_slots_

	for !stop.Is_set() {
		cv.Acquire(true, -1)
		for !stop.Is_set() && tf.Q_.Len() == 0 {
			cv.Wait(0.1)
		}
		if stop.Is_set() {
			_ = cv.Release()
			break
		}
		_, item, err := tf.Q_.Pop_next()
		_ = cv.Release()
		if err != nil {
			continue
		}
		tup := item.(py.Tuple)
		runner := tup[0]
		args, _ := tup[1].([]any)
		kwargs := tup[2].(py.Kw)

		for !stop.Is_set() && !slots.Acquire(true, 0.1) {
		}
		if stop.Is_set() {
			_, release := tf.Q_.Atomic()
			tf.Q_.Setitem(kwargs["fut"].(*ProcessPackageFuture).Global_id(), py.Tuple{runner, args, kwargs})
			release()
			break
		}

		fut := kwargs["fut"].(*ProcessPackageFuture)
		task := kwargs["task"]
		process_kwargs := py.Kw{}
		for k, v := range kwargs {
			if k != "task" && k != "fut" {
				process_kwargs[k] = v
			}
		}
		fut.Set_status(FutureStatus_RUNNING)
		native, err := tf.Place_(runner, task, args, process_kwargs)
		if err != nil {
			if py.IsInstance(err, py.BrokenProcessPool) {
				// A worker died and the pool could not be rebuilt; the
				// taskforce cannot make progress any more.
				slots.Release(1)
				tf.Crash_(fut, err)
				break
			}
			// Anything else (executor already shut down, ...) fails this
			// task only; the dispatcher keeps serving the queue.
			slots.Release(1)
			fut.Set_exception(err)
			fut.Set_result(nil)
			fut.Set_status(FutureStatus_ERROR)
			continue
		}
		_ = fut.Set_native_future(native)
		native.Add_done_callback(func(_ *py.CFuture) { slots.Release(1) })
	}
}

// Place_ submits to the executor, rebuilding it once if a worker died.
//
// A BrokenProcessPool means a worker process vanished; the executor has
// already failed its in-flight futures. The task we are placing has *not*
// run, so it is retried on a fresh executor rather than failed. Returns
// BrokenProcessPool only if the fresh executor is unusable as well.
func (tf *PythonProcessPoolTaskForce) Place_(runner any, task any, args []any, process_kwargs py.Kw) (*py.CFuture, error) {
	_ = runner // the worker side is Process_runner_: task(*args, **kwargs)
	native, err := tf.Worker_pool_.Submit(task, args, process_kwargs)
	if err == nil {
		return native, nil
	}
	if !py.IsInstance(err, py.BrokenProcessPool) {
		return nil, err
	}
	process_tf_log_.Warning(
		"Process pool taskforce %s: worker died (%s); rebuilding the pool.",
		tf.Global_id(), err,
	)
	old := tf.Worker_pool_
	var fresh *py.ProcessPoolExecutor
	if rebuild_err := attempt(func() error { fresh = tf.New_pool_(); return nil }); rebuild_err != nil { // pragma: no cover - resource exhaustion
		return nil, py.BrokenProcessPool.New(
			fmt.Sprintf("could not rebuild the worker pool after a worker died: %s", py.Repr(rebuild_err)),
		).From(rebuild_err)
	}
	tf.Worker_pool_ = fresh
	_ = attempt(func() error { old.Shutdown(false, py.Kw{"cancel_futures": true}); return nil })
	return fresh.Submit(task, args, process_kwargs)
}

// Crash_ flips to ``CRASHED``: fails fut and every queued task, stops
// dispatching.
func (tf *PythonProcessPoolTaskForce) Crash_(fut *ProcessPackageFuture, exc error) {
	process_tf_log_.Error(
		"Process pool taskforce %s crashed: %s; failing %d queued task(s).",
		tf.Global_id(), exc, tf.Q_.Len(),
	)
	fut.Set_exception(exc)
	fut.Set_result(nil)
	fut.Set_status(FutureStatus_ERROR)
	tf.Lock()
	tf.Status = TaskForceStatus_CRASHED
	tf.Unlock()
	tf.Cancel_queued_(
		fmt.Sprintf("Process pool taskforce crashed: %s", py.Repr(exc)),
		FutureStatus_ERROR,
	)
	if tf.Stop_flag_ != nil {
		tf.Stop_flag_.Set()
	}
}
