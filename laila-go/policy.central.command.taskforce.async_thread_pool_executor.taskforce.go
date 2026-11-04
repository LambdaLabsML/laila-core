package laila

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/command/taskforce/async_thread_pool_executor/taskforce.py
// -- async-thread-pool taskforce: N owned threads, each running its own
// asyncio loop.
//
// Each loop hosts up to ``max_async_per_thread`` concurrent in-flight
// tasks (*slots*). Submitted callables must be coroutine functions
// (zero-arg). Plain sync callables are auto-wrapped (at the
// ``Command.submit`` boundary and again, idempotently, in Queue_submit_
// for direct ``taskforce.submit`` callers) into ``async def`` shims that
// offload the body to this taskforce's ``sync`` executor (see
// Ensure_coroutine_function), so a sync body never blocks a loop thread.
// Async bodies yield on every ``await`` and let the loop interleave other
// in-flight coroutines.
//
// The dispatcher routes each task to the loop with the lowest current
// in-flight count whose count is strictly below ``max_async_per_thread``;
// when every loop is at the cap, the dispatcher blocks until one frees up.
//
// # Slot parking
//
// A task that awaits a laila future while holding a slot *parks*: the
// slot is released for the duration of the wait and re-acquired afterwards
// (see policy.central.command.schema.parking.go). Parked tasks are resumed
// with priority over freshly dispatched roots -- a loop thread hands a
// freed slot to a waiting parked task before it decrements its in-flight
// count. This makes nested submissions (``await laila.remember`` inside a
// submitted coroutine, constitution bodies realising manifests, ...)
// deadlock-free regardless of ``num_workers`` and ``max_async_per_thread``.
//
// # Sync offload
//
// Sync bodies (auto-wrapped sync callables and constitution bodies) run via
// Run_sync on an owned executor whose *thread count* is effectively
// unbounded, while the number of bodies *executing* at any time is bounded
// by ``sync_workers`` compute permits (a semaphore). A body that blocks on
// a laila future releases both its slot and its permit (Park_sync), so
// deep nesting of blocking sync bodies can never exhaust the pool -- a
// bounded thread pool would reintroduce the hold-and-wait pattern one
// level down -- while CPU parallelism stays capped at a sane level. The
// executor is also installed as each loop's default executor so
// py.To_thread inside user coroutines lands on it (without a permit).
//
// Because ``ThreadPoolExecutor`` never reaps idle workers, the executor is
// *recycled*: once the taskforce has been completely idle (nothing queued,
// in flight or parked) for EXECUTOR_IDLE_RECYCLE_S_ seconds, the
// dispatcher swaps in a fresh executor and shuts the old one down without
// waiting, so the threads a burst of sync work spawned exit instead of
// lingering for the life of the policy. A body still running on the old
// executor finishes normally.
//
// # Shutdown
//
// ``shutdown(wait=True)`` (the default, also what laila.Terminate uses) is
// a graceful stop with the stdlib ``ThreadPoolExecutor.shutdown(wait=True)``
// contract: every task already queued is still dispatched and run to
// completion before the threads are joined. ``cancel_pending=True`` drops
// queued tasks (their futures end ``CANCELLED``) and cancels in-flight
// coroutines. Calling ``shutdown(wait=True)`` from a task running on the
// taskforce itself cannot drain (the caller would wait for itself) and
// falls back to cancelling queued tasks with a warning.
//
// Each LoopThread_ registers its loop with ASYNC_LOOP_THREAD_IDS_ while
// running so the future Wait guard can detect (and reject) blocking waits
// called from inside a loop thread.
//
// Reuses ConcurrentPackageFuture for the per-task future state; the runner
// coroutine sets ``fut.exception``, ``fut.result``, and ``fut.status``
// directly.

// CHAIN_KW_ is the kwargs key carrying the submitter's resolve chain.
const CHAIN_KW_ = "_laila_resolve_chain"

// EXECUTOR_MAX_WORKERS_ is the (effectively unbounded) sync executor size.
const EXECUTOR_MAX_WORKERS_ = 1_000_000

// EXECUTOR_IDLE_RECYCLE_S_: seconds of complete idleness (nothing queued,
// in flight or parked) after which the sync executor is swapped for a
// fresh one so the threads it spawned can exit.
const EXECUTOR_IDLE_RECYCLE_S_ = 1.0

var async_tf_log_ = py.Get_logger("laila.policy.central.command.taskforce.async_thread_pool_executor.taskforce")

// LoopThread_ is a single owned thread running its own asyncio event
// loop.
//
// Concurrency budget per loop is tracked via Inflight_ against Cap; the
// dispatcher calls Try_reserve to claim a slot for a new root task,
// runners and parked tasks call Release_slot and Try_reserve_or_wait.
//
// # Slot hand-off
//
// Resume_waiters_ holds callbacks of parked tasks waiting to get a slot
// back. Release_slot pops the oldest waiter *instead of* decrementing the
// in-flight count, so the slot is transferred directly to that parked task
// and the dispatcher never sees it as free. Only when nobody is waiting is
// the count decremented and true returned so the caller can notify the
// dispatcher's capacity condition.
//
// Registers its loop with the global ASYNC_LOOP_THREAD_IDS_ set while
// running so blocking ``Future.wait()`` calls from inside the loop can be
// rejected with LoopBlockingWaitError.
type LoopThread_ struct {
	Name            string
	Cap             int
	Loop            *py.Loop
	Inflight_       int
	Inflight_lock_  sync.Mutex
	Resume_waiters_ []*Slot_waiter
	Ready_          *py.Event
	Thread_ident_   *int64
	Thread          *py.Thread
}

// New_LoopThread_ is ``_LoopThread(name, cap)``: starts the thread and
// blocks until its loop is ready.
func New_LoopThread_(name string, cap int) *LoopThread_ {
	lt := &LoopThread_{Name: name, Cap: cap, Loop: py.New_event_loop(), Ready_: py.NewEvent()}
	lt.Thread = py.New_Thread(py.Kw{"target": lt.Run_, "name": name, "daemon": true})
	_ = lt.Thread.Start()
	lt.Ready_.Wait(-1)
	return lt
}

// Run_ is the thread body: run the loop forever, then unwind pending
// tasks and close it.
func (lt *LoopThread_) Run_() {
	py.Set_event_loop(lt.Loop)
	ident := py.Get_ident()
	lt.Thread_ident_ = &ident
	Register_async_loop_thread_(lt.Loop)
	_, _ = lt.Loop.Call_soon(lt.Ready_.Set)
	defer func() {
		_ = attempt(func() error {
			for _, t := range lt.Loop.All_tasks() {
				t.Cancel()
			}
			// A few iterations so cancelled tasks can unwind their
			// ``finally`` blocks (parking exit, runner bookkeeping).
			for i := 0; i < 3; i++ {
				if len(lt.Loop.All_tasks()) == 0 {
					break
				}
				_, _ = lt.Loop.Run_until_complete(func() (any, error) { return nil, py.Sleep(0) })
			}
			return nil
		})
		_ = lt.Loop.Close()
		if lt.Thread_ident_ != nil {
			Unregister_async_loop_thread_(lt.Loop)
		}
	}()
	_ = lt.Loop.Run_forever()
}

// ---------- slot accounting ----------

// Inflight_count is the number of slots currently held on this loop.
func (lt *LoopThread_) Inflight_count() int {
	lt.Inflight_lock_.Lock()
	defer lt.Inflight_lock_.Unlock()
	return lt.Inflight_
}

// Parked_waiting is the number of parked tasks currently waiting to
// re-acquire a slot.
func (lt *LoopThread_) Parked_waiting() int {
	lt.Inflight_lock_.Lock()
	defer lt.Inflight_lock_.Unlock()
	return len(lt.Resume_waiters_)
}

// Try_reserve claims a slot for a new root task if under the cap.
func (lt *LoopThread_) Try_reserve() bool {
	lt.Inflight_lock_.Lock()
	defer lt.Inflight_lock_.Unlock()
	if lt.Inflight_ >= lt.Cap {
		return false
	}
	lt.Inflight_++
	return true
}

// Try_reserve_or_wait claims a slot now (true) or enqueues cb to be
// handed one later.
//
// Used by parked tasks re-acquiring. The callback is invoked -- from
// whichever thread releases the slot -- exactly once when a slot has been
// transferred to the waiter.
func (lt *LoopThread_) Try_reserve_or_wait(cb *Slot_waiter) bool {
	lt.Inflight_lock_.Lock()
	defer lt.Inflight_lock_.Unlock()
	if lt.Inflight_ < lt.Cap {
		lt.Inflight_++
		return true
	}
	lt.Resume_waiters_ = append(lt.Resume_waiters_, cb)
	return false
}

// Cancel_wait removes cb from the waiters. false if it was already served.
func (lt *LoopThread_) Cancel_wait(cb *Slot_waiter) bool {
	lt.Inflight_lock_.Lock()
	defer lt.Inflight_lock_.Unlock()
	for i, w := range lt.Resume_waiters_ {
		if w == cb {
			lt.Resume_waiters_ = append(lt.Resume_waiters_[:i:i], lt.Resume_waiters_[i+1:]...)
			return true
		}
	}
	return false
}

// Release_slot gives up one slot.
//
// Returns true if the in-flight count was decremented (the dispatcher may
// now place a new root), false if the slot was transferred to a parked
// waiter instead.
func (lt *LoopThread_) Release_slot() bool {
	lt.Inflight_lock_.Lock()
	var cb *Slot_waiter
	if len(lt.Resume_waiters_) > 0 {
		cb = lt.Resume_waiters_[0]
		lt.Resume_waiters_ = lt.Resume_waiters_[1:]
	} else if lt.Inflight_ > 0 {
		lt.Inflight_--
	}
	lt.Inflight_lock_.Unlock()
	if cb != nil {
		cb.Fn()
		return false
	}
	return true
}

// Submit_coro is ``asyncio.run_coroutine_threadsafe(coro, self.loop)``.
func (lt *LoopThread_) Submit_coro(coro func() (any, error)) *py.CFuture {
	return py.Run_coroutine_threadsafe(coro, lt.Loop)
}

// Request_stop asks the loop to stop without joining its thread.
//
// With drain=true the loop keeps running until every task currently
// scheduled on it (runners, parked re-acquires, pending sync offloads) has
// completed, then stops -- this is what makes ``shutdown(wait=True)``
// honour its "block until in-flight tasks finish" contract. Without it
// (``wait=False`` or an explicit ``cancel_pending=True``) the loop stops
// immediately and Run_ cancels whatever is still pending.
func (lt *LoopThread_) Request_stop(drain bool) {
	if drain {
		drain_then_stop_ := func() (any, error) {
			me := py.Current_task()
			for {
				others := []*py.Task{}
				for _, t := range lt.Loop.All_tasks() {
					if t != me {
						others = append(others, t)
					}
				}
				if len(others) == 0 {
					break
				}
				if _, _, err := py.Wait(others, 0.1, py.ALL_COMPLETED); err != nil {
					return nil, err
				}
			}
			lt.Loop.Stop()
			return nil, nil
		}
		// RuntimeError (loop closed) is swallowed.
		_ = py.Run_coroutine_threadsafe(drain_then_stop_, lt.Loop)
		return
	}
	_, _ = lt.Loop.Call_soon_threadsafe(lt.Loop.Stop)
}

// Join joins the loop thread (no-op when called from that very thread).
func (lt *LoopThread_) Join(timeout float64) {
	if py.Current_loop() == lt.Loop || lt.Thread.Ident() == py.Get_ident() {
		return
	}
	_ = lt.Thread.Join(timeout)
}

// Stop is Request_stop followed by Join.
func (lt *LoopThread_) Stop(timeout float64, drain bool) {
	lt.Request_stop(drain)
	lt.Join(timeout)
}

// PythonAsyncThreadPoolTaskForce is the TaskForce that owns N threads,
// each running its own asyncio loop.
//
// Concurrency knobs:
//
//   - Num_workers -- number of owned threads (each with its own loop).
//   - Max_async_per_thread -- maximum concurrent in-flight tasks per loop.
//     A task consumes a slot from scheduling until the coroutine
//     returns/raises, *minus* the time it spends parked on laila futures.
//   - Sync_workers -- maximum number of sync bodies *executing*
//     concurrently on executor threads (bodies blocked on laila futures do
//     not count).
type PythonAsyncThreadPoolTaskForce struct {
	LAILA_IDENTIFIABLE_TASK_FORCE_

	Backend              string `py:"backend" default:"async_threads"`
	Num_workers          int    `py:"num_workers,ge=1,factory=Default_num_workers_"`
	Max_async_per_thread int    `py:"max_async_per_thread,ge=1" default:"64"`
	Sync_workers         int    `py:"sync_workers,ge=1,factory=Default_sync_workers_"`

	Cv_                  *py.Condition          `py:"_cv"`
	Capacity_cv_         *py.Condition          `py:"_capacity_cv"`
	Stop_flag_           *py.Event              `py:"_stop"`
	Dispatcher_          *py.Thread             `py:"_dispatcher"`
	Dispatching_         atomic.Bool            `py:"_dispatching"`
	Loops_               []*LoopThread_         `py:"_loops"`
	Executor_            *py.ThreadPoolExecutor `py:"_executor"`
	Executor_lock_       *py.Lock               `py:"_executor_lock"`
	Executor_generation_ int                    `py:"_executor_generation"`
	Executor_used_       int                    `py:"_executor_used"`
	Idle_since_          *float64               `py:"_idle_since"`
	Compute_permits_     *py.Semaphore          `py:"_compute_permits"`
}

// PythonAsyncThreadPoolTaskForce_class is the class descriptor.
var PythonAsyncThreadPoolTaskForce_class = py.NewClass(
	"PythonAsyncThreadPoolTaskForce", (*PythonAsyncThreadPoolTaskForce)(nil),
	LAILA_IDENTIFIABLE_TASK_FORCE_class,
).Set_private_default("_loops", func() any { return []*LoopThread_{} })

// New_PythonAsyncThreadPoolTaskForce is
// ``PythonAsyncThreadPoolTaskForce(**data)``.
func New_PythonAsyncThreadPoolTaskForce(kw ...py.Kw) (*PythonAsyncThreadPoolTaskForce, error) {
	tf := PythonAsyncThreadPoolTaskForce_class.New().(*PythonAsyncThreadPoolTaskForce)
	if err := tf.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return tf, nil
}

// Default_num_workers_ is ``max(1, os.cpu_count() or 1)``.
func (tf *PythonAsyncThreadPoolTaskForce) Default_num_workers_() int {
	return max(1, runtime.NumCPU())
}

// Default_sync_workers_ is ``max(4, (os.cpu_count() or 1) * 2)``.
func (tf *PythonAsyncThreadPoolTaskForce) Default_sync_workers_() int {
	return max(4, runtime.NumCPU()*2)
}

// Get_capacity_cv_ exposes ``_capacity_cv`` to the parking helpers
// (Capacity_notifier).
func (tf *PythonAsyncThreadPoolTaskForce) Get_capacity_cv_() *py.Condition { return tf.Capacity_cv_ }

// Get_stop_ exposes ``_stop`` to the parking helpers (Stop_flagged).
func (tf *PythonAsyncThreadPoolTaskForce) Get_stop_() *py.Event { return tf.Stop_flag_ }

// status_ reads the lifecycle status under the instance lock (the Python
// attribute read is GIL-atomic; the port keeps it race-free).
func (tf *PythonAsyncThreadPoolTaskForce) status_() TaskForceStatus {
	tf.Lock()
	defer tf.Unlock()
	return tf.Status
}

// On_start_ spawns the loops and the dispatcher on a cold start.
func (tf *PythonAsyncThreadPoolTaskForce) On_start_() error {
	if strings.ToLower(tf.Backend) != "async_threads" {
		return py.ValueError.New("PythonAsyncThreadPoolTaskForce supports async_threads only.")
	}

	// ``pause()`` -> ``start()`` resumes the existing machinery; only a
	// cold start (or a start after shutdown) spawns loops and a dispatcher.
	if tf.Dispatcher_ != nil && tf.Dispatcher_.Is_alive() {
		return nil
	}

	tag := gid_tag_(tf.Global_id())
	tf.Cv_ = py.NewCondition(nil)
	tf.Capacity_cv_ = py.NewCondition(nil)
	tf.Stop_flag_ = py.NewEvent()
	tf.Executor_lock_ = py.NewLock()
	tf.Executor_ = tf.New_executor_()
	tf.Executor_used_ = 0
	tf.Idle_since_ = nil
	tf.Compute_permits_ = py.NewSemaphore(tf.Sync_workers)
	tf.Loops_ = make([]*LoopThread_, 0, tf.Num_workers)
	for i := 0; i < tf.Num_workers; i++ {
		tf.Loops_ = append(tf.Loops_, New_LoopThread_(fmt.Sprintf("AsyncTF-%s-Loop-%d", tag, i), tf.Max_async_per_thread))
	}
	for _, lt := range tf.Loops_ {
		lt.Loop.Set_default_executor(tf.Executor_)
	}
	tf.Dispatcher_ = py.New_Thread(py.Kw{"target": tf.Loop_, "name": fmt.Sprintf("AsyncTF-%s-Dispatcher", tag), "daemon": true})
	return tf.Dispatcher_.Start()
}

// gid_tag_ is ``self.global_id[-8:]``.
func gid_tag_(gid string) string {
	if len(gid) > 8 {
		return gid[len(gid)-8:]
	}
	return gid
}

// New_executor_ builds a fresh sync executor for the next generation.
func (tf *PythonAsyncThreadPoolTaskForce) New_executor_() *py.ThreadPoolExecutor {
	tag := gid_tag_(tf.Global_id())
	tf.Executor_generation_++
	return py.New_ThreadPoolExecutor(EXECUTOR_MAX_WORKERS_, py.Kw{
		"thread_name_prefix": fmt.Sprintf("AsyncTF-%s-Sync%d", tag, tf.Executor_generation_),
	})
}

// On_pause_ is a safe no-op (see the base-class contract): the status
// flips to ``PAUSED`` so Queue_submit_ rejects new work; in-flight tasks
// keep running and Start resumes without re-spawning threads.
func (tf *PythonAsyncThreadPoolTaskForce) On_pause_() error { return nil }

// Is_own_worker_thread_ is true when called from one of this taskforce's
// loop or body threads.
func (tf *PythonAsyncThreadPoolTaskForce) Is_own_worker_thread_() bool {
	cur := py.Current_loop()
	if cur != nil {
		for _, lt := range tf.Loops_ {
			if lt.Loop == cur {
				return true
			}
		}
	}
	slot := CURRENT_SLOT_.Value()
	return slot != nil && py.Is(slot.Tf, tf.Self())
}

// On_shutdown_ stops the dispatcher, loops and executor
// (``_on_shutdown(*, wait=True, cancel_pending=True)``).
func (tf *PythonAsyncThreadPoolTaskForce) On_shutdown_(wait bool, cancel_pending bool) error {
	// ``wait=True`` on its own is a graceful stop: every task already
	// queued is still dispatched and run to completion (queue drains,
	// then loops drain) before the threads are joined -- the same
	// contract as ``ThreadPoolExecutor.shutdown(wait=True)``.
	// ``cancel_pending=True`` is an explicit request to drop work, so
	// queued tasks are cancelled and in-flight coroutines are cancelled
	// as before (``wait`` then only governs whether we join the threads).
	drain := wait && !cancel_pending
	self_call := tf.Is_own_worker_thread_()
	if drain && self_call {
		// The caller occupies a slot / body thread of this very
		// taskforce: draining would wait for the caller itself.
		async_tf_log_.Warning(
			"shutdown(wait=True) called from a task running on taskforce %s; "+
				"queued tasks are cancelled instead of drained.", tf.Global_id())
		drain = false
	}

	if drain {
		// 1. Keep the dispatcher alive until it has placed every queued
		//    task on a loop.
		tf.Drain_queue_()
		// 2. Let every loop finish what it holds. Nested submissions
		//    made meanwhile are still dispatched (the dispatcher is
		//    alive) onto loops that have not stopped yet.
		for _, lt := range tf.Loops_ {
			lt.Request_stop(true)
		}
		for _, lt := range tf.Loops_ {
			lt.Join(-1)
		}
	}

	if tf.Stop_flag_ != nil {
		tf.Stop_flag_.Set()
	}
	if tf.Cv_ != nil {
		tf.Cv_.Acquire(true, -1)
		tf.Cv_.Notify_all()
		_ = tf.Cv_.Release()
	}
	if tf.Capacity_cv_ != nil {
		tf.Capacity_cv_.Acquire(true, -1)
		tf.Capacity_cv_.Notify_all()
		_ = tf.Capacity_cv_.Release()
	}

	// Always let the dispatcher exit (it polls every 0.1 s and re-queues
	// an item it popped but could not place) *before* the cancel pass,
	// so that pass sees every undispatched task.
	if tf.Dispatcher_ != nil && tf.Dispatcher_.Ident() != py.Get_ident() {
		timeout := 1.0
		if wait {
			timeout = -1
		}
		_ = tf.Dispatcher_.Join(timeout)
	}

	// ``cancel_pending``: explicit drop. After a drain the queue is
	// normally empty; anything left (a nested submit that raced the
	// loop stop) is failed rather than abandoned in ``NOT_STARTED``.
	if cancel_pending || drain {
		tf.Cancel_queued_("Task canceled before dispatch.")
	}

	if !drain {
		for _, lt := range tf.Loops_ {
			timeout := 0.0
			if wait {
				timeout = -1
			}
			lt.Stop(timeout, false)
		}
	}

	var executor *py.ThreadPoolExecutor
	if lock := tf.Executor_lock_; lock != nil {
		lock.Lock_()
		executor = tf.Executor_
		lock.Unlock()
	} else {
		executor = tf.Executor_
	}
	if executor != nil {
		executor.Shutdown(wait && !self_call, py.Kw{"cancel_futures": cancel_pending})
	}
	return nil
}

// Drain_queue_ blocks until the dispatcher has placed every queued task
// on a loop.
func (tf *PythonAsyncThreadPoolTaskForce) Drain_queue_() {
	dispatcher := tf.Dispatcher_
	if dispatcher == nil || !dispatcher.Is_alive() {
		return
	}
	for dispatcher.Is_alive() && (tf.Q_.Len() > 0 || tf.Dispatching_.Load()) {
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
// and empties the queue.
func (tf *PythonAsyncThreadPoolTaskForce) Cancel_queued_(reason string) {
	_, release := tf.Q_.Atomic("cancel")
	defer release()
	for _, it := range tf.Q_.Items() {
		item := it.Value.(py.Tuple)
		kwargs := item[2].(py.Kw)
		fut, _ := kwargs.Get("fut", nil).(*ConcurrentPackageFuture)
		if fut == nil {
			continue
		}
		fut.Set_exception(py.RuntimeError.New(reason))
		fut.Set_result(nil)
		fut.Set_status(FutureStatus_CANCELLED)
	}
	tf.Q_.Clear()
}

// =========================================================
// Sync offload
// =========================================================

// Run_sync runs ``fn(*args)`` on an executor thread under a compute
// permit.
//
// Must be called from a coroutine running on one of this taskforce's
// loops. The current context (slot, resolve chain) is propagated so the
// body can park its slot -- and release its permit -- when it blocks on
// laila futures. Returns the awaited result (Python returns an awaitable
// ``asyncio.Future``; the port's callers always ``await`` it immediately,
// so Run_sync performs the leaf await itself).
func (tf *PythonAsyncThreadPoolTaskForce) Run_sync(fn any, args ...any) (any, error) {
	permits := tf.Compute_permits_
	lock := tf.Executor_lock_
	if tf.Executor_ == nil || permits == nil || lock == nil {
		return nil, py.RuntimeError.New("TaskForce must be running before offloading sync work.")
	}
	if _, err := py.Get_running_loop(); err != nil {
		return nil, err
	}
	pos, kw := py.SplitKw(args)

	body_ := func() (any, error) {
		permit := New_Permit_(permits)
		CURRENT_PERMIT_.Set(permit)
		permit.Acquire()
		defer permit.Release()
		return py.Call(fn, pos, kw)
	}

	// Read and submit under the lock so an idle recycle (see
	// Maybe_recycle_executor_) can never shut the executor down between
	// the two.
	lock.Lock_()
	executor := tf.Executor_
	if executor == nil {
		lock.Unlock()
		return nil, py.RuntimeError.New("TaskForce must be running before offloading sync work.")
	}
	tf.Executor_used_++
	// ``loop.run_in_executor(executor, functools.partial(ctx.run, _body))``:
	// Run_in_executor copies the caller's context onto the worker.
	ctx := py.Copy_context()
	cf, err := executor.Submit(func() (res any, err error) {
		ctx.Run(func() { res, err = body_() })
		return
	})
	lock.Unlock()
	if err != nil {
		return nil, err
	}
	return py.Await_cfuture(cf)
}

// Maybe_recycle_executor_ retires idle sync threads.
//
// ``ThreadPoolExecutor`` never reaps its workers, so every burst of
// concurrent sync bodies would leave that many idle threads alive for the
// life of the taskforce. The dispatcher calls this on each idle poll: once
// nothing is in flight, parked or queued for EXECUTOR_IDLE_RECYCLE_S_, and
// the current executor has actually run something, a fresh (empty)
// executor is swapped in under the same lock Run_sync takes and the old
// one is shut down without waiting -- its idle threads exit on the
// sentinel, a body still running (a parked ``wait()`` inside a sync
// builder) finishes normally and its thread exits afterwards.
func (tf *PythonAsyncThreadPoolTaskForce) Maybe_recycle_executor_() {
	lock := tf.Executor_lock_
	if lock == nil {
		return
	}
	busy := tf.Q_.Len() > 0 || tf.Inflight() > 0 || tf.Parked() > 0
	now := py.Monotonic()
	if busy {
		tf.Idle_since_ = nil
		return
	}
	if tf.Idle_since_ == nil {
		tf.Idle_since_ = &now
		return
	}
	if now-*tf.Idle_since_ < EXECUTOR_IDLE_RECYCLE_S_ {
		return
	}
	lock.Lock_()
	if tf.Executor_used_ == 0 {
		tf.Idle_since_ = &now
		lock.Unlock()
		return
	}
	old := tf.Executor_
	fresh := tf.New_executor_()
	tf.Executor_ = fresh
	tf.Executor_used_ = 0
	tf.Idle_since_ = &now
	lock.Unlock()
	for _, lt := range tf.Loops_ {
		l := lt.Loop
		_, _ = l.Call_soon_threadsafe(func() { l.Set_default_executor(fresh) })
	}
	if old != nil {
		old.Shutdown(false)
	}
}

// =========================================================
// Observability
// =========================================================

// Inflight is the number of slots currently occupied across all loops.
func (tf *PythonAsyncThreadPoolTaskForce) Inflight() int {
	n := 0
	for _, lt := range tf.Loops_ {
		n += lt.Inflight_count()
	}
	return n
}

// Parked is the number of parked tasks currently waiting to re-acquire a
// slot.
func (tf *PythonAsyncThreadPoolTaskForce) Parked() int {
	n := 0
	for _, lt := range tf.Loops_ {
		n += lt.Parked_waiting()
	}
	return n
}

// =========================================================
// Submission
// =========================================================

// Queue_submit_ enqueues ``task(*args, **kwargs)`` and returns its
// future.
func (tf *PythonAsyncThreadPoolTaskForce) Queue_submit_(task any, args ...any) (*ConcurrentPackageFuture, error) {
	if tf.status_() != TaskForceStatus_RUNNING {
		return nil, py.RuntimeError.New("TaskForce must be running before submitting tasks.")
	}
	pos, kwargs := py.SplitKw(args)
	kwargs = kwargs.Copy()

	// Direct ``taskforce.submit`` / ``imap`` callers bypass
	// ``Command.submit``; wrap here as well (idempotent) so a plain sync
	// body is offloaded to the executor instead of blocking a loop thread.
	coro := Ensure_coroutine_function(task)

	fut, err := New_ConcurrentPackageFuture(py.Kw{
		"taskforce_id": tf.Global_id(),
		"policy_id":    tf.Policy_id,
	})
	if err != nil {
		return nil, err
	}

	tf.Cv_.Acquire(true, -1)
	defer tf.Cv_.Release()
	_, release := tf.Q_.Atomic()
	kwargs["task"] = coro
	kwargs["fut"] = fut
	// Snapshot the caller's resolve chain so the child root inherits it
	// even though it runs on another loop thread.
	kwargs[CHAIN_KW_] = RESOLVE_CHAIN_.Value()
	tf.Q_.Setitem(fut.Global_id(), py.Tuple{nil, pos, kwargs})
	release()
	tf.Cv_.Notify(1)

	return fut, nil
}

// Imap submits tasks lazily, yielding each future as it is queued.
//
// Yields the future itself: it already *is* a LAILA_IDENTIFIABLE_FUTURE_,
// so building a separate identity handle per task is pure overhead.
func (tf *PythonAsyncThreadPoolTaskForce) Imap(tasks any) (py.Iterator, error) {
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

// Submit batch-submits tasks (``submit(tasks, wait=False)``): a single
// ConcurrentPackageFuture for one task, a GroupFuture for several; with
// ``wait=True`` the result value(s) instead, releasing the future(s).
func (tf *PythonAsyncThreadPoolTaskForce) Submit(tasks any, args ...any) (any, error) {
	pos, kw := py.SplitKw(args)
	wait := false
	if len(pos) > 0 {
		wait = py.Truthy(pos[0])
	}
	if kw.Has("wait") {
		wait = py.Truthy(kw["wait"])
	}
	task_list := py.Iter(tasks)

	futures := make([]*ConcurrentPackageFuture, 0, len(task_list))
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
		// Return the concrete future (an identity-compatible object)
		// rather than a fresh identity handle; accessors resolve directly
		// instead of scanning every local policy's bank.
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

// =========================================================
// Dispatcher
// =========================================================

// Pick_loop_ reserves a slot on the loop with the lowest in-flight count,
// or nil.
func (tf *PythonAsyncThreadPoolTaskForce) Pick_loop_() *LoopThread_ {
	cap := tf.Max_async_per_thread
	var best *LoopThread_
	best_count := cap
	for _, lt := range tf.Loops_ {
		c := lt.Inflight_count()
		if c < best_count {
			best_count = c
			best = lt
		}
	}
	if best != nil && best.Try_reserve() {
		return best
	}
	return nil
}

// Loop_ is the dispatcher thread body.
func (tf *PythonAsyncThreadPoolTaskForce) Loop_() {
	cv := tf.Cv_
	cap_cv := tf.Capacity_cv_
	stop := tf.Stop_flag_

	for !stop.Is_set() {
		var (
			args   []any
			kwargs py.Kw
		)
		// with cv: wait for work
		cv.Acquire(true, -1)
		for !stop.Is_set() && tf.Q_.Len() == 0 {
			if !cv.Wait(0.1) {
				tf.Maybe_recycle_executor_()
			}
		}
		if stop.Is_set() {
			_ = cv.Release()
			break
		}
		tf.Dispatching_.Store(true)
		_, item, err := tf.Q_.Pop_next()
		_ = cv.Release()
		if err != nil {
			tf.Dispatching_.Store(false)
			continue
		}
		tup := item.(py.Tuple)
		args, _ = tup[1].([]any)
		kwargs = tup[2].(py.Kw)

		func() {
			defer tf.Dispatching_.Store(false)
			var picked *LoopThread_
			for !stop.Is_set() && picked == nil {
				picked = tf.Pick_loop_()
				if picked != nil {
					break
				}
				cap_cv.Acquire(true, -1)
				cap_cv.Wait(0.1)
				_ = cap_cv.Release()
			}

			if stop.Is_set() {
				_, release := tf.Q_.Atomic()
				tf.Q_.Setitem(kwargs["fut"].(*ConcurrentPackageFuture).Global_id(), py.Tuple{nil, args, kwargs})
				release()
				return
			}

			task := kwargs["task"]
			fut := kwargs["fut"].(*ConcurrentPackageFuture)
			chain, _ := kwargs.Get(CHAIN_KW_, nil).([]string)
			user_kwargs := py.Kw{}
			for k, v := range kwargs {
				if k != "task" && k != "fut" && k != CHAIN_KW_ {
					user_kwargs[k] = v
				}
			}

			if err := attempt(func() error {
				coro := tf.Make_runner_coro_(task, args, user_kwargs, fut, picked, chain)
				picked.Submit_coro(coro)
				return nil
			}); err != nil {
				if picked.Release_slot() {
					cap_cv.Acquire(true, -1)
					cap_cv.Notify(1)
					_ = cap_cv.Release()
				}
				fut.Set_exception(err)
				fut.Set_result(nil)
				fut.Set_status(FutureStatus_ERROR)
			}
		}()
	}
	// A popped-but-unplaced item was re-queued above; the dispatcher exits
	// with Dispatching_ cleared.
}

// Make_runner_coro_ builds the runner coroutine for one dispatched task.
func (tf *PythonAsyncThreadPoolTaskForce) Make_runner_coro_(task any, args []any, kwargs py.Kw, fut *ConcurrentPackageFuture, lt *LoopThread_, chain []string) func() (any, error) {
	cap_cv := tf.Capacity_cv_
	self := fut.Self()

	runner_ := func() (any, error) {
		ctx := New_SlotCtx_(tf.Self(), lt)
		slot_token := CURRENT_SLOT_.Set(ctx)
		chain_token := RESOLVE_CHAIN_.Set(append([]string(nil), chain...))
		fut.Set_status(FutureStatus_RUNNING)
		var retErr error
		func() {
			defer func() {
				// finally:
				_ = RESOLVE_CHAIN_.Reset(chain_token)
				_ = CURRENT_SLOT_.Reset(slot_token)
				ctx.Lock.Lock()
				holds := ctx.Holds_slot
				ctx.Holds_slot = false
				ctx.Lock.Unlock()
				if holds && lt.Release_slot() {
					cap_cv.Acquire(true, -1)
					cap_cv.Notify(1)
					_ = cap_cv.Release()
				}
			}()
			var (
				out any
				err error
			)
			func() {
				defer py.Recover(&err)
				call_args := args
				if len(kwargs) > 0 {
					call_args = append(append([]any(nil), args...), kwargs)
				}
				out, err = py.Call(task, call_args, nil)
				if err == nil && py.Iscoroutine(out) {
					out, err = py.Await(out)
				}
			}()
			switch {
			case err == nil:
				fut.Set_exception(nil)
				fut.Set_result(out)
				fut.Set_status(FutureStatus_FINISHED)
			case py.IsInstance(err, py.CancelledError):
				fut.Set_exception(py.RuntimeError.New("Task cancelled during taskforce shutdown."))
				fut.Set_result(nil)
				fut.Set_status(FutureStatus_CANCELLED)
				retErr = err
			default:
				fut.Set_exception(err)
				fut.Set_result(nil)
				fut.Set_status(FutureStatus_ERROR)
			}
		}()
		_ = self
		return nil, retErr
	}
	return runner_
}

func init() {
	py.Register_property("inflight", "parked")
}
