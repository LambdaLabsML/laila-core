package laila

import (
	"sync"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/command/taskforce/base.py -- abstract base task-force
// with lifecycle management and a submission queue.
//
// A *task-force* is laila's name for a worker pool. Concrete subclasses
// (PythonAsyncThreadPoolTaskForce, PythonProcessPoolTaskForce) implement
// the actual scheduling -- this base just nails down:
//
//   - a uniform TaskForceStatus lifecycle
//     (NOT_STARTED -> RUNNING -> [PAUSED] -> STOPPED);
//   - a process-wide registry of *live* taskforces (used by
//     laila.Terminate to sweep orphans -- those not reachable through any
//     policy);
//   - a uniform Submit / Imap contract (subclass-defined);
//   - context-manager sugar so a task-force can be used as ``with tf:``;
//   - a ``_q`` AtomicDict slot subclasses can repurpose as a
//     per-task-force submission queue.
//
// # Lifecycle invariants
//
//   - Model_post_init calls Start for any taskforce constructed in the
//     ``NOT_STARTED`` state, so freshly-instantiated taskforces are
//     immediately usable.
//   - Start and Shutdown only flip the status flag *after* the
//     subclass-specific hook (On_start_ / On_shutdown_) returns, so
//     partial transitions cannot leave a taskforce in an inconsistent
//     state.

// LIVE_TASKFORCES_ is ``_LIVE_TASKFORCES: set[Any]`` (identity set,
// insertion-ordered for deterministic sweeps).
var LIVE_TASKFORCES_ = []any{}

// LIVE_TASKFORCES_LOCK_ guards LIVE_TASKFORCES_.
var LIVE_TASKFORCES_LOCK_ sync.Mutex

// Register_live_taskforce_ tracks tf in the process-wide live set.
//
// The set is consumed by laila.Terminate to sweep "orphan" task-forces --
// those constructed directly (e.g. ``TaskForce.process_pool()`` in user
// code) and never attached to a policy. Without this set those would leak
// their worker threads / processes through to interpreter shutdown.
//
// Thread-safe via LIVE_TASKFORCES_LOCK_.
func Register_live_taskforce_(tf any) {
	LIVE_TASKFORCES_LOCK_.Lock()
	defer LIVE_TASKFORCES_LOCK_.Unlock()
	for _, t := range LIVE_TASKFORCES_ {
		if py.Is(t, tf) {
			return
		}
	}
	LIVE_TASKFORCES_ = append(LIVE_TASKFORCES_, tf)
}

// Unregister_live_taskforce_ removes tf from the live set. Best-effort;
// idempotent. Called during LAILA_IDENTIFIABLE_TASK_FORCE_.Shutdown.
func Unregister_live_taskforce_(tf any) {
	LIVE_TASKFORCES_LOCK_.Lock()
	defer LIVE_TASKFORCES_LOCK_.Unlock()
	for i, t := range LIVE_TASKFORCES_ {
		if py.Is(t, tf) {
			LIVE_TASKFORCES_ = append(LIVE_TASKFORCES_[:i:i], LIVE_TASKFORCES_[i+1:]...)
			return
		}
	}
}

// Live_taskforces_snapshot_ returns a snapshot list of
// currently-registered live task-forces.
//
// The snapshot is detached from the live set, so callers can iterate
// without holding the registry lock and without worrying about concurrent
// mutation.
func Live_taskforces_snapshot_() []any {
	LIVE_TASKFORCES_LOCK_.Lock()
	defer LIVE_TASKFORCES_LOCK_.Unlock()
	return append([]any(nil), LIVE_TASKFORCES_...)
}

func init() { Live_taskforces_snapshot_hook_ = Live_taskforces_snapshot_ }

// Task_force_hooks is the subclass-hook surface the base lifecycle
// dispatches through (``_on_start`` / ``_on_pause`` / ``_on_shutdown``).
type Task_force_hooks interface {
	On_start_() error
	On_pause_() error
	On_shutdown_(wait bool, cancel_pending bool) error
}

// LAILA_IDENTIFIABLE_TASK_FORCE_ is the abstract base for task-forces --
// worker-pool registries with a uniform submit / shutdown contract.
//
// Concrete subclasses provide the actual scheduling backend (asyncio
// thread pool, ProcessPoolExecutor, or remote pools). The base class
// enforces a single status state machine and queue-length introspection
// so callers can write backend-agnostic code.
//
// Fields:
//   - Policy_id (CLI-exempt): the owning policy's gid, or nil.
//   - Rank: dependency rank. Work running on a taskforce may only submit
//     to taskforces of equal or lower rank; the central command rejects
//     upward submissions so the wait-for graph between taskforces stays
//     acyclic. laila's internal taskforce has rank 1, user-facing ones 2.
//   - Status (CLI-exempt): current lifecycle status of this TaskForce.
type LAILA_IDENTIFIABLE_TASK_FORCE_ struct {
	LAILA_CLI_CAPABLE_CLASS_
	LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_

	Policy_id any             `py:"policy_id,cli_exempt"`
	Rank      int             `py:"rank,ge=0" default:"2"`
	Status    TaskForceStatus `py:"status,cli_exempt" default:"not_started"`

	// Q_ is the shared runtime state (available to subclasses): the
	// per-task-force submission queue ``_q``.
	Q_ *AtomicDict `py:"_q"`
}

// LAILA_IDENTIFIABLE_TASK_FORCE_class is the class descriptor.
// ``_scopes`` defaults to ``[_TASK_FORCE_SCOPE]``; ``_wraps_sync`` is a
// ClassVar: whether ``Command.submit`` should wrap plain sync callables
// into the coroutine shim (Ensure_coroutine_function) before handing them
// to this backend. Thread/async backends want the shim so a sync body is
// offloaded to the sync executor; backends that ship the callable to
// *another process* must receive the original, picklable object instead
// (the shim is a local closure and cannot be pickled).
var LAILA_IDENTIFIABLE_TASK_FORCE_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_TASK_FORCE", (*LAILA_IDENTIFIABLE_TASK_FORCE_)(nil),
	LAILA_CLI_CAPABLE_CLASS_class, LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_class,
).
	Set_classvar("_wraps_sync", true).
	Set_private_default("_scopes", func() any { return []string{TASK_FORCE_SCOPE_} }).
	Set_private_default("_q", func() any { return py.Must(New_AtomicDict()) })

// Task_force_base_ exposes the embedded base to code holding a subclass.
func (t *LAILA_IDENTIFIABLE_TASK_FORCE_) Task_force_base_() *LAILA_IDENTIFIABLE_TASK_FORCE_ {
	return t
}

// Wraps_sync_ is ``type(self)._wraps_sync``.
func (t *LAILA_IDENTIFIABLE_TASK_FORCE_) Wraps_sync_() bool {
	v, ok := t.Class_().Classvar("_wraps_sync")
	return !ok || py.Truthy(v)
}

// Model_post_init auto-starts the task-force when constructed in
// ``NOT_STARTED``.
//
// This makes ``MyTaskForce()`` immediately usable -- callers rarely need
// to remember a separate ``.start()`` call. To construct without
// starting, pass ``status=TaskForceStatus.PAUSED`` explicitly.
func (t *LAILA_IDENTIFIABLE_TASK_FORCE_) Model_post_init(ctx any) error {
	if err := t.LAILA_IDENTIFIABLE_OBJECT_.Model_post_init(ctx); err != nil {
		return err
	}
	if err := CLI_CAPABLE_model_post_init(t.Self()); err != nil {
		return err
	}
	if t.Status == TaskForceStatus_NOT_STARTED {
		return t.Start()
	}
	return nil
}

// ---------- Observability ----------

// Queue_len is the number of submitted tasks not yet observed as
// completed.
//
// Backed by the per-instance AtomicDict queue ``_q``; thread-safe by
// virtue of the underlying atomic-dict locking.
func (t *LAILA_IDENTIFIABLE_TASK_FORCE_) Queue_len() int { return t.Q_.Len() }

// Len is ``len(taskforce)``: the current queue length, not the worker
// count. Defined this way so ``if not tf:`` reads naturally as "the
// taskforce has nothing pending".
func (t *LAILA_IDENTIFIABLE_TASK_FORCE_) Len() int { return t.Queue_len() }

// ---------- Public lifecycle ----------

// Start starts the task-force.
//
// Idempotent: a second call on a running task-force is a no-op. Calls the
// subclass-defined On_start_ hook first; only flips the status to
// ``RUNNING`` if the hook returns without raising. The taskforce is also
// added to the process-wide live registry here so laila.Terminate can
// find it.
func (t *LAILA_IDENTIFIABLE_TASK_FORCE_) Start() error {
	if t.Status == TaskForceStatus_RUNNING {
		return nil
	}
	if err := t.Self().(Task_force_hooks).On_start_(); err != nil {
		return err
	}
	t.Status = TaskForceStatus_RUNNING
	Register_live_taskforce_(t.Self())
	return nil
}

// Pause quiesces the task-force without destroying its resources.
//
// Calls On_pause_ and flips the status to ``PAUSED`` -- future
// submissions are still accepted by some backends but not actively
// dispatched until Start is called again. No-op when the task-force is
// not currently ``RUNNING``.
func (t *LAILA_IDENTIFIABLE_TASK_FORCE_) Pause() error {
	if t.Status != TaskForceStatus_RUNNING {
		return nil
	}
	if err := t.Self().(Task_force_hooks).On_pause_(); err != nil {
		return err
	}
	t.Status = TaskForceStatus_PAUSED
	return nil
}

// Shutdown tears down the task-force and releases its resources
// (``shutdown(wait=True, cancel_pending=False)``).
//
// Idempotent: a second call on a stopped task-force still unregisters it
// from the live set (cheap) and returns. The subclass-defined
// On_shutdown_ hook runs inside a ``try`` so the status flag is *always*
// updated to ``STOPPED`` and the live-set registration removed, even if
// the hook raises.
//
//   - wait: block until in-flight tasks finish before returning.
//   - cancel_pending: drop tasks that have been queued but not yet started.
func (t *LAILA_IDENTIFIABLE_TASK_FORCE_) Shutdown(args ...any) (err error) {
	wait, cancel_pending := Shutdown_args_(args)
	if t.Status == TaskForceStatus_STOPPED {
		Unregister_live_taskforce_(t.Self())
		return nil
	}
	defer func() {
		t.Status = TaskForceStatus_STOPPED
		Unregister_live_taskforce_(t.Self())
	}()
	func() {
		defer py.Recover(&err)
		err = t.Self().(Task_force_hooks).On_shutdown_(wait, cancel_pending)
	}()
	return err
}

// Shutdown_args_ decodes ``shutdown(wait=True, cancel_pending=False)``
// from positional bools and/or a trailing py.Kw.
func Shutdown_args_(args []any) (wait bool, cancel_pending bool) {
	pos, kw := py.SplitKw(args)
	wait, cancel_pending = true, false
	if len(pos) > 0 {
		wait = py.Truthy(pos[0])
	}
	if len(pos) > 1 {
		cancel_pending = py.Truthy(pos[1])
	}
	if kw.Has("wait") {
		wait = py.Truthy(kw["wait"])
	}
	if kw.Has("cancel_pending") {
		cancel_pending = py.Truthy(kw["cancel_pending"])
	}
	return wait, cancel_pending
}

// Enter is ``__enter__``: ensure the task-force is running and return
// ``self``.
func (t *LAILA_IDENTIFIABLE_TASK_FORCE_) Enter() (any, error) {
	if err := t.Start(); err != nil {
		return nil, err
	}
	return t.Self(), nil
}

// Exit is ``__exit__``: shut down with ``wait=True`` regardless of
// exception state.
//
// Pending tasks are *not* cancelled by default, matching the executor
// convention from the standard library. Call Shutdown directly if you
// need different semantics.
func (t *LAILA_IDENTIFIABLE_TASK_FORCE_) Exit(exc error) error {
	if err := t.Shutdown(py.Kw{"wait": true}); err != nil {
		return err
	}
	return exc
}

// With runs fn inside ``with tf:``.
func (t *LAILA_IDENTIFIABLE_TASK_FORCE_) With(fn func(tf any) error) (err error) {
	self, err := t.Enter()
	if err != nil {
		return err
	}
	func() {
		defer py.Recover(&err)
		err = fn(self)
	}()
	return t.Exit(err)
}

// ---------- Subclass hooks (must implement) ----------

// On_start_ allocates or starts backend resources (worker threads,
// dispatchers, ...).
//
// Called from Start *before* the status flag flips to ``RUNNING``. If
// this raises, the task-force stays in its previous state and is *not*
// added to the live registry.
func (t *LAILA_IDENTIFIABLE_TASK_FORCE_) On_start_() error {
	return py.NotImplementedError.New()
}

// On_pause_ quiesces backend resources without tearing them down.
//
// Called from Pause. The default base implementation raises
// NotImplementedError; backends that cannot meaningfully pause should
// still implement it as a no-op so Pause is at least safe to call.
func (t *LAILA_IDENTIFIABLE_TASK_FORCE_) On_pause_() error {
	return py.NotImplementedError.New()
}

// On_shutdown_ tears down backend resources, honouring wait and
// cancel_pending.
//
// Called from Shutdown inside a ``try``: any exception raised here is
// allowed to propagate, but the status flag and live-set registration are
// still cleaned up by the caller.
func (t *LAILA_IDENTIFIABLE_TASK_FORCE_) On_shutdown_(wait bool, cancel_pending bool) error {
	return py.NotImplementedError.New()
}

// ---------- Mapping / submit API (must implement) ----------

// Submit batch-submits an iterable of zero-arg callables
// (``submit(funcs, wait=False)``).
//
// Subclasses must accept funcs of any length and obey the return-shape
// contract below so callers can write backend-agnostic code.
//
//	len(funcs)   wait     Return type
//	1            False    A single Future
//	> 1          False    A grouped/aggregate Future (e.g. GroupFuture)
//	1            True     The single result value
//	> 1          True     A list of result values
func (t *LAILA_IDENTIFIABLE_TASK_FORCE_) Submit(funcs any, args ...any) (any, error) {
	return nil, py.NotImplementedError.New()
}

// Imap submits funcs lazily, yielding Future instances in submission
// order.
//
// Unlike Submit, this is an iterator: each ``next(...)`` triggers the
// next submission. Useful for streaming pipelines where the iterable is
// very large or even unbounded.
//
// The yielded futures complete in arbitrary order; pair with Future.Wait
// or ``await`` if you need the results.
func (t *LAILA_IDENTIFIABLE_TASK_FORCE_) Imap(funcs any) (py.Iterator, error) {
	return nil, py.NotImplementedError.New()
}

func init() {
	py.Register_property("queue_len")
}
