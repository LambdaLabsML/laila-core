package laila

import (
	"fmt"
	"sync"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/command/schema/base.py -- base schema for the central
// command sub-system.
//
// Central command is the policy's submission hub. It owns:
//
//   - a registry of LAILA_IDENTIFIABLE_TASK_FORCE_ instances (keyed by
//     gid) plus a designated ``alpha_taskforce`` that receives work when
//     the caller does not specify a target;
//   - a thread-local *guarantee stack* used by ``with laila.guarantee:``
//     to record every future created inside a scope so the scope can
//     block on them all at exit.
//
// Submit is the single entry point for all queued work. Sync callables
// submitted through it are auto-wrapped into trivial coroutine functions
// so the runner sees a uniform contract -- no separate "sync vs async
// submit" plumbing is needed downstream.

// DefaultTaskForce is the class descriptor behind ``DefaultTaskForce``
// (PythonAsyncThreadPoolTaskForce).
var DefaultTaskForce = PythonAsyncThreadPoolTaskForce_class

// New_DefaultTaskForce is ``DefaultTaskForce(**kw)``.
func New_DefaultTaskForce(kw ...py.Kw) (any, error) {
	return newInstance(DefaultTaskForce, py.MergeKw(kw...))
}

// Inflight_build_key_ is the ``(entry gid, id(entry))`` key of
// ``_inflight_builds``.
type Inflight_build_key_ struct {
	Gid string
	Id  uintptr
}

// LAILA_IDENTIFIABLE_CENTRAL_COMMAND_ is central command -- task-force
// registry, work submission, and guarantee scopes.
//
// # Owns
//
//   - Taskforces: ``{gid: TaskForce}`` -- every registered taskforce.
//   - Alpha_taskforce: the gid of the default (user-facing) taskforce.
//   - Internal_taskforce: the gid of the taskforce laila's own machinery
//     (memory fetches/writes, manifest realisation) runs on. Both are
//     created automatically by Model_post_init if no taskforces were
//     provided.
//   - Policy_id: back-reference to the owning policy's gid.
//
// # Provides
//
//   - Submit -- the universal submission API.
//   - Shutdown -- shuts down every registered taskforce.
//   - The thread-local guarantee stack hooks (Guarantee_enter_,
//     Guarantee_exit_, Register_future_with_active_guarantees_) used by
//     the ``laila.guarantee`` context manager.
type LAILA_IDENTIFIABLE_CENTRAL_COMMAND_ struct {
	LAILA_CLI_CAPABLE_CLASS_
	LAILA_IDENTIFIABLE_OBJECT_

	Taskforces         *py.Dict `py:"taskforces,cli_exempt,factory=New_taskforces_"`
	Alpha_taskforce    any      `py:"alpha_taskforce"`
	Internal_taskforce any      `py:"internal_taskforce"`
	Policy_id          any      `py:"policy_id,cli_exempt"`

	Guarantee_local_ *py.Local `py:"_guarantee_local"`
	// ``laila.build`` coalescing: ``(entry gid, id(entry)) -> in-flight
	// future`` so concurrent builds of one entry object run its
	// constitution once.
	Inflight_builds_ map[Inflight_build_key_]any `py:"_inflight_builds"`
	Inflight_lock_   *sync.Mutex                 `py:"_inflight_lock"`
}

// LAILA_IDENTIFIABLE_CENTRAL_COMMAND_class is the class descriptor;
// ``_scopes`` defaults to ``[_CENTRAL_COMMAND_SCOPE]``.
var LAILA_IDENTIFIABLE_CENTRAL_COMMAND_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_CENTRAL_COMMAND", (*LAILA_IDENTIFIABLE_CENTRAL_COMMAND_)(nil),
	LAILA_CLI_CAPABLE_CLASS_class, LAILA_IDENTIFIABLE_OBJECT_class,
).
	Set_private_default("_scopes", func() any { return []string{CENTRAL_COMMAND_SCOPE_} }).
	Set_private_default("_guarantee_local", func() any { return py.NewLocal() }).
	Set_private_default("_inflight_builds", func() any { return map[Inflight_build_key_]any{} }).
	Set_private_default("_inflight_lock", func() any { return &sync.Mutex{} })

// DefaultCentralCommand is the class descriptor behind
// ``DefaultCentralCommand``.
var DefaultCentralCommand = LAILA_IDENTIFIABLE_CENTRAL_COMMAND_class

// New_LAILA_IDENTIFIABLE_CENTRAL_COMMAND_ is
// ``_LAILA_IDENTIFIABLE_CENTRAL_COMMAND(**data)``.
func New_LAILA_IDENTIFIABLE_CENTRAL_COMMAND_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_CENTRAL_COMMAND_, error) {
	c := LAILA_IDENTIFIABLE_CENTRAL_COMMAND_class.New().(*LAILA_IDENTIFIABLE_CENTRAL_COMMAND_)
	if err := c.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return c, nil
}

// New_DefaultCentralCommand is ``DefaultCentralCommand(**kw)``.
func New_DefaultCentralCommand(kw ...py.Kw) (*LAILA_IDENTIFIABLE_CENTRAL_COMMAND_, error) {
	return New_LAILA_IDENTIFIABLE_CENTRAL_COMMAND_(kw...)
}

// New_taskforces_ is the ``taskforces`` default factory (``dict``).
func (c *LAILA_IDENTIFIABLE_CENTRAL_COMMAND_) New_taskforces_() *py.Dict { return py.NewDict() }

// Central_command_base_ exposes the embedded base to code holding a
// subclass.
func (c *LAILA_IDENTIFIABLE_CENTRAL_COMMAND_) Central_command_base_() *LAILA_IDENTIFIABLE_CENTRAL_COMMAND_ {
	return c
}

// Model_post_init creates the default taskforces if none were registered.
//
// When Taskforces is empty, auto-creates two ``DefaultTaskForce``
// instances (PythonAsyncThreadPoolTaskForce):
//
//   - the **alpha** taskforce (``rank=2``) receives user work submitted
//     without an explicit ``taskforce_id``;
//   - the **internal** taskforce (``rank=1``) runs laila's own machinery
//     (``remember`` / ``memorize`` / ``forget`` fetches, manifest
//     realisation, ``laila.build``).
//
// The split keeps a flood of user jobs from starving the internal fetches
// those very jobs depend on, and the rank guard in Submit keeps the
// dependency between the two acyclic. Users can still register additional
// taskforces (e.g. process pools) via Add_taskforce. When the registry is
// restored from an environment that predates the split (one taskforce),
// Internal_taskforce falls back to Alpha_taskforce.
func (c *LAILA_IDENTIFIABLE_CENTRAL_COMMAND_) Model_post_init(ctx any) error {
	if err := c.LAILA_IDENTIFIABLE_OBJECT_.Model_post_init(ctx); err != nil {
		return err
	}
	if err := CLI_CAPABLE_model_post_init(c.Self()); err != nil {
		return err
	}
	if c.Taskforces.Len() == 0 {
		alpha, err := New_DefaultTaskForce(py.Kw{"policy_id": c.Policy_id, "rank": 2})
		if err != nil {
			return err
		}
		c.Taskforces.Set(global_id_of_(alpha), alpha)
		c.Alpha_taskforce = global_id_of_(alpha)

		internal, err := New_DefaultTaskForce(py.Kw{"policy_id": c.Policy_id, "rank": 1})
		if err != nil {
			return err
		}
		c.Taskforces.Set(global_id_of_(internal), internal)
		c.Internal_taskforce = global_id_of_(internal)
	}

	if py.IsNone(c.Alpha_taskforce) || !c.Taskforces.Contains(c.Alpha_taskforce) {
		c.Alpha_taskforce = c.Taskforces.Keys()[0]
	}
	if py.IsNone(c.Internal_taskforce) || !c.Taskforces.Contains(c.Internal_taskforce) {
		c.Internal_taskforce = c.Alpha_taskforce
	}
	return nil
}

// global_id_of_ is ``obj.global_id`` for any identifiable object.
func global_id_of_(obj any) string {
	if g, ok := obj.(interface{ Global_id() string }); ok {
		return g.Global_id()
	}
	return py.Str(getattr(obj, "global_id"))
}

// Add_taskforce registers a task-force with this central command.
//
// Subsequent Submit calls can target it via ``taskforce_id=<gid>``.
// Re-registering the same gid silently overwrites the previous instance,
// which is sometimes useful when hot-swapping a taskforce implementation.
func (c *LAILA_IDENTIFIABLE_CENTRAL_COMMAND_) Add_taskforce(taskforce any) {
	c.Taskforces.Set(global_id_of_(taskforce), taskforce)
}

// Guarantee_stack_ returns the thread-local stack of open guarantee scopes
// (a list of ``{gid: Future}`` dicts).
//
// The stack lives on a ``threading.local`` so each thread has its own
// independent set of scopes -- nested ``with laila.guarantee:`` blocks in
// one thread don't see futures created on another. Created lazily on
// first access.
func (c *LAILA_IDENTIFIABLE_CENTRAL_COMMAND_) Guarantee_stack_() *py.List {
	stack, _ := c.Guarantee_local_.GetOr("stack", nil).(*py.List)
	if stack == nil {
		stack = py.NewList()
		c.Guarantee_local_.Set("stack", stack)
	}
	return stack
}

// Guarantee_enter_ pushes a fresh empty scope onto the calling thread's
// guarantee stack.
//
// Called by the ``laila.guarantee`` context manager on ``__enter__``.
// Subsequent futures created on this thread are registered into this
// scope by Register_future_with_active_guarantees_ until the scope is
// popped.
func (c *LAILA_IDENTIFIABLE_CENTRAL_COMMAND_) Guarantee_enter_() {
	c.Guarantee_stack_().Append(py.NewDict())
}

// Guarantee_exit_ pops the top guarantee scope and returns its
// accumulated futures.
//
// Returns ``[]`` (rather than raising) when no scope is open, so that
// nested context-manager unwinding paths are robust to partial setup.
func (c *LAILA_IDENTIFIABLE_CENTRAL_COMMAND_) Guarantee_exit_() []any {
	stack := c.Guarantee_stack_()
	if stack.Len() == 0 {
		return []any{}
	}
	top, _ := stack.Pop(-1)
	return top.(*py.Dict).Values()
}

// Register_future_with_active_guarantees_ records future in every
// guarantee scope open on this thread.
//
// Called by every Future / GroupFuture inside Model_post_init so that any
// future created while ``with laila.guarantee:`` is in effect is
// automatically tracked for the scope's join-on-exit pass. No-op when no
// scopes are open (the common case).
func (c *LAILA_IDENTIFIABLE_CENTRAL_COMMAND_) Register_future_with_active_guarantees_(future any) {
	stack, _ := c.Guarantee_local_.GetOr("stack", nil).(*py.List)
	if stack == nil || stack.Len() == 0 {
		return
	}
	for _, scope_futures := range stack.Items() {
		scope_futures.(*py.Dict).Set(global_id_of_(future), future)
	}
}

// Submit submits zero-argument callables to a task-force for execution
// (``submit(tasks, wait=False, *, taskforce_id=None)``).
//
// # Calling protocol
//
//   - With one task and ``wait=False``: returns a single Future.
//   - With many tasks and ``wait=False``: returns a GroupFuture
//     aggregating per-task futures.
//   - With ``wait=True``: blocks until all tasks finish, then returns the
//     lone result (one task) or a list of results (many tasks).
//
// Sync callables are auto-wrapped into coroutine functions at submission
// time so the runner sees a uniform awaitable contract; the wrapped sync
// body is offloaded to the taskforce's sync executor so it never blocks a
// loop thread. Backends that execute in *another process*
// (PythonProcessPoolTaskForce) receive the callables untouched, since the
// wrapper is a local closure that cannot be pickled -- submit registered
// process tasks (or a py.Partial of one) to those.
//
// ``wait=True`` consumes the futures: once the results are collected the
// per-task futures (and the aggregating group, if any) are released from
// the policy's ``future_bank`` because no handle is returned through
// which the caller could release them. Open laila.Guarantee scopes keep
// their own direct references and are unaffected.
//
// # Nested submission
//
// Submitting from *inside* a running task is supported and
// deadlock-free: while the parent awaits (or blocks in ``wait()`` on) the
// child future its taskforce slot is parked, so the child can always be
// dispatched -- see policy.central.command.schema.parking.
//
// # Guards
//
//   - Calling Submit from inside a function decorated
//     ``@no_command_submit`` raises NestedCommandSubmitError.
//   - Submitting from a task running on taskforce *A* to a taskforce *B*
//     with a strictly higher ``rank`` raises NestedCommandSubmitError:
//     dependencies between taskforces must point downward (user ->
//     internal), never upward, so the inter-taskforce wait-for graph stays
//     acyclic.
//
// Raises KeyError if ``taskforce_id`` is supplied but not registered.
func (c *LAILA_IDENTIFIABLE_CENTRAL_COMMAND_) Submit(tasks any, args ...any) (any, error) {
	pos, kw := py.SplitKw(args)
	wait := false
	if len(pos) > 0 {
		wait = py.Truthy(pos[0])
	}
	if kw.Has("wait") {
		wait = py.Truthy(kw["wait"])
	}
	taskforce_id := kw.Get("taskforce_id", nil)

	if err := Check_no_pending_submit_owner_(); err != nil {
		return nil, err
	}

	if py.IsNone(taskforce_id) {
		taskforce_id = c.Alpha_taskforce
	}

	target, err := c.Taskforces.Getitem(taskforce_id)
	if err != nil {
		return nil, err
	}
	if err := c.Check_rank_(target); err != nil {
		return nil, err
	}

	var wrapped []any
	if wraps_sync_(target) {
		for _, t := range py.Iter(tasks) {
			wrapped = append(wrapped, Ensure_coroutine_function(t))
		}
	} else {
		wrapped = append([]any{}, py.Iter(tasks)...)
	}
	if wrapped == nil {
		wrapped = []any{}
	}

	if s, ok := target.(interface {
		Submit(tasks any, args ...any) (any, error)
	}); ok {
		return s.Submit(wrapped, py.Kw{"wait": wait})
	}
	return callm(target, "submit", wrapped, py.Kw{"wait": wait})
}

// wraps_sync_ is ``getattr(target, "_wraps_sync", True)``.
func wraps_sync_(target any) bool {
	if w, ok := target.(interface{ Wraps_sync_() bool }); ok {
		return w.Wraps_sync_()
	}
	v, err := py.Getattr(target, "_wraps_sync")
	if err != nil {
		return true
	}
	return py.Truthy(v)
}

// Check_rank_ rejects a submission that would point *up* the taskforce
// ranks.
//
// Only applies when the caller is itself running inside a slot of one of
// this command's taskforces; submissions from user threads are never
// restricted.
func (c *LAILA_IDENTIFIABLE_CENTRAL_COMMAND_) Check_rank_(target any) error {
	slot := CURRENT_SLOT_.Value()
	if slot == nil {
		return nil
	}
	source := slot.Tf
	if py.Is(source, target) || !c.Taskforces.Contains(getattr(source, "global_id")) {
		return nil
	}
	src_rank := rank_of_(source)
	dst_rank := rank_of_(target)
	if dst_rank > src_rank {
		return NestedCommandSubmitError.New(fmt.Sprintf(
			"Task running on taskforce %s (rank %d) "+
				"may not submit to taskforce %s (rank %d): "+
				"submissions must target equal or lower rank so the taskforce "+
				"dependency graph stays acyclic.",
			global_id_of_(source), src_rank, global_id_of_(target), dst_rank))
	}
	return nil
}

// rank_of_ is ``getattr(tf, "rank", 2)``.
func rank_of_(tf any) int {
	if b, ok := tf.(interface {
		Task_force_base_() *LAILA_IDENTIFIABLE_TASK_FORCE_
	}); ok {
		return b.Task_force_base_().Rank
	}
	v, err := py.Getattr(tf, "rank")
	if err != nil {
		return 2
	}
	r, err := py.Int(v)
	if err != nil {
		return 2
	}
	return r
}

// Shutdown shuts down every registered task-force
// (``shutdown(wait=True, cancel_pending=False)``).
//
// Iterates over ``self.taskforces.values()`` and calls ``shutdown(wait,
// cancel_pending)`` on each. Order is iteration order (currently
// insertion order). Failures in one taskforce do NOT prevent the others
// from being shut down -- exceptions propagate after the iteration to the
// caller, but the laila.Terminate call site catches them so partial
// teardown still completes.
//
//   - wait: block until workers finish in-flight tasks.
//   - cancel_pending: drop tasks that have been queued but not yet
//     started. Already-running tasks are not cancelled regardless of this
//     flag.
func (c *LAILA_IDENTIFIABLE_CENTRAL_COMMAND_) Shutdown(args ...any) error {
	wait, cancel_pending := Shutdown_args_(args)
	for _, tf := range c.Taskforces.Values() {
		var err error
		if s, ok := tf.(interface{ Shutdown(args ...any) error }); ok {
			err = s.Shutdown(py.Kw{"wait": wait, "cancel_pending": cancel_pending})
		} else {
			_, err = callm(tf, "shutdown", py.Kw{"wait": wait, "cancel_pending": cancel_pending})
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Await is ``__await__``: async awaiting is not yet supported.
func (c *LAILA_IDENTIFIABLE_CENTRAL_COMMAND_) Await() (any, error) {
	return nil, py.NotImplementedError.New()
}
