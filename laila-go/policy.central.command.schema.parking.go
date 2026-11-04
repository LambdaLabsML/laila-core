package laila

import (
	"strings"
	"sync"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/command/schema/parking.py -- slot parking: release a
// taskforce slot while awaiting another future.
//
// # Problem
//
// A taskforce has a bounded number of execution *slots* (for the default
// PythonAsyncThreadPoolTaskForce that is ``num_workers *
// max_async_per_thread``). A task that holds a slot and then blocks on a
// *child* future that itself needs a slot on the same taskforce creates a
// hold-and-wait dependency. When every slot is held by such a parent, no
// child can ever be dispatched: a classic deadlock. Nesting is pervasive in
// laila -- ``laila.remember`` submits per-entry fetches, constitution
// bodies call ``manifest.realized`` which remembers children, and users
// legitimately ``await laila.remember(...)`` from inside their own
// submitted coroutines -- so this must be solved once, in the scheduler,
// not by asking every caller to reason about slot budgets.
//
// # Solution
//
// Every runner coroutine publishes a SlotCtx_ in the CURRENT_SLOT_ context
// variable. Whenever code that is running *inside* a slot waits on a laila
// future (``await fut``, ``fut.wait()``, ``fut.result``/``fut.data``), the
// wait is wrapped in Park_async or Park_sync. Those helpers
//
//  1. hand the slot back to the loop thread (Park_enter_), so the
//     dispatcher -- or a previously parked sibling -- can use it, and
//  2. re-acquire a slot before returning to the caller (Park_exit_*), so
//     the invariant "at most ``cap`` coroutines *run* per loop thread"
//     still holds for the CPU/IO work between waits.
//
// While parked, the coroutine is suspended at an ``await`` and consumes no
// slot; only the parent's stack frame (memory) is retained. Because a
// parked task can never hold a slot while it waits for a child, the
// wait-for graph has no slot cycle and progress is guaranteed as long as
// the leaf tasks terminate.
//
// Re-acquisition is *prioritised*: a loop thread hands a freed slot to a
// waiting parked task before decrementing its in-flight count, so parked
// parents resume ahead of newly dispatched roots. This bounds the number of
// simultaneously live trees instead of letting the dispatcher keep opening
// new ones.
//
// # Nesting and threads
//
// Parking is reentrant per slot. A depth counter in SlotCtx_ ensures that
// a wait nested inside another wait (e.g. ``GroupFuture.wait`` calling each
// child's ``wait``) releases the slot exactly once and re-acquires it
// exactly once, at the outermost wait. The context is propagated through
// py.To_thread / Run_in_executor (``contextvars.copy_context``), so a
// *synchronous* constitution body running on an executor thread that calls
// ``fut.wait()`` parks the slot of the coroutine that offloaded it.
//
// # Compute permits
//
// Sync bodies are offloaded to an executor thread. Threads are cheap to
// park on but expensive to run concurrently (GIL contention), so the
// taskforce bounds the number of *executing* sync bodies with a semaphore
// of ``sync_workers`` permits rather than bounding the number of threads.
// CURRENT_PERMIT_ carries the permit held by the current executor thread;
// Park_sync releases it while the body blocks on a laila future and
// re-acquires it afterwards. A body that is blocked therefore consumes
// neither a slot nor a permit, which is what makes arbitrarily deep sync
// nesting safe.
//
// # Cycle guard
//
// RESOLVE_CHAIN_ carries the tuple of entry global-ids that are currently
// being resolved (remembered or built) along the *causal* chain of nested
// submissions. It is snapshotted at submit time and re-installed by the
// runner, so a child root task inherits its parent's chain even though it
// runs on a different loop thread. ``laila.remember`` and ``laila.build``
// consult it to raise CyclicDependencyError instead of recursing forever
// when a constitution ends up depending on itself.

// CyclicDependencyError is raised when an entry resolution depends
// (transitively) on itself.
//
// Detected via RESOLVE_CHAIN_: if ``laila.remember`` or ``laila.build`` is
// invoked for a global-id that is already on the current chain of nested
// resolutions, the dependency graph has a cycle and no amount of
// scheduling can complete it.
var CyclicDependencyError = py.NewType("CyclicDependencyError", py.RuntimeError)

// Slot_loop_thread is the duck type of SlotCtx_.Lt: the loop thread whose
// slot the context represents (needs ``release_slot``,
// ``try_reserve_or_wait``, ``cancel_wait``).
type Slot_loop_thread interface {
	// Release_slot hands the slot back; returns True when the dispatcher
	// should be notified that capacity became available.
	Release_slot() bool
	// Try_reserve_or_wait grabs a free slot immediately (True) or queues
	// cb to be invoked once a slot is handed to the caller (False).
	Try_reserve_or_wait(cb *Slot_waiter) bool
	// Cancel_wait removes cb from the wait queue; True when it was still
	// queued (i.e. no slot had been handed over).
	Cancel_wait(cb *Slot_waiter) bool
}

// Slot_waiter is a queued slot re-acquisition callback (identity-compared,
// which is what the Python code relies on with ``cb`` closures).
type Slot_waiter struct{ Fn func() }

// Capacity_notifier is the ``_capacity_cv`` duck-type of SlotCtx_.Tf.
type Capacity_notifier interface {
	Get_capacity_cv_() *py.Condition
}

// Stop_flagged is the ``_stop`` duck-type of SlotCtx_.Tf.
type Stop_flagged interface {
	Get_stop_() *py.Event
}

// SlotCtx_ is bookkeeping for one occupied taskforce slot.
//
//   - Tf: the owning taskforce (duck-typed: needs ``_capacity_cv`` and
//     ``_stop``; see Capacity_notifier and Stop_flagged).
//   - Lt: the loop thread whose slot this context represents.
//   - Depth: number of nested parks currently active. Only the transition
//     ``0 -> 1`` releases the slot and only ``1 -> 0`` re-acquires it.
//   - Holds_slot: whether this context currently owns a slot on Lt.
//   - Lock: guards Depth and Holds_slot -- a sync body may park from an
//     executor thread while the owning coroutine's loop thread is
//     inspecting the same context.
type SlotCtx_ struct {
	Tf         any
	Lt         Slot_loop_thread
	Depth      int
	Holds_slot bool
	Lock       sync.Mutex
}

// New_SlotCtx_ is ``_SlotCtx(tf, lt)``.
func New_SlotCtx_(tf any, lt Slot_loop_thread) *SlotCtx_ {
	return &SlotCtx_{Tf: tf, Lt: lt, Depth: 0, Holds_slot: true}
}

// CURRENT_SLOT_ is the slot context of the runner coroutine the current
// code is executing under.
//
// nil when the caller is not inside any taskforce slot (main thread, plain
// user threads, the dispatcher). Propagates into executor threads via
// ``contextvars.copy_context`` so sync bodies can park too.
var CURRENT_SLOT_ = py.NewContextVar[*SlotCtx_]("_laila_current_slot", nil)

// RESOLVE_CHAIN_ is the tuple of entry global-ids being resolved along the
// current causal chain.
var RESOLVE_CHAIN_ = py.NewContextVar[[]string]("_laila_resolve_chain", nil)

// Permit_ is a compute permit held by a sync body running on an executor
// thread.
//
// Sem is the taskforce-wide semaphore bounding concurrently *executing*
// sync bodies; Held tracks whether this thread currently owns one of its
// permits (released while parked).
type Permit_ struct {
	Sem  *py.Semaphore
	Held bool
}

// New_Permit_ is ``_Permit(sem)``.
func New_Permit_(sem *py.Semaphore) *Permit_ { return &Permit_{Sem: sem, Held: false} }

// Acquire takes the permit if not already held.
func (p *Permit_) Acquire() {
	if !p.Held {
		p.Sem.Acquire(true, -1)
		p.Held = true
	}
}

// Release gives the permit back if held.
func (p *Permit_) Release() {
	if p.Held {
		p.Held = false
		_ = p.Sem.Release(1)
	}
}

// CURRENT_PERMIT_ is the compute permit of the executor thread the current
// sync body runs on.
var CURRENT_PERMIT_ = py.NewContextVar[*Permit_]("_laila_current_permit", nil)

// ---------------------------------------------------------------------------
// release / re-acquire primitives
// ---------------------------------------------------------------------------

func Notify_capacity_(tf any) {
	n, ok := tf.(Capacity_notifier)
	if !ok {
		return
	}
	cv := n.Get_capacity_cv_()
	if cv == nil {
		return
	}
	cv.Acquire(true, -1)
	defer cv.Release()
	cv.Notify(1)
}

// Park_enter_ increments park depth; on the outermost park hands the slot
// back.
func Park_enter_(ctx *SlotCtx_) {
	ctx.Lock.Lock()
	ctx.Depth++
	if ctx.Depth > 1 || !ctx.Holds_slot {
		ctx.Lock.Unlock()
		return
	}
	ctx.Holds_slot = false
	ctx.Lock.Unlock()
	if ctx.Lt.Release_slot() {
		Notify_capacity_(ctx.Tf)
	}
}

func Stop_requested_(ctx *SlotCtx_) bool {
	s, ok := ctx.Tf.(Stop_flagged)
	if !ok {
		return false
	}
	stop := s.Get_stop_()
	return stop != nil && stop.Is_set()
}

// Park_exit_async_ decrements park depth; on the outermost exit
// re-acquires a slot.
//
// If the taskforce is shutting down the re-acquire is skipped: the task
// will be cancelled by the loop teardown anyway and must not block on a
// slot that will never be handed out.
func Park_exit_async_(ctx *SlotCtx_) error {
	ctx.Lock.Lock()
	ctx.Depth--
	if ctx.Depth > 0 || ctx.Holds_slot {
		ctx.Lock.Unlock()
		return nil
	}
	ctx.Lock.Unlock()
	if Stop_requested_(ctx) {
		return nil
	}
	loop, err := py.Get_running_loop()
	if err != nil {
		return err
	}
	fut := loop.New_future()

	wake_ := func() {
		if !fut.Done() {
			_ = fut.Set_result(nil)
		}
	}

	cb := &Slot_waiter{Fn: func() {
		// Loop closed during shutdown; nothing to resume (RuntimeError
		// swallowed).
		_, _ = loop.Call_soon_threadsafe(wake_)
	}}

	if ctx.Lt.Try_reserve_or_wait(cb) {
		ctx.Lock.Lock()
		ctx.Holds_slot = true
		ctx.Lock.Unlock()
		return nil
	}
	if _, err := fut.Await(); err != nil {
		// Cancelled while waiting. If the slot was already handed to us we
		// own it and the runner's finally-block must release it.
		if !ctx.Lt.Cancel_wait(cb) {
			ctx.Lock.Lock()
			ctx.Holds_slot = true
			ctx.Lock.Unlock()
		}
		return err
	}
	ctx.Lock.Lock()
	ctx.Holds_slot = true
	ctx.Lock.Unlock()
	return nil
}

// Park_exit_sync_ is the sync counterpart of Park_exit_async_ for executor
// threads.
func Park_exit_sync_(ctx *SlotCtx_) {
	ctx.Lock.Lock()
	ctx.Depth--
	if ctx.Depth > 0 || ctx.Holds_slot {
		ctx.Lock.Unlock()
		return
	}
	ctx.Lock.Unlock()
	if Stop_requested_(ctx) {
		return
	}
	ev := py.NewEvent()
	cb := &Slot_waiter{Fn: ev.Set}
	if ctx.Lt.Try_reserve_or_wait(cb) {
		ctx.Lock.Lock()
		ctx.Holds_slot = true
		ctx.Lock.Unlock()
		return
	}
	for !ev.Wait(0.05) {
		if Stop_requested_(ctx) {
			if ctx.Lt.Cancel_wait(cb) {
				return
			}
			break
		}
	}
	ctx.Lock.Lock()
	ctx.Holds_slot = true
	ctx.Lock.Unlock()
}

// ---------------------------------------------------------------------------
// public helpers
// ---------------------------------------------------------------------------

// Park_async awaits awaitable, parking the current slot (if any) for the
// duration.
//
// Outside a taskforce slot this is a plain ``await``.
func Park_async(awaitable any) (any, error) {
	ctx := CURRENT_SLOT_.Value()
	if ctx == nil {
		return py.Await(awaitable)
	}
	Park_enter_(ctx)
	res, err := py.Await(awaitable)
	if exitErr := Park_exit_async_(ctx); exitErr != nil {
		// ``finally: await _park_exit_async(ctx)`` -- an exception raised
		// in the finally clause replaces the in-flight one.
		return nil, exitErr
	}
	return res, err
}

// Park_sync calls ``fn(*args)``, parking the current slot (if any).
//
// Intended for blocking waits executed on executor threads that were
// spawned from inside a slot (py.To_thread copies the context, so
// CURRENT_SLOT_ is visible there). Outside a slot this is a plain call.
func Park_sync(fn func() (any, error)) (any, error) {
	ctx := CURRENT_SLOT_.Value()
	permit := CURRENT_PERMIT_.Value()
	if ctx == nil && permit == nil {
		return fn()
	}
	// Give up the compute permit first (never hold a permit while waiting),
	// then the slot.
	released_permit := permit != nil && permit.Held
	if released_permit {
		permit.Release()
	}
	if ctx != nil {
		Park_enter_(ctx)
	}
	defer func() {
		// Re-acquire in the opposite order: slot, then permit. A thread that
		// holds a slot and waits for a permit cannot deadlock because every
		// permit holder is executing (and will finish or park).
		if ctx != nil {
			Park_exit_sync_(ctx)
		}
		if released_permit {
			permit.Acquire()
		}
	}()
	return fn()
}

// Check_resolve_cycle raises CyclicDependencyError if any id is already
// being resolved.
func Check_resolve_cycle(global_ids ...string) error {
	chain := RESOLVE_CHAIN_.Value()
	if len(chain) == 0 {
		return nil
	}
	for _, gid := range global_ids {
		for _, c := range chain {
			if c == gid {
				return CyclicDependencyError.Newf(
					"Cyclic dependency: %s is already being resolved on the "+
						"current chain (%s -> %s).", gid, strings.Join(chain, " -> "), gid)
			}
		}
	}
	return nil
}

// Resolve_chain_with returns the current chain extended with global_ids
// (no mutation).
func Resolve_chain_with(global_ids ...string) []string {
	chain := RESOLVE_CHAIN_.Value()
	out := make([]string, 0, len(chain)+len(global_ids))
	out = append(out, chain...)
	return append(out, global_ids...)
}
