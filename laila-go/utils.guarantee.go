package laila

import (
	"sync"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// utils/guarantee.py -- guarantee context managers that block until
// tracked futures complete.
//
// The "guarantee" pattern is laila's lexical answer to "I want to be sure
// every async operation kicked off in *this block* has finished before
// the block returns". Two mirror implementations live here:
//
//   - Guarantee_ (``with laila.guarantee:``): on exit every future created
//     inside the block is awaited via Future.wait; the first exception
//     observed is re-raised.
//   - AsyncGuarantee_ (``async with laila.guarantee_async:``): same idea
//     with awaits, plus a background watcher task that immediately cancels
//     the parent task if any in-scope future errors.
//
// Both co-operate with _LAILA_IDENTIFIABLE_CENTRAL_COMMAND's
// ``_guarantee_stack`` / ``_guarantee_enter`` / ``_guarantee_exit``.
//
// Port note: ``with laila.guarantee: body`` is
//
//	err := laila.Guarantee.With(func() error { body; return nil })
//
// or, spelled out, ``Guarantee.Enter()`` ... ``Guarantee.Exit(bodyErr)``.

// Guarantee_ is the synchronous context manager that waits for futures
// created in its scope.
type Guarantee_ struct{}

// Enter is ``__enter__``: pushes a fresh frame onto the active policy's
// guarantee stack. Newly-submitted futures register with this frame until
// Exit pops it.
func (g *Guarantee_) Enter() (*Guarantee_, error) {
	cmd, err := active_command_()
	if err != nil {
		return nil, err
	}
	if _, err := callm(cmd, "_guarantee_enter"); err != nil {
		return nil, err
	}
	return g, nil
}

// Exit is ``__exit__(exc_type, exc, tb)``: pops the frame and
// synchronously waits for every registered future. The caller's error
// (if any) is returned unchanged; when the body did not fail but one of
// the in-scope futures did, the first wait-error is returned instead.
func (g *Guarantee_) Exit(exc error) error {
	cmd, err := active_command_()
	if err != nil {
		return err
	}
	created, err := callm(cmd, "_guarantee_exit")
	if err != nil {
		return err
	}
	var wait_errors []error
	for _, future := range py.Iter(created) {
		if _, werr := callm(future, "wait", py.Kw{"timeout": nil}); werr != nil {
			wait_errors = append(wait_errors, werr)
		}
	}
	if exc != nil {
		return exc
	}
	if len(wait_errors) > 0 {
		return wait_errors[0]
	}
	return nil
}

// With runs fn inside the guarantee scope (``with laila.guarantee:``).
func (g *Guarantee_) With(fn func() error) (err error) {
	if _, err = g.Enter(); err != nil {
		return err
	}
	func() {
		defer py.Recover(&err)
		err = fn()
	}()
	return g.Exit(err)
}

// active_command_ is ``laila.get_active_policy().central.command``.
func active_command_() (any, error) {
	policy, err := Get_active_policy()
	if err != nil {
		return nil, err
	}
	central, err := py.Getattr(policy, "central")
	if err != nil {
		return nil, err
	}
	return py.Getattr(central, "command")
}

// AsyncGuaranteeScope_ is the per-entry state for one open
// ``async with guarantee_async`` frame.
type AsyncGuaranteeScope_ struct {
	Command              any
	Scope                any
	Parent_task          *py.Task
	Watcher_task         *py.Task
	Background_exception error
	mu                   sync.Mutex
}

// AsyncGuarantee_ is the asynchronous context manager that awaits futures
// created in its scope. Each entry allocates its own AsyncGuaranteeScope_,
// kept on a per-task stack, so nested or concurrent ``async with`` blocks
// from different tasks never share state.
type AsyncGuarantee_ struct {
	mu          sync.Mutex
	Task_stacks map[*py.Task][]*AsyncGuaranteeScope_ `py:"_task_stacks"`
}

// New_AsyncGuarantee_ is ``_AsyncGuarantee()``.
func New_AsyncGuarantee_() *AsyncGuarantee_ {
	return &AsyncGuarantee_{Task_stacks: map[*py.Task][]*AsyncGuaranteeScope_{}}
}

// Aenter is ``__aenter__``: enters the async guarantee scope and starts
// the background watcher. Must run inside a py asyncio task.
func (g *AsyncGuarantee_) Aenter() (*AsyncGuarantee_, error) {
	state := &AsyncGuaranteeScope_{}
	cmd, err := active_command_()
	if err != nil {
		return nil, err
	}
	state.Command = cmd
	if _, err := callm(cmd, "_guarantee_enter"); err != nil {
		return nil, err
	}
	stack, err := callm(cmd, "_guarantee_stack")
	if err != nil {
		return nil, err
	}
	items := py.Iter(stack)
	state.Scope = items[len(items)-1]
	state.Parent_task = py.Current_task()
	loop, err := py.Get_running_loop()
	if err != nil {
		return nil, err
	}
	state.Watcher_task = loop.Create_task(func() (any, error) {
		return nil, g.Watch_for_future_errors_(state)
	})

	g.mu.Lock()
	g.Task_stacks[state.Parent_task] = append(g.Task_stacks[state.Parent_task], state)
	g.mu.Unlock()
	return g, nil
}

// Aexit is ``__aexit__``: exits the scope, cancels the watcher and awaits
// all enclosed futures. The body's error (exc) propagates unchanged
// unless the watcher recorded a background exception, which wins.
func (g *AsyncGuarantee_) Aexit(exc error) error {
	task := py.Current_task()
	g.mu.Lock()
	stack := g.Task_stacks[task]
	if len(stack) == 0 {
		g.mu.Unlock()
		return exc
	}
	state := stack[len(stack)-1]
	stack = stack[:len(stack)-1]
	if len(stack) == 0 {
		delete(g.Task_stacks, task)
	} else {
		g.Task_stacks[task] = stack
	}
	g.mu.Unlock()

	created, err := callm(state.Command, "_guarantee_exit")
	if err != nil {
		return err
	}

	if state.Watcher_task != nil && !state.Watcher_task.Done() {
		state.Watcher_task.Cancel()
		_, _ = state.Watcher_task.Await() // suppress CancelledError
	}

	// If the watcher saw an in-scope future error it cancelled our parent
	// task; clear that pending cancellation so the original exception
	// surfaces from Background_exception instead of CancelledError.
	state.mu.Lock()
	background := state.Background_exception
	state.mu.Unlock()
	if background != nil {
		if current := py.Current_task(); current != nil {
			current.Uncancel()
		}
	}

	var wait_errors []error
	for _, future := range py.Iter(created) {
		if _, werr := await_future_(future); werr != nil {
			wait_errors = append(wait_errors, werr)
		}
	}

	if background != nil {
		return background
	}
	if exc != nil {
		return exc
	}
	if len(wait_errors) > 0 {
		return wait_errors[0]
	}
	return nil
}

// With runs fn inside the async guarantee scope
// (``async with laila.guarantee_async:``).
func (g *AsyncGuarantee_) With(fn func() error) (err error) {
	if _, err = g.Aenter(); err != nil {
		return err
	}
	func() {
		defer py.Recover(&err)
		err = fn()
	}()
	return g.Aexit(err)
}

// Watch_for_future_errors_ polls for newly registered futures and cancels
// the parent on error.
func (g *AsyncGuarantee_) Watch_for_future_errors_(state *AsyncGuaranteeScope_) error {
	watched := map[string]*py.Task{}
	loop, err := py.Get_running_loop()
	if err != nil {
		return err
	}
	for {
		for _, it := range mappingItems(state.Scope) {
			future_id := py.Str(it.Key)
			if _, ok := watched[future_id]; ok {
				continue
			}
			future := it.Value
			watched[future_id] = loop.Create_task(func() (any, error) { return await_future_(future) })
		}

		if len(watched) == 0 {
			if err := py.Sleep(0.01); err != nil {
				return err
			}
			continue
		}

		tasks := make([]*py.Task, 0, len(watched))
		for _, t := range watched {
			tasks = append(tasks, t)
		}
		done, _, err := py.Wait(tasks, 0.01, py.FIRST_COMPLETED)
		if err != nil {
			return err
		}
		doneSet := map[*py.Task]bool{}
		for _, t := range done {
			doneSet[t] = true
		}
		for future_id, task := range watched {
			if !doneSet[task] {
				continue
			}
			delete(watched, future_id)
			if _, err := task.Await(); err != nil && !py.IsInstance(err, py.CancelledError) {
				state.mu.Lock()
				state.Background_exception = err
				state.mu.Unlock()
				if state.Parent_task != nil && !state.Parent_task.Done() {
					state.Parent_task.Cancel()
				}
				return nil
			}
		}
	}
}

// Awaitable is implemented by futures with ``__await__``.
type Awaitable interface{ Await() (any, error) }

// await_future_ awaits a single future, adapting sync-only futures via
// ``asyncio.to_thread(future.wait, None)``.
func await_future_(future any) (any, error) {
	if aw, ok := future.(Awaitable); ok {
		return aw.Await()
	}
	return py.To_thread(func() (any, error) {
		return callm(future, "wait", py.Kw{"timeout": nil})
	})
}

// Guarantee is the ``laila.guarantee`` singleton.
var Guarantee = &Guarantee_{}

// Guarantee_async is the ``laila.guarantee_async`` singleton.
var Guarantee_async = New_AsyncGuarantee_()
