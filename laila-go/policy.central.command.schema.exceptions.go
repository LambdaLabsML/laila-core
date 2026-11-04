package laila

import (
	"sync"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/command/schema/exceptions.py -- exceptions and runtime
// guards for the central command system.
//
// Provides:
//
//   - LoopBlockingWaitError -- raised when ``Future.wait()`` is called from
//     a thread that is currently driving an async taskforce event loop
//     (which would deadlock the loop on a future the loop itself needs to
//     advance).
//   - NestedCommandSubmitError -- raised when a function decorated
//     ``@no_command_submit`` synchronously invokes ``Command.submit``.
//   - No_command_submit -- decorator that marks a function as forbidden
//     from calling ``Command.submit`` while it executes; works on both sync
//     and async callables via a shared ContextVar.
//   - ASYNC_LOOP_THREAD_IDS_ -- module-level set of thread ids that own an
//     async event loop. Populated/cleared by LoopThread_ in the async
//     taskforce. Consulted by future Wait implementations.
//   - NO_SUBMIT_OWNER_ -- ContextVar carrying the qualname of the innermost
//     ``@no_command_submit`` function currently executing on the context.
//     Read by ``Command.submit`` to enforce the contract.

// ASYNC_LOOP_THREAD_IDS_ is ``_ASYNC_LOOP_THREAD_IDS: set[int]``.
//
// Port note: in CPython one OS thread runs every coroutine of a loop, so
// the registry holds thread idents. In the port every task is its own
// goroutine, so "the current thread owns an async loop" is "the current
// goroutine belongs to a registered loop" (py.Current_loop). The registry
// is therefore keyed by the loop; executor-thread bodies spawned from a
// task do not inherit loop membership and are correctly *not* rejected.
var ASYNC_LOOP_THREAD_IDS_ = map[*py.Loop]struct{}{}

// ASYNC_LOOP_THREAD_IDS_LOCK_ guards ASYNC_LOOP_THREAD_IDS_.
var ASYNC_LOOP_THREAD_IDS_LOCK_ sync.Mutex

// Register_async_loop_thread_ marks loop's thread as owning an async
// event loop.
func Register_async_loop_thread_(loop *py.Loop) {
	ASYNC_LOOP_THREAD_IDS_LOCK_.Lock()
	defer ASYNC_LOOP_THREAD_IDS_LOCK_.Unlock()
	ASYNC_LOOP_THREAD_IDS_[loop] = struct{}{}
}

// Unregister_async_loop_thread_ drops loop's thread from the async-loop
// registry.
func Unregister_async_loop_thread_(loop *py.Loop) {
	ASYNC_LOOP_THREAD_IDS_LOCK_.Lock()
	defer ASYNC_LOOP_THREAD_IDS_LOCK_.Unlock()
	delete(ASYNC_LOOP_THREAD_IDS_, loop)
}

// LoopBlockingWaitError is raised when ``Future.wait()`` is called from
// inside an async loop thread.
//
// The blocking wait would freeze the loop on a future that needs the same
// loop to advance. Use ``await fut`` from inside a coroutine instead.
var LoopBlockingWaitError = py.NewType("LoopBlockingWaitError", py.RuntimeError)

// Check_not_loop_thread_ raises LoopBlockingWaitError if the current
// thread owns a loop.
func Check_not_loop_thread_() error {
	loop := py.Current_loop()
	if loop == nil {
		return nil
	}
	ASYNC_LOOP_THREAD_IDS_LOCK_.Lock()
	_, owns := ASYNC_LOOP_THREAD_IDS_[loop]
	ASYNC_LOOP_THREAD_IDS_LOCK_.Unlock()
	if owns {
		return LoopBlockingWaitError.New(
			"Future.wait() called from inside an async taskforce loop thread. " +
				"Use `await fut` instead of `.wait()` from coroutines.")
	}
	return nil
}

// NO_SUBMIT_OWNER_ is ``_NO_SUBMIT_OWNER: ContextVar[str | None]``.
var NO_SUBMIT_OWNER_ = py.NewContextVar[*string]("_no_submit_owner", nil)

// NestedCommandSubmitError is raised when a ``@no_command_submit`` function
// calls ``cmd.submit``.
var NestedCommandSubmitError = py.NewType("NestedCommandSubmitError", py.RuntimeError)

// No_command_submit marks fn as forbidden from synchronously invoking
// ``cmd.submit``.
//
// Sets a ContextVar while fn runs; ``Command.submit`` reads the var and
// raises NestedCommandSubmitError if it is set. Works for both sync and
// async callables (a py.CoroutineFunc stays a coroutine function; any other
// callable becomes a plain variadic func). The contextvar propagates
// through ``await`` and across ``create_task`` boundaries.
func No_command_submit(fn any) any {
	qualname := py.Qualname(fn)
	if py.Iscoroutinefunction(fn) {
		coro := fn.(py.CoroutineFunc)
		var async_wrap_ py.CoroutineFunc = func(args ...any) (any, error) {
			token := NO_SUBMIT_OWNER_.Set(&qualname)
			defer NO_SUBMIT_OWNER_.Reset(token)
			return coro(args...)
		}
		return async_wrap_
	}
	sync_wrap_ := func(args ...any) (any, error) {
		token := NO_SUBMIT_OWNER_.Set(&qualname)
		defer NO_SUBMIT_OWNER_.Reset(token)
		return py.Call(fn, args, nil)
	}
	return sync_wrap_
}

// Check_no_pending_submit_owner_ raises NestedCommandSubmitError if
// ``@no_command_submit`` is active.
func Check_no_pending_submit_owner_() error {
	owner := NO_SUBMIT_OWNER_.Value()
	if owner != nil {
		return NestedCommandSubmitError.Newf(
			"`%s` is decorated `@no_command_submit` but called "+
				"`Command.submit`. Build functions must not nest command "+
				"submissions; use `await` on a future passed in from outside "+
				"the build, or restructure to submit from the orchestrator.", *owner)
	}
	return nil
}

// Run_syncer is the duck type ``hasattr(tf, "run_sync")`` checked by
// Ensure_coroutine_function: a taskforce that can offload a sync body to
// its own sync executor.
type Run_syncer interface {
	Run_sync(task any, args ...any) (any, error)
}

// Ensure_coroutine_function returns a coroutine function for task.
//
// Coroutine functions (including ``functools.partial`` of one) are returned
// unchanged. Plain sync callables are wrapped in an ``async def`` shim so
// the runner sees a uniform awaitable. The shim offloads the sync body to
// the owning taskforce's sync executor
// (PythonAsyncThreadPoolTaskForce.Run_sync) -- or py.To_thread when not
// running inside a taskforce slot -- so a CPU-bound or blocking body never
// stalls the event loop. If the body itself returns a coroutine (e.g. a
// lambda factory ``lambda: my_async_fn()``), the shim awaits it on the
// loop so the runner gets the final value rather than an unawaited
// coroutine.
//
// Prefer submitting coroutine functions / ``functools.partial(coro_fn,
// ...)`` for async work: they skip the thread hop entirely.
func Ensure_coroutine_function(task any) py.CoroutineFunc {
	if py.Iscoroutinefunction(task) {
		return task.(py.CoroutineFunc)
	}
	var wrap_ py.CoroutineFunc = func(args ...any) (any, error) {
		slot := CURRENT_SLOT_.Value()
		var tf Run_syncer
		if slot != nil {
			tf, _ = slot.Tf.(Run_syncer)
		}
		var out any
		var err error
		if tf != nil {
			out, err = tf.Run_sync(task, args...)
		} else {
			out, err = py.To_thread(func() (any, error) { return py.Call(task, args, nil) })
		}
		if err != nil {
			return nil, err
		}
		if py.Iscoroutine(out) {
			out, err = py.Await(out)
			if err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	return wrap_
}
