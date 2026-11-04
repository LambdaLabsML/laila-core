package laila

import (
	"math"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/command/taskforce/thread_pool_executor/future.py --
// pydantic-wrapped ``concurrent.futures.Future`` with Laila status
// lifecycle.

// TERMINAL_ are the terminal future statuses.
var TERMINAL_ = []FutureStatus{FutureStatus_FINISHED, FutureStatus_ERROR, FutureStatus_CANCELLED}

func is_terminal_status_(s FutureStatus) bool {
	return s == FutureStatus_FINISHED || s == FutureStatus_ERROR || s == FutureStatus_CANCELLED
}

// ConcurrentPackageFuture is the pydantic v2 wrapper around
// ``concurrent.futures.Future`` (py.CFuture) with lifecycle +
// introspection.
//
// Status lifecycle:
//   - NOT_STARTED -> initial state
//   - RUNNING     -> after set_running_or_notify_cancel()
//   - FINISHED    -> after successful result()
//   - ERROR       -> after exception set or raised
type ConcurrentPackageFuture struct {
	Future
	Native_future_ *py.CFuture `py:"_native_future"`
}

// ConcurrentPackageFuture_class is the class descriptor.
var ConcurrentPackageFuture_class = py.NewClass("ConcurrentPackageFuture", (*ConcurrentPackageFuture)(nil), Future_class)

// New_ConcurrentPackageFuture is ``ConcurrentPackageFuture(**data)``.
func New_ConcurrentPackageFuture(kw ...py.Kw) (*ConcurrentPackageFuture, error) {
	f := ConcurrentPackageFuture_class.New().(*ConcurrentPackageFuture)
	if err := f.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return f, nil
}

// Model_post_init applies identity, registers with the active local
// policy and attaches the done-callback if a native future was supplied.
func (f *ConcurrentPackageFuture) Model_post_init(ctx any) error {
	if err := f.Future.Model_post_init(ctx); err != nil {
		return err
	}
	if f.Native_future_ != nil {
		f.Add_default_concurrent_future_done_callback_()
	}
	return nil
}

// Native_future returns the underlying ``concurrent.futures.Future``.
func (f *ConcurrentPackageFuture) Native_future() *py.CFuture { return f.Native_future_ }

// Set_native_future sets the underlying native future (one-shot; raises
// on reassignment).
func (f *ConcurrentPackageFuture) Set_native_future(native_future *py.CFuture) error {
	// The attribute store is GIL-atomic in Python; readers (Snapshot_,
	// Is_terminal_) take the instance lock, so the writer must too.
	f.Lock()
	if f.Native_future_ != nil {
		f.Unlock()
		return py.RuntimeError.New("Native future already set.")
	}
	f.Native_future_ = native_future
	f.Unlock()
	f.Add_default_concurrent_future_done_callback_()
	return nil
}

// Add_default_concurrent_future_done_callback_ attaches a done-callback
// that syncs native future outcome to Laila status.
func (f *ConcurrentPackageFuture) Add_default_concurrent_future_done_callback_() {
	self := f.Self()
	default_done_callback_ := func(n_fut *py.CFuture) {
		if n_fut.Cancelled() {
			f.Set_result(nil)
			f.Set_exception(nil)
			f.Default_callbacks_[FutureStatus_CANCELLED](self)
			return
		}
		exc, _ := n_fut.Exception(-1)
		if exc != nil {
			f.Set_exception(exc)
			f.Set_result(nil)
			f.Default_callbacks_[FutureStatus_ERROR](self)
			return
		}
		f.Set_exception(nil)
		res, _ := n_fut.Result(-1)
		f.Set_result(res)
		f.Default_callbacks_[FutureStatus_FINISHED](self)
	}
	f.Native_future_.Add_done_callback(default_done_callback_)
}

// Wait blocks until the future completes or timeout seconds elapse.
//
// Raises LoopBlockingWaitError if called from a thread that owns an async
// event loop.
func (f *ConcurrentPackageFuture) Wait(timeout ...any) (any, error) {
	if err := Check_not_loop_thread_(); err != nil {
		return nil, err
	}
	t, has := Timeout_arg_(timeout)
	if f.Is_terminal_() {
		return f.Wait_impl_(0.0, true)
	}
	return Park_sync(func() (any, error) { return f.Wait_impl_(t, has) })
}

// Is_terminal_ reports whether the native future is done (or, without a
// native future, whether the status is terminal).
func (f *ConcurrentPackageFuture) Is_terminal_() bool {
	f.Lock()
	n_fut := f.Native_future_
	status := f.Status_
	f.Unlock()
	if n_fut != nil {
		return n_fut.Done()
	}
	return is_terminal_status_(status)
}

// Snapshot_ is ``(self._native_future, self._status, self._exception,
// self._return_value)`` under the atomic lock.
func (f *ConcurrentPackageFuture) Snapshot_() (*py.CFuture, FutureStatus, error, any) {
	f.Lock()
	defer f.Unlock()
	return f.Native_future_, f.Status_, f.Exception_, f.Return_value_
}

// Outcome_ returns the value or raises for a terminal status.
//
// The FINISHED path goes through Future.Materialize_result_ so a
// lazily-stored raw value is wrapped into an Entry exactly once, on first
// read.
func (f *ConcurrentPackageFuture) Outcome_(status FutureStatus, exc error, value any) (any, error) {
	if status == FutureStatus_FINISHED {
		return f.Materialize_result_()
	}
	if exc != nil {
		return nil, exc
	}
	return nil, py.RuntimeError.Newf("Future ended with status=%s and no exception.", status.Repr())
}

// Subscribe_terminal_ fires fn(self) once the future reaches any terminal
// status.
//
// Uses Add_status_callback, which also fires immediately if the status is
// already terminal, so there is no lost-wakeup window between the
// caller's last status check and the subscription. fn must be idempotent
// (it may fire twice in the race window).
func (f *ConcurrentPackageFuture) Subscribe_terminal_(fn Future_callback) {
	for _, st := range TERMINAL_ {
		f.Add_status_callback(st, fn)
	}
}

// Unsubscribe_terminal_ removes fn from every terminal status bucket.
func (f *ConcurrentPackageFuture) Unsubscribe_terminal_(fn Future_callback) {
	f.Lock()
	defer f.Unlock()
	for _, st := range TERMINAL_ {
		bucket := f.Status_callbacks_[st]
		if len(bucket) == 0 {
			continue
		}
		// ``bucket.remove(fn)``: first identical entry only.
		for i, cb := range bucket {
			if py.Is(cb, fn) {
				f.Status_callbacks_[st] = append(bucket[:i:i], bucket[i+1:]...)
				break
			}
		}
	}
}

// remaining_ is ``None if deadline is None else max(0.0, deadline -
// time.monotonic())`` with -1 standing for None.
func remaining_(deadline float64, has bool) float64 {
	if !has {
		return -1
	}
	return math.Max(0.0, deadline-py.Monotonic())
}

// Wait_impl_ is the event-driven blocking wait (no polling). timeout < 0 /
// has=false means wait forever.
func (f *ConcurrentPackageFuture) Wait_impl_(timeout float64, has bool) (any, error) {
	deadline := 0.0
	if has {
		deadline = py.Monotonic() + timeout
	}

	n_fut, status, exc, value := f.Snapshot_()
	if n_fut != nil {
		return n_fut.Result(remaining_(deadline, has))
	}
	if is_terminal_status_(status) {
		return f.Outcome_(status, exc, value)
	}

	done := py.NewEvent()
	on_terminal_ := func(_f any) { done.Set() }

	f.Subscribe_terminal_(on_terminal_)
	res, err, returned := func() (any, error, bool) {
		defer f.Unsubscribe_terminal_(on_terminal_)
		if !done.Wait(remaining_(deadline, has)) {
			// Native future may have been attached late; check once more.
			n_fut, status, exc, value := f.Snapshot_()
			if n_fut != nil {
				r, e := n_fut.Result(0.0)
				return r, e, true
			}
			if !is_terminal_status_(status) {
				f.Default_callbacks_[FutureStatus_POLL_TIMEOUT](f.Self())
				return nil, py.TimeoutError.New(), true
			}
			_, _ = exc, value
		}
		return nil, nil, false
	}()
	if returned {
		return res, err
	}

	n_fut, status, exc, value = f.Snapshot_()
	if n_fut != nil {
		return n_fut.Result(0.0)
	}
	return f.Outcome_(status, exc, value)
}

// Await_native_or_terminal_ is the coroutine body of Await.
func (f *ConcurrentPackageFuture) Await_native_or_terminal_() (any, error) {
	n_fut, status, exc, value := f.Snapshot_()
	if n_fut != nil {
		return n_fut.Await()
	}
	if is_terminal_status_(status) {
		return f.Outcome_(status, exc, value)
	}

	loop, err := py.Get_running_loop()
	if err != nil {
		return nil, err
	}
	done := loop.New_future()

	set_ := func() {
		if !done.Done() {
			_ = done.Set_result(nil)
		}
	}

	on_terminal_ := func(_f any) {
		_, _ = loop.Call_soon_threadsafe(set_) // loop closed during shutdown: ignored
	}

	f.Subscribe_terminal_(on_terminal_)
	_, awaitErr := done.Await()
	f.Unsubscribe_terminal_(on_terminal_)
	if awaitErr != nil {
		return nil, awaitErr
	}

	n_fut, status, exc, value = f.Snapshot_()
	if n_fut != nil {
		return n_fut.Await()
	}
	return f.Outcome_(status, exc, value)
}

// Await awaits the native future or a terminal status (event-driven, no
// polling).
//
// Parks the current taskforce slot (if any) while pending so nested awaits
// can never deadlock the scheduler.
func (f *ConcurrentPackageFuture) Await() (any, error) {
	if f.Is_terminal_() {
		return f.Await_native_or_terminal_()
	}
	return Park_async(py.Coroutine(f.Await_native_or_terminal_))
}

func init() {
	py.Register_property("native_future")
}
