package py

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// Loop is asyncio.AbstractEventLoop under the "loop lock" model.
//
// In CPython one thread runs every coroutine of a loop, so coroutine code
// mutates loop-owned state without locks and only interleaves at await
// points. Here each task is a goroutine, and the loop lock reproduces that
// interleaving: a task goroutine runs holding the lock and releases it only
// inside the leaf await primitives (Sleep, Await, To_thread, Event.Wait,
// Semaphore.Acquire, I/O wrappers). Waiters are queued FIFO, which is the
// call_soon ordering asyncio guarantees.
type Loop struct {
	mu        sync.Mutex
	held      bool
	holder    int64
	queue     []*loopWaiter
	closed    bool
	running   bool
	stopping  bool // stop() requested before run_forever() (CPython's _stopping)
	stopCh    chan struct{}
	stopped   chan struct{}
	tasks     map[*Task]struct{}
	closers   []io.Closer
	execMu    sync.Mutex
	executor  *ThreadPoolExecutor
	thread    *Thread
	debug     bool
	exception func(ctx *Dict)
	id        int64
	// call_soon ready queue (guarded by mu, so a callback's slot and its
	// queue position are taken atomically): CPython appends a Handle to
	// ``_ready``; one pump goroutine per loop runs them in slot order so a
	// flood of callbacks costs a queue entry each, not a parked goroutine.
	soon     []soonItem
	soonPump bool
}

type soonItem struct {
	slot *loopWaiter
	h    *Handle
	fn   func()
}

type loopWaiter struct {
	ch chan struct{}
}

var loopCounter atomic.Int64

// New_event_loop is asyncio.new_event_loop().
func New_event_loop() *Loop {
	return &Loop{tasks: map[*Task]struct{}{}, id: loopCounter.Add(1)}
}

// acquire takes the loop lock, queueing FIFO behind earlier waiters.
func (l *Loop) acquire() {
	w := l.reserve()
	l.wait(w)
}

// reserve claims a FIFO slot for the loop lock on behalf of the caller
// (or of a goroutine about to start). Like call_soon, the position in the
// queue is fixed synchronously; the goroutine then blocks in wait.
func (l *Loop) reserve() *loopWaiter {
	l.mu.Lock()
	w := l.reserveLocked()
	l.mu.Unlock()
	return w
}

func (l *Loop) reserveLocked() *loopWaiter {
	w := &loopWaiter{ch: make(chan struct{})}
	if !l.held && len(l.queue) == 0 {
		l.held, l.holder = true, 0
		close(w.ch)
	} else {
		l.queue = append(l.queue, w)
	}
	return w
}

// wait blocks until the reserved slot is granted and records the holder.
func (l *Loop) wait(w *loopWaiter) {
	<-w.ch
	l.mu.Lock()
	l.holder = Goid()
	l.mu.Unlock()
}

// release hands the loop lock to the next waiter, if any.
func (l *Loop) release() {
	l.mu.Lock()
	if len(l.queue) > 0 {
		w := l.queue[0]
		l.queue = l.queue[1:]
		l.holder = 0 // set by the woken goroutine in wait()
		l.mu.Unlock()
		close(w.ch)
		return
	}
	l.held, l.holder = false, 0
	l.mu.Unlock()
}

func (l *Loop) holding() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.held && l.holder == Goid()
}

// Current_loop returns the loop the calling goroutine belongs to (its task
// goroutine, callback goroutine or run_forever goroutine), or nil. This is
// the Go form of "threading.current_thread() is loop_thread".
func Current_loop() *Loop {
	if s := glsGet(); s != nil {
		return s.loop
	}
	return nil
}

// Get_running_loop is asyncio.get_running_loop().
func Get_running_loop() (*Loop, error) {
	if l := Current_loop(); l != nil {
		return l, nil
	}
	return nil, RuntimeError.New("no running event loop")
}

// Get_event_loop is asyncio.get_event_loop() (the running loop, or the loop
// set for this goroutine with Set_event_loop).
func Get_event_loop() (*Loop, error) { return Get_running_loop() }

// Set_event_loop is asyncio.set_event_loop(loop): binds the calling
// goroutine to loop (it becomes "the loop thread").
func Set_event_loop(l *Loop) { glsEnsure().loop = l }

// Current_task is asyncio.current_task().
func Current_task() *Task {
	if s := glsGet(); s != nil {
		return s.task
	}
	return nil
}

// Is_running is loop.is_running().
func (l *Loop) Is_running() bool { l.mu.Lock(); defer l.mu.Unlock(); return l.running }

// Is_closed is loop.is_closed().
func (l *Loop) Is_closed() bool { l.mu.Lock(); defer l.mu.Unlock(); return l.closed }

// Set_debug is loop.set_debug(flag).
func (l *Loop) Set_debug(flag bool) { l.debug = flag }

// Get_debug is loop.get_debug().
func (l *Loop) Get_debug() bool { return l.debug }

// Run_forever blocks the calling goroutine until Stop(); the goroutine is
// flagged as the loop thread meanwhile.
func (l *Loop) Run_forever() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return RuntimeError.New("Event loop is closed")
	}
	if l.running {
		l.mu.Unlock()
		return RuntimeError.New("This event loop is already running")
	}
	l.running = true
	l.stopCh = make(chan struct{})
	l.stopped = make(chan struct{})
	l.thread = Current_thread()
	stopCh, stopped := l.stopCh, l.stopped
	if l.stopping {
		// ``loop.stop()`` issued before ``run_forever()``: CPython runs a
		// single iteration and returns.
		l.stopping = false
		close(stopCh)
	}
	l.mu.Unlock()
	s := glsEnsure()
	prev := s.loop
	s.loop = l
	defer func() { s.loop = prev }()
	<-stopCh
	l.mu.Lock()
	l.running = false
	l.thread = nil
	l.mu.Unlock()
	close(stopped)
	return nil
}

// Stop is loop.stop(): makes Run_forever return.
func (l *Loop) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.running && l.stopCh != nil {
		select {
		case <-l.stopCh:
		default:
			close(l.stopCh)
		}
		return
	}
	l.stopping = true
}

// Run_until_complete runs a coroutine on the loop and returns its result.
// May be called from any goroutine that is not already a task of this loop.
//
// Like CPython's ``run_until_complete`` (which is ``run_forever`` plus a
// done-callback that stops the loop) the loop reports ``is_running()``
// for the duration, and ``loop.stop()`` from another thread makes it
// return early with ``RuntimeError("Event loop stopped before Future
// completed.")``. A stop that lands after the task already completed is
// left pending so the next ``run_forever`` returns at once instead of
// being silently consumed (CPython would consume it in the same
// iteration; keeping it is the safer reading for thread-driven loops).
func (l *Loop) Run_until_complete(fn func() (any, error)) (any, error) {
	if l.Is_closed() {
		return nil, RuntimeError.New("Event loop is closed")
	}
	if l.holding() {
		// Called from a task of this loop (CPython would raise "already
		// running"); run it inline as a nested await.
		return l.Create_task(fn).Await()
	}
	l.mu.Lock()
	if l.running {
		l.mu.Unlock()
		return nil, RuntimeError.New("This event loop is already running")
	}
	l.running = true
	l.stopCh = make(chan struct{})
	l.stopped = make(chan struct{})
	l.thread = Current_thread()
	stopCh, stopped := l.stopCh, l.stopped
	if l.stopping {
		// ``loop.stop()`` issued before ``run_until_complete()``: CPython
		// runs a single iteration and returns.
		l.stopping = false
		close(stopCh)
	}
	l.mu.Unlock()
	s := glsEnsure()
	prev := s.loop
	s.loop = l
	defer func() { s.loop = prev }()
	t := l.Create_task(fn)
	select {
	case <-t.done:
	case <-stopCh:
	}
	l.mu.Lock()
	l.running = false
	l.thread = nil
	done := t.Done()
	if done {
		select {
		case <-stopCh:
			l.stopping = true
		default:
		}
	}
	l.mu.Unlock()
	close(stopped)
	if !done {
		return nil, RuntimeError.New("Event loop stopped before Future completed.")
	}
	return t.resultErr()
}

// Close is loop.close(): closes registered I/O objects so blocked leaf
// awaits return, and shuts the default executor down.
func (l *Loop) Close() error {
	l.mu.Lock()
	if l.running {
		l.mu.Unlock()
		return RuntimeError.New("Cannot close a running event loop")
	}
	l.closed = true
	closers := l.closers
	l.closers = nil
	l.mu.Unlock()
	for _, c := range closers {
		_ = c.Close()
	}
	l.execMu.Lock()
	ex := l.executor
	l.executor = nil
	l.execMu.Unlock()
	if ex != nil {
		ex.Shutdown(false)
	}
	return nil
}

// Register_closer registers an I/O object to be closed by Close().
func (l *Loop) Register_closer(c io.Closer) {
	l.mu.Lock()
	l.closers = append(l.closers, c)
	l.mu.Unlock()
}

// Unregister_closer removes a registered closer.
func (l *Loop) Unregister_closer(c io.Closer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, x := range l.closers {
		if x == c {
			l.closers = append(l.closers[:i], l.closers[i+1:]...)
			return
		}
	}
}

// Default_executor returns the loop's default executor (to_thread pool),
// bounded like CPython's: min(32, cpu + 4) workers.
func (l *Loop) Default_executor() *ThreadPoolExecutor {
	l.execMu.Lock()
	defer l.execMu.Unlock()
	if l.executor == nil {
		l.executor = New_ThreadPoolExecutor(0, Kw{"thread_name_prefix": "asyncio"})
	}
	return l.executor
}

// Set_default_executor is loop.set_default_executor(ex).
func (l *Loop) Set_default_executor(ex *ThreadPoolExecutor) {
	l.execMu.Lock()
	l.executor = ex
	l.execMu.Unlock()
}

// Thread returns the thread running Run_forever (nil when not running).
func (l *Loop) Thread() *Thread { l.mu.Lock(); defer l.mu.Unlock(); return l.thread }

// In_loop_thread reports whether the caller belongs to this loop.
func (l *Loop) In_loop_thread() bool { return Current_loop() == l }

// Repr is <_UnixSelectorEventLoop running=... closed=...>.
func (l *Loop) Repr() string {
	return fmt.Sprintf("<Loop id=%d running=%v closed=%v debug=%v>", l.id, l.Is_running(), l.Is_closed(), l.debug)
}

// ---------------------------------------------------------------------------
// Task / AsyncFuture
// ---------------------------------------------------------------------------

// Task is asyncio.Task (and asyncio.Future when created with New_future).
type Task struct {
	loop        *Loop
	name        string
	mu          sync.Mutex
	done        chan struct{}
	finished    bool
	result      any
	err         error
	cancelled   bool
	cancelMsg   any
	cancelCh    chan struct{} // one-shot cancel delivery (buffered 1)
	cancelPend  atomic.Bool
	callbacks   []func(*Task)
	coroutine   bool
	goid        int64
	// wakeMu guards waiting / wakeSlot: while the task goroutine is parked
	// in a leaf await with the loop lock released, Cancel reserves its
	// re-entry slot synchronously so the cancelled step is ordered ahead
	// of anything call_soon'ed afterwards -- exactly CPython's
	// ``cancel() -> call_soon(__step)``. Without it a cancelled task would
	// only re-enter the FIFO once the Go scheduler happened to run it,
	// and a subsequent ``run_until_complete(sleep(0))`` could overtake it.
	wakeMu   sync.Mutex
	waiting  bool
	wakeSlot *loopWaiter
}

var taskCounter atomic.Int64

// New_future is loop.create_future(): a Task-shaped future resolved by hand.
func (l *Loop) New_future() *Task {
	return &Task{loop: l, done: make(chan struct{}), cancelCh: make(chan struct{}, 1), name: fmt.Sprintf("Future-%d", taskCounter.Add(1))}
}

// Create_task is loop.create_task(coro): the coroutine body runs in its own
// goroutine as soon as it obtains the loop lock (FIFO behind the caller).
func (l *Loop) Create_task(fn func() (any, error), kw ...Kw) *Task {
	t := l.New_future()
	t.coroutine = true
	t.name = fmt.Sprintf("Task-%d", taskCounter.Add(1))
	for _, k := range kw {
		if n, ok := k["name"].(string); ok && n != "" {
			t.name = n
		}
	}
	l.mu.Lock()
	l.tasks[t] = struct{}{}
	l.mu.Unlock()
	slot := l.reserve() // queue position fixed now, like call_soon(step)
	goWithState(func(s *glsState) {
		s.loop = l
		s.task = t
	}, func() {
		t.goid = Goid()
		l.wait(slot)
		var (
			res any
			err error
		)
		func() {
			defer Recover(&err)
			// cancel() before the first step delivers CancelledError immediately
			if t.cancelPend.Load() {
				select {
				case <-t.cancelCh:
				default:
				}
				t.cancelPend.Store(false)
				err = CancelledError.New()
				return
			}
			res, err = fn()
		}()
		t.complete(res, err)
		l.mu.Lock()
		delete(l.tasks, t)
		l.mu.Unlock()
		l.release()
	})
	return t
}

// Ensure_future is asyncio.ensure_future(coro) on the running loop.
func Ensure_future(fn func() (any, error)) (*Task, error) {
	l, err := Get_running_loop()
	if err != nil {
		return nil, err
	}
	return l.Create_task(fn), nil
}

func (t *Task) complete(res any, err error) {
	t.mu.Lock()
	if t.finished {
		t.mu.Unlock()
		return
	}
	t.finished = true
	if err != nil && IsInstance(err, CancelledError) {
		t.cancelled = true
		t.err = err
	} else {
		t.result, t.err = res, err
	}
	close(t.done)
	cbs := t.callbacks
	t.callbacks = nil
	t.mu.Unlock()
	for _, cb := range cbs {
		t.safeCallback(cb)
	}
}

func (t *Task) safeCallback(cb func(*Task)) {
	defer func() {
		if r := recover(); r != nil {
			Logger_warning("Exception in callback of %s: %v", t.Repr(), r)
		}
	}()
	cb(t)
}

func (t *Task) resultErr() (any, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.cancelled {
		return nil, CancelledError.New()
	}
	return t.result, t.err
}

// Set_result is future.set_result(v).
func (t *Task) Set_result(v any) error {
	if t.Done() {
		return InvalidStateError.New("invalid state")
	}
	t.complete(v, nil)
	return nil
}

// Set_exception is future.set_exception(exc).
func (t *Task) Set_exception(err error) error {
	if t.Done() {
		return InvalidStateError.New("invalid state")
	}
	t.complete(nil, err)
	return nil
}

// Done is task.done().
func (t *Task) Done() bool {
	select {
	case <-t.done:
		return true
	default:
		return false
	}
}

// Cancelled is task.cancelled().
func (t *Task) Cancelled() bool { t.mu.Lock(); defer t.mu.Unlock(); return t.cancelled }

// Cancel is task.cancel(msg): requests delivery of CancelledError at the
// task's next await point (or resolves a plain future immediately).
func (t *Task) Cancel(msg ...any) bool {
	if t.Done() {
		return false
	}
	if len(msg) > 0 {
		t.cancelMsg = msg[0]
	}
	if !t.coroutine {
		t.complete(nil, CancelledError.New(msg...))
		return true
	}
	t.cancelPend.Store(true)
	t.wakeMu.Lock()
	if t.waiting && t.wakeSlot == nil {
		t.wakeSlot = t.loop.reserve()
	}
	t.wakeMu.Unlock()
	select {
	case t.cancelCh <- struct{}{}:
	default:
	}
	return true
}

// Cancelling is task.cancelling(): number of pending cancel requests (0/1).
func (t *Task) Cancelling() int {
	if t.cancelPend.Load() {
		return 1
	}
	return 0
}

// Result is task.result(): InvalidStateError when not done.
func (t *Task) Result() (any, error) {
	if !t.Done() {
		return nil, InvalidStateError.New("Result is not set.")
	}
	return t.resultErr()
}

// Exception is task.exception().
func (t *Task) Exception() (error, error) {
	if !t.Done() {
		return nil, InvalidStateError.New("Exception is not set.")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.cancelled {
		return nil, CancelledError.New()
	}
	return t.err, nil
}

// Add_done_callback is task.add_done_callback(fn).
func (t *Task) Add_done_callback(cb func(*Task)) {
	t.mu.Lock()
	if t.finished {
		t.mu.Unlock()
		t.safeCallback(cb)
		return
	}
	t.callbacks = append(t.callbacks, cb)
	t.mu.Unlock()
}

// Get_name is task.get_name().
func (t *Task) Get_name() string { return t.name }

// Set_name is task.set_name(name).
func (t *Task) Set_name(n string) { t.name = n }

// Get_loop is task.get_loop().
func (t *Task) Get_loop() *Loop { return t.loop }

// Chan exposes the completion channel for select.
func (t *Task) Chan() <-chan struct{} { return t.done }

// Await is `await task`: a leaf await on completion.
func (t *Task) Await() (any, error) {
	err := leafAwait(func(cancel <-chan struct{}) error {
		select {
		case <-t.done:
			return nil
		case <-cancel:
			return CancelledError.New()
		}
	})
	if err != nil {
		return nil, err
	}
	return t.resultErr()
}

// Repr is <Task name=... state=...>.
func (t *Task) Repr() string {
	state := "pending"
	if t.Done() {
		state = "finished"
		if t.Cancelled() {
			state = "cancelled"
		}
	}
	return fmt.Sprintf("<Task %s name=%s>", state, Repr_str(t.name))
}

// All_tasks is asyncio.all_tasks(loop): tasks not yet done.
func (l *Loop) All_tasks() []*Task {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]*Task, 0, len(l.tasks))
	for t := range l.tasks {
		if !t.Done() {
			out = append(out, t)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// leaf await primitives
// ---------------------------------------------------------------------------

// leafAwait runs wait with the loop lock released (when the caller holds
// it) and reacquires afterwards. Pending cancellation is delivered once,
// before the wait or on wake-up, exactly like CPython's Task.__step.
func leafAwait(wait func(cancel <-chan struct{}) error) error {
	s := glsGet()
	var (
		t *Task
		l *Loop
	)
	if s != nil {
		t, l = s.task, s.loop
	}
	var cancel <-chan struct{}
	if t != nil && t.coroutine {
		if t.cancelPend.Load() {
			t.cancelPend.Store(false)
			select {
			case <-t.cancelCh:
			default:
			}
			return CancelledError.New(t.cancelMsgArgs()...)
		}
		cancel = t.cancelCh
	}
	holding := l != nil && l.holding()
	if holding {
		l.release()
		if t != nil {
			t.wakeMu.Lock()
			t.waiting = true
			t.wakeMu.Unlock()
		}
		defer func() {
			// Re-enter through the slot Cancel reserved for us, if any, so
			// the cancelled step keeps its call_soon position.
			var slot *loopWaiter
			if t != nil {
				t.wakeMu.Lock()
				t.waiting = false
				slot, t.wakeSlot = t.wakeSlot, nil
				t.wakeMu.Unlock()
			}
			if slot != nil {
				l.wait(slot)
			} else {
				l.acquire()
			}
		}()
	}
	err := wait(cancel)
	if err != nil && IsInstance(err, CancelledError) && t != nil {
		t.cancelPend.Store(false)
		return CancelledError.New(t.cancelMsgArgs()...)
	}
	return err
}

func (t *Task) cancelMsgArgs() []any {
	if t.cancelMsg != nil {
		return []any{t.cancelMsg}
	}
	return nil
}

// Sleep is `await asyncio.sleep(seconds)`; Sleep(0) yields to every task
// already queued for the loop lock.
func Sleep(seconds float64) error {
	if seconds <= 0 {
		return leafAwait(func(cancel <-chan struct{}) error {
			select {
			case <-cancel:
				return CancelledError.New()
			default:
				return nil
			}
		})
	}
	d, _ := Duration(seconds)
	return leafAwait(func(cancel <-chan struct{}) error {
		tm := time.NewTimer(d)
		defer tm.Stop()
		select {
		case <-tm.C:
			return nil
		case <-cancel:
			return CancelledError.New()
		}
	})
}

// Await_chan awaits an arbitrary channel as a leaf await.
func Await_chan(ch <-chan struct{}) error {
	return leafAwait(func(cancel <-chan struct{}) error {
		select {
		case <-ch:
			return nil
		case <-cancel:
			return CancelledError.New()
		}
	})
}

// Await_cfuture is `await asyncio.wrap_future(cf)`.
func Await_cfuture(cf *CFuture) (any, error) {
	err := leafAwait(func(cancel <-chan struct{}) error {
		select {
		case <-cf.done:
			return nil
		case <-cancel:
			return CancelledError.New()
		}
	})
	if err != nil {
		return nil, err
	}
	return cf.Result(-1)
}

// Blocking runs a blocking Go call as a leaf await (loop lock released);
// cancellation is not delivered until the call returns.
func Blocking(fn func() error) error {
	return leafAwait(func(<-chan struct{}) error { return fn() })
}

// To_thread is `await asyncio.to_thread(fn)`: fn runs on the loop's
// default executor with the caller's contextvars.
func To_thread(fn func() (any, error)) (any, error) {
	l := Current_loop()
	var ex *ThreadPoolExecutor
	if l != nil {
		ex = l.Default_executor()
	} else {
		ex = fallbackExecutor()
	}
	return Run_in_executor(ex, fn)
}

var (
	fallbackExecMu sync.Mutex
	fallbackExec   *ThreadPoolExecutor
)

func fallbackExecutor() *ThreadPoolExecutor {
	fallbackExecMu.Lock()
	defer fallbackExecMu.Unlock()
	if fallbackExec == nil {
		fallbackExec = New_ThreadPoolExecutor(0, Kw{"thread_name_prefix": "asyncio-fallback"})
	}
	return fallbackExec
}

// Run_in_executor is `await loop.run_in_executor(ex, fn)`.
func Run_in_executor(ex *ThreadPoolExecutor, fn func() (any, error)) (any, error) {
	ctx := Copy_context()
	cf, err := ex.Submit(func() (res any, err error) {
		ctx.Run(func() { res, err = fn() })
		return
	})
	if err != nil {
		return nil, err
	}
	return Await_cfuture(cf)
}

// Run_coroutine_threadsafe is asyncio.run_coroutine_threadsafe(coro, loop).
func Run_coroutine_threadsafe(fn func() (any, error), l *Loop) *CFuture {
	cf := NewCFuture()
	if l.Is_closed() {
		_ = cf.Set_exception(RuntimeError.New("Event loop is closed"))
		return cf
	}
	t := l.Create_task(fn)
	t.Add_done_callback(func(t *Task) {
		res, err := t.resultErr()
		if err != nil {
			_ = cf.Set_exception(err)
		} else {
			_ = cf.Set_result(res)
		}
	})
	cf.Add_done_callback(func(cf *CFuture) {
		if cf.Cancelled() {
			t.Cancel()
		}
	})
	return cf
}

// Handle is the object returned by call_soon*/call_later.
type Handle struct {
	cancelled atomic.Bool
	when      time.Time
}

// Cancel is handle.cancel().
func (h *Handle) Cancel() { h.cancelled.Store(true) }

// Cancelled is handle.cancelled().
func (h *Handle) Cancelled() bool { return h.cancelled.Load() }

// When is timerhandle.when() in loop time seconds.
func (h *Handle) When() float64 { return Seconds(h.when.Sub(processStart)) }

// Call_soon_threadsafe is loop.call_soon_threadsafe(fn): fn runs holding
// the loop lock, FIFO behind earlier callbacks and tasks.
func (l *Loop) Call_soon_threadsafe(fn func()) (*Handle, error) {
	if l.Is_closed() {
		return nil, RuntimeError.New("Event loop is closed")
	}
	h := &Handle{}
	// The slot is reserved synchronously (the position in the loop's FIFO
	// is fixed at call time, like ``_ready.append``); the pump waits on
	// the slots in order.
	l.mu.Lock()
	slot := l.reserveLocked()
	l.soon = append(l.soon, soonItem{slot: slot, h: h, fn: fn})
	start := !l.soonPump
	l.soonPump = true
	l.mu.Unlock()
	if start {
		goWithState(func(s *glsState) { s.loop = l }, l.soonPump_)
	}
	return h, nil
}

// soonPump_ drains the call_soon queue in slot order. Like CPython's
// ``_run_once`` it runs consecutive ready callbacks as one batch: when the
// next callback's slot is the next in the loop's FIFO the lock is kept
// rather than handed off and re-acquired, so a flood of callbacks drains
// at the speed of the callbacks themselves. A task step whose slot sits
// between two callbacks still gets the lock in its turn.
func (l *Loop) soonPump_() {
	held := false
	for {
		l.mu.Lock()
		if len(l.soon) == 0 {
			l.soonPump = false
			l.mu.Unlock()
			if held {
				l.release()
			}
			return
		}
		it := l.soon[0]
		l.soon[0] = soonItem{}
		l.soon = l.soon[1:]
		direct := held && len(l.queue) > 0 && l.queue[0] == it.slot
		if direct {
			l.queue = l.queue[1:]
		}
		l.mu.Unlock()
		if !direct {
			if held {
				l.release()
			}
			l.wait(it.slot)
		}
		held = true
		l.runSoon_(it)
	}
}

// runSoon_ runs one call_soon callback under the loop lock (already held).
func (l *Loop) runSoon_(it soonItem) {
	if it.h.cancelled.Load() {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			l.Call_exception_handler(DictOf("message", "Exception in callback", "exception", PanicToError(r)))
		}
	}()
	it.fn()
}

// Call_soon is loop.call_soon(fn) (same mechanics in this model).
func (l *Loop) Call_soon(fn func()) (*Handle, error) { return l.Call_soon_threadsafe(fn) }

// Call_later is loop.call_later(delay, fn).
func (l *Loop) Call_later(delay float64, fn func()) (*Handle, error) {
	if l.Is_closed() {
		return nil, RuntimeError.New("Event loop is closed")
	}
	d, _ := Duration(delay)
	h := &Handle{when: time.Now().Add(d)}
	goWithState(func(s *glsState) { s.loop = l }, func() {
		time.Sleep(d)
		if h.cancelled.Load() {
			return
		}
		l.acquire()
		defer l.release()
		if h.cancelled.Load() {
			return
		}
		fn()
	})
	return h, nil
}

// Time is loop.time().
func (l *Loop) Time() float64 { return Monotonic() }

// Call_exception_handler is loop.call_exception_handler(context).
func (l *Loop) Call_exception_handler(ctx *Dict) {
	if l.exception != nil {
		l.exception(ctx)
		return
	}
	Logger_warning("%s: %v", Str(ctx.GetDefault("message", "Unhandled exception in event loop")), ctx.GetDefault("exception", nil))
}

// Set_exception_handler is loop.set_exception_handler(fn).
func (l *Loop) Set_exception_handler(fn func(ctx *Dict)) { l.exception = fn }

// Wait_for is `await asyncio.wait_for(coro, timeout)`: on timeout the inner
// task is cancelled, awaited, and TimeoutError is raised.
func Wait_for(fn func() (any, error), timeout float64) (any, error) {
	l := Current_loop()
	if l == nil {
		return nil, RuntimeError.New("no running event loop")
	}
	t := l.Create_task(fn)
	if timeout < 0 {
		return t.Await()
	}
	d, _ := Duration(timeout)
	err := leafAwait(func(cancel <-chan struct{}) error {
		tm := time.NewTimer(d)
		defer tm.Stop()
		select {
		case <-t.done:
			return nil
		case <-tm.C:
			return TimeoutError.New()
		case <-cancel:
			return CancelledError.New()
		}
	})
	if err != nil {
		t.Cancel()
		_, _ = t.Await() // leaf await: the inner task needs the loop to unwind
		if IsInstance(err, CancelledError) {
			return nil, err
		}
		return nil, TimeoutError.New()
	}
	return t.resultErr()
}

// asyncio.wait return_when constants.
const (
	FIRST_COMPLETED = "FIRST_COMPLETED"
	FIRST_EXCEPTION = "FIRST_EXCEPTION"
	ALL_COMPLETED   = "ALL_COMPLETED"
)

// Wait is `await asyncio.wait(tasks, timeout=..., return_when=...)`:
// returns the (done, pending) sets; timeout < 0 waits forever. Unlike
// Wait_for nothing is cancelled on timeout.
func Wait(tasks []*Task, timeout float64, return_when string) ([]*Task, []*Task, error) {
	if len(tasks) == 0 {
		return nil, nil, ValueError.New("Set of Tasks/Futures is empty.")
	}
	split := func() ([]*Task, []*Task) {
		var done, pending []*Task
		for _, t := range tasks {
			if t.Done() {
				done = append(done, t)
			} else {
				pending = append(pending, t)
			}
		}
		return done, pending
	}
	satisfied := func() bool {
		done, pending := split()
		switch return_when {
		case FIRST_COMPLETED:
			return len(done) > 0
		case FIRST_EXCEPTION:
			for _, t := range done {
				if _, err := t.resultErr(); err != nil {
					return true
				}
			}
			return len(pending) == 0
		}
		return len(pending) == 0
	}
	if satisfied() {
		d, p := split()
		return d, p, nil
	}
	wake := make(chan struct{}, 1)
	for _, t := range tasks {
		t.Add_done_callback(func(*Task) {
			select {
			case wake <- struct{}{}:
			default:
			}
		})
	}
	var tm *time.Timer
	var timeoutCh <-chan time.Time
	if timeout >= 0 {
		d, _ := Duration(timeout)
		tm = time.NewTimer(d)
		defer tm.Stop()
		timeoutCh = tm.C
	}
	err := leafAwait(func(cancel <-chan struct{}) error {
		for !satisfied() {
			select {
			case <-wake:
			case <-timeoutCh:
				return nil
			case <-cancel:
				return CancelledError.New()
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	d, p := split()
	return d, p, nil
}

// Uncancel is task.uncancel() (3.11+): clears a pending cancellation
// request so the next await does not raise CancelledError again.
func (t *Task) Uncancel() int {
	t.cancelPend.Store(false)
	select {
	case <-t.cancelCh:
	default:
	}
	return 0
}

// Gather is `await asyncio.gather(*coros, return_exceptions=...)`.
func Gather(fns []func() (any, error), kw ...Kw) ([]any, error) {
	l := Current_loop()
	if l == nil {
		return nil, RuntimeError.New("no running event loop")
	}
	returnExc := false
	for _, k := range kw {
		if r, ok := k["return_exceptions"].(bool); ok {
			returnExc = r
		}
	}
	tasks := make([]*Task, len(fns))
	for i, fn := range fns {
		tasks[i] = l.Create_task(fn)
	}
	results := make([]any, len(fns))
	var first error
	for i, t := range tasks {
		res, err := t.Await()
		if err != nil {
			if IsInstance(err, CancelledError) && Current_task() != nil && Current_task().Cancelling() > 0 {
				for _, o := range tasks {
					o.Cancel()
				}
				return nil, err
			}
			if returnExc {
				results[i] = err
				continue
			}
			if first == nil {
				first = err
			}
			continue
		}
		results[i] = res
	}
	if first != nil {
		return results, first
	}
	return results, nil
}

// Shield is asyncio.shield(coro): the inner task keeps running when the
// awaiting task is cancelled.
func Shield(fn func() (any, error)) (any, error) {
	l := Current_loop()
	if l == nil {
		return nil, RuntimeError.New("no running event loop")
	}
	return l.Create_task(fn).Await()
}

// ---------------------------------------------------------------------------
// asyncio.Event / Lock / Semaphore / Queue
// ---------------------------------------------------------------------------

// AsyncEvent is asyncio.Event (used only from loop goroutines).
type AsyncEvent struct{ ev Event }

// New_AsyncEvent creates a cleared event.
func New_AsyncEvent() *AsyncEvent { return &AsyncEvent{} }

// Set is event.set().
func (e *AsyncEvent) Set() { e.ev.Set() }

// Clear is event.clear().
func (e *AsyncEvent) Clear() { e.ev.Clear() }

// Is_set is event.is_set().
func (e *AsyncEvent) Is_set() bool { return e.ev.Is_set() }

// Wait is `await event.wait()`.
func (e *AsyncEvent) Wait() error {
	if e.ev.Is_set() {
		return nil
	}
	return Await_chan(e.ev.Chan())
}

// AsyncSemaphore is asyncio.Semaphore / BoundedSemaphore.
type AsyncSemaphore struct {
	mu      sync.Mutex
	value   int
	initial int
	bounded bool
	waiters []chan struct{}
}

// New_AsyncSemaphore creates asyncio.Semaphore(value).
func New_AsyncSemaphore(value int) *AsyncSemaphore {
	return &AsyncSemaphore{value: value, initial: value}
}

// New_AsyncBoundedSemaphore creates asyncio.BoundedSemaphore(value).
func New_AsyncBoundedSemaphore(value int) *AsyncSemaphore {
	s := New_AsyncSemaphore(value)
	s.bounded = true
	return s
}

// Locked is sem.locked().
func (s *AsyncSemaphore) Locked() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.value == 0 }

// Acquire is `await sem.acquire()`.
func (s *AsyncSemaphore) Acquire() error {
	for {
		s.mu.Lock()
		if s.value > 0 {
			s.value--
			s.mu.Unlock()
			return nil
		}
		ch := make(chan struct{})
		s.waiters = append(s.waiters, ch)
		s.mu.Unlock()
		err := Await_chan(ch)
		if err != nil {
			s.mu.Lock()
			for i, w := range s.waiters {
				if w == ch {
					s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
					break
				}
			}
			s.mu.Unlock()
			return err
		}
	}
}

// Release is sem.release().
func (s *AsyncSemaphore) Release() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bounded && s.value >= s.initial {
		return ValueError.New("BoundedSemaphore released too many times")
	}
	s.value++
	if len(s.waiters) > 0 {
		close(s.waiters[0])
		s.waiters = s.waiters[1:]
	}
	return nil
}

// AsyncLock is asyncio.Lock.
type AsyncLock struct{ sem *AsyncSemaphore }

// New_AsyncLock creates an asyncio.Lock.
func New_AsyncLock() *AsyncLock { return &AsyncLock{sem: New_AsyncSemaphore(1)} }

// Acquire is `await lock.acquire()`.
func (l *AsyncLock) Acquire() error { return l.sem.Acquire() }

// Release is lock.release().
func (l *AsyncLock) Release() error {
	if !l.sem.Locked() {
		return RuntimeError.New("Lock is not acquired.")
	}
	return l.sem.Release()
}

// Locked is lock.locked().
func (l *AsyncLock) Locked() bool { return l.sem.Locked() }

// AsyncQueue is asyncio.Queue.
type AsyncQueue struct {
	mu      sync.Mutex
	items   []any
	maxsize int
	getters []chan struct{}
	putters []chan struct{}
	unfinished int
	joinCh  chan struct{}
}

// New_AsyncQueue creates asyncio.Queue(maxsize).
func New_AsyncQueue(maxsize int) *AsyncQueue {
	return &AsyncQueue{maxsize: maxsize, joinCh: make(chan struct{})}
}

// Qsize is queue.qsize().
func (q *AsyncQueue) Qsize() int { q.mu.Lock(); defer q.mu.Unlock(); return len(q.items) }

// Empty is queue.empty().
func (q *AsyncQueue) Empty() bool { return q.Qsize() == 0 }

// Full is queue.full().
func (q *AsyncQueue) Full() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.maxsize > 0 && len(q.items) >= q.maxsize
}

// Put_nowait is queue.put_nowait(item); QueueFull when bounded and full.
func (q *AsyncQueue) Put_nowait(item any) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.maxsize > 0 && len(q.items) >= q.maxsize {
		return QueueFull.New()
	}
	q.items = append(q.items, item)
	q.unfinished++
	if len(q.getters) > 0 {
		close(q.getters[0])
		q.getters = q.getters[1:]
	}
	return nil
}

// Put is `await queue.put(item)`.
func (q *AsyncQueue) Put(item any) error {
	for {
		q.mu.Lock()
		if !(q.maxsize > 0 && len(q.items) >= q.maxsize) {
			q.mu.Unlock()
			return q.Put_nowait(item)
		}
		ch := make(chan struct{})
		q.putters = append(q.putters, ch)
		q.mu.Unlock()
		if err := Await_chan(ch); err != nil {
			return err
		}
	}
}

// Get_nowait is queue.get_nowait(); QueueEmpty when empty.
func (q *AsyncQueue) Get_nowait() (any, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return nil, QueueEmpty.New()
	}
	v := q.items[0]
	q.items = q.items[1:]
	if len(q.putters) > 0 {
		close(q.putters[0])
		q.putters = q.putters[1:]
	}
	return v, nil
}

// Get is `await queue.get()`.
func (q *AsyncQueue) Get() (any, error) {
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			q.mu.Unlock()
			return q.Get_nowait()
		}
		ch := make(chan struct{})
		q.getters = append(q.getters, ch)
		q.mu.Unlock()
		if err := Await_chan(ch); err != nil {
			return nil, err
		}
	}
}

// Task_done is queue.task_done().
func (q *AsyncQueue) Task_done() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.unfinished <= 0 {
		return ValueError.New("task_done() called too many times")
	}
	q.unfinished--
	if q.unfinished == 0 {
		close(q.joinCh)
		q.joinCh = make(chan struct{})
	}
	return nil
}

// Join is `await queue.join()`.
func (q *AsyncQueue) Join() error {
	q.mu.Lock()
	if q.unfinished == 0 {
		q.mu.Unlock()
		return nil
	}
	ch := q.joinCh
	q.mu.Unlock()
	return Await_chan(ch)
}

// Iscoroutinefunction reports whether fn is a coroutine function in the
// port's convention: the named type CoroutineFunc (user tasks declared
// async) or any func already carrying the (any, error) coroutine shape
// registered through AsCoroutine.
func Iscoroutinefunction(fn any) bool {
	switch fn.(type) {
	case CoroutineFunc, *CoroutineFunc:
		return true
	}
	return false
}

// CoroutineFunc marks a user-supplied `async def` task: calling it runs the
// coroutine body (on the loop goroutine, with the loop lock held).
type CoroutineFunc func(args ...any) (any, error)

// AsCoroutine wraps a plain function as a coroutine function.
func AsCoroutine(fn func(args ...any) (any, error)) CoroutineFunc { return CoroutineFunc(fn) }

// Partial is functools.partial(coro_fn, *args): the result is still a
// coroutine function (inspect.iscoroutinefunction(partial(coro)) is True).
func (f CoroutineFunc) Partial(args ...any) CoroutineFunc {
	bound := append([]any(nil), args...)
	return func(more ...any) (any, error) { return f(append(append([]any(nil), bound...), more...)...) }
}

// Coroutine is an un-awaited coroutine *object*: what calling an `async
// def` without awaiting it produces (``lambda: aiowork()``). Awaiting it
// runs the body on the current task goroutine.
type Coroutine func() (any, error)

// Await is `await coro`.
func (c Coroutine) Await() (any, error) { return c() }

// Coro is ``coro_fn(*args)`` without the await: builds the coroutine
// object for fn so a sync shim can return it to be awaited by the runner.
func Coro(fn CoroutineFunc, args ...any) Coroutine {
	return func() (any, error) { return fn(args...) }
}

// Iscoroutine is inspect.iscoroutine(v).
func Iscoroutine(v any) bool {
	switch v.(type) {
	case Coroutine, *Coroutine:
		return true
	}
	return false
}

// Awaitable is anything `await x` accepts in the port: coroutine objects,
// Tasks, CFutures and laila futures all expose Await().
type Awaitable interface {
	Await() (any, error)
}

// Await is `await v` for an arbitrary awaitable value (TypeError when v
// cannot be awaited).
func Await(v any) (any, error) {
	switch a := v.(type) {
	case Awaitable:
		return a.Await()
	case *Coroutine:
		return (*a)()
	case func() (any, error):
		return a()
	}
	return nil, TypeError.Newf("object %s can't be used in 'await' expression", Pytypename(v))
}

// Partial is functools.partial(fn, *args) for the port's callable shapes;
// coroutine functions stay coroutine functions.
func Partial(fn any, args ...any) any {
	if f, ok := fn.(CoroutineFunc); ok {
		return f.Partial(args...)
	}
	// A partial of a sync callable stays sync (PartialFunc is callable via
	// py.Call and Iscoroutinefunction reports False for it); ``.func`` /
	// ``.args`` stay inspectable, which the process pool relies on to
	// resolve the registered task behind a partial.
	return &PartialFunc{Func: fn, Args: append([]any(nil), args...)}
}

// Run is asyncio.run(coro): a fresh loop runs fn to completion on the
// calling goroutine's behalf and is closed afterwards.
func Run(fn func() (any, error)) (any, error) {
	if Current_loop() != nil {
		return nil, RuntimeError.New("asyncio.run() cannot be called from a running event loop")
	}
	l := New_event_loop()
	res, err := l.Run_until_complete(fn)
	for _, t := range l.All_tasks() {
		t.Cancel()
	}
	_ = l.Close()
	return res, err
}

