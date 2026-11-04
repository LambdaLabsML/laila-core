package py

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// CFuture is concurrent.futures.Future.
type CFuture struct {
	mu        sync.Mutex
	done      chan struct{}
	state     string // "PENDING", "RUNNING", "CANCELLED", "FINISHED"
	result    any
	err       error
	callbacks []func(*CFuture)
}

// NewCFuture creates a pending future.
func NewCFuture() *CFuture { return &CFuture{done: make(chan struct{}), state: "PENDING"} }

// Cancel is future.cancel(): only pending futures can be cancelled.
func (f *CFuture) Cancel() bool {
	f.mu.Lock()
	if f.state != "PENDING" {
		cancelled := f.state == "CANCELLED"
		f.mu.Unlock()
		return cancelled
	}
	f.state = "CANCELLED"
	close(f.done)
	cbs := f.callbacks
	f.callbacks = nil
	f.mu.Unlock()
	for _, cb := range cbs {
		f.safeCallback(cb)
	}
	return true
}

// Cancelled is future.cancelled().
func (f *CFuture) Cancelled() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.state == "CANCELLED" }

// Running is future.running().
func (f *CFuture) Running() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.state == "RUNNING" }

// Done is future.done().
func (f *CFuture) Done() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.state == "CANCELLED" || f.state == "FINISHED"
}

// Set_running_or_notify_cancel transitions PENDING -> RUNNING.
func (f *CFuture) Set_running_or_notify_cancel() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state == "CANCELLED" {
		return false
	}
	if f.state != "PENDING" {
		panic(RuntimeError.Newf("Future in unexpected state: %s", f.state))
	}
	f.state = "RUNNING"
	return true
}

func (f *CFuture) finish(result any, err error) error {
	f.mu.Lock()
	if f.state == "CANCELLED" || f.state == "FINISHED" {
		f.mu.Unlock()
		return InvalidStateError.Newf("%s: %s", f.state, f.Repr())
	}
	f.result, f.err = result, err
	f.state = "FINISHED"
	close(f.done)
	cbs := f.callbacks
	f.callbacks = nil
	f.mu.Unlock()
	for _, cb := range cbs {
		f.safeCallback(cb)
	}
	return nil
}

// Set_result is future.set_result(v).
func (f *CFuture) Set_result(v any) error { return f.finish(v, nil) }

// Set_exception is future.set_exception(exc).
func (f *CFuture) Set_exception(err error) error { return f.finish(nil, err) }

func (f *CFuture) safeCallback(cb func(*CFuture)) {
	defer func() {
		if r := recover(); r != nil {
			Logger_warning("exception calling callback for %s: %v", f.Repr(), r)
		}
	}()
	cb(f)
}

// Add_done_callback is future.add_done_callback(fn); runs immediately if done.
func (f *CFuture) Add_done_callback(cb func(*CFuture)) {
	f.mu.Lock()
	if f.state == "CANCELLED" || f.state == "FINISHED" {
		f.mu.Unlock()
		f.safeCallback(cb)
		return
	}
	f.callbacks = append(f.callbacks, cb)
	f.mu.Unlock()
}

// Wait blocks until done or timeout (seconds, < 0 forever); returns false on timeout.
func (f *CFuture) Wait(timeout float64) bool {
	// A finished future never times out, whatever the timeout (Python
	// checks the state before waiting; a zero timeout must not race).
	select {
	case <-f.done:
		return true
	default:
	}
	d, ok := Duration(timeout)
	if !ok {
		<-f.done
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-f.done:
		return true
	case <-t.C:
		return false
	}
}

// Result is future.result(timeout): the value, the stored exception, a
// CancelledError, or TimeoutError when the wait expires.
func (f *CFuture) Result(timeout float64) (any, error) {
	if !f.Wait(timeout) {
		return nil, TimeoutError.New()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state == "CANCELLED" {
		return nil, CancelledError.New()
	}
	return f.result, f.err
}

// Exception is future.exception(timeout).
func (f *CFuture) Exception(timeout float64) (error, error) {
	if !f.Wait(timeout) {
		return nil, TimeoutError.New()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state == "CANCELLED" {
		return nil, CancelledError.New()
	}
	return f.err, nil
}

// Chan exposes the completion channel for select.
func (f *CFuture) Chan() <-chan struct{} { return f.done }

// Await is `await asyncio.wrap_future(f)`: a leaf await on completion.
func (f *CFuture) Await() (any, error) { return Await_cfuture(f) }

// Repr is <Future at 0x... state=...>.
func (f *CFuture) Repr() string {
	return fmt.Sprintf("<Future at %p state=%s>", f, map[string]string{
		"PENDING": "pending", "RUNNING": "running", "CANCELLED": "cancelled", "FINISHED": "finished",
	}[f.state])
}

// ---------------------------------------------------------------------------
// ThreadPoolExecutor
// ---------------------------------------------------------------------------

// ThreadPoolExecutor is concurrent.futures.ThreadPoolExecutor: workers are
// started lazily (one per submit while idle workers are absent) up to
// max_workers, exactly like CPython's _adjust_thread_count.
type ThreadPoolExecutor struct {
	mu          sync.Mutex
	cond        *sync.Cond
	maxWorkers  int
	prefix      string
	queue       []queuedJob
	idle        atomic.Int32
	workers     int
	shutdown    bool
	wg          sync.WaitGroup
	threads     []*Thread
	initializer func()
}

// Default_max_workers is min(32, os.cpu_count() + 4).
func Default_max_workers() int {
	n := runtime.NumCPU() + 4
	if n > 32 {
		n = 32
	}
	return n
}

// New_ThreadPoolExecutor creates an executor; max_workers <= 0 means the default.
func New_ThreadPoolExecutor(max_workers int, kw ...Kw) *ThreadPoolExecutor {
	if max_workers <= 0 {
		max_workers = Default_max_workers()
	}
	e := &ThreadPoolExecutor{maxWorkers: max_workers}
	e.cond = sync.NewCond(&e.mu)
	for _, k := range kw {
		if p, ok := k["thread_name_prefix"].(string); ok {
			e.prefix = p
		}
		if init, ok := k["initializer"].(func()); ok {
			e.initializer = init
		}
	}
	if e.prefix == "" {
		e.prefix = fmt.Sprintf("ThreadPoolExecutor-%d", executorCounter.Add(1)-1)
	}
	return e
}

var executorCounter atomic.Int64

type queuedJob struct {
	fut *CFuture
	run func()
}

// Max_workers returns the configured bound.
func (e *ThreadPoolExecutor) Max_workers() int { return e.maxWorkers }

func (e *ThreadPoolExecutor) spawnWorker() {
	n := e.workers
	e.workers++
	e.wg.Add(1)
	t := New_Thread(Kw{"name": fmt.Sprintf("%s_%d", e.prefix, n), "daemon": true, "target": func() {
		defer e.wg.Done()
		if e.initializer != nil {
			e.initializer()
		}
		for {
			e.mu.Lock()
			for len(e.queue) == 0 && !e.shutdown {
				e.cond.Wait()
			}
			if len(e.queue) == 0 {
				e.mu.Unlock()
				return
			}
			q := e.queue[0]
			e.queue = e.queue[1:]
			e.mu.Unlock()
			q.run()
			e.idle.Add(1)
		}
	}})
	e.threads = append(e.threads, t)
	_ = t.Start()
}

// Submit is executor.submit(fn): fn's result/error resolve the future. fn
// runs with a copy of the submitter's contextvars (Python copies nothing,
// but laila's tasks set their own vars before use, so this is harmless and
// lets GLS-based locals behave).
func (e *ThreadPoolExecutor) Submit(fn func() (any, error)) (*CFuture, error) {
	e.mu.Lock()
	if e.shutdown {
		e.mu.Unlock()
		return nil, RuntimeError.New("cannot schedule new futures after shutdown")
	}
	fut := NewCFuture()
	job := func() {
		if !fut.Set_running_or_notify_cancel() {
			return
		}
		var (
			res any
			err error
		)
		func() {
			defer Recover(&err)
			res, err = fn()
		}()
		_ = fut.finish(res, err)
	}
	// _adjust_thread_count: spawn when no idle worker and below the bound.
	if e.idle.Load() > 0 {
		e.idle.Add(-1)
	} else if e.workers < e.maxWorkers {
		e.spawnWorker()
	}
	e.queue = append(e.queue, queuedJob{fut: fut, run: job})
	e.cond.Signal()
	e.mu.Unlock()
	return fut, nil
}

// Go submits a function without a result (fire and forget).
func (e *ThreadPoolExecutor) Go(fn func()) (*CFuture, error) {
	return e.Submit(func() (any, error) { fn(); return nil, nil })
}

// Shutdown is executor.shutdown(wait, cancel_futures).
func (e *ThreadPoolExecutor) Shutdown(wait bool, kw ...Kw) {
	e.mu.Lock()
	if e.shutdown {
		e.mu.Unlock()
		if wait {
			e.joinWorkers()
		}
		return
	}
	e.shutdown = true
	for _, k := range kw {
		if c, _ := k["cancel_futures"].(bool); c {
			for _, q := range e.queue {
				q.fut.Cancel()
			}
			e.queue = nil
		}
	}
	e.cond.Broadcast()
	e.mu.Unlock()
	if wait {
		e.joinWorkers()
	}
}

// joinWorkers waits for every worker to drain and exit. Python joins the
// worker threads, so is_alive() is False once shutdown(wait=True)
// returns; a worker calling shutdown on its own pool skips itself.
func (e *ThreadPoolExecutor) joinWorkers() {
	self := Goid()
	for _, th := range e.Threads() {
		if th.ident != self {
			_ = th.Join(-1)
		}
	}
}

// Threads returns the worker threads created so far.
func (e *ThreadPoolExecutor) Threads() []*Thread {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]*Thread(nil), e.threads...)
}

// Wait_futures is concurrent.futures.wait(fs, timeout, return_when):
// return_when is "ALL_COMPLETED" (default), "FIRST_COMPLETED" or
// "FIRST_EXCEPTION". Returns (done, not_done).
func Wait_futures(fs []*CFuture, timeout float64, return_when string) ([]*CFuture, []*CFuture) {
	if return_when == "" {
		return_when = "ALL_COMPLETED"
	}
	var deadline time.Time
	d, bounded := Duration(timeout)
	if bounded {
		deadline = time.Now().Add(d)
	}
	split := func() ([]*CFuture, []*CFuture) {
		var done, notDone []*CFuture
		for _, f := range fs {
			if f.Done() {
				done = append(done, f)
			} else {
				notDone = append(notDone, f)
			}
		}
		return done, notDone
	}
	for {
		done, notDone := split()
		switch return_when {
		case "FIRST_COMPLETED":
			if len(done) > 0 {
				return done, notDone
			}
		case "FIRST_EXCEPTION":
			for _, f := range done {
				if f.err != nil {
					return done, notDone
				}
			}
			if len(notDone) == 0 {
				return done, notDone
			}
		default:
			if len(notDone) == 0 {
				return done, notDone
			}
		}
		if bounded && !time.Now().Before(deadline) {
			return done, notDone
		}
		// wait for any pending future (or a short tick when bounded)
		cases := make([]<-chan struct{}, 0, len(notDone))
		for _, f := range notDone {
			cases = append(cases, f.done)
		}
		waitAny(cases, deadline, bounded)
	}
}

func waitAny(chans []<-chan struct{}, deadline time.Time, bounded bool) {
	agg := make(chan struct{}, 1)
	var once sync.Once
	for _, ch := range chans {
		ch := ch
		go func() {
			<-ch
			once.Do(func() { agg <- struct{}{} })
		}()
	}
	if bounded {
		t := time.NewTimer(time.Until(deadline))
		defer t.Stop()
		select {
		case <-agg:
		case <-t.C:
		}
		return
	}
	<-agg
}

// As_completed is concurrent.futures.as_completed(fs, timeout): a channel
// that yields futures as they finish; it is closed after the last one, or
// after delivering a TimeoutError through the error pointer.
func As_completed(fs []*CFuture, timeout float64) <-chan *CFuture {
	out := make(chan *CFuture)
	go func() {
		defer close(out)
		pending := append([]*CFuture(nil), fs...)
		for len(pending) > 0 {
			done, notDone := Wait_futures(pending, timeout, "FIRST_COMPLETED")
			if len(done) == 0 {
				return
			}
			for _, f := range done {
				out <- f
			}
			pending = notDone
		}
	}()
	return out
}

// Logger_warning is the hook used by py for "exception in callback" style
// diagnostics; laila's logger module replaces it.
var Logger_warning = func(format string, a ...any) {
	fmt.Printf("WARNING: "+format+"\n", a...)
}
