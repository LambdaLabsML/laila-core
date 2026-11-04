package py

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Duration converts Python's float seconds into a time.Duration; negative
// means "block forever" and is reported through the bool.
func Duration(seconds float64) (time.Duration, bool) {
	if seconds < 0 {
		return 0, false
	}
	return time.Duration(seconds * float64(time.Second)), true
}

// Seconds converts a Duration into Python float seconds.
func Seconds(d time.Duration) float64 { return float64(d) / float64(time.Second) }

// Locker is the common surface of Lock and RLock (what Condition needs).
type Locker interface {
	Acquire(blocking bool, timeout float64) bool
	Release() error
	Locked() bool
	releaseAll() int
	reacquire(n int)
}

// ---------------------------------------------------------------------------
// Lock
// ---------------------------------------------------------------------------

// Lock is threading.Lock: a non-reentrant lock with timeouts.
//
// Uncontended acquire/release is a single CAS; contended waiters park on a
// one-slot wakeup channel so timeouts can be honoured with a timer.
type Lock struct {
	state   atomic.Int32 // 0 unlocked, 1 locked
	waiters atomic.Int32
	once    sync.Once
	wake    chan struct{}
}

// NewLock creates an unlocked Lock.
func NewLock() *Lock { l := &Lock{}; l.init(); return l }

func (l *Lock) init() {
	l.once.Do(func() { l.wake = make(chan struct{}, 1) })
}

// Acquire is lock.acquire(blocking, timeout); timeout < 0 blocks forever.
func (l *Lock) Acquire(blocking bool, timeout float64) bool {
	if l.state.CompareAndSwap(0, 1) {
		return true
	}
	if !blocking {
		return false
	}
	l.init()
	l.waiters.Add(1)
	defer l.waiters.Add(-1)
	d, ok := Duration(timeout)
	if !ok {
		for {
			if l.state.CompareAndSwap(0, 1) {
				return true
			}
			<-l.wake
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	for {
		if l.state.CompareAndSwap(0, 1) {
			return true
		}
		select {
		case <-l.wake:
		case <-t.C:
			return l.state.CompareAndSwap(0, 1)
		}
	}
}

// Lock_ is lock.acquire() (blocking, no timeout).
func (l *Lock) Lock_() { l.Acquire(true, -1) }

// Release is lock.release(); RuntimeError when not held.
func (l *Lock) Release() error {
	if !l.state.CompareAndSwap(1, 0) {
		return RuntimeError.New("release unlocked lock")
	}
	if l.waiters.Load() > 0 {
		l.init()
		select {
		case l.wake <- struct{}{}:
		default:
		}
	}
	return nil
}

// Unlock is Release ignoring the error (for defer).
func (l *Lock) Unlock() { _ = l.Release() }

// Locked is lock.locked().
func (l *Lock) Locked() bool { return l.state.Load() == 1 }

func (l *Lock) releaseAll() int { _ = l.Release(); return 1 }
func (l *Lock) reacquire(n int) { l.Acquire(true, -1) }

// ---------------------------------------------------------------------------
// RLock
// ---------------------------------------------------------------------------

// RLock is threading.RLock: reentrant for the owning goroutine.
type RLock struct {
	lock  Lock
	owner atomic.Int64
	count int
}

// NewRLock creates an unlocked RLock.
func NewRLock() *RLock { r := &RLock{}; r.lock.init(); return r }

// Acquire is rlock.acquire(blocking, timeout).
func (r *RLock) Acquire(blocking bool, timeout float64) bool {
	me := Goid()
	if r.owner.Load() == me {
		r.count++
		return true
	}
	if !r.lock.Acquire(blocking, timeout) {
		return false
	}
	r.owner.Store(me)
	r.count = 1
	return true
}

// Lock_ is rlock.acquire().
func (r *RLock) Lock_() { r.Acquire(true, -1) }

// Release is rlock.release(); RuntimeError when not owned by the caller.
func (r *RLock) Release() error {
	if r.owner.Load() != Goid() {
		return RuntimeError.New("cannot release un-acquired lock")
	}
	r.count--
	if r.count == 0 {
		r.owner.Store(0)
		return r.lock.Release()
	}
	return nil
}

// Unlock is Release ignoring the error (for defer).
func (r *RLock) Unlock() { _ = r.Release() }

// Locked reports whether any goroutine holds the lock.
func (r *RLock) Locked() bool { return r.lock.Locked() }

// Is_owned reports whether the calling goroutine holds the lock.
func (r *RLock) Is_owned() bool { return r.owner.Load() == Goid() }

func (r *RLock) releaseAll() int {
	n := r.count
	r.count = 0
	r.owner.Store(0)
	_ = r.lock.Release()
	return n
}

func (r *RLock) reacquire(n int) {
	r.lock.Acquire(true, -1)
	r.owner.Store(Goid())
	r.count = n
}

// ---------------------------------------------------------------------------
// Event
// ---------------------------------------------------------------------------

// Event is threading.Event.
type Event struct {
	mu  sync.Mutex
	ch  chan struct{}
	set bool
}

// NewEvent creates a cleared Event.
func NewEvent() *Event { return &Event{ch: make(chan struct{})} }

func (e *Event) init() {
	if e.ch == nil {
		e.ch = make(chan struct{})
	}
}

// Set is event.set().
func (e *Event) Set() {
	e.mu.Lock()
	e.init()
	if !e.set {
		e.set = true
		close(e.ch)
	}
	e.mu.Unlock()
}

// Clear is event.clear().
func (e *Event) Clear() {
	e.mu.Lock()
	e.init()
	if e.set {
		e.set = false
		e.ch = make(chan struct{})
	}
	e.mu.Unlock()
}

// Is_set is event.is_set().
func (e *Event) Is_set() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.set
}

// Wait is event.wait(timeout) -> bool; timeout < 0 waits forever.
func (e *Event) Wait(timeout float64) bool {
	e.mu.Lock()
	e.init()
	ch := e.ch
	e.mu.Unlock()
	d, ok := Duration(timeout)
	if !ok {
		<-ch
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ch:
		return true
	case <-t.C:
		return e.Is_set()
	}
}

// Chan exposes the signalling channel (for select in leaf awaits).
func (e *Event) Chan() <-chan struct{} {
	e.mu.Lock()
	e.init()
	ch := e.ch
	e.mu.Unlock()
	return ch
}

// ---------------------------------------------------------------------------
// Condition
// ---------------------------------------------------------------------------

// Condition is threading.Condition over a Lock or RLock.
type Condition struct {
	L       Locker
	mu      sync.Mutex
	waiters []chan struct{}
}

// NewCondition creates a condition; a nil lock means a fresh RLock.
func NewCondition(lock Locker) *Condition {
	if lock == nil {
		lock = NewRLock()
	}
	return &Condition{L: lock}
}

// Acquire acquires the underlying lock.
func (c *Condition) Acquire(blocking bool, timeout float64) bool {
	return c.L.Acquire(blocking, timeout)
}

// Release releases the underlying lock.
func (c *Condition) Release() error { return c.L.Release() }

// Wait is cond.wait(timeout) -> bool. The lock must be held.
func (c *Condition) Wait(timeout float64) bool {
	ch := make(chan struct{})
	c.mu.Lock()
	c.waiters = append(c.waiters, ch)
	c.mu.Unlock()
	n := c.L.releaseAll()
	defer c.L.reacquire(n)
	d, ok := Duration(timeout)
	if !ok {
		<-ch
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ch:
		return true
	case <-t.C:
		c.mu.Lock()
		defer c.mu.Unlock()
		for i, w := range c.waiters {
			if w == ch {
				c.waiters = append(c.waiters[:i], c.waiters[i+1:]...)
				return false
			}
		}
		return true // notified between timeout and cleanup
	}
}

// Wait_for is cond.wait_for(predicate, timeout).
func (c *Condition) Wait_for(pred func() bool, timeout float64) bool {
	var deadline time.Time
	d, bounded := Duration(timeout)
	if bounded {
		deadline = time.Now().Add(d)
	}
	for !pred() {
		if bounded {
			rem := time.Until(deadline)
			if rem <= 0 {
				return pred()
			}
			c.Wait(Seconds(rem))
		} else {
			c.Wait(-1)
		}
	}
	return true
}

// Notify is cond.notify(n).
func (c *Condition) Notify(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < n && len(c.waiters) > 0; i++ {
		close(c.waiters[0])
		c.waiters = c.waiters[1:]
	}
}

// Notify_all is cond.notify_all().
func (c *Condition) Notify_all() { c.Notify(len(c.snapshot())) }

func (c *Condition) snapshot() []chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]chan struct{}(nil), c.waiters...)
}

// ---------------------------------------------------------------------------
// Semaphore / BoundedSemaphore
// ---------------------------------------------------------------------------

// Semaphore is threading.Semaphore with direct hand-off to waiters.
type Semaphore struct {
	mu      sync.Mutex
	value   int
	initial int
	bounded bool
	waiters []chan struct{}
}

// NewSemaphore creates threading.Semaphore(value).
func NewSemaphore(value int) *Semaphore {
	if value < 0 {
		panic(ValueError.New("semaphore initial value must be >= 0"))
	}
	return &Semaphore{value: value, initial: value}
}

// NewBoundedSemaphore creates threading.BoundedSemaphore(value).
func NewBoundedSemaphore(value int) *Semaphore {
	s := NewSemaphore(value)
	s.bounded = true
	return s
}

// Acquire is sem.acquire(blocking, timeout) -> bool. Like CPython's
// Condition-based implementation, a woken waiter re-checks the counter.
func (s *Semaphore) Acquire(blocking bool, timeout float64) bool {
	var deadline time.Time
	d, bounded := Duration(timeout)
	if bounded {
		deadline = time.Now().Add(d)
	}
	s.mu.Lock()
	for s.value == 0 {
		if !blocking {
			s.mu.Unlock()
			return false
		}
		ch := make(chan struct{})
		s.waiters = append(s.waiters, ch)
		s.mu.Unlock()
		if bounded {
			rem := time.Until(deadline)
			if rem <= 0 {
				s.dropWaiter(ch)
				return false
			}
			t := time.NewTimer(rem)
			select {
			case <-ch:
				t.Stop()
			case <-t.C:
				s.dropWaiter(ch)
				s.mu.Lock()
				if s.value == 0 {
					s.mu.Unlock()
					return false
				}
				s.mu.Unlock()
			}
		} else {
			<-ch
		}
		s.mu.Lock()
	}
	s.value--
	s.mu.Unlock()
	return true
}

func (s *Semaphore) dropWaiter(ch chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, w := range s.waiters {
		if w == ch {
			s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
			return
		}
	}
}

// Release is sem.release(n); BoundedSemaphore raises ValueError when the
// counter would exceed its initial value.
func (s *Semaphore) Release(n int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bounded && s.value+n > s.initial {
		return ValueError.New("Semaphore released too many times")
	}
	s.value += n
	for i := 0; i < n && len(s.waiters) > 0; i++ {
		close(s.waiters[0])
		s.waiters = s.waiters[1:]
	}
	return nil
}

// Value returns the current counter (sem._value).
func (s *Semaphore) Value() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.value
}

// ---------------------------------------------------------------------------
// Thread
// ---------------------------------------------------------------------------

// Thread is threading.Thread backed by a goroutine.
type Thread struct {
	mu      sync.Mutex
	name    string
	daemon  bool
	target  func()
	ident   int64
	started bool
	done    chan struct{}
	dummy   bool
}

var (
	threadsMu     sync.Mutex
	activeThreads = map[*Thread]struct{}{}
	threadCounter atomic.Int64
	mainThread    = &Thread{name: "MainThread", ident: 1, started: true, done: make(chan struct{})}
)

func init() {
	activeThreads[mainThread] = struct{}{}
}

// New_Thread is threading.Thread(target=..., name=..., daemon=...).
func New_Thread(kw Kw) *Thread {
	t := &Thread{done: make(chan struct{})}
	if fn, ok := kw["target"].(func()); ok {
		t.target = fn
	}
	if n, ok := kw["name"].(string); ok && n != "" {
		t.name = n
	} else {
		t.name = fmt.Sprintf("Thread-%d", threadCounter.Add(1))
	}
	if d, ok := kw["daemon"].(bool); ok {
		t.daemon = d
	}
	return t
}

// Start is thread.start().
func (t *Thread) Start() error {
	t.mu.Lock()
	if t.started {
		t.mu.Unlock()
		return RuntimeError.New("threads can only be started once")
	}
	t.started = true
	t.mu.Unlock()
	threadsMu.Lock()
	activeThreads[t] = struct{}{}
	threadsMu.Unlock()
	ready := make(chan struct{})
	goWithState(func(s *glsState) {
		s.thread = t
		t.ident = Goid()
	}, func() {
		close(ready)
		defer func() {
			threadsMu.Lock()
			delete(activeThreads, t)
			threadsMu.Unlock()
			close(t.done)
		}()
		defer func() {
			if r := recover(); r != nil {
				// Python prints the traceback to stderr and continues.
				Excepthook(t, PanicToError(r))
			}
		}()
		if t.target != nil {
			t.target()
		}
	})
	<-ready
	return nil
}

// Excepthook is threading.excepthook; replaceable by tests.
var Excepthook = func(t *Thread, err error) {
	fmt.Printf("Exception in thread %s:\n%s: %s\n", t.name, Typename(err), err.Error())
}

// Run executes the target synchronously (thread.run()).
func (t *Thread) Run() {
	if t.target != nil {
		t.target()
	}
}

// Join is thread.join(timeout).
func (t *Thread) Join(timeout float64) error {
	t.mu.Lock()
	started := t.started
	t.mu.Unlock()
	if !started {
		return RuntimeError.New("cannot join thread before it is started")
	}
	if t.ident == Goid() {
		return RuntimeError.New("cannot join current thread")
	}
	d, ok := Duration(timeout)
	if !ok {
		<-t.done
		return nil
	}
	tm := time.NewTimer(d)
	defer tm.Stop()
	select {
	case <-t.done:
	case <-tm.C:
	}
	return nil
}

// Is_alive is thread.is_alive().
func (t *Thread) Is_alive() bool {
	t.mu.Lock()
	started := t.started
	t.mu.Unlock()
	if !started {
		return false
	}
	select {
	case <-t.done:
		return false
	default:
		return true
	}
}

// Name is thread.name.
func (t *Thread) Name() string { t.mu.Lock(); defer t.mu.Unlock(); return t.name }

// Set_name sets thread.name.
func (t *Thread) Set_name(n string) { t.mu.Lock(); t.name = n; t.mu.Unlock() }

// Daemon is thread.daemon.
func (t *Thread) Daemon() bool { return t.daemon }

// Set_daemon sets thread.daemon (before start).
func (t *Thread) Set_daemon(d bool) { t.daemon = d }

// Ident is thread.ident (the goroutine id).
func (t *Thread) Ident() int64 { return t.ident }

// Native_id is thread.native_id.
func (t *Thread) Native_id() int64 { return t.ident }

// Repr is <Thread(name, state)>.
func (t *Thread) Repr() string {
	state := "initial"
	if t.Is_alive() {
		state = "started"
	} else if t.started {
		state = "stopped"
	}
	if t.daemon {
		state += " daemon"
	}
	return fmt.Sprintf("<Thread(%s, %s %d)>", t.Name(), state, t.ident)
}

// Current_thread is threading.current_thread(); foreign goroutines get a
// registered dummy thread ("Dummy-N"), like foreign OS threads in Python.
func Current_thread() *Thread {
	id := Goid()
	if id == 1 {
		return mainThread
	}
	s := glsEnsure()
	if s.thread != nil {
		return s.thread
	}
	t := &Thread{name: fmt.Sprintf("Dummy-%d", threadCounter.Add(1)), ident: id, started: true, daemon: true, dummy: true, done: make(chan struct{})}
	s.thread = t
	threadsMu.Lock()
	activeThreads[t] = struct{}{}
	threadsMu.Unlock()
	return t
}

// Main_thread is threading.main_thread().
func Main_thread() *Thread { return mainThread }

// Get_ident is threading.get_ident().
func Get_ident() int64 { return Goid() }

// Enumerate is threading.enumerate(): all live threads, main first.
func Enumerate() []*Thread {
	threadsMu.Lock()
	defer threadsMu.Unlock()
	out := make([]*Thread, 0, len(activeThreads))
	for t := range activeThreads {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i] == mainThread {
			return true
		}
		if out[j] == mainThread {
			return false
		}
		return out[i].ident < out[j].ident
	})
	return out
}

// Active_count is threading.active_count().
func Active_count() int { return len(Enumerate()) }

// Timer is threading.Timer(interval, function).
type Timer struct {
	*Thread
	cancel chan struct{}
}

// New_Timer creates a timer thread that runs fn after interval seconds.
func New_Timer(interval float64, fn func()) *Timer {
	tm := &Timer{cancel: make(chan struct{})}
	d, _ := Duration(interval)
	tm.Thread = New_Thread(Kw{"target": func() {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-t.C:
			fn()
		case <-tm.cancel:
		}
	}})
	return tm
}

// Cancel is timer.cancel().
func (t *Timer) Cancel() {
	select {
	case <-t.cancel:
	default:
		close(t.cancel)
	}
}

// Sleep is time.sleep(seconds).
func Sleep_sync(seconds float64) {
	d, _ := Duration(seconds)
	time.Sleep(d)
}

// Time is time.time().
func Time() float64 { return float64(time.Now().UnixNano()) / 1e9 }

// Monotonic is time.monotonic().
func Monotonic() float64 { return Seconds(time.Since(processStart)) }

// Perf_counter is time.perf_counter().
func Perf_counter() float64 { return Monotonic() }

var processStart = time.Now()
