package py

import (
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// errors
// ---------------------------------------------------------------------------

func TestErrorsHierarchy(t *testing.T) {
	err := Raise(FileNotFoundError, 2, "No such file or directory")
	for _, base := range []*Type{FileNotFoundError, OSError, Exception, BaseException} {
		if !errors.Is(err, base) {
			t.Errorf("FileNotFoundError should be a %s", base.Name)
		}
	}
	if errors.Is(err, KeyError) {
		t.Error("FileNotFoundError is not a KeyError")
	}
	if got := err.Error(); got != "(2, 'No such file or directory')" {
		t.Errorf("str(exc) = %q", got)
	}
	if Typename(err) != "FileNotFoundError" {
		t.Errorf("typename %s", Typename(err))
	}
	wrapped := FromGo(errors.New("dial tcp 127.0.0.1:1: connect: connection refused"))
	if !errors.Is(wrapped, ConnectionRefusedError) || !errors.Is(wrapped, OSError) {
		t.Errorf("FromGo classification: %v", wrapped.Repr())
	}
	var perr error
	func() {
		defer Recover(&perr)
		panic(ValueError.New("boom"))
	}()
	if !IsInstance(perr, ValueError) || perr.Error() != "boom" {
		t.Errorf("Recover: %v", perr)
	}
}

// ---------------------------------------------------------------------------
// values
// ---------------------------------------------------------------------------

func TestDictSemantics(t *testing.T) {
	d := NewDict()
	d.Set(1, "int")
	d.Set(1.0, "float") // same key as 1
	d.Set(true, "bool") // same key as 1
	d.Set("1", "str")
	if d.Len() != 2 {
		t.Fatalf("len = %d, want 2 (1 == 1.0 == True)", d.Len())
	}
	if v, _ := d.Get(int64(1)); v != "bool" {
		t.Errorf("d[1] = %v", v)
	}
	if ks := d.Keys(); Repr(ks[0]) != "1" || ks[1] != "1" {
		t.Errorf("key identity preserved as first inserted: %v", Repr(ks))
	}
	d.Set(Tuple{"a", int64(1)}, "tuple-key")
	if v, ok := d.Get(Tuple{"a", 1.0}); !ok || v != "tuple-key" {
		t.Errorf("tuple key lookup with 1.0: %v %v", v, ok)
	}
	if _, err := d.Getitem("missing"); !IsInstance(err, KeyError) || err.Error() != "'missing'" {
		t.Errorf("KeyError: %v", err)
	}
	for i := 0; i < 100; i++ {
		d.Set(i+1000, i)
	}
	for i := 0; i < 90; i++ {
		_ = d.Del(i + 1000)
	}
	if d.Len() != 13 || Repr(d.Keys()[0]) != "1" {
		t.Errorf("after compaction len=%d keys=%s", d.Len(), short(Repr(d.Keys())))
	}
	l := NewList(int64(3), int64(1), int64(2))
	if v, _ := l.Get(-1); v != int64(2) {
		t.Errorf("l[-1] = %v", v)
	}
	if _, err := l.Get(3); !IsInstance(err, IndexError) {
		t.Errorf("IndexError expected: %v", err)
	}
	if !Eq(l.Slice(-2, 10), NewList(int64(1), int64(2))) {
		t.Errorf("slice: %s", Repr(l.Slice(-2, 10)))
	}
	if !Truthy(NewList(0)) || Truthy(NewList()) || Truthy("") || !Truthy("x") || Truthy(0.0) || !Truthy(-1) {
		t.Error("truthiness")
	}
	if !Eq(NewSet(1, 2), NewSet(2.0, int64(1))) {
		t.Error("set equality across numeric types")
	}
}

func TestReprFloat(t *testing.T) {
	cases := map[float64]string{0.1: "0.1", 1e16: "1e+16", 1e15: "1000000000000000.0", 1e-4: "0.0001", 1e-5: "1e-05", 123.0: "123.0", math.Copysign(0, -1): "-0.0", 2.5e-7: "2.5e-07", 1e22: "1e+22", 1.5e300: "1.5e+300"}
	for f, want := range cases {
		if got := Repr_float(f); got != want {
			t.Errorf("repr(%v) = %s want %s", f, got, want)
		}
	}
	if Repr_str("it's") != `"it's"` || Repr_str(`a"b`) != `'a"b'` || Repr_str("both'\"") != `'both\'"'` {
		t.Error("string quoting")
	}
}

// ---------------------------------------------------------------------------
// contextvars / GLS
// ---------------------------------------------------------------------------

func TestContextVarInheritance(t *testing.T) {
	cv := NewContextVar[string]("who", "nobody")
	tok := cv.Set("parent")
	var child string
	var wg sync.WaitGroup
	wg.Add(1)
	Go(func() {
		defer wg.Done()
		child = cv.Value()
		cv.Set("child") // must not leak back
	})
	wg.Wait()
	if child != "parent" {
		t.Errorf("child saw %q", child)
	}
	if cv.Value() != "parent" {
		t.Errorf("parent now sees %q", cv.Value())
	}
	if err := cv.Reset(tok); err != nil {
		t.Fatal(err)
	}
	if cv.Value() != "nobody" {
		t.Errorf("after reset %q", cv.Value())
	}
	if err := cv.Reset(tok); !IsInstance(err, RuntimeError) {
		t.Errorf("double reset: %v", err)
	}
	// plain goroutines (not via py.Go) see the default
	done := make(chan string)
	cv.Set("outer")
	go func() { done <- cv.Value() }()
	if v := <-done; v != "nobody" {
		t.Errorf("plain goroutine saw %q", v)
	}
	// clear-on-exit: no entry left behind
	before := glsCount()
	wg.Add(1)
	Go(func() { defer wg.Done(); cv.Set("x") })
	wg.Wait()
	if after := glsCount(); after != before {
		t.Errorf("gls leak: %d -> %d", before, after)
	}
}

func glsCount() int {
	n := 0
	for i := range gls {
		gls[i].mu.RLock()
		n += len(gls[i].m)
		gls[i].mu.RUnlock()
	}
	return n
}

// ---------------------------------------------------------------------------
// threading
// ---------------------------------------------------------------------------

func TestRLockReentrancy(t *testing.T) {
	r := NewRLock()
	if !r.Acquire(true, -1) || !r.Acquire(true, -1) {
		t.Fatal("reentrant acquire")
	}
	if !r.Is_owned() || !r.Locked() {
		t.Fatal("owned/locked")
	}
	other := make(chan bool)
	go func() { other <- r.Acquire(true, 0.05) }()
	if <-other {
		t.Fatal("other goroutine should time out")
	}
	_ = r.Release()
	_ = r.Release()
	if err := r.Release(); !IsInstance(err, RuntimeError) {
		t.Errorf("over-release: %v", err)
	}
	go func() { other <- r.Acquire(true, 1) }()
	if !<-other {
		t.Fatal("other goroutine should acquire now")
	}
	if err := r.Release(); !IsInstance(err, RuntimeError) {
		t.Errorf("release from non-owner must fail: %v", err)
	}
}

func TestLockContended(t *testing.T) {
	var l Lock // zero value must be usable
	if l.Locked() {
		t.Fatal("zero lock locked")
	}
	if err := l.Release(); !IsInstance(err, RuntimeError) {
		t.Fatalf("release unlocked: %v", err)
	}
	if !l.Acquire(false, 0) || l.Acquire(false, 0) {
		t.Fatal("non-blocking acquire semantics")
	}
	if l.Acquire(true, 0.01) {
		t.Fatal("timed acquire on held lock must fail")
	}
	l.Unlock()

	// many goroutines hammering a counter under the lock; every waiter must
	// eventually be woken (no lost wakeups) and mutual exclusion must hold.
	const G, N = 16, 2000
	var wg sync.WaitGroup
	counter, inside := 0, int32(0)
	for g := 0; g < G; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < N; i++ {
				if g%2 == 0 {
					l.Lock_()
				} else {
					for !l.Acquire(true, 0.001) {
					}
				}
				if atomic.AddInt32(&inside, 1) != 1 {
					t.Error("mutual exclusion violated")
				}
				counter++
				atomic.AddInt32(&inside, -1)
				l.Unlock()
			}
		}(g)
	}
	wg.Wait()
	if counter != G*N {
		t.Fatalf("counter %d != %d", counter, G*N)
	}
	if l.Locked() {
		t.Fatal("lock left held")
	}
}

func TestThreadRegistryAndEvent(t *testing.T) {
	ev := NewEvent()
	seenCh := make(chan *Thread, 1)
	th := New_Thread(Kw{"name": "worker-1", "target": func() {
		seenCh <- Current_thread()
		ev.Wait(-1)
	}})
	if th.Is_alive() {
		t.Fatal("not started yet")
	}
	_ = th.Start()
	seen := <-seenCh
	if !th.Is_alive() || seen != th || seen.Name() != "worker-1" {
		t.Fatalf("thread identity: alive=%v seen=%v", th.Is_alive(), seen)
	}
	found := false
	for _, x := range Enumerate() {
		if x == th {
			found = true
		}
	}
	if !found {
		t.Error("enumerate should list the worker")
	}
	if err := th.Start(); !IsInstance(err, RuntimeError) {
		t.Errorf("second start: %v", err)
	}
	ev.Set()
	_ = th.Join(1)
	if th.Is_alive() {
		t.Error("should have finished")
	}
	if Main_thread().Name() != "MainThread" || !strings.HasPrefix(Current_thread().Name(), "Dummy-") {
		t.Errorf("main thread %s / test goroutine %s", Main_thread().Name(), Current_thread().Name())
	}
	cond := NewCondition(nil)
	var got int32
	cond.Acquire(true, -1)
	go func() {
		time.Sleep(10 * time.Millisecond)
		cond.Acquire(true, -1)
		atomic.StoreInt32(&got, 1)
		cond.Notify_all()
		_ = cond.Release()
	}()
	if !cond.Wait_for(func() bool { return atomic.LoadInt32(&got) == 1 }, 1) {
		t.Error("condition wait_for timed out")
	}
	_ = cond.Release()
	sem := NewBoundedSemaphore(1)
	if !sem.Acquire(true, -1) || sem.Acquire(false, 0) {
		t.Error("semaphore")
	}
	_ = sem.Release(1)
	if err := sem.Release(1); !IsInstance(err, ValueError) {
		t.Errorf("bounded over-release: %v", err)
	}
}

func TestThreadPoolExecutor(t *testing.T) {
	ex := New_ThreadPoolExecutor(2, Kw{"thread_name_prefix": "pool"})
	var running int32
	var peak int32
	futs := []*CFuture{}
	for i := 0; i < 6; i++ {
		f, err := ex.Submit(func() (any, error) {
			n := atomic.AddInt32(&running, 1)
			for {
				p := atomic.LoadInt32(&peak)
				if n <= p || atomic.CompareAndSwapInt32(&peak, p, n) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			atomic.AddInt32(&running, -1)
			return "ok", nil
		})
		if err != nil {
			t.Fatal(err)
		}
		futs = append(futs, f)
	}
	done, notDone := Wait_futures(futs, 5, "ALL_COMPLETED")
	if len(done) != 6 || len(notDone) != 0 {
		t.Fatalf("wait: %d/%d", len(done), len(notDone))
	}
	if peak > 2 {
		t.Errorf("max_workers violated: peak %d", peak)
	}
	if len(ex.Threads()) != 2 || !strings.HasPrefix(ex.Threads()[0].Name(), "pool_") {
		t.Errorf("threads: %v", ex.Threads())
	}
	f, _ := ex.Submit(func() (any, error) { return nil, ValueError.New("bad") })
	if _, err := f.Result(1); !IsInstance(err, ValueError) {
		t.Errorf("result error: %v", err)
	}
	ex.Shutdown(true)
	if _, err := ex.Submit(func() (any, error) { return nil, nil }); !IsInstance(err, RuntimeError) {
		t.Errorf("submit after shutdown: %v", err)
	}
	for _, th := range ex.Threads() {
		if th.Is_alive() {
			t.Error("worker still alive after shutdown(wait=True)")
		}
	}
}

// ---------------------------------------------------------------------------
// asyncio
// ---------------------------------------------------------------------------

func startLoop(t *testing.T) (*Loop, func()) {
	t.Helper()
	l := New_event_loop()
	th := New_Thread(Kw{"name": "loop", "target": func() { _ = l.Run_forever() }})
	_ = th.Start()
	for !l.Is_running() {
		time.Sleep(time.Millisecond)
	}
	return l, func() {
		l.Stop()
		_ = th.Join(2)
		_ = l.Close()
	}
}

func TestLoopFIFOInterleaving(t *testing.T) {
	l, stop := startLoop(t)
	defer stop()
	var order []string
	var mu sync.Mutex
	log := func(s string) { mu.Lock(); order = append(order, s); mu.Unlock() }
	cf := Run_coroutine_threadsafe(func() (any, error) {
		log("A1")
		tb := l.Create_task(func() (any, error) {
			log("B1")
			if err := Sleep(0); err != nil {
				return nil, err
			}
			log("B2")
			return "b", nil
		})
		log("A2") // B must not have run yet: A holds the loop until its first await
		if err := Sleep(0); err != nil {
			return nil, err
		}
		log("A3")
		v, err := tb.Await()
		log("A4")
		return v, err
	}, l)
	v, err := cf.Result(2)
	if err != nil || v != "b" {
		t.Fatalf("result %v %v", v, err)
	}
	want := "A1 A2 B1 A3 B2 A4"
	if got := strings.Join(order, " "); got != want {
		t.Errorf("interleaving\n got %s\nwant %s", got, want)
	}
}

func TestLoopCancellationAndTimeout(t *testing.T) {
	l, stop := startLoop(t)
	defer stop()
	started := make(chan struct{})
	cleaned := int32(0)
	task := l.Create_task(func() (any, error) {
		close(started)
		err := Sleep(10) // long
		if IsInstance(err, CancelledError) {
			// cleanup awaits still work after the one-shot cancel delivery
			if e2 := Sleep(0.01); e2 != nil {
				return nil, e2
			}
			atomic.StoreInt32(&cleaned, 1)
			return nil, err
		}
		return "finished", err
	})
	<-started
	time.Sleep(5 * time.Millisecond)
	if !task.Cancel() {
		t.Fatal("cancel should be accepted")
	}
	<-task.Chan()
	if !task.Cancelled() || atomic.LoadInt32(&cleaned) != 1 {
		t.Errorf("cancelled=%v cleaned=%d", task.Cancelled(), cleaned)
	}
	if _, err := task.Result(); !IsInstance(err, CancelledError) {
		t.Errorf("result: %v", err)
	}
	cf := Run_coroutine_threadsafe(func() (any, error) {
		return Wait_for(func() (any, error) { return nil, Sleep(5) }, 0.02)
	}, l)
	if _, err := cf.Result(2); !IsInstance(err, TimeoutError) {
		t.Errorf("wait_for: %v", err)
	}
	cf = Run_coroutine_threadsafe(func() (any, error) {
		res, err := Gather([]func() (any, error){
			func() (any, error) { return 1, Sleep(0.01) },
			func() (any, error) { return 2, nil },
			func() (any, error) { return nil, ValueError.New("x") },
		}, Kw{"return_exceptions": true})
		return res, err
	}, l)
	v, err := cf.Result(2)
	if err != nil {
		t.Fatal(err)
	}
	res := v.([]any)
	if res[0] != 1 || res[1] != 2 || !IsInstance(res[2].(error), ValueError) {
		t.Errorf("gather: %v", Repr(res))
	}
	if len(l.All_tasks()) != 0 {
		t.Errorf("tasks left: %d", len(l.All_tasks()))
	}
}

func TestToThreadReleasesLoop(t *testing.T) {
	l, stop := startLoop(t)
	defer stop()
	cv := NewContextVar[string]("ctx", "")
	cf := Run_coroutine_threadsafe(func() (any, error) {
		cv.Set("task")
		var inThread string
		other := l.Create_task(func() (any, error) { return "other ran", nil })
		v, err := To_thread(func() (any, error) {
			inThread = cv.Value()
			if Current_loop() != nil {
				return nil, RuntimeError.New("to_thread worker must not be a loop goroutine")
			}
			<-other.Chan() // the other task must be able to run while we block here
			return "thread", nil
		})
		if err != nil {
			return nil, err
		}
		return Tuple{v, inThread}, nil
	}, l)
	v, err := cf.Result(2)
	if err != nil {
		t.Fatal(err)
	}
	if !Eq(v, Tuple{"thread", "task"}) {
		t.Errorf("to_thread: %s", Repr(v))
	}
	if _, err := Get_running_loop(); !IsInstance(err, RuntimeError) {
		t.Errorf("no running loop here: %v", err)
	}
}

func TestAsyncQueueAndEvent(t *testing.T) {
	l, stop := startLoop(t)
	defer stop()
	q := New_AsyncQueue(1)
	ev := New_AsyncEvent()
	cf := Run_coroutine_threadsafe(func() (any, error) {
		producer := l.Create_task(func() (any, error) {
			for i := 0; i < 3; i++ {
				if err := q.Put(i); err != nil {
					return nil, err
				}
			}
			ev.Set()
			return nil, nil
		})
		var got []any
		for i := 0; i < 3; i++ {
			v, err := q.Get()
			if err != nil {
				return nil, err
			}
			got = append(got, v)
		}
		if err := ev.Wait(); err != nil {
			return nil, err
		}
		if _, err := producer.Await(); err != nil {
			return nil, err
		}
		return got, nil
	}, l)
	v, err := cf.Result(2)
	if err != nil || !Eq(v, []any{0, 1, 2}) {
		t.Errorf("queue: %v %v", Repr(v), err)
	}
}

// ---------------------------------------------------------------------------
// pydantic
// ---------------------------------------------------------------------------

type Base struct {
	Object
	Name  string `py:"name"`
	Count int    `py:"count" default:"0"`
	Tags  []string
	Lock_ *RLock `py:"-"`
	hits  int
}

type Child struct {
	Base
	Ratio float64 `py:"ratio,gt=0,le=1" default:"0.5"`
	Extra *Dict
	post  []string
}

var (
	Base_class  = NewClass("Base", (*Base)(nil))
	Child_class = NewClass("Child", (*Child)(nil), Base_class)
)

func (b *Base) Model_post_init(ctx any) error {
	b.hits++
	if c, ok := b.Self().(*Child); ok {
		c.post = append(c.post, "Base")
	}
	return nil
}

func (c *Child) Model_post_init(ctx any) error {
	if err := c.Base.Model_post_init(ctx); err != nil {
		return err
	}
	c.post = append(c.post, "Child")
	return nil
}

func (c *Child) Describe() string { return c.Name + "!" }

func TestPydanticInitAndDump(t *testing.T) {
	Child_class.Add_before(func(cls *Class, data Kw) (Kw, error) {
		if _, ok := data["name"]; !ok {
			data["name"] = "defaulted"
		}
		return data, nil
	})
	c := &Child{}
	if err := Init(c, Child_class, Kw{"count": "7", "tags": NewList("a", "b"), "ratio": 1}); err != nil {
		t.Fatal(err)
	}
	if c.Name != "defaulted" || c.Count != 7 || len(c.Tags) != 2 || c.Ratio != 1 || c.hits != 1 {
		t.Errorf("init: %+v", c)
	}
	if strings.Join(c.post, ",") != "Base,Child" {
		t.Errorf("post_init chain: %v", c.post)
	}
	if !Isinstance(c, Base_class) || !Isinstance(c, Child_class) || Isinstance(&Base{}, Child_class) {
		t.Error("isinstance")
	}
	if got := Repr(Model_dump(c)); got != "{'name': 'defaulted', 'count': 7, 'tags': ['a', 'b'], 'ratio': 1.0, 'extra': None}" {
		t.Errorf("model_dump: %s", got)
	}
	if v, err := Getattr(c, "count"); err != nil || v != 7 {
		t.Errorf("getattr: %v %v", v, err)
	}
	m, err := Getattr(c, "describe")
	if err != nil {
		t.Fatal(err)
	}
	if v, err := Call(m, nil, nil); err != nil || v != "defaulted!" {
		t.Errorf("bound method call: %v %v", v, err)
	}
	if _, err := Getattr(c, "nope"); !IsInstance(err, AttributeError) || err.Error() != "'Child' object has no attribute 'nope'" {
		t.Errorf("AttributeError: %v", err)
	}
	// validation errors
	bad := &Child{}
	err = Init(bad, Child_class, Kw{"name": "x", "ratio": 2, "count": "seven"})
	if !IsInstance(err, ValidationError) {
		t.Fatalf("expected ValidationError, got %v", err)
	}
	msg := err.Error()
	if !strings.HasPrefix(msg, "2 validation errors for Child\n") || !strings.Contains(msg, "Input should be a valid integer") || !strings.Contains(msg, "Input should be less than or equal to 1") {
		t.Errorf("validation message:\n%s", msg)
	}
	// model_copy rewires self; copies don't share slices after deep copy
	cp := Model_copy(c, Kw{"update": Kw{"name": "copy"}, "deep": true}).(*Child)
	if cp.Name != "copy" || c.Name != "defaulted" || cp.Self() != cp {
		t.Errorf("model_copy: %+v", cp)
	}
	cp.Tags[0] = "changed"
	if c.Tags[0] != "a" {
		t.Error("deep copy shares slice")
	}
	if Goname("_remote_memorize") != "Remote_memorize_" || Goname("__getitem__") != "Getitem" || Goname("global_id") != "Global_id" || Goname("_LAILA_IDENTIFIABLE_POLICY") != "LAILA_IDENTIFIABLE_POLICY_" {
		t.Error("naming rule")
	}
	if Pyname("Remote_memorize_") != "_remote_memorize" || Pyname("Global_id") != "global_id" || Pyname("LAILA_IDENTIFIABLE_POLICY_") != "_LAILA_IDENTIFIABLE_POLICY" {
		t.Error("inverse naming rule")
	}
}

// ---------------------------------------------------------------------------
// benchmark gates (plan Phase 0): RLock < 100ns, ContextVar.Get < 50ns,
// loop hand-off < 1us
// ---------------------------------------------------------------------------

func BenchmarkRLock(b *testing.B) {
	r := NewRLock()
	for i := 0; i < b.N; i++ {
		r.Acquire(true, -1)
		_ = r.Release()
	}
}

func BenchmarkContextVarGet(b *testing.B) {
	cv := NewContextVar[int]("n", 0)
	cv.Set(1)
	for i := 0; i < b.N; i++ {
		_ = cv.Value()
	}
}

func BenchmarkGoid(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Goid()
	}
}

func BenchmarkLoopHandoff(b *testing.B) {
	l := New_event_loop()
	th := New_Thread(Kw{"target": func() { _ = l.Run_forever() }})
	_ = th.Start()
	for !l.Is_running() {
		time.Sleep(time.Millisecond)
	}
	cf := Run_coroutine_threadsafe(func() (any, error) {
		other := l.Create_task(func() (any, error) {
			for i := 0; i < b.N; i++ {
				if err := Sleep(0); err != nil {
					return nil, err
				}
			}
			return nil, nil
		})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := Sleep(0); err != nil {
				return nil, err
			}
		}
		b.StopTimer()
		return other.Await()
	}, l)
	_, _ = cf.Result(-1)
	l.Stop()
	_ = th.Join(-1)
}

func BenchmarkDictSetGet(b *testing.B) {
	d := NewDict()
	for i := 0; i < b.N; i++ {
		d.Set("key", i)
		_, _ = d.Get("key")
	}
}
