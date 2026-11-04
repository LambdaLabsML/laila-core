package py

import (
	"errors"
	"os"
	"testing"
)

// Workers are this test binary re-executed with LAILA_PROCESS_WORKER=1
// (the spawn start method); TestMain is the ``__main__`` guard.
func TestMain(m *testing.M) {
	Process_worker_main()
	os.Exit(m.Run())
}

func ppAdd(args ...any) (any, error) {
	a, _ := Int(args[0])
	b, _ := Int(args[1])
	return a + b, nil
}

func ppPid(args ...any) (any, error) { return Getpid(), nil }

func ppBoom(args ...any) (any, error) { return nil, ValueError.New("boom") }

func ppExit(args ...any) (any, error) {
	code, _ := Int(args[0])
	Exit_(code)
	return nil, nil
}

var (
	ppAddTask  = Register_process_task("py_test.ppAdd", ppAdd)
	ppPidTask  = Register_process_task("py_test.ppPid", ppPid)
	ppBoomTask = Register_process_task("py_test.ppBoom", ppBoom)
	ppExitTask = Register_process_task("py_test.ppExit", ppExit)
)

func TestProcessPool_runs_in_another_process(t *testing.T) {
	e := New_ProcessPoolExecutor(2)
	defer e.Shutdown(true)
	f, err := e.Submit(ppPidTask, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	v, err := f.Result(30)
	if err != nil {
		t.Fatal(err)
	}
	if pid, _ := Int(v); pid == Getpid() || pid == 0 {
		t.Fatalf("expected a worker pid, got %v", v)
	}
	// Registered Go func value and partial are accepted too.
	g, err := e.Submit(Partial(ppAdd, 2), []any{3}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := g.Result(30); !Eq(v, 5) {
		t.Fatalf("got %v", v)
	}
	_ = ppAddTask
}

func TestProcessPool_rejects_closures_and_unserialisable_args(t *testing.T) {
	e := New_ProcessPoolExecutor(1)
	defer e.Shutdown(true)
	if _, err := e.Submit(func(args ...any) (any, error) { return 1, nil }, nil, nil); !errors.Is(err, TypeError) {
		t.Fatalf("expected TypeError, got %v", err)
	}
	if _, err := e.Submit(ppAddTask, []any{NewSet(1)}, nil); !errors.Is(err, TypeError) {
		t.Fatalf("expected TypeError, got %v", err)
	}
}

func TestProcessPool_propagates_exception_class(t *testing.T) {
	e := New_ProcessPoolExecutor(1)
	defer e.Shutdown(true)
	f, _ := e.Submit(ppBoomTask, nil, nil)
	_, err := f.Result(30)
	if !errors.Is(err, ValueError) || err.Error() != "boom" {
		t.Fatalf("got %v", err)
	}
}

func TestProcessPool_worker_death_breaks_pool(t *testing.T) {
	e := New_ProcessPoolExecutor(1)
	defer e.Shutdown(false, Kw{"cancel_futures": true})
	f, _ := e.Submit(ppExitTask, []any{3}, nil)
	_, err := f.Result(30)
	if !errors.Is(err, BrokenProcessPool) {
		t.Fatalf("expected BrokenProcessPool, got %v", err)
	}
	if _, err := e.Submit(ppPidTask, nil, nil); !errors.Is(err, BrokenProcessPool) {
		t.Fatalf("submit after break: %v", err)
	}
	if !e.Join_workers(10) {
		t.Fatal("workers did not exit")
	}
}

func TestProcessPool_shutdown_wait_runs_pending(t *testing.T) {
	e := New_ProcessPoolExecutor(1)
	var futs []*CFuture
	for i := 0; i < 5; i++ {
		f, err := e.Submit(ppAddTask, []any{i, 1}, nil)
		if err != nil {
			t.Fatal(err)
		}
		futs = append(futs, f)
	}
	e.Shutdown(true)
	for i, f := range futs {
		if v, err := f.Result(0); err != nil || !Eq(v, i+1) {
			t.Fatalf("fut %d: %v %v", i, v, err)
		}
	}
	if _, err := e.Submit(ppAddTask, []any{1, 1}, nil); !errors.Is(err, RuntimeError) {
		t.Fatalf("expected RuntimeError after shutdown, got %v", err)
	}
}
