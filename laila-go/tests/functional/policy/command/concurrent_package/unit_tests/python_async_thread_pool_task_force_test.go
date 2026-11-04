// Port of tests/functional/policy/command/concurrent_package/unit_tests/
// test_python_async_thread_pool_task_force.py -- functional tests for
// PythonAsyncThreadPoolTaskForce.
package unit_tests

import (
	"errors"
	"sort"
	"sync"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// active_policy is ``laila.active_policy``.
func active_policy(t *testing.T) any {
	t.Helper()
	p, err := laila.Active_policy()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// command_of is ``policy.central.command``.
func command_of(t *testing.T, policy any) *laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_ {
	t.Helper()
	central, err := py.Getattr(policy, "central")
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := py.Getattr(central, "command")
	if err != nil {
		t.Fatal(err)
	}
	return cmd.(*laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_)
}

func new_tf(t *testing.T, num_workers int, max_async_per_thread int) *laila.PythonAsyncThreadPoolTaskForce {
	t.Helper()
	policy := active_policy(t)
	tf, err := laila.New_PythonAsyncThreadPoolTaskForce(py.Kw{
		"policy_id":            py.Str(py.Must(py.Getattr(policy, "global_id"))),
		"num_workers":          num_workers,
		"max_async_per_thread": max_async_per_thread,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tf.Start(); err != nil {
		t.Fatal(err)
	}
	command_of(t, policy).Taskforces.Set(tf.Global_id(), tf)
	return tf
}

// sync_task is ``lambda: value``.
func sync_task(value any) any {
	return func() (any, error) { return value, nil }
}

// coro_task is ``lambda: aio()`` -- a zero-arg sync callable returning an
// un-awaited coroutine object.
func coro_task(body func() (any, error)) any {
	return func() (any, error) { return py.Coroutine(body), nil }
}

// unwrap_data is the test's ``while hasattr(v, "data"): v = v.data``.
func unwrap_data(t *testing.T, v any) any {
	t.Helper()
	for {
		e, ok := v.(*laila.Entry)
		if !ok {
			return v
		}
		d, err := e.Data()
		if err != nil {
			t.Fatal(err)
		}
		v = d
	}
}

func wait_data(t *testing.T, ident any, timeout float64) any {
	t.Helper()
	res, err := py.Must(py.Getattr(ident, "wait")).(*py.BoundMethod).Call(nil, py.Kw{"timeout": timeout})
	if err != nil {
		t.Fatal(err)
	}
	return unwrap_data(t, res)
}

func Test_001_sync_task_runs(t *testing.T) {
	tf := new_tf(t, 2, 8)
	defer tf.Shutdown()
	ident, err := tf.Submit([]any{sync_task(7)})
	if err != nil {
		t.Fatal(err)
	}
	if got := wait_data(t, ident, 5); !py.Eq(got, 7) {
		t.Fatalf("got %v", got)
	}
}

func Test_002_async_task_runs(t *testing.T) {
	tf := new_tf(t, 2, 8)
	defer tf.Shutdown()
	aiowork := func() (any, error) {
		if err := py.Sleep(0.01); err != nil {
			return nil, err
		}
		return "ok", nil
	}
	ident, err := tf.Submit([]any{coro_task(aiowork)})
	if err != nil {
		t.Fatal(err)
	}
	if got := wait_data(t, ident, 5); !py.Eq(got, "ok") {
		t.Fatalf("got %v", got)
	}
}

func Test_003_mixed_batch_returns_group_future(t *testing.T) {
	tf := new_tf(t, 2, 8)
	defer tf.Shutdown()
	a := func() (any, error) {
		if err := py.Sleep(0.005); err != nil {
			return nil, err
		}
		return 1, nil
	}
	out, err := tf.Submit([]any{coro_task(a), sync_task(2), coro_task(a)})
	if err != nil {
		t.Fatal(err)
	}
	gf, ok := out.(*laila.GroupFuture)
	if !ok {
		t.Fatalf("expected GroupFuture, got %T", out)
	}
	outs, err := gf.Wait(py.Kw{"timeout": 5})
	if err != nil {
		t.Fatal(err)
	}
	data := []int{}
	for _, o := range py.Iter(outs) {
		data = append(data, py.Must(py.Int(unwrap_data(t, o))))
	}
	sort.Ints(data)
	if !py.Eq(data, []int{1, 1, 2}) {
		t.Fatalf("got %v", data)
	}
}

func Test_004_max_async_per_thread_caps_concurrency(t *testing.T) {
	cap_ := 3
	tf := new_tf(t, 1, cap_)
	defer tf.Shutdown()
	var (
		lock          sync.Mutex
		inflight_peak int
		inflight_now  int
	)
	slow := func() (any, error) {
		lock.Lock()
		inflight_now++
		inflight_peak = max(inflight_peak, inflight_now)
		lock.Unlock()
		if err := py.Sleep(0.05); err != nil {
			return nil, err
		}
		lock.Lock()
		inflight_now--
		lock.Unlock()
		return 1, nil
	}
	tasks := make([]any, 20)
	for i := range tasks {
		tasks[i] = coro_task(slow)
	}
	gf, err := tf.Submit(tasks)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gf.(*laila.GroupFuture).Wait(py.Kw{"timeout": 20}); err != nil {
		t.Fatal(err)
	}
	lock.Lock()
	defer lock.Unlock()
	if inflight_peak > cap_ {
		t.Fatalf("inflight peak %d > cap %d", inflight_peak, cap_)
	}
}

func Test_005_async_task_exception_records_error(t *testing.T) {
	tf := new_tf(t, 2, 8)
	defer tf.Shutdown()
	boom := func() (any, error) { return nil, py.ValueError.New("nope") }
	ident, err := tf.Submit([]any{coro_task(boom)})
	if err != nil {
		t.Fatal(err)
	}
	fut := ident.(*laila.ConcurrentPackageFuture)
	if _, err := fut.Wait(py.Kw{"timeout": 5}); !errors.Is(err, py.ValueError) {
		t.Fatalf("expected ValueError, got %v", err)
	}
	if fut.Status() != laila.FutureStatus_ERROR {
		t.Fatalf("status %v", fut.Status())
	}
}

func Test_006_sync_task_exception_records_error(t *testing.T) {
	tf := new_tf(t, 2, 8)
	defer tf.Shutdown()
	boom := func() (any, error) { return nil, py.RuntimeError.New("xx") }
	ident, err := tf.Submit([]any{boom})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ident.(*laila.ConcurrentPackageFuture).Wait(py.Kw{"timeout": 5}); !errors.Is(err, py.RuntimeError) {
		t.Fatalf("expected RuntimeError, got %v", err)
	}
}

func Test_007_two_workers_actually_parallel(t *testing.T) {
	tf := new_tf(t, 2, 1)
	defer tf.Shutdown()
	slow := func() (any, error) {
		if err := py.Sleep(0.2); err != nil {
			return nil, err
		}
		return 1, nil
	}
	t0 := py.Monotonic()
	gf, err := tf.Submit([]any{coro_task(slow), coro_task(slow)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gf.(*laila.GroupFuture).Wait(py.Kw{"timeout": 5}); err != nil {
		t.Fatal(err)
	}
	if elapsed := py.Monotonic() - t0; elapsed >= 0.35 {
		t.Fatalf("elapsed %.3f", elapsed)
	}
}

func Test_008_shutdown_cancels_pending(t *testing.T) {
	tf := new_tf(t, 1, 1)
	hold := func() (any, error) { return nil, py.Sleep(2.0) }
	if _, err := tf.Submit([]any{coro_task(hold)}); err != nil {
		t.Fatal(err)
	}
	queued, err := tf.Submit([]any{sync_task(1)})
	if err != nil {
		t.Fatal(err)
	}
	py.Sleep_sync(0.05)
	if err := tf.Shutdown(py.Kw{"wait": true, "cancel_pending": true}); err != nil {
		t.Fatal(err)
	}
	if st := queued.(*laila.ConcurrentPackageFuture).Status(); st != laila.FutureStatus_CANCELLED {
		t.Fatalf("status %v", st)
	}
}

func Test_009_single_future_bank(t *testing.T) {
	tf := new_tf(t, 2, 8)
	defer tf.Shutdown()
	ident, err := tf.Submit([]any{sync_task(99)})
	if err != nil {
		t.Fatal(err)
	}
	wait_data(t, ident, 5)
	bank := py.Must(py.Getattr(active_policy(t), "future_bank")).(*py.Dict)
	if !bank.Contains(ident.(*laila.ConcurrentPackageFuture).Global_id()) {
		t.Fatal("future not in bank")
	}
}
