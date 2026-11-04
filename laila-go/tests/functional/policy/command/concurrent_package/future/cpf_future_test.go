// Port of tests/functional/policy/command/concurrent_package/future/
// test_cpf_future.py.
package future

import (
	"errors"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

func make_cpf(t *testing.T) *laila.ConcurrentPackageFuture {
	t.Helper()
	fut, err := laila.New_ConcurrentPackageFuture(py.Kw{
		"taskforce_id": "LAILA:TASK_FORCE:test-taskforce",
		"policy_id":    "LAILA:POLICY:test-policy",
	})
	if err != nil {
		t.Fatal(err)
	}
	return fut
}

func set_native(t *testing.T, fut *laila.ConcurrentPackageFuture, n *py.CFuture) {
	t.Helper()
	if err := fut.Set_native_future(n); err != nil {
		t.Fatal(err)
	}
}

func TestCPF_001_construct_without_native_future_allowed(t *testing.T) {
	if make_cpf(t).Native_future() != nil {
		t.Fatal("expected no native future")
	}
}

func TestCPF_002_native_future_setter_once_only(t *testing.T) {
	fut := make_cpf(t)
	set_native(t, fut, py.NewCFuture())
	if err := fut.Set_native_future(py.NewCFuture()); !errors.Is(err, py.RuntimeError) {
		t.Fatalf("expected RuntimeError, got %v", err)
	}
}

func TestCPF_003_wait_without_native_times_out(t *testing.T) {
	fut := make_cpf(t)
	if _, err := fut.Wait(py.Kw{"timeout": 0.02}); !errors.Is(err, py.TimeoutError) {
		t.Fatalf("expected TimeoutError, got %v", err)
	}
	if fut.Status() != laila.FutureStatus_POLL_TIMEOUT {
		t.Fatalf("status %v", fut.Status())
	}
}

func TestCPF_004_wait_without_native_finished_returns_value(t *testing.T) {
	fut := make_cpf(t)
	fut.Set_status(laila.FutureStatus_FINISHED)
	fut.Set_result(py.DictFromKw(py.Kw{"ok": 1}))
	res, err := fut.Wait(py.Kw{"timeout": 0.02})
	if err != nil {
		t.Fatal(err)
	}
	data, err := res.(*laila.Entry).Data()
	if err != nil {
		t.Fatal(err)
	}
	if !py.Eq(data, py.DictFromKw(py.Kw{"ok": 1})) {
		t.Fatalf("data %v", data)
	}
}

func TestCPF_005_wait_without_native_error_raises_exception(t *testing.T) {
	fut := make_cpf(t)
	fut.Set_exception(py.ValueError.New("boom"))
	fut.Set_status(laila.FutureStatus_ERROR)
	if _, err := fut.Wait(py.Kw{"timeout": 0.02}); !errors.Is(err, py.ValueError) {
		t.Fatalf("expected ValueError, got %v", err)
	}
}

func TestCPF_006_wait_with_native_success_returns_value(t *testing.T) {
	fut := make_cpf(t)
	n := py.NewCFuture()
	set_native(t, fut, n)
	_ = n.Set_result(7)
	if v, err := fut.Wait(py.Kw{"timeout": 0.5}); err != nil || !py.Eq(v, 7) {
		t.Fatalf("got %v %v", v, err)
	}
}

func TestCPF_007_wait_with_native_pending_timeout_raises(t *testing.T) {
	fut := make_cpf(t)
	set_native(t, fut, py.NewCFuture())
	if _, err := fut.Wait(py.Kw{"timeout": 0.01}); !errors.Is(err, py.TimeoutError) {
		t.Fatalf("expected TimeoutError, got %v", err)
	}
}

func TestCPF_008_done_callback_sets_finished_and_result(t *testing.T) {
	fut := make_cpf(t)
	n := py.NewCFuture()
	set_native(t, fut, n)
	_ = n.Set_result("ok")
	py.Sleep_sync(0.01)
	if fut.Status() != laila.FutureStatus_FINISHED {
		t.Fatalf("status %v", fut.Status())
	}
	if d, err := fut.Data(); err != nil || !py.Eq(d, "ok") {
		t.Fatalf("data %v %v", d, err)
	}
}

func TestCPF_009_done_callback_sets_cancelled(t *testing.T) {
	fut := make_cpf(t)
	n := py.NewCFuture()
	set_native(t, fut, n)
	n.Cancel()
	py.Sleep_sync(0.01)
	if fut.Status() != laila.FutureStatus_CANCELLED {
		t.Fatalf("status %v", fut.Status())
	}
}

func TestCPF_010_done_callback_sets_error_and_exception(t *testing.T) {
	fut := make_cpf(t)
	n := py.NewCFuture()
	set_native(t, fut, n)
	err := py.RuntimeError.New("fail")
	_ = n.Set_exception(err)
	py.Sleep_sync(0.01)
	if fut.Status() != laila.FutureStatus_ERROR {
		t.Fatalf("status %v", fut.Status())
	}
	if fut.Exception() != error(err) {
		t.Fatalf("exception %v", fut.Exception())
	}
}

func TestCPF_011_result_blocks_until_native_completes(t *testing.T) {
	fut := make_cpf(t)
	n := py.NewCFuture()
	set_native(t, fut, n)
	th := py.New_Thread(py.Kw{"target": func() {
		py.Sleep_sync(0.02)
		_ = n.Set_result("later")
	}})
	_ = th.Start()
	if d, err := fut.Data(); err != nil || !py.Eq(d, "later") {
		t.Fatalf("data %v %v", d, err)
	}
	_ = th.Join(1.0)
}

func TestCPF_012_wait_not_started_then_native_assigned(t *testing.T) {
	fut := make_cpf(t)
	th := py.New_Thread(py.Kw{"target": func() {
		py.Sleep_sync(0.02)
		n := py.NewCFuture()
		_ = fut.Set_native_future(n)
		py.Sleep_sync(0.02)
		_ = n.Set_result("done")
	}})
	_ = th.Start()
	if v, err := fut.Wait(py.Kw{"timeout": 0.5}); err != nil || !py.Eq(v, "done") {
		t.Fatalf("got %v %v", v, err)
	}
	_ = th.Join(1.0)
}

func TestCPF_013_predicates_initial_state(t *testing.T) {
	fut := make_cpf(t)
	if !fut.Not_started() || fut.Running() || fut.Finished() || fut.Error() || fut.Cancelled() {
		t.Fatal("predicates wrong")
	}
}

func TestCPF_014_predicate_finished_after_success(t *testing.T) {
	fut := make_cpf(t)
	n := py.NewCFuture()
	set_native(t, fut, n)
	_ = n.Set_result(1)
	py.Sleep_sync(0.01)
	if !fut.Finished() {
		t.Fatal("not finished")
	}
}

func TestCPF_015_predicate_cancelled_after_cancel(t *testing.T) {
	fut := make_cpf(t)
	n := py.NewCFuture()
	set_native(t, fut, n)
	n.Cancel()
	py.Sleep_sync(0.01)
	if !fut.Cancelled() {
		t.Fatal("not cancelled")
	}
}

func TestCPF_016_predicate_error_after_exception(t *testing.T) {
	fut := make_cpf(t)
	n := py.NewCFuture()
	set_native(t, fut, n)
	_ = n.Set_exception(py.ValueError.New("bad"))
	py.Sleep_sync(0.01)
	if !fut.Error() {
		t.Fatal("not error")
	}
}
