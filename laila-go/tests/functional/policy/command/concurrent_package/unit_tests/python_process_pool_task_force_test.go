// Port of tests/functional/policy/command/concurrent_package/unit_tests/
// test_python_process_pool_task_force.py.
package unit_tests

import (
	"errors"
	"os"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// TestMain is the spawn-context ``__main__`` guard: worker processes are
// this test binary re-executed with LAILA_PROCESS_WORKER=1 and must serve
// tasks instead of running the suite.
func TestMain(m *testing.M) {
	laila.Process_worker_main()
	os.Exit(m.Run())
}

// Module-level functions -- the picklable tasks; registered under their
// Python qualified names so both the parent and the worker agree.
func return_1_(args ...any) (any, error)  { return 1, nil }
func return_2_(args ...any) (any, error)  { return 2, nil }
func return_3_(args ...any) (any, error)  { return 3, nil }
func return_7_(args ...any) (any, error)  { return 7, nil }
func return_a_(args ...any) (any, error)  { return "a", nil }
func return_b_(args ...any) (any, error)  { return "b", nil }
func return_10_(args ...any) (any, error) { return 10, nil }
func return_20_(args ...any) (any, error) { return 20, nil }

var (
	return_1  = laila.Register_process_task("test_python_process_pool_task_force._return_1", return_1_)
	return_2  = laila.Register_process_task("test_python_process_pool_task_force._return_2", return_2_)
	return_3  = laila.Register_process_task("test_python_process_pool_task_force._return_3", return_3_)
	return_7  = laila.Register_process_task("test_python_process_pool_task_force._return_7", return_7_)
	return_a  = laila.Register_process_task("test_python_process_pool_task_force._return_a", return_a_)
	return_b  = laila.Register_process_task("test_python_process_pool_task_force._return_b", return_b_)
	return_10 = laila.Register_process_task("test_python_process_pool_task_force._return_10", return_10_)
	return_20 = laila.Register_process_task("test_python_process_pool_task_force._return_20", return_20_)
)

func make_force(t *testing.T, num_workers int) *laila.PythonProcessPoolTaskForce {
	t.Helper()
	tf, err := laila.New_PythonProcessPoolTaskForce(py.Kw{
		"policy_id":   "LAILA:POLICY:test-policy",
		"num_workers": num_workers,
	})
	if err != nil {
		t.Fatal(err)
	}
	return tf
}

func future_bank(t *testing.T) *py.Dict {
	t.Helper()
	policy, err := laila.Get_active_policy()
	if err != nil {
		t.Fatal(err)
	}
	return py.Must(py.Getattr(policy, "future_bank")).(*py.Dict)
}

func TestProcessPool_001_start_and_shutdown(t *testing.T) {
	tf := make_force(t, 4)
	if err := tf.Start(); err != nil {
		t.Fatal(err)
	}
	if tf.Status.Value() != "running" {
		t.Fatalf("status %v", tf.Status)
	}
	if err := tf.Shutdown(py.Kw{"wait": true}); err != nil {
		t.Fatal(err)
	}
	if tf.Status.Value() != "stopped" {
		t.Fatalf("status %v", tf.Status)
	}
}

func TestProcessPool_002_submit_single_task_returns_future_identity(t *testing.T) {
	tf := make_force(t, 4)
	if err := tf.Start(); err != nil {
		t.Fatal(err)
	}
	defer tf.Shutdown(py.Kw{"wait": true})
	identity, err := tf.Submit([]any{return_7}, py.Kw{"wait": false})
	if err != nil {
		t.Fatal(err)
	}
	if !py.Isinstance(identity, laila.LAILA_IDENTIFIABLE_FUTURE_class) {
		t.Fatalf("expected a future identity, got %T", identity)
	}
	gid := identity.(*laila.ProcessPackageFuture).Global_id()
	fut, err := future_bank(t).Getitem(gid)
	if err != nil {
		t.Fatal(err)
	}
	ppf, ok := fut.(*laila.ProcessPackageFuture)
	if !ok {
		t.Fatalf("expected ProcessPackageFuture, got %T", fut)
	}
	if v, err := ppf.Wait(); err != nil || !py.Eq(v, 7) {
		t.Fatalf("wait: %v %v", v, err)
	}
	st, err := laila.Status(identity)
	if err != nil || st != laila.FutureStatus_FINISHED {
		t.Fatalf("status %v %v", st, err)
	}
}

func TestProcessPool_003_submit_multiple_tasks_returns_group_future(t *testing.T) {
	tf := make_force(t, 4)
	if err := tf.Start(); err != nil {
		t.Fatal(err)
	}
	defer tf.Shutdown(py.Kw{"wait": true})
	out, err := tf.Submit([]any{return_1, return_2, return_3}, py.Kw{"wait": false})
	if err != nil {
		t.Fatal(err)
	}
	gf, ok := out.(*laila.GroupFuture)
	if !ok {
		t.Fatalf("expected GroupFuture, got %T", out)
	}
	values, err := gf.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if !py.Eq(values, []any{1, 2, 3}) {
		t.Fatalf("values %v", values)
	}
	st, err := laila.Status(out)
	if err != nil {
		t.Fatal(err)
	}
	pct := st.(*py.Dict).GetDefault("percentages", nil).(*py.Dict)
	if !py.Eq(pct.GetDefault("finished", nil), 100.0) {
		t.Fatalf("percentages %v", pct)
	}
}

func TestProcessPool_004_submit_wait_true_returns_values(t *testing.T) {
	tf := make_force(t, 4)
	if err := tf.Start(); err != nil {
		t.Fatal(err)
	}
	defer tf.Shutdown(py.Kw{"wait": true})
	values, err := tf.Submit([]any{return_a, return_b}, py.Kw{"wait": true})
	if err != nil {
		t.Fatal(err)
	}
	if !py.Eq(values, []any{"a", "b"}) {
		t.Fatalf("values %v", values)
	}
}

func TestProcessPool_005_imap_yields_identities_in_order(t *testing.T) {
	tf := make_force(t, 4)
	if err := tf.Start(); err != nil {
		t.Fatal(err)
	}
	defer tf.Shutdown(py.Kw{"wait": true})
	it, err := tf.Imap([]any{return_10, return_20})
	if err != nil {
		t.Fatal(err)
	}
	identities, err := py.Collect(it)
	if err != nil {
		t.Fatal(err)
	}
	if len(identities) != 2 {
		t.Fatalf("len %d", len(identities))
	}
	bank := future_bank(t)
	got := []any{}
	for _, i := range identities {
		fut, err := bank.Getitem(i.(*laila.ProcessPackageFuture).Global_id())
		if err != nil {
			t.Fatal(err)
		}
		v, err := fut.(*laila.ProcessPackageFuture).Wait()
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	if !py.Eq(got, []any{10, 20}) {
		t.Fatalf("got %v", got)
	}
}

func TestProcessPool_006_queue_submit_allows_submission_on_auto_started_force(t *testing.T) {
	tf := make_force(t, 4)
	defer tf.Shutdown(py.Kw{"wait": true})
	fut, err := tf.Queue_submit_(return_1)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := fut.Wait(); err != nil || !py.Eq(v, 1) {
		t.Fatalf("wait: %v %v", v, err)
	}
}

func TestProcessPool_007_submit_after_shutdown_raises(t *testing.T) {
	tf := make_force(t, 4)
	if err := tf.Start(); err != nil {
		t.Fatal(err)
	}
	if err := tf.Shutdown(py.Kw{"wait": true}); err != nil {
		t.Fatal(err)
	}
	if _, err := tf.Submit([]any{return_1}, py.Kw{"wait": false}); !errors.Is(err, py.RuntimeError) {
		t.Fatalf("expected RuntimeError, got %v", err)
	}
}

func TestProcessPool_008_lambda_task_raises_type_error(t *testing.T) {
	tf := make_force(t, 4)
	defer tf.Shutdown(py.Kw{"wait": true})
	if _, err := tf.Submit([]any{sync_task(1)}, py.Kw{"wait": false}); !errors.Is(err, py.TypeError) {
		t.Fatalf("expected TypeError, got %v", err)
	}
}
