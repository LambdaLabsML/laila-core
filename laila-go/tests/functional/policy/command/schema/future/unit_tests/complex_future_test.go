// Port of tests/functional/policy/command/schema/future/unit_tests/
// test_complex_future.py -- functional tests for ComplexFuture (Mode A
// pipeline composition).
package unit_tests

import (
	"errors"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// cf_ids is ``_ids()``: (cmd, compute_id, io_id, policy_gid).
func cf_ids(t *testing.T) (*laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_, string, string, string) {
	t.Helper()
	policy, err := laila.Active_policy()
	if err != nil {
		t.Fatal(err)
	}
	c, err := laila.Command()
	if err != nil {
		t.Fatal(err)
	}
	cmd := c.(*laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_)
	alpha := py.Str(cmd.Alpha_taskforce)
	gid := py.Str(py.Must(py.Getattr(policy, "global_id")))
	return cmd, alpha, alpha, gid
}

// lam is ``lambda: value``.
func lam(value any) any { return func() (any, error) { return value, nil } }

// submit1 is ``cmd.submit(tasks=[task], taskforce_id=tf)``.
func submit1(cmd *laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_, tf string, tasks ...any) (any, error) {
	return cmd.Submit(tasks, py.Kw{"taskforce_id": tf})
}

// data_of is ``prev.data``.
func data_of(prev any) (any, error) {
	return prev.(interface{ Data() (any, error) }).Data()
}

func as_int(t *testing.T, v any) int {
	t.Helper()
	i, err := py.Int(v)
	if err != nil {
		t.Fatalf("not an int: %v (%T)", v, v)
	}
	return i
}

// stage is a ``stage_fn`` closure: ``lambda prev: ...``.
type stage = func(prev any) (any, error)

func new_cf(t *testing.T, io_id, policy_gid string, stage_fns []any) *laila.ComplexFuture {
	t.Helper()
	cf, err := laila.New_ComplexFuture(py.Kw{
		"taskforce_id": io_id,
		"policy_id":    policy_gid,
		"stage_fns":    stage_fns,
	})
	if err != nil {
		t.Fatal(err)
	}
	return cf
}

func cf_wait_data(t *testing.T, cf *laila.ComplexFuture, timeout float64) any {
	t.Helper()
	out, err := cf.Wait(timeout)
	if err != nil {
		t.Fatal(err)
	}
	d, err := data_of(out)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestComplexFuture_001_two_stage_success(t *testing.T) {
	cmd, compute_id, io_id, policy_gid := cf_ids(t)
	s1 := stage(func(_ any) (any, error) { return submit1(cmd, compute_id, lam(7)) })
	s2 := stage(func(prev any) (any, error) {
		v, err := data_of(prev)
		if err != nil {
			return nil, err
		}
		return submit1(cmd, compute_id, lam(as_int(t, v)*6))
	})
	cf := new_cf(t, io_id, policy_gid, []any{s1, s2})
	if d := cf_wait_data(t, cf, 10); as_int(t, d) != 42 {
		t.Fatalf("data %v", d)
	}
	if cf.Status() != laila.FutureStatus_FINISHED {
		t.Fatalf("status %v", cf.Status())
	}
	if len(cf.Stage_future_ids) != 2 {
		t.Fatalf("stage ids %v", cf.Stage_future_ids)
	}
}

func TestComplexFuture_002_single_stage_success(t *testing.T) {
	cmd, compute_id, io_id, policy_gid := cf_ids(t)
	s1 := stage(func(_ any) (any, error) { return submit1(cmd, compute_id, lam("hi")) })
	cf := new_cf(t, io_id, policy_gid, []any{s1})
	if d := cf_wait_data(t, cf, 5); d != "hi" {
		t.Fatalf("data %v", d)
	}
	if cf.Status() != laila.FutureStatus_FINISHED {
		t.Fatalf("status %v", cf.Status())
	}
	if len(cf.Stage_future_ids) != 1 {
		t.Fatalf("stage ids %v", cf.Stage_future_ids)
	}
}

func TestComplexFuture_003_empty_stage_fns_raises(t *testing.T) {
	_, _, io_id, policy_gid := cf_ids(t)
	_, err := laila.New_ComplexFuture(py.Kw{
		"taskforce_id": io_id, "policy_id": policy_gid, "stage_fns": []any{},
	})
	if !errors.Is(err, py.ValueError) {
		t.Fatalf("expected ValueError, got %v", err)
	}
}

func TestComplexFuture_004_non_callable_stage_raises(t *testing.T) {
	_, _, io_id, policy_gid := cf_ids(t)
	_, err := laila.New_ComplexFuture(py.Kw{
		"taskforce_id": io_id, "policy_id": policy_gid,
		"stage_fns": []any{stage(func(_ any) (any, error) { return nil, nil }), "not_callable"},
	})
	if !errors.Is(err, py.TypeError) {
		t.Fatalf("expected TypeError, got %v", err)
	}
}

func TestComplexFuture_005_stage_fn_returning_non_future_propagates_error(t *testing.T) {
	_, _, io_id, policy_gid := cf_ids(t)
	cf := new_cf(t, io_id, policy_gid, []any{stage(func(_ any) (any, error) { return 123, nil })})
	if _, err := cf.Wait(2); err == nil {
		t.Fatal("expected an exception")
	}
	if cf.Status() != laila.FutureStatus_ERROR {
		t.Fatalf("status %v", cf.Status())
	}
}

func TestComplexFuture_006_first_stage_failure_propagates(t *testing.T) {
	cmd, compute_id, io_id, policy_gid := cf_ids(t)
	boom := func() (any, error) { return nil, py.ValueError.New("kaboom") }
	s1 := stage(func(_ any) (any, error) { return submit1(cmd, compute_id, boom) })
	cf := new_cf(t, io_id, policy_gid, []any{s1})
	if _, err := cf.Wait(5); !errors.Is(err, py.ValueError) {
		t.Fatalf("expected ValueError, got %v", err)
	}
	if cf.Status() != laila.FutureStatus_ERROR {
		t.Fatalf("status %v", cf.Status())
	}
}

func TestComplexFuture_007_second_stage_failure_propagates(t *testing.T) {
	cmd, compute_id, io_id, policy_gid := cf_ids(t)
	s1 := stage(func(_ any) (any, error) { return submit1(cmd, compute_id, lam(1)) })
	kaboom := func(_ any) (any, error) { return nil, py.RuntimeError.New("x") }
	s2 := stage(func(prev any) (any, error) {
		return submit1(cmd, compute_id, func() (any, error) { return kaboom(prev) })
	})
	cf := new_cf(t, io_id, policy_gid, []any{s1, s2})
	if _, err := cf.Wait(5); !errors.Is(err, py.RuntimeError) {
		t.Fatalf("expected RuntimeError, got %v", err)
	}
	if cf.Status() != laila.FutureStatus_ERROR {
		t.Fatalf("status %v", cf.Status())
	}
}

func TestComplexFuture_008_async_stage_runs_on_alpha_taskforce(t *testing.T) {
	cmd, compute_id, io_id, policy_gid := cf_ids(t)
	// ``async def aiowork(x): return x + 100``
	aiowork := func(x int) any {
		return py.Coroutine(func() (any, error) { return x + 100, nil })
	}
	s1 := stage(func(_ any) (any, error) { return submit1(cmd, compute_id, lam(1)) })
	s2 := stage(func(prev any) (any, error) {
		v, err := data_of(prev)
		if err != nil {
			return nil, err
		}
		x := as_int(t, v)
		return submit1(cmd, io_id, func() (any, error) { return aiowork(x), nil })
	})
	cf := new_cf(t, io_id, policy_gid, []any{s1, s2})
	if d := cf_wait_data(t, cf, 5); as_int(t, d) != 101 {
		t.Fatalf("data %v", d)
	}
}

func TestComplexFuture_009_nested_complex_future_as_inner_stage(t *testing.T) {
	cmd, compute_id, io_id, policy_gid := cf_ids(t)
	s1 := stage(func(_ any) (any, error) { return submit1(cmd, compute_id, lam(5)) })
	s2 := stage(func(prev any) (any, error) {
		v, err := data_of(prev)
		if err != nil {
			return nil, err
		}
		return submit1(cmd, compute_id, lam(as_int(t, v)*3))
	})
	inner := stage(func(_ any) (any, error) {
		return laila.New_ComplexFuture(py.Kw{
			"taskforce_id": io_id, "policy_id": policy_gid, "stage_fns": []any{s1, s2},
		})
	})
	outer_s2 := stage(func(prev any) (any, error) {
		v, err := data_of(prev)
		if err != nil {
			return nil, err
		}
		return submit1(cmd, compute_id, lam(as_int(t, v)+7))
	})
	outer := new_cf(t, io_id, policy_gid, []any{inner, outer_s2})
	if d := cf_wait_data(t, outer, 10); as_int(t, d) != 22 {
		t.Fatalf("data %v", d)
	}
}

func TestComplexFuture_010_branching_in_stage_fn(t *testing.T) {
	cmd, compute_id, io_id, policy_gid := cf_ids(t)
	s1 := stage(func(_ any) (any, error) { return submit1(cmd, compute_id, lam(4)) })
	branch := stage(func(prev any) (any, error) {
		d, err := data_of(prev)
		if err != nil {
			return nil, err
		}
		v := as_int(t, d)
		if v%2 == 0 {
			return submit1(cmd, compute_id, lam(v*10))
		}
		return submit1(cmd, compute_id, lam(v+1))
	})
	cf := new_cf(t, io_id, policy_gid, []any{s1, branch})
	if d := cf_wait_data(t, cf, 5); as_int(t, d) != 40 {
		t.Fatalf("data %v", d)
	}
}

func TestComplexFuture_011_registered_in_future_bank_once_per_layer(t *testing.T) {
	cmd, compute_id, io_id, policy_gid := cf_ids(t)
	policy, err := laila.Active_policy()
	if err != nil {
		t.Fatal(err)
	}
	bank := policy.(interface{ Get_future_bank_() *py.Dict }).Get_future_bank_()
	before := bank.Len()
	s1 := stage(func(_ any) (any, error) { return submit1(cmd, compute_id, lam(1)) })
	cf := new_cf(t, io_id, policy_gid, []any{s1})
	if _, err := cf.Wait(5); err != nil {
		t.Fatal(err)
	}
	if !bank.Contains(cf.Global_id()) {
		t.Fatal("complex future not in bank")
	}
	if bank.Len() <= before {
		t.Fatalf("bank did not grow: %d <= %d", bank.Len(), before)
	}
}

func TestComplexFuture_012_future_identity_roundtrips_through_bank(t *testing.T) {
	cmd, compute_id, io_id, policy_gid := cf_ids(t)
	s1 := stage(func(_ any) (any, error) { return submit1(cmd, compute_id, lam("abc")) })
	cf := new_cf(t, io_id, policy_gid, []any{s1})
	ident, err := cf.Future_identity()
	if err != nil {
		t.Fatal(err)
	}
	result, err := ident.Wait(5)
	if err != nil {
		t.Fatal(err)
	}
	d, err := data_of(result)
	if err != nil {
		t.Fatal(err)
	}
	if d != "abc" {
		t.Fatalf("data %v", d)
	}
}

func TestComplexFuture_013_group_future_as_stage_aggregates(t *testing.T) {
	cmd, compute_id, io_id, policy_gid := cf_ids(t)
	fanout := stage(func(_ any) (any, error) {
		tasks := []any{}
		for _, v := range []int{1, 2, 3} {
			tasks = append(tasks, lam(v*2))
		}
		return cmd.Submit(tasks, py.Kw{"taskforce_id": compute_id})
	})
	cf := new_cf(t, io_id, policy_gid, []any{fanout})
	if _, err := cf.Wait(5); err != nil {
		t.Fatal(err)
	}
	if cf.Status() != laila.FutureStatus_FINISHED {
		t.Fatalf("status %v", cf.Status())
	}
}
