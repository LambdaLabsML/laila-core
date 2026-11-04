// Port of tests/functional/policy/command/schema/future/unit_tests/
// group_future_base_class_tests.py.
package unit_tests

import (
	"errors"
	"math"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// mk_child is ``_mk_child()``.
func mk_child(t *testing.T) *laila.Future {
	t.Helper()
	f, err := laila.New_Future(py.Kw{
		"taskforce_id": "LAILA:TASK_FORCE:child",
		"policy_id":    "LAILA:POLICY:child",
	})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// make_group is ``_make_group(children)``.
func make_group(t *testing.T, children ...*laila.Future) *laila.GroupFuture {
	t.Helper()
	ids := []string{}
	for _, f := range children {
		ids = append(ids, f.Global_id())
	}
	g, err := laila.New_GroupFuture(py.Kw{
		"taskforce_id": "LAILA:TASK_FORCE:test-taskforce",
		"policy_id":    "LAILA:POLICY:test-policy",
		"future_ids":   ids,
	})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func group_status(t *testing.T, g *laila.GroupFuture) *py.Dict {
	t.Helper()
	s, err := g.Status()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func pct(t *testing.T, s *py.Dict, key string) float64 {
	t.Helper()
	p := s.GetDefault("percentages", nil).(*py.Dict)
	v, err := py.Float(p.GetDefault(key, nil))
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func almost_equal(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-7 {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestGroupFutureBase_001_requires_identity_fields(t *testing.T) {
	if _, err := laila.New_GroupFuture(); !errors.Is(err, py.ValidationError) {
		t.Fatalf("expected ValidationError, got %v", err)
	}
}

func TestGroupFutureBase_002_empty_group_status_shape(t *testing.T) {
	g := make_group(t)
	s := group_status(t, g)
	if total, _ := py.Float(s.GetDefault("total", nil)); total != 0.0 {
		t.Fatalf("total %v", total)
	}
	almost_equal(t, pct(t, s, "not_started"), 100.0)
}

func TestGroupFutureBase_003_status_percentages_from_children(t *testing.T) {
	f1, f2, f3 := mk_child(t), mk_child(t), mk_child(t)
	f1.Set_status(laila.FutureStatus_FINISHED)
	f2.Set_status(laila.FutureStatus_RUNNING)
	f3.Set_status(laila.FutureStatus_ERROR)
	g := make_group(t, f1, f2, f3)
	s := group_status(t, g)
	if total, _ := py.Float(s.GetDefault("total", nil)); total != 3.0 {
		t.Fatalf("total %v", total)
	}
	almost_equal(t, pct(t, s, "finished"), 100.0/3.0)
	almost_equal(t, pct(t, s, "running"), 100.0/3.0)
	almost_equal(t, pct(t, s, "error"), 100.0/3.0)
}

func TestGroupFutureBase_004_append_merges_children(t *testing.T) {
	g := make_group(t)
	f1, f2 := mk_child(t), mk_child(t)
	g.Append([]string{f1.Global_id()})
	g.Append([]string{f2.Global_id()})
	if g.Len() != 2 {
		t.Fatalf("len %d", g.Len())
	}
}

func TestGroupFutureBase_005_add_merges_in_place(t *testing.T) {
	f1, f2 := mk_child(t), mk_child(t)
	g1 := make_group(t, f1)
	g2 := make_group(t, f2)
	out, err := g1.Add(g2)
	if err != nil {
		t.Fatal(err)
	}
	if out != g1 {
		t.Fatal("__add__ must return self")
	}
	if g1.Len() != 2 {
		t.Fatalf("len %d", g1.Len())
	}
}

func TestGroupFutureBase_006_add_non_group_returns_not_implemented(t *testing.T) {
	// ``NotImplemented`` with no reflected operand surfaces as TypeError.
	g := make_group(t)
	if _, err := g.Add(123); !errors.Is(err, py.TypeError) {
		t.Fatalf("expected TypeError, got %v", err)
	}
}

func TestGroupFutureBase_007_wait_requires_children_with_wait(t *testing.T) {
	f := mk_child(t)
	g := make_group(t, f)
	if _, err := g.Wait(0.01); !errors.Is(err, py.RuntimeError) {
		t.Fatalf("expected RuntimeError, got %v", err)
	}
}

func TestGroupFutureBase_008_what_includes_summary(t *testing.T) {
	f := mk_child(t)
	f.Set_status(laila.FutureStatus_FINISHED)
	g := make_group(t, f)
	what, err := g.What()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := what.Getitem(g.Global_id())
	if err != nil {
		t.Fatal(err)
	}
	pd := payload.(*py.Dict)
	for _, k := range []string{"status", "summary", "futures"} {
		if !pd.Contains(k) {
			t.Fatalf("missing %q in %v", k, pd)
		}
	}
}

func TestGroupFutureBase_009_repr_and_str_are_json(t *testing.T) {
	g := make_group(t)
	for _, s := range []string{g.String(), g.Repr()} {
		v, err := py.Json_loads(s)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := v.(*py.Dict); !ok {
			t.Fatalf("expected dict, got %T", v)
		}
	}
}

func TestGroupFutureBase_010_len_and_iter(t *testing.T) {
	f1, f2 := mk_child(t), mk_child(t)
	g := make_group(t, f1, f2)
	if len(g.Iter()) != 2 {
		t.Fatalf("iter len %d", len(g.Iter()))
	}
	if g.Len() != 2 {
		t.Fatalf("len %d", g.Len())
	}
}

func TestGroupFutureBase_011_data_returns_all_child_payloads(t *testing.T) {
	f1, f2 := mk_child(t), mk_child(t)
	f1.Set_result("hello")
	f2.Set_result("world")
	f1.Set_status(laila.FutureStatus_FINISHED)
	f2.Set_status(laila.FutureStatus_FINISHED)
	g := make_group(t, f1, f2)
	data, err := g.Data()
	if err != nil {
		t.Fatal(err)
	}
	if !py.Eq(data, py.NewList("hello", "world")) {
		t.Fatalf("data %v", py.Repr(data))
	}
}

func TestGroupFutureBase_012_data_returns_entry_payloads_for_entry_results(t *testing.T) {
	f1, f2 := mk_child(t), mk_child(t)
	e1, err := laila.Entry_constant(py.DictOf("a", 1))
	if err != nil {
		t.Fatal(err)
	}
	e2, err := laila.Entry_constant(py.DictOf("b", 2))
	if err != nil {
		t.Fatal(err)
	}
	f1.Set_result(e1)
	f2.Set_result(e2)
	f1.Set_status(laila.FutureStatus_FINISHED)
	f2.Set_status(laila.FutureStatus_FINISHED)
	g := make_group(t, f1, f2)
	data, err := g.Data()
	if err != nil {
		t.Fatal(err)
	}
	if !py.Eq(data, py.NewList(py.DictOf("a", 1), py.DictOf("b", 2))) {
		t.Fatalf("data %v", py.Repr(data))
	}
}

func TestGroupFutureBase_013_data_raises_when_any_child_result_is_none(t *testing.T) {
	f1, f2 := mk_child(t), mk_child(t)
	f1.Set_result("ok")
	f1.Set_status(laila.FutureStatus_FINISHED)
	f2.Return_value_ = nil
	f2.Set_status(laila.FutureStatus_FINISHED)
	g := make_group(t, f1, f2)
	if _, err := g.Data(); !errors.Is(err, py.RuntimeError) {
		t.Fatalf("expected RuntimeError, got %v", err)
	}
}
