//go:build workload

// Port of tests/workloads/test_w13_future_composition.py.
//
// W13 -- future composition at scale.
//
// GroupFuture aggregation (thousands of children, nested groups, partial
// release), ComplexFuture pipelines (long stage chains, group stages,
// failures mid-pipeline), future identity resolution through the runtime
// helpers, and future-bank hygiene across policies.
package workloads

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

const T13_ = 120.0

// tf13 is the ``tf`` fixture (2x8, 4 sync workers) on a fresh policy.
func tf13(t *testing.T) (*laila.LAILA_IDENTIFIABLE_POLICY_, *laila.PythonAsyncThreadPoolTaskForce) {
	t.Helper()
	policy := Fresh_policy(t)
	return policy, tf_fixture(t, policy)
}

// ident_ is ``_ident(f)``.
func ident_(f *laila.ConcurrentPackageFuture) *laila.LAILA_IDENTIFIABLE_FUTURE_ {
	return must(laila.New_LAILA_IDENTIFIABLE_FUTURE_(py.Kw{
		"uuid": f.Uuid_, "taskforce_id": f.Taskforce_id, "policy_id": f.Policy_id,
	}))
}

// lams is ``[lambda i=i: i for i in range(n)]``.
func lams(n int) []any {
	out := make([]any, n)
	for i := range out {
		out[i] = lam(i)
	}
	return out
}

// repeat_lam is ``[lambda: v for _ in range(n)]``.
func repeat_lam(v any, n int) []any {
	out := make([]any, n)
	for i := range out {
		out[i] = lam(v)
	}
	return out
}

// new_cf is ``ComplexFuture(taskforce_id=..., policy_id=..., stage_fns=...)``.
func new_cf(policy *laila.LAILA_IDENTIFIABLE_POLICY_, tf *laila.PythonAsyncThreadPoolTaskForce, stages []any) (*laila.ComplexFuture, error) {
	return laila.New_ComplexFuture(py.Kw{
		"taskforce_id": tf.Global_id(),
		"policy_id":    policy.Global_id(),
		"stage_fns":    stages,
	})
}

// stage is a ``def stage(prev)`` body.
func stage(fn func(prev any) (any, error)) any { return laila.StageFn(fn) }

// ---------------------------------------------------------------------------
// GroupFuture
// ---------------------------------------------------------------------------

func TestGroupFuture13_group_of_ten_thousand(t *testing.T) {
	policy, tf := tf13(t)
	n := N_scaled(10_000)
	gf := gfut(must(submit_(tf, lams(n))))
	assertEq(t, gf.Len(), n)
	vals := unwrap_all(must(gf.Wait(T13_)))
	assertEq(t, vals, range_list(n))
	assertEq(t, unwrap_all(must(gf.Result())), vals)
	assertEq(t, must(gf.Data()), vals)
	gf.Release()
	assertEq(t, policy.Future_bank.Len(), 0)
}

func TestGroupFuture13_status_snapshot_cost_scales_linearly(t *testing.T) {
	_, tf := tf13(t)
	gate := py.NewEvent()
	waiter := func() (any, error) { return gate.Wait(60.0), nil }
	small_tasks := make([]any, 100)
	for i := range small_tasks {
		small_tasks[i] = waiter
	}
	big_tasks := make([]any, 2000)
	for i := range big_tasks {
		big_tasks[i] = waiter
	}
	small := gfut(must(submit_(tf, small_tasks)))
	big := gfut(must(submit_(tf, big_tasks)))
	s1 := Timed(func() {
		for i := 0; i < 20; i++ {
			must(small.Status())
		}
	})
	s2 := Timed(func() {
		for i := 0; i < 20; i++ {
			must(big.Status())
		}
	})
	gate.Set()
	must(small.Wait(T13_))
	must(big.Wait(T13_))
	small.Release()
	big.Release()
	assertTrue(t, s2.Elapsed < s1.Elapsed*20*3+0.5, s1.Elapsed, s2.Elapsed)
}

func TestGroupFuture13_what_summary_on_mixed_group(t *testing.T) {
	_, tf := tf13(t)
	bad := func() (any, error) { return nil, py.ValueError.New("w") }
	gf := gfut(must(submit_(tf, []any{lam(1), bad, lam(2)})))
	if _, err := gf.Wait(T13_); !errors.Is(err, py.ValueError) {
		t.Fatalf("want ValueError, got %v", err)
	}
	w := must(gf.What())
	assertTrue(t, strings.Contains(py.Str(w), gf.Global_id()))
	exc := gf.Exception()
	assertTrue(t, exc != nil && errors.Is(exc, py.ValueError), exc)
	gf.Release()
}

func TestGroupFuture13_group_iteration_and_membership(t *testing.T) {
	policy, tf := tf13(t)
	gf := gfut(must(submit_(tf, repeat_lam(1, 50))))
	ids := gf.Iter()
	assertEq(t, len(ids), 50)
	for _, i := range ids {
		assertTrue(t, policy.Future_bank.Contains(i))
	}
	must(gf.Wait(T13_))
	gf.Release()
	for _, i := range ids {
		assertTrue(t, !policy.Future_bank.Contains(i))
	}
}

func TestGroupFuture13_nested_group_addition_chain(t *testing.T) {
	policy, tf := tf13(t)
	groups := make([]*laila.GroupFuture, 20)
	for k := range groups {
		groups[k] = gfut(must(submit_(tf, repeat_lam(k, 10))))
	}
	total := groups[0]
	for _, g := range groups[1:] {
		total = must(total.Add(g))
	}
	assertEq(t, total.Len(), 200)
	vals := unwrap_all(must(total.Wait(T13_))).Items()
	want := []any{}
	for k := 0; k < 20; k++ {
		for i := 0; i < 10; i++ {
			want = append(want, k)
		}
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i].(int) < vals[j].(int) })
	assertEq(t, vals, want)
	total.Release()
	for _, g := range groups {
		g.Release_with(py.Kw{"children": false})
	}
	assertEq(t, policy.Future_bank.Len(), 0)
}

func TestGroupFuture13_append_ids_then_wait(t *testing.T) {
	policy, tf := tf13(t)
	gf := gfut(must(submit_(tf, []any{lam(1), lam(2)})))
	extra := []string{}
	for i := 10; i < 20; i++ {
		extra = append(extra, cpf(must(submit_(tf, []any{lam(i)}))).Global_id())
	}
	gf.Append(extra)
	assertEq(t, gf.Len(), 12)
	vals := unwrap_all(must(gf.Wait(T13_)))
	want := py.NewList(1, 2)
	for i := 10; i < 20; i++ {
		want.Append(i)
	}
	assertEq(t, vals, want)
	gf.Release()
	assertEq(t, policy.Future_bank.Len(), 0)
}

func TestGroupFuture13_partial_child_release_then_wait_raises(t *testing.T) {
	policy, tf := tf13(t)
	gf := gfut(must(submit_(tf, repeat_lam(1, 5))))
	must(gf.Wait(T13_))
	child := must(policy.Future_bank.Getitem(gf.Future_ids[2]))
	child.(interface{ Release() }).Release()
	if _, err := gf.Wait(T13_); !errors.Is(err, py.KeyError) {
		t.Fatalf("want KeyError, got %v", err)
	}
	// introspection must stay safe
	total := must(must(gf.Status()).Getitem("total")).(float64)
	assertTrue(t, total == 4 || total == 5, total)
	gf.Release()
}

func TestGroupFuture13_group_await_concurrently_from_many_coroutines(t *testing.T) {
	_, tf := tf13(t)
	tasks := make([]any, 100)
	for i := range tasks {
		tasks[i] = sleep_then(0.01, i)
	}
	gf := gfut(must(submit_(tf, tasks)))

	await_group := func(g *laila.GroupFuture) func() (any, error) {
		return func() (any, error) {
			vals, err := py.Await(g)
			if err != nil {
				return nil, err
			}
			return unwrap_all(vals), nil
		}
	}
	main := func() (any, error) {
		fns := make([]func() (any, error), 10)
		for i := range fns {
			fns[i] = await_group(gf)
		}
		return py.Gather(fns)
	}
	outs := must(Run(main)).([]any)
	for _, o := range outs {
		assertEq(t, o, range_list(100))
	}
	gf.Release()
}

func TestGroupFuture13_group_wait_from_many_threads(t *testing.T) {
	_, tf := tf13(t)
	tasks := make([]any, 200)
	for i := range tasks {
		tasks[i] = sleep_then(0.01, i)
	}
	gf := gfut(must(submit_(tf, tasks)))

	w := func(_ int) (any, error) {
		vals, err := gf.Wait(T13_)
		if err != nil {
			return nil, err
		}
		return unwrap_all(vals), nil
	}
	outs := must(Run_threads(w, 8))
	for _, o := range outs {
		assertEq(t, o, range_list(200))
	}
	gf.Release()
}

func TestGroupFuture13_group_from_another_policy_resolves(t *testing.T) {
	policy, tf := tf13(t)
	gf := gfut(must(submit_(tf, repeat_lam(1, 5))))
	must(gf.Wait(T13_))
	other := must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	laila.Activate_policy(other)
	func() {
		defer func() {
			laila.Activate_policy(policy)
			_ = Cmd(other).Shutdown(py.Kw{"wait": true, "cancel_pending": true})
			laila.Local_policies_.PopDefault(other.Global_id(), nil)
			gf.Release()
		}()
		vals := must(gf.Wait(T13_))
		assertEq(t, must(py.Len(vals)), 5)
	}()
}

func TestGroupFuture13_submit_wait_true_leaks_futures(t *testing.T) {
	policy, tf := tf13(t)
	before := policy.Future_bank.Len()
	for i := 0; i < 50; i++ {
		must(submit_(tf, []any{lam(1), lam(2)}, py.Kw{"wait": true}))
		must(submit_(tf, []any{lam(1)}, py.Kw{"wait": true}))
	}
	assertEq(t, policy.Future_bank.Len(), before)
}

// ---------------------------------------------------------------------------
// ComplexFuture
// ---------------------------------------------------------------------------

// linear_stage is the ``mk(i)`` / ``mk()`` stage: ``base + 1``.
func linear_stage(tf *laila.PythonAsyncThreadPoolTaskForce) any {
	return stage(func(prev any) (any, error) {
		base := 0
		if prev != nil {
			base = Unwrap(prev).(int)
		}
		return submit_(tf, []any{lam(base + 1)})
	})
}

func TestComplexFuture13_long_linear_pipeline(t *testing.T) {
	for _, stages := range []int{5, 50, 300} {
		stages := stages
		t.Run(fmt.Sprintf("stages=%d", stages), func(t *testing.T) {
			policy, tf := tf13(t)
			fns := make([]any, stages)
			for i := range fns {
				fns[i] = linear_stage(tf)
			}
			cf := must(new_cf(policy, tf, fns))
			assertEq(t, Unwrap(must(cf.Wait(T13_))), stages)
			assertEq(t, cf.Num_stages(), stages)
			assertEq(t, cf.Current_stage_index(), stages-1)
			assertEq(t, len(cf.Stage_future_ids), stages)
			cf.Release()
			assertEq(t, policy.Future_bank.Len(), 0)
		})
	}
}

func TestComplexFuture13_pipeline_with_group_stages(t *testing.T) {
	policy, tf := tf13(t)
	var mu sync.Mutex
	kinds := []string{}

	fan_out := stage(func(prev any) (any, error) {
		mu.Lock()
		kinds = append(kinds, py.Pytypename(prev))
		mu.Unlock()
		// a group stage hands the *list* of child results to the next stage
		var vals any = prev
		if d, ok := prev.(interface{ Data() (any, error) }); ok {
			vals, _ = d.Data()
		}
		n := 2
		if prev != nil {
			n = must(py.Len(vals)) + 1
		}
		return submit_(tf, lams(n))
	})
	fns := make([]any, 20)
	for i := range fns {
		fns[i] = fan_out
	}
	cf := must(new_cf(policy, tf, fns))
	out := must(cf.Wait(T13_))
	assertEq(t, must(py.Len(Unwrap(out))), 21)
	mu.Lock()
	defer mu.Unlock()
	assertEq(t, kinds[0], "NoneType")
	rest := py.NewSet()
	for _, k := range kinds[1:] {
		rest.Add(k)
	}
	assertEq(t, rest, py.NewSet("list"))
	cf.Release()
}

func TestComplexFuture13_pipeline_failure_mid_way(t *testing.T) {
	policy, tf := tf13(t)
	var mu sync.Mutex
	seen := []int{}

	ok := stage(func(prev any) (any, error) {
		mu.Lock()
		seen = append(seen, 1)
		mu.Unlock()
		return submit_(tf, []any{lam(1)})
	})
	bad := stage(func(prev any) (any, error) {
		return submit_(tf, []any{func() (any, error) { return nil, py.ZeroDivisionError.New("division by zero") }})
	})
	cf := must(new_cf(policy, tf, []any{ok, ok, bad, ok, ok}))
	if _, err := cf.Wait(T13_); !errors.Is(err, py.ZeroDivisionError) {
		t.Fatalf("want ZeroDivisionError, got %v", err)
	}
	assertEq(t, cf.Status(), laila.FutureStatus_ERROR)
	mu.Lock()
	assertEq(t, len(seen), 2)
	mu.Unlock()
	cf.Release()
}

func TestComplexFuture13_stage_fn_raising_synchronously(t *testing.T) {
	policy, tf := tf13(t)
	boom := stage(func(prev any) (any, error) { return nil, py.KeyError.New("stage") })
	cf := must(new_cf(policy, tf, []any{boom}))
	if _, err := cf.Wait(T13_); !errors.Is(err, py.KeyError) {
		t.Fatalf("want KeyError, got %v", err)
	}
	cf.Release()
}

func TestComplexFuture13_stage_returns_identity(t *testing.T) {
	policy, tf := tf13(t)
	st := stage(func(prev any) (any, error) {
		f, err := submit_(tf, []any{lam(5)})
		if err != nil {
			return nil, err
		}
		return ident_(cpf(f)), nil
	})
	cf := must(new_cf(policy, tf, []any{st, st}))
	assertEq(t, Unwrap(must(cf.Wait(T13_))), 5)
	cf.Release()
}

func TestComplexFuture13_stage_returns_garbage(t *testing.T) {
	policy, tf := tf13(t)
	cf := must(new_cf(policy, tf, []any{stage(func(p any) (any, error) { return 42, nil })}))
	if _, err := cf.Wait(T13_); !errors.Is(err, py.TypeError) {
		t.Fatalf("want TypeError, got %v", err)
	}
	cf.Release()
}

func TestComplexFuture13_empty_stages_rejected(t *testing.T) {
	policy, tf := tf13(t)
	if _, err := new_cf(policy, tf, []any{}); !errors.Is(err, py.ValueError) {
		t.Fatalf("want ValueError, got %v", err)
	}
}

func TestComplexFuture13_many_pipelines_concurrently(t *testing.T) {
	policy, tf := tf13(t)
	cfs := make([]*laila.ComplexFuture, 50)
	for i := range cfs {
		fns := make([]any, 10)
		for j := range fns {
			fns[j] = linear_stage(tf)
		}
		cfs[i] = must(new_cf(policy, tf, fns))
	}
	for _, cf := range cfs {
		assertEq(t, Unwrap(must(cf.Wait(T13_))), 10)
	}
	for _, cf := range cfs {
		cf.Release()
	}
	assertEq(t, policy.Future_bank.Len(), 0)
}

func TestComplexFuture13_pipeline_await_from_coroutine(t *testing.T) {
	policy, tf := tf13(t)
	cf := must(new_cf(policy, tf, []any{
		stage(func(p any) (any, error) { return submit_(tf, []any{lam(1)}) }),
		stage(func(p any) (any, error) { return submit_(tf, []any{lam(2)}) }),
	}))
	main := func() (any, error) {
		v, err := py.Await(cf)
		if err != nil {
			return nil, err
		}
		return Unwrap(v), nil
	}
	assertEq(t, must(Run(main)), 2)
	cf.Release()
}

func TestComplexFuture13_pipeline_timeout(t *testing.T) {
	policy, tf := tf13(t)
	cf := must(new_cf(policy, tf, []any{
		stage(func(p any) (any, error) { return submit_(tf, []any{sleep_then(0.5, 1)}) }),
	}))
	if _, err := cf.Wait(0.05); !errors.Is(err, py.TimeoutError) {
		t.Fatalf("want TimeoutError, got %v", err)
	}
	assertEq(t, Unwrap(must(cf.Wait(T13_))), 1)
	cf.Release()
}

func TestComplexFuture13_pipeline_nested_inside_task(t *testing.T) {
	policy, tf := tf13(t)
	body := func() (any, error) {
		cf, err := new_cf(policy, tf, []any{
			stage(func(p any) (any, error) { return submit_(tf, []any{lam(3)}) }),
			stage(func(p any) (any, error) {
				return submit_(tf, []any{lam(Unwrap(p).(int) * 2)})
			}),
		})
		if err != nil {
			return nil, err
		}
		v, err := cf.Wait(T13_)
		if err != nil {
			return nil, err
		}
		out := Unwrap(v)
		cf.Release()
		return out, nil
	}
	f := cpf(must(submit_(tf, []any{body})))
	assertEq(t, Unwrap(must(f.Wait(T13_))), 6)
	f.Release()
}

func TestComplexFuture13_group_stage_watcher_threads_are_reclaimed(t *testing.T) {
	policy, tf := tf13(t)
	before := py.NewSet()
	for _, th := range py.Enumerate() {
		before.Add(th.Name())
	}
	cfs := make([]*laila.ComplexFuture, 50)
	for i := range cfs {
		cfs[i] = must(new_cf(policy, tf, []any{
			stage(func(p any) (any, error) { return submit_(tf, []any{lam(1), lam(2)}) }),
		}))
	}
	for _, cf := range cfs {
		must(cf.Wait(T13_))
		cf.Release()
	}
	assertTrue(t, Wait_until(func() bool {
		for _, th := range py.Enumerate() {
			if strings.Contains(th.Name(), "ComplexFuture-GF-watch") && !before.Contains(th.Name()) {
				return false
			}
		}
		return true
	}, 10))
}

// ---------------------------------------------------------------------------
// identities and runtime helpers
// ---------------------------------------------------------------------------

func TestIdentity13_resolve_by_string_and_identity_thousands(t *testing.T) {
	_, tf := tf13(t)
	n := N_scaled(2000)
	futs := make([]*laila.ConcurrentPackageFuture, n)
	for i := range futs {
		futs[i] = cpf(must(submit_(tf, []any{lam(i)})))
	}
	sw := Timed(func() {
		for i, f := range futs {
			assertEq(t, Unwrap(must(laila.Wait(f.Global_id(), T13_))), i)
			assertEq(t, must(laila.Status(ident_(f))), laila.FutureStatus_FINISHED)
			exc, err := laila.Exception(f.Global_id())
			assertTrue(t, err == nil && exc == nil, err, exc)
		}
	})
	assertTrue(t, sw.Elapsed < 30, sw.Elapsed)
	for _, f := range futs {
		f.Release()
	}
}

func TestIdentity13_resolve_unknown_raises_keyerror(t *testing.T) {
	Fresh_policy(t)
	if _, err := laila.Status("LAILA:FUTURE:does-not-exist"); !errors.Is(err, py.KeyError) {
		t.Fatalf("want KeyError, got %v", err)
	}
}

func TestIdentity13_resolve_released_future_raises(t *testing.T) {
	_, tf := tf13(t)
	f := cpf(must(submit_(tf, []any{lam(1)})))
	must(f.Wait(T13_))
	gid := f.Global_id()
	f.Release()
	if _, err := laila.Status(gid); !errors.Is(err, py.KeyError) {
		t.Fatalf("want KeyError, got %v", err)
	}
}

func TestIdentity13_future_identity_property(t *testing.T) {
	_, tf := tf13(t)
	f := cpf(must(submit_(tf, []any{lam(1)})))
	ident := must(f.Future_identity())
	assertTrue(t, py.Isinstance(ident, laila.LAILA_IDENTIFIABLE_FUTURE_class))
	assertEq(t, ident.Global_id(), f.Global_id())
	must(f.Wait(T13_))
	f.Release()
}

func TestIdentity13_result_and_data_runtime_helpers(t *testing.T) {
	_, tf := tf13(t)
	f := cpf(must(submit_(tf, []any{lam(py.DictOf("k", py.NewList(1, 2)))})))
	assertEq(t, Unwrap(must(laila.Result(f))), py.DictOf("k", py.NewList(1, 2)))
	f.Release()
}

func TestIdentity13_future_from_other_policy_resolves_via_scan(t *testing.T) {
	policy, tf := tf13(t)
	f := cpf(must(submit_(tf, []any{lam(7)})))
	must(f.Wait(T13_))
	other := must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	laila.Activate_policy(other)
	func() {
		defer func() {
			laila.Activate_policy(policy)
			_ = Cmd(other).Shutdown(py.Kw{"wait": true, "cancel_pending": true})
			laila.Local_policies_.PopDefault(other.Global_id(), nil)
			f.Release()
		}()
		assertEq(t, Unwrap(must(laila.Wait(f.Global_id(), T13_))), 7)
	}()
}

func TestIdentity13_concurrent_release_and_resolve(t *testing.T) {
	policy, tf := tf13(t)
	futs := make([]*laila.ConcurrentPackageFuture, 2000)
	for i := range futs {
		futs[i] = cpf(must(submit_(tf, []any{lam(1)})))
	}
	for _, f := range futs {
		must(f.Wait(T13_))
	}
	var mu sync.Mutex
	errs := []string{}

	releaser := func(_ int) (any, error) {
		for i := 0; i < len(futs); i += 2 {
			futs[i].Release()
		}
		return nil, nil
	}
	resolver := func(_ int) (any, error) {
		for i := 1; i < len(futs); i += 2 {
			if _, err := laila.Status(futs[i].Global_id()); errors.Is(err, py.KeyError) {
				mu.Lock()
				errs = append(errs, futs[i].Global_id())
				mu.Unlock()
			} else if err != nil {
				return nil, err
			}
		}
		return nil, nil
	}
	must(Run_threads(func(i int) (any, error) {
		if i%2 == 0 {
			return releaser(i)
		}
		return resolver(i)
	}, 4))
	assertEq(t, errs, []string{})
	for i := 1; i < len(futs); i += 2 {
		futs[i].Release()
	}
	assertEq(t, policy.Future_bank.Len(), 0)
}

func TestIdentity13_future_bank_under_concurrent_churn(t *testing.T) {
	policy, tf := tf13(t)
	worker := func(i int) (any, error) {
		for k := 0; k < 200; k++ {
			f, err := submit_(tf, []any{lam(1)})
			if err != nil {
				return nil, err
			}
			if _, err := cpf(f).Wait(T13_); err != nil {
				return nil, err
			}
			cpf(f).Release()
		}
		return true, nil
	}
	outs := must(Run_threads(worker, 8))
	for _, o := range outs {
		assertEq(t, o, true)
	}
	assertEq(t, policy.Future_bank.Len(), 0)
}

func TestIdentity13_concurrent_package_future_repr_and_str(t *testing.T) {
	_, tf := tf13(t)
	raw := must(submit_(tf, []any{lam(1)}))
	f, ok := raw.(*laila.ConcurrentPackageFuture)
	assertTrue(t, ok, py.Pytypename(raw))
	assertTrue(t, strings.Contains(py.Str(f), f.Global_id()))
	must(f.Wait(T13_))
	f.Release()
}

func TestIdentity13_group_future_is_not_a_future(t *testing.T) {
	_, tf := tf13(t)
	raw := must(submit_(tf, []any{lam(1), lam(2)}))
	gf, ok := raw.(*laila.GroupFuture)
	assertTrue(t, ok, py.Pytypename(raw))
	_, is_cpf := raw.(*laila.ConcurrentPackageFuture)
	assertTrue(t, !is_cpf)
	must(gf.Wait(T13_))
	gf.Release()
}
