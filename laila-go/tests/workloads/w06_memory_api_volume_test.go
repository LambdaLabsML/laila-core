//go:build workload

// Port of tests/workloads/test_w06_memory_api_volume.py: the high-level
// memory verbs at volume and at their edges: huge batches, duplicate ids,
// partial failures inside a batch, nickname-derived ids, persist
// semantics, status/wait helpers, and future bookkeeping.
package workloads

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// ---------------------------------------------------------------------------
// batches
// ---------------------------------------------------------------------------

func TestBatches_batch_20k_small_constants(t *testing.T) {
	Fresh_policy(t)
	n := N_scaled(20_000, 2_000)
	es, ids := constants(t, n, func(i int) any { return i })
	var f, g any
	w := Timed(func() {
		f = must(laila.Memorize(entries_list(es)))
		must(Wait(f, 900))
	})
	var got *py.List
	r := Timed(func() {
		g = must(laila.Remember(ids, py.Kw{"persist": false}))
		got = must(Wait(g, 900)).(*py.List)
	})
	assertTrue(t, got.Len() == n && py.Eq(Unwrap(must(got.Get(-1))), n-1))
	Release_all(f, g)
	assertTrue(t, w.Elapsed/float64(n) < 0.02 && r.Elapsed/float64(n) < 0.02, fmt.Sprint(w.Elapsed, r.Elapsed))
}

func TestBatches_duplicate_ids_in_remember_batch(t *testing.T) {
	Fresh_policy(t)
	e := must(laila.Constant(py.DictOf("x", 1)))
	must(Wait(must(laila.Memorize(e)), 30))
	n := N_scaled(2_000, 200)
	ids := py.NewList()
	for i := 0; i < n; i++ {
		ids.Append(e.Global_id())
	}
	got := must(Wait(must(laila.Remember(ids, py.Kw{"persist": false})), 120)).(*py.List)
	assertEq(t, got.Len(), n)
	for _, g := range got.Items() {
		assertEq(t, Unwrap(g), py.DictOf("x", 1))
	}
}

func keys_with_uuid(t *testing.T, p *Pool, uuid string) int {
	t.Helper()
	return len(keys_with(t, p, uuid))
}

func TestBatches_same_constant_twice_in_memorize_batch(t *testing.T) {
	Fresh_policy(t)
	e := must(laila.Constant(py.DictOf("x", 1)))
	must(Wait(must(laila.Memorize(py.NewList(e, e, e))), 30))
	assertEq(t, keys_with_uuid(t, alpha_pool(t), e.Uuid()), 1)
}

func TestBatches_same_variable_twice_in_memorize_batch(t *testing.T) {
	Fresh_policy(t)
	v := must(laila.Variable(py.Kw{"data": py.DictOf("x", 1)}))
	must(Wait(must(laila.Memorize(py.NewList(v, v))), 30))
	assertEq(t, keys_with_uuid(t, alpha_pool(t), v.Uuid()), 1) // same payload -> same evolution, one key
}

func TestBatches_partial_missing_in_remember_batch_fails_group(t *testing.T) {
	Fresh_policy(t)
	es, ids := constants(t, 50, func(i int) any { return i })
	must(Wait(must(laila.Memorize(entries_list(es))), 30))
	ids.Insert(25, "LAILA:ENTRY:00000000-0000-4000-8000-0000000000ff")
	f := must(laila.Remember(ids, py.Kw{"persist": false}))
	_, err := Wait(f, 60)
	if !errors.Is(err, py.KeyError) {
		t.Fatalf("expected KeyError, got %v", err)
	}
	// the group future's children are still individually inspectable
	g := f.(*laila.GroupFuture)
	assertTrue(t, errors.Is(g.Exception(), py.KeyError))
	what := must(g.What())
	assertTrue(t, what != nil)
	assertEq(t, g.Len(), ids.Len())
}

func TestBatches_batch_with_mixed_payload_types(t *testing.T) {
	Fresh_policy(t)
	arange := make([]int64, 10)
	for i := range arange {
		arange[i] = int64(i)
	}
	payloads := []any{
		1,
		2.5,
		"s",
		[]byte("bytes"),
		nil,
		true,
		py.NewList(1, 2),
		py.DictOf("a", 1),
		py.Tuple{1, 2},
		py.NDArray_int64([]int{10}, arange),
		py.NewSet(1, 2),
	}
	reps := N_scaled(50, 5)
	es := make([]*laila.Entry, 0, len(payloads)*reps)
	ids := py.NewList()
	for r := 0; r < reps; r++ {
		for _, p := range payloads {
			e := must(laila.Constant(p))
			es = append(es, e)
			ids.Append(e.Global_id())
		}
	}
	must(Wait(must(laila.Memorize(entries_list(es))), 60))
	got := must(Wait(must(laila.Remember(ids, py.Kw{"persist": false})), 60)).(*py.List)
	for i, e := range es {
		a, b := must(e.Data()), Unwrap(must(got.Get(i)))
		if arr, ok := a.(*py.NDArray); ok {
			assertTrue(t, arr.Eq(b))
		} else {
			assertEq(t, b, a)
		}
	}
}

func TestBatches_forget_batch_then_remember_fails(t *testing.T) {
	Fresh_policy(t)
	es, ids := constants(t, N_scaled(2_000, 200), func(i int) any { return i })
	must(Wait(must(laila.Memorize(entries_list(es))), 60))
	must(Wait(must(laila.Forget(ids)), 60))
	f := must(laila.Remember(py.NewList(ids.Items()[:5]...), py.Kw{"persist": false}))
	_, err := Wait(f, 30)
	if !errors.Is(err, py.KeyError) {
		t.Fatalf("expected KeyError, got %v", err)
	}
}

func TestBatches_memorize_empty_list(t *testing.T) {
	Fresh_policy(t)
	f := must(laila.Memorize(py.NewList()))
	out := must(Wait(f, 30))
	assertTrue(t, out == nil || py.Eq(out, py.NewList()))
}

func TestBatches_remember_empty_list(t *testing.T) {
	Fresh_policy(t)
	f := must(laila.Remember(py.NewList(), py.Kw{"persist": false}))
	out := must(Wait(f, 30))
	assertTrue(t, out == nil || py.Eq(out, py.NewList()))
}

func TestBatches_large_batch_of_large_payloads(t *testing.T) {
	Fresh_policy(t)
	n := N_scaled(60, 10)
	es, ids := constants(t, n, func(i int) any { return py.DictOf("blob", Rand_bytes(1<<20, i)) })
	must(Wait(must(laila.Memorize(entries_list(es))), 300))
	got := must(Wait(must(laila.Remember(ids, py.Kw{"persist": false})), 300)).(*py.List)
	for i, e := range es {
		want := must(must(e.Data()).(*py.Dict).Getitem("blob"))
		assertEq(t, must(Unwrap(must(got.Get(i))).(*py.Dict).Getitem("blob")), want)
	}
}

// ---------------------------------------------------------------------------
// nicknames
// ---------------------------------------------------------------------------

func TestNicknames_nickname_constants_deterministic_at_scale(t *testing.T) {
	Fresh_policy(t)
	n := N_scaled(2_000, 200)
	es := make([]*laila.Entry, n)
	for i := range es {
		es[i] = must(laila.Constant(py.DictOf("i", i), py.Kw{"nickname": fmt.Sprintf("cfg-%d", i)}))
	}
	must(Wait(must(laila.Memorize(entries_list(es))), 120))
	for i := 0; i < n; i += 97 {
		e2 := must(laila.Entry_constant(nil, py.Kw{"nickname": fmt.Sprintf("cfg-%d", i)}))
		assertEq(t, e2.Global_id(), es[i].Global_id())
		got := must(Wait(must(laila.Remember(fmt.Sprintf("ENTRY:cfg-%d", i), py.Kw{"persist": false})), 30))
		assertEq(t, Unwrap(got), py.DictOf("i", i))
	}
}

func TestNicknames_nickname_variable_lineage(t *testing.T) {
	Fresh_policy(t)
	v := must(laila.Variable(py.Kw{"data": py.DictOf("v", 0), "nickname": "train_config"}))
	n := N_scaled(200, 40)
	for i := 1; i <= n; i++ {
		check(t, v.Set_data(py.DictOf("v", i)))
		must(Wait(must(laila.Memorize(v)), 30))
	}
	got := must(Wait(must(laila.Remember("ENTRY:train_config@evolution=-1", py.Kw{"persist": false})), 30))
	assertEq(t, Unwrap(got), py.DictOf("v", n))
	got = must(Wait(must(laila.Remember("ENTRY:train_config@evolution=1", py.Kw{"persist": false})), 30))
	assertEq(t, Unwrap(got), py.DictOf("v", 1))
}

func TestNicknames_nickname_collision_overwrites(t *testing.T) {
	Fresh_policy(t)
	a := must(laila.Constant(1, py.Kw{"nickname": "same"}))
	b := must(laila.Constant(2, py.Kw{"nickname": "same"}))
	assertEq(t, a.Global_id(), b.Global_id())
	must(Wait(must(laila.Memorize(a)), 30))
	must(Wait(must(laila.Memorize(b)), 30))
	assertEq(t, Unwrap(must(Wait(must(laila.Remember("ENTRY:same", py.Kw{"persist": false})), 30))), 2)
}

func TestNicknames_unicode_nicknames(t *testing.T) {
	Fresh_policy(t)
	n := N_scaled(300, 30)
	names := make([]string, n)
	es := make([]*laila.Entry, n)
	for i := range names {
		names[i] = fmt.Sprintf("配置-%d-✓", i)
		es[i] = must(laila.Constant(i, py.Kw{"nickname": names[i]}))
	}
	must(Wait(must(laila.Memorize(entries_list(es))), 60))
	for i := 0; i < n; i += 37 {
		assertEq(t, Unwrap(must(Wait(must(laila.Remember("ENTRY:"+names[i], py.Kw{"persist": false})), 30))), i)
	}
}

// ---------------------------------------------------------------------------
// persist semantics
// ---------------------------------------------------------------------------

func TestPersistSemantics_persist_true_caches_into_alpha(t *testing.T) {
	policy := Fresh_policy(t)
	src := default_pool(t)
	mem_extend(t, policy, src.Self(), "src")
	es, ids := constants(t, N_scaled(500, 50), func(i int) any { return i })
	must(Wait(must(laila.Memorize(entries_list(es), py.Kw{"pool_nickname": "src"})), 60))
	must(Wait(must(laila.Remember(ids, py.Kw{"pool_nickname": "src", "persist": true})), 120))
	for _, i := range ids.Items() {
		assertTrue(t, must(alpha_pool(t).Exists(py.Str(i))))
	}
}

func TestPersistSemantics_persist_false_leaves_alpha_untouched(t *testing.T) {
	policy := Fresh_policy(t)
	src := default_pool(t)
	mem_extend(t, policy, src.Self(), "src")
	es, ids := constants(t, N_scaled(500, 50), func(i int) any { return i })
	must(Wait(must(laila.Memorize(entries_list(es), py.Kw{"pool_nickname": "src"})), 60))
	must(Wait(must(laila.Remember(ids, py.Kw{"pool_nickname": "src", "persist": false})), 120))
	for _, i := range ids.Items() {
		assertTrue(t, !must(alpha_pool(t).Exists(py.Str(i))))
	}
}

func TestPersistSemantics_persist_from_alpha_to_alpha_is_noop(t *testing.T) {
	Fresh_policy(t)
	es, ids := constants(t, N_scaled(500, 50), func(i int) any { return i })
	must(Wait(must(laila.Memorize(entries_list(es))), 60))
	before := pkeys(t, alpha_pool(t)).Len()
	must(Wait(must(laila.Remember(ids, py.Kw{"persist": true})), 60))
	assertEq(t, pkeys(t, alpha_pool(t)).Len(), before)
}

func TestPersistSemantics_persist_variable_latest_keeps_evolution_key(t *testing.T) {
	policy := Fresh_policy(t)
	src := default_pool(t)
	mem_extend(t, policy, src.Self(), "src")
	v := variable0(t)
	for i := 1; i < 6; i++ {
		check(t, v.Set_data(py.DictOf("v", i)))
		must(Wait(must(laila.Memorize(v, py.Kw{"pool_nickname": "src"})), 30))
	}
	base := strings.SplitN(v.Global_id(), "@", 2)[0]
	got := must(Wait(must(laila.Remember(base+"@evolution=-1", py.Kw{"pool_nickname": "src", "persist": true})), 30))
	assertEq(t, Unwrap(got), py.DictOf("v", 5))
	assertTrue(t, must(alpha_pool(t).Exists(base+"@evolution=5")))
}

// ---------------------------------------------------------------------------
// status / wait
// ---------------------------------------------------------------------------

func TestStatusWait_status_transitions_over_many_futures(t *testing.T) {
	Fresh_policy(t)
	es, _ := constants(t, N_scaled(300, 50), func(i int) any { return i })
	futs := make([]any, len(es))
	for i, e := range es {
		futs[i] = must(laila.Memorize(e))
	}
	for _, f := range futs {
		must(Wait(f, 60))
	}
	statuses := map[string]struct{}{}
	for _, f := range futs {
		statuses[py.Str(must(laila.Status(f)))] = struct{}{}
	}
	assertTrue(t, len(statuses) >= 1)
	Release_all(futs...)
}

func TestStatusWait_wait_by_future_id(t *testing.T) {
	Fresh_policy(t)
	f := must(laila.Memorize(must(laila.Constant(1))))
	gid := py.Str(must(py.Getattr(f, "global_id")))
	out := must(laila.Wait(gid, py.Kw{"timeout": 30}))
	assertTrue(t, out != nil || f.(interface{ Finished() bool }).Finished())
	Release_all(f)
}

func TestStatusWait_wait_timeout_on_never_finishing(t *testing.T) {
	policy := Fresh_policy(t)
	gate := py.NewEvent()
	blocker := func(args ...any) (any, error) {
		gate.Wait(60)
		return 1, nil
	}
	f := must(Cmd(policy).Submit([]any{blocker}))
	var err error
	sw := Timed(func() { _, err = Wait(f, 0.5) })
	assertTrue(t, err != nil, "expected a timeout")
	assertTrue(t, sw.Elapsed < 5)
	gate.Set()
	must(Wait(f, 30))
	Release_all(f)
}

func TestStatusWait_runtime_status_for_unknown_id(t *testing.T) {
	Fresh_policy(t)
	_, err := laila.Status("LAILA:FUTURE:00000000-0000-4000-8000-000000000000")
	assertTrue(t, err != nil, "expected an error")
}

// ---------------------------------------------------------------------------
// concurrent verbs
// ---------------------------------------------------------------------------

func TestConcurrentVerbs_threads_mixing_memorize_remember_forget(t *testing.T) {
	Fresh_policy(t)
	per := N_scaled(100, 20)
	var mu sync.Mutex
	errs := []string{}
	check(t, Go_threads(func(tt int) error {
		err := func() error {
			for j := 0; j < per; j++ {
				e, err := laila.Constant(py.DictOf("t", tt, "j", j))
				if err != nil {
					return err
				}
				f, err := laila.Memorize(e)
				if err != nil {
					return err
				}
				if _, err := Wait(f, 60); err != nil {
					return err
				}
				g, err := laila.Remember(e.Global_id(), py.Kw{"persist": false})
				if err != nil {
					return err
				}
				got, err := Wait(g, 60)
				if err != nil {
					return err
				}
				data, _ := e.Data()
				if !py.Eq(Unwrap(got), data) {
					return py.AssertionError.Newf("%v != %v", Unwrap(got), data)
				}
				if j%2 == 1 {
					h, err := laila.Forget(e.Global_id())
					if err != nil {
						return err
					}
					if _, err := Wait(h, 60); err != nil {
						return err
					}
				}
			}
			return nil
		}()
		if err != nil {
			mu.Lock()
			errs = append(errs, err.Error())
			mu.Unlock()
		}
		return nil
	}, 8))
	assertEq(t, errs, []string{})
	assertEq(t, pkeys(t, alpha_pool(t)).Len(), 8*((per+1)/2))
}

func TestConcurrentVerbs_many_threads_small_batches_bank_drains(t *testing.T) {
	policy := Fresh_policy(t)
	per := N_scaled(50, 10)
	check(t, Go_threads(func(tt int) error {
		for j := 0; j < per; j++ {
			batch := py.NewList()
			for k := 0; k < 5; k++ {
				e, err := laila.Constant(k)
				if err != nil {
					return err
				}
				batch.Append(e)
			}
			f, err := laila.Memorize(batch)
			if err != nil {
				return err
			}
			if _, err := Wait(f, 60); err != nil {
				return err
			}
			Release_all(f)
		}
		return nil
	}, 16))
	time.Sleep(200 * time.Millisecond)
	assertEq(t, policy.Future_bank.Len(), 0)
}

func TestConcurrentVerbs_remember_while_forget_races_never_hang(t *testing.T) {
	Fresh_policy(t)
	es, idl := constants(t, N_scaled(2_000, 200), func(i int) any { return i })
	must(Wait(must(laila.Memorize(entries_list(es))), 60))
	ids := make([]string, idl.Len())
	for i, v := range idl.Items() {
		ids[i] = py.Str(v)
	}
	var mu sync.Mutex
	ok, missing := 0, 0
	forgetter := func(tt int) error {
		for i := tt; i < len(ids); i += 2 {
			f, err := laila.Forget(ids[i])
			if err != nil {
				return err
			}
			if _, err := Wait(f, 30); err != nil {
				return err
			}
		}
		return nil
	}
	rememberer := func(tt int) error {
		for _, i := range ids {
			f, err := laila.Remember(i, py.Kw{"persist": false})
			if err == nil {
				_, err = Wait(f, 30)
			}
			mu.Lock()
			if err == nil {
				ok++
			} else if errors.Is(err, py.KeyError) {
				missing++
			} else {
				mu.Unlock()
				return err
			}
			mu.Unlock()
		}
		return nil
	}
	sw := Timed(func() {
		check(t, Go_threads(func(tt int) error {
			if tt < 2 {
				return forgetter(tt)
			}
			return rememberer(tt)
		}, 4))
	})
	assertTrue(t, sw.Elapsed < 600)
	assertEq(t, ok+missing, 2*len(ids))
}

// ---------------------------------------------------------------------------
// memorize scaling
// ---------------------------------------------------------------------------

func memorize_bench(t *testing.T, probe int) float64 {
	t.Helper()
	es, _ := constants(t, probe, func(i int) any { return i })
	var f any
	sw := Timed(func() {
		f = must(laila.Memorize(entries_list(es)))
		must(Wait(f, 600))
	})
	Release_all(f)
	return sw.Elapsed
}

func fill_pool(t *testing.T, fill int) {
	t.Helper()
	es, _ := constants(t, fill, func(i int) any { return i })
	f := must(laila.Memorize(entries_list(es)))
	must(Wait(f, 900))
	Release_all(f)
}

func TestMemorizeScaling_memorize_cost_independent_of_pool_size(t *testing.T) {
	// F-12 (fixed): shards live under constant keys, so loading or
	// writing one never lists the pool's keys.
	Fresh_policy(t)
	probe := N_scaled(600, 200)
	fill := N_scaled(12_000, 4_000)
	empty := memorize_bench(t, probe)
	fill_pool(t, fill)
	full := memorize_bench(t, probe)
	assertTrue(t, full < empty*2.0,
		fmt.Sprintf("%d memorizes: %.2fs on empty pool vs %.2fs with %d keys", probe, empty, full, fill))
}

func TestMemorizeScaling_memorize_cost_flat_with_index_disabled(t *testing.T) {
	Fresh_policy(t)
	alpha_pool(t).Index_enabled = false
	probe := N_scaled(600, 200)
	fill := N_scaled(12_000, 4_000)
	empty := memorize_bench(t, probe)
	fill_pool(t, fill)
	full := memorize_bench(t, probe)
	assertTrue(t, full < max(empty, 0.2)*2.5, fmt.Sprint(empty, full))
}

func TestMemorizeScaling_remember_cost_independent_of_pool_size(t *testing.T) {
	Fresh_policy(t)
	probe := N_scaled(600, 200)
	es, ids := constants(t, probe, func(i int) any { return i })
	must(Wait(must(laila.Memorize(entries_list(es))), 300))
	bench := func() float64 {
		var g any
		sw := Timed(func() {
			g = must(laila.Remember(ids, py.Kw{"persist": false}))
			must(Wait(g, 600))
		})
		Release_all(g)
		return sw.Elapsed
	}
	small := bench()
	fill_pool(t, N_scaled(12_000, 4_000))
	large := bench()
	assertTrue(t, large < max(small, 0.2)*2.5, fmt.Sprint(small, large))
}
