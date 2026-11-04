//go:build workload

// Port of tests/workloads/test_w01_mempool_volume.py: in-memory
// ``DefaultPool`` under volume: 100k keys, 100 MB values, churn, lock
// behaviour under contention, index growth, and the memorize/remember path
// through the alpha pool at scale.
package workloads

import (
	"fmt"
	"iter"
	"strings"
	"sync"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

const BASE = "LAILA:ENTRY:00000000-0000-4000-8000-00000000%04x"

func base_(i int) string { return fmt.Sprintf(BASE, i) }

type Pool = laila.LAILA_IDENTIFIABLE_POOL_

func default_pool(t *testing.T, kw ...py.Kw) *Pool {
	t.Helper()
	p, err := laila.New_DefaultPool(kw...)
	if err != nil {
		t.Fatal(err)
	}
	return laila.As_pool_(p)
}

func pset(t *testing.T, p *Pool, k string, v any) {
	if err := p.Setitem(k, v); err != nil {
		t.Helper()
		t.Fatal(err)
	}
}

func pget(t *testing.T, p *Pool, k string) any {
	v, err := p.Getitem(k)
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}
	return v
}

func pdel(t *testing.T, p *Pool, k string) {
	if err := p.Delitem(k); err != nil {
		t.Helper()
		t.Fatal(err)
	}
}

func pkeys(t *testing.T, p *Pool, kw ...py.Kw) *py.List {
	t.Helper()
	return must(p.Keys(kw...)).(*py.List)
}

func pkeys_gen(t *testing.T, p *Pool, kw ...py.Kw) py.Iterator {
	t.Helper()
	opts := py.MergeKw(kw...)
	opts["as_generator"] = true
	return must(p.Keys(opts)).(py.Iterator)
}

func iter_list(it py.Iterator) *py.List {
	out := py.NewList()
	for k, err := range it {
		if err != nil {
			panic(err)
		}
		out.Append(k)
	}
	return out
}

// ---------------------------------------------------------------------------
// raw pool volume
// ---------------------------------------------------------------------------

func TestRawVolume_write_read_100k_keys(t *testing.T) {
	Setup(t)
	pool := default_pool(t, py.Kw{"index_enabled": false})
	n := N_scaled(100_000)
	sw := Timed(func() {
		for i := 0; i < n; i++ {
			pset(t, pool, fmt.Sprintf("k%d", i), py.DictOf("i", i))
		}
	})
	assertEq(t, pkeys(t, pool).Len(), n)
	for i := 0; i < n; i += 997 {
		assertEq(t, pget(t, pool, fmt.Sprintf("k%d", i)), py.DictOf("i", i))
	}
	assertTrue(t, sw.Elapsed < 60, sw.Elapsed)
}

func TestRawVolume_keys_listing_scales_linearly(t *testing.T) {
	Setup(t)
	small, large := default_pool(t, py.Kw{"index_enabled": false}), default_pool(t, py.Kw{"index_enabled": false})
	ns, nl := N_scaled(10_000), N_scaled(100_000)
	for i := 0; i < ns; i++ {
		pset(t, small, fmt.Sprintf("k%d", i), i)
	}
	for i := 0; i < nl; i++ {
		pset(t, large, fmt.Sprintf("k%d", i), i)
	}
	s1 := Timed(func() {
		for j := 0; j < 5; j++ {
			assertEq(t, pkeys(t, small).Len(), ns)
		}
	})
	s2 := Timed(func() {
		for j := 0; j < 5; j++ {
			assertEq(t, pkeys(t, large).Len(), nl)
		}
	})
	check(t, Assert_subquadratic(s1.Elapsed, s2.Elapsed, float64(nl)/float64(ns)))
}

func TestRawVolume_large_values_roundtrip_identity(t *testing.T) {
	for _, mb := range []int{1, 10, 100} {
		t.Run(fmt.Sprintf("mb=%d", mb), func(t *testing.T) {
			Setup(t)
			pool := default_pool(t, py.Kw{"index_enabled": false})
			blob := Rand_bytes(mb*1024*1024, mb)
			pset(t, pool, "big", py.DictOf("blob", blob))
			got := pget(t, pool, "big").(*py.Dict)
			// mem pool stores by reference: same backing array
			gotBlob := must(got.Getitem("blob")).([]byte)
			assertTrue(t, len(gotBlob) == len(blob) && &gotBlob[0] == &blob[0], "mem pool must store by reference")
			pdel(t, pool, "big")
			assertTrue(t, py.IsNone(pget(t, pool, "big")))
		})
	}
}

func TestRawVolume_churn_cycles_leave_pool_empty(t *testing.T) {
	Setup(t)
	pool := default_pool(t)
	cycles, per := N_scaled(50, 5), N_scaled(2_000, 100)
	for c := 0; c < cycles; c++ {
		for i := 0; i < per; i++ {
			pset(t, pool, fmt.Sprintf("%s@evolution=%d", base_(i), c), py.DictOf("c", c))
		}
		for i := 0; i < per; i++ {
			check(t, pool.Delete(fmt.Sprintf("%s@evolution=%d", base_(i), c)))
		}
	}
	assertEq(t, pkeys(t, pool), py.NewList())
	assertEq(t, pkeys(t, pool, py.Kw{"include_index": true}), py.NewList())
	assertEq(t, pool.Resource.Len(), 0)
}

func TestRawVolume_overwrite_same_key_many_times(t *testing.T) {
	Setup(t)
	pool := default_pool(t)
	n := N_scaled(100_000)
	for i := 0; i < n; i++ {
		pset(t, pool, "hot", py.DictOf("i", i))
	}
	assertEq(t, pget(t, pool, "hot"), py.DictOf("i", n-1))
	assertEq(t, pkeys(t, pool).Len(), 1)
}

func any_index_key(keys *py.List) bool {
	for _, k := range keys.Items() {
		if laila.Is_index_key(k) {
			return true
		}
	}
	return false
}

func TestRawVolume_empty_at_scale_clears_data_and_index(t *testing.T) {
	Setup(t)
	pool := default_pool(t)
	n := N_scaled(20_000)
	for i := 0; i < n; i++ {
		pset(t, pool, fmt.Sprintf("%s@evolution=%d", base_(i%2000), i/2000), py.DictOf("i", i))
	}
	assertEq(t, pkeys(t, pool).Len(), n)
	assertTrue(t, any_index_key(pkeys(t, pool, py.Kw{"include_index": true})))
	sw := Timed(func() { check(t, pool.Empty()) })
	assertEq(t, pkeys(t, pool), py.NewList())
	assertEq(t, pkeys(t, pool, py.Kw{"include_index": true}), py.NewList())
	assertTrue(t, sw.Elapsed < 60)
}

func TestRawVolume_many_pool_instances(t *testing.T) {
	Setup(t)
	n := N_scaled(300, 20)
	var pools []*Pool
	sw := Timed(func() {
		for i := 0; i < n; i++ {
			pools = append(pools, default_pool(t))
		}
	})
	gids := map[string]struct{}{}
	for _, p := range pools {
		gids[p.Global_id()] = struct{}{}
	}
	assertEq(t, len(gids), n)
	for i, p := range pools {
		pset(t, p, fmt.Sprintf("k%d", i), i)
	}
	for i := 0; i < n; i++ {
		assertEq(t, pget(t, pools[i], fmt.Sprintf("k%d", i)), i)
	}
	assertTrue(t, sw.Elapsed < 120)
}

func TestRawVolume_none_value_reads_as_miss(t *testing.T) {
	Setup(t)
	pool := default_pool(t, py.Kw{"index_enabled": false})
	for i := 0; i < N_scaled(10_000); i++ {
		pset(t, pool, fmt.Sprintf("k%d", i), nil)
	}
	assertTrue(t, py.IsNone(pget(t, pool, "k0")))
	assertTrue(t, must(pool.Exists("k0"))) // stored, but unreadable through __getitem__
	assertEq(t, pkeys(t, pool).Len(), N_scaled(10_000))
}

func TestRawVolume_unicode_and_long_keys(t *testing.T) {
	Setup(t)
	pool := default_pool(t, py.Kw{"index_enabled": false})
	var keys []any
	for i := 0; i < N_scaled(5_000); i++ {
		keys = append(keys, strings.Repeat("ключ✓", 50)+fmt.Sprint(i))
	}
	for _, k := range keys {
		pset(t, pool, k.(string), py.DictOf("k", k))
	}
	assertEq(t, py.NewSet(pkeys(t, pool).Items()...), py.NewSet(keys...))
	for i := 0; i < len(keys); i += 97 {
		k := keys[i].(string)
		assertEq(t, must(pget(t, pool, k).(*py.Dict).Getitem("k")), k)
	}
}

// ---------------------------------------------------------------------------
// concurrency
// ---------------------------------------------------------------------------

func TestConcurrency_parallel_writers_disjoint_keys(t *testing.T) {
	for _, n_threads := range []int{4, 16, 32} {
		t.Run(fmt.Sprintf("n_threads=%d", n_threads), func(t *testing.T) {
			Setup(t)
			pool := default_pool(t, py.Kw{"index_enabled": false})
			per := N_scaled(5_000, 200)
			check(t, Go_threads(func(i int) error {
				for j := 0; j < per; j++ {
					if err := pool.Setitem(fmt.Sprintf("%d-%d", i, j), py.DictOf("v", j)); err != nil {
						return err
					}
				}
				return nil
			}, n_threads))
			assertEq(t, pkeys(t, pool).Len(), n_threads*per)
		})
	}
}

func TestConcurrency_parallel_writers_same_key_consistent(t *testing.T) {
	Setup(t)
	pool := default_pool(t, py.Kw{"index_enabled": false})
	n_threads, per := 8, N_scaled(20_000, 500)
	check(t, Go_threads(func(i int) error {
		for j := 0; j < per; j++ {
			if err := pool.Setitem("hot", py.DictOf("t", i, "j", j)); err != nil {
				return err
			}
		}
		return nil
	}, n_threads))
	v := pget(t, pool, "hot").(*py.Dict)
	assertEq(t, must(v.Getitem("j")), per-1)
	tt := must(py.Int(must(v.Getitem("t"))))
	assertTrue(t, 0 <= tt && tt < n_threads)
}

func TestConcurrency_readers_during_writes_never_see_torn_state(t *testing.T) {
	Setup(t)
	pool := default_pool(t, py.Kw{"index_enabled": false})
	n := N_scaled(20_000, 1000)
	for i := 0; i < n; i++ {
		pset(t, pool, fmt.Sprintf("k%d", i), py.DictOf("i", i))
	}
	var stop py.Event
	var mu sync.Mutex
	var misses []int

	reader := func() {
		for !stop.Is_set() {
			for j := 0; j < n; j += 211 {
				v, _ := pool.Getitem(fmt.Sprintf("k%d", j))
				d, ok := v.(*py.Dict)
				if !ok || !py.Eq(must(d.Getitem("i")), j) {
					mu.Lock()
					misses = append(misses, j)
					mu.Unlock()
				}
			}
		}
	}
	writer := func() {
		for j := n; j < 2*n; j++ {
			_ = pool.Setitem(fmt.Sprintf("k%d", j), py.DictOf("i", j))
		}
		stop.Set()
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); reader() }()
	}
	wg.Add(1)
	go func() { defer wg.Done(); writer() }()
	wg.Wait()
	assertEq(t, len(misses), 0)
	assertEq(t, pkeys(t, pool).Len(), 2*n)
}

// Documented: ``keys(as_generator=True)`` holds the pool lock. In Go a
// half-consumed generator is an ``iter.Pull`` handle; ``stop()`` is the
// explicit counterpart of exhausting (or dropping) the Python generator.
func TestConcurrency_generator_keys_holds_lock_until_exhausted(t *testing.T) {
	Setup(t)
	pool := default_pool(t, py.Kw{"index_enabled": false})
	for i := 0; i < 100; i++ {
		pset(t, pool, fmt.Sprintf("k%d", i), i)
	}
	next, stop := iter.Pull2(iter.Seq2[any, error](pkeys_gen(t, pool)))
	_, _, _ = next()
	var done py.Event
	go func() {
		_ = pool.Setitem("w", 1)
		done.Set()
	}()
	assertTrue(t, !done.Wait(0.5), "writer must be blocked by the live generator")
	for {
		if _, _, ok := next(); !ok {
			break
		}
	}
	stop() // exhaust -> lock released
	assertTrue(t, done.Wait(5))
}

func TestConcurrency_abandoned_generator_releases_lock_on_gc(t *testing.T) {
	Setup(t)
	pool := default_pool(t, py.Kw{"index_enabled": false})
	for i := 0; i < 100; i++ {
		pset(t, pool, fmt.Sprintf("k%d", i), i)
	}
	next, stop := iter.Pull2(iter.Seq2[any, error](pkeys_gen(t, pool)))
	_, _, _ = next()
	stop() // ``del gen; gc.collect()`` -- Go has no finalizer-driven close; stop() is the explicit drop
	var done py.Event
	go func() {
		_ = pool.Write("w", 1)
		done.Set()
	}()
	assertTrue(t, done.Wait(5))
}

func TestConcurrency_contains_and_exists_under_contention(t *testing.T) {
	Setup(t)
	pool := default_pool(t, py.Kw{"index_enabled": false})
	n := N_scaled(20_000, 1000)
	var mu sync.Mutex
	var errs []string
	check(t, Go_threads(func(i int) error {
		if i == 0 {
			for j := 0; j < n; j++ {
				if err := pool.Setitem(fmt.Sprintf("k%d", j), j); err != nil {
					return err
				}
			}
			return nil
		}
		for j := 0; j < n; j++ {
			k := fmt.Sprintf("k%d", j)
			in, err := pool.Contains(k)
			if err != nil {
				return err
			}
			if in {
				ex, err := pool.Exists(k)
				if err != nil {
					return err
				}
				if !ex {
					mu.Lock()
					errs = append(errs, k)
					mu.Unlock()
				}
			}
		}
		return nil
	}, 5))
	assertEq(t, len(errs), 0)
}

func TestConcurrency_delete_storm_with_concurrent_reads(t *testing.T) {
	Setup(t)
	pool := default_pool(t, py.Kw{"index_enabled": false})
	n := N_scaled(20_000, 1000)
	for i := 0; i < n; i++ {
		pset(t, pool, fmt.Sprintf("k%d", i), i)
	}
	check(t, Go_threads(func(i int) error {
		if i < 4 {
			for j := i; j < n; j += 4 {
				if err := pool.Delitem(fmt.Sprintf("k%d", j)); err != nil {
					return err
				}
			}
			return nil
		}
		for j := 0; j < n; j++ {
			if _, err := pool.Getitem(fmt.Sprintf("k%d", j)); err != nil { // must never raise
				return err
			}
		}
		return nil
	}, 8))
	assertEq(t, pkeys(t, pool), py.NewList())
}

// ---------------------------------------------------------------------------
// index growth
// ---------------------------------------------------------------------------

func deref(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}

func TestIndexGrowth_hot_variable_index_cost_is_not_quadratic(t *testing.T) {
	Setup(t)
	pool := default_pool(t)
	base := base_(1)
	n := N_scaled(4_000, 400)
	batch := n / 4
	first := Timed(func() {
		for i := 0; i < batch; i++ {
			pset(t, pool, fmt.Sprintf("%s@evolution=%d", base, i), py.DictOf("i", i))
		}
	})
	for i := batch; i < n-batch; i++ {
		pset(t, pool, fmt.Sprintf("%s@evolution=%d", base, i), py.DictOf("i", i))
	}
	last := Timed(func() {
		for i := n - batch; i < n; i++ {
			pset(t, pool, fmt.Sprintf("%s@evolution=%d", base, i), py.DictOf("i", i))
		}
	})
	// the shard is rewritten with the whole evolution list on every write;
	// we tolerate growth but not more than 25x between first and last batch
	assertTrue(t, last.Elapsed <= max(first.Elapsed, 0.01)*25, first.Elapsed, last.Elapsed)
	assertEq(t, deref(pool.Index().Latest(base)), fmt.Sprintf("%s@evolution=%d", base, n-1))
	assertEq(t, deref(pool.Index().Nth(base, -1)), fmt.Sprintf("%s@evolution=%d", base, n-1))
	assertEq(t, deref(pool.Index().Nth(base, 0)), base+"@evolution=0")
}

func TestIndexGrowth_shard_count_matches_bases(t *testing.T) {
	Setup(t)
	pool := default_pool(t)
	n_bases, n_evo := N_scaled(2_000, 100), 3
	for b := 0; b < n_bases; b++ {
		for e := 0; e < n_evo; e++ {
			pset(t, pool, fmt.Sprintf("%s@evolution=%d", base_(b), e), py.DictOf("b", b, "e", e))
		}
	}
	shards := 0
	for _, k := range pkeys(t, pool, py.Kw{"include_index": true}).Items() {
		if laila.Is_index_key(k) {
			shards++
		}
	}
	assertEq(t, shards, n_bases)
	assertEq(t, pkeys(t, pool).Len(), n_bases*n_evo)
}

func TestIndexGrowth_index_disabled_has_no_shards_and_is_faster(t *testing.T) {
	Setup(t)
	on, off := default_pool(t, py.Kw{"index_enabled": true}), default_pool(t, py.Kw{"index_enabled": false})
	n := N_scaled(5_000, 500)
	t_on := Timed(func() {
		for i := 0; i < n; i++ {
			pset(t, on, base_(i)+"@evolution=0", py.DictOf("i", i))
		}
	})
	t_off := Timed(func() {
		for i := 0; i < n; i++ {
			pset(t, off, base_(i)+"@evolution=0", py.DictOf("i", i))
		}
	})
	assertTrue(t, !any_index_key(pkeys(t, off, py.Kw{"include_index": true})))
	assertTrue(t, any_index_key(pkeys(t, on, py.Kw{"include_index": true})))
	assertTrue(t, t_off.Elapsed <= t_on.Elapsed, t_off.Elapsed, t_on.Elapsed)
}

func TestIndexGrowth_remove_all_evolutions_drops_shard(t *testing.T) {
	Setup(t)
	pool := default_pool(t)
	base := base_(7)
	for e := 0; e < 50; e++ {
		pset(t, pool, fmt.Sprintf("%s@evolution=%d", base, e), py.DictOf("e", e))
	}
	for e := 0; e < 50; e++ {
		check(t, pool.Delete(fmt.Sprintf("%s@evolution=%d", base, e)))
	}
	assertTrue(t, !any_index_key(pkeys(t, pool, py.Kw{"include_index": true})))
	assertTrue(t, pool.Index().Latest(base) == nil)
}

func TestIndexGrowth_index_rebuild_after_mass_write(t *testing.T) {
	Setup(t)
	pool := default_pool(t)
	n := N_scaled(3_000, 300)
	for i := 0; i < n; i++ {
		pset(t, pool, fmt.Sprintf("%s@evolution=%d", base_(i%100), i/100), py.DictOf("i", i))
	}
	pool.Index().Invalidate()
	sw := Timed(func() { pool.Index().Rebuild() })
	assertTrue(t, sw.Elapsed < 60)
	for b := 0; b < 100; b++ {
		assertEq(t, deref(pool.Index().Latest(base_(b))), fmt.Sprintf("%s@evolution=%d", base_(b), n/100-1))
	}
}

func TestIndexGrowth_index_missing_cache_bounded_by_touches(t *testing.T) {
	Setup(t)
	pool := default_pool(t)
	n := N_scaled(20_000, 1000)
	for i := 0; i < n; i++ {
		pool.Index().Latest(base_(i)) // miss
	}
	// caches are per base ever touched; document the growth
	pool.Lock()
	missing := len(pool.Index().Missing_)
	pool.Unlock()
	assertTrue(t, missing <= n)
}

// ---------------------------------------------------------------------------
// through the policy (alpha pool is a DefaultPool)
// ---------------------------------------------------------------------------

func alpha_pool(t *testing.T) *Pool {
	t.Helper()
	return laila.As_pool_(must(laila.Alpha_pool()))
}

func constants(t *testing.T, n int, data func(i int) any) ([]*laila.Entry, *py.List) {
	t.Helper()
	es := make([]*laila.Entry, n)
	ids := py.NewList()
	for i := range es {
		es[i] = must(laila.Constant(data(i)))
		ids.Append(es[i].Global_id())
	}
	return es, ids
}

func entries_list(es []*laila.Entry) *py.List {
	l := py.NewList()
	for _, e := range es {
		l.Append(e)
	}
	return l
}

func TestMemorizeThroughAlpha_memorize_remember_forget_10k_batch(t *testing.T) {
	Fresh_policy(t)
	n := N_scaled(10_000, 500)
	entries, ids := constants(t, n, func(i int) any { return py.DictOf("i", i) })
	var f, g, h any
	w := Timed(func() {
		f = must(laila.Memorize(entries_list(entries)))
		must(Wait(f, 300))
	})
	var got *py.List
	r := Timed(func() {
		g = must(laila.Remember(ids))
		got = must(Wait(g, 300)).(*py.List)
	})
	assertEq(t, got.Len(), n)
	for i := 0; i < n; i += 101 {
		assertEq(t, must(Unwrap(must(got.Get(i))).(*py.Dict).Getitem("i")), i)
	}
	h = must(laila.Forget(ids))
	must(Wait(h, 300))
	Release_all(f, g, h)
	assertEq(t, pkeys(t, alpha_pool(t)), py.NewList())
	assertTrue(t, w.Elapsed < 240 && r.Elapsed < 240)
}

func TestMemorizeThroughAlpha_memorize_10k_single_futures(t *testing.T) {
	policy := Fresh_policy(t)
	n := N_scaled(5_000, 300)
	futs := make([]any, n)
	for i := range futs {
		futs[i] = must(laila.Memorize(must(laila.Constant(i))))
	}
	for _, f := range futs {
		must(Wait(f, 120))
	}
	Release_all(futs...)
	assertEq(t, pkeys(t, alpha_pool(t)).Len(), n)
	assertEq(t, policy.Future_bank.Len(), 0)
}

func TestMemorizeThroughAlpha_bank_growth_without_release(t *testing.T) {
	policy := Fresh_policy(t)
	n := N_scaled(2_000, 200)
	before := policy.Future_bank.Len()
	futs := make([]any, n)
	for i := range futs {
		futs[i] = must(laila.Memorize(must(laila.Constant(i))))
	}
	for _, f := range futs {
		must(Wait(f, 60))
	}
	assertEq(t, policy.Future_bank.Len()-before, n)
	Release_all(futs...)
	assertEq(t, policy.Future_bank.Len(), before)
}

func TestMemorizeThroughAlpha_guarantee_scope_over_many_futures(t *testing.T) {
	Fresh_policy(t)
	n := N_scaled(3_000, 200)
	es, _ := constants(t, n, func(i int) any { return i })
	futs := make([]any, 0, n)
	check(t, laila.Guarantee.With(func() error {
		for _, e := range es {
			f, err := laila.Memorize(e)
			if err != nil {
				return err
			}
			futs = append(futs, f)
		}
		return nil
	}))
	for _, f := range futs {
		assertTrue(t, f.(interface{ Finished() bool }).Finished())
	}
	Release_all(futs...)
	assertEq(t, pkeys(t, alpha_pool(t)).Len(), n)
}

func TestMemorizeThroughAlpha_large_payload_entries(t *testing.T) {
	Fresh_policy(t)
	sizes := []int{1, 8, 32}
	var es []*laila.Entry
	ids := py.NewList()
	for _, mb := range sizes {
		e := must(laila.Constant(py.DictOf("blob", Rand_bytes(mb<<20, mb))))
		es = append(es, e)
		ids.Append(e.Global_id())
	}
	must(Wait(must(laila.Memorize(entries_list(es))), 120))
	got := must(Wait(must(laila.Remember(ids)), 120)).(*py.List)
	for i, e := range es {
		want := must(must(e.Data()).(*py.Dict).Getitem("blob"))
		assertEq(t, must(Unwrap(must(got.Get(i))).(*py.Dict).Getitem("blob")), want)
	}
}

func TestMemorizeThroughAlpha_remember_latest_evolution_after_many_writes(t *testing.T) {
	Fresh_policy(t)
	v := must(laila.Variable(py.Kw{"data": py.DictOf("v", 0)}))
	n := N_scaled(500, 50)
	for i := 1; i <= n; i++ {
		check(t, v.Set_data(py.DictOf("v", i)))
		must(Wait(must(laila.Memorize(v)), 30))
	}
	base := strings.SplitN(v.Global_id(), "@", 2)[0]
	got := must(Wait(must(laila.Remember(base+"@evolution=-1")), 30))
	assertEq(t, Unwrap(got), py.DictOf("v", n))
	got0 := must(Wait(must(laila.Remember(base+"@evolution=1")), 30))
	assertEq(t, Unwrap(got0), py.DictOf("v", 1))
}

func TestMemorizeThroughAlpha_concurrent_memorize_from_threads(t *testing.T) {
	Fresh_policy(t)
	n_threads, per := 8, N_scaled(300, 30)
	check(t, Go_threads(func(i int) error {
		futs := make([]any, 0, per)
		for j := 0; j < per; j++ {
			e, err := laila.Constant(py.DictOf("t", i, "j", j))
			if err != nil {
				return err
			}
			f, err := laila.Memorize(e)
			if err != nil {
				return err
			}
			futs = append(futs, f)
		}
		for _, f := range futs {
			if _, err := Wait(f, 60); err != nil {
				return err
			}
		}
		Release_all(futs...)
		return nil
	}, n_threads))
	assertEq(t, pkeys(t, alpha_pool(t)).Len(), n_threads*per)
}

// F-01/F-02 (fixed): memorize into a mem pool stores a deep snapshot.
func TestMemorizeThroughAlpha_constant_is_snapshotted_at_memorize(t *testing.T) {
	Fresh_policy(t)
	e := must(laila.Constant(py.DictOf("x", py.NewList(1, 2, 3))))
	must(Wait(must(laila.Memorize(e)), 30))
	must(must(e.Data()).(*py.Dict).Getitem("x")).(*py.List).Append(99)
	got := must(Wait(must(laila.Remember(e.Global_id())), 30))
	assertEq(t, Unwrap(got), py.DictOf("x", py.NewList(1, 2, 3)))
}

// F-01/F-02 (fixed): memorize into a mem pool stores a deep snapshot.
func TestMemorizeThroughAlpha_variable_snapshot_is_deep(t *testing.T) {
	Fresh_policy(t)
	v := must(laila.Variable(py.Kw{"data": py.DictOf("x", py.NewList(1, 2, 3))}))
	must(Wait(must(laila.Memorize(v)), 30))
	must(must(v.Data()).(*py.Dict).Getitem("x")).(*py.List).Append(99)
	got := must(Wait(must(laila.Remember(v.Global_id())), 30))
	assertEq(t, Unwrap(got), py.DictOf("x", py.NewList(1, 2, 3)))
}

func TestMemorizeThroughAlpha_big_dict_payloads(t *testing.T) {
	Fresh_policy(t)
	n_keys := N_scaled(200_000, 10_000)
	e := must(laila.Constant(Big_dict(n_keys)))
	must(Wait(must(laila.Memorize(e)), 120))
	got := must(Wait(must(laila.Remember(e.Global_id())), 120))
	assertEq(t, Unwrap(got).(*py.Dict).Len(), n_keys)
}

// ---------------------------------------------------------------------------
// MultiBuffer ring
// ---------------------------------------------------------------------------

func TestMultiBufferRing_producer_consumer_in_lockstep(t *testing.T) {
	Setup(t)
	cap_ := 64
	mb := must(laila.New_MultiBuffer(py.Kw{"capacity": cap_}))
	n := N_scaled(50_000, 2_000)
	seen := py.NewList()
	for i := 0; i < n; i++ {
		must(mb.Write(py.DictOf("i", i)))
		seen.Append(must(Unwrap(must(mb.Read())).(*py.Dict).Getitem("i")))
	}
	assertEq(t, seen, Range_list(n))
}

func TestMultiBufferRing_writer_lapping_reader_overwrites_silently(t *testing.T) {
	Setup(t)
	mb := must(laila.New_MultiBuffer(py.Kw{"capacity": 8}))
	for i := 0; i < 20; i++ {
		must(mb.Write(py.DictOf("i", i)))
	}
	first := Unwrap(must(mb.Read())).(*py.Dict)
	// ring of 8: slots hold 12..19; read_head starts at 0 -> slot 0 holds 16
	assertTrue(t, must(py.Int(must(first.Getitem("i")))) >= 12)
}

func TestMultiBufferRing_threaded_producer_consumer(t *testing.T) {
	Setup(t)
	cap_ := 256
	mb := must(laila.New_MultiBuffer(py.Kw{"capacity": cap_}))
	n := N_scaled(20_000, 1_000)
	got := py.NewList()
	var lock sync.Mutex
	got_len := func() int { lock.Lock(); defer lock.Unlock(); return got.Len() }

	producer := func() error {
		for j := 0; j < n; j++ {
			lock.Lock()
			_, err := mb.Write(py.DictOf("j", j))
			lock.Unlock()
			if err != nil {
				return err
			}
			for {
				lock.Lock()
				pending := ((mb.Write_head() - mb.Read_head()) % cap_ + cap_) % cap_
				lock.Unlock()
				if pending < cap_-1 {
					break
				}
				time.Sleep(0)
			}
		}
		return nil
	}
	consumer := func() error {
		for got_len() < n {
			lock.Lock()
			if mb.Write_head() != mb.Read_head() {
				v, err := mb.Read()
				if err != nil {
					lock.Unlock()
					return err
				}
				got.Append(must(Unwrap(v).(*py.Dict).Getitem("j")))
				lock.Unlock()
				continue
			}
			lock.Unlock()
			time.Sleep(0)
		}
		return nil
	}
	check(t, Go_threads(func(i int) error {
		if i == 0 {
			return producer()
		}
		return consumer()
	}, 2))
	assertEq(t, got, Range_list(n))
}

func TestMultiBufferRing_mapped_slots_shared_by_reference(t *testing.T) {
	Setup(t)
	slots := py.NewList()
	for i := 0; i < 16; i++ {
		slots.Append(nil)
	}
	mb := must(laila.New_MultiBuffer(py.Kw{"slots": slots, "mapped": true}))
	assertEq(t, mb.Capacity, 16)
	for i := 0; i < 16; i++ {
		check(t, slots.Set(i, py.DictOf("raw", i)))
	}
	for i := 0; i < 16; i++ {
		assertEq(t, Unwrap(must(mb.Getitem(i))), py.DictOf("raw", i))
	}
}

func TestMultiBufferRing_empty_resets_heads_after_heavy_use(t *testing.T) {
	Setup(t)
	mb := must(laila.New_MultiBuffer(py.Kw{"capacity": 32}))
	for i := 0; i < N_scaled(10_000, 500); i++ {
		must(mb.Write(i))
	}
	check(t, mb.Empty())
	assertTrue(t, mb.Read_head() == 0 && mb.Write_head() == 0)
}
