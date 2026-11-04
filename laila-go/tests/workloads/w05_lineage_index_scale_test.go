//go:build workload

// Port of tests/workloads/test_w05_lineage_index_scale.py: variable
// lineage and the pool index at scale: thousands of evolutions of one
// variable, hundreds of variables, negative / timestamp lookups, index
// self-repair after external deletes, separate ``index_pool``, lineage
// split across proxy tiers, concurrent evolution of one variable, and
// ``evolve()`` branches.
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

// lineage_ is ``_lineage(v, n, nickname=None, start=1)``: memorize *n*
// successive payloads of variable *v*; return the base id.
func lineage_(t *testing.T, v *laila.Entry, n int, nickname string, start ...int) string {
	t.Helper()
	s := 1
	if len(start) > 0 {
		s = start[0]
	}
	for i := s; i < s+n; i++ {
		check(t, v.Set_data(py.DictOf("v", i)))
		var f any
		if nickname != "" {
			f = must(laila.Memorize(v, py.Kw{"pool_nickname": nickname}))
		} else {
			f = must(laila.Memorize(v))
		}
		must(Wait(f, 60))
		Release_all(f)
	}
	return strings.SplitN(v.Global_id(), "@", 2)[0]
}

// rem_ is ``_rem(ref, nickname=None)``.
func rem_(ref string, nickname ...string) (any, error) {
	kw := py.Kw{"persist": false}
	if len(nickname) > 0 && nickname[0] != "" {
		kw["pool_nickname"] = nickname[0]
	}
	f, err := laila.Remember(ref, kw)
	if err != nil {
		return nil, err
	}
	out, err := Wait(f, 120)
	if err != nil {
		return nil, err
	}
	Release_all(f)
	return Unwrap(out), nil
}

func rem(t *testing.T, ref string, nickname ...string) any {
	t.Helper()
	return must(rem_(ref, nickname...))
}

// keys_with is ``[k for k in pool.keys() if base in k]``.
func keys_with(t *testing.T, p *Pool, base string) []string {
	t.Helper()
	out := []string{}
	for _, k := range pkeys(t, p).Items() {
		if strings.Contains(py.Str(k), base) {
			out = append(out, py.Str(k))
		}
	}
	return out
}

func variable0(t *testing.T) *laila.Entry {
	t.Helper()
	return must(laila.Variable(py.Kw{"data": py.DictOf("v", 0)}))
}

// ---------------------------------------------------------------------------
// single variable, deep lineage
// ---------------------------------------------------------------------------

func TestSingleVariableDeepLineage_thousands_of_evolutions(t *testing.T) {
	Fresh_policy(t)
	v := variable0(t)
	n := N_scaled(1_500, 200)
	var base string
	sw := Timed(func() { base = lineage_(t, v, n, "") })
	assertTrue(t, sw.Elapsed/float64(n) < 0.1, fmt.Sprintf("%.1f ms per evolution", sw.Elapsed/float64(n)*1000))
	assertEq(t, rem(t, base+"@evolution=-1"), py.DictOf("v", n))
	assertEq(t, rem(t, base+"@evolution=1"), py.DictOf("v", 1))
	assertEq(t, rem(t, fmt.Sprintf("%s@evolution=%d", base, n/2)), py.DictOf("v", n/2))
	for _, k := range []int{2, 7, 100} {
		assertEq(t, rem(t, fmt.Sprintf("%s@evolution=-%d", base, k)), py.DictOf("v", n-k+1))
	}
	assertEq(t, rem(t, base), py.DictOf("v", n))
}

func TestSingleVariableDeepLineage_latest_lookup_is_index_fast(t *testing.T) {
	Fresh_policy(t)
	v := variable0(t)
	n := N_scaled(800, 100)
	base := lineage_(t, v, n, "")
	idx := Timed(func() {
		for i := 0; i < 20; i++ {
			assertEq(t, rem(t, base+"@evolution=-1"), py.DictOf("v", n))
		}
	})
	alpha := alpha_pool(t)
	alpha.Index().Invalidate()
	alpha.Index_enabled = false
	var scan *Stopwatch
	func() {
		defer func() { alpha.Index_enabled = true }()
		scan = Timed(func() {
			for i := 0; i < 20; i++ {
				assertEq(t, rem(t, base+"@evolution=-1"), py.DictOf("v", n))
			}
		})
	}()
	// both must be correct; index path should not be slower than the scan
	assertTrue(t, idx.Elapsed <= scan.Elapsed*1.5+0.5)
}

func TestSingleVariableDeepLineage_negative_out_of_range_raises(t *testing.T) {
	Fresh_policy(t)
	v := variable0(t)
	base := lineage_(t, v, 10, "")
	_, err := rem_(base + "@evolution=-50")
	if !errors.Is(err, py.KeyError) {
		t.Fatalf("expected KeyError, got %v", err)
	}
	_, err = rem_(base + "@evolution=999")
	assertTrue(t, err != nil, "expected an error")
}

func TestSingleVariableDeepLineage_unsupported_attribute_raises_valueerror(t *testing.T) {
	Fresh_policy(t)
	v := variable0(t)
	base := lineage_(t, v, 3, "")
	_, err := rem_(base + "@colour=red")
	if !errors.Is(err, py.ValueError) {
		t.Fatalf("expected ValueError, got %v", err)
	}
}

func TestSingleVariableDeepLineage_forget_latest_shifts_minus_one(t *testing.T) {
	Fresh_policy(t)
	v := variable0(t)
	n := N_scaled(300, 50)
	base := lineage_(t, v, n, "")
	for k := n; k > n-20; k-- {
		must(Wait(must(laila.Forget(fmt.Sprintf("%s@evolution=%d", base, k))), 30))
		assertEq(t, rem(t, base+"@evolution=-1"), py.DictOf("v", k-1))
	}
	assertEq(t, strp(alpha_pool(t).Index().Latest(base)), fmt.Sprintf("%s@evolution=%d", base, n-20))
}

func TestSingleVariableDeepLineage_forget_entire_lineage_clears_shard(t *testing.T) {
	Fresh_policy(t)
	v := variable0(t)
	n := N_scaled(200, 50)
	base := lineage_(t, v, n, "")
	ids := py.NewList()
	for k := 1; k <= n; k++ {
		ids.Append(fmt.Sprintf("%s@evolution=%d", base, k))
	}
	must(Wait(must(laila.Forget(ids)), 120))
	assertEq(t, keys_with(t, alpha_pool(t), base), []string{})
	assertTrue(t, alpha_pool(t).Index().Latest(base) == nil)
	_, err := rem_(base + "@evolution=-1")
	if !errors.Is(err, py.KeyError) {
		t.Fatalf("expected KeyError, got %v", err)
	}
}

func TestSingleVariableDeepLineage_idempotent_rememorize_without_change_keeps_evolution(t *testing.T) {
	Fresh_policy(t)
	v := must(laila.Variable(py.Kw{"data": py.DictOf("v", 1)}))
	for i := 0; i < N_scaled(300, 50); i++ {
		must(Wait(must(laila.Memorize(v)), 30))
	}
	base := strings.SplitN(v.Global_id(), "@", 2)[0]
	assertEq(t, len(keys_with(t, alpha_pool(t), base)), 1)
}

func TestSingleVariableDeepLineage_timestamp_lookup_at_scale(t *testing.T) {
	Fresh_policy(t)
	v := variable0(t)
	n := N_scaled(300, 50)
	stamps := map[int]string{}
	for i := 1; i <= n; i++ {
		// Stamps have millisecond resolution; two evolutions created in
		// the same millisecond legitimately share a stamp (the highest
		// wins), so keep every stamp distinct for an exact check.
		for *v.Creation_timestamp() == laila.Now_creation_timestamp_() {
			time.Sleep(500 * time.Microsecond)
		}
		check(t, v.Set_data(py.DictOf("v", i)))
		must(Wait(must(laila.Memorize(v)), 30))
		stamps[i] = *v.Creation_timestamp()
	}
	uniq := map[string]struct{}{}
	for _, s := range stamps {
		uniq[s] = struct{}{}
	}
	assertEq(t, len(uniq), n)
	base := strings.SplitN(v.Global_id(), "@", 2)[0]
	for _, i := range []int{1, n / 3, n / 2, n} {
		assertEq(t, rem(t, fmt.Sprintf("%s@creation_timestamp=%s", base, stamps[i])), py.DictOf("v", i))
	}
	_, err := rem_(base + "@creation_timestamp=1999-01-01T00:00:00+00:00")
	if !errors.Is(err, py.KeyError) {
		t.Fatalf("expected KeyError, got %v", err)
	}
}

func TestSingleVariableDeepLineage_evolve_branches_do_not_collide(t *testing.T) {
	Fresh_policy(t)
	v := variable0(t)
	must(Wait(must(laila.Memorize(v)), 30))
	cur := v
	n := N_scaled(300, 50)
	for i := 1; i <= n; i++ {
		cur = must(cur.Evolve(py.DictOf("v", i)))
		must(Wait(must(laila.Memorize(cur)), 30))
	}
	base := strings.SplitN(v.Global_id(), "@", 2)[0]
	assertEq(t, rem(t, base+"@evolution=-1"), py.DictOf("v", n))
	assertEq(t, len(keys_with(t, alpha_pool(t), base)), n+1)
}

// ---------------------------------------------------------------------------
// index repair
// ---------------------------------------------------------------------------

func TestIndexRepair_external_deletes_bypassing_index_are_repaired(t *testing.T) {
	Fresh_policy(t)
	v := variable0(t)
	n := N_scaled(300, 60)
	base := lineage_(t, v, n, "")
	pool := alpha_pool(t)
	// bypass the index: raw storage delete of the newest 30 evolutions
	for k := n; k > n-30; k-- {
		check(t, pool.Delete_(fmt.Sprintf("%s@evolution=%d", base, k)))
	}
	assertEq(t, strp(pool.Index().Latest(base)), fmt.Sprintf("%s@evolution=%d", base, n)) // stale
	assertEq(t, rem(t, base+"@evolution=-1"), py.DictOf("v", n-30))
	assertEq(t, strp(pool.Index().Latest(base)), fmt.Sprintf("%s@evolution=%d", base, n-30)) // repaired
}

func TestIndexRepair_more_than_64_stale_entries_fall_back_to_scan(t *testing.T) {
	// F-10 (fixed): self-repair is bounded by the shard size, not a fixed
	// retry budget, and a fully stale index falls back to a key scan.
	Fresh_policy(t)
	v := variable0(t)
	n := N_scaled(200, 120)
	base := lineage_(t, v, n, "")
	pool := alpha_pool(t)
	for k := n; k > n-100; k-- {
		check(t, pool.Delete_(fmt.Sprintf("%s@evolution=%d", base, k)))
	}
	assertEq(t, rem(t, base+"@evolution=-1"), py.DictOf("v", n-100))
}

func TestIndexRepair_external_writes_bypassing_index_are_found_by_scan(t *testing.T) {
	Fresh_policy(t)
	v := variable0(t)
	base := lineage_(t, v, 5, "")
	pool := alpha_pool(t)
	rec := pget(t, pool, base+"@evolution=5")
	check(t, pool.Write_(base+"@evolution=50", rec)) // raw write, index unaware
	// index says 5; scan should still find 50 as the newest only if the
	// index misses -- document the actual contract: index answer wins
	got := strp(pool.Index().Latest(base))
	assertTrue(t, got == base+"@evolution=5" || got == base+"@evolution=50")
}

func TestIndexRepair_invalidate_and_rebuild_at_scale(t *testing.T) {
	Fresh_policy(t)
	nv := N_scaled(200, 40)
	vs := make([]*laila.Entry, nv)
	for i := range vs {
		vs[i] = variable0(t)
		lineage_(t, vs[i], 5, "")
	}
	pool := alpha_pool(t)
	pool.Index().Invalidate()
	sw := Timed(func() { pool.Index().Rebuild() })
	assertTrue(t, sw.Elapsed < 120)
	for i := 0; i < nv; i += 13 {
		base := strings.SplitN(vs[i].Global_id(), "@", 2)[0]
		assertEq(t, strp(pool.Index().Latest(base)), base+"@evolution=5")
	}
}

func has_index_key(t *testing.T, p *Pool) bool {
	t.Helper()
	return any_index_key(pkeys(t, p, py.Kw{"include_index": true}))
}

func TestIndexRepair_separate_index_pool(t *testing.T) {
	policy := Fresh_policy(t)
	idx := no_index(t)
	data := default_pool(t, py.Kw{"index_pool": idx.Self()})
	mem_extend(t, policy, data.Self(), "data")
	v := variable0(t)
	n := N_scaled(200, 40)
	base := lineage_(t, v, n, "data")
	assertTrue(t, !has_index_key(t, data))
	assertTrue(t, has_index_key(t, idx))
	assertEq(t, rem(t, base+"@evolution=-1", "data"), py.DictOf("v", n))
	check(t, data.Empty())
	assertEq(t, pkeys(t, idx, py.Kw{"include_index": true}), py.NewList())
}

func TestIndexRepair_separate_index_pool_persistent(t *testing.T) {
	policy := Fresh_policy(t)
	dir := Disk_tmp(t)
	idx_raw := must(Make_pool("sqlite", dir, py.Kw{"index_enabled": false}))
	data_raw := must(Make_pool("sqlite", dir, py.Kw{"index_pool": idx_raw}))
	defer Close_pool(idx_raw)
	defer Close_pool(data_raw)
	mem_extend(t, policy, data_raw, "data")
	v := variable0(t)
	n := N_scaled(60, 20)
	base := lineage_(t, v, n, "data")
	assertTrue(t, has_index_key(t, laila.As_pool_(idx_raw)))
	assertEq(t, rem(t, base+"@evolution=-1", "data"), py.DictOf("v", n))
}

// ---------------------------------------------------------------------------
// many variables
// ---------------------------------------------------------------------------

func TestManyVariables_hundreds_of_variables_latest_each(t *testing.T) {
	Fresh_policy(t)
	nv, ne := N_scaled(300, 50), 10
	vs := make([]*laila.Entry, nv)
	for i := range vs {
		vs[i] = variable0(t)
	}
	w := Timed(func() {
		for _, v := range vs {
			lineage_(t, v, ne, "")
		}
	})
	r := Timed(func() {
		for _, v := range vs {
			base := strings.SplitN(v.Global_id(), "@", 2)[0]
			assertEq(t, rem(t, base+"@evolution=-1"), py.DictOf("v", ne))
		}
	})
	assertTrue(t, w.Elapsed/float64(nv*ne) < 0.1 && r.Elapsed/float64(nv) < 0.1)
}

func TestManyVariables_batch_remember_of_latest_refs(t *testing.T) {
	Fresh_policy(t)
	nv := N_scaled(200, 40)
	refs := py.NewList()
	for i := 0; i < nv; i++ {
		v := variable0(t)
		lineage_(t, v, 4, "")
		refs.Append(strings.SplitN(v.Global_id(), "@", 2)[0] + "@evolution=-1")
	}
	got := must(Wait(must(laila.Remember(refs, py.Kw{"persist": false})), 300)).(*py.List)
	assertEq(t, got.Len(), nv)
	for _, g := range got.Items() {
		assertEq(t, Unwrap(g), py.DictOf("v", 4))
	}
}

func TestManyVariables_batch_mixed_constants_and_variable_refs(t *testing.T) {
	Fresh_policy(t)
	cs, cids := constants(t, N_scaled(200, 40), func(i int) any { return py.DictOf("c", i) })
	must(Wait(must(laila.Memorize(entries_list(cs))), 60))
	nv := N_scaled(50, 10)
	refs := py.NewList(cids.Items()...)
	for i := 0; i < nv; i++ {
		v := variable0(t)
		lineage_(t, v, 3, "")
		refs.Append(strings.SplitN(v.Global_id(), "@", 2)[0] + "@evolution=-2")
	}
	got := must(Wait(must(laila.Remember(refs, py.Kw{"persist": false})), 300)).(*py.List)
	for i := 0; i < len(cs); i++ {
		assertEq(t, Unwrap(must(got.Get(i))), py.DictOf("c", i))
	}
	for i := len(cs); i < got.Len(); i++ {
		assertEq(t, Unwrap(must(got.Get(i))), py.DictOf("v", 2))
	}
}

func TestManyVariables_lineage_on_persistent_backend(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		policy := Fresh_policy(t)
		_, raw := with_pool(t, kind)
		mem_extend(t, policy, raw, "p")
		v := variable0(t)
		n := N_scaled(120, 30)
		base := lineage_(t, v, n, "p")
		sw := Timed(func() {
			for i := 0; i < 10; i++ {
				assertEq(t, rem(t, base+"@evolution=-1", "p"), py.DictOf("v", n))
			}
		})
		assertTrue(t, sw.Elapsed/10 < 1.0)
		assertEq(t, rem(t, fmt.Sprintf("%s@evolution=-%d", base, n), "p"), py.DictOf("v", 1))
	})
}

// ---------------------------------------------------------------------------
// lineage across tiers
// ---------------------------------------------------------------------------

func TestLineageAcrossTiers_evolutions_split_between_cache_and_origin(t *testing.T) {
	policy := Fresh_policy(t)
	origin, cache := default_pool(t), default_pool(t)
	lshift(t, cache, origin)
	mem_extend(t, policy, origin.Self(), "origin")
	mem_extend(t, policy, cache.Self(), "cache")
	v := variable0(t)
	base := lineage_(t, v, 10, "origin")
	lineage_(t, v, 10, "cache", 11) // newer ones only in cache
	assertEq(t, rem(t, base+"@evolution=-1", "cache"), py.DictOf("v", 20))
	assertEq(t, rem(t, base+"@evolution=-1", "origin"), py.DictOf("v", 10))
	assertEq(t, rem(t, base+"@evolution=3", "cache"), py.DictOf("v", 3)) // falls through
}

func TestLineageAcrossTiers_newer_in_origin_than_cache(t *testing.T) {
	for _, ref := range []string{"{base}", "{base}@evolution=-1"} {
		ref := ref
		t.Run("ref="+ref, func(t *testing.T) {
			// F-11 (fixed): lineage queries rank the union of every tier's candidates.
			policy := Fresh_policy(t)
			origin, cache := default_pool(t), default_pool(t)
			lshift(t, cache, origin)
			mem_extend(t, policy, origin.Self(), "origin")
			mem_extend(t, policy, cache.Self(), "cache")
			v := variable0(t)
			base := lineage_(t, v, 5, "cache")
			lineage_(t, v, 5, "origin", 6)
			assertEq(t, rem(t, strings.ReplaceAll(ref, "{base}", base), "cache"), py.DictOf("v", 10))
		})
	}
}

func TestLineageAcrossTiers_newer_in_origin_than_cache_without_index(t *testing.T) {
	// Same scenario with indexes off: the scan path spans the chain.
	policy := Fresh_policy(t)
	origin, cache := no_index(t), no_index(t)
	lshift(t, cache, origin)
	mem_extend(t, policy, origin.Self(), "origin")
	mem_extend(t, policy, cache.Self(), "cache")
	v := variable0(t)
	base := lineage_(t, v, 5, "cache")
	lineage_(t, v, 5, "origin", 6)
	assertEq(t, rem(t, base+"@evolution=-1", "cache"), py.DictOf("v", 10))
	assertEq(t, rem(t, base, "cache"), py.DictOf("v", 10))
}

func TestLineageAcrossTiers_three_tier_lineage(t *testing.T) {
	policy := Fresh_policy(t)
	s_raw := must(Make_pool("sqlite", Disk_tmp(t)))
	defer Close_pool(s_raw)
	s := laila.As_pool_(s_raw)
	m, c := default_pool(t), default_pool(t)
	// c << m << s
	lshift(t, c, m)
	lshift(t, m, s)
	mem_extend(t, policy, s_raw, "s")
	mem_extend(t, policy, m.Self(), "m")
	mem_extend(t, policy, c.Self(), "c")
	v := variable0(t)
	base := lineage_(t, v, 5, "s")
	lineage_(t, v, 5, "m", 6)
	lineage_(t, v, 5, "c", 11)
	assertEq(t, rem(t, base+"@evolution=-1", "c"), py.DictOf("v", 15))
	assertEq(t, rem(t, base+"@evolution=-15", "c"), py.DictOf("v", 1))
	_ = s
}

// ---------------------------------------------------------------------------
// concurrent evolution
// ---------------------------------------------------------------------------

func TestConcurrentEvolution_threads_evolving_one_variable_produce_unique_evolutions(t *testing.T) {
	Fresh_policy(t)
	v := variable0(t)
	must(Wait(must(laila.Memorize(v)), 30))
	n_threads, per := 8, N_scaled(40, 10)
	var lock sync.Mutex
	check(t, Go_threads(func(tt int) error {
		for j := 0; j < per; j++ {
			err := func() error {
				lock.Lock() // serialise mutate+memorize as the docs require for shared entries
				defer lock.Unlock()
				if err := v.Set_data(py.DictOf("t", tt, "j", j)); err != nil {
					return err
				}
				f, err := laila.Memorize(v)
				if err != nil {
					return err
				}
				_, err = Wait(f, 60)
				return err
			}()
			if err != nil {
				return err
			}
		}
		return nil
	}, n_threads))
	base := strings.SplitN(v.Global_id(), "@", 2)[0]
	assertEq(t, len(keys_with(t, alpha_pool(t), base)), n_threads*per+1)
	assertEq(t, *v.Evolution(), n_threads*per)
}

func TestConcurrentEvolution_unsynchronised_threads_evolving_one_variable(t *testing.T) {
	// Without external locking the data/evolution pairing may interleave,
	// but the pool must stay consistent: no exceptions, no duplicate keys,
	// evolution counter == number of successful bumps.
	Fresh_policy(t)
	v := variable0(t)
	must(Wait(must(laila.Memorize(v)), 30))
	n_threads, per := 8, N_scaled(40, 10)
	var mu sync.Mutex
	errs := []string{}
	check(t, Go_threads(func(tt int) error {
		for j := 0; j < per; j++ {
			err := v.Set_data(py.DictOf("t", tt, "j", j))
			if err == nil {
				var f any
				if f, err = laila.Memorize(v); err == nil {
					_, err = Wait(f, 60)
				}
			}
			if err != nil {
				mu.Lock()
				errs = append(errs, err.Error())
				mu.Unlock()
			}
		}
		return nil
	}, n_threads))
	assertEq(t, errs, []string{})
	base := strings.SplitN(v.Global_id(), "@", 2)[0]
	keys := keys_with(t, alpha_pool(t), base)
	uniq := map[string]struct{}{}
	for _, k := range keys {
		uniq[k] = struct{}{}
	}
	assertEq(t, len(keys), len(uniq))
	assertTrue(t, *v.Evolution() <= n_threads*per)
	assertEq(t, strp(alpha_pool(t).Index().Latest(base)), fmt.Sprintf("%s@evolution=%d", base, *v.Evolution()))
}

func TestConcurrentEvolution_concurrent_readers_of_latest_while_writer_evolves(t *testing.T) {
	Fresh_policy(t)
	v := variable0(t)
	base := lineage_(t, v, 1, "")
	stop := py.NewEvent()
	var mu sync.Mutex
	seen := []int{}
	errs := []string{}
	top := N_scaled(150, 30) + 1
	writer := func(tt int) error {
		defer stop.Set()
		for i := 2; i <= top; i++ {
			if err := v.Set_data(py.DictOf("v", i)); err != nil {
				return err
			}
			f, err := laila.Memorize(v)
			if err != nil {
				return err
			}
			if _, err := Wait(f, 60); err != nil {
				return err
			}
		}
		return nil
	}
	reader := func(tt int) error {
		for !stop.Is_set() {
			got, err := rem_(base + "@evolution=-1")
			mu.Lock()
			if err != nil {
				errs = append(errs, err.Error())
			} else {
				seen = append(seen, must(py.Int(must(got.(*py.Dict).Getitem("v")))))
			}
			mu.Unlock()
		}
		return nil
	}
	check(t, Go_threads(func(tt int) error {
		if tt == 0 {
			return writer(tt)
		}
		return reader(tt)
	}, 4))
	assertEq(t, errs, []string{})
	mx := 0
	for _, s := range seen {
		if s > mx {
			mx = s
		}
		// every value must be one that was actually written
		assertTrue(t, 1 <= s && s <= top)
	}
	assertTrue(t, len(seen) > 0 && mx == top)
}

func TestConcurrentEvolution_release_keeps_bank_flat_over_long_lineage(t *testing.T) {
	policy := Fresh_policy(t)
	v := variable0(t)
	before := policy.Future_bank.Len()
	for i := 1; i <= N_scaled(500, 100); i++ {
		check(t, v.Set_data(py.DictOf("v", i)))
		f := must(laila.Memorize(v))
		must(Wait(f, 30))
		Release_all(f)
	}
	assertEq(t, policy.Future_bank.Len(), before)
}

func TestConcurrentEvolution_group_remember_release(t *testing.T) {
	policy := Fresh_policy(t)
	cs, ids := constants(t, N_scaled(500, 100), func(i int) any { return i })
	f := must(laila.Memorize(entries_list(cs)))
	must(Wait(f, 60))
	g := must(laila.Remember(ids, py.Kw{"persist": false}))
	must(Wait(g, 60))
	Release_all(f, g)
	assertEq(t, policy.Future_bank.Len(), 0)
}
