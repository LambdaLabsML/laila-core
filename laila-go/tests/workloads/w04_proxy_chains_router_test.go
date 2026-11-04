//go:build workload

// Port of tests/workloads/test_w04_proxy_chains_router.py: proxy chains
// (``cache << origin``) and the pool router under load: tiered
// fall-through and write-back at volume, staleness, cycles, closed
// origins, attribute lookups through a cache, bulk duplication, and
// hundreds of registered pools.
package workloads

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// no_index is ``DefaultPool(index_enabled=False)``.
func no_index(t *testing.T) *Pool {
	t.Helper()
	return default_pool(t, py.Kw{"index_enabled": false})
}

// lshift is ``front << back``.
func lshift(t *testing.T, front, back *Pool) {
	t.Helper()
	must(front.Lshift(back.Self()))
}

// ---------------------------------------------------------------------------
// proxy chains
// ---------------------------------------------------------------------------

func TestProxyChains_two_tier_fall_through_and_write_back(t *testing.T) {
	for _, origin_kind := range ALL_KINDS {
		origin_kind := origin_kind
		t.Run("origin_kind="+origin_kind, func(t *testing.T) {
			origin_raw := must(Make_pool(origin_kind, Disk_tmp(t), py.Kw{"index_enabled": false}))
			defer Close_pool(origin_raw)
			origin := laila.As_pool_(origin_raw)
			cache := no_index(t)
			lshift(t, cache, origin)
			n := N_scaled(600, 100)
			for i := 0; i < n; i++ {
				pset(t, origin, fmt.Sprintf("k%d", i), py.DictOf("i", i))
			}
			assertEq(t, pkeys(t, cache), py.NewList())
			cold := Timed(func() {
				for i := 0; i < n; i++ {
					assertEq(t, pget(t, cache, fmt.Sprintf("k%d", i)), py.DictOf("i", i))
				}
			})
			assertEq(t, pkeys(t, cache).Len(), n)
			warm := Timed(func() {
				for i := 0; i < n; i++ {
					assertEq(t, pget(t, cache, fmt.Sprintf("k%d", i)), py.DictOf("i", i))
				}
			})
			if origin_kind != "mem" {
				assertTrue(t, warm.Elapsed < cold.Elapsed)
			}
		})
	}
}

func TestProxyChains_three_tier_chain_populates_every_tier(t *testing.T) {
	dir := Disk_tmp(t)
	s_raw := must(Make_pool("sqlite", dir, py.Kw{"index_enabled": false}))
	defer Close_pool(s_raw)
	var h_raw any
	if contains_str(PERSISTENT_KINDS, "hdf5") {
		h_raw = must(Make_pool("hdf5", dir, py.Kw{"index_enabled": false}))
	} else {
		h_raw = must(laila.New_DefaultPool(py.Kw{"index_enabled": false}))
	}
	defer Close_pool(h_raw)
	s, h := laila.As_pool_(s_raw), laila.As_pool_(h_raw)
	m := no_index(t)
	// m << h << s
	lshift(t, m, h)
	lshift(t, h, s)
	n := N_scaled(200, 50)
	for i := 0; i < n; i++ {
		pset(t, s, fmt.Sprintf("k%d", i), py.DictOf("i", i))
	}
	for i := 0; i < n; i++ {
		assertEq(t, pget(t, m, fmt.Sprintf("k%d", i)), py.DictOf("i", i))
	}
	assertEq(t, pkeys(t, h).Len(), n)
	assertEq(t, pkeys(t, m).Len(), n)
}

func TestProxyChains_rshift_builds_same_chain(t *testing.T) {
	s, h, m := no_index(t), no_index(t), no_index(t)
	// s >> h >> m
	must(s.Rshift(h.Self()))
	must(h.Rshift(m.Self()))
	pset(t, s, "k", py.DictOf("v", 1))
	assertEq(t, pget(t, m, "k"), py.DictOf("v", 1))
	assertEq(t, pget(t, h, "k"), py.DictOf("v", 1))
}

func TestProxyChains_miss_everywhere_returns_none_and_writes_nothing(t *testing.T) {
	a, b, c := no_index(t), no_index(t), no_index(t)
	lshift(t, a, b)
	lshift(t, b, c)
	for i := 0; i < N_scaled(2_000, 200); i++ {
		assertEq(t, pget(t, a, fmt.Sprintf("missing%d", i)), nil)
	}
	assertTrue(t, pkeys(t, a).Len() == 0 && pkeys(t, b).Len() == 0 && pkeys(t, c).Len() == 0)
}

func TestProxyChains_stale_cache_is_not_invalidated_by_origin_update(t *testing.T) {
	origin, cache := no_index(t), no_index(t)
	lshift(t, cache, origin)
	pset(t, origin, "k", py.DictOf("v", 1))
	assertEq(t, pget(t, cache, "k"), py.DictOf("v", 1))
	pset(t, origin, "k", py.DictOf("v", 2))
	assertEq(t, pget(t, cache, "k"), py.DictOf("v", 1)) // documented: write-through only, no invalidation
	pdel(t, cache, "k")
	assertEq(t, pget(t, cache, "k"), py.DictOf("v", 2))
}

func TestProxyChains_delete_in_cache_refetches_from_origin(t *testing.T) {
	origin, cache := no_index(t), no_index(t)
	lshift(t, cache, origin)
	n := N_scaled(1_000, 100)
	for i := 0; i < n; i++ {
		pset(t, origin, fmt.Sprintf("k%d", i), py.DictOf("i", i))
		pget(t, cache, fmt.Sprintf("k%d", i))
	}
	for i := 0; i < n; i++ {
		pdel(t, cache, fmt.Sprintf("k%d", i))
	}
	assertEq(t, pkeys(t, cache), py.NewList())
	for i := 0; i < n; i++ {
		assertEq(t, pget(t, cache, fmt.Sprintf("k%d", i)), py.DictOf("i", i))
	}
}

func TestProxyChains_empty_cache_does_not_touch_origin(t *testing.T) {
	origin, cache := default_pool(t), default_pool(t)
	lshift(t, cache, origin)
	for i := 0; i < 200; i++ {
		pset(t, origin, base_(i)+"@evolution=0", py.DictOf("i", i))
		pget(t, cache, base_(i)+"@evolution=0")
	}
	check(t, cache.Empty())
	assertEq(t, pkeys(t, cache), py.NewList())
	assertEq(t, pkeys(t, origin).Len(), 200)
}

func TestProxyChains_deep_chain_fall_through(t *testing.T) {
	for _, depth := range []int{10, 50} {
		depth := depth
		t.Run(fmt.Sprintf("depth=%d", depth), func(t *testing.T) {
			pools := make([]*Pool, depth)
			for i := range pools {
				pools[i] = no_index(t)
			}
			for i := 0; i+1 < len(pools); i++ {
				lshift(t, pools[i], pools[i+1])
			}
			n := N_scaled(300, 50)
			for i := 0; i < n; i++ {
				pset(t, pools[len(pools)-1], fmt.Sprintf("k%d", i), py.DictOf("i", i))
			}
			sw := Timed(func() {
				for i := 0; i < n; i++ {
					assertEq(t, pget(t, pools[0], fmt.Sprintf("k%d", i)), py.DictOf("i", i))
				}
			})
			for _, p := range pools {
				assertEq(t, pkeys(t, p).Len(), n)
			}
			assertTrue(t, sw.Elapsed < 120)
		})
	}
}

func TestProxyChains_proxy_cycle_is_rejected(t *testing.T) {
	// F-09 (fixed): wiring that would close a cycle raises ValueError.
	a, b := no_index(t), no_index(t)
	lshift(t, a, b)
	if _, err := b.Lshift(a.Self()); !errors.Is(err, py.ValueError) {
		t.Fatalf("expected ValueError, got %v", err)
	}
	// if wiring was (wrongly) accepted, at least a miss must not blow the stack
	pget(t, a, "missing")
	c := no_index(t)
	lshift(t, b, c)
	if err := c.Set_proxy_to(a.Self()); !errors.Is(err, py.ValueError) { // three-tier cycle
		t.Fatalf("expected ValueError, got %v", err)
	}
	assertEq(t, pget(t, c, "missing"), nil)
}

func TestProxyChains_self_proxy_is_rejected(t *testing.T) {
	a := no_index(t)
	if err := a.Set_proxy(a.Self()); !errors.Is(err, py.ValueError) {
		t.Fatalf("expected ValueError, got %v", err)
	}
	pget(t, a, "missing")
}

func TestProxyChains_concurrent_readers_through_chain(t *testing.T) {
	origin, cache := no_index(t), no_index(t)
	lshift(t, cache, origin)
	n := N_scaled(2_000, 200)
	for i := 0; i < n; i++ {
		pset(t, origin, fmt.Sprintf("k%d", i), py.DictOf("i", i))
	}
	var mu sync.Mutex
	bad := [][2]int{}
	check(t, Go_threads(func(tt int) error {
		for i := 0; i < n; i++ {
			v, err := cache.Getitem(fmt.Sprintf("k%d", i))
			if err != nil {
				return err
			}
			if !py.Eq(v, py.DictOf("i", i)) {
				mu.Lock()
				bad = append(bad, [2]int{tt, i})
				mu.Unlock()
			}
		}
		return nil
	}, 8))
	assertEq(t, bad, [][2]int{})
	assertEq(t, pkeys(t, cache).Len(), n)
}

func TestProxyChains_closed_origin_cached_keys_still_readable(t *testing.T) {
	origin_raw := must(Make_pool("sqlite", Disk_tmp(t), py.Kw{"index_enabled": false}))
	origin := laila.As_pool_(origin_raw)
	cache := no_index(t)
	lshift(t, cache, origin)
	for i := 0; i < 100; i++ {
		pset(t, origin, fmt.Sprintf("k%d", i), py.DictOf("i", i))
	}
	for i := 0; i < 50; i++ {
		pget(t, cache, fmt.Sprintf("k%d", i))
	}
	Close_pool(origin_raw)
	assertEq(t, pget(t, cache, "k0"), py.DictOf("i", 0))
	if _, err := cache.Getitem("k99"); !errors.Is(err, py.RuntimeError) {
		t.Fatalf("expected RuntimeError, got %v", err)
	}
}

func TestProxyChains_rewire_chain_at_runtime(t *testing.T) {
	o1, o2, cache := no_index(t), no_index(t), no_index(t)
	pset(t, o1, "a", py.DictOf("src", 1))
	pset(t, o2, "b", py.DictOf("src", 2))
	lshift(t, cache, o1)
	assertEq(t, pget(t, cache, "a"), py.DictOf("src", 1))
	assertEq(t, pget(t, cache, "b"), nil)
	check(t, cache.Set_proxy_to(o2.Self()))
	assertEq(t, pget(t, cache, "b"), py.DictOf("src", 2))
	assertEq(t, pget(t, cache, "a"), py.DictOf("src", 1)) // still cached
	check(t, cache.Set_proxy_to(nil))
	assertEq(t, pget(t, cache, "zzz"), nil)
}

// ---------------------------------------------------------------------------
// proxy chains through the policy
// ---------------------------------------------------------------------------

func TestProxyThroughPolicy_remember_exact_key_through_cache(t *testing.T) {
	policy := Fresh_policy(t)
	origin_raw := must(Make_pool("sqlite", Disk_tmp(t)))
	defer Close_pool(origin_raw)
	origin := laila.As_pool_(origin_raw)
	cache := default_pool(t)
	lshift(t, cache, origin)
	mem_extend(t, policy, origin_raw, "origin")
	mem_extend(t, policy, cache.Self(), "cache")
	n := N_scaled(300, 50)
	es, ids := constants(t, n, func(i int) any { return py.DictOf("i", i) })
	must(Wait(must(laila.Memorize(entries_list(es), py.Kw{"pool_nickname": "origin"})), 300))
	got := must(Wait(must(laila.Remember(ids, py.Kw{"pool_nickname": "cache", "persist": false})), 300)).(*py.List)
	for i, g := range got.Items() {
		assertEq(t, must(Unwrap(g).(*py.Dict).Getitem("i")), i)
	}
	for _, i := range ids.Items() {
		assertTrue(t, must(cache.Exists(py.Str(i))))
	}
}

func TestProxyThroughPolicy_remember_latest_evolution_through_cache(t *testing.T) {
	policy := Fresh_policy(t)
	origin, cache := default_pool(t), default_pool(t)
	lshift(t, cache, origin)
	mem_extend(t, policy, origin.Self(), "origin")
	mem_extend(t, policy, cache.Self(), "cache")
	v := must(laila.Variable(py.Kw{"data": py.DictOf("v", 0)}))
	for i := 1; i < 6; i++ {
		check(t, v.Set_data(py.DictOf("v", i)))
		must(Wait(must(laila.Memorize(v, py.Kw{"pool_nickname": "origin"})), 30))
	}
	base := strings.SplitN(v.Global_id(), "@", 2)[0]
	got := must(Wait(must(laila.Remember(base+"@evolution=-1", py.Kw{"pool_nickname": "cache", "persist": false})), 30))
	assertEq(t, Unwrap(got), py.DictOf("v", 5))
}

func TestProxyThroughPolicy_duplicate_pool_at_volume(t *testing.T) {
	policy := Fresh_policy(t)
	dst_raw := must(Make_pool("sqlite", Disk_tmp(t)))
	defer Close_pool(dst_raw)
	dst := laila.As_pool_(dst_raw)
	mem_extend(t, policy, dst_raw, "dst")
	n := N_scaled(400, 100)
	es, _ := constants(t, n, func(i int) any { return py.DictOf("i", i) })
	must(Wait(must(laila.Memorize(entries_list(es))), 120))
	sw := Timed(func() {
		must(Wait(must(dst.Le(must(laila.Alpha_pool()))), 900))
	})
	for _, e := range es {
		assertTrue(t, must(dst.Exists(e.Global_id())))
	}
	assertTrue(t, sw.Elapsed/float64(n) < 0.5)
}

func TestProxyThroughPolicy_duplicate_pool_by_nickname_and_gid(t *testing.T) {
	policy := Fresh_policy(t)
	a, b, c := default_pool(t), default_pool(t), default_pool(t)
	mem_extend(t, policy, a.Self(), "a")
	mem_extend(t, policy, b.Self(), "b")
	mem_extend(t, policy, c.Self(), "c")
	es, _ := constants(t, N_scaled(200, 50), func(i int) any { return py.DictOf("i", i) })
	must(Wait(must(laila.Memorize(entries_list(es), py.Kw{"pool_nickname": "a"})), 60))
	must(Wait(must(b.Le("a")), 300))
	must(Wait(must(c.Le(a.Global_id())), 300))
	assertTrue(t, pkeys(t, b).Len() == len(es) && pkeys(t, c).Len() == len(es))
}

func TestProxyThroughPolicy_duplicate_with_variables_preserves_lineage(t *testing.T) {
	policy := Fresh_policy(t)
	a, b := default_pool(t), default_pool(t)
	mem_extend(t, policy, a.Self(), "a")
	mem_extend(t, policy, b.Self(), "b")
	v := must(laila.Variable(py.Kw{"data": py.DictOf("v", 0)}))
	n := N_scaled(40, 10)
	for i := 1; i <= n; i++ {
		check(t, v.Set_data(py.DictOf("v", i)))
		must(Wait(must(laila.Memorize(v, py.Kw{"pool_nickname": "a"})), 30))
	}
	must(Wait(must(b.Le(a.Self())), 300))
	base := strings.SplitN(v.Global_id(), "@", 2)[0]
	count := 0
	for _, k := range pkeys(t, b).Items() {
		if strings.Contains(py.Str(k), base) {
			count++
		}
	}
	assertEq(t, count, n)
	got := must(Wait(must(laila.Remember(base+"@evolution=-1", py.Kw{"pool_nickname": "b", "persist": false})), 30))
	assertEq(t, Unwrap(got), py.DictOf("v", n))
}

func TestProxyThroughPolicy_duplicate_empty_pool_is_noop(t *testing.T) {
	policy := Fresh_policy(t)
	a, b := default_pool(t), default_pool(t)
	mem_extend(t, policy, a.Self(), "a")
	mem_extend(t, policy, b.Self(), "b")
	f := must(b.Le(a.Self()))
	must(Wait(f, 60))
	assertEq(t, pkeys(t, b), py.NewList())
}

func TestProxyThroughPolicy_duplicate_self_is_rejected_or_noop(t *testing.T) {
	policy := Fresh_policy(t)
	a := default_pool(t)
	mem_extend(t, policy, a.Self(), "a")
	es, _ := constants(t, 20, func(i int) any { return i })
	must(Wait(must(laila.Memorize(entries_list(es), py.Kw{"pool_nickname": "a"})), 30))
	f, err := a.Le(a.Self())
	if err == nil {
		_, err = Wait(f, 60)
	}
	if err != nil {
		if errors.Is(err, py.ValueError) || errors.Is(err, py.RuntimeError) {
			return
		}
		t.Fatal(err)
	}
	assertEq(t, pkeys(t, a).Len(), 20)
}

// ---------------------------------------------------------------------------
// router
// ---------------------------------------------------------------------------

func TestRouter_hundreds_of_pools_by_nickname(t *testing.T) {
	policy := Fresh_policy(t)
	n := N_scaled(300, 50)
	pools := make([]*Pool, 0, n)
	sw := Timed(func() {
		for i := 0; i < n; i++ {
			p := default_pool(t)
			mem_extend(t, policy, p.Self(), fmt.Sprintf("p%d", i))
			pools = append(pools, p)
		}
	})
	assertTrue(t, sw.Elapsed < 120)
	futs := make([]any, n)
	for i := range futs {
		futs[i] = must(laila.Memorize(must(laila.Constant(i)), py.Kw{"pool_nickname": fmt.Sprintf("p%d", i)}))
	}
	for _, f := range futs {
		must(Wait(f, 60))
	}
	Release_all(futs...)
	for i := 0; i < n; i++ {
		assertEq(t, pkeys(t, pools[i]).Len(), 1)
	}
}

func TestRouter_round_robin_memorize_from_threads(t *testing.T) {
	policy := Fresh_policy(t)
	n_pools, per := 20, N_scaled(100, 20)
	pools := make([]*Pool, n_pools)
	for i := range pools {
		pools[i] = default_pool(t)
		mem_extend(t, policy, pools[i].Self(), fmt.Sprintf("rr%d", i))
	}
	check(t, Go_threads(func(tt int) error {
		futs := make([]any, 0, per)
		for j := 0; j < per; j++ {
			e, err := laila.Constant(py.DictOf("t", tt, "j", j))
			if err != nil {
				return err
			}
			f, err := laila.Memorize(e, py.Kw{"pool_nickname": fmt.Sprintf("rr%d", (tt+j)%n_pools)})
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
	}, 8))
	total := 0
	for _, p := range pools {
		total += pkeys(t, p).Len()
	}
	assertEq(t, total, 8*per)
}

func TestRouter_unknown_nickname_raises_promptly(t *testing.T) {
	Fresh_policy(t)
	sw := Timed(func() {
		f, err := laila.Memorize(must(laila.Constant(1)), py.Kw{"pool_nickname": "nope"})
		if err != nil {
			if errors.Is(err, py.KeyError) {
				return
			}
			t.Fatal(err)
		}
		if _, err := Wait(f, 30); err == nil {
			t.Fatal("expected the memorize to fail")
		}
	})
	assertTrue(t, sw.Elapsed < 10)
}

func TestRouter_unknown_pool_id_raises_promptly(t *testing.T) {
	Fresh_policy(t)
	sw := Timed(func() {
		f, err := laila.Remember("LAILA:ENTRY:"+strings.Repeat("0", 8)+"-0000-4000-8000-000000000000",
			py.Kw{"pool_id": "LAILA:POOL:nope"})
		if err != nil {
			if errors.Is(err, py.KeyError) {
				return
			}
			t.Fatal(err)
		}
		if _, err := Wait(f, 30); err == nil {
			t.Fatal("expected the remember to fail")
		}
	})
	assertTrue(t, sw.Elapsed < 10)
}

func TestRouter_hot_swap_pool_under_nickname_while_writing(t *testing.T) {
	policy := Fresh_policy(t)
	p1, p2 := default_pool(t), default_pool(t)
	mem_extend(t, policy, p1.Self(), "hot")
	n := N_scaled(400, 100)
	es, _ := constants(t, n, func(i int) any { return i })
	futs := make([]any, 0, n)
	for i, e := range es {
		if i == n/2 {
			mem_extend(t, policy, p2.Self(), "hot")
		}
		futs = append(futs, must(laila.Memorize(e, py.Kw{"pool_nickname": "hot"})))
	}
	for _, f := range futs {
		must(Wait(f, 60))
	}
	Release_all(futs...)
	assertEq(t, pkeys(t, p1).Len()+pkeys(t, p2).Len(), n)
	assertTrue(t, pkeys(t, p2).Len() >= 1)
}

func TestRouter_re_extend_same_pool_many_times(t *testing.T) {
	policy := Fresh_policy(t)
	p := default_pool(t)
	router := Router(policy)
	before := router.Pools_pq.Qsize()
	n := N_scaled(2_000, 200)
	for i := 0; i < n; i++ {
		mem_extend(t, policy, p.Self(), "same")
	}
	assertTrue(t, must(router.Pools.Getitem(p.Global_id())) == p.Self())
	// the affinity queue is append-only: every re-registration adds a row
	assertEq(t, router.Pools_pq.Qsize(), before+n)
}

func TestRouter_resolve_pool_ref_variants(t *testing.T) {
	policy := Fresh_policy(t)
	p := default_pool(t)
	mem_extend(t, policy, p.Self(), "nick")
	mem := Mem(policy)
	assertTrue(t, must(mem.Resolve_pool_ref_(p.Self())) == p.Self())
	assertTrue(t, must(mem.Resolve_pool_ref_(p.Global_id())) == p.Self())
	assertTrue(t, must(mem.Resolve_pool_ref_("nick")) == p.Self())
	if _, err := mem.Resolve_pool_ref_("missing"); !errors.Is(err, py.KeyError) {
		t.Fatalf("expected KeyError, got %v", err)
	}
	if _, err := mem.Resolve_pool_ref_(123); !errors.Is(err, py.TypeError) {
		t.Fatalf("expected TypeError, got %v", err)
	}
}

func TestRouter_default_pool_stays_alpha_after_many_extends(t *testing.T) {
	policy := Fresh_policy(t)
	for i := 0; i < N_scaled(100, 20); i++ {
		check(t, Mem(policy).Extend(must(laila.New_DefaultPool()),
			py.Kw{"pool_nickname": fmt.Sprintf("x%d", i), "affinity": float64(i)}))
	}
	e := must(laila.Constant(1))
	must(Wait(must(laila.Memorize(e)), 30))
	assertTrue(t, must(alpha_pool(t).Exists(e.Global_id())))
}

func TestRouter_memorize_to_many_pools_then_forget_everywhere(t *testing.T) {
	policy := Fresh_policy(t)
	pools := make([]*Pool, 10)
	for i := range pools {
		pools[i] = default_pool(t)
		mem_extend(t, policy, pools[i].Self(), fmt.Sprintf("m%d", i))
	}
	es, ids := constants(t, N_scaled(100, 20), func(i int) any { return i })
	for i := 0; i < 10; i++ {
		must(Wait(must(laila.Memorize(entries_list(es), py.Kw{"pool_nickname": fmt.Sprintf("m%d", i)})), 120))
	}
	for i := 0; i < 10; i++ {
		must(Wait(must(laila.Forget(ids, py.Kw{"pool_nickname": fmt.Sprintf("m%d", i)})), 120))
	}
	for _, p := range pools {
		assertEq(t, pkeys(t, p), py.NewList())
	}
}

func TestRouter_pool_wrapper_view_via_manifest_key(t *testing.T) {
	Fresh_policy(t)
	es, _ := constants(t, N_scaled(100, 20), func(i int) any { return py.DictOf("i", i) })
	must(Wait(must(laila.Memorize(entries_list(es))), 60))
	data := py.NewDict()
	for i, e := range es {
		data.Set(fmt.Sprintf("item%d", i), e.Global_id())
	}
	m := must(laila.New_Manifest(py.Kw{"data": data}))
	must(Wait(must(laila.Memorize(m)), 60))
	view := must(alpha_pool(t).Getitem(m))
	assertTrue(t, view != nil)
}

func TestRouter_router_threads_extend_concurrently(t *testing.T) {
	policy := Fresh_policy(t)
	router := Router(policy)
	per := N_scaled(50, 10)
	var mu sync.Mutex
	errs := []error{}
	check(t, Go_threads(func(tt int) error {
		for j := 0; j < per; j++ {
			p, err := laila.New_DefaultPool()
			if err == nil {
				err = Mem(policy).Extend(p, py.Kw{"pool_nickname": fmt.Sprintf("t%d-%d", tt, j)})
			}
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
				return nil
			}
		}
		return nil
	}, 8))
	assertEq(t, errs, []error{})
	count := 0
	for _, k := range router.Pools_nicknames.Keys() {
		if strings.HasPrefix(py.Str(k), "t") {
			count++
		}
	}
	assertEq(t, count, 8*per)
}

// ---------------------------------------------------------------------------
// chains with events
// ---------------------------------------------------------------------------

func TestChainsWithEvents_writer_and_reader_threads_on_chain_never_deadlock(t *testing.T) {
	Setup(t)
	origin, cache := no_index(t), no_index(t)
	lshift(t, cache, origin)
	n := N_scaled(3_000, 300)
	done := py.NewEvent()
	writer := func(tt int) error {
		defer done.Set()
		for i := 0; i < n; i++ {
			if err := origin.Setitem(fmt.Sprintf("k%d", i), py.DictOf("i", i)); err != nil {
				return err
			}
		}
		return nil
	}
	reader := func(tt int) error {
		i := 0
		for !done.Is_set() || i < n {
			v, err := cache.Getitem(fmt.Sprintf("k%d", i%n))
			if err != nil {
				return err
			}
			if v != nil && !py.Eq(v, py.DictOf("i", i%n)) {
				return py.AssertionError.Newf("torn read at %d: %v", i%n, v)
			}
			i++
			if i > 5*n {
				break
			}
		}
		return nil
	}
	check(t, Go_threads(func(tt int) error {
		if tt == 0 {
			return writer(tt)
		}
		return reader(tt)
	}, 4))
	assertEq(t, pkeys(t, origin).Len(), n)
}
