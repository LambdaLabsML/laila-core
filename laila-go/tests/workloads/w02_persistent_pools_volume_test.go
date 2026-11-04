//go:build workload

// Port of tests/workloads/test_w02_persistent_pools_volume.py: SQLite /
// DuckDB / HDF5 pools under volume: the generic pool contract at scale,
// write-cost curves (commit per op + index shard rewrite), large values,
// thread contention on the single shared connection, reopen persistence
// and the memorize/remember path through each backend.
package workloads

import (
	"errors"
	"fmt"
	"iter"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

// for_kinds is the ``@pytest.fixture(params=PERSISTENT_KINDS)`` ``kind``
// fixture: one subtest per persistent backend.
func for_kinds(t *testing.T, body func(t *testing.T, kind string)) {
	t.Helper()
	for _, kind := range PERSISTENT_KINDS {
		kind := kind
		t.Run("kind="+kind, func(t *testing.T) { body(t, kind) })
	}
}

// with_pool is the ``pool`` fixture: ``make_pool(kind, disk_tmp)`` closed
// at teardown.
func with_pool(t *testing.T, kind string) (*Pool, any) {
	t.Helper()
	raw := must(Make_pool(kind, Disk_tmp(t)))
	t.Cleanup(func() { Close_pool(raw) })
	return laila.As_pool_(raw), raw
}

// file_path is ``pool.file_path`` for whichever persistent backend.
func file_path(pool any) string {
	switch p := pool.(type) {
	case *laila.SQLitePool:
		return *p.File_path
	case *laila.DuckDBPool:
		return *p.File_path
	case *laila.HDF5Pool:
		return *p.File_path
	}
	return py.Str(py.Must(py.Getattr(pool, "file_path")))
}

// reopen_ is ``_reopen(kind, pool, *, same_uuid=True)``: close *pool* and
// open a fresh instance on the same file.
//
// Index shards are keyed by the *owner pool uuid*, so a faithful reopen
// must pass the same ``uuid`` -- see ``TestReopenIdentity``.
func reopen_(t *testing.T, kind string, pool any, same_uuid ...bool) (*Pool, any) {
	t.Helper()
	same := true
	if len(same_uuid) > 0 {
		same = same_uuid[0]
	}
	path := file_path(pool)
	kw := py.Kw{"file_path": path}
	if same {
		kw["uuid"] = laila.As_pool_(pool).Uuid()
	}
	Close_pool(pool)
	var raw any
	switch kind {
	case "sqlite":
		raw = must(laila.New_SQLitePool(kw))
	case "duckdb":
		raw = must(laila.New_DuckDBPool(kw))
	default:
		raw = must(laila.New_HDF5Pool(kw))
	}
	return laila.As_pool_(raw), raw
}

// mem_extend is ``laila.memory.extend(pool, pool_nickname=nick)``.
func mem_extend(t *testing.T, policy *laila.LAILA_IDENTIFIABLE_POLICY_, pool any, nick string) {
	t.Helper()
	check(t, Mem(policy).Extend(pool, py.Kw{"pool_nickname": nick}))
}

func index_keys(t *testing.T, p *Pool) []string {
	t.Helper()
	out := []string{}
	for _, k := range pkeys(t, p, py.Kw{"include_index": true}).Items() {
		if laila.Is_index_key(k) {
			out = append(out, py.Str(k))
		}
	}
	return out
}

func strp(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

// ---------------------------------------------------------------------------
// contract at volume
// ---------------------------------------------------------------------------

func TestContractAtVolume_write_read_many_keys(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		pool, _ := with_pool(t, kind)
		n := N_scaled(1_500, 300)
		sw := Timed(func() {
			for i := 0; i < n; i++ {
				pset(t, pool, fmt.Sprintf("k%d", i), py.DictOf("i", i))
			}
		})
		assertEq(t, pkeys(t, pool).Len(), n)
		for i := 0; i < n; i += 97 {
			assertEq(t, pget(t, pool, fmt.Sprintf("k%d", i)), py.DictOf("i", i))
		}
		per_op_ms := sw.Elapsed / float64(n) * 1000
		assertTrue(t, per_op_ms < 200, fmt.Sprintf("%s: %.1f ms per raw write", kind, per_op_ms))
	})
}

func TestContractAtVolume_write_cost_scales_linearly(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		dir := Disk_tmp(t)
		small_raw := must(Make_pool(kind, dir, py.Kw{"index_enabled": false}))
		large_raw := must(Make_pool(kind, dir, py.Kw{"index_enabled": false}))
		defer Close_pool(small_raw)
		defer Close_pool(large_raw)
		small, large := laila.As_pool_(small_raw), laila.As_pool_(large_raw)
		ns, nl := N_scaled(300, 100), N_scaled(1_200, 400)
		s1 := Timed(func() {
			for i := 0; i < ns; i++ {
				pset(t, small, fmt.Sprintf("k%d", i), py.DictOf("i", i))
			}
		})
		s2 := Timed(func() {
			for i := 0; i < nl; i++ {
				pset(t, large, fmt.Sprintf("k%d", i), py.DictOf("i", i))
			}
		})
		check(t, Assert_subquadratic(s1.Elapsed, s2.Elapsed, float64(nl)/float64(ns)))
	})
}

func TestContractAtVolume_keys_listing_cost(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		pool, _ := with_pool(t, kind)
		n := N_scaled(1_500, 300)
		for i := 0; i < n; i++ {
			pset(t, pool, fmt.Sprintf("k%d", i), py.DictOf("i", i))
		}
		sw := Timed(func() {
			for r := 0; r < 5; r++ {
				assertEq(t, pkeys(t, pool).Len(), n)
			}
		})
		assertTrue(t, sw.Elapsed < 30)
		gen := pkeys_gen(t, pool)
		assertEq(t, iter_list(gen).Len(), n)
	})
}

func TestContractAtVolume_large_json_values(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		for _, mb := range []int{1, 8, 32} {
			mb := mb
			t.Run(fmt.Sprintf("mb=%d", mb), func(t *testing.T) {
				pool, _ := with_pool(t, kind)
				text := strings.Repeat("x", mb<<20)
				pset(t, pool, "big", py.DictOf("blob", text))
				got := pget(t, pool, "big").(*py.Dict)
				assertEq(t, must(got.Getitem("blob")), text)
				pdel(t, pool, "big")
				assertEq(t, pget(t, pool, "big"), nil)
			})
		}
	})
}

func TestContractAtVolume_many_large_values_disk_growth(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		pool, raw := with_pool(t, kind)
		n := N_scaled(40, 8)
		text := strings.Repeat("y", 1<<20)
		for i := 0; i < n; i++ {
			pset(t, pool, fmt.Sprintf("big%d", i), py.DictOf("i", i, "blob", text))
		}
		// DuckDB keeps committed data in `<file>.wal` until its 16 MB
		// auto-checkpoint threshold; count every file the pool owns.
		fp := file_path(raw)
		var size int64
		for _, f := range []string{fp, fp + ".wal", fp + "-wal"} {
			if st, err := os.Stat(f); err == nil {
				size += st.Size()
			}
		}
		assertTrue(t, float64(size) >= float64(n)*float64(1<<20)*0.9,
			fmt.Sprintf("%s file is suspiciously small: %d", kind, size))
		for i := 0; i < n; i++ {
			pdel(t, pool, fmt.Sprintf("big%d", i))
		}
		assertEq(t, pkeys(t, pool), py.NewList())
	})
}

func TestContractAtVolume_churn_cycles(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		pool, _ := with_pool(t, kind)
		cycles, per := N_scaled(5, 2), N_scaled(150, 50)
		for c := 0; c < cycles; c++ {
			for i := 0; i < per; i++ {
				pset(t, pool, fmt.Sprintf("%s@evolution=%d", base_(i), c), py.DictOf("c", c))
			}
			for i := 0; i < per; i++ {
				check(t, pool.Delete(fmt.Sprintf("%s@evolution=%d", base_(i), c)))
			}
		}
		assertEq(t, pkeys(t, pool), py.NewList())
		// every shard dropped; only the (stable) index identity record remains
		assertEq(t, pkeys(t, pool, py.Kw{"include_index": true}), py.NewList(laila.INDEX_IDENTITY_KEY))
	})
}

func TestContractAtVolume_overwrite_hot_key(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		pool, _ := with_pool(t, kind)
		n := N_scaled(1_000, 300)
		for i := 0; i < n; i++ {
			pset(t, pool, "hot", py.DictOf("i", i))
		}
		assertEq(t, pget(t, pool, "hot"), py.DictOf("i", n-1))
		assertEq(t, pkeys(t, pool).Len(), 1)
	})
}

func TestContractAtVolume_empty_at_scale(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		pool, _ := with_pool(t, kind)
		n := N_scaled(1_000, 300)
		for i := 0; i < n; i++ {
			pset(t, pool, fmt.Sprintf("%s@evolution=%d", base_(i%100), i/100), py.DictOf("i", i))
		}
		check(t, pool.Empty())
		assertEq(t, pkeys(t, pool), py.NewList())
		assertEq(t, pkeys(t, pool, py.Kw{"include_index": true}), py.NewList())
	})
}

func TestContractAtVolume_unicode_and_long_keys(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		pool, _ := with_pool(t, kind)
		n := N_scaled(500, 50)
		keys := make([]string, n)
		want := py.NewSet()
		for i := range keys {
			keys[i] = strings.Repeat("ключ✓/slash?q=1&x#", 10) + fmt.Sprint(i)
			want.Add(keys[i])
		}
		for _, k := range keys {
			pset(t, pool, k, py.DictOf("k", k))
		}
		got := py.NewSet()
		for _, k := range pkeys(t, pool).Items() {
			got.Add(k)
		}
		assertEq(t, got, want)
		for i := 0; i < n; i += 31 {
			assertEq(t, must(pget(t, pool, keys[i]).(*py.Dict).Getitem("k")), keys[i])
		}
	})
}

func TestContractAtVolume_non_json_value_rejected(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		pool, _ := with_pool(t, kind)
		if err := pool.Setitem("bad", 123); !errors.Is(err, py.TypeError) {
			t.Fatalf("expected TypeError, got %v", err)
		}
		if err := pool.Setitem("bad", []byte("bytes")); !errors.Is(err, py.TypeError) {
			t.Fatalf("expected TypeError, got %v", err)
		}
	})
}

func TestContractAtVolume_string_values_roundtrip_as_json(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		pool, _ := with_pool(t, kind)
		for i := 0; i < N_scaled(300, 50); i++ {
			pset(t, pool, fmt.Sprintf("s%d", i),
				must(py.Json_dumps(py.DictOf("i", i, "s", strings.Repeat("é", 10)))))
		}
		assertEq(t, pget(t, pool, "s0"), py.DictOf("i", 0, "s", strings.Repeat("é", 10)))
	})
}

// ---------------------------------------------------------------------------
// threads on the shared connection
// ---------------------------------------------------------------------------

func TestThreads_parallel_writers_disjoint(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		for _, n_threads := range []int{4, 16} {
			n_threads := n_threads
			t.Run(fmt.Sprintf("n_threads=%d", n_threads), func(t *testing.T) {
				pool, _ := with_pool(t, kind)
				per := N_scaled(60, 20)
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
	})
}

func TestThreads_parallel_same_key(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		pool, _ := with_pool(t, kind)
		per := N_scaled(100, 30)
		check(t, Go_threads(func(i int) error {
			for j := 0; j < per; j++ {
				if err := pool.Setitem("hot", py.DictOf("t", i, "j", j)); err != nil {
					return err
				}
			}
			return nil
		}, 8))
		assertEq(t, must(pget(t, pool, "hot").(*py.Dict).Getitem("j")), per-1)
	})
}

func TestThreads_readers_during_writes(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		pool, _ := with_pool(t, kind)
		n := N_scaled(400, 100)
		for i := 0; i < n; i++ {
			pset(t, pool, fmt.Sprintf("k%d", i), py.DictOf("i", i))
		}
		stop := py.NewEvent()
		var mu sync.Mutex
		bad := []int{}

		reader := func(i int) error {
			for !stop.Is_set() {
				for j := 0; j < n; j += 53 {
					v, err := pool.Getitem(fmt.Sprintf("k%d", j))
					if err != nil {
						return err
					}
					if v == nil || !py.Eq(must(v.(*py.Dict).Getitem("i")), j) {
						mu.Lock()
						bad = append(bad, j)
						mu.Unlock()
					}
				}
			}
			return nil
		}
		writer := func(i int) error {
			defer stop.Set()
			for j := n; j < n+N_scaled(200, 50); j++ {
				if err := pool.Setitem(fmt.Sprintf("k%d", j), py.DictOf("i", j)); err != nil {
					return err
				}
			}
			return nil
		}
		check(t, Go_threads(func(i int) error {
			if i == 0 {
				return writer(i)
			}
			return reader(i)
		}, 4))
		assertEq(t, bad, []int{})
	})
}

func TestThreads_delete_storm_with_reads(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		pool, _ := with_pool(t, kind)
		n := N_scaled(400, 100)
		for i := 0; i < n; i++ {
			pset(t, pool, fmt.Sprintf("k%d", i), py.DictOf("i", i))
		}
		deleter := func(i int) error {
			for j := i; j < n; j += 4 {
				if err := pool.Delitem(fmt.Sprintf("k%d", j)); err != nil {
					return err
				}
			}
			return nil
		}
		reader := func(i int) error {
			for j := 0; j < n; j++ {
				if _, err := pool.Getitem(fmt.Sprintf("k%d", j)); err != nil {
					return err
				}
			}
			return nil
		}
		check(t, Go_threads(func(i int) error {
			if i < 4 {
				return deleter(i)
			}
			return reader(i)
		}, 6))
		assertEq(t, pkeys(t, pool), py.NewList())
	})
}

func TestThreads_generator_holds_lock(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		pool, _ := with_pool(t, kind)
		for i := 0; i < 50; i++ {
			pset(t, pool, fmt.Sprintf("k%d", i), py.DictOf("i", i))
		}
		gen := pkeys_gen(t, pool)
		next, stop := iter.Pull2(gen)
		defer stop()
		next()
		done := py.NewEvent()
		go func() {
			_ = pool.Write("w", py.DictOf("w", 1))
			done.Set()
		}()
		blocked := !done.Wait(0.3)
		for {
			if _, _, ok := next(); !ok {
				break
			}
		}
		assertTrue(t, done.Wait(10))
		// sqlite/duckdb fetchall under the lock then yield -> lock still held
		// while the consumer iterates; hdf5 same. Document either way.
		assertTrue(t, blocked || kind == "sqlite" || kind == "duckdb" || kind == "hdf5")
	})
}

// ---------------------------------------------------------------------------
// persistence / reopen
// ---------------------------------------------------------------------------

func TestReopen_reopen_sees_all_keys(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		pool, raw := with_pool(t, kind)
		n := N_scaled(500, 100)
		for i := 0; i < n; i++ {
			pset(t, pool, fmt.Sprintf("%s@evolution=%d", base_(i%50), i/50), py.DictOf("i", i))
		}
		p2, raw2 := reopen_(t, kind, raw)
		defer Close_pool(raw2)
		assertEq(t, pkeys(t, p2).Len(), n)
		assertEq(t, pget(t, p2, base_(1)+"@evolution=1"), py.DictOf("i", 51))
		// index shards persisted and reloadable
		assertEq(t, strp(p2.Index().Latest(base_(1))), fmt.Sprintf("%s@evolution=%d", base_(1), n/50-1))
	})
}

func TestReopen_reopen_after_empty(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		pool, raw := with_pool(t, kind)
		for i := 0; i < 200; i++ {
			pset(t, pool, fmt.Sprintf("k%d", i), py.DictOf("i", i))
		}
		check(t, pool.Empty())
		p2, raw2 := reopen_(t, kind, raw)
		defer Close_pool(raw2)
		assertEq(t, pkeys(t, p2), py.NewList())
	})
}

func TestReopen_closed_pool_raises(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		pool, raw := with_pool(t, kind)
		pset(t, pool, "k", py.DictOf("v", 1))
		Close_pool(raw)
		if _, err := pool.Getitem("k"); !errors.Is(err, py.RuntimeError) {
			t.Fatalf("expected RuntimeError, got %v", err)
		}
		if err := pool.Setitem("k", py.DictOf("v", 2)); !errors.Is(err, py.RuntimeError) {
			t.Fatalf("expected RuntimeError, got %v", err)
		}
		if _, err := pool.Keys(); !errors.Is(err, py.RuntimeError) {
			t.Fatalf("expected RuntimeError, got %v", err)
		}
	})
}

func TestReopen_repeated_reopen_cycles(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		p, raw := with_pool(t, kind)
		cycles := N_scaled(10, 3)
		for c := 0; c < cycles; c++ {
			pset(t, p, fmt.Sprintf("c%d", c), py.DictOf("c", c))
			p, raw = reopen_(t, kind, raw)
		}
		defer Close_pool(raw)
		assertEq(t, pkeys(t, p).Len(), cycles)
	})
}

func TestReopen_index_survives_reopen_for_many_bases(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		pool, raw := with_pool(t, kind)
		nb := N_scaled(100, 30)
		for b := 0; b < nb; b++ {
			for e := 0; e < 3; e++ {
				pset(t, pool, fmt.Sprintf("%s@evolution=%d", base_(b), e), py.DictOf("b", b, "e", e))
			}
		}
		p2, raw2 := reopen_(t, kind, raw)
		defer Close_pool(raw2)
		shards := []string{}
		for _, k := range index_keys(t, p2) {
			if k != laila.INDEX_IDENTITY_KEY {
				shards = append(shards, k)
			}
		}
		assertEq(t, len(shards), nb)
		for b := 0; b < nb; b += 7 {
			assertEq(t, strp(p2.Index().Nth(base_(b), -1)), base_(b)+"@evolution=2")
		}
	})
}

// ---------------------------------------------------------------------------
// through the policy
// ---------------------------------------------------------------------------

func TestThroughPolicy_memorize_remember_forget_batch(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		policy := Fresh_policy(t)
		pool, raw := with_pool(t, kind)
		mem_extend(t, policy, raw, "p")
		n := N_scaled(600, 150)
		es, ids := constants(t, n, func(i int) any { return py.DictOf("i", i, "s", strings.Repeat("z", 200)) })
		var f, g, h any
		w := Timed(func() {
			f = must(laila.Memorize(entries_list(es), py.Kw{"pool_nickname": "p"}))
			must(Wait(f, 600))
		})
		var got *py.List
		r := Timed(func() {
			g = must(laila.Remember(ids, py.Kw{"pool_nickname": "p", "persist": false}))
			got = must(Wait(g, 600)).(*py.List)
		})
		for i := 0; i < n; i += 37 {
			assertEq(t, must(Unwrap(must(got.Get(i))).(*py.Dict).Getitem("i")), i)
		}
		h = must(laila.Forget(ids, py.Kw{"pool_nickname": "p"}))
		must(Wait(h, 600))
		Release_all(f, g, h)
		assertEq(t, pkeys(t, pool), py.NewList())
		assertTrue(t, w.Elapsed/float64(n) < 0.2 && r.Elapsed/float64(n) < 0.2,
			fmt.Sprint(kind, w.Elapsed, r.Elapsed))
	})
}

func TestThroughPolicy_variable_lineage_and_latest(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		policy := Fresh_policy(t)
		pool, raw := with_pool(t, kind)
		mem_extend(t, policy, raw, "p")
		v := must(laila.Variable(py.Kw{"data": py.DictOf("v", 0)}))
		n := N_scaled(80, 30)
		for i := 1; i <= n; i++ {
			check(t, v.Set_data(py.DictOf("v", i)))
			must(Wait(must(laila.Memorize(v, py.Kw{"pool_nickname": "p"})), 60))
		}
		base := strings.SplitN(v.Global_id(), "@", 2)[0]
		latest := must(Wait(must(laila.Remember(base+"@evolution=-1", py.Kw{"pool_nickname": "p", "persist": false})), 60))
		assertEq(t, Unwrap(latest), py.DictOf("v", n))
		mid := must(laila.Remember(fmt.Sprintf("%s@evolution=%d", base, n/2), py.Kw{"pool_nickname": "p", "persist": false}))
		assertEq(t, Unwrap(must(Wait(mid, 60))), py.DictOf("v", n/2))
		count := 0
		for _, k := range pkeys(t, pool).Items() {
			if strings.Contains(py.Str(k), base) {
				count++
			}
		}
		assertEq(t, count, n)
	})
}

func TestThroughPolicy_persist_cache_back_to_alpha(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		policy := Fresh_policy(t)
		_, raw := with_pool(t, kind)
		mem_extend(t, policy, raw, "p")
		n := N_scaled(150, 50)
		es, ids := constants(t, n, func(i int) any { return py.DictOf("i", i) })
		must(Wait(must(laila.Memorize(entries_list(es), py.Kw{"pool_nickname": "p"})), 300))
		got := must(Wait(must(laila.Remember(ids, py.Kw{"pool_nickname": "p", "persist": true})), 300)).(*py.List)
		assertEq(t, got.Len(), n)
		// cached into alpha
		alpha := alpha_pool(t)
		for _, i := range ids.Items() {
			assertTrue(t, must(alpha.Exists(py.Str(i))))
		}
	})
}

func TestThroughPolicy_large_entries_through_backend(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		policy := Fresh_policy(t)
		_, raw := with_pool(t, kind)
		mem_extend(t, policy, raw, "p")
		var es []*laila.Entry
		ids := py.NewList()
		for _, mb := range []int{1, 4, 16} {
			e := must(laila.Constant(py.DictOf("blob", Rand_bytes(mb<<20, mb))))
			es = append(es, e)
			ids.Append(e.Global_id())
		}
		must(Wait(must(laila.Memorize(entries_list(es), py.Kw{"pool_nickname": "p"})), 300))
		got := must(Wait(must(laila.Remember(ids, py.Kw{"pool_nickname": "p", "persist": false})), 300)).(*py.List)
		for i, e := range es {
			want := must(must(e.Data()).(*py.Dict).Getitem("blob"))
			assertEq(t, must(Unwrap(must(got.Get(i))).(*py.Dict).Getitem("blob")), want)
		}
	})
}

func TestThroughPolicy_concurrent_memorize_threads(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		policy := Fresh_policy(t)
		pool, raw := with_pool(t, kind)
		mem_extend(t, policy, raw, "p")
		n_threads, per := 6, N_scaled(30, 10)
		check(t, Go_threads(func(i int) error {
			futs := make([]any, 0, per)
			for j := 0; j < per; j++ {
				e, err := laila.Constant(py.DictOf("t", i, "j", j))
				if err != nil {
					return err
				}
				f, err := laila.Memorize(e, py.Kw{"pool_nickname": "p"})
				if err != nil {
					return err
				}
				futs = append(futs, f)
			}
			for _, f := range futs {
				if _, err := Wait(f, 120); err != nil {
					return err
				}
			}
			Release_all(futs...)
			return nil
		}, n_threads))
		assertEq(t, pkeys(t, pool).Len(), n_threads*per)
	})
}

func TestThroughPolicy_duplicate_pool_into_backend(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		policy := Fresh_policy(t)
		pool, raw := with_pool(t, kind)
		mem_extend(t, policy, raw, "dst")
		n := N_scaled(150, 50)
		es, _ := constants(t, n, func(i int) any { return py.DictOf("i", i) })
		must(Wait(must(laila.Memorize(entries_list(es))), 120)) // alpha
		gf := must(pool.Le(must(laila.Alpha_pool())))
		must(Wait(gf, 600))
		for _, e := range es {
			assertTrue(t, must(pool.Exists(e.Global_id())))
		}
	})
}

// ---------------------------------------------------------------------------
// reopen identity
// ---------------------------------------------------------------------------

// CountingSQLitePool / CountingDuckDBPool / CountingHDF5Pool are
// ``p2._keys = counting``: a subclass whose ``_keys`` counts scans.
type CountingSQLitePool struct {
	laila.SQLitePool
	Scans int `py:"-"`
}

var CountingSQLitePool_class = py.NewClass("SQLitePool", (*CountingSQLitePool)(nil), laila.SQLitePool_class)

func (p *CountingSQLitePool) Keys_(kw ...py.Kw) (any, error) {
	p.Scans += 1
	return p.SQLitePool.Keys_(kw...)
}

type CountingDuckDBPool struct {
	laila.DuckDBPool
	Scans int `py:"-"`
}

var CountingDuckDBPool_class = py.NewClass("DuckDBPool", (*CountingDuckDBPool)(nil), laila.DuckDBPool_class)

func (p *CountingDuckDBPool) Keys_(kw ...py.Kw) (any, error) {
	p.Scans += 1
	return p.DuckDBPool.Keys_(kw...)
}

type CountingHDF5Pool struct {
	laila.HDF5Pool
	Scans int `py:"-"`
}

var CountingHDF5Pool_class = py.NewClass("HDF5Pool", (*CountingHDF5Pool)(nil), laila.HDF5Pool_class)

func (p *CountingHDF5Pool) Keys_(kw ...py.Kw) (any, error) {
	p.Scans += 1
	return p.HDF5Pool.Keys_(kw...)
}

// reopen_counting_ is ``_reopen(kind, pool, same_uuid=False)`` followed by
// ``p2._keys = counting``: the fresh instance is the counting subclass.
func reopen_counting_(t *testing.T, kind string, pool any) (*Pool, any, func() int) {
	t.Helper()
	path := file_path(pool)
	Close_pool(pool)
	kw := py.Kw{"file_path": path}
	switch kind {
	case "sqlite":
		p := CountingSQLitePool_class.New().(*CountingSQLitePool)
		check(t, p.Init_(kw))
		return laila.As_pool_(p), p, func() int { return p.Scans }
	case "duckdb":
		p := CountingDuckDBPool_class.New().(*CountingDuckDBPool)
		check(t, p.Init_(kw))
		return laila.As_pool_(p), p, func() int { return p.Scans }
	}
	p := CountingHDF5Pool_class.New().(*CountingHDF5Pool)
	check(t, p.Init_(kw))
	return laila.As_pool_(p), p, func() int { return p.Scans }
}

func TestReopenIdentity_reopen_same_uuid_keeps_index(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		pool, raw := with_pool(t, kind)
		base := base_(3)
		for e := 0; e < 20; e++ {
			pset(t, pool, fmt.Sprintf("%s@evolution=%d", base, e), py.DictOf("e", e))
		}
		p2, raw2 := reopen_(t, kind, raw, true)
		defer Close_pool(raw2)
		assertEq(t, strp(p2.Index().Latest(base)), base+"@evolution=19")
	})
}

func TestReopenIdentity_reopen_with_fresh_uuid_keeps_index(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		// F-03 (fixed): the index identity is persisted in the backing store,
		// so a fresh pool instance over the same file finds its shards.
		pool, raw := with_pool(t, kind)
		base := base_(3)
		for e := 0; e < 20; e++ {
			pset(t, pool, fmt.Sprintf("%s@evolution=%d", base, e), py.DictOf("e", e))
		}
		identity := pool.Index().Identity()
		p2, raw2 := reopen_(t, kind, raw, false)
		defer Close_pool(raw2)
		assertEq(t, p2.Index().Identity(), identity)
		assertEq(t, strp(p2.Index().Latest(base)), base+"@evolution=19")
	})
}

func TestReopenIdentity_fresh_uuid_reopen_resolves_without_scan(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		policy := Fresh_policy(t)
		pool, raw := with_pool(t, kind)
		base := base_(4)
		for e := 0; e < 10; e++ {
			pset(t, pool, fmt.Sprintf("%s@evolution=%d", base, e), py.DictOf("e", e))
		}
		p2, raw2, scans := reopen_counting_(t, kind, raw)
		defer Close_pool(raw2)
		mem_extend(t, policy, raw2, "re")
		v := must(laila.Variable(py.Kw{"data": py.DictOf("v", 1)}))
		must(Wait(must(laila.Memorize(v, py.Kw{"pool_nickname": "re"})), 60))
		vb := strings.SplitN(v.Global_id(), "@", 2)[0]
		// (the counting subclass starts counting here -- the Python test
		// installs ``counting`` after ``memorize``; reset to mirror that)
		before := scans()
		got := must(Wait(must(laila.Remember(vb+"@evolution=-1", py.Kw{"pool_nickname": "re", "persist": false})), 60))
		assertEq(t, Unwrap(got), py.DictOf("v", 1))
		assertTrue(t, scans()-before == 0, "lineage lookup fell back to a key scan")
		// Both bases' shards are reachable (shards + the identity record).
		keys := index_keys(t, p2)
		assertTrue(t, contains_str(keys, p2.Index().Shard_id(base)))
		assertTrue(t, contains_str(keys, p2.Index().Shard_id(vb)))
	})
}

func TestReopenIdentity_no_orphan_shards_accumulate_over_reopen_cycles(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		p, raw := with_pool(t, kind)
		base := base_(5)
		cycles := N_scaled(5, 3)
		for c := 0; c < cycles; c++ {
			pset(t, p, fmt.Sprintf("%s@evolution=%d", base, c), py.DictOf("c", c))
			p, raw = reopen_(t, kind, raw, false)
		}
		defer Close_pool(raw)
		shards := index_keys(t, p)
		// exactly one shard for the base (plus the identity record), however
		// many instances touched it
		want := []string{p.Index().Shard_id(base), laila.INDEX_IDENTITY_KEY}
		sort.Strings(shards)
		sort.Strings(want)
		assertEq(t, shards, want)
		cands := make([]string, cycles)
		for c := range cands {
			cands[c] = fmt.Sprintf("%s@evolution=%d", base, c)
		}
		assertEq(t, p.Index().Candidates(base), cands)
	})
}

func contains_str(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// index cost on plain keys
// ---------------------------------------------------------------------------

func TestIndexCostOnPlainKeys_plain_keys_create_no_index_shards(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		// F-04 (fixed): non-LAILA keys carry no lineage and are not indexed.
		pool, _ := with_pool(t, kind)
		n := N_scaled(200, 50)
		for i := 0; i < n; i++ {
			pset(t, pool, fmt.Sprintf("plain-%d", i), py.DictOf("i", i))
		}
		assertEq(t, index_keys(t, pool), []string{})
		for i := 0; i < n; i++ {
			pdel(t, pool, fmt.Sprintf("plain-%d", i))
		}
		assertEq(t, pkeys(t, pool, py.Kw{"include_index": true}), py.NewList())
	})
}

func TestIndexCostOnPlainKeys_index_adds_no_write_cost_for_plain_keys(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		dir := Disk_tmp(t)
		on_raw := must(Make_pool(kind, dir, py.Kw{"index_enabled": true}))
		off_raw := must(Make_pool(kind, dir, py.Kw{"index_enabled": false}))
		defer Close_pool(on_raw)
		defer Close_pool(off_raw)
		on, off := laila.As_pool_(on_raw), laila.As_pool_(off_raw)
		n := N_scaled(150, 50)
		t_on := Timed(func() {
			for i := 0; i < n; i++ {
				pset(t, on, fmt.Sprintf("plain-%d", i), py.DictOf("i", i))
			}
		})
		t_off := Timed(func() {
			for i := 0; i < n; i++ {
				pset(t, off, fmt.Sprintf("plain-%d", i), py.DictOf("i", i))
			}
		})
		// One backend write per plain key either way (generous bound for jitter).
		assertTrue(t, t_on.Elapsed <= t_off.Elapsed*1.5+0.25)
	})
}
