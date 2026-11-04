//go:build workload

// Port of tests/workloads/test_w24_memory_pressure_leaks.py.
//
// W24 -- memory pressure and resource leaks.
//
// Long loops that must not grow: RSS over repeated large memorize / forget
// cycles, the future bank with and without ``release()``, thread and file
// descriptor counts across policy / pool / peer-link lifecycles, taskforce
// in-flight accounting after tens of thousands of tasks, and object
// collection after forget + release.
//
// Port notes: ``gc.collect()`` is ``runtime.GC()`` plus
// ``debug.FreeOSMemory()`` (CPython hands freed numpy buffers straight
// back to the OS; Go's scavenger does so lazily, so the RSS probes force
// it); ``threading.active_count()`` is ``runtime.NumGoroutine()``;
// ``weakref.ref(e)`` is ``py.WeakRef_self(e)``; ``gc.get_objects()`` has
// no Go twin, so the "no big objects left on the heap" check reads the
// live heap size instead.
package workloads

import (
	"bytes"
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

const T24_ = 300.0

// w24_settle is ``_settle(probe, n=3)``: ``n`` collections, then a snapshot.
func w24_settle(probe ResourceProbe, n ...int) *py.Dict {
	rounds := 3
	if len(n) > 0 {
		rounds = n[0]
	}
	for i := 0; i < rounds; i++ {
		runtime.GC()
	}
	debug.FreeOSMemory()
	return probe.Snapshot()
}

// w24_f is ``snapshot["key"]`` as a float.
func w24_f(snap *py.Dict, key string) float64 { return py.Must(py.Float(py.Must(snap.Getitem(key)))) }

// w24_i is ``snapshot["key"]`` as an int.
func w24_i(snap *py.Dict, key string) int { return py.Must(py.Int(py.Must(snap.Getitem(key)))) }

// w24_ints is ``[int(x) for x in xs]``.
func w24_ints(xs []float64) []int {
	out := make([]int, len(xs))
	for i, x := range xs {
		out[i] = int(x)
	}
	return out
}

// w24_wait_release is ``f.wait(_T); f.release()`` returning the value.
func w24_wait_release(t *testing.T, f any) any {
	t.Helper()
	v, err := Wait(f, T24_)
	check(t, err)
	Release_all(f)
	return v
}

// w24_one is ``async def one(i): return i``.
func w24_one(i int) py.CoroutineFunc {
	return func(_ ...any) (any, error) { return i, nil }
}

// w24_lambda is ``lambda i=i: one(i)``: a sync callable returning the
// coroutine object.
func w24_lambda(i int) any {
	coro := w24_one(i)
	return func() (any, error) { return py.Coro(coro), nil }
}

// w24_submit is ``laila.command.submit(tasks)``.
func w24_submit(tasks []any) (any, error) {
	cmd, err := laila.Command()
	if err != nil {
		return nil, err
	}
	return cmd.(*laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_).Submit(tasks)
}

// w24_remote_roundtrip memorizes *value* into B's "store" and reads it back.
func w24_remote_roundtrip(t *testing.T, tp *TwoPolicies, value int) {
	t.Helper()
	e := must(laila.Entry_constant(value))
	must(Wait(must(laila.Memorize(e, py.Kw{"dst_policy": tp.Bid, "dst_pool": "store"})), T24_))
	got := must(Wait(must(laila.Remember(e.Global_id(), py.Kw{"dst_policy": tp.Bid, "dst_pool": "store", "persist": false})), T24_))
	assertEq(t, Unwrap(got), value)
}

// ---------------------------------------------------------------------------
// RSS bounded
// ---------------------------------------------------------------------------

func TestRssBounded_large_memorize_forget_cycles(t *testing.T) {
	policy := Fresh_policy(t)
	probe := Probe()
	pool := default_pool(t)
	mem_extend(t, policy, pool.Self(), "big")
	mb := 32
	cycles := N_scaled(12, 4)
	// warm-up cycle so allocator high-water marks are established
	rss := []float64{}
	for c := 0; c < cycles+1; c++ {
		arr := Rand_array(mb*1024*1024, c)
		e := must(laila.Entry_constant(arr))
		w24_wait_release(t, must(laila.Memorize(e, py.Kw{"pool_nickname": "big"})))
		back := Unwrap(w24_wait_release(t, must(laila.Remember(e.Global_id(), py.Kw{"pool_nickname": "big", "persist": false})))).(*py.NDArray)
		assertTrue(t, w22_shape_is(back, arr.Shape...))
		back, arr, e = nil, nil, nil
		w24_wait_release(t, must(laila.Forget(pkeys(t, pool), py.Kw{"pool_nickname": "big"})))
		assertEq(t, pkeys(t, pool).Len(), 0)
		rss = append(rss, w24_f(w24_settle(probe), "rss_mb"))
	}
	growth := rss[len(rss)-1] - rss[1]
	assertTrue(t, growth < float64(4*mb),
		fmt.Sprintf("RSS grew %.0f MB over %d cycles: %v", growth, cycles, w24_ints(rss)))
}

func TestRssBounded_persistent_backend_cycles(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		policy := Fresh_policy(t)
		probe := Probe()
		// F-W24-01 (fixed): DuckDBPool caps memory_limit and checkpoints after
		// delete churn, so RSS and the file plateau like the other backends.
		pool := must(Make_pool(kind, Disk_tmp(t)))
		mem_extend(t, policy, pool, "p")
		defer func() {
			Quiesce(policy)
			Close_pool(pool)
		}()
		rss := []float64{}
		for c := 0; c < N_scaled(10, 4); c++ {
			es := make([]*laila.Entry, 16)
			ids := py.NewList()
			for i := range es {
				es[i] = must(laila.Entry_constant(Rand_array(512*1024, c*100+i)))
				ids.Append(es[i].Global_id())
			}
			w24_wait_release(t, must(laila.Memorize(entries_list(es), py.Kw{"pool_nickname": "p"})))
			got := w24_wait_release(t, must(laila.Remember(ids, py.Kw{"pool_nickname": "p", "persist": false})))
			assertEq(t, got.(*py.List).Len(), 16)
			w24_wait_release(t, must(laila.Forget(ids, py.Kw{"pool_nickname": "p"})))
			es, got = nil, nil
			rss = append(rss, w24_f(w24_settle(probe), "rss_mb"))
		}
		assertTrue(t, rss[len(rss)-1]-rss[1] < 150, rss)
	})
}

// A memory cache in front of an origin never evicts; documents the growth
// so operators size it deliberately.
func TestRssBounded_proxy_cache_is_unbounded_by_design(t *testing.T) {
	Fresh_policy(t)
	origin, cache := no_index(t), no_index(t)
	lshift(t, cache, origin)
	n := N_scaled(20_000, 2_000)
	for i := 0; i < n; i++ {
		pset(t, origin, fmt.Sprintf("k%d", i), py.DictOf("i", i, "pad", strings.Repeat("x", 64)))
	}
	for i := 0; i < n; i++ {
		pget(t, cache, fmt.Sprintf("k%d", i))
	}
	assertEq(t, pkeys(t, cache).Len(), n)
}

// ---------------------------------------------------------------------------
// future bank
// ---------------------------------------------------------------------------

func TestFutureBank24_release_returns_bank_to_baseline(t *testing.T) {
	policy := Fresh_policy(t)
	bank := policy.Future_bank
	base := bank.Len()
	n := N_scaled(5000, 500)
	futs := make([]any, n)
	for i := range futs {
		futs[i] = must(w24_submit([]any{w24_lambda(i)}))
	}
	for _, f := range futs {
		must(Wait(f, T24_))
	}
	assertTrue(t, bank.Len() >= base+n)
	Release_all(futs...)
	assertTrue(t, bank.Len() <= base+2)
}

func TestFutureBank24_group_release_drops_children(t *testing.T) {
	policy := Fresh_policy(t)
	bank := policy.Future_bank
	base := bank.Len()
	n := N_scaled(3000, 300)
	tasks := make([]any, n)
	for i := range tasks {
		tasks[i] = w24_lambda(i)
	}
	gf := must(w24_submit(tasks)).(*laila.GroupFuture)
	must(gf.Wait(T24_))
	assertTrue(t, bank.Len() >= base+n)
	gf.Release()
	assertTrue(t, bank.Len() <= base+2, bank.Len()-base)
}

func TestFutureBank24_memory_api_futures_released_by_caller(t *testing.T) {
	policy := Fresh_policy(t)
	bank := policy.Future_bank
	base := bank.Len()
	n := N_scaled(2000, 200)
	es, _ := constants(t, n, func(i int) any { return i })
	futs := make([]any, n)
	for i, e := range es {
		futs[i] = must(laila.Memorize(e))
	}
	for _, f := range futs {
		must(Wait(f, T24_))
	}
	assertTrue(t, bank.Len() >= base+n) // documented: nothing auto-releases
	Release_all(futs...)
	for i, e := range es {
		futs[i] = must(laila.Remember(e.Global_id(), py.Kw{"persist": false}))
	}
	for _, f := range futs {
		must(Wait(f, T24_))
	}
	Release_all(futs...)
	assertTrue(t, bank.Len() <= base+2)
}

func TestFutureBank24_entries_collectable_after_forget_and_release(t *testing.T) {
	policy := Fresh_policy(t)
	pool := default_pool(t)
	mem_extend(t, policy, pool.Self(), "c")
	refs := []py.Weak{}
	for i := 0; i < N_scaled(500, 100); i++ {
		e := must(laila.Entry_constant(py.DictOf("i", i, "pad", strings.Repeat("y", 1000))))
		refs = append(refs, py.WeakRef_self(e))
		w24_wait_release(t, must(laila.Memorize(e, py.Kw{"pool_nickname": "c"})))
		w24_wait_release(t, must(laila.Forget(e.Global_id(), py.Kw{"pool_nickname": "c"})))
		e = nil
	}
	Quiesce(policy)
	for i := 0; i < 3; i++ {
		runtime.GC()
	}
	alive := 0
	for _, r := range refs {
		if r.Value() != nil {
			alive++
		}
	}
	assertTrue(t, float64(alive) <= float64(len(refs))*0.05,
		fmt.Sprintf("%d/%d entries still referenced after forget+release", alive, len(refs)))
}

// ---------------------------------------------------------------------------
// threads and fds
// ---------------------------------------------------------------------------

func TestThreadsAndFds_policy_lifecycle_cycles_do_not_leak_threads(t *testing.T) {
	policy := Fresh_policy(t)
	probe := Probe()
	base := w24_settle(probe)
	for i := 0; i < N_scaled(25, 8); i++ {
		p := As_policy(must(laila.New_DefaultPolicy()))
		laila.Activate_policy(p)
		func() {
			defer func() {
				_ = Cmd(p).Shutdown(py.Kw{"wait": true, "cancel_pending": true})
				laila.Activate_policy(policy)
				laila.Local_policies_.PopDefault(p.Global_id(), nil)
			}()
			e := must(laila.Entry_constant(i))
			must(Wait(must(laila.Memorize(e)), T24_))
			assertEq(t, Unwrap(must(Wait(must(laila.Remember(e.Global_id(), py.Kw{"persist": false})), T24_))), i)
		}()
	}
	now := w24_settle(probe)
	assertTrue(t, Wait_until(func() bool { return probe.Threads() <= w24_i(base, "threads")+4 }, 20), base, now)
	assertTrue(t, probe.Fds() <= w24_i(base, "fds")+10)
}

func TestThreadsAndFds_pool_open_close_cycles_do_not_leak_fds(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		Fresh_policy(t)
		disk_tmp := Disk_tmp(t)
		probe := Probe()
		base := w24_settle(probe)
		for c := 0; c < N_scaled(60, 15); c++ {
			raw := must(Make_pool(kind, disk_tmp))
			func() {
				defer Close_pool(raw)
				pool := laila.As_pool_(raw)
				for i := 0; i < 10; i++ {
					pset(t, pool, fmt.Sprintf("k%d", i), py.DictOf("c", c, "i", i))
				}
				assertEq(t, pget(t, pool, "k9"), py.DictOf("c", c, "i", 9))
			}()
			raw = nil
		}
		now := w24_settle(probe)
		assertTrue(t, w24_i(now, "fds") <= w24_i(base, "fds")+5, base, now)
		assertTrue(t, w24_i(now, "threads") <= w24_i(base, "threads")+2)
	})
}

func TestThreadsAndFds_peer_link_cycles_do_not_leak(t *testing.T) {
	Fresh_policy(t)
	probe := Probe()
	base := w24_settle(probe)
	for c := 0; c < N_scaled(15, 5); c++ {
		func() {
			tp := must(New_TwoPolicies("tcp", py.Kw{"liveness_interval": 60.0}).Enter())
			defer tp.Exit()
			must(tp.Extend_b("store"))
			w24_remote_roundtrip(t, tp, c)
			Quiesce(tp.A)
			Quiesce(tp.B)
		}()
	}
	assertTrue(t, Wait_until(func() bool { return probe.Threads() <= w24_i(base, "threads")+6 }, 30),
		base, probe.Snapshot())
	assertTrue(t, Wait_until(func() bool { return probe.Fds() <= w24_i(base, "fds")+12 }, 30),
		base, probe.Snapshot())
}

func TestThreadsAndFds_taskforce_accounting_after_many_tasks(t *testing.T) {
	policy := Fresh_policy(t)
	probe := Probe()
	cmd := Cmd(policy)
	tf := must(cmd.Taskforces.Getitem(cmd.Internal_taskforce)).(*laila.PythonAsyncThreadPoolTaskForce)
	base := w24_settle(probe)
	one := py.CoroutineFunc(func(_ ...any) (any, error) { return 1, nil })
	n := N_scaled(20_000, 2_000)
	sw := Timed(func() {
		for s := 0; s < n; s += 500 {
			// coroutine *functions* run on the loop threads, not the sync executor
			tasks := make([]any, 500)
			for i := range tasks {
				tasks[i] = one
			}
			gf := must(w24_submit(tasks)).(*laila.GroupFuture)
			assertEq(t, must(gf.Wait(T24_)).(*py.List).Len(), 500)
			gf.Release()
		}
	})
	assertTrue(t, tf.Inflight() == 0 && tf.Parked() == 0)
	assertTrue(t, probe.Threads() <= w24_i(base, "threads")+2, base, probe.Snapshot())
	assertTrue(t, sw.Elapsed < 240)
}

func TestThreadsAndFds_sync_callables_do_not_leave_idle_executor_threads(t *testing.T) {
	Fresh_policy(t)
	probe := Probe()
	base := w24_settle(probe)
	for s := 0; s < N_scaled(10_000, 2_000); s += 500 {
		tasks := make([]any, 500)
		for i := range tasks {
			tasks[i] = w24_lambda(s + i)
		}
		gf := must(w24_submit(tasks)).(*laila.GroupFuture)
		assertEq(t, must(gf.Wait(T24_)).(*py.List).Len(), 500)
		gf.Release()
	}
	assertTrue(t, Wait_until(func() bool { return probe.Threads() <= w24_i(base, "threads")+4 }, 10),
		base, probe.Snapshot())
}

func TestThreadsAndFds_channel_churn_does_not_leak_lanes_or_threads(t *testing.T) {
	Fresh_policy(t)
	probe := Probe()
	tp := must(New_TwoPolicies("tcp", py.Kw{"liveness_interval": 60.0}).Enter())
	defer tp.Exit()
	base := w24_settle(probe)
	for i := 0; i < N_scaled(400, 100); i++ {
		ch := must(tp.Peer().Channel(fmt.Sprintf("c%d", i%50)))
		check(t, ch.Send([]byte("x")))
		ch.Close()
	}
	assertTrue(t, Wait_until(func() bool { return len(laila.As_carrier_(tp.Pa).Channel_names(tp.Bid)) == 0 }, 15))
	assertTrue(t, Wait_until(func() bool { return len(laila.As_carrier_(tp.Pb).Channel_names(tp.Aid)) == 0 }, 15))
	assertTrue(t, probe.Threads() <= w24_i(base, "threads")+2)
	func() {
		defer func() { _ = recover() }() // contextlib.suppress(Exception)
		Quiesce(tp.A)
	}()
}

// ---------------------------------------------------------------------------
// big objects
// ---------------------------------------------------------------------------

// 256 MB array through memorize / remember / forget, three rounds.
//
// After the first round the python heap holds no big objects (verified
// with ``gc.get_objects``); what remains in RSS is glibc arena
// retention, so the invariant is *no further growth* across rounds
// rather than a return to baseline.
func TestBigObjects_quarter_gb_array_round_trip_and_reclaim(t *testing.T) {
	policy := Fresh_policy(t)
	probe := Probe()
	pool := default_pool(t)
	mem_extend(t, policy, pool.Self(), "huge")
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	heap_base := ms.HeapAlloc
	base := w24_f(w24_settle(probe), "rss_mb")
	after := []float64{}
	for r := 0; r < 3; r++ {
		arr := Rand_array(256*1024*1024, r)
		e := must(laila.Entry_constant(arr))
		w24_wait_release(t, must(laila.Memorize(e, py.Kw{"pool_nickname": "huge"})))
		back := Unwrap(w24_wait_release(t, must(laila.Remember(e.Global_id(), py.Kw{"pool_nickname": "huge", "persist": false})))).(*py.NDArray)
		// back.shape == arr.shape and np.array_equal(back[:1000], arr[:1000])
		assertTrue(t, w22_shape_is(back, arr.Shape...) && bytes.Equal(back.Data[:1000*8], arr.Data[:1000*8]))
		peak := w24_f(w24_settle(probe), "rss_mb")
		back, arr, e = nil, nil, nil
		w24_wait_release(t, must(laila.Forget(pkeys(t, pool), py.Kw{"pool_nickname": "huge"})))
		assertEq(t, pkeys(t, pool).Len(), 0)
		Quiesce(policy)
		after = append(after, w24_f(w24_settle(probe), "rss_mb"))
		if r == 0 {
			assertTrue(t, peak-base > 200, fmt.Sprintf("peak %.0f base %.0f", peak, base))
		}
	}
	// gc.get_objects(): no ndarray / bytes object above 64 MB is still
	// alive -- the live heap must have dropped back below one such object.
	runtime.GC()
	runtime.ReadMemStats(&ms)
	heap_mb := int64(ms.HeapAlloc-min(ms.HeapAlloc, heap_base)) / 1_000_000
	assertTrue(t, ms.HeapAlloc < heap_base+64*1024*1024, fmt.Sprintf("live heap grew by %d MB", heap_mb))
	assertTrue(t, after[len(after)-1]-after[0] < 64,
		fmt.Sprintf("base %.0f after-each-round %v MB", base, w24_ints(after)))
}
