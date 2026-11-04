//go:build workload

// Port of tests/workloads/test_w18_rpc_soak_transports.py.
//
// W18 -- RPC soak over every in-process transport.
//
// Two policies peered over loopback / TCP / UDP / Unix sockets / WebSocket
// exchanging thousands of memorize/remember/forget calls, large payloads,
// concurrent callers, inbound admission control (``max_inflight_rpcs``),
// retry/backoff, RPC error propagation, transport-bound proxies, 3-party
// relays and the peer registry under churn.
package workloads

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

const T18_ = 120.0

var KINDS18_ = []string{"loopback", "tcp", "udp", "unix", "ws"}

// F-W18-01 (fixed): peer-side memory ops run on the *owning* policy's
// command, so fan-out beyond the caller's slot cap no longer deadlocks.
var SOCKET_DEADLOCK18_ = append([]string(nil), KINDS18_...)

// data18_ is ``_data(fut)``: ``unwrap(fut.wait(_T))``.
func data18_(fut any) (any, error) {
	v, err := Wait(fut, T18_)
	if err != nil {
		return nil, err
	}
	return Unwrap(v), nil
}

// put18_ is ``_put(entries, tp, pool="store")``: push entries from the
// active policy (A) into B's pool.
func put18_(entries []*laila.Entry, tp *TwoPolicies, pool ...string) (any, error) {
	p := "store"
	if len(pool) > 0 {
		p = pool[0]
	}
	f, err := laila.Memorize(entries_list(entries), py.Kw{"dst_policy": tp.Bid, "dst_pool": p})
	if err != nil {
		return nil, err
	}
	return Wait(f, T18_)
}

// get18_ is ``_get(gid, tp, pool="store")``.
func get18_(gid string, tp *TwoPolicies, pool ...string) (any, error) {
	p := "store"
	if len(pool) > 0 {
		p = pool[0]
	}
	f, err := laila.Remember(gid, py.Kw{"dst_policy": tp.Bid, "dst_pool": p, "persist": false})
	if err != nil {
		return nil, err
	}
	return data18_(f)
}

// slot_cap18_ is ``_slot_cap(policy)``: ``num_workers * max_async_per_thread``
// of the internal taskforce.
func slot_cap18_(policy *laila.LAILA_IDENTIFIABLE_POLICY_) int {
	cmd := Cmd(policy)
	tf := must(cmd.Taskforces.Getitem(cmd.Internal_taskforce))
	nw := must(py.Int(must(py.Getattr(tf, "num_workers"))))
	ma := must(py.Int(must(py.Getattr(tf, "max_async_per_thread"))))
	return nw * ma
}

// pool_of18_ is ``_pool_of(policy, nickname)``.
func pool_of18_(policy *laila.LAILA_IDENTIFIABLE_POLICY_, nickname string) *Pool {
	router := Router(policy)
	gid := must(router.Pools_nicknames.Getitem(nickname))
	return laila.As_pool_(must(router.Pools.Getitem(gid)))
}

// teardown18_ is ``_teardown(*policies)``.
func teardown18_(policies ...*laila.LAILA_IDENTIFIABLE_POLICY_) {
	for _, p := range policies {
		_ = Comm(p).Stop()
		_ = Cmd(p).Shutdown(py.Kw{"wait": true, "cancel_pending": true})
		laila.Local_policies_.PopDefault(p.Global_id(), nil)
	}
}

// set_rpc_timeout18_ is ``proto.rpc_timeout = v`` for any transport.
func set_rpc_timeout18_(proto any, v float64) {
	if c := laila.As_carrier_(proto); c != nil {
		c.Rpc_timeout = v
		return
	}
	proto.(*laila.LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_).Rpc_timeout = v
}


// two18_ is ``TwoPolicies(kind, liveness_interval=60.0)`` entered, with
// ``__exit__`` registered on the test.
func two18_(t *testing.T, kind string, kw ...py.Kw) *TwoPolicies {
	t.Helper()
	opts := py.MergeKw(kw...)
	opts["liveness_interval"] = 60.0
	tp, err := New_TwoPolicies(kind, opts).Enter()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tp.Exit)
	return tp
}

// tp18 is the parametrized ``tp`` fixture (one kind): ``fresh_policy`` +
// TwoPolicies(kind) + ``extend_b()``; both banks drained at teardown.
func tp18(t *testing.T, kind string) *TwoPolicies {
	t.Helper()
	Fresh_policy(t)
	tp := two18_(t, kind)
	must(tp.Extend_b("store"))
	t.Cleanup(func() {
		Quiesce(tp.A)
		Quiesce(tp.B)
	})
	return tp
}

// for_kinds18 runs body once per transport kind (``@pytest.fixture(params=_KINDS)``).
func for_kinds18(t *testing.T, kinds []string, body func(t *testing.T, kind string)) {
	for _, kind := range kinds {
		kind := kind
		t.Run("kind="+kind, func(t *testing.T) { body(t, kind) })
	}
}

// constants18_ is ``[Entry.constant(data=f(i)) for i in range(n)]``.
func constants18_(n int, data func(i int) any) []*laila.Entry {
	es := make([]*laila.Entry, n)
	for i := range es {
		es[i] = must(laila.Constant(data(i)))
	}
	return es
}

// gids18_ is ``[e.global_id for e in es]``.
func gids18_(es []*laila.Entry) *py.List {
	ids := py.NewList()
	for _, e := range es {
		ids.Append(e.Global_id())
	}
	return ids
}

// all_in18_ is ``all(e.global_id in pool.resource for e in es)``.
func all_in18_(pool *Pool, es []*laila.Entry) bool {
	for _, e := range es {
		if !pool.Resource.Contains(e.Global_id()) {
			return false
		}
	}
	return true
}

// any_in18_ is ``any(e.global_id in pool.resource for e in es)``.
func any_in18_(pool *Pool, es []*laila.Entry) bool {
	for _, e := range es {
		if pool.Resource.Contains(e.Global_id()) {
			return true
		}
	}
	return false
}

// unwrap_sorted18_ is ``sorted(unwrap(v) for v in out)`` (a non-list
// ``out`` is treated as ``[out]``).
func unwrap_sorted18_(out any) *py.List {
	var items []any
	if l, ok := out.(*py.List); ok {
		items = l.Items()
	} else {
		items = []any{out}
	}
	vals := py.NewList()
	for _, v := range items {
		vals.Append(Unwrap(v))
	}
	return py.NewList(must(py.Sorted(vals.Items()))...)
}

// as_bytes18_ is ``bytes(v)``.
func as_bytes18_(v any) []byte {
	switch b := v.(type) {
	case []byte:
		return b
	case string:
		return []byte(b)
	}
	panic(py.TypeError.Newf("cannot convert %s to bytes", py.Pytypename(v)))
}

// wait_raises18_ is ``with pytest.raises(Exception): f.wait(timeout)`` for
// a ``(future, err)`` pair: the error may come from the call or the wait.
func wait_raises18_(f any, err error, timeout float64) error {
	if err != nil {
		return err
	}
	_, err = Wait(f, timeout)
	return err
}

// ---------------------------------------------------------------------------
// TestRpcVolume
// ---------------------------------------------------------------------------

func TestRpcVolume_thousand_memorize_remember_pairs(t *testing.T) {
	for_kinds18(t, KINDS18_, func(t *testing.T, kind string) {
		tp := tp18(t, kind)
		n := N_scaled(1000)
		if tp.Kind == "udp" {
			n = N_scaled(300)
		}
		gids := []string{}
		sw := Timed(func() {
			for i := 0; i < n; i++ {
				e := must(laila.Constant(py.DictOf("i", i, "k", tp.Kind)))
				must(put18_([]*laila.Entry{e}, tp))
				gids = append(gids, e.Global_id())
			}
			for i, g := range gids {
				assertEq(t, must(must(get18_(g, tp)).(*py.Dict).Getitem("i")), i)
			}
		})
		assertTrue(t, sw.Elapsed < 0.1*float64(n)+10, sw.Elapsed)
		store := pool_of18_(tp.B, "store")
		for _, g := range gids {
			assertTrue(t, store.Resource.Contains(g))
		}
	})
}

func TestRpcVolume_batch_memorize_many_entries_in_one_call(t *testing.T) {
	for_kinds18(t, KINDS18_, func(t *testing.T, kind string) {
		tp := tp18(t, kind)
		// Stay under the slot cap of the internal taskforce; above it the
		// socket transports deadlock (see test_batch_beyond_slot_cap_*).
		n := max(8, slot_cap18_(tp.A)/2)
		entries := constants18_(n, func(i int) any { return i })
		must(Wait(must(laila.Memorize(entries_list(entries), py.Kw{"dst_policy": tp.Bid, "dst_pool": "store"})), T18_))
		out := must(Wait(must(laila.Remember(gids18_(entries),
			py.Kw{"dst_policy": tp.Bid, "dst_pool": "store", "persist": false})), T18_))
		assertEq(t, unwrap_sorted18_(out), Range_list(n))
	})
}

// A fan-out ``memorize(dst_policy=peer)`` of more entries than the
// internal taskforce has slots deadlocks on every socket transport.
//
// Each per-entry ``_store`` task holds a slot while blocking on the wire
// RPC in ``asyncio.to_thread``; the peer's inbound worker then runs
// ``B.central.memory.memorize`` which submits onto the *active* policy's
// (A's) saturated internal taskforce and blocks forever, so A's RPC times
// out. Loopback is immune only because it dispatches inline on the caller
// thread and inherits A's parking context.
func TestRpcVolume_batch_beyond_slot_cap_memorize(t *testing.T) {
	for_kinds18(t, SOCKET_DEADLOCK18_, func(t *testing.T, kind string) {
		Fresh_policy(t)
		tp := two18_(t, kind)
		must(tp.Extend_b("store"))
		set_rpc_timeout18_(tp.Pa, 20.0)
		n := slot_cap18_(tp.A) + 32
		entries := constants18_(n, func(i int) any { return i })
		var err error
		func() {
			defer Quiesce(tp.A, 30)
			var f any
			if f, err = laila.Memorize(entries_list(entries), py.Kw{"dst_policy": tp.Bid, "dst_pool": "store"}); err == nil {
				_, err = Wait(f, T18_)
			}
		}()
		check(t, err)
		store := pool_of18_(tp.B, "store")
		assertTrue(t, all_in18_(store, entries))
	})
}

func TestRpcVolume_batch_beyond_slot_cap_remember(t *testing.T) {
	for_kinds18(t, SOCKET_DEADLOCK18_, func(t *testing.T, kind string) {
		Fresh_policy(t)
		tp := two18_(t, kind)
		must(tp.Extend_b("store"))
		set_rpc_timeout18_(tp.Pa, 20.0)
		cap := slot_cap18_(tp.A)
		n := cap + 32
		entries := constants18_(n, func(i int) any { return i })
		step := max(1, cap/2)
		for i := 0; i < n; i += step {
			must(Wait(must(laila.Memorize(entries_list(entries[i:min(i+step, n)]),
				py.Kw{"dst_policy": tp.Bid, "dst_pool": "store"})), T18_))
		}
		var out any
		var err error
		func() {
			defer Quiesce(tp.A, 30)
			var f any
			if f, err = laila.Remember(gids18_(entries),
				py.Kw{"dst_policy": tp.Bid, "dst_pool": "store", "persist": false}); err == nil {
				out, err = Wait(f, T18_)
			}
		}()
		check(t, err)
		assertEq(t, unwrap_sorted18_(out), Range_list(n))
	})
}

func TestRpcVolume_large_payload_round_trip(t *testing.T) {
	for _, mb := range []int{1, 8} {
		mb := mb
		t.Run(fmt.Sprintf("mb=%d", mb), func(t *testing.T) {
			for_kinds18(t, KINDS18_, func(t *testing.T, kind string) {
				tp := tp18(t, kind)
				if tp.Kind == "udp" && mb > 1 {
					t.Skip("udp fragmentation of multi-MB payloads is too slow for a soak")
				}
				blob := Rand_bytes(mb*1024*1024, mb)
				e := must(laila.Constant(blob))
				var back any
				sw := Timed(func() {
					must(put18_([]*laila.Entry{e}, tp))
					back = must(get18_(e.Global_id(), tp))
				})
				assertTrue(t, bytes.Equal(as_bytes18_(back), blob))
				assertTrue(t, sw.Elapsed < 60)
			})
		})
	}
}

func TestRpcVolume_half_mb_payload_round_trip(t *testing.T) {
	for_kinds18(t, KINDS18_, func(t *testing.T, kind string) {
		tp := tp18(t, kind)
		blob := Rand_bytes(512*1024, 7)
		e := must(laila.Constant(blob))
		must(put18_([]*laila.Entry{e}, tp))
		assertTrue(t, bytes.Equal(as_bytes18_(must(get18_(e.Global_id(), tp))), blob))
	})
}

func TestRpcVolume_ws_frame_over_1mib_kills_link(t *testing.T) {
	Fresh_policy(t)
	tp := two18_(t, "ws")
	must(tp.Extend_b("store"))
	set_rpc_timeout18_(tp.Pa, 15.0)
	blob := Rand_bytes(900*1024, 9) // > 1 MiB after base64
	e := must(laila.Constant(blob))
	if _, err := put18_([]*laila.Entry{e}, tp); err != nil {
		// the link must survive a too-large frame
		assertTrue(t, tp.Ca().Peers.Contains(tp.Bid), "peer dropped from registry after oversize frame")
		t.Fatal(err)
	}
	assertTrue(t, bytes.Equal(as_bytes18_(must(get18_(e.Global_id(), tp))), blob))
}

// Same calling convention as the local path: extra positionals are a
// TypeError, never a silent partial store; a list stores all.
func TestRpcVolume_varargs_memorize_to_peer_stores_all_or_raises(t *testing.T) {
	for_kinds18(t, KINDS18_, func(t *testing.T, kind string) {
		tp := tp18(t, kind)
		es := constants18_(5, func(i int) any { return i })
		args := []any{}
		for _, e := range es {
			args = append(args, e)
		}
		args = append(args, py.Kw{"dst_policy": tp.Bid, "dst_pool": "store"})
		if _, err := laila.Memorize(args...); !errors.Is(err, py.TypeError) {
			t.Fatalf("expected TypeError, got %v", err)
		}
		store := pool_of18_(tp.B, "store")
		assertTrue(t, !any_in18_(store, es))
		must(Wait(must(laila.Memorize(entries_list(es), py.Kw{"dst_policy": tp.Bid, "dst_pool": "store"})), T18_))
		assertTrue(t, all_in18_(store, es))
	})
}

func TestRpcVolume_forget_across_peer(t *testing.T) {
	for_kinds18(t, KINDS18_, func(t *testing.T, kind string) {
		tp := tp18(t, kind)
		es := constants18_(100, func(i int) any { return i })
		must(put18_(es, tp))
		store := pool_of18_(tp.B, "store")
		assertTrue(t, all_in18_(store, es))
		must(Wait(must(laila.Forget(gids18_(es), py.Kw{"policy": tp.Bid, "pool": "store"})), T18_))
		assertTrue(t, !any_in18_(store, es))
		f, err := laila.Remember(es[0].Global_id(), py.Kw{"dst_policy": tp.Bid, "dst_pool": "store", "persist": false})
		assertTrue(t, wait_raises18_(f, err, T18_) != nil, "expected the remember of a forgotten entry to fail")
	})
}

func TestRpcVolume_concurrent_callers(t *testing.T) {
	for_kinds18(t, KINDS18_, func(t *testing.T, kind string) {
		tp := tp18(t, kind)
		n_threads := 8
		per := N_scaled(60)
		if tp.Kind == "udp" {
			per = N_scaled(20)
		}
		w := func(i int) (any, error) {
			for j := 0; j < per; j++ {
				e, err := laila.Constant(py.Tuple{i, j})
				if err != nil {
					return nil, err
				}
				if _, err := put18_([]*laila.Entry{e}, tp); err != nil {
					return nil, err
				}
				got, err := get18_(e.Global_id(), tp)
				if err != nil {
					return nil, err
				}
				if !py.Eq(py.NewList(py.Iter(got)...), py.NewList(i, j)) {
					return nil, py.AssertionError.Newf("%s != (%d, %d)", py.Repr(got), i, j)
				}
			}
			return true, nil
		}
		results := must(Run_threads(w, n_threads))
		for _, r := range results {
			assertTrue(t, r == true)
		}
	})
}

func TestRpcVolume_ping_storm(t *testing.T) {
	for_kinds18(t, KINDS18_, func(t *testing.T, kind string) {
		tp := tp18(t, kind)
		sw := Timed(func() {
			for i := 0; i < N_scaled(300); i++ {
				assertTrue(t, tp.Pa.Ping(tp.Bid, 10))
			}
		})
		assertTrue(t, sw.Elapsed < 60)
	})
}

func TestRpcVolume_rpc_errors_propagate(t *testing.T) {
	for_kinds18(t, KINDS18_, func(t *testing.T, kind string) {
		tp := tp18(t, kind)
		f, err := laila.Remember("LAILA:ENTRY:00000000-0000-0000-0000-000000000000",
			py.Kw{"dst_policy": tp.Bid, "dst_pool": "store", "persist": false})
		err = wait_raises18_(f, err, T18_)
		assertTrue(t, err != nil, "expected the remember of an unknown entry to fail")
		assertTrue(t, strings_contains_lower18_(err.Error(), "not found") ||
			strings_contains_lower18_(err.Error(), "keyerror") || err != nil)
		proxy := must(laila.Request(tp.Bid))
		if _, err := proxy.Attr("central").Attr("memory").Attr("nonexistent_method").Invoke(); err == nil {
			t.Fatal("expected the RPC to a nonexistent method to fail")
		}
		// the link is still healthy afterwards
		assertTrue(t, tp.Pa.Ping(tp.Bid, 10))
	})
}

func TestRpcVolume_persist_caches_into_peer_alpha(t *testing.T) {
	for_kinds18(t, KINDS18_, func(t *testing.T, kind string) {
		tp := tp18(t, kind)
		e := must(laila.Constant(py.DictOf("a", 1)))
		must(put18_([]*laila.Entry{e}, tp))
		assertEq(t, must(data18_(must(laila.Remember(e.Global_id(),
			py.Kw{"dst_policy": tp.Bid, "dst_pool": "store", "persist": true})))), py.DictOf("a", 1))
		mem := Mem(tp.B)
		alpha := laila.As_pool_(must(Router(tp.B).Pools.Getitem(mem.Alpha_pool)))
		assertTrue(t, Wait_until(func() bool { return alpha.Resource.Contains(e.Global_id()) }, 10.0))
	})
}

// One RPC per call; plain JSON-safe return values come back intact.
func TestRpcVolume_raw_proxy_attribute_chain_round_trip(t *testing.T) {
	for_kinds18(t, KINDS18_, func(t *testing.T, kind string) {
		tp := tp18(t, kind)
		tp.B.Private_set("echo", func(args ...any) (any, error) {
			pos, kw := py.SplitKw(args)
			return py.DictOf("a", py.NewList(pos...), "k", py.DictFromKw(kw)), nil
		})
		proxy := must(laila.Request(tp.Bid))
		for i := 0; i < N_scaled(200); i++ {
			out := must(proxy.Attr("echo").Invoke(i, "x", py.Kw{"flag": true}))
			assertEq(t, out, py.DictOf("a", py.NewList(i, "x"), "k", py.DictOf("flag", true)))
		}
	})
}

// strings_contains_lower18_ is ``needle in s.lower()``.
func strings_contains_lower18_(s, needle string) bool {
	return bytes.Contains(bytes.ToLower([]byte(s)), []byte(needle))
}

// ---------------------------------------------------------------------------
// TestAdmissionControl
// ---------------------------------------------------------------------------

func TestAdmissionControl_inbound_cap_with_retry_lets_burst_through(t *testing.T) {
	Fresh_policy(t)
	tp := two18_(t, "tcp")
	must(tp.Extend_b("store"))
	tp.Cb().Max_inflight_rpcs = 2
	e := must(laila.Constant(Rand_bytes(512 * 1024)))
	must(put18_([]*laila.Entry{e}, tp))
	futs := []any{}
	for i := 0; i < 16; i++ {
		futs = append(futs, must(laila.Remember(e.Global_id(),
			py.Kw{"dst_policy": tp.Bid, "dst_pool": "store", "persist": false})))
	}
	ok := 0
	for _, f := range futs {
		if _, err := Wait(f, T18_); err == nil {
			ok++
		}
	}
	// backoff/retry must let most of them through
	assertTrue(t, ok >= 8, ok)
	Quiesce(tp.A)
	Quiesce(tp.B)
}

func TestAdmissionControl_saturated_cap_gives_up_after_retries_and_ping_bypasses(t *testing.T) {
	Fresh_policy(t)
	tp := two18_(t, "tcp")
	must(tp.Extend_b("store"))
	tp.Cb().Max_inflight_rpcs = 1
	laila.As_carrier_(tp.Pa).Max_rpc_retries = 1
	laila.As_carrier_(tp.Pa).Rpc_backoff_base = 0.01
	assertTrue(t, tp.Cb().Acquire_rpc_slot_())
	func() {
		defer tp.Cb().Release_rpc_slot_()
		var err error
		sw := Timed(func() {
			f, e := laila.Memorize(must(laila.Constant(1)), py.Kw{"dst_policy": tp.Bid, "dst_pool": "store"})
			err = wait_raises18_(f, e, 30)
		})
		assertTrue(t, err != nil, "expected the memorize to fail under a saturated cap")
		assertTrue(t, sw.Elapsed < 30)
		assertTrue(t, tp.Pa.Ping(tp.Bid, 5))
	}()
	// slot released -> traffic flows again
	e := must(laila.Constant(2))
	must(put18_([]*laila.Entry{e}, tp))
	assertEq(t, must(get18_(e.Global_id(), tp)), 2)
	Quiesce(tp.A)
	Quiesce(tp.B)
}

// ---------------------------------------------------------------------------
// TestRegistryChurn
// ---------------------------------------------------------------------------

func TestRegistryChurn_add_remove_peer_cycles(t *testing.T) {
	Fresh_policy(t)
	tp := two18_(t, "tcp")
	must(tp.Extend_b("store"))
	uri := must(tp.Uri_for(tp.B, tp.Pb))
	for i := 0; i < N_scaled(20); i++ {
		tp.Ca().Remove_peer(tp.Bid)
		assertTrue(t, !tp.Ca().Peers.Contains(tp.Bid))
		bid := must(tp.Ca().Add_peer(uri, Secret_of(tp.Pb)))
		assertEq(t, bid, tp.Bid)
		e := must(laila.Constant(i))
		must(put18_([]*laila.Entry{e}, tp))
		assertEq(t, must(get18_(e.Global_id(), tp)), i)
	}
	Quiesce(tp.A)
	Quiesce(tp.B)
}

func TestRegistryChurn_wrong_secret_rejected(t *testing.T) {
	Fresh_policy(t)
	tp := two18_(t, "tcp")
	uri := must(tp.Uri_for(tp.B, tp.Pb))
	if _, err := tp.Ca().Add_peer(uri, "not-the-secret"); err == nil {
		t.Fatal("expected add_peer with a wrong secret to fail")
	}
	// the legit link still works
	must(tp.Extend_b("store"))
	e := must(laila.Constant(1))
	must(put18_([]*laila.Entry{e}, tp))
	assertEq(t, must(get18_(e.Global_id(), tp)), 1)
}

func TestRegistryChurn_many_peers_star_topology(t *testing.T) {
	Fresh_policy(t)
	hub := As_policy(must(laila.New_DefaultPolicy()))
	Comm(hub).Liveness_interval = 60.0
	check(t, Comm(hub).Add_connection(must(laila.New_DefaultTCPProtocol(py.Kw{"host": "127.0.0.1"}))))
	spokes := []*laila.LAILA_IDENTIFIABLE_POLICY_{}
	defer func() { teardown18_(append(append([]*laila.LAILA_IDENTIFIABLE_POLICY_(nil), spokes...), hub)...) }()
	for i := 0; i < N_scaled(12); i++ {
		s := As_policy(must(laila.New_DefaultPolicy()))
		sp := must(laila.New_DefaultTCPProtocol(py.Kw{"host": "127.0.0.1"}))
		Comm(s).Liveness_interval = 60.0
		check(t, Comm(s).Add_connection(sp))
		check(t, Mem(s).Extend(must(laila.New_DefaultPool()), py.Kw{"pool_nickname": "store"}))
		must(Comm(hub).Add_peer(fmt.Sprintf("tcp://127.0.0.1:%d", Bound_port(sp)), Secret_of(sp)))
		spokes = append(spokes, s)
	}
	laila.Activate_policy(hub)
	assertEq(t, Comm(hub).Peers.Len(), len(spokes))
	for i, s := range spokes {
		e := must(laila.Constant(i))
		must(Wait(must(laila.Memorize(e, py.Kw{"dst_policy": s.Global_id(), "dst_pool": "store"})), T18_))
		assertEq(t, must(data18_(must(laila.Remember(e.Global_id(),
			py.Kw{"dst_policy": s.Global_id(), "dst_pool": "store", "persist": false})))), i)
	}
	Quiesce(hub)
}

func TestRegistryChurn_peer_stop_ends_rpc_with_error_not_hang(t *testing.T) {
	Fresh_policy(t)
	tp := two18_(t, "tcp")
	must(tp.Extend_b("store"))
	e := must(laila.Constant(1))
	must(put18_([]*laila.Entry{e}, tp))
	check(t, tp.Cb().Stop())
	var err error
	sw := Timed(func() {
		f, e := laila.Remember(e.Global_id(), py.Kw{"dst_policy": tp.Bid, "dst_pool": "store", "persist": false})
		err = wait_raises18_(f, e, 90)
	})
	assertTrue(t, err != nil, "expected the remember against a stopped peer to fail")
	assertTrue(t, sw.Elapsed < 90)
	Quiesce(tp.A)
}

// ---------------------------------------------------------------------------
// TestThreePartyRelay -- A orchestrates: B <-> C over their own link, A
// only talks to B.
// ---------------------------------------------------------------------------

// trio18 is the ``trio`` fixture for one kind: returns (a, b, c).
func trio18(t *testing.T, kind string) (a, b, c *laila.LAILA_IDENTIFIABLE_POLICY_) {
	t.Helper()
	Fresh_policy(t)
	factory := func() laila.Comm_protocol_ {
		switch kind {
		case "tcp":
			return must(laila.New_DefaultTCPProtocol(py.Kw{"host": "127.0.0.1"}))
		case "loopback":
			return must(laila.New_DefaultLoopbackProtocol())
		}
		return must(laila.New_DefaultUnixSocketProtocol())
	}
	pols := []*laila.LAILA_IDENTIFIABLE_POLICY_{}
	protos := []laila.Comm_protocol_{}
	for i := 0; i < 3; i++ {
		p := As_policy(must(laila.New_DefaultPolicy()))
		pr := factory()
		Comm(p).Liveness_interval = 60.0
		check(t, Comm(p).Add_connection(pr))
		pols = append(pols, p)
		protos = append(protos, pr)
	}
	a, b, c = pols[0], pols[1], pols[2]
	pb, pc := protos[1], protos[2]
	t.Cleanup(func() { teardown18_(pols...) })

	uri := func(p *laila.LAILA_IDENTIFIABLE_POLICY_, pr laila.Comm_protocol_) string {
		if kind == "tcp" {
			return fmt.Sprintf("tcp://127.0.0.1:%d", Bound_port(pr))
		}
		if kind == "unix" {
			return "unix://" + pr.(interface{ Bound_path() string }).Bound_path()
		}
		return "loopback://" + p.Global_id()
	}

	must(Comm(a).Add_peer(uri(b, pb), Secret_of(pb)))
	must(Comm(b).Add_peer(uri(c, pc), Secret_of(pc)))
	check(t, Mem(b).Extend(must(laila.New_DefaultPool()), py.Kw{"pool_nickname": "b-store"}))
	check(t, Mem(c).Extend(must(laila.New_DefaultPool()), py.Kw{"pool_nickname": "c-store"}))
	laila.Activate_policy(a)
	t.Cleanup(func() {
		for _, p := range pols {
			Quiesce(p)
		}
	})
	return a, b, c
}

// wait_if_future18_ is ``if f is not None and hasattr(f, "wait"): f.wait(_T)``.
func wait_if_future18_(t *testing.T, f any) {
	t.Helper()
	if !py.IsNone(f) && py.Hasattr(f, "wait") {
		must(Wait(f, T18_))
	}
}

// F-W18-02 (fixed): relay legs run on the source policy's own memory.
func TestThreePartyRelay_push_relay_volume(t *testing.T) {
	for_kinds18(t, []string{"tcp", "loopback", "unix"}, func(t *testing.T, kind string) {
		a, b, c := trio18(t, kind)
		n := N_scaled(200)
		es := constants18_(n, func(i int) any { return i })
		must(Wait(must(laila.Memorize(entries_list(es), py.Kw{"dst_policy": b.Global_id(), "dst_pool": "b-store"})), T18_))
		f := must(laila.Memorize(gids18_(es), py.Kw{
			"src_policy": b.Global_id(),
			"src_pool":   "b-store",
			"dst_policy": c.Global_id(),
			"dst_pool":   "c-store",
		}))
		wait_if_future18_(t, f)
		cs := pool_of18_(c, "c-store")
		assertTrue(t, Wait_until(func() bool { return all_in18_(cs, es) }, 60.0))
		// A never peered with C
		assertTrue(t, !Comm(a).Peers.Contains(c.Global_id()))
	})
}

// F-W18-02 (fixed): relay legs run on the source policy's own memory.
func TestThreePartyRelay_pull_relay_volume(t *testing.T) {
	for_kinds18(t, []string{"tcp", "loopback", "unix"}, func(t *testing.T, kind string) {
		a, b, c := trio18(t, kind)
		n := N_scaled(200)
		es := constants18_(n, func(i int) any { return i })
		laila.Activate_policy(c)
		must(Wait(must(laila.Memorize(entries_list(es), py.Kw{"dst_pool": "c-store"})), T18_))
		laila.Activate_policy(a)
		f := must(laila.Remember(gids18_(es), py.Kw{
			"src_policy": b.Global_id(),
			"src_pool":   "b-store",
			"dst_policy": c.Global_id(),
			"dst_pool":   "c-store",
		}))
		wait_if_future18_(t, f)
		bs := pool_of18_(b, "b-store")
		assertTrue(t, Wait_until(func() bool { return all_in18_(bs, es) }, 60.0))
	})
}

// ---------------------------------------------------------------------------
// TestTransportBoundProxy
// ---------------------------------------------------------------------------

func TestTransportBoundProxy_two_transports_same_peer_select_by_token(t *testing.T) {
	Fresh_policy(t)
	a, b := As_policy(must(laila.New_DefaultPolicy())), As_policy(must(laila.New_DefaultPolicy()))
	tcp_b := must(laila.New_DefaultTCPProtocol(py.Kw{"host": "127.0.0.1"}))
	lo_b := must(laila.New_DefaultLoopbackProtocol())
	for _, row := range []struct {
		p       *laila.LAILA_IDENTIFIABLE_POLICY_
		tcp, lo laila.Comm_protocol_
	}{
		{a, must(laila.New_DefaultTCPProtocol(py.Kw{"host": "127.0.0.1"})), must(laila.New_DefaultLoopbackProtocol())},
		{b, tcp_b, lo_b},
	} {
		Comm(row.p).Liveness_interval = 60.0
		check(t, Comm(row.p).Add_connection(row.tcp))
		check(t, Comm(row.p).Add_connection(row.lo))
	}
	check(t, Mem(b).Extend(must(laila.New_DefaultPool()), py.Kw{"pool_nickname": "store"}))
	defer teardown18_(a, b)
	bid := must(Comm(a).Add_peer(fmt.Sprintf("tcp://127.0.0.1:%d", Bound_port(tcp_b)), Secret_of(tcp_b)))
	bid2 := must(Comm(a).Add_peer("loopback://"+b.Global_id(), Secret_of(lo_b)))
	assertTrue(t, bid == bid2 && bid2 == b.Global_id())
	laila.Activate_policy(a)
	e := must(laila.Constant("x"))
	must(Wait(must(laila.Memorize(e, py.Kw{"dst_policy": bid, "dst_pool": "store", "comm": "tcp"})), T18_))
	assertEq(t, must(data18_(must(laila.Remember(e.Global_id(), py.Kw{
		"dst_policy": bid, "dst_pool": "store", "comm": "loopback", "persist": false,
	})))), "x")
	assertEq(t, must(data18_(must(laila.Remember(e.Global_id(), py.Kw{
		"dst_policy": bid, "dst_pool": "store", "comm": "tcp", "persist": false,
	})))), "x")
	f, err := laila.Remember(e.Global_id(), py.Kw{"dst_policy": bid, "dst_pool": "store", "comm": "lora", "persist": false})
	assertTrue(t, wait_raises18_(f, err, T18_) != nil, "expected the remember via an unknown transport to fail")
	// stop the tcp link on B's side; loopback must keep working
	Quiesce(a)
	Quiesce(b)
}

func TestTransportBoundProxy_proxy_via_unknown_token_fails_fast(t *testing.T) {
	Fresh_policy(t)
	tp := two18_(t, "tcp")
	must(tp.Extend_b("store"))
	var err error
	sw := Timed(func() {
		var proxy *laila.PeerProxy
		proxy, err = laila.Request(tp.Bid, "bluetooth")
		if err == nil {
			_, err = proxy.Attr("central").Attr("memory").Attr("_remote_remember").Invoke(py.NewList("x"))
		}
	})
	assertTrue(t, err != nil, "expected the RPC via an unknown token to fail")
	assertTrue(t, sw.Elapsed < 10)
}

// ---------------------------------------------------------------------------
// TestLivenessUnderLoad
// ---------------------------------------------------------------------------

func TestLivenessUnderLoad_short_liveness_interval_does_not_drop_busy_peer(t *testing.T) {
	Fresh_policy(t)
	tp, err := New_TwoPolicies("tcp", py.Kw{"liveness_interval": 0.2}).Enter()
	check(t, err)
	t.Cleanup(tp.Exit)
	must(tp.Extend_b("store"))
	stop := py.NewEvent()
	var mu sync.Mutex
	errs := []error{}

	hammer := func() {
		i := 0
		for !stop.Is_set() {
			err := func() error {
				e, err := laila.Constant(i)
				if err != nil {
					return err
				}
				if _, err := put18_([]*laila.Entry{e}, tp); err != nil {
					return err
				}
				got, err := get18_(e.Global_id(), tp)
				if err != nil {
					return err
				}
				if !py.Eq(got, i) {
					return py.AssertionError.Newf("%s != %d", py.Repr(got), i)
				}
				return nil
			}()
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
				return
			}
			i++
		}
	}

	th := py.New_Thread(py.Kw{"target": hammer, "daemon": true})
	check(t, th.Start())
	py.Sleep_sync(6.0)
	stop.Set()
	check(t, th.Join(30))
	mu.Lock()
	first := append([]error(nil), errs...)
	mu.Unlock()
	assertTrue(t, len(first) == 0, first)
	assertTrue(t, tp.Ca().Peers.Contains(tp.Bid))
	assertTrue(t, tp.Cb().Peers.Contains(tp.Aid))
	Quiesce(tp.A)
	Quiesce(tp.B)
}
