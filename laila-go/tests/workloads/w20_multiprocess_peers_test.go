//go:build workload

// Port of tests/workloads/test_w20_multiprocess_peers.py.
//
// W20 -- real cross-process peers.
//
// The parent policy talks to laila policies running in child processes over
// TCP / WebSocket: memory volume, large payloads, concurrent callers,
// 3-party relays that cross three processes, star topologies and
// nickname-derived routing.
package workloads

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

const T20_ = 120.0

// attach20_ is ``_attach(kind, policy, liveness=60.0)``: a TCP (or
// WebSocket for ``ws``) protocol on 127.0.0.1 registered on *policy*.
func attach20_(t *testing.T, kind string, policy *laila.LAILA_IDENTIFIABLE_POLICY_, liveness ...float64) laila.Comm_protocol_ {
	t.Helper()
	lv := 60.0
	if len(liveness) > 0 {
		lv = liveness[0]
	}
	var proto laila.Comm_protocol_
	if kind == "ws" {
		proto = must(laila.New_DefaultTCPIPProtocol(py.Kw{"host": "127.0.0.1"}))
	} else {
		proto = must(laila.New_DefaultTCPProtocol(py.Kw{"host": "127.0.0.1"}))
	}
	Comm(policy).Liveness_interval = lv
	check(t, Comm(policy).Add_connection(proto))
	return proto
}

// child_peer20_ is ``with ChildPeer(nickname, kind=kind, home=disk_tmp,
// liveness=60.0, **kw) as cp`` with ``__exit__`` registered on the test.
func child_peer20_(t *testing.T, nickname string, kind string, home string, kw ...py.Kw) *ChildPeer {
	t.Helper()
	opts := py.MergeKw(kw...)
	opts["kind"], opts["home"], opts["liveness"] = kind, home, 60.0
	cp, err := New_ChildPeer(nickname, opts).Enter()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cp.Exit)
	return cp
}

// child20 is the parametrized ``child`` fixture (one kind): a child peer
// of the fresh policy; the fresh bank is drained and the peer removed at
// teardown.
func child20(t *testing.T, kind string) *ChildPeer {
	t.Helper()
	fresh := Fresh_policy(t)
	disk_tmp := Disk_tmp(t)
	attach20_(t, kind, fresh)
	cp := child_peer20_(t, fmt.Sprintf("child-%d-%s", os.Getpid(), kind), kind, disk_tmp)
	gid := must(laila.Add_peer(cp.Uri(), cp.Secret()))
	assertEq(t, gid, cp.Gid())
	t.Cleanup(func() {
		Quiesce(fresh)
		func() {
			defer func() { _ = recover() }()
			Comm(fresh).Remove_peer(gid)
		}()
	})
	return cp
}

// for_child_kinds20 runs body once per ``child`` fixture param.
func for_child_kinds20(t *testing.T, body func(t *testing.T, kind string)) {
	for _, kind := range []string{"tcp", "ws"} {
		kind := kind
		t.Run("kind="+kind, func(t *testing.T) { body(t, kind) })
	}
}

// put20_ is ``_put(entries, gid, pool="store")``.
func put20_(entries []*laila.Entry, gid string, pool ...string) (any, error) {
	p := "store"
	if len(pool) > 0 {
		p = pool[0]
	}
	f, err := laila.Memorize(entries_list(entries), py.Kw{"dst_policy": gid, "dst_pool": p})
	if err != nil {
		return nil, err
	}
	return Wait(f, T20_)
}

// get20_ is ``_get(eid, gid, pool="store")``.
func get20_(eid string, gid string, pool ...string) (any, error) {
	p := "store"
	if len(pool) > 0 {
		p = pool[0]
	}
	f, err := laila.Remember(eid, py.Kw{"dst_policy": gid, "dst_pool": p, "persist": false})
	if err != nil {
		return nil, err
	}
	v, err := Wait(f, T20_)
	if err != nil {
		return nil, err
	}
	return Unwrap(v), nil
}

// alive20_ is ``_alive(comm, gid)``.
func alive20_(comm *laila.LAILA_IDENTIFIABLE_COMMUNICATION_, gid string) bool {
	proto, err := comm.Select_protocol_for_peer_(gid, nil)
	if err != nil {
		return false
	}
	alive := false
	func() {
		defer func() { _ = recover() }()
		alive = proto.Ping(gid, 2)
	}()
	return alive
}

// remember_raises20_ is ``with pytest.raises(Exception): laila.remember(eid,
// dst_policy=gid, dst_pool="store", persist=False).wait(_T)``.
func remember_raises20_(t *testing.T, eid string, gid string) {
	t.Helper()
	f, err := laila.Remember(eid, py.Kw{"dst_policy": gid, "dst_pool": "store", "persist": false})
	if err == nil {
		_, err = Wait(f, T20_)
	}
	assertTrue(t, err != nil, "expected the remember to fail")
}

// w20_relay_b is the ``extra`` of the ``relay-b`` child: peer with C and
// expose a ``b-store`` pool.
func w20_relay_b(args map[string]string) error {
	if _, err := laila.Add_peer(args["uri"], args["secret"]); err != nil {
		return err
	}
	pool, err := laila.New_DefaultPool()
	if err != nil {
		return err
	}
	mem, err := laila.Memory()
	if err != nil {
		return err
	}
	return laila.As_central_memory_(mem).Extend(pool, py.Kw{"pool_nickname": "b-store"})
}

func init() {
	Child_extras["w20_relay_b"] = w20_relay_b
}

// ---------------------------------------------------------------------------
// TestCrossProcessMemory
// ---------------------------------------------------------------------------

func TestCrossProcessMemory_volume_round_trip(t *testing.T) {
	for_child_kinds20(t, func(t *testing.T, kind string) {
		child := child20(t, kind)
		n := N_scaled(600)
		sw := Timed(func() {
			es := constants18_(n, func(i int) any { return py.DictOf("i", i) })
			for _, e := range es {
				must(put20_([]*laila.Entry{e}, child.Gid()))
			}
			for i, e := range es {
				assertEq(t, must(must(get20_(e.Global_id(), child.Gid())).(*py.Dict).Getitem("i")), i)
			}
		})
		assertTrue(t, sw.Elapsed < 0.15*float64(n)+10)
	})
}

func TestCrossProcessMemory_batched_under_slot_cap(t *testing.T) {
	for_child_kinds20(t, func(t *testing.T, kind string) {
		child := child20(t, kind)
		cmd := Cmd(As_policy(must(laila.Get_active_policy())))
		tf := must(cmd.Taskforces.Getitem(cmd.Internal_taskforce))
		nw := must(py.Int(must(py.Getattr(tf, "num_workers"))))
		ma := must(py.Int(must(py.Getattr(tf, "max_async_per_thread"))))
		n := max(8, (nw*ma)/2)
		es := constants18_(n, func(i int) any { return i })
		must(put20_(es, child.Gid()))
		out := must(Wait(must(laila.Remember(gids18_(es),
			py.Kw{"dst_policy": child.Gid(), "dst_pool": "store", "persist": false})), T20_))
		assertEq(t, unwrap_sorted18_(out), Range_list(n))
	})
}

func TestCrossProcessMemory_payload_sizes(t *testing.T) {
	for _, kb := range []int{64, 512} {
		kb := kb
		t.Run(fmt.Sprintf("kb=%d", kb), func(t *testing.T) {
			for_child_kinds20(t, func(t *testing.T, kind string) {
				child := child20(t, kind)
				blob := Rand_bytes(kb*1024, kb)
				e := must(laila.Constant(blob))
				must(put20_([]*laila.Entry{e}, child.Gid()))
				assertTrue(t, bytes.Equal(as_bytes18_(must(get20_(e.Global_id(), child.Gid()))), blob))
			})
		})
	}
}

func TestCrossProcessMemory_eight_mb_over_tcp(t *testing.T) {
	fresh := Fresh_policy(t)
	disk_tmp := Disk_tmp(t)
	attach20_(t, "tcp", fresh)
	cp := child_peer20_(t, "big-tcp", "tcp", disk_tmp)
	must(laila.Add_peer(cp.Uri(), cp.Secret()))
	blob := Rand_bytes(8*1024*1024, 8)
	e := must(laila.Constant(blob))
	sw := Timed(func() {
		must(put20_([]*laila.Entry{e}, cp.Gid()))
		assertTrue(t, bytes.Equal(as_bytes18_(must(get20_(e.Global_id(), cp.Gid()))), blob))
	})
	assertTrue(t, sw.Elapsed < 60)
	Quiesce(fresh)
}

func TestCrossProcessMemory_concurrent_callers(t *testing.T) {
	for_child_kinds20(t, func(t *testing.T, kind string) {
		child := child20(t, kind)
		per := N_scaled(40)

		w := func(i int) (any, error) {
			for j := 0; j < per; j++ {
				e, err := laila.Constant(py.NewList(i, j))
				if err != nil {
					return nil, err
				}
				if _, err := put20_([]*laila.Entry{e}, child.Gid()); err != nil {
					return nil, err
				}
				got, err := get20_(e.Global_id(), child.Gid())
				if err != nil {
					return nil, err
				}
				if !py.Eq(got, py.NewList(i, j)) {
					return nil, py.AssertionError.Newf("%s != [%d, %d]", py.Repr(got), i, j)
				}
			}
			return true, nil
		}

		for _, r := range must(Run_threads(w, 6)) {
			assertTrue(t, r == true)
		}
	})
}

func TestCrossProcessMemory_forget_and_miss(t *testing.T) {
	for_child_kinds20(t, func(t *testing.T, kind string) {
		child := child20(t, kind)
		es := constants18_(50, func(i int) any { return i })
		must(put20_(es, child.Gid()))
		must(Wait(must(laila.Forget(gids18_(es), py.Kw{"policy": child.Gid(), "pool": "store"})), T20_))
		remember_raises20_(t, es[0].Global_id(), child.Gid())
	})
}

func TestCrossProcessMemory_evolving_variable_entry_across_processes(t *testing.T) {
	for_child_kinds20(t, func(t *testing.T, kind string) {
		child := child20(t, kind)
		e := must(laila.Variable(0, py.Kw{"nickname": "counter-x"}))
		must(put20_([]*laila.Entry{e}, child.Gid()))
		for i := 1; i < N_scaled(30); i++ {
			check(t, e.Set_data(i))
			must(put20_([]*laila.Entry{e}, child.Gid()))
		}
		latest := must(Wait(must(laila.Remember(e.Global_id(),
			py.Kw{"dst_policy": child.Gid(), "dst_pool": "store", "persist": false})), T20_))
		assertEq(t, Unwrap(latest), N_scaled(30)-1)
	})
}

// dst_policy may be the child's nickname (uuid5-derived gid).
func TestCrossProcessMemory_nickname_routing_to_child(t *testing.T) {
	for_child_kinds20(t, func(t *testing.T, kind string) {
		child := child20(t, kind)
		e := must(laila.Constant("by-nick"))
		must(Wait(must(laila.Memorize(e, py.Kw{"dst_policy": child.Nickname, "dst_pool": "store"})), T20_))
		assertEq(t, must(get20_(e.Global_id(), child.Nickname)), "by-nick")
	})
}

func TestCrossProcessMemory_ping_latency_distribution(t *testing.T) {
	for_child_kinds20(t, func(t *testing.T, kind string) {
		child := child20(t, kind)
		comm := Comm(As_policy(must(laila.Get_active_policy())))
		proto := comm.Connections.Values()[0].(laila.Comm_protocol_)
		lat := []float64{}
		for i := 0; i < N_scaled(200); i++ {
			sw := Timed(func() { assertTrue(t, proto.Ping(child.Gid(), 10)) })
			lat = append(lat, sw.Elapsed)
		}
		sort.Float64s(lat)
		assertTrue(t, lat[len(lat)/2] < 0.05, fmt.Sprintf("median ping %.1fms", lat[len(lat)/2]*1000))
		assertTrue(t, lat[int(float64(len(lat))*0.99)] < 1.0)
	})
}

// ---------------------------------------------------------------------------
// TestThreeProcessRelay -- A (parent) orchestrates B -> C where B and C
// are child processes.
// ---------------------------------------------------------------------------

// bc20 is the ``bc`` fixture: returns (b, c).
func bc20(t *testing.T) (*ChildPeer, *ChildPeer) {
	t.Helper()
	fresh := Fresh_policy(t)
	disk_tmp := Disk_tmp(t)
	attach20_(t, "tcp", fresh)
	c := child_peer20_(t, "relay-c", "tcp", disk_tmp)
	b := child_peer20_(t, "relay-b", "tcp", disk_tmp, py.Kw{
		"extra":      "w20_relay_b",
		"extra_args": map[string]string{"uri": fmt.Sprintf("tcp://127.0.0.1:%d", c.Port()), "secret": c.Secret()},
	})
	must(laila.Add_peer(b.Uri(), b.Secret()))
	t.Cleanup(func() { Quiesce(fresh) })
	return b, c
}

func TestThreeProcessRelay_push_relay_cross_process(t *testing.T) {
	b, c := bc20(t)
	n := N_scaled(60)
	es := constants18_(n, func(i int) any { return i })
	must(Wait(must(laila.Memorize(entries_list(es), py.Kw{"dst_policy": b.Gid(), "dst_pool": "b-store"})), T20_))
	f := must(laila.Memorize(gids18_(es), py.Kw{
		"src_policy": b.Gid(),
		"src_pool":   "b-store",
		"dst_policy": c.Gid(),
		"dst_pool":   "store",
	}))
	if !py.IsNone(f) && py.Hasattr(f, "wait") {
		must(Wait(f, T20_))
	}
	// A is not peered to C; verify through B's link by pulling back via B
	g := must(laila.Remember(gids18_(es), py.Kw{
		"src_policy": b.Gid(),
		"src_pool":   "store",
		"dst_policy": c.Gid(),
		"dst_pool":   "store",
	}))
	if !py.IsNone(g) && py.Hasattr(g, "wait") {
		must(Wait(g, T20_))
	}
	back := must(Wait(must(laila.Remember(gids18_(es),
		py.Kw{"dst_policy": b.Gid(), "dst_pool": "store", "persist": false})), T20_))
	assertEq(t, unwrap_sorted18_(back), Range_list(n))
}

// ---------------------------------------------------------------------------
// TestStar
// ---------------------------------------------------------------------------

func TestStar_four_children(t *testing.T) {
	fresh := Fresh_policy(t)
	disk_tmp := Disk_tmp(t)
	attach20_(t, "tcp", fresh)
	kids := []*ChildPeer{}
	defer func() {
		for _, cp := range kids {
			func() {
				defer func() { _ = recover() }()
				cp.Exit()
			}()
		}
	}()
	for i := 0; i < 4; i++ {
		cp := New_ChildPeer(fmt.Sprintf("star-%d", i), py.Kw{"kind": "tcp", "home": disk_tmp, "liveness": 60.0})
		must(cp.Enter())
		kids = append(kids, cp)
		must(laila.Add_peer(cp.Uri(), cp.Secret()))
	}
	n := N_scaled(100)
	for i, cp := range kids {
		es := constants18_(n, func(j int) any { return py.Tuple{i, j} })
		must(put20_(es, cp.Gid()))
	}
	// each child only has its own entries
	for i, cp := range kids {
		probe := must(laila.Constant("probe"))
		must(put20_([]*laila.Entry{probe}, cp.Gid()))
		assertEq(t, must(get20_(probe.Global_id(), cp.Gid())), "probe")
		other := kids[(i+1)%4]
		remember_raises20_(t, probe.Global_id(), other.Gid())
	}
	Quiesce(fresh)
}

func TestStar_child_restart_same_nickname_reconnects(t *testing.T) {
	fresh := Fresh_policy(t)
	disk_tmp := Disk_tmp(t)
	attach20_(t, "tcp", fresh)
	comm := Comm(fresh)
	var gid string
	var e *laila.Entry
	func() {
		cp := must(New_ChildPeer("phoenix", py.Kw{"kind": "tcp", "home": disk_tmp, "liveness": 60.0}).Enter())
		defer cp.Exit()
		gid = must(laila.Add_peer(cp.Uri(), cp.Secret()))
		e = must(laila.Constant(1))
		must(put20_([]*laila.Entry{e}, gid))
		cp.Kill()
		assertTrue(t, Wait_until(func() bool { return !alive20_(comm, gid) }, 30))
	}()
	func() {
		cp2 := must(New_ChildPeer("phoenix", py.Kw{"kind": "tcp", "home": disk_tmp, "liveness": 60.0}).Enter())
		defer cp2.Exit()
		assertTrue(t, cp2.Gid() == gid, "nickname-derived gid must be stable across restarts")
		func() {
			defer func() { _ = recover() }()
			comm.Remove_peer(gid)
		}()
		gid2 := must(laila.Add_peer(cp2.Uri(), cp2.Secret()))
		assertEq(t, gid2, gid)
		e2 := must(laila.Constant(2))
		must(put20_([]*laila.Entry{e2}, gid))
		assertEq(t, must(get20_(e2.Global_id(), gid)), 2)
		// the old entry lived only in the dead process
		remember_raises20_(t, e.Global_id(), gid)
	}()
	Quiesce(fresh)
}
