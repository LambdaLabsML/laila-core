//go:build workload

// Port of tests/workloads/test_w21_remote_futures_morph.py.
//
// W21 -- RemoteFuture lifecycle, morph mode and peer loss.
//
// A child process exposes task-submitting methods; the parent drives
// hundreds of RemoteFuture handles (status polling, wait,
// materialize/caching, exceptions, group futures, release / bank growth),
// activates the remote proxy ("morph mode") so plain ``laila.*`` calls RPC,
// and then kills / restarts the peer under in-flight work.
package workloads

import (
	"errors"
	"strings"
	"sync"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

const T21_ = 120.0

// w21_extra is ``_EXTRA``: the task-submitting methods bound on the child
// policy (``object.__setattr__(policy, name, fn)`` -> ``Private_set``).
//
//	async def _slow(n, delay): await asyncio.sleep(delay); return n * 2
//	async def _boom(msg): raise ValueError(msg)
//	def slow(n, delay=0.0): return laila.command.submit([lambda: _slow(n, delay)])
//	def boom(msg="kaboom"): return laila.command.submit([lambda: _boom(msg)])
//	def batch(k, delay=0.0): return laila.command.submit([(lambda i=i: _slow(i, delay)) for i in range(k)])
//	def bank_size(): return len(policy.future_bank)
//	def echo(x): return x
func w21_extra(args map[string]string) error {
	policy := As_policy(must(laila.Get_active_policy()))
	cmd := Cmd(policy)

	slow_ := func(n int, delay float64) py.CoroutineFunc {
		return py.CoroutineFunc(func(...any) (any, error) {
			if err := py.Sleep(delay); err != nil {
				return nil, err
			}
			return n * 2, nil
		})
	}
	boom_ := func(msg string) py.CoroutineFunc {
		return py.CoroutineFunc(func(...any) (any, error) { return nil, py.ValueError.New(msg) })
	}
	// delay_of is ``delay=0.0`` from the positional or keyword form.
	delay_of := func(pos []any, kw py.Kw, idx int) (float64, error) {
		if len(pos) > idx {
			return py.Float(pos[idx])
		}
		if v, ok := kw["delay"]; ok {
			return py.Float(v)
		}
		return 0.0, nil
	}

	policy.Private_set("slow", func(args ...any) (any, error) {
		pos, kw := py.SplitKw(args)
		n, err := py.Int(pos[0])
		if err != nil {
			return nil, err
		}
		delay, err := delay_of(pos, kw, 1)
		if err != nil {
			return nil, err
		}
		return cmd.Submit([]any{slow_(n, delay)})
	})
	policy.Private_set("boom", func(args ...any) (any, error) {
		pos, kw := py.SplitKw(args)
		msg := "kaboom"
		if len(pos) > 0 {
			msg = py.Str(pos[0])
		} else if v, ok := kw["msg"]; ok {
			msg = py.Str(v)
		}
		return cmd.Submit([]any{boom_(msg)})
	})
	policy.Private_set("batch", func(args ...any) (any, error) {
		pos, kw := py.SplitKw(args)
		k, err := py.Int(pos[0])
		if err != nil {
			return nil, err
		}
		delay, err := delay_of(pos, kw, 1)
		if err != nil {
			return nil, err
		}
		tasks := make([]any, k)
		for i := range tasks {
			tasks[i] = slow_(i, delay)
		}
		return cmd.Submit(tasks)
	})
	policy.Private_set("bank_size", func() int { return policy.Future_bank.Len() })
	policy.Private_set("echo", func(x any) any { return x })
	return nil
}

func init() {
	Child_extras["w21_rf"] = w21_extra
}

// attach21_ is ``_attach(policy, liveness=60.0)``: a TCP protocol on
// 127.0.0.1 registered on *policy*.
func attach21_(t *testing.T, policy *laila.LAILA_IDENTIFIABLE_POLICY_, liveness ...float64) *laila.CarrierRPCProtocol_ {
	t.Helper()
	lv := 60.0
	if len(liveness) > 0 {
		lv = liveness[0]
	}
	proto := must(laila.New_DefaultTCPProtocol(py.Kw{"host": "127.0.0.1"}))
	Comm(policy).Liveness_interval = lv
	check(t, Comm(policy).Add_connection(proto))
	return laila.As_carrier_(proto)
}

// child21_ is ``ChildPeer(nickname, kind="tcp", home=disk_tmp,
// liveness=60.0, extra=_EXTRA)`` entered.
func child21_(t *testing.T, nickname string, home string) *ChildPeer {
	t.Helper()
	cp, err := New_ChildPeer(nickname, py.Kw{"kind": "tcp", "home": home, "liveness": 60.0, "extra": "w21_rf"}).Enter()
	if err != nil {
		t.Fatal(err)
	}
	return cp
}

// peer21 is the ``peer`` fixture: the ``rf-child`` peer of the fresh
// policy. Returns the child and the fresh policy.
func peer21(t *testing.T) (*ChildPeer, *laila.LAILA_IDENTIFIABLE_POLICY_) {
	t.Helper()
	fresh := Fresh_policy(t)
	disk_tmp := Disk_tmp(t)
	attach21_(t, fresh)
	cp := child21_(t, "rf-child", disk_tmp)
	t.Cleanup(cp.Exit)
	gid := must(laila.Add_peer(cp.Uri(), cp.Secret()))
	assertEq(t, gid, cp.Gid())
	t.Cleanup(func() { Quiesce(fresh) })
	return cp, fresh
}

// proxy21_ is ``_proxy(cp)``.
func proxy21_(cp *ChildPeer) *laila.PeerProxy { return must(laila.Request(cp.Gid())) }

// rf21_ is ``proxy.<name>(*args)`` typed as the RemoteFuture it returns.
func rf21_(t *testing.T, proxy *laila.PeerProxy, name string, args ...any) *laila.RemoteFuture {
	t.Helper()
	v, err := proxy.Attr(name).Invoke(args...)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	f, ok := v.(*laila.RemoteFuture)
	if !ok {
		t.Fatalf("%s returned %s, want RemoteFuture", name, py.Repr(v))
	}
	return f
}

// unwrap_wait21_ is ``unwrap(f.wait(timeout))``.
func unwrap_wait21_(f *laila.RemoteFuture, timeout float64) (any, error) {
	v, err := f.Wait(timeout)
	if err != nil {
		return nil, err
	}
	return Unwrap(v), nil
}

// doubles21_ is ``[2 * i for i in range(n)]``.
func doubles21_(n int) *py.List {
	out := py.NewList()
	for i := 0; i < n; i++ {
		out.Append(2 * i)
	}
	return out
}

// only_local21_ is ``_only_local(policy)``: morph mode can only register
// RemoteFutures when exactly one local policy exists
// (``_get_active_local_policy``); narrow the registry to *policy* for the
// duration so the morph path itself can be exercised. Returns the restore
// function.
func only_local21_(policy *laila.LAILA_IDENTIFIABLE_POLICY_) func() {
	saved := laila.Local_policies_.Copy()
	laila.Local_policies_.Clear()
	laila.Local_policies_.Set(policy.Global_id(), policy)
	return func() {
		laila.Local_policies_.Clear()
		laila.Local_policies_.Update(saved)
	}
}

// peers21_ is ``laila.peers[gid]``.
func peers21_(gid string) *laila.PeerProxy {
	return must(must(laila.Peers()).(*laila.PeerRegistry).Getitem(gid)).(*laila.PeerProxy)
}

// wait_morph21_ is ``unwrap(r.wait(_T) if hasattr(r, "wait") else r)``.
func wait_morph21_(r any) (any, error) {
	if py.Hasattr(r, "wait") {
		v, err := Wait(r, T21_)
		if err != nil {
			return nil, err
		}
		return Unwrap(v), nil
	}
	return Unwrap(r), nil
}

// wait_if_future21_ is ``if f is not None and hasattr(f, "wait"): f.wait(_T)``.
func wait_if_future21_(f any) error {
	if !py.IsNone(f) && py.Hasattr(f, "wait") {
		_, err := Wait(f, T21_)
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// TestRemoteFutureLifecycle
// ---------------------------------------------------------------------------

func TestRemoteFutureLifecycle_hundreds_of_remote_futures_wait_and_data(t *testing.T) {
	peer, _ := peer21(t)
	proxy := proxy21_(peer)
	n := N_scaled(300)
	futs := []*laila.RemoteFuture{}
	vals := py.NewList()
	sw := Timed(func() {
		for i := 0; i < n; i++ {
			f := must(proxy.Attr("slow").Invoke(i))
			rf, ok := f.(*laila.RemoteFuture)
			assertTrue(t, ok, py.Repr(f))
			futs = append(futs, rf)
		}
		for _, f := range futs {
			vals.Append(must(unwrap_wait21_(f, T21_)))
		}
	})
	assertEq(t, vals, doubles21_(n))
	assertTrue(t, sw.Elapsed < 60)
	// cached materialization: second wait makes no RPC and is instant
	sw2 := Timed(func() {
		for _, f := range futs {
			assertEq(t, must(unwrap_wait21_(f, 1)), must(unwrap_wait21_(f, 1)))
		}
	})
	assertTrue(t, sw2.Elapsed < 1.0)
}

func TestRemoteFutureLifecycle_status_polling_until_finished(t *testing.T) {
	peer, _ := peer21(t)
	proxy := proxy21_(peer)
	f := rf21_(t, proxy, "slow", 21, py.Kw{"delay": 1.5})
	seen := map[string]bool{}
	deadline := py.Monotonic() + 30
	for py.Monotonic() < deadline {
		st := strings.ToLower(py.Str(must(f.Status_())))
		seen[st] = true
		if strings.Contains(st, "finish") {
			break
		}
		py.Sleep_sync(0.05)
	}
	finished := false
	for s := range seen {
		if strings.Contains(s, "finish") {
			finished = true
		}
	}
	assertTrue(t, finished, seen)
	assertEq(t, must(unwrap_wait21_(f, T21_)), 42)
}

func TestRemoteFutureLifecycle_remote_exception_surfaces(t *testing.T) {
	peer, _ := peer21(t)
	proxy := proxy21_(peer)
	f := rf21_(t, proxy, "boom", "remote-failure")
	_, err := f.Wait(T21_)
	assertTrue(t, err != nil, "expected the remote failure to surface")
	assertTrue(t, strings.Contains(err.Error(), "remote-failure") || strings.Contains(err.Error(), "ValueError"), err)
	exc := f.Exception()
	assertTrue(t, exc != nil)
}

func TestRemoteFutureLifecycle_group_future_round_trip(t *testing.T) {
	peer, _ := peer21(t)
	proxy := proxy21_(peer)
	k := N_scaled(200)
	gf := rf21_(t, proxy, "batch", k)
	assertTrue(t, gf.Is_group())
	res := must(gf.Wait(T21_))
	assertEq(t, unwrap_sorted18_(res), doubles21_(k))
	data := must(gf.Data())
	assertEq(t, py.NewList(must(py.Sorted(py.Iter(data)))...), doubles21_(k))
}

func TestRemoteFutureLifecycle_local_bank_grows_until_release(t *testing.T) {
	peer, _ := peer21(t)
	proxy := proxy21_(peer)
	bank := As_policy(must(laila.Get_active_policy())).Future_bank
	base := bank.Len()
	futs := []*laila.RemoteFuture{}
	for i := 0; i < N_scaled(200); i++ {
		futs = append(futs, rf21_(t, proxy, "slow", i))
	}
	for _, f := range futs {
		must(f.Wait(T21_))
	}
	assertTrue(t, bank.Len() >= base+len(futs))
	for _, f := range futs {
		f.Release()
	}
	assertTrue(t, bank.Len() <= base+5, "RemoteFuture.release() must drop the local bank entry")
}

// The peer keeps every future it created for us; nothing on our side
// ever tells it to drop them. Documented 'never released automatically'.
func TestRemoteFutureLifecycle_remote_bank_grows_without_remote_release(t *testing.T) {
	peer, _ := peer21(t)
	proxy := proxy21_(peer)
	before := must(py.Int(must(proxy.Attr("bank_size").Invoke())))
	futs := []*laila.RemoteFuture{}
	for i := 0; i < N_scaled(100); i++ {
		futs = append(futs, rf21_(t, proxy, "slow", i))
	}
	for _, f := range futs {
		must(f.Wait(T21_))
		f.Release()
	}
	after := must(py.Int(must(proxy.Attr("bank_size").Invoke())))
	assertTrue(t, after >= before+len(futs))
}

func TestRemoteFutureLifecycle_concurrent_waiters_on_distinct_remote_futures(t *testing.T) {
	peer, _ := peer21(t)
	proxy := proxy21_(peer)

	w := func(i int) (any, error) {
		fs := []*laila.RemoteFuture{}
		for j := 0; j < N_scaled(30); j++ {
			v, err := proxy.Attr("slow").Invoke(i*100+j, py.Kw{"delay": 0.01})
			if err != nil {
				return nil, err
			}
			rf, ok := v.(*laila.RemoteFuture)
			if !ok {
				return nil, py.AssertionError.Newf("slow returned %s", py.Repr(v))
			}
			fs = append(fs, rf)
		}
		vals := py.NewList()
		for _, f := range fs {
			v, err := unwrap_wait21_(f, T21_)
			if err != nil {
				return nil, err
			}
			vals.Append(v)
		}
		return vals, nil
	}

	out := must(Run_threads(w, 6))
	for i, vals := range out {
		want := py.NewList()
		for j := 0; j < N_scaled(30); j++ {
			want.Append(2 * (i*100 + j))
		}
		assertEq(t, vals, want)
	}
}

func TestRemoteFutureLifecycle_runtime_status_and_wait_accept_remote_gid(t *testing.T) {
	peer, _ := peer21(t)
	proxy := proxy21_(peer)
	f := rf21_(t, proxy, "slow", 5, py.Kw{"delay": 0.2})
	st := must(laila.Status(f))
	assertTrue(t, !py.IsNone(st))
	assertEq(t, Unwrap(must(laila.Wait(f, T21_))), 10)
}

func TestRemoteFutureLifecycle_wait_timeout_on_slow_remote_future(t *testing.T) {
	peer, _ := peer21(t)
	proxy := proxy21_(peer)
	f := rf21_(t, proxy, "slow", 1, py.Kw{"delay": 5.0})
	var err error
	sw := Timed(func() { _, err = f.Wait(0.5) })
	assertTrue(t, err != nil, "expected wait(0.5) on a 5 s task to fail")
	assertTrue(t, sw.Elapsed < 5.0)
	assertEq(t, must(unwrap_wait21_(f, T21_)), 2)
}

// ---------------------------------------------------------------------------
// TestMorphMode
// ---------------------------------------------------------------------------

func TestMorphMode_morph_with_several_local_policies(t *testing.T) {
	peer, fresh := peer21(t)
	extra := As_policy(must(laila.New_DefaultPolicy())) // a second local policy, never activated
	assertTrue(t, laila.Local_policies_.Len() >= 2)
	proxy := peers21_(peer.Gid())
	laila.Activate_policy(proxy)
	defer func() {
		laila.Activate_policy(fresh)
		laila.Local_policies_.PopDefault(extra.Global_id(), nil)
	}()
	f := must(laila.Memorize(must(laila.Constant(1)), py.Kw{"pool_nickname": "store"}))
	check(t, wait_if_future21_(f))
}

func TestMorphMode_morph_memorize_entry(t *testing.T) {
	peer, fresh := peer21(t)
	proxy := peers21_(peer.Gid())
	defer only_local21_(fresh)()
	laila.Activate_policy(proxy)
	defer laila.Activate_policy(fresh)
	e := must(laila.Constant(py.DictOf("k", 1)))
	f := must(laila.Memorize(e, py.Kw{"pool_nickname": "store"}))
	check(t, wait_if_future21_(f))
	r := must(laila.Remember(e.Global_id(), py.Kw{"pool_nickname": "store", "persist": false}))
	assertEq(t, must(wait_morph21_(r)), py.DictOf("k", 1))
}

func TestMorphMode_morph_remember_volume(t *testing.T) {
	peer, fresh := peer21(t)
	n := N_scaled(150)
	es := constants18_(n, func(i int) any { return i })
	must(Wait(must(laila.Memorize(entries_list(es), py.Kw{"dst_policy": peer.Gid(), "dst_pool": "store"})), T21_))
	proxy := peers21_(peer.Gid())
	func() {
		defer only_local21_(fresh)()
		laila.Activate_policy(proxy)
		defer laila.Activate_policy(fresh)
		assertEq(t, py.Str(must(py.Getattr(must(laila.Get_active_policy()), "global_id"))), peer.Gid())
		for i, e := range es {
			r := must(laila.Remember(e.Global_id(), py.Kw{"pool_nickname": "store", "persist": false}))
			assertEq(t, must(wait_morph21_(r)), i)
		}
		// forget via morph (gids are JSON-safe)
		f := must(laila.Forget(gids18_(es[:n/2]), py.Kw{"pool_nickname": "store"}))
		check(t, wait_if_future21_(f))
	}()
	f, err := laila.Remember(es[0].Global_id(), py.Kw{"dst_policy": peer.Gid(), "dst_pool": "store", "persist": false})
	assertTrue(t, wait_raises18_(f, err, T21_) != nil, "expected the forgotten entry to be gone")
	assertEq(t, Unwrap(must(Wait(must(laila.Remember(es[n-1].Global_id(),
		py.Kw{"dst_policy": peer.Gid(), "dst_pool": "store", "persist": false})), T21_))), n-1)
}

func TestMorphMode_morph_switch_thrash(t *testing.T) {
	peer, fresh := peer21(t)
	proxy := peers21_(peer.Gid())
	defer only_local21_(fresh)()
	for i := 0; i < N_scaled(50); i++ {
		e := must(laila.Constant(i))
		must(Wait(must(laila.Memorize(e, py.Kw{"dst_policy": peer.Gid(), "dst_pool": "store"})), T21_))
		laila.Activate_policy(proxy)
		func() {
			defer laila.Activate_policy(fresh)
			r := must(laila.Remember(e.Global_id(), py.Kw{"pool_nickname": "store", "persist": false}))
			assertEq(t, must(wait_morph21_(r)), i)
		}()
		assertTrue(t, must(laila.Get_active_policy()) == any(fresh))
	}
}

func TestMorphMode_morph_command_submit_remote_task(t *testing.T) {
	peer, fresh := peer21(t)
	proxy := peers21_(peer.Gid())
	vals := py.NewList()
	func() {
		defer only_local21_(fresh)()
		laila.Activate_policy(proxy)
		defer laila.Activate_policy(fresh)
		for i := 0; i < N_scaled(50); i++ {
			slow := must(py.Getattr(must(laila.Get_active_policy()), "slow"))
			f := must(py.Call(slow, []any{i}, nil))
			vals.Append(Unwrap(must(Wait(f, T21_))))
		}
	}()
	assertEq(t, vals, doubles21_(N_scaled(50)))
}

func TestMorphMode_relay_refuses_morph_mode(t *testing.T) {
	peer, fresh := peer21(t)
	proxy := peers21_(peer.Gid())
	laila.Activate_policy(proxy)
	defer laila.Activate_policy(fresh)
	if _, err := laila.Get_relay(peer.Gid(), "video"); !errors.Is(err, py.RuntimeError) {
		t.Fatalf("expected RuntimeError, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestPeerLoss
// ---------------------------------------------------------------------------

func TestPeerLoss_kill_peer_mid_wait_fails_fast(t *testing.T) {
	fresh := Fresh_policy(t)
	disk_tmp := Disk_tmp(t)
	proto := attach21_(t, fresh, 1.0)
	proto.Rpc_timeout = 30.0
	func() {
		cp := child21_(t, "doomed", disk_tmp)
		defer cp.Exit()
		gid := must(laila.Add_peer(cp.Uri(), cp.Secret()))
		proxy := must(laila.Request(gid))
		f := rf21_(t, proxy, "slow", 1, py.Kw{"delay": 20.0})
		py.Sleep_sync(0.5)
		cp.Kill()
		var err error
		sw := Timed(func() { _, err = f.Wait(60) })
		assertTrue(t, err != nil, "expected the wait on a killed peer to fail")
		assertTrue(t, sw.Elapsed < 30, "killed peer should fail the wait well before rpc_timeout")
		comm := Comm(fresh)
		assertTrue(t, Wait_until(func() bool { return !comm.Peers.Contains(gid) }, 20),
			"liveness never evicted the dead peer")
	}()
	Quiesce(fresh)
}

func TestPeerLoss_many_inflight_rpcs_when_peer_dies(t *testing.T) {
	fresh := Fresh_policy(t)
	disk_tmp := Disk_tmp(t)
	proto := attach21_(t, fresh, 1.0)
	proto.Rpc_timeout = 30.0
	func() {
		cp := child21_(t, "doomed2", disk_tmp)
		defer cp.Exit()
		gid := must(laila.Add_peer(cp.Uri(), cp.Secret()))
		proxy := must(laila.Request(gid))
		futs := []*laila.RemoteFuture{}
		for i := 0; i < N_scaled(50); i++ {
			futs = append(futs, rf21_(t, proxy, "slow", i, py.Kw{"delay": 15.0}))
		}
		results := make([]any, len(futs))
		var mu sync.Mutex

		waiter := func(i int) {
			_, err := futs[i].Wait(60)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				results[i] = py.Typename(err)
			} else {
				results[i] = "ok"
			}
		}

		threads := []*py.Thread{}
		for i := range futs {
			i := i
			threads = append(threads, py.New_Thread(py.Kw{"target": func() { waiter(i) }, "daemon": true}))
		}
		for _, th := range threads {
			check(t, th.Start())
		}
		py.Sleep_sync(1.0)
		cp.Kill()
		sw := Timed(func() {
			for _, th := range threads {
				check(t, th.Join(45))
			}
		})
		for _, th := range threads {
			assertTrue(t, !th.Is_alive(), "waiters hung after peer death")
		}
		mu.Lock()
		snapshot := append([]any(nil), results...)
		mu.Unlock()
		for _, r := range snapshot {
			assertTrue(t, r != "ok" && !py.IsNone(r), snapshot[:min(5, len(snapshot))])
		}
		assertTrue(t, sw.Elapsed < 40)
	}()
	Quiesce(fresh)
}

func TestPeerLoss_reconnect_after_restart_and_resume_work(t *testing.T) {
	fresh := Fresh_policy(t)
	disk_tmp := Disk_tmp(t)
	attach21_(t, fresh, 1.0)
	comm := Comm(fresh)
	var gid string
	func() {
		cp := child21_(t, "phoenix2", disk_tmp)
		defer cp.Exit()
		gid = must(laila.Add_peer(cp.Uri(), cp.Secret()))
		assertEq(t, must(unwrap_wait21_(rf21_(t, must(laila.Request(gid)), "slow", 3), T21_)), 6)
		cp.Kill()
		assertTrue(t, Wait_until(func() bool { return !comm.Peers.Contains(gid) }, 20))
	}()
	for round := 0; round < 3; round++ {
		func() {
			cp2 := child21_(t, "phoenix2", disk_tmp)
			defer cp2.Exit()
			assertEq(t, must(laila.Add_peer(cp2.Uri(), cp2.Secret())), gid)
			vals := py.NewList()
			for i := 0; i < 20; i++ {
				vals.Append(must(unwrap_wait21_(rf21_(t, must(laila.Request(gid)), "slow", i), T21_)))
			}
			assertEq(t, vals, doubles21_(20))
			cp2.Kill()
			assertTrue(t, Wait_until(func() bool { return !comm.Peers.Contains(gid) }, 20))
		}()
	}
	Quiesce(fresh)
}

func TestPeerLoss_stale_remote_future_after_peer_restart(t *testing.T) {
	fresh := Fresh_policy(t)
	disk_tmp := Disk_tmp(t)
	attach21_(t, fresh, 1.0)
	comm := Comm(fresh)
	var gid string
	var f *laila.RemoteFuture
	func() {
		cp := child21_(t, "phoenix3", disk_tmp)
		defer cp.Exit()
		gid = must(laila.Add_peer(cp.Uri(), cp.Secret()))
		f = rf21_(t, must(laila.Request(gid)), "slow", 3, py.Kw{"delay": 0.0})
		py.Sleep_sync(0.5) // finished on the peer, never materialized locally
		cp.Kill()
		assertTrue(t, Wait_until(func() bool { return !comm.Peers.Contains(gid) }, 20))
	}()
	func() {
		cp2 := child21_(t, "phoenix3", disk_tmp)
		defer cp2.Exit()
		assertEq(t, must(laila.Add_peer(cp2.Uri(), cp2.Secret())), gid)
		// the new process has no such future: must raise, not hang
		var err error
		sw := Timed(func() { _, err = f.Wait(30) })
		assertTrue(t, err != nil, "expected the stale remote future to fail")
		assertTrue(t, sw.Elapsed < 30)
	}()
	Quiesce(fresh)
}

func TestPeerLoss_parent_stop_with_inflight_remote_waits(t *testing.T) {
	fresh := Fresh_policy(t)
	disk_tmp := Disk_tmp(t)
	attach21_(t, fresh, 60.0)
	func() {
		cp := child21_(t, "stopper", disk_tmp)
		defer cp.Exit()
		gid := must(laila.Add_peer(cp.Uri(), cp.Secret()))
		proxy := must(laila.Request(gid))
		futs := []*laila.RemoteFuture{}
		for i := 0; i < 10; i++ {
			futs = append(futs, rf21_(t, proxy, "slow", i, py.Kw{"delay": 10.0}))
		}
		var mu sync.Mutex
		outcomes := []any{}

		waiter := func(f *laila.RemoteFuture) {
			_, err := f.Wait(30)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				outcomes = append(outcomes, py.Typename(err))
			} else {
				outcomes = append(outcomes, "ok")
			}
		}

		threads := []*py.Thread{}
		for _, f := range futs {
			f := f
			threads = append(threads, py.New_Thread(py.Kw{"target": func() { waiter(f) }, "daemon": true}))
		}
		for _, th := range threads {
			check(t, th.Start())
		}
		py.Sleep_sync(0.5)
		sw := Timed(func() {
			check(t, Comm(fresh).Stop())
			for _, th := range threads {
				check(t, th.Join(35))
			}
		})
		for _, th := range threads {
			assertTrue(t, !th.Is_alive())
		}
		assertTrue(t, sw.Elapsed < 35)
		mu.Lock()
		n := len(outcomes)
		mu.Unlock()
		assertEq(t, n, 10)
	}()
	func() {
		defer func() { _ = recover() }()
		Quiesce(fresh)
	}()
}
