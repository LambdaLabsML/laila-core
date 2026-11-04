// Port of tests/functional/policy/communication/unit_tests/test_rpc_morph_and_carrier_resilience.py.
//
// Fast regression guards for the W18 / W21 communication findings.
//
// Covered (see ``tests/workloads/FINDINGS.md``):
//
//   - F-W21-01: morph mode (``activate_policy(proxy)``) keeps working when
//     the process holds several local policies -- the future owner is the
//     local policy that owns the link to the proxy.
//   - F-W21-02: ``communication.stop()`` / peer loss fails outstanding
//     outbound RPCs at once with ConnectionError instead of making waiters
//     sit out ``rpc_timeout``.
//   - F-W21-03: an Entry passed as an RPC argument crosses the wire as its
//     canonical serialized envelope and is rebuilt on the peer.
//   - F-W18-03: ``laila.memorize(a, b, dst_policy=peer)`` is a TypeError
//     (one positional, like the local verbs) -- extra entries are never
//     silently dropped.
//   - F-W18-04: ``laila.remember(..., persist=True, dst_policy=peer)``
//     reaches the peer's ``remember(persist=True)``.
//   - F-W18-05: a WebSocket frame over ``MAX_FRAME_BYTES`` is refused up
//     front with a descriptive ConnectionError and the link survives.
//   - F-W18-06: the datagram carrier fails the pending RPC when a message's
//     fragments exhaust their retransmits instead of leaving it to time out.
package unit_tests

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// mk_morph_ is ``_mk(proto)``.
func mk_morph_(t *testing.T, proto laila.Comm_protocol_) *laila.LAILA_IDENTIFIABLE_POLICY_ {
	t.Helper()
	p := must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	if err := comm(p).Add_connection(proto); err != nil {
		t.Fatal(err)
	}
	return p
}

// proto_of_ is ``_proto(p)``: the first connection.
func proto_of_(p *laila.LAILA_IDENTIFIABLE_POLICY_) laila.Comm_protocol_ {
	return comm(p).Connections.Values()[0].(laila.Comm_protocol_)
}

// link_morph_ is ``_link(a, b, scheme)``.
func link_morph_(t *testing.T, a, b *laila.LAILA_IDENTIFIABLE_POLICY_, scheme string) *laila.PeerProxy {
	t.Helper()
	pb := proto_of_(b)
	port := py.Str(must(py.Getattr(pb, "bound_port")))
	secret := py.Str(must(py.Getattr(pb, "peer_secret_key")))
	if _, err := comm(a).Add_peer(scheme+"://127.0.0.1:"+port, secret); err != nil {
		t.Fatal(err)
	}
	proxy, ok := comm(a).Peers.Get(b.Global_id())
	if !ok {
		t.Fatalf("KeyError: %s", b.Global_id())
	}
	return proxy.(*laila.PeerProxy)
}

// ``_alpha_pool(policy)`` is alpha_pool_ (transport_memory_roundtrip_test.go).

// wait_for_morph_ is ``_wait_for(pred, timeout)``.
func wait_for_morph_(pred func() bool, timeout float64) bool {
	deadline := time.Now().Add(time.Duration(timeout * float64(time.Second)))
	for time.Now().Before(deadline) {
		if pred() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return pred()
}

// morphTwo_ is ``_TwoPolicies``: two peered policies, A active.
type morphTwo_ struct {
	A, B  *laila.LAILA_IDENTIFIABLE_POLICY_
	proxy *laila.PeerProxy
}

type protoFactory_ func() (laila.Comm_protocol_, error)

func tcpFactory_() (laila.Comm_protocol_, error) { return laila.New_DefaultTCPProtocol() }
func wsFactory_() (laila.Comm_protocol_, error)  { return laila.New_DefaultTCPIPProtocol() }

func setUpMorph_(t *testing.T, scheme string, factory protoFactory_) *morphTwo_ {
	t.Helper()
	original := must(laila.Get_active_policy())
	s := &morphTwo_{}
	s.A = mk_morph_(t, must(factory()))
	s.B = mk_morph_(t, must(factory()))
	s.proxy = link_morph_(t, s.A, s.B, scheme)
	laila.Activate_policy(s.A)
	t.Cleanup(func() {
		laila.Activate_policy(original)
		stopAll(s.A, s.B)
	})
	return s
}

// ----------------------------------------------------------------------
// TestMorphModeOwnerResolution
// ----------------------------------------------------------------------

func TestMorphModeOwnerResolution_morph_mode_with_several_local_policies(t *testing.T) {
	s := setUpMorph_(t, "tcp", tcpFactory_)
	// F-W21-01: a third local policy exists; morph mode must still
	// register the RemoteFuture on the proxy's *owner* (A).
	extra := must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	t.Cleanup(func() { laila.Activate_policy(s.A) })
	attach_(s.B, "echo", echo_any_)

	laila.Activate_policy(s.proxy)
	owner := must(laila.Get_active_local_policy_())
	if owner != any(s.A) {
		t.Fatalf("owner is %s, want A", py.Repr(owner))
	}
	if owner == any(extra) {
		t.Fatal("owner must not be the extra policy")
	}

	fut, ok := must(laila.Memorize(must(laila.Constant(py.DictOf("m", 1))))).(*laila.RemoteFuture)
	if !ok {
		t.Fatal("memorize in morph mode must return a RemoteFuture")
	}
	if !s.A.Future_bank.Contains(fut.Global_id()) {
		t.Fatal("RemoteFuture not registered in A's future bank")
	}
	must(fut.Wait(10))
}

// ----------------------------------------------------------------------
// TestEntryEnvelopeOverRpc
// ----------------------------------------------------------------------

func TestEntryEnvelopeOverRpc_entry_argument_is_rehydrated_on_peer(t *testing.T) {
	s := setUpMorph_(t, "tcp", tcpFactory_)
	// F-W21-03: the peer sees a real Entry, not a dict.
	seen := py.NewDict()
	take := func(entry any) (any, error) {
		seen.Set("type", py.Pytypename(entry))
		data, err := py.Getattr(entry, "data")
		if err != nil {
			return nil, err
		}
		seen.Set("data", data)
		return py.Getattr(entry, "global_id")
	}
	attach_(s.B, "take", take)
	entry := must(laila.Constant(py.DictOf("k", py.NewList(1, 2, 3))))
	gid := call_(t, s.proxy, "take", entry)
	assertEq(t, gid, entry.Global_id())
	assertEq(t, must(seen.Getitem("type")), "Entry")
	assertEq(t, must(seen.Getitem("data")), py.DictOf("k", py.NewList(1, 2, 3)))
}

func TestEntryEnvelopeOverRpc_morph_memorize_entry_lands_in_peer_pool(t *testing.T) {
	s := setUpMorph_(t, "tcp", tcpFactory_)
	laila.Activate_policy(s.proxy)
	t.Cleanup(func() { laila.Activate_policy(s.A) })
	entry := must(laila.Constant(py.DictOf("morph", true)))
	fut := must(laila.Memorize(entry))
	must(callm_(fut, "wait", py.Kw{"timeout": 10}))
	if !wait_for_morph_(func() bool { return alpha_pool_(t, s.B).Resource.Contains(entry.Global_id()) }, 8.0) {
		t.Fatal("entry never landed in the peer's alpha pool")
	}
}

func callm_(obj any, name string, args ...any) (any, error) {
	fn, err := py.Getattr(obj, name)
	if err != nil {
		return nil, err
	}
	pos, kw := py.SplitKw(args)
	return py.Call(fn, pos, kw)
}

// ----------------------------------------------------------------------
// TestRoutedMemoryVerbs
// ----------------------------------------------------------------------

func TestRoutedMemoryVerbs_extra_positional_is_a_type_error(t *testing.T) {
	s := setUpMorph_(t, "tcp", tcpFactory_)
	// F-W18-03
	e1, e2 := must(laila.Constant(1)), must(laila.Constant(2))
	_, err := laila.Memorize(e1, e2, py.Kw{"dst_policy": s.B.Global_id()})
	assertRaises(t, py.TypeError, err)
}

func TestRoutedMemoryVerbs_persist_is_forwarded_to_peer(t *testing.T) {
	s := setUpMorph_(t, "tcp", tcpFactory_)
	// F-W18-04
	entry := must(laila.Constant(py.DictOf("p", 1)))
	f := must(laila.Memorize(entry, py.Kw{"dst_policy": s.B.Global_id()}))
	if !py.IsNone(f) && py.Hasattr(f, "wait") {
		must(callm_(f, "wait", py.Kw{"timeout": 10}))
	}
	if !wait_for_morph_(func() bool { return alpha_pool_(t, s.B).Resource.Contains(entry.Global_id()) }, 8.0) {
		t.Fatal("entry never landed in the peer's alpha pool")
	}

	calls := py.NewList()
	mem := memory_of_(s.B)
	real := mem.Remember
	spy := func(args ...any) (any, error) {
		_, kwargs := py.SplitKw(args)
		calls.Append(kwargs.Get("persist", nil))
		return real(args...)
	}

	// pydantic models reject attribute patching; bind the spy on the
	// instance dict directly and restore it afterwards.
	mem.Private_set("remember", spy)
	func() {
		defer mem.Private_del("remember")
		f := must(laila.Remember(entry.Global_id(), py.Kw{"persist": true, "dst_policy": s.B.Global_id()}))
		if !py.IsNone(f) && py.Hasattr(f, "wait") {
			must(callm_(f, "wait", py.Kw{"timeout": 10}))
		}
	}()
	if !wait_for_morph_(func() bool {
		for _, c := range calls.Items() {
			if c == true {
				return true
			}
		}
		return false
	}, 8.0) {
		t.Fatalf("persist=True never reached the peer's remember(): %s", py.Repr(calls))
	}
}

// ----------------------------------------------------------------------
// TestFailPendingOnStopAndPeerLoss
// ----------------------------------------------------------------------

func slow_peer_(s *morphTwo_, delay float64) {
	attach_(s.B, "slow", func() (any, error) {
		sleep(delay)
		return "late", nil
	})
}

type callResult_ struct {
	value any
	exc   error
	done  bool
}

func TestFailPendingOnStopAndPeerLoss_stop_fails_in_flight_rpc_immediately(t *testing.T) {
	s := setUpMorph_(t, "tcp", tcpFactory_)
	// F-W21-02
	slow_peer_(s, 5.0)
	pa := proto_of_(s.A).(*laila.LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_)
	pa.Rpc_timeout = 60.0
	result := &callResult_{}

	th := py.New_Thread(py.Kw{"target": func() {
		result.value, result.exc = s.proxy.Attr("slow").Invoke()
		result.done = true
	}, "daemon": true})
	must(0, th.Start())
	if !wait_for_morph_(func() bool { return pa.Pending_rpcs_.Len() == 1 }, 5.0) {
		t.Fatal("RPC never became pending")
	}
	t0 := time.Now()
	must(0, comm(s.A).Stop())
	must(0, th.Join(5.0))
	if th.Is_alive() {
		t.Fatal("caller thread still alive")
	}
	if elapsed := time.Since(t0).Seconds(); elapsed >= 5.0 {
		t.Fatalf("stop took %.2fs", elapsed)
	}
	assertRaises(t, py.ConnectionError, result.exc)
	assertEq(t, pa.Pending_rpcs_.Len(), 0)
}

func TestFailPendingOnStopAndPeerLoss_peer_drop_fails_only_that_peers_rpcs(t *testing.T) {
	s := setUpMorph_(t, "tcp", tcpFactory_)
	slow_peer_(s, 5.0)
	pa := proto_of_(s.A).(*laila.LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_)
	pa.Rpc_timeout = 60.0
	result := &callResult_{}

	th := py.New_Thread(py.Kw{"target": func() {
		result.value, result.exc = s.proxy.Attr("slow").Invoke()
		result.done = true
	}, "daemon": true})
	must(0, th.Start())
	if !wait_for_morph_(func() bool { return pa.Pending_rpcs_.Len() == 1 }, 5.0) {
		t.Fatal("RPC never became pending")
	}
	t0 := time.Now()
	must(0, comm(s.B).Stop())
	must(0, th.Join(5.0))
	if th.Is_alive() {
		t.Fatal("caller thread still alive")
	}
	if elapsed := time.Since(t0).Seconds(); elapsed >= 5.0 {
		t.Fatalf("peer drop took %.2fs", elapsed)
	}
	assertRaises(t, py.ConnectionError, result.exc)
}

// ----------------------------------------------------------------------
// TestFrameCap (WebSocket carrier)
// ----------------------------------------------------------------------

func TestFrameCap_oversized_frame_is_refused_up_front_and_link_survives(t *testing.T) {
	s := setUpMorph_(t, "ws", wsFactory_)
	// F-W18-05 (WebSocket carrier)
	attach_(s.B, "echo", echo_any_)
	func() {
		// mock.patch.object(connection, "MAX_FRAME_BYTES", 4096)
		saved := laila.MAX_FRAME_BYTES
		laila.MAX_FRAME_BYTES = 4096
		defer func() { laila.MAX_FRAME_BYTES = saved }()
		_, err := s.proxy.Attr("echo").Invoke(strings.Repeat("x", 10_000))
		assertRaises(t, py.ConnectionError, err)
		assertIn_(t, "exceeds", err)
	}()
	// the socket was never poisoned: a normal call still works
	assertEq(t, call_(t, s.proxy, "echo", "ok"), "ok")
	if !proto_of_(s.A).(*laila.LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_).Has_peer(s.B.Global_id()) {
		t.Fatal("peer lost")
	}
}

// ----------------------------------------------------------------------
// TestDatagramGiveUp (UDP carrier)
// ----------------------------------------------------------------------

// BlackholeUDPProtocol_ is a UDP carrier whose ``_sendto`` can be
// black-holed (``mock.patch.object(pa, "_sendto", lambda addr, packet:
// None)``).
type BlackholeUDPProtocol_ struct {
	laila.LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_

	blackhole atomic.Bool
}

var BlackholeUDPProtocol_class = py.NewClass(
	"_BlackholeUDPProtocol", (*BlackholeUDPProtocol_)(nil), laila.LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_class,
)

func New_BlackholeUDPProtocol_(kw ...py.Kw) (*BlackholeUDPProtocol_, error) {
	p := BlackholeUDPProtocol_class.New().(*BlackholeUDPProtocol_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *BlackholeUDPProtocol_) Sendto_(addr any, packet []byte) error {
	if p.blackhole.Load() {
		return nil
	}
	return p.LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_.Sendto_(addr, packet)
}

func TestDatagramGiveUp_lost_fragments_fail_the_pending_rpc(t *testing.T) {
	// ``pa.rpc_timeout = 60.0; pa.ack_timeout = 0.05; pa.max_retries = 3``
	// are applied at construction time here: the retransmit loop reads
	// them on its own thread, so a post-start assignment would be a data
	// race in Go (Python's GIL makes it benign).
	s := setUpMorph_(t, "udp", func() (laila.Comm_protocol_, error) {
		return New_BlackholeUDPProtocol_(py.Kw{"rpc_timeout": 60.0, "ack_timeout": 0.05, "max_retries": 3})
	})
	// F-W18-06: black-hole outbound packets; the RPC must fail with
	// ConnectionError well before rpc_timeout.
	attach_(s.B, "echo", echo_any_)
	assertEq(t, call_(t, s.proxy, "echo", 1), 1) // link is healthy

	pa := proto_of_(s.A).(*BlackholeUDPProtocol_)
	var err error
	func() {
		pa.blackhole.Store(true)
		defer pa.blackhole.Store(false)
		t0 := time.Now()
		_, err = s.proxy.Attr("echo").Invoke("lost")
		assertRaises(t, py.ConnectionError, err)
		if elapsed := time.Since(t0).Seconds(); elapsed >= 10.0 {
			t.Fatalf("give-up took %.2fs", elapsed)
		}
	}()
	assertIn_(t, "gave up", err)
	assertEq(t, pa.Pending_rpcs_.Len(), 0)
	// ``self.assertEqual(pa._unacked, {})`` -- read on the loop thread,
	// which owns the table.
	unacked := must(py.Run_coroutine_threadsafe(func() (any, error) { return len(pa.Unacked_), nil }, pa.Loop()).Result(5))
	assertEq(t, unacked, 0)
}

// ----------------------------------------------------------------------
// TestOwnerBankForUnactivatedPolicy
// ----------------------------------------------------------------------

// Futures stamped with a never-activated policy's gid live in *its* bank
// and stay resolvable through every gid-based lookup.
func TestOwnerBankForUnactivatedPolicy_memory_future_registers_in_owner_bank(t *testing.T) {
	original := must(laila.Get_active_policy())
	a := must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	b := must(laila.New_LAILA_IDENTIFIABLE_POLICY_()) // constructed, never activated
	laila.Activate_policy(a)
	t.Cleanup(func() {
		laila.Activate_policy(original)
		for _, p := range []*laila.LAILA_IDENTIFIABLE_POLICY_{a, b} {
			cmd := p.Central.Command.(interface {
				Central_command_base_() *laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_
			}).Central_command_base_()
			_ = cmd.Shutdown(py.Kw{"wait": false})
		}
	})

	if laila.Local_policies_.Contains(b.Global_id()) {
		t.Fatal("b must not be a registered local policy")
	}
	entries := py.NewList()
	for i := 0; i < 3; i++ {
		entries.Append(must(laila.Constant(i)))
	}
	group := must(memory_of_(b).Memorize(entries)).(*laila.GroupFuture)
	if !b.Future_bank.Contains(group.Global_id()) {
		t.Fatal("group not in b's bank")
	}
	if a.Future_bank.Contains(group.Global_id()) {
		t.Fatal("group leaked into a's bank")
	}
	for _, fid := range group.Future_ids {
		if !b.Future_bank.Contains(fid) {
			t.Fatalf("child %s not in b's bank", fid)
		}
	}
	must(group.Wait(10)) // children resolved across live policies
	assertEq(t, must(laila.Status(group.Global_id())), must(group.Status()))
	group.Release()
	if b.Future_bank.Contains(group.Global_id()) {
		t.Fatal("group still in b's bank after release")
	}
	for _, fid := range group.Future_ids {
		if b.Future_bank.Contains(fid) {
			t.Fatalf("child %s still in b's bank after release", fid)
		}
	}
}

// ----------------------------------------------------------------------
// TestCarrierBaseFailPending
// ----------------------------------------------------------------------

func TestCarrierBaseFailPending_fail_pending_is_scoped_by_peer(t *testing.T) {
	p := must(laila.New_DefaultUnixSocketProtocol())
	r1, s1 := p.Register_pending_("peer-1")
	r2, s2 := p.Register_pending_("peer-2")
	p.Fail_pending_("peer-1 gone", py.Ptr("peer-1"))
	if !s1.Event.Is_set() {
		t.Fatal("peer-1 slot not failed")
	}
	assertRaises(t, py.ConnectionError, s1.Exception())
	if s2.Event.Is_set() {
		t.Fatal("peer-2 slot must be untouched")
	}
	if p.Pending_rpcs_.Get(r1) != nil {
		t.Fatal("r1 still pending")
	}
	if p.Pending_rpcs_.Get(r2) == nil {
		t.Fatal("r2 no longer pending")
	}
	p.Fail_pending_("transport stopped", nil)
	if !s2.Event.Is_set() {
		t.Fatal("peer-2 slot not failed on stop")
	}
	assertEq(t, p.Pending_rpcs_.Len(), 0)
}

// ----------------------------------------------------------------------
// TestEnvelopeHelpers
// ----------------------------------------------------------------------

func TestEnvelopeHelpers_non_ready_entry_and_non_entries_are_untouched(t *testing.T) {
	if laila.Serialize_entry_arg(42) != nil {
		t.Fatal("42 is not an entry")
	}
	if laila.Serialize_entry_arg(py.DictOf("_uuid", "x")) != nil {
		t.Fatal("a bare dict is not an entry")
	}
	args, kwargs, err := laila.Rehydrate_entry_args(py.NewList(1, py.DictOf("a", 1)), py.Kw{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	assertEq(t, py.NewList(args...), py.NewList(1, py.DictOf("a", 1)))
	assertEq(t, py.DictFromKw(kwargs), py.DictOf("k", "v"))
}

func TestEnvelopeHelpers_nested_list_of_entries_round_trips(t *testing.T) {
	entries := []*laila.Entry{}
	for i := 0; i < 3; i++ {
		entries = append(entries, must(laila.Constant(i)))
	}
	env := py.NewList()
	for _, e := range entries {
		env.Append(laila.Serialize_entry_arg(e))
	}
	for _, x := range env.Items() {
		if !laila.Is_entry_envelope(x) {
			t.Fatalf("not an envelope: %s", py.Repr(x))
		}
	}
	args, _, err := laila.Rehydrate_entry_args(py.NewList(env), py.Kw{})
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 1 {
		t.Fatalf("expected one positional, got %d", len(args))
	}
	rebuilt := py.Iter(args[0])
	data, gids := py.NewList(), py.NewList()
	for _, e := range rebuilt {
		data.Append(must(py.Getattr(e, "data")))
		gids.Append(must(py.Getattr(e, "global_id")))
	}
	assertEq(t, data, py.NewList(0, 1, 2))
	want := py.NewList()
	for _, e := range entries {
		want.Append(e.Global_id())
	}
	assertEq(t, gids, want)
}
