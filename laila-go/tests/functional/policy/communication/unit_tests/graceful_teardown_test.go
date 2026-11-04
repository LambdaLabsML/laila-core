// Port of tests/functional/policy/communication/unit_tests/test_graceful_teardown.py.
//
// Graceful teardown conformance for every transport family.
//
// For each carrier the same contract is checked:
//
//  1. stop() is clean -- no asyncio error is reported on either side's
//     loop, the loop is closed and its thread gone, both peer registries
//     (and ``laila._remote_policies``) are empty, and a full
//     start/peer/stop cycle leaks no file descriptor (two cycles are
//     compared, so lazily-created resources do not confuse the count).
//  2. remove_peer() is noticed by the remote promptly -- via the
//     ``peer.disconnect`` goodbye, not the liveness sweep (which is set
//     far out of reach) -- and the pair can re-peer afterwards.
//  3. A one-sided stop() is noticed by the remote the same way.
//  4. Sends after stop() raise ConnectionError (never an AttributeError /
//     asyncio RuntimeError).
//  5. laila.terminate() tears everything down and reports no errors.
//
// Wire transports run for real on localhost / a pty: loopback, raw TCP,
// Unix socket, UDP, WebSocket (``tcpip``) and UART. The broker, register,
// datagram and point-to-point *carriers* are additionally exercised over
// the in-memory fakes defined in the sibling test files, so the shared
// carrier logic is covered even where no broker / bus is available.
package unit_tests

import (
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
	streaming "github.com/amirzadehlambda/laila/laila-go/tests/functional/policy/communication/streaming/unit_tests"
)

// NO_LIVENESS_: liveness must never be what drops a peer in these tests.
const NO_LIVENESS_ = 600.0

// ----------------------------------------------------------------------
// probes
// ----------------------------------------------------------------------

// AsyncioErrors_ captures loop-level exceptions and ``asyncio`` /
// ``websockets`` ERROR logs.
type AsyncioErrors_ struct {
	py.BaseHandler
	contexts []*py.Dict
	records  []*py.LogRecord
}

func New_AsyncioErrors_() *AsyncioErrors_ {
	p := &AsyncioErrors_{}
	p.Set_level(py.ERROR)
	return p
}

// Handle is the logging.Handler emit hook.
func (p *AsyncioErrors_) Handle(r *py.LogRecord) {
	if r.Levelno >= p.Level() {
		p.records = append(p.records, r)
	}
}

func (p *AsyncioErrors_) Close() {}

// Watch installs the loop exception handler on each protocol's loop.
func (p *AsyncioErrors_) Watch(protos ...any) {
	for _, proto := range protos {
		loop := loop_of_(proto)
		if loop == nil || loop.Is_closed() {
			continue
		}
		done := py.NewEvent()
		_, _ = loop.Call_soon_threadsafe(func() {
			loop.Set_exception_handler(func(ctx *py.Dict) { p.contexts = append(p.contexts, ctx) })
			done.Set()
		})
		done.Wait(2.0)
	}
}

// Enter is ``__enter__``: attach to the asyncio/websockets loggers.
func (p *AsyncioErrors_) Enter() *AsyncioErrors_ {
	for _, name := range []string{"asyncio", "websockets"} {
		py.Get_logger(name).Add_handler(p)
	}
	return p
}

// Exit is ``__exit__``.
func (p *AsyncioErrors_) Exit() {
	for _, name := range []string{"asyncio", "websockets"} {
		py.Get_logger(name).Remove_handler(p)
	}
}

// Report lists everything captured.
func (p *AsyncioErrors_) Report() []string {
	out := []string{}
	for _, c := range p.contexts {
		out = append(out, fmt.Sprintf("%s: %s", py.Str(c.GetDefault("message", nil)), py.Repr(c.GetDefault("exception", nil))))
	}
	for _, r := range p.records {
		out = append(out, fmt.Sprintf("%s: %s", r.Name, r.Get_message()))
	}
	return out
}

// loop_of_ is ``getattr(proto, "_event_loop", None)``.
func loop_of_(proto any) *py.Loop {
	if o, ok := proto.(laila.Loop_thread_owner_); ok {
		return o.Loop_state_().Loop()
	}
	return nil
}

// open_fds_ is ``_open_fds()``.
func open_fds_() map[string]bool {
	runtime.GC()
	out := map[string]bool{}
	names, err := os.ReadDir("/proc/self/fd")
	if err != nil { // pragma: no cover - non-Linux
		return out
	}
	for _, n := range names {
		out[n.Name()] = true
	}
	return out
}

func fds_minus_(a, b map[string]bool) []string {
	out := []string{}
	for k := range a {
		if !b[k] {
			out = append(out, k)
		}
	}
	return out
}

// ----------------------------------------------------------------------
// transport harnesses: build (pa, pb) and the URI A uses to reach B
// ----------------------------------------------------------------------

type pair_spec_ struct {
	factory func(kw py.Kw) (laila.Comm_protocol_, error)
	uri_fn  func(pb laila.Comm_protocol_, b *laila.LAILA_IDENTIFIABLE_POLICY_) string
	kw_a    py.Kw
	kw_b    py.Kw
	reset   func()
}

// Pair_ is ``_Pair``: two policies, two protocol instances, A peered to B.
type Pair_ struct {
	name   string
	spec   pair_spec_
	a, b   *laila.LAILA_IDENTIFIABLE_POLICY_
	pa, pb laila.Comm_protocol_
}

func (p *Pair_) build(t *testing.T) *Pair_ {
	t.Helper()
	if p.spec.reset != nil {
		p.spec.reset()
	}
	p.pa = must(p.spec.factory(p.spec.kw_a))
	p.pb = must(p.spec.factory(p.spec.kw_b))
	p.a, p.b = must(laila.New_LAILA_IDENTIFIABLE_POLICY_()), must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	for _, pol := range []*laila.LAILA_IDENTIFIABLE_POLICY_{p.a, p.b} {
		comm(pol).Liveness_interval = NO_LIVENESS_
	}
	must(0, comm(p.a).Add_connection(p.pa))
	must(0, comm(p.b).Add_connection(p.pb))
	return p
}

func (p *Pair_) ca() *laila.LAILA_IDENTIFIABLE_COMMUNICATION_ { return comm(p.a) }
func (p *Pair_) cb() *laila.LAILA_IDENTIFIABLE_COMMUNICATION_ { return comm(p.b) }
func (p *Pair_) uri() string                                   { return p.spec.uri_fn(p.pb, p.b) }

func (p *Pair_) peer(t *testing.T) {
	t.Helper()
	bid, err := p.ca().Add_peer(p.uri(), secret_of_(p.pb))
	if err != nil {
		t.Fatalf("%s: add_peer: %v", p.name, err)
	}
	if bid != p.b.Global_id() {
		t.Fatalf("%s: %s != %s", p.name, bid, p.b.Global_id())
	}
	if !streaming.Wait_until(func() bool { return p.cb().Peers.Contains(p.a.Global_id()) }, 5) {
		t.Fatalf("%s: B never registered A", p.name)
	}
	if !p.pa.Ping(p.b.Global_id()) {
		t.Fatalf("%s: ping A->B failed", p.name)
	}
}

func (p *Pair_) stop_all() {
	for _, c := range []*laila.LAILA_IDENTIFIABLE_COMMUNICATION_{p.ca(), p.cb()} {
		_ = c.Stop()
	}
}

func reset_fakes_() {
	reset_bus_()
	reg_links_mu_.Lock()
	reg_links_ = map[string]*mailboxPair_{}
	reg_links_mu_.Unlock()
	ether_mu_.Lock()
	ether_ = map[string]*LossyDatagramProtocol_{}
	ether_mu_.Unlock()
	p2p_links_mu_.Lock()
	p2p_links_ = map[string]map[string]p2pEnd_{}
	p2p_links_mu_.Unlock()
}

func ip_uri_(scheme string) func(pb laila.Comm_protocol_, b *laila.LAILA_IDENTIFIABLE_POLICY_) string {
	return func(pb laila.Comm_protocol_, b *laila.LAILA_IDENTIFIABLE_POLICY_) string {
		return fmt.Sprintf("%s://127.0.0.1:%d", scheme, pb.(interface{ Bound_port() int }).Bound_port())
	}
}

// PAIR_NAMES_ / PAIRS_ is ``PAIRS`` (insertion order preserved).
var PAIR_NAMES_ = []string{
	"loopback", "tcp", "unix", "udp", "tcpip-ws",
	"broker(fake)", "register(fake)", "datagram(fake)", "p2p(fake)",
}

var PAIRS_ = map[string]pair_spec_{
	"loopback": {
		factory: func(kw py.Kw) (laila.Comm_protocol_, error) { return laila.New_DefaultLoopbackProtocol(kw) },
		uri_fn: func(pb laila.Comm_protocol_, b *laila.LAILA_IDENTIFIABLE_POLICY_) string {
			return "loopback://" + b.Global_id()
		},
		kw_a: py.Kw{}, kw_b: py.Kw{},
	},
	"tcp": {
		factory: func(kw py.Kw) (laila.Comm_protocol_, error) { return laila.New_DefaultTCPProtocol(kw) },
		uri_fn:  ip_uri_("tcp"),
		kw_a:    py.Kw{"host": "127.0.0.1"}, kw_b: py.Kw{"host": "127.0.0.1"},
	},
	"unix": {
		factory: func(kw py.Kw) (laila.Comm_protocol_, error) { return laila.New_DefaultUnixSocketProtocol(kw) },
		uri_fn: func(pb laila.Comm_protocol_, b *laila.LAILA_IDENTIFIABLE_POLICY_) string {
			return "unix://" + pb.(*laila.LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_).Bound_path()
		},
		kw_a: py.Kw{}, kw_b: py.Kw{},
	},
	"udp": {
		factory: func(kw py.Kw) (laila.Comm_protocol_, error) { return laila.New_DefaultUDPProtocol(kw) },
		uri_fn:  ip_uri_("udp"),
		kw_a:    py.Kw{"host": "127.0.0.1"}, kw_b: py.Kw{"host": "127.0.0.1"},
	},
	"tcpip-ws": {
		factory: func(kw py.Kw) (laila.Comm_protocol_, error) { return laila.New_DefaultTCPIPProtocol(kw) },
		uri_fn:  ip_uri_("ws"),
		kw_a:    py.Kw{"host": "127.0.0.1"}, kw_b: py.Kw{"host": "127.0.0.1"},
	},
	"broker(fake)": {
		factory: func(kw py.Kw) (laila.Comm_protocol_, error) { return New_FakeBrokerProtocol_(kw) },
		uri_fn: func(pb laila.Comm_protocol_, b *laila.LAILA_IDENTIFIABLE_POLICY_) string {
			return "fakebroker://" + b.Global_id()
		},
		kw_a: py.Kw{}, kw_b: py.Kw{},
		reset: reset_fakes_,
	},
	"register(fake)": {
		factory: func(kw py.Kw) (laila.Comm_protocol_, error) { return New_FakeRegisterProtocol_(kw) },
		uri_fn:  func(pb laila.Comm_protocol_, b *laila.LAILA_IDENTIFIABLE_POLICY_) string { return "fakereg://bus" },
		kw_a:    py.Kw{"link_id": "GT", "side": "a", "poll_interval": 0.005},
		kw_b:    py.Kw{"link_id": "GT", "side": "b", "poll_interval": 0.005},
		reset:   reset_fakes_,
	},
	"datagram(fake)": {
		factory: func(kw py.Kw) (laila.Comm_protocol_, error) { return New_LossyDatagramProtocol_(kw) },
		uri_fn:  func(pb laila.Comm_protocol_, b *laila.LAILA_IDENTIFIABLE_POLICY_) string { return "fakedg://B" },
		kw_a:    py.Kw{"addr": "A", "mtu": 64},
		kw_b:    py.Kw{"addr": "B", "mtu": 64},
		reset:   reset_fakes_,
	},
	"p2p(fake)": {
		factory: func(kw py.Kw) (laila.Comm_protocol_, error) { return New_FakeP2PProtocol_(kw) },
		uri_fn:  func(pb laila.Comm_protocol_, b *laila.LAILA_IDENTIFIABLE_POLICY_) string { return "fakep2p://bus" },
		kw_a:    py.Kw{"link_id": "GT", "side": "a"},
		kw_b:    py.Kw{"link_id": "GT", "side": "b"},
		reset:   reset_fakes_,
	},
}

func pair_(t *testing.T, name string) *Pair_ {
	t.Helper()
	return (&Pair_{name: name, spec: PAIRS_[name]}).build(t)
}

// assert_proto_down_ is ``_assert_proto_down(tc, proto, label)``.
func assert_proto_down_(t *testing.T, proto any, label string) {
	t.Helper()
	if py.Truthy(getattr_default_(proto, "_started", false)) {
		t.Fatalf("%s: still started", label)
	}
	conns := getattr_default_(proto, "_connections", py.NewDict())
	if d, ok := conns.(*py.Dict); !ok || d.Len() != 0 {
		t.Fatalf("%s: connections left: %s", label, py.Repr(conns))
	}
	if !py.IsNone(getattr_default_(proto, "_event_loop", nil)) {
		t.Fatalf("%s: event loop not released", label)
	}
	if !py.IsNone(getattr_default_(proto, "_loop_thread", nil)) {
		t.Fatalf("%s: loop thread not joined", label)
	}
}

func getattr_default_(obj any, name string, def any) any {
	v, err := py.Getattr(obj, name)
	if err != nil {
		return def
	}
	return v
}

func setUpGracefulTeardown_(t *testing.T) {
	t.Helper()
	reset_fakes_()
	laila.Terminate()
	t.Cleanup(func() {
		laila.Terminate()
		reset_fakes_()
	})
}

func assertNoErrors_(t *testing.T, report []string, msg string) {
	t.Helper()
	if len(report) != 0 {
		t.Fatalf("%s: %v", msg, report)
	}
}

// ----------------------------------------------------------------------
// tests
// ----------------------------------------------------------------------

// 1 -----------------------------------------------------------------
func TestGracefulTeardownAllTransports_stop_is_clean_and_leaks_nothing(t *testing.T) {
	setUpGracefulTeardown_(t)
	for _, name := range PAIR_NAMES_ {
		t.Run(name, func(t *testing.T) {
			pr := pair_(t, name)
			defer pr.stop_all()
			// cycle 1: peer + stop (absorbs any lazily created resources)
			pr.peer(t)
			pr.stop_all()
			baseline := open_fds_()
			// cycle 2: restart the very same protocol instances
			must(0, pr.pa.Start())
			must(0, pr.pb.Start())
			pr.peer(t)
			aid, bid := pr.a.Global_id(), pr.b.Global_id()
			if !laila.Remote_policies_.Contains(bid) {
				t.Fatalf("%s: %s not in laila._remote_policies", name, bid)
			}
			probe := New_AsyncioErrors_().Enter()
			probe.Watch(pr.pa, pr.pb)
			pr.stop_all()
			sleep(0.2)
			probe.Exit()
			assertNoErrors_(t, probe.Report(), name+": asyncio errors on stop")
			for _, proto := range []laila.Comm_protocol_{pr.pa, pr.pb} {
				assert_proto_down_(t, proto, name)
			}
			if pr.ca().Peers.Len() != 0 || pr.cb().Peers.Len() != 0 {
				t.Fatalf("%s: peers left: %s / %s", name, py.Repr(pr.ca().Peers), py.Repr(pr.cb().Peers))
			}
			if laila.Remote_policies_.Contains(bid) || laila.Remote_policies_.Contains(aid) {
				t.Fatalf("%s: _remote_policies not cleared", name)
			}
			leaked := fds_minus_(open_fds_(), baseline)
			if len(leaked) != 0 {
				t.Fatalf("%s: fds leaked per lifecycle: %v", name, leaked)
			}
		})
	}
}

// 2 -----------------------------------------------------------------
func TestGracefulTeardownAllTransports_remove_peer_is_noticed_by_remote(t *testing.T) {
	setUpGracefulTeardown_(t)
	for _, name := range PAIR_NAMES_ {
		t.Run(name, func(t *testing.T) {
			pr := pair_(t, name)
			defer pr.stop_all()
			pr.peer(t)
			aid, bid := pr.a.Global_id(), pr.b.Global_id()
			probe := New_AsyncioErrors_().Enter()
			probe.Watch(pr.pa, pr.pb)
			t0 := time.Now()
			pr.ca().Remove_peer(bid)
			if pr.ca().Peers.Contains(bid) {
				t.Fatalf("%s: %s still in A.peers", name, bid)
			}
			if pr.pa.Has_peer(bid) {
				t.Fatalf("%s: A still has peer B", name)
			}
			noticed := streaming.Wait_until(func() bool { return !pr.cb().Peers.Contains(aid) }, 3.0)
			dt := time.Since(t0).Seconds()
			probe.Exit()
			if !noticed {
				t.Fatalf("%s: remote kept us after remove_peer", name)
			}
			if dt >= 3.0 {
				t.Fatalf("%s: took %.2fs", name, dt)
			}
			if pr.pb.Has_peer(aid) {
				t.Fatalf("%s: B still has peer A", name)
			}
			assertNoErrors_(t, probe.Report(), name)
			// both transports are still up; re-peer works
			if !py.Truthy(getattr_default_(pr.pa, "_started", false)) || !py.Truthy(getattr_default_(pr.pb, "_started", false)) {
				t.Fatalf("%s: transports not started", name)
			}
			pr.peer(t)
			if !pr.cb().Peers.Contains(aid) {
				t.Fatalf("%s: re-peer failed", name)
			}
		})
	}
}

// 3 -----------------------------------------------------------------
func TestGracefulTeardownAllTransports_one_sided_stop_is_noticed_by_remote(t *testing.T) {
	setUpGracefulTeardown_(t)
	for _, name := range PAIR_NAMES_ {
		t.Run(name, func(t *testing.T) {
			pr := pair_(t, name)
			defer pr.stop_all()
			pr.peer(t)
			aid, bid := pr.a.Global_id(), pr.b.Global_id()
			probe := New_AsyncioErrors_().Enter()
			probe.Watch(pr.pa, pr.pb)
			must(0, pr.ca().Stop())
			noticed := streaming.Wait_until(func() bool { return !pr.cb().Peers.Contains(aid) }, 3.0)
			sleep(0.1)
			probe.Exit()
			if !noticed {
				t.Fatalf("%s: remote kept us after one-sided stop", name)
			}
			if pr.pb.Has_peer(aid) {
				t.Fatalf("%s: B still has peer A", name)
			}
			assertNoErrors_(t, probe.Report(), name)
			assert_proto_down_(t, pr.pa, name)
			// B is untouched and still serves
			if !py.Truthy(getattr_default_(pr.pb, "_started", false)) {
				t.Fatalf("%s: B stopped", name)
			}
			if pr.ca().Peers.Contains(bid) {
				t.Fatalf("%s: A still lists B", name)
			}
		})
	}
}

// 4 -----------------------------------------------------------------
func TestGracefulTeardownAllTransports_send_after_stop_raises_connection_error(t *testing.T) {
	setUpGracefulTeardown_(t)
	for _, name := range PAIR_NAMES_ {
		t.Run(name, func(t *testing.T) {
			pr := pair_(t, name)
			defer pr.stop_all()
			pr.peer(t)
			bid := pr.b.Global_id()
			must(0, pr.ca().Stop())
			_, err := pr.pa.Send_rpc(bid, []string{"__comm_ping__"}, nil, py.Kw{})
			assertRaises(t, py.ConnectionError, err)
			if pr.pa.Ping(bid) {
				t.Fatalf("%s: ping after stop succeeded", name)
			}
			_, err = pr.ca().Send_rpc_(bid, []string{"echo"}, []any{"x"}, py.Kw{}, nil)
			assertRaises(t, py.ConnectionError, err)
			// stop is idempotent
			must(0, pr.ca().Stop())
			must(0, pr.pa.Stop())
		})
	}
}

// 5 -----------------------------------------------------------------

// The liveness sweep must only drop peers that stop answering.
//
// Guards the WebSocket regression where ``ping()`` was the base
// ``return False`` and every ws peer was dropped on the first sweep.
func TestGracefulTeardownAllTransports_liveness_keeps_healthy_peers(t *testing.T) {
	setUpGracefulTeardown_(t)
	for _, name := range PAIR_NAMES_ {
		t.Run(name, func(t *testing.T) {
			pr := pair_(t, name)
			defer pr.stop_all()
			pr.peer(t)
			aid, bid := pr.a.Global_id(), pr.b.Global_id()
			for _, pol := range []*laila.LAILA_IDENTIFIABLE_POLICY_{pr.a, pr.b} {
				comm(pol).Liveness_interval = 0.2
				comm(pol).Stop_liveness_()
				comm(pol).Start_liveness_()
			}
			sleep(1.2) // ~6 sweeps on each side
			if !pr.ca().Peers.Contains(bid) {
				t.Fatalf("%s: A dropped healthy B", name)
			}
			if !pr.cb().Peers.Contains(aid) {
				t.Fatalf("%s: B dropped healthy A", name)
			}
			if !pr.pa.Ping(bid) {
				t.Fatalf("%s: ping A->B failed", name)
			}
			if !pr.pb.Ping(aid) {
				t.Fatalf("%s: ping B->A failed", name)
			}
		})
	}
}

// 6 -----------------------------------------------------------------
func TestGracefulTeardownAllTransports_laila_terminate_tears_everything_down(t *testing.T) {
	setUpGracefulTeardown_(t)
	for _, name := range PAIR_NAMES_ {
		t.Run(name, func(t *testing.T) {
			pr := pair_(t, name)
			defer pr.stop_all()
			pr.peer(t)
			laila.Activate_policy(pr.a)
			laila.Activate_policy(pr.b)
			probe := New_AsyncioErrors_().Enter()
			probe.Watch(pr.pa, pr.pb)
			errors := laila.Terminate()
			sleep(0.1)
			probe.Exit()
			if len(errors) != 0 {
				t.Fatalf("%s: terminate reported errors: %v", name, errors)
			}
			assertNoErrors_(t, probe.Report(), name)
			for _, proto := range []laila.Comm_protocol_{pr.pa, pr.pb} {
				assert_proto_down_(t, proto, name)
			}
			if laila.Remote_policies_.Len() != 0 {
				t.Fatalf("%s: _remote_policies: %s", name, py.Repr(laila.Remote_policies_))
			}
			if laila.Local_policies_.Len() != 0 {
				t.Fatalf("%s: _local_policies: %s", name, py.Repr(laila.Local_policies_))
			}
		})
	}
}

// ----------------------------------------------------------------------
// TestFailedStartLeaksNothing: a ``start()`` whose boot raises must leave
// no loop / thread behind.
// ----------------------------------------------------------------------

func TestFailedStartLeaksNothing_failed_boot_closes_loop(t *testing.T) {
	proto := must(laila.New_DefaultUARTProtocol()) // no port -> _open_stream raises
	baseline := open_fds_()
	assertRaises(t, py.RuntimeError, proto.Start())
	assert_proto_down_(t, proto, "uart-no-port")
	if !py.IsNone(getattr_default_(proto, "_inbound_executor", nil)) {
		t.Fatal("inbound executor left behind")
	}
	if leaked := fds_minus_(open_fds_(), baseline); len(leaked) != 0 {
		t.Fatalf("fds leaked: %v", leaked)
	}
	must(0, proto.Stop()) // idempotent on a never-started protocol
}

// ----------------------------------------------------------------------
// TestUartGoodbye: the serial carrier says goodbye on the wire before
// closing the port.
// ----------------------------------------------------------------------

func TestUartGoodbye_stop_sends_peer_disconnect_notification(t *testing.T) {
	up, err := streaming.New_UartPtyPair(streaming.Rpc_only_peer_).Enter()
	if err != nil {
		t.Fatal(err)
	}
	defer up.Exit()
	gid := up.Peer_gid
	if !up.Proto.Ping(gid) {
		t.Fatal("ping failed")
	}
	probe := New_AsyncioErrors_().Enter()
	probe.Watch(up.Proto)
	must(0, up.Comm_().Stop())
	sleep(0.2)
	probe.Exit()
	assertNoErrors_(t, probe.Report(), "uart")
	assert_proto_down_(t, up.Proto, "uart")
	// The fake peer does not implement peer.disconnect: it answers
	// the unknown method with a JSON-RPC error (id None) -- proof
	// that the notification reached it before the port closed.
	peer := up.Rpc_peer()
	if !streaming.Wait_until(func() bool {
		for _, r := range peer.Responses_seen_() {
			if py.IsNone(r.GetDefault("id", nil)) && r.Contains("error") {
				return true
			}
		}
		return peer.Handshakes_() >= 1
	}, 2.0) {
		t.Fatal("goodbye never reached the peer")
	}
}

func TestUartGoodbye_remove_connection_then_add_connection_again(t *testing.T) {
	up, err := streaming.New_UartPtyPair(streaming.Rpc_only_peer_).Enter()
	if err != nil {
		t.Fatal(err)
	}
	defer up.Exit()
	gid := up.Peer_gid
	must(0, up.Comm_().Remove_connection(up.Proto))
	if up.Comm_().Connections.Len() != 0 {
		t.Fatal(py.Repr(up.Comm_().Connections))
	}
	if up.Comm_().Peers.Contains(gid) {
		t.Fatal("peer still registered")
	}
	assert_proto_down_(t, up.Proto, "uart")
	must(0, up.Comm_().Add_connection(up.Proto))
	assertEq(t, must(up.Comm_().Add_peer("serial://"+up.Proto.Port, up.Proto.Peer_secret_key)), gid)
	if !up.Proto.Ping(gid) {
		t.Fatal("ping failed after re-add")
	}
	assertEq(t, up.Rpc_peer().Handshakes_(), 2)
}

// ----------------------------------------------------------------------
// TestLoopThreadHelpers: unit checks on the shared loop-thread lifecycle.
// ----------------------------------------------------------------------

type loopOwner_ struct{ laila.Loop_thread_state_ }

func TestLoopThreadHelpers_stop_closes_loop_and_cancels_tasks(t *testing.T) {
	p := &loopOwner_{}
	started := py.NewEvent()
	var hung []any

	async_start := func(ready *py.Event) error {
		forever := func() (any, error) {
			if err := py.Sleep(3600); err != nil {
				if py.IsInstance(err, py.CancelledError) {
					hung = append(hung, "cancelled")
				}
				return nil, err
			}
			return nil, nil
		}
		task := must(py.Ensure_future(forever))
		hung = append(hung, task)
		started.Set()
		ready.Set()
		return nil
	}

	must(0, laila.Start_loop_thread(p, async_start, 5.0))
	if !started.Wait(2.0) {
		t.Fatal("async_start never ran")
	}
	loop := p.Loop()
	if !loop.Is_running() {
		t.Fatal("loop not running")
	}
	laila.Stop_loop_thread(p, laila.Cancel_pending_tasks)
	if p.Loop() != nil {
		t.Fatal("_event_loop not cleared")
	}
	if p.Loop_thread_ != nil {
		t.Fatal("_loop_thread not cleared")
	}
	if !loop.Is_closed() {
		t.Fatal("loop not closed")
	}
	found := false
	for _, h := range hung {
		if s, ok := h.(string); ok && s == "cancelled" {
			found = true
		}
	}
	if !found {
		t.Fatal("pending task was not cancelled")
	}
	// idempotent
	laila.Stop_loop_thread(p, nil)
}

func TestLoopThreadHelpers_failed_start_raises_and_closes_loop(t *testing.T) {
	p := &loopOwner_{}
	boom := func(ready *py.Event) error { return py.RuntimeError.New("no device") }
	assertRaises(t, py.RuntimeError, laila.Start_loop_thread(p, boom, 5.0))
	if p.Loop() != nil {
		t.Fatal("_event_loop not cleared")
	}
	if p.Loop_thread_ != nil {
		t.Fatal("_loop_thread not cleared")
	}
}
