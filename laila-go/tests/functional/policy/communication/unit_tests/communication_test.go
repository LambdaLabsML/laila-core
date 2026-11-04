// Port of tests/functional/policy/communication/unit_tests/test_communication.py.
//
// Loopback localhost tests for the inter-policy communication system.
//
// Covers: lifecycle, bidirectional peering, secret-key auth, transparent
// remote proxy / RPC, the laila.add_peer public API, protocol helpers,
// and graceful cleanup.
package unit_tests

import (
	"errors"
	"strings"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func assertEq(t *testing.T, got, want any) {
	t.Helper()
	if !py.Eq(got, want) {
		t.Fatalf("got %s, want %s", py.Repr(got), py.Repr(want))
	}
}

func assertRaises(t *testing.T, exc *py.Type, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s, got nil", exc.Name)
	}
	if !errors.Is(err, exc) {
		t.Fatalf("expected %s, got %v", exc.Name, err)
	}
}

// make_policy creates a fresh policy with a TCP/IP protocol wired up.
func make_policy(t *testing.T) *laila.LAILA_IDENTIFIABLE_POLICY_ {
	t.Helper()
	p := must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	tcp := must(laila.New_DefaultTCPIPProtocol())
	if err := comm(p).Add_connection(tcp); err != nil {
		t.Fatal(err)
	}
	return p
}

func comm(p *laila.LAILA_IDENTIFIABLE_POLICY_) *laila.LAILA_IDENTIFIABLE_COMMUNICATION_ {
	return laila.As_communication_(p.Central.Communication)
}

// get_tcp returns the first TCP/IP protocol from a policy's communication.
func get_tcp(p *laila.LAILA_IDENTIFIABLE_POLICY_) *laila.LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_ {
	return comm(p).Connections.Values()[0].(*laila.LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_)
}

// peer peers policy *a* to policy *b* over localhost and returns the
// remote id.
func peer(t *testing.T, a, b *laila.LAILA_IDENTIFIABLE_POLICY_) string {
	t.Helper()
	tcp_b := get_tcp(b)
	if err := comm(b).Start(); err != nil {
		t.Fatal(err)
	}
	gid, err := comm(a).Add_tcpip_peer("127.0.0.1", tcp_b.Bound_port(), tcp_b.Peer_secret_key)
	if err != nil {
		t.Fatal(err)
	}
	return gid
}

func stopAll(ps ...*laila.LAILA_IDENTIFIABLE_POLICY_) {
	for _, p := range ps {
		_ = comm(p).Stop()
	}
}

func sleep(s float64) { time.Sleep(time.Duration(s * float64(time.Second))) }

// ------------------------------------------------------------------ #
// Protocol helpers (pure, no networking)
// ------------------------------------------------------------------ #

func TestProtocolHelpers_make_request_has_jsonrpc_version(t *testing.T) {
	req := laila.Make_request("rpc.call", py.DictOf("path", py.NewList("x")), "")
	assertEq(t, must(req.Getitem("jsonrpc")), "2.0")
	assertEq(t, must(req.Getitem("method")), "rpc.call")
	if !req.Contains("id") {
		t.Fatal("id missing")
	}
}

func TestProtocolHelpers_make_request_custom_id(t *testing.T) {
	req := laila.Make_request("peer.connect", py.NewDict(), "abc")
	assertEq(t, must(req.Getitem("id")), "abc")
}

func TestProtocolHelpers_make_result(t *testing.T) {
	res := laila.Make_result("req-1", py.DictOf("ok", true))
	assertEq(t, must(res.Getitem("id")), "req-1")
	assertEq(t, must(res.Getitem("result")), py.DictOf("ok", true))
}

func TestProtocolHelpers_make_error_without_data(t *testing.T) {
	err := laila.Make_error("req-1", -32600, "bad request", nil)
	e := must(err.Getitem("error")).(*py.Dict)
	assertEq(t, must(e.Getitem("code")), -32600)
	assertEq(t, must(e.Getitem("message")), "bad request")
	if e.Contains("data") {
		t.Fatal("data should be absent")
	}
}

func TestProtocolHelpers_make_error_with_data(t *testing.T) {
	err := laila.Make_error("req-1", -32002, "boom", py.DictOf("detail", 1))
	e := must(err.Getitem("error")).(*py.Dict)
	assertEq(t, must(e.Getitem("data")), py.DictOf("detail", 1))
}

func TestProtocolHelpers_encode_decode_roundtrip(t *testing.T) {
	obj := py.DictOf("key", py.NewList(1, 2, 3), "nested", py.DictOf("a", true))
	assertEq(t, must(laila.Decode(must(laila.Encode(obj)))), obj)
}

func TestProtocolHelpers_is_request_and_is_response(t *testing.T) {
	req := laila.Make_request("rpc.call", py.NewDict(), "")
	if !laila.Is_request(req) || laila.Is_response(req) {
		t.Fatal("request classification wrong")
	}
	res := laila.Make_result("r1", nil)
	if laila.Is_request(res) || !laila.Is_response(res) {
		t.Fatal("response classification wrong")
	}
}

func TestProtocolHelpers_laila_json_encoder_handles_model_dump(t *testing.T) {
	entry := must(laila.Constant(py.Kw{"data": 42}))
	encoded := must(laila.Encode(py.DictOf("entry", entry)))
	decoded := must(laila.Decode(encoded)).(*py.Dict)
	if !decoded.Contains("entry") {
		t.Fatal("entry missing")
	}
}

// ------------------------------------------------------------------ #
// Lifecycle
// ------------------------------------------------------------------ #

func TestCommunicationLifecycle_start_binds_a_port(t *testing.T) {
	p := make_policy(t)
	tcp := get_tcp(p)
	must(0, comm(p).Start())
	defer stopAll(p)
	if !tcp.Started_ {
		t.Fatal("not started")
	}
	if tcp.Bound_port() <= 0 {
		t.Fatalf("bound_port %d", tcp.Bound_port())
	}
}

func TestCommunicationLifecycle_start_is_idempotent(t *testing.T) {
	p := make_policy(t)
	tcp := get_tcp(p)
	must(0, comm(p).Start())
	port_first := tcp.Bound_port()
	must(0, comm(p).Start())
	defer stopAll(p)
	assertEq(t, tcp.Bound_port(), port_first)
}

func TestCommunicationLifecycle_stop_clears_state(t *testing.T) {
	p := make_policy(t)
	tcp := get_tcp(p)
	must(0, comm(p).Start())
	must(0, comm(p).Stop())
	if tcp.Started_ {
		t.Fatal("still started")
	}
	assertEq(t, comm(p).Peers.Len(), 0)
}

func TestCommunicationLifecycle_stop_without_start_is_noop(t *testing.T) {
	p := make_policy(t)
	must(0, comm(p).Stop())
}

// ------------------------------------------------------------------ #
// Connection management
// ------------------------------------------------------------------ #

func TestConnectionManagement_add_connection_stores_protocol(t *testing.T) {
	c := must(laila.New_LAILA_IDENTIFIABLE_COMMUNICATION_(py.Kw{"policy_id": "test"}))
	tcp := must(laila.New_DefaultTCPIPProtocol())
	must(0, c.Add_connection(tcp))
	defer tcp.Stop()
	if !c.Connections.Contains(tcp.Global_id()) {
		t.Fatal("protocol not stored")
	}
	if !py.Is(tcp.Communication_, c) {
		t.Fatal("back-reference not set")
	}
}

func TestConnectionManagement_add_peer_with_no_protocols_raises(t *testing.T) {
	c := must(laila.New_LAILA_IDENTIFIABLE_COMMUNICATION_(py.Kw{"policy_id": "test"}))
	_, err := c.Add_peer("ws://127.0.0.1:9999", "secret")
	assertRaises(t, py.ConnectionError, err)
}

func TestConnectionManagement_can_handle_uri_for_tcpip(t *testing.T) {
	cls := laila.DefaultTCPIPProtocol
	if !laila.Comm_protocol_can_handle_uri_(cls, "ws://host:1234") {
		t.Fatal("ws:// not claimed")
	}
	if !laila.Comm_protocol_can_handle_uri_(cls, "wss://host:1234") {
		t.Fatal("wss:// not claimed")
	}
	if laila.Comm_protocol_can_handle_uri_(cls, "shm://region") {
		t.Fatal("shm:// claimed")
	}
}

// ------------------------------------------------------------------ #
// Peering
// ------------------------------------------------------------------ #

func TestPeering_bidirectional_peering(t *testing.T) {
	a, b := make_policy(t), make_policy(t)
	defer stopAll(a, b)
	peer(t, a, b)
	sleep(0.3)
	if !comm(a).Peers.Contains(b.Global_id()) {
		t.Fatal("b not in a.peers")
	}
	if !comm(b).Peers.Contains(a.Global_id()) {
		t.Fatal("a not in b.peers")
	}
}

func TestPeering_peer_returns_remote_global_id(t *testing.T) {
	a, b := make_policy(t), make_policy(t)
	defer stopAll(a, b)
	assertEq(t, peer(t, a, b), b.Global_id())
}

func TestPeering_peers_are_remote_policy_proxies(t *testing.T) {
	a, b := make_policy(t), make_policy(t)
	defer stopAll(a, b)
	peer(t, a, b)
	sleep(0.3)
	proxy := must(comm(a).Peers.Getitem(b.Global_id()))
	if !py.Isinstance(proxy, laila.RemotePolicyProxy_class) {
		t.Fatalf("%T is not a RemotePolicyProxy", proxy)
	}
	assertEq(t, proxy.(*laila.PeerProxy).Global_id(), b.Global_id())
}

// ------------------------------------------------------------------ #
// Secret key authentication
// ------------------------------------------------------------------ #

func TestSecretKeyAuth_wrong_secret_raises_connection_error(t *testing.T) {
	a, b := make_policy(t), make_policy(t)
	must(0, comm(b).Start())
	defer stopAll(a, b)
	tcp_b := get_tcp(b)
	_, err := comm(a).Add_tcpip_peer("127.0.0.1", tcp_b.Bound_port(), "definitely-wrong-secret")
	assertRaises(t, py.ConnectionError, err)
}

func TestSecretKeyAuth_correct_secret_succeeds(t *testing.T) {
	a, b := make_policy(t), make_policy(t)
	must(0, comm(b).Start())
	defer stopAll(a, b)
	tcp_b := get_tcp(b)
	remote_id := must(comm(a).Add_tcpip_peer("127.0.0.1", tcp_b.Bound_port(), tcp_b.Peer_secret_key))
	assertEq(t, remote_id, b.Global_id())
}

// ------------------------------------------------------------------ #
// Remote proxy
// ------------------------------------------------------------------ #

func TestRemoteProxy_proxy_global_id(t *testing.T) {
	c := must(laila.New_LAILA_IDENTIFIABLE_COMMUNICATION_(py.Kw{"policy_id": "sender"}))
	proxy := laila.New_RemotePolicyProxy("LAILA:POLICY:abcd-1234", c)
	assertEq(t, proxy.Global_id(), "LAILA:POLICY:abcd-1234")
}

func TestRemoteProxy_proxy_repr(t *testing.T) {
	c := must(laila.New_LAILA_IDENTIFIABLE_COMMUNICATION_(py.Kw{"policy_id": "sender"}))
	proxy := laila.New_RemotePolicyProxy("P1", c)
	if !strings.Contains(py.Repr(proxy), "P1") {
		t.Fatal(py.Repr(proxy))
	}
}

func TestRemoteProxy_getattr_returns_remote_attr_chain(t *testing.T) {
	c := must(laila.New_LAILA_IDENTIFIABLE_COMMUNICATION_(py.Kw{"policy_id": "sender"}))
	proxy := laila.New_RemotePolicyProxy("P1", c)
	chain := must(py.Getattr(proxy, "central"))
	if _, ok := chain.(*laila.RemoteAttrChain_); !ok {
		t.Fatalf("%T", chain)
	}
}

func TestRemoteProxy_attr_chain_extends_on_dotted_access(t *testing.T) {
	c := must(laila.New_LAILA_IDENTIFIABLE_COMMUNICATION_(py.Kw{"policy_id": "sender"}))
	proxy := laila.New_RemotePolicyProxy("P1", c)
	chain := proxy.Attr("central").Attr("memory").Attr("memorize")
	r := py.Repr(chain)
	for _, s := range []string{"central", "memory", "memorize"} {
		if !strings.Contains(r, s) {
			t.Fatal(r)
		}
	}
}

// ------------------------------------------------------------------ #
// RPC execution (end-to-end over loopback)
// ------------------------------------------------------------------ #

func rpcPair(t *testing.T) (a, b *laila.LAILA_IDENTIFIABLE_POLICY_) {
	t.Helper()
	a, b = make_policy(t), make_policy(t)
	peer(t, a, b)
	sleep(0.3)
	return a, b
}

func TestRemoteRPC_rpc_call_extend_on_remote(t *testing.T) {
	// B calls extend on A via proxy; verify pool appears on A.
	a, b := rpcPair(t)
	defer stopAll(a, b)
	pool := must(laila.New_LAILA_IDENTIFIABLE_POOL_())
	must(0, laila.As_central_memory_(a.Central.Memory).Extend(pool, py.Kw{"affinity": 0.5, "pool_nickname": "rpc-pool"}))

	remote_a := must(comm(b).Peers.Getitem(a.Global_id())).(*laila.PeerProxy)
	nicknames := must(remote_a.Attr("central").Attr("memory").Attr("pool_router").Attr("route").Invoke(
		py.Kw{"entries": py.NewList("x"), "pool_nickname": "rpc-pool"}))
	if py.IsNone(nicknames) {
		t.Fatal("route returned None")
	}
}

func TestRemoteRPC_rpc_round_trip_returns_result(t *testing.T) {
	// A exposes a simple callable; B invokes it via RPC and gets the result.
	a, b := rpcPair(t)
	defer stopAll(a, b)
	a.Private_set("echo", func(msg string) string { return "echo: " + msg })

	remote_a := must(comm(b).Peers.Getitem(a.Global_id())).(*laila.PeerProxy)
	result := must(remote_a.Attr("echo").Invoke("hello"))
	assertEq(t, result, "echo: hello")
}

func TestRemoteRPC_rpc_error_propagates_as_runtime_error(t *testing.T) {
	// A has a method that raises; B should get a RuntimeError.
	a, b := rpcPair(t)
	defer stopAll(a, b)
	a.Private_set("boom", func() (any, error) { return nil, py.ValueError.New("test-explosion") })

	remote_a := must(comm(b).Peers.Getitem(a.Global_id())).(*laila.PeerProxy)
	_, err := remote_a.Attr("boom").Invoke()
	assertRaises(t, py.RuntimeError, err)
	if !strings.Contains(err.Error(), "test-explosion") {
		t.Fatal(err)
	}
}

func TestRemoteRPC_rpc_bidirectional(t *testing.T) {
	// Both A and B can call methods on each other over the same connection.
	a, b := rpcPair(t)
	defer stopAll(a, b)
	a.Private_set("ping", func() string { return "pong-from-a" })
	b.Private_set("ping", func() string { return "pong-from-b" })

	remote_b := must(comm(a).Peers.Getitem(b.Global_id())).(*laila.PeerProxy)
	remote_a := must(comm(b).Peers.Getitem(a.Global_id())).(*laila.PeerProxy)

	assertEq(t, must(remote_b.Attr("ping").Invoke()), "pong-from-b")
	assertEq(t, must(remote_a.Attr("ping").Invoke()), "pong-from-a")
}

func TestRemoteRPC_send_rpc_to_disconnected_peer_raises(t *testing.T) {
	a, b := rpcPair(t)
	defer stopAll(a, b)
	_, err := comm(a).Send_rpc_("nonexistent-peer-id", []string{"foo"}, nil, py.Kw{}, nil)
	assertRaises(t, py.ConnectionError, err)
}

// ------------------------------------------------------------------ #
// Multiple peers
// ------------------------------------------------------------------ #

func TestMultiplePeers_one_policy_can_peer_with_multiple(t *testing.T) {
	a, b, c := make_policy(t), make_policy(t), make_policy(t)
	defer stopAll(a, b, c)
	peer(t, a, b)
	peer(t, a, c)
	sleep(0.3)

	peers_a := comm(a).Peers
	assertEq(t, peers_a.Len(), 2)
	if !peers_a.Contains(b.Global_id()) || !peers_a.Contains(c.Global_id()) {
		t.Fatal("peers missing")
	}
}

// ------------------------------------------------------------------ #
// Public API (laila.add_peer, laila.peers, activate_policy)
// ------------------------------------------------------------------ #

func publicAPI(t *testing.T) (a, b *laila.LAILA_IDENTIFIABLE_POLICY_, restore func()) {
	t.Helper()
	original := must(laila.Get_active_policy())
	a, b = make_policy(t), make_policy(t)
	laila.Activate_policy(a)
	return a, b, func() {
		stopAll(a, b)
		laila.Activate_policy(original)
	}
}

func TestPublicAPI_laila_add_peer(t *testing.T) {
	a, b, restore := publicAPI(t)
	defer restore()
	_ = a
	must(0, comm(b).Start())
	tcp_b := get_tcp(b)

	hub := laila.As_communication_(must(laila.Communication()))
	remote_id := must(hub.Add_tcpip_peer("127.0.0.1", tcp_b.Bound_port(), tcp_b.Peer_secret_key))
	assertEq(t, remote_id, b.Global_id())
}

func TestPublicAPI_laila_peers_returns_dict(t *testing.T) {
	_, _, restore := publicAPI(t)
	defer restore()
	peers := must(laila.Peers())
	if _, ok := peers.(*laila.PeerRegistry); !ok {
		t.Fatalf("%T", peers)
	}
}

func TestPublicAPI_activate_policy_with_proxy(t *testing.T) {
	a, b, restore := publicAPI(t)
	defer restore()
	peer(t, a, b)
	sleep(0.3)

	proxy := must(must(laila.Peers()).(*laila.PeerRegistry).Getitem(b.Global_id()))
	laila.Activate_policy(proxy)
	defer laila.Activate_policy(a)
	assertEq(t, must(py.Getattr(must(laila.Get_active_policy()), "global_id")), b.Global_id())
}

// ------------------------------------------------------------------ #
// execute_rpc edge cases
// ------------------------------------------------------------------ #

func TestExecuteRPC_execute_rpc_without_local_policy_raises(t *testing.T) {
	c := must(laila.New_LAILA_IDENTIFIABLE_COMMUNICATION_(py.Kw{"policy_id": "orphan"}))
	_, err := c.Execute_rpc_([]string{"central", "memory"}, nil, py.Kw{})
	assertRaises(t, py.RuntimeError, err)
}

func TestExecuteRPC_execute_rpc_bad_path_raises_attribute_error(t *testing.T) {
	p := make_policy(t)
	defer stopAll(p)
	_, err := comm(p).Execute_rpc_([]string{"nonexistent", "path"}, nil, py.Kw{})
	assertRaises(t, py.AttributeError, err)
}
