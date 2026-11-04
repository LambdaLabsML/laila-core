// Port of tests/functional/policy/communication/streaming/unit_tests/test_peer_registry.py.
//
// Tests 1-2: ``laila.peers`` registry semantics and RPC-only transports.
//
//  1. ``peers`` is a PeerRegistry (a dict) of PeerProxy objects that are
//     still RemotePolicyProxy; ``peers[gid]`` is the very object in
//     ``laila.remote_policies``; ``peers[gid]["default"]`` is the proxy
//     itself; proxies are not iterable; non-string keys and the reserved
//     name are rejected; every existing ``laila.peers`` /
//     ``laila.request`` usage is unchanged.
//  2. A transport whose class has ``supports_channels = False`` keeps
//     working for RPC but ``peers[gid][name]`` raises ``ConnectionError``
//     immediately (no RPC is attempted, no lane is bound).
package unit_tests

import (
	"fmt"
	"sync"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// ----------------------------------------------------------------------
// TestPeerRegistry
// ----------------------------------------------------------------------

func TestPeerRegistry_registry_and_proxy_semantics(t *testing.T) {
	original := must(laila.Get_active_policy())
	t.Cleanup(func() { laila.Activate_policy(original) })
	for _, kind := range []string{"loopback", "tcp"} {
		t.Run(kind, func(t *testing.T) {
			tp := enter_(t, New_TwoPolicies(kind))
			peers := tp.Ca().Peers
			assertTrue(t, fmt.Sprintf("%T", peers) == "*laila.PeerRegistry", "peers is "+fmt.Sprintf("%T", peers))
			assertTrue(t, peers.Dict != nil, "PeerRegistry must be a dict")
			assertEq(t, peers.Len(), 1)
			assertTrue(t, peers.Contains(tp.Bid), "B not in peers")
			assertEq(t, py.NewList(peers.Keys()...), py.NewList(tp.Bid))
			proxy := proxy_(t, tp.Ca(), tp.Bid)
			assertTrue(t, py.Isinstance(proxy, laila.PeerProxy_class), "not a PeerProxy")
			assertTrue(t, py.Isinstance(proxy, laila.RemotePolicyProxy_class), "not a RemotePolicyProxy")
			assertTrue(t, peers.GetDefault(tp.Bid, nil) == any(proxy), "peers.get differs")
			assertTrue(t, laila.Remote_policies().GetDefault(tp.Bid, nil) == any(proxy), "remote_policies differs")
			laila.Activate_policy(tp.A)
			assertTrue(t, must(laila.Peers()) == any(peers), "laila.peers differs")
			assertTrue(t, must(laila.Request(tp.Bid)) == proxy, "laila.request differs")
			// "default" is the RPC proxy
			assertTrue(t, must(proxy.Getitem("default")) == any(proxy), "proxy['default'] is not the proxy")
			via_default := must(proxy.Via(tp.Pa).Getitem("default"))
			assertTrue(t, py.Classof(via_default) == laila.PeerProxy_class, "via()['default'] is not a PeerProxy")
			// not iterable / no sequence fallback: a PeerProxy exposes no
			// Iter / Contains protocol in Go (statically); ``proxy[0]`` is a
			// TypeError exactly as in Python.
			_, err := proxy.Getitem(0)
			assertRaises(t, py.TypeError, err)
			_, err = peers.Channel(tp.Ca(), tp.Bid, "default", nil)
			assertRaises(t, py.ValueError, err)
			// attribute chains are still proxies (RPC happens on call)
			assertTrue(t, py.Hasattr(proxy.Attr("central").Attr("memory"), "remember"), "chain lost attribute access")
			// RPC still works
			assertTrue(t, tp.Pa.Ping(tp.Bid), "ping failed")
			// a channel name gives a Channel, cached
			ch := must(proxy.Channel("video"))
			assertTrue(t, must(proxy.Getitem("video")) == any(ch), "channel not cached")
			assertEq(t, str_list_(must(proxy.Channels())), py.NewList("video"))
			assertEq(t, py.Repr(proxy), fmt.Sprintf("PeerProxy(%s)", py.Repr(tp.Bid)))
		})
	}
}

func TestPeerRegistry_registry_dict_ops_and_unregister(t *testing.T) {
	tp := enter_(t, New_TwoPolicies("loopback"))
	peers := tp.Ca().Peers
	assertEq(t, peers.Dict.Copy(), py.DictOf(tp.Bid, peers.GetDefault(tp.Bid, nil)))
	tp.Ca().Remove_peer(tp.Bid)
	assertEq(t, peers.Len(), 0)
	assertTrue(t, !laila.Remote_policies().Contains(tp.Bid), "B still in remote_policies")
}

func TestPeerRegistry_peers_field_coerces_plain_dict(t *testing.T) {
	comm := Comm(must(laila.New_LAILA_IDENTIFIABLE_POLICY_()))
	assertTrue(t, comm.Peers != nil, "peers must be a PeerRegistry")
	comm2 := must(laila.New_LAILA_IDENTIFIABLE_COMMUNICATION_(py.Kw{"peers": py.NewDict()}))
	assertTrue(t, comm2.Peers != nil && fmt.Sprintf("%T", comm2.Peers) == "*laila.PeerRegistry", "peers={} not coerced")
}

// ----------------------------------------------------------------------
// TestRpcOnlyTransport
// ----------------------------------------------------------------------

// RpcOnlyLoopback_ is loopback with lanes switched off: stands in for a
// framed-RPC-only transport.
type RpcOnlyLoopback_ struct {
	laila.LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_
}

var RpcOnlyLoopback_class = py.NewClass(
	"_RpcOnlyLoopback", (*RpcOnlyLoopback_)(nil), laila.LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_class,
).
	Set_classvar("protocol_name", "rpconly").
	Set_classvar("supports_channels", false)

func New_RpcOnlyLoopback_(kw ...py.Kw) (*RpcOnlyLoopback_, error) {
	p := RpcOnlyLoopback_class.New().(*RpcOnlyLoopback_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Stream_enqueue_ must never run.
func (p *RpcOnlyLoopback_) Stream_enqueue_(channel *laila.Channel, payload []byte) error {
	panic(py.AssertionError.New("stream path reached on an RPC-only transport"))
}

func TestRpcOnlyTransport_channel_access_raises_without_rpc(t *testing.T) {
	original := must(laila.Get_active_policy())
	t.Cleanup(func() { laila.Activate_policy(original) })
	a := must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	b := must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	pa := must(New_RpcOnlyLoopback_())
	pb := must(New_RpcOnlyLoopback_())
	must(0, Comm(a).Add_connection(pa))
	must(0, Comm(b).Add_connection(pb))
	defer func() {
		_ = Comm(a).Stop()
		_ = Comm(b).Stop()
	}()
	bid := must(Comm(a).Add_peer("loopback://"+b.Global_id(), pb.Peer_secret_key))
	assertTrue(t, Wait_until(func() bool { return Comm(b).Peers.Contains(a.Global_id()) }), "B never registered A")
	proxy := proxy_(t, Comm(a), bid)
	assertTrue(t, py.Isinstance(proxy, laila.PeerProxy_class), "not a PeerProxy")
	assertTrue(t, pa.Ping(bid), "ping failed")
	assertTrue(t, must(proxy.Getitem("default")) == any(proxy), "proxy['default'] is not the proxy")
	assertTrue(t, !laila.Comm_protocol_supports_channels_(pa), "supports_channels must be False")
	assertEq(t, pa.Local_caps_(), py.NewDict())
	var mu sync.Mutex
	calls := []*py.Dict{}
	orig := pb.Build_response_
	spy := func(msg *py.Dict, peer_id any) *py.Dict {
		mu.Lock()
		calls = append(calls, msg)
		mu.Unlock()
		return orig(msg, peer_id)
	}
	pb.Private_set("_build_response", spy)
	t0 := py.Monotonic()
	_, err := proxy.Getitem("video")
	assertRaises(t, py.ConnectionError, err)
	assertTrue(t, py.Monotonic()-t0 < 1.0, "channel access blocked")
	assertIn_(t, "stream lanes", err)
	mu.Lock()
	n := len(calls)
	mu.Unlock()
	assertEq(t, n, 0) // no control RPC was sent
	assertEq(t, str_list_(must(proxy.Channels())), py.NewList())
	assertEq(t, str_list_(pa.Channel_names(bid)), py.NewList())
	laila.Activate_policy(a)
	_, err = laila.Get_relay(bid, "video")
	assertRaises(t, py.ConnectionError, err)
}

func TestRpcOnlyTransport_websocket_tcpip_is_rpc_only(t *testing.T) {
	assertTrue(t, !laila.Comm_protocol_supports_channels_(must(laila.New_DefaultTCPIPProtocol())),
		"DefaultTCPIPProtocol must not support channels")
}
