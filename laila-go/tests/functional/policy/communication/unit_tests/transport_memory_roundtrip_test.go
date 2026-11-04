// Port of tests/functional/policy/communication/unit_tests/test_transport_memory_roundtrip.py.
//
// Real memory-over-transport checks for the working transports.
//
// Verifies that a policy's *memory state* is queryable across each
// transport: policy B memorizes an Entry into its own pool, then policy A
// reads that state back over the transport via a remote call and gets the
// correct, real value. One case per carrier family (stream, datagram,
// Unix-stream, in-process loopback, IP-subclass).
//
// Note: passing a live ``Entry`` object as an RPC *argument* (e.g.
// ``proxy.central.memory.memorize(entry)``) is covered separately in
// rpc_morph_and_carrier_resilience_test.go -- the JSON-RPC layer ships a
// READY entry as its serialized envelope and the receiver rebuilds a real
// ``Entry`` before invoking the target. This file sticks to the
// ``remember(..., policy_id=peer)`` result-id pattern per transport.
package unit_tests

import (
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// alpha_pool_ is ``_alpha_pool(policy)``.
func alpha_pool_(t *testing.T, policy *laila.LAILA_IDENTIFIABLE_POLICY_) *laila.LAILA_IDENTIFIABLE_POOL_ {
	t.Helper()
	mem := memory_of_(policy)
	router := mem.Pool_router.(*laila.LAILA_IDENTIFIABLE_POOL_ROUTER_)
	pool, ok := router.Pools.Get(mem.Alpha_pool)
	if !ok {
		t.Fatalf("KeyError: %s", py.Repr(mem.Alpha_pool))
	}
	return laila.As_pool_(pool)
}

// ROUNDTRIP_CASES_ is ``_CASES``: name -> factory (working transports
// across every carrier family; tls is covered in tier1_transports_test.go).
var ROUNDTRIP_CASES_ = []emu_case_{
	{"tcp", func(kw ...py.Kw) (laila.Comm_protocol_, error) { return laila.New_DefaultTCPProtocol(kw...) }},
	{"udp", func(kw ...py.Kw) (laila.Comm_protocol_, error) { return laila.New_DefaultUDPProtocol(kw...) }},
	{"unix", func(kw ...py.Kw) (laila.Comm_protocol_, error) { return laila.New_DefaultUnixSocketProtocol(kw...) }},
	{"loopback", func(kw ...py.Kw) (laila.Comm_protocol_, error) { return laila.New_DefaultLoopbackProtocol(kw...) }},
	{"matter", func(kw ...py.Kw) (laila.Comm_protocol_, error) { return laila.New_DefaultMatterProtocol(kw...) }},
	{"sixlowpan", func(kw ...py.Kw) (laila.Comm_protocol_, error) { return laila.New_DefaultSixLoWPANProtocol(kw...) }},
}

// run_roundtrip_ is ``_run(name, factory)``.
func run_roundtrip_(t *testing.T, name string, factory transport_factory_) {
	t.Helper()
	a := mk_(t, must(factory()))
	b := mk_(t, must(factory()))
	defer stopAll(a, b)

	pb := proto_(b)
	uri := emu_uri_(name, b, pb)
	must(comm(a).Add_peer(uri, secret_of_(pb)))

	// B memorizes an Entry into its own pool (B active locally).
	laila.Activate_policy(b)
	entry := must(laila.Constant(py.DictOf("transport", name, "n", 7)))
	gid := entry.Global_id()
	must(memory_of_(b).Memorize(entry))
	pool_b := alpha_pool_(t, b)
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) && !pool_b.Resource.Contains(gid) {
		time.Sleep(50 * time.Millisecond)
	}
	if !pool_b.Resource.Contains(gid) {
		t.Fatalf("%s: B never stored the entry", name)
	}

	// A reads B's real memory state back over the transport.
	b.Private_set("pool_size", func() int { return pool_b.Resource.Len() })
	b.Private_set("has_entry", func(g string) bool { return pool_b.Resource.Contains(g) })
	laila.Activate_policy(a)
	proxy := must(comm(a).Peers.Getitem(b.Global_id())).(*laila.PeerProxy)
	assertEq(t, must(proxy.Attr("pool_size").Invoke()), pool_b.Resource.Len())
	if !py.Truthy(must(proxy.Attr("has_entry").Invoke(gid))) {
		t.Fatalf("%s: remote read missed entry", name)
	}
	if py.Truthy(must(proxy.Attr("has_entry").Invoke("LAILA:ENTRY:absent"))) {
		t.Fatalf("%s: absent entry reported present", name)
	}
}

func TestMemoryOverTransport_memorize_round_trips_over_each_transport(t *testing.T) {
	original := must(laila.Get_active_policy())
	t.Cleanup(func() { laila.Activate_policy(original) })
	for _, c := range ROUNDTRIP_CASES_ {
		t.Run(c.name, func(t *testing.T) { run_roundtrip_(t, c.name, c.factory) })
	}
}
