// Port of tests/functional/policy/communication/unit_tests/test_emulated_peers_memory.py.
//
// Cross-peer laila.Memorize / laila.Remember over emulated transports.
//
// This is the headline scenario: two peers (emulated over loopback,
// localhost TCP/UDP/Unix, or TLS) where one calls ``laila.memorize`` and
// the other's data is read back with ``laila.remember`` -- moving real
// ``Entry`` data across the wire (no shared pool), via the canonical
// serialize/rebuild pipeline.
//
// Also includes a latency smoke check: a localhost round-trip must stay
// well within a generous bound, guarding against accidental fixed delays.
package unit_tests

import (
	"fmt"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// emu_uri_ is ``_uri(name, policy_b, proto_b)``.
func emu_uri_(name string, policy_b *laila.LAILA_IDENTIFIABLE_POLICY_, proto_b any) string {
	if name == "unix" {
		return "unix://" + proto_b.(*laila.LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_).Bound_path()
	}
	if name == "loopback" {
		return "loopback://" + policy_b.Global_id()
	}
	return fmt.Sprintf("%s://127.0.0.1:%d", name, proto_b.(interface{ Bound_port() int }).Bound_port())
}

type emu_case_ struct {
	name    string
	factory transport_factory_
}

// EMU_CASES_ is ``_CASES`` (insertion order preserved).
var EMU_CASES_ = []emu_case_{
	{"loopback", func(kw ...py.Kw) (laila.Comm_protocol_, error) { return laila.New_DefaultLoopbackProtocol(kw...) }},
	{"tcp", func(kw ...py.Kw) (laila.Comm_protocol_, error) { return laila.New_DefaultTCPProtocol(kw...) }},
	{"udp", func(kw ...py.Kw) (laila.Comm_protocol_, error) { return laila.New_DefaultUDPProtocol(kw...) }},
	{"unix", func(kw ...py.Kw) (laila.Comm_protocol_, error) { return laila.New_DefaultUnixSocketProtocol(kw...) }},
}

func setUpEmulatedPeers_(t *testing.T) {
	t.Helper()
	original := must(laila.Get_active_policy())
	t.Cleanup(func() { laila.Activate_policy(original) })
}

// emu_peer_ is ``_peer(name, factory)``.
func emu_peer_(t *testing.T, name string, factory transport_factory_) (a, b *laila.LAILA_IDENTIFIABLE_POLICY_, bid string) {
	t.Helper()
	a = mk_(t, must(factory()))
	b = mk_(t, must(factory()))
	pb := proto_(b)
	bid = must(comm(a).Add_peer(emu_uri_(name, b, pb), secret_of_(pb)))
	return a, b, bid
}

func TestEmulatedPeersMemory_memorize_then_remember_across_each_transport(t *testing.T) {
	setUpEmulatedPeers_(t)
	for _, c := range EMU_CASES_ {
		t.Run(c.name, func(t *testing.T) {
			a, b, bid := emu_peer_(t, c.name, c.factory)
			defer stopAll(a, b)
			laila.Activate_policy(a)
			payload := py.DictOf("transport", c.name, "vals", py.NewList(1, 2, 3), "ok", true)
			entry := must(laila.Constant(payload))
			gid := entry.Global_id()

			// A memorizes INTO peer B -> a LOCAL future resolving to the gid
			mfut := must(laila.Memorize(entry, py.Kw{"policy_id": bid}))
			assertEq(t, data_of_(t, mfut), gid)

			// A remembers the real Entry data back FROM peer B ->
			// a LOCAL future whose .data is the payload, .wait() the Entry
			got := must(laila.Remember(gid, py.Kw{"policy_id": bid}))
			assertEq(t, data_of_(t, got), payload)
			waited := must(got.(interface{ Wait(timeout ...any) (any, error) }).Wait())
			assertEq(t, must(py.Getattr(waited, "global_id")), gid)
		})
	}
}

func TestEmulatedPeersMemory_localhost_round_trip_latency_is_small(t *testing.T) {
	setUpEmulatedPeers_(t)
	a, b, bid := emu_peer_(t, "tcp", func(kw ...py.Kw) (laila.Comm_protocol_, error) { return laila.New_DefaultTCPProtocol(kw...) })
	defer stopAll(a, b)
	laila.Activate_policy(a)
	entry := must(laila.Constant(py.DictOf("x", 1)))
	must(must(laila.Memorize(entry, py.Kw{"policy_id": bid})).(interface{ Wait(timeout ...any) (any, error) }).Wait())
	// warm, then time a remember round-trip (force resolution via .data)
	t0 := time.Now()
	data_of_(t, must(laila.Remember(entry.Global_id(), py.Kw{"policy_id": bid})))
	elapsed := time.Since(t0).Seconds()
	// generous bound: a localhost RPC must not be anywhere near a
	// stray sleep; this guards against accidental fixed delays.
	if elapsed >= 1.0 {
		t.Fatalf("localhost round-trip too slow: %.3fs", elapsed)
	}
}
