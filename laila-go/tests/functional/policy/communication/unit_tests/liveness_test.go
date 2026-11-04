// Port of tests/functional/policy/communication/unit_tests/test_liveness.py.
//
// Liveness: per-peer ping + the hub's async liveness loop.
//
// Verifies that:
//   - a protocol's ``ping`` round-trips for a live peer and is ``False`` for
//     an unknown peer,
//   - the communication layer's async liveness loop drops a peer whose
//     protocol reports it dead (exercised with a stub protocol so the test
//     is fast and deterministic).
package unit_tests

import (
	"fmt"
	"sync"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

func TestPing_ping_live_and_unknown_peer(t *testing.T) {
	a := mk_(t, must(laila.New_DefaultTCPProtocol()))
	b := mk_(t, must(laila.New_DefaultTCPProtocol()))
	defer stopAll(a, b)

	pb := proto_(b).(*laila.LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_)
	bid, err := comm(a).Add_peer(fmt.Sprintf("tcp://127.0.0.1:%d", pb.Bound_port()), pb.Peer_secret_key)
	if err != nil {
		t.Fatal(err)
	}
	pa := proto_(a).(*laila.LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_)
	if !pa.Ping(bid) {
		t.Fatal("live peer should ping True")
	}
	if pa.Ping("nobody") {
		t.Fatal("unknown peer pinged True")
	}
	// after disconnect, the peer is gone and ping is False
	comm(a).Remove_peer(bid)
	if pa.Has_peer(bid) {
		t.Fatal("has_peer after remove")
	}
	if pa.Ping(bid) {
		t.Fatal("ping after remove")
	}
}

// DeadProtocol_ is ``_DeadProtocol``: a stub transport that holds peers
// but always reports them dead.
type DeadProtocol_ struct {
	laila.LAILA_IDENTIFIABLE_COMM_PROTOCOL_

	mu    sync.Mutex
	Held_ map[string]bool `py:"_held"`
}

var DeadProtocol_class = py.NewClass("_DeadProtocol", (*DeadProtocol_)(nil), laila.LAILA_IDENTIFIABLE_COMM_PROTOCOL_class).
	Set_classvar("protocol_name", "deadstub")

func New_DeadProtocol_(kw ...py.Kw) (*DeadProtocol_, error) {
	p := DeadProtocol_class.New().(*DeadProtocol_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	p.Held_ = map[string]bool{}
	return p, nil
}

func (p *DeadProtocol_) Start() error { return nil }
func (p *DeadProtocol_) Stop() error  { return nil }

func (p *DeadProtocol_) Connect(uri string, secret string) (string, error) {
	return "", py.NotImplementedError.New()
}

func (p *DeadProtocol_) Has_peer(peer_id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Held_[peer_id]
}

func (p *DeadProtocol_) Ping(peer_id string, timeout ...any) bool { return false } // always "dead"

func (p *DeadProtocol_) Disconnect(peer_id string) error {
	p.mu.Lock()
	delete(p.Held_, peer_id)
	p.mu.Unlock()
	return nil
}

func (p *DeadProtocol_) Send_rpc(peer_id string, path []string, args []any, kwargs py.Kw) (any, error) {
	return nil, py.ConnectionError.New("dead")
}

func TestLivenessLoop_dead_peer_is_unregistered(t *testing.T) {
	c := must(laila.New_LAILA_IDENTIFIABLE_COMMUNICATION_(py.Kw{"policy_id": "liveness-test"}))
	c.Liveness_interval = 0.2
	proto := must(New_DeadProtocol_())
	if err := c.Add_connection(proto); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Stop() }()

	proto.mu.Lock()
	proto.Held_["PEER1"] = true
	proto.mu.Unlock()
	c.Register_peer_("PEER1") // also starts the liveness loop
	if !c.Peers.Contains("PEER1") {
		t.Fatal("PEER1 not registered")
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && c.Peers.Contains("PEER1") {
		sleep(0.1)
	}
	if c.Peers.Contains("PEER1") {
		t.Fatal("liveness loop did not drop dead peer")
	}
}
