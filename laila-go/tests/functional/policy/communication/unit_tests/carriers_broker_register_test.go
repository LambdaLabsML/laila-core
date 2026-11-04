// Port of tests/functional/policy/communication/unit_tests/test_carriers_broker_register.py.
//
// Unit tests for the broker and register RPC carriers.
//
// Both are exercised over in-process fakes -- a tiny pub/sub bus for the
// broker carrier and a shared mailbox-pair for the register carrier -- so
// the carrier logic (handshake, addressing, request/response correlation,
// off-thread dispatch) is validated without a real broker or hardware bus.
package unit_tests

import (
	"strings"
	"sync"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// ------------------------------------------------------------------ #
// Broker carrier over an in-process pub/sub bus
// ------------------------------------------------------------------ #

var (
	bus_mu_ sync.Mutex
	bus_    = map[string][]*FakeBrokerProtocol_{}
)

// FakeBrokerProtocol_ is ``_FakeBrokerProtocol``.
type FakeBrokerProtocol_ struct {
	laila.BrokerRPCProtocol_
}

var FakeBrokerProtocol_class = py.NewClass(
	"_FakeBrokerProtocol", (*FakeBrokerProtocol_)(nil), laila.BrokerRPCProtocol_class,
).
	Set_classvar("protocol_name", "fakebroker").
	Set_classvar("matches_token", laila.Token_aliases_classmethod_("fakebroker")).
	Set_classvar("can_handle_uri", laila.Uri_prefix_classmethod_("fakebroker://"))

func New_FakeBrokerProtocol_(kw ...py.Kw) (*FakeBrokerProtocol_, error) {
	p := FakeBrokerProtocol_class.New().(*FakeBrokerProtocol_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *FakeBrokerProtocol_) Matches_token(token string) bool {
	return strings.ToLower(token) == "fakebroker"
}

func (p *FakeBrokerProtocol_) Can_handle_uri(uri string) bool {
	return strings.HasPrefix(uri, "fakebroker://")
}

func (p *FakeBrokerProtocol_) Broker_connect_() error { return nil }

func (p *FakeBrokerProtocol_) Broker_subscribe_(topic string) error {
	bus_mu_.Lock()
	bus_[topic] = append(bus_[topic], p)
	bus_mu_.Unlock()
	return nil
}

func (p *FakeBrokerProtocol_) Broker_publish_(topic string, data []byte) error {
	bus_mu_.Lock()
	subs := append([]*FakeBrokerProtocol_(nil), bus_[topic]...)
	bus_mu_.Unlock()
	for _, sub := range subs {
		if sub.Loop() != nil {
			buf := append([]byte(nil), data...)
			_, _ = sub.Loop().Call_soon_threadsafe(func() { sub.Feed_message_(buf) })
		}
	}
	return nil
}

func (p *FakeBrokerProtocol_) Broker_close_() error {
	bus_mu_.Lock()
	defer bus_mu_.Unlock()
	for topic, subs := range bus_ {
		kept := subs[:0]
		for _, s := range subs {
			if s != p {
				kept = append(kept, s)
			}
		}
		bus_[topic] = kept
	}
	return nil
}

func reset_bus_() {
	bus_mu_.Lock()
	bus_ = map[string][]*FakeBrokerProtocol_{}
	bus_mu_.Unlock()
}

func TestBrokerCarrier_broker_echo_round_trip(t *testing.T) {
	reset_bus_()
	a := mk_(t, must(New_FakeBrokerProtocol_()))
	b := mk_(t, must(New_FakeBrokerProtocol_()))
	defer stopAll(a, b)

	pb := proto_(b)
	rid, err := comm(a).Add_peer("fakebroker://"+b.Global_id(), secret_of_(pb))
	if err != nil {
		t.Fatal(err)
	}
	assertEq(t, rid, b.Global_id())
	b.Private_set("echo", func(m string) string { return "b:" + m })
	remote_b := must(comm(a).Peers.Getitem(b.Global_id())).(*laila.PeerProxy)
	assertEq(t, must(remote_b.Attr("echo").Invoke("hi")), "b:hi")
}

func TestBrokerCarrier_broker_wrong_secret_rejected(t *testing.T) {
	reset_bus_()
	a := mk_(t, must(New_FakeBrokerProtocol_(py.Kw{"handshake_timeout": 2.0})))
	b := mk_(t, must(New_FakeBrokerProtocol_()))
	defer stopAll(a, b)

	_, err := comm(a).Add_peer("fakebroker://"+b.Global_id(), "nope")
	assertRaises(t, py.ConnectionError, err)
}

// ------------------------------------------------------------------ #
// Register carrier over a shared mailbox pair
// ------------------------------------------------------------------ #

type mailboxPair_ struct {
	a, b [][]byte
}

var (
	reg_links_mu_ sync.Mutex
	reg_links_    = map[string]*mailboxPair_{}
)

// FakeRegisterProtocol_ is ``_FakeRegisterProtocol``.
type FakeRegisterProtocol_ struct {
	laila.RegisterRPCProtocol_

	Link_id string `py:"link_id" default:""`
	Side    string `py:"side" default:"a"`
}

var FakeRegisterProtocol_class = py.NewClass(
	"_FakeRegisterProtocol", (*FakeRegisterProtocol_)(nil), laila.RegisterRPCProtocol_class,
).
	Set_classvar("protocol_name", "fakereg").
	Set_classvar("matches_token", laila.Token_aliases_classmethod_("fakereg")).
	Set_classvar("can_handle_uri", laila.Uri_prefix_classmethod_("fakereg://"))

func New_FakeRegisterProtocol_(kw ...py.Kw) (*FakeRegisterProtocol_, error) {
	p := FakeRegisterProtocol_class.New().(*FakeRegisterProtocol_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *FakeRegisterProtocol_) Matches_token(token string) bool {
	return strings.ToLower(token) == "fakereg"
}

func (p *FakeRegisterProtocol_) Can_handle_uri(uri string) bool {
	return strings.HasPrefix(uri, "fakereg://")
}

func (p *FakeRegisterProtocol_) Open_bus_() error {
	reg_links_mu_.Lock()
	if reg_links_[p.Link_id] == nil {
		reg_links_[p.Link_id] = &mailboxPair_{}
	}
	reg_links_mu_.Unlock()
	return nil
}

func (p *FakeRegisterProtocol_) Deliver_(data []byte) error {
	reg_links_mu_.Lock()
	defer reg_links_mu_.Unlock()
	link := reg_links_[p.Link_id]
	buf := append([]byte(nil), data...)
	if p.Side == "a" {
		link.b = append(link.b, buf)
	} else {
		link.a = append(link.a, buf)
	}
	return nil
}

func (p *FakeRegisterProtocol_) Poll_inbound_() ([]byte, error) {
	reg_links_mu_.Lock()
	defer reg_links_mu_.Unlock()
	link := reg_links_[p.Link_id]
	q := &link.a
	if p.Side == "b" {
		q = &link.b
	}
	if len(*q) == 0 {
		return nil, nil
	}
	head := (*q)[0]
	*q = (*q)[1:]
	return head, nil
}

func TestRegisterCarrier_register_echo_round_trip(t *testing.T) {
	reg_links_mu_.Lock()
	reg_links_ = map[string]*mailboxPair_{}
	reg_links_mu_.Unlock()

	a := mk_(t, must(New_FakeRegisterProtocol_(py.Kw{"link_id": "L1", "side": "a", "poll_interval": 0.01})))
	b := mk_(t, must(New_FakeRegisterProtocol_(py.Kw{"link_id": "L1", "side": "b", "poll_interval": 0.01})))
	defer stopAll(a, b)

	pb := proto_(b)
	rid, err := comm(a).Add_peer("fakereg://bus", secret_of_(pb))
	if err != nil {
		t.Fatal(err)
	}
	assertEq(t, rid, b.Global_id())
	b.Private_set("echo", func(m string) string { return "r:" + m })
	remote_b := must(comm(a).Peers.Getitem(b.Global_id())).(*laila.PeerProxy)
	assertEq(t, must(remote_b.Attr("echo").Invoke("hi")), "r:hi")
}
