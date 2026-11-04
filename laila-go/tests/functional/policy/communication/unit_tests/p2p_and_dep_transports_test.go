// Port of tests/functional/policy/communication/unit_tests/test_p2p_and_dep_transports.py.
//
// Tests for the point-to-point stream carrier and dependency-bound
// transports.
//
//   - The P2P carrier is validated over an in-memory connected duplex.
//   - The serial (UART/RS-232/RS-485), CAN and MQTT transports are checked
//     for clean token/URI routing and -- where their driver/device is
//     absent -- a clear, actionable capability error (never a bare
//     ImportError or a silent stub).
package unit_tests

import (
	"strings"
	"sync"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// ------------------------------------------------------------------ #
// P2P carrier over an in-memory connected duplex
// ------------------------------------------------------------------ #

type p2pEnd_ struct {
	reader *py.StreamReader
	loop   *py.Loop
}

var (
	p2p_links_mu_ sync.Mutex
	p2p_links_    = map[string]map[string]p2pEnd_{}
)

// FakeWriter_ is the ``_FakeWriter`` duplex half.
type FakeWriter_ struct {
	Link_id string
	Side    string
	Closed  bool
}

func (w *FakeWriter_) Write(data []byte) {
	if w.Closed {
		return
	}
	other := "b"
	if w.Side == "b" {
		other = "a"
	}
	p2p_links_mu_.Lock()
	entry, ok := p2p_links_[w.Link_id][other]
	p2p_links_mu_.Unlock()
	if ok {
		buf := append([]byte(nil), data...)
		_, _ = entry.loop.Call_soon_threadsafe(func() { entry.reader.Feed_data(buf) })
	}
}

func (w *FakeWriter_) Drain() error { return nil }

func (w *FakeWriter_) Close() { w.Closed = true }

// FakeP2PProtocol_ is ``_FakeP2PProtocol``.
type FakeP2PProtocol_ struct {
	laila.P2PStreamRPCProtocol_

	Link_id string `py:"link_id" default:""`
	Side    string `py:"side" default:"a"`
}

var FakeP2PProtocol_class = py.NewClass(
	"_FakeP2PProtocol", (*FakeP2PProtocol_)(nil), laila.P2PStreamRPCProtocol_class,
).
	Set_classvar("protocol_name", "fakep2p").
	Set_classvar("matches_token", laila.Token_aliases_classmethod_("fakep2p")).
	Set_classvar("can_handle_uri", laila.Uri_prefix_classmethod_("fakep2p://"))

func New_FakeP2PProtocol_(kw ...py.Kw) (*FakeP2PProtocol_, error) {
	p := FakeP2PProtocol_class.New().(*FakeP2PProtocol_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *FakeP2PProtocol_) Matches_token(token string) bool {
	return strings.ToLower(token) == "fakep2p"
}

func (p *FakeP2PProtocol_) Can_handle_uri(uri string) bool {
	return strings.HasPrefix(uri, "fakep2p://")
}

func (p *FakeP2PProtocol_) Open_stream_() (*py.StreamReader, laila.Stream_writer_, error) {
	reader := py.New_StreamReader()
	loop, err := py.Get_running_loop()
	if err != nil {
		return nil, nil, err
	}
	p2p_links_mu_.Lock()
	if p2p_links_[p.Link_id] == nil {
		p2p_links_[p.Link_id] = map[string]p2pEnd_{}
	}
	p2p_links_[p.Link_id][p.Side] = p2pEnd_{reader: reader, loop: loop}
	p2p_links_mu_.Unlock()
	return reader, &FakeWriter_{Link_id: p.Link_id, Side: p.Side}, nil
}

func TestP2PCarrier_p2p_echo_round_trip(t *testing.T) {
	p2p_links_mu_.Lock()
	p2p_links_ = map[string]map[string]p2pEnd_{}
	p2p_links_mu_.Unlock()

	a := mk_(t, must(New_FakeP2PProtocol_(py.Kw{"link_id": "L", "side": "a"})))
	b := mk_(t, must(New_FakeP2PProtocol_(py.Kw{"link_id": "L", "side": "b"})))
	defer stopAll(a, b)

	pb := proto_(b)
	rid, err := comm(a).Add_peer("fakep2p://bus", secret_of_(pb))
	if err != nil {
		t.Fatal(err)
	}
	assertEq(t, rid, b.Global_id())
	b.Private_set("echo", func(m string) string { return "p2p:" + m })
	remote_b := must(comm(a).Peers.Getitem(b.Global_id())).(*laila.PeerProxy)
	assertEq(t, must(remote_b.Attr("echo").Invoke("hi")), "p2p:hi")
}

// ------------------------------------------------------------------ #
// Dependency-bound transports: routing + capability errors
// ------------------------------------------------------------------ #

func TestSerialTransportsRouting_tokens_and_uris_disjoint(t *testing.T) {
	if !laila.Comm_protocol_matches_token_(laila.DefaultUARTProtocol, "serial") {
		t.Fatal("uart should match 'serial'")
	}
	if !laila.Comm_protocol_can_handle_uri_(laila.DefaultUARTProtocol, "serial:///dev/ttyUSB0") {
		t.Fatal("uart should claim serial://")
	}
	if !laila.Comm_protocol_matches_token_(laila.DefaultRS232Protocol, "rs-232") {
		t.Fatal("rs232 should match 'rs-232'")
	}
	if !laila.Comm_protocol_can_handle_uri_(laila.DefaultRS232Protocol, "rs232:///dev/ttyS0") {
		t.Fatal("rs232 should claim rs232://")
	}
	if !laila.Comm_protocol_matches_token_(laila.DefaultRS485Protocol, "rs485") {
		t.Fatal("rs485 should match 'rs485'")
	}
	if laila.Comm_protocol_matches_token_(laila.DefaultUARTProtocol, "rs485") {
		t.Fatal("uart must not match 'rs485'")
	}
}

func TestSerialTransportsRouting_uart_without_port_raises_clear_error(t *testing.T) {
	proto := must(laila.New_DefaultUARTProtocol()) // no port set
	err := proto.Start()
	assertRaises(t, py.RuntimeError, err)
	if !strings.Contains(strings.ToLower(err.Error()), "port") {
		t.Fatalf("expected 'port' in %q", err.Error())
	}
	if err := proto.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestSerialTransportsRouting_serial_modem_transports_route(t *testing.T) {
	if !laila.Comm_protocol_can_handle_uri_(laila.DefaultSigfoxProtocol, "sigfox:///dev/ttyUSB0") {
		t.Fatal("sigfox should claim sigfox://")
	}
	if !laila.Comm_protocol_matches_token_(laila.DefaultSatelliteProtocol, "iridium") {
		t.Fatal("satellite should match 'iridium'")
	}
	if !laila.Comm_protocol_can_handle_uri_(laila.DefaultUSBProtocol, "usb:///dev/ttyACM0") {
		t.Fatal("usb should claim usb://")
	}
	// serial-backed; without a port they raise the same clear error
	for _, factory := range []func(kw ...py.Kw) (laila.Comm_protocol_, error){
		wrap_(laila.New_DefaultSigfoxProtocol),
		wrap_(laila.New_DefaultSatelliteProtocol),
		wrap_(laila.New_DefaultUSBProtocol),
	} {
		proto := must(factory())
		assertRaises(t, py.RuntimeError, proto.Start())
		if err := proto.Stop(); err != nil {
			t.Fatal(err)
		}
	}
}

func assert_start_hint_(t *testing.T, proto laila.Comm_protocol_, hint string) {
	t.Helper()
	defer func() { _ = proto.Stop() }()
	err := proto.Start()
	assertRaises(t, py.RuntimeError, err)
	if !strings.Contains(err.Error(), hint) {
		t.Fatalf("expected %q in %q", hint, err.Error())
	}
}

func TestCapabilityErrors_can_missing_driver_raises_install_hint(t *testing.T) {
	assert_start_hint_(t, must(laila.New_DefaultCANProtocol(py.Kw{"channel": "vcan0"})), "laila-core[can]")
}

func TestCapabilityErrors_mqtt_missing_driver_raises_install_hint(t *testing.T) {
	assert_start_hint_(t, must(laila.New_DefaultMQTTProtocol()), "laila-core[mqtt]")
}

func TestCapabilityErrors_amqp_missing_driver_raises_install_hint(t *testing.T) {
	assert_start_hint_(t, must(laila.New_DefaultAMQPProtocol()), "laila-core[amqp]")
}

func TestCapabilityErrors_zeromq_missing_driver_raises_install_hint(t *testing.T) {
	assert_start_hint_(t, must(laila.New_DefaultZeroMQProtocol()), "laila-core[zmq]")
}

func TestCapabilityErrors_register_bus_missing_driver_raises_install_hint(t *testing.T) {
	cases := []struct {
		factory transport_factory_
		hint    string
	}{
		{wrap_(laila.New_DefaultModbusTCPProtocol), "laila-core[modbus]"},
		{wrap_(laila.New_DefaultI2CProtocol), "laila-core[i2c]"},
		{wrap_(laila.New_DefaultSPIProtocol), "laila-core[spi]"},
		{wrap_(laila.New_DefaultENIPProtocol), "laila-core[enip]"},
	}
	for _, c := range cases {
		assert_start_hint_(t, must(c.factory()), c.hint)
	}
}
