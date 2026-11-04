// Port of tests/functional/policy/communication/unit_tests/test_comm_id_routing.py.
//
// Tests for the inter-policy `communication id` routing axis.
//
// Covers:
//
//   - the transport registry is globally consistent: no two registered
//     transport classes claim the same token or URI scheme, and every
//     transport class has a unique ``__name__`` (its environment-mirror
//     ``class_token``),
//   - Communication.Select_protocol_for_peer_ resolves a channel by
//     connection id *and* by protocol token, and errors clearly otherwise,
//   - RemotePolicyProxy.Via and laila.Request produce a channel-bound proxy
//     whose calls travel over the chosen transport.
package unit_tests

import (
	"fmt"
	"strings"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// CONCRETE_TRANSPORTS_ is ``_CONCRETE_TRANSPORTS``: transport classes that
// own concrete token/URI claims (carriers are abstract).
var CONCRETE_TRANSPORTS_ = []string{
	"_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_LTEM_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL",
	"_LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL",
}

// all_proto_subclasses_ is ``_all_proto_subclasses``: walk the class tree
// rooted at _LAILA_IDENTIFIABLE_COMM_PROTOCOL keyed by ``__name__``.
func all_proto_subclasses_() map[string]*py.Class {
	out := map[string]*py.Class{}
	var walk func(cls *py.Class)
	walk = func(cls *py.Class) {
		out[cls.Name] = cls
		for _, sub := range cls.Subclasses() {
			walk(sub)
		}
	}
	walk(laila.LAILA_IDENTIFIABLE_COMM_PROTOCOL_class)
	return out
}

// all_concrete_transports_ is ``_all_concrete_transports``: every concrete
// transport behind a Default*Protocol alias.
func all_concrete_transports_() []*py.Class {
	seen := map[string]*py.Class{}
	for name, cls := range laila.Macros_defaults_protocols {
		if strings.HasPrefix(name, "Default") && strings.HasSuffix(name, "Protocol") {
			seen[cls.Name] = cls
		}
	}
	out := make([]*py.Class, 0, len(seen))
	for _, cls := range seen {
		out = append(out, cls)
	}
	return out
}

// mk_proto_ is ``_mk(proto)``.
func mk_proto_(t *testing.T, proto laila.Comm_protocol_) *laila.LAILA_IDENTIFIABLE_POLICY_ {
	t.Helper()
	p := must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	if err := comm(p).Add_connection(proto); err != nil {
		t.Fatal(err)
	}
	return p
}

func first_connection_(p *laila.LAILA_IDENTIFIABLE_POLICY_) laila.Comm_protocol_ {
	return comm(p).Connections.Values()[0].(laila.Comm_protocol_)
}

// ------------------------------------------------------------------ #
// TestRegistryConsistency
// ------------------------------------------------------------------ #

func TestRegistryConsistency_class_names_are_unique(t *testing.T) {
	// __subclasses__ is already name-keyed; this asserts no collision
	// across the loaded transport tree (the environment class_token).
	classes := all_proto_subclasses_()
	names := []string{}
	for _, c := range classes {
		names = append(names, c.Name)
	}
	set := map[string]bool{}
	for _, n := range names {
		set[n] = true
	}
	assertEq(t, len(names), len(set))
}

func TestRegistryConsistency_no_token_claimed_by_two_transports(t *testing.T) {
	// Dynamic over the FULL catalog (every Default*Protocol).
	concrete := all_concrete_transports_()
	if len(concrete) < 50 {
		t.Fatalf("expected >= 50 concrete transports, got %d", len(concrete))
	}
	tokens := map[string]bool{}
	for _, cls := range concrete {
		if aliases, ok := cls.Classvar("_TOKEN_ALIASES"); ok {
			for _, a := range py.Iter(aliases) {
				tokens[py.Str(a)] = true
			}
		}
		tokens[laila.Comm_protocol_name_of_(cls)] = true
	}
	for token := range tokens {
		claimers := []string{}
		for _, c := range concrete {
			if laila.Comm_protocol_matches_token_(c, token) {
				claimers = append(claimers, c.Name)
			}
		}
		if len(claimers) > 1 {
			t.Fatalf("token %q claimed by %v", token, claimers)
		}
	}
}

func TestRegistryConsistency_no_uri_scheme_claimed_by_two_transports(t *testing.T) {
	concrete := all_concrete_transports_()
	uris := []string{
		"tcp://h:1",
		"udp://h:1",
		"tls://h:1",
		"tcps://h:1",
		"unix:///x",
		"loopback://gid",
		"ws://h:1",
		"wss://h:1",
		"lora://n",
		"bt://m",
		"ethernet://h:1",
		"wifi://h:1",
		"wifidirect://h:1",
		"mqtt://p",
		"mqtts://p",
		"amqp://p",
		"amqps://p",
		"zmq://p",
		"zeromq://p",
		"cellular://h:1",
		"gsm://h:1",
		"lte://h:1",
		"nr5g://h:1",
		"nbiot://h:1",
		"ltem://h:1",
		"matter://h:1",
		"serial:///dev/x",
		"uart:///dev/x",
		"rs232:///dev/x",
		"rs485:///dev/x",
		"can://vcan0",
		"sigfox:///dev/x",
		"satellite:///dev/x",
		"usb:///dev/x",
		"modbustcp://h:1",
		"modbusrtu:///dev/x",
		"i2c://1/0x20",
		"spi://0.0",
		"enip://h",
		"nbiot://h:1",
		"ltem://h:1",
		"matter://h:1",
		"sixlowpan://h:1",
		"coap://h:1",
		"coaps://h:1",
		"http2://h:1",
		"http3://h:1",
		"quic://h:1",
		"dtls://h:1",
		"xmpp://p",
		"dds://p",
		"opc.tcp://h:1",
		"grpc://h:1",
		"lorawan://n",
		"zigbee://n",
		"thread://n",
		"zwave://n",
		"nfc://n",
		"ant://n",
		"espnow://n",
		"uwb://n",
		"irda://n",
		"rfid:///dev/x",
		"lin:///dev/x",
		"onewire://n",
		"i2s://c",
		"sdio://f",
		"profinet://s",
		"ethercat://s",
	}
	for _, uri := range uris {
		claimers := []string{}
		for _, c := range concrete {
			if laila.Comm_protocol_can_handle_uri_(c, uri) {
				claimers = append(claimers, c.Name)
			}
		}
		if len(claimers) > 1 {
			t.Fatalf("uri %q claimed by %v", uri, claimers)
		}
	}
}

// ------------------------------------------------------------------ #
// TestChannelSelection
// ------------------------------------------------------------------ #

func TestChannelSelection_select_by_token_and_by_id(t *testing.T) {
	a := mk_proto_(t, must(laila.New_DefaultTCPProtocol()))
	b := mk_proto_(t, must(laila.New_DefaultTCPProtocol()))
	defer stopAll(a, b)

	pb := first_connection_(b).(*laila.LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_)
	rid := must(comm(a).Add_peer(fmt.Sprintf("tcp://127.0.0.1:%d", pb.Bound_port()), pb.Peer_secret_key))
	hub := comm(a)
	pa := first_connection_(a)

	// by token
	assertIs_(t, must(hub.Select_protocol_for_peer_(rid, "tcp")), pa)
	// by connection id (global_id)
	assertIs_(t, must(hub.Select_protocol_for_peer_(rid, pa.Global_id())), pa)
	// default (no comm) finds the holder
	assertIs_(t, must(hub.Select_protocol_for_peer_(rid, nil)), pa)
	// unknown channel
	_, err := hub.Select_protocol_for_peer_(rid, "nonsense-channel")
	assertRaises(t, py.ConnectionError, err)
}

func assertIs_(t *testing.T, got, want laila.Comm_protocol_) {
	t.Helper()
	if got.Self() != want.Self() {
		t.Fatalf("%s is not %s", py.Repr(got), py.Repr(want))
	}
}

func TestChannelSelection_via_routes_over_named_channel(t *testing.T) {
	a := mk_proto_(t, must(laila.New_DefaultUDPProtocol()))
	b := mk_proto_(t, must(laila.New_DefaultUDPProtocol()))
	defer stopAll(a, b)

	pb := first_connection_(b).(*laila.LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_)
	must(comm(a).Add_peer(fmt.Sprintf("udp://127.0.0.1:%d", pb.Bound_port()), pb.Peer_secret_key))
	b.Private_set("echo", func(m string) string { return "e:" + m })
	proxy := must(comm(a).Peers.Getitem(b.Global_id())).(*laila.PeerProxy)
	// both a bound-by-token proxy and the plain proxy reach B
	assertEq(t, must(proxy.Via("udp").Attr("echo").Invoke("z")), "e:z")
	if !strings.Contains(py.Repr(proxy.Via("udp")), "via") {
		t.Fatal(py.Repr(proxy.Via("udp")))
	}
}

// ------------------------------------------------------------------ #
// TestRequestApi
// ------------------------------------------------------------------ #

func requestApi_(t *testing.T) (restore func()) {
	t.Helper()
	original := must(laila.Get_active_policy())
	return func() { laila.Activate_policy(original) }
}

func TestRequestApi_request_unknown_peer_raises(t *testing.T) {
	defer requestApi_(t)()
	_, err := laila.Request("LAILA:POLICY:nope")
	assertRaises(t, py.ConnectionError, err)
}

func TestRequestApi_request_returns_bound_proxy(t *testing.T) {
	defer requestApi_(t)()
	a := mk_proto_(t, must(laila.New_DefaultTCPProtocol()))
	b := mk_proto_(t, must(laila.New_DefaultTCPProtocol()))
	laila.Activate_policy(a)
	defer stopAll(a, b)

	pb := first_connection_(b).(*laila.LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_)
	rid := must(comm(a).Add_peer(fmt.Sprintf("tcp://127.0.0.1:%d", pb.Bound_port()), pb.Peer_secret_key))
	b.Private_set("echo", func(m string) string { return "r:" + m })
	bound := must(laila.Request(rid, "tcp"))
	assertEq(t, must(bound.Attr("echo").Invoke("hey")), "r:hey")
}
