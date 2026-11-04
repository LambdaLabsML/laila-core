// Port of tests/functional/policy/communication/unit_tests/test_tier1_transports.py.
//
// Acceptance tests for the Tier-1 communication transports.
//
// Each transport (raw TCP, UDP, Unix domain socket, in-process loopback,
// and TLS) is exercised end-to-end between two in-process policies:
//
//   - peering handshake returns the correct remote ``global_id``,
//   - an RPC dispatched through the RemotePolicyProxy reaches the peer
//     and returns its result,
//   - a wrong secret is rejected,
//   - both the ``json`` and ``msgpack`` wire codecs work.
package unit_tests

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

func mk_(t *testing.T, proto laila.Comm_protocol_) *laila.LAILA_IDENTIFIABLE_POLICY_ {
	t.Helper()
	p := must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	if err := comm(p).Add_connection(proto); err != nil {
		t.Fatal(err)
	}
	return p
}

func proto_(p *laila.LAILA_IDENTIFIABLE_POLICY_) any {
	return comm(p).Connections.Values()[0]
}

var ip_schemes_ = map[string]bool{
	"tcp": true, "tls": true, "udp": true, "ethernet": true, "wifi": true, "wifidirect": true,
	"cellular": true, "gsm": true, "lte": true, "nr5g": true, "nbiot": true, "ltem": true, "matter": true,
}

func uri_for_(t *testing.T, name string, policy_b *laila.LAILA_IDENTIFIABLE_POLICY_, proto_b any) string {
	t.Helper()
	if ip_schemes_[name] {
		return fmt.Sprintf("%s://127.0.0.1:%d", name, proto_b.(interface{ Bound_port() int }).Bound_port())
	}
	if name == "unix" {
		return "unix://" + proto_b.(*laila.LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_).Bound_path()
	}
	if name == "loopback" {
		return "loopback://" + policy_b.Global_id()
	}
	t.Fatalf("unknown transport %s", name)
	return ""
}

type transport_factory_ func(kw ...py.Kw) (laila.Comm_protocol_, error)

func wrap_[T laila.Comm_protocol_](f func(kw ...py.Kw) (T, error)) transport_factory_ {
	return func(kw ...py.Kw) (laila.Comm_protocol_, error) {
		p, err := f(kw...)
		if err != nil {
			return nil, err
		}
		return p, nil
	}
}

var transports_ = []struct {
	name    string
	factory transport_factory_
}{
	{"tcp", wrap_(laila.New_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_)},
	{"udp", wrap_(laila.New_LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_)},
	{"unix", wrap_(laila.New_LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_)},
	{"loopback", wrap_(laila.New_LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_)},
	{"ethernet", wrap_(laila.New_LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL_)},
	{"wifi", wrap_(laila.New_LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL_)},
	{"wifidirect", wrap_(laila.New_LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL_)},
	{"cellular", wrap_(laila.New_LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL_)},
	{"gsm", wrap_(laila.New_LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL_)},
	{"lte", wrap_(laila.New_LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL_)},
	{"nr5g", wrap_(laila.New_LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL_)},
	{"nbiot", wrap_(laila.New_LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL_)},
	{"ltem", wrap_(laila.New_LAILA_IDENTIFIABLE_LTEM_COMM_PROTOCOL_)},
	{"matter", wrap_(laila.New_LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL_)},
}

// secret_of_ is ``proto.peer_secret_key`` for any transport (carrier or
// the WebSocket ``tcpip`` protocol).
func secret_of_(proto any) string {
	if c := laila.As_carrier_(proto); c != nil {
		return c.Peer_secret_key
	}
	return py.Str(must(py.Getattr(proto, "peer_secret_key")))
}

func run_case_(t *testing.T, name string, factory transport_factory_, codec string) {
	t.Helper()
	a := mk_(t, must(factory(py.Kw{"codec": codec})))
	b := mk_(t, must(factory(py.Kw{"codec": codec})))
	defer stopAll(a, b)

	pb := proto_(b)
	if err := comm(b).Start(); err != nil {
		t.Fatal(err)
	}
	uri := uri_for_(t, name, b, pb)
	remote_id, err := comm(a).Add_peer(uri, secret_of_(pb))
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	assertEq(t, remote_id, b.Global_id())

	b.Private_set("echo", func(m string) string { return "echo:" + m })
	remote_b := must(comm(a).Peers.Getitem(b.Global_id())).(*laila.PeerProxy)
	assertEq(t, must(remote_b.Attr("echo").Invoke("hi")), "echo:hi")

	// bidirectional: B can call A too
	a.Private_set("ping", func() string { return "pong:" + name })
	remote_a := must(comm(b).Peers.Getitem(a.Global_id())).(*laila.PeerProxy)
	assertEq(t, must(remote_a.Attr("ping").Invoke()), "pong:"+name)
}

func TestTier1Transports_each_transport_carries_rpc_json(t *testing.T) {
	for _, tr := range transports_ {
		t.Run(tr.name+"/json", func(t *testing.T) { run_case_(t, tr.name, tr.factory, "json") })
	}
}

func TestTier1Transports_each_transport_carries_rpc_msgpack(t *testing.T) {
	for _, tr := range transports_ {
		t.Run(tr.name+"/msgpack", func(t *testing.T) { run_case_(t, tr.name, tr.factory, "msgpack") })
	}
}

func TestTier1Transports_wrong_secret_rejected(t *testing.T) {
	for _, tr := range transports_ {
		t.Run(tr.name, func(t *testing.T) {
			a := mk_(t, must(tr.factory()))
			b := mk_(t, must(tr.factory()))
			defer stopAll(a, b)
			pb := proto_(b)
			if err := comm(b).Start(); err != nil {
				t.Fatal(err)
			}
			uri := uri_for_(t, tr.name, b, pb)
			_, err := comm(a).Add_peer(uri, "wrong-secret")
			assertRaises(t, py.ConnectionError, err)
		})
	}
}

func TestTier1Transports_disconnected_peer_send_raises(t *testing.T) {
	a := mk_(t, must(laila.New_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_()))
	defer stopAll(a)
	_, err := comm(a).Send_rpc_("nobody", []string{"x"}, nil, nil, nil)
	assertRaises(t, py.ConnectionError, err)
}

func TestTLSTransport_tls_round_trip(t *testing.T) {
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skip("openssl required to generate a test cert")
	}
	dir := t.TempDir()
	cert := filepath.Join(dir, "cert.pem")
	key := filepath.Join(dir, "key.pem")
	cmd := exec.Command("openssl", "req", "-x509", "-newkey", "rsa:2048", "-keyout", key, "-out", cert,
		"-days", "1", "-nodes", "-subj", "/CN=localhost")
	cmd.Stdout, cmd.Stderr = nil, os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}

	a := mk_(t, must(laila.New_LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL_())) // client-only (no cert)
	b := mk_(t, must(laila.New_LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL_(py.Kw{"certfile": cert, "keyfile": key})))
	defer stopAll(a, b)
	pb := proto_(b).(*laila.LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL_)
	if err := comm(b).Start(); err != nil {
		t.Fatal(err)
	}
	remote_id, err := comm(a).Add_peer(fmt.Sprintf("tls://127.0.0.1:%d", pb.Bound_port()), pb.Peer_secret_key)
	if err != nil {
		t.Fatal(err)
	}
	assertEq(t, remote_id, b.Global_id())
	b.Private_set("echo", func(m string) string { return "tls:" + m })
	remote_b := must(comm(a).Peers.Getitem(b.Global_id())).(*laila.PeerProxy)
	assertEq(t, must(remote_b.Attr("echo").Invoke("x")), "tls:x")
}
