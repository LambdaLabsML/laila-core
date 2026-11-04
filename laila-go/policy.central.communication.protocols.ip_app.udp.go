package laila

import (
	"fmt"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/ip_app/udp.py -- UDP
// communication transport.
//
// Connectionless datagram transport built on the DatagramRPCProtocol_
// carrier, which layers fragmentation, ack/retry and dedup on top of
// raw UDP so the lossy link still carries reliable JSON-RPC.
//
//   - ``protocol_name`` ``"udp"`` (aliases ``udp4`` / ``udp6``)
//   - URI scheme ``udp://host:port``

// LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_ is the UDP peer-to-peer
// transport with a reliability layer.
//
// Fields:
//
//   - Host (default ``"0.0.0.0"``) -- local bind address.
//   - Port (default ``0``) -- local UDP port (``0`` lets the OS choose;
//     see Bound_port).
type LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_ struct {
	DatagramRPCProtocol_

	Host string `py:"host" default:"0.0.0.0"`
	Port int    `py:"port" default:"0"`

	Bound_port_ *int `py:"_bound_port"`
}

// LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_class is the class descriptor
// (``protocol_name = "udp"``).
var LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_)(nil),
	DatagramRPCProtocol_class,
).
	Set_classvar("protocol_name", "udp").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("udp", "udp4", "udp6")).
	Set_classvar("matches_token", Token_aliases_classmethod_("udp", "udp4", "udp6")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("udp://"))

// New_LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_ is
// ``_LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Bound_port is the OS-assigned local port after Start.
func (p *LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_) Bound_port() int {
	if p.Bound_port_ != nil {
		return *p.Bound_port_
	}
	return p.Port
}

// Matches_token accepts ``"udp"`` plus aliases.
func (p *LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_) Matches_token(token string) bool {
	return Comm_protocol_matches_token_(p.Class_(), token)
}

// Can_handle_uri claims ``udp://`` URIs.
func (p *LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_) Can_handle_uri(uri string) bool {
	return Comm_protocol_can_handle_uri_(p.Class_(), uri)
}

// Create_datagram_endpoint_ binds the local UDP socket and records the
// bound port.
func (p *LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_) Create_datagram_endpoint_() (any, any, error) {
	transport, proto, err := py.Create_datagram_endpoint(
		func() py.DatagramProtocol { return New_Datagram_endpoint_(p.Feed_packet_) },
		py.Addr{Host: p.Host, Port: p.Port},
	)
	if err != nil {
		return nil, nil, err
	}
	port := transport.Get_extra_info("sockname").(py.Addr).Port
	p.Bound_port_ = &port
	return transport, proto, nil
}

// Resolve_peer_addr_ parses ``udp://host:port`` into ``(host, port)``.
func (p *LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_) Resolve_peer_addr_(uri string) (any, error) {
	host, port, err := Split_host_port(uri)
	if err != nil {
		return nil, err
	}
	return py.Addr{Host: host, Port: port}, nil
}

// Connect_udp is the convenience wrapper building the ``udp://`` URI for
// Connect.
func (p *LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_) Connect_udp(host string, port int, secret string) (string, error) {
	return p.Connect(fmt.Sprintf("udp://%s:%d", host, port), secret)
}
