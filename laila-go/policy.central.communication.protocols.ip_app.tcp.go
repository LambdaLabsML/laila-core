package laila

import (
	"fmt"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/ip_app/tcp.py -- raw TCP
// communication transport.
//
// A direct, framed TCP transport (no WebSocket/HTTP upgrade) built on
// the reliable StreamRPCProtocol_ carrier. Lighter than the
// WebSocket-based ``tcpip`` transport and the natural choice for
// embedded peers that just want a socket.
//
//   - ``protocol_name`` ``"tcp"`` (aliases ``tcp4`` / ``tcp6`` /
//     ``raw-tcp``)
//   - URI scheme ``tcp://host:port``

// LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_ is the raw-TCP peer-to-peer
// transport (framed JSON-RPC over a TCP socket).
//
// Fields:
//
//   - Host (default ``"0.0.0.0"``) -- bind address for the listener.
//   - Port (default ``0``) -- TCP port (``0`` lets the OS choose; see
//     Bound_port).
type LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_ struct {
	StreamRPCProtocol_

	Host string `py:"host" default:"0.0.0.0"`
	Port int    `py:"port" default:"0"`

	Bound_port_ *int `py:"_bound_port"`
}

// LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_class is the class descriptor
// (``protocol_name = "tcp"``).
var LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_)(nil),
	StreamRPCProtocol_class,
).
	Set_classvar("protocol_name", "tcp").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("tcp", "tcp4", "tcp6", "raw-tcp", "rawtcp")).
	Set_classvar("matches_token", Token_aliases_classmethod_("tcp", "tcp4", "tcp6", "raw-tcp", "rawtcp")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("tcp://"))

// New_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_ is
// ``_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Tcp_base_ exposes the embedded TCP transport to code holding a
// subclass.
func (p *LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_) Tcp_base_() *LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_ {
	return p
}

// Bound_port is the OS-assigned port after Start (falls back to Port).
func (p *LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_) Bound_port() int {
	if p.Bound_port_ != nil {
		return *p.Bound_port_
	}
	return p.Port
}

// Matches_token accepts ``"tcp"`` plus aliases.
func (p *LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_) Matches_token(token string) bool {
	return Comm_protocol_matches_token_(p.Class_(), token)
}

// Can_handle_uri claims ``tcp://`` URIs.
func (p *LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_) Can_handle_uri(uri string) bool {
	return Comm_protocol_can_handle_uri_(p.Class_(), uri)
}

// On_stream_ready_ disables Nagle (TCP_NODELAY) so small RPC frames go
// out immediately.
func (p *LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_) On_stream_ready_(writer *py.StreamWriter) {
	if sock, ok := writer.Get_extra_info("socket").(*py.Socket); ok && sock != nil {
		_ = sock.Setsockopt_nodelay(true)
	}
}

// Serve_ binds the TCP listener and records the bound port.
func (p *LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_) Serve_() (any, error) {
	server, err := py.Start_server(p.Handle_inbound_stream_, p.Host, p.Port)
	if err != nil {
		return nil, err
	}
	port := server.Sockets()[0].Getsockname().Port
	p.Bound_port_ = &port
	return server, nil
}

// Open_connection_ dials ``host:port`` from the ``tcp://`` URI.
func (p *LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_) Open_connection_(uri string) (*py.StreamReader, *py.StreamWriter, error) {
	host, port, err := Split_host_port(uri)
	if err != nil {
		return nil, nil, err
	}
	return py.Open_connection(host, port)
}

// Connect_tcp is the convenience wrapper building the ``tcp://`` URI for
// Connect.
func (p *LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_) Connect_tcp(host string, port int, secret string) (string, error) {
	return p.Connect(fmt.Sprintf("tcp://%s:%d", host, port), secret)
}
