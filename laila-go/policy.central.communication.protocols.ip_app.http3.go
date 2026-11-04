package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/ip_app/http3.py -- HTTP/3 (QUIC) transport (datagram): fragmented JSON-RPC over QUIC via aioquic.
//
//   - ``protocol_name`` ``"http3"`` (aliases: http3,h3,quic)
//   - URI scheme(s): http3://,quic://
//
// Built on the lazy datagram carrier (LazyDatagramRPCProtocol_): the
// driver is resolved on start and a missing driver raises a clear
// capability error.

// LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL_ is the HTTP/3 (QUIC) transport (datagram): fragmented JSON-RPC over QUIC via aioquic.
type LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL_ struct {
	LazyDatagramRPCProtocol_
}

// LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "http3"``).
var LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL_)(nil),
	LazyDatagramRPCProtocol_class,
).
	Set_classvar("protocol_name", "http3").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("http3", "h3", "quic")).
	Set_classvar("matches_token", Token_aliases_classmethod_("http3", "h3", "quic")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("http3://", "quic://")).
	Set_classvar("_DRIVER_MODULES", []string{"aioquic"}).
	Set_classvar("_DRIVER_EXTRA", "quic")

// New_LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
