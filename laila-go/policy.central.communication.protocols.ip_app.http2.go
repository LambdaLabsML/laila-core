package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/ip_app/http2.py -- HTTP/2 stream transport (JSON-RPC over a long-lived HTTP/2 stream via h2).
//
//   - ``protocol_name`` ``"http2"`` (aliases: http2,h2)
//   - URI scheme(s): http2://
//
// Built on the point-to-point stream carrier; the driver is resolved on
// start and a missing driver raises a clear capability error.

// LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL_ is the HTTP/2 stream transport (JSON-RPC over a long-lived HTTP/2 stream via h2).
type LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL_ struct {
	P2PStreamRPCProtocol_
}

// LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "http2"``).
var LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL_)(nil),
	P2PStreamRPCProtocol_class,
).
	Set_classvar("protocol_name", "http2").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("http2", "h2")).
	Set_classvar("matches_token", Token_aliases_classmethod_("http2", "h2")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("http2://"))

// New_LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Open_stream_ requires the driver, then reports the configuration the
// transport still needs before peering.
func (p *LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL_) Open_stream_() (*py.StreamReader, Stream_writer_, error) {
	if _, err := p.Require_drivers_([]string{"h2"}, "http"); err != nil {
		return nil, nil, err
	}
	return nil, nil, py.RuntimeError.New("HTTP/2 transport requires an established h2 connection/stream; configure the HTTP/2 endpoint before peering.")
}
