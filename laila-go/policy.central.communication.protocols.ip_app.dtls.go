package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/ip_app/dtls.py -- DTLS-secured datagram transport. Requires a DTLS-capable stack (OpenSSL DTLS bindings) the stdlib does not expose, so start raises a clear requirement error.
//
//   - ``protocol_name`` ``"dtls"`` (aliases: dtls)
//   - URI scheme(s): dtls://
//
// Built on the lazy datagram carrier (LazyDatagramRPCProtocol_): the
// driver is resolved on start and a missing driver raises a clear
// capability error.

// LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL_ is the DTLS-secured datagram transport. Requires a DTLS-capable stack (OpenSSL DTLS bindings) the stdlib does not expose, so start raises a clear requirement error.
type LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL_ struct {
	LazyDatagramRPCProtocol_
}

// LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "dtls"``).
var LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL_)(nil),
	LazyDatagramRPCProtocol_class,
).
	Set_classvar("protocol_name", "dtls").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("dtls")).
	Set_classvar("matches_token", Token_aliases_classmethod_("dtls")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("dtls://")).
	Set_classvar("_DRIVER_MODULES", []string{}).
	Set_classvar("_DRIVER_EXTRA", "")

// New_LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
