package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/ip_app/coap.py -- CoAP transport (datagram): fragmented JSON-RPC over CoAP (UDP) via aiocoap.
//
//   - ``protocol_name`` ``"coap"`` (aliases: coap,coaps)
//   - URI scheme(s): coap://,coaps://
//
// Built on the lazy datagram carrier (LazyDatagramRPCProtocol_): the
// driver is resolved on start and a missing driver raises a clear
// capability error.

// LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL_ is the CoAP transport (datagram): fragmented JSON-RPC over CoAP (UDP) via aiocoap.
type LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL_ struct {
	LazyDatagramRPCProtocol_
}

// LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "coap"``).
var LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL_)(nil),
	LazyDatagramRPCProtocol_class,
).
	Set_classvar("protocol_name", "coap").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("coap", "coaps")).
	Set_classvar("matches_token", Token_aliases_classmethod_("coap", "coaps")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("coap://", "coaps://")).
	Set_classvar("_DRIVER_MODULES", []string{"aiocoap"}).
	Set_classvar("_DRIVER_EXTRA", "coap")

// New_LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
