package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/short_range/zigbee.py -- Zigbee transport via zigpy.
//
//   - ``protocol_name`` ``"zigbee"`` (aliases: zigbee)
//   - URI scheme(s): zigbee://
//
// Built on the lazy datagram carrier (LazyDatagramRPCProtocol_): the
// driver is resolved on start and a missing driver raises a clear
// capability error.

// LAILA_IDENTIFIABLE_ZIGBEE_COMM_PROTOCOL_ is the Zigbee transport via zigpy.
type LAILA_IDENTIFIABLE_ZIGBEE_COMM_PROTOCOL_ struct {
	LazyDatagramRPCProtocol_
}

// LAILA_IDENTIFIABLE_ZIGBEE_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "zigbee"``).
var LAILA_IDENTIFIABLE_ZIGBEE_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_ZIGBEE_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_ZIGBEE_COMM_PROTOCOL_)(nil),
	LazyDatagramRPCProtocol_class,
).
	Set_classvar("protocol_name", "zigbee").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("zigbee")).
	Set_classvar("matches_token", Token_aliases_classmethod_("zigbee")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("zigbee://")).
	Set_classvar("_DRIVER_MODULES", []string{"zigpy"}).
	Set_classvar("_DRIVER_EXTRA", "zigbee")

// New_LAILA_IDENTIFIABLE_ZIGBEE_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_ZIGBEE_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_ZIGBEE_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_ZIGBEE_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_ZIGBEE_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_ZIGBEE_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
