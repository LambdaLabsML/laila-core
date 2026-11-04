package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/short_range/espnow.py -- ESP-NOW transport (device firmware; no host-side driver).
//
//   - ``protocol_name`` ``"esp-now"`` (aliases: esp-now,espnow)
//   - URI scheme(s): espnow://
//
// Built on the lazy datagram carrier (LazyDatagramRPCProtocol_): the
// driver is resolved on start and a missing driver raises a clear
// capability error.

// LAILA_IDENTIFIABLE_ESPNOW_COMM_PROTOCOL_ is the ESP-NOW transport (device firmware; no host-side driver).
type LAILA_IDENTIFIABLE_ESPNOW_COMM_PROTOCOL_ struct {
	LazyDatagramRPCProtocol_
}

// LAILA_IDENTIFIABLE_ESPNOW_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "esp-now"``).
var LAILA_IDENTIFIABLE_ESPNOW_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_ESPNOW_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_ESPNOW_COMM_PROTOCOL_)(nil),
	LazyDatagramRPCProtocol_class,
).
	Set_classvar("protocol_name", "esp-now").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("esp-now", "espnow")).
	Set_classvar("matches_token", Token_aliases_classmethod_("esp-now", "espnow")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("espnow://")).
	Set_classvar("_DRIVER_MODULES", []string{}).
	Set_classvar("_DRIVER_EXTRA", "")

// New_LAILA_IDENTIFIABLE_ESPNOW_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_ESPNOW_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_ESPNOW_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_ESPNOW_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_ESPNOW_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_ESPNOW_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
