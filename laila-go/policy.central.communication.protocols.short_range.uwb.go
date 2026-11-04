package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/short_range/uwb.py -- UWB transport (device firmware; no host-side driver).
//
//   - ``protocol_name`` ``"uwb"`` (aliases: uwb)
//   - URI scheme(s): uwb://
//
// Built on the lazy datagram carrier (LazyDatagramRPCProtocol_): the
// driver is resolved on start and a missing driver raises a clear
// capability error.

// LAILA_IDENTIFIABLE_UWB_COMM_PROTOCOL_ is the UWB transport (device firmware; no host-side driver).
type LAILA_IDENTIFIABLE_UWB_COMM_PROTOCOL_ struct {
	LazyDatagramRPCProtocol_
}

// LAILA_IDENTIFIABLE_UWB_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "uwb"``).
var LAILA_IDENTIFIABLE_UWB_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_UWB_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_UWB_COMM_PROTOCOL_)(nil),
	LazyDatagramRPCProtocol_class,
).
	Set_classvar("protocol_name", "uwb").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("uwb")).
	Set_classvar("matches_token", Token_aliases_classmethod_("uwb")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("uwb://")).
	Set_classvar("_DRIVER_MODULES", []string{}).
	Set_classvar("_DRIVER_EXTRA", "")

// New_LAILA_IDENTIFIABLE_UWB_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_UWB_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_UWB_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_UWB_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_UWB_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_UWB_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
