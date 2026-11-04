package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/short_range/zwave.py -- Z-Wave transport via zwave-js-server-python.
//
//   - ``protocol_name`` ``"zwave"`` (aliases: zwave,z-wave)
//   - URI scheme(s): zwave://
//
// Built on the lazy datagram carrier (LazyDatagramRPCProtocol_): the
// driver is resolved on start and a missing driver raises a clear
// capability error.

// LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL_ is the Z-Wave transport via zwave-js-server-python.
type LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL_ struct {
	LazyDatagramRPCProtocol_
}

// LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "zwave"``).
var LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL_)(nil),
	LazyDatagramRPCProtocol_class,
).
	Set_classvar("protocol_name", "zwave").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("zwave", "z-wave")).
	Set_classvar("matches_token", Token_aliases_classmethod_("zwave", "z-wave")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("zwave://")).
	Set_classvar("_DRIVER_MODULES", []string{"zwave_js_server"}).
	Set_classvar("_DRIVER_EXTRA", "zwave")

// New_LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
