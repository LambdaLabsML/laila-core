package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/short_range/ant.py -- ANT / ANT+ transport via openant.
//
//   - ``protocol_name`` ``"ant"`` (aliases: ant,ant+)
//   - URI scheme(s): ant://
//
// Built on the lazy datagram carrier (LazyDatagramRPCProtocol_): the
// driver is resolved on start and a missing driver raises a clear
// capability error.

// LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL_ is the ANT / ANT+ transport via openant.
type LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL_ struct {
	LazyDatagramRPCProtocol_
}

// LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "ant"``).
var LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL_)(nil),
	LazyDatagramRPCProtocol_class,
).
	Set_classvar("protocol_name", "ant").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("ant", "ant+")).
	Set_classvar("matches_token", Token_aliases_classmethod_("ant", "ant+")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("ant://")).
	Set_classvar("_DRIVER_MODULES", []string{"openant"}).
	Set_classvar("_DRIVER_EXTRA", "ant")

// New_LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
