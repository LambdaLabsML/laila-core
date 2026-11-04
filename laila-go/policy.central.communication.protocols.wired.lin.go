package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/wired/lin.py -- LIN bus transport (serial, UART-based).
//
//   - ``protocol_name`` ``"lin"`` (aliases: lin)
//   - URI scheme(s): lin://

// LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL_ is the LIN bus transport (serial, UART-based).
type LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL_ struct {
	LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_
}

// LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "lin"``).
var LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL_)(nil),
	LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_class,
).
	Set_classvar("protocol_name", "lin").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("lin")).
	Set_classvar("matches_token", Token_aliases_classmethod_("lin")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("lin://"))

// New_LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
