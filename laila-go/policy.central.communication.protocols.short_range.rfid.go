package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/short_range/rfid.py -- RFID reader transport (serial).
//
//   - ``protocol_name`` ``"rfid"`` (aliases: rfid)
//   - URI scheme(s): rfid://

// LAILA_IDENTIFIABLE_RFID_COMM_PROTOCOL_ is the RFID reader transport (serial).
type LAILA_IDENTIFIABLE_RFID_COMM_PROTOCOL_ struct {
	LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_
}

// LAILA_IDENTIFIABLE_RFID_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "rfid"``).
var LAILA_IDENTIFIABLE_RFID_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_RFID_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_RFID_COMM_PROTOCOL_)(nil),
	LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_class,
).
	Set_classvar("protocol_name", "rfid").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("rfid")).
	Set_classvar("matches_token", Token_aliases_classmethod_("rfid")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("rfid://"))

// New_LAILA_IDENTIFIABLE_RFID_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_RFID_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_RFID_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_RFID_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_RFID_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_RFID_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
