package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/wired/rs232.py -- RS-232 serial transport. RS-232 is a serial-line electrical standard; at the byte level it is the same asynchronous UART stream, so this is a thin subclass of the UART transport with its own token/URI.
//
//   - ``protocol_name`` ``"rs232"`` (aliases: rs232,rs-232)
//   - URI scheme(s): rs232://

// LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL_ is the RS-232 serial transport. RS-232 is a serial-line electrical standard; at the byte level it is the same asynchronous UART stream, so this is a thin subclass of the UART transport with its own token/URI.
type LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL_ struct {
	LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_
}

// LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "rs232"``).
var LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL_)(nil),
	LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_class,
).
	Set_classvar("protocol_name", "rs232").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("rs232", "rs-232")).
	Set_classvar("matches_token", Token_aliases_classmethod_("rs232", "rs-232")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("rs232://"))

// New_LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
