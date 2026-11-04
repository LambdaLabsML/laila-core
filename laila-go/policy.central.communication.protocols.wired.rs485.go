package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/wired/rs485.py -- RS-485 half-duplex multidrop serial transport. At the byte level it is the same UART stream; enables RS-485 direction control (DE/RE toggling) when the driver supports it.
//
//   - ``protocol_name`` ``"rs485"`` (aliases: rs485,rs-485)
//   - URI scheme(s): rs485://

// LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL_ is the RS-485 half-duplex multidrop serial transport. At the byte level it is the same UART stream; enables RS-485 direction control (DE/RE toggling) when the driver supports it.
type LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL_ struct {
	LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_

	// Enable RS-485 direction control on the port.
	Rs485_mode bool `py:"rs485_mode" default:"true"`
}

// LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "rs485"``).
var LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL_)(nil),
	LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_class,
).
	Set_classvar("protocol_name", "rs485").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("rs485", "rs-485")).
	Set_classvar("matches_token", Token_aliases_classmethod_("rs485", "rs-485")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("rs485://"))

// New_LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

var rs485_log_ = py.Get_logger("laila.policy.central.communication.protocols.wired.rs485")

// Configure_serial_ enables RS-485 direction control
// (``ser.rs485_mode = RS485Settings()``) when requested.
func (p *LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL_) Configure_serial_(ser *py.Serial) {
	if !p.Rs485_mode {
		return
	}
	if ser == nil {
		rs485_log_.Debug("RS-485 direction control unavailable on %s", p.Port)
		return
	}
	ser.Rs485_mode = true
}
