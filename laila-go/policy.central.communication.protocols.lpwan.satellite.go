package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/lpwan/satellite.py -- Satellite SBD transport (serial AT modem: Iridium SBD, Swarm). Payloads are small and costly; the caller keeps messages within the modem's limits.
//
//   - ``protocol_name`` ``"satellite"`` (aliases: satellite,iridium,swarm,sbd)
//   - URI scheme(s): satellite://

// LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL_ is the Satellite SBD transport (serial AT modem: Iridium SBD, Swarm). Payloads are small and costly; the caller keeps messages within the modem's limits.
type LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL_ struct {
	LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_
}

// LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "satellite"``).
var LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL_)(nil),
	LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_class,
).
	Set_classvar("protocol_name", "satellite").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("satellite", "iridium", "swarm", "sbd")).
	Set_classvar("matches_token", Token_aliases_classmethod_("satellite", "iridium", "swarm", "sbd")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("satellite://"))

// New_LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
