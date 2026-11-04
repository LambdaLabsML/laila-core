package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/short_range/matter.py -- Matter transport (IP data path over Thread/Wi-Fi); fabric commissioning is handled out of band.
//
//   - ``protocol_name`` ``"matter"`` (aliases: matter,chip)
//   - URI scheme ``matter://host:port``

// LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL_ is the Matter transport (IP data path over Thread/Wi-Fi); fabric commissioning is handled out of band.
type LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL_ struct {
	LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_
}

// LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "matter"``).
var LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL_)(nil),
	LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_class,
).
	Set_classvar("protocol_name", "matter").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("matter", "chip")).
	Set_classvar("matches_token", Token_aliases_classmethod_("matter", "chip")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("matter://"))

// New_LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
