package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/lpwan/nbiot.py -- NB-IoT cellular LPWAN transport (IP data path).
//
//   - ``protocol_name`` ``"nbiot"`` (aliases: nbiot,nb-iot,nb1)
//   - URI scheme ``nbiot://host:port``

// LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL_ is the NB-IoT cellular LPWAN transport (IP data path).
type LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL_ struct {
	LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL_
}

// LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "nbiot"``).
var LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL_)(nil),
	LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL_class,
).
	Set_classvar("protocol_name", "nbiot").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("nbiot", "nb-iot", "nb1")).
	Set_classvar("matches_token", Token_aliases_classmethod_("nbiot", "nb-iot", "nb1")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("nbiot://"))

// New_LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
