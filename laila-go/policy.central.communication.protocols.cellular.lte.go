package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/cellular/lte.py -- LTE (4G) transport.
//
//   - ``protocol_name`` ``"lte"`` (aliases: lte,4g)
//   - URI scheme ``lte://host:port``

// LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL_ is the LTE (4G) transport.
type LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL_ struct {
	LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL_
}

// LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "lte"``).
var LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL_)(nil),
	LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL_class,
).
	Set_classvar("protocol_name", "lte").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("lte", "4g")).
	Set_classvar("matches_token", Token_aliases_classmethod_("lte", "4g")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("lte://"))

// New_LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
