package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/cellular/gsm_gprs.py -- GSM/GPRS (2G) transport.
//
//   - ``protocol_name`` ``"gsm"`` (aliases: gsm,gprs,2g,edge)
//   - URI scheme ``gsm://host:port``

// LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL_ is the GSM/GPRS (2G) transport.
type LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL_ struct {
	LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL_
}

// LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "gsm"``).
var LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL_)(nil),
	LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL_class,
).
	Set_classvar("protocol_name", "gsm").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("gsm", "gprs", "2g", "edge")).
	Set_classvar("matches_token", Token_aliases_classmethod_("gsm", "gprs", "2g", "edge")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("gsm://"))

// New_LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
