package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/cellular/nr5g.py -- 5G NR transport (URI schemes cannot begin with a digit, so 5g:// is spelled nr5g://).
//
//   - ``protocol_name`` ``"nr5g"`` (aliases: nr5g,5g,5g-nr,nr)
//   - URI scheme ``nr5g://host:port``

// LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL_ is the 5G NR transport (URI schemes cannot begin with a digit, so 5g:// is spelled nr5g://).
type LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL_ struct {
	LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL_
}

// LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "nr5g"``).
var LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL_)(nil),
	LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL_class,
).
	Set_classvar("protocol_name", "nr5g").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("nr5g", "5g", "5g-nr", "nr")).
	Set_classvar("matches_token", Token_aliases_classmethod_("nr5g", "5g", "5g-nr", "nr")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("nr5g://"))

// New_LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
