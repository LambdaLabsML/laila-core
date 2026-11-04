package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/cellular/cellular.py -- Generic cellular WWAN transport (TCP data path over a modem). Bringing the data session up is a modem/OS concern; Apn / Modem_device are recorded for diagnostics. Specific generations (2G/4G/5G) subclass this with their own tokens.
//
//   - ``protocol_name`` ``"cellular"`` (aliases: cellular,wwan,modem)
//   - URI scheme ``cellular://host:port``

// LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL_ is the Generic cellular WWAN transport (TCP data path over a modem). Bringing the data session up is a modem/OS concern; Apn / Modem_device are recorded for diagnostics. Specific generations (2G/4G/5G) subclass this with their own tokens.
type LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL_ struct {
	LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_

	// Access Point Name for the data session (carrier-specific).
	Apn *string `py:"apn"`
	// Modem control device (e.g. ``"/dev/cdc-wdm0"``). Informational.
	Modem_device *string `py:"modem_device"`
}

// LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "cellular"``).
var LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL_)(nil),
	LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_class,
).
	Set_classvar("protocol_name", "cellular").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("cellular", "wwan", "modem")).
	Set_classvar("matches_token", Token_aliases_classmethod_("cellular", "wwan", "modem")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("cellular://"))

// New_LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
