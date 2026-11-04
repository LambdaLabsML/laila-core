package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/short_range/wifi_direct.py -- Wi-Fi Direct (P2P) transport (TCP data path over a P2P group). Group formation is an OS concern.
//
//   - ``protocol_name`` ``"wifi-direct"`` (aliases: wifi-direct,wifidirect,wfd,p2p)
//   - URI scheme ``wifidirect://host:port``

// LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL_ is the Wi-Fi Direct (P2P) transport (TCP data path over a P2P group). Group formation is an OS concern.
type LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL_ struct {
	LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_
}

// LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "wifi-direct"``).
var LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL_)(nil),
	LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_class,
).
	Set_classvar("protocol_name", "wifi-direct").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("wifi-direct", "wifidirect", "wfd", "p2p")).
	Set_classvar("matches_token", Token_aliases_classmethod_("wifi-direct", "wifidirect", "wfd", "p2p")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("wifidirect://"))

// New_LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
