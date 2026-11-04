package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/short_range/sixlowpan.py -- 6LoWPAN transport: IPv6 over IEEE 802.15.4, UDP data path once the lowpan/wpan link is up (an OS concern).
//
//   - ``protocol_name`` ``"6lowpan"`` (aliases: 6lowpan,sixlowpan)
//   - URI scheme(s): sixlowpan://

// LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL_ is the 6LoWPAN transport: IPv6 over IEEE 802.15.4, UDP data path once the lowpan/wpan link is up (an OS concern).
type LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL_ struct {
	LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_
}

// LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "6lowpan"``).
var LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL_)(nil),
	LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_class,
).
	Set_classvar("protocol_name", "6lowpan").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("6lowpan", "sixlowpan")).
	Set_classvar("matches_token", Token_aliases_classmethod_("6lowpan", "sixlowpan")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("sixlowpan://"))

// New_LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
