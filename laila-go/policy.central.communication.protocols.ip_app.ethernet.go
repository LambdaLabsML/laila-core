package laila

import (
	"fmt"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/ip_app/ethernet.py -- Ethernet/PoE transport (TCP data path over a wired NIC). Physical link bring-up (cable/PoE, DHCP) is handled by the OS; Interface is recorded for diagnostics and future link-management.
//
//   - ``protocol_name`` ``"ethernet"`` (aliases: ethernet,eth,poe)
//   - URI scheme ``ethernet://host:port``

// LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL_ is the Ethernet/PoE transport (TCP data path over a wired NIC). Physical link bring-up (cable/PoE, DHCP) is handled by the OS; Interface is recorded for diagnostics and future link-management.
type LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL_ struct {
	LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_

	// Optional NIC to bind/diagnose (e.g. ``"eth0"``). Informational; the
	// socket still binds by ``host``.
	Interface *string `py:"interface"`
}

// LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "ethernet"``).
var LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL_)(nil),
	LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_class,
).
	Set_classvar("protocol_name", "ethernet").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("ethernet", "eth", "poe")).
	Set_classvar("matches_token", Token_aliases_classmethod_("ethernet", "eth", "poe")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("ethernet://"))

// New_LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Connect_ethernet is the convenience wrapper building the ``ethernet://`` URI.
func (p *LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL_) Connect_ethernet(host string, port int, secret string) (string, error) {
	return p.Connect(fmt.Sprintf("ethernet://%s:%d", host, port), secret)
}
