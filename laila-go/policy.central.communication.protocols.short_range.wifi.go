package laila

import (
	"fmt"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/short_range/wifi.py -- Wi-Fi station/AP transport (TCP data path over a wireless NIC). Association to an AP (or running as an AP) is an OS concern; Ssid / Mode are recorded for diagnostics.
//
//   - ``protocol_name`` ``"wifi"`` (aliases: wifi,wlan,sta,ap)
//   - URI scheme ``wifi://host:port``

// LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL_ is the Wi-Fi station/AP transport (TCP data path over a wireless NIC). Association to an AP (or running as an AP) is an OS concern; Ssid / Mode are recorded for diagnostics.
type LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL_ struct {
	LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_

	// Wi-Fi network name to associate with (station mode) or advertise (AP mode).
	Ssid *string `py:"ssid"`
	// ``"station"`` or ``"ap"``. Informational for OS-level bring-up.
	Mode string `py:"mode" default:"station"`
}

// LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "wifi"``).
var LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL_)(nil),
	LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_class,
).
	Set_classvar("protocol_name", "wifi").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("wifi", "wlan", "sta", "ap")).
	Set_classvar("matches_token", Token_aliases_classmethod_("wifi", "wlan", "sta", "ap")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("wifi://"))

// New_LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Connect_wifi is the convenience wrapper building the ``wifi://`` URI.
func (p *LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL_) Connect_wifi(host string, port int, secret string) (string, error) {
	return p.Connect(fmt.Sprintf("wifi://%s:%d", host, port), secret)
}
