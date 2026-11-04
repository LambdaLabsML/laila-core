package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/wired/usb.py -- USB CDC-ACM transport (serial over USB). The OS exposes CDC-ACM as a serial port (e.g. /dev/ttyACM0); bulk/HID modes are out of scope.
//
//   - ``protocol_name`` ``"usb"`` (aliases: usb,cdc-acm,cdcacm)
//   - URI scheme(s): usb://

// LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL_ is the USB CDC-ACM transport (serial over USB). The OS exposes CDC-ACM as a serial port (e.g. /dev/ttyACM0); bulk/HID modes are out of scope.
type LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL_ struct {
	LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_
}

// LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "usb"``).
var LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL_)(nil),
	LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_class,
).
	Set_classvar("protocol_name", "usb").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("usb", "cdc-acm", "cdcacm")).
	Set_classvar("matches_token", Token_aliases_classmethod_("usb", "cdc-acm", "cdcacm")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("usb://"))

// New_LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
