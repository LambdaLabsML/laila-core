package laila

import (
	"encoding/base64"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/wired/enip.py -- EtherNet/IP
// (CIP) communication transport.
//
// Carries JSON-RPC over an EtherNet/IP tag mailbox using the
// RegisterRPCProtocol_ carrier and a ``pycomm3``-like driver. The local
// policy reads/writes string tags on the peer PLC that serve as the
// inbox/outbox.
//
//   - ``protocol_name`` ``"ethernet-ip"`` (aliases ``enip`` /
//     ``ethernetip``)
//   - URI scheme ``enip://host``
//
// The driver is resolved lazily (registry name ``"pycomm3"``); a missing
// driver raises a clear error.

// Cip_driver_ is the ``pycomm3`` module contract (``CIPDriver(host)``).
type Cip_driver_ interface {
	CIPDriver(host string) Cip_plc_
}

// Cip_plc_ is a ``pycomm3.CIPDriver`` handle.
type Cip_plc_ interface {
	Open() error
	Close() error
	Write(tag string, value string) error
	Read(tag string) (string, error)
}

// LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL_ is the EtherNet/IP tag-mailbox
// transport.
type LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL_ struct {
	RegisterRPCProtocol_

	Host       string `py:"host" default:"127.0.0.1"`
	Inbox_tag  string `py:"inbox_tag" default:"LAILA_IN"`
	Outbox_tag string `py:"outbox_tag" default:"LAILA_OUT"`

	Plc_ Cip_plc_ `py:"_plc"`
}

// LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL_class is the class descriptor
// (``protocol_name = "ethernet-ip"``).
var LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL_)(nil),
	RegisterRPCProtocol_class,
).
	Set_classvar("protocol_name", "ethernet-ip").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("ethernet-ip", "enip", "ethernetip")).
	Set_classvar("matches_token", Token_aliases_classmethod_("ethernet-ip", "enip", "ethernetip")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("enip://"))

// New_LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL_ is
// ``_LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Matches_token accepts ``"ethernet-ip"`` plus aliases.
func (p *LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL_) Matches_token(token string) bool {
	return Comm_protocol_matches_token_(p.Class_(), token)
}

// Can_handle_uri claims ``enip://`` URIs.
func (p *LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL_) Can_handle_uri(uri string) bool {
	return Comm_protocol_can_handle_uri_(p.Class_(), uri)
}

// Open_bus_ opens the CIP session.
func (p *LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL_) Open_bus_() error {
	mods, err := p.Require_drivers_([]string{"pycomm3"}, "enip")
	if err != nil {
		return err
	}
	drv, ok := mods["pycomm3"].(Cip_driver_)
	if !ok {
		return py.TypeError.New("registered 'pycomm3' driver does not implement Cip_driver_")
	}
	plc := drv.CIPDriver(p.Host)
	if err := plc.Open(); err != nil {
		return err
	}
	p.Plc_ = plc
	return nil
}

// Close_bus_ closes the CIP session.
func (p *LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL_) Close_bus_() error {
	if p.Plc_ != nil {
		_ = p.Plc_.Close()
		p.Plc_ = nil
	}
	return nil
}

// Deliver_ stores the frame as a base64 string tag (PLC string tags are
// ASCII).
func (p *LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL_) Deliver_(data []byte) error {
	return p.Plc_.Write(p.Outbox_tag, base64.StdEncoding.EncodeToString(data))
}

// Poll_inbound_ reads and clears the inbox tag, or nil.
func (p *LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL_) Poll_inbound_() ([]byte, error) {
	value, err := p.Plc_.Read(p.Inbox_tag)
	if err != nil {
		return nil, err
	}
	if value == "" {
		return nil, nil
	}
	if err := p.Plc_.Write(p.Inbox_tag, ""); err != nil {
		return nil, err
	}
	out, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return nil, py.ValueError.New(err.Error()).From(err)
	}
	return out, nil
}
