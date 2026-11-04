package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/wired/one_wire.py -- 1-Wire transport (master/slave sensor bus, Linux w1 sysfs; no host-side driver).
//
//   - ``protocol_name`` ``"1-wire"`` (aliases: 1-wire,onewire,w1)
//   - URI scheme(s): onewire://
//
// Built on the lazy datagram carrier (LazyDatagramRPCProtocol_): the
// driver is resolved on start and a missing driver raises a clear
// capability error.

// LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL_ is the 1-Wire transport (master/slave sensor bus, Linux w1 sysfs; no host-side driver).
type LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL_ struct {
	LazyDatagramRPCProtocol_
}

// LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "1-wire"``).
var LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL_)(nil),
	LazyDatagramRPCProtocol_class,
).
	Set_classvar("protocol_name", "1-wire").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("1-wire", "onewire", "w1")).
	Set_classvar("matches_token", Token_aliases_classmethod_("1-wire", "onewire", "w1")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("onewire://")).
	Set_classvar("_DRIVER_MODULES", []string{}).
	Set_classvar("_DRIVER_EXTRA", "")

// New_LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
