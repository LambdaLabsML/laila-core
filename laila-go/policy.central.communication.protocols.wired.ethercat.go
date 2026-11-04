package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/wired/ethercat.py -- EtherCAT transport via pysoem on a dedicated NIC.
//
//   - ``protocol_name`` ``"ethercat"`` (aliases: ethercat)
//   - URI scheme(s): ethercat://
//
// Built on the lazy datagram carrier (LazyDatagramRPCProtocol_): the
// driver is resolved on start and a missing driver raises a clear
// capability error.

// LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL_ is the EtherCAT transport via pysoem on a dedicated NIC.
type LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL_ struct {
	LazyDatagramRPCProtocol_
}

// LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "ethercat"``).
var LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL_)(nil),
	LazyDatagramRPCProtocol_class,
).
	Set_classvar("protocol_name", "ethercat").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("ethercat")).
	Set_classvar("matches_token", Token_aliases_classmethod_("ethercat")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("ethercat://")).
	Set_classvar("_DRIVER_MODULES", []string{"pysoem"}).
	Set_classvar("_DRIVER_EXTRA", "ethercat")

// New_LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
