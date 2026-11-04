package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/short_range/thread.py -- Thread transport via pyspinel and an NCP radio.
//
//   - ``protocol_name`` ``"thread"`` (aliases: thread)
//   - URI scheme(s): thread://
//
// Built on the lazy datagram carrier (LazyDatagramRPCProtocol_): the
// driver is resolved on start and a missing driver raises a clear
// capability error.

// LAILA_IDENTIFIABLE_THREAD_COMM_PROTOCOL_ is the Thread transport via pyspinel and an NCP radio.
type LAILA_IDENTIFIABLE_THREAD_COMM_PROTOCOL_ struct {
	LazyDatagramRPCProtocol_
}

// LAILA_IDENTIFIABLE_THREAD_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "thread"``).
var LAILA_IDENTIFIABLE_THREAD_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_THREAD_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_THREAD_COMM_PROTOCOL_)(nil),
	LazyDatagramRPCProtocol_class,
).
	Set_classvar("protocol_name", "thread").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("thread")).
	Set_classvar("matches_token", Token_aliases_classmethod_("thread")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("thread://")).
	Set_classvar("_DRIVER_MODULES", []string{"spinel"}).
	Set_classvar("_DRIVER_EXTRA", "thread")

// New_LAILA_IDENTIFIABLE_THREAD_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_THREAD_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_THREAD_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_THREAD_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_THREAD_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_THREAD_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
