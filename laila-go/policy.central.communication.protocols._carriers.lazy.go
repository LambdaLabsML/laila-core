package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/_carriers/lazy.py -- lazy
// datagram carrier for driver/hardware-backed packet radios.
//
// Many packet transports (Zigbee, Thread, Z-Wave, ANT, ESP-NOW, ...)
// ride on the DatagramRPCProtocol_ carrier but need a third-party driver
// and real radio hardware. LazyDatagramRPCProtocol_ standardises their
// connection hook: it imports the declared driver(s) via
// Require_drivers_ (raising a clear ``laila-core[<extra>]`` error when
// missing) and then defers to Setup_endpoint_, which a concrete
// transport overrides with the real radio bring-up.
//
// For transports with no off-the-shelf driver (the device is firmware,
// e.g. ESP-NOW), ``_DRIVER_MODULES`` is empty and Setup_endpoint_ raises
// a clear, actionable runtime error describing the hardware requirement
// -- never a silent stub.

// Lazy_hooks_ is the radio bring-up hook a concrete lazy transport
// overrides.
type Lazy_hooks_ interface {
	Setup_endpoint_(drivers map[string]any) (any, any, error)
}

// LazyDatagramRPCProtocol_ is the datagram carrier that lazy-loads a
// radio driver on start.
type LazyDatagramRPCProtocol_ struct {
	DatagramRPCProtocol_
}

// LazyDatagramRPCProtocol_class is the class descriptor
// (``_DRIVER_MODULES = ()``, ``_DRIVER_EXTRA = ""``).
var LazyDatagramRPCProtocol_class = py.NewClass(
	"_LazyDatagramRPCProtocol", (*LazyDatagramRPCProtocol_)(nil),
	DatagramRPCProtocol_class,
).
	Set_classvar("_DRIVER_MODULES", []string{}).
	Set_classvar("_DRIVER_EXTRA", "")

// Lazy_base_ exposes the embedded lazy carrier to code holding a
// subclass.
func (l *LazyDatagramRPCProtocol_) Lazy_base_() *LazyDatagramRPCProtocol_ { return l }

// Driver_modules_ is ``type(self)._DRIVER_MODULES``: import names
// required before the radio can be brought up.
func (l *LazyDatagramRPCProtocol_) Driver_modules_() []string {
	if v, ok := l.Class_().Classvar("_DRIVER_MODULES"); ok {
		switch m := v.(type) {
		case []string:
			return m
		default:
			out := []string{}
			for _, x := range py.Iter(v) {
				out = append(out, py.Str(x))
			}
			return out
		}
	}
	return nil
}

// Driver_extra_ is ``type(self)._DRIVER_EXTRA``: optional-extra name for
// the install hint.
func (l *LazyDatagramRPCProtocol_) Driver_extra_() string {
	if v, ok := l.Class_().Classvar("_DRIVER_EXTRA"); ok {
		return py.Str(v)
	}
	return ""
}

// Create_datagram_endpoint_ imports the declared drivers then defers to
// Setup_endpoint_.
func (l *LazyDatagramRPCProtocol_) Create_datagram_endpoint_() (any, any, error) {
	mods, err := l.Require_drivers_(l.Driver_modules_(), l.Driver_extra_())
	if err != nil {
		return nil, nil, err
	}
	if h, ok := l.Self().(Lazy_hooks_); ok {
		return h.Setup_endpoint_(mods)
	}
	return l.Setup_endpoint_(mods)
}

// Setup_endpoint_ brings up the real radio endpoint.
//
// Overridden by transports with a concrete driver integration. The
// default makes the hardware requirement explicit for transports whose
// device is firmware with no host-side driver.
func (l *LazyDatagramRPCProtocol_) Setup_endpoint_(drivers map[string]any) (any, any, error) {
	return nil, nil, py.RuntimeError.Newf(
		"The %s transport requires dedicated radio hardware/firmware and an out-of-band interface configuration; no host-side driver endpoint is available to bring up automatically.",
		py.Repr(l.Protocol_name()))
}

// Resolve_peer_addr_: by default the URI authority is the radio node
// address.
func (l *LazyDatagramRPCProtocol_) Resolve_peer_addr_(uri string) (any, error) {
	return Uri_authority(uri), nil
}
