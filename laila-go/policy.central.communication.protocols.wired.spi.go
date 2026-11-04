package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/wired/spi.py -- SPI
// communication transport.
//
// Carries JSON-RPC over an SPI byte mailbox using the
// RegisterRPCProtocol_ carrier and a ``spidev``-like driver. The local
// policy is the SPI master; the peer exposes a length-prefixed
// inbox/outbox.
//
//   - ``protocol_name`` ``"spi"``
//   - URI scheme ``spi://<bus>.<device>``
//
// The driver is resolved lazily (registry name ``"spidev"``); a missing
// driver raises a clear error.

// Spidev_driver_ is the ``spidev`` module contract (``SpiDev()``).
type Spidev_driver_ interface {
	SpiDev() Spidev_
}

// Spidev_ is a ``spidev.SpiDev`` handle.
type Spidev_ interface {
	Open(bus int, device int) error
	Set_max_speed_hz(hz int)
	Xfer2(data []byte) ([]byte, error)
	Close() error
}

// LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL_ is the SPI register-mailbox
// transport (master side).
type LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL_ struct {
	RegisterRPCProtocol_

	Bus          int `py:"bus" default:"0"`
	Device       int `py:"device" default:"0"`
	Max_speed_hz int `py:"max_speed_hz" default:"1000000"`

	Spi_ Spidev_ `py:"_spi"`
}

// LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL_class is the class descriptor
// (``protocol_name = "spi"``).
var LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL_)(nil),
	RegisterRPCProtocol_class,
).
	Set_classvar("protocol_name", "spi").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("spi")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("spi://"))

// New_LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL_ is
// ``_LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Open_bus_ opens the SPI device.
func (p *LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL_) Open_bus_() error {
	mods, err := p.Require_drivers_([]string{"spidev"}, "spi")
	if err != nil {
		return err
	}
	drv, ok := mods["spidev"].(Spidev_driver_)
	if !ok {
		return py.TypeError.New("registered 'spidev' driver does not implement Spidev_driver_")
	}
	spi := drv.SpiDev()
	if err := spi.Open(p.Bus, p.Device); err != nil {
		return err
	}
	spi.Set_max_speed_hz(p.Max_speed_hz)
	p.Spi_ = spi
	return nil
}

// Close_bus_ closes the SPI device.
func (p *LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL_) Close_bus_() error {
	if p.Spi_ != nil {
		_ = p.Spi_.Close()
		p.Spi_ = nil
	}
	return nil
}

// Deliver_ transfers one length-prefixed frame.
func (p *LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL_) Deliver_(data []byte) error {
	frame := append([]byte{byte(len(data) >> 8), byte(len(data))}, data...)
	_, err := p.Spi_.Xfer2(frame)
	return err
}

// Poll_inbound_ clocks out the next inbound frame, or nil.
func (p *LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL_) Poll_inbound_() ([]byte, error) {
	header, err := p.Spi_.Xfer2([]byte{0, 0})
	if err != nil {
		return nil, err
	}
	length := int(header[0])<<8 | int(header[1])
	if length == 0 {
		return nil, nil
	}
	return p.Spi_.Xfer2(make([]byte, length))
}
