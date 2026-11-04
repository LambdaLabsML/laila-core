package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/wired/i2c.py -- I2C
// communication transport.
//
// Carries JSON-RPC over an I2C byte mailbox using the
// RegisterRPCProtocol_ carrier and an ``smbus2``-like driver. The local
// policy is the I2C master; the peer device exposes an inbox/outbox
// register region. Frames are length-prefixed and chunked to the
// 32-byte SMBus block limit.
//
//   - ``protocol_name`` ``"i2c"``
//   - URI scheme ``i2c://<bus>/<addr>``
//
// The driver is resolved lazily (registry name ``"smbus2"``); a missing
// driver raises a clear error.

// Smbus_driver_ is the ``smbus2`` module contract (``SMBus(bus)``).
type Smbus_driver_ interface {
	SMBus(bus int) (Smbus_, error)
}

// Smbus_ is an open ``smbus2.SMBus`` handle.
type Smbus_ interface {
	Write_i2c_block_data(addr int, reg int, data []byte) error
	Read_i2c_block_data(addr int, reg int, n int) ([]byte, error)
	Close() error
}

// LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL_ is the I2C register-mailbox
// transport (master side).
type LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL_ struct {
	RegisterRPCProtocol_

	Bus_number int `py:"bus_number" default:"1"`
	Address    int `py:"address" default:"32"`
	Inbox_reg  int `py:"inbox_reg" default:"0"`
	Outbox_reg int `py:"outbox_reg" default:"64"`

	Bus_ Smbus_ `py:"_bus"`
}

// LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL_class is the class descriptor
// (``protocol_name = "i2c"``).
var LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL_)(nil),
	RegisterRPCProtocol_class,
).
	Set_classvar("protocol_name", "i2c").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("i2c")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("i2c://"))

// New_LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL_ is
// ``_LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Open_bus_ opens the SMBus device.
func (p *LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL_) Open_bus_() error {
	mods, err := p.Require_drivers_([]string{"smbus2"}, "i2c")
	if err != nil {
		return err
	}
	drv, ok := mods["smbus2"].(Smbus_driver_)
	if !ok {
		return py.TypeError.New("registered 'smbus2' driver does not implement Smbus_driver_")
	}
	bus, err := drv.SMBus(p.Bus_number)
	if err != nil {
		return err
	}
	p.Bus_ = bus
	return nil
}

// Close_bus_ closes the SMBus device.
func (p *LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL_) Close_bus_() error {
	if p.Bus_ != nil {
		_ = p.Bus_.Close()
		p.Bus_ = nil
	}
	return nil
}

// Deliver_ writes one length-prefixed frame to the peer's outbox region,
// chunked to the 32-byte SMBus block limit.
func (p *LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL_) Deliver_(data []byte) error {
	payload := append([]byte{byte(len(data) >> 8), byte(len(data))}, data...)
	for offset := 0; offset < len(payload); offset += 30 {
		chunk := payload[offset:min(offset+30, len(payload))]
		if err := p.Bus_.Write_i2c_block_data(p.Address, p.Outbox_reg, chunk); err != nil {
			return err
		}
	}
	return nil
}

// Poll_inbound_ reads the next inbound frame from our inbox region, or
// nil.
func (p *LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL_) Poll_inbound_() ([]byte, error) {
	header, err := p.Bus_.Read_i2c_block_data(p.Address, p.Inbox_reg, 2)
	if err != nil {
		return nil, err
	}
	length := int(header[0])<<8 | int(header[1])
	if length == 0 {
		return nil, nil
	}
	out := []byte{}
	remaining := length
	for remaining > 0 {
		n := min(30, remaining)
		chunk, err := p.Bus_.Read_i2c_block_data(p.Address, p.Inbox_reg+2, n)
		if err != nil {
			return nil, err
		}
		out = append(out, chunk...)
		remaining -= n
	}
	// acknowledge by zeroing the length register
	if err := p.Bus_.Write_i2c_block_data(p.Address, p.Inbox_reg, []byte{0, 0}); err != nil {
		return nil, err
	}
	return out[:length], nil
}
