package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/wired/modbus_rtu.py --
// Modbus-RTU (over RS-485) communication transport.
//
// Same register-mailbox approach as Modbus-TCP but over an RS-485
// serial line via the ``pymodbus`` async serial client.
//
//   - ``protocol_name`` ``"modbus-rtu"`` (aliases ``modbusrtu``)
//   - URI scheme ``modbusrtu:///dev/ttyUSB0``
//
// The driver is resolved lazily; a missing driver raises a clear error.

// LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL_ is the Modbus-RTU
// register-mailbox transport over RS-485.
type LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL_ struct {
	LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL_

	// Serial device, e.g. /dev/ttyUSB0.
	Serial_port string `py:"port" default:""`
	Baudrate    int    `py:"baudrate" default:"9600"`
}

// LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL_class is the class
// descriptor (``protocol_name = "modbus-rtu"``).
var LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL_)(nil),
	LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL_class,
).
	Set_classvar("protocol_name", "modbus-rtu").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("modbus-rtu", "modbusrtu")).
	Set_classvar("matches_token", Token_aliases_classmethod_("modbus-rtu", "modbusrtu")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("modbusrtu://"))

// New_LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL_ is
// ``_LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Open_bus_ connects the Modbus serial client.
func (p *LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL_) Open_bus_() error {
	drv, err := p.modbus_driver_()
	if err != nil {
		return err
	}
	if p.Serial_port == "" {
		return py.RuntimeError.Newf("%s requires a serial `port` (e.g. '/dev/ttyUSB0').", p.Class_().Name)
	}
	p.Client_ = drv.AsyncModbusSerialClient(p.Serial_port, p.Baudrate)
	return p.Client_.Connect()
}
