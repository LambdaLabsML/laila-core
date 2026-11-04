package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/ip_app/modbus_tcp.py --
// Modbus-TCP communication transport.
//
// Carries JSON-RPC over a Modbus-TCP holding-register *mailbox* using
// the RegisterRPCProtocol_ carrier and a ``pymodbus``-like driver. The
// local policy acts as the Modbus master: it writes framed messages into
// the peer's inbox register block and polls its own outbox block for
// replies. The peer must expose a cooperating Modbus datastore as the
// mailbox.
//
//   - ``protocol_name`` ``"modbus-tcp"`` (aliases ``modbus`` /
//     ``modbustcp``)
//   - URI scheme ``modbustcp://host:port``
//
// The driver is resolved lazily (registry name ``"pymodbus"``); a
// missing driver raises a clear, actionable error.

// Modbus_driver_ is the ``pymodbus.client`` contract.
type Modbus_driver_ interface {
	AsyncModbusTcpClient(host string, port int) Modbus_client_
	AsyncModbusSerialClient(port string, baudrate int) Modbus_client_
}

// Modbus_client_ is a ``pymodbus`` async client handle.
type Modbus_client_ interface {
	Connect() error
	Close()
	Write_registers(address int, values []int, slave int) error
	Write_register(address int, value int, slave int) error
	// Read_holding_registers returns the registers, or an error-flagged
	// response (``rr.isError()``) as ``(nil, nil)``.
	Read_holding_registers(address int, count int, slave int) ([]int, error)
}

// LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL_ is the Modbus-TCP
// register-mailbox transport (master side).
type LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL_ struct {
	RegisterRPCProtocol_

	Host        string `py:"host" default:"127.0.0.1"`
	Port        int    `py:"port" default:"502"`
	Unit_id     int    `py:"unit_id" default:"1"`
	Inbox_base  int    `py:"inbox_base" default:"0"`
	Outbox_base int    `py:"outbox_base" default:"2000"`

	Client_ Modbus_client_ `py:"_client"`
}

// LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL_class is the class
// descriptor (``protocol_name = "modbus-tcp"``).
var LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL_)(nil),
	RegisterRPCProtocol_class,
).
	Set_classvar("protocol_name", "modbus-tcp").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("modbus-tcp", "modbus", "modbustcp")).
	Set_classvar("matches_token", Token_aliases_classmethod_("modbus-tcp", "modbus", "modbustcp")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("modbustcp://"))

// New_LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL_ is
// ``_LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Matches_token accepts ``"modbus-tcp"`` plus aliases.
func (p *LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL_) Matches_token(token string) bool {
	return Comm_protocol_matches_token_(p.Class_(), token)
}

// Can_handle_uri claims ``modbustcp://`` URIs.
func (p *LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL_) Can_handle_uri(uri string) bool {
	return Comm_protocol_can_handle_uri_(p.Class_(), uri)
}

// modbus_driver_ resolves the registered ``pymodbus`` driver.
func (p *LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL_) modbus_driver_() (Modbus_driver_, error) {
	mods, err := p.Require_drivers_([]string{"pymodbus"}, "modbus")
	if err != nil {
		return nil, err
	}
	drv, ok := mods["pymodbus"].(Modbus_driver_)
	if !ok {
		return nil, py.TypeError.New("registered 'pymodbus' driver does not implement Modbus_driver_")
	}
	return drv, nil
}

// Open_bus_ connects the Modbus-TCP client.
func (p *LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL_) Open_bus_() error {
	drv, err := p.modbus_driver_()
	if err != nil {
		return err
	}
	p.Client_ = drv.AsyncModbusTcpClient(p.Host, p.Port)
	return p.Client_.Connect()
}

// Close_bus_ closes the client.
func (p *LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL_) Close_bus_() error {
	if p.Client_ != nil {
		func() {
			defer func() { _ = recover() }()
			p.Client_.Close()
		}()
		p.Client_ = nil
	}
	return nil
}

// To_words_ packs *data* as ``[len, word, word, ...]`` (big-endian 16-bit).
func Modbus_to_words_(data []byte) []int {
	padded := data
	if len(data)%2 == 1 {
		padded = append(append([]byte(nil), data...), 0)
	}
	words := []int{len(data)}
	for i := 0; i < len(padded); i += 2 {
		words = append(words, int(padded[i])<<8|int(padded[i+1]))
	}
	return words
}

// From_words_ is the inverse of Modbus_to_words_.
func Modbus_from_words_(words []int) []byte {
	if len(words) == 0 {
		return nil
	}
	length := words[0]
	body := []byte{}
	for _, w := range words[1:] {
		body = append(body, byte(w>>8), byte(w))
	}
	if length > len(body) {
		length = len(body)
	}
	return body[:length]
}

// Deliver_ writes one frame into the peer's outbox block.
func (p *LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL_) Deliver_(data []byte) error {
	return p.Client_.Write_registers(p.Outbox_base, Modbus_to_words_(data), p.Unit_id)
}

// Poll_inbound_ reads the next frame from our inbox block, or nil.
func (p *LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL_) Poll_inbound_() ([]byte, error) {
	regs, err := p.Client_.Read_holding_registers(p.Inbox_base, 1, p.Unit_id)
	if err != nil {
		return nil, err
	}
	if len(regs) == 0 || regs[0] == 0 {
		return nil, nil
	}
	length := regs[0]
	words_needed := 1 + (length+1)/2
	full, err := p.Client_.Read_holding_registers(p.Inbox_base, words_needed, p.Unit_id)
	if err != nil {
		return nil, err
	}
	// clear the length cell to acknowledge consumption
	if err := p.Client_.Write_register(p.Inbox_base, 0, p.Unit_id); err != nil {
		return nil, err
	}
	return Modbus_from_words_(full), nil
}
