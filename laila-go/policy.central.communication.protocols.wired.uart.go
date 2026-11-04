package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/wired/uart.py -- UART / serial
// communication transport.
//
// Point-to-point framed JSON-RPC over an asynchronous serial line, built
// on the P2PStreamRPCProtocol_ carrier. The serial device is opened in
// raw 8N1 mode (py.Serial_open, the pyserial equivalent) and wrapped as
// an asyncio stream pair (py.Stream_pair, the ``connect_read_pipe`` /
// ``connect_write_pipe`` trick) so the blocking serial device
// participates as an asyncio stream.
//
//   - ``protocol_name`` ``"uart"`` (aliases ``serial``)
//   - URI scheme ``serial:///dev/ttyUSB0`` (informational; the port
//     comes from the ``port`` field)
//
// A missing device raises a clear, actionable error.

// INSTALL_HINT_serial_ is the pyserial install hint (kept for message
// parity; the Go port needs no driver).
const INSTALL_HINT_serial_ = "The serial transport requires pyserial. Install it with `pip install laila-core[serial]` (or `pip install pyserial`)."

// Uart_hooks_ is the serial configuration hook a subclass overrides
// (RS-485 direction control, etc.).
type Uart_hooks_ interface {
	Configure_serial_(ser *py.Serial)
}

// LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_ is the serial-line transport
// (UART).
//
// Fields:
//
//   - Port -- serial device path (e.g. ``"/dev/ttyUSB0"``, ``"COM3"``).
//   - Baudrate (default ``115200``) -- line speed.
type LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_ struct {
	P2PStreamRPCProtocol_

	Port     string `py:"port" default:""`
	Baudrate int    `py:"baudrate" default:"115200"`

	Serial_         *py.Serial `py:"_serial"`
	Read_transport_ any        `py:"_read_transport"`
}

// LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_class is the class descriptor
// (``protocol_name = "uart"``).
var LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_)(nil),
	P2PStreamRPCProtocol_class,
).
	Set_classvar("protocol_name", "uart").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("uart", "serial")).
	Set_classvar("matches_token", Token_aliases_classmethod_("uart", "serial")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("serial://", "uart://"))

// New_LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_ is
// ``_LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Uart_base_ exposes the embedded UART transport to code holding a
// subclass.
func (p *LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_) Uart_base_() *LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_ {
	return p
}

// Matches_token accepts ``"uart"`` / ``"serial"``.
func (p *LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_) Matches_token(token string) bool {
	return Comm_protocol_matches_token_(p.Class_(), token)
}

// Can_handle_uri claims ``serial://`` and ``uart://`` URIs.
func (p *LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_) Can_handle_uri(uri string) bool {
	return Comm_protocol_can_handle_uri_(p.Class_(), uri)
}

// Make_serial_ opens the configured port (``serial.Serial(port,
// baudrate, timeout=0)``).
func (p *LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_) Make_serial_() (*py.Serial, error) {
	if p.Port == "" {
		return nil, py.RuntimeError.Newf("%s requires a `port` (e.g. '/dev/ttyUSB0').", p.Class_().Name)
	}
	return py.Serial_open(p.Port, p.Baudrate)
}

// Open_stream_ opens the serial device and wraps it as a stream pair.
func (p *LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_) Open_stream_() (*py.StreamReader, Stream_writer_, error) {
	ser, err := p.Make_serial_()
	if err != nil {
		return nil, nil, err
	}
	if h, ok := p.Self().(Uart_hooks_); ok {
		h.Configure_serial_(ser)
	} else {
		p.Configure_serial_(ser)
	}
	p.Serial_ = ser
	// The read side is registered on the *original* serial fd; the
	// writer owns the same device and is closed first in Close_stream_
	// (serial_dup_writer_ never closes the fd itself).
	reader, writer := py.Stream_pair(ser, serial_dup_writer_{ser}, nil)
	p.Read_transport_ = reader
	return reader, writer, nil
}

// serial_dup_writer_ is the ``os.fdopen(os.dup(fd), "wb")`` write side:
// closing it must not close the serial fd (the carrier closes the device
// itself, last).
type serial_dup_writer_ struct{ ser *py.Serial }

func (w serial_dup_writer_) Write(p []byte) (int, error) { return w.ser.Write(p) }
func (w serial_dup_writer_) Close() error                { return nil }

// Configure_serial_ is the hook for subclasses (RS-485 direction
// control, etc.).
func (p *LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_) Configure_serial_(ser *py.Serial) {}

// Close_stream_ unregisters the read side, closes link + writer, then
// closes the serial device -- in that order, so nothing is ever woken
// on a dead fd.
func (p *LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_) Close_stream_() error {
	// 1. Unregister the serial fd from the selector (the reader goroutine
	//    ends when the device is closed in step 3).
	p.Read_transport_ = nil
	// 2. Close link + writer.
	if err := p.P2PStreamRPCProtocol_.Close_stream_(); err != nil {
		return err
	}
	// 3. Now nothing else holds the serial fd: safe to close it.
	if p.Serial_ != nil {
		_ = p.Serial_.Close()
		p.Serial_ = nil
	}
	return nil
}
