package laila

import (
	"io"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/wired/can.py -- CAN / CAN-FD
// communication transport.
//
// CAN is a multidrop bus with tiny frames (8 bytes classic, up to 64 for
// CAN-FD), so JSON-RPC messages must be segmented. This transport uses
// ISO-TP over SocketCAN to expose a reliable, segmented stream and
// carries RPC on top of the point-to-point stream carrier.
//
//   - ``protocol_name`` ``"can"`` (aliases ``canfd`` / ``can-fd``)
//   - URI scheme ``can://<channel>``
//
// The ISO-TP driver is resolved lazily from the comm-driver registry
// under the name ``"isotp"``; a missing driver raises a clear,
// actionable error. End-to-end verification uses a SocketCAN virtual
// interface (``vcan0``).

// INSTALL_HINT_can_ is the capability error for a missing CAN driver.
const INSTALL_HINT_can_ = "The CAN transport requires python-can and can-isotp. Install them with `pip install laila-core[can]` (or `pip install python-can can-isotp`), and bring up a CAN interface (e.g. `sudo modprobe vcan && sudo ip link add dev vcan0 type vcan && sudo ip link set up vcan0`)."

// Isotp_driver_ is the ``isotp`` module contract: open a bound ISO-TP
// socket on *channel* with the given arbitration ids and flow-control
// options, exposing it as a duplex byte stream.
type Isotp_driver_ interface {
	Socket(channel string, rxid int, txid int, fd bool, stmin int, bs int) (io.ReadWriteCloser, error)
}

// LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL_ is the CAN / CAN-FD transport
// (ISO-TP segmented stream over SocketCAN).
//
// Fields:
//
//   - Channel (default ``"vcan0"``) -- SocketCAN interface name.
//   - Bustype (default ``"socketcan"``) -- bus backend.
//   - Tx_id, Rx_id -- ISO-TP arbitration ids for this endpoint's
//     transmit / receive.
//   - Fd (default ``false``) -- use CAN-FD (64-byte frames).
type LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL_ struct {
	P2PStreamRPCProtocol_

	Channel string `py:"channel" default:"vcan0"`
	Bustype string `py:"bustype" default:"socketcan"`
	Tx_id   int    `py:"tx_id" default:"291"`
	Rx_id   int    `py:"rx_id" default:"1110"`
	Fd      bool   `py:"fd" default:"false"`

	Bus_            any                `py:"_bus"`
	Isotp_sock_     io.ReadWriteCloser `py:"_isotp_sock"`
	Read_transport_ any                `py:"_read_transport"`
}

// LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL_class is the class descriptor
// (``protocol_name = "can"``).
var LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL_)(nil),
	P2PStreamRPCProtocol_class,
).
	Set_classvar("protocol_name", "can").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("can", "canfd", "can-fd")).
	Set_classvar("matches_token", Token_aliases_classmethod_("can", "canfd", "can-fd")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("can://"))

// New_LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL_ is
// ``_LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Matches_token accepts ``"can"`` plus CAN-FD aliases.
func (p *LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL_) Matches_token(token string) bool {
	return Comm_protocol_matches_token_(p.Class_(), token)
}

// Can_handle_uri claims ``can://`` URIs.
func (p *LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL_) Can_handle_uri(uri string) bool {
	return Comm_protocol_can_handle_uri_(p.Class_(), uri)
}

// Open_stream_ binds the ISO-TP socket and wraps it as a stream pair.
func (p *LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL_) Open_stream_() (*py.StreamReader, Stream_writer_, error) {
	comm_drivers_mu_.Lock()
	drv, ok := Comm_drivers_["isotp"].(Isotp_driver_)
	comm_drivers_mu_.Unlock()
	if !ok {
		return nil, nil, py.RuntimeError.New(INSTALL_HINT_can_).From(py.ImportError.New("No module named 'isotp'"))
	}

	// ISO-TP socket gives a reliable, segmented byte stream over CAN.
	sock, err := drv.Socket(p.Channel, p.Rx_id, p.Tx_id, p.Fd, 5, 10)
	if err != nil {
		return nil, nil, err
	}
	p.Isotp_sock_ = sock
	// Keep the read transport so Close_stream_ can release the socket
	// before the ISO-TP socket closes it.
	reader, writer := py.Stream_pair(sock, nop_close_writer_{sock}, nil)
	p.Read_transport_ = reader
	return reader, writer, nil
}

// nop_close_writer_ is the ``os.fdopen(os.dup(fd), "wb")`` write side of
// a shared fd: closing it leaves the underlying socket open.
type nop_close_writer_ struct{ w io.Writer }

func (w nop_close_writer_) Write(p []byte) (int, error) { return w.w.Write(p) }
func (w nop_close_writer_) Close() error                { return nil }

// Close_stream_ releases the read side, closes link + writer, then the
// ISO-TP socket.
func (p *LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL_) Close_stream_() error {
	p.Read_transport_ = nil
	if err := p.P2PStreamRPCProtocol_.Close_stream_(); err != nil {
		return err
	}
	if p.Isotp_sock_ != nil {
		_ = p.Isotp_sock_.Close()
		p.Isotp_sock_ = nil
	}
	return nil
}
