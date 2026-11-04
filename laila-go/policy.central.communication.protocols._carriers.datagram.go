package laila

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/_carriers/datagram.py --
// unreliable / segmented datagram RPC carrier.
//
// DatagramRPCProtocol_ turns a lossy, MTU-limited packet link (UDP,
// CoAP, LoRa, ESP-NOW, CAN, ...) into the same reliable JSON-RPC channel
// the stream carrier provides. On top of a raw ``sendto(addr, bytes)`` /
// inbound-packet pair it adds:
//
//   - **Fragmentation / reassembly** -- each encoded JSON-RPC message is
//     split into ``mtu``-sized fragments tagged with a 16-byte message
//     id, a fragment index and a fragment count, and reassembled on
//     arrival.
//   - **Per-fragment ack + retransmit** -- every received fragment is
//     acknowledged; unacked fragments are resent every ``ack_timeout``
//     up to ``max_retries`` times, so messages survive moderate loss.
//   - **Dedup** -- a recently-seen message-id set prevents a
//     retransmitted message from being processed twice after an ack is
//     lost.
//
// The reliable JSON-RPC layer (handshake, request/response correlation,
// off-thread dispatch) is inherited from CarrierRPCProtocol_.
//
// Concrete transports supply three coroutines/helpers:
// Create_datagram_endpoint_, Resolve_peer_addr_ and (optionally)
// Close_endpoint_.
//
// Stream lanes over datagrams: a stream message is **one datagram**
// carrying the standard lane header (``[0x01][lane][seq][flags][data]``,
// ``START|END`` always set), sent with ``sendto`` and nothing else: no
// ack, no retransmit, no dedup, no fragmentation -- that machinery stays
// RPC-only. Lanes are therefore *lossy and unordered by design*; the
// receiver's chunk-sequence check only makes a lost datagram visible, it
// cannot recover it. A message larger than ``mtu - 7`` raises
// ``ValueError`` on ``send()``. Inbound packets are discriminated on
// their first byte before the fragment parser (the ``LDG`` magic starts
// with ``0x4C``, so there is no clash).

var datagram_carrier_log_ = py.Get_logger("laila.policy.central.communication.protocols._carriers.datagram")

var MAGIC_ = []byte("LDG")

const (
	TYPE_DATA_ = 0
	TYPE_ACK_  = 1
	// HEADER_SIZE_ is ``struct.Struct(">3sBHH").size``: magic(3) + type(1)
	// + frag_index(2) + frag_count(2).
	HEADER_SIZE_ = 8
	MSG_ID_LEN_  = 16
	PREFIX_LEN_  = HEADER_SIZE_ + MSG_ID_LEN_
)

// Header_pack_ is ``_HEADER.pack(_MAGIC, ptype, frag_index, frag_count)``.
func Header_pack_(ptype int, frag_index int, frag_count int) []byte {
	out := make([]byte, HEADER_SIZE_)
	copy(out, MAGIC_)
	out[3] = byte(ptype)
	binary.BigEndian.PutUint16(out[4:], uint16(frag_index))
	binary.BigEndian.PutUint16(out[6:], uint16(frag_count))
	return out
}

// Header_unpack_ is ``_HEADER.unpack(data[:8])``.
func Header_unpack_(data []byte) (ptype int, frag_index int, frag_count int) {
	return int(data[3]), int(binary.BigEndian.Uint16(data[4:6])), int(binary.BigEndian.Uint16(data[6:8]))
}

// Datagram_endpoint_ is a minimal asyncio.DatagramProtocol forwarding to
// a callback.
type Datagram_endpoint_ struct {
	On_packet_ func(addr any, data []byte)
	Transport  *py.DatagramTransport
}

// New_Datagram_endpoint_ is ``_DatagramEndpoint(on_packet)``.
func New_Datagram_endpoint_(on_packet func(addr any, data []byte)) *Datagram_endpoint_ {
	return &Datagram_endpoint_{On_packet_: on_packet}
}

func (e *Datagram_endpoint_) Connection_made(transport *py.DatagramTransport) { e.Transport = transport }

func (e *Datagram_endpoint_) Datagram_received(data []byte, addr py.Addr) { e.On_packet_(addr, data) }

func (e *Datagram_endpoint_) Error_received(err error) {
	datagram_carrier_log_.Debug("Datagram error: %s", err)
}

// Datagram_sender_ is the ``sendto``-capable transport a datagram carrier
// drives (py.DatagramTransport or a driver-specific object).
type Datagram_sender_ interface {
	Sendto(data []byte, addr py.Addr) error
}

// unackedKey_ is ``(addr, msg_id, index)``.
type unackedKey_ struct {
	addr   string
	msg_id string
	index  int
}

// unackedSlot_ is ``{"packet", "tries", "request_id"}``.
type unackedSlot_ struct {
	addr       any
	packet     []byte
	tries      int
	request_id string
	has_rid    bool
}

// reasmKey_ is ``(addr, msg_id)``.
type reasmKey_ struct {
	addr   string
	msg_id string
}

// reasmEntry_ is ``{"count", "frags"}``.
type reasmEntry_ struct {
	count int
	frags map[int][]byte
}

// handshakeSlot_ is ``{"event", "msg", "addr"}``.
type handshakeSlot_ struct {
	event *py.Event
	msg   *py.Dict
	addr  any
}

// addr_key_ is the hashable identity of an address (tuples hash by
// value in Python; here we key on the canonical repr).
func addr_key_(addr any) string { return py.Repr(addr) }

// Datagram_hooks_ are the transport hooks a concrete datagram transport
// overrides.
type Datagram_hooks_ interface {
	Create_datagram_endpoint_() (any, any, error)
	Resolve_peer_addr_(uri string) (any, error)
	Close_endpoint_() error
	Sendto_(addr any, packet []byte) error
}

// DatagramRPCProtocol_ is the carrier for unreliable, MTU-limited packet
// links.
//
// Fields:
//
//   - Mtu (default ``1200``) -- maximum payload bytes per fragment (link
//     MTU minus headers).
//   - Ack_timeout (default ``0.5``) -- seconds between retransmits of
//     unacked fragments.
//   - Max_retries (default ``5``) -- how many times an unacked fragment
//     is resent before giving up.
type DatagramRPCProtocol_ struct {
	CarrierRPCProtocol_

	Mtu         int     `py:"mtu" default:"1200"`
	Ack_timeout float64 `py:"ack_timeout" default:"0.5"`
	Max_retries int     `py:"max_retries" default:"5"`

	Transport_         any                        `py:"_transport"`
	Endpoint_          any                        `py:"_endpoint"`
	Reasm_             map[reasmKey_]*reasmEntry_ `py:"_reasm"`
	Seen_              map[string]struct{}        `py:"_seen"`
	Seen_order_        []string                   `py:"_seen_order"`
	Unacked_           map[unackedKey_]*unackedSlot_
	unacked_order_     []unackedKey_
	Handshake_pending_ map[string]*handshakeSlot_ `py:"_handshake_pending"`
	Retransmit_task_   *py.Task                   `py:"_retransmit_task"`
}

// DatagramRPCProtocol_class is the class descriptor (``supports_channels
// = True``).
var DatagramRPCProtocol_class = py.NewClass(
	"_DatagramRPCProtocol", (*DatagramRPCProtocol_)(nil),
	CarrierRPCProtocol_class,
).
	Set_classvar("supports_channels", true)

// Datagram_base_ exposes the embedded datagram carrier to code holding a
// subclass.
func (d *DatagramRPCProtocol_) Datagram_base_() *DatagramRPCProtocol_ { return d }

// datagram_hooks is the most-derived transport implementation.
func (d *DatagramRPCProtocol_) datagram_hooks() Datagram_hooks_ {
	if h, ok := d.Self().(Datagram_hooks_); ok {
		return h
	}
	return d
}

func (d *DatagramRPCProtocol_) init_datagram_tables_() {
	d.init_tables_()
	if d.Reasm_ == nil {
		d.Reasm_ = map[reasmKey_]*reasmEntry_{}
	}
	if d.Unacked_ == nil {
		d.Unacked_ = map[unackedKey_]*unackedSlot_{}
	}
	if d.Handshake_pending_ == nil {
		d.Handshake_pending_ = map[string]*handshakeSlot_{}
	}
}

// ------------------------------------------------------------------
// Subclass hooks
// ------------------------------------------------------------------

// Create_datagram_endpoint_ creates and returns ``(transport, protocol)``
// for the local socket.
//
// Subclasses typically call
// ``py.Create_datagram_endpoint(func() py.DatagramProtocol { return
// New_Datagram_endpoint_(self.Feed_packet_) }, ...)``. Runs inside the
// carrier event loop.
func (d *DatagramRPCProtocol_) Create_datagram_endpoint_() (any, any, error) {
	return nil, nil, py.NotImplementedError.New()
}

// Resolve_peer_addr_ parses *uri* into the transport address used by
// ``sendto``.
func (d *DatagramRPCProtocol_) Resolve_peer_addr_(uri string) (any, error) {
	return nil, py.NotImplementedError.New()
}

// Close_endpoint_ closes the datagram endpoint. Default: close the
// transport.
func (d *DatagramRPCProtocol_) Close_endpoint_() error {
	if !py.IsNone(d.Transport_) {
		func() {
			defer func() { _ = recover() }()
			if c, ok := d.Transport_.(interface{ Close() }); ok {
				c.Close()
			}
		}()
		d.Transport_ = nil
	}
	return nil
}

// Sendto_ sends one raw datagram. Default uses the asyncio transport.
func (d *DatagramRPCProtocol_) Sendto_(addr any, packet []byte) error {
	if py.IsNone(d.Transport_) {
		return py.AttributeError.New("'NoneType' object has no attribute 'sendto'")
	}
	switch t := d.Transport_.(type) {
	case Datagram_sender_:
		a, ok := addr.(py.Addr)
		if !ok {
			return py.TypeError.Newf("datagram address must be (host, port), got %s", py.Repr(addr))
		}
		return t.Sendto(packet, a)
	case interface {
		Sendto_any(data []byte, addr any) error
	}:
		return t.Sendto_any(packet, addr)
	}
	return py.AttributeError.Newf("%s object has no attribute 'sendto'", py.Repr(fmt.Sprintf("%T", d.Transport_)))
}

// ------------------------------------------------------------------
// Lifecycle
// ------------------------------------------------------------------

// Start boots the event loop and binds the datagram endpoint
// (idempotent).
func (d *DatagramRPCProtocol_) Start() error {
	if d.Started_ {
		return nil
	}
	d.init_datagram_tables_()
	d.Seen_ = map[string]struct{}{}
	d.Ensure_executor_()
	if err := Start_loop_thread(d, d.Async_start_, max(d.Handshake_timeout, 10.0)); err != nil {
		d.Shutdown_executor_()
		return err
	}
	d.Started_ = true
	return nil
}

// Async_start_ binds the endpoint and starts the retransmit loop.
func (d *DatagramRPCProtocol_) Async_start_(ready *py.Event) error {
	transport, endpoint, err := d.datagram_hooks().Create_datagram_endpoint_()
	if err != nil {
		return err
	}
	d.Transport_, d.Endpoint_ = transport, endpoint
	task, err := py.Ensure_future(func() (any, error) { return nil, d.Retransmit_loop_() })
	if err != nil {
		return err
	}
	d.Retransmit_task_ = task
	ready.Set()
	return nil
}

// Stop tears down the endpoint and loop (idempotent).
//
// Order: goodbye to every peer (a datagram peer has no EOF to notice, so
// without it the remote only drops us on liveness timeout), unregister,
// stop the retransmit timer, close the endpoint, cancel and await
// remaining tasks, stop and close the loop.
func (d *DatagramRPCProtocol_) Stop() error {
	if !d.Started_ {
		return nil
	}

	shutdown := func() error {
		_ = d.Goodbye_all_()
		for _, peer_id := range d.Peer_ids_() {
			d.hooks().Unregister_peer_(peer_id)
		}
		d.Shutdown_lanes_()
		if d.Retransmit_task_ != nil {
			d.Retransmit_task_.Cancel()
			_, _ = d.Retransmit_task_.Await()
			d.Retransmit_task_ = nil
		}
		if err := d.datagram_hooks().Close_endpoint_(); err != nil {
			return err
		}
		return Cancel_pending_tasks()
	}

	Stop_loop_thread(d, shutdown)
	d.Shutdown_lanes_()
	d.Reasm_ = map[reasmKey_]*reasmEntry_{}
	d.Unacked_ = map[unackedKey_]*unackedSlot_{}
	d.unacked_order_ = nil
	d.Handshake_pending_ = map[string]*handshakeSlot_{}
	d.Fail_pending_("transport stopped", nil)
	d.Shutdown_executor_()
	d.Started_ = false
	return nil
}

// ------------------------------------------------------------------
// Reliable message layer
// ------------------------------------------------------------------

// Send_message_ fragments *payload* and queues every fragment for
// reliable delivery.
//
// *request_id* tags the fragments of an outbound ``rpc.call`` so a
// delivery give-up can fail that call's pending slot.
func (d *DatagramRPCProtocol_) Send_message_(addr any, payload []byte, request_id ...string) {
	msg_id := py.Uuid4().Bytes()
	mtu := max(1, d.Mtu)
	fragments := [][]byte{}
	for i := 0; i < len(payload); i += mtu {
		fragments = append(fragments, payload[i:min(i+mtu, len(payload))])
	}
	if len(fragments) == 0 {
		fragments = [][]byte{{}}
	}
	frag_count := len(fragments)
	akey := addr_key_(addr)
	for index, frag := range fragments {
		packet := append(append(Header_pack_(TYPE_DATA_, index, frag_count), msg_id...), frag...)
		key := unackedKey_{akey, string(msg_id), index}
		slot := &unackedSlot_{addr: addr, packet: packet}
		if len(request_id) > 0 {
			slot.request_id, slot.has_rid = request_id[0], true
		}
		if _, exists := d.Unacked_[key]; !exists {
			d.unacked_order_ = append(d.unacked_order_, key)
		}
		d.Unacked_[key] = slot
		_ = d.datagram_hooks().Sendto_(addr, packet)
	}
}

// unacked_items_ is ``list(self._unacked.items())`` in insertion order
// (compacting keys that have since been removed).
func (d *DatagramRPCProtocol_) unacked_items_() []unackedKey_ {
	live := d.unacked_order_[:0]
	for _, k := range d.unacked_order_ {
		if _, ok := d.Unacked_[k]; ok {
			live = append(live, k)
		}
	}
	d.unacked_order_ = live
	return append([]unackedKey_(nil), live...)
}

// Give_up_ abandons a message: drops its sibling fragments and fails its
// RPC slot.
//
// Without this the caller of a lost request would sit out the full
// ``rpc_timeout`` even though the carrier already knows the message will
// never arrive.
func (d *DatagramRPCProtocol_) Give_up_(key unackedKey_, slot *unackedSlot_, reason string) {
	for other := range d.Unacked_ {
		if other.addr == key.addr && other.msg_id == key.msg_id {
			delete(d.Unacked_, other)
		}
	}
	if slot.has_rid {
		d.Fail_request_(slot.request_id, py.ConnectionError.Newf(
			"datagram delivery to %s gave up (%s) after %d retransmits on %s.",
			py.Str(slot.addr), reason, d.Max_retries, d.Class_().Name))
	}
}

// Retransmit_loop_ resends unacked fragments every ``ack_timeout`` up to
// ``max_retries``.
//
// A fragment that exhausts its retries (or whose send fails) takes its
// whole message down with it -- see Give_up_.
func (d *DatagramRPCProtocol_) Retransmit_loop_() error {
	for {
		if err := py.Sleep(d.Ack_timeout); err != nil {
			if py.IsInstance(err, py.CancelledError) {
				return nil
			}
			return err
		}
		for _, key := range d.unacked_items_() {
			slot, ok := d.Unacked_[key]
			if !ok {
				continue // purged with a sibling that gave up
			}
			if slot.tries >= d.Max_retries {
				d.Give_up_(key, slot, "no ack")
				continue
			}
			slot.tries++
			if err := d.datagram_hooks().Sendto_(slot.addr, slot.packet); err != nil {
				d.Give_up_(key, slot, "send failed: "+py.Repr(err))
			}
		}
	}
}

// Peer_for_addr_ reverse-maps a transport address to the peer registered
// on it ("" / false when unknown).
func (d *DatagramRPCProtocol_) Peer_for_addr_(addr any) (string, bool) {
	for _, item := range d.Connections_.Items() {
		if py.Eq(item.Value, addr) {
			return py.Str(item.Key), true
		}
	}
	return "", false
}

// peer_for_addr_any_ is Peer_for_addr_ as ``str | None``.
func (d *DatagramRPCProtocol_) peer_for_addr_any_(addr any) any {
	if pid, ok := d.Peer_for_addr_(addr); ok {
		return pid
	}
	return nil
}

// Feed_packet_ is the inbound-packet entry point (called by the endpoint
// protocol).
//
// Stream-lane datagrams (first byte ``< 0x20``) are routed before the
// fragment parser; they are only accepted from registered peers.
func (d *DatagramRPCProtocol_) Feed_packet_(addr any, data []byte) {
	if Is_reserved_frame(data) {
		peer_id, ok := d.Peer_for_addr_(addr)
		if !ok {
			d.Unknown_lane_drops_++
			return
		}
		d.On_stream_frame_(peer_id, data)
		return
	}
	if len(data) < PREFIX_LEN_ || !bytes.Equal(data[:3], MAGIC_) {
		return
	}
	ptype, frag_index, frag_count := Header_unpack_(data[:HEADER_SIZE_])
	msg_id := string(data[HEADER_SIZE_:PREFIX_LEN_])
	payload := data[PREFIX_LEN_:]
	akey := addr_key_(addr)

	if ptype == TYPE_ACK_ {
		delete(d.Unacked_, unackedKey_{akey, msg_id, frag_index})
		return
	}

	// DATA: acknowledge this fragment, then try to reassemble.
	ack := append(Header_pack_(TYPE_ACK_, frag_index, frag_count), msg_id...)
	_ = d.datagram_hooks().Sendto_(addr, ack)

	if _, seen := d.Seen_[msg_id]; seen {
		return
	}

	key := reasmKey_{akey, msg_id}
	entry := d.Reasm_[key]
	if entry == nil {
		entry = &reasmEntry_{count: frag_count, frags: map[int][]byte{}}
		d.Reasm_[key] = entry
	}
	entry.frags[frag_index] = payload
	if len(entry.frags) < entry.count {
		return
	}

	delete(d.Reasm_, key)
	d.Mark_seen_(msg_id)
	full := []byte{}
	for i := 0; i < entry.count; i++ {
		full = append(full, entry.frags[i]...)
	}
	d.On_message_(addr, full)
}

// Mark_seen_ records *msg_id* as processed, bounding the dedup set.
func (d *DatagramRPCProtocol_) Mark_seen_(msg_id string) {
	d.Seen_[msg_id] = struct{}{}
	d.Seen_order_ = append(d.Seen_order_, msg_id)
	if len(d.Seen_order_) > 4096 {
		old := d.Seen_order_[0]
		d.Seen_order_ = d.Seen_order_[1:]
		delete(d.Seen_, old)
	}
}

// On_message_ decodes a fully-reassembled JSON-RPC message and dispatches
// it.
func (d *DatagramRPCProtocol_) On_message_(addr any, payload []byte) {
	msg, err := d.Decode_msg_(payload)
	if err != nil {
		return
	}

	if Is_request(msg) {
		method := msg.GetDefault("method", nil)
		if py.Eq(method, PEER_CONNECT_) {
			d.Handle_handshake_(addr, msg)
		} else if Is_goodbye_(msg) {
			// only honoured from the address the peer is registered on
			if peer_id, ok := d.Peer_for_addr_(addr); ok &&
				py.Eq(peer_id, dict_or_empty_(msg.GetDefault("params", nil)).GetDefault("from_id", nil)) {
				d.On_peer_goodbye_(peer_id)
			}
		} else {
			d.Dispatch_rpc_(addr, msg)
		}
	} else if Is_response(msg) {
		rid := py.Str(msg.GetDefault("id", nil))
		if slot, ok := d.Handshake_pending_[rid]; ok {
			if slot != nil {
				slot.msg = msg
				slot.addr = addr
				slot.event.Set()
			}
		} else {
			d.Complete_pending_(msg)
		}
	}
}

// Handle_handshake_ is the server side of ``peer.connect`` over
// datagrams.
func (d *DatagramRPCProtocol_) Handle_handshake_(addr any, msg *py.Dict) {
	params := dict_or_empty_(msg.GetDefault("params", nil))
	peer_id := params.GetDefault("from_id", nil)
	if !py.Eq(params.GetDefault("secret", nil), d.Peer_secret_key) {
		resp := Make_error(msg.GetDefault("id", nil), ERR_AUTH_FAILED, "Invalid peer secret key.", nil)
		if data, err := d.Encode_(resp); err == nil {
			d.Send_message_(addr, data)
		}
		return
	}
	resp := Make_result(msg.GetDefault("id", nil), py.DictOf("peer_id", d.Policy_id_(), "caps", d.hooks().Local_caps_()))
	pid := py.Str(peer_id)
	d.Set_peer_caps_(pid, params.GetDefault("caps", nil))
	d.hooks().Register_peer_(pid, addr)
	if data, err := d.Encode_(resp); err == nil {
		d.Send_message_(addr, data)
	}
}

// Dispatch_rpc_ runs an inbound ``rpc.call`` off the loop and replies to
// *addr*.
func (d *DatagramRPCProtocol_) Dispatch_rpc_(addr any, msg *py.Dict) {
	reply := func(resp *py.Dict) {
		data, err := d.Encode_(resp)
		if err != nil {
			return
		}
		_ = d.Loop_call_(func() { d.Send_message_(addr, data) })
	}
	d.Handle_request_frame_(msg, reply, d.peer_for_addr_any_(addr))
}

// Send_goodbye_ sends ``peer.disconnect`` to the peer's address (first
// try goes out at once).
func (d *DatagramRPCProtocol_) Send_goodbye_(peer_id string) error {
	addr := d.Connection_handle_(peer_id)
	if py.IsNone(addr) || py.IsNone(d.Transport_) {
		return nil
	}
	data, err := d.Encode_(d.Goodbye_message_())
	if err != nil {
		return err
	}
	d.Send_message_(addr, data)
	return nil
}

// ------------------------------------------------------------------
// Peering / RPC
// ------------------------------------------------------------------

// Connect handshakes with the remote endpoint named by *uri*.
func (d *DatagramRPCProtocol_) Connect(uri string, secret string) (string, error) {
	if err := d.Start(); err != nil {
		return "", err
	}
	addr_fut := py.Run_coroutine_threadsafe(func() (any, error) { return d.datagram_hooks().Resolve_peer_addr_(uri) }, d.Loop())
	addr, err := addr_fut.Result(10.0)
	if err != nil {
		return "", err
	}

	req := Make_request(
		PEER_CONNECT_,
		py.DictOf("from_id", d.Policy_id_(), "secret", secret, "caps", d.hooks().Local_caps_()),
		"",
	)
	rid := py.Str(req.GetDefault("id", nil))
	slot := &handshakeSlot_{event: py.NewEvent()}
	d.Handshake_pending_[rid] = slot
	data, err := d.Encode_(req)
	if err != nil {
		delete(d.Handshake_pending_, rid)
		return "", err
	}
	if err := d.Loop_call_(func() { d.Send_message_(addr, data) }); err != nil {
		delete(d.Handshake_pending_, rid)
		return "", err
	}

	completed := slot.event.Wait(d.Handshake_timeout)
	delete(d.Handshake_pending_, rid)
	if !completed {
		return "", py.ConnectionError.New("Peer handshake timed out.")
	}
	reply := slot.msg
	if e, found := reply.Get("error"); found {
		return "", py.ConnectionError.Newf("Peer rejected connection: %s", py.Str(error_message_(e)))
	}
	result := dict_or_empty_(reply.GetDefault("result", nil))
	peer_id := result.GetDefault("peer_id", nil)
	if py.IsNone(peer_id) {
		return "", py.ConnectionError.New("Peer response missing peer_id.")
	}
	pid := py.Str(peer_id)
	d.Set_peer_caps_(pid, result.GetDefault("caps", nil))
	d.hooks().Register_peer_(pid, addr)
	return pid, nil
}

// Send_once_ sends one ``rpc.call`` to *peer_id* and blocks for the
// response.
func (d *DatagramRPCProtocol_) Send_once_(peer_id string, path []string, args []any, kwargs py.Kw) (any, error) {
	addr := d.Connection_handle_(peer_id)
	if py.IsNone(addr) {
		return nil, py.ConnectionError.Newf("No connection to peer %s", peer_id)
	}
	request_id, slot := d.Register_pending_(peer_id)
	req := d.Make_rpc_request_(path, args, kwargs, request_id)
	data, err := d.Encode_(req)
	if err != nil {
		d.Pending_rpcs_.Pop(request_id)
		return nil, err
	}
	if err := d.Loop_call_(func() { d.Send_message_(addr, data, request_id) }); err != nil {
		d.Pending_rpcs_.Pop(request_id)
		return nil, err
	}
	return d.Await_pending_(request_id, slot)
}

// Send_oneway_ sends a control request (reliably) without waiting for
// its reply.
func (d *DatagramRPCProtocol_) Send_oneway_(peer_id string, path []string, kwargs py.Kw) error {
	addr := d.Connection_handle_(peer_id)
	if py.IsNone(addr) {
		return py.ConnectionError.Newf("No connection to peer %s", peer_id)
	}
	req := d.Make_rpc_request_(path, nil, kwargs, py.Uuid4().String())
	data, err := d.Encode_(req)
	if err != nil {
		return err
	}
	return d.Loop_call_(func() { d.Send_message_(addr, data) })
}

// ------------------------------------------------------------------
// Stream lanes (single datagram per message, best effort)
// ------------------------------------------------------------------

// Max_message_bytes_: a stream message must fit one datagram: ``mtu -
// header``.
func (d *DatagramRPCProtocol_) Max_message_bytes_() int {
	return max(0, min(d.Max_stream_frame_bytes, d.Mtu-STREAM_HEADER_LEN))
}

// Stream_enqueue_ sends *payload* as one unacknowledged datagram (user
// thread).
func (d *DatagramRPCProtocol_) Stream_enqueue_(channel *Channel, payload []byte) error {
	addr := d.Connection_handle_(channel.Peer_id)
	if py.IsNone(addr) {
		return py.ConnectionError.Newf("No connection to peer %s", channel.Peer_id)
	}
	limit := d.hooks().Max_message_bytes_()
	if len(payload) > limit {
		return py.ValueError.Newf(
			"Stream message of %d bytes does not fit one datagram (mtu=%d - %d header = %d). Datagram lanes do not fragment.",
			len(payload), d.Mtu, STREAM_HEADER_LEN, limit)
	}
	seq := channel.Next_chunk_seqs_(1)
	lane := 0
	if tx := channel.Tx_lane_id(); tx != nil {
		lane = *tx
	}
	packet := Pack_stream_frame(lane, seq, FLAG_START|FLAG_END, payload)
	return d.Loop_call_(func() { d.Safe_sendto_(addr, packet) })
}

// Safe_sendto_ sends one stream datagram, logging (not raising) on
// failure.
func (d *DatagramRPCProtocol_) Safe_sendto_(addr any, packet []byte) {
	if err := d.datagram_hooks().Sendto_(addr, packet); err != nil {
		datagram_carrier_log_.Debug("stream datagram send failed", py.Kw{"exc_info": err})
	}
}
