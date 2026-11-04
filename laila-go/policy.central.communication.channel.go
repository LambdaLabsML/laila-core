package laila

import (
	"fmt"
	"sync"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/channel.py -- stream lanes: Channel, Relay,
// StreamEntry.
//
// A *channel* is an opaque, bidirectional byte stream between two peered
// policies that rides on the same transport connection as RPC. laila does
// not interpret the bytes (H.264/H.265 access units, JPEGs, telemetry
// structs, audio -- all the same here); interpretation is the policy's
// job. The user-facing shape is::
//
//	video := laila.Peers()[peer_gid]["video"]   // Channel; opens lazily
//	for entry := range laila.Relay(video)        // blocks on the caller's thread
//	    au := entry.Data()                       // bytes: exactly one sender-side send()
//	video.Send(b"...")                           // channels are bidirectional
//
// Three pieces live here:
//
//   - Channel -- per ``(peer, name)`` handle owned by the carrier that
//     negotiated it. Holds the bounded inbound queue, the send-side entry
//     point, message-reassembly state and the drop counters.
//   - Relay -- the blocking iterator returned by laila.Relay. One per
//     channel; it wraps each delivered message into a StreamEntry *on the
//     consumer thread*, never on the carrier's I/O loop.
//   - StreamEntry -- an Entry whose payload is the received ``bytes`` plus a
//     read-only Stream() StreamMeta describing where the packet came from.
//     It is a plain in-memory constant: not memorized, not pooled.
//     ``laila.memorize(entry)`` sends it through the ordinary memory path;
//     the ``.stream`` metadata is local-only and is not preserved by a
//     memorize/remember round trip.
//
// # Data path (what stream bytes *do not* touch)
//
// Stream frames use the carrier's event loop, reader/writer and outer
// framing, and nothing else: no ``_handle_request_frame``, no admission
// semaphore, no inbound executor, no ``_execute_rpc``, no codec decode.
// No futures, no ``future_bank``, no taskforce, no threads spawned by
// laila for relays. The receive loop routes them by a first-byte
// discriminator straight into the lane's inbound queue.
//
// # Reconnect semantics
//
// A channel is bound to one peer *connection*. When the peer drops
// (liveness, EOF, ``remove_peer``, ``stop()``) every channel for that peer
// is closed: its relay ends with StopIteration, Channel.Closed becomes
// true and Channel.Send raises ConnectionError. After the peer reconnects,
// ``laila.peers[gid]`` is a *new* proxy and ``laila.peers[gid][name]``
// returns a *new* channel; objects held from before the drop stay closed.
// Re-index to resume.
//
// # Drop policy
//
// Inbound queues are bounded by message count (``channel_queue_size``) and
// bytes (``channel_queue_bytes``). When a consumer is too slow the oldest
// queued message is evicted (``dropped`` increments) so the carrier loop
// never blocks and memory never grows unbounded, even for channels nobody
// calls laila.Relay on. Message boundaries are always preserved: a
// partially-received message (lost chunk, oversize) is discarded whole
// (``discarded`` increments) and never surfaces to the relay.

// SENTINEL_ is the queue marker that ends a Relay.
var SENTINEL_ = &struct{ name string }{"_SENTINEL"}

// StreamMeta is the provenance of one received stream message (a frozen
// dataclass).
//
//   - Peer_id: ``global_id`` of the sending policy.
//   - Channel: channel name (e.g. ``"video"``).
//   - Lane: local (receive-side) lane id the message arrived on.
//   - Seq: message-level sequence number assigned on arrival (``0, 1, 2
//     ...`` per channel). Gaps seen by a consumer equal the number of
//     messages evicted by the drop-oldest policy before it.
//   - Arrived_at: ``time.monotonic()`` when the complete message was
//     enqueued. Monotonic across entries of one channel; use for ordering
//     and latency, not for wall-clock stamps.
//   - Arrived_wall: ``time.time()`` at the same instant.
//   - Dropped_before: cumulative inbound drop count of the channel when
//     *this* message was enqueued (messages evicted later, while it
//     waited, are not included). ``seq`` gaps are the authoritative loss
//     signal.
type StreamMeta struct {
	Peer_id        string
	Channel        string
	Lane           int
	Seq            int
	Arrived_at     float64
	Arrived_wall   float64
	Dropped_before int
}

// Repr is the dataclass repr.
func (m StreamMeta) Repr() string {
	return fmt.Sprintf("StreamMeta(peer_id=%s, channel=%s, lane=%d, seq=%d, arrived_at=%s, arrived_wall=%s, dropped_before=%d)",
		py.Repr(m.Peer_id), py.Repr(m.Channel), m.Lane, m.Seq, py.Repr(m.Arrived_at), py.Repr(m.Arrived_wall), m.Dropped_before)
}

// StreamEntry is an Entry produced by Relay for one stream message.
//
// Identical to ``Entry.constant(payload)`` -- fresh ``uuid4``,
// ``evolution`` None, scopes ``["ENTRY"]``, state ``READY`` -- plus a
// read-only Stream() carrying StreamMeta. Nothing in the entry package
// changes for this; the subclass only adds one private attribute.
//
// Rationale for *constant* identity: a packet is an immutable fact; a new
// ``global_id`` per packet is the current model. Identity creation is
// centralised in Relay.Wrap_ so a later evolution-based scheme
// (``nickname=f"{peer}/{channel}", evolution=seq``) is a one-function
// change.
type StreamEntry struct {
	Entry
	Stream_ *StreamMeta `py:"_stream"`
}

// StreamEntry_class is the class descriptor.
var StreamEntry_class = py.NewClass("StreamEntry", (*StreamEntry)(nil), Entry_class)

// Stream is where this packet came from (nil for non-stream entries).
func (e *StreamEntry) Stream() *StreamMeta { return e.Stream_ }

// StreamEntry_from_payload_ is ``StreamEntry._from_payload(payload)``:
// build a READY constant entry around payload.
//
// Mirrors the body of Entry_constant (which hard-codes ``Entry(...)`` and
// therefore cannot return a subclass).
func StreamEntry_from_payload_(payload []byte) (*StreamEntry, error) {
	e, err := New_entry_of_(StreamEntry_class, py.Kw{
		"uuid": nil, "data": payload, "state": EntryState_READY, "evolution": nil,
	})
	if err != nil {
		return nil, err
	}
	return e.Self().(*StreamEntry), nil
}

func init() {
	py.Register_property("stream", "protocol", "opened", "closed", "closed_reason",
		"tx_lane_id", "rx_seq", "tx_seq", "dropped", "discarded", "tx_dropped")
}

// Channel_carrier_ is the surface a Channel needs from its owning carrier
// (``_CarrierRPCProtocol``): the outbound hand-off and the local-close
// notification. ``rpc_timeout`` is read through py.Getattr.
type Channel_carrier_ interface {
	Object_() *py.Object
	Stream_enqueue_(channel *Channel, payload []byte) error
	On_channel_closed_locally_(ch *Channel) error
}

// streamItem_ is the queued ``(payload, seq, arrived_at, wall,
// dropped_before)`` tuple.
type streamItem_ struct {
	payload        []byte
	seq            int
	arrived_at     float64
	wall           float64
	dropped_before int
}

// Channel is a named, bidirectional byte lane to one peer over one
// carrier.
//
// Obtained through ``laila.peers[gid][name]`` (or
// ``laila.peers[gid].via(token)[name]`` to pin the transport). Channels
// are created and cached by the carrier; user code never constructs them.
//
// Attributes:
//   - Closed: true once closed by either side, by peer loss or by shutdown.
//   - Closed_reason: human-readable reason set at close time (nil while
//     open).
//   - Rx_seq: messages enqueued so far (message-level sequence counter).
//   - Tx_seq: messages accepted by Send.
//   - Dropped: inbound messages evicted because the consumer was too slow.
//   - Discarded: inbound messages thrown away because a chunk was lost or
//     the message exceeded ``max_message_bytes``.
//   - Tx_dropped: outbound messages evicted from the carrier's bounded send
//     queue.
//
// After a peer drop this object stays closed forever; re-index
// ``laila.peers[gid][name]`` after the peer reconnects (see the file
// comment).
type Channel struct {
	Peer_id           string
	Name              string
	Lane_id           int
	Max_message_bytes int

	// Mutable observability state. Python exposes these as plain
	// attributes (``ch.closed``, ``ch.rx_seq`` ...) that any thread may
	// read while the carrier thread updates them; under Go's memory model
	// that is a data race, so they live behind ``lock_`` and are read
	// through the same-named accessors (Closed(), Rx_seq(), ...).
	tx_lane_id_    *int
	closed_        bool
	closed_reason_ *string
	rx_seq_        int
	tx_seq_        int
	dropped_       int
	discarded_     int
	tx_dropped_    int

	proto_ref_       py.Weak
	queue_           *py.Queue
	queue_bytes_     int
	queue_bytes_cap_ int
	lock_            sync.Mutex
	opened_          py.Event
	relay_           *Relay
	// reassembly state (touched only by the carrier's inbound thread)
	rx_buf_      [][]byte
	rx_len_      int
	rx_expected_ *int64
	rx_active_   bool
	rx_poisoned_ bool
	tx_chunk_seq_ int64
}

// New_Channel is ``Channel(protocol, peer_id, name, lane_id, *,
// queue_size, queue_bytes, max_message_bytes, tx_lane_id=None)``.
//
//   - protocol: owning carrier (held by weak reference).
//   - peer_id: remote policy ``global_id``.
//   - name: channel name (free-form; ``"default"`` is reserved for RPC).
//   - lane_id: local receive-side lane id (``1..255``). Frames the peer
//     sends to us carry this id.
//   - queue_size, queue_bytes: inbound bounds (messages / bytes); oldest
//     messages are evicted when exceeded.
//   - max_message_bytes: largest message accepted on either direction.
//   - tx_lane_id: the peer's receive-side lane id (what *we* put in
//     frames). nil while an open handshake is still in flight.
func New_Channel(protocol Channel_carrier_, peer_id string, name string, lane_id int, kw py.Kw) *Channel {
	queue_size, _ := py.Int(kw.Get("queue_size", 256))
	queue_bytes, _ := py.Int(kw.Get("queue_bytes", 64*1024*1024))
	max_message_bytes, _ := py.Int(kw.Get("max_message_bytes", 64*1024*1024))
	var tx_lane_id *int
	if v := kw.Get("tx_lane_id", nil); !py.IsNone(v) {
		n, _ := py.Int(v)
		tx_lane_id = &n
	}
	ch := &Channel{
		Peer_id:           peer_id,
		Name:              name,
		Lane_id:           lane_id,
		tx_lane_id_:       tx_lane_id,
		Max_message_bytes: max_message_bytes,
		queue_:            py.New_Queue(max(1, queue_size)),
		queue_bytes_cap_:  max(1, queue_bytes),
		rx_buf_:           [][]byte{},
	}
	if protocol != nil {
		ch.proto_ref_ = py.WeakRef_self(protocol)
	}
	if tx_lane_id != nil {
		ch.opened_.Set()
	}
	return ch
}

// ------------------------------------------------------------------
// Introspection
// ------------------------------------------------------------------

// Protocol is the owning carrier, or nil if it has been garbage-collected.
func (c *Channel) Protocol() Channel_carrier_ {
	if c.proto_ref_ == nil {
		return nil
	}
	v := c.proto_ref_.Value()
	if v == nil {
		return nil
	}
	p, _ := v.(Channel_carrier_)
	return p
}

// Opened is true once the lane handshake completed (``tx_lane_id`` known).
func (c *Channel) Opened() bool {
	c.lock_.Lock()
	defer c.lock_.Unlock()
	return c.tx_lane_id_ != nil && !c.closed_
}

// Tx_lane_id is the peer's receive-side lane id (nil while opening).
func (c *Channel) Tx_lane_id() *int { c.lock_.Lock(); defer c.lock_.Unlock(); return c.tx_lane_id_ }

// Closed reports whether the channel is closed (either side / peer loss).
func (c *Channel) Closed() bool { c.lock_.Lock(); defer c.lock_.Unlock(); return c.closed_ }

// Closed_reason is the human-readable reason set at close time (nil while
// open).
func (c *Channel) Closed_reason() *string { c.lock_.Lock(); defer c.lock_.Unlock(); return c.closed_reason_ }

// Rx_seq is the number of messages enqueued so far (message-level
// sequence counter).
func (c *Channel) Rx_seq() int { c.lock_.Lock(); defer c.lock_.Unlock(); return c.rx_seq_ }

// Tx_seq is the number of messages accepted by Send.
func (c *Channel) Tx_seq() int { c.lock_.Lock(); defer c.lock_.Unlock(); return c.tx_seq_ }

// Dropped is the number of inbound messages evicted because the consumer
// was too slow.
func (c *Channel) Dropped() int { c.lock_.Lock(); defer c.lock_.Unlock(); return c.dropped_ }

// Discarded is the number of inbound messages thrown away because a chunk
// was lost or the message exceeded the size limit.
func (c *Channel) Discarded() int { c.lock_.Lock(); defer c.lock_.Unlock(); return c.discarded_ }

// Tx_dropped is the number of outbound messages evicted from the
// carrier's bounded send queue.
func (c *Channel) Tx_dropped() int { c.lock_.Lock(); defer c.lock_.Unlock(); return c.tx_dropped_ }

// Tx_dropped_inc_ is ``channel.tx_dropped += 1`` (carrier send queue).
func (c *Channel) Tx_dropped_inc_() { c.lock_.Lock(); c.tx_dropped_++; c.lock_.Unlock() }

// Set_tx_lane_id_ is ``ch.tx_lane_id = lane`` (peer re-opened after a lost
// close).
func (c *Channel) Set_tx_lane_id_(lane int) { c.lock_.Lock(); c.tx_lane_id_ = &lane; c.lock_.Unlock() }

func (c *Channel) discarded_inc_() { c.lock_.Lock(); c.discarded_++; c.lock_.Unlock() }

// Queue_bytes_ is ``ch._queue_bytes``: bytes currently held in the inbound
// queue.
func (c *Channel) Queue_bytes_() int { c.lock_.Lock(); defer c.lock_.Unlock(); return c.queue_bytes_ }

// Tx_chunk_seq_ is ``ch._tx_chunk_seq``: the next outbound chunk sequence.
func (c *Channel) Tx_chunk_seq_() int64 { c.lock_.Lock(); defer c.lock_.Unlock(); return c.tx_chunk_seq_ }

// Set_tx_chunk_seq_ is ``ch._tx_chunk_seq = v`` (tests only).
func (c *Channel) Set_tx_chunk_seq_(v int64) { c.lock_.Lock(); c.tx_chunk_seq_ = v; c.lock_.Unlock() }

// Qsize is the number of messages currently buffered for the consumer.
func (c *Channel) Qsize() int { return c.queue_.Qsize() }

// Repr is ``Channel('peer', 'name', rx_lane=1, tx_lane=1, open)``.
func (c *Channel) Repr() string {
	state := "opening"
	if c.Closed() {
		state = "closed"
	} else if c.Opened() {
		state = "open"
	}
	return fmt.Sprintf("Channel(%s, %s, rx_lane=%d, tx_lane=%s, %s)",
		py.Repr(c.Peer_id), py.Repr(c.Name), c.Lane_id, py.Repr(py.Deref_any(c.Tx_lane_id())), state)
}

func (c *Channel) String() string { return c.Repr() }

// Closed_reason_str is ``closed_reason or 'no reason'``-style access ("" while open).
func (c *Channel) closedReason(def string) string {
	r := c.Closed_reason()
	if r == nil || *r == "" {
		return def
	}
	return *r
}

// ------------------------------------------------------------------
// Send side (user threads)
// ------------------------------------------------------------------

// Send queues one message for the peer. One ``send()`` -> one entry
// remotely. ``b""`` is a valid message.
//
// ConnectionError if the channel is closed, the carrier is gone, or the
// lane handshake did not complete in time. ValueError if ``len(payload)``
// exceeds the carrier's ``max_stream_frame_bytes`` (or, on datagram
// carriers, the per-datagram limit).
//
// Non-blocking: the bytes are handed to the carrier's bounded send queue.
// On a saturated link the oldest *unsent* message is evicted and
// Tx_dropped increments.
func (c *Channel) Send(payload []byte) error {
	if c.Closed() {
		return py.ConnectionError.Newf(
			"Channel %s to peer %s is closed (%s). Re-index laila.peers[gid][name] after the peer reconnects.",
			py.Repr(c.Name), c.Peer_id, c.closedReason("no reason"))
	}
	if payload == nil {
		payload = []byte{}
	}
	if len(payload) > c.Max_message_bytes {
		return py.ValueError.Newf(
			"Stream message of %d bytes exceeds the %d-byte limit of channel %s (max_stream_frame_bytes, or mtu - header on datagram carriers).",
			len(payload), c.Max_message_bytes, py.Repr(c.Name))
	}
	proto := c.Protocol()
	if proto == nil {
		return py.ConnectionError.New("The carrier owning this channel no longer exists.")
	}
	if c.Tx_lane_id() == nil {
		wait := 60.0
		if v, err := py.Getattr(proto, "rpc_timeout"); err == nil {
			if f, err := py.Float(v); err == nil {
				wait = f
			}
		}
		if !c.opened_.Wait(wait) || c.Tx_lane_id() == nil {
			return py.ConnectionError.Newf("Channel %s to peer %s is still opening.", py.Repr(c.Name), c.Peer_id)
		}
		if c.Closed() {
			return py.ConnectionError.Newf("Channel %s to peer %s closed while opening (%s).",
				py.Repr(c.Name), c.Peer_id, py.Str(py.Deref_any(c.Closed_reason())))
		}
	}
	if err := proto.Stream_enqueue_(c, payload); err != nil {
		return err
	}
	c.lock_.Lock()
	c.tx_seq_++
	c.lock_.Unlock()
	return nil
}

// Next_chunk_seqs_ reserves count consecutive wire chunk sequence numbers;
// returns the first.
func (c *Channel) Next_chunk_seqs_(count int) int64 {
	c.lock_.Lock()
	defer c.lock_.Unlock()
	first := c.tx_chunk_seq_
	c.tx_chunk_seq_ = (first + int64(count)) % SEQ_MODULUS
	return first
}

// ------------------------------------------------------------------
// Receive side (carrier inbound thread)
// ------------------------------------------------------------------

// On_chunk_ feeds one wire chunk into the reassembly buffer.
//
// seq is the per-lane chunk counter; a gap means a chunk was lost and the
// in-progress message is discarded whole. ``START`` resets the buffer,
// ``END`` delivers. Runs on the carrier's inbound thread only, so it is
// lock-free.
func (c *Channel) On_chunk_(seq int64, flags int, data []byte, arrived_at float64) {
	start := flags&FLAG_START != 0
	end := flags&FLAG_END != 0
	if start {
		if c.rx_active_ {
			// previous message never saw its END -> lost tail
			c.discarded_inc_()
		}
		c.rx_buf_ = [][]byte{}
		c.rx_len_ = 0
		c.rx_active_ = true
		c.rx_poisoned_ = false
	} else {
		if !c.rx_active_ {
			// continuation without a START -> head was lost; swallow
			next := (seq + 1) % SEQ_MODULUS
			c.rx_expected_ = &next
			if end {
				c.discarded_inc_()
			}
			return
		}
		if c.rx_expected_ != nil && seq != *c.rx_expected_ {
			c.rx_poisoned_ = true
		}
	}
	next := (seq + 1) % SEQ_MODULUS
	c.rx_expected_ = &next

	if !c.rx_poisoned_ {
		c.rx_len_ += len(data)
		if c.rx_len_ > c.Max_message_bytes {
			c.rx_poisoned_ = true
			c.rx_buf_ = [][]byte{}
		} else {
			c.rx_buf_ = append(c.rx_buf_, data)
		}
	}

	if !end {
		return
	}
	c.rx_active_ = false
	buf := c.rx_buf_
	c.rx_buf_ = [][]byte{}
	c.rx_len_ = 0
	if c.rx_poisoned_ {
		c.rx_poisoned_ = false
		c.discarded_inc_()
		return
	}
	var payload []byte
	switch len(buf) {
	case 1:
		payload = buf[0]
	case 0:
		payload = []byte{}
	default:
		n := 0
		for _, b := range buf {
			n += len(b)
		}
		payload = make([]byte, 0, n)
		for _, b := range buf {
			payload = append(payload, b...)
		}
	}
	c.Enqueue_inbound_(payload, arrived_at)
}

// Enqueue_inbound_ enqueues one complete message, evicting the oldest when
// over bounds. ``arrived_at < 0`` means ``None`` (stamp now).
//
// Safe from any thread (loopback delivers from the sender's thread).
// Never blocks.
func (c *Channel) Enqueue_inbound_(payload []byte, arrived_at float64) {
	if arrived_at < 0 {
		arrived_at = py.Monotonic()
	}
	wall := py.Time()
	size := len(payload)
	c.lock_.Lock()
	defer c.lock_.Unlock()
	if c.closed_ {
		return
	}
	seq := c.rx_seq_
	c.rx_seq_++
	item := &streamItem_{payload: payload, seq: seq, arrived_at: arrived_at, wall: wall, dropped_before: c.dropped_}
	for {
		over_bytes := c.queue_bytes_ > 0 && c.queue_bytes_+size > c.queue_bytes_cap_
		if !over_bytes {
			if err := c.queue_.Put_nowait(item); err == nil {
				c.queue_bytes_ += size
				return
			}
		}
		old, err := c.queue_.Get_nowait()
		if err != nil {
			// consumer drained concurrently; account and retry
			c.queue_bytes_ = 0
			continue
		}
		if old == SENTINEL_ {
			// cannot happen while not closed; be defensive
			_ = c.queue_.Put_nowait(old)
			return
		}
		c.queue_bytes_ -= len(old.(*streamItem_).payload)
		c.dropped_++
	}
}

// Consumed_ is the book-keeping hook called by Relay after a successful get.
func (c *Channel) Consumed_(item *streamItem_) {
	c.lock_.Lock()
	c.queue_bytes_ -= len(item.payload)
	if c.queue_bytes_ < 0 {
		c.queue_bytes_ = 0
	}
	c.lock_.Unlock()
}

// ------------------------------------------------------------------
// Lifecycle
// ------------------------------------------------------------------

// Mark_opened_ finalizes the open handshake (called by the carrier).
func (c *Channel) Mark_opened_(tx_lane_id int) {
	c.lock_.Lock()
	c.tx_lane_id_ = &tx_lane_id
	c.lock_.Unlock()
	c.opened_.Set()
}

// Close_local_ closes without notifying the peer. Idempotent; returns true
// on first close.
//
// Wakes a blocked Relay by enqueuing the sentinel (evicting one message if
// the queue is full so the wake-up cannot be lost).
func (c *Channel) Close_local_(reason string) bool {
	c.lock_.Lock()
	defer c.lock_.Unlock()
	if c.closed_ {
		return false
	}
	c.closed_ = true
	c.closed_reason_ = &reason
	c.opened_.Set()
	for {
		if err := c.queue_.Put_nowait(SENTINEL_); err == nil {
			break
		}
		old, err := c.queue_.Get_nowait()
		if err != nil {
			continue
		}
		if old != SENTINEL_ {
			c.queue_bytes_ -= len(old.(*streamItem_).payload)
			c.dropped_++
		}
	}
	c.rx_buf_ = [][]byte{}
	c.rx_len_ = 0
	c.rx_active_ = false
	return true
}

// Close closes this channel and tells the peer (best effort). Idempotent.
// ``reason`` defaults to ``"closed locally"``.
//
// The peer's matching channel ends its relay with StopIteration. Pending
// inbound messages are dropped; a blocked relay wakes up.
func (c *Channel) Close(reason ...string) {
	r := "closed locally"
	if len(reason) > 0 {
		r = reason[0]
	}
	proto := c.Protocol()
	if !c.Close_local_(r) {
		return
	}
	if proto != nil {
		func() {
			defer func() { _ = recover() }()
			_ = proto.On_channel_closed_locally_(c)
		}()
	}
}

// Relay returns the channel's single Relay, updating its timeout (nil =
// wait forever).
//
// See laila.Relay. Two threads iterating the same relay split the packets
// between them (it is one queue).
func (c *Channel) Relay(timeout *float64) *Relay {
	c.lock_.Lock()
	defer c.lock_.Unlock()
	if c.relay_ == nil {
		c.relay_ = New_Relay(c, timeout)
	} else {
		c.relay_.Timeout = timeout
	}
	return c.relay_
}

// Relay_ is Relay for callers holding a py-style ``timeout`` value (nil /
// number).
func (c *Channel) Relay_(timeout any) *Relay {
	return c.Relay(optFloat_(timeout))
}

func optFloat_(v any) *float64 {
	if py.IsNone(v) {
		return nil
	}
	f, err := py.Float(v)
	if err != nil {
		return nil
	}
	return &f
}

// Relay is a blocking iterator over one Channel yielding StreamEntry.
//
// Returned by laila.Relay / Channel.Relay. Runs on the caller's thread; no
// futures, no taskforce, no threads spawned by laila.
//
//   - Timeout: per-message wait. nil waits until a message arrives or the
//     channel closes; otherwise TimeoutError is raised when no message
//     arrives within Timeout seconds.
//
// Iteration ends (StopIteration) when the channel closes: peer loss,
// remote ``close()``, local ``close()``, carrier/communication ``stop()``
// or laila.Terminate. Messages already buffered are drained first.
// Closed_reason says why. The wait loop wakes at least once a second to
// re-check the closed flag, so a missed sentinel or a Ctrl-C cannot strand
// the consumer. After a peer reconnects this relay stays finished;
// re-index ``laila.peers[gid][name]`` and call laila.Relay again.
type Relay struct {
	Channel *Channel
	Timeout *float64
}

// New_Relay is ``Relay(channel, timeout=None)``.
func New_Relay(channel *Channel, timeout *float64) *Relay {
	return &Relay{Channel: channel, Timeout: timeout}
}

// Closed_reason is why the underlying channel closed (nil while open).
func (r *Relay) Closed_reason() *string { return r.Channel.Closed_reason() }

// Iter is ``__iter__``: the relay is its own iterator.
func (r *Relay) Iter() *Relay { return r }

// Next is ``__next__``: the next StreamEntry, or a StopIteration error
// once the channel has closed and drained (TimeoutError when Timeout
// elapses without a message).
func (r *Relay) Next() (*StreamEntry, error) {
	ch := r.Channel
	q := ch.queue_
	timeout := r.Timeout
	var deadline *float64
	if timeout != nil {
		d := py.Monotonic() + *timeout
		deadline = &d
	}
	for {
		if ch.Closed() && q.Empty() {
			return nil, py.StopIteration.New()
		}
		var wait float64
		if deadline == nil {
			wait = 1.0
		} else {
			remaining := *deadline - py.Monotonic()
			if remaining <= 0 {
				return nil, py.TimeoutError.Newf("No message on channel %s from %s within %ss.",
					py.Repr(ch.Name), ch.Peer_id, py.Str(*timeout))
			}
			wait = min(1.0, remaining)
		}
		item, err := q.Get(wait)
		if err != nil {
			continue
		}
		if item == SENTINEL_ {
			return nil, py.StopIteration.New()
		}
		it := item.(*streamItem_)
		ch.Consumed_(it)
		return r.Wrap_(it)
	}
}

// Range drives the iterator: ``for entry in relay``. fn returning an
// error stops early and propagates it; TimeoutError from Next propagates
// too; StopIteration ends the loop cleanly.
func (r *Relay) Range(fn func(entry *StreamEntry) error) error {
	for {
		entry, err := r.Next()
		if err != nil {
			if py.IsInstance(err, py.StopIteration) {
				return nil
			}
			return err
		}
		if err := fn(entry); err != nil {
			return err
		}
	}
}

// Wrap_ turns a queued ``(payload, seq, arrived_at, wall, dropped_before)``
// into an entry. The single place where stream identity is minted (see
// StreamEntry notes).
func (r *Relay) Wrap_(item *streamItem_) (*StreamEntry, error) {
	ch := r.Channel
	entry, err := StreamEntry_from_payload_(item.payload)
	if err != nil {
		return nil, err
	}
	entry.Stream_ = &StreamMeta{
		Peer_id:        ch.Peer_id,
		Channel:        ch.Name,
		Lane:           ch.Lane_id,
		Seq:            item.seq,
		Arrived_at:     item.arrived_at,
		Arrived_wall:   item.wall,
		Dropped_before: item.dropped_before,
	}
	return entry, nil
}

// Repr is ``Relay(Channel(...), timeout=None)``.
func (r *Relay) Repr() string {
	return fmt.Sprintf("Relay(%s, timeout=%s)", r.Channel.Repr(), py.Repr(py.Deref_any(r.Timeout)))
}

func (r *Relay) String() string { return r.Repr() }
