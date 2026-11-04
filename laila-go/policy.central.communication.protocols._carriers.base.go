package laila

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/_carriers/base.py -- shared base
// for all RPC carriers.
//
// CarrierRPCProtocol_ factors out everything that is identical across
// transports regardless of the underlying wire:
//
//   - **Config fields** common to every carrier: the wire ``codec``, the
//     blocking ``rpc_timeout`` and ``handshake_timeout``, and the
//     ``peer_secret_key`` presented during the handshake.
//   - **Peer bookkeeping**: a ``_connections`` map (``peer_id`` -> opaque
//     transport handle) plus the two-tier Register_peer_ / Unregister_peer_
//     that also notify the owning LAILA_IDENTIFIABLE_COMMUNICATION_ so a
//     RemotePolicyProxy is created/destroyed.
//   - **Outbound correlation**: a ``_pending_rpcs`` table and the
//     Register_pending_ / Complete_pending_ / Await_pending_ trio that let
//     a synchronous ``send_rpc`` block on a threading.Event until the
//     matching response frame arrives on the carrier's I/O thread.
//   - **Inbound dispatch**: Build_response_ runs the actual ``_execute_rpc``
//     on a worker thread (never the I/O loop) so a blocking remote call --
//     e.g. ``_wait_future`` -- cannot stall the transport.
//   - **Stream lanes**: the per-peer lane tables behind
//     ``laila.peers[gid][name]`` -- Open_channel, the
//     ``__comm_channel_open__`` / ``__comm_channel_close__`` control frames
//     (intercepted exactly like ``__comm_ping__``: before admission, before
//     ``_execute_rpc``), On_stream_frame_ reassembly, and the
//     close-all-lanes step that runs before a peer is unregistered.
//     Carriers that can stream set ``supports_channels`` and implement
//     Stream_enqueue_.
//
// Concrete carriers (StreamRPCProtocol_, DatagramRPCProtocol_) build their
// wire-specific logic on top of these helpers.

var carrier_log_ = py.Get_logger("laila.policy.central.communication.protocols._carriers.base")

// Reserved dotted-paths. COMM_PING_PATH_ is the liveness ping control
// frame, intercepted in Build_response_ before it can reach the policy;
// the channel paths are the stream-lane control, answered at the same
// interception point on the I/O thread before admission / executor.
var (
	COMM_CHANNEL_OPEN_PATH_  = []string{"__comm_channel_open__"}
	COMM_CHANNEL_CLOSE_PATH_ = []string{"__comm_channel_close__"}
)

// MAX_LANE_ is the highest allocatable lane id; ``0`` is reserved for
// RPC/control.
const MAX_LANE_ = 255

// Peering control methods (top-level JSON-RPC ``method`` values).
const (
	PEER_CONNECT_ = "peer.connect"
	// PEER_DISCONNECT_ is the notification a side sends right before
	// dropping a peer / stopping, so the other side unregisters it at
	// once rather than on liveness timeout.
	PEER_DISCONNECT_ = "peer.disconnect"
)

// GOODBYE_TIMEOUT_ is the upper bound on how long ``disconnect()`` waits
// for the goodbye to go out.
const GOODBYE_TIMEOUT_ = 1.0

// BackpressureError is raised when a peer rejected an RPC with
// ``ERR_BUSY``.
//
// Distinct from a generic remote error so the sender's retry loop can
// scope exponential backoff to overload only, and surface a clear final
// error if the peer stays saturated.
var BackpressureError = py.NewType("BackpressureError", py.RuntimeError)

// Carrier_hooks_ is the set of carrier methods Python resolves virtually
// (wire carriers and concrete transports override them); the base
// dispatches through Self() so the most-derived implementation runs.
type Carrier_hooks_ interface {
	Send_once_(peer_id string, path []string, args []any, kwargs py.Kw) (any, error)
	Send_oneway_(peer_id string, path []string, kwargs py.Kw) error
	Send_goodbye_(peer_id string) error
	Stream_enqueue_(channel *Channel, payload []byte) error
	Register_peer_(peer_id string, handle any)
	Unregister_peer_(peer_id string)
	Max_message_bytes_() int
	Build_response_(msg *py.Dict, peer_id any) *py.Dict
	Handle_control_(path []string, kwargs *py.Dict, peer_id any) (any, error)
	Local_caps_() *py.Dict
}

// CarrierRPCProtocol_ is the abstract carrier holding wire-agnostic RPC
// machinery.
//
// This class is never registered directly -- it has no ``protocol_name``
// of its own. Subclasses set ``protocol_name``, the URI/token routing,
// and the transport endpoint factories.
//
// Fields:
//
//   - Codec (default ``"json"``) -- wire serialisation, one of CODECS.
//     ``"msgpack"`` is far more compact for bandwidth-constrained links.
//   - Rpc_timeout (default ``60.0``) -- seconds a blocking Send_rpc waits
//     for the response.
//   - Handshake_timeout (default ``10.0``) -- seconds the peering
//     handshake waits for the remote reply.
//   - Peer_secret_key -- shared secret a remote peer must present during
//     the handshake. Defaults to a fresh UUID4 hex.
//   - Channel_queue_size (default ``256``) -- max *messages* buffered per
//     inbound stream lane before the oldest is evicted (``Channel.dropped``).
//   - Channel_queue_bytes (default ``64 MiB``) -- max *bytes* buffered per
//     inbound stream lane (same eviction). Bounds memory for lanes nobody
//     is relaying from.
//   - Stream_chunk_bytes (default ``65536``) -- outbound stream messages
//     are sliced into chunks of this size so RPC/ping frames can
//     interleave between chunks. Lower it on slow links (serial carriers
//     default to ``4096``): worst-case RPC delay is roughly
//     ``2 * stream_chunk_bytes / link bandwidth``.
//   - Max_stream_frame_bytes (default ``64 MiB``) -- largest single stream
//     message accepted in either direction (``send()`` raises ValueError;
//     oversized inbound messages are discarded). Independent of
//     CODEC_MAX_FRAME_BYTES.
//   - Static_lanes -- ``{channel_name: lane_id}`` bound at peer
//     registration without a control handshake. Strictly for peers that
//     emit lane frames but cannot answer ``__comm_channel_open__`` (e.g.
//     MCU firmware). Never configure it against an older laila peer: it
//     would feed ``0x01`` frames to a receiver that treats them as RPC and
//     drops the link.
type CarrierRPCProtocol_ struct {
	LAILA_IDENTIFIABLE_COMM_PROTOCOL_
	Loop_thread_state_

	Codec             string  `py:"codec" default:"json"`
	Rpc_timeout       float64 `py:"rpc_timeout" default:"60.0"`
	Handshake_timeout float64 `py:"handshake_timeout" default:"10.0"`
	Ping_timeout      float64 `py:"ping_timeout" default:"5.0"`
	Peer_secret_key   string  `py:"peer_secret_key,factory=Default_peer_secret_key_"`
	// Sender-side backoff when a peer replies ``ERR_BUSY`` (backpressure).
	Rpc_backoff_base float64 `py:"rpc_backoff_base" default:"0.05"`
	Rpc_backoff_max  float64 `py:"rpc_backoff_max" default:"5.0"`
	Max_rpc_retries  int     `py:"max_rpc_retries" default:"5"`

	// Stream-lane tuning (inert on carriers with ``supports_channels=False``).
	Channel_queue_size     int      `py:"channel_queue_size" default:"256"`
	Channel_queue_bytes    int      `py:"channel_queue_bytes" default:"67108864"`
	Stream_chunk_bytes     int      `py:"stream_chunk_bytes" default:"65536"`
	Max_stream_frame_bytes int      `py:"max_stream_frame_bytes" default:"67108864"`
	Static_lanes           *py.Dict `py:"static_lanes,factory=New_static_lanes_"`

	Started_          bool                   `py:"_started"`
	Connections_      *py.Dict               `py:"_connections,factory=New_connections_"`
	Pending_rpcs_     *Pending_rpcs_         `py:"_pending_rpcs,factory=New_pending_rpcs_"`
	Inbound_executor_ *py.ThreadPoolExecutor `py:"_inbound_executor"`
	// per-thread RPC wait override (set by Ping; see Await_pending_)
	Rpc_wait_override_ *py.Local `py:"_rpc_wait_override,factory=New_rpc_wait_override_"`

	// stream-lane state: peer -> rx-lane -> Channel, peer -> name -> Channel
	Lanes_                  map[string]map[int]*Channel    `py:"_lanes"`
	Lane_names_             map[string]map[string]*Channel `py:"_lane_names"`
	Lane_cursor_            map[string]int                 `py:"_lane_cursor"`
	Peer_caps_              map[string]*py.Dict            `py:"_peer_caps"`
	Lane_lock_              sync.Mutex                     `py:"-"`
	Unknown_lane_drops_     int                            `py:"_unknown_lane_drops"`
	Reserved_marker_drops_  int                            `py:"_reserved_marker_drops"`
	Malformed_stream_drops_ int                            `py:"_malformed_stream_drops"`
}

// CarrierRPCProtocol_class is the class descriptor (``protocol_name =
// "carrier"``; carriers are abstract, concrete transports override).
var CarrierRPCProtocol_class = py.NewClass(
	"_CarrierRPCProtocol", (*CarrierRPCProtocol_)(nil),
	LAILA_IDENTIFIABLE_COMM_PROTOCOL_class,
).
	Set_classvar("protocol_name", "carrier")

// Carrier_base_ exposes the embedded carrier to code holding a subclass.
func (b *CarrierRPCProtocol_) Carrier_base_() *CarrierRPCProtocol_ { return b }

// As_carrier_ returns the embedded carrier of any derived instance (nil
// when v is not one).
func As_carrier_(v any) *CarrierRPCProtocol_ {
	if c, ok := v.(*CarrierRPCProtocol_); ok {
		return c
	}
	if b, ok := v.(interface{ Carrier_base_() *CarrierRPCProtocol_ }); ok {
		return b.Carrier_base_()
	}
	return nil
}

// hooks is the most-derived dispatch implementation.
// proto_ is ``self`` as the (virtual) protocol interface: ``self.send_rpc``
// / ``self.has_peer`` must reach a subclass override (loopback replaces
// ``send_rpc`` with a direct in-process dispatch).
func (b *CarrierRPCProtocol_) proto_() Comm_protocol_ {
	if self, ok := b.Self().(Comm_protocol_); ok {
		return self
	}
	return b
}

func (b *CarrierRPCProtocol_) hooks() Carrier_hooks_ {
	if h, ok := b.Self().(Carrier_hooks_); ok {
		return h
	}
	return b
}

// Default_peer_secret_key_ is ``Field(default_factory=lambda: uuid4().hex)``.
func (b *CarrierRPCProtocol_) Default_peer_secret_key_() string { return py.Uuid4().Hex() }

// New_static_lanes_ is ``Field(default_factory=dict)``.
func (b *CarrierRPCProtocol_) New_static_lanes_() *py.Dict { return py.NewDict() }

// New_connections_ is ``PrivateAttr(default_factory=dict)``.
func (b *CarrierRPCProtocol_) New_connections_() *py.Dict { return py.NewDict() }

// New_pending_rpcs_ is ``PrivateAttr(default_factory=dict)``.
func (b *CarrierRPCProtocol_) New_pending_rpcs_() *Pending_rpcs_ { return New_pending_rpcs_() }

// New_rpc_wait_override_ is ``PrivateAttr(default_factory=threading.local)``.
func (b *CarrierRPCProtocol_) New_rpc_wait_override_() *py.Local { return py.NewLocal() }

// Model_post_init initialises the private tables (pydantic private
// attributes with ``default_factory``).
func (b *CarrierRPCProtocol_) Model_post_init(ctx any) error {
	if err := b.LAILA_IDENTIFIABLE_COMM_PROTOCOL_.Model_post_init(ctx); err != nil {
		return err
	}
	b.init_tables_()
	return nil
}

func (b *CarrierRPCProtocol_) init_tables_() {
	if b.Connections_ == nil {
		b.Connections_ = py.NewDict()
	}
	if b.Pending_rpcs_ == nil {
		b.Pending_rpcs_ = New_pending_rpcs_()
	}
	if b.Rpc_wait_override_ == nil {
		b.Rpc_wait_override_ = py.NewLocal()
	}
	if b.Static_lanes == nil {
		b.Static_lanes = py.NewDict()
	}
	if b.Lanes_ == nil {
		b.Lanes_ = map[string]map[int]*Channel{}
	}
	if b.Lane_names_ == nil {
		b.Lane_names_ = map[string]map[string]*Channel{}
	}
	if b.Lane_cursor_ == nil {
		b.Lane_cursor_ = map[string]int{}
	}
	if b.Peer_caps_ == nil {
		b.Peer_caps_ = map[string]*py.Dict{}
	}
}

// Supports_channels_ is ``type(self).supports_channels``.
func (b *CarrierRPCProtocol_) Supports_channels_() bool {
	return Comm_protocol_supports_channels_(b.Self())
}

// ------------------------------------------------------------------
// Inbound dispatch (runs the real call off the I/O thread)
// ------------------------------------------------------------------

// Ensure_executor_ lazily creates the worker pool used to run inbound
// RPCs.
func (b *CarrierRPCProtocol_) Ensure_executor_() *py.ThreadPoolExecutor {
	if b.Inbound_executor_ == nil {
		b.Inbound_executor_ = py.New_ThreadPoolExecutor(8, py.Kw{
			"thread_name_prefix": fmt.Sprintf("%s-inbound", b.Class_().Name),
		})
	}
	return b.Inbound_executor_
}

// Build_response_ executes an inbound ``rpc.call`` *msg* and returns a
// response dict.
//
// Runs synchronously on the calling thread (carriers call this from a
// worker thread, never the I/O loop). Unknown methods and execution
// failures are mapped to JSON-RPC error envelopes.
//
// *peer_id* is the authenticated sender (known to the carrier from the
// connection). It is only needed by the stream-lane control frames, which
// carriers normally intercept earlier in Handle_request_frame_; it is
// threaded here too so a carrier that dispatches directly (loopback)
// still gets them.
func (b *CarrierRPCProtocol_) Build_response_(msg *py.Dict, peer_id any) *py.Dict {
	return b.build_response_(msg, peer_id)
}

// Build_response_via_ is ``proto._build_response(msg, peer_id)`` with
// Python attribute semantics: an instance-dict override
// (``proto.Private_set("_build_response", fn)`` -- the tests' spy) wins
// over the (virtually dispatched) class method.
func Build_response_via_(proto Carrier_hooks_, msg *py.Dict, peer_id any) *py.Dict {
	if o, ok := proto.(interface{ Private_get(string) (any, bool) }); ok {
		if fn, ok := o.Private_get("_build_response"); ok && !py.IsNone(fn) {
			out, err := py.Call(fn, []any{msg, peer_id}, nil)
			if err != nil {
				return Make_error(msg.GetDefault("id", nil), ERR_EXECUTION, fmt.Sprintf("%s: %s", py.Typename(err), err.Error()), nil)
			}
			d, _ := out.(*py.Dict)
			return d
		}
	}
	return proto.Build_response_(msg, peer_id)
}

func (b *CarrierRPCProtocol_) build_response_(msg *py.Dict, peer_id any) *py.Dict {
	request_id := msg.GetDefault("id", nil)
	method := msg.GetDefault("method", nil)
	if py.Str(method) != "rpc.call" {
		return Make_error(request_id, ERR_METHOD_NOT_FOUND, fmt.Sprintf("Unknown method: %s", py.Str(method)), nil)
	}
	params := dict_or_empty_(msg.GetDefault("params", nil))
	path := Str_list_(params.GetDefault("path", py.NewList()))
	args := py.Iter(params.GetDefault("args", py.NewList()))
	kwargs := Kw_of_(params.GetDefault("kwargs", py.NewDict()))
	// Liveness control frame: answered here, before the policy/worker
	// pool, so a ping never touches central.memory or the executor.
	if Str_list_eq_(path, COMM_PING_PATH_) {
		return Make_result(request_id, "pong")
	}
	if b.Is_control_path_(path) {
		return b.Control_response_(msg, peer_id)
	}
	comm := b.Communication()
	var result any
	var err error
	if comm == nil {
		err = py.AttributeError.New("'NoneType' object has no attribute '_execute_rpc'")
	} else {
		result, err = Execute_rpc_via_(comm, path, args, kwargs)
	}
	if err != nil {
		return Make_error(request_id, ERR_EXECUTION, fmt.Sprintf("%s: %s", py.Typename(err), err.Error()), nil)
	}
	return Make_result(request_id, result)
}

// ------------------------------------------------------------------
// Inbound admission control (per-policy backpressure)
// ------------------------------------------------------------------

// Is_ping_frame_ is ``True`` for the reserved liveness ``__comm_ping__``
// request.
//
// Pings must bypass admission entirely: a busy-but-alive peer must never
// be wrongly dropped by the liveness loop just because its RPC queue is
// full.
func Is_ping_frame_(msg *py.Dict) bool {
	if py.Str(msg.GetDefault("method", nil)) != "rpc.call" {
		return false
	}
	path := Str_list_(dict_or_empty_(msg.GetDefault("params", nil)).GetDefault("path", py.NewList()))
	return Str_list_eq_(path, COMM_PING_PATH_)
}

// Try_admit_ tries to claim an inbound-RPC slot from the hub.
// Non-blocking.
//
// Returns ``True`` if admitted (caller must later Release_admit_),
// ``False`` if the policy is at capacity and the request should be
// rejected with ``ERR_BUSY``. When no hub is attached (e.g. unit-testing
// a bare carrier) admission always succeeds.
func (b *CarrierRPCProtocol_) Try_admit_() bool {
	comm := b.Communication()
	if comm == nil {
		return true
	}
	return comm.Acquire_rpc_slot_()
}

// Release_admit_ releases a previously-claimed inbound-RPC slot.
func (b *CarrierRPCProtocol_) Release_admit_() {
	if comm := b.Communication(); comm != nil {
		comm.Release_rpc_slot_()
	}
}

// Busy_response_ builds the ``ERR_BUSY`` envelope for a rejected inbound
// request.
func (b *CarrierRPCProtocol_) Busy_response_(msg *py.Dict) *py.Dict {
	return Make_error(
		msg.GetDefault("id", nil),
		ERR_BUSY,
		fmt.Sprintf("%s peer is busy: inbound RPC queue at capacity.", py.Repr(b.Protocol_name())),
		nil,
	)
}

// Handle_request_frame_ is the centralized inbound dispatch: ping
// fast-path + admission + queue.
//
// ``reply(resp_dict)`` sends a response back over the transport and must
// be safe to invoke from a worker thread. *peer_id* is the authenticated
// sender; carriers pass it so stream-lane control frames are keyed by
// connection, never by a wire-supplied id.
//
//  1. A liveness ping is answered inline, *before* admission, so it never
//     queues and never gets rejected.
//  2. Stream-lane control frames (``__comm_channel_open__`` /
//     ``__comm_channel_close__``) are answered inline the same way: they
//     never take an admission slot and never reach the policy.
//  3. Otherwise a slot is claimed non-blocking; past capacity the request
//     is rejected immediately with ``ERR_BUSY`` (it never enters the
//     executor backlog, so the backlog stays bounded).
//  4. Admitted requests run Build_response_ on a worker thread, releasing
//     the slot when the reply is ready.
func (b *CarrierRPCProtocol_) Handle_request_frame_(msg *py.Dict, reply func(resp *py.Dict), peer_id any) {
	// Python attribute lookup: an instance-dict ``_handle_request_frame``
	// (``object.__setattr__(proto, "_handle_request_frame", fn)`` -- the
	// tests' way of making a peer deaf) wins over the class method.
	if fn, ok := b.Private_get("_handle_request_frame"); ok && !py.IsNone(fn) {
		_, _ = py.Call(fn, []any{msg, reply, peer_id}, nil)
		return
	}
	b.handle_request_frame_(msg, reply, peer_id)
}

func (b *CarrierRPCProtocol_) handle_request_frame_(msg *py.Dict, reply func(resp *py.Dict), peer_id any) {
	if Is_ping_frame_(msg) {
		reply(Make_result(msg.GetDefault("id", nil), "pong"))
		return
	}
	if py.Str(msg.GetDefault("method", nil)) == "rpc.call" &&
		b.Is_control_path_(Str_list_(dict_or_empty_(msg.GetDefault("params", nil)).GetDefault("path", py.NewList()))) {
		reply(b.Control_response_(msg, peer_id))
		return
	}
	if !b.Try_admit_() {
		reply(b.Busy_response_(msg))
		return
	}

	work := func() (any, error) {
		var resp *py.Dict
		func() {
			defer b.Release_admit_()
			resp = Build_response_via_(b.hooks(), msg, nil)
		}()
		reply(resp)
		return nil, nil
	}

	_, _ = b.Ensure_executor_().Submit(work)
}

// ------------------------------------------------------------------
// Peer registry (two-tier)
// ------------------------------------------------------------------

// Register_peer_ records a live transport *handle* for *peer_id* and
// notifies the hub.
//
// The communication hub creates the corresponding PeerProxy so user code
// can immediately reach the new peer. Afterwards every Static_lanes entry
// is pre-bound so early stream frames are buffered instead of dropped as
// unknown-lane.
func (b *CarrierRPCProtocol_) Register_peer_(peer_id string, handle any) {
	b.Connections_.Set(peer_id, handle)
	if comm := b.Communication(); comm != nil {
		comm.Register_peer_(peer_id)
	}
	b.Bind_static_lanes_(peer_id)
}

// Unregister_peer_ drops *peer_id*: close its lanes, drop the handle,
// notify the hub. Idempotent.
//
// Lanes are closed *first* (waking every relay with a sentinel) so this
// single place covers liveness drops, receive-loop EOF, ``remove_peer``,
// ``protocol.stop()``, ``comm.stop()`` and ``remove_connection``.
func (b *CarrierRPCProtocol_) Unregister_peer_(peer_id string) {
	b.Close_peer_lanes_(peer_id, "peer disconnected")
	b.Fail_pending_(fmt.Sprintf("peer %s disconnected", peer_id), &peer_id)
	b.Lane_lock_.Lock()
	delete(b.Peer_caps_, peer_id)
	delete(b.Lane_cursor_, peer_id)
	b.Lane_lock_.Unlock()
	b.Connections_.PopDefault(peer_id, nil)
	if comm := b.Communication(); comm != nil {
		comm.Unregister_peer_(peer_id)
	}
}

// Has_peer returns ``True`` if a live connection to *peer_id* is held.
func (b *CarrierRPCProtocol_) Has_peer(peer_id string) bool {
	return b.Connections_.Contains(peer_id)
}

// Connection_handle_ is ``self._connections.get(peer_id)``.
func (b *CarrierRPCProtocol_) Connection_handle_(peer_id string) any {
	v, _ := b.Connections_.Get(peer_id)
	return v
}

// Peer_ids_ is ``list(self._connections)``.
func (b *CarrierRPCProtocol_) Peer_ids_() []string {
	return Str_list_(b.Connections_.Keys())
}

// Disconnect gracefully drops a single peer. Idempotent.
//
// Sends the ``peer.disconnect`` notification (bounded, best effort) so
// the remote unregisters us immediately, then unregisters the peer
// locally -- which closes its lanes and, on carriers that hold a closable
// handle, the handle itself (Unregister_peer_).
func (b *CarrierRPCProtocol_) Disconnect(peer_id string) error {
	if !b.Connections_.Contains(peer_id) {
		return nil
	}
	b.Goodbye_threadsafe_(peer_id)
	b.hooks().Unregister_peer_(peer_id)
	return nil
}

// ------------------------------------------------------------------
// Graceful goodbye (``peer.disconnect``)
// ------------------------------------------------------------------

// Goodbye_message_ is the ``peer.disconnect`` notification this endpoint
// sends.
func (b *CarrierRPCProtocol_) Goodbye_message_() *py.Dict {
	return Make_notification(PEER_DISCONNECT_, py.DictOf("from_id", b.Policy_id_()))
}

// Is_goodbye_ is ``True`` for an inbound ``peer.disconnect`` notification.
func Is_goodbye_(msg *py.Dict) bool {
	return py.Str(msg.GetDefault("method", nil)) == PEER_DISCONNECT_
}

// Send_goodbye_ writes the goodbye to *peer_id* and waits (bounded) for
// it to leave.
//
// Runs on the carrier loop. Default no-op; wire carriers override with
// their own write primitive. Must never raise.
func (b *CarrierRPCProtocol_) Send_goodbye_(peer_id string) error { return nil }

// Goodbye_all_ says goodbye to every registered peer (first step of
// ``stop()``).
func (b *CarrierRPCProtocol_) Goodbye_all_() error {
	for _, peer_id := range b.Peer_ids_() {
		peer_id := peer_id
		if _, err := py.Wait_for(func() (any, error) { return nil, b.hooks().Send_goodbye_(peer_id) }, GOODBYE_TIMEOUT_); err != nil {
			carrier_log_.Debug("goodbye to %s failed", peer_id, py.Kw{"exc_info": err})
		}
	}
	return nil
}

// Goodbye_threadsafe_ runs Send_goodbye_ from a user thread, bounded;
// never raises.
func (b *CarrierRPCProtocol_) Goodbye_threadsafe_(peer_id string) {
	loop := b.Loop()
	if loop == nil || loop.Is_closed() || !loop.Is_running() {
		return
	}
	if running, err := py.Get_running_loop(); err == nil && running == loop {
		// On the loop thread we cannot block; the caller is tearing the
		// connection down right after, so skip the goodbye.
		return
	}
	fut := py.Run_coroutine_threadsafe(func() (any, error) { return nil, b.hooks().Send_goodbye_(peer_id) }, loop)
	if _, err := fut.Result(GOODBYE_TIMEOUT_); err != nil {
		carrier_log_.Debug("goodbye to %s failed", peer_id, py.Kw{"exc_info": err})
	}
}

// On_peer_goodbye_ handles an inbound ``peer.disconnect`` from an
// authenticated *peer_id*: drop it.
func (b *CarrierRPCProtocol_) On_peer_goodbye_(peer_id any) {
	if !py.IsNone(peer_id) && b.Connections_.Contains(py.Str(peer_id)) {
		b.hooks().Unregister_peer_(py.Str(peer_id))
	}
}

// Loop_call_ is ``call_soon_threadsafe`` on the carrier loop, or
// ConnectionError.
//
// The single place user-thread sends cross into the loop thread, so a
// send after ``stop()`` (loop gone or closed) always surfaces as a clear
// ConnectionError rather than an AttributeError or asyncio's
// RuntimeError.
func (b *CarrierRPCProtocol_) Loop_call_(fn func()) error {
	loop := b.Loop()
	if loop == nil || loop.Is_closed() {
		return py.ConnectionError.Newf("%s transport is shut down.", py.Repr(b.Protocol_name()))
	}
	if _, err := loop.Call_soon_threadsafe(fn); err != nil {
		if py.IsInstance(err, py.RuntimeError) {
			return py.ConnectionError.Newf("%s transport is shut down.", py.Repr(b.Protocol_name())).From(err)
		}
		return err
	}
	return nil
}

// ------------------------------------------------------------------
// Stream lanes
// ------------------------------------------------------------------

// Local_caps_ is the capabilities advertised in ``peer.connect``
// (``{"lanes": 1}`` if streaming).
func (b *CarrierRPCProtocol_) Local_caps_() *py.Dict {
	if b.Supports_channels_() {
		return py.DictOf("lanes", 1)
	}
	return py.NewDict()
}

// Set_peer_caps_ remembers the capabilities a peer advertised during the
// handshake.
//
// Call *before* Register_peer_ so a user opening a channel right after
// the handshake already sees them. Older peers send no ``caps`` at all;
// that is recorded as ``{}``.
func (b *CarrierRPCProtocol_) Set_peer_caps_(peer_id string, caps any) {
	b.Lane_lock_.Lock()
	defer b.Lane_lock_.Unlock()
	if d, ok := caps.(*py.Dict); ok {
		b.Peer_caps_[peer_id] = d.Copy()
	} else {
		b.Peer_caps_[peer_id] = py.NewDict()
	}
}

// Peer_has_lanes_ is ``True`` if *peer_id* advertised ``caps.lanes``
// during the handshake.
func (b *CarrierRPCProtocol_) Peer_has_lanes_(peer_id string) bool {
	b.Lane_lock_.Lock()
	caps := b.Peer_caps_[peer_id]
	b.Lane_lock_.Unlock()
	if caps == nil {
		return false
	}
	return py.Truthy(caps.GetDefault("lanes", nil))
}

// Channel_names returns the names of the lanes currently open to
// *peer_id* on this carrier.
func (b *CarrierRPCProtocol_) Channel_names(peer_id string) []string {
	b.Lane_lock_.Lock()
	defer b.Lane_lock_.Unlock()
	out := []string{}
	for n, ch := range b.Lane_names_[peer_id] {
		if !ch.Closed() {
			out = append(out, n)
		}
	}
	sort_strings_(out)
	return out
}

// New_channel_ creates and binds a Channel. Caller holds Lane_lock_.
func (b *CarrierRPCProtocol_) New_channel_(peer_id string, name string, lane int, tx_lane *int) *Channel {
	var tx any
	if tx_lane != nil {
		tx = *tx_lane
	}
	ch := New_Channel(b.Self().(Channel_carrier_), peer_id, name, lane, py.Kw{
		"queue_size":        b.Channel_queue_size,
		"queue_bytes":       b.Channel_queue_bytes,
		"max_message_bytes": b.hooks().Max_message_bytes_(),
		"tx_lane_id":        tx,
	})
	if b.Lanes_[peer_id] == nil {
		b.Lanes_[peer_id] = map[int]*Channel{}
	}
	b.Lanes_[peer_id][lane] = ch
	if b.Lane_names_[peer_id] == nil {
		b.Lane_names_[peer_id] = map[string]*Channel{}
	}
	b.Lane_names_[peer_id][name] = ch
	return ch
}

// Max_message_bytes_ is the largest stream message this carrier accepts
// (datagram carriers lower it).
func (b *CarrierRPCProtocol_) Max_message_bytes_() int { return b.Max_stream_frame_bytes }

// Alloc_lane_ picks a free rx lane id for *peer_id*. Caller holds
// Lane_lock_.
//
// Prefers *preferred* (the opener's own lane id, so both sides usually
// share one number); otherwise walks round-robin so an id freed by
// ``close()`` is not reused immediately while late frames for it may
// still be in flight.
func (b *CarrierRPCProtocol_) Alloc_lane_(peer_id string, preferred *int) (int, error) {
	lanes := b.Lanes_[peer_id]
	if lanes == nil {
		lanes = map[int]*Channel{}
		b.Lanes_[peer_id] = lanes
	}
	if preferred != nil && 1 <= *preferred && *preferred <= MAX_LANE_ {
		if _, used := lanes[*preferred]; !used {
			return *preferred, nil
		}
	}
	cursor := b.Lane_cursor_[peer_id]
	for i := 0; i < MAX_LANE_; i++ {
		cursor = cursor%MAX_LANE_ + 1
		if _, used := lanes[cursor]; !used {
			b.Lane_cursor_[peer_id] = cursor
			return cursor, nil
		}
	}
	return 0, py.ConnectionError.Newf("All %d stream lanes to peer %s are in use.", MAX_LANE_, peer_id)
}

// Drop_channel_ removes *ch* from the lane tables. Caller holds
// Lane_lock_.
func (b *CarrierRPCProtocol_) Drop_channel_(ch *Channel) {
	if lanes := b.Lanes_[ch.Peer_id]; lanes != nil && lanes[ch.Lane_id] == ch {
		delete(lanes, ch.Lane_id)
	}
	if names := b.Lane_names_[ch.Peer_id]; names != nil && names[ch.Name] == ch {
		delete(names, ch.Name)
	}
}

// Open_channel returns the (cached) Channel *name* to *peer_id*, opening
// it if needed.
//
// Idempotent and race-safe: if both sides open the same name at the same
// time they converge on one channel per side. The lane lock is never held
// across the network round trip (see below).
//
// *peer_id* is a peer this carrier currently holds; *name* is the channel
// name (``"default"`` is reserved for RPC).
//
// Raises ConnectionError if the carrier cannot stream, does not hold
// *peer_id*, the peer advertised no lanes and no Static_lanes entry
// exists for *name*, or the open handshake failed; ValueError if *name*
// is ``"default"``.
//
// Over loopback the peer's control handler runs synchronously on *this*
// thread, and on wire carriers it runs on the loop thread that must also
// read our reply -- holding the lock across the RPC would deadlock either
// way. The channel is therefore inserted in a *pending* state, the RPC is
// sent lock-free, and the result is committed under the lock afterwards.
func (b *CarrierRPCProtocol_) Open_channel(peer_id string, name string) (*Channel, error) {
	if name == "default" {
		return nil, py.ValueError.New("'default' is the RPC proxy, not a stream channel.")
	}
	if !b.Supports_channels_() {
		return nil, py.ConnectionError.Newf("%s has no stream lanes.", py.Repr(b.Protocol_name()))
	}
	if !b.proto_().Has_peer(peer_id) {
		return nil, py.ConnectionError.Newf("No connection to peer %s", peer_id)
	}

	opener := false
	var ch *Channel
	var pending bool
	err := func() error {
		b.Lane_lock_.Lock()
		defer b.Lane_lock_.Unlock()
		ch = b.Lane_names_[peer_id][name]
		if ch != nil && ch.Closed() {
			b.Drop_channel_(ch)
			ch = nil
		}
		if ch == nil {
			if static, ok := b.Static_lanes.Get(name); ok && !py.IsNone(static) {
				lane, err := py.Int(static)
				if err != nil {
					return err
				}
				if !(1 <= lane && lane <= MAX_LANE_) {
					return py.ConnectionError.Newf("static_lanes[%s]=%d is outside 1..%d.", py.Repr(name), lane, MAX_LANE_)
				}
				if other := b.Lanes_[peer_id][lane]; other != nil && !other.Closed() {
					return py.ConnectionError.Newf("Lane %d to peer %s is already bound to channel %s.", lane, peer_id, py.Repr(other.Name))
				}
				ch = b.New_channel_(peer_id, name, lane, &lane)
				pending = false
				return nil
			}
			if !b.Peer_has_lanes_locked_(peer_id) {
				return py.ConnectionError.Newf(
					"Peer %s did not advertise stream lanes during the handshake "+
						"(an older laila or an RPC-only firmware). If it emits lane frames on "+
						"a fixed lane, configure static_lanes={%s: <lane>} on this connection.",
					peer_id, py.Repr(name))
			}
			lane, err := b.Alloc_lane_(peer_id, nil)
			if err != nil {
				return err
			}
			ch = b.New_channel_(peer_id, name, lane, nil)
			opener = true
		}
		pending = ch.Tx_lane_id() == nil
		return nil
	}()
	if err != nil {
		return nil, err
	}

	if !pending {
		return ch, nil
	}
	if !opener {
		// someone else's open is in flight; wait for it
		if ch.opened_.Wait(b.Rpc_timeout) && !ch.Closed() {
			return ch, nil
		}
		return nil, py.ConnectionError.Newf("Channel %s to peer %s did not finish opening (%s).",
			py.Repr(name), peer_id, ch.closedReason("timeout"))
	}

	result, err := b.proto_().Send_rpc(peer_id, append([]string(nil), COMM_CHANNEL_OPEN_PATH_...), nil,
		py.Kw{"name": name, "lane": ch.Lane_id, "options": py.NewDict()})
	var tx_lane int
	if err == nil {
		d, ok := result.(*py.Dict)
		if !ok {
			err = py.TypeError.Newf("'%s' object is not subscriptable", py.Pytypename(result))
		} else if lane_v, found := d.Get("lane"); !found {
			err = py.KeyError.New("lane")
		} else if tx_lane, err = py.Int(lane_v); err == nil && !(1 <= tx_lane && tx_lane <= MAX_LANE_) {
			err = py.ValueError.Newf("peer returned lane %d", tx_lane)
		}
	}
	if err != nil {
		raced := func() bool {
			b.Lane_lock_.Lock()
			defer b.Lane_lock_.Unlock()
			if ch.Tx_lane_id() != nil && !ch.Closed() {
				// a remote open for the same name raced us and finished first
				return true
			}
			b.Drop_channel_(ch)
			return false
		}()
		if raced {
			return ch, nil
		}
		ch.Close_local_(fmt.Sprintf("open failed: %s", err.Error()))
		return nil, py.ConnectionError.Newf("Could not open channel %s to peer %s: %s", py.Repr(name), peer_id, err.Error()).From(err)
	}

	b.Lane_lock_.Lock()
	defer b.Lane_lock_.Unlock()
	if ch.Closed() {
		return nil, py.ConnectionError.Newf("Channel %s to peer %s closed while opening (%s).",
			py.Repr(name), peer_id, ch.closedReason("None"))
	}
	if ch.Tx_lane_id() == nil {
		ch.Mark_opened_(tx_lane)
	}
	return ch, nil
}

// Peer_has_lanes_locked_ is Peer_has_lanes_ for callers already holding
// Lane_lock_.
func (b *CarrierRPCProtocol_) Peer_has_lanes_locked_(peer_id string) bool {
	caps := b.Peer_caps_[peer_id]
	if caps == nil {
		return false
	}
	return py.Truthy(caps.GetDefault("lanes", nil))
}

// Bind_static_lanes_ pre-binds every Static_lanes entry for a freshly
// registered peer.
func (b *CarrierRPCProtocol_) Bind_static_lanes_(peer_id string) {
	if b.Static_lanes == nil || b.Static_lanes.Len() == 0 || !b.Supports_channels_() {
		return
	}
	b.Lane_lock_.Lock()
	defer b.Lane_lock_.Unlock()
	for _, it := range b.Static_lanes.Items() {
		name := py.Str(it.Key)
		lane, err := py.Int(it.Value)
		if err != nil || !(1 <= lane && lane <= MAX_LANE_) || name == "default" {
			carrier_log_.Debug("Ignoring invalid static lane %s=%s", py.Repr(name), py.Repr(it.Value))
			continue
		}
		if _, used := b.Lanes_[peer_id][lane]; used {
			continue
		}
		if _, used := b.Lane_names_[peer_id][name]; used {
			continue
		}
		b.New_channel_(peer_id, name, lane, &lane)
	}
}

// Close_peer_lanes_ closes every lane to *peer_id* (waking relays).
// Idempotent.
func (b *CarrierRPCProtocol_) Close_peer_lanes_(peer_id string, reason string) {
	b.Lane_lock_.Lock()
	lanes := b.Lanes_[peer_id]
	delete(b.Lanes_, peer_id)
	names := b.Lane_names_[peer_id]
	delete(b.Lane_names_, peer_id)
	b.Lane_lock_.Unlock()
	seen := map[*Channel]bool{}
	var all []*Channel
	for _, ch := range lanes {
		all = append(all, ch)
	}
	for _, ch := range names {
		all = append(all, ch)
	}
	for _, ch := range all {
		if seen[ch] {
			continue
		}
		seen[ch] = true
		ch.Close_local_(reason)
	}
}

// Shutdown_lanes_ closes every lane on every peer (used by carriers'
// ``stop()``). ``reason`` defaults to ``"protocol stopped"``.
func (b *CarrierRPCProtocol_) Shutdown_lanes_(reason ...string) {
	r := "protocol stopped"
	if len(reason) > 0 {
		r = reason[0]
	}
	b.Lane_lock_.Lock()
	peers := map[string]bool{}
	for p := range b.Lanes_ {
		peers[p] = true
	}
	for p := range b.Lane_names_ {
		peers[p] = true
	}
	b.Lane_lock_.Unlock()
	for peer_id := range peers {
		b.Close_peer_lanes_(peer_id, r)
	}
}

// Is_control_path_ is ``True`` for the reserved stream-lane control
// paths.
func (b *CarrierRPCProtocol_) Is_control_path_(path []string) bool {
	return Str_list_eq_(path, COMM_CHANNEL_OPEN_PATH_) || Str_list_eq_(path, COMM_CHANNEL_CLOSE_PATH_)
}

// Control_response_ answers a stream-lane control request; never touches
// the policy.
func (b *CarrierRPCProtocol_) Control_response_(msg *py.Dict, peer_id any) *py.Dict {
	params := dict_or_empty_(msg.GetDefault("params", nil))
	kwargs := dict_or_empty_(params.GetDefault("kwargs", nil))
	result, err := b.hooks().Handle_control_(Str_list_(params.GetDefault("path", py.NewList())), kwargs, peer_id)
	if err != nil {
		return Make_error(msg.GetDefault("id", nil), ERR_EXECUTION, fmt.Sprintf("%s: %s", py.Typename(err), err.Error()), nil)
	}
	return Make_result(msg.GetDefault("id", nil), result)
}

// Handle_control_ allocates / releases a lane on behalf of the
// authenticated *peer_id*.
//
// ``__comm_channel_open__`` kwargs ``{"name", "lane", "options"}`` ->
// ``{"lane": <our rx lane>}``. ``lane`` is the opener's rx lane (what we
// will write in frames to it); ``options`` is reserved and ignored.
// ``__comm_channel_close__`` kwargs ``{"lane"}`` (our rx lane) -> ``True``.
func (b *CarrierRPCProtocol_) Handle_control_(path []string, kwargs *py.Dict, peer_id any) (any, error) {
	if py.IsNone(peer_id) {
		return nil, py.RuntimeError.New("Stream-lane control requires an authenticated peer.")
	}
	pid := py.Str(peer_id)
	if !b.Supports_channels_() {
		return nil, py.ConnectionError.Newf("%s has no stream lanes.", py.Repr(b.Protocol_name()))
	}
	if Str_list_eq_(path, COMM_CHANNEL_OPEN_PATH_) {
		name_v := kwargs.GetDefault("name", nil)
		name, ok := name_v.(string)
		if !ok || name == "" || name == "default" {
			return nil, py.ValueError.Newf("Invalid channel name %s.", py.Repr(name_v))
		}
		remote_lane, err := py.Int(kwargs.GetDefault("lane", 0))
		if err != nil {
			return nil, err
		}
		if !(1 <= remote_lane && remote_lane <= MAX_LANE_) {
			return nil, py.ValueError.Newf("Invalid lane %d.", remote_lane)
		}
		b.Lane_lock_.Lock()
		defer b.Lane_lock_.Unlock()
		ch := b.Lane_names_[pid][name]
		if ch != nil && ch.Closed() {
			b.Drop_channel_(ch)
			ch = nil
		}
		if ch != nil {
			// Either our own open is in flight (adopt the peer's lane) or
			// the peer re-opened after a lost close.
			if ch.Tx_lane_id() == nil {
				ch.Mark_opened_(remote_lane)
			} else {
				ch.Set_tx_lane_id_(remote_lane)
			}
			return py.DictOf("lane", ch.Lane_id), nil
		}
		lane, err := b.Alloc_lane_(pid, &remote_lane)
		if err != nil {
			return nil, err
		}
		b.New_channel_(pid, name, lane, &remote_lane)
		return py.DictOf("lane", lane), nil
	}
	if Str_list_eq_(path, COMM_CHANNEL_CLOSE_PATH_) {
		lane, err := py.Int(kwargs.GetDefault("lane", 0))
		if err != nil {
			return nil, err
		}
		b.Lane_lock_.Lock()
		ch := b.Lanes_[pid][lane]
		if ch != nil {
			b.Drop_channel_(ch)
		}
		b.Lane_lock_.Unlock()
		if ch != nil {
			ch.Close_local_("closed by peer")
		}
		return true, nil
	}
	return nil, py.ValueError.Newf("Unknown control path %s.", py.Repr(py.NewList(any_strings_(path)...)))
}

// On_channel_closed_locally_ handles a user ``Channel.close()``: unbind
// and tell the peer (best effort).
func (b *CarrierRPCProtocol_) On_channel_closed_locally_(ch *Channel) error {
	b.Lane_lock_.Lock()
	b.Drop_channel_(ch)
	b.Lane_lock_.Unlock()
	tx := ch.Tx_lane_id()
	if tx == nil || !b.proto_().Has_peer(ch.Peer_id) {
		return nil
	}
	if err := b.hooks().Send_oneway_(ch.Peer_id, append([]string(nil), COMM_CHANNEL_CLOSE_PATH_...), py.Kw{"lane": *tx}); err != nil {
		carrier_log_.Debug("channel close notice to %s failed", ch.Peer_id, py.Kw{"exc_info": err})
	}
	return nil
}

// Send_oneway_ sends a control request without waiting for its reply.
//
// Default: fire Send_rpc on a daemon thread and discard the outcome.
// Carriers with a cheap non-blocking write path override.
func (b *CarrierRPCProtocol_) Send_oneway_(peer_id string, path []string, kwargs py.Kw) error {
	go_ := func() {
		_, _ = b.proto_().Send_rpc(peer_id, path, nil, kwargs)
	}
	return py.New_Thread(py.Kw{"target": go_, "daemon": true, "name": "comm-channel-close"}).Start()
}

// Stream_enqueue_ hands one outbound message to the wire. Carriers
// override.
//
// Called on the user's thread by Channel.Send; must not block on I/O
// (marshal onto the carrier loop instead).
func (b *CarrierRPCProtocol_) Stream_enqueue_(channel *Channel, payload []byte) error {
	return py.ConnectionError.Newf("%s cannot carry stream lanes.", py.Repr(b.Protocol_name()))
}

// Lookup_lane_ resolves a receive-side lane id to its channel (nil if
// unknown).
func (b *CarrierRPCProtocol_) Lookup_lane_(peer_id any, lane int) *Channel {
	if py.IsNone(peer_id) {
		return nil
	}
	b.Lane_lock_.Lock()
	defer b.Lane_lock_.Unlock()
	lanes := b.Lanes_[py.Str(peer_id)]
	if lanes == nil {
		return nil
	}
	return lanes[lane]
}

// On_stream_frame_ routes one reserved-marker frame from *peer_id*
// (carrier inbound thread).
//
// Any failure is counted and logged at debug; it never propagates, so a
// malformed stream frame cannot end a receive loop or unregister a peer.
// Unknown lanes and unknown reserved markers are dropped silently.
func (b *CarrierRPCProtocol_) On_stream_frame_(peer_id any, raw []byte) {
	defer func() {
		if r := recover(); r != nil {
			b.Malformed_stream_drops_++
			carrier_log_.Debug("malformed stream frame from %s dropped", py.Str(peer_id), py.Kw{"exc_info": py.PanicToError(r)})
		}
	}()
	if !Is_stream_frame(raw) {
		b.Reserved_marker_drops_++
		marker := -1
		if len(raw) > 0 {
			marker = int(raw[0])
		}
		carrier_log_.Debug("dropping frame with reserved marker 0x%02x", marker)
		return
	}
	lane, seq, flags, err := Unpack_stream_header(raw)
	if err != nil {
		b.Malformed_stream_drops_++
		carrier_log_.Debug("malformed stream frame from %s dropped", py.Str(peer_id), py.Kw{"exc_info": err})
		return
	}
	ch := b.Lookup_lane_(peer_id, lane)
	if ch == nil || ch.Closed() {
		b.Unknown_lane_drops_++
		return
	}
	ch.On_chunk_(seq, flags, raw[STREAM_HEADER_LEN:], py.Monotonic())
}

// On_stream_payload_ delivers an already-complete message to our rx
// *lane* (loopback path).
func (b *CarrierRPCProtocol_) On_stream_payload_(peer_id string, lane int, payload []byte) {
	ch := b.Lookup_lane_(peer_id, lane)
	if ch == nil || ch.Closed() {
		b.Unknown_lane_drops_++
		return
	}
	ch.Enqueue_inbound_(payload, py.Monotonic())
}

// Stream_chunks_ yields the wire frames (header + data) for one outbound
// message.
//
// Slices *payload* into Stream_chunk_bytes pieces with ``START`` on the
// first and ``END`` on the last; ``b""`` yields one empty ``START|END``
// chunk.
func (b *CarrierRPCProtocol_) Stream_chunks_(channel *Channel, payload []byte) [][]byte {
	size := max(1, b.Stream_chunk_bytes)
	n := len(payload)
	count := max(1, (n+size-1)/size)
	first := channel.Next_chunk_seqs_(count)
	lane := 0
	if tx := channel.Tx_lane_id(); tx != nil {
		lane = *tx
	}
	if count == 1 {
		return [][]byte{Pack_stream_frame(lane, first, FLAG_START|FLAG_END, payload)}
	}
	out := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		flags := 0
		if i == 0 {
			flags |= FLAG_START
		}
		if i == count-1 {
			flags |= FLAG_END
		}
		end := min((i+1)*size, n)
		out = append(out, Pack_stream_frame(lane, first+int64(i), flags, payload[i*size:end]))
	}
	return out
}

// Ping round-trips a liveness control frame to *peer_id*.
//
// Reuses the carrier's own Send_rpc path with the reserved
// ``__comm_ping__`` frame, which the peer answers with ``"pong"`` before
// it ever reaches the policy. Returns ``False`` on any transport error.
// The communication liveness loop bounds the call with its own deadline,
// so a silently-dead peer cannot stall it.
func (b *CarrierRPCProtocol_) Ping(peer_id string, timeout ...any) bool {
	self := b.proto_()
	if !self.Has_peer(peer_id) {
		return false
	}
	// Bound the *wait* by the ping deadline (not rpc_timeout): a deaf peer
	// must not pin a liveness worker for 60 s.
	t := b.Ping_timeout
	if len(timeout) > 0 && !py.IsNone(timeout[0]) {
		if f, err := py.Float(timeout[0]); err == nil {
			t = f
		}
	}
	b.Rpc_wait_override_.Set("timeout", t)
	defer b.Rpc_wait_override_.Set("timeout", nil)
	res, err := self.Send_rpc(peer_id, append([]string(nil), COMM_PING_PATH_...), nil, py.Kw{})
	if err != nil {
		return false
	}
	return py.Eq(res, "pong")
}

// ------------------------------------------------------------------
// Outbound correlation
// ------------------------------------------------------------------

// Register_pending_ allocates a request id + pending slot for an outbound
// RPC.
//
// The slot remembers *peer_id* so Fail_pending_ can fail exactly the
// calls in flight to a peer that just went away.
func (b *CarrierRPCProtocol_) Register_pending_(peer_id string) (string, *Pending_rpc_slot_) {
	request_id := py.Uuid4().String()
	slot := New_pending_rpc_slot_(peer_id)
	b.Pending_rpcs_.Set(request_id, slot)
	return request_id, slot
}

// Fail_pending_ fails outstanding outbound RPCs with
// ``ConnectionError(reason)``.
//
// With *peer_id* only the slots registered for that peer are failed (a
// peer dropped: liveness, EOF, ``remove_peer``); without it every slot is
// (transport ``stop()``). Waiters blocked in Await_pending_ -- and
// therefore ``RemoteFuture.wait()`` -- wake immediately instead of sitting
// out Rpc_timeout.
func (b *CarrierRPCProtocol_) Fail_pending_(reason string, peer_id *string) {
	for _, it := range b.Pending_rpcs_.Items() {
		if peer_id != nil && it.Slot.Peer_id != *peer_id {
			continue
		}
		it.Slot.Setdefault_exception(py.ConnectionError.New(reason))
		it.Slot.Event.Set()
		b.Pending_rpcs_.Pop(it.Id)
	}
}

// Fail_request_ fails one pending RPC (carrier-level delivery failure).
func (b *CarrierRPCProtocol_) Fail_request_(request_id string, exc error) {
	slot := b.Pending_rpcs_.Pop(request_id)
	if slot == nil {
		return
	}
	slot.Setdefault_exception(exc)
	slot.Event.Set()
}

// Complete_pending_ resolves the pending slot named by response *msg*'s
// id.
func (b *CarrierRPCProtocol_) Complete_pending_(msg *py.Dict) {
	request_id := msg.GetDefault("id", nil)
	if py.IsNone(request_id) {
		return
	}
	slot := b.Pending_rpcs_.Get(py.Str(request_id))
	if slot == nil {
		return
	}
	if e, found := msg.Get("error"); found {
		slot.Set_error(e)
	} else {
		slot.Set_result(msg.GetDefault("result", nil))
	}
	slot.Event.Set()
}

// Await_pending_ blocks until *slot* is resolved (or Rpc_timeout
// elapses).
//
// Ping shortens the wait for the calling thread only via the thread-local
// ``_rpc_wait_override``.
func (b *CarrierRPCProtocol_) Await_pending_(request_id string, slot *Pending_rpc_slot_) (any, error) {
	timeout := b.Rpc_timeout
	if v, ok := b.Rpc_wait_override_.Get("timeout"); ok && py.Truthy(v) {
		if f, err := py.Float(v); err == nil {
			timeout = f
		}
	}
	completed := slot.Event.Wait(timeout)
	b.Pending_rpcs_.Pop(request_id)
	if !completed {
		return nil, py.TimeoutError.Newf("RPC to peer timed out after %ss on %s.", py.Str(timeout), b.Class_().Name)
	}
	if exc := slot.Exception(); exc != nil {
		return nil, exc
	}
	if e, found := slot.Error_(); found {
		if d, ok := e.(*py.Dict); ok && py.Eq(d.GetDefault("code", nil), ERR_BUSY) {
			return nil, BackpressureError.New(py.Str(d.GetDefault("message", "peer busy")))
		}
		return nil, py.RuntimeError.Newf("Remote RPC error: %s", py.Str(error_message_(e)))
	}
	return slot.Result(), nil
}

// Send_once_ is the carrier-specific single attempt behind Send_rpc.
// Abstract here (Python's base does not define it).
func (b *CarrierRPCProtocol_) Send_once_(peer_id string, path []string, args []any, kwargs py.Kw) (any, error) {
	return nil, py.AttributeError.Newf("'%s' object has no attribute '_send_once'", b.Class_().Name)
}

// Send_rpc sends one ``rpc.call`` to *peer_id*, retrying under
// backpressure.
//
// Wraps the carrier-specific Send_once_ in an exponential backoff +
// jitter retry loop scoped to BackpressureError (an ``ERR_BUSY`` reply).
// Liveness pings never hit this path's retries because the peer answers
// them before admission, so a busy peer still reads as alive. After
// Max_rpc_retries BUSY replies the final BackpressureError propagates so
// the caller learns the peer stayed overwhelmed.
func (b *CarrierRPCProtocol_) Send_rpc(peer_id string, path []string, args []any, kwargs py.Kw) (any, error) {
	attempt := 0
	for {
		res, err := b.hooks().Send_once_(peer_id, path, args, kwargs)
		if err == nil || !py.IsInstance(err, BackpressureError) {
			return res, err
		}
		if attempt >= b.Max_rpc_retries {
			return nil, err
		}
		delay := math.Min(b.Rpc_backoff_max, b.Rpc_backoff_base*math.Pow(2, float64(attempt)))
		time.Sleep(time.Duration((delay + rand.Float64()*delay) * float64(time.Second)))
		attempt++
	}
}

// ------------------------------------------------------------------
// Wire helpers
// ------------------------------------------------------------------

// Encode_ serialises *obj* with this carrier's configured codec.
func (b *CarrierRPCProtocol_) Encode_(obj any) ([]byte, error) { return Codec_encode(obj, b.Codec) }

// Decode_ deserialises *data* with this carrier's configured codec.
func (b *CarrierRPCProtocol_) Decode_(data []byte) (any, error) { return Codec_decode(data, b.Codec) }

// Decode_msg_ is Decode_ plus the ``dict`` view the dispatchers rely on.
func (b *CarrierRPCProtocol_) Decode_msg_(data []byte) (*py.Dict, error) {
	v, err := b.Decode_(data)
	if err != nil {
		return nil, err
	}
	d, ok := v.(*py.Dict)
	if !ok {
		return nil, py.AttributeError.Newf("'%s' object has no attribute 'get'", py.Pytypename(v))
	}
	return d, nil
}

// Make_rpc_request_ builds the standard ``rpc.call`` request envelope.
func (b *CarrierRPCProtocol_) Make_rpc_request_(path []string, args []any, kwargs py.Kw, request_id string) *py.Dict {
	return Make_request(
		"rpc.call",
		py.DictOf(
			"path", py.NewList(any_strings_(path)...),
			"args", py.NewList(args...),
			"kwargs", py.DictFromKw(kwargs),
		),
		request_id,
	)
}

// Shutdown_executor_ tears down the inbound worker pool (called from
// ``stop()``).
func (b *CarrierRPCProtocol_) Shutdown_executor_() {
	if b.Inbound_executor_ != nil {
		b.Inbound_executor_.Shutdown(false, py.Kw{"cancel_futures": true})
		b.Inbound_executor_ = nil
	}
}

// Comm_drivers_ is the registry of host-side driver objects a concrete
// transport may require (``importlib.import_module(name)`` in Python).
// Register one with Register_comm_driver; a transport whose driver is
// absent raises the same actionable RuntimeError as the Python port.
var (
	comm_drivers_mu_ sync.Mutex
	Comm_drivers_    = map[string]any{}
)

// Register_comm_driver makes *driver* importable as *name* for
// Require_drivers_.
func Register_comm_driver(name string, driver any) {
	comm_drivers_mu_.Lock()
	Comm_drivers_[name] = driver
	comm_drivers_mu_.Unlock()
}

// Require_drivers_ imports each name in *modules*, raising a clear
// capability error.
//
// Concrete transports that wrap a third-party driver call this at the
// top of their connection hook. When a driver is missing the raised
// RuntimeError names both the package and the ``pip install
// laila-core[<extra>]`` that provides it -- never a bare ImportError and
// never a silent stub.
func (b *CarrierRPCProtocol_) Require_drivers_(modules []string, extra string) (map[string]any, error) {
	loaded := map[string]any{}
	for _, name := range modules {
		comm_drivers_mu_.Lock()
		drv, ok := Comm_drivers_[name]
		comm_drivers_mu_.Unlock()
		if !ok {
			return nil, py.RuntimeError.Newf(
				"The %s transport requires %s, which is not installed. Install it with `pip install laila-core[%s]`.",
				py.Repr(b.Protocol_name()), py.Repr(name), extra).From(py.ImportError.Newf("No module named %s", py.Repr(name)))
		}
		loaded[name] = drv
	}
	return loaded, nil
}

// sort_strings_ sorts in place (lane name listings are sets in Python;
// a stable order keeps the Go port deterministic).
func sort_strings_(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
