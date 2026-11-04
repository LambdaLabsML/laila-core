// Port of tests/functional/policy/communication/streaming/unit_tests/_helpers.py.
//
// Shared, hardware-free fixtures for the stream-lane tests.
//
// Everything here runs in-process on a developer machine or CI runner:
//
//   - TwoPolicies -- two ``DefaultPolicy`` instances peered over the
//     loopback transport, raw TCP on 127.0.0.1, or UDP on 127.0.0.1.
//   - UartPtyPair -- ``os.openpty()``; laila's ``DefaultUARTProtocol``
//     opens the *slave* as its serial port, the test drives the *master*
//     fd. A ``throttle_bytes_per_sec`` knob emulates 115200-1000000 baud
//     (a pty itself ignores baud).
//   - FakeRpcOnlyPeer -- an asyncio peer on the pty master that speaks
//     only the *current* wire format (4-byte BE length prefix + JSON-RPC
//     2.0): answers ``peer.connect`` **without** ``caps``,
//     ``__comm_ping__`` with ``"pong"``, ``central.memory._remote_remember``
//     with a canned serialized Entry, and returns a JSON-RPC error for any
//     unknown method. It never emits ``0x01`` frames. Stands in for older
//     laila peers and MCU firmware.
//   - FakeLanePeer -- the above plus the ability to *emit*
//     ``[0x01][lane][seq][flags][data]`` frames on a static lane without
//     ever answering ``__comm_channel_open__``; supports chunking and
//     random chunk loss.
//
// This file is a regular (non-test) Go file so the sibling communication
// test package can import it the way ``test_graceful_teardown.py`` does
// via ``sys.path``.
package unit_tests

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// Make_policy is ``make_policy(proto)``: ``DefaultPolicy`` with *proto*
// registered and started.
func Make_policy(proto laila.Comm_protocol_) (*laila.LAILA_IDENTIFIABLE_POLICY_, error) {
	p, err := laila.New_LAILA_IDENTIFIABLE_POLICY_()
	if err != nil {
		return nil, err
	}
	if err := Comm(p).Add_connection(proto); err != nil {
		return nil, err
	}
	return p, nil
}

// Comm is ``policy.central.communication``.
func Comm(p *laila.LAILA_IDENTIFIABLE_POLICY_) *laila.LAILA_IDENTIFIABLE_COMMUNICATION_ {
	return laila.As_communication_(p.Central.Communication)
}

// Wait_until is ``wait_until(pred, timeout=5.0, step=0.01)``: poll *pred*
// until true or *timeout*; return the final value.
func Wait_until(pred func() bool, timeout_step ...float64) bool {
	timeout, step := 5.0, 0.01
	if len(timeout_step) > 0 {
		timeout = timeout_step[0]
	}
	if len(timeout_step) > 1 {
		step = timeout_step[1]
	}
	deadline := time.Now().Add(time.Duration(timeout * float64(time.Second)))
	for time.Now().Before(deadline) {
		if pred() {
			return true
		}
		time.Sleep(time.Duration(step * float64(time.Second)))
	}
	return pred()
}

// ----------------------------------------------------------------------
// TwoPolicies
// ----------------------------------------------------------------------

// TwoPolicies is two in-process policies peered over one transport kind.
//
//   - Kind: {"loopback", "tcp", "udp"}
//   - Proto_kwargs: extra pydantic fields for both protocol instances
//     (e.g. ``channel_queue_size=8``, ``codec="msgpack"``).
//   - Liveness_interval: nil keeps the hub default.
type TwoPolicies struct {
	Kind              string
	Proto_kwargs      py.Kw
	Liveness_interval *float64
	A, B              *laila.LAILA_IDENTIFIABLE_POLICY_
	Pa, Pb            laila.Comm_protocol_
	Aid, Bid          string
}

// New_TwoPolicies is ``TwoPolicies(kind, *, liveness_interval=None, **proto_kwargs)``.
func New_TwoPolicies(kind string, kw ...py.Kw) *TwoPolicies {
	opts := py.MergeKw(kw...)
	t := &TwoPolicies{Kind: kind, Proto_kwargs: py.Kw{}}
	if v, ok := opts["liveness_interval"]; ok && !py.IsNone(v) {
		f, _ := py.Float(v)
		t.Liveness_interval = &f
	}
	for k, v := range opts {
		if k != "liveness_interval" {
			t.Proto_kwargs[k] = v
		}
	}
	return t
}

func (t *TwoPolicies) factory_() (func(kw ...py.Kw) (laila.Comm_protocol_, error), error) {
	switch t.Kind {
	case "loopback":
		return func(kw ...py.Kw) (laila.Comm_protocol_, error) { return laila.New_DefaultLoopbackProtocol(kw...) }, nil
	case "tcp":
		return func(kw ...py.Kw) (laila.Comm_protocol_, error) { return laila.New_DefaultTCPProtocol(kw...) }, nil
	case "udp":
		return func(kw ...py.Kw) (laila.Comm_protocol_, error) { return laila.New_DefaultUDPProtocol(kw...) }, nil
	}
	return nil, py.KeyError.New(t.Kind)
}

func (t *TwoPolicies) uri_() (string, error) {
	switch t.Kind {
	case "loopback":
		return "loopback://" + t.B.Global_id(), nil
	case "tcp":
		return fmt.Sprintf("tcp://127.0.0.1:%d", Bound_port(t.Pb)), nil
	case "udp":
		return fmt.Sprintf("udp://127.0.0.1:%d", Bound_port(t.Pb)), nil
	}
	return "", py.ValueError.New(t.Kind)
}

// Bound_port is ``proto.bound_port`` for any IP transport.
func Bound_port(proto any) int {
	return proto.(interface{ Bound_port() int }).Bound_port()
}

// Secret_of is ``proto.peer_secret_key``.
func Secret_of(proto any) string { return laila.As_carrier_(proto).Peer_secret_key }

// Enter is ``__enter__``.
func (t *TwoPolicies) Enter() (*TwoPolicies, error) {
	factory, err := t.factory_()
	if err != nil {
		return nil, err
	}
	kw := t.Proto_kwargs.Copy()
	if t.Kind == "tcp" || t.Kind == "udp" {
		if _, ok := kw["host"]; !ok {
			kw["host"] = "127.0.0.1"
		}
	}
	if t.Pa, err = factory(kw); err != nil {
		return nil, err
	}
	if t.Pb, err = factory(kw); err != nil {
		return nil, err
	}
	if t.A, err = laila.New_LAILA_IDENTIFIABLE_POLICY_(); err != nil {
		return nil, err
	}
	if t.B, err = laila.New_LAILA_IDENTIFIABLE_POLICY_(); err != nil {
		return nil, err
	}
	if t.Liveness_interval != nil {
		Comm(t.A).Liveness_interval = *t.Liveness_interval
		Comm(t.B).Liveness_interval = *t.Liveness_interval
	}
	if err := Comm(t.A).Add_connection(t.Pa); err != nil {
		return nil, err
	}
	if err := Comm(t.B).Add_connection(t.Pb); err != nil {
		return nil, err
	}
	t.Aid = t.A.Global_id()
	uri, err := t.uri_()
	if err != nil {
		return nil, err
	}
	if t.Bid, err = Comm(t.A).Add_peer(uri, Secret_of(t.Pb)); err != nil {
		return nil, err
	}
	if t.Bid != t.B.Global_id() {
		return nil, py.AssertionError.Newf("%s != %s", t.Bid, t.B.Global_id())
	}
	if !Wait_until(func() bool { return Comm(t.B).Peers.Contains(t.Aid) }) {
		return nil, py.RuntimeError.New("peer B never registered A")
	}
	return t, nil
}

// Exit is ``__exit__``.
func (t *TwoPolicies) Exit() {
	for _, pol := range []*laila.LAILA_IDENTIFIABLE_POLICY_{t.A, t.B} {
		if pol != nil {
			_ = Comm(pol).Stop()
		}
	}
}

// Ca is ``self.ca``.
func (t *TwoPolicies) Ca() *laila.LAILA_IDENTIFIABLE_COMMUNICATION_ { return Comm(t.A) }

// Cb is ``self.cb``.
func (t *TwoPolicies) Cb() *laila.LAILA_IDENTIFIABLE_COMMUNICATION_ { return Comm(t.B) }

// ----------------------------------------------------------------------
// pty + fake peers
// ----------------------------------------------------------------------

// Throttle_ is ``_Throttle``: token-bucket pacing for a byte rate (0 =
// unlimited).
type Throttle_ struct {
	Rate float64
	next float64
	mu   sync.Mutex
}

func New_Throttle_(bytes_per_sec float64) *Throttle_ {
	return &Throttle_{Rate: bytes_per_sec, next: py.Monotonic()}
}

// Pace is ``await throttle.pace(nbytes)``.
func (t *Throttle_) Pace(nbytes int) error {
	if t.Rate == 0 {
		return nil
	}
	t.mu.Lock()
	now := py.Monotonic()
	if t.next < now {
		t.next = now
	}
	t.next += float64(nbytes) / t.Rate
	delay := t.next - now
	t.mu.Unlock()
	if delay > 0 {
		return py.Sleep(delay)
	}
	return nil
}

// noCloseFile_ wraps an *os.File so a StreamWriter close does not close
// the underlying descriptor (the fixture owns the fd lifetime).
type noCloseFile_ struct{ f *os.File }

func (n noCloseFile_) Write(p []byte) (int, error) { return n.f.Write(p) }
func (n noCloseFile_) Close() error               { return nil }

// FakeRpcOnlyPeer is an RPC-only peer driving a file descriptor (pty
// master).
//
//   - Fd: master side of the pty.
//   - Gid: ``global_id`` it reports in the ``peer.connect`` reply.
//   - Throttle: pace both directions to emulate a slow serial link.
//   - Remember_blob: canned ``Entry.serialize(transformation_base64)``
//     payload returned for ``central.memory._remote_remember``.
//   - Codec_name: {"json", "msgpack"}.
//   - Read_chunk: read granularity on the inbound side (smaller = finer
//     throttle).
type FakeRpcOnlyPeer struct {
	Fd            *os.File
	Gid           string
	Throttle      *Throttle_
	Remember_blob any
	Codec_name    string
	Read_chunk    int
	Loop          *py.Loop

	thread_ *py.Thread
	stop_   *py.Event
	ready_  *py.Event
	reader_ *py.StreamReader
	writer_ *py.StreamWriter
	wlock_  *py.AsyncLock

	// observability
	mu                 sync.Mutex
	Handshakes         int
	Handshake_params   []*py.Dict
	Pings_answered     int
	Rpc_paths          [][]string
	Stream_frames_seen int
	Stream_bytes_seen  int
	Responses_seen     []*py.Dict
	Errors             []string

	// On_request_ lets a subclass (FakeLanePeer) extend request handling;
	// nil uses the base behaviour.
	on_frame_hook_ func(raw []byte) (handled bool, err error)
}

// New_FakeRpcOnlyPeer is ``FakeRpcOnlyPeer(fd, *, gid=None,
// throttle_bytes_per_sec=None, remember_blob=None, codec_name="json",
// read_chunk=4096)``.
func New_FakeRpcOnlyPeer(fd *os.File, kw ...py.Kw) *FakeRpcOnlyPeer {
	opts := py.MergeKw(kw...)
	p := &FakeRpcOnlyPeer{
		Fd:         fd,
		Codec_name: "json",
		Read_chunk: 4096,
		stop_:      py.NewEvent(),
		ready_:     py.NewEvent(),
	}
	if v, ok := opts["gid"]; ok && !py.IsNone(v) {
		p.Gid = py.Str(v)
	} else {
		p.Gid = "LAILA:POLICY:" + py.Uuid4().String()
	}
	rate := 0.0
	if v, ok := opts["throttle_bytes_per_sec"]; ok && !py.IsNone(v) {
		rate, _ = py.Float(v)
	}
	p.Throttle = New_Throttle_(rate)
	if v, ok := opts["remember_blob"]; ok {
		p.Remember_blob = v
	}
	if v, ok := opts["codec_name"]; ok && !py.IsNone(v) {
		p.Codec_name = py.Str(v)
	}
	if v, ok := opts["read_chunk"]; ok && !py.IsNone(v) {
		p.Read_chunk, _ = py.Int(v)
	}
	return p
}

// -- lifecycle --------------------------------------------------------

// Start boots the peer's loop thread and waits until it is reading.
func (p *FakeRpcOnlyPeer) Start() (*FakeRpcOnlyPeer, error) {
	p.thread_ = py.New_Thread(py.Kw{"target": p.run_, "daemon": true, "name": "fake-peer"})
	if err := p.thread_.Start(); err != nil {
		return nil, err
	}
	if !p.ready_.Wait(5.0) {
		return nil, py.RuntimeError.New("fake peer did not start")
	}
	return p, nil
}

// Stop cancels the read loop and joins the thread.
func (p *FakeRpcOnlyPeer) Stop() {
	p.stop_.Set()
	if p.Loop != nil {
		_, _ = p.Loop.Call_soon_threadsafe(p.shutdown_cb_)
	}
	if p.thread_ != nil {
		_ = p.thread_.Join(5.0)
	}
}

func (p *FakeRpcOnlyPeer) shutdown_cb_() {
	for _, t := range p.Loop.All_tasks() {
		t.Cancel()
	}
}

func (p *FakeRpcOnlyPeer) run_() {
	p.Loop = py.New_event_loop()
	py.Set_event_loop(p.Loop)
	defer func() {
		if p.writer_ != nil {
			p.writer_.Close()
		}
		_ = p.Loop.Close()
	}()
	if _, err := p.Loop.Run_until_complete(p.main_); err != nil && !py.IsInstance(err, py.CancelledError) {
		p.mu.Lock()
		p.Errors = append(p.Errors, fmt.Sprintf("loop: %s", py.Repr(err)))
		p.mu.Unlock()
	}
}

func (p *FakeRpcOnlyPeer) main_() (any, error) {
	// ``os.dup`` + ``connect_read_pipe`` / ``connect_write_pipe``: the
	// reader goroutine feeds the StreamReader, the writer wraps the fd
	// without owning it.
	p.reader_, p.writer_ = py.Stream_pair(p.Fd, noCloseFile_{p.Fd}, nil, p.Read_chunk*2) // StreamReader(limit=read_chunk * 2)
	p.wlock_ = py.New_AsyncLock()
	p.ready_.Set()
	err := p.read_loop_()
	if err != nil && py.IsInstance(err, py.CancelledError) {
		return nil, nil
	}
	return nil, err
}

// -- inbound -----------------------------------------------------------

// read_exact_ is a throttled ``readexactly``: read in small pieces,
// pacing each.
func (p *FakeRpcOnlyPeer) read_exact_(n int) ([]byte, error) {
	buf := make([]byte, 0, n)
	for len(buf) < n {
		want := p.Read_chunk
		if rem := n - len(buf); rem < want {
			want = rem
		}
		piece, err := p.reader_.Readexactly(want)
		if err != nil {
			return nil, err
		}
		if err := p.Throttle.Pace(len(piece)); err != nil {
			return nil, err
		}
		buf = append(buf, piece...)
	}
	return buf, nil
}

func (p *FakeRpcOnlyPeer) read_loop_() error {
	for !p.stop_.Is_set() {
		header, err := p.read_exact_(4)
		if err != nil {
			if py.IsInstance(err, py.IncompleteReadError) {
				return nil
			}
			return err
		}
		length := int(binary.BigEndian.Uint32(header))
		raw := []byte{}
		if length > 0 {
			if raw, err = p.read_exact_(length); err != nil {
				if py.IsInstance(err, py.IncompleteReadError) {
					return nil
				}
				return err
			}
		}
		if err := p.on_frame_(raw); err != nil {
			return err
		}
	}
	return nil
}

func (p *FakeRpcOnlyPeer) on_frame_(raw []byte) error {
	if len(raw) > 0 && int(raw[0]) <= laila.RESERVED_MARKER_MAX {
		p.mu.Lock()
		p.Stream_frames_seen++
		p.Stream_bytes_seen += len(raw)
		p.mu.Unlock()
		return nil
	}
	msg, err := laila.Codec_decode(raw, p.Codec_name)
	if err != nil {
		p.mu.Lock()
		p.Errors = append(p.Errors, fmt.Sprintf("decode: %s", py.Repr(err)))
		p.mu.Unlock()
		return nil
	}
	d, _ := msg.(*py.Dict)
	if d == nil {
		d = py.NewDict()
	}
	if d.Contains("method") {
		return p.on_request_(d)
	}
	p.mu.Lock()
	p.Responses_seen = append(p.Responses_seen, d)
	p.mu.Unlock()
	return nil
}

func (p *FakeRpcOnlyPeer) on_request_(msg *py.Dict) error {
	rid := msg.GetDefault("id", nil)
	method := py.Str(msg.GetDefault("method", nil))
	params, _ := msg.GetDefault("params", nil).(*py.Dict)
	if params == nil {
		params = py.NewDict()
	}
	if method == "peer.connect" {
		p.mu.Lock()
		p.Handshakes++
		p.Handshake_params = append(p.Handshake_params, params)
		p.mu.Unlock()
		// Older peers: no "caps" key in the result.
		return p.Send_obj(py.DictOf("jsonrpc", "2.0", "id", rid, "result", py.DictOf("peer_id", p.Gid)))
	}
	if method != "rpc.call" {
		return p.Send_obj(Error_(rid, -32601, "Unknown method: "+method))
	}
	path := []string{}
	for _, seg := range py.Iter(params.GetDefault("path", py.NewList())) {
		path = append(path, py.Str(seg))
	}
	p.mu.Lock()
	p.Rpc_paths = append(p.Rpc_paths, append([]string(nil), path...))
	p.mu.Unlock()
	if len(path) == 1 && path[0] == "__comm_ping__" {
		p.mu.Lock()
		p.Pings_answered++
		p.mu.Unlock()
		return p.Send_obj(py.DictOf("jsonrpc", "2.0", "id", rid, "result", "pong"))
	}
	if strings.Join(path, ".") == "central.memory._remote_remember" && p.Remember_blob != nil {
		return p.Send_obj(py.DictOf("jsonrpc", "2.0", "id", rid, "result", py.NewList(p.Remember_blob)))
	}
	return p.Send_obj(Error_(rid, -32601, "Unknown method: "+strings.Join(path, ".")))
}

// Error_ is ``_error(rid, code, message)``.
func Error_(rid any, code int, message string) *py.Dict {
	return py.DictOf("jsonrpc", "2.0", "id", rid, "error", py.DictOf("code", code, "message", message))
}

// -- observability accessors (thread-safe snapshots) ----------------------

func (p *FakeRpcOnlyPeer) Handshakes_() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Handshakes
}

func (p *FakeRpcOnlyPeer) Pings_answered_() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Pings_answered
}

func (p *FakeRpcOnlyPeer) Rpc_paths_() [][]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([][]string(nil), p.Rpc_paths...)
}

func (p *FakeRpcOnlyPeer) Responses_seen_() []*py.Dict {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*py.Dict(nil), p.Responses_seen...)
}

func (p *FakeRpcOnlyPeer) Handshake_params_() []*py.Dict {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*py.Dict(nil), p.Handshake_params...)
}

func (p *FakeRpcOnlyPeer) Stream_frames_seen_() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Stream_frames_seen
}

func (p *FakeRpcOnlyPeer) Stream_bytes_seen_() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Stream_bytes_seen
}

func (p *FakeRpcOnlyPeer) Errors_() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.Errors...)
}

// -- outbound ----------------------------------------------------------

func (p *FakeRpcOnlyPeer) write_raw_(data []byte) error {
	if err := p.wlock_.Acquire(); err != nil {
		return err
	}
	defer p.wlock_.Release()
	// pace in read_chunk pieces so RPC replies interleave realistically
	for i := 0; i < len(data); i += p.Read_chunk {
		end := i + p.Read_chunk
		if end > len(data) {
			end = len(data)
		}
		piece := data[i:end]
		p.writer_.Write(piece)
		if err := p.writer_.Drain(); err != nil {
			return err
		}
		if err := p.Throttle.Pace(len(piece)); err != nil {
			return err
		}
	}
	return nil
}

// Send_obj is ``await send_obj(obj)`` (loop thread).
func (p *FakeRpcOnlyPeer) Send_obj(obj *py.Dict) error {
	enc, err := laila.Codec_encode(obj, p.Codec_name)
	if err != nil {
		return err
	}
	return p.write_raw_(laila.Frame(enc))
}

// Send_obj_threadsafe is ``send_obj_threadsafe(obj)``.
func (p *FakeRpcOnlyPeer) Send_obj_threadsafe(obj *py.Dict) error {
	_, err := py.Run_coroutine_threadsafe(func() (any, error) { return nil, p.Send_obj(obj) }, p.Loop).Result(10)
	return err
}

// Send_raw_frame_threadsafe writes one length-prefixed frame with an
// arbitrary payload.
func (p *FakeRpcOnlyPeer) Send_raw_frame_threadsafe(payload []byte) error {
	_, err := py.Run_coroutine_threadsafe(func() (any, error) { return nil, p.write_raw_(laila.Frame(payload)) }, p.Loop).Result(30)
	return err
}

// ----------------------------------------------------------------------
// FakeLanePeer
// ----------------------------------------------------------------------

// FakeLanePeer is a FakeRpcOnlyPeer that can emit lane frames on a static
// lane.
//
// Never answers ``__comm_channel_open__`` (returns the generic JSON-RPC
// error like any unknown method), so laila must use ``static_lanes``.
//
//   - Lane: static lane id.
//   - Chunk_size: bytes per emitted chunk.
//   - Drop_probability: per-chunk probability of *not* writing the chunk
//     (its ``seq`` is still consumed, so the receiver sees a gap).
//   - Seed: seed for the drop RNG.
type FakeLanePeer struct {
	*FakeRpcOnlyPeer
	Lane               int
	Chunk_size         int
	Drop_probability   float64
	Rng                *rand.Rand
	Seq                int64
	Chunks_dropped     int
	Messages_emitted   int
	Messages_with_drop int
}

// New_FakeLanePeer is ``FakeLanePeer(fd, *, lane=1, chunk_size=4096,
// drop_probability=0.0, seed=7, **kw)``.
func New_FakeLanePeer(fd *os.File, kw ...py.Kw) *FakeLanePeer {
	opts := py.MergeKw(kw...)
	base := py.Kw{}
	for k, v := range opts {
		switch k {
		case "lane", "chunk_size", "drop_probability", "seed":
		default:
			base[k] = v
		}
	}
	p := &FakeLanePeer{FakeRpcOnlyPeer: New_FakeRpcOnlyPeer(fd, base), Lane: 1, Chunk_size: 4096}
	if v, ok := opts["lane"]; ok {
		p.Lane, _ = py.Int(v)
	}
	if v, ok := opts["chunk_size"]; ok {
		p.Chunk_size, _ = py.Int(v)
	}
	if v, ok := opts["drop_probability"]; ok {
		p.Drop_probability, _ = py.Float(v)
	}
	seed := int64(7)
	if v, ok := opts["seed"]; ok {
		s, _ := py.Int(v)
		seed = int64(s)
	}
	p.Rng = rand.New(rand.NewSource(seed))
	return p
}

// Start boots the peer (returns the lane peer for chaining).
func (p *FakeLanePeer) Start() (*FakeLanePeer, error) {
	if _, err := p.FakeRpcOnlyPeer.Start(); err != nil {
		return nil, err
	}
	return p, nil
}

// Frames_for chunks *payload* into lane frames; nil marks a dropped chunk.
func (p *FakeLanePeer) Frames_for(payload []byte) [][]byte {
	size := p.Chunk_size
	count := (len(payload) + size - 1) / size
	if count < 1 {
		count = 1
	}
	out := [][]byte{}
	had_drop := false
	for i := 0; i < count; i++ {
		flags := 0
		if i == 0 {
			flags |= laila.FLAG_START
		}
		if i == count-1 {
			flags |= laila.FLAG_END
		}
		lo, hi := i*size, (i+1)*size
		if lo > len(payload) {
			lo = len(payload)
		}
		if hi > len(payload) {
			hi = len(payload)
		}
		frame := laila.Pack_stream_frame(p.Lane, p.Seq, flags, payload[lo:hi])
		p.Seq = (p.Seq + 1) % laila.SEQ_MODULUS
		if p.Drop_probability > 0 && p.Rng.Float64() < p.Drop_probability {
			p.Chunks_dropped++
			had_drop = true
			out = append(out, nil)
		} else {
			out = append(out, frame)
		}
	}
	p.Messages_emitted++
	if had_drop {
		p.Messages_with_drop++
	}
	return out
}

// Emit emits one message; returns false if any chunk was dropped.
func (p *FakeLanePeer) Emit(payload []byte) (bool, error) {
	frames := p.Frames_for(payload)
	all := true
	for _, f := range frames {
		if f != nil {
			if err := p.write_raw_(laila.Frame(f)); err != nil {
				return false, err
			}
		} else {
			all = false
		}
	}
	return all, nil
}

// Emit_threadsafe is ``emit_threadsafe(payload, timeout=120.0)``.
func (p *FakeLanePeer) Emit_threadsafe(payload []byte, timeout ...float64) (bool, error) {
	t := 120.0
	if len(timeout) > 0 {
		t = timeout[0]
	}
	v, err := py.Run_coroutine_threadsafe(func() (any, error) { return p.Emit(payload) }, p.Loop).Result(t)
	if err != nil {
		return false, err
	}
	return py.Truthy(v), nil
}

// ----------------------------------------------------------------------
// UartNullModem
// ----------------------------------------------------------------------

// UartNullModem is two laila policies on two ptys bridged master-to-master
// (a null-modem).
//
// A copier goroutine moves bytes ``master1 -> master2`` and ``master2 ->
// master1``; an optional byte-rate throttle emulates a slow serial link.
// Policy A initiates the handshake over its port.
type UartNullModem struct {
	Throttle          float64
	Liveness_interval *float64
	Uart_kwargs       py.Kw
	M1, S1, M2, S2    *os.File
	A, B              *laila.LAILA_IDENTIFIABLE_POLICY_
	Pa, Pb            *laila.LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_
	Aid, Bid          string
	bytes_copied_     int64
	stop_             *py.Event
	wg_               sync.WaitGroup
	mu                sync.Mutex
}

// New_UartNullModem is ``UartNullModem(*, throttle_bytes_per_sec=None,
// liveness_interval=None, **uart_kwargs)``.
func New_UartNullModem(kw ...py.Kw) *UartNullModem {
	opts := py.MergeKw(kw...)
	m := &UartNullModem{Uart_kwargs: py.Kw{}, stop_: py.NewEvent()}
	for k, v := range opts {
		switch k {
		case "throttle_bytes_per_sec":
			if !py.IsNone(v) {
				m.Throttle, _ = py.Float(v)
			}
		case "liveness_interval":
			if !py.IsNone(v) {
				f, _ := py.Float(v)
				m.Liveness_interval = &f
			}
		default:
			m.Uart_kwargs[k] = v
		}
	}
	return m
}

// Bytes_copied is the bridge byte counter.
func (m *UartNullModem) Bytes_copied() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.bytes_copied_
}

// copy_ is one direction of ``_bridge``.
func (m *UartNullModem) copy_(src, dst *os.File) {
	defer m.wg_.Done()
	buf := make([]byte, 4096)
	budget_t := py.Monotonic()
	for !m.stop_.Is_set() {
		n, err := src.Read(buf)
		if err != nil {
			return
		}
		if n == 0 {
			return
		}
		data := buf[:n]
		if m.Throttle > 0 {
			now := py.Monotonic()
			if budget_t < now {
				budget_t = now
			}
			budget_t += float64(n) / m.Throttle
			if delay := budget_t - now; delay > 0 {
				time.Sleep(time.Duration(delay * float64(time.Second)))
			}
		}
		for len(data) > 0 && !m.stop_.Is_set() {
			w, err := dst.Write(data)
			if err != nil {
				return
			}
			data = data[w:]
			m.mu.Lock()
			m.bytes_copied_ += int64(w)
			m.mu.Unlock()
		}
	}
}

// Enter is ``__enter__``.
func (m *UartNullModem) Enter() (*UartNullModem, error) {
	var err error
	if m.M1, m.S1, err = py.Openpty(); err != nil {
		return nil, err
	}
	if m.M2, m.S2, err = py.Openpty(); err != nil {
		return nil, err
	}
	m.wg_.Add(2)
	go m.copy_(m.M1, m.M2)
	go m.copy_(m.M2, m.M1)
	kwa := m.Uart_kwargs.Copy()
	kwa["port"] = py.Ttyname(m.S1)
	kwb := m.Uart_kwargs.Copy()
	kwb["port"] = py.Ttyname(m.S2)
	if m.Pa, err = laila.New_DefaultUARTProtocol(kwa); err != nil {
		return nil, err
	}
	if m.Pb, err = laila.New_DefaultUARTProtocol(kwb); err != nil {
		return nil, err
	}
	if m.A, err = laila.New_LAILA_IDENTIFIABLE_POLICY_(); err != nil {
		return nil, err
	}
	if m.B, err = laila.New_LAILA_IDENTIFIABLE_POLICY_(); err != nil {
		return nil, err
	}
	if m.Liveness_interval != nil {
		Comm(m.A).Liveness_interval = *m.Liveness_interval
		Comm(m.B).Liveness_interval = *m.Liveness_interval
	}
	if err := Comm(m.A).Add_connection(m.Pa); err != nil {
		return nil, err
	}
	if err := Comm(m.B).Add_connection(m.Pb); err != nil {
		return nil, err
	}
	m.Aid = m.A.Global_id()
	if m.Bid, err = Comm(m.A).Add_peer("serial://"+py.Ttyname(m.S1), m.Pb.Peer_secret_key); err != nil {
		return nil, err
	}
	if m.Bid != m.B.Global_id() {
		return nil, py.AssertionError.Newf("%s != %s", m.Bid, m.B.Global_id())
	}
	if !Wait_until(func() bool { return Comm(m.B).Peers.Contains(m.Aid) }) {
		return nil, py.RuntimeError.New("peer B never registered A over the null-modem")
	}
	return m, nil
}

// Exit is ``__exit__``.
func (m *UartNullModem) Exit() {
	for _, pol := range []*laila.LAILA_IDENTIFIABLE_POLICY_{m.A, m.B} {
		if pol != nil {
			_ = Comm(pol).Stop()
		}
	}
	m.stop_.Set()
	for _, f := range []*os.File{m.M1, m.S1, m.M2, m.S2} {
		if f != nil {
			_ = f.Close()
		}
	}
	m.wg_.Wait()
}

// Ca is ``self.ca``.
func (m *UartNullModem) Ca() *laila.LAILA_IDENTIFIABLE_COMMUNICATION_ { return Comm(m.A) }

// Cb is ``self.cb``.
func (m *UartNullModem) Cb() *laila.LAILA_IDENTIFIABLE_COMMUNICATION_ { return Comm(m.B) }

// ----------------------------------------------------------------------
// UartPtyPair
// ----------------------------------------------------------------------

// Fake_peer_ is the common surface of FakeRpcOnlyPeer / FakeLanePeer as
// seen by UartPtyPair.
type Fake_peer_ interface {
	Stop()
	Base() *FakeRpcOnlyPeer
}

// Base returns the RPC-only core.
func (p *FakeRpcOnlyPeer) Base() *FakeRpcOnlyPeer { return p }

// Peer_factory_ builds and starts a fake peer on *fd*.
type Peer_factory_ func(fd *os.File, kw py.Kw) (Fake_peer_, error)

// Rpc_only_peer_ is the ``FakeRpcOnlyPeer`` factory (the default
// ``peer_cls``).
func Rpc_only_peer_(fd *os.File, kw py.Kw) (Fake_peer_, error) {
	return New_FakeRpcOnlyPeer(fd, kw).Start()
}

// Lane_peer_ is the ``FakeLanePeer`` factory.
func Lane_peer_(fd *os.File, kw py.Kw) (Fake_peer_, error) {
	return New_FakeLanePeer(fd, kw).Start()
}

// UartPtyPair is a laila UART protocol on a pty slave + a fake peer on
// the master.
//
//   - Peer_cls: Rpc_only_peer_ or Lane_peer_.
//   - Throttle: link emulation applied by the fake peer in both directions.
//   - Uart_kwargs: extra fields for ``DefaultUARTProtocol``
//     (``static_lanes``, ``channel_queue_size``, ``codec`` ...).
//   - Peer_kwargs: extra kwargs for the fake peer.
type UartPtyPair struct {
	Peer_cls          Peer_factory_
	Throttle          float64
	Uart_kwargs       py.Kw
	Peer_kwargs       py.Kw
	Liveness_interval *float64
	Master, Slave     *os.File
	Proto             *laila.LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_
	Policy            *laila.LAILA_IDENTIFIABLE_POLICY_
	Peer              Fake_peer_
	Peer_gid          string
}

// New_UartPtyPair is ``UartPtyPair(peer_cls=FakeRpcOnlyPeer, *,
// throttle_bytes_per_sec=None, uart_kwargs=None, peer_kwargs=None,
// liveness_interval=None)``.
func New_UartPtyPair(peer_cls Peer_factory_, kw ...py.Kw) *UartPtyPair {
	opts := py.MergeKw(kw...)
	u := &UartPtyPair{Peer_cls: peer_cls, Uart_kwargs: py.Kw{}, Peer_kwargs: py.Kw{}}
	if u.Peer_cls == nil {
		u.Peer_cls = Rpc_only_peer_
	}
	if v, ok := opts["throttle_bytes_per_sec"]; ok && !py.IsNone(v) {
		u.Throttle, _ = py.Float(v)
	}
	if v, ok := opts["uart_kwargs"].(py.Kw); ok {
		u.Uart_kwargs = v.Copy()
	}
	if v, ok := opts["peer_kwargs"].(py.Kw); ok {
		u.Peer_kwargs = v.Copy()
	}
	if v, ok := opts["liveness_interval"]; ok && !py.IsNone(v) {
		f, _ := py.Float(v)
		u.Liveness_interval = &f
	}
	return u
}

// Enter is ``__enter__``.
func (u *UartPtyPair) Enter() (*UartPtyPair, error) {
	var err error
	if u.Master, u.Slave, err = py.Openpty(); err != nil {
		return nil, err
	}
	codec_name := "json"
	if v, ok := u.Uart_kwargs["codec"]; ok {
		codec_name = py.Str(v)
	}
	pk := u.Peer_kwargs.Copy()
	pk["throttle_bytes_per_sec"] = u.Throttle
	pk["codec_name"] = codec_name
	if u.Peer, err = u.Peer_cls(u.Master, pk); err != nil {
		return nil, err
	}
	uk := u.Uart_kwargs.Copy()
	uk["port"] = py.Ttyname(u.Slave)
	if u.Proto, err = laila.New_DefaultUARTProtocol(uk); err != nil {
		return nil, err
	}
	if u.Policy, err = laila.New_LAILA_IDENTIFIABLE_POLICY_(); err != nil {
		return nil, err
	}
	if u.Liveness_interval != nil {
		Comm(u.Policy).Liveness_interval = *u.Liveness_interval
	}
	if err := Comm(u.Policy).Add_connection(u.Proto); err != nil {
		return nil, err
	}
	if u.Peer_gid, err = Comm(u.Policy).Add_peer("serial://"+py.Ttyname(u.Slave), u.Proto.Peer_secret_key); err != nil {
		return nil, err
	}
	return u, nil
}

// Exit is ``__exit__``.
func (u *UartPtyPair) Exit() {
	if u.Policy != nil {
		_ = Comm(u.Policy).Stop()
	}
	if u.Peer != nil {
		u.Peer.Stop()
	}
	for _, f := range []*os.File{u.Master, u.Slave} {
		if f != nil {
			_ = f.Close()
		}
	}
}

// Comm_ is ``self.comm``.
func (u *UartPtyPair) Comm_() *laila.LAILA_IDENTIFIABLE_COMMUNICATION_ { return Comm(u.Policy) }

// Rpc_peer is the fake peer viewed as a FakeRpcOnlyPeer.
func (u *UartPtyPair) Rpc_peer() *FakeRpcOnlyPeer { return u.Peer.Base() }

// Lane_peer is the fake peer viewed as a FakeLanePeer (nil if it is not one).
func (u *UartPtyPair) Lane_peer() *FakeLanePeer {
	lp, _ := u.Peer.(*FakeLanePeer)
	return lp
}
