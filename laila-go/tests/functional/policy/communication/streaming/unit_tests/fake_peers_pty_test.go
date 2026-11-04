// Tests 3, 4, 6, 15 + malformed frames: laila's UART carrier against fake peers on a pty.
//
// ``openpty()`` gives laila's ``DefaultUARTProtocol`` a real serial
// device (the slave) while the test drives the master with a loop-thread
// peer that speaks only the *current* length-prefixed JSON-RPC wire
// format (``FakeRpcOnlyPeer``) or additionally emits raw lane frames on a
// static lane (``FakeLanePeer``). The fake peer throttles both directions
// to emulate a 1 Mbaud-class link.
//
//  3. Older / RPC-only peer (no ``caps``): ``peers[gid][name]`` raises
//     ``ConnectionError`` at once, pings and ``laila.remember(dst_policy=)``
//     keep working, laila never emits a ``0x01`` frame.
//  4. Saturated outbound stream on a static lane while pings and a
//     remember RPC stay under budget; the peer is never dropped.
//  6. Inbound static lane from firmware that never answers
//     ``__comm_channel_open__``: byte-exact messages incl. multi-chunk;
//     RPC in both directions still works.
//  15. Lossy link: with random chunk loss, exactly the fully-received
//     messages are delivered, byte-exact; no partial message surfaces.
//  +  Malformed / unknown reserved frames are dropped without harming
//     the connection.
//
// Port of tests/functional/policy/communication/streaming/unit_tests/test_fake_peers_pty.py.
package unit_tests

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// canned_blob_ is ``_canned_blob(data)``:
// ``Entry.constant(data).serialize(transformations=transformation_base64)``.
func canned_blob_(data []byte) any {
	return must(must(laila.Entry_constant(data)).Serialize(laila.Transformation_base64))
}

// rpc_paths_contains_ is ``path in peer.rpc_paths``.
func rpc_paths_contains_(peer *FakeRpcOnlyPeer, path []string) bool {
	for _, p := range peer.Rpc_paths_() {
		if reflect.DeepEqual(p, path) {
			return true
		}
	}
	return false
}

// ----------------------------------------------------------------------
// TestRpcOnlyPeer -- Test 3.
// ----------------------------------------------------------------------

func TestRpcOnlyPeer(t *testing.T) {
	original := must(laila.Get_active_policy())
	t.Cleanup(func() { laila.Activate_policy(original) })
	canned := canned_blob_([]byte("from the old peer"))
	up := enter_uart_(t, New_UartPtyPair(Rpc_only_peer_, py.Kw{"peer_kwargs": py.Kw{"remember_blob": canned}}))
	gid := up.Peer_gid
	peer := up.Rpc_peer()
	assertEq(t, gid, peer.Gid)
	assertEq(t, peer.Handshakes_(), 1)
	// laila advertised lanes; the old peer did not
	assertEq(t, peer.Handshake_params_()[0].GetDefault("caps", nil), py.DictOf("lanes", 1))
	assertTrue(t, !up.Proto.Peer_has_lanes_(gid), "_peer_has_lanes should be False")
	proxy := proxy_(t, up.Comm_(), gid)
	assertTrue(t, must(proxy.Getitem("default")) == any(proxy), "proxy['default'] is not the proxy")
	t0 := time.Now()
	_, err := proxy.Getitem("video")
	assertRaises(t, py.ConnectionError, err)
	assertTrue(t, time.Since(t0).Seconds() < 1.0, "channel lookup should fail fast")
	assertIn_(t, "did not advertise stream lanes", err)
	assertIn_(t, "static_lanes", err)
	// no control RPC was attempted, nothing bound
	assertTrue(t, !rpc_paths_contains_(peer, []string{"__comm_channel_open__"}), "unexpected __comm_channel_open__")
	assertEq(t, len(must(proxy.Channels())), 0)
	// RPC paths are untouched
	assertTrue(t, up.Proto.Ping(gid), "ping")
	assertTrue(t, peer.Pings_answered_() >= 1, "pings_answered")
	laila.Activate_policy(up.Policy)
	back := wait_result_(t, must(laila.Remember("LAILA:ENTRY:anything", py.Kw{"dst_policy": gid})), 20)
	assertBytes(t, bytes_of_(t, back), []byte("from the old peer"))
	assertTrue(t, rpc_paths_contains_(peer, []string{"central", "memory", "_remote_remember"}), "_remote_remember not seen")
	_, err = laila.Get_relay(gid, "video")
	assertRaises(t, py.ConnectionError, err)
	sleep(0.2)
	assertEq(t, peer.Stream_frames_seen_(), 0)
	assertTrue(t, up.Comm_().Peers.Contains(gid), "peer dropped")
}

// ----------------------------------------------------------------------
// TestSaturatedUart -- Test 4: 1 Mbaud-class link saturated by 30 KB
// frames; RPC stays interactive.
// ----------------------------------------------------------------------

func TestSaturatedUart(t *testing.T) {
	original := must(laila.Get_active_policy())
	t.Cleanup(func() { laila.Activate_policy(original) })
	rate := 100_000.0 // bytes/s ~ 1 Mbaud
	canned := canned_blob_([]byte("ok"))
	up := enter_uart_(t, New_UartPtyPair(Rpc_only_peer_, py.Kw{
		"throttle_bytes_per_sec": rate,
		"uart_kwargs":            py.Kw{"static_lanes": py.DictOf("video", 1), "ping_timeout": 3.0},
		"peer_kwargs":            py.Kw{"remember_blob": canned},
		"liveness_interval":      0.5,
	}))
	gid := up.Peer_gid
	peer := up.Rpc_peer()
	ch := channel_(t, up.Comm_(), gid, "video")
	assertEq(t, []any{ch.Lane_id, py.Deref_any(ch.Tx_lane_id())}, []any{1, 1})
	assertTrue(t, !rpc_paths_contains_(peer, []string{"__comm_channel_open__"}), "unexpected __comm_channel_open__")
	assertEq(t, up.Proto.Stream_chunk_bytes, 4096)

	stop := py.NewEvent()
	frame := blob_(30_000, 4)
	producer_done := make(chan struct{})
	go func() {
		defer close(producer_done)
		// 30 KB at 30 fps = 900 KB/s requested vs 100 KB/s link.
		for !stop.Is_set() {
			if err := ch.Send(frame); err != nil {
				return // ConnectionError
			}
			sleep(1.0 / 30)
		}
	}()
	var rtts []float64
	var rpc_rtt float64
	var queued_msgs int
	func() {
		defer stop.Set()
		sleep(0.5) // let the link saturate
		deadline := py.Monotonic() + 6.0
		for py.Monotonic() < deadline {
			t0 := py.Monotonic()
			assertTrue(t, up.Proto.Ping(gid), "ping failed under load")
			rtts = append(rtts, py.Monotonic()-t0)
			sleep(0.3)
		}
		laila.Activate_policy(up.Policy)
		t0 := py.Monotonic()
		back := wait_result_(t, must(laila.Remember("LAILA:ENTRY:x", py.Kw{"dst_policy": gid})), 20)
		rpc_rtt = py.Monotonic() - t0
		assertBytes(t, bytes_of_(t, back), []byte("ok"))
		// ``len(link.stream_q)`` is loop-thread state: read it on the loop.
		loop := up.Proto.Loop()
		v, err := py.Run_coroutine_threadsafe(func() (any, error) {
			link := up.Proto.Link_
			if link == nil {
				return 0, nil
			}
			return len(link.Stream_q), nil
		}, loop).Result(5)
		must(0, err)
		queued_msgs = v.(int)
	}()
	<-producer_done
	sorted := append([]float64(nil), rtts...)
	sort.Float64s(sorted)
	med := sorted[len(sorted)/2]
	worst := sorted[len(sorted)-1]
	queued_seconds := float64(queued_msgs) * float64(len(frame)) / rate
	t.Logf("[uart 100KB/s saturated] ping median=%.0fms max=%.0fms remember=%.0fms queued_stream=%d msgs "+
		"(~%.1fs of wire time) tx_dropped=%d stream_frames_rx=%d (%d B)",
		med*1e3, worst*1e3, rpc_rtt*1e3, queued_msgs, queued_seconds, ch.Tx_dropped(),
		peer.Stream_frames_seen_(), peer.Stream_bytes_seen_())
	// The link really was saturated: seconds of stream data queued
	// behind the writer...
	assertTrue(t, queued_seconds > 2.0, "link should have been saturated")
	// ...yet RPC never waited behind it. The residual latency is the
	// kernel pty buffer (~64 KB ~ 0.65 s at 100 KB/s) that sits ahead of
	// the fake peer's throttled reader; a real UART FIFO is bytes, not
	// kilobytes. Without priority interleaving a ping would wait the full
	// queued_seconds and liveness would drop the peer.
	assertTrue(t, worst < queued_seconds/2, fmt.Sprintf("max ping %.3fs", worst))
	assertTrue(t, worst < up.Proto.Ping_timeout, fmt.Sprintf("max ping %.3fs", worst))
	assertTrue(t, med < 1.0, fmt.Sprintf("median ping %.3fs", med))
	assertTrue(t, up.Comm_().Peers.Contains(gid), "peer was dropped by liveness under load")
	assertTrue(t, peer.Stream_frames_seen_() > 10, "too few stream frames received")
	assertTrue(t, float64(peer.Stream_bytes_seen_()) > 0.5*rate*6, "too few stream bytes received")
	assertNoErrors(t, peer)
}

// ----------------------------------------------------------------------
// TestFirmwareLanePeer -- Test 6 + malformed frames.
// ----------------------------------------------------------------------

func TestFirmwareLanePeer(t *testing.T) {
	t.Run("static_inbound_lane", func(t *testing.T) {
		original := must(laila.Get_active_policy())
		t.Cleanup(func() { laila.Activate_policy(original) })
		up := enter_uart_(t, New_UartPtyPair(Lane_peer_, py.Kw{
			"uart_kwargs": py.Kw{"static_lanes": py.DictOf("video", 1)},
			"peer_kwargs": py.Kw{"lane": 1, "chunk_size": 4096},
		}))
		gid := up.Peer_gid
		peer := up.Lane_peer()
		// bound at registration -> early frames are buffered, not dropped
		assertEq(t, up.Proto.Channel_names(gid), []string{"video"})
		must(peer.Emit_threadsafe([]byte("early")))
		ch := channel_(t, up.Comm_(), gid, "video")
		assertTrue(t, ch.Opened(), "channel not opened")
		assertTrue(t, !rpc_paths_contains_(peer.FakeRpcOnlyPeer, []string{"__comm_channel_open__"}), "unexpected __comm_channel_open__")
		payloads := [][]byte{}
		for _, n := range []int{1, 100, 4096, 5000, 50_000} {
			payloads = append(payloads, blob_(n, 6))
		}
		for _, p := range payloads {
			must(peer.Emit_threadsafe(p))
		}
		laila.Activate_policy(up.Policy)
		relay := must(laila.Get_relay(gid, "video", py.Kw{"timeout": 20}))
		assertBytes(t, bytes_of_(t, next_(t, relay)), []byte("early"))
		got := [][]byte{}
		for range payloads {
			got = append(got, bytes_of_(t, next_(t, relay)))
		}
		assertTrue(t, reflect.DeepEqual(got, payloads), "payloads differ")
		assertEq(t, ch.Discarded(), 0)
		assertEq(t, up.Proto.Unknown_lane_drops_, 0)
		// RPC laila -> peer
		assertTrue(t, up.Proto.Ping(gid), "ping")
		// RPC peer -> laila (a ping request from the firmware side)
		must(0, peer.Send_obj_threadsafe(py.DictOf(
			"jsonrpc", "2.0",
			"id", "fw-ping",
			"method", "rpc.call",
			"params", py.DictOf("path", py.NewList("__comm_ping__"), "args", py.NewList(), "kwargs", py.NewDict()),
		)))
		find_resp := func() *py.Dict {
			for _, r := range peer.Responses_seen_() {
				if py.Eq(r.GetDefault("id", nil), "fw-ping") {
					return r
				}
			}
			return nil
		}
		assertTrue(t, Wait_until(func() bool { return find_resp() != nil }), "fw-ping response never seen")
		resp := find_resp()
		assertEq(t, resp.GetDefault("result", nil), "pong")
		// laila -> firmware on the same static lane
		must(0, ch.Send([]byte("to-firmware")))
		assertTrue(t, Wait_until(func() bool { return peer.Stream_frames_seen_() >= 1 }), "firmware never saw the stream frame")
	})

	t.Run("malformed_and_unknown_reserved_frames_are_dropped", func(t *testing.T) {
		original := must(laila.Get_active_policy())
		t.Cleanup(func() { laila.Activate_policy(original) })
		up := enter_uart_(t, New_UartPtyPair(Lane_peer_, py.Kw{
			"uart_kwargs": py.Kw{"static_lanes": py.DictOf("video", 1)},
		}))
		gid := up.Peer_gid
		peer := up.Lane_peer()
		ch := channel_(t, up.Comm_(), gid, "video")
		bad := [][]byte{
			{0x01}, // too short for a header
			laila.Pack_stream_frame(1, 0, 0, []byte{})[:5],                              // truncated header
			append([]byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03}, 'x'),               // lane 0
			laila.Pack_stream_frame(9, 0, laila.FLAG_START|laila.FLAG_END, []byte("q")), // unbound lane
			[]byte("\x05garbage"), // unknown reserved marker
			append([]byte{0x1f}, bytes.Repeat([]byte{0xff}, 20)...),
		}
		for _, raw := range bad {
			must(0, peer.Send_raw_frame_threadsafe(raw))
		}
		must(peer.Emit_threadsafe([]byte("still fine")))
		laila.Activate_policy(up.Policy)
		assertBytes(t, bytes_of_(t, next_(t, relay_(t, ch, 10))), []byte("still fine"))
		assertTrue(t, up.Proto.Ping(gid), "ping")
		assertTrue(t, up.Comm_().Peers.Contains(gid), "peer dropped")
		assertTrue(t, up.Proto.Malformed_stream_drops_ >= 3, fmt.Sprintf("malformed drops %d", up.Proto.Malformed_stream_drops_))
		assertTrue(t, up.Proto.Reserved_marker_drops_ >= 2, fmt.Sprintf("reserved drops %d", up.Proto.Reserved_marker_drops_))
		assertTrue(t, up.Proto.Unknown_lane_drops_ >= 1, fmt.Sprintf("unknown lane drops %d", up.Proto.Unknown_lane_drops_))
		assertEq(t, ch.Discarded(), 0)
	})
}

// ----------------------------------------------------------------------
// TestLossyLane -- Test 15.
// ----------------------------------------------------------------------

func TestLossyLane(t *testing.T) {
	original := must(laila.Get_active_policy())
	t.Cleanup(func() { laila.Activate_policy(original) })
	n_msgs := 150
	up := enter_uart_(t, New_UartPtyPair(Lane_peer_, py.Kw{
		"uart_kwargs": py.Kw{"static_lanes": py.DictOf("video", 1), "channel_queue_size": 1000},
		"peer_kwargs": py.Kw{"lane": 1, "chunk_size": 4096, "drop_probability": 0.06, "seed": 15},
	}))
	gid := up.Peer_gid
	peer := up.Lane_peer()
	ch := channel_(t, up.Comm_(), gid, "video")
	expected_ok := []string{}
	payloads := [][]byte{}
	for i := 0; i < n_msgs; i++ {
		payloads = append(payloads, blob_(20_000+(i%7)*1000, i)) // 5-7 chunks
	}
	for _, p := range payloads {
		if must(peer.Emit_threadsafe(p)) {
			expected_ok = append(expected_ok, py.Sha256_hex(p))
		}
	}
	// sentinel message that cannot be dropped (fresh peer-side state)
	peer.Drop_probability = 0.0
	must(peer.Emit_threadsafe([]byte("END")))
	laila.Activate_policy(up.Policy)
	got := []string{}
	var got_mu sync.Mutex
	err := relay_(t, ch, 30).Range(func(e *laila.StreamEntry) error {
		d := bytes_of_(t, e)
		if bytes.Equal(d, []byte("END")) {
			return errStopRange_
		}
		got_mu.Lock()
		got = append(got, py.Sha256_hex(d))
		got_mu.Unlock()
		return nil
	})
	if err != nil && err != errStopRange_ {
		t.Fatal(err)
	}
	t.Logf("[lossy uart] emitted=%d chunks_dropped=%d messages_with_drop=%d delivered=%d discarded=%d",
		n_msgs, peer.Chunks_dropped, peer.Messages_with_drop, len(got), ch.Discarded())
	assertTrue(t, peer.Messages_with_drop > 0, "test needs some loss")
	assertTrue(t, reflect.DeepEqual(got, expected_ok), "delivered set differs from the intact messages") // exactly the intact ones, in order
	assertEq(t, len(got)+peer.Messages_with_drop, n_msgs)
	assertTrue(t, ch.Discarded() <= peer.Messages_with_drop, "discarded exceeds messages_with_drop")
	assertEq(t, ch.Dropped(), 0)
	assertTrue(t, up.Proto.Ping(gid), "ping")
}

// errStopRange_ is the ``break`` inside ``for e in relay``.
var errStopRange_ = fmt.Errorf("stop range")
