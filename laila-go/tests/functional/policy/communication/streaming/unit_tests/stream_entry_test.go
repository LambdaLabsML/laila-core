// Transport-free unit tests: wire format, Channel reassembly / bounds, StreamEntry.
//
// Covers the pieces that do not need two policies: the ``0x01`` frame
// header, the first-byte discriminator against real JSON / msgpack RPC
// payloads, Channel chunk reassembly (gaps, oversize, missing START),
// drop-oldest by count and by bytes, Relay termination, and the
// StreamEntry identity contract (test 7's entry half).
//
// Port of tests/functional/policy/communication/streaming/unit_tests/test_stream_entry.py.
package unit_tests

import (
	"bytes"
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// dummyProto_ is ``_DummyProto``: minimal carrier stand-in that loops
// ``send()`` straight back into the channel.
type dummyProto_ struct {
	py.Object
	Rpc_timeout float64

	mu             sync.Mutex
	sent           [][]byte
	closed_notices int
}

var dummyProto_class = py.NewClass("_DummyProto", (*dummyProto_)(nil))

func new_dummyProto_() *dummyProto_ {
	p := &dummyProto_{}
	must(0, py.Init(p, dummyProto_class, py.Kw{"rpc_timeout": 1.0}))
	return p
}

func (p *dummyProto_) Stream_enqueue_(channel *laila.Channel, payload []byte) error {
	p.mu.Lock()
	p.sent = append(p.sent, payload)
	p.mu.Unlock()
	channel.Enqueue_inbound_(payload, -1)
	return nil
}

func (p *dummyProto_) On_channel_closed_locally_(ch *laila.Channel) error {
	p.mu.Lock()
	p.closed_notices++
	p.mu.Unlock()
	return nil
}

func (p *dummyProto_) Sent() [][]byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([][]byte(nil), p.sent...)
}

func (p *dummyProto_) Closed_notices() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed_notices
}

// chan_ is ``_chan(proto=None, **kw)``.
func chan_(proto *dummyProto_, kw ...py.Kw) *laila.Channel {
	if proto == nil {
		proto = new_dummyProto_()
	}
	defaults := py.Kw{"queue_size": 256, "queue_bytes": 1 << 20, "max_message_bytes": 1 << 20, "tx_lane_id": 1}
	for _, k := range kw {
		for n, v := range k {
			defaults[n] = v
		}
	}
	return laila.New_Channel(proto, "LAILA:POLICY:peer", "video", 1, defaults)
}

func relay_timeout_(ch *laila.Channel, timeout float64) *laila.Relay {
	return ch.Relay(&timeout)
}

func assertBytes(t *testing.T, got, want []byte) {
	t.Helper()
	if !bytes.Equal(got, want) {
		t.Fatalf("got %s, want %s", py.Repr(got), py.Repr(want))
	}
}

func rep_(b byte, n int) []byte { return bytes.Repeat([]byte{b}, n) }

// ----------------------------------------------------------------------
// TestWireFormat
// ----------------------------------------------------------------------

func TestWireFormat(t *testing.T) {
	t.Run("header_roundtrip", func(t *testing.T) {
		frame := laila.Pack_stream_frame(7, 123456, laila.FLAG_START|laila.FLAG_END, []byte("abc"))
		assertEq(t, int(frame[0]), laila.STREAM_MARKER)
		assertEq(t, len(frame), laila.STREAM_HEADER_LEN+3)
		lane, seq, flags, err := laila.Unpack_stream_header(frame)
		must(0, err)
		assertEq(t, []any{lane, seq, flags}, []any{7, int64(123456), 3})
		assertBytes(t, frame[laila.STREAM_HEADER_LEN:], []byte("abc"))
		assertTrue(t, laila.Is_stream_frame(frame), "is_stream_frame")
		assertTrue(t, laila.Is_reserved_frame(frame), "is_reserved_frame")
	})

	t.Run("seq_wraps", func(t *testing.T) {
		frame := laila.Pack_stream_frame(1, laila.SEQ_MODULUS+5, 0, []byte{})
		_, seq, _, err := laila.Unpack_stream_header(frame)
		must(0, err)
		assertEq(t, seq, int64(5))
	})

	t.Run("invalid_headers", func(t *testing.T) {
		_, _, _, err := laila.Unpack_stream_header([]byte{0x01, 0x01})
		assertRaises(t, py.ValueError, err)
		_, _, _, err = laila.Unpack_stream_header(laila.Pack_stream_frame(0, 0, 0, []byte("x"))) // lane 0
		assertRaises(t, py.ValueError, err)
		_, _, _, err = laila.Unpack_stream_header(append([]byte{0x7b}, make([]byte, 6)...))
		assertRaises(t, py.ValueError, err)
	})

	t.Run("discriminator_never_matches_rpc_payloads", func(t *testing.T) {
		req := laila.Make_request("rpc.call", py.DictOf(
			"path", py.NewList("a"), "args", py.NewList(), "kwargs", py.NewDict()), "")
		for _, name := range []string{"json", "msgpack"} {
			raw, err := laila.Codec_encode(req, name)
			if err != nil {
				if name == "msgpack" {
					continue // msgpack not installed
				}
				t.Fatal(err)
			}
			assertTrue(t, !laila.Is_reserved_frame(raw), name)
			assertTrue(t, !laila.Is_stream_frame(raw), name)
		}
		for b := 0x00; b < 0x20; b++ {
			assertTrue(t, laila.Is_reserved_frame(append([]byte{byte(b)}, []byte("zz")...)), "reserved byte")
		}
		assertTrue(t, !laila.Is_reserved_frame([]byte{}), "empty is not reserved")
		// codec module re-exports the same names: in the Go port ``codec`` and
		// ``wire`` share one package, so the identity is by construction.
		assertEq(t, laila.STREAM_MARKER, 0x01)
	})
}

// ----------------------------------------------------------------------
// TestChannelReassembly
// ----------------------------------------------------------------------

type frame_ struct {
	seq   int64
	flags int
	data  []byte
}

// frames_ is ``TestChannelReassembly._frames(payload, chunk, seq0=0)``.
func frames_(payload []byte, chunk int, seq0 int64) []frame_ {
	count := max(1, (len(payload)+chunk-1)/chunk)
	out := []frame_{}
	for i := 0; i < count; i++ {
		flags := 0
		if i == 0 {
			flags |= laila.FLAG_START
		}
		if i == count-1 {
			flags |= laila.FLAG_END
		}
		lo := min(i*chunk, len(payload))
		hi := min((i+1)*chunk, len(payload))
		out = append(out, frame_{(seq0 + int64(i)) % laila.SEQ_MODULUS, flags, payload[lo:hi]})
	}
	return out
}

// feed_ is ``TestChannelReassembly._feed(ch, frames)``.
func feed_(ch *laila.Channel, frames []frame_) {
	for _, f := range frames {
		ch.On_chunk_(f.seq, f.flags, f.data, py.Monotonic())
	}
}

func TestChannelReassembly(t *testing.T) {
	t.Run("single_and_multi_chunk_messages", func(t *testing.T) {
		ch := chan_(nil)
		feed_(ch, frames_([]byte{}, 4, 0))
		feed_(ch, frames_([]byte("hello world!!"), 4, 1))
		feed_(ch, frames_(rep_('x', 9), 4, 5))
		r := relay_timeout_(ch, 1)
		assertBytes(t, bytes_of_(t, next_(t, r)), []byte{})
		assertBytes(t, bytes_of_(t, next_(t, r)), []byte("hello world!!"))
		assertBytes(t, bytes_of_(t, next_(t, r)), rep_('x', 9))
		assertEq(t, ch.Rx_seq(), 3)
		assertEq(t, ch.Discarded(), 0)
	})

	t.Run("gap_discards_whole_message", func(t *testing.T) {
		ch := chan_(nil)
		frames := frames_(rep_('A', 12), 4, 0)     // seqs 0,1,2
		frames = append(frames[:1], frames[2:]...) // lose the middle chunk
		feed_(ch, frames)
		feed_(ch, frames_(rep_('B', 3), 4, 3))
		r := relay_timeout_(ch, 1)
		assertBytes(t, bytes_of_(t, next_(t, r)), rep_('B', 3))
		assertEq(t, ch.Discarded(), 1)
		assertEq(t, ch.Qsize(), 0)
	})

	t.Run("lost_end_then_new_start", func(t *testing.T) {
		ch := chan_(nil)
		frames := frames_(rep_('A', 12), 4, 0)
		frames = frames[:len(frames)-1] // END lost
		feed_(ch, frames)
		feed_(ch, frames_(rep_('B', 2), 4, 3))
		assertBytes(t, bytes_of_(t, next_(t, relay_timeout_(ch, 1))), rep_('B', 2))
		assertEq(t, ch.Discarded(), 1)
	})

	t.Run("lost_start_swallows_continuations", func(t *testing.T) {
		ch := chan_(nil)
		frames := frames_(rep_('A', 12), 4, 0)
		frames = frames[1:] // START lost
		feed_(ch, frames)
		feed_(ch, frames_(rep_('B', 2), 4, 3))
		assertBytes(t, bytes_of_(t, next_(t, relay_timeout_(ch, 1))), rep_('B', 2))
		assertEq(t, ch.Discarded(), 1)
		assertEq(t, ch.Rx_seq(), 1)
	})

	t.Run("oversize_message_discarded", func(t *testing.T) {
		ch := chan_(nil, py.Kw{"max_message_bytes": 10})
		feed_(ch, frames_(rep_('A', 12), 4, 0))
		feed_(ch, frames_([]byte("ok"), 4, 3))
		assertBytes(t, bytes_of_(t, next_(t, relay_timeout_(ch, 1))), []byte("ok"))
		assertEq(t, ch.Discarded(), 1)
	})

	t.Run("seq_wraparound_is_not_a_gap", func(t *testing.T) {
		ch := chan_(nil)
		feed_(ch, frames_(rep_('A', 8), 4, laila.SEQ_MODULUS-1))
		assertBytes(t, bytes_of_(t, next_(t, relay_timeout_(ch, 1))), rep_('A', 8))
		assertEq(t, ch.Discarded(), 0)
	})
}

// ----------------------------------------------------------------------
// TestChannelBounds
// ----------------------------------------------------------------------

func TestChannelBounds(t *testing.T) {
	t.Run("drop_oldest_by_count", func(t *testing.T) {
		ch := chan_(nil, py.Kw{"queue_size": 4})
		for i := 0; i < 10; i++ {
			ch.Enqueue_inbound_([]byte{byte(i)}, -1)
		}
		assertEq(t, ch.Dropped(), 6)
		r := relay_timeout_(ch, 1)
		got := []*laila.StreamEntry{}
		for i := 0; i < 4; i++ {
			got = append(got, next_(t, r))
		}
		datas := [][]byte{}
		seqs := []int{}
		dropped_before := []int{}
		for _, g := range got {
			datas = append(datas, bytes_of_(t, g))
			seqs = append(seqs, g.Stream().Seq)
			dropped_before = append(dropped_before, g.Stream().Dropped_before)
		}
		assertTrue(t, reflect.DeepEqual(datas, [][]byte{{6}, {7}, {8}, {9}}), "data order")
		assertEq(t, seqs, []int{6, 7, 8, 9})
		// dropped_before is the drop count when *that* message was enqueued:
		// message 6 arrived after 0 and 1 had been evicted; the seq gap tells
		// the consumer how many were lost in total.
		assertEq(t, dropped_before, []int{2, 3, 4, 5})
		assertEq(t, got[0].Stream().Seq-0, ch.Dropped())
	})

	t.Run("drop_oldest_by_bytes", func(t *testing.T) {
		ch := chan_(nil, py.Kw{"queue_size": 1000, "queue_bytes": 100})
		for i := 0; i < 10; i++ {
			ch.Enqueue_inbound_(rep_(byte(i), 30), -1)
		}
		// 100 bytes holds 3 messages of 30
		assertEq(t, ch.Qsize(), 3)
		assertEq(t, ch.Dropped(), 7)
		assertEq(t, ch.Queue_bytes_(), 90)
		r := relay_timeout_(ch, 1)
		assertBytes(t, bytes_of_(t, next_(t, r)), rep_(7, 30))
		assertEq(t, ch.Queue_bytes_(), 60)
	})

	t.Run("single_oversize_message_still_enqueues_on_empty_queue", func(t *testing.T) {
		ch := chan_(nil, py.Kw{"queue_bytes": 10})
		ch.Enqueue_inbound_(rep_('x', 50), -1)
		assertEq(t, ch.Qsize(), 1)
		assertBytes(t, bytes_of_(t, next_(t, relay_timeout_(ch, 1))), rep_('x', 50))
	})

	t.Run("send_validates", func(t *testing.T) {
		proto := new_dummyProto_()
		ch := chan_(proto, py.Kw{"max_message_bytes": 8})
		must(0, ch.Send([]byte("12345678")))
		must(0, ch.Send([]byte("abc"))) // memoryview(b"abc")
		assertTrue(t, reflect.DeepEqual(proto.Sent(), [][]byte{[]byte("12345678"), []byte("abc")}), "sent payloads")
		assertRaises(t, py.ValueError, ch.Send([]byte("123456789")))
		ch.Close()
		assertEq(t, proto.Closed_notices(), 1)
		assertRaises(t, py.ConnectionError, ch.Send([]byte("x")))
		ch.Close() // idempotent
		assertEq(t, proto.Closed_notices(), 1)
	})

	t.Run("send_while_opening_times_out", func(t *testing.T) {
		proto := new_dummyProto_()
		ch := chan_(proto, py.Kw{"tx_lane_id": nil})
		assertTrue(t, !ch.Opened(), "not opened")
		t0 := time.Now()
		assertRaises(t, py.ConnectionError, ch.Send([]byte("x")))
		assertTrue(t, time.Since(t0).Seconds() >= 0.9, "send should wait rpc_timeout before failing")
		ch.Mark_opened_(3)
		assertTrue(t, ch.Opened(), "opened")
		must(0, ch.Send([]byte("x")))
		assertEq(t, ch.Tx_seq(), 1)
	})

	t.Run("chunk_seq_reservation", func(t *testing.T) {
		ch := chan_(nil)
		assertEq(t, ch.Next_chunk_seqs_(3), int64(0))
		assertEq(t, ch.Next_chunk_seqs_(1), int64(3))
		ch.Set_tx_chunk_seq_(laila.SEQ_MODULUS - 1)
		assertEq(t, ch.Next_chunk_seqs_(2), int64(laila.SEQ_MODULUS-1))
		assertEq(t, ch.Tx_chunk_seq_(), int64(1))
	})
}

// ----------------------------------------------------------------------
// TestRelay
// ----------------------------------------------------------------------

func TestRelay(t *testing.T) {
	t.Run("relay_is_singleton_and_ends_on_close", func(t *testing.T) {
		ch := chan_(nil)
		r1 := relay_timeout_(ch, 0.5)
		r2 := relay_(t, ch, 2.0)
		assertTrue(t, r1 == r2, "relay is a singleton")
		assertEq(t, *r1.Timeout, 2.0)
		var _ *laila.Relay = r1
		ch.Enqueue_inbound_([]byte("a"), -1)
		ch.Close_local_("test")
		ch.Enqueue_inbound_([]byte("late"), -1) // ignored after close
		assertBytes(t, bytes_of_(t, next_(t, r1)), []byte("a"))
		_, err := r1.Next()
		assertRaises(t, py.StopIteration, err)
		_, err = r1.Next()
		assertRaises(t, py.StopIteration, err)
		assertEq(t, py.Deref_any(r1.Closed_reason()), "test")
		rest := []*laila.StreamEntry{}
		must(0, r1.Range(func(e *laila.StreamEntry) error { rest = append(rest, e); return nil }))
		assertEq(t, len(rest), 0)
	})

	t.Run("timeout", func(t *testing.T) {
		ch := chan_(nil)
		r := relay_timeout_(ch, 0.2)
		t0 := time.Now()
		_, err := r.Next()
		assertRaises(t, py.TimeoutError, err)
		assertTrue(t, time.Since(t0).Seconds() < 1.5, "timeout should be prompt")
	})

	t.Run("blocked_consumer_wakes_on_close_with_full_queue", func(t *testing.T) {
		ch := chan_(nil, py.Kw{"queue_size": 1})
		ch.Enqueue_inbound_([]byte("only"), -1)
		var mu sync.Mutex
		got := [][]byte{}
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = ch.Relay(nil).Range(func(e *laila.StreamEntry) error {
				mu.Lock()
				got = append(got, bytes_of_(t, e))
				mu.Unlock()
				return nil
			})
		}()
		sleep(0.1)
		ch.Close_local_("bye")
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("consumer thread still alive")
		}
		mu.Lock()
		defer mu.Unlock()
		assertTrue(t, reflect.DeepEqual(got, [][]byte{[]byte("only")}), "got")
	})

	t.Run("relay_arg_validation", func(t *testing.T) {
		ch := chan_(nil)
		_, err := laila.Get_relay(ch, "video")
		assertRaises(t, py.TypeError, err)
		_, err = laila.Get_relay("LAILA:POLICY:x", nil)
		assertRaises(t, py.TypeError, err)
	})
}

// ----------------------------------------------------------------------
// TestStreamEntry
// ----------------------------------------------------------------------

func TestStreamEntry(t *testing.T) {
	t.Run("entry_contract", func(t *testing.T) {
		ch := chan_(nil)
		ch.Enqueue_inbound_([]byte("\x00\x01payload"), -1)
		e := next_(t, relay_timeout_(ch, 1))
		var _ *laila.StreamEntry = e
		assertTrue(t, py.Isinstance(e, laila.Entry_class), "isinstance Entry")
		assertBytes(t, bytes_of_(t, e), []byte("\x00\x01payload"))
		assertEq(t, e.State(), laila.EntryState_READY)
		assertTrue(t, e.Evolution() == nil, "evolution is None")
		assertTrue(t, e.Global_id() != "", "global_id truthy")
		ref := must(laila.Entry_constant([]byte("\x00\x01payload")))
		assertEq(t, e.Scopes(), ref.Scopes())
		assertTrue(t, e.Stream() != nil, "isinstance StreamMeta")
		assertEq(t, e.Stream().Channel, "video")
		assertEq(t, e.Stream().Peer_id, "LAILA:POLICY:peer")
		assertEq(t, e.Stream().Lane, 1)
		assertEq(t, e.Stream().Seq, 0)
		assertEq(t, e.Stream().Dropped_before, 0)
		// Entry.constant(b"x").__dict__.get("stream") is None
		plain := must(laila.Entry_constant([]byte("x")))
		_, has_stream := any(plain).(interface{ Stream() *laila.StreamMeta })
		assertTrue(t, !has_stream, "plain constants carry no stream meta")
		_, err := e.Evolve([]byte("other")) // constants are immutable
		assertTrue(t, err != nil && errors.Is(err, py.Exception), "evolve on constant must raise")
	})

	t.Run("unique_ids_and_monotonic_arrival", func(t *testing.T) {
		ch := chan_(nil)
		for i := 0; i < 5; i++ {
			ch.Enqueue_inbound_([]byte{byte(i)}, -1)
		}
		r := relay_timeout_(ch, 1)
		es := []*laila.StreamEntry{}
		for i := 0; i < 5; i++ {
			es = append(es, next_(t, r))
		}
		gids := map[string]bool{}
		arrivals := []float64{}
		seqs := []int{}
		for _, e := range es {
			gids[e.Global_id()] = true
			arrivals = append(arrivals, e.Stream().Arrived_at)
			seqs = append(seqs, e.Stream().Seq)
		}
		assertEq(t, len(gids), 5)
		assertTrue(t, sort.Float64sAreSorted(arrivals), "arrived_at monotonic")
		assertEq(t, seqs, []int{0, 1, 2, 3, 4})
	})

	t.Run("memorize_remember_stream_entry", func(t *testing.T) {
		original := must(laila.Get_active_policy())
		t.Cleanup(func() { laila.Activate_policy(original) })
		pol := must(laila.New_DefaultPolicy())
		laila.Activate_policy(pol)
		defer func() {
			comm := must(py.Getattr_path(pol, []string{"central", "communication"}))
			must(0, comm.(*laila.LAILA_IDENTIFIABLE_COMMUNICATION_).Stop())
		}()
		ch := chan_(nil)
		ch.Enqueue_inbound_([]byte("store me"), -1)
		e := next_(t, relay_timeout_(ch, 1))
		wait_result_(t, must(laila.Memorize(e)), 10)
		back := wait_result_(t, must(laila.Remember(e.Global_id())), 10)
		assertBytes(t, bytes_of_(t, back), []byte("store me"))
	})
}
