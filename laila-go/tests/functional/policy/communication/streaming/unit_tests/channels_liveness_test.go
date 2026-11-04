// Port of tests/functional/policy/communication/streaming/unit_tests/test_channels_liveness.py.
//
// Tests 8 and 14: liveness under streaming load, and detection of a dead
// peer.
//
//  8. Heavy stream with a tight ``liveness_interval``: the peer is never
//     falsely dropped; when the sender's transport dies mid-stream the
//     receiver's relay ends with ``StopIteration`` within the liveness
//     budget and ``send`` raises.
//  14. (slow -- skipped under ``go test -short``) Sustained max-rate
//     streaming for ``LAILA_STREAM_STRESS_SECONDS`` (default 10) with
//     ``liveness_interval=0.5``: zero false drops, ping RTT under budget
//     the whole time, memory bounded by the drop-oldest policy.
package unit_tests

import (
	"bytes"
	"os"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

var STRESS_SECONDS = func() float64 {
	if v := os.Getenv("LAILA_STREAM_STRESS_SECONDS"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return 10.0
}()

// ----------------------------------------------------------------------
// TestLivenessDuringStream -- Test 8.
// ----------------------------------------------------------------------

func TestLivenessDuringStream_stream_then_peer_death_is_detected(t *testing.T) {
	tp := enter_(t, New_TwoPolicies("tcp", py.Kw{"liveness_interval": 0.3, "ping_timeout": 2.0}))
	ch_a := channel_(t, tp.Ca(), tp.Bid, "video")
	ch_b := channel_(t, tp.Cb(), tp.Aid, "video")
	stop := py.NewEvent()
	frame := blob_(200_000, 8)

	producer := func() {
		for !stop.Is_set() {
			if err := ch_a.Send(frame); err != nil {
				if py.IsInstance(err, py.ConnectionError) {
					return
				}
				panic(err)
			}
			sleep(1.0 / 60)
		}
	}
	must(0, py.New_Thread(py.Kw{"target": producer, "daemon": true}).Start())
	var received atomic.Int64
	ended := py.NewEvent()

	consumer := func() {
		_ = relay_(t, ch_b).Range(func(*laila.StreamEntry) error {
			received.Add(1)
			return nil
		})
		ended.Set()
	}
	must(0, py.New_Thread(py.Kw{"target": consumer, "daemon": true}).Start())
	sleep(2.0) // several liveness rounds under load
	assertTrue(t, tp.Ca().Peers.Contains(tp.Bid), "A dropped B")
	assertTrue(t, tp.Cb().Peers.Contains(tp.Aid), "B dropped A")
	assertTrue(t, received.Load() > 30, "received="+py.Str(received.Load()))
	assertTrue(t, !ended.Is_set(), "relay ended early")

	// kill A's transport abruptly: B's socket sees EOF -> relay ends
	t0 := py.Monotonic()
	must(0, tp.Pa.Stop())
	stop.Set()
	assertTrue(t, ended.Wait(10), "relay did not end after peer death")
	detect := py.Monotonic() - t0
	t.Logf("[tcp liveness] peer death -> relay ended in %.0fms", detect*1e3)
	assertTrue(t, ch_b.Closed(), "B channel not closed")
	assertTrue(t, ch_a.Closed(), "A channel not closed")
	assertRaises(t, py.ConnectionError, ch_b.Send([]byte("x")))
	assertRaises(t, py.ConnectionError, ch_a.Send([]byte("x")))
	assertTrue(t, Wait_until(func() bool { return !tp.Cb().Peers.Contains(tp.Aid) }, 10), "B still lists A")
}

// B keeps *receiving* frames but A stops answering pings -> B drops A.
func TestLivenessDuringStream_silent_peer_is_dropped_by_liveness_while_streaming_in(t *testing.T) {
	tp := enter_(t, New_TwoPolicies("tcp", py.Kw{"liveness_interval": 0.3, "ping_timeout": 1.0}))
	ch_a := channel_(t, tp.Ca(), tp.Bid, "video")
	ch_b := channel_(t, tp.Cb(), tp.Aid, "video")
	// Make A deaf to every inbound request (so B's pings go unanswered)
	// while A keeps streaming. Instance-level patch: instance __dict__
	// wins over the class method.
	carrier_(tp.Pa).Private_set("_handle_request_frame", func(args ...any) (any, error) { return nil, nil })
	stop := py.NewEvent()
	defer stop.Set()

	producer := func() {
		for !stop.Is_set() {
			if err := ch_a.Send(bytes.Repeat([]byte("tick"), 1000)); err != nil {
				if py.IsInstance(err, py.ConnectionError) {
					return
				}
				panic(err)
			}
			sleep(0.01)
		}
	}
	must(0, py.New_Thread(py.Kw{"target": producer, "daemon": true}).Start())
	t0 := py.Monotonic()
	assertTrue(t, Wait_until(func() bool { return !tp.Cb().Peers.Contains(tp.Aid) }, 10),
		"B kept A alive on stream traffic alone")
	t.Logf("[tcp liveness] deaf-but-streaming peer dropped in %.2fs", py.Monotonic()-t0)
	assertTrue(t, Wait_until(func() bool { return ch_b.Closed() }, 5), "B channel never closed")
	// buffered frames drain, then the relay ends
	drained := 0
	must(0, relay_(t, ch_b, 5).Range(func(*laila.StreamEntry) error { drained++; return nil }))
	assertTrue(t, drained <= carrier_(tp.Pb).Channel_queue_size, "drained more than the queue bound")
	_, err := relay_(t, ch_b, 5).Next()
	assertRaises(t, py.StopIteration, err)
}

// ----------------------------------------------------------------------
// TestSustainedStress -- Test 14 (slow).
// ----------------------------------------------------------------------

type stressPair_ interface {
	Ca() *laila.LAILA_IDENTIFIABLE_COMMUNICATION_
	Cb() *laila.LAILA_IDENTIFIABLE_COMMUNICATION_
}

type stressFixture_ struct {
	pair    stressPair_
	aid     string
	bid     string
	pa, pb  *laila.CarrierRPCProtocol_
	pa_ping func(string, ...any) bool
}

func stress_(t *testing.T, make_pair func() *stressFixture_, label string, frame_bytes int, fps float64) {
	if testing.Short() {
		t.Skip("slow: sustained stress (run without -short)")
	}
	fx := make_pair()
	ch_a := channel_(t, fx.pair.Ca(), fx.bid, "video")
	ch_b := channel_(t, fx.pair.Cb(), fx.aid, "video")
	stop := py.NewEvent()
	frame := blob_(frame_bytes, 14)
	var sent, received, bad atomic.Int64

	producer := func() {
		period := 1.0 / fps
		nxt := py.Monotonic()
		for !stop.Is_set() {
			if err := ch_a.Send(frame); err != nil {
				if py.IsInstance(err, py.ConnectionError) {
					return
				}
				panic(err)
			}
			sent.Add(1)
			nxt += period
			if delay := nxt - py.Monotonic(); delay > 0 {
				sleep(delay)
			} else {
				nxt = py.Monotonic()
			}
		}
	}
	consumer := func() {
		_ = relay_(t, ch_b).Range(func(e *laila.StreamEntry) error {
			received.Add(1)
			if len(bytes_of_(t, e)) != frame_bytes {
				bad.Add(1)
			}
			return nil
		})
	}
	must(0, py.New_Thread(py.Kw{"target": producer, "daemon": true}).Start())
	must(0, py.New_Thread(py.Kw{"target": consumer, "daemon": true}).Start())
	rtts := []float64{}
	t_end := py.Monotonic() + STRESS_SECONDS
	for py.Monotonic() < t_end {
		t0 := py.Monotonic()
		ok := fx.pa_ping(fx.bid)
		rtts = append(rtts, py.Monotonic()-t0)
		assertTrue(t, ok, "ping failed under sustained load")
		sleep(0.25)
	}
	stop.Set()
	sleep(0.2)
	sorted := append([]float64(nil), rtts...)
	sort.Float64s(sorted)
	med := sorted[len(sorted)/2]
	worst := sorted[len(sorted)-1]
	rate_mb := float64(received.Load()) * float64(frame_bytes) / STRESS_SECONDS / 1e6
	t.Logf("[%s stress %.0fs] sent=%d rcvd=%d (%.1f MB/s delivered) tx_dropped=%d rx_dropped=%d discarded=%d ping median=%.1fms max=%.1fms",
		label, STRESS_SECONDS, sent.Load(), received.Load(), rate_mb, ch_a.Tx_dropped(), ch_b.Dropped(), ch_b.Discarded(), med*1e3, worst*1e3)
	assertTrue(t, fx.pair.Ca().Peers.Contains(fx.bid), "false liveness drop (A side)")
	assertTrue(t, fx.pair.Cb().Peers.Contains(fx.aid), "false liveness drop (B side)")
	assertEq(t, bad.Load(), int64(0))
	assertEq(t, ch_b.Discarded(), 0)
	assertTrue(t, worst < fx.pa.Ping_timeout, "ping max "+py.Str(worst)+"s")
	assertTrue(t, received.Load() > 0, "nothing received")
	assertTrue(t, ch_b.Qsize() <= fx.pb.Channel_queue_size, "queue over bound")
}

func two_policies_fixture_(t *testing.T, kind string, kw py.Kw) *stressFixture_ {
	tp := enter_(t, New_TwoPolicies(kind, kw))
	return &stressFixture_{pair: tp, aid: tp.Aid, bid: tp.Bid, pa: carrier_(tp.Pa), pb: carrier_(tp.Pb), pa_ping: tp.Pa.Ping}
}

func TestSustainedStress_tcp_sustained(t *testing.T) {
	stress_(t, func() *stressFixture_ {
		return two_policies_fixture_(t, "tcp", py.Kw{"liveness_interval": 0.5, "ping_timeout": 3.0})
	}, "tcp", 500_000, 120)
}

func TestSustainedStress_loopback_sustained(t *testing.T) {
	stress_(t, func() *stressFixture_ {
		return two_policies_fixture_(t, "loopback", py.Kw{"liveness_interval": 0.5})
	}, "loopback", 500_000, 240)
}

func TestSustainedStress_uart_null_modem_sustained(t *testing.T) {
	stress_(t, func() *stressFixture_ {
		m, err := New_UartNullModem(py.Kw{"liveness_interval": 0.5, "ping_timeout": 3.0}).Enter()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(m.Exit)
		return &stressFixture_{pair: m, aid: m.Aid, bid: m.Bid, pa: carrier_(m.Pa), pb: carrier_(m.Pb), pa_ping: m.Pa.Ping}
	}, "uart-nullmodem", 30_000, 30)
}
