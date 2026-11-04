// Port of tests/functional/policy/communication/streaming/unit_tests/test_channels_basic.py.
//
// Tests 5, 7, 9-12, 16 and the datagram / drop-policy / race cases.
//
// Two in-process policies peered over loopback, TCP (127.0.0.1) or UDP.
//
//  5. Open + bidirectional byte-exact transfer for sizes 0 .. 300 KB, in
//     order, with provenance metadata; ``laila.relay`` in both call forms.
//  7. Entries from ``laila.relay`` are StreamEntry with the
//     ``Entry.constant`` contract and can be memorized.
//  9. RPC (ping, remote memorize/remember) interleaves with a saturated
//     stream and stays fast.
//  10. Both directions stream at once on two channels each.
//  11. msgpack codec: streams and RPC share the link.
//  12. Several channels per peer are isolated; closing one does not affect
//     the others and ends the peer's relay for that channel only.
//  16. Peer loss ends the relay with ``StopIteration``; ``send`` raises;
//     after reconnect, re-indexing gives a fresh working channel.
//     - UDP: single-datagram lanes, MTU ``ValueError``.
//     - Drop-oldest by count with consumer-visible seq gaps.
//     - Simultaneous open from both sides converges on one channel.
//     - Control frames never reach the policy's ``_execute_rpc``.
package unit_tests

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

var SIZES = []int{0, 1, 1024, 4096, 65536, 300_000}

func enter_(t *testing.T, tp *TwoPolicies) *TwoPolicies {
	t.Helper()
	out, err := tp.Enter()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(out.Exit)
	return out
}

func carrier_(p laila.Comm_protocol_) *laila.CarrierRPCProtocol_ { return laila.As_carrier_(p) }

// ----------------------------------------------------------------------
// TestOpenAndTransfer -- Test 5.
// ----------------------------------------------------------------------

func roundtrip_(t *testing.T, kind string, sizes []int) {
	original := must(laila.Get_active_policy())
	t.Cleanup(func() { laila.Activate_policy(original) })
	tp := enter_(t, New_TwoPolicies(kind))
	ch_a := channel_(t, tp.Ca(), tp.Bid, "video")
	assertTrue(t, ch_a.Opened(), "channel not opened")
	// same object on repeated indexing, name visible on both sides
	if channel_(t, tp.Ca(), tp.Bid, "video") != ch_a {
		t.Fatal("re-indexing returned a different channel")
	}
	assertTrue(t, Wait_until(func() bool {
		for _, n := range channels_(t, tp.Cb(), tp.Aid) {
			if n == "video" {
				return true
			}
		}
		return false
	}), "B never saw the channel")
	ch_b := channel_(t, tp.Cb(), tp.Aid, "video")
	assertEq(t, *ch_a.Tx_lane_id(), ch_b.Lane_id)
	assertEq(t, *ch_b.Tx_lane_id(), ch_a.Lane_id)

	payloads := [][]byte{}
	for i, n := range sizes {
		payloads = append(payloads, blob_(n, i))
	}
	t_send := py.Monotonic()
	for _, p := range payloads {
		must(0, ch_a.Send(p))
	}
	laila.Activate_policy(tp.B)
	relay := must(laila.Get_relay(tp.Aid, "video", py.Kw{"timeout": 10}))
	if must(laila.Get_relay(ch_b, nil)) != relay {
		t.Fatal("laila.relay(channel) must return the same Relay")
	}
	got := []*laila.StreamEntry{}
	for range payloads {
		got = append(got, next_(t, relay))
	}
	for i, g := range got {
		if !bytes.Equal(bytes_of_(t, g), payloads[i]) {
			t.Fatalf("payload %d mismatch", i)
		}
		assertEq(t, g.Stream().Seq, i)
		assertEq(t, g.Stream().Peer_id, tp.Aid)
		assertEq(t, g.Stream().Channel, "video")
		assertEq(t, g.Stream().Lane, ch_b.Lane_id)
		if g.Stream().Arrived_at < t_send {
			t.Fatal("arrived_at before send")
		}
		assertEq(t, g.Stream().Dropped_before, 0)
	}
	assertEq(t, ch_b.Dropped(), 0)
	assertEq(t, ch_b.Discarded(), 0)
	assertEq(t, ch_a.Tx_seq(), len(payloads))

	// reverse direction on the same channel
	for i := len(payloads) - 1; i >= 0; i-- {
		must(0, ch_b.Send(payloads[i]))
	}
	r_a := relay_(t, ch_a, 10)
	for i := len(payloads) - 1; i >= 0; i-- {
		if !bytes.Equal(bytes_of_(t, next_(t, r_a)), payloads[i]) {
			t.Fatalf("reverse payload %d mismatch", i)
		}
	}
	// RPC untouched
	assertTrue(t, tp.Pa.Ping(tp.Bid), "ping failed")
}

func TestOpenAndTransfer_loopback(t *testing.T) { roundtrip_(t, "loopback", SIZES) }

func TestOpenAndTransfer_tcp(t *testing.T) { roundtrip_(t, "tcp", SIZES) }

func TestOpenAndTransfer_udp_single_datagram(t *testing.T) {
	original := must(laila.Get_active_policy())
	t.Cleanup(func() { laila.Activate_policy(original) })
	tp := enter_(t, New_TwoPolicies("udp"))
	ch_a := channel_(t, tp.Ca(), tp.Bid, "telemetry")
	limit := tp.Pa.(*laila.LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL_).Mtu - 7
	assertEq(t, ch_a.Max_message_bytes, limit)
	payloads := [][]byte{}
	for _, n := range []int{0, 1, 100, limit} {
		payloads = append(payloads, blob_(n, n))
	}
	for _, p := range payloads {
		must(0, ch_a.Send(p))
	}
	ch_b := channel_(t, tp.Cb(), tp.Aid, "telemetry")
	for i := range payloads {
		if !bytes.Equal(bytes_of_(t, next_(t, relay_(t, ch_b, 10))), payloads[i]) {
			t.Fatalf("payload %d mismatch", i)
		}
	}
	assertRaises(t, py.ValueError, ch_a.Send(bytes.Repeat([]byte("x"), limit+1)))
	assertRaises(t, py.ValueError, ch_a.Send(bytes.Repeat([]byte("x"), 5000)))
	assertTrue(t, tp.Pa.Ping(tp.Bid), "ping failed")
	// RPC fragmentation/ack path still fine after lane traffic
	e := must(laila.Entry_constant(bytes.Repeat([]byte("y"), 5000)))
	laila.Activate_policy(tp.A)
	wait_result_(t, must(laila.Memorize(e, py.Kw{"dst_policy": tp.Bid})), 20)
	back := wait_result_(t, must(laila.Remember(e.Global_id(), py.Kw{"dst_policy": tp.Bid})), 20)
	assertTrue(t, bytes.Equal(bytes_of_(t, back), bytes.Repeat([]byte("y"), 5000)), "remembered data mismatch")
}

// ----------------------------------------------------------------------
// TestStreamEntryOverTransport -- Test 7.
// ----------------------------------------------------------------------

func TestStreamEntryOverTransport_entries_are_constant_entries(t *testing.T) {
	original := must(laila.Get_active_policy())
	t.Cleanup(func() { laila.Activate_policy(original) })
	for _, kind := range []string{"loopback", "tcp"} {
		t.Run(kind, func(t *testing.T) {
			tp := enter_(t, New_TwoPolicies(kind))
			ch_a := channel_(t, tp.Ca(), tp.Bid, "video")
			ch_b := channel_(t, tp.Cb(), tp.Aid, "video")
			payloads := [][]byte{blob_(3, 99), blob_(300, 99), blob_(30_000, 99)}
			for _, p := range payloads {
				must(0, ch_a.Send(p))
			}
			laila.Activate_policy(tp.B)
			es := []*laila.StreamEntry{}
			for range payloads {
				es = append(es, next_(t, relay_(t, ch_b, 10)))
			}
			ref := must(laila.Entry_constant([]byte("x")))
			for i, e := range es {
				assertTrue(t, py.Isinstance(e, laila.StreamEntry_class), "not a StreamEntry")
				assertTrue(t, py.Isinstance(e, laila.Entry_class), "not an Entry")
				data := must(e.Data())
				b, ok := data.([]byte)
				assertTrue(t, ok, fmt.Sprintf("data is %T, want bytes", data))
				assertTrue(t, bytes.Equal(b, payloads[i]), "data mismatch")
				assertEq(t, e.State(), laila.EntryState_READY)
				assertTrue(t, e.Evolution() == nil, "evolution must be None")
				assertEq(t, str_list_(e.Scopes()), str_list_(ref.Scopes()))
			}
			gids := map[string]bool{}
			for _, e := range es {
				gids[e.Global_id()] = true
			}
			assertEq(t, len(gids), 3)
			arr := []float64{}
			for _, e := range es {
				arr = append(arr, e.Stream().Arrived_at)
			}
			assertTrue(t, sort.Float64sAreSorted(arr), "arrived_at not monotonic")
			// a stream entry can be memorized / remembered like any constant
			wait_result_(t, must(laila.Memorize(es[1])), 10)
			back := wait_result_(t, must(laila.Remember(es[1].Global_id())), 10)
			assertTrue(t, bytes.Equal(bytes_of_(t, back), payloads[1]), "remembered data mismatch")
		})
	}
}

// ----------------------------------------------------------------------
// TestInterleaving -- Tests 9 and 11.
// ----------------------------------------------------------------------

func rpc_under_load_(t *testing.T, kind string, proto_kwargs py.Kw) (median, p99 float64, sent, received int64) {
	original := must(laila.Get_active_policy())
	t.Cleanup(func() { laila.Activate_policy(original) })
	tp := enter_(t, New_TwoPolicies(kind, proto_kwargs))
	ch_a := channel_(t, tp.Ca(), tp.Bid, "video")
	ch_b := channel_(t, tp.Cb(), tp.Aid, "video")
	stop := py.NewEvent()
	var sent_n, received_n atomic.Int64
	big := blob_(2_000_000, 1)

	producer := func() {
		// ~2 MB every ms requested (2 GB/s) saturates the link while
		// still yielding like a real frame source would.
		for !stop.Is_set() {
			if err := ch_a.Send(big); err != nil {
				if py.IsInstance(err, py.ConnectionError) {
					return
				}
				panic(err)
			}
			sent_n.Add(1)
			sleep(0.001)
		}
	}
	var consumer_err error
	consumer := func() {
		consumer_err = relay_(t, ch_b).Range(func(e *laila.StreamEntry) error {
			if n := len(bytes_of_(t, e)); n != len(big) {
				return fmt.Errorf("got %d bytes, want %d", n, len(big))
			}
			received_n.Add(1)
			if stop.Is_set() {
				return py.StopIteration.New()
			}
			return nil
		})
		if consumer_err != nil && py.IsInstance(consumer_err, py.StopIteration) {
			consumer_err = nil
		}
	}

	tp_ := py.New_Thread(py.Kw{"target": producer, "daemon": true})
	tc := py.New_Thread(py.Kw{"target": consumer, "daemon": true})
	must(0, tp_.Start())
	must(0, tc.Start())
	rtts := []float64{}
	func() {
		defer func() {
			stop.Set()
			ch_a.Close()
		}()
		deadline := py.Monotonic() + 3.0
		for py.Monotonic() < deadline || len(rtts) < 3 {
			t0 := py.Monotonic()
			assertTrue(t, tp.Pa.Ping(tp.Bid), "ping failed under load")
			rtts = append(rtts, py.Monotonic()-t0)
			sleep(0.05)
		}
		laila.Activate_policy(tp.A)
		e := must(laila.Entry_constant(blob_(10_000, 5)))
		wait_result_(t, must(laila.Memorize(e, py.Kw{"dst_policy": tp.Bid})), 30)
		back := wait_result_(t, must(laila.Remember(e.Global_id(), py.Kw{"dst_policy": tp.Bid})), 30)
		assertTrue(t, bytes.Equal(bytes_of_(t, back), bytes_of_(t, e)), "remembered data mismatch")
	}()
	must(0, tp_.Join(5))
	must(0, tc.Join(5))
	if consumer_err != nil {
		t.Fatal(consumer_err)
	}
	sent, received = sent_n.Load(), received_n.Load()
	assertTrue(t, sent > 5, fmt.Sprintf("sent=%d", sent))
	assertTrue(t, received > 0, "nothing received")
	assertTrue(t, len(rtts) >= 3, "too few pings")
	sorted := append([]float64(nil), rtts...)
	sort.Float64s(sorted)
	p99 = sorted[int(0.99*float64(len(sorted)-1))]
	// RPC stayed interactive under a saturated stream.
	if ping_timeout := carrier_(tp.Pa).Ping_timeout; p99 >= ping_timeout/2 {
		t.Fatalf("ping p99=%.3fs", p99)
	}
	assertTrue(t, tp.Ca().Peers.Contains(tp.Bid), "peer lost")
	if n := len(sorted); n%2 == 1 {
		median = sorted[n/2]
	} else {
		median = (sorted[n/2-1] + sorted[n/2]) / 2
	}
	return median, p99, sent, received
}

func TestInterleaving_rpc_interleaves_with_stream_tcp(t *testing.T) {
	med, p99, s, r := rpc_under_load_(t, "tcp", py.Kw{})
	t.Logf("[tcp json] ping median=%.2fms p99=%.2fms sent=%d rcvd=%d", med*1e3, p99*1e3, s, r)
}

func TestInterleaving_rpc_interleaves_with_stream_loopback(t *testing.T) {
	rpc_under_load_(t, "loopback", py.Kw{})
}

func TestInterleaving_msgpack_codec_shares_link(t *testing.T) {
	med, p99, s, r := rpc_under_load_(t, "tcp", py.Kw{"codec": "msgpack"})
	t.Logf("[tcp msgpack] ping median=%.2fms p99=%.2fms sent=%d rcvd=%d", med*1e3, p99*1e3, s, r)
}

// ----------------------------------------------------------------------
// TestBidirectionalAndMultiChannel -- Tests 10 and 12.
// ----------------------------------------------------------------------

type sideKey_ struct {
	side, name string
	i          int
}

func TestBidirectionalAndMultiChannel_both_directions_two_channels(t *testing.T) {
	for _, kind := range []string{"loopback", "tcp"} {
		t.Run(kind, func(t *testing.T) {
			tp := enter_(t, New_TwoPolicies(kind))
			names := []string{"video", "audio"}
			a_ch := map[string]*laila.Channel{}
			b_ch := map[string]*laila.Channel{}
			for _, n := range names {
				a_ch[n] = channel_(t, tp.Ca(), tp.Bid, n)
				b_ch[n] = channel_(t, tp.Cb(), tp.Aid, n)
			}
			n_msgs := 60
			var mu sync.Mutex
			digests := map[sideKey_]string{}
			results := map[[2]string][]string{}

			send_all := func(side string, chans map[string]*laila.Channel) {
				for i := 0; i < n_msgs; i++ {
					for _, n := range names {
						p := blob_(20_000+i, int(py.Sha256([]byte(side+n+py.Str(i)))[0])<<8|int(py.Sha256([]byte(side+n+py.Str(i)))[1]))
						mu.Lock()
						digests[sideKey_{side, n, i}] = py.Sha256_hex(p)
						mu.Unlock()
						must(0, chans[n].Send(p))
					}
				}
			}
			recv_all := func(side string, chans map[string]*laila.Channel) {
				for _, n := range names {
					got := []string{}
					for i := 0; i < n_msgs; i++ {
						got = append(got, py.Sha256_hex(bytes_of_(t, next_(t, relay_(t, chans[n], 20)))))
					}
					mu.Lock()
					results[[2]string{side, n}] = got
					mu.Unlock()
				}
			}

			ts := []*py.Thread{
				py.New_Thread(py.Kw{"target": func() { send_all("a", a_ch) }}),
				py.New_Thread(py.Kw{"target": func() { send_all("b", b_ch) }}),
				py.New_Thread(py.Kw{"target": func() { recv_all("a", a_ch) }}), // a receives b's
				py.New_Thread(py.Kw{"target": func() { recv_all("b", b_ch) }}),
			}
			for _, th := range ts {
				must(0, th.Start())
			}
			for _, th := range ts {
				must(0, th.Join(60))
				assertTrue(t, !th.Is_alive(), "thread still alive")
			}
			for _, n := range names {
				want_b, want_a := []string{}, []string{}
				for i := 0; i < n_msgs; i++ {
					want_b = append(want_b, digests[sideKey_{"a", n, i}])
					want_a = append(want_a, digests[sideKey_{"b", n, i}])
				}
				assertEq(t, str_list_(results[[2]string{"b", n}]), str_list_(want_b))
				assertEq(t, str_list_(results[[2]string{"a", n}]), str_list_(want_a))
			}
			for _, ch := range []*laila.Channel{a_ch["video"], a_ch["audio"], b_ch["video"], b_ch["audio"]} {
				assertEq(t, ch.Dropped(), 0)
				assertEq(t, ch.Discarded(), 0)
			}
			assertTrue(t, tp.Pa.Ping(tp.Bid), "ping failed")
		})
	}
}

func TestBidirectionalAndMultiChannel_channels_are_isolated_and_close_independently(t *testing.T) {
	for _, kind := range []string{"loopback", "tcp"} {
		t.Run(kind, func(t *testing.T) {
			tp := enter_(t, New_TwoPolicies(kind))
			names := []string{"video", "telemetry", "audio"}
			a_ch := map[string]*laila.Channel{}
			for _, n := range names {
				a_ch[n] = channel_(t, tp.Ca(), tp.Bid, n)
			}
			lanes := map[int]bool{}
			for _, c := range a_ch {
				lanes[c.Lane_id] = true
			}
			assertEq(t, len(lanes), 3)
			assertEq(t, str_list_(sorted_(channels_(t, tp.Ca(), tp.Bid))), str_list_(sorted_(names)))
			assertTrue(t, Wait_until(func() bool {
				return py.Eq(str_list_(sorted_(channels_(t, tp.Cb(), tp.Aid))), str_list_(sorted_(names)))
			}), "B never saw all channels")
			b_ch := map[string]*laila.Channel{}
			for _, n := range names {
				b_ch[n] = channel_(t, tp.Cb(), tp.Aid, n)
			}
			for _, n := range names {
				for i := 0; i < 5; i++ {
					must(0, a_ch[n].Send([]byte(fmt.Sprintf("%s:%d", n, i))))
				}
			}
			for _, n := range names {
				for i := 0; i < 5; i++ {
					assertEq(t, string(bytes_of_(t, next_(t, relay_(t, b_ch[n], 10)))), fmt.Sprintf("%s:%d", n, i))
				}
			}

			// close one on A; B's matching relay ends, the others live on
			a_ch["telemetry"].Close()
			assertTrue(t, a_ch["telemetry"].Closed(), "A telemetry not closed")
			assertTrue(t, Wait_until(func() bool { return b_ch["telemetry"].Closed() }), "B telemetry never closed")
			_, err := relay_(t, b_ch["telemetry"], 5).Next()
			assertRaises(t, py.StopIteration, err)
			assertEq(t, *b_ch["telemetry"].Closed_reason(), "closed by peer")
			assertRaises(t, py.ConnectionError, a_ch["telemetry"].Send([]byte("nope")))
			assertEq(t, str_list_(sorted_(channels_(t, tp.Ca(), tp.Bid))), py.NewList("audio", "video"))
			assertTrue(t, Wait_until(func() bool {
				return py.Eq(str_list_(sorted_(channels_(t, tp.Cb(), tp.Aid))), py.NewList("audio", "video"))
			}), "B still lists telemetry")
			must(0, a_ch["video"].Send([]byte("still")))
			must(0, b_ch["audio"].Send([]byte("alive")))
			assertEq(t, string(bytes_of_(t, next_(t, relay_(t, b_ch["video"], 10)))), "still")
			assertEq(t, string(bytes_of_(t, next_(t, relay_(t, a_ch["audio"], 10)))), "alive")

			// re-indexing a closed name opens a fresh channel
			new := channel_(t, tp.Ca(), tp.Bid, "telemetry")
			assertTrue(t, new != a_ch["telemetry"], "re-index returned the closed channel")
			assertTrue(t, new.Opened(), "new channel not opened")
			must(0, new.Send([]byte("again")))
			assertTrue(t, Wait_until(func() bool {
				for _, n := range channels_(t, tp.Cb(), tp.Aid) {
					if n == "telemetry" {
						return true
					}
				}
				return false
			}), "B never saw the re-opened channel")
			nb := channel_(t, tp.Cb(), tp.Aid, "telemetry")
			assertEq(t, string(bytes_of_(t, next_(t, relay_(t, nb, 10)))), "again")
		})
	}
}

// ----------------------------------------------------------------------
// TestLifecycle -- Test 16 + drop policy + race + control isolation.
// ----------------------------------------------------------------------

func TestLifecycle_peer_loss_ends_relay_and_reconnect_gives_new_channel(t *testing.T) {
	tp := enter_(t, New_TwoPolicies("tcp"))
	ch_a := channel_(t, tp.Ca(), tp.Bid, "video")
	ch_b := channel_(t, tp.Cb(), tp.Aid, "video")
	must(0, ch_a.Send([]byte("one")))
	relay_b := relay_(t, ch_b, 10)
	assertEq(t, string(bytes_of_(t, next_(t, relay_b))), "one")
	// timeout form
	relay_b.Timeout = py.Ptr(0.2)
	_, err := relay_b.Next()
	assertRaises(t, py.TimeoutError, err)
	// A drops B
	tp.Ca().Remove_peer(tp.Bid)
	assertTrue(t, ch_a.Closed(), "A channel not closed")
	assertRaises(t, py.ConnectionError, ch_a.Send([]byte("x")))
	_, err = relay_(t, ch_a).Next()
	assertRaises(t, py.StopIteration, err)
	// B notices the socket EOF: its relay ends too
	assertTrue(t, Wait_until(func() bool { return ch_b.Closed() }, 10), "B channel never closed")
	relay_b.Timeout = py.Ptr(5.0)
	_, err = relay_b.Next()
	assertRaises(t, py.StopIteration, err)
	assertEq(t, *ch_b.Closed_reason(), "peer disconnected")
	assertTrue(t, Wait_until(func() bool { return !tp.Cb().Peers.Contains(tp.Aid) }, 10), "B still lists A")

	// reconnect: new proxy, new channel; old objects stay closed
	bid2 := must(tp.Ca().Add_peer(fmt.Sprintf("tcp://127.0.0.1:%d", Bound_port(tp.Pb)), Secret_of(tp.Pb)))
	assertEq(t, bid2, tp.Bid)
	assertTrue(t, Wait_until(func() bool { return tp.Cb().Peers.Contains(tp.Aid) }, 10), "B never re-registered A")
	new_a := channel_(t, tp.Ca(), tp.Bid, "video")
	assertTrue(t, new_a != ch_a, "re-index returned the old channel")
	assertTrue(t, ch_a.Closed(), "old A channel reopened")
	must(0, new_a.Send([]byte("two")))
	new_b := channel_(t, tp.Cb(), tp.Aid, "video")
	assertTrue(t, new_b != ch_b, "B re-index returned the old channel")
	assertEq(t, string(bytes_of_(t, next_(t, relay_(t, new_b, 10)))), "two")
}

func TestLifecycle_comm_stop_closes_channels(t *testing.T) {
	tp := enter_(t, New_TwoPolicies("loopback"))
	ch_a := channel_(t, tp.Ca(), tp.Bid, "video")
	ch_b := channel_(t, tp.Cb(), tp.Aid, "video")
	ended := py.NewEvent()

	consume := func() {
		_ = relay_(t, ch_b).Range(func(*laila.StreamEntry) error { return nil })
		ended.Set()
	}
	must(0, py.New_Thread(py.Kw{"target": consume, "daemon": true}).Start())
	sleep(0.1)
	must(0, tp.Cb().Stop())
	assertTrue(t, ended.Wait(5), "relay never ended")
	assertTrue(t, ch_b.Closed(), "B channel not closed")
	must(0, tp.Ca().Stop())
	assertTrue(t, ch_a.Closed(), "A channel not closed")
}

func TestLifecycle_drop_oldest_visible_to_consumer(t *testing.T) {
	for _, kind := range []string{"loopback", "tcp"} {
		t.Run(kind, func(t *testing.T) {
			tp := enter_(t, New_TwoPolicies(kind, py.Kw{"channel_queue_size": 4}))
			ch_a := channel_(t, tp.Ca(), tp.Bid, "video")
			ch_b := channel_(t, tp.Cb(), tp.Aid, "video")
			for i := 0; i < 20; i++ {
				must(0, ch_a.Send([]byte{byte(i)}))
			}
			// Both the sender's link queue and the receiver's lane queue
			// are bounded (drop-oldest); between them 16 of 20 go.
			assertTrue(t, Wait_until(func() bool { return ch_b.Rx_seq()+ch_a.Tx_dropped() == 20 }, 10),
				fmt.Sprintf("rx_seq=%d tx_dropped=%d", ch_b.Rx_seq(), ch_a.Tx_dropped()))
			assertEq(t, ch_b.Dropped()+ch_a.Tx_dropped(), 16)
			assertEq(t, ch_b.Qsize(), 4)
			if kind == "loopback" {
				assertEq(t, ch_b.Dropped(), 16) // no send queue on loopback
			}
			got := []*laila.StreamEntry{}
			for i := 0; i < 4; i++ {
				got = append(got, next_(t, relay_(t, ch_b, 5)))
			}
			for i, g := range got {
				assertTrue(t, bytes.Equal(bytes_of_(t, g), []byte{byte(0x10 + i)}), "wrong survivor")
			}
			rx := ch_b.Rx_seq()
			for i, g := range got {
				assertEq(t, g.Stream().Seq, rx-4+i)
			}
			assertEq(t, got[0].Stream().Seq, ch_b.Dropped())
			// oversize rejected locally
			carrier_(tp.Pa).Max_stream_frame_bytes = 1024
			big_ch := channel_(t, tp.Ca(), tp.Bid, "big")
			assertRaises(t, py.ValueError, big_ch.Send(bytes.Repeat([]byte("x"), 2048)))
		})
	}
}

func TestLifecycle_simultaneous_open_converges(t *testing.T) {
	for _, kind := range []string{"loopback", "tcp"} {
		t.Run(kind, func(t *testing.T) {
			tp := enter_(t, New_TwoPolicies(kind))
			var out_a, out_b *laila.Channel
			open_a := func() { out_a = channel_(t, tp.Ca(), tp.Bid, "race") }
			open_b := func() { out_b = channel_(t, tp.Cb(), tp.Aid, "race") }
			ts := []*py.Thread{py.New_Thread(py.Kw{"target": open_a}), py.New_Thread(py.Kw{"target": open_b})}
			for _, th := range ts {
				must(0, th.Start())
			}
			for _, th := range ts {
				must(0, th.Join(30))
			}
			ch_a, ch_b := out_a, out_b
			assertTrue(t, ch_a.Opened() && ch_b.Opened(), "channels not opened")
			assertEq(t, str_list_(channels_(t, tp.Ca(), tp.Bid)), py.NewList("race"))
			assertEq(t, str_list_(channels_(t, tp.Cb(), tp.Aid)), py.NewList("race"))
			assertTrue(t, channel_(t, tp.Ca(), tp.Bid, "race") == ch_a, "A re-index differs")
			assertTrue(t, channel_(t, tp.Cb(), tp.Aid, "race") == ch_b, "B re-index differs")
			assertEq(t, *ch_a.Tx_lane_id(), ch_b.Lane_id)
			assertEq(t, *ch_b.Tx_lane_id(), ch_a.Lane_id)
			must(0, ch_a.Send([]byte("from a")))
			must(0, ch_b.Send([]byte("from b")))
			assertEq(t, string(bytes_of_(t, next_(t, relay_(t, ch_b, 10)))), "from a")
			assertEq(t, string(bytes_of_(t, next_(t, relay_(t, ch_a, 10)))), "from b")
		})
	}
}

func TestLifecycle_control_frames_never_reach_policy(t *testing.T) {
	original := must(laila.Get_active_policy())
	t.Cleanup(func() { laila.Activate_policy(original) })
	for _, kind := range []string{"loopback", "tcp"} {
		t.Run(kind, func(t *testing.T) {
			tp := enter_(t, New_TwoPolicies(kind))
			var mu sync.Mutex
			seen := [][]any{}
			hub := tp.Cb()
			orig := hub.Execute_rpc_
			spy := func(path []string, args []any, kwargs py.Kw) (any, error) {
				mu.Lock()
				seen = append(seen, []any{path, args, kwargs})
				mu.Unlock()
				return orig(path, args, kwargs)
			}
			// mock.patch.object(hub_cls, "_execute_rpc", spy)
			hub.Private_set("_execute_rpc", spy)
			defer hub.Private_del("_execute_rpc")
			ch_a := channel_(t, tp.Ca(), tp.Bid, "video")
			must(0, ch_a.Send([]byte("data")))
			ch_b := channel_(t, tp.Cb(), tp.Aid, "video")
			assertEq(t, string(bytes_of_(t, next_(t, relay_(t, ch_b, 10)))), "data")
			ch_a.Close()
			assertTrue(t, Wait_until(func() bool { return ch_b.Closed() }), "B channel never closed")
			assertTrue(t, tp.Pa.Ping(tp.Bid), "ping failed")
			mu.Lock()
			n := len(seen)
			mu.Unlock()
			assertEq(t, n, 0)
			// a real RPC does go through it
			laila.Activate_policy(tp.A)
			e := must(laila.Entry_constant([]byte("rpc")))
			wait_result_(t, must(laila.Memorize(e, py.Kw{"dst_policy": tp.Bid})), 20)
			mu.Lock()
			n = len(seen)
			mu.Unlock()
			assertEq(t, n, 1)
		})
	}
}

var _ = time.Second
