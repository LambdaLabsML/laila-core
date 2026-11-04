//go:build workload

// Port of tests/workloads/test_w19_streaming_channels.py.
//
// W19 -- stream channels under sustained load.
//
// Byte-lane throughput (loopback / tcp / unix / udp), many lanes at once,
// bounded-queue eviction accounting, large messages, RPC interleaving while
// a stream saturates the link, multi-consumer relays, open/close churn,
// lane exhaustion (255 per peer) and memorizing streamed entries.
package workloads

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

var KINDS19_ = []string{"loopback", "tcp", "unix", "udp"}

// blob19_ is ``_blob(n, tag)``: n deterministic bytes seeded by sha256.
func blob19_(n int, tag int) []byte {
	seed := py.Sha256([]byte(fmt.Sprintf("%d:%d", n, tag)))
	if n == 0 {
		return []byte{}
	}
	reps := (n + len(seed) - 1) / len(seed)
	out := make([]byte, 0, reps*len(seed))
	for i := 0; i < reps; i++ {
		out = append(out, seed...)
	}
	return out[:n]
}

// two19_ is ``TwoPolicies(kind, liveness_interval=60.0, **kw)`` entered,
// ``__exit__`` registered on the test.
func two19_(t *testing.T, kind string, kw ...py.Kw) *TwoPolicies {
	t.Helper()
	opts := py.MergeKw(kw...)
	opts["liveness_interval"] = 60.0
	tp, err := New_TwoPolicies(kind, opts).Enter()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tp.Exit)
	return tp
}

// tp19 is the parametrized ``tp`` fixture (one kind).
func tp19(t *testing.T, kind string, kw ...py.Kw) *TwoPolicies {
	t.Helper()
	Fresh_policy(t)
	tp := two19_(t, kind, kw...)
	t.Cleanup(func() {
		Quiesce(tp.A)
		Quiesce(tp.B)
	})
	return tp
}

// tp_big19 is the ``tp_big`` fixture: peers whose per-link send queue and
// per-lane rx queue are large enough that a burst of ``n`` messages is
// never evicted (drop-oldest is the documented default; lossless
// throughput needs a big enough bound).
func tp_big19(t *testing.T, kind string) *TwoPolicies {
	t.Helper()
	return tp19(t, kind, py.Kw{"channel_queue_size": 100_000})
}

// for_kinds19 runs body once per transport kind.
func for_kinds19(t *testing.T, body func(t *testing.T, kind string)) {
	for _, kind := range KINDS19_ {
		kind := kind
		t.Run("kind="+kind, func(t *testing.T) { body(t, kind) })
	}
}

// peer19_ is ``comm.peers[gid]``.
func peer19_(t *testing.T, c *laila.LAILA_IDENTIFIABLE_COMMUNICATION_, gid string) *laila.PeerProxy {
	t.Helper()
	v, ok := c.Peers.Get(gid)
	if !ok {
		t.Fatalf("KeyError: %s", gid)
	}
	return v.(*laila.PeerProxy)
}

// chan19_ is ``comm.peers[gid][name]``.
func chan19_(t *testing.T, c *laila.LAILA_IDENTIFIABLE_COMMUNICATION_, gid string, name string) *laila.Channel {
	t.Helper()
	ch, err := peer19_(t, c, gid).Channel(name)
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

// chans19_ is ``comm.peers[gid].channels()``.
func chans19_(t *testing.T, c *laila.LAILA_IDENTIFIABLE_COMMUNICATION_, gid string) []string {
	t.Helper()
	v, ok := c.Peers.Get(gid)
	if !ok {
		return []string{}
	}
	names, err := v.(*laila.PeerProxy).Channels()
	if err != nil {
		t.Fatal(err)
	}
	return names
}

// has19_ is ``name in names``.
func has19_(names []string, name string) bool {
	for _, n := range names {
		if n == name {
			return true
		}
	}
	return false
}

// pair19_ is ``_pair(tp, name="video")``: the channel on A and its twin on B.
func pair19_(t *testing.T, tp *TwoPolicies, name ...string) (*laila.Channel, *laila.Channel) {
	t.Helper()
	nm := "video"
	if len(name) > 0 {
		nm = name[0]
	}
	ch_a := chan19_(t, tp.Ca(), tp.Bid, nm)
	assertTrue(t, Wait_until(func() bool { return has19_(chans19_(t, tp.Cb(), tp.Aid), nm) }, 15))
	ch_b := chan19_(t, tp.Cb(), tp.Aid, nm)
	return ch_a, ch_b
}

// relay19_ is ``laila.relay(ch, timeout=timeout)``.
func relay19_(ch *laila.Channel, timeout ...float64) *laila.Relay {
	kw := py.Kw{}
	if len(timeout) > 0 {
		kw["timeout"] = timeout[0]
	}
	return must(laila.Get_relay(ch, nil, kw))
}

// drain19_ is ``_drain(ch, n, timeout=30.0)``: ``[next(r) for _ in range(n)]``.
func drain19_(ch *laila.Channel, n int, timeout ...float64) ([]*laila.StreamEntry, error) {
	to := 30.0
	if len(timeout) > 0 {
		to = timeout[0]
	}
	r := relay19_(ch, to)
	out := make([]*laila.StreamEntry, 0, n)
	for i := 0; i < n; i++ {
		e, err := r.Next()
		if err != nil {
			return out, err
		}
		out = append(out, e)
	}
	return out, nil
}

// data19_ is ``entry.data`` as bytes.
func data19_(e *laila.StreamEntry) []byte {
	v := must(e.Data())
	switch b := v.(type) {
	case []byte:
		return b
	case string:
		return []byte(b)
	}
	panic(py.TypeError.Newf("stream data is %s, want bytes", py.Pytypename(v)))
}

// unwrap19_ is ``unwrap(v)`` for any Entry subclass (the shared Unwrap
// only matches ``*laila.Entry``; ``isinstance`` also covers StreamEntry).
func unwrap19_(v any) any {
	for {
		e, ok := v.(interface{ Data() (any, error) })
		if !ok {
			return v
		}
		v = must(e.Data())
	}
}

// be19_ is ``i.to_bytes(width, "big")``.
func be19_(i int, width int) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf[8-width:]
}

// from_be19_ is ``int.from_bytes(b, "big")``.
func from_be19_(b []byte) int {
	v := 0
	for _, x := range b {
		v = v<<8 | int(x)
	}
	return v
}

// bytes_list19_ builds a *py.List of byte strings for assertEq.
func bytes_list19_(items [][]byte) *py.List {
	l := py.NewList()
	for _, b := range items {
		l.Append(b)
	}
	return l
}

// ---------------------------------------------------------------------------
// TestThroughput
// ---------------------------------------------------------------------------

func TestThroughput19_many_small_messages_in_order(t *testing.T) {
	for_kinds19(t, func(t *testing.T, kind string) {
		tp := tp_big19(t, kind)
		if tp.Kind == "udp" {
			t.Skip("udp stream lanes are lossy by design: see test_udp_lossy_stream_is_ordered_subsequence")
		}
		n := N_scaled(5000)
		ch_a, ch_b := pair19_(t, tp)
		got := [][]byte{}
		var consume_err error
		done := py.NewEvent()

		consume := func() {
			defer done.Set()
			r := relay19_(ch_b, 60)
			for i := 0; i < n; i++ {
				e, err := r.Next()
				if err != nil {
					consume_err = err
					return
				}
				got = append(got, data19_(e))
			}
		}

		th := py.New_Thread(py.Kw{"target": consume, "daemon": true})
		check(t, th.Start())
		sw := Timed(func() {
			for i := 0; i < n; i++ {
				check(t, ch_a.Send(be19_(i, 4)))
				if tp.Kind == "udp" && i%50 == 0 {
					py.Sleep_sync(0.001)
				}
			}
			assertTrue(t, done.Wait(120))
		})
		check(t, consume_err)
		want := [][]byte{}
		for i := 0; i < n; i++ {
			want = append(want, be19_(i, 4))
		}
		assertEq(t, bytes_list19_(got), bytes_list19_(want))
		assertTrue(t, ch_b.Dropped() == 0 && ch_b.Discarded() == 0)
		assertTrue(t, sw.Elapsed < 90)
	})
}

// UDP lanes are documented lossy/unordered: what arrives must still be
// an in-order subsequence of what was sent, with every loss accounted
// for in ``discarded`` / ``dropped`` -- never a corrupted message.
func TestThroughput19_udp_lossy_stream_is_ordered_subsequence(t *testing.T) {
	Fresh_policy(t)
	tp := two19_(t, "udp", py.Kw{"channel_queue_size": 100_000})
	ch_a, ch_b := pair19_(t, tp)
	n := N_scaled(3000)
	for i := 0; i < n; i++ {
		check(t, ch_a.Send(be19_(i, 4)))
		if i%20 == 0 {
			py.Sleep_sync(0.0005)
		}
	}
	py.Sleep_sync(2.0)
	vals := []int{}
	for i, q := 0, ch_b.Qsize(); i < q; i++ {
		e, err := relay19_(ch_b, 2).Next()
		check(t, err)
		vals = append(vals, from_be19_(data19_(e)))
	}
	sorted_vals := append([]int(nil), vals...)
	sort.Ints(sorted_vals)
	assertTrue(t, py.Eq(vals, sorted_vals), "udp lane delivered out of order")
	seen := map[int]bool{}
	for _, v := range vals {
		seen[v] = true
	}
	assertTrue(t, len(seen) == len(vals), "duplicate delivery")
	for _, v := range vals {
		assertTrue(t, 0 <= v && v < n)
	}
	assertTrue(t, float64(len(vals)) >= 0.5*float64(n), fmt.Sprintf("lost more than half: %d/%d", len(vals), n))
	Quiesce(tp.A)
	Quiesce(tp.B)
}

func TestThroughput19_sustained_bulk_bytes(t *testing.T) {
	for_kinds19(t, func(t *testing.T, kind string) {
		tp := tp_big19(t, kind)
		if tp.Kind == "udp" {
			t.Skip("per-datagram message limit on udp")
		}
		msg := blob19_(256*1024, 1)
		n := N_scaled(200)
		ch_a, ch_b := pair19_(t, tp)
		total := []int{}
		var consume_err error

		consume := func() {
			r := relay19_(ch_b, 60)
			for i := 0; i < n; i++ {
				e, err := r.Next()
				if err != nil {
					consume_err = err
					return
				}
				total = append(total, len(data19_(e)))
			}
		}

		th := py.New_Thread(py.Kw{"target": consume, "daemon": true})
		check(t, th.Start())
		sw := Timed(func() {
			for i := 0; i < n; i++ {
				check(t, ch_a.Send(msg))
			}
			check(t, th.Join(180))
		})
		assertTrue(t, !th.Is_alive())
		check(t, consume_err)
		sum := 0
		for _, v := range total {
			sum += v
		}
		assertEq(t, sum, n*len(msg))
		mbps := float64(sum) / 1e6 / sw.Elapsed
		assertTrue(t, mbps > 2, fmt.Sprintf("only %.1f MB/s", mbps))
	})
}

func TestThroughput19_large_single_message(t *testing.T) {
	for _, mb := range []int{1, 8, 32} {
		mb := mb
		t.Run(fmt.Sprintf("mb=%d", mb), func(t *testing.T) {
			for_kinds19(t, func(t *testing.T, kind string) {
				tp := tp19(t, kind)
				if tp.Kind == "udp" {
					t.Skip("per-datagram message limit on udp")
				}
				ch_a, ch_b := pair19_(t, tp)
				blob := blob19_(mb*1024*1024, mb)
				check(t, ch_a.Send(blob))
				got, err := relay19_(ch_b, 120).Next()
				check(t, err)
				assertTrue(t, bytes.Equal(data19_(got), blob))
				assertEq(t, ch_b.Discarded(), 0)
			})
		})
	}
}

func TestThroughput19_over_max_frame_rejected_locally(t *testing.T) {
	for_kinds19(t, func(t *testing.T, kind string) {
		tp := tp19(t, kind)
		laila.As_carrier_(tp.Pa).Max_stream_frame_bytes = 4096
		ch_a, _ := pair19_(t, tp, "small")
		if err := ch_a.Send(bytes.Repeat([]byte("x"), 8192)); !errors.Is(err, py.ValueError) {
			t.Fatalf("expected ValueError, got %v", err)
		}
		assertEq(t, ch_a.Tx_seq(), 0)
	})
}

func TestThroughput19_bidirectional_simultaneous(t *testing.T) {
	for_kinds19(t, func(t *testing.T, kind string) {
		tp := tp_big19(t, kind)
		if tp.Kind == "udp" {
			t.Skip("udp stream lanes are lossy by design")
		}
		n := N_scaled(1000)
		ch_a, ch_b := pair19_(t, tp, "duplex")

		side := func(i int) (any, error) {
			// ch_a receives what ch_b sends and vice versa
			tx, rx := ch_a, ch_a
			tag := []byte("A")
			if i != 0 {
				tx, rx = ch_b, ch_b
				tag = []byte("B")
			}
			got := [][]byte{}
			var consume_err error
			r := relay19_(rx, 60)

			consume := func() {
				for k := 0; k < n; k++ {
					e, err := r.Next()
					if err != nil {
						consume_err = err
						return
					}
					got = append(got, data19_(e))
				}
			}

			th := py.New_Thread(py.Kw{"target": consume, "daemon": true})
			if err := th.Start(); err != nil {
				return nil, err
			}
			for k := 0; k < n; k++ {
				if err := tx.Send(append(append([]byte(nil), tag...), be19_(k, 4)...)); err != nil {
					return nil, err
				}
				if tp.Kind == "udp" && k%50 == 0 {
					py.Sleep_sync(0.001)
				}
			}
			if err := th.Join(120); err != nil {
				return nil, err
			}
			if th.Is_alive() {
				return nil, py.AssertionError.New("consumer still alive")
			}
			if consume_err != nil {
				return nil, consume_err
			}
			return got, nil
		}

		out := must(Run_threads(side, 2))
		a_got, b_got := out[0].([][]byte), out[1].([][]byte)
		want_a, want_b := [][]byte{}, [][]byte{}
		for k := 0; k < n; k++ {
			want_a = append(want_a, append([]byte("B"), be19_(k, 4)...))
			want_b = append(want_b, append([]byte("A"), be19_(k, 4)...))
		}
		assertEq(t, bytes_list19_(a_got), bytes_list19_(want_a))
		assertEq(t, bytes_list19_(b_got), bytes_list19_(want_b))
	})
}

// ---------------------------------------------------------------------------
// TestLanes
// ---------------------------------------------------------------------------

func TestLanes_many_channels_isolated(t *testing.T) {
	for_kinds19(t, func(t *testing.T, kind string) {
		tp := tp_big19(t, kind)
		names := []string{}
		for i := 0; i < N_scaled(40); i++ {
			names = append(names, fmt.Sprintf("lane%d", i))
		}
		per := N_scaled(100)
		if tp.Kind == "udp" {
			per = N_scaled(30)
		}
		chans_a := map[string]*laila.Channel{}
		for _, nm := range names {
			chans_a[nm] = chan19_(t, tp.Ca(), tp.Bid, nm)
		}
		assertTrue(t, Wait_until(func() bool {
			have := chans19_(t, tp.Cb(), tp.Aid)
			for _, nm := range names {
				if !has19_(have, nm) {
					return false
				}
			}
			return true
		}, 30))
		chans_b := map[string]*laila.Channel{}
		for _, nm := range names {
			chans_b[nm] = chan19_(t, tp.Cb(), tp.Aid, nm)
		}
		for k := 0; k < per; k++ {
			for _, nm := range names {
				check(t, chans_a[nm].Send([]byte(fmt.Sprintf("%s:%d", nm, k))))
			}
			if tp.Kind == "udp" {
				py.Sleep_sync(0.002)
			}
		}
		for _, nm := range names {
			entries, err := drain19_(chans_b[nm], per, 60)
			check(t, err)
			got, want := py.NewList(), py.NewList()
			for k, e := range entries {
				got.Append(string(data19_(e)))
				want.Append(fmt.Sprintf("%s:%d", nm, k))
			}
			assertEq(t, got, want)
		}
		for _, ch := range chans_b {
			assertTrue(t, ch.Dropped() == 0)
		}
	})
}

func TestLanes_lane_exhaustion_255(t *testing.T) {
	for_kinds19(t, func(t *testing.T, kind string) {
		tp := tp19(t, kind)
		if tp.Kind == "udp" {
			t.Skip("slow handshake storm on udp")
		}
		chans := []*laila.Channel{}
		for i := 0; i < 255; i++ {
			chans = append(chans, chan19_(t, tp.Ca(), tp.Bid, fmt.Sprintf("c%d", i)))
		}
		for _, c := range chans {
			_, ok := any(c).(*laila.Channel) // isinstance(c, Channel): a plain Go type, no py.Class
			assertTrue(t, ok && c != nil)
		}
		if _, err := tp.Peer().Getitem("c255"); err == nil {
			t.Fatal("expected the 256th lane to be refused")
		}
		// free one, then a new one fits
		chans[0].Close()
		assertTrue(t, Wait_until(func() bool { return chans[0].Closed() }, 5))
		extra := chan19_(t, tp.Ca(), tp.Bid, "c-extra")
		assertTrue(t, extra.Opened() || Wait_until(func() bool { return extra.Opened() }, 10))
		check(t, extra.Send([]byte("ok")))
		assertTrue(t, Wait_until(func() bool { return has19_(chans19_(t, tp.Cb(), tp.Aid), "c-extra") }, 10))
		got, err := relay19_(chan19_(t, tp.Cb(), tp.Aid, "c-extra"), 10).Next()
		check(t, err)
		assertTrue(t, bytes.Equal(data19_(got), []byte("ok")))
	})
}

func TestLanes_open_close_churn(t *testing.T) {
	for_kinds19(t, func(t *testing.T, kind string) {
		tp := tp19(t, kind)
		if tp.Kind == "udp" {
			t.Skip("slow handshake storm on udp")
		}
		for i := 0; i < N_scaled(300); i++ {
			ch := chan19_(t, tp.Ca(), tp.Bid, fmt.Sprintf("churn%d", i%7))
			check(t, ch.Send([]byte("x")))
			ch.Close()
			assertTrue(t, ch.Closed())
		}
		// the peer side observes closes; no lane leak
		pa, pb := laila.As_carrier_(tp.Pa), laila.As_carrier_(tp.Pb)
		assertTrue(t, Wait_until(func() bool { return len(pa.Channel_names(tp.Bid)) == 0 }, 10))
		assertTrue(t, Wait_until(func() bool { return len(pb.Channel_names(tp.Aid)) == 0 }, 10))
		// still works
		ch := chan19_(t, tp.Ca(), tp.Bid, "fresh")
		check(t, ch.Send([]byte("y")))
		assertTrue(t, Wait_until(func() bool { return has19_(chans19_(t, tp.Cb(), tp.Aid), "fresh") }, 10))
		got, err := relay19_(chan19_(t, tp.Cb(), tp.Aid, "fresh"), 10).Next()
		check(t, err)
		assertTrue(t, bytes.Equal(data19_(got), []byte("y")))
	})
}

func TestLanes_channel_reuse_after_close_is_new_object(t *testing.T) {
	for_kinds19(t, func(t *testing.T, kind string) {
		tp := tp19(t, kind)
		ch1 := chan19_(t, tp.Ca(), tp.Bid, "re")
		ch1.Close()
		ch2 := chan19_(t, tp.Ca(), tp.Bid, "re")
		assertTrue(t, ch2 != ch1)
		assertTrue(t, !ch2.Closed())
		if err := ch1.Send([]byte("dead")); !errors.Is(err, py.ConnectionError) {
			t.Fatalf("expected ConnectionError, got %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// TestBackpressure
// ---------------------------------------------------------------------------

func TestBackpressure_bounded_queue_drop_oldest_accounting(t *testing.T) {
	Fresh_policy(t)
	for _, kind := range []string{"loopback", "tcp"} {
		func() {
			tp, err := New_TwoPolicies(kind, py.Kw{"liveness_interval": 60.0, "channel_queue_size": 8}).Enter()
			check(t, err)
			defer tp.Exit()
			ch_a, ch_b := pair19_(t, tp)
			n := N_scaled(2000)
			for i := 0; i < n; i++ {
				check(t, ch_a.Send(be19_(i, 4)))
			}
			assertTrue(t, Wait_until(func() bool { return ch_b.Rx_seq()+ch_a.Tx_dropped() == n }, 60))
			assertTrue(t, ch_b.Qsize() <= 8)
			got := []*laila.StreamEntry{}
			for i, q := 0, ch_b.Qsize(); i < q; i++ {
				e, err := relay19_(ch_b, 5).Next()
				check(t, err)
				got = append(got, e)
			}
			seqs := []int{}
			for _, g := range got {
				seqs = append(seqs, g.Stream().Seq)
			}
			sorted_seqs := append([]int(nil), seqs...)
			sort.Ints(sorted_seqs)
			assertTrue(t, py.Eq(seqs, sorted_seqs))
			assertEq(t, got[0].Stream().Seq, ch_b.Dropped())
			assertTrue(t, got[0].Stream().Dropped_before <= ch_b.Dropped())
			assertEq(t, ch_b.Dropped()+ch_a.Tx_dropped()+len(got), n)
			Quiesce(tp.A)
			Quiesce(tp.B)
		}()
	}
}

func TestBackpressure_byte_cap_evicts(t *testing.T) {
	Fresh_policy(t)
	tp := two19_(t, "loopback", py.Kw{"channel_queue_bytes": 1024 * 1024})
	ch_a, ch_b := pair19_(t, tp)
	msg := bytes.Repeat([]byte("z"), 256*1024)
	for i := 0; i < 20; i++ {
		check(t, ch_a.Send(msg))
	}
	assertTrue(t, ch_b.Qsize() <= 4)
	assertTrue(t, ch_b.Dropped() >= 16)
	for i, q := 0, ch_b.Qsize(); i < q; i++ {
		g, err := relay19_(ch_b, 5).Next()
		check(t, err)
		assertTrue(t, len(data19_(g)) == len(msg))
	}
}

func TestBackpressure_slow_consumer_keeps_rpc_alive(t *testing.T) {
	Fresh_policy(t)
	tp := two19_(t, "tcp", py.Kw{"channel_queue_size": 16})
	must(tp.Extend_b("store"))
	ch_a, _ := pair19_(t, tp)
	stop := py.NewEvent()
	// Go-only guard: the Python flood is paced by the GIL, so the carrier's
	// bounded send queue keeps memory flat. Here a tight ``send()`` loop can
	// outrun the loop thread and pile up pending ``call_soon_threadsafe``
	// callbacks until the OOM killer takes the whole test binary down; stop
	// the flood (and fail the test below) once RSS passes the cap instead.
	const rss_cap_mb = 1536.0
	var blowup atomic.Bool
	var goroutines_at_blowup atomic.Int64

	flood := func() {
		msg := blob19_(64*1024, 3)
		for i := 0; !stop.Is_set(); i++ {
			if err := ch_a.Send(msg); err != nil {
				if errors.Is(err, py.ConnectionError) {
					return
				}
				panic(err)
			}
			if i%64 == 0 && Probe().Rss_mb() > rss_cap_mb {
				blowup.Store(true)
				goroutines_at_blowup.Store(int64(Probe().Threads()))
				return
			}
		}
	}

	th := py.New_Thread(py.Kw{"target": flood, "daemon": true})
	check(t, th.Start())
	func() {
		defer func() {
			stop.Set()
			_ = th.Join(10)
		}()
		latencies := []float64{}
		for i := 0; i < N_scaled(100); i++ {
			e := must(laila.Constant(i))
			sw := Timed(func() {
				must(Wait(must(laila.Memorize(e, py.Kw{"dst_policy": tp.Bid, "dst_pool": "store"})), 60))
				assertEq(t, Unwrap(must(Wait(must(laila.Remember(e.Global_id(),
					py.Kw{"dst_policy": tp.Bid, "dst_pool": "store", "persist": false})), 60))), i)
			})
			latencies = append(latencies, sw.Elapsed)
		}
		assertTrue(t, tp.Pa.Ping(tp.Bid, 10))
		max_lat := 0.0
		for _, l := range latencies {
			max_lat = max(max_lat, l)
		}
		assertTrue(t, max_lat < 20, max_lat)
	}()
	assertTrue(t, !blowup.Load(), fmt.Sprintf(
		"sender RSS passed %.0f MiB while flooding a 16-message send queue (%d goroutines): outbound stream sends are not bounded",
		rss_cap_mb, goroutines_at_blowup.Load()))
	Quiesce(tp.A)
	Quiesce(tp.B)
}

// ---------------------------------------------------------------------------
// TestRelaySemantics
// ---------------------------------------------------------------------------

func TestRelaySemantics_two_consumers_split_packets(t *testing.T) {
	for_kinds19(t, func(t *testing.T, kind string) {
		tp := tp_big19(t, kind)
		if tp.Kind == "udp" {
			t.Skip("not needed on udp")
		}
		n := N_scaled(2000)
		ch_a, ch_b := pair19_(t, tp)
		r := relay19_(ch_b, 30)
		var mu sync.Mutex
		buckets := [2][]int{{}, {}}
		total := func() int {
			mu.Lock()
			defer mu.Unlock()
			return len(buckets[0]) + len(buckets[1])
		}

		consume := func(i int) {
			for {
				e, err := r.Next()
				if err != nil {
					// (StopIteration, TimeoutError) -> return len(buckets[i])
					return
				}
				mu.Lock()
				buckets[i] = append(buckets[i], e.Stream().Seq)
				mu.Unlock()
			}
		}

		threads := []*py.Thread{}
		for i := 0; i < 2; i++ {
			i := i
			threads = append(threads, py.New_Thread(py.Kw{"target": func() { consume(i) }, "daemon": true}))
		}
		for _, th := range threads {
			check(t, th.Start())
		}
		for i := 0; i < n; i++ {
			check(t, ch_a.Send(be19_(i, 4)))
		}
		assertTrue(t, Wait_until(func() bool { return total() == n }, 60))
		ch_a.Close()
		for _, th := range threads {
			check(t, th.Join(40))
		}
		mu.Lock()
		all := append(append([]int(nil), buckets[0]...), buckets[1]...)
		b0, b1 := buckets[0], buckets[1]
		mu.Unlock()
		sort.Ints(all)
		assertTrue(t, py.Eq(all, Range(n)))
		in0 := map[int]bool{}
		for _, s := range b0 {
			in0[s] = true
		}
		for _, s := range b1 {
			assertTrue(t, !in0[s], "a seq was delivered to both consumers")
		}
	})
}

func TestRelaySemantics_relay_timeout_then_resume(t *testing.T) {
	for_kinds19(t, func(t *testing.T, kind string) {
		tp := tp19(t, kind)
		ch_a, ch_b := pair19_(t, tp)
		r := relay19_(ch_b, 0.3)
		if _, err := r.Next(); !errors.Is(err, py.TimeoutError) {
			t.Fatalf("expected TimeoutError, got %v", err)
		}
		check(t, ch_a.Send([]byte("late")))
		got, err := relay19_(ch_b, 10).Next()
		check(t, err)
		assertTrue(t, bytes.Equal(data19_(got), []byte("late")))
	})
}

func TestRelaySemantics_close_from_sender_ends_relay_after_drain(t *testing.T) {
	for_kinds19(t, func(t *testing.T, kind string) {
		tp := tp19(t, kind)
		ch_a, ch_b := pair19_(t, tp)
		n := 50
		for i := 0; i < n; i++ {
			check(t, ch_a.Send([]byte{byte(i)}))
		}
		assertTrue(t, Wait_until(func() bool { return ch_b.Rx_seq() == n }, 30))
		ch_a.Close()
		assertTrue(t, Wait_until(func() bool { return ch_b.Closed() }, 15))
		got := [][]byte{}
		check(t, relay19_(ch_b, 10).Range(func(e *laila.StreamEntry) error {
			got = append(got, data19_(e))
			return nil
		}))
		want := [][]byte{}
		for i := 0; i < n; i++ {
			want = append(want, []byte{byte(i)})
		}
		assertEq(t, bytes_list19_(got), bytes_list19_(want))
		assertTrue(t, ch_b.Closed_reason() != nil && *ch_b.Closed_reason() != "")
	})
}

func TestRelaySemantics_stream_entries_memorize_and_remember(t *testing.T) {
	for_kinds19(t, func(t *testing.T, kind string) {
		tp := tp19(t, kind)
		if tp.Kind == "udp" {
			t.Skip("udp stream lanes are lossy by design")
		}
		ch_a, ch_b := pair19_(t, tp)
		payloads := [][]byte{}
		for i := 0; i < N_scaled(100); i++ {
			payloads = append(payloads, blob19_(1024, i))
		}
		for _, p := range payloads {
			check(t, ch_a.Send(p))
		}
		entries, err := drain19_(ch_b, len(payloads))
		check(t, err)
		for _, e := range entries {
			assertTrue(t, py.Isinstance(e, laila.StreamEntry_class))
		}
		laila.Activate_policy(tp.B)
		func() {
			defer laila.Activate_policy(tp.A)
			lst, ids := py.NewList(), py.NewList()
			for _, e := range entries {
				lst.Append(e)
				ids.Append(e.Global_id())
			}
			must(Wait(must(laila.Memorize(lst)), 60))
			back := must(Wait(must(laila.Remember(ids, py.Kw{"persist": false})), 60))
			got := [][]byte{}
			for _, b := range py.Iter(back) {
				got = append(got, as_bytes18_(unwrap19_(b)))
			}
			assertEq(t, bytes_list19_(got), bytes_list19_(payloads))
		}()
	})
}

func TestRelaySemantics_comm_stop_closes_all_lanes_and_wakes_consumers(t *testing.T) {
	Fresh_policy(t)
	tp := two19_(t, "tcp")
	chans := []*laila.Channel{}
	for i := 0; i < 10; i++ {
		chans = append(chans, chan19_(t, tp.Ca(), tp.Bid, fmt.Sprintf("s%d", i)))
	}
	assertTrue(t, Wait_until(func() bool { return len(chans19_(t, tp.Cb(), tp.Aid)) == 10 }, 15))
	b_chans := []*laila.Channel{}
	for i := 0; i < 10; i++ {
		b_chans = append(b_chans, chan19_(t, tp.Cb(), tp.Aid, fmt.Sprintf("s%d", i)))
	}
	results := make([]any, 10)

	consume := func(i int) {
		err := relay19_(b_chans[i]).Range(func(*laila.StreamEntry) error { return nil })
		if err != nil {
			results[i] = py.Repr(err)
			return
		}
		results[i] = "stopped"
	}

	threads := []*py.Thread{}
	for i := 0; i < 10; i++ {
		i := i
		threads = append(threads, py.New_Thread(py.Kw{"target": func() { consume(i) }, "daemon": true}))
	}
	for _, th := range threads {
		check(t, th.Start())
	}
	py.Sleep_sync(0.5)
	sw := Timed(func() {
		check(t, tp.Ca().Stop())
		for _, th := range threads {
			check(t, th.Join(30))
		}
	})
	for _, th := range threads {
		assertTrue(t, !th.Is_alive())
	}
	want := py.NewList()
	for i := 0; i < 10; i++ {
		want.Append("stopped")
	}
	assertEq(t, py.NewList(results...), want)
	for _, c := range chans {
		assertTrue(t, c.Closed())
	}
	assertTrue(t, sw.Elapsed < 30)
}

// ---------------------------------------------------------------------------
// TestStaticLaneFirmwareStyle
// ---------------------------------------------------------------------------

func TestStaticLaneFirmwareStyle_static_lane_no_handshake(t *testing.T) {
	Fresh_policy(t)
	tp := two19_(t, "tcp", py.Kw{"static_lanes": py.DictOf("telemetry", 7), "channel_queue_size": 10_000})
	ch_a := chan19_(t, tp.Ca(), tp.Bid, "telemetry")
	assertEq(t, ch_a.Lane_id, 7)
	ch_b := chan19_(t, tp.Cb(), tp.Aid, "telemetry")
	assertEq(t, ch_b.Lane_id, 7)
	n := N_scaled(500)
	for i := 0; i < n; i++ {
		check(t, ch_a.Send(be19_(i, 2)))
	}
	got, err := drain19_(ch_b, n)
	check(t, err)
	got_b, want := [][]byte{}, [][]byte{}
	for i, g := range got {
		got_b = append(got_b, data19_(g))
		want = append(want, be19_(i, 2))
	}
	assertEq(t, bytes_list19_(got_b), bytes_list19_(want))
}
