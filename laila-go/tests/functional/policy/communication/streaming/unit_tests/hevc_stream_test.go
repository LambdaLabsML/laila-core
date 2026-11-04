// Test 13: synthetic H.265 Annex B stream, byte-exact, over every carrier.
//
// A seeded hevc.go stream (default 300 access units ~ 10 s at 30 fps,
// ~5.7 MB: IDR every 30 frames, 1-3 slices per picture, VPS/SPS/PPS, SEI,
// AUD) is sent at real-time pace over loopback, TCP and a UART null-modem
// in two packetizations:
//
//   - per access unit -- one ``send()`` per frame; every entry must parse
//     to exactly one AU with the generator's NAL types and byte length;
//   - per NAL unit -- one ``send()`` per NAL; entries must regroup into
//     the same access units.
//
// In both cases the concatenated received bytes equal the sent bytes, and
// the independent reference parser's AU count / IDR count / NAL histogram
// match the ground truth. Throughput and per-message latency (send ->
// ``arrived_at``, same clock, same process) are logged.
//
// ``LAILA_HEVC_FRAMES`` (default 300) and ``LAILA_HEVC_FPS`` (default 30)
// tune duration. The lane tests are slow (skipped under ``-short``).
//
// Port of tests/functional/policy/communication/streaming/unit_tests/test_hevc_stream.py.
package unit_tests

import (
	"bytes"
	"os"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

var (
	HEVC_N_FRAMES = hevc_env_int_("LAILA_HEVC_FRAMES", 300)
	HEVC_FPS      = hevc_env_float_("LAILA_HEVC_FPS", 30)
)

func hevc_env_int_(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func hevc_env_float_(name string, def float64) float64 {
	if v := os.Getenv(name); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

// ----------------------------------------------------------------------
// TestGeneratorAndParser -- the reference parser reproduces the
// generator's ground truth offline.
// ----------------------------------------------------------------------

func TestGeneratorAndParser(t *testing.T) {
	aus := Hevc_generate(1234, HEVC_N_FRAMES)
	gt := Hevc_ground_truth(aus)
	assertEq(t, gt.Au_count, HEVC_N_FRAMES)
	assertEq(t, gt.Idr_count, (HEVC_N_FRAMES+29)/30)
	parsed := Hevc_parse_annexb(bytes.Join(Hevc_packetize_per_au(aus), nil))
	assertEq(t, len(parsed), gt.Au_count)
	idr := 0
	sizes := []int{}
	types := [][]int{}
	for _, p := range parsed {
		if p.Is_idr() {
			idr++
		}
		sizes = append(sizes, p.Size)
		types = append(types, p.Types)
	}
	assertEq(t, idr, gt.Idr_count)
	assertTrue(t, reflect.DeepEqual(sizes, gt.Au_sizes), "AU sizes differ")
	want_types := [][]int{}
	for _, au := range aus {
		want_types = append(want_types, au.Types())
	}
	assertTrue(t, reflect.DeepEqual(types, want_types), "NAL types differ")
	assertTrue(t, reflect.DeepEqual(Hevc_histogram(parsed), gt.Nal_histogram), "NAL histogram differs")
	for _, au := range aus {
		for _, nal := range au.Nals {
			body := nal.Data[4:]
			assertTrue(t, !bytes.Contains(body, []byte{0, 0, 1}), "start code inside NAL body")
			assertTrue(t, !bytes.Contains(body, []byte{0, 0, 0}), "three zero bytes inside NAL body")
			assertTrue(t, body[len(body)-1] != 0, "NAL body ends in zero")
		}
	}
	a := Hevc_ground_truth(Hevc_generate(1234, 5)).Au_sizes
	b := Hevc_ground_truth(Hevc_generate(1234, 5)).Au_sizes
	sort.Ints(a)
	sort.Ints(b)
	assertTrue(t, reflect.DeepEqual(a, b), "generator is not deterministic")
}

// ----------------------------------------------------------------------
// TestHevcOverLanes
// ----------------------------------------------------------------------

// hevcPair_ is the common surface of TwoPolicies / UartNullModem.
type hevcPair_ struct {
	ca, cb   *laila.LAILA_IDENTIFIABLE_COMMUNICATION_
	aid, bid string
	pa_ping  func(string, ...any) bool
}

var (
	hevc_once_ sync.Once
	hevc_aus_  []*AccessUnit
	hevc_gt_   GroundTruth
)

// hevc_fixture_ is ``setUpClass``.
func hevc_fixture_() ([]*AccessUnit, GroundTruth) {
	hevc_once_.Do(func() {
		hevc_aus_ = Hevc_generate(1234, HEVC_N_FRAMES)
		hevc_gt_ = Hevc_ground_truth(hevc_aus_)
	})
	return hevc_aus_, hevc_gt_
}

func hevc_run_(t *testing.T, pair hevcPair_, label string, per_nal bool) {
	t.Helper()
	aus, gt := hevc_fixture_()
	var messages [][]byte
	if per_nal {
		messages = Hevc_packetize_per_nal(aus)
	} else {
		messages = Hevc_packetize_per_au(aus)
	}
	expected_hashes := make([]string, 0, len(messages))
	total := 0
	for _, m := range messages {
		expected_hashes = append(expected_hashes, py.Sha256_hex(m))
		total += len(m)
	}
	ch_a := channel_(t, pair.ca, pair.bid, "video")
	ch_b := channel_(t, pair.cb, pair.aid, "video")
	send_times := []float64{}
	var got_mu sync.Mutex
	got := []*laila.StreamEntry{}
	done := py.NewEvent()

	go func() {
		defer done.Set()
		_ = relay_(t, ch_b, 30).Range(func(e *laila.StreamEntry) error {
			got_mu.Lock()
			got = append(got, e)
			n := len(got)
			got_mu.Unlock()
			if n == len(messages) {
				return errStopRange_
			}
			return nil
		})
	}()
	// pace at FPS access units per second (per-NAL: all NALs of an AU go
	// out together, like an encoder callback would)
	period := 1.0 / HEVC_FPS
	t_start := py.Monotonic()
	nxt := t_start
	var groups [][][]byte
	for _, au := range aus {
		if per_nal {
			g := [][]byte{}
			for _, n := range au.Nals {
				g = append(g, n.Data)
			}
			groups = append(groups, g)
		} else {
			groups = append(groups, [][]byte{au.Data()})
		}
	}
	for _, group := range groups {
		for _, m := range group {
			send_times = append(send_times, py.Monotonic())
			must(0, ch_a.Send(m))
		}
		nxt += period
		if delay := nxt - py.Monotonic(); delay > 0 {
			sleep(delay)
		}
	}
	t_sent := py.Monotonic()
	if !done.Wait(60) {
		got_mu.Lock()
		n := len(got)
		got_mu.Unlock()
		t.Fatalf("%s: only %d/%d received", label, n, len(messages))
	}
	t_done := py.Monotonic()
	got_mu.Lock()
	defer got_mu.Unlock()

	// byte-exact, in order, no loss
	assertEq(t, len(got), len(messages))
	hashes := []string{}
	seqs := []int{}
	datas := make([][]byte, 0, len(got))
	for _, e := range got {
		d := bytes_of_(t, e)
		datas = append(datas, d)
		hashes = append(hashes, py.Sha256_hex(d))
		seqs = append(seqs, e.Stream().Seq)
	}
	assertTrue(t, reflect.DeepEqual(hashes, expected_hashes), "received payload hashes differ")
	want_seqs := make([]int, len(messages))
	for i := range want_seqs {
		want_seqs[i] = i
	}
	assertTrue(t, reflect.DeepEqual(seqs, want_seqs), "stream seqs are not 0..n-1")
	assertEq(t, ch_b.Dropped(), 0)
	assertEq(t, ch_b.Discarded(), 0)
	assertEq(t, ch_a.Tx_dropped(), 0)

	// parse what arrived with the independent parser
	stream := bytes.Join(datas, nil)
	parsed := Hevc_parse_annexb(stream)
	assertEq(t, len(parsed), gt.Au_count)
	idr := 0
	sizes := []int{}
	for _, p := range parsed {
		if p.Is_idr() {
			idr++
		}
		sizes = append(sizes, p.Size)
	}
	assertEq(t, idr, gt.Idr_count)
	assertTrue(t, reflect.DeepEqual(sizes, gt.Au_sizes), "AU sizes differ")
	assertTrue(t, reflect.DeepEqual(Hevc_histogram(parsed), gt.Nal_histogram), "NAL histogram differs")
	if per_nal {
		for _, d := range datas {
			assertEq(t, len(Hevc_split_nals(d)), 1) // per-NAL entry must hold exactly one NAL
		}
	} else {
		for i, d := range datas {
			p := Hevc_parse_annexb(d)
			assertEq(t, len(p), 1) // per-AU entry must hold exactly one AU
			assertTrue(t, reflect.DeepEqual(p[0].Types, aus[i].Types()), "per-AU NAL types differ")
			assertEq(t, p[0].Size, aus[i].Size())
		}
	}

	lat := make([]float64, 0, len(got))
	for i, e := range got {
		lat = append(lat, e.Stream().Arrived_at-send_times[i])
	}
	lat_sorted := append([]float64(nil), lat...)
	sort.Float64s(lat_sorted)
	p50 := lat_sorted[len(lat_sorted)/2]
	p99 := lat_sorted[int(0.99*float64(len(lat)-1))]
	wall := t_done - t_start
	mode := "per-AU"
	if per_nal {
		mode = "per-NAL"
	}
	t.Logf("[hevc %s %s] %d msgs %.2f MB in %.1fs (%.2f MB/s, paced %.0f fps) latency p50=%.2fms p99=%.2fms max=%.2fms tail=%.0fms; RPC ping=%v",
		label, mode, len(messages), float64(total)/1e6, wall, float64(total)/wall/1e6, HEVC_FPS,
		p50*1e3, p99*1e3, lat_sorted[len(lat_sorted)-1]*1e3, (t_done-t_sent)*1e3, pair.pa_ping(pair.bid))
	assertTrue(t, pair.ca.Peers.Contains(pair.bid), "A lost B")
	assertTrue(t, pair.cb.Peers.Contains(pair.aid), "B lost A")
}

func hevc_two_policies_(t *testing.T, kind string, kw ...py.Kw) hevcPair_ {
	t.Helper()
	tp := enter_(t, New_TwoPolicies(kind, kw...))
	return hevcPair_{ca: tp.Ca(), cb: tp.Cb(), aid: tp.Aid, bid: tp.Bid, pa_ping: tp.Pa.Ping}
}

func hevc_uart_null_modem_(t *testing.T) hevcPair_ {
	t.Helper()
	m, err := New_UartNullModem().Enter()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Exit)
	return hevcPair_{ca: m.Ca(), cb: m.Cb(), aid: m.Aid, bid: m.Bid, pa_ping: m.Pa.Ping}
}

func hevc_slow_(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("slow: paced HEVC stream (run without -short)")
	}
}

func TestHevcOverLanes(t *testing.T) {
	original := must(laila.Get_active_policy())
	t.Cleanup(func() { laila.Activate_policy(original) })

	t.Run("loopback_per_au", func(t *testing.T) {
		hevc_slow_(t)
		hevc_run_(t, hevc_two_policies_(t, "loopback"), "loopback", false)
	})
	t.Run("loopback_per_nal", func(t *testing.T) {
		hevc_slow_(t)
		hevc_run_(t, hevc_two_policies_(t, "loopback"), "loopback", true)
	})
	t.Run("tcp_per_au", func(t *testing.T) {
		hevc_slow_(t)
		hevc_run_(t, hevc_two_policies_(t, "tcp"), "tcp", false)
	})
	t.Run("tcp_per_nal", func(t *testing.T) {
		hevc_slow_(t)
		hevc_run_(t, hevc_two_policies_(t, "tcp"), "tcp", true)
	})
	t.Run("uart_null_modem_per_au", func(t *testing.T) {
		hevc_slow_(t)
		hevc_run_(t, hevc_uart_null_modem_(t), "uart-nullmodem", false)
	})
	t.Run("uart_null_modem_per_nal", func(t *testing.T) {
		hevc_slow_(t)
		hevc_run_(t, hevc_uart_null_modem_(t), "uart-nullmodem", true)
	})
	t.Run("tcp_msgpack_per_au", func(t *testing.T) {
		hevc_slow_(t)
		hevc_run_(t, hevc_two_policies_(t, "tcp", py.Kw{"codec": "msgpack"}), "tcp-msgpack", false)
	})
}
