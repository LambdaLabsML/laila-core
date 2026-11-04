// Port of tests/functional/policy/communication/unit_tests/test_carriers.py.
//
// Unit tests for the reusable RPC carriers.
//
// Two layers, neither of which needs real hardware:
//
//  1. The wire codec (``json`` / ``msgpack`` + length-prefix framing).
//  2. The datagram carrier's reliability machinery, exercised over an
//     in-memory "ether" fake link that deterministically drops the first
//     data packets to force fragmentation + retransmit recovery.
package unit_tests

import (
	"encoding/binary"
	"strings"
	"sync"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// ------------------------------------------------------------------ #
// Codec
// ------------------------------------------------------------------ #

func TestCodec_json_roundtrip(t *testing.T) {
	msg := py.DictOf("jsonrpc", "2.0", "method", "rpc.call", "params", py.DictOf("path", py.NewList("a", "b")), "id", "1")
	assertEq(t, must(laila.Codec_decode(must(laila.Codec_encode(msg, "json")), "json")), msg)
}

func TestCodec_msgpack_roundtrip(t *testing.T) {
	msg := py.DictOf("jsonrpc", "2.0", "result", py.NewList(1, 2, py.DictOf("x", true)), "id", "2")
	assertEq(t, must(laila.Codec_decode(must(laila.Codec_encode(msg, "msgpack")), "msgpack")), msg)
}

func TestCodec_unknown_codec_raises(t *testing.T) {
	_, err := laila.Codec_encode(py.NewDict(), "yaml")
	assertRaises(t, py.ValueError, err)
	_, err = laila.Codec_decode([]byte{}, "yaml")
	assertRaises(t, py.ValueError, err)
}

func TestCodec_encodes_laila_entry_under_both_codecs(t *testing.T) {
	// A real laila Entry must survive both codecs as the canonical
	// ``Entry.serialize`` envelope (F-W21-03) so a peer can rebuild it
	// with build_by_scope instead of receiving a lossy ``{}``.
	entry := must(laila.Constant(py.DictOf("k", py.NewList(1, 2, 3))))
	msg := laila.Make_result("rid-1", entry)
	for _, c := range []string{"json", "msgpack"} {
		decoded := must(laila.Codec_decode(must(laila.Codec_encode(msg, c)), c)).(*py.Dict)
		assertEq(t, must(decoded.Getitem("id")), "rid-1")
		result, ok := must(decoded.Getitem("result")).(*py.Dict)
		if !ok {
			t.Fatalf("%s: result is %T, want dict", c, must(decoded.Getitem("result")))
		}
		if !laila.Is_entry_envelope(result) {
			t.Fatalf("%s: result is not an entry envelope", c)
		}
		assertEq(t, must(result.Getitem("_uuid")), py.Str(entry.Uuid_))
		rebuilt, _, err := laila.Rehydrate_entry_args([]any{result}, py.Kw{})
		if err != nil {
			t.Fatal(err)
		}
		assertEq(t, must(rebuilt[0].(*laila.Entry).Data()), py.DictOf("k", py.NewList(1, 2, 3)))
	}
}

func TestCodec_frame_prefix_is_length(t *testing.T) {
	payload := []byte("hello world")
	framed := laila.Frame(payload)
	length := binary.BigEndian.Uint32(framed[:4])
	assertEq(t, int(length), len(payload))
	assertEq(t, framed[4:], payload)
}

func TestCodec_read_frame_roundtrip(t *testing.T) {
	_, err := py.Run(func() (any, error) {
		reader := py.New_StreamReader()
		reader.Feed_data(append(laila.Frame([]byte("abc")), laila.Frame([]byte("defg"))...))
		reader.Feed_eof()
		assertEq(t, must(laila.Read_frame(reader)), []byte("abc"))
		assertEq(t, must(laila.Read_frame(reader)), []byte("defg"))
		if got := must(laila.Read_frame(reader)); got != nil {
			t.Fatalf("expected nil at EOF, got %s", py.Repr(got))
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// ------------------------------------------------------------------ #
// Datagram carrier over an in-memory lossy fake link
// ------------------------------------------------------------------ #

var (
	ether_mu_ sync.Mutex
	ether_    = map[string]*LossyDatagramProtocol_{}
)

// LossyDatagramProtocol_ is a datagram carrier whose 'wire' is an
// in-process registry.
//
// Drops the first ``drop_first_data`` DATA packets to force the
// ack/retransmit path; small ``mtu`` forces fragmentation.
type LossyDatagramProtocol_ struct {
	laila.DatagramRPCProtocol_

	Addr            string `py:"addr" default:""`
	Drop_first_data int    `py:"drop_first_data" default:"0"`

	Dropped_ int `py:"_dropped"`
}

var LossyDatagramProtocol_class = py.NewClass(
	"_LossyDatagramProtocol", (*LossyDatagramProtocol_)(nil), laila.DatagramRPCProtocol_class,
).
	Set_classvar("protocol_name", "fakedg").
	Set_classvar("matches_token", laila.Token_aliases_classmethod_("fakedg")).
	Set_classvar("can_handle_uri", laila.Uri_prefix_classmethod_("fakedg://"))

func New_LossyDatagramProtocol_(kw ...py.Kw) (*LossyDatagramProtocol_, error) {
	p := LossyDatagramProtocol_class.New().(*LossyDatagramProtocol_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *LossyDatagramProtocol_) Matches_token(token string) bool {
	return strings.ToLower(token) == "fakedg"
}

func (p *LossyDatagramProtocol_) Can_handle_uri(uri string) bool {
	return strings.HasPrefix(uri, "fakedg://")
}

// dummyTransport_ is the fake ``_DummyTransport``.
type dummyTransport_ struct{ addr string }

func (d *dummyTransport_) Close() {
	ether_mu_.Lock()
	delete(ether_, d.addr)
	ether_mu_.Unlock()
}

func (d *dummyTransport_) Get_extra_info(key string) any { return py.Addr{Host: d.addr, Port: 0} }

func (p *LossyDatagramProtocol_) Create_datagram_endpoint_() (any, any, error) {
	ether_mu_.Lock()
	ether_[p.Addr] = p
	ether_mu_.Unlock()
	return &dummyTransport_{addr: p.Addr}, nil, nil
}

func (p *LossyDatagramProtocol_) Resolve_peer_addr_(uri string) (any, error) {
	return laila.Uri_authority(uri), nil
}

func (p *LossyDatagramProtocol_) Sendto_(addr any, packet []byte) error {
	// Deterministically drop the first few DATA packets to test retry.
	ptype, _, _ := laila.Header_unpack_(packet[:laila.HEADER_SIZE_])
	if ptype == laila.TYPE_DATA_ && p.Dropped_ < p.Drop_first_data {
		p.Dropped_++
		return nil
	}
	ether_mu_.Lock()
	target := ether_[py.Str(addr)]
	ether_mu_.Unlock()
	if target == nil || target.Loop() == nil {
		return nil
	}
	loop := target.Loop()
	src := p.Addr
	pkt := append([]byte(nil), packet...)
	_, _ = loop.Call_soon_threadsafe(func() { target.Feed_packet_(src, pkt) })
	return nil
}

func run_echo_(t *testing.T, drop_first_data int, payload string) any {
	t.Helper()
	ether_mu_.Lock()
	ether_ = map[string]*LossyDatagramProtocol_{}
	ether_mu_.Unlock()

	a := mk_(t, must(New_LossyDatagramProtocol_(py.Kw{"addr": "A", "mtu": 64, "ack_timeout": 0.05})))
	b := mk_(t, must(New_LossyDatagramProtocol_(py.Kw{
		"addr": "B", "mtu": 64, "ack_timeout": 0.05, "drop_first_data": drop_first_data,
	})))
	defer stopAll(a, b)

	pb := proto_(b)
	rid, err := comm(a).Add_peer("fakedg://B", secret_of_(pb))
	if err != nil {
		t.Fatal(err)
	}
	assertEq(t, rid, b.Global_id())
	b.Private_set("echo", func(m string) string { return "len=" + py.Str(len(m)) })
	remote_b := must(comm(a).Peers.Getitem(b.Global_id())).(*laila.PeerProxy)
	return must(remote_b.Attr("echo").Invoke(payload))
}

func TestDatagramReliability_fragmented_echo_no_loss(t *testing.T) {
	// ~4KB payload over a 64-byte MTU => ~64 fragments.
	assertEq(t, run_echo_(t, 0, strings.Repeat("z", 4000)), "len=4000")
}

func TestDatagramReliability_fragmented_echo_with_loss_recovers(t *testing.T) {
	// Drop the first 3 data packets; ack/retransmit must recover.
	assertEq(t, run_echo_(t, 3, strings.Repeat("q", 2000)), "len=2000")
}
