// Synthetic H.265 / HEVC Annex B elementary stream (port of ``_hevc.py``).
//
// The goal is a byte stream with the *shape* of real H.265 -- NAL unit
// framing, start codes, parameter sets, IDR / trailing pictures, size
// distribution, emulation-prevention-safe payloads -- so stream framing,
// message boundaries, reassembly and parsing all get exercised without
// any codec library. The payload bits are random; nothing here decodes
// video.
//
// Generator
//
// Hevc_generate builds a list of AccessUnit (one per frame) from a seeded
// RNG. Per GOP (default 30 frames): VPS (32), SPS (33), PPS (34) once per
// IDR, an ``IDR_W_RADL`` (19) frame of 60-250 KB in 1-3 slice NALs, then
// ``TRAIL_R`` (1) / ``TRAIL_N`` (0) frames of 4-40 KB (log-normal) in 1-2
// slice NALs, with an occasional SEI (39) and AUD (35) at access-unit
// start. NAL headers follow ITU-T H.265 7.3.1.2 (``forbidden_zero_bit=0``,
// 6-bit ``nal_unit_type``, ``nuh_layer_id=0``, ``nuh_temporal_id_plus1=1``).
// The first slice of a picture has ``first_slice_segment_in_pic_flag=1``
// (MSB of its first payload byte set), later slices have it cleared, so
// an independent parser can group NALs into access units.
//
// Two packetizations share the same AU list: Hevc_packetize_per_au (one
// ``send()`` per access unit, Annex B) and Hevc_packetize_per_nal (one
// ``send()`` per NAL unit).
//
// Reference parser
//
// Hevc_parse_annexb splits on start codes, reads ``nal_unit_type``, groups
// into access units by AUD / parameter-set / first-slice detection and
// reports per-AU lengths and a NAL-type histogram. It is written
// independently of the generator (it never looks at AccessUnit), so a
// byte-exact round trip through a stream lane can be checked against the
// generator's ground truth.
package unit_tests

import (
	"bytes"
	"math"
	"math/rand"
	"sort"
)

var HEVC_START_CODE = []byte{0x00, 0x00, 0x00, 0x01}

const (
	NAL_TRAIL_N    = 0
	NAL_TRAIL_R    = 1
	NAL_IDR_W_RADL = 19
	NAL_VPS        = 32
	NAL_SPS        = 33
	NAL_PPS        = 34
	NAL_AUD        = 35
	NAL_SEI_PREFIX = 39
)

// hevc_is_slice_type_ is ``t in _SLICE_TYPES`` (VCL NAL unit types 0..31).
func hevc_is_slice_type_(t int) bool { return t >= 0 && t < 32 }

// hevc_is_param_type_ is ``t in _PARAM_TYPES``.
func hevc_is_param_type_(t int) bool { return t == NAL_VPS || t == NAL_SPS || t == NAL_PPS }

// Hevc_nal_header is the two-byte HEVC NAL header: f=0, type, layer=0,
// tid_plus1=1.
func Hevc_nal_header(nal_type int) []byte {
	if nal_type < 0 || nal_type >= 64 {
		panic(nal_type)
	}
	return []byte{byte((nal_type << 1) & 0x7E), 0x01}
}

// Hevc_nal_type_of is ``nal_unit_type`` from a NAL unit (header + payload,
// no start code).
func Hevc_nal_type_of(nal []byte) int { return int(nal[0]>>1) & 0x3F }

// Hevc_emulation_prevention inserts ``0x03`` after any ``00 00`` followed
// by ``00..03`` (H.265 7.4.2) -- ``re.sub(rb"\x00\x00(?=[\x00-\x03])",
// b"\x00\x00\x03", payload)``.
func Hevc_emulation_prevention(payload []byte) []byte {
	out := make([]byte, 0, len(payload)+len(payload)/64+1)
	i := 0
	for i < len(payload) {
		if i+2 < len(payload) && payload[i] == 0 && payload[i+1] == 0 && payload[i+2] <= 0x03 {
			out = append(out, 0x00, 0x00, 0x03)
			i += 2 // the lookahead byte is not consumed (regex lookahead)
			continue
		}
		out = append(out, payload[i])
		i++
	}
	return out
}

// hevc_rbsp_ is ``_rbsp(rng, n, first_slice=...)``: random EP-safe payload
// of about *n* bytes with a non-zero last byte. ``first_slice`` nil means
// ``None``.
func hevc_rbsp_(rng *rand.Rand, n int, first_slice *bool) []byte {
	raw := make([]byte, max(1, n))
	rng.Read(raw)
	if first_slice != nil {
		// first_slice_segment_in_pic_flag lives in the MSB of byte 0; keep
		// byte 0 outside 0x00..0x03 so EP insertion never touches it.
		if *first_slice {
			raw[0] = 0x80 | (raw[0] & 0x7F)
		} else {
			raw[0] = 0x04 | (raw[0] & 0x7B)
		}
	}
	if raw[len(raw)-1] == 0 {
		raw[len(raw)-1] = 0x80
	}
	out := Hevc_emulation_prevention(raw)
	if out[len(out)-1] == 0 {
		out[len(out)-1] = 0x80
	}
	return out
}

// Hevc_make_nal is ``start code + header + EP-safe payload`` of roughly
// *size* bytes.
func Hevc_make_nal(rng *rand.Rand, nal_type int, size int, first_slice *bool) []byte {
	payload := hevc_rbsp_(rng, max(1, size-2), first_slice)
	out := make([]byte, 0, 4+2+len(payload))
	out = append(out, HEVC_START_CODE...)
	out = append(out, Hevc_nal_header(nal_type)...)
	return append(out, payload...)
}

// HevcNal is one ``(nal_type, nal_bytes_with_start_code)`` tuple.
type HevcNal struct {
	Type int
	Data []byte
}

// AccessUnit is one frame: an ordered list of NALs.
type AccessUnit struct {
	Index int
	Kind  string // "IDR" | "P" | "B"
	Nals  []HevcNal
}

// Data is the Annex B concatenation of the AU's NALs.
func (au *AccessUnit) Data() []byte {
	out := make([]byte, 0, au.Size())
	for _, n := range au.Nals {
		out = append(out, n.Data...)
	}
	return out
}

// Size is the AU byte length.
func (au *AccessUnit) Size() int {
	s := 0
	for _, n := range au.Nals {
		s += len(n.Data)
	}
	return s
}

// Types is the ordered NAL type list.
func (au *AccessUnit) Types() []int {
	out := make([]int, 0, len(au.Nals))
	for _, n := range au.Nals {
		out = append(out, n.Type)
	}
	return out
}

// Is_idr is ``kind == "IDR"``.
func (au *AccessUnit) Is_idr() bool { return au.Kind == "IDR" }

// GroundTruth summarises a generated stream the receiver must reproduce.
type GroundTruth struct {
	Au_count      int
	Idr_count     int
	Nal_histogram map[int]int
	Au_sizes      []int
	Total_bytes   int
	Nal_count     int
}

func hevc_lognormal_size_(rng *rand.Rand, lo, hi, median int, sigma float64) int {
	mu := math.Log(float64(median))
	v := int(math.Exp(mu + sigma*rng.NormFloat64()))
	return max(lo, min(hi, v))
}

// hevc_randint_ is ``rng.randint(a, b)`` (inclusive).
func hevc_randint_(rng *rand.Rand, a, b int) int { return a + rng.Intn(b-a+1) }

// Hevc_generate is ``generate(seed=1234, n_frames=300, gop=30, *,
// idr_bytes=(60_000, 250_000), p_bytes=(4_000, 40_000), p_median=12_000)``:
// *n_frames* access units (an IDR every *gop* frames).
func Hevc_generate(seed int64, n_frames int) []*AccessUnit {
	const gop = 30
	idr_lo, idr_hi := 60_000, 250_000
	p_lo, p_hi, p_median := 4_000, 40_000, 12_000
	rng := rand.New(rand.NewSource(seed))
	aus := []*AccessUnit{}
	for i := 0; i < n_frames; i++ {
		is_idr := i%gop == 0
		kind := "P"
		if is_idr {
			kind = "IDR"
		} else if rng.Float64() < 0.3 {
			kind = "B"
		}
		au := &AccessUnit{Index: i, Kind: kind}
		if is_idr || rng.Float64() < 0.15 {
			au.Nals = append(au.Nals, HevcNal{NAL_AUD, Hevc_make_nal(rng, NAL_AUD, 3, nil)})
		}
		if is_idr {
			au.Nals = append(au.Nals, HevcNal{NAL_VPS, Hevc_make_nal(rng, NAL_VPS, 30, nil)})
			au.Nals = append(au.Nals, HevcNal{NAL_SPS, Hevc_make_nal(rng, NAL_SPS, 60, nil)})
			au.Nals = append(au.Nals, HevcNal{NAL_PPS, Hevc_make_nal(rng, NAL_PPS, 10, nil)})
		}
		if rng.Float64() < 0.2 {
			au.Nals = append(au.Nals, HevcNal{NAL_SEI_PREFIX, Hevc_make_nal(rng, NAL_SEI_PREFIX, 100, nil)})
		}
		var total, n_slices, slice_type int
		if is_idr {
			total = hevc_randint_(rng, idr_lo, idr_hi)
			n_slices = hevc_randint_(rng, 1, 3)
			slice_type = NAL_IDR_W_RADL
		} else {
			total = hevc_lognormal_size_(rng, p_lo, p_hi, p_median, 0.6)
			n_slices = hevc_randint_(rng, 1, 2)
			if kind == "B" {
				slice_type = NAL_TRAIL_N
			} else {
				slice_type = NAL_TRAIL_R
			}
		}
		cuts := []int{}
		for s := 0; s < n_slices-1; s++ {
			cuts = append(cuts, hevc_randint_(rng, 1, total-1))
		}
		sort.Ints(cuts)
		bounds := append(append([]int{0}, cuts...), total)
		for s := 0; s < n_slices; s++ {
			size := max(16, bounds[s+1]-bounds[s])
			first := s == 0
			au.Nals = append(au.Nals, HevcNal{slice_type, Hevc_make_nal(rng, slice_type, size, &first)})
		}
		aus = append(aus, au)
	}
	return aus
}

// Hevc_ground_truth is ``ground_truth(aus)``.
func Hevc_ground_truth(aus []*AccessUnit) GroundTruth {
	gt := GroundTruth{Nal_histogram: map[int]int{}}
	for _, au := range aus {
		for _, t := range au.Types() {
			gt.Nal_histogram[t]++
		}
		gt.Au_count++
		if au.Is_idr() {
			gt.Idr_count++
		}
		gt.Au_sizes = append(gt.Au_sizes, au.Size())
		gt.Total_bytes += au.Size()
		gt.Nal_count += len(au.Nals)
	}
	return gt
}

// Hevc_packetize_per_au is one message per access unit (all its NALs
// concatenated, Annex B).
func Hevc_packetize_per_au(aus []*AccessUnit) [][]byte {
	out := make([][]byte, 0, len(aus))
	for _, au := range aus {
		out = append(out, au.Data())
	}
	return out
}

// Hevc_packetize_per_nal is one message per NAL unit (with its start code).
func Hevc_packetize_per_nal(aus []*AccessUnit) [][]byte {
	out := [][]byte{}
	for _, au := range aus {
		for _, n := range au.Nals {
			out = append(out, n.Data)
		}
	}
	return out
}

// ----------------------------------------------------------------------
// Reference parser (independent of the generator)
// ----------------------------------------------------------------------

// ParsedAU is one access unit as seen by the reference parser.
type ParsedAU struct {
	Types []int
	Size  int
}

// Is_idr is ``any(16 <= t <= 23 for t in types)``.
func (p *ParsedAU) Is_idr() bool {
	for _, t := range p.Types {
		if t >= 16 && t <= 23 {
			return true
		}
	}
	return false
}

var hevc_short_start_ = []byte{0x00, 0x00, 0x01}

// Hevc_split_nals splits an Annex B byte stream on ``00 00 01`` start
// codes.
//
// Returns NAL units (header + payload) without start codes. A
// ``zero_byte`` belonging to a following 4-byte start code is stripped
// from the end of the preceding NAL (RBSP payloads never end in ``0x00``).
func Hevc_split_nals(stream []byte) [][]byte {
	nals := [][]byte{}
	n := len(stream)
	idx := bytes.Index(stream, hevc_short_start_)
	for idx != -1 {
		start := idx + 3
		var nxt int
		if rel := bytes.Index(stream[start:], hevc_short_start_); rel == -1 {
			nxt = -1
		} else {
			nxt = start + rel
		}
		end := n
		if nxt != -1 {
			end = nxt
		}
		nal := stream[start:end]
		for len(nal) > 0 && nal[len(nal)-1] == 0 {
			nal = nal[:len(nal)-1]
		}
		if len(nal) > 0 {
			nals = append(nals, nal)
		}
		idx = nxt
	}
	return nals
}

func hevc_first_slice_flag_(nal []byte) bool { return len(nal) > 2 && nal[2]&0x80 != 0 }

// Hevc_group_access_units groups NAL units into access units.
//
// A new access unit starts at an AUD, at a VPS/SPS/PPS/SEI that follows a
// VCL NAL, or at a VCL NAL with ``first_slice_segment_in_pic_flag`` set
// when the previous NAL was also VCL.
func Hevc_group_access_units(nals [][]byte) []*ParsedAU {
	aus := []*ParsedAU{}
	var cur *ParsedAU
	prev_vcl := false
	for _, nal := range nals {
		t := Hevc_nal_type_of(nal)
		new_au := false
		switch {
		case cur == nil:
			new_au = true
		case t == NAL_AUD:
			new_au = true
		case (hevc_is_param_type_(t) || t == NAL_SEI_PREFIX) && prev_vcl:
			new_au = true
		case hevc_is_slice_type_(t) && prev_vcl && hevc_first_slice_flag_(nal):
			new_au = true
		}
		if new_au {
			cur = &ParsedAU{Types: []int{}}
			aus = append(aus, cur)
		}
		cur.Types = append(cur.Types, t)
		cur.Size += len(nal) + 4 // account for the 4-byte start code
		prev_vcl = hevc_is_slice_type_(t)
	}
	return aus
}

// Hevc_parse_annexb is the full reference parse: NAL split + AU grouping.
func Hevc_parse_annexb(stream []byte) []*ParsedAU {
	return Hevc_group_access_units(Hevc_split_nals(stream))
}

// Hevc_histogram is ``histogram(aus)``.
func Hevc_histogram(aus []*ParsedAU) map[int]int {
	hist := map[int]int{}
	for _, au := range aus {
		for _, t := range au.Types {
			hist[t]++
		}
	}
	return hist
}
