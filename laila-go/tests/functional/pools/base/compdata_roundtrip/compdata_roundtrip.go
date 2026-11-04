// Port of tests/functional/pools/base/compdata_roundtrip.py.
//
// Shared helpers for dtype-travel round-trip tests across pool backends.
//
// Used by:
//
//	tests/functional/pools/sqlite/unit_tests/sqlite_compdata_roundtrip_test.go
//	tests/functional/pools/filesystem/unit_tests/filesystem_compdata_roundtrip_test.go
//	tests/functional/pools/mempool/unit_tests/memory_compdata_roundtrip_test.go
//
// This package is intentionally NOT a ``unit_tests`` package, so it holds
// no test functions of its own.
//
// Provides:
//   - NUMPY_DTYPES / TORCH_DTYPES (torch list empty: the port has no torch)
//   - Build_np(dtype, shape): payload generator
//   - Envelope_encode(cd) / Envelope_decode(envelope): wrap/unwrap a
//     serialized ComputationalData as a JSON-encodable dict carrying the
//     transformed blob and its full recovery sequence
//   - Register_dtype_matrix(t, make_pool, store_as_json_string): runs a dense
//     grid of subtests (14 numpy dtypes x 2 modes) plus edge cases
//
// numpy payloads are the port's NDArray (dtype descr + shape + buffer).
// numpy's non-contiguous views are materialised by np.save, so the strided
// edge cases are expressed exactly as np.save would write them (C-order
// copies for slices / reversed views, ``fortran_order=True`` for ``.T``).
package compdata_roundtrip

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// HAVE_TORCH mirrors the ``try: import torch`` probe; the port has no torch.
const HAVE_TORCH = false

// =============================================================================
// Payload generators
// =============================================================================

// NUMPY_DTYPES are the numpy dtype names of the matrix.
var NUMPY_DTYPES = []string{
	"int8",
	"int16",
	"int32",
	"int64",
	"uint8",
	"uint16",
	"uint32",
	"uint64",
	"float16",
	"float32",
	"float64",
	"bool_",
	"complex64",
	"complex128",
}

// TORCH_DTYPES is empty when torch is missing.
var TORCH_DTYPES = []string{}

// descr_ maps a numpy dtype name to its ``np.dtype(name).str`` descr.
var descr_ = map[string]string{
	"int8":       "|i1",
	"int16":      "<i2",
	"int32":      "<i4",
	"int64":      "<i8",
	"uint8":      "|u1",
	"uint16":     "<u2",
	"uint32":     "<u4",
	"uint64":     "<u8",
	"float16":    "<f2",
	"float32":    "<f4",
	"float64":    "<f8",
	"bool_":      "|b1",
	"complex64":  "<c8",
	"complex128": "<c16",
}

// Dtype_kind_ is ``np.dtype(descr).kind`` for the descrs used here.
func Dtype_kind_(descr string) string {
	if py.Is_structured_dtype(descr) {
		return "V"
	}
	for _, c := range descr {
		switch c {
		case '<', '>', '|', '=':
			continue
		default:
			return string(c)
		}
	}
	return "V"
}

func linspace_(i, n int, lo, hi float64) float64 {
	if n <= 1 {
		return lo
	}
	return lo + (hi-lo)*float64(i)/float64(n-1)
}

func put_float_(b []byte, descr string, v float64) {
	switch descr {
	case "<f2":
		binary.LittleEndian.PutUint16(b, py.Float16bits(v))
	case "<f4":
		binary.LittleEndian.PutUint32(b, math.Float32bits(float32(v)))
	case "<f8":
		binary.LittleEndian.PutUint64(b, math.Float64bits(v))
	}
}

func put_int_(b []byte, itemsize int, v int64) {
	switch itemsize {
	case 1:
		b[0] = byte(v)
	case 2:
		binary.LittleEndian.PutUint16(b, uint16(v))
	case 4:
		binary.LittleEndian.PutUint32(b, uint32(v))
	case 8:
		binary.LittleEndian.PutUint64(b, uint64(v))
	}
}

// Build_np builds a deterministic numpy array covering positive + negative
// values (``build_np(dtype_name, shape=(2, 3))``).
func Build_np(dtype_name string, shape []int) *py.NDArray {
	if shape == nil {
		shape = []int{2, 3}
	}
	descr, ok := descr_[dtype_name]
	if !ok {
		panic(fmt.Sprintf("unknown dtype %q", dtype_name))
	}
	size := 1
	for _, d := range shape {
		size *= d
	}
	itemsize := py.NDArray_itemsize_(descr)
	flat := make([]byte, size*itemsize)
	kind := Dtype_kind_(descr)
	for i := 0; i < size; i++ {
		b := flat[i*itemsize:]
		switch kind {
		case "b":
			if i%2 == 0 {
				b[0] = 1
			}
		case "u":
			put_int_(b, itemsize, int64(i))
		case "i":
			// Mix of negatives and positives, safe for int8
			put_int_(b, itemsize, int64(i-size/2))
		case "f":
			put_float_(b, descr, linspace_(i, size, -1.0, 1.0))
		case "c":
			half := itemsize / 2
			fdescr := "<f8"
			if half == 4 {
				fdescr = "<f4"
			}
			put_float_(b, fdescr, linspace_(i, size, -1.0, 1.0))
			put_float_(b[half:], fdescr, linspace_(i, size, 0.5, -0.5))
		}
	}
	if len(shape) == 0 {
		// np.array(flat[0] if size else 0, dtype=dt)
		return &py.NDArray{Dtype: descr, Shape: []int{}, Data: flat[:itemsize]}
	}
	return &py.NDArray{Dtype: descr, Shape: append([]int{}, shape...), Data: flat}
}

// =============================================================================
// Envelope: wraps a serialized ComputationalData for storage in a pool
// =============================================================================

// Envelope_encode serializes *cd* and wraps it in a JSON-encodable dict:
// ``{"blob": <str>, "recovery": [<inverse codes...>, <serializer_backward>]}``.
// *chain* defaults to the pool-standard Base64 chain so the blob is a
// UTF-8 string.
func Envelope_encode(cd laila.Computational_data, chain *laila.TransformationSequence) (*py.Dict, error) {
	payload, ser_back, err := cd.Serialize()
	if err != nil {
		return nil, err
	}
	ts := chain
	if ts == nil {
		ts = laila.Transformation_base64
	}
	transformed, inverse_codes, err := ts.Forward(payload)
	if err != nil {
		return nil, err
	}
	recovery := py.NewList()
	for _, code := range inverse_codes {
		recovery.Append(code)
	}
	recovery.Append(ser_back)
	env := py.NewDict()
	env.Set("blob", transformed)
	env.Set("recovery", recovery)
	return env, nil
}

// Envelope_decode reverses Envelope_encode; returns the recovered raw
// payload (not the CD).
func Envelope_decode(envelope *py.Dict) (any, error) {
	recovery, err := envelope.Getitem("recovery")
	if err != nil {
		return nil, err
	}
	blob, err := envelope.Getitem("blob")
	if err != nil {
		return nil, err
	}
	sc, err := laila.New_SimpleConstitution(py.Kw{"codes": recovery})
	if err != nil {
		return nil, err
	}
	recovered, err := sc.Build(blob)
	if err != nil {
		return nil, err
	}
	if cd, ok := recovered.(laila.Computational_data); ok {
		return cd.Base_().Data, nil
	}
	return recovered, nil
}

// =============================================================================
// Pool round-trip helper
// =============================================================================

// Pool is the ``pool[key] = v`` / ``pool[key]`` surface every backend has.
type Pool interface {
	Getitem(key any) (any, error)
	Setitem(key any, entry any) error
}

// Pool_roundtrip puts *cd* into *pool* under *key*, reads it back, and
// returns the recovered payload.
//
// Most pool backends (SQLite, Filesystem) accept a JSON string (or a dict
// which they json-dump). The in-memory base pool accepts any object. The
// *store_as_json_string* flag selects the encoding.
func Pool_roundtrip(pool Pool, key string, cd laila.Computational_data, store_as_json_string bool, chain *laila.TransformationSequence) (any, error) {
	env, err := Envelope_encode(cd, chain)
	if err != nil {
		return nil, err
	}
	if store_as_json_string {
		dumped, err := py.Json_dumps(env)
		if err != nil {
			return nil, err
		}
		if err := pool.Setitem(key, dumped); err != nil {
			return nil, err
		}
	} else {
		if err := pool.Setitem(key, env); err != nil {
			return nil, err
		}
	}
	got, err := pool.Getitem(key)
	if err != nil {
		return nil, err
	}
	// Backends that go through JSON re-parse during read, yielding a dict.
	if s, ok := got.(string); ok {
		got, err = py.Json_loads(s)
		if err != nil {
			return nil, err
		}
	}
	var dict *py.Dict
	switch x := got.(type) {
	case *py.Dict:
		dict = x
	case map[string]any:
		dict = py.DictFromMap(x)
	case py.Kw:
		dict = py.DictFromKw(x)
	default:
		return nil, py.TypeError.Newf("pool returned %T, expected dict", got)
	}
	return Envelope_decode(dict)
}

// =============================================================================
// Assertion helpers
// =============================================================================

// Assert_np_roundtrip checks dtype, shape and buffer equality (NaN-aware:
// the buffer comparison preserves NaN positions exactly).
func Assert_np_roundtrip(t *testing.T, recovered any, original *py.NDArray) {
	t.Helper()
	arr, ok := recovered.(*py.NDArray)
	if !ok {
		t.Fatalf("recovered %T, want *py.NDArray", recovered)
	}
	if arr.Dtype != original.Dtype {
		t.Fatalf("numpy dtype drift: %s -> %s", original.Dtype, arr.Dtype)
	}
	if !py.Eq(py.Iter(arr.Shape), py.Iter(original.Shape)) {
		t.Fatalf("shape drift: %v != %v", arr.Shape, original.Shape)
	}
	if !py.Eq(arr, original) {
		t.Fatalf("buffer drift for dtype=%s shape=%v", original.Dtype, original.Shape)
	}
}

// =============================================================================
// Test registration: run the dtype matrix + edges as subtests
// =============================================================================

// Register_dtype_matrix runs the full dtype-travel matrix as subtests of *t*.
//
// *make_pool* is a zero-arg factory producing a fresh pool instance; it is
// closed via Safe_close after each case. *store_as_json_string* wraps the
// envelope dict with json.dumps before writing to the pool; use false for the
// in-memory base pool that accepts any object.
//
// Returns the number of subtests registered.
func Register_dtype_matrix(t *testing.T, make_pool func() Pool, store_as_json_string bool) int {
	t.Helper()
	registered := 0

	roundtrip := func(pool Pool, key string, cd laila.Computational_data, mode string) (any, error) {
		var chain *laila.TransformationSequence
		if mode != "raw_b64" {
			b64, err := laila.New_Base64()
			if err != nil {
				return nil, err
			}
			zlib, err := laila.New_Zlib()
			if err != nil {
				return nil, err
			}
			chain, err = laila.New_TransformationSequence(py.Kw{"transformations": []any{b64, zlib}})
			if err != nil {
				return nil, err
			}
		}
		return Pool_roundtrip(pool, key, cd, store_as_json_string, chain)
	}

	run := func(name, key, mode string, build func() *py.NDArray) {
		registered++
		t.Run(name, func(t *testing.T) {
			arr := build()
			pool := make_pool()
			defer Safe_close(pool)
			cd, err := laila.New_ComputationalData(arr)
			if err != nil {
				t.Fatal(err)
			}
			recovered, err := roundtrip(pool, key, cd, mode)
			if err != nil {
				t.Fatal(err)
			}
			Assert_np_roundtrip(t, recovered, arr)
		})
	}

	// --- numpy dtype matrix ---
	for _, dtype_name := range NUMPY_DTYPES {
		for _, mode := range []string{"raw_b64", "chain_b64_zlib"} {
			dtype_name, mode := dtype_name, mode
			run(fmt.Sprintf("test_pool_np_%s_%s", dtype_name, mode),
				fmt.Sprintf("np_%s_%s", dtype_name, mode), mode,
				func() *py.NDArray { return Build_np(dtype_name, []int{2, 3}) })
		}
	}

	// --- numpy edges ---
	np_edge := func(name string, build func() *py.NDArray) {
		run("test_pool_np_edge_"+name, "np_edge_"+name, "raw_b64", build)
	}

	// np.arange(24, dtype=np.float32).reshape(4, 6)[:, ::2]
	np_edge("noncontiguous_slice", func() *py.NDArray {
		vals := make([]float32, 0, 12)
		for r := 0; r < 4; r++ {
			for c := 0; c < 6; c += 2 {
				vals = append(vals, float32(r*6+c))
			}
		}
		return py.NDArray_float32([]int{4, 3}, vals)
	})
	// np.arange(12, dtype=np.int32).reshape(3, 4).T -- F-contiguous, so
	// np.save keeps the original buffer and sets fortran_order=True.
	np_edge("noncontiguous_transpose", func() *py.NDArray {
		buf := make([]byte, 0, 48)
		for i := int32(0); i < 12; i++ {
			buf = binary.LittleEndian.AppendUint32(buf, uint32(i))
		}
		return &py.NDArray{Dtype: "<i4", Shape: []int{4, 3}, Data: buf, Fortran_order: true}
	})
	// np.asfortranarray(np.arange(12, dtype=np.float64).reshape(3, 4))
	np_edge("fortran_order", func() *py.NDArray {
		arr := py.NDArray_float64([]int{3, 4}, []float64{0, 4, 8, 1, 5, 9, 2, 6, 10, 3, 7, 11})
		arr.Fortran_order = true
		return arr
	})
	// np.arange(8).astype(">i4")
	np_edge("big_endian_i4", func() *py.NDArray {
		buf := make([]byte, 0, 32)
		for i := int32(0); i < 8; i++ {
			buf = binary.BigEndian.AppendUint32(buf, uint32(i))
		}
		return &py.NDArray{Dtype: ">i4", Shape: []int{8}, Data: buf}
	})
	// np.arange(8).astype("<i4")
	np_edge("little_endian_i4", func() *py.NDArray {
		buf := make([]byte, 0, 32)
		for i := int32(0); i < 8; i++ {
			buf = binary.LittleEndian.AppendUint32(buf, uint32(i))
		}
		return &py.NDArray{Dtype: "<i4", Shape: []int{8}, Data: buf}
	})
	// np.linspace(-1.0, 1.0, 6).astype(">f8")
	np_edge("big_endian_f8", func() *py.NDArray {
		buf := make([]byte, 0, 48)
		for i := 0; i < 6; i++ {
			buf = binary.BigEndian.AppendUint64(buf, math.Float64bits(linspace_(i, 6, -1.0, 1.0)))
		}
		return &py.NDArray{Dtype: ">f8", Shape: []int{6}, Data: buf}
	})
	// np.array([np.nan, np.inf, -np.inf, 0.0, 1.0], dtype=np.float32)
	np_edge("nan_inf_float32", func() *py.NDArray {
		return py.NDArray_float32([]int{5}, []float32{float32(math.NaN()), float32(math.Inf(1)), float32(math.Inf(-1)), 0.0, 1.0})
	})
	// np.array([tiny / 2, tiny, 0.0], dtype=np.float64)
	np_edge("subnormal_float64", func() *py.NDArray {
		tiny := 2.2250738585072014e-308 // np.finfo(np.float64).tiny
		return py.NDArray_float64([]int{3}, []float64{tiny / 2, tiny, 0.0})
	})
	// np.array([iinfo.min, 0, iinfo.max], dtype=np.int64)
	np_edge("min_max_int64", func() *py.NDArray {
		return py.NDArray_int64([]int{3}, []int64{math.MinInt64, 0, math.MaxInt64})
	})
	// np.array(3.5, dtype=np.float32)
	np_edge("zero_d_scalar", func() *py.NDArray {
		return py.NDArray_float32([]int{}, []float32{3.5})
	})
	// np.zeros((0,), dtype=np.int32)
	np_edge("empty_1d", func() *py.NDArray {
		return &py.NDArray{Dtype: "<i4", Shape: []int{0}, Data: []byte{}}
	})
	// np.array([1 + 2j, -3 + 4j, 0 - 1j], dtype=np.complex64)
	np_edge("complex_with_imag", func() *py.NDArray {
		buf := make([]byte, 0, 24)
		for _, p := range [][2]float32{{1, 2}, {-3, 4}, {0, -1}} {
			buf = binary.LittleEndian.AppendUint32(buf, math.Float32bits(p[0]))
			buf = binary.LittleEndian.AppendUint32(buf, math.Float32bits(p[1]))
		}
		return &py.NDArray{Dtype: "<c8", Shape: []int{3}, Data: buf}
	})
	// np.array([(1, 2.0), (3, 4.0)], dtype=[("a", "i4"), ("b", "f8")])
	np_edge("structured_dtype", func() *py.NDArray {
		buf := make([]byte, 0, 24)
		for _, rec := range []struct {
			a int32
			b float64
		}{{1, 2.0}, {3, 4.0}} {
			buf = binary.LittleEndian.AppendUint32(buf, uint32(rec.a))
			buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(rec.b))
		}
		return &py.NDArray{Dtype: "[('a', '<i4'), ('b', '<f8')]", Shape: []int{2}, Data: buf}
	})
	np_edge("subarray_dtype", Make_subarray_np_)
	// np.array(["2024-01-01", "2024-12-31"], dtype="datetime64[D]")
	np_edge("datetime64", func() *py.NDArray {
		buf := make([]byte, 0, 16)
		for _, d := range []int64{19723, 20088} {
			buf = binary.LittleEndian.AppendUint64(buf, uint64(d))
		}
		return &py.NDArray{Dtype: "<M8[D]", Shape: []int{2}, Data: buf}
	})
	// np.arange(10, dtype=np.int64)[::-1]
	np_edge("negative_strides_view", func() *py.NDArray {
		vals := make([]int64, 10)
		for i := range vals {
			vals[i] = int64(9 - i)
		}
		return py.NDArray_int64([]int{10}, vals)
	})

	// --- torch dtype matrix + edges: skipped, the port has no torch ---

	return registered
}

// Make_subarray_np_ is ``_make_subarray_np``: a 4-record array with a
// ``("coord", "f4", (3,))`` field holding arange(12).reshape(4, 3).
func Make_subarray_np_() *py.NDArray {
	buf := make([]byte, 0, 48)
	for i := 0; i < 12; i++ {
		buf = binary.LittleEndian.AppendUint32(buf, math.Float32bits(float32(i)))
	}
	return &py.NDArray{Dtype: "[('coord', '<f4', (3,))]", Shape: []int{4}, Data: buf}
}

// Safe_close closes *pool* if it exposes ``close``; swallows errors.
func Safe_close(pool any) {
	if closer, ok := pool.(interface{ Close() error }); ok {
		_ = closer.Close()
	}
}

// =============================================================================
// Small temp-dir factory (shared)
// =============================================================================

// Make_tmpdir is ``tempfile.mkdtemp(prefix=prefix)``.
func Make_tmpdir(prefix string) string {
	dir, err := os.MkdirTemp("", prefix)
	if err != nil {
		panic(err)
	}
	return dir
}
