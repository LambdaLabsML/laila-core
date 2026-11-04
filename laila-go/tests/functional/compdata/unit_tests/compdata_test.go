// Port of tests/functional/compdata/unit_tests/test_compdata.py.
//
// The numpy dtype x shape x mode matrix is expressed on the port's NDArray
// (dtype descriptor + shape + raw buffer): a round trip must preserve the
// dtype string, the shape and the buffer bit-for-bit, which subsumes the
// Python value / nan-mask checks. Non-contiguous views (slices,
// transposes, negative strides) have no NDArray counterpart -- numpy
// materialises them to C order on save -- and are covered by the
// Fortran-order case. Torch cases are skipped exactly as in Python when
// torch is not installed (TorchSerializer is nil in the port).
package unit_tests

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func assertEq(t *testing.T, got, want any) {
	t.Helper()
	if !py.Eq(got, want) {
		t.Fatalf("got %s, want %s", py.Repr(got), py.Repr(want))
	}
}

func assertRaises(t *testing.T, exc *py.Type, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s, got nil", exc.Name)
	}
	if !errors.Is(err, exc) {
		t.Fatalf("expected %s, got %v", exc.Name, err)
	}
}

func cd(v any) laila.Computational_data { return must(laila.New_ComputationalData(v)) }

func className(c laila.Computational_data) string { return c.Class_().Name }

// buildSimple builds a value through a SimpleConstitution chain; wraps
// in a ComputationalData if needed.
func buildSimple(payload any, codes []string) (laila.Computational_data, error) {
	sc, err := laila.New_SimpleConstitution(py.Kw{"codes": codes})
	if err != nil {
		return nil, err
	}
	result, err := sc.Build(payload)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}
	if c, ok := result.(laila.Computational_data); ok {
		return c, nil
	}
	return laila.New_ComputationalData(result)
}

func serialize(c laila.Computational_data) (any, string) {
	payload, code, err := c.Serialize()
	if err != nil {
		panic(err)
	}
	return payload, code
}

// roundtrip is the serializer-only round trip.
func roundtrip(c laila.Computational_data) any {
	payload, code := serialize(c)
	out := must(buildSimple(payload, []string{code}))
	return out.Base_().Data
}

func base64Zlib() *laila.TransformationSequence {
	return must(laila.New_TransformationSequence(py.Kw{
		"transformations": []any{must(laila.New_Base64()), must(laila.New_Zlib())},
	}))
}

// roundtripChain round-trips through Base64 -> Zlib + serializer.
func roundtripChain(c laila.Computational_data) any {
	payload, code := serialize(c)
	transformed, inverse, err := base64Zlib().Forward(payload)
	if err != nil {
		panic(err)
	}
	out := must(buildSimple(transformed, append(append([]string{}, inverse...), code)))
	return out.Base_().Data
}

func npInt64(vals ...int64) *py.NDArray { return py.NDArray_int64([]int{len(vals)}, vals) }

func isBytes(v any) bool {
	_, ok := v.([]byte)
	return ok
}

// --- 1..14, 18 --------------------------------------------------------------

func Test001InitVariants(t *testing.T) {
	a := must(laila.New_ComputationalData(py.Kw{"data": npInt64(1, 2)}))
	b := cd(npInt64(3, 4))
	assertEq(t, must(a.Shape()), py.Tuple{2})
	assertEq(t, must(b.Shape()), py.Tuple{2})
}

func Test002ConflictingInit(t *testing.T) {
	_, err := laila.New_ComputationalData(npInt64(1, 2), py.Kw{"data": npInt64(3, 4)})
	assertRaises(t, py.TypeError, err)
}

func Test003FactoryNumpy(t *testing.T) {
	a := cd(npInt64(1, 2, 3))
	if !strings.Contains(className(a), "CD_numpyarray") {
		t.Fatal(className(a))
	}
	assertEq(t, must(a.Len()), 3)
	assertEq(t, must(a.Shape()), py.Tuple{3})
}

func Test004FactoryList(t *testing.T) {
	a := cd(py.NewList(1, 2, 3, 4))
	if !strings.Contains(className(a), "CD_list") {
		t.Fatal(className(a))
	}
	assertEq(t, must(a.Len()), 4)
	assertEq(t, must(a.Shape()), py.Tuple{4})
}

func Test005FactoryTuple(t *testing.T) {
	a := cd(py.Tuple{1, 2})
	if !strings.Contains(className(a), "CD_list") {
		t.Fatal(className(a))
	}
	assertEq(t, must(a.Len()), 2)
	assertEq(t, must(a.Shape()), py.Tuple{2})
}

func Test006FactoryDict(t *testing.T) {
	a := cd(py.DictOf("x", 1))
	if !strings.Contains(className(a), "CD_dict") {
		t.Fatal(className(a))
	}
	assertEq(t, must(a.Len()), 1)
	_, err := a.Shape()
	assertRaises(t, py.AttributeError, err)
}

func Test007FactoryGenericFallback(t *testing.T) {
	a := cd(py.NewSet(1, 2, 3))
	if !strings.Contains(className(a), "CD_generic") {
		t.Fatal(className(a))
	}
	assertEq(t, must(a.Len()), 3) // sets have __len__
}

func Test008SerializerRoundtripNoChain(t *testing.T) {
	arr := npInt64(1, 2, 3, 4)
	payload, code := serialize(cd(arr))
	if !isBytes(payload) {
		t.Fatalf("numpy serializer must produce bytes, got %T", payload)
	}
	b := must(buildSimple(payload, []string{code}))
	assertEq(t, b.Base_().Data, arr)
}

func Test009TransformChainBase64Zlib(t *testing.T) {
	arr := npInt64(1, 2, 3, 4)
	payload, code := serialize(cd(arr))
	transformed, inverse := must2(base64Zlib().Forward(payload))
	if _, ok := transformed.(string); !ok {
		t.Fatalf("transformed payload must be str, got %T", transformed)
	}
	if len(inverse) != 2 { // zlib backward, base64 backward
		t.Fatal(len(inverse))
	}
	recovered := must(buildSimple(transformed, append(inverse, code)))
	assertEq(t, recovered.Base_().Data, arr)
}

func must2[A, B any](a A, b B, err error) (A, B) {
	if err != nil {
		panic(err)
	}
	return a, b
}

func Test010TransformChainGetSet(t *testing.T) {
	a := cd(npInt64(0))
	old := a.Serializer()
	if old == nil {
		t.Fatal("serializer is None")
	}
	fresh := must(laila.New_transformation_(py.Classof(old), py.Kw{}))
	if err := a.Set_serializer(fresh); err != nil {
		t.Fatal(err)
	}
	if !py.Isinstance(a.Serializer(), py.Classof(old)) {
		t.Fatal("serializer class changed")
	}
	assertRaises(t, py.TypeError, a.Set_serializer(must(laila.New_Base64())))
}

func Test011GetitemProxy(t *testing.T) {
	assertEq(t, must(cd(py.NewList(10, 20, 30)).Getitem(1)), 20)
}

func Test012Repr(t *testing.T) {
	a := cd(py.DictOf("k", 9))
	r := py.Repr(a)
	if !strings.Contains(r, className(a)) || !strings.Contains(r, "'k': 9") {
		t.Fatal(r)
	}
}

func Test013ScalarIntRoundtrip(t *testing.T) {
	a := cd(12345)
	if !strings.Contains(className(a), "CD_generic") {
		t.Fatal(className(a))
	}
	_, err := a.Len()
	assertRaises(t, py.TypeError, err)
	payload, code := serialize(a)
	if !isBytes(payload) {
		t.Fatalf("%T", payload)
	}
	assertEq(t, must(buildSimple(payload, []string{code})).Base_().Data, 12345)
}

func Test014ScalarFloatChainRoundtrip(t *testing.T) {
	payload, code := serialize(cd(3.14159))
	transformed, inverse := must2(base64Zlib().Forward(payload))
	if _, ok := transformed.(string); !ok {
		t.Fatalf("%T", transformed)
	}
	assertEq(t, must(buildSimple(transformed, append(inverse, code))).Base_().Data, 3.14159)
}

func Test015to017TorchSkipped(t *testing.T) {
	if laila.TorchSerializer_class == nil {
		t.Skip("PyTorch not installed")
	}
}

type noLen struct{}

func Test018NoLenGeneric(t *testing.T) {
	a := cd(noLen{})
	if !strings.Contains(className(a), "CD_generic") {
		t.Fatal(className(a))
	}
	_, err := a.Len()
	assertRaises(t, py.TypeError, err)
}

// =============================================================================
// Dtype-travel test matrix for numpy entries
// =============================================================================

var npDtypes = []string{
	"<i1", "<i2", "<i4", "<i8", "|u1", "<u2", "<u4", "<u8",
	"<f2", "<f4", "<f8", "|b1", "<c8", "<c16",
}

var shapes = [][]int{
	{},        // 0-d scalar
	{0},       // empty 1-D
	{1},       // singleton
	{5},       // small 1-D
	{3, 4},    // 2-D
	{2, 3, 4}, // 3-D
}

func shapeTag(shape []int) string {
	if len(shape) == 0 {
		return "scalar"
	}
	parts := make([]string, len(shape))
	for i, d := range shape {
		parts[i] = fmt.Sprint(d)
	}
	return strings.Join(parts, "x")
}

// genNp builds a deterministic array of the given dtype and shape: ints
// count up (arange), floats sweep linspace(-1, 1), complex pairs a sweep
// with a reversed one, bools alternate.
func genNp(dtype string, shape []int) *py.NDArray {
	normalized := dtype
	if normalized[0] == '|' {
		normalized = "<" + normalized[1:]
	}
	size := 1
	for _, d := range shape {
		size *= d
	}
	itemsize := py.NDArray_itemsize_(dtype)
	buf := make([]byte, size*itemsize)
	lin := func(i, n int, lo, hi float64) float64 {
		if n <= 1 {
			return lo
		}
		return lo + (hi-lo)*float64(i)/float64(n-1)
	}
	for i := 0; i < size; i++ {
		b := buf[i*itemsize:]
		switch normalized {
		case "<i1", "<u1":
			b[0] = byte(i)
		case "<i2", "<u2":
			binary.LittleEndian.PutUint16(b, uint16(i))
		case "<i4", "<u4":
			binary.LittleEndian.PutUint32(b, uint32(i))
		case "<i8", "<u8":
			binary.LittleEndian.PutUint64(b, uint64(i))
		case "<f2":
			binary.LittleEndian.PutUint16(b, py.Float16bits(lin(i, size, -1, 1)))
		case "<f4":
			binary.LittleEndian.PutUint32(b, math.Float32bits(float32(lin(i, size, -1, 1))))
		case "<f8":
			binary.LittleEndian.PutUint64(b, math.Float64bits(lin(i, size, -1, 1)))
		case "<b1":
			if i%2 == 0 {
				b[0] = 1
			}
		case "<c8":
			binary.LittleEndian.PutUint32(b, math.Float32bits(float32(lin(i, size, -1, 1))))
			binary.LittleEndian.PutUint32(b[4:], math.Float32bits(float32(lin(i, size, 0.5, -0.5))))
		case "<c16":
			binary.LittleEndian.PutUint64(b, math.Float64bits(lin(i, size, -1, 1)))
			binary.LittleEndian.PutUint64(b[8:], math.Float64bits(lin(i, size, 0.5, -0.5)))
		}
	}
	return &py.NDArray{Dtype: dtype, Shape: append([]int{}, shape...), Data: buf}
}

func assertNpEqual(t *testing.T, recovered any, original *py.NDArray) {
	t.Helper()
	r, ok := recovered.(*py.NDArray)
	if !ok {
		t.Fatalf("recovered %T, want *py.NDArray", recovered)
	}
	if r.Dtype != original.Dtype {
		t.Fatalf("dtype drift: %s != %s", r.Dtype, original.Dtype)
	}
	if !py.Eq(r.Shape, original.Shape) {
		t.Fatalf("shape drift: %v != %v", r.Shape, original.Shape)
	}
	if !py.Eq(r, original) {
		t.Fatalf("buffer drift for dtype=%s shape=%v", original.Dtype, original.Shape)
	}
}

func TestNpMatrix(t *testing.T) {
	for _, dt := range npDtypes {
		for _, sh := range shapes {
			for _, mode := range []string{"raw", "chain"} {
				tag := fmt.Sprintf("%s_%s_%s", strings.TrimLeft(dt, "<|"), shapeTag(sh), mode)
				t.Run(tag, func(t *testing.T) {
					arr := genNp(dt, sh)
					var recovered any
					if mode == "raw" {
						recovered = roundtrip(cd(arr))
					} else {
						recovered = roundtripChain(cd(arr))
					}
					assertNpEqual(t, recovered, arr)
				})
			}
		}
	}
}

// --- Numpy edge cases ---------------------------------------------------------

func f64s(vals ...float64) *py.NDArray { return py.NDArray_float64([]int{len(vals)}, vals) }
func f32s(vals ...float32) *py.NDArray { return py.NDArray_float32([]int{len(vals)}, vals) }

func withDtype(dtype string, shape []int, data []byte) *py.NDArray {
	return &py.NDArray{Dtype: dtype, Shape: shape, Data: data}
}

func beI4(vals ...int32) *py.NDArray {
	buf := make([]byte, 4*len(vals))
	for i, v := range vals {
		binary.BigEndian.PutUint32(buf[4*i:], uint32(v))
	}
	return withDtype(">i4", []int{len(vals)}, buf)
}

func beF8(vals ...float64) *py.NDArray {
	buf := make([]byte, 8*len(vals))
	for i, v := range vals {
		binary.BigEndian.PutUint64(buf[8*i:], math.Float64bits(v))
	}
	return withDtype(">f8", []int{len(vals)}, buf)
}

func f16s(vals ...float64) *py.NDArray {
	buf := make([]byte, 2*len(vals))
	for i, v := range vals {
		binary.LittleEndian.PutUint16(buf[2*i:], py.Float16bits(v))
	}
	return withDtype("<f2", []int{len(vals)}, buf)
}

func TestNpEdge(t *testing.T) {
	edge := func(name string, arr *py.NDArray, chain bool) {
		t.Run(name, func(t *testing.T) {
			var recovered any
			if chain {
				recovered = roundtripChain(cd(arr))
			} else {
				recovered = roundtrip(cd(arr))
			}
			assertNpEqual(t, recovered, arr)
		})
	}
	nan, inf := math.NaN(), math.Inf(1)

	// Fortran-order buffer (np.asfortranarray): the flag travels in the header.
	fortran := py.NDArray_float64([]int{3, 4}, []float64{0, 4, 8, 1, 5, 9, 2, 6, 10, 3, 7, 11})
	fortran.Fortran_order = true
	edge("fortran_order", fortran, false)
	edge("big_endian_i4", beI4(0, 1, 2, 3, 4), false)
	edge("little_endian_i4", withDtype("<i4", []int{5}, []byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0}), false)
	edge("big_endian_f8", beF8(-1, -0.6, -0.2, 0.2, 0.6, 1), false)
	edge("nan_float16", f16s(1.0, nan, -1.0, nan), false)
	edge("nan_float32", f32s(1.0, float32(nan), -1.0), false)
	edge("nan_float64", f64s(nan, 0.0, nan), false)
	edge("inf_float32", f32s(float32(inf), float32(-inf), 0.0), false)
	edge("inf_float64", f64s(inf, -inf, nan), false)
	tiny := 2.2250738585072014e-308 // np.finfo(np.float64).tiny
	edge("subnormal_float64", f64s(tiny/2, tiny, -tiny/2), false)

	// dtype min / max
	edge("min_max_int8", withDtype("|i1", []int{3}, []byte{0x80, 0, 0x7f}), false)
	edge("min_max_int16", withDtype("<i2", []int{3}, []byte{0, 0x80, 0, 0, 0xff, 0x7f}), false)
	edge("min_max_int32", withDtype("<i4", []int{3}, []byte{0, 0, 0, 0x80, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0x7f}), false)
	edge("min_max_int64", npInt64(math.MinInt64, 0, math.MaxInt64), false)
	edge("min_max_uint8", py.NDArray_uint8([]int{3}, []byte{0, 0, 255}), false)
	edge("min_max_uint64", withDtype("<u8", []int{3}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}), false)
	edge("float_min_max_float16", f16s(-65504, -0.000977, 0, 0.000977, 65504), false)
	edge("float_min_max_float32", f32s(-math.MaxFloat32, -1.1920929e-07, 0, 1.1920929e-07, math.MaxFloat32), false)
	edge("float_min_max_float64", f64s(-math.MaxFloat64, -2.220446049250313e-16, 0, 2.220446049250313e-16, math.MaxFloat64), false)

	// complex64 [1+2j, -3+4j, 0-1j]
	c64 := make([]byte, 0, 24)
	for _, p := range [][2]float32{{1, 2}, {-3, 4}, {0, -1}} {
		c64 = binary.LittleEndian.AppendUint32(c64, math.Float32bits(p[0]))
		c64 = binary.LittleEndian.AppendUint32(c64, math.Float32bits(p[1]))
	}
	edge("complex_with_imag", withDtype("<c8", []int{3}, c64), false)

	large := make([]float32, 100_000)
	for i := range large {
		large[i] = float32(-1 + 2*float64(i)/float64(len(large)-1))
	}
	edge("large_1d_float32", f32s(large...), false)

	// structured dtypes: the descr travels as its Python literal.
	structured := make([]byte, 0, 24)
	for _, rec := range []struct {
		a int32
		b float64
	}{{1, 2.0}, {3, 4.0}} {
		structured = binary.LittleEndian.AppendUint32(structured, uint32(rec.a))
		structured = binary.LittleEndian.AppendUint64(structured, math.Float64bits(rec.b))
	}
	edge("structured_dtype", withDtype("[('a', '<i4'), ('b', '<f8')]", []int{2}, structured), false)
	aligned := make([]byte, 0, 32)
	for _, rec := range []struct {
		a int8
		b float64
	}{{1, 2.0}, {3, 4.0}} {
		aligned = append(aligned, byte(rec.a), 0, 0, 0, 0, 0, 0, 0)
		aligned = binary.LittleEndian.AppendUint64(aligned, math.Float64bits(rec.b))
	}
	alignedArr := withDtype("[('a', '|i1'), ('', '|V7'), ('b', '<f8')]", []int{2}, aligned)
	if alignedArr.Itemsize() != 16 {
		t.Fatalf("aligned itemsize %d", alignedArr.Itemsize())
	}
	edge("structured_aligned", alignedArr, false)
	sub := make([]byte, 0, 48)
	for i := 0; i < 12; i++ {
		sub = binary.LittleEndian.AppendUint32(sub, math.Float32bits(float32(i)))
	}
	edge("subarray_dtype", withDtype("[('coord', '<f4', (3,))]", []int{4}, sub), false)
	rec := make([]byte, 0, 16)
	for _, r := range []struct {
		x int32
		y float32
	}{{1, 1.5}, {2, 2.5}} {
		rec = binary.LittleEndian.AppendUint32(rec, uint32(r.x))
		rec = binary.LittleEndian.AppendUint32(rec, math.Float32bits(r.y))
	}
	edge("record_array", withDtype("[('x', '<i4'), ('y', '<f4')]", []int{2}, rec), false)
	// datetime64[D]: days since epoch for 2024-01-01 and 2024-12-31
	dates := make([]byte, 0, 16)
	for _, d := range []int64{19723, 20088} {
		dates = binary.LittleEndian.AppendUint64(dates, uint64(d))
	}
	edge("datetime64", withDtype("<M8[D]", []int{2}, dates), false)
	deltas := make([]byte, 0, 24)
	for _, d := range []int64{1, 2, 3} {
		deltas = binary.LittleEndian.AppendUint64(deltas, uint64(d))
	}
	edge("timedelta64", withDtype("<m8[D]", []int{3}, deltas), false)
	edge("void_bytes", withDtype("[('v', '|V3')]", []int{2}, []byte("abcdef")), false)
	edge("chain_big_endian_i4", beI4(0, 1, 2, 3, 4, 5, 6, 7), true)
	edge("chain_structured_dtype", withDtype("[('a', '<i4'), ('b', '<f8')]", []int{2}, structured), true)
}

// --- Torch matrix and edges: skipped (TorchSerializer is nil in the port).

func TestTorchMatrixSkipped(t *testing.T) {
	if laila.TorchSerializer_class == nil {
		t.Skip("PyTorch not installed")
	}
}
