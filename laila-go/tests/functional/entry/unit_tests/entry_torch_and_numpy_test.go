// Port of tests/functional/entry/unit_tests/test_entry_torch_and_numpy.py.
//
// NumPy payloads are the port's NDArray (dtype + shape + buffer); numpy's
// non-contiguous views are materialised to C order by np.save, so the
// strided / transposed cases are expressed as their C-order equivalents.
// Torch cases are skipped (TorchSerializer is nil in the port).
package unit_tests

import (
	"encoding/binary"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

var testFernetKey = py.Fernet_generate_key()

func setKey(t *testing.T) {
	t.Helper()
	// Recovery code looks the key up in ``laila.encryption_key``; the
	// reading side (this process) must therefore configure the same key.
	original := laila.Encryption_key()
	laila.Set_encryption_key(testFernetKey)
	t.Cleanup(func() { laila.Set_encryption_key(original) })
}

func assertNpEqual(t *testing.T, got any, expected *py.NDArray) {
	t.Helper()
	arr, ok := got.(*py.NDArray)
	if !ok {
		t.Fatalf("got %T, want *py.NDArray", got)
	}
	if !py.Eq(arr.Shape, expected.Shape) {
		t.Fatalf("shape %v != %v", arr.Shape, expected.Shape)
	}
	if arr.Dtype != expected.Dtype {
		t.Fatalf("dtype %s != %s", arr.Dtype, expected.Dtype)
	}
	if !py.Eq(arr, expected) {
		t.Fatal("array values differ")
	}
}

func rt(t *testing.T, arr *py.NDArray, preset *laila.TransformationSequence) *laila.Entry {
	t.Helper()
	msg := must(must(laila.Constant(arr)).Serialize(preset))
	return must(laila.Entry_build_from_dict_sync_(msg)).(*laila.Entry)
}

func encPreset() *laila.TransformationSequence {
	return must(laila.Transformation_base64_compression_encryption(testFernetKey))
}

func i32s(vals ...int32) *py.NDArray {
	buf := make([]byte, 4*len(vals))
	for i, v := range vals {
		binary.LittleEndian.PutUint32(buf[4*i:], uint32(v))
	}
	return &py.NDArray{Dtype: "<i4", Shape: []int{len(vals)}, Data: buf}
}

func i16s(shape []int, vals ...int16) *py.NDArray {
	buf := make([]byte, 2*len(vals))
	for i, v := range vals {
		binary.LittleEndian.PutUint16(buf[2*i:], uint16(v))
	}
	return &py.NDArray{Dtype: "<i2", Shape: shape, Data: buf}
}

func arangeF64(n int, shape []int) *py.NDArray {
	vals := make([]float64, n)
	for i := range vals {
		vals[i] = float64(i)
	}
	return py.NDArray_float64(shape, vals)
}

func arangeF32(n int, shape []int) *py.NDArray {
	vals := make([]float32, n)
	for i := range vals {
		vals[i] = float32(i)
	}
	return py.NDArray_float32(shape, vals)
}

// -------------------- NumPy: core round-trips --------------------

func Test001NumpyB64Int32_1d(t *testing.T) {
	setKey(t)
	arr := i32s(1, 2, 3)
	assertNpEqual(t, data(t, rt(t, arr, laila.Transformation_base64)), arr)
}

func Test002NumpyB64ZlibFloat64_2d(t *testing.T) {
	setKey(t)
	arr := arangeF64(12, []int{3, 4})
	assertNpEqual(t, data(t, rt(t, arr, laila.Transformation_base64_compression)), arr)
}

func Test003NumpyB64ZlibFernetInt16_3dNoncontig(t *testing.T) {
	setKey(t)
	// np.arange(24, dtype=int16).reshape(2, 3, 4).T materialised in C order.
	vals := make([]int16, 24)
	k := 0
	for i := 0; i < 4; i++ {
		for j := 0; j < 3; j++ {
			for l := 0; l < 2; l++ {
				vals[k] = int16(l*12 + j*4 + i)
				k++
			}
		}
	}
	arr := i16s([]int{4, 3, 2}, vals...)
	assertNpEqual(t, data(t, rt(t, arr, encPreset())), arr)
}

func Test004NumpyEmptyFloat32(t *testing.T) {
	setKey(t)
	arr := py.NDArray_float32([]int{0}, nil)
	assertNpEqual(t, data(t, rt(t, arr, laila.Transformation_base64)), arr)
}

func Test005NumpyFortranOrderFloat32(t *testing.T) {
	setKey(t)
	arr := py.NDArray_float32([]int{3, 3}, []float32{0, 3, 6, 1, 4, 7, 2, 5, 8})
	arr.Fortran_order = true
	assertNpEqual(t, data(t, rt(t, arr, laila.Transformation_base64_compression)), arr)
}

func Test006NumpyBool_1d(t *testing.T) {
	setKey(t)
	arr := py.NDArray_bool([]int{3}, []bool{true, false, true})
	assertNpEqual(t, data(t, rt(t, arr, encPreset())), arr)
}

// -------------------- NumPy: dtype & shape fidelity --------------------

func Test007NumpyDtypeShapeUint8(t *testing.T) {
	setKey(t)
	arr := py.NDArray_uint8([]int{5}, []byte{0, 1, 2, 3, 4})
	r := data(t, rt(t, arr, laila.Transformation_base64)).(*py.NDArray)
	if r.Dtype != "|u1" || !py.Eq(r.Shape, []int{5}) {
		t.Fatalf("dtype=%s shape=%v", r.Dtype, r.Shape)
	}
}

func Test008NumpyStridedSliceInt64(t *testing.T) {
	setKey(t)
	// np.arange(60).reshape(3, 4, 5)[:, :, ::2] materialised in C order.
	vals := []int64{}
	for i := 0; i < 3; i++ {
		for j := 0; j < 4; j++ {
			for l := 0; l < 5; l += 2 {
				vals = append(vals, int64(i*20+j*5+l))
			}
		}
	}
	arr := py.NDArray_int64([]int{3, 4, 3}, vals)
	assertNpEqual(t, data(t, rt(t, arr, laila.Transformation_base64_compression)), arr)
}

// -------------------- Torch: skipped (not installed) --------------------

func Test009to014TorchSkipped(t *testing.T) {
	if laila.TorchSerializer_class == nil {
		t.Skip("PyTorch not installed")
	}
}

// -------------------- Mixed preset smoke (NumPy) --------------------

func Test015NumpyAllPresetsSmoke(t *testing.T) {
	setKey(t)
	vals := make([]float64, 17)
	for i := range vals {
		vals[i] = float64(i) / 16
	}
	arr := py.NDArray_float64([]int{17, 1}, vals)
	assertNpEqual(t, data(t, rt(t, arr, laila.Transformation_base64)), arr)
	assertNpEqual(t, data(t, rt(t, arr, laila.Transformation_base64_compression)), arr)
	assertNpEqual(t, data(t, rt(t, arr, encPreset())), arr)
}

func Test016TorchAllPresetsSkipped(t *testing.T) {
	if laila.TorchSerializer_class == nil {
		t.Skip("PyTorch not installed")
	}
}
