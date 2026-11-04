package py

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
)

// NDArray is the port's numpy.ndarray: a dtype descriptor string in
// numpy's array-protocol form ("<f8", "<i4", "|b1", "|u1", "<U3", "|S4"),
// a shape, and the raw C-order (or Fortran-order) buffer.
type NDArray struct {
	Dtype         string
	Shape         []int
	Data          []byte
	Fortran_order bool
}

// Pytype is "ndarray".
func (a *NDArray) Pytype() string { return "ndarray" }

// Size is a.size.
func (a *NDArray) Size() int {
	n := 1
	for _, d := range a.Shape {
		n *= d
	}
	return n
}

// Ndim is a.ndim.
func (a *NDArray) Ndim() int { return len(a.Shape) }

// Itemsize is a.itemsize derived from the dtype string.
func (a *NDArray) Itemsize() int { return dtypeItemsize(a.Dtype) }

// dtypeItemsize is np.dtype(descr).itemsize for the array-protocol
// strings the port handles: simple descrs ("<f8", "|b1", "<U3", "|V3",
// datetime / timedelta "<M8[D]" / "<m8[ns]"), structured list descrs
// ("[('a', '<i4'), ('b', '<f8'), ('c', '<f4', (3,))]") and the dict form
// numpy emits for aligned / offset layouts ({'names': ..., 'formats': ...,
// 'offsets': ..., 'itemsize': N}).
func dtypeItemsize(d string) int {
	if len(d) < 2 {
		return 0
	}
	if d[0] == '[' || d[0] == '{' {
		v, err := Literal_eval(d)
		if err != nil {
			return 0
		}
		return structuredItemsize(v)
	}
	kind := d[1]
	num := d[2:]
	if i := strings.IndexByte(num, '['); i >= 0 { // datetime64[unit]
		num = num[:i]
	}
	n, err := strconv.Atoi(num)
	if err != nil {
		return 0
	}
	if kind == 'U' {
		return 4 * n
	}
	return n
}

// structuredItemsize computes the itemsize of a parsed structured descr.
func structuredItemsize(v any) int {
	switch x := v.(type) {
	case *Dict:
		if n, ok := x.Get("itemsize"); ok {
			if i, ok := n.(int); ok {
				return i
			}
		}
		total := 0
		for _, f := range Iter(x.GetDefault("formats", NewList())) {
			total += fieldItemsize(f)
		}
		return total
	case *List:
		total := 0
		for _, field := range Iter(x) {
			t, ok := field.(Tuple)
			if !ok || len(t) < 2 {
				return 0
			}
			size := fieldItemsize(t[1])
			if len(t) >= 3 {
				for _, dim := range Iter(t[2]) {
					if n, ok := dim.(int); ok {
						size *= n
					}
				}
			}
			total += size
		}
		return total
	}
	return 0
}

func fieldItemsize(f any) int {
	switch x := f.(type) {
	case string:
		return dtypeItemsize(normalizeDescr(x))
	default:
		return structuredItemsize(f)
	}
}

// normalizeDescr gives byte-order-less descrs ("i4") the native prefix.
func normalizeDescr(d string) string {
	if d == "" {
		return d
	}
	switch d[0] {
	case '<', '>', '|', '=':
		return d
	}
	if dtypeItemsize("<"+d) == 1 {
		return "|" + d
	}
	return "<" + d
}

// NDArray_itemsize_ is np.dtype(descr).itemsize.
func NDArray_itemsize_(descr string) int { return dtypeItemsize(descr) }

// Float16bits is np.float16(f).view(np.uint16): IEEE 754 binary16 with
// round-to-nearest-even, inf / nan preserved.
func Float16bits(f float64) uint16 {
	bits := math.Float64bits(f)
	sign := uint16(bits>>48) & 0x8000
	exp := int((bits>>52)&0x7ff) - 1023
	mant := bits & ((1 << 52) - 1)
	switch {
	case math.IsNaN(f):
		return sign | 0x7e00
	case math.IsInf(f, 0):
		return sign | 0x7c00
	case f == 0:
		return sign
	}
	if exp > 15 {
		return sign | 0x7c00 // overflow -> inf
	}
	if exp < -14 {
		// subnormal: value = mant16 * 2^-24
		m := (mant | (1 << 52)) >> uint(-exp-14+42)
		rem := (mant | (1 << 52)) & ((1 << uint(-exp-14+42)) - 1)
		half := uint64(1) << uint(-exp-14+41)
		if rem > half || (rem == half && m&1 == 1) {
			m++
		}
		return sign | uint16(m)
	}
	m := mant >> 42
	rem := mant & ((1 << 42) - 1)
	if rem > 1<<41 || (rem == 1<<41 && m&1 == 1) {
		m++
		if m == 1<<10 {
			m = 0
			exp++
			if exp > 15 {
				return sign | 0x7c00
			}
		}
	}
	return sign | uint16(exp+15)<<10 | uint16(m)
}

// Float16frombits is the inverse of Float16bits.
func Float16frombits(h uint16) float64 {
	sign := 1.0
	if h&0x8000 != 0 {
		sign = -1
	}
	exp := int(h>>10) & 0x1f
	mant := float64(h & 0x3ff)
	switch exp {
	case 0:
		return sign * mant * math.Pow(2, -24)
	case 0x1f:
		if mant == 0 {
			return math.Inf(int(sign))
		}
		return math.NaN()
	}
	return sign * (1 + mant/1024) * math.Pow(2, float64(exp-15))
}

// Is_structured_dtype reports whether descr is a structured (record) dtype.
func Is_structured_dtype(descr string) bool {
	return len(descr) > 0 && (descr[0] == '[' || descr[0] == '{')
}

// Eq is np.array_equal semantics with dtype/shape equality.
func (a *NDArray) Eq(other any) bool {
	o, ok := other.(*NDArray)
	if !ok {
		return false
	}
	if a.Dtype != o.Dtype || len(a.Shape) != len(o.Shape) {
		return false
	}
	for i := range a.Shape {
		if a.Shape[i] != o.Shape[i] {
			return false
		}
	}
	return bytes.Equal(a.Data, o.Data) && a.Fortran_order == o.Fortran_order
}

// Repr is a compact array(...) form.
func (a *NDArray) Repr() string {
	return fmt.Sprintf("array(shape=%v, dtype=%s)", a.Shape, strings.TrimLeft(a.Dtype, "<>|="))
}

// NDArray_float64 builds a C-order float64 array.
func NDArray_float64(shape []int, data []float64) *NDArray {
	buf := make([]byte, 8*len(data))
	for i, f := range data {
		binary.LittleEndian.PutUint64(buf[8*i:], math.Float64bits(f))
	}
	return &NDArray{Dtype: "<f8", Shape: append([]int(nil), shape...), Data: buf}
}

// NDArray_int64 builds a C-order int64 array.
func NDArray_int64(shape []int, data []int64) *NDArray {
	buf := make([]byte, 8*len(data))
	for i, v := range data {
		binary.LittleEndian.PutUint64(buf[8*i:], uint64(v))
	}
	return &NDArray{Dtype: "<i8", Shape: append([]int(nil), shape...), Data: buf}
}

// NDArray_float32 builds a C-order float32 array.
func NDArray_float32(shape []int, data []float32) *NDArray {
	buf := make([]byte, 4*len(data))
	for i, f := range data {
		binary.LittleEndian.PutUint32(buf[4*i:], math.Float32bits(f))
	}
	return &NDArray{Dtype: "<f4", Shape: append([]int(nil), shape...), Data: buf}
}

// NDArray_uint8 builds a C-order uint8 array.
func NDArray_uint8(shape []int, data []byte) *NDArray {
	return &NDArray{Dtype: "|u1", Shape: append([]int(nil), shape...), Data: append([]byte(nil), data...)}
}

// NDArray_bool builds a C-order bool array.
func NDArray_bool(shape []int, data []bool) *NDArray {
	buf := make([]byte, len(data))
	for i, b := range data {
		if b {
			buf[i] = 1
		}
	}
	return &NDArray{Dtype: "|b1", Shape: append([]int(nil), shape...), Data: buf}
}

// Float64s decodes the buffer as float64 (dtype must be <f8).
func (a *NDArray) Float64s() ([]float64, error) {
	if a.Dtype != "<f8" {
		return nil, TypeError.Newf("array dtype is %s, not <f8", a.Dtype)
	}
	out := make([]float64, len(a.Data)/8)
	for i := range out {
		out[i] = math.Float64frombits(binary.LittleEndian.Uint64(a.Data[8*i:]))
	}
	return out, nil
}

// Int64s decodes the buffer as int64 (dtype must be <i8).
func (a *NDArray) Int64s() ([]int64, error) {
	if a.Dtype != "<i8" {
		return nil, TypeError.Newf("array dtype is %s, not <i8", a.Dtype)
	}
	out := make([]int64, len(a.Data)/8)
	for i := range out {
		out[i] = int64(binary.LittleEndian.Uint64(a.Data[8*i:]))
	}
	return out, nil
}

// Item is ``a.flat[i]`` as a Python scalar (int64 / float64 / bool) for
// the plain numeric dtypes (b1, i1..i8, u1..u8, f4, f8 in either byte
// order). NotImplementedError for anything else.
func (a *NDArray) Item(i int) (any, error) {
	d := a.Dtype
	if len(d) < 3 {
		return nil, NotImplementedError.Newf("item for dtype %s", d)
	}
	var order binary.ByteOrder = binary.LittleEndian
	if d[0] == '>' {
		order = binary.BigEndian
	}
	size := a.Itemsize()
	if size <= 0 || (i+1)*size > len(a.Data) {
		return nil, IndexError.Newf("index %d is out of bounds", i)
	}
	b := a.Data[i*size:]
	switch d[1:] {
	case "b1":
		return b[0] != 0, nil
	case "i1":
		return int64(int8(b[0])), nil
	case "u1":
		return int64(b[0]), nil
	case "i2":
		return int64(int16(order.Uint16(b))), nil
	case "u2":
		return int64(order.Uint16(b)), nil
	case "i4":
		return int64(int32(order.Uint32(b))), nil
	case "u4":
		return int64(order.Uint32(b)), nil
	case "i8":
		return int64(order.Uint64(b)), nil
	case "u8":
		v := order.Uint64(b)
		if v > math.MaxInt64 {
			return new(big.Int).SetUint64(v), nil
		}
		return int64(v), nil
	case "f4":
		return float64(math.Float32frombits(order.Uint32(b))), nil
	case "f8":
		return math.Float64frombits(order.Uint64(b)), nil
	}
	return nil, NotImplementedError.Newf("item for dtype %s", d)
}

// Tolist is ``a.tolist()``: nested *List of Python scalars following the
// array's shape (a 0-d array yields a single-element list holding the
// scalar, callers index ``[0]``).
func (a *NDArray) Tolist() (*List, error) {
	if len(a.Shape) == 0 {
		v, err := a.Item(0)
		if err != nil {
			return nil, err
		}
		return NewList(v), nil
	}
	strides := make([]int, len(a.Shape))
	if a.Fortran_order {
		s := 1
		for i := range a.Shape {
			strides[i] = s
			s *= a.Shape[i]
		}
	} else {
		s := 1
		for i := len(a.Shape) - 1; i >= 0; i-- {
			strides[i] = s
			s *= a.Shape[i]
		}
	}
	var rec func(dim, base int) (*List, error)
	rec = func(dim, base int) (*List, error) {
		out := NewList()
		for j := 0; j < a.Shape[dim]; j++ {
			idx := base + j*strides[dim]
			if dim == len(a.Shape)-1 {
				v, err := a.Item(idx)
				if err != nil {
					return nil, err
				}
				out.Append(v)
			} else {
				sub, err := rec(dim+1, idx)
				if err != nil {
					return nil, err
				}
				out.Append(sub)
			}
		}
		return out, nil
	}
	return rec(0, 0)
}

var npyMagic = []byte("\x93NUMPY")

// Npy_save is np.save(buf, arr, allow_pickle=False): format 1.0 (2.0 when
// the header does not fit 16 bits), header padded to a 64-byte boundary
// exactly like numpy.lib.format._write_array_header.
func Npy_save(a *NDArray) ([]byte, error) {
	if a == nil {
		return nil, TypeError.New("array is None")
	}
	shape := make([]string, len(a.Shape))
	for i, d := range a.Shape {
		shape[i] = strconv.Itoa(d)
	}
	shapeStr := "(" + strings.Join(shape, ", ")
	if len(a.Shape) == 1 {
		shapeStr += ","
	}
	shapeStr += ")"
	fortran := "False"
	if a.Fortran_order {
		fortran = "True"
	}
	descr := "'" + a.Dtype + "'"
	if Is_structured_dtype(a.Dtype) {
		descr = a.Dtype
	}
	header := fmt.Sprintf("{'descr': %s, 'fortran_order': %s, 'shape': %s, }", descr, fortran, shapeStr)
	hlen := len(header) + 1
	var major, minor byte = 1, 0
	hlenSize := 2
	if len(npyMagic)+2+hlenSize+hlen > 0xffff {
		major, hlenSize = 2, 4
	}
	prefix := len(npyMagic) + 2 + hlenSize
	pad := 64 - (prefix+hlen)%64
	if pad == 64 {
		pad = 0
	}
	header += strings.Repeat(" ", pad) + "\n"
	out := make([]byte, 0, prefix+len(header)+len(a.Data))
	out = append(out, npyMagic...)
	out = append(out, major, minor)
	if hlenSize == 2 {
		out = binary.LittleEndian.AppendUint16(out, uint16(len(header)))
	} else {
		out = binary.LittleEndian.AppendUint32(out, uint32(len(header)))
	}
	out = append(out, header...)
	out = append(out, a.Data...)
	return out, nil
}

// Npy_load is np.load(buf, allow_pickle=False).
func Npy_load(b []byte) (*NDArray, error) {
	if len(b) < 10 || !bytes.Equal(b[:6], npyMagic) {
		return nil, ValueError.New("Cannot load file containing pickled data when allow_pickle=False")
	}
	major := b[6]
	pos := 8
	var hlen int
	switch major {
	case 1:
		hlen = int(binary.LittleEndian.Uint16(b[8:10]))
		pos = 10
	case 2, 3:
		if len(b) < 12 {
			return nil, ValueError.New("npy header truncated")
		}
		hlen = int(binary.LittleEndian.Uint32(b[8:12]))
		pos = 12
	default:
		return nil, ValueError.Newf("we only support format version (1,0), (2,0), and (3,0), not (%d,%d)", b[6], b[7])
	}
	if pos+hlen > len(b) {
		return nil, ValueError.New("npy header truncated")
	}
	header := string(b[pos : pos+hlen])
	pos += hlen
	descr, fortran, shape, err := parseNpyHeader(header)
	if err != nil {
		return nil, err
	}
	if descr[0] == '|' && dtypeItemsize(descr) > 1 && !strings.HasPrefix(descr, "|S") && !strings.HasPrefix(descr, "|V") {
		return nil, ValueError.Newf("unsupported dtype %q", descr)
	}
	if strings.Contains(descr, "O") && !Is_structured_dtype(descr) || descr == "|O" {
		return nil, ValueError.New("Object arrays cannot be loaded when allow_pickle=False")
	}
	if dtypeItemsize(descr) == 0 {
		return nil, ValueError.Newf("unsupported dtype %q", descr)
	}
	a := &NDArray{Dtype: descr, Shape: shape, Fortran_order: fortran}
	n := a.Size() * dtypeItemsize(descr)
	if pos+n > len(b) {
		return nil, ValueError.New("npy data truncated")
	}
	a.Data = append([]byte(nil), b[pos:pos+n]...)
	return a, nil
}

// parseNpyHeader is numpy.lib.format._read_array_header: the header is a
// Python dict literal evaluated with ast.literal_eval; structured descrs
// are re-rendered in their canonical repr form so they round-trip
// byte-for-byte through Npy_save.
func parseNpyHeader(h string) (descr string, fortran bool, shape []int, err error) {
	bad := func() error { return ValueError.Newf("Cannot parse header: %q", strings.TrimSpace(h)) }
	v, perr := Literal_eval(strings.TrimSpace(h))
	if perr != nil {
		return "", false, nil, bad()
	}
	d, ok := v.(*Dict)
	if !ok {
		return "", false, nil, bad()
	}
	switch x := d.GetDefault("descr", nil).(type) {
	case string:
		descr = x
	case *List, *Dict:
		descr = Repr(x)
	default:
		return "", false, nil, bad()
	}
	fo, ok := d.GetDefault("fortran_order", nil).(bool)
	if !ok {
		return "", false, nil, bad()
	}
	fortran = fo
	rawShape, ok := d.GetDefault("shape", nil).(Tuple)
	if !ok {
		return "", false, nil, bad()
	}
	shape = []int{}
	for _, dim := range rawShape {
		n, ok := dim.(int)
		if !ok {
			return "", false, nil, bad()
		}
		shape = append(shape, n)
	}
	return descr, fortran, shape, nil
}
