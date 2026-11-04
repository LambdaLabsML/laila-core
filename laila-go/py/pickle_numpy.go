package py

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

// numpy arrays inside pickled containers.
//
// A top-level ndarray goes through the numpy transformation, but a dict
// or list holding one reaches pickle, where CPython relies on numpy's
// ``ndarray.__reduce_ex__``. The Go pickler writes the same stream numpy
// 2.x writes and the unpickler rebuilds both numpy forms:
//
//   - protocol 5, contiguous: ``numpy._core.numeric._frombuffer(
//     PickleBuffer(a), dtype, shape, order)`` -- the buffer is in-band as a
//     ``bytearray`` (BYTEARRAY8);
//   - otherwise: ``numpy._core.multiarray._reconstruct(ndarray, (0,), b'b')``
//     followed by BUILD with ``(1, shape, dtype, is_fortran, rawdata)``.
//
// ``numpy.dtype`` itself is ``numpy.dtype(<kind><size>, False, True)``
// plus BUILD with ``(3, byteorder, subarray, names, fields, elsize,
// alignment, flags)``. The numpy 1.x module paths (``numpy.core.*``) are
// accepted on input.

// np_dtype_ is a ``numpy.dtype`` under construction (REDUCE then BUILD).
type np_dtype_ struct {
	name  string // "i8", "f4", "b1", "S5", "V12", ...
	descr string // array-protocol descr once the state is applied
}

func (d *np_dtype_) Pytype() string { return "numpy.dtype" }

// Setstate is ``dtype.__setstate__``: version 3/4 state tuples.
func (d *np_dtype_) Setstate(state any) error {
	st, ok := state.(Tuple)
	if !ok || len(st) < 8 {
		return UnpicklingError.New("invalid numpy.dtype state")
	}
	byteorder := Str(st[1])
	if byteorder == "=" {
		byteorder = "<"
	}
	names, fields := st[3], st[4]
	if !IsNone(names) && !IsNone(fields) {
		return d.set_structured_(byteorder, names, fields, st[5])
	}
	name := d.name
	if len(name) == 1 && (name[0] == 'S' || name[0] == 'U' || name[0] == 'V') {
		// unsized flexible type: the element size is in the state
		if n, err := Int(st[5]); err == nil && n > 0 {
			size := n
			if name[0] == 'U' {
				size = n / 4
			}
			name = name + strconv.Itoa(size)
		}
	}
	if (name[0] == 'M' || name[0] == 'm') && len(st) >= 9 {
		// version 4: datetime64 / timedelta64 unit metadata
		// ``(None, (b'D', num, 1, 1))``
		if meta, ok := st[8].(Tuple); ok && len(meta) == 2 {
			if unit, ok := meta[1].(Tuple); ok && len(unit) >= 2 {
				u := Str(unit[0])
				if b, ok := unit[0].([]byte); ok {
					u = string(b)
				}
				if num, err := Int(unit[1]); err == nil && num > 1 {
					u = strconv.Itoa(num) + u
				}
				if u != "generic" {
					name += "[" + u + "]"
				}
			}
		}
	}
	if dtypeItemsize("<"+name) == 1 || name[0] == 'S' || name[0] == 'V' {
		byteorder = "|"
	}
	d.descr = byteorder + name
	return nil
}

// set_structured_ rebuilds the list / dict descr the port uses for record
// dtypes from numpy's ``names`` tuple and ``fields`` dict.
func (d *np_dtype_) set_structured_(byteorder string, names any, fields any, elsize any) error {
	fd, ok := fields.(*Dict)
	if !ok {
		return UnpicklingError.New("invalid numpy.dtype fields")
	}
	type field struct {
		name   string
		descr  string
		offset int
	}
	var fs []field
	for _, n := range Iter(names) {
		spec, ok := fd.Get(n)
		if !ok {
			return UnpicklingError.Newf("numpy.dtype field %q missing", Str(n))
		}
		t, ok := spec.(Tuple)
		if !ok || len(t) < 2 {
			return UnpicklingError.New("invalid numpy.dtype field spec")
		}
		sub, ok := t[0].(*np_dtype_)
		if !ok {
			return UnpicklingError.New("invalid numpy.dtype field type")
		}
		off, _ := Int(t[1])
		fs = append(fs, field{Str(n), sub.descr, off})
	}
	packed, total := true, 0
	for _, f := range fs {
		if f.offset != total {
			packed = false
		}
		total += dtypeItemsize(f.descr)
	}
	itemsize, _ := Int(elsize)
	if packed && (itemsize <= 0 || itemsize == total) {
		parts := make([]string, len(fs))
		for i, f := range fs {
			parts[i] = fmt.Sprintf("(%s, %s)", Repr(f.name), Repr(f.descr))
		}
		d.descr = "[" + strings.Join(parts, ", ") + "]"
		return nil
	}
	names_, formats, offsets := make([]string, len(fs)), make([]string, len(fs)), make([]string, len(fs))
	for i, f := range fs {
		names_[i], formats[i], offsets[i] = Repr(f.name), Repr(f.descr), strconv.Itoa(f.offset)
	}
	d.descr = fmt.Sprintf("{'names': [%s], 'formats': [%s], 'offsets': [%s], 'itemsize': %d}",
		strings.Join(names_, ", "), strings.Join(formats, ", "), strings.Join(offsets, ", "), itemsize)
	return nil
}

// np_reconstruct_ is the ``_reconstruct(ndarray, (0,), b'b')`` placeholder
// that BUILD turns into the real array.
type np_reconstruct_ struct{ arr *NDArray }

func (r *np_reconstruct_) Pytype() string { return "numpy.ndarray" }

// Setstate is ``ndarray.__setstate__((version, shape, dtype, is_fortran,
// rawdata))``.
func (r *np_reconstruct_) Setstate(state any) error {
	st, ok := state.(Tuple)
	if !ok || len(st) < 5 {
		return UnpicklingError.New("invalid numpy.ndarray state")
	}
	shape, err := np_shape_(st[1])
	if err != nil {
		return err
	}
	dt, ok := st[2].(*np_dtype_)
	if !ok {
		return UnpicklingError.New("invalid numpy.ndarray dtype")
	}
	data, ok := st[4].([]byte)
	if !ok {
		return UnpicklingError.Newf("numpy.ndarray raw data of %s is not supported by the Go port", Pytypename(st[4]))
	}
	r.arr.Dtype = dt.descr
	r.arr.Shape = shape
	r.arr.Data = append([]byte(nil), data...)
	r.arr.Fortran_order = Truthy(st[3])
	return nil
}

func np_shape_(v any) ([]int, error) {
	shape := []int{}
	for _, d := range Iter(v) {
		n, err := Int(d)
		if err != nil {
			return nil, UnpicklingError.New("invalid numpy.ndarray shape")
		}
		shape = append(shape, n)
	}
	return shape, nil
}

func np_frombuffer_(args Tuple) (any, error) {
	if len(args) < 4 {
		return nil, UnpicklingError.New("_frombuffer expects (buffer, dtype, shape, order)")
	}
	data, ok := args[0].([]byte)
	if !ok {
		return nil, UnpicklingError.New("_frombuffer buffer is not bytes")
	}
	dt, ok := args[1].(*np_dtype_)
	if !ok {
		return nil, UnpicklingError.New("_frombuffer dtype is not a numpy.dtype")
	}
	shape, err := np_shape_(args[2])
	if err != nil {
		return nil, err
	}
	return &NDArray{Dtype: dt.descr, Shape: shape, Data: append([]byte(nil), data...), Fortran_order: Str(args[3]) == "F"}, nil
}

func np_reconstruct_ctor_(args Tuple) (any, error) {
	return &np_reconstruct_{arr: &NDArray{}}, nil
}

func np_dtype_ctor_(args Tuple) (any, error) {
	if len(args) == 0 {
		return nil, UnpicklingError.New("numpy.dtype expects a type name")
	}
	if sub, ok := args[0].(*np_dtype_); ok { // dtype(dtype(...)) -- already built
		return sub, nil
	}
	return &np_dtype_{name: Str(args[0])}, nil
}

func init() {
	Global_constructor["numpy._core.numeric._frombuffer"] = np_frombuffer_
	Global_constructor["numpy.core.numeric._frombuffer"] = np_frombuffer_
	Global_constructor["numpy._core.multiarray._reconstruct"] = np_reconstruct_ctor_
	Global_constructor["numpy.core.multiarray._reconstruct"] = np_reconstruct_ctor_
	Global_constructor["numpy.dtype"] = np_dtype_ctor_
}

// unwrap_np_ replaces a BUILD-completed ``_reconstruct`` placeholder by its
// array.
func unwrap_np_(v any) any {
	if r, ok := v.(*np_reconstruct_); ok && r.arr.Dtype != "" {
		return r.arr
	}
	return v
}

// memoIndexOf finds the memo slot holding a ``_reconstruct`` placeholder
// (memoized right after REDUCE, before BUILD) so later BINGETs see the
// array; -1 for anything else.
func (u *unpickler) memoIndexOf(v any) int {
	if _, ok := v.(*np_reconstruct_); !ok {
		return -1
	}
	for idx, m := range u.memo {
		if m == v {
			return idx
		}
	}
	return -1
}

// ---------------------------------------------------------------------------
// pickler
// ---------------------------------------------------------------------------

// np_dtype_name_ splits an array-protocol descr into numpy's pickle name
// (``<kind><size>``) and byteorder; false for dtypes the port does not
// pickle (structured, datetime, object).
func np_dtype_name_(descr string) (name, byteorder string, ok bool) {
	if len(descr) < 2 || Is_structured_dtype(descr) {
		return "", "", false
	}
	bo, rest := descr[:1], descr[1:]
	switch bo {
	case "<", ">", "|":
	case "=":
		bo = "<"
	default:
		return "", "", false
	}
	switch rest[0] {
	case 'b', 'i', 'u', 'f', 'c', 'S', 'U', 'V':
		if _, err := strconv.Atoi(rest[1:]); err != nil {
			return "", "", false
		}
	case 'M', 'm':
		if _, err := strconv.Atoi(strings.SplitN(rest[1:], "[", 2)[0]); err != nil {
			return "", "", false
		}
	default: // 'O', ...
		return "", "", false
	}
	if dtypeItemsize(descr) == 1 || rest[0] == 'S' || rest[0] == 'V' {
		bo = "|"
	}
	return rest, bo, true
}

// saveDtype writes ``numpy.dtype(name, False, True)`` + BUILD state.
func (p *pickler) saveDtype(descr string) error {
	name, byteorder, ok := np_dtype_name_(descr)
	if !ok {
		return PicklingError.Newf("Can't pickle numpy array of dtype %s in the Go port", descr)
	}
	if err := p.saveGlobal("numpy.dtype"); err != nil {
		return err
	}
	// datetime64[unit] pickles as ``M8`` with the unit in the state
	unit := ""
	if i := strings.IndexByte(name, '['); i >= 0 {
		unit = name[i+1 : len(name)-1]
		name = name[:i]
	}
	p.saveStr(name)
	p.memoize(memoKey{}, false)
	p.write(opNEWFALSE, opNEWTRUE, opTUPLE3)
	p.memoize(memoKey{}, false)
	p.write(opREDUCE)
	p.memoize(memoKey{}, false)
	// (3, byteorder, None, None, None, elsize, alignment, flags)
	// flexible types carry their size; the rest use -1 / -1 / 0
	elsize, alignment, flags := -1, -1, 0
	switch name[0] {
	case 'S':
		elsize, alignment = dtypeItemsize(descr), 1
	case 'V':
		elsize, alignment = dtypeItemsize(descr), 1
	case 'U':
		elsize, alignment, flags = dtypeItemsize(descr), 4, 8
	}
	version := byte(3)
	if unit != "" {
		version = 4
	}
	p.write(opMARK, opBININT1, version)
	p.saveStr(byteorder)
	p.memoize(memoKey{}, false)
	p.write(opNONE, opNONE, opNONE)
	p.saveInt(big.NewInt(int64(elsize)))
	p.saveInt(big.NewInt(int64(alignment)))
	p.saveInt(big.NewInt(int64(flags)))
	if unit != "" {
		// (None, (b'D', num, 1, 1))
		num, u := 1, unit
		for i := 0; i < len(u) && u[i] >= '0' && u[i] <= '9'; i++ {
			num, _ = strconv.Atoi(u[:i+1])
			unit = u[i+1:]
		}
		p.write(opNONE, opMARK)
		p.saveBytes([]byte(unit))
		p.memoize(memoKey{}, false)
		p.saveInt(big.NewInt(int64(num)))
		p.write(opBININT1, 1, opBININT1, 1)
		p.write(opTUPLE)
		p.memoize(memoKey{}, false)
		p.write(opTUPLE2)
		p.memoize(memoKey{}, false)
	}
	p.write(opTUPLE)
	p.memoize(memoKey{}, false)
	p.write(opBUILD)
	return nil
}

// saveBytearray is the in-band form of a writable PickleBuffer.
func (p *pickler) saveBytearray(b []byte) {
	header := append([]byte{opBYTEARRAY8}, binary.LittleEndian.AppendUint64(nil, uint64(len(b)))...)
	if len(b) >= pickleFrameSizeTarget {
		p.writeLarge(header, b)
	} else {
		p.write(header...)
		p.writeBytes(b)
	}
	p.memoize(memoKey{}, false)
}

func (p *pickler) saveShape(shape []int) error {
	t := make(Tuple, len(shape))
	for i, d := range shape {
		t[i] = d
	}
	return p.saveTuple(t, memoKey{}, false)
}

// saveNDArray is ``ndarray.__reduce_ex__(protocol)`` as numpy 2.x writes
// it; *key* is the array's memo identity.
func (p *pickler) saveNDArray(a *NDArray, key memoKey, hasID bool) error {
	if _, _, ok := np_dtype_name_(a.Dtype); !ok {
		return PicklingError.Newf("Can't pickle numpy array of dtype %s in the Go port", a.Dtype)
	}
	order := "C"
	if a.Fortran_order {
		order = "F"
	}
	// numpy only hands out a PickleBuffer for plain (non-datetime) dtypes
	kind := a.Dtype[1]
	if p.proto >= 5 && kind != 'M' && kind != 'm' {
		if err := p.saveGlobal("numpy._core.numeric._frombuffer"); err != nil {
			return err
		}
		p.write(opMARK)
		p.saveBytearray(a.Data)
		if err := p.saveDtype(a.Dtype); err != nil {
			return err
		}
		if err := p.saveShape(a.Shape); err != nil {
			return err
		}
		p.saveStr(order)
		p.memoize(memoKey{}, false)
		p.write(opTUPLE)
		p.memoize(memoKey{}, false)
		p.write(opREDUCE)
		p.memoize(key, hasID)
		return nil
	}
	// _reconstruct(ndarray, (0,), b'b') then BUILD (1, shape, dtype, is_fortran, data)
	if err := p.saveGlobal("numpy._core.multiarray._reconstruct"); err != nil {
		return err
	}
	if err := p.saveGlobal("numpy.ndarray"); err != nil {
		return err
	}
	p.write(opBININT1, 0, opTUPLE1)
	p.memoize(memoKey{}, false)
	p.saveBytes([]byte("b"))
	p.memoize(memoKey{}, false)
	p.write(opTUPLE3)
	p.memoize(memoKey{}, false)
	p.write(opREDUCE)
	p.memoize(key, hasID)
	p.write(opMARK, opBININT1, 1)
	if err := p.saveShape(a.Shape); err != nil {
		return err
	}
	if err := p.saveDtype(a.Dtype); err != nil {
		return err
	}
	if a.Fortran_order {
		p.write(opNEWTRUE)
	} else {
		p.write(opNEWFALSE)
	}
	p.saveBytes(a.Data)
	p.memoize(memoKey{}, false)
	p.write(opTUPLE)
	p.memoize(memoKey{}, false)
	p.write(opBUILD)
	return nil
}
