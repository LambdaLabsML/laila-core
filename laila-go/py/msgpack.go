package py

import (
	"bytes"
	"math/big"
	"reflect"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// msgpack.packb / msgpack.unpackb over the port's value model.
//
// Packing follows ``msgpack.packb(obj, default=..., use_bin_type=True)``:
// str -> str type, bytes -> bin type, *Dict / Kw / map -> map (insertion
// order kept), *List / slices -> array, ints -> int, floats -> float64,
// bool, None. Anything else goes through *default* (like json.dumps's
// hook); without one a TypeError is raised. Unpacking follows
// ``msgpack.unpackb(data, raw=False)``: maps -> *Dict (wire order kept),
// arrays -> *List, ints -> int64 (uint64 beyond int64 -> *big.Int), str
// -> string, bin -> []byte.

// MsgpackOpts are the ``msgpack.packb`` keyword arguments the port uses:
// ``strict_types`` (tuples are rejected instead of packed as arrays),
// ``use_single_float`` (floats packed as float32) and ``default`` (the
// fallback hook for unknown objects).
type MsgpackOpts struct {
	Strict_types     bool
	Use_single_float bool
	Default          func(any) (any, error)
}

// Msgpack_packb is msgpack.packb(obj, use_bin_type=True, **opts).
func Msgpack_packb(v any, opts ...MsgpackOpts) ([]byte, error) {
	var o MsgpackOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.UseCompactInts(true)
	if err := msgpackEncode(enc, v, o, 0); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func msgpackEncode(enc *msgpack.Encoder, v any, o MsgpackOpts, depth int) error {
	if depth > 512 {
		return ValueError.New("msgpack: recursion limit exceeded")
	}
	def := o.Default
	switch x := v.(type) {
	case nil:
		return enc.EncodeNil()
	case bool:
		return enc.EncodeBool(x)
	case string:
		return enc.EncodeString(x)
	case []byte:
		return enc.EncodeBytes(x)
	case int:
		return enc.EncodeInt(int64(x))
	case int8:
		return enc.EncodeInt(int64(x))
	case int16:
		return enc.EncodeInt(int64(x))
	case int32:
		return enc.EncodeInt(int64(x))
	case int64:
		return enc.EncodeInt(x)
	case uint:
		return enc.EncodeUint(uint64(x))
	case uint8:
		return enc.EncodeUint(uint64(x))
	case uint16:
		return enc.EncodeUint(uint64(x))
	case uint32:
		return enc.EncodeUint(uint64(x))
	case uint64:
		return enc.EncodeUint(x)
	case *big.Int:
		if x.IsInt64() {
			return enc.EncodeInt(x.Int64())
		}
		if x.IsUint64() {
			return enc.EncodeUint(x.Uint64())
		}
		return OverflowError.New("Integer value out of range")
	case float32:
		return enc.EncodeFloat32(x)
	case float64:
		if o.Use_single_float {
			return enc.EncodeFloat32(float32(x))
		}
		return enc.EncodeFloat64(x)
	case *Dict:
		return msgpackEncodeItems(enc, x.Items(), o, depth)
	case map[string]any:
		return msgpackEncodeItems(enc, DictFromMap(x).Items(), o, depth)
	case Kw:
		return msgpackEncodeItems(enc, DictFromKw(x).Items(), o, depth)
	case *List:
		return msgpackEncodeList(enc, x.Items(), o, depth)
	case []any:
		return msgpackEncodeList(enc, x, o, depth)
	case Tuple:
		if o.Strict_types {
			break
		}
		return msgpackEncodeList(enc, []any(x), o, depth)
	case []string:
		return msgpackEncodeList(enc, Iter(x), o, depth)
	case *Set, FrozenSet:
		if def == nil {
			return TypeError.Newf("can not serialize '%s' object", Pytypename(v))
		}
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr:
		if rv.IsNil() {
			return enc.EncodeNil()
		}
		if _, isObj := v.(interface{ Class_() *Class }); !isObj {
			return msgpackEncode(enc, rv.Elem().Interface(), o, depth)
		}
	case reflect.String:
		// Named string kinds (``class X(str, Enum)``) serialize as their
		// value, exactly like a str subclass does in Python.
		return enc.EncodeString(rv.String())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return enc.EncodeInt(rv.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return enc.EncodeUint(rv.Uint())
	case reflect.Float32, reflect.Float64:
		return enc.EncodeFloat64(rv.Float())
	case reflect.Bool:
		return enc.EncodeBool(rv.Bool())
	case reflect.Slice, reflect.Array:
		if _, isTuple := v.(Tuple); !isTuple {
			return msgpackEncodeList(enc, Iter(v), o, depth)
		}
	case reflect.Map:
		if rv.Type().Key().Kind() == reflect.String {
			m := map[string]any{}
			for _, k := range rv.MapKeys() {
				m[k.String()] = rv.MapIndex(k).Interface()
			}
			return msgpackEncodeItems(enc, DictFromMap(m).Items(), o, depth)
		}
	}
	if def != nil {
		alt, err := def(v)
		if err != nil {
			return err
		}
		return msgpackEncode(enc, alt, o, depth+1)
	}
	return TypeError.Newf("can not serialize '%s' object", Pytypename(v))
}

func msgpackEncodeItems(enc *msgpack.Encoder, items []Item, o MsgpackOpts, depth int) error {
	if err := enc.EncodeMapLen(len(items)); err != nil {
		return err
	}
	for _, it := range items {
		if err := msgpackEncode(enc, it.Key, o, depth+1); err != nil {
			return err
		}
		if err := msgpackEncode(enc, it.Value, o, depth+1); err != nil {
			return err
		}
	}
	return nil
}

func msgpackEncodeList(enc *msgpack.Encoder, items []any, o MsgpackOpts, depth int) error {
	if err := enc.EncodeArrayLen(len(items)); err != nil {
		return err
	}
	for _, it := range items {
		if err := msgpackEncode(enc, it, o, depth+1); err != nil {
			return err
		}
	}
	return nil
}

// Msgpack_unpackb is msgpack.unpackb(data, raw=False).
func Msgpack_unpackb(data []byte) (any, error) {
	dec := msgpack.NewDecoder(bytes.NewReader(data))
	v, err := msgpackDecode(dec)
	if err != nil {
		return nil, ValueError.New(err.Error()).From(err)
	}
	if _, err := dec.PeekCode(); err == nil {
		return nil, ValueError.New("unpack(b) received extra data.")
	}
	return v, nil
}

func msgpackDecode(dec *msgpack.Decoder) (any, error) {
	code, err := dec.PeekCode()
	if err != nil {
		return nil, err
	}
	switch {
	case code == msgpcode.Nil:
		return nil, dec.DecodeNil()
	case code == msgpcode.True || code == msgpcode.False:
		return dec.DecodeBool()
	case msgpcode.IsFixedNum(code), code == msgpcode.Int8, code == msgpcode.Int16, code == msgpcode.Int32, code == msgpcode.Int64:
		return dec.DecodeInt64()
	case code == msgpcode.Uint8, code == msgpcode.Uint16, code == msgpcode.Uint32:
		n, err := dec.DecodeUint64()
		return int64(n), err
	case code == msgpcode.Uint64:
		n, err := dec.DecodeUint64()
		if err != nil {
			return nil, err
		}
		if n <= uint64(1<<63-1) {
			return int64(n), nil
		}
		return new(big.Int).SetUint64(n), nil
	case code == msgpcode.Float:
		f, err := dec.DecodeFloat32()
		return float64(f), err
	case code == msgpcode.Double:
		return dec.DecodeFloat64()
	case msgpcode.IsString(code):
		return dec.DecodeString()
	case msgpcode.IsBin(code):
		return dec.DecodeBytes()
	case msgpcode.IsFixedArray(code), code == msgpcode.Array16, code == msgpcode.Array32:
		n, err := dec.DecodeArrayLen()
		if err != nil {
			return nil, err
		}
		items := make([]any, 0, max(n, 0))
		for i := 0; i < n; i++ {
			it, err := msgpackDecode(dec)
			if err != nil {
				return nil, err
			}
			items = append(items, it)
		}
		return NewList(items...), nil
	case msgpcode.IsFixedMap(code), code == msgpcode.Map16, code == msgpcode.Map32:
		n, err := dec.DecodeMapLen()
		if err != nil {
			return nil, err
		}
		d := NewDict()
		for i := 0; i < n; i++ {
			k, err := msgpackDecode(dec)
			if err != nil {
				return nil, err
			}
			v, err := msgpackDecode(dec)
			if err != nil {
				return nil, err
			}
			if _, ok := k.(*List); ok {
				return nil, TypeError.New("unhashable type: 'list'")
			}
			d.Set(k, v)
		}
		return d, nil
	case msgpcode.IsExt(code):
		id, data, err := dec.DecodeExtHeader()
		if err != nil {
			return nil, err
		}
		raw := make([]byte, data)
		if _, err := dec.Buffered().Read(raw); err != nil {
			return nil, err
		}
		return Tuple{int(id), raw}, nil
	}
	return nil, ValueError.Newf("msgpack: unknown code %#x", code)
}
