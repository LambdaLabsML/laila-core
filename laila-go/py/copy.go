package py

import (
	"math/big"
	"reflect"
	"sync"
	"unsafe"
)

// Memo is the copy.deepcopy memo dictionary (identity -> copy).
type Memo struct{ m map[uintptr]any }

// Deepcopier is implemented by objects with a custom __deepcopy__.
type Deepcopier interface{ Deepcopy(memo *Memo) any }

// Copier is implemented by objects with a custom __copy__.
type Copier interface{ Copy_() any }

// Copy is copy.copy(v): a shallow copy with Python semantics.
func Copy(v any) any {
	switch x := v.(type) {
	case Copier:
		return x.Copy_()
	case *Dict:
		return x.Copy()
	case *List:
		return x.Copy()
	case *Set:
		return x.Copy()
	case []any:
		return append([]any(nil), x...)
	case []byte:
		return x // bytes are immutable in Python; copy.copy returns the same object
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[k] = val
		}
		return out
	case Kw:
		return x.Copy()
	}
	return v
}

// Deepcopy is copy.deepcopy(v) over the value model. Scalars, strings and
// bytes are immutable and shared; containers are copied recursively with a
// memo so shared/cyclic references are preserved. Objects implementing
// Deepcopier control their own copy; other pointer-to-struct values are
// copied field by field (locks are reset to their zero value, matching
// _LAILA_LOCALLY_ATOMIC_OBJECT.__deepcopy__ which skips every lock).
func Deepcopy(v any) any {
	return deepcopy(v, &Memo{m: map[uintptr]any{}})
}

// DeepcopyMemo is deepcopy with an explicit memo (for __deepcopy__ overrides).
func DeepcopyMemo(v any, memo *Memo) any {
	if memo == nil {
		memo = &Memo{m: map[uintptr]any{}}
	}
	return deepcopy(v, memo)
}

var lockTypes = map[reflect.Type]bool{
	reflect.TypeOf(sync.Mutex{}):   true,
	reflect.TypeOf(sync.RWMutex{}): true,
	reflect.TypeOf(sync.Once{}):    true,
}

func deepcopy(v any, memo *Memo) any {
	switch x := v.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64,
		float32, float64, string, *big.Int:
		return v
	case []byte:
		return v
	case Tuple:
		out := make(Tuple, len(x))
		for i, e := range x {
			out[i] = deepcopy(e, memo)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = deepcopy(e, memo)
		}
		return out
	case []string:
		return append([]string(nil), x...)
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = deepcopy(e, memo)
		}
		return out
	case Kw:
		out := make(Kw, len(x))
		for k, e := range x {
			out[k] = deepcopy(e, memo)
		}
		return out
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return v
		}
		if c, ok := memo.m[rv.Pointer()]; ok {
			return c
		}
	}
	switch x := v.(type) {
	case Deepcopier:
		c := x.Deepcopy(memo)
		if rv.Kind() == reflect.Ptr {
			memo.m[rv.Pointer()] = c
		}
		return c
	case *Dict:
		out := NewDict()
		memo.m[rv.Pointer()] = out
		for _, it := range x.Items() {
			out.Set(deepcopy(it.Key, memo), deepcopy(it.Value, memo))
		}
		return out
	case *List:
		out := NewList()
		memo.m[rv.Pointer()] = out
		for _, e := range x.Items() {
			out.Append(deepcopy(e, memo))
		}
		return out
	case *Set:
		out := NewSet()
		memo.m[rv.Pointer()] = out
		for _, e := range x.Items() {
			out.Add(deepcopy(e, memo))
		}
		return out
	case *Error:
		return v
	case *Lock:
		// locks are process-local primitives: a copy never shares one
		// (_LAILA_LOCALLY_ATOMIC_OBJECT.__deepcopy__ -> _LOCK_FACTORIES)
		return NewLock()
	case *RLock:
		return NewRLock()
	}
	switch rv.Kind() {
	case reflect.Ptr:
		if rv.Elem().Kind() != reflect.Struct {
			return v
		}
		out := reflect.New(rv.Elem().Type())
		memo.m[rv.Pointer()] = out.Interface()
		copyStruct(out.Elem(), rv.Elem(), memo)
		if o, ok := out.Interface().(interface{ Rewire_self_(self any) }); ok {
			o.Rewire_self_(out.Interface())
		}
		return out.Interface()
	case reflect.Struct:
		out := reflect.New(rv.Type()).Elem()
		copyStruct(out, rv, memo)
		return out.Interface()
	case reflect.Slice:
		out := reflect.MakeSlice(rv.Type(), rv.Len(), rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out.Index(i).Set(reflect.ValueOf(deepcopy(rv.Index(i).Interface(), memo)))
		}
		return out.Interface()
	case reflect.Map:
		out := reflect.MakeMapWithSize(rv.Type(), rv.Len())
		for _, k := range rv.MapKeys() {
			out.SetMapIndex(k, reflect.ValueOf(deepcopy(rv.MapIndex(k).Interface(), memo)))
		}
		return out.Interface()
	}
	return v
}

func copyStruct(dst, src reflect.Value, memo *Memo) {
	t := src.Type()
	for i := 0; i < src.NumField(); i++ {
		f := t.Field(i)
		sf, df := src.Field(i), dst.Field(i)
		if lockTypes[f.Type] || f.Type.PkgPath() == "sync/atomic" {
			continue // locks and atomics are never copied (the copy gets zero values)
		}
		if f.Type == objectType {
			// the class header: keep class and fields_set, drop the stale
			// self pointer (Rewire_self_ installs the copy's own)
			dst.Field(i).Addr().Interface().(*Object).copyHeaderFrom(sf.Addr().Interface().(*Object))
			continue
		}
		if !f.IsExported() {
			// unexported fields are implementation details of the port;
			// Python copies every attribute, so copy them shallowly
			// (deep semantics are reserved for exported, tagged fields).
			if sf.CanAddr() && df.CanAddr() {
				src := reflect.NewAt(sf.Type(), unsafe.Pointer(sf.UnsafeAddr())).Elem()
				reflect.NewAt(df.Type(), unsafe.Pointer(df.UnsafeAddr())).Elem().Set(src)
			}
			continue
		}
		if f.Anonymous && sf.Kind() == reflect.Struct {
			copyStruct(df, sf, memo)
			continue
		}
		if !sf.IsValid() || sf.IsZero() {
			continue
		}
		c := deepcopy(sf.Interface(), memo)
		if c == nil {
			continue
		}
		cv := reflect.ValueOf(c)
		if cv.Type().AssignableTo(df.Type()) {
			df.Set(cv)
		} else if cv.Type().ConvertibleTo(df.Type()) {
			df.Set(cv.Convert(df.Type()))
		}
	}
}
