package py

import (
	"reflect"
	"strings"
)

// Pickling of class-bound objects (pydantic models and their laila
// subclasses).
//
// CPython pickles a ``BaseModel`` through ``object.__reduce_ex__(2+)``:
//
//	copyreg.__newobj__(cls)            -> GLOBAL cls; EMPTY_TUPLE; NEWOBJ
//	state = obj.__getstate__()         -> {'__dict__': ..., '__pydantic_extra__': ...,
//	                                       '__pydantic_fields_set__': ..., '__pydantic_private__': ...}
//	obj.__setstate__(state)            -> BUILD
//
// The Go port emits exactly that layout so blobs cross the wire in both
// directions; the class global is ``<python module>.<Name>`` where the
// module is derived from the flat-package file that declared the class
// (``atomic.types.atomic_dict.go`` -> ``laila.atomic.types.atomic_dict``).
// Locks never travel: every lock-typed slot is a ``py:"-"`` field, so the
// state carries none and the unpickled instance recreates them lazily --
// the same effect ``_LAILA_LOCALLY_ATOMIC_OBJECT.__getstate__`` /
// ``__setstate__`` achieve with ``_LOCK_FACTORIES``.

// Getstater overrides the pickled state (``__getstate__``).
type Getstater interface{ Getstate() (*Dict, error) }

// Setstater overrides state restoration (``__setstate__``).
type Setstater interface{ Setstate(state any) error }

// Pickle_global is the ``module.qualname`` a class is pickled under.
func Pickle_global(cls *Class) string {
	if cls.Module == "" {
		return "__main__." + cls.Name
	}
	return cls.Module + "." + cls.Name
}

// Object_getstate is ``BaseModel.__getstate__``: the model's ``__dict__``
// (every non-private field, declaration order), ``__pydantic_extra__``,
// ``__pydantic_fields_set__`` and ``__pydantic_private__``.
func Object_getstate(self any) (*Dict, error) {
	if g, ok := self.(Getstater); ok {
		return g.Getstate()
	}
	cls := Classof(self)
	if cls == nil {
		return nil, PicklingError.Newf("Can't pickle <class '%s'> object", Pytypename(self))
	}
	rv := reflect.ValueOf(self).Elem()
	dict := NewDict()
	private := NewDict()
	dict_locks := NewDict()
	private_locks := NewDict()
	for _, f := range cls.Fields() {
		if f.ClassVar {
			continue
		}
		fv := rv.FieldByIndex(f.Index)
		var v any
		if fv.CanInterface() {
			v = fv.Interface()
		}
		v = nil_slice_is_none_(deref_optional_(v))
		switch {
		case isLockValue(v) && f.Private:
			private_locks.Set(f.Name, lockKind_(v))
		case isLockValue(v):
			dict_locks.Set(f.Name, lockKind_(v))
		case f.Private:
			private.Set(f.Name, v)
		default:
			dict.Set(f.Name, v)
		}
	}
	state := NewDict()
	state.Set("__dict__", dict)
	var extra any
	var fields_set any = NewSet()
	if o, ok := self.(interface{ Object_() *Object }); ok {
		obj := o.Object_()
		if obj.extra != nil {
			extra = obj.extra
		}
		fs := NewSet()
		for _, n := range obj.Fields_set() {
			fs.Add(n)
		}
		fields_set = fs
		obj.privMu.RLock()
		names := make([]string, 0, len(obj.private))
		for n := range obj.private {
			names = append(names, n)
		}
		obj.privMu.RUnlock()
		sortStrings(names)
		for _, n := range names {
			v, _ := obj.Private_get(n)
			if isLockValue(v) {
				private_locks.Set(n, lockKind_(v))
				continue
			}
			private.Set(n, v)
		}
	}
	state.Set("__pydantic_extra__", extra)
	state.Set("__pydantic_fields_set__", fields_set)
	if private.Len() == 0 && private_locks.Len() == 0 {
		state.Set("__pydantic_private__", nil)
	} else {
		state.Set("__pydantic_private__", private)
	}
	state.Set("__laila_dict_locks__", dict_locks)
	if private.Len() != 0 || private_locks.Len() != 0 {
		state.Set("__laila_private_locks__", private_locks)
	}
	return state, nil
}

// lockKind_ is the ``_LOCK_FACTORIES`` key of a lock value.
func lockKind_(v any) string {
	if _, ok := v.(*RLock); ok {
		return "rlock"
	}
	return "lock"
}

// newLock_ is ``_LOCK_FACTORIES[name]()``.
func newLock_(kind any) any {
	if Str(kind) == "rlock" {
		return NewRLock()
	}
	return NewLock()
}

// Object_setstate is ``BaseModel.__setstate__``: restore the fields, extra,
// fields-set and private attributes bypassing validation and frozen
// checks (``object.__setattr__``). Keys the Go port has no slot for (the
// Python ``__laila_*_locks__`` lock maps) are ignored.
func Object_setstate(self any, state any) error {
	if s, ok := self.(Setstater); ok {
		return s.Setstate(state)
	}
	cls := Classof(self)
	if cls == nil {
		return UnpicklingError.Newf("BUILD on object without __setstate__: %s", Pytypename(self))
	}
	st, ok := state.(*Dict)
	if !ok {
		return UnpicklingError.Newf("__setstate__ expects a dict state, got %s", Pytypename(state))
	}
	rv := reflect.ValueOf(self).Elem()
	setField := func(f *FieldInfo, v any) {
		fv := rv.FieldByIndex(f.Index)
		if cv, err := Coerce(v, f.Type); err == nil {
			fv.Set(cv)
		} else {
			assignValue(fv, v)
		}
		for _, idx := range f.Shadowed {
			sv := rv.FieldByIndex(idx)
			if cv, err := Coerce(v, sv.Type()); err == nil {
				sv.Set(cv)
			}
		}
	}
	if d, ok := st.GetDefault("__dict__", nil).(*Dict); ok {
		for _, it := range d.Items() {
			name := Str(it.Key)
			if f := cls.Field_by_name(name); f != nil {
				setField(f, it.Value)
			} else if o, ok := self.(interface{ Private_set(string, any) }); ok {
				o.Private_set(name, it.Value)
			}
		}
	}
	obj, hasObj := self.(interface{ Object_() *Object })
	if hasObj {
		o := obj.Object_()
		if extra, ok := st.GetDefault("__pydantic_extra__", nil).(*Dict); ok {
			o.setExtra(extra)
		}
		switch fs := st.GetDefault("__pydantic_fields_set__", nil).(type) {
		case *Set:
			for _, n := range fs.Items() {
				o.Mark_field_set(Str(n))
			}
		case FrozenSet:
			for _, n := range fs.Items() {
				o.Mark_field_set(Str(n))
			}
		}
	}
	if private, ok := st.GetDefault("__pydantic_private__", nil).(*Dict); ok {
		for _, it := range private.Items() {
			name := Str(it.Key)
			if isLockValue(it.Value) {
				continue
			}
			if f := cls.Field_by_name(name); f != nil && f.Private {
				setField(f, it.Value)
			} else if hasObj {
				obj.Object_().Private_set(name, it.Value)
			}
		}
	}
	// ``_LAILA_LOCALLY_ATOMIC_OBJECT.__setstate__``: every lock the state
	// split out is recreated fresh.
	for _, key := range []string{"__laila_dict_locks__", "__laila_private_locks__"} {
		locks, ok := st.GetDefault(key, nil).(*Dict)
		if !ok {
			continue
		}
		for _, it := range locks.Items() {
			name := Str(it.Key)
			lock := newLock_(it.Value)
			if f := cls.Field_by_name(name); f != nil {
				setField(f, lock)
			} else if hasObj {
				obj.Object_().Private_set(name, lock)
			}
		}
	}
	return nil
}

// saveObject writes ``GLOBAL cls; EMPTY_TUPLE; NEWOBJ; state; BUILD``.
func (p *pickler) saveObject(v any, cls *Class, key memoKey, hasID bool) error {
	if err := p.saveGlobal(Pickle_global(cls)); err != nil {
		return err
	}
	p.write(opEMPTY_TUPLE)
	p.write(opNEWOBJ)
	p.memoize(key, hasID)
	state, err := Object_getstate(v)
	if err != nil {
		return err
	}
	if err := p.save(state); err != nil {
		return err
	}
	p.write(opBUILD)
	return nil
}

// class_for_global_ resolves a pickled ``module.Name`` to a registered
// class: an exact module match first, then the bare name (CPython blobs
// carry the Python module path, Go blobs the derived one).
func class_for_global_(global string) (*Class, bool) {
	dot := strings.LastIndexByte(global, '.')
	name := global
	if dot >= 0 {
		name = global[dot+1:]
	}
	c, ok := Class_by_name(name)
	if !ok || c.New == nil {
		return nil, false
	}
	return c, true
}

// new_object_for_global_ is ``copyreg.__newobj__(cls, *args)`` for a
// registered class (NEWOBJ / NEWOBJ_EX).
func new_object_for_global_(global string, args Tuple) (any, bool, error) {
	cls, ok := class_for_global_(global)
	if !ok {
		return nil, false, nil
	}
	if cls.Abstract {
		return nil, true, UnpicklingError.Newf("Can't instantiate abstract class %s", cls.Name)
	}
	v := cls.New()
	return v, true, nil
}

// deref_optional_ unwraps the pointer-spelled optionals of the port
// (*string, *int, *float64, *bool, ...) into the value or None; pointers to
// structs (nested objects, *Dict, *List) are kept as they are.
func deref_optional_(v any) any {
	if v == nil {
		return nil
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr {
		return v
	}
	if rv.IsNil() {
		return nil
	}
	if rv.Elem().Kind() == reflect.Struct {
		return v
	}
	return rv.Elem().Interface()
}

// nil_slice_is_none_ maps the port's ``nil is None`` slices / maps to None.
func nil_slice_is_none_(v any) any {
	if v == nil {
		return nil
	}
	rv := reflect.ValueOf(v)
	if (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Map) && rv.IsNil() {
		return nil
	}
	return v
}

func isLockValue(v any) bool {
	switch v.(type) {
	case *Lock, *RLock:
		return true
	}
	return false
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// ---------------------------------------------------------------------------
// enums
// ---------------------------------------------------------------------------

// enum_pickle_ describes how a Go enum type pickles: ``Enum.__reduce_ex__``
// is ``(cls, (value,))`` -> ``GLOBAL cls; (value,); REDUCE``.
type enum_pickle_ struct {
	global string
	value  func(member any) any
}

var enum_types_ = map[reflect.Type]enum_pickle_{}

// Register_enum declares a Go enum type (by a sample member) as picklable
// under ``global`` (``module.Name``): ``value`` extracts the member's
// ``.value`` and ``ctor`` is ``cls(value)``.
func Register_enum(sample any, global string, value func(member any) any, ctor func(value any) (any, error)) {
	enum_types_[reflect.TypeOf(sample)] = enum_pickle_{global: global, value: value}
	Global_constructor[global] = func(args Tuple) (any, error) {
		if len(args) != 1 {
			return nil, UnpicklingError.Newf("%s() takes exactly one argument", global)
		}
		return ctor(args[0])
	}
}

// enum_pickle_of_ is the pickling of v if it is a registered enum type.
func enum_pickle_of_(v any) (enum_pickle_, bool) {
	if v == nil {
		return enum_pickle_{}, false
	}
	e, ok := enum_types_[reflect.TypeOf(v)]
	return e, ok
}
