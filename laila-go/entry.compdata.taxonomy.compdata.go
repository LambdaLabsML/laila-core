package laila

import (
	"errors"
	"reflect"
	"strings"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// entry/compdata/taxonomy/compdata.py -- base ComputationalData class:
// the type-dispatched payload wrapper.
//
// Every Entry payload is wrapped in a ComputationalData (or one of its
// registered subclasses) so the system has a uniform handle on the data
// regardless of the underlying type. The wrapper provides a ``data``
// attribute exposing the unwrapped value, a swappable ``serializer``
// (PickleSerializer by default) used by pool-side Serialize to produce
// bytes plus an inverse-decode source string, and a stub Len / Shape /
// Copy_ / Deepcopy API that subclasses fill in for their payload type.
//
// Type dispatch: subclasses register themselves with Register_cdtype to
// claim ownership of one or more payload types. New_ComputationalData(v)
// looks up the value's type in TYPE_TO_WRAPPER (then walks the class MRO
// for laila objects) and instantiates the matching subclass; CD_generic is
// the fallback for anything unknown.
//
// Port note: Python's ``__get__`` descriptor behaviour has no Go
// counterpart and is not ported.

// Computational_data is the instance protocol of ComputationalData and
// its subclasses.
type Computational_data interface {
	// Base_ exposes the shared state (``data``, ``_serializer``).
	Base_() *ComputationalData
	Class_() *py.Class
	// Serializer is the ``serializer`` property getter.
	Serializer() Data_transformer
	// Set_serializer is the ``serializer`` property setter (TypeError on
	// an incompatible serializer).
	Set_serializer(value any) error
	// Serialize is ``(serialized_bytes, backward_code)``.
	Serialize() (any, string, error)
	Getitem(index any) (any, error)
	Len() (int, error)
	Shape() (any, error)
	Copy_() any
	Deepcopy(memo *py.Memo) any
	Repr() string
}

// TYPE_TO_WRAPPER maps a payload Go type to its wrapper class
// (``TYPE_TO_WRAPPER: dict[type, type[ComputationalData]]``).
var TYPE_TO_WRAPPER = map[reflect.Type]*py.Class{}

// CLASS_TO_WRAPPER_ is the same registry for laila classes (payloads that
// are py.Object instances); the dispatch walks their MRO.
var CLASS_TO_WRAPPER_ = map[*py.Class]*py.Class{}

// Register_cdtype registers cls as the wrapper for one or more payload
// types (``@register_cdtype(*payload_types)``). Types are given as sample
// values or reflect.Type / *py.Class descriptors. Returns cls.
func Register_cdtype(cls *py.Class, payload_types ...any) *py.Class {
	for _, t := range payload_types {
		switch x := t.(type) {
		case reflect.Type:
			TYPE_TO_WRAPPER[x] = cls
		case *py.Class:
			CLASS_TO_WRAPPER_[x] = cls
		default:
			TYPE_TO_WRAPPER[reflect.TypeOf(t)] = cls
		}
	}
	return cls
}

// Scalar_len_ is the TypeError for payloads that have no meaningful length.
func Scalar_len_() (int, error) {
	return 0, py.TypeError.New("Length undefined for scalars / objects without __len__")
}

// ComputationalData is the generic computational-data wrapper with
// dynamic subclass dispatch and serializer-based transformation.
type ComputationalData struct {
	py.Object
	Data any `py:"data,required"`
	// Serializer_ is ``_serializer``: created lazily on first access.
	Serializer_ Data_transformer `py:"_serializer"`
}

// ComputationalData_class is the class descriptor. ``_SERIALIZER_CLS`` is
// the class default serializer; ``_SERIALIZER_ACCEPTS`` the classes the
// setter accepts and ``_SERIALIZER_ERROR`` its message.
var ComputationalData_class = py.NewClass("ComputationalData", (*ComputationalData)(nil)).
	Set_classvar("_SERIALIZER_CLS", PickleSerializer_class).
	Set_classvar("_SERIALIZER_ACCEPTS", []*py.Class{PickleSerializer_class}).
	Set_classvar("_SERIALIZER_ERROR", "serializer must be a PickleSerializer or compatible object, got %s")

// Base_ returns the shared state.
func (c *ComputationalData) Base_() *ComputationalData { return c }

// Payload_from_args_ is the ``__init__`` argument normalisation: accept a
// positional payload or ``data=``; both is a TypeError.
func Payload_from_args_(args []any) (py.Kw, error) {
	pos, kw := py.SplitKw(args)
	if len(pos) > 0 {
		if len(pos) > 1 {
			return nil, py.TypeError.New("At most one positional argument (the payload)")
		}
		if kw.Has("data") {
			return nil, py.TypeError.New("Payload given both positionally and as 'data='")
		}
		kw["data"] = pos[0]
	}
	return kw, nil
}

// Wrapper_for_ is the dispatch of ``ComputationalData.__new__``: exact type
// match, then the laila class MRO, then CD_generic.
func Wrapper_for_(data any) *py.Class {
	if data != nil {
		if chosen, ok := TYPE_TO_WRAPPER[reflect.TypeOf(data)]; ok {
			return chosen
		}
		if cls := py.Classof(data); cls != nil {
			for _, parent := range cls.Mro() {
				if chosen, ok := CLASS_TO_WRAPPER_[parent]; ok {
					return chosen
				}
			}
		}
	}
	return CD_generic_class
}

// New_ComputationalData is ``ComputationalData(data)`` /
// ``ComputationalData(data=...)``: dispatches to the registered subclass
// for the payload's type. TypeError when no payload is given.
func New_ComputationalData(args ...any) (Computational_data, error) {
	kw, err := Payload_from_args_(args)
	if err != nil {
		return nil, err
	}
	data := kw.Get("data", nil)
	if data == nil {
		return nil, py.TypeError.New("Missing required argument 'data'")
	}
	return New_computational_data_of_(Wrapper_for_(data), kw)
}

// New_computational_data_of_ instantiates a concrete wrapper class with
// already-normalised keyword arguments (direct subclass call: no dispatch).
func New_computational_data_of_(cls *py.Class, kw py.Kw) (Computational_data, error) {
	inst := cls.New()
	if err := py.Init(inst, cls, kw); err != nil {
		return nil, err
	}
	return inst.(Computational_data), nil
}

// Ensure_serializer_ returns the instance serializer, constructing the
// class default on first use (``_ensure_serializer``).
func (c *ComputationalData) Ensure_serializer_() Data_transformer {
	if c.Serializer_ == nil {
		cls, _ := c.Class_().Classvar("_SERIALIZER_CLS")
		c.Serializer_ = py.Must(New_transformation_(cls.(*py.Class), py.Kw{}))
	}
	return c.Serializer_
}

// Serializer is the serializer used by Serialize (PickleSerializer by
// default; subclasses override ``_SERIALIZER_CLS``).
func (c *ComputationalData) Serializer() Data_transformer {
	if e, ok := c.Self().(interface{ Ensure_serializer_() Data_transformer }); ok {
		return e.Ensure_serializer_()
	}
	return c.Ensure_serializer_()
}

// Set_serializer replaces the serializer; the value must be an instance
// of one of the class's accepted serializer classes.
func (c *ComputationalData) Set_serializer(value any) error {
	accepts, _ := c.Class_().Classvar("_SERIALIZER_ACCEPTS")
	for _, a := range accepts.([]*py.Class) {
		if py.Isinstance(value, a) {
			c.Serializer_ = value.(Data_transformer)
			return nil
		}
	}
	msg, _ := c.Class_().Classvar("_SERIALIZER_ERROR")
	return py.TypeError.Newf(msg.(string), py.Pytypename(value))
}

// Serialize serialises ``data`` with the current serializer and returns
// ``(serialized_bytes, backward_code)``. Format-specific serializers reject
// values their wire format cannot express (TypeError / OverflowError /
// ValueError); such payloads fall back to the universal PickleSerializer.
func (c *ComputationalData) Serialize() (any, string, error) {
	serializer := c.Self().(Computational_data).Serializer()
	out, err := serializer.Forward(c.Data)
	if err == nil {
		return out, serializer.Base_().Backward_code, nil
	}
	if !(errors.Is(err, py.TypeError) || errors.Is(err, py.OverflowError) || errors.Is(err, py.ValueError)) {
		return nil, "", err
	}
	if py.Isinstance(serializer, PickleSerializer_class) {
		return nil, "", err
	}
	fallback, ferr := New_PickleSerializer()
	if ferr != nil {
		return nil, "", ferr
	}
	out, err = fallback.Forward(c.Data)
	if err != nil {
		return nil, "", err
	}
	return out, fallback.Backward_code, nil
}

// Getitem indexes into the underlying payload (``__getitem__``).
func (c *ComputationalData) Getitem(index any) (any, error) {
	return py.Getitem(c.Data, index)
}

// Repr is ``ClassName(data=<repr>)``.
func (c *ComputationalData) Repr() string {
	return c.Class_().Name + "(data=" + py.Repr(c.Data) + ")"
}

// String is ``__str__ = __repr__``.
func (c *ComputationalData) String() string { return c.Self().(Computational_data).Repr() }

// Len is the payload length (subclasses override; NotImplementedError).
func (c *ComputationalData) Len() (int, error) {
	return 0, py.NotImplementedError.New()
}

// Shape is the payload shape (subclasses override; NotImplementedError).
func (c *ComputationalData) Shape() (any, error) {
	return nil, py.NotImplementedError.New()
}

// Copy_ is ``__copy__`` (subclasses override).
func (c *ComputationalData) Copy_() any {
	panic(py.NotImplementedError.New())
}

// Deepcopy is ``__deepcopy__`` (subclasses override).
func (c *ComputationalData) Deepcopy(memo *py.Memo) any {
	panic(py.NotImplementedError.New())
}

// Rewrap_ is ``type(self)(data)``: a new wrapper of the same class.
func (c *ComputationalData) Rewrap_(data any) any {
	return py.Must(New_computational_data_of_(c.Class_(), py.Kw{"data": data}))
}

// Dtype_name_ renders a numpy array-protocol dtype string the way
// ``str(arr.dtype)`` does ("<f8" -> "float64", "|b1" -> "bool").
func Dtype_name_(dtype string) string {
	d := strings.TrimLeft(dtype, "<>|=")
	if len(d) < 2 {
		return dtype
	}
	kind, size := d[0], d[1:]
	bits := func() string {
		switch size {
		case "1":
			return "8"
		case "2":
			return "16"
		case "4":
			return "32"
		case "8":
			return "64"
		case "16":
			return "128"
		}
		return size
	}()
	switch kind {
	case 'b':
		return "bool"
	case 'i':
		return "int" + bits
	case 'u':
		return "uint" + bits
	case 'f':
		return "float" + bits
	case 'c':
		return "complex" + bits
	case 'U':
		return "<U" + size
	case 'S':
		return "|S" + size
	}
	return dtype
}

func init() {
	py.Register_property("serializer", "shape", "data")
}
