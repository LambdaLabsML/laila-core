package laila

import (
	"reflect"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// entry/compdata/taxonomy/cd_list.py -- ComputationalData subclass for
// list and tuple payloads.
//
// Both sequence types share one wrapper. Lists serialise as msgpack
// arrays; tuples default to pickle because msgpack has no tuple type,
// which is what keeps the original type (list vs tuple) intact on
// round-trip. A tuple nested inside a list is caught too: strict-mode
// msgpack raises and Serialize falls back to pickle.

// CD_list wraps ``list`` (``*py.List``, ``[]any``) and ``tuple``
// (``py.Tuple``) payloads. Shape is the 1-D ``(len,)``.
type CD_list struct {
	ComputationalData
}

// CD_list_class is the class descriptor (``@register_cdtype(list, tuple)``).
var CD_list_class = Register_cdtype(
	py.NewClass("CD_list", (*CD_list)(nil), ComputationalData_class).
		Set_classvar("_SERIALIZER_CLS", MsgpackSerializer_class).
		Set_classvar("_SERIALIZER_ACCEPTS", []*py.Class{MsgpackSerializer_class, PickleSerializer_class}).
		Set_classvar("_SERIALIZER_ERROR", "serializer must be a MsgpackSerializer or PickleSerializer, got %s").
		Add_field_validator("data", func(self any, v any) (any, error) {
			switch v.(type) {
			case *py.List, []any, py.Tuple:
				return v, nil
			}
			return nil, py.ValueError.New("Input should be an instance of list or tuple")
		}),
	reflect.TypeOf((*py.List)(nil)), reflect.TypeOf([]any(nil)), reflect.TypeOf(py.Tuple(nil)),
)

// New_CD_list is ``CD_list(data)``.
func New_CD_list(args ...any) (*CD_list, error) {
	kw, err := Payload_from_args_(args)
	if err != nil {
		return nil, err
	}
	c, err := New_computational_data_of_(CD_list_class, kw)
	if err != nil {
		return nil, err
	}
	return c.(*CD_list), nil
}

// Ensure_serializer_ is msgpack for lists, pickle for tuples.
func (c *CD_list) Ensure_serializer_() Data_transformer {
	if c.Serializer_ == nil {
		if _, isTuple := c.Data.(py.Tuple); isTuple {
			c.Serializer_ = py.Must(New_PickleSerializer())
		} else {
			cls, _ := c.Class_().Classvar("_SERIALIZER_CLS")
			c.Serializer_ = py.Must(New_transformation_(cls.(*py.Class), py.Kw{}))
		}
	}
	return c.Serializer_
}

// Len is the number of elements.
func (c *CD_list) Len() (int, error) { return py.Len(c.Data) }

// Shape is the 1-D shape tuple ``(len,)``.
func (c *CD_list) Shape() (any, error) {
	n, err := py.Len(c.Data)
	if err != nil {
		return nil, err
	}
	return py.Tuple{n}, nil
}

// Copy_ returns a shallow copy (lists are copied; tuples are immutable
// and shared).
func (c *CD_list) Copy_() any {
	if _, isTuple := c.Data.(py.Tuple); isTuple {
		return c.Rewrap_(c.Data)
	}
	return c.Rewrap_(py.Copy(c.Data))
}

// Deepcopy returns a deep copy preserving the sequence type.
func (c *CD_list) Deepcopy(memo *py.Memo) any {
	return c.Rewrap_(py.DeepcopyMemo(c.Data, memo))
}

// Repr is ``CD_list(type=list|tuple, len=N)``.
func (c *CD_list) Repr() string {
	tname := "list"
	if _, isTuple := c.Data.(py.Tuple); isTuple {
		tname = "tuple"
	}
	n, _ := py.Len(c.Data)
	return "CD_list(type=" + tname + ", len=" + py.Str(n) + ")"
}
