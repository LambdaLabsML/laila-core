package laila

import (
	"reflect"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// entry/compdata/taxonomy/cd_dict.py -- ComputationalData subclass for
// dict payloads.
//
// Dicts are serialised with msgpack rather than pickle so the on-disk
// representation is language-neutral and substantially smaller for the
// common shallow-string-keyed cases. msgpack packs in strict mode: a dict
// holding values it cannot express faithfully raises at pack time and
// ComputationalData.Serialize falls back to pickle.

// CD_dict wraps ``dict`` payloads (``*py.Dict``, ``map[string]any``,
// ``py.Kw``). Len reports the number of keys; Shape is undefined.
type CD_dict struct {
	ComputationalData
}

// CD_dict_class is the class descriptor (``@register_cdtype(dict)``). The
// ``data`` field is ``InstanceOf[dict]``: the caller's object is kept.
var CD_dict_class = Register_cdtype(
	py.NewClass("CD_dict", (*CD_dict)(nil), ComputationalData_class).
		Set_classvar("_SERIALIZER_CLS", MsgpackSerializer_class).
		Set_classvar("_SERIALIZER_ACCEPTS", []*py.Class{MsgpackSerializer_class}).
		Set_classvar("_SERIALIZER_ERROR", "serializer must be a MsgpackSerializer, got %s").
		Add_field_validator("data", func(self any, v any) (any, error) {
			switch v.(type) {
			case *py.Dict, map[string]any, py.Kw:
				return v, nil
			}
			return nil, py.ValueError.New("Input should be an instance of dict")
		}),
	reflect.TypeOf((*py.Dict)(nil)), reflect.TypeOf(map[string]any(nil)), reflect.TypeOf(py.Kw(nil)),
)

// New_CD_dict is ``CD_dict(data)``.
func New_CD_dict(args ...any) (*CD_dict, error) {
	kw, err := Payload_from_args_(args)
	if err != nil {
		return nil, err
	}
	c, err := New_computational_data_of_(CD_dict_class, kw)
	if err != nil {
		return nil, err
	}
	return c.(*CD_dict), nil
}

// Len is the number of keys in the dict.
func (c *CD_dict) Len() (int, error) { return py.Len(c.Data) }

// Shape is not applicable for dicts (AttributeError).
func (c *CD_dict) Shape() (any, error) {
	return nil, py.AttributeError.New("dict has no 'shape' attribute")
}

// Copy_ returns a shallow copy (``self.data.copy()``).
func (c *CD_dict) Copy_() any { return c.Rewrap_(py.Copy(c.Data)) }

// Deepcopy returns a deep copy.
func (c *CD_dict) Deepcopy(memo *py.Memo) any {
	return c.Rewrap_(py.DeepcopyMemo(c.Data, memo))
}

// Repr is ``CD_dict(<data repr>)``.
func (c *CD_dict) Repr() string { return "CD_dict(" + py.Repr(c.Data) + ")" }
