package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// entry/compdata/transformation/jsonstring/jsonstring.py -- JSON string
// serialisation / deserialisation data transformation.
//
// JsonString is the canonical "round-trip through JSON text" step:
// Forward accepts any JSON-serialisable value and emits a compact JSON
// string (no whitespace, unicode-preserving); Backward parses it back.
// Most useful as the final step of a pipeline whose pool backend stores
// plain strings. For binary serialisation of arbitrary objects prefer
// PickleSerializer; for compact binary JSON-shaped data MsgpackSerializer.

// JsonString is the reversible JSON string transformation.
type JsonString struct {
	Data_transformation_
}

// JsonString_class is the class descriptor (registered as "json_string").
var JsonString_class = Register_transformation_(
	py.NewClass("JsonString", (*JsonString)(nil), Data_transformation_class), "json_string", nil)

// New_JsonString is ``JsonString(**kw)``.
func New_JsonString(kw ...py.Kw) (*JsonString, error) {
	t := JsonString_class.New().(*JsonString)
	if err := t.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return t, nil
}

// Model_post_init builds the standalone backward recovery code (slot-less:
// an exact-match template).
func (t *JsonString) Model_post_init(ctx any) error {
	t.Set_backward_code(
		"\ndef backward(data):\n" +
			"    import json\n" +
			"    if not isinstance(data, str):\n" +
			"        raise TypeError(\"JsonString.backward expects a JSON string (str)\")\n" +
			"    return json.loads(data)\n")
	return nil
}

// Forward serialises data to a compact JSON string
// (``separators=(",", ":"), ensure_ascii=False``). TypeError if the value
// is not JSON serialisable.
func (t *JsonString) Forward(data any) (any, error) {
	f := false
	opts := py.JsonOpts{Item_sep: ",", Key_sep: ":", Ensure_ascii: &f}
	if py.Truthy(t.Forward_kwargs.GetDefault("sort_keys", false)) {
		opts.Sort_keys = true
	}
	out, err := py.Json_dumps_opts(data, opts)
	if err != nil {
		return nil, py.TypeError.Newf("JsonString.forward: object not JSON serializable: %s", err.Error())
	}
	return out, nil
}

// Backward parses a JSON string back to a value. TypeError if data is not
// a string or contains invalid JSON.
func (t *JsonString) Backward(data any) (any, error) {
	s, ok := data.(string)
	if !ok {
		return nil, py.TypeError.New("JsonString.backward expects a JSON string (str)")
	}
	out, err := py.Json_loads(s)
	if err != nil {
		msg := err.Error()
		if pe, ok := err.(*py.Error); ok {
			if m := pe.Attr("msg"); m != nil {
				msg = py.Str(m)
			}
		}
		return nil, py.TypeError.Newf("JsonString.backward: invalid JSON string: %s", msg)
	}
	return out, nil
}
