package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// entry/constitution/simple_constitution.py -- simple constitution: an
// ordered chain of single-callable code strings.
//
// Build(payload_input) threads payload_input through every callable in
// order. The canonical use case is the inverse transformation chain
// emitted by Entry.Serialize: when a pool serialises an entry through
// ``base64 -> zlib -> msgpack`` the constitution holds three snippets whose
// composition decodes the bytes back -- already in reverse order, so the
// constitution is a flat list applied left-to-right.

// SimpleConstitution is an ordered chain of single-callable code strings.
// An empty chain is valid and returns payload_input unchanged. Treat
// instances as immutable from the outside (Codes returns a copy).
type SimpleConstitution struct {
	Constitution_
	Codes_ []string `py:"_codes"`
}

// SimpleConstitution_class is the class descriptor (kind "simple").
var SimpleConstitution_class = py.NewClass("SimpleConstitution", (*SimpleConstitution)(nil), Constitution_class)

func init() { Register_kind_("simple", SimpleConstitution_class, SimpleConstitution_from_dict_) }

// New_SimpleConstitution is ``SimpleConstitution(codes=[...])``. The list
// is shallow-copied. TypeError unless codes is a list of strings.
func New_SimpleConstitution(kw ...py.Kw) (*SimpleConstitution, error) {
	c := SimpleConstitution_class.New().(*SimpleConstitution)
	data := py.MergeKw(kw...)
	if err := py.Init(c, SimpleConstitution_class, py.Kw{}); err != nil {
		return nil, err
	}
	codes, err := codesFrom(data.Get("codes", []string{}))
	if err != nil {
		return nil, err
	}
	c.Codes_ = codes
	return c, nil
}

func codesFrom(raw any) ([]string, error) {
	bad := py.TypeError.New("codes must be a list of Python source strings")
	switch x := raw.(type) {
	case []string:
		return append([]string{}, x...), nil
	case *py.List, []any:
		out := []string{}
		for _, it := range py.Iter(x) {
			s, ok := it.(string)
			if !ok {
				return nil, bad
			}
			out = append(out, s)
		}
		return out, nil
	}
	return nil, bad
}

// Codes is a defensive copy of the ordered list of source strings.
func (c *SimpleConstitution) Codes() []string { return append([]string{}, c.Codes_...) }

// Build threads payload_input through every code in order. Each step is
// resolved via Exec_one_fn_ on every build (no cache).
func (c *SimpleConstitution) Build(payload_input any) (any, error) {
	current := payload_input
	for _, code := range c.Codes_ {
		fn, err := Exec_one_fn_(code)
		if err != nil {
			return nil, err
		}
		current, err = fn(current)
		if err != nil {
			return nil, err
		}
	}
	return current, nil
}

// As_dict serialises as ``{"_kind": "simple", "codes": [...], "_hmac": ...}``.
// ``_hmac`` signs the code list with the per-user constitution secret so a
// hand-written simple constitution round-trips for the same user.
func (c *SimpleConstitution) As_dict() (*py.Dict, error) {
	codes := py.NewList(stringsToAny(c.Codes_)...)
	return py.DictOf("_kind", "simple", "codes", codes, "_hmac", Sign_simple(c.Codes_)), nil
}

// SimpleConstitution_from_dict_ rebuilds from the serialised dict. Missing
// ``codes`` is an empty chain. Every snippet must pass the trust gate
// (Check_simple_codes) before anything could be executed.
func SimpleConstitution_from_dict_(in_dict *py.Dict) (Constitution, error) {
	codes := in_dict.GetDefault("codes", py.NewList())
	if err := Check_simple_codes(codes, in_dict.GetDefault("_hmac", nil)); err != nil {
		return nil, err
	}
	return New_SimpleConstitution(py.Kw{"codes": codes})
}

func init() { py.Register_property("codes") }
