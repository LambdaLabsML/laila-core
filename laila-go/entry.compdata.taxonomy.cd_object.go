package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// entry/compdata/taxonomy/cd_object.py -- ComputationalData catch-all
// subclass for arbitrary objects.
//
// The fallback of Wrapper_for_ (registered against ``object``), so the
// dispatch always finds at least this class. Uses PickleSerializer -- the
// broadest-compatibility option, at the cost of being Python-specific on
// the wire.

// CD_generic is the catch-all computational-data wrapper. Len defers to
// the payload's own length when it has one and otherwise raises
// (Scalar_len_).
type CD_generic struct {
	ComputationalData
}

// CD_generic_class is the class descriptor (``@register_cdtype(object)``:
// the fallback needs no registration entry in the port).
var CD_generic_class = py.NewClass("CD_generic", (*CD_generic)(nil), ComputationalData_class).
	Set_classvar("_SERIALIZER_CLS", PickleSerializer_class).
	Set_classvar("_SERIALIZER_ACCEPTS", []*py.Class{PickleSerializer_class}).
	Set_classvar("_SERIALIZER_ERROR", "serializer must be a PickleSerializer, got %s")

// New_CD_generic is ``CD_generic(data)``.
func New_CD_generic(args ...any) (*CD_generic, error) {
	kw, err := Payload_from_args_(args)
	if err != nil {
		return nil, err
	}
	c, err := New_computational_data_of_(CD_generic_class, kw)
	if err != nil {
		return nil, err
	}
	return c.(*CD_generic), nil
}

// Len returns the length if the payload supports ``__len__``.
func (c *CD_generic) Len() (int, error) {
	if n, err := py.Len(c.Data); err == nil {
		return n, nil
	}
	return Scalar_len_()
}

// Shape is not applicable for generic objects (AttributeError).
func (c *CD_generic) Shape() (any, error) {
	return nil, py.AttributeError.Newf("%s has no 'shape' attribute", py.Pytypename(c.Data))
}

// Copy_ returns a shallow copy.
func (c *CD_generic) Copy_() any { return c.Rewrap_(c.Data) }

// Deepcopy returns a deep copy.
func (c *CD_generic) Deepcopy(memo *py.Memo) any {
	return c.Rewrap_(py.DeepcopyMemo(c.Data, memo))
}

// Repr is ``CD_generic(<data repr>)``.
func (c *CD_generic) Repr() string { return "CD_generic(" + py.Repr(c.Data) + ")" }
