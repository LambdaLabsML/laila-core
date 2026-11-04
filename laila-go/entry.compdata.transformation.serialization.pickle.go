package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// entry/compdata/transformation/serialization/pickle.py -- pickle
// serialisation / deserialisation transformation.

// PickleSerializer is the reversible pickle serialiser for arbitrary
// objects (the universal fallback; version-coupled, Python-specific blobs).
type PickleSerializer struct {
	Data_transformation_
}

// PickleSerializer_class is the class descriptor (registered as "pickle").
var PickleSerializer_class = Register_transformation_(
	py.NewClass("PickleSerializer", (*PickleSerializer)(nil), Data_transformation_class), "pickle", nil)

// New_PickleSerializer is ``PickleSerializer(**kw)``.
func New_PickleSerializer(kw ...py.Kw) (*PickleSerializer, error) {
	t := PickleSerializer_class.New().(*PickleSerializer)
	if err := t.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return t, nil
}

// Model_post_init builds backward_code dynamically after model creation.
func (t *PickleSerializer) Model_post_init(ctx any) error {
	t.Set_backward_code(
		"\ndef backward(inp):\n" +
			"    import pickle\n" +
			"    kwargs = " + py.Repr(t.Backward_kwargs) + "\n" +
			"    return pickle.loads(inp, **kwargs)\n")
	return nil
}

// Forward serialises a value into pickle bytes (``forward_kwargs`` may
// carry ``protocol``).
func (t *PickleSerializer) Forward(inp any) (any, error) {
	if p, ok := t.Forward_kwargs.Get("protocol"); ok && !py.IsNone(p) {
		n, err := toIntPtr(p)
		if err != nil {
			return nil, err
		}
		return py.Pickle_dumps(inp, *n)
	}
	return py.Pickle_dumps(inp)
}

// Backward deserialises pickle bytes back into a value.
func (t *PickleSerializer) Backward(inp any) (any, error) {
	b, ok := As_bytes_(inp)
	if !ok {
		return nil, py.TypeError.Newf("a bytes-like object is required, not '%s'", py.Pytypename(inp))
	}
	return py.Pickle_loads(b)
}
