package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// entry/compdata/transformation/serialization/numpy.py -- NumPy array
// serialisation / deserialisation transformation (``.npy`` format).

// NumpySerializer is the reversible NumPy serialiser using np.save /
// np.load (``allow_pickle=False`` on both sides).
type NumpySerializer struct {
	Data_transformation_
}

// NumpySerializer_class is the class descriptor (registered as "numpy").
var NumpySerializer_class = Register_transformation_(
	py.NewClass("NumpySerializer", (*NumpySerializer)(nil), Data_transformation_class), "numpy", nil)

// New_NumpySerializer is ``NumpySerializer(**kw)``.
func New_NumpySerializer(kw ...py.Kw) (*NumpySerializer, error) {
	t := NumpySerializer_class.New().(*NumpySerializer)
	if err := t.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return t, nil
}

// Model_post_init builds backward_code dynamically after model creation.
func (t *NumpySerializer) Model_post_init(ctx any) error {
	t.Set_backward_code(
		"\ndef backward(inp):\n" +
			"    import io\n" +
			"    import numpy as np\n" +
			"    buf = io.BytesIO(inp)\n" +
			"    kwargs = {'allow_pickle': False, **" + py.Repr(t.Backward_kwargs) + "}\n" +
			"    return np.load(buf, **kwargs)\n")
	return nil
}

// Forward serialises an NDArray into ``.npy`` bytes.
func (t *NumpySerializer) Forward(inp any) (any, error) {
	a, ok := inp.(*py.NDArray)
	if !ok {
		return nil, py.TypeError.Newf("NumpySerializer.forward expects a numpy.ndarray, got %s", py.Pytypename(inp))
	}
	return py.Npy_save(a)
}

// Backward deserialises ``.npy`` bytes back into an NDArray.
func (t *NumpySerializer) Backward(inp any) (any, error) {
	b, ok := As_bytes_(inp)
	if !ok {
		return nil, py.TypeError.Newf("a bytes-like object is required, not '%s'", py.Pytypename(inp))
	}
	return py.Npy_load(b)
}
