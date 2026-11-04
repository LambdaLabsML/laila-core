package laila

import (
	"reflect"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// entry/compdata/taxonomy/cd_numpy.py -- ComputationalData subclass for
// numpy.ndarray payloads (``*py.NDArray`` in the port).
//
// Uses NumpySerializer (the ``.npy`` format) so arrays are persisted with
// their dtype and shape in a portable, language-neutral binary format.

// CD_numpyarray wraps ndarray payloads. Len follows NumPy semantics (size
// of the first axis; raises for 0-D arrays); Shape is the native shape
// tuple. Both copies emit a contiguous copy of the buffer.
type CD_numpyarray struct {
	ComputationalData
}

// CD_numpyarray_class is the class descriptor (``@register_cdtype(np.ndarray)``).
var CD_numpyarray_class = Register_cdtype(
	py.NewClass("CD_numpyarray", (*CD_numpyarray)(nil), ComputationalData_class).
		Set_classvar("_SERIALIZER_CLS", NumpySerializer_class).
		Set_classvar("_SERIALIZER_ACCEPTS", []*py.Class{NumpySerializer_class}).
		Set_classvar("_SERIALIZER_ERROR", "serializer must be a NumpySerializer, got %s").
		Add_field_validator("data", func(self any, v any) (any, error) {
			if _, ok := v.(*py.NDArray); ok {
				return v, nil
			}
			return nil, py.ValueError.New("Input should be an instance of ndarray")
		}),
	reflect.TypeOf((*py.NDArray)(nil)),
)

// New_CD_numpyarray is ``CD_numpyarray(data)``.
func New_CD_numpyarray(args ...any) (*CD_numpyarray, error) {
	kw, err := Payload_from_args_(args)
	if err != nil {
		return nil, err
	}
	c, err := New_computational_data_of_(CD_numpyarray_class, kw)
	if err != nil {
		return nil, err
	}
	return c.(*CD_numpyarray), nil
}

func (c *CD_numpyarray) array() *py.NDArray { return c.Data.(*py.NDArray) }

// Len is the size of the first dimension (TypeError for 0-D arrays).
func (c *CD_numpyarray) Len() (int, error) {
	a := c.array()
	if a.Ndim() == 0 {
		return Scalar_len_()
	}
	return a.Shape[0], nil
}

// Shape is the array shape tuple.
func (c *CD_numpyarray) Shape() (any, error) {
	a := c.array()
	out := make(py.Tuple, len(a.Shape))
	for i, d := range a.Shape {
		out[i] = d
	}
	return out, nil
}

// Copy_ returns a shallow (contiguous) copy of the array.
func (c *CD_numpyarray) Copy_() any { return c.Rewrap_(NDArray_copy_(c.array())) }

// Deepcopy returns a deep copy of the array.
func (c *CD_numpyarray) Deepcopy(memo *py.Memo) any { return c.Rewrap_(NDArray_copy_(c.array())) }

// Repr is ``CD_numpyarray(shape=(...), dtype=<name>)``.
func (c *CD_numpyarray) Repr() string {
	shape, _ := c.Shape()
	return "CD_numpyarray(shape=" + py.Repr(shape) + ", dtype=" + Dtype_name_(c.array().Dtype) + ")"
}

// NDArray_copy_ is ``arr.copy()``: a C-contiguous copy of the buffer.
func NDArray_copy_(a *py.NDArray) *py.NDArray {
	return &py.NDArray{
		Dtype:         a.Dtype,
		Shape:         append([]int(nil), a.Shape...),
		Data:          append([]byte(nil), a.Data...),
		Fortran_order: a.Fortran_order,
	}
}
