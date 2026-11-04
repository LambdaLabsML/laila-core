package laila

import "github.com/amirzadehlambda/laila/laila-go/py"

// atomic/definitions/locally_atomic_identifiable_object.py -- identifiable
// + locally-atomic mixin: the most-used base class in laila (pools,
// taskforces, comm protocols, futures, ...).

// LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_ is an identifiable object with
// a per-instance reentrant lock. Pure mixin: no extra fields or methods.
type LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_ struct {
	LAILA_IDENTIFIABLE_OBJECT_
	Locally_atomic_mixin
}

// LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_class is the class descriptor;
// the MRO is intentional: _LAILA_LOCALLY_ATOMIC_OBJECT first, then
// _LAILA_IDENTIFIABLE_OBJECT.
var LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_class = py.NewClass(
	"_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT",
	(*LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_)(nil),
	LAILA_LOCALLY_ATOMIC_OBJECT_class, LAILA_IDENTIFIABLE_OBJECT_class,
)

// New_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_ is the constructor.
func New_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_(kw ...py.Kw) (*LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_, error) {
	o := LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_class.New().(*LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_)
	if err := o.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return o, nil
}

// Atomic holds the local lock for the duration of the block and yields
// the locked instance (``with obj.atomic() as locked``).
func (o *LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_) Atomic(kw ...py.Kw) (any, func(), error) {
	return o.Atomic_(o.Self(), py.MergeKw(kw...))
}
