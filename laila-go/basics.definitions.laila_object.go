package laila

import "github.com/amirzadehlambda/laila/laila-go/py"

// basics/definitions/laila_object.py -- root base class for every laila
// object.
//
// LAILA_OBJECT_ sits at the very bottom of the laila class hierarchy: both
// LAILA_IDENTIFIABLE_OBJECT_ (identity / global ids) and
// LAILA_LOCALLY_ATOMIC_OBJECT_ (per-instance locking) derive from it. Today
// that is a single concern: a creation timestamp (ISO-8601 UTC, millisecond
// precision) stamped in Init_ *after* the pydantic constructor returns.
//
// Construction protocol of the port (mirrors Python's __init__ chain):
//
//	New_X(kw...)  ->  x.Init_(kw)  ->  ... base Init_ hooks ...  ->
//	LAILA_OBJECT_.Init_ = py.Init(self) (BaseModel.__init__: validators,
//	field assignment, Model_post_init chain) then the timestamp.
//
// Every class with a Python ``__init__`` override defines Init_ and chains
// to its base's Init_ explicitly, exactly where Python calls
// ``super().__init__``.

// Now_creation_timestamp_ returns the current UTC time as an ISO-8601
// string (millisecond precision). Shared by LAILA_OBJECT_ and by
// deserializers that need to stamp objects constructed outside Init_.
func Now_creation_timestamp_() string { return py.Now_utc_iso_ms() }

// LAILA_OBJECT_ is the pydantic base model shared by every laila object.
type LAILA_OBJECT_ struct {
	py.Object
	Creation_timestamp_ *string `py:"_creation_timestamp"`
}

// LAILA_OBJECT_class is the class descriptor of _LAILA_OBJECT.
var LAILA_OBJECT_class = py.NewClass("_LAILA_OBJECT", (*LAILA_OBJECT_)(nil))

// New_LAILA_OBJECT_ is ``_LAILA_OBJECT(**data)``.
func New_LAILA_OBJECT_(kw ...py.Kw) (*LAILA_OBJECT_, error) {
	o := LAILA_OBJECT_class.New().(*LAILA_OBJECT_)
	if err := o.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return o, nil
}

// Init_ delegates to pydantic, then stamps the creation timestamp.
func (o *LAILA_OBJECT_) Init_(data py.Kw) error {
	if err := py.Init(o.Self(), o.Class_(), data); err != nil {
		return err
	}
	ts := Now_creation_timestamp_()
	o.Creation_timestamp_ = &ts
	return nil
}

// Model_post_init is the cooperative no-op hook every class below this
// root chains to.
func (o *LAILA_OBJECT_) Model_post_init(ctx any) error { return nil }

// Creation_timestamp is the ISO-8601 UTC timestamp of when this object was
// constructed. Read-only on purpose; nil only for instances built through
// paths that bypass Init_ and have not yet restored the value.
func (o *LAILA_OBJECT_) Creation_timestamp() *string { return o.Creation_timestamp_ }

func init() {
	py.Register_property("creation_timestamp")
}
