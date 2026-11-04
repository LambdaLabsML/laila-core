package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// entry/compdata/transformation/serialization/msgpack.py -- msgpack
// serialisation / deserialisation transformation.
//
// Backward (and the emitted backward_code) unpack with
// ``strict_map_key=False`` so dict payloads keyed by int / float / bool
// round-trip. Forward packs with ``strict_types=True``: msgpack has no
// tuple type and no notion of dict / list / int / str *subclasses*; in
// strict mode the packer raises TypeError for such values and
// ComputationalData.Serialize falls back to the universal
// PickleSerializer, so what you memorize is exactly what you remember.
// Pass ``forward_kwargs={"strict_types": False}`` to opt back into
// coercion for a specific instance.

// MsgpackSerializer is the reversible msgpack serialiser.
type MsgpackSerializer struct {
	Data_transformation_
}

// MsgpackSerializer_class is the class descriptor (registered as "msgpack").
var MsgpackSerializer_class = Register_transformation_(
	py.NewClass("MsgpackSerializer", (*MsgpackSerializer)(nil), Data_transformation_class), "msgpack", nil)

// New_MsgpackSerializer is ``MsgpackSerializer(**kw)``.
func New_MsgpackSerializer(kw ...py.Kw) (*MsgpackSerializer, error) {
	t := MsgpackSerializer_class.New().(*MsgpackSerializer)
	if err := t.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return t, nil
}

// Model_post_init builds backward_code dynamically after model creation.
func (t *MsgpackSerializer) Model_post_init(ctx any) error {
	t.Set_backward_code(
		"\ndef backward(inp):\n" +
			"    import msgpack\n" +
			"    kwargs = {\"raw\": False, \"strict_map_key\": False, **" + py.Repr(t.Backward_kwargs) + "}\n" +
			"    return msgpack.unpackb(inp, **kwargs)\n")
	return nil
}

// Forward serialises a value into msgpack bytes. TypeError for values
// msgpack cannot express faithfully (tuples, subclasses, arbitrary objects).
func (t *MsgpackSerializer) Forward(inp any) (any, error) {
	opts := py.MsgpackOpts{Strict_types: true}
	if v, ok := t.Forward_kwargs.Get("strict_types"); ok {
		opts.Strict_types = py.Truthy(v)
	}
	if v, ok := t.Forward_kwargs.Get("use_single_float"); ok {
		opts.Use_single_float = py.Truthy(v)
	}
	return py.Msgpack_packb(inp, opts)
}

// Backward deserialises msgpack bytes back into a value.
func (t *MsgpackSerializer) Backward(inp any) (any, error) {
	b, ok := As_bytes_(inp)
	if !ok {
		return nil, py.TypeError.Newf("a bytes-like object is required, not '%s'", py.Pytypename(inp))
	}
	return py.Msgpack_unpackb(b)
}
