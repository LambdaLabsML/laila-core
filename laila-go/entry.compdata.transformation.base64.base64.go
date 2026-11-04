package laila

import (
	"encoding/base64"
	"strings"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// entry/compdata/transformation/base64/base64.py -- Base64 encode / decode
// data transformation.
//
// Base64 makes binary blobs JSON-safe by mapping bytes <-> ASCII strings
// via base64.b64encode / b64decode. It is the standard "tail" step in
// pool transformation pipelines whose backing store wants only printable
// text (filesystem JSON, S3 JSON objects, postgres TEXT columns, ...).
//
// Forward takes bytes (``[]byte``) and returns a UTF-8 string; Backward
// accepts a string or bytes and returns bytes. ``forward_kwargs`` /
// ``backward_kwargs`` are forwarded to the underlying functions
// (``altchars=b'-_'`` for a URL-safe alphabet, ``validate=True``).

// Base64 is the reversible Base64 encoding transformation.
type Base64 struct {
	Data_transformation_
}

// Base64_class is the class descriptor (registered as "base64").
var Base64_class = Register_transformation_(
	py.NewClass("Base64", (*Base64)(nil), Data_transformation_class), "base64", nil)

// New_Base64 is ``Base64(**kw)``.
func New_Base64(kw ...py.Kw) (*Base64, error) {
	t := Base64_class.New().(*Base64)
	if err := t.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return t, nil
}

// Model_post_init builds the standalone backward recovery code with the
// embedded kwargs (e.g. altchars, validate).
func (t *Base64) Model_post_init(ctx any) error {
	t.Set_backward_code(
		"def backward(inp):\n" +
			"    import base64\n" +
			"    kwargs = " + py.Repr(t.Backward_kwargs) + "\n" +
			"    if isinstance(inp, memoryview):\n" +
			"        inp = inp.tobytes()\n" +
			"    return base64.b64decode(inp, **kwargs)\n")
	return nil
}

// Forward encodes binary data -> Base64 UTF-8 string. TypeError unless
// data is bytes.
func (t *Base64) Forward(data any) (any, error) {
	b, ok := As_bytes_(data)
	if !ok {
		return nil, py.TypeError.New("Base64.forward expects bytes/bytearray/memoryview")
	}
	out := string(py.B64encode(b))
	if alt, ok := t.Forward_kwargs.Get("altchars"); ok && !py.IsNone(alt) {
		a, _ := As_bytes_(alt)
		if s, isStr := alt.(string); isStr {
			a = []byte(s)
		}
		if len(a) != 2 {
			return nil, py.ValueError.New("altchars must be a 2-byte sequence")
		}
		out = strings.NewReplacer("+", string(a[0:1]), "/", string(a[1:2])).Replace(out)
	}
	return out, nil
}

// Backward decodes Base64 (str / bytes) -> raw bytes. ValueError on an
// invalid payload.
func (t *Base64) Backward(payload any) (any, error) {
	var s string
	switch x := payload.(type) {
	case string:
		s = x
	default:
		b, ok := As_bytes_(payload)
		if !ok {
			return nil, py.ValueError.New("Invalid Base64 payload").From(
				py.TypeError.Newf("argument should be a bytes-like object or ASCII string, not '%s'", py.Pytypename(payload)))
		}
		s = string(b)
	}
	if alt, ok := t.Backward_kwargs.Get("altchars"); ok && !py.IsNone(alt) {
		a, _ := As_bytes_(alt)
		if str, isStr := alt.(string); isStr {
			a = []byte(str)
		}
		if len(a) == 2 {
			s = strings.NewReplacer(string(a[0:1]), "+", string(a[1:2]), "/").Replace(s)
		}
	}
	if py.Truthy(t.Backward_kwargs.GetDefault("validate", false)) {
		for i := 0; i < len(s); i++ {
			c := s[i]
			if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '+' || c == '/' || c == '=') {
				return nil, py.ValueError.New("Invalid Base64 payload").From(py.ValueError.New("Only base64 data is allowed"))
			}
		}
		out, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, py.ValueError.New("Invalid Base64 payload").From(err)
		}
		return out, nil
	}
	out, err := py.B64decode(s)
	if err != nil {
		return nil, py.ValueError.New("Invalid Base64 payload").From(err)
	}
	return out, nil
}

// As_bytes_ is ``isinstance(v, (bytes, bytearray, memoryview))`` with the
// value as bytes: the port's bytes model is ``[]byte``.
func As_bytes_(v any) ([]byte, bool) {
	switch x := v.(type) {
	case []byte:
		return x, true
	case *[]byte:
		if x == nil {
			return nil, false
		}
		return *x, true
	}
	return nil, false
}
