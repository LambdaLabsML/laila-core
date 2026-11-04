package laila

import (
	"encoding/base64"
	"unicode/utf8"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// entry/compdata/transformation/compression/zlib.py -- zlib compression /
// decompression data transformation.
//
// Zlib slots into a pipeline as the compression step. It accepts a UTF-8
// string, compresses it with zlib.compress and re-encodes the compressed
// bytes with base64 so the result is still a plain string (matching the
// contract assumed by downstream steps and by JSON-only pools). The
// integrated base64 wrap keeps every pipeline output a string and makes
// the emitted recovery code self-contained.
//
// ``forward_kwargs`` is forwarded to zlib.compress (``level``);
// ``backward_kwargs`` to zlib.decompress.
//
// Known, accepted deviation: Go's compress/zlib produces different (valid)
// zlib bytes than C zlib for the same input; both sides decode each other.

// Zlib is the reversible zlib compression transformation.
type Zlib struct {
	Data_transformation_
}

// Zlib_class is the class descriptor (registered as "zlib").
var Zlib_class = Register_transformation_(
	py.NewClass("Zlib", (*Zlib)(nil), Data_transformation_class), "zlib", nil)

// New_Zlib is ``Zlib(**kw)``.
func New_Zlib(kw ...py.Kw) (*Zlib, error) {
	t := Zlib_class.New().(*Zlib)
	if err := t.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return t, nil
}

// Model_post_init builds the standalone backward recovery code mirroring
// Backward with embedded kwargs.
func (t *Zlib) Model_post_init(ctx any) error {
	t.Set_backward_code(
		"\ndef backward(data):\n" +
			"    import zlib, base64\n" +
			"    if not isinstance(data, str):\n" +
			"        raise TypeError(\"Zlib.backward expects a Base64 string (str)\")\n" +
			"    compressed = base64.b64decode(data, validate=True)\n" +
			"    return zlib.decompress(compressed, **" + py.Repr(t.Backward_kwargs) + ").decode(\"utf-8\")\n")
	return nil
}

// Forward compresses a UTF-8 string and returns a Base64-encoded result.
// TypeError unless data is a string.
func (t *Zlib) Forward(data any) (any, error) {
	s, ok := data.(string)
	if !ok {
		return nil, py.TypeError.New("Zlib.forward expects a UTF-8 string (str)")
	}
	level := -1
	if lv, ok := t.Forward_kwargs.Get("level"); ok && !py.IsNone(lv) {
		n, err := toIntPtr(lv)
		if err != nil {
			return nil, err
		}
		level = *n
	}
	compressed, err := py.Zlib_compress([]byte(s), level)
	if err != nil {
		return nil, err
	}
	return string(py.B64encode(compressed)), nil
}

// Backward decompresses a Base64-encoded compressed string. TypeError
// unless data is a string; ``binascii.Error`` (ValueError) for invalid
// base64, ``zlib.error`` for invalid compressed data.
func (t *Zlib) Backward(data any) (any, error) {
	s, ok := data.(string)
	if !ok {
		return nil, py.TypeError.New("Zlib.backward expects a Base64 string (str)")
	}
	// base64.b64decode(data, validate=True)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '+' || c == '/' || c == '=') {
			return nil, py.ValueError.New("Only base64 data is allowed")
		}
	}
	compressed, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, py.ValueError.New("Incorrect padding").From(err)
	}
	out, err := py.Zlib_decompress(compressed)
	if err != nil {
		return nil, err
	}
	if !utf8.Valid(out) {
		return nil, py.UnicodeDecodeError.New("'utf-8' codec can't decode bytes: invalid start byte")
	}
	return string(out), nil
}
