package laila

import "github.com/amirzadehlambda/laila/laila-go/py"

// entry/__init__.py -- the Entry data unit and ready-made transformation
// pipelines.
//
// Each preset is the *forward* pipeline applied at memorize time; the
// inverse pipeline is recorded in the entry's SimpleConstitution so
// reading back goes through the exact reverse sequence.
//
//   - Transformation_base64: base64 only (UTF-8-safe text backends).
//   - Transformation_base64_compression: base64 *of* zlib output; the
//     default for textual / network-friendly storage.
//   - Transformation_base64_compression_encryption(key): compress,
//     encrypt (Fernet), then base64.
//   - Transformation_encryption(key): base64 then Fernet (no compression).
//
// With a nil key the Fernet step reads the process-wide key from
// ``laila.args.encryption.key`` (alias Encryption_key). The key never
// travels with the data.

// Transformation_base64 is ``TransformationSequence([Base64()])``.
var Transformation_base64 = py.Must(New_TransformationSequence(py.Kw{
	"transformations": []any{py.Must(New_Base64())},
}))

// Transformation_base64_compression is
// ``TransformationSequence([Base64(), Zlib()])``.
var Transformation_base64_compression = py.Must(New_TransformationSequence(py.Kw{
	"transformations": []any{py.Must(New_Base64()), py.Must(New_Zlib())},
}))

// Transformation_base64_compression_encryption is the factory
// ``lambda key=None: TransformationSequence([Base64(), Zlib(),
// FernetEncryption(key=key)])``.
func Transformation_base64_compression_encryption(key ...any) (*TransformationSequence, error) {
	var k any
	if len(key) > 0 {
		k = key[0]
	}
	enc, err := New_FernetEncryption(py.Kw{"key": k})
	if err != nil {
		return nil, err
	}
	return New_TransformationSequence(py.Kw{
		"transformations": []any{py.Must(New_Base64()), py.Must(New_Zlib()), enc},
	})
}

// Transformation_encryption is the factory
// ``lambda key=None: TransformationSequence([Base64(), FernetEncryption(key=key)])``.
func Transformation_encryption(key ...any) (*TransformationSequence, error) {
	var k any
	if len(key) > 0 {
		k = key[0]
	}
	enc, err := New_FernetEncryption(py.Kw{"key": k})
	if err != nil {
		return nil, err
	}
	return New_TransformationSequence(py.Kw{
		"transformations": []any{py.Must(New_Base64()), enc},
	})
}
