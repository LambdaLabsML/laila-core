package laila

import (
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// entry/compdata/transformation/encryption/encryption.py -- Fernet
// symmetric encryption / decryption data transformation.
//
// FernetEncryption wraps a UTF-8 string in a Fernet token (AES-128-CBC +
// HMAC-SHA256, time-stamped, URL-safe base64). ``backward_kwargs={"ttl":
// seconds}`` rejects tokens older than ``seconds`` (ValueError).
//
// Key handling: the key is never written into backward_code or any
// serialised form (``exclude=True``). It lives in ``laila.args`` --
// canonical location ``laila.args.encryption.key``, alias
// ``laila.Encryption_key()`` / ``Set_encryption_key``. Construct with no
// ``key=`` to pick it up from there; an explicit key still wins. The
// recovery snippet carries only a short SHA-256 fingerprint of the
// write-time key, so a reader configured with a different key fails with
// a descriptive ValueError instead of a bare InvalidToken.

// FINGERPRINT_HEX_LEN_ is the fingerprint length embedded in recovery code.
const FINGERPRINT_HEX_LEN_ = 12

// FINGERPRINT_SLOT_ is the template sentinel for the fingerprint.
var FINGERPRINT_SLOT_ = strings.Repeat("0", FINGERPRINT_HEX_LEN_)

var FINGERPRINT_RE_ = regexp.MustCompile(`^[0-9a-f]{12}$`)

// Render_backward_code_ renders the standalone recovery code: no key
// material, only its fingerprint. The reader resolves the key from its own
// ``laila.args``. Shared by the template probe and the emitted code.
func Render_backward_code_(fingerprint string, backward_kwargs *py.Dict) string {
	return "\ndef backward(inp):\n" +
		"    from cryptography.fernet import Fernet, InvalidToken\n" +
		"    from laila.entry.compdata.transformation.encryption.encryption import (\n" +
		"        resolve_encryption_key,\n" +
		"    )\n" +
		"    if not isinstance(inp, str):\n" +
		"        raise TypeError(\"Encryption.backward expects a Fernet token string (str)\")\n" +
		"    f = Fernet(resolve_encryption_key(expected_fingerprint=" + py.Repr(fingerprint) + "))\n" +
		"    token = inp.encode(\"utf-8\")\n" +
		"    kwargs = " + py.Repr(backward_kwargs) + "\n" +
		"    ttl = kwargs.get(\"ttl\", None)\n" +
		"    try:\n" +
		"        out = f.decrypt(token, ttl=ttl)\n" +
		"    except InvalidToken as e:\n" +
		"        raise ValueError(\"Invalid Fernet token or TTL expired\") from e\n" +
		"    return out.decode(\"utf-8\")\n"
}

// Coerce_key_bytes_ is ``_coerce_key_bytes``: str -> utf-8 bytes; bytes
// pass through; TypeError otherwise.
func Coerce_key_bytes_(v any) ([]byte, error) {
	switch x := v.(type) {
	case string:
		return []byte(x), nil
	case []byte:
		return py.Bytes_clone(x), nil
	}
	return nil, py.TypeError.New("Encryption.key must be bytes or str")
}

// Key_fingerprint returns the short SHA-256 fingerprint laila embeds in
// recovery code.
func Key_fingerprint(key any) (string, error) {
	b, err := Coerce_key_bytes_(key)
	if err != nil {
		return "", err
	}
	return py.Sha256_hex(b)[:FINGERPRINT_HEX_LEN_], nil
}

// Resolve_encryption_key returns the process-wide encryption key configured
// in ``laila.args.encryption.key`` without auto-creating DotMap nodes.
// With expected_fingerprint (non-nil) the configured key must produce the
// same fingerprint. RuntimeError when no key is configured; ValueError on
// a fingerprint mismatch.
func Resolve_encryption_key(expected_fingerprint *string) ([]byte, error) {
	key := Encryption_key()
	if py.IsNone(key) {
		return nil, py.RuntimeError.New(
			"no encryption key configured in this process; set " +
				"`laila.encryption_key = <fernet key>` (i.e. laila.args.encryption.key) " +
				"on both the writing and the reading side")
	}
	key_bytes, err := Coerce_key_bytes_(key)
	if err != nil {
		return nil, err
	}
	if expected_fingerprint != nil {
		fp, _ := Key_fingerprint(key_bytes)
		if fp != *expected_fingerprint {
			return nil, py.ValueError.Newf(
				"configured encryption key (laila.encryption_key) does not match the key used "+
					"at write time (fingerprint %s); set the same key as the writer", *expected_fingerprint)
		}
	}
	return key_bytes, nil
}

// FernetEncryption is the reversible Fernet symmetric encryption
// transformation. ``key`` (str or bytes) is optional: when omitted the key
// is read from ``laila.args.encryption.key`` and re-resolved on every
// forward / backward so a rotation mid-run is honoured.
type FernetEncryption struct {
	Data_transformation_
	Key           []byte     `py:"key,exclude"`
	Fernet_       *py.Fernet `py:"_fernet"`
	Explicit_key_ bool       `py:"_explicit_key"`
}

// FernetEncryption_class is the class descriptor (registered as "fernet").
var FernetEncryption_class = py.NewClass("FernetEncryption", (*FernetEncryption)(nil), Data_transformation_class).
	Add_field_validator("key", func(self any, v any) (any, error) {
		if py.IsNone(v) {
			return nil, nil
		}
		return Coerce_key_bytes_(v)
	})

func init() {
	Register_transformation_(FernetEncryption_class, "fernet", &Transformation_hooks{
		Probe: func() (Data_transformer, error) {
			probe, err := New_FernetEncryption(py.Kw{"key": py.Fernet_generate_key(), "backward_kwargs": SLOT_KWARGS_.Copy()})
			if err != nil {
				return nil, err
			}
			probe.Set_backward_code(Render_backward_code_(FINGERPRINT_SLOT_, SLOT_KWARGS_.Copy()))
			return probe, nil
		},
		Slots: func() []Template_slot {
			// The fingerprint is rendered via repr inside quotes, so the slot
			// text is the bare hex and the captured group must be hex too.
			return []Template_slot{
				{Text: FINGERPRINT_SLOT_, Validator: FINGERPRINT_RE_.MatchString},
				{Text: SLOT_KWARGS_REPR_, Validator: Is_literal_dict_},
			}
		},
		Recover: func(groups []string) (Backward_fn, error) {
			fingerprint := groups[0]
			parsed, err := py.Literal_eval(groups[1])
			if err != nil {
				return nil, err
			}
			kwargs, _ := parsed.(*py.Dict)
			if kwargs == nil {
				kwargs = py.NewDict()
			}
			// The native equivalent of the emitted snippet: resolve the key
			// (checking the fingerprint) at *call* time.
			return func(inp any) (any, error) {
				s, ok := inp.(string)
				if !ok {
					return nil, py.TypeError.New("Encryption.backward expects a Fernet token string (str)")
				}
				key, err := Resolve_encryption_key(&fingerprint)
				if err != nil {
					return nil, err
				}
				f, err := py.New_Fernet(key)
				if err != nil {
					return nil, err
				}
				return fernetDecrypt(f, s, kwargs)
			}, nil
		},
	})
}

// New_FernetEncryption is ``FernetEncryption(key=None, **kw)``.
func New_FernetEncryption(kw ...py.Kw) (*FernetEncryption, error) {
	t := FernetEncryption_class.New().(*FernetEncryption)
	if err := t.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return t, nil
}

// Model_post_init initialises the Fernet encryptor and builds the backward
// recovery code. Fails fast (RuntimeError) when no key is configured.
func (t *FernetEncryption) Model_post_init(ctx any) error {
	t.Explicit_key_ = t.Key != nil
	if t.Key == nil {
		key, err := Resolve_encryption_key(nil)
		if err != nil {
			return err
		}
		t.Key = key
	}
	return t.Bind_key_(t.Key)
}

// Bind_key_ points the encryptor and the recovery code at key.
func (t *FernetEncryption) Bind_key_(key []byte) error {
	f, err := py.New_Fernet(key)
	if err != nil {
		return err
	}
	t.Key = key
	t.Fernet_ = f
	fp, _ := Key_fingerprint(key)
	t.Set_backward_code(Render_backward_code_(fp, t.Backward_kwargs))
	return nil
}

// Sync_key_ re-resolves ``laila.encryption_key`` unless a key was pinned.
func (t *FernetEncryption) Sync_key_() error {
	if t.Explicit_key_ {
		return nil
	}
	live, err := Resolve_encryption_key(nil)
	if err != nil {
		return err
	}
	if string(live) != string(t.Key) {
		return t.Bind_key_(live)
	}
	return nil
}

// Forward encrypts a UTF-8 string and returns the Fernet token as a
// string. TypeError unless data is a string (place Base64 before Fernet).
func (t *FernetEncryption) Forward(data any) (any, error) {
	s, ok := data.(string)
	if !ok {
		return nil, py.TypeError.New(
			"Encryption.forward expects a str (e.g. Base64 text); place Base64() before " +
				"FernetEncryption() -- see the transformation_encryption preset")
	}
	if err := t.Sync_key_(); err != nil {
		return nil, err
	}
	token, err := t.Fernet_.Encrypt([]byte(s))
	if err != nil {
		return nil, err
	}
	return string(token), nil
}

// Backward decrypts a Fernet token back to the original string. TypeError
// unless data is a string; ValueError if the token is invalid or the TTL
// has expired.
func (t *FernetEncryption) Backward(data any) (any, error) {
	s, ok := data.(string)
	if !ok {
		return nil, py.TypeError.New("Encryption.backward expects a Fernet token string (str)")
	}
	if err := t.Sync_key_(); err != nil {
		return nil, err
	}
	return fernetDecrypt(t.Fernet_, s, t.Backward_kwargs)
}

func fernetDecrypt(f *py.Fernet, token string, kwargs *py.Dict) (any, error) {
	ttl := -1.0
	if v, ok := Optional_float_(kwargs.GetDefault("ttl", nil)); ok {
		ttl = v
	}
	out, err := f.Decrypt(token, ttl)
	if err != nil {
		return nil, py.ValueError.New("Invalid Fernet token or TTL expired").From(err)
	}
	if !utf8.Valid(out) {
		return nil, py.UnicodeDecodeError.New("'utf-8' codec can't decode bytes")
	}
	return string(out), nil
}

// Repr hides the key (``repr=False``).
func (t *FernetEncryption) Repr() string {
	return t.Data_transformation_.Repr()
}
