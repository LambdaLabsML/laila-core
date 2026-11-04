package py

import (
	"bytes"
	"compress/zlib"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// base64
// ---------------------------------------------------------------------------

// B64encode is base64.b64encode(data).
func B64encode(data []byte) []byte { return []byte(base64.StdEncoding.EncodeToString(data)) }

// B64decode is base64.b64decode(data) with validate=False: characters
// outside the alphabet are discarded before decoding.
func B64decode(data any) ([]byte, error) {
	s := asciiString(data)
	var sb strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '+' || c == '/' || c == '=' {
			sb.WriteByte(c)
		}
	}
	out, err := base64.StdEncoding.DecodeString(sb.String())
	if err != nil {
		return nil, ValueError.New("Incorrect padding").From(err)
	}
	return out, nil
}

// Urlsafe_b64encode is base64.urlsafe_b64encode(data).
func Urlsafe_b64encode(data []byte) []byte { return []byte(base64.URLEncoding.EncodeToString(data)) }

// Urlsafe_b64decode is base64.urlsafe_b64decode(data).
func Urlsafe_b64decode(data any) ([]byte, error) {
	s := asciiString(data)
	s = strings.NewReplacer("-", "+", "_", "/").Replace(s)
	return B64decode(s)
}

func asciiString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	}
	panic(TypeError.Newf("argument should be a bytes-like object or ASCII string, not '%s'", Pytypename(v)))
}

// ---------------------------------------------------------------------------
// zlib
// ---------------------------------------------------------------------------

// Zlib_compress is zlib.compress(data, level=-1).
func Zlib_compress(data []byte, level int) ([]byte, error) {
	var buf bytes.Buffer
	w, err := zlib.NewWriterLevel(&buf, level)
	if err != nil {
		return nil, ValueError.New("Bad compression level").From(err)
	}
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Zlib_decompress is zlib.decompress(data).
func Zlib_decompress(data []byte) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, ZlibError.New("Error -3 while decompressing data: incorrect header check").From(err)
	}
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, ZlibError.New("Error -3 while decompressing data: invalid stored block lengths").From(err)
	}
	return out, nil
}

// ZlibError is zlib.error.
var ZlibError = NewType("error", Exception)

// ---------------------------------------------------------------------------
// urllib.parse
// ---------------------------------------------------------------------------

// Quote is urllib.parse.quote(s, safe): every byte outside
// [A-Za-z0-9_.-~] and `safe` is percent-encoded (UTF-8).
func Quote(s string, safe string) string {
	var sb strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '.' || c == '-' || c == '~' || strings.IndexByte(safe, c) >= 0 {
			sb.WriteByte(c)
		} else {
			fmt.Fprintf(&sb, "%%%02X", c)
		}
	}
	return sb.String()
}

// Unquote is urllib.parse.unquote(s): %XX sequences decode to UTF-8 bytes;
// malformed escapes are kept verbatim.
func Unquote(s string) string {
	var out []byte
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s)+0 && i+2 <= len(s)-1 {
			if b, err := hex.DecodeString(s[i+1 : i+3]); err == nil {
				out = append(out, b[0])
				i += 2
				continue
			}
		}
		out = append(out, s[i])
	}
	return string(out)
}

// ---------------------------------------------------------------------------
// uuid
// ---------------------------------------------------------------------------

// UUID is uuid.UUID (16 bytes), formatted in the canonical hex form.
type UUID [16]byte

// String is str(uuid).
func (u UUID) String() string {
	h := hex.EncodeToString(u[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// Hex is uuid.hex.
func (u UUID) Hex() string { return hex.EncodeToString(u[:]) }

// Bytes is uuid.bytes.
func (u UUID) Bytes() []byte { return append([]byte(nil), u[:]...) }

// Repr is UUID('...').
func (u UUID) Repr() string { return "UUID('" + u.String() + "')" }

// Version is uuid.version.
func (u UUID) Version() int { return int(u[6] >> 4) }

// Uuid4 is uuid.uuid4().
func Uuid4() UUID {
	var u UUID
	if _, err := rand.Read(u[:]); err != nil {
		panic(err)
	}
	u[6] = (u[6] & 0x0f) | 0x40
	u[8] = (u[8] & 0x3f) | 0x80
	return u
}

// Uuid5 is uuid.uuid5(namespace, name).
func Uuid5(namespace UUID, name string) UUID {
	h := sha1.New()
	h.Write(namespace[:])
	h.Write([]byte(name))
	sum := h.Sum(nil)
	var u UUID
	copy(u[:], sum[:16])
	u[6] = (u[6] & 0x0f) | 0x50
	u[8] = (u[8] & 0x3f) | 0x80
	return u
}

// Uuid3 is uuid.uuid3(namespace, name).
func Uuid3(namespace UUID, name string) UUID {
	h := md5.New()
	h.Write(namespace[:])
	h.Write([]byte(name))
	sum := h.Sum(nil)
	var u UUID
	copy(u[:], sum[:16])
	u[6] = (u[6] & 0x0f) | 0x30
	u[8] = (u[8] & 0x3f) | 0x80
	return u
}

// Parse_uuid is uuid.UUID(hex_string) accepting hyphenated or bare hex.
func Parse_uuid(s string) (UUID, error) {
	h := strings.NewReplacer("-", "", "{", "", "}", "", "urn:", "", "uuid:", "").Replace(strings.TrimSpace(s))
	var u UUID
	if len(h) != 32 {
		return u, ValueError.New("badly formed hexadecimal UUID string")
	}
	b, err := hex.DecodeString(h)
	if err != nil {
		return u, ValueError.New("badly formed hexadecimal UUID string")
	}
	copy(u[:], b)
	return u, nil
}

// Well-known namespaces.
var (
	NAMESPACE_DNS = Must(Parse_uuid("6ba7b810-9dad-11d1-80b4-00c04fd430c8"))
	NAMESPACE_URL = Must(Parse_uuid("6ba7b811-9dad-11d1-80b4-00c04fd430c8"))
	NAMESPACE_OID = Must(Parse_uuid("6ba7b812-9dad-11d1-80b4-00c04fd430c8"))
)

// ---------------------------------------------------------------------------
// hashlib / secrets
// ---------------------------------------------------------------------------

// Sha256_hex is hashlib.sha256(data).hexdigest().
func Sha256_hex(data []byte) string {
	s := sha256.Sum256(data)
	return hex.EncodeToString(s[:])
}

// Sha256 is hashlib.sha256(data).digest().
func Sha256(data []byte) []byte {
	s := sha256.Sum256(data)
	return s[:]
}

// Sha1_hex is hashlib.sha1(data).hexdigest().
func Sha1_hex(data []byte) string {
	s := sha1.Sum(data)
	return hex.EncodeToString(s[:])
}

// Md5_hex is hashlib.md5(data).hexdigest().
func Md5_hex(data []byte) string {
	s := md5.Sum(data)
	return hex.EncodeToString(s[:])
}

// Token_hex is secrets.token_hex(n).
func Token_hex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// Token_bytes is secrets.token_bytes(n).
func Token_bytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
}

// Token_urlsafe is secrets.token_urlsafe(n).
func Token_urlsafe(n int) string {
	return strings.TrimRight(base64.URLEncoding.EncodeToString(Token_bytes(n)), "=")
}

// ---------------------------------------------------------------------------
// datetime
// ---------------------------------------------------------------------------

// Isoformat_ms is datetime.now(UTC).isoformat(timespec="milliseconds") for t.
func Isoformat_ms(t time.Time) string {
	t = t.UTC()
	return fmt.Sprintf("%04d-%02d-%02dT%02d:%02d:%02d.%03d+00:00",
		t.Year(), int(t.Month()), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond()/1e6)
}

// Isoformat is datetime.isoformat() with microseconds (omitted when zero).
func Isoformat(t time.Time) string {
	t = t.UTC()
	s := fmt.Sprintf("%04d-%02d-%02dT%02d:%02d:%02d", t.Year(), int(t.Month()), t.Day(), t.Hour(), t.Minute(), t.Second())
	if us := t.Nanosecond() / 1000; us != 0 {
		s += fmt.Sprintf(".%06d", us)
	}
	return s + "+00:00"
}

// Now_utc_iso_ms is datetime.now(UTC).isoformat(timespec="milliseconds").
func Now_utc_iso_ms() string { return Isoformat_ms(time.Now()) }

// Fromisoformat parses the ISO-8601 forms datetime.fromisoformat accepts
// for the stamps laila writes.
func Fromisoformat(s string) (time.Time, error) {
	layouts := []string{
		"2006-01-02T15:04:05.000-07:00", "2006-01-02T15:04:05.000000-07:00", "2006-01-02T15:04:05-07:00",
		"2006-01-02T15:04:05.000Z07:00", "2006-01-02T15:04:05Z07:00", "2006-01-02T15:04:05.000", "2006-01-02T15:04:05",
		"2006-01-02 15:04:05.000-07:00", "2006-01-02 15:04:05-07:00", "2006-01-02 15:04:05", "2006-01-02",
	}
	for _, l := range layouts {
		if t, err := time.Parse(l, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, ValueError.Newf("Invalid isoformat string: %s", Repr_str(s))
}

// ---------------------------------------------------------------------------
// os / pathlib helpers
// ---------------------------------------------------------------------------

// Expanduser is os.path.expanduser / Path.expanduser.
func Expanduser(p string) string {
	if p == "~" || strings.HasPrefix(p, "~/") {
		home, _ := os.UserHomeDir()
		if h := os.Getenv("HOME"); h != "" {
			home = h
		}
		return filepath.Join(home, p[1:])
	}
	return p
}

// Home is Path.home().
func Home() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	h, _ := os.UserHomeDir()
	return h
}

// Getenv is os.environ.get(name, default).
func Getenv(name, def string) string {
	if v, ok := os.LookupEnv(name); ok {
		return v
	}
	return def
}

// Mkdir_p is Path.mkdir(parents=True, exist_ok=True).
func Mkdir_p(p string) error {
	if err := os.MkdirAll(p, 0o755); err != nil {
		return FromGo(err)
	}
	return nil
}

// Read_text is Path.read_text().
func Read_text(p string) (string, error) {
	b, err := os.ReadFile(p)
	if err != nil {
		return "", FromGo(err)
	}
	return string(b), nil
}

// Read_bytes is Path.read_bytes().
func Read_bytes(p string) ([]byte, error) {
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, FromGo(err)
	}
	return b, nil
}

// Write_text is Path.write_text(s).
func Write_text(p string, s string) error {
	if err := os.WriteFile(p, []byte(s), 0o644); err != nil {
		return FromGo(err)
	}
	return nil
}

// Write_bytes is Path.write_bytes(b).
func Write_bytes(p string, b []byte) error {
	if err := os.WriteFile(p, b, 0o644); err != nil {
		return FromGo(err)
	}
	return nil
}

// Exists is Path.exists().
func Exists(p string) bool { _, err := os.Stat(p); return err == nil }

// Is_dir is Path.is_dir().
func Is_dir(p string) bool { st, err := os.Stat(p); return err == nil && st.IsDir() }

// Is_file is Path.is_file().
func Is_file(p string) bool { st, err := os.Stat(p); return err == nil && st.Mode().IsRegular() }
