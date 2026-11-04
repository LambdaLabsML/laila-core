package laila

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// entry/constitution/trust.py -- trust gate for constitutions rebuilt from
// storage or the wire.
//
// A constitution is source that gets executed when an entry is
// materialised. Entries are rebuilt from pool files and from peer replies,
// i.e. from inputs the reading process did not produce. Two gates make
// sure only code the reader could have produced itself ever runs:
//
//   - Simple constitutions (transformation inverse chains) are accepted
//     only when every snippet matches the recovery-code template of a
//     loaded transformation class, or is byte-identical to a snippet this
//     process generated (Is_trusted_backward_code).
//   - Complex constitutions (user builders) carry an HMAC-SHA256 over the
//     canonical ``{code, manifest_global_id}`` pair, keyed by a per-user
//     secret in ``~/.laila/constitution.key`` (created on first use, mode
//     0600; override with LAILA_CONSTITUTION_KEY_FILE). It is deliberately
//     not derived from the data-encryption key and is independent of the
//     default directory, so every process of the same user on a host --
//     Python or Go -- trusts the same code.

const (
	SECRET_ENV_      = "LAILA_CONSTITUTION_KEY_FILE"
	SECRET_FILENAME_ = "constitution.key"
	SECRET_NBYTES_   = 32
)

var (
	trust_lock_    sync.Mutex
	cached_secret_ []byte
)

// Secret_path_ is the location of the per-user secret file.
func Secret_path_() string {
	if override := os.Getenv(SECRET_ENV_); override != "" {
		return py.Expanduser(override)
	}
	return filepath.Join(py.Home(), ".laila", SECRET_FILENAME_)
}

// Reset_constitution_secret_cache_ drops the cached secret (tests that
// switch LAILA_CONSTITUTION_KEY_FILE).
func Reset_constitution_secret_cache_() {
	trust_lock_.Lock()
	cached_secret_ = nil
	trust_lock_.Unlock()
}

// Constitution_secret returns the per-user HMAC secret, creating it on
// first use. Falls back to a process-local random secret (with a logged
// warning) when the home directory is not writable; constitutions written
// in that mode are only readable by the same process.
func Constitution_secret() []byte {
	trust_lock_.Lock()
	defer trust_lock_.Unlock()
	if cached_secret_ != nil {
		return cached_secret_
	}
	path := Secret_path_()
	secret, err := func() ([]byte, error) {
		if py.Exists(path) {
			data, err := os.ReadFile(path)
			if err != nil {
				return nil, err
			}
			data = []byte(strings.TrimSpace(string(data)))
			if len(data) >= 16 {
				return data, nil
			}
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
		fresh := []byte(py.Token_hex(SECRET_NBYTES_))
		tmp := strings.TrimSuffix(path, filepath.Ext(path)) + ".tmp"
		if err := os.WriteFile(tmp, fresh, 0o600); err != nil {
			return nil, err
		}
		_ = os.Chmod(tmp, 0o600)
		if err := os.Rename(tmp, path); err != nil {
			return nil, err
		}
		// Another process may have raced us: honour whatever won.
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		return []byte(strings.TrimSpace(string(data))), nil
	}()
	if err != nil {
		if lg, lerr := Get_logger(); lerr == nil {
			lg.Warning("constitution secret file " + path + " is not usable (" + err.Error() + "); using a " +
				"process-local secret -- complex constitutions written now are only " +
				"readable by this process")
		}
		secret = py.Token_bytes(SECRET_NBYTES_)
	}
	cached_secret_ = secret
	return cached_secret_
}

func compactJSON(v any) []byte {
	f := false
	s, err := py.Json_dumps_opts(v, py.JsonOpts{Sort_keys: true, Item_sep: ",", Key_sep: ":", Ensure_ascii: &f})
	if err != nil {
		panic(err)
	}
	return []byte(s)
}

// Canonical_ is the canonical complex-constitution payload:
// ``json.dumps({"code": code, "manifest_global_id": gid}, sort_keys=True,
// separators=(",", ":"), ensure_ascii=False)``.
func Canonical_(code any, manifest_global_id any) []byte {
	return compactJSON(py.DictOf("code", code, "manifest_global_id", manifest_global_id))
}

// Sign_ is the hex HMAC-SHA256 of payload under the local secret.
func Sign_(payload []byte) string {
	mac := hmac.New(sha256.New, Constitution_secret())
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

// Verify_ is a constant-time check of signature against payload.
func Verify_(payload []byte, signature any) bool {
	s, ok := signature.(string)
	if !ok || s == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(Sign_(payload)), []byte(s)) == 1
}

// Sign_complex is the HMAC-SHA256 (hex) over the canonical
// complex-constitution payload.
func Sign_complex(code any, manifest_global_id any) string {
	return Sign_(Canonical_(code, manifest_global_id))
}

// Verify_complex is the constant-time check of signature against the
// local secret.
func Verify_complex(code any, manifest_global_id any, signature any) bool {
	return Verify_(Canonical_(code, manifest_global_id), signature)
}

// Canonical_codes_ is ``json.dumps(list(codes), separators=(",", ":"),
// ensure_ascii=False)``.
func Canonical_codes_(codes any) []byte {
	f := false
	items := py.NewList()
	if !py.IsNone(codes) {
		items = py.NewList(py.Iter(codes)...)
	}
	s, err := py.Json_dumps_opts(items, py.JsonOpts{Item_sep: ",", Key_sep: ":", Ensure_ascii: &f})
	if err != nil {
		panic(err)
	}
	return []byte(s)
}

// Sign_simple is the HMAC-SHA256 (hex) over a simple constitution's code list.
func Sign_simple(codes any) string { return Sign_(Canonical_codes_(codes)) }

// Check_simple_codes raises ValueError unless codes may run in this
// process: every snippet matches a loaded transformation's recovery-code
// template (or was generated here), or the list as a whole carries a valid
// ``_hmac`` from a writer sharing this user's secret.
func Check_simple_codes(codes any, signature any) error {
	if !py.IsNone(signature) && Verify_(Canonical_codes_(codes), signature) {
		return nil
	}
	if py.IsNone(codes) {
		return nil
	}
	for _, code := range py.Iter(codes) {
		if Is_trusted_backward_code(code) {
			continue
		}
		text, ok := code.(string)
		if !ok {
			text = py.Repr(code)
		}
		head := py.NewList()
		if lines := strings.SplitN(strings.TrimSpace(text), "\n", 2); len(lines) > 0 && strings.TrimSpace(text) != "" {
			head.Append(lines[0])
		}
		return py.ValueError.Newf(
			"untrusted constitution: recovery code does not match any loaded "+
				"transformation template and was not generated by this process (%s)", py.Repr(head))
	}
	return nil
}
