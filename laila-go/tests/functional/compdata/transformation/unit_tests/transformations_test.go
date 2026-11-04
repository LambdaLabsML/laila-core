// Port of tests/functional/compdata/transformation/unit_tests/test_transformations.py.
//
// ``exec(op.backward_code)`` checks become Compile_backward_code_ checks:
// the port recognises its own recovery snippets and compiles them to the
// native inverse instead of executing Python.
package unit_tests

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func assertEq(t *testing.T, got, want any) {
	t.Helper()
	if !py.Eq(got, want) {
		t.Fatalf("got %s, want %s", py.Repr(got), py.Repr(want))
	}
}

func assertRaises(t *testing.T, exc *py.Type, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s, got nil", exc.Name)
	}
	if exc != nil && !errors.Is(err, exc) {
		t.Fatalf("expected %s, got %v", exc.Name, err)
	}
}

// compiledBackward is the port's ``exec(backward_code)`` + "exactly one
// callable" check.
func compiledBackward(t *testing.T, code string) laila.Backward_fn {
	t.Helper()
	fn, recognised, err := laila.Compile_backward_code_(code)
	if err != nil {
		t.Fatal(err)
	}
	if !recognised {
		t.Fatalf("backward_code not recognised:\n%s", code)
	}
	return fn
}

func newSeq(ts ...any) *laila.TransformationSequence {
	return must(laila.New_TransformationSequence(py.Kw{"transformations": ts}))
}

func urandom(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// ── Base64 ──────────────────────────────────────────────────

func TestBase64(t *testing.T) {
	op := must(laila.New_Base64())
	t.Run("forward_known_bytes", func(t *testing.T) { assertEq(t, must(op.Forward([]byte("hello"))), "aGVsbG8=") })
	t.Run("backward_known_string", func(t *testing.T) { assertEq(t, must(op.Backward("aGVsbG8=")), []byte("hello")) })
	t.Run("round_trip", func(t *testing.T) {
		data := []byte("\x00\xffbinary")
		assertEq(t, must(op.Backward(must(op.Forward(data)))), data)
	})
	t.Run("forward_non_bytes_raises", func(t *testing.T) {
		_, err := op.Forward("not-bytes")
		assertRaises(t, py.TypeError, err)
	})
	t.Run("forward_empty_bytes", func(t *testing.T) { assertEq(t, must(op.Forward([]byte{})), "") })
	t.Run("backward_empty_string", func(t *testing.T) { assertEq(t, must(op.Backward("")), []byte{}) })
	// bytearray / memoryview are modelled as a mutable ``*[]byte`` view in
	// the port's bytes model.
	t.Run("forward_bytearray", func(t *testing.T) {
		buf := []byte("hello")
		assertEq(t, must(op.Forward(&buf)), "aGVsbG8=")
	})
	t.Run("forward_memoryview", func(t *testing.T) {
		buf := []byte("hello")
		assertEq(t, must(op.Forward(&buf)), "aGVsbG8=")
	})
	t.Run("backward_invalid_base64_raises", func(t *testing.T) {
		_, err := op.Backward("!!!not-base64!!!")
		assertRaises(t, py.ValueError, err)
	})
	t.Run("backward_bytes_input", func(t *testing.T) { assertEq(t, must(op.Backward([]byte("aGVsbG8="))), []byte("hello")) })
	t.Run("round_trip_large_payload", func(t *testing.T) {
		data := urandom(100_000)
		assertEq(t, must(op.Backward(must(op.Forward(data)))), data)
	})
	t.Run("forward_int_raises", func(t *testing.T) {
		_, err := op.Forward(42)
		assertRaises(t, py.TypeError, err)
	})
	t.Run("forward_none_raises", func(t *testing.T) {
		_, err := op.Forward(nil)
		assertRaises(t, py.TypeError, err)
	})
	t.Run("backward_code_is_executable", func(t *testing.T) {
		fn := compiledBackward(t, op.Base_().Backward_code)
		assertEq(t, must(fn("aGVsbG8=")), []byte("hello"))
	})
}

// ── Zlib ────────────────────────────────────────────────────

func TestZlib(t *testing.T) {
	op := must(laila.New_Zlib())
	t.Run("round_trip", func(t *testing.T) {
		text := "unicode: \u2603 and ascii"
		assertEq(t, must(op.Backward(must(op.Forward(text)))), text)
	})
	t.Run("forward_non_string_raises", func(t *testing.T) {
		_, err := op.Forward([]byte("bytes"))
		assertRaises(t, py.TypeError, err)
	})
	t.Run("forward_empty_string", func(t *testing.T) {
		assertEq(t, must(op.Backward(must(op.Forward("")))), "")
	})
	t.Run("round_trip_large_text", func(t *testing.T) {
		text := strings.Repeat("x", 100_000)
		assertEq(t, must(op.Backward(must(op.Forward(text)))), text)
	})
	t.Run("forward_none_raises", func(t *testing.T) {
		_, err := op.Forward(nil)
		assertRaises(t, py.TypeError, err)
	})
	t.Run("backward_invalid_base64_raises", func(t *testing.T) {
		_, err := op.Backward("!!!not-valid!!!")
		assertRaises(t, nil, err)
	})
	t.Run("backward_valid_base64_but_not_zlib_raises", func(t *testing.T) {
		fake := base64.StdEncoding.EncodeToString([]byte("not-zlib-data"))
		_, err := op.Backward(fake)
		assertRaises(t, nil, err)
	})
	t.Run("backward_code_is_executable", func(t *testing.T) {
		encoded := must(op.Forward("test-recovery"))
		fn := compiledBackward(t, op.Base_().Backward_code)
		assertEq(t, must(fn(encoded)), "test-recovery")
	})
	t.Run("forward_int_raises", func(t *testing.T) {
		_, err := op.Forward(123)
		assertRaises(t, py.TypeError, err)
	})
}

// ── TransformationSequence ──────────────────────────────────

func TestTransformationSequence(t *testing.T) {
	t.Run("empty_identity", func(t *testing.T) {
		payload := py.DictOf("a", 1, "b", py.NewList(2, 3))
		out, inv, err := newSeq().Forward(payload)
		if err != nil {
			t.Fatal(err)
		}
		if out != any(payload) {
			t.Fatal("identity must return the same object")
		}
		assertEq(t, len(inv), 0)
	})
	t.Run("single_base64", func(t *testing.T) {
		raw := []byte("pipeline")
		out, inv, err := newSeq(must(laila.New_Base64())).Forward(raw)
		if err != nil {
			t.Fatal(err)
		}
		assertEq(t, out, must(must(laila.New_Base64()).Forward(raw)))
		if len(inv) != 1 || !strings.Contains(inv[0], "backward") {
			t.Fatal(inv)
		}
	})
	t.Run("append_single", func(t *testing.T) {
		seq := newSeq()
		b64 := must(laila.New_Base64())
		must(seq.Append(b64))
		if len(seq.Transformations) != 1 || seq.Transformations[0] != laila.Data_transformer(b64) {
			t.Fatal("append")
		}
	})
	t.Run("append_list", func(t *testing.T) {
		seq := newSeq()
		must(seq.Append([]any{must(laila.New_Base64()), must(laila.New_Zlib())}))
		assertEq(t, len(seq.Transformations), 2)
	})
	t.Run("repr_empty", func(t *testing.T) { assertEq(t, py.Repr(newSeq()), "TransformationSequence(identity)") })
	t.Run("repr_single", func(t *testing.T) {
		assertEq(t, py.Repr(newSeq(must(laila.New_Base64()))), "TransformationSequence(base64)")
	})
	t.Run("repr_chain", func(t *testing.T) {
		assertEq(t, py.Repr(newSeq(must(laila.New_Base64()), must(laila.New_Base64()))),
			"TransformationSequence(base64 -> base64)")
	})
	t.Run("append_none_raises", func(t *testing.T) {
		_, err := newSeq().Append(nil)
		assertRaises(t, py.TypeError, err)
	})
	t.Run("append_string_raises", func(t *testing.T) {
		_, err := newSeq().Append("not-a-transformation")
		assertRaises(t, py.TypeError, err)
	})
	t.Run("append_tuple_raises", func(t *testing.T) {
		_, err := newSeq().Append(py.Tuple{must(laila.New_Base64())})
		assertRaises(t, py.TypeError, err)
	})
	t.Run("append_empty_list_no_change", func(t *testing.T) {
		seq := newSeq()
		must(seq.Append([]any{}))
		assertEq(t, len(seq.Transformations), 0)
	})
	t.Run("append_mixed_list_raises", func(t *testing.T) {
		_, err := newSeq().Append([]any{must(laila.New_Base64()), "bad"})
		assertRaises(t, py.TypeError, err)
	})
	t.Run("append_returns_self", func(t *testing.T) {
		seq := newSeq()
		if must(seq.Append(must(laila.New_Base64()))) != seq {
			t.Fatal("append must return self")
		}
	})
	t.Run("iter", func(t *testing.T) {
		b64, z := must(laila.New_Base64()), must(laila.New_Zlib())
		got := newSeq(b64, z).Iter()
		if len(got) != 2 || got[0] != laila.Data_transformer(b64) || got[1] != laila.Data_transformer(z) {
			t.Fatal("iter")
		}
	})
	t.Run("forward_none_with_empty_seq", func(t *testing.T) {
		out, _, err := newSeq().Forward(nil)
		if err != nil || out != nil {
			t.Fatal(out, err)
		}
	})
	t.Run("forward_none_with_base64_raises", func(t *testing.T) {
		_, _, err := newSeq(must(laila.New_Base64())).Forward(nil)
		assertRaises(t, py.TypeError, err)
	})
	t.Run("inverse_codes_reversed", func(t *testing.T) {
		_, inv, err := newSeq(must(laila.New_Base64()), must(laila.New_Zlib())).Forward([]byte("test"))
		if err != nil {
			t.Fatal(err)
		}
		if len(inv) != 2 || !strings.Contains(strings.ToLower(inv[0]), "zlib") || !strings.Contains(strings.ToLower(inv[1]), "base64") {
			t.Fatal(inv)
		}
	})
	t.Run("chained_round_trip_base64_then_zlib", func(t *testing.T) {
		data := []byte("round-trip-chain")
		b64, z := must(laila.New_Base64()), must(laila.New_Zlib())
		out, _, err := newSeq(b64, z).Forward(data)
		if err != nil {
			t.Fatal(err)
		}
		current := must(z.Backward(out))
		current = must(b64.Backward(current))
		assertEq(t, current, data)
	})
}

// ── FernetEncryption ────────────────────────────────────────

func TestFernetEncryption(t *testing.T) {
	setUp := func(t *testing.T) *laila.FernetEncryption {
		t.Helper()
		key := py.Fernet_generate_key()
		// The recovery code resolves the key from ``laila.encryption_key``
		// (never from the stored data), so the "reader" side sets it too.
		previous := laila.Encryption_key()
		laila.Set_encryption_key(key)
		t.Cleanup(func() { laila.Set_encryption_key(previous) })
		return must(laila.New_FernetEncryption(py.Kw{"key": key}))
	}
	t.Run("round_trip", func(t *testing.T) {
		op := setUp(t)
		token := must(op.Forward("secret message"))
		assertEq(t, must(op.Backward(token)), "secret message")
	})
	t.Run("wrong_key_fails", func(t *testing.T) {
		op := setUp(t)
		other := must(laila.New_FernetEncryption(py.Kw{"key": py.Fernet_generate_key()}))
		token := must(op.Forward("payload"))
		_, err := other.Backward(token)
		assertRaises(t, py.ValueError, err)
	})
	t.Run("forward_empty_string", func(t *testing.T) {
		op := setUp(t)
		assertEq(t, must(op.Backward(must(op.Forward("")))), "")
	})
	t.Run("forward_non_string_raises", func(t *testing.T) {
		_, err := setUp(t).Forward([]byte("bytes"))
		assertRaises(t, py.TypeError, err)
	})
	t.Run("forward_none_raises", func(t *testing.T) {
		_, err := setUp(t).Forward(nil)
		assertRaises(t, py.TypeError, err)
	})
	t.Run("backward_non_string_raises", func(t *testing.T) {
		_, err := setUp(t).Backward(42)
		assertRaises(t, py.TypeError, err)
	})
	t.Run("backward_garbage_token_raises", func(t *testing.T) {
		_, err := setUp(t).Backward("definitely-not-a-fernet-token")
		assertRaises(t, py.ValueError, err)
	})
	t.Run("key_as_string", func(t *testing.T) {
		setUp(t)
		op := must(laila.New_FernetEncryption(py.Kw{"key": string(py.Fernet_generate_key())}))
		assertEq(t, must(op.Backward(must(op.Forward("test")))), "test")
	})
	t.Run("round_trip_unicode", func(t *testing.T) {
		op := setUp(t)
		text := "\u2603 snowman \u2764 heart"
		assertEq(t, must(op.Backward(must(op.Forward(text)))), text)
	})
	t.Run("different_encryptions_produce_different_tokens", func(t *testing.T) {
		op := setUp(t)
		if must(op.Forward("same")) == must(op.Forward("same")) {
			t.Fatal("tokens must differ")
		}
	})
	t.Run("backward_code_is_executable", func(t *testing.T) {
		op := setUp(t)
		token := must(op.Forward("recovery-test"))
		fn := compiledBackward(t, op.Base_().Backward_code)
		assertEq(t, must(fn(token)), "recovery-test")
	})
}
