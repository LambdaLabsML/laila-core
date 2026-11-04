// Port of tests/functional/entry/edge_cases/unit_tests/test_fidelity_and_trust.py
// -- the cases that do not need a policy / pool. The memorize / remember
// round trips (TestPayloadFidelity._rt, TestEncryption key rotation,
// TestReadDoesNotAliasStoredRecord, TestConstitutionTrustGate via pools,
// TestManifestFixes) are ported with the memory module (Phase 4).
package unit_tests

import (
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

func TestPayloadFidelityLocal(t *testing.T) {
	t.Run("plain_containers_still_msgpack", func(t *testing.T) {
		_, code := serializeCD(t, py.DictOf("a", py.NewList(1, 2)))
		assertEq(t, code, must(laila.New_MsgpackSerializer()).Base_().Backward_code)
		_, code = serializeCD(t, py.DictOf("a", py.Tuple{1, 2}))
		assertEq(t, code, must(laila.New_PickleSerializer()).Base_().Backward_code)
	})
	t.Run("nested_tuples_and_tuple_keys_survive_serialize", func(t *testing.T) {
		for _, val := range []any{
			py.DictOf("k", py.Tuple{1, 2}),
			py.NewList(py.DictOf("b", py.Tuple{1, 2})),
			py.NewList(py.NewList(py.Tuple{1})),
			py.DictOf(py.Tuple{1, 2}, "a"),
			py.DictOf("o", py.DictOf(py.Tuple{0, 0}, 1)),
		} {
			e := constant(val)
			blob := must(e.Serialize(laila.Transformation_base64))
			got := data(t, buildSync(blob))
			assertEq(t, got, val)
			if py.Pytypename(got) != py.Pytypename(val) {
				t.Fatalf("type drift: %s != %s", py.Pytypename(got), py.Pytypename(val))
			}
		}
	})
}

func serializeCD(t *testing.T, v any) (any, string) {
	t.Helper()
	payload, code, err := must(laila.New_ComputationalData(v)).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	return payload, code
}

func TestEncryptionLocal(t *testing.T) {
	t.Run("explicit_key_is_pinned", func(t *testing.T) {
		pinned, other := py.Fernet_generate_key(), py.Fernet_generate_key()
		previous := laila.Encryption_key()
		laila.Set_encryption_key(other)
		t.Cleanup(func() { laila.Set_encryption_key(previous) })
		f := must(laila.New_FernetEncryption(py.Kw{"key": pinned}))
		must(f.Forward("x"))
		assertEq(t, f.Key, pinned)
	})
	t.Run("fernet_first_pipeline_fails_loudly", func(t *testing.T) {
		previous := laila.Encryption_key()
		laila.Set_encryption_key(py.Fernet_generate_key())
		t.Cleanup(func() { laila.Set_encryption_key(previous) })
		seq := must(laila.New_TransformationSequence(py.Kw{
			"transformations": []any{must(laila.New_FernetEncryption())},
		}))
		_, err := constant(1).Serialize(seq)
		assertRaises(t, py.TypeError, err)
	})
}

func TestConstitutionTrustGateLocal(t *testing.T) {
	t.Run("tampered_simple_constitution_is_refused", func(t *testing.T) {
		e := constant(py.DictOf("x", 1))
		rec := must(e.Serialize(laila.Transformation_base64)).(*py.Dict)
		rec.Set("constitution", py.DictOf(
			"_kind", "simple",
			"codes", py.NewList("def backward(inp):\n    import os\n    os.environ['LAILA_TRUST_GATE'] = '1'\n    return inp\n"),
		))
		_, err := laila.Entry_build_from_dict_sync_(rec)
		assertRaises(t, py.ValueError, err)
	})
	t.Run("complex_constitution_round_trips_when_signed", func(t *testing.T) {
		c := must(laila.New_ComplexConstitution(py.Kw{
			"code": "def build(m):\n    return m.realized['a'].data * 2\n",
			"manifest_global_id": "LAILA:MANIFEST:00000000-0000-4000-8000-000000000001",
		}))
		e := must(laila.New_Entry(py.Kw{"constitution": c}))
		assertEq(t, e.State(), laila.EntryState_STAGED)
		back := must(laila.Entry_from_dict(must(e.As_dict())))
		assertEq(t, back.State(), laila.EntryState_STAGED)
		cc, ok := back.Constitution().(*laila.ComplexConstitution)
		if !ok {
			t.Fatalf("constitution is %T", back.Constitution())
		}
		assertEq(t, *cc.Code(), *c.Code())
		assertEq(t, *cc.Manifest_global_id(), *c.Manifest_global_id())
	})
	t.Run("complex_constitution_without_signature_is_refused", func(t *testing.T) {
		e := constant(1)
		rec := must(e.Serialize(laila.Transformation_base64)).(*py.Dict)
		rec.Set("constitution", py.DictOf(
			"_kind", "complex",
			"code", "def build(m):\n    import os\n    return 1\n",
			"manifest_global_id", nil,
		))
		rec.Set("_state", "STAGED")
		_, err := laila.Entry_build_from_dict_sync_(rec)
		assertRaises(t, py.ValueError, err)
	})
}
