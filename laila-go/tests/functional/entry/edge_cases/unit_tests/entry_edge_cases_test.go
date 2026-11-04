// Port of tests/functional/entry/edge_cases/unit_tests/test_entry_edge_cases.py.
package unit_tests

import (
	"errors"
	"fmt"
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

func data(t *testing.T, e *laila.Entry) any {
	t.Helper()
	return must(e.Data())
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
	if !errors.Is(err, exc) {
		t.Fatalf("expected %s, got %v", exc.Name, err)
	}
}

func constant(data any, kw ...py.Kw) *laila.Entry { return must(laila.Entry_constant(data, kw...)) }
func variable(data any, kw ...py.Kw) *laila.Entry { return must(laila.Entry_variable(data, kw...)) }

func buildSync(in any) *laila.Entry {
	return must(laila.Entry_build_from_dict_sync_(in)).(*laila.Entry)
}

func base64Sequence() *laila.TransformationSequence {
	return must(laila.New_TransformationSequence(py.Kw{"transformations": []any{must(laila.New_Base64())}}))
}

func fromiso(t *testing.T, ts *string) {
	t.Helper()
	if ts == nil {
		t.Fatal("creation_timestamp is None")
	}
	if _, err := py.Fromisoformat(*ts); err != nil {
		t.Fatal(err)
	}
}

// --- TestEntryCreationTimestamp ------------------------------------------

func TestEntryCreationTimestamp(t *testing.T) {
	t.Run("constant_is_stamped", func(t *testing.T) {
		fromiso(t, constant(1).Creation_timestamp())
	})
	t.Run("as_dict_carries_stamp", func(t *testing.T) {
		e := constant(1)
		assertEq(t, must(e.As_dict()).GetDefault("_creation_timestamp", nil), *e.Creation_timestamp())
	})
	t.Run("serialize_roundtrip_preserves_stamp", func(t *testing.T) {
		e := constant(py.DictOf("a", py.NewList(1, 2, 3)))
		blob := must(e.Serialize(laila.Transformation_base64)).(*py.Dict)
		assertEq(t, blob.GetDefault("_creation_timestamp", nil), *e.Creation_timestamp())
		rebuilt := buildSync(must(py.Json_dumps(blob)))
		assertEq(t, *rebuilt.Creation_timestamp(), *e.Creation_timestamp())
		assertEq(t, data(t, rebuilt), py.DictOf("a", py.NewList(1, 2, 3)))
	})
	t.Run("from_dict_without_stamp_falls_back_to_now", func(t *testing.T) {
		e := constant(1)
		blob := must(e.Serialize(laila.Transformation_base64)).(*py.Dict)
		must(blob.Pop("_creation_timestamp"))
		rebuilt := buildSync(blob)
		fromiso(t, rebuilt.Creation_timestamp())
	})
	t.Run("evolve_gets_fresh_stamp", func(t *testing.T) {
		v := variable(1)
		old := "2000-01-01T00:00:00.000+00:00"
		v.Creation_timestamp_ = &old
		v2 := must(v.Evolve(2))
		if *v2.Creation_timestamp() == *v.Creation_timestamp() || !(*v2.Creation_timestamp() > *v.Creation_timestamp()) {
			t.Fatalf("stamp not refreshed: %s vs %s", *v2.Creation_timestamp(), *v.Creation_timestamp())
		}
	})
}

// --- TestEntryConstantEdgeCases ------------------------------------------

func TestEntryConstantEdgeCases(t *testing.T) {
	t.Run("constant_string_data", func(t *testing.T) {
		e := constant("hello")
		assertEq(t, data(t, e), "hello")
		if e.Evolution() != nil {
			t.Fatal("evolution")
		}
	})
	t.Run("constant_dict_data", func(t *testing.T) {
		assertEq(t, data(t, constant(py.DictOf("key", "val"))), py.DictOf("key", "val"))
	})
	t.Run("constant_list_data", func(t *testing.T) {
		assertEq(t, data(t, constant(py.NewList(1, 2, 3))), py.NewList(1, 2, 3))
	})
	t.Run("constant_int_data", func(t *testing.T) {
		assertEq(t, data(t, constant(42)), 42)
	})
	t.Run("constant_float_data", func(t *testing.T) {
		assertEq(t, data(t, constant(3.14)), 3.14)
	})
	t.Run("constant_boolean_data", func(t *testing.T) {
		if data(t, constant(true)) != true {
			t.Fatal("bool identity")
		}
	})
	t.Run("constant_empty_string", func(t *testing.T) {
		assertEq(t, data(t, constant("")), "")
	})
	t.Run("constant_empty_dict", func(t *testing.T) {
		assertEq(t, data(t, constant(py.NewDict())), py.NewDict())
	})
	t.Run("constant_empty_list", func(t *testing.T) {
		assertEq(t, data(t, constant(py.NewList())), py.NewList())
	})
	t.Run("constant_with_nickname_produces_deterministic_uuid", func(t *testing.T) {
		a := constant("x", py.Kw{"nickname": "same-nick"})
		b := constant("y", py.Kw{"nickname": "same-nick"})
		if a.Uuid() != b.Uuid() {
			t.Fatal("uuid differs")
		}
	})
	t.Run("constant_different_nicknames_differ", func(t *testing.T) {
		a := constant("x", py.Kw{"nickname": "nick-a"})
		b := constant("x", py.Kw{"nickname": "nick-b"})
		if a.Uuid() == b.Uuid() {
			t.Fatal("uuid same")
		}
	})
	t.Run("constant_global_id_and_uuid_raises", func(t *testing.T) {
		uid := py.Uuid4().String()
		_, err := laila.Entry_constant("x", py.Kw{"global_id": "LAILA:ENTRY:" + uid, "uuid": uid})
		assertRaises(t, py.RuntimeError, err)
	})
	t.Run("constant_has_no_evolution", func(t *testing.T) {
		if constant("x").Evolution() != nil {
			t.Fatal("evolution")
		}
	})
	t.Run("constant_state_is_ready", func(t *testing.T) {
		assertEq(t, constant("x").State(), laila.EntryState_READY)
	})
}

// --- TestEntryVariableEdgeCases ------------------------------------------

func TestEntryVariableEdgeCases(t *testing.T) {
	t.Run("variable_has_evolution_zero", func(t *testing.T) {
		assertEq(t, *variable("x").Evolution(), 0)
	})
	t.Run("variable_state_is_ready", func(t *testing.T) {
		assertEq(t, variable("x").State(), laila.EntryState_READY)
	})
	t.Run("variable_with_explicit_evolution", func(t *testing.T) {
		assertEq(t, *variable("x", py.Kw{"evolution": 5}).Evolution(), 5)
	})
	t.Run("variable_global_id_and_uuid_raises", func(t *testing.T) {
		uid := py.Uuid4().String()
		_, err := laila.Entry_variable("x", py.Kw{"global_id": fmt.Sprintf("LAILA:ENTRY:%s@evolution=0", uid), "uuid": uid})
		assertRaises(t, py.RuntimeError, err)
	})
	t.Run("variable_constitution_requires_manifest", func(t *testing.T) {
		_, err := laila.Entry_variable(nil, py.Kw{"constitution": "def f(m):\n    return 0\n"})
		assertRaises(t, py.ValueError, err)
	})
}

// --- TestEntryEvolve ------------------------------------------------------

func TestEntryEvolve(t *testing.T) {
	t.Run("evolve_increments_evolution", func(t *testing.T) {
		e := variable("v0")
		assertEq(t, *e.Evolution(), 0)
		e1 := must(e.Evolve("v1"))
		assertEq(t, *e1.Evolution(), 1)
		e2 := must(e1.Evolve("v2"))
		assertEq(t, *e2.Evolution(), 2)
		assertEq(t, *e.Evolution(), 0)
		if e1.Uuid() != e.Uuid() || e2.Uuid() != e.Uuid() {
			t.Fatal("uuid changed")
		}
	})
	t.Run("evolve_updates_data", func(t *testing.T) {
		e := variable("old")
		e2 := must(e.Evolve("new"))
		assertEq(t, data(t, e2), "new")
		assertEq(t, data(t, e), "old")
	})
	t.Run("evolve_constant_raises", func(t *testing.T) {
		_, err := constant("fixed").Evolve("changed")
		assertRaises(t, py.RuntimeError, err)
	})
	// evolve_rejects_constitution_kwarg: ``e.evolve(data=..., constitution=...)``
	// is a TypeError in Python because evolve takes no such keyword. The
	// Go signature ``Evolve(data any)`` enforces this at compile time.
	t.Run("evolve_with_none_data", func(t *testing.T) {
		e := variable("start")
		e2 := must(e.Evolve(nil))
		if data(t, e2) != nil {
			t.Fatal("data should be None")
		}
		assertEq(t, *e2.Evolution(), 1)
		assertEq(t, data(t, e), "start")
		assertEq(t, *e.Evolution(), 0)
	})
}

// --- TestEntrySerialize ---------------------------------------------------

func TestEntrySerialize(t *testing.T) {
	t.Run("serialize_none_transformations_returns_entry", func(t *testing.T) {
		e := constant("x")
		result := must(e.Serialize(nil)).(*laila.Entry)
		// A detached snapshot with the same identity and payload (so an
		// in-memory pool never aliases the live object), not ``e`` itself.
		if result == e {
			t.Fatal("aliased")
		}
		if result.Global_id() != e.Global_id() {
			t.Fatal("global_id")
		}
		assertEq(t, data(t, result), data(t, e))
	})
	t.Run("serialize_with_transformations_returns_dict", func(t *testing.T) {
		result := must(constant("hello").Serialize(base64Sequence())).(*py.Dict)
		if !result.Contains("payload") || !result.Contains("constitution") {
			t.Fatal("missing keys")
		}
		assertEq(t, result.GetDefault("constitution", nil).(*py.Dict).GetDefault("_kind", nil), "simple")
	})
	t.Run("serialize_preserves_global_id_info", func(t *testing.T) {
		e := constant("x", py.Kw{"nickname": "ser-test"})
		result := must(e.Serialize(base64Sequence())).(*py.Dict)
		assertEq(t, result.GetDefault("_uuid", nil), e.Uuid())
	})
}

// --- TestEntryBuild -------------------------------------------------------

func TestEntryBuild(t *testing.T) {
	t.Run("build_from_entry_passthrough", func(t *testing.T) {
		e := constant("x")
		if buildSync(e) != e {
			t.Fatal("not passthrough")
		}
	})
	t.Run("build_from_invalid_string_raises", func(t *testing.T) {
		_, err := laila.Entry_build_from_dict_sync_("not json")
		assertRaises(t, py.ValueError, err)
	})
	t.Run("build_from_empty_dict_raises", func(t *testing.T) {
		_, err := laila.Entry_build_from_dict_sync_(py.NewDict())
		assertRaises(t, py.KeyError, err)
	})
	t.Run("build_invalid_state_raises", func(t *testing.T) {
		d := py.DictOf(
			"_uuid", "00000000-0000-0000-0000-000000000000",
			"_evolution", nil,
			"_scopes", py.NewList("ENTRY"),
			"_state", "NONEXISTENT_STATE",
			"payload", nil,
			"constitution", py.DictOf("_kind", "simple", "codes", py.NewList()),
		)
		_, err := laila.Entry_build_from_dict_sync_(d)
		assertRaises(t, py.KeyError, err)
	})
	t.Run("serialize_build_round_trip", func(t *testing.T) {
		serialized := must(constant("round-trip-data").Serialize(base64Sequence()))
		assertEq(t, data(t, buildSync(serialized)), "round-trip-data")
	})
	t.Run("serialize_non_ready_raises", func(t *testing.T) {
		e := constant("x")
		e.State_ = laila.EntryState_STAGED
		_, err := e.Serialize(base64Sequence())
		assertRaises(t, py.RuntimeError, err)
	})
}

// --- TestEntryGlobalId ----------------------------------------------------

func TestEntryGlobalId(t *testing.T) {
	t.Run("constant_global_id_has_no_evolution_suffix", func(t *testing.T) {
		if strings.Contains(constant("x").Global_id(), "@") {
			t.Fatal("has @")
		}
	})
	t.Run("variable_global_id_has_evolution_suffix", func(t *testing.T) {
		if !strings.HasSuffix(variable("x").Global_id(), "@evolution=0") {
			t.Fatal("missing suffix")
		}
	})
	t.Run("global_id_is_valid_laila_resource", func(t *testing.T) {
		if !laila.Is_laila_resource(constant("x").Global_id()) {
			t.Fatal("not a laila resource")
		}
	})
	t.Run("str_is_global_id", func(t *testing.T) {
		e := constant("x")
		assertEq(t, py.Str(e), e.Global_id())
	})
	t.Run("repr_is_global_id", func(t *testing.T) {
		e := constant("x")
		assertEq(t, py.Repr(e), e.Global_id())
	})
}
