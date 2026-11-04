// Port of tests/functional/entry/unit_tests/test_entry.py.
package unit_tests

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

func uuid4() string { return py.Uuid4().String() }

// must unwraps (v, err), failing the test on error (via panic so it can
// be used inline in expressions).
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

func assertReady(t *testing.T, e *laila.Entry) {
	t.Helper()
	if e.State() != laila.EntryState_READY {
		t.Fatalf("state = %v, want READY", e.State())
	}
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

func buildSync(t *testing.T, in any) *laila.Entry {
	t.Helper()
	out := must(laila.Entry_build_from_dict_sync_(in))
	return out.(*laila.Entry)
}

func Test001ConstantReadyAndData(t *testing.T) {
	e := must(laila.Constant(py.NewList(1, 2, 3)))
	assertReady(t, e)
	assertEq(t, data(t, e), py.NewList(1, 2, 3))
	if e.Evolution() != nil {
		t.Fatal("evolution should be None")
	}
}

func Test002VariableReadyAndData(t *testing.T) {
	e := must(laila.Variable(py.Kw{"data": py.DictOf("a", 1)}))
	assertReady(t, e)
	assertEq(t, data(t, e), py.DictOf("a", 1))
}

func Test003EntryAcceptsPayloadKey(t *testing.T) {
	e := must(laila.New_Entry(py.Kw{"payload": py.NewList(9, 8, 7), "state": laila.EntryState_READY}))
	assertReady(t, e)
	assertEq(t, data(t, e), py.NewList(9, 8, 7))
}

func Test004EntryAcceptsDataKey(t *testing.T) {
	e := must(laila.New_Entry(py.Kw{"data": py.NewList(4, 5, 6), "state": laila.EntryState_READY}))
	assertReady(t, e)
	assertEq(t, data(t, e), py.NewList(4, 5, 6))
}

func Test005ConstantRejectsUuidAndGlobalIdTogether(t *testing.T) {
	gid := fmt.Sprintf("LAILA:ENTRY:%s", uuid4())
	_, err := laila.Entry_constant(1, py.Kw{"uuid": uuid4(), "global_id": gid})
	assertRaises(t, py.RuntimeError, err)
}

func Test006ConstantRejectsGlobalIdWithEvolution(t *testing.T) {
	gid := fmt.Sprintf("LAILA:ENTRY:%s@evolution=1", uuid4())
	_, err := laila.Entry_constant(1, py.Kw{"global_id": gid})
	assertRaises(t, py.RuntimeError, err)
}

func Test007ConstantFromGlobalIdUsesUuidAndNoEvolution(t *testing.T) {
	raw := uuid4()
	e := must(laila.Entry_constant(123, py.Kw{"global_id": "LAILA:ENTRY:" + raw}))
	if e.Uuid() != raw {
		t.Fatalf("uuid %s != %s", e.Uuid(), raw)
	}
	if e.Evolution() != nil {
		t.Fatal("evolution should be None")
	}
}

func Test008VariableRejectsGlobalIdWithUuidOrEvolution(t *testing.T) {
	gid := fmt.Sprintf("LAILA:ENTRY:%s", uuid4())
	_, err := laila.Entry_variable(1, py.Kw{"global_id": gid, "uuid": uuid4()})
	assertRaises(t, py.RuntimeError, err)
	_, err = laila.Entry_variable(1, py.Kw{"global_id": gid, "evolution": 2})
	assertRaises(t, py.RuntimeError, err)
}

func Test009VariableConstitutionRequiresManifest(t *testing.T) {
	_, err := laila.Entry_variable(nil, py.Kw{"constitution": "def f(m):\n    return 0\n"})
	assertRaises(t, py.ValueError, err)
}

func Test010EvolveRejectsConstant(t *testing.T) {
	e := must(laila.Entry_constant(py.DictOf("x", 1)))
	_, err := e.Evolve(py.DictOf("x", 2))
	assertRaises(t, py.RuntimeError, err)
}

func Test013EvolveOnEntryWithEvolutionIncrementsEvolution(t *testing.T) {
	e := must(laila.New_Entry(py.Kw{"data": py.DictOf("x", 1), "evolution": 0, "state": laila.EntryState_READY}))
	out := must(e.Evolve(py.DictOf("x", 2)))
	if out == e {
		t.Fatal("evolve must return a new entry")
	}
	if out.Uuid() != e.Uuid() {
		t.Fatal("uuid changed")
	}
	assertEq(t, *out.Evolution(), 1)
	assertEq(t, data(t, out), py.DictOf("x", 2))
	assertReady(t, out)
	assertEq(t, *e.Evolution(), 0)
	assertEq(t, data(t, e), py.DictOf("x", 1))
}

func Test014GlobalIdShapeAndScopes(t *testing.T) {
	e := must(laila.Entry_constant(1))
	if !strings.HasPrefix(e.Global_id(), "LAILA:ENTRY:") {
		t.Fatal(e.Global_id())
	}
	if strings.Contains(e.Global_id(), "@") {
		t.Fatal(e.Global_id())
	}
	assertEq(t, e.Scopes(), []string{"ENTRY"})
}

func Test015StrAndReprReturnGlobalId(t *testing.T) {
	e := must(laila.Entry_constant(1))
	if py.Str(e) != e.Global_id() || py.Repr(e) != e.Global_id() {
		t.Fatal("str/repr mismatch")
	}
}

func Test016SerializeNoneProtocolReturnsSnapshot(t *testing.T) {
	e := must(laila.Entry_constant(py.NewList(1, 2, 3)))
	out := must(e.Serialize(nil)).(*laila.Entry)
	if out == e {
		t.Fatal("snapshot must be a distinct object")
	}
	if out.Global_id() != e.Global_id() {
		t.Fatal("global_id mismatch")
	}
	assertEq(t, data(t, out), py.NewList(1, 2, 3))
	// The payload is deep-copied: mutating the live entry does not leak.
	data(t, e).(*py.List).Append(4)
	assertEq(t, data(t, out), py.NewList(1, 2, 3))
}

func Test017BuildEntryIsIdentity(t *testing.T) {
	e := must(laila.Entry_constant(py.NewList(1)))
	if buildSync(t, e) != e {
		t.Fatal("not identity")
	}
}

func Test018BuildInvalidJsonRaises(t *testing.T) {
	_, err := laila.Entry_build_from_dict_sync_("{not valid json")
	assertRaises(t, py.ValueError, err)
}

func Test019BuildFromDictReconstructsEntry(t *testing.T) {
	payload := py.DictOf(
		"_uuid", uuid4(),
		"_evolution", 3,
		"_scopes", py.NewList("ENTRY"),
		"_state", "READY",
		"payload", nil,
		"constitution", py.DictOf("_kind", "simple", "codes", py.NewList()),
	)
	e := buildSync(t, payload)
	assertEq(t, *e.Evolution(), 3)
	assertReady(t, e)
	if data(t, e) != nil {
		t.Fatal("data should be None")
	}
}

func Test020BuildFromJsonDictString(t *testing.T) {
	payload := py.DictOf(
		"_uuid", uuid4(),
		"_evolution", nil,
		"_scopes", py.NewList("ENTRY"),
		"_state", "READY",
		"payload", nil,
		"constitution", py.DictOf("_kind", "simple", "codes", py.NewList()),
	)
	e := buildSync(t, must(py.Json_dumps(payload)))
	if e.Evolution() != nil {
		t.Fatal("evolution should be None")
	}
}

func Test021RoundtripWithTransformationBase64(t *testing.T) {
	original := py.DictOf("a", py.NewList(1, 2, 3), "b", py.DictOf("nested", true))
	e := must(laila.Entry_constant(original))
	serialized := must(e.Serialize(laila.Transformation_base64))
	recovered := buildSync(t, serialized)
	assertEq(t, data(t, recovered), original)
	assertReady(t, recovered)
}

func Test022RoundtripWithTransformationBase64Compression(t *testing.T) {
	nums := make([]any, 100)
	for i := range nums {
		nums[i] = i
	}
	original := py.DictOf(
		"text", strings.Repeat("abc123", 100),
		"nums", py.NewList(nums...),
		"meta", py.DictOf("ok", true),
	)
	e := must(laila.Entry_constant(original))
	serialized := must(e.Serialize(laila.Transformation_base64_compression))
	recovered := buildSync(t, serialized)
	assertEq(t, data(t, recovered), original)
	assertReady(t, recovered)
}

func Test023ConstantNicknameIsDeterministic(t *testing.T) {
	nickname := "unit:test:constant:nickname"
	e1 := must(laila.Entry_constant(py.DictOf("v", 1), py.Kw{"nickname": nickname}))
	e2 := must(laila.Entry_constant(py.DictOf("v", 2), py.Kw{"nickname": nickname}))
	expected := laila.Generate_uuid_from_nickname(nickname)
	if e1.Uuid() != expected || e2.Uuid() != expected || e1.Global_id() != e2.Global_id() {
		t.Fatal("nickname identity not deterministic")
	}
	if e1.Evolution() != nil || e2.Evolution() != nil {
		t.Fatal("constants have no evolution")
	}
}

func Test024ConstantNicknameOverridesUuid(t *testing.T) {
	nickname := "unit:test:constant:override"
	explicit := uuid4()
	e := must(laila.Entry_constant(7, py.Kw{"uuid": explicit, "nickname": nickname}))
	expected := laila.Generate_uuid_from_nickname(nickname)
	if explicit == expected || e.Uuid() != expected || e.Evolution() != nil {
		t.Fatal("nickname should override uuid")
	}
}

func Test025VariableNicknameOverridesUuidAndSetsEvolution(t *testing.T) {
	nickname := "unit:test:variable:override"
	explicit := uuid4()
	e := must(laila.Entry_variable(py.DictOf("x", 1), py.Kw{"uuid": explicit, "nickname": nickname}))
	expected := laila.Generate_uuid_from_nickname(nickname)
	if explicit == expected || e.Uuid() != expected {
		t.Fatal("nickname should override uuid")
	}
	assertEq(t, *e.Evolution(), 0)
}

func Test026EntryInitNicknameOverridesUuid(t *testing.T) {
	nickname := "unit:test:entry:init:nickname"
	explicit := uuid4()
	e := must(laila.New_Entry(py.Kw{
		"data": py.DictOf("payload", true), "uuid": explicit, "nickname": nickname, "state": laila.EntryState_READY,
	}))
	expected := laila.Generate_uuid_from_nickname(nickname)
	if explicit == expected || e.Uuid() != expected {
		t.Fatal("nickname should override uuid")
	}
	assertReady(t, e)
}

func Test027FromDictValidatesIdentityFields(t *testing.T) {
	// F-16: identity fields coming from storage are validated.
	e := must(laila.Entry_constant(py.DictOf("x", 1)))
	good := must(e.As_dict())
	if must(laila.Entry_from_dict(good)).Uuid() != e.Uuid() {
		t.Fatal("uuid mismatch")
	}
	badUuid := good.Copy()
	badUuid.Set("_uuid", "not-a-uuid")
	_, err := laila.Entry_from_dict(badUuid)
	assertRaises(t, py.ValueError, err)
	for _, bad := range []any{-1, "3", true, 1.5} {
		d := good.Copy()
		d.Set("_evolution", bad)
		_, err := laila.Entry_from_dict(d)
		assertRaises(t, py.ValueError, err)
	}
	v := must(laila.Entry_variable(py.NewList(1)))
	assertEq(t, *must(laila.Entry_from_dict(must(v.As_dict()))).Evolution(), 0)
}
