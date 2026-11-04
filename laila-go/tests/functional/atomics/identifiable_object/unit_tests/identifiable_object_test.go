// Port of tests/functional/atomics/identifiable_object/unit_tests/
// test_identifiable_object.py.
package unit_tests

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

func newIdentifiable(t *testing.T, kw ...py.Kw) *laila.LAILA_IDENTIFIABLE_OBJECT_ {
	t.Helper()
	o, err := laila.New_LAILA_IDENTIFIABLE_OBJECT_(kw...)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func uuid4() string { return py.Uuid4().String() }

func mustParseIso(t *testing.T, ts *string) {
	t.Helper()
	if ts == nil {
		t.Fatal("creation_timestamp is None")
	}
	if _, err := py.Fromisoformat(*ts); err != nil {
		t.Fatalf("fromisoformat(%q): %v", *ts, err)
	}
}

func process(t *testing.T, gid any) py.Kw {
	t.Helper()
	parsed, err := laila.Process_global_id(gid)
	if err != nil {
		t.Fatalf("process_global_id(%v): %v", gid, err)
	}
	return parsed
}

func resolve(t *testing.T, ref any, kw ...py.Kw) string {
	t.Helper()
	gid, err := laila.Resolve_global_id(nil, ref, kw...)
	if err != nil {
		t.Fatalf("resolve_global_id(%v): %v", ref, err)
	}
	return gid
}

func TestCreationTimestamp(t *testing.T) {
	t.Run("root_object_is_stamped_with_iso_utc", func(t *testing.T) {
		obj, err := laila.New_LAILA_OBJECT_()
		if err != nil {
			t.Fatal(err)
		}
		mustParseIso(t, obj.Creation_timestamp())
		parsed, _ := py.Fromisoformat(*obj.Creation_timestamp())
		if _, off := parsed.Zone(); off != 0 {
			t.Error("not UTC")
		}
	})
	t.Run("identifiable_object_inherits_stamp", func(t *testing.T) {
		if !laila.LAILA_IDENTIFIABLE_OBJECT_class.Issubclass(laila.LAILA_OBJECT_class) {
			t.Error("issubclass")
		}
		mustParseIso(t, newIdentifiable(t).Creation_timestamp())
	})
	t.Run("sequential_construction_is_non_decreasing", func(t *testing.T) {
		a, b := newIdentifiable(t), newIdentifiable(t)
		if *a.Creation_timestamp() > *b.Creation_timestamp() {
			t.Error("timestamps decreased")
		}
	})
	t.Run("stamp_is_read_only", func(t *testing.T) {
		obj := newIdentifiable(t)
		if err := py.Setattr(obj, "creation_timestamp", "2000-01-01T00:00:00.000+00:00"); !errors.Is(err, py.AttributeError) {
			t.Errorf("want AttributeError, got %v", err)
		}
	})
	t.Run("identity_still_applied_through_atomic_diamond", func(t *testing.T) {
		uid := uuid4()
		obj, err := laila.New_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_(py.Kw{"uuid": uid, "evolution": 3})
		if err != nil {
			t.Fatal(err)
		}
		if obj.Uuid() != uid || obj.Evolution() == nil || *obj.Evolution() != 3 {
			t.Errorf("uuid=%s evolution=%v", obj.Uuid(), obj.Evolution())
		}
		mustParseIso(t, obj.Creation_timestamp())
	})
	t.Run("atomic_types_are_stamped", func(t *testing.T) {
		objs := map[string]interface{ Creation_timestamp() *string }{}
		i, _ := laila.New_AtomicInt()
		objs["AtomicInt"] = i
		d, _ := laila.New_AtomicDict()
		objs["AtomicDict"] = d
		l, _ := laila.New_AtomicList()
		objs["AtomicList"] = l
		f, _ := laila.New_AtomicFlag()
		objs["AtomicFlag"] = f
		s, _ := laila.New_AtomicStr()
		objs["AtomicStr"] = s
		objs["AtomicDotMap"] = laila.New_AtomicDotMap()
		for name, obj := range objs {
			if !py.Isinstance(obj, laila.LAILA_OBJECT_class) {
				t.Errorf("%s is not a _LAILA_OBJECT", name)
			}
			mustParseIso(t, obj.Creation_timestamp())
		}
	})
	t.Run("atomic_dotmap_stamp_not_leaked_into_data", func(t *testing.T) {
		m := laila.New_AtomicDotMap()
		m.Set("foo", 1)
		if !py.Eq(m.To_dict(), py.DictOf("foo", 1)) {
			t.Errorf("to_dict = %s", py.Repr(m.To_dict()))
		}
		for _, k := range m.Keys() {
			if k == "_creation_timestamp" {
				t.Error("stamp leaked")
			}
		}
	})
}

func TestGlobalIdFormat(t *testing.T) {
	t.Run("valid_global_id_without_evolution", func(t *testing.T) {
		if !laila.Is_laila_resource("LAILA:OBJECT:" + uuid4()) {
			t.Error("valid gid rejected")
		}
	})
	t.Run("valid_global_id_with_evolution", func(t *testing.T) {
		if !laila.Is_laila_resource("LAILA:OBJECT:" + uuid4() + "@evolution=0") {
			t.Error("valid gid rejected")
		}
	})
	t.Run("invalid_global_ids", func(t *testing.T) {
		uid := uuid4()
		for _, bad := range []string{"", "not-a-global-id", uid, "LAILA" + uid} {
			if laila.Is_laila_resource(bad) {
				t.Errorf("accepted %q", bad)
			}
		}
	})
	t.Run("process_global_id_bare_string_is_entry_nickname", func(t *testing.T) {
		parsed := process(t, "garbage")
		if !py.Eq(parsed["scopes"], []string{"ENTRY"}) {
			t.Errorf("scopes = %v", parsed["scopes"])
		}
		if parsed["uuid"] != laila.Generate_uuid_from_nickname("garbage") {
			t.Error("uuid")
		}
		if !py.Eq(parsed, process(t, "ENTRY:garbage")) {
			t.Error("ENTRY: shorthand differs")
		}
	})
	t.Run("process_global_id_structurally_malformed_raises", func(t *testing.T) {
		uid := uuid4()
		for _, bad := range []string{"A::b", ":x", "LAILA:", "LAILA:ENTRY:" + uid[:len(uid)-1], "LAILA:ENTRY:" + uid + "-3"} {
			if _, err := laila.Process_global_id(bad); !errors.Is(err, py.ValueError) {
				t.Errorf("%q: want ValueError, got %v", bad, err)
			}
		}
	})
	t.Run("process_global_id_empty_raises", func(t *testing.T) {
		if _, err := laila.Process_global_id(""); !errors.Is(err, py.ValueError) {
			t.Error("want ValueError")
		}
	})
	t.Run("process_global_id_none_raises", func(t *testing.T) {
		_, err := laila.Process_global_id(nil)
		if !errors.Is(err, py.ValueError) && !errors.Is(err, py.TypeError) {
			t.Errorf("want TypeError/ValueError, got %v", err)
		}
	})
	t.Run("process_global_id_extracts_parts", func(t *testing.T) {
		uid := uuid4()
		parsed := process(t, "LAILA:ENTRY:"+uid)
		if parsed["uuid"] != uid {
			t.Error("uuid")
		}
		if !py.Eq(parsed["scopes"], []string{"ENTRY"}) {
			t.Error("scopes")
		}
		if parsed["evolution"].(*int) != nil {
			t.Error("evolution should be None")
		}
		parsed = process(t, "LAILA:ENTRY:"+uid+"@evolution=42")
		if ev := parsed["evolution"].(*int); ev == nil || *ev != 42 {
			t.Error("evolution")
		}
	})
}

func TestToGlobalId(t *testing.T) {
	t.Run("basic_to_global_id", func(t *testing.T) {
		uid := uuid4()
		gid := laila.To_global_id(py.Kw{"uuid": uid, "scopes": []string{"ENTRY"}})
		if !strings.Contains(gid, uid) || !strings.HasPrefix(gid, "LAILA:") {
			t.Errorf("gid = %s", gid)
		}
	})
	t.Run("to_global_id_with_evolution", func(t *testing.T) {
		gid := laila.To_global_id(py.Kw{"uuid": uuid4(), "scopes": []string{"ENTRY"}, "evolution": 5})
		if !strings.HasSuffix(gid, "@evolution=5") {
			t.Errorf("gid = %s", gid)
		}
	})
	t.Run("to_global_id_none_or_empty_scopes_uses_default", func(t *testing.T) {
		if !strings.Contains(laila.To_global_id(py.Kw{"uuid": uuid4(), "scopes": nil}), "OBJECT") {
			t.Error("None scopes")
		}
		if !strings.Contains(laila.To_global_id(py.Kw{"uuid": uuid4(), "scopes": []string{}}), "OBJECT") {
			t.Error("empty scopes")
		}
	})
	t.Run("to_global_id_with_nickname", func(t *testing.T) {
		if !laila.Is_laila_resource(laila.To_global_id(py.Kw{"nickname": "test-nick", "scopes": []string{"ENTRY"}})) {
			t.Error("nickname gid invalid")
		}
	})
	t.Run("to_global_id_round_trip", func(t *testing.T) {
		uid := uuid4()
		parsed := process(t, laila.To_global_id(py.Kw{"uuid": uid, "scopes": []string{"ENTRY"}, "evolution": 3}))
		if parsed["uuid"] != uid || *parsed["evolution"].(*int) != 3 {
			t.Error("round trip")
		}
	})
}

func TestGenerateUuidFromNickname(t *testing.T) {
	t.Run("deterministic", func(t *testing.T) {
		if laila.Generate_uuid_from_nickname("hello") != laila.Generate_uuid_from_nickname("hello") {
			t.Error("non-deterministic")
		}
	})
	t.Run("different_nicknames_differ", func(t *testing.T) {
		if laila.Generate_uuid_from_nickname("alpha") == laila.Generate_uuid_from_nickname("beta") {
			t.Error("collision")
		}
	})
	t.Run("result_is_valid_uuid", func(t *testing.T) {
		for _, nick := range []string{"test", "", "\u2603snowman"} {
			if _, err := py.Parse_uuid(laila.Generate_uuid_from_nickname(nick)); err != nil {
				t.Errorf("%q: %v", nick, err)
			}
		}
	})
	// none_nickname_raises: ``generate_uuid_from_nickname(None)`` is a
	// compile-time TypeError in Go (the parameter is a string).
}

func TestIdentifiableObjectInit(t *testing.T) {
	t.Run("default_creates_random_uuid", func(t *testing.T) {
		if _, err := py.Parse_uuid(newIdentifiable(t).Uuid()); err != nil {
			t.Error(err)
		}
	})
	t.Run("two_defaults_differ", func(t *testing.T) {
		if newIdentifiable(t).Uuid() == newIdentifiable(t).Uuid() {
			t.Error("same uuid")
		}
	})
	t.Run("explicit_uuid", func(t *testing.T) {
		uid := uuid4()
		if newIdentifiable(t, py.Kw{"uuid": uid}).Uuid() != uid {
			t.Error("uuid")
		}
	})
	t.Run("nickname_overrides_uuid", func(t *testing.T) {
		obj := newIdentifiable(t, py.Kw{"uuid": "should-be-overridden", "nickname": "winner"})
		if obj.Uuid() != laila.Generate_uuid_from_nickname("winner") {
			t.Error("nickname did not win")
		}
	})
	t.Run("evolution_stored_and_none_by_default", func(t *testing.T) {
		if ev := newIdentifiable(t, py.Kw{"evolution": 7}).Evolution(); ev == nil || *ev != 7 {
			t.Error("evolution")
		}
		if newIdentifiable(t).Evolution() != nil {
			t.Error("default evolution")
		}
	})
	t.Run("has_evolution", func(t *testing.T) {
		if !newIdentifiable(t, py.Kw{"evolution": 0}).Has_evolution() || newIdentifiable(t).Has_evolution() {
			t.Error("has_evolution")
		}
	})
	t.Run("global_id_property", func(t *testing.T) {
		if !laila.Is_laila_resource(newIdentifiable(t).Global_id()) {
			t.Error("global_id")
		}
	})
	t.Run("global_id_setter", func(t *testing.T) {
		uid := uuid4()
		obj := newIdentifiable(t)
		if err := obj.Set_global_id("LAILA:CUSTOM:" + uid + "@evolution=10"); err != nil {
			t.Fatal(err)
		}
		if obj.Uuid() != uid || *obj.Evolution() != 10 {
			t.Error("setter")
		}
	})
	t.Run("global_id_setter_bare_string_rebinds_to_entry_nickname", func(t *testing.T) {
		obj := newIdentifiable(t)
		if err := obj.Set_global_id("not-valid"); err != nil {
			t.Fatal(err)
		}
		if !py.Eq(obj.Scopes(), []string{"ENTRY"}) || obj.Uuid() != laila.Generate_uuid_from_nickname("not-valid") {
			t.Error("rebind")
		}
	})
	t.Run("global_id_setter_malformed_raises", func(t *testing.T) {
		if err := newIdentifiable(t).Set_global_id("A::b"); !errors.Is(err, py.ValueError) {
			t.Errorf("want ValueError, got %v", err)
		}
	})
	t.Run("str_and_repr", func(t *testing.T) {
		obj := newIdentifiable(t)
		if py.Str(obj) != obj.Global_id() || py.Repr(obj) != obj.Global_id() {
			t.Error("str/repr")
		}
	})
	t.Run("hash_consistent", func(t *testing.T) {
		obj := newIdentifiable(t)
		if obj.Hash() != py.Must(py.Hash(obj.Global_id())) {
			t.Error("hash")
		}
	})
	t.Run("identity_dict", func(t *testing.T) {
		ident := newIdentifiable(t, py.Kw{"evolution": 3}).Identity()
		if _, ok := ident.Get("uuid"); !ok {
			t.Error("uuid")
		}
		if ev, ok := ident.Get("evolution"); !ok || !py.Eq(ev, 3) {
			t.Errorf("evolution = %v", ev)
		}
	})
	t.Run("identity_as_json", func(t *testing.T) {
		obj := newIdentifiable(t)
		parsed, err := py.Json_loads(obj.Identity_as_json())
		if err != nil {
			t.Fatal(err)
		}
		if parsed.(*py.Dict).GetDefault("uuid", nil) != obj.Uuid() {
			t.Error("json uuid")
		}
	})
	t.Run("type_variable_and_constant", func(t *testing.T) {
		uid := uuid4()
		if ty, _ := laila.LAILA_IDENTIFIABLE_OBJECT_type("LAILA:ENTRY:" + uid + "@evolution=0"); ty != "variable" {
			t.Error("variable")
		}
		if ty, _ := laila.LAILA_IDENTIFIABLE_OBJECT_type("LAILA:ENTRY:" + uid); ty != "constant" {
			t.Error("constant")
		}
	})
	t.Run("from_global_id_valid", func(t *testing.T) {
		uid := uuid4()
		obj, err := laila.LAILA_IDENTIFIABLE_OBJECT_from_global_id("LAILA:ENTRY:" + uid + "@evolution=5")
		if err != nil {
			t.Fatal(err)
		}
		if obj.Uuid() != uid || *obj.Evolution() != 5 {
			t.Error("from_global_id")
		}
	})
	t.Run("from_global_id_bare_string_is_entry_nickname", func(t *testing.T) {
		obj, err := laila.LAILA_IDENTIFIABLE_OBJECT_from_global_id("bad-id")
		if err != nil {
			t.Fatal(err)
		}
		if !py.Eq(obj.Scopes(), []string{"ENTRY"}) || obj.Uuid() != laila.Generate_uuid_from_nickname("bad-id") {
			t.Error("bare string")
		}
	})
	t.Run("from_global_id_malformed_raises", func(t *testing.T) {
		if _, err := laila.LAILA_IDENTIFIABLE_OBJECT_from_global_id("LAILA:ENTRY:" + uuid4() + "-5"); !errors.Is(err, py.ValueError) {
			t.Errorf("want ValueError, got %v", err)
		}
	})
	t.Run("bare_reference_defaults_to_entry_scope", func(t *testing.T) {
		base := resolve(t, "run-3")
		if base != resolve(t, "ENTRY:run-3") || !strings.HasPrefix(base, "LAILA:ENTRY:") {
			t.Errorf("base = %s", base)
		}
		// A bare *object* keeps the OBJECT scope; only references default to ENTRY.
		if !py.Eq(newIdentifiable(t).Scopes(), []string{"OBJECT"}) {
			t.Error("object scope")
		}
		// Subclasses keep their own default for scope-less references
		// (the Manifest case is exercised with the memory module).
		sub := py.NewClass("_ScopedThing", (*laila.LAILA_IDENTIFIABLE_OBJECT_)(nil), laila.LAILA_IDENTIFIABLE_OBJECT_class).
			Set_classvar("_DEFAULT_SCOPES", []string{"MANIFEST"})
		gid, err := laila.Resolve_global_id(sub, "x")
		if err != nil || !strings.HasPrefix(gid, "LAILA:MANIFEST:") {
			t.Errorf("subclass default: %s %v", gid, err)
		}
	})
	t.Run("truncated_uuid_tail_is_rejected", func(t *testing.T) {
		uid := uuid4()
		for _, tail := range []string{uid[:len(uid)-1], uid + "0", strings.ReplaceAll(uid, "-", "")} {
			if _, err := laila.Resolve_global_id(nil, "ENTRY:"+tail); !errors.Is(err, py.ValueError) {
				t.Errorf("%q: want ValueError, got %v", tail, err)
			}
		}
	})
	t.Run("negative_evolution_allowed_in_reference_only", func(t *testing.T) {
		gid := resolve(t, "ENTRY:x@evolution=-1")
		if !strings.HasSuffix(gid, "@evolution=-1") {
			t.Errorf("gid = %s", gid)
		}
		if _, err := laila.Process_global_id(gid); !errors.Is(err, py.ValueError) {
			t.Errorf("process: want ValueError, got %v", err)
		}
		if _, err := laila.Resolve_global_id(nil, "ENTRY:x@evolution=--1"); !errors.Is(err, py.ValueError) {
			t.Errorf("resolve: want ValueError, got %v", err)
		}
	})
}

func TestGlobalIdAttributes(t *testing.T) {
	const HB = "2026-09-15T17:50:00.123+00:00"

	t.Run("no_global_id_marker_in_emitted_ids", func(t *testing.T) {
		obj := newIdentifiable(t, py.Kw{"scopes": []string{"ENTRY"}})
		if obj.Global_id() != "LAILA:ENTRY:"+obj.Uuid() || strings.Contains(obj.Global_id(), "GLOBAL_ID") {
			t.Errorf("gid = %s", obj.Global_id())
		}
	})
	t.Run("evolution_round_trip", func(t *testing.T) {
		uid := uuid4()
		gid := laila.To_global_id(py.Kw{"uuid": uid, "scopes": []string{"A", "B"}, "evolution": 7})
		if gid != fmt.Sprintf("LAILA:A:B:%s@evolution=7", uid) {
			t.Errorf("gid = %s", gid)
		}
		parsed := process(t, gid)
		if parsed["uuid"] != uid || !py.Eq(parsed["scopes"], []string{"A", "B"}) || *parsed["evolution"].(*int) != 7 {
			t.Errorf("parsed = %v", parsed)
		}
		if laila.To_global_id(parsed) != gid {
			t.Error("to_global_id(**parsed)")
		}
	})
	t.Run("unknown_attribute_is_ignored_for_identity_but_exposed", func(t *testing.T) {
		uid := uuid4()
		gid := fmt.Sprintf("LAILA:ENTRY:%s@evolution=2,creation_timestamp=%s", uid, HB)
		if *process(t, gid)["evolution"].(*int) != 2 {
			t.Error("evolution")
		}
		attrs, err := laila.Get_attributes_from_global_id(gid)
		if err != nil || !py.Eq(attrs, py.DictOf("evolution", "2", "creation_timestamp", HB)) {
			t.Errorf("attrs = %s %v", py.Repr(attrs), err)
		}
		obj, err := laila.LAILA_IDENTIFIABLE_OBJECT_from_global_id(gid)
		if err != nil {
			t.Fatal(err)
		}
		if obj.Global_id() != fmt.Sprintf("LAILA:ENTRY:%s@evolution=2", uid) {
			t.Errorf("re-emitted gid = %s", obj.Global_id())
		}
		if laila.Strip_global_id_attributes(gid) != "LAILA:ENTRY:"+uid {
			t.Error("strip")
		}
	})
	t.Run("creation_timestamp_only_attribute_is_a_constant_identity", func(t *testing.T) {
		gid := fmt.Sprintf("LAILA:ENTRY:%s@creation_timestamp=%s", uuid4(), HB)
		if !laila.Is_laila_resource(gid) {
			t.Error("is_laila_resource")
		}
		if process(t, gid)["evolution"].(*int) != nil {
			t.Error("evolution should be None")
		}
		if ty, _ := laila.LAILA_IDENTIFIABLE_OBJECT_type(gid); ty != "constant" {
			t.Error("type")
		}
	})
	t.Run("malformed_suffixes_raise", func(t *testing.T) {
		uid := uuid4()
		for _, suffix := range []string{"@", "@evolution", "@=3", "@evolution=3,", "@evolution=x", "@evolution=1,evolution=2", "@1abc=2"} {
			bad := "LAILA:ENTRY:" + uid + suffix
			if _, err := laila.Process_global_id(bad); !errors.Is(err, py.ValueError) {
				t.Errorf("%q: want ValueError, got %v", bad, err)
			}
		}
	})
	t.Run("legacy_dash_suffix_is_rejected", func(t *testing.T) {
		bad := "LAILA:ENTRY:" + uuid4() + "-3"
		if laila.Is_laila_resource(bad) {
			t.Error("is_laila_resource")
		}
		if _, err := laila.Process_global_id(bad); !errors.Is(err, py.ValueError) {
			t.Error("process")
		}
	})
	t.Run("shorthand_with_evolution_attribute", func(t *testing.T) {
		want := fmt.Sprintf("LAILA:ENTRY:%s@evolution=3", laila.Generate_uuid_from_nickname("counter"))
		if got := resolve(t, "ENTRY:counter@evolution=3"); got != want {
			t.Errorf("got %s", got)
		}
	})
	t.Run("shorthand_passes_search_attributes_through", func(t *testing.T) {
		want := fmt.Sprintf("LAILA:ENTRY:%s@creation_timestamp=%s", laila.Generate_uuid_from_nickname("counter"), HB)
		if got := resolve(t, "ENTRY:counter@creation_timestamp="+HB); got != want {
			t.Errorf("got %s", got)
		}
	})
	t.Run("explicit_evolution_kwarg_overrides_attribute", func(t *testing.T) {
		uid := uuid4()
		if got := resolve(t, "LAILA:ENTRY:"+uid, py.Kw{"evolution": 4}); got != "LAILA:ENTRY:"+uid+"@evolution=4" {
			t.Errorf("got %s", got)
		}
		if got := resolve(t, "ENTRY:"+uid+"@evolution=1", py.Kw{"evolution": 4}); got != "LAILA:ENTRY:"+uid+"@evolution=4" {
			t.Errorf("got %s", got)
		}
	})
	t.Run("nickname_with_dash_digits_is_literal", func(t *testing.T) {
		if got := resolve(t, "ENTRY:run-3"); got != "LAILA:ENTRY:"+laila.Generate_uuid_from_nickname("run-3") {
			t.Errorf("got %s", got)
		}
	})
	t.Run("parse_evolution_false_takes_tail_literally", func(t *testing.T) {
		got := resolve(t, "ENTRY:run@evolution=3", py.Kw{"parse_evolution": false})
		if got != "LAILA:ENTRY:"+laila.Generate_uuid_from_nickname("run@evolution=3") {
			t.Errorf("got %s", got)
		}
	})
	t.Run("full_gid_with_attributes_is_returned_unchanged", func(t *testing.T) {
		gid := fmt.Sprintf("LAILA:ENTRY:%s@evolution=2,creation_timestamp=%s", uuid4(), HB)
		if resolve(t, gid) != gid {
			t.Error("changed")
		}
	})
}

func TestSetActiveNamespace(t *testing.T) {
	t.Cleanup(laila.Reset_active_namespace_)
	t.Run("set_active_namespace_changes_uuid_generation", func(t *testing.T) {
		laila.Reset_active_namespace_()
		def := laila.Generate_uuid_from_nickname("ns-test")
		laila.Set_active_namespace("custom-namespace")
		if laila.Generate_uuid_from_nickname("ns-test") == def {
			t.Error("namespace had no effect")
		}
	})
	t.Run("set_active_namespace_is_deterministic", func(t *testing.T) {
		laila.Set_active_namespace("ns1")
		a := laila.Generate_uuid_from_nickname("test")
		laila.Set_active_namespace("ns1")
		if laila.Generate_uuid_from_nickname("test") != a {
			t.Error("non-deterministic")
		}
	})
}
