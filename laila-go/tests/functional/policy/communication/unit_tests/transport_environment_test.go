// Environment-mirror conformance for every transport.
//
// CLI-capable objects are dumped into ``laila.args.environment`` and must
// be reconstructable from JSON config alone (see agentic/internal docs).
// This asserts, for every transport, that:
//
//   - its public config (``model_dump``) is strictly JSON-serializable, so
//     it never leaks a live handle (socket/loop/driver) into the mirror, and
//   - it can be re-instantiated from that dumped config (the
//     environment-restore path), preserving ``protocol_name``.
//
// This catches the classic mistake of declaring a runtime handle as a
// ``Field`` instead of a ``PrivateAttr``.
//
// Port of tests/functional/policy/communication/unit_tests/test_transport_environment.py.
package unit_tests

import (
	"reflect"
	"testing"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// ``_unique_protocol_classes()`` is unique_protocol_classes_ from
// transport_conformance_test.go (``dir(defaults)`` -> ``Default*Protocol``
// aliases, deduped by class name).

func transport_env_setUp_(t *testing.T) []*py.Class {
	t.Helper()
	classes := unique_protocol_classes_()
	if len(classes) < 50 {
		t.Fatalf("expected >= 50 transport classes, got %d", len(classes))
	}
	return classes
}

// is_json_config_value_ is ``isinstance(val, (NoneType, bool, int, float,
// str, list, dict, tuple))`` for the Go representations of those types.
func is_json_config_value_(v any) bool {
	if py.IsNone(v) {
		return true
	}
	switch v.(type) {
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64,
		float32, float64, string, *py.List, *py.Dict:
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array, reflect.Map:
		return true
	case reflect.Ptr:
		if rv.IsNil() {
			return true
		}
		return is_json_config_value_(rv.Elem().Interface())
	}
	return false
}

func TestTransportEnvironment(t *testing.T) {
	t.Run("config_is_json_serializable", func(t *testing.T) {
		for _, cls := range transport_env_setUp_(t) {
			t.Run(cls.Name, func(t *testing.T) {
				proto, err := py.New_instance(cls, py.Kw{})
				if err != nil {
					t.Fatal(err)
				}
				dump := py.Model_dump(proto, py.Kw{"mode": "json"})
				// must not raise: no live handles leak into the mirror
				if _, err := py.Json_dumps(dump); err != nil {
					t.Fatalf("%s.model_dump(mode='json') is not JSON-serializable: %v", cls.Name, err)
				}
			})
		}
	})

	t.Run("reconstructable_from_config", func(t *testing.T) {
		for _, cls := range transport_env_setUp_(t) {
			t.Run(cls.Name, func(t *testing.T) {
				proto, err := py.New_instance(cls, py.Kw{})
				if err != nil {
					t.Fatal(err)
				}
				dump := py.Model_dump(proto, py.Kw{"mode": "json"})
				// rebuild from the dumped config like _load_environment does
				kw := py.Kw{}
				for _, it := range dump.Items() {
					k := py.Str(it.Key)
					if k != "class_token" {
						kw[k] = it.Value
					}
				}
				rebuilt, err := py.New_instance(cls, kw)
				if err != nil {
					t.Fatalf("%s(**model_dump()) failed: %v", cls.Name, err)
				}
				want := must(py.Getattr(proto, "protocol_name"))
				got := must(py.Getattr(rebuilt, "protocol_name"))
				if !py.Eq(got, want) {
					t.Fatalf("protocol_name changed on rebuild: %s != %s", py.Repr(got), py.Repr(want))
				}
			})
		}
	})

	t.Run("no_runtime_handle_fields", func(t *testing.T) {
		// every model Field value on a fresh instance must be a JSON scalar
		// / container -- live handles belong in PrivateAttr.
		for _, cls := range transport_env_setUp_(t) {
			t.Run(cls.Name, func(t *testing.T) {
				proto, err := py.New_instance(cls, py.Kw{})
				if err != nil {
					t.Fatal(err)
				}
				for _, f := range py.Classof(proto).Model_fields() {
					val := must(py.Getattr(proto, f.Name))
					if !is_json_config_value_(val) {
						t.Fatalf("%s.%s is a non-config handle (%T)", cls.Name, f.Name, val)
					}
				}
			})
		}
	})
}
