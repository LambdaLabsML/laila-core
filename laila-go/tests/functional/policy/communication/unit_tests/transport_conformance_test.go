// Port of tests/functional/policy/communication/unit_tests/test_transport_conformance.py
// and test_transport_environment.py.
//
// Interface-conformance sweep over *every* registered transport.
//
// This is the "are all comm interfaces working?" guard. It discovers every
// ``Default*Protocol`` exported by ``laila.macros.defaults`` and, for each,
// verifies the full LAILA_IDENTIFIABLE_COMM_PROTOCOL_ contract:
//
//   - it instantiates with no required args,
//   - it has a real ``protocol_name`` and answers ``matches_token`` for it,
//   - ``can_handle_uri`` is a total boolean function,
//   - it gets a unique ``global_id`` per instance,
//   - ``has_peer`` works before any peering,
//   - ``start()`` either succeeds (working transports) or fails with a
//     *clean, actionable* error -- RuntimeError (missing driver / needs
//     config), NotImplementedError (declared scaffold) or ConnectionError --
//     never a programming error (AttributeError / TypeError / KeyError ...)
//     that would mean a broken interface,
//   - ``stop()`` is idempotent and safe to call even if ``start()`` failed.
//
// Transports whose data path is real and dependency-free (IP carriers,
// loopback, Unix socket) must actually start and stop.
package unit_tests

import (
	"sort"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// must_work_ are transports that must genuinely start/stop here (no
// driver, no hardware).
var must_work_ = map[string]bool{
	"tcp": true, "tcpip": true, "udp": true, "tls": true, "ethernet": true, "wifi": true,
	"wifi-direct": true, "cellular": true, "gsm": true, "lte": true, "nr5g": true, "nbiot": true,
	"ltem": true, "matter": true, "unix": true, "loopback": true, "6lowpan": true,
}

// clean_errors_ are the clean failure modes for transports needing a
// driver / device / config.
var clean_errors_ = []*py.Type{py.RuntimeError, py.NotImplementedError, py.ConnectionError, py.TimeoutError}

// unique_protocol_classes_ returns the unique transport classes behind the
// Default* aliases (sorted by class name for deterministic subtests).
func unique_protocol_classes_() []*py.Class {
	seen := map[string]*py.Class{}
	for _, cls := range laila.Macros_defaults_protocols {
		seen[cls.Name] = cls
	}
	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]*py.Class, 0, len(names))
	for _, n := range names {
		out = append(out, seen[n])
	}
	return out
}

func new_proto_(t *testing.T, cls *py.Class, kw ...py.Kw) laila.Comm_protocol_ {
	t.Helper()
	inst, err := py.New_instance(cls, py.MergeKw(kw...))
	if err != nil {
		t.Fatalf("%s(): %v", cls.Name, err)
	}
	p, ok := inst.(laila.Comm_protocol_)
	if !ok {
		t.Fatalf("%s is not a Comm_protocol_ (%T)", cls.Name, inst)
	}
	return p
}

func setUpConformance_(t *testing.T) []*py.Class {
	t.Helper()
	classes := unique_protocol_classes_()
	// sanity: we expect the full catalog to be discoverable
	if len(classes) < 30 {
		t.Fatalf("only %d transport classes discovered", len(classes))
	}
	return classes
}

func TestTransportConformance_every_transport_metadata(t *testing.T) {
	for _, cls := range setUpConformance_(t) {
		t.Run(cls.Name, func(t *testing.T) {
			name := laila.Comm_protocol_name_of_(cls)
			if name == "" || name == "base" || name == "carrier" {
				t.Fatal(name)
			}
			if !laila.Comm_protocol_matches_token_(cls, name) {
				t.Fatalf("%s !match self", name)
			}
			if laila.Comm_protocol_can_handle_uri_(cls, "nope://x") {
				t.Fatal("claimed nope://x")
			}
		})
	}
}

func TestTransportConformance_every_transport_has_uniform_connection_surface(t *testing.T) {
	// The real abstraction: every transport exposes the same interface.
	for _, cls := range setUpConformance_(t) {
		t.Run(cls.Name, func(t *testing.T) {
			proto := new_proto_(t, cls)
			for _, m := range []string{"connect", "disconnect", "ping", "send_rpc", "has_peer", "start", "stop"} {
				attr, err := py.Getattr(proto, m)
				if err != nil || !py.Callable(attr) {
					t.Fatalf("%s: %v", m, err)
				}
			}
			persistent, _ := cls.Classvar("persistent")
			if _, ok := persistent.(bool); !ok {
				t.Fatalf("persistent = %s", py.Repr(persistent))
			}
			supports_ping, _ := cls.Classvar("supports_ping")
			if _, ok := supports_ping.(bool); !ok {
				t.Fatalf("supports_ping = %s", py.Repr(supports_ping))
			}
			// ping/disconnect are safe with no peer (never raise)
			if proto.Ping("nobody") {
				t.Fatal("ping('nobody') returned True")
			}
			if err := proto.Disconnect("nobody"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTransportConformance_every_transport_instantiates_with_identity(t *testing.T) {
	for _, cls := range setUpConformance_(t) {
		t.Run(cls.Name, func(t *testing.T) {
			a := new_proto_(t, cls)
			b := new_proto_(t, cls)
			if a.Global_id() == "" || a.Global_id() == b.Global_id() {
				t.Fatalf("%q vs %q", a.Global_id(), b.Global_id())
			}
			if a.Has_peer("anyone") {
				t.Fatal("has_peer before peering")
			}
		})
	}
}

func is_clean_error_(err error) bool {
	for _, e := range clean_errors_ {
		if py.IsInstance(err, e) {
			return true
		}
	}
	return false
}

func TestTransportConformance_every_transport_start_is_clean(t *testing.T) {
	for _, cls := range setUpConformance_(t) {
		t.Run(cls.Name, func(t *testing.T) {
			proto := new_proto_(t, cls)
			name := laila.Comm_protocol_name_(proto)
			if must_work_[name] {
				defer func() {
					_ = proto.Stop()
					_ = proto.Stop() // idempotent
				}()
				if err := proto.Start(); err != nil {
					t.Fatal(err)
				}
				// idempotent
				if err := proto.Start(); err != nil {
					t.Fatal(err)
				}
				if proto.Has_peer("x") {
					t.Fatal("has_peer('x')")
				}
			} else {
				err := proto.Start()
				if err == nil || !is_clean_error_(err) {
					t.Fatalf("expected a clean error, got %v", err)
				}
				// stop must be a safe no-op after a failed start
				if err := proto.Stop(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestTransportConformance_unique_class_names_for_environment_round_trip(t *testing.T) {
	classes := setUpConformance_(t)
	seen := map[string]bool{}
	for _, c := range classes {
		if seen[c.Name] {
			t.Fatalf("duplicate class name %s", c.Name)
		}
		seen[c.Name] = true
	}
}

func TestTransportConformance_macros_exposes_protocols(t *testing.T) {
	// defensive: the defaults module is importable via laila.macros
	if len(laila.Macros_defaults_protocols) == 0 {
		t.Fatal("no Default*Protocol aliases")
	}
}

// ------------------------------------------------------------------ #
// test_transport_environment.py -- environment-mirror conformance
// ------------------------------------------------------------------ #

func setUpEnvironment_(t *testing.T) []*py.Class {
	t.Helper()
	classes := unique_protocol_classes_()
	if len(classes) < 50 {
		t.Fatalf("only %d transport classes discovered", len(classes))
	}
	return classes
}

func TestTransportEnvironment_config_is_json_serializable(t *testing.T) {
	for _, cls := range setUpEnvironment_(t) {
		t.Run(cls.Name, func(t *testing.T) {
			proto := new_proto_(t, cls)
			dump := py.Model_dump(proto, py.Kw{"mode": "json"})
			// must not raise: no live handles leak into the mirror
			if _, err := py.Json_dumps(dump); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTransportEnvironment_reconstructable_from_config(t *testing.T) {
	for _, cls := range setUpEnvironment_(t) {
		t.Run(cls.Name, func(t *testing.T) {
			proto := new_proto_(t, cls)
			dump := py.Model_dump(proto, py.Kw{"mode": "json"})
			// rebuild from the dumped config like _load_environment does
			kw := py.Kw{}
			for _, it := range dump.Items() {
				if k := py.Str(it.Key); k != "class_token" {
					kw[k] = it.Value
				}
			}
			rebuilt := new_proto_(t, cls, kw)
			assertEq(t, laila.Comm_protocol_name_(rebuilt), laila.Comm_protocol_name_(proto))
		})
	}
}

func is_json_scalar_or_container_(v any) bool {
	switch v.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, string,
		*py.List, *py.Dict, py.Tuple, []any, []string, map[string]any:
		return true
	}
	if py.IsNone(v) {
		return true
	}
	return false
}

func TestTransportEnvironment_no_runtime_handle_fields(t *testing.T) {
	// every model Field value on a fresh instance must be a JSON scalar /
	// container -- live handles belong in PrivateAttr.
	for _, cls := range setUpEnvironment_(t) {
		t.Run(cls.Name, func(t *testing.T) {
			proto := new_proto_(t, cls)
			for _, f := range cls.Model_fields() {
				val := must(py.Getattr(proto, f.Name))
				if !is_json_scalar_or_container_(val) {
					t.Fatalf("%s.%s is a non-config handle (%T)", cls.Name, f.Name, val)
				}
			}
		})
	}
}
