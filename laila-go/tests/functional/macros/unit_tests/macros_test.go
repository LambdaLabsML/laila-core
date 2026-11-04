// Port of tests/functional/macros/unit_tests/test_macros.py.
//
// The alias tests that construct Entry objects and the defaults tests that
// instantiate DefaultPool / DefaultPolicy need the entry and policy
// modules; they are ported with those modules.
package unit_tests

import (
	"strings"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// scopeNames is ``[n for n in dir(strings) if n.endswith("_SCOPE")]``.
func scopeNames() map[string]string {
	out := map[string]string{}
	for name, val := range laila.Macros_strings {
		if strings.HasSuffix(name, "_SCOPE") {
			out[name] = val
		}
	}
	return out
}

func TestMacrosStrings(t *testing.T) {
	t.Run("scope_constants_are_nonempty_strings", func(t *testing.T) {
		for name, val := range scopeNames() {
			if val == "" {
				t.Errorf("%s is empty", name)
			}
		}
	})
	t.Run("default_pool_nickname", func(t *testing.T) {
		if laila.DEFAULT_POOL_NICKNAME_ != "_memory" || laila.Macros_strings["_DEFAULT_POOL_NICKNAME"] != "_memory" {
			t.Error("_DEFAULT_POOL_NICKNAME")
		}
	})
	t.Run("topmost_scope", func(t *testing.T) {
		if laila.TOPMOST_SCOPE_ != "LAILA" {
			t.Error("_TOPMOST_SCOPE")
		}
	})
	t.Run("string_constant_values_unique_except_laila_alias_pair", func(t *testing.T) {
		counts := map[string]int{}
		lailaNames := py.NewSet()
		for name, val := range laila.Macros_strings {
			counts[val]++
			if val == "LAILA" {
				lailaNames.Add(name)
			}
		}
		duplicated := py.NewSet()
		for v, c := range counts {
			if c > 1 {
				duplicated.Add(v)
			}
		}
		if !py.Eq(duplicated, py.NewSet("LAILA")) {
			t.Errorf("duplicated = %s; only _LAILA_SCOPE and _TOPMOST_SCOPE may share the same value", py.Repr(duplicated))
		}
		if counts["LAILA"] != 2 {
			t.Error("LAILA count")
		}
		if !py.Eq(lailaNames, py.NewSet("_LAILA_SCOPE", "_TOPMOST_SCOPE")) {
			t.Errorf("laila names = %s", py.Repr(lailaNames))
		}
	})
	t.Run("scope_values_are_uppercase", func(t *testing.T) {
		for name, val := range scopeNames() {
			if val != strings.ToUpper(val) {
				t.Errorf("%s should be uppercase", name)
			}
		}
	})
	t.Run("scope_values_contain_no_whitespace", func(t *testing.T) {
		for name, val := range scopeNames() {
			if strings.ContainsAny(val, " \t") {
				t.Errorf("%s contains whitespace", name)
			}
		}
	})
	t.Run("default_pool_nickname_starts_with_underscore", func(t *testing.T) {
		if !strings.HasPrefix(laila.DEFAULT_POOL_NICKNAME_, "_") {
			t.Error("prefix")
		}
	})
	t.Run("well_known_scopes_exist", func(t *testing.T) {
		if laila.ENTRY_SCOPE_ != "ENTRY" || laila.POOL_SCOPE_ != "POOL" || laila.FUTURE_SCOPE_ != "FUTURE" {
			t.Error("scope values")
		}
	})
	t.Run("all_scope_names_follow_naming_convention", func(t *testing.T) {
		for name := range scopeNames() {
			if !strings.HasPrefix(name, "_") || name != strings.ToUpper(name) {
				t.Errorf("%s violates the naming convention", name)
			}
		}
	})
}

func TestMacrosAliases(t *testing.T) {
	t.Run("callables", func(t *testing.T) {
		if !py.Callable(laila.Constant) || !py.Callable(laila.Variable) || !py.Callable(laila.Contingent) {
			t.Error("aliases must be callable")
		}
	})
}

func TestMacrosDefaults(t *testing.T) {
	t.Run("laila_universal_namespace_is_uuid", func(t *testing.T) {
		if _, err := py.Parse_uuid(laila.LAILA_UNIVERSAL_NAMESPACE.String()); err != nil {
			t.Error(err)
		}
	})
	t.Run("auto_initialize_policy", func(t *testing.T) {
		if laila.AUTO_INITIALIZE_POLICY != true {
			t.Error("AUTO_INITIALIZE_POLICY")
		}
	})
	t.Run("namespace_is_deterministic", func(t *testing.T) {
		if laila.LAILA_UNIVERSAL_NAMESPACE != py.Uuid5(py.NAMESPACE_DNS, "laila") {
			t.Error("namespace")
		}
	})
}
