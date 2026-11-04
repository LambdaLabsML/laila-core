// Port of tests/functional/policy/communication/unit_tests/test_rpc.py.
//
// Comprehensive RPC tests between two local policies over loopback.
//
// Validates that the JSON-RPC 2.0 dispatch layer correctly serializes
// arguments, deserializes return values, propagates errors, and handles
// every interesting data type and call pattern across the WebSocket
// bridge.
package unit_tests

import (
	"math"
	"sort"
	"strings"
	"sync"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// ────────────────────────────────────────────────────────────────────
// Fixtures
// ────────────────────────────────────────────────────────────────────

// attach_ is ``_attach``: attach *obj* as attribute *name* on *policy*
// bypassing Pydantic.
func attach_(policy *laila.LAILA_IDENTIFIABLE_POLICY_, name string, obj any) {
	policy.Private_set(name, obj)
}

// rpcBase_ is ``_RPCTestBase``: two peered policies with proxies ready to
// use.
type rpcBase_ struct {
	A, B             *laila.LAILA_IDENTIFIABLE_POLICY_
	proxy_b, proxy_a *laila.PeerProxy
}

func setUpRPC_(t *testing.T) *rpcBase_ {
	t.Helper()
	s := &rpcBase_{A: make_policy(t), B: make_policy(t)}
	peer(t, s.A, s.B)
	sleep(0.3)
	s.proxy_b = must(comm(s.A).Peers.Getitem(s.B.Global_id())).(*laila.PeerProxy)
	s.proxy_a = must(comm(s.B).Peers.Getitem(s.A.Global_id())).(*laila.PeerProxy)
	t.Cleanup(func() { stopAll(s.A, s.B) })
	return s
}

func call_(t *testing.T, p *laila.PeerProxy, name string, args ...any) any {
	t.Helper()
	v, err := p.Attr(name).Invoke(args...)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return v
}

func call_err_(t *testing.T, chain *laila.RemoteAttrChain_, args ...any) error {
	t.Helper()
	_, err := chain.Invoke(args...)
	assertRaises(t, py.RuntimeError, err)
	return err
}

func assertIn_(t *testing.T, needle string, err error) {
	t.Helper()
	if !strings.Contains(err.Error(), needle) {
		t.Fatalf("expected %q in %q", needle, err.Error())
	}
}

func assertAlmostEq_(t *testing.T, got any, want float64) {
	t.Helper()
	f, err := py.Float(got)
	if err != nil || math.Abs(f-want) > 1e-7 {
		t.Fatalf("got %s, want ~%v", py.Repr(got), want)
	}
}

// ────────────────────────────────────────────────────────────────────
// 1. Return value types
// ────────────────────────────────────────────────────────────────────

func TestReturnTypes_return_none(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "noop", func() any { return nil })
	if v := call_(t, s.proxy_b, "noop"); !py.IsNone(v) {
		t.Fatal(py.Repr(v))
	}
}

func TestReturnTypes_return_int(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "get_int", func() int { return 42 })
	assertEq(t, call_(t, s.proxy_b, "get_int"), 42)
}

func TestReturnTypes_return_negative_int(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "neg", func() int { return -99 })
	assertEq(t, call_(t, s.proxy_b, "neg"), -99)
}

func TestReturnTypes_return_zero(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "zero", func() int { return 0 })
	assertEq(t, call_(t, s.proxy_b, "zero"), 0)
}

func TestReturnTypes_return_large_int(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "big", func() int64 { return 1_000_000_000_000_000_000 })
	assertEq(t, call_(t, s.proxy_b, "big"), int64(1_000_000_000_000_000_000))
}

func TestReturnTypes_return_float(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "pi", func() float64 { return 3.14159 })
	assertAlmostEq_(t, call_(t, s.proxy_b, "pi"), 3.14159)
}

func TestReturnTypes_return_bool_true(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "yes", func() bool { return true })
	assertEq(t, call_(t, s.proxy_b, "yes"), true)
}

func TestReturnTypes_return_bool_false(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "no", func() bool { return false })
	assertEq(t, call_(t, s.proxy_b, "no"), false)
}

func TestReturnTypes_return_string(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "greet", func() string { return "hello world" })
	assertEq(t, call_(t, s.proxy_b, "greet"), "hello world")
}

func TestReturnTypes_return_empty_string(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "empty", func() string { return "" })
	assertEq(t, call_(t, s.proxy_b, "empty"), "")
}

func TestReturnTypes_return_unicode_string(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "uni", func() string { return "日本語テスト 🚀" })
	assertEq(t, call_(t, s.proxy_b, "uni"), "日本語テスト 🚀")
}

func TestReturnTypes_return_list(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "items", func() *py.List { return py.NewList(1, 2, 3) })
	assertEq(t, call_(t, s.proxy_b, "items"), py.NewList(1, 2, 3))
}

func TestReturnTypes_return_empty_list(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "empty_list", func() *py.List { return py.NewList() })
	assertEq(t, call_(t, s.proxy_b, "empty_list"), py.NewList())
}

func TestReturnTypes_return_nested_list(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "nested", func() *py.List { return py.NewList(py.NewList(1, 2), py.NewList(3, py.NewList(4, 5))) })
	assertEq(t, call_(t, s.proxy_b, "nested"), py.NewList(py.NewList(1, 2), py.NewList(3, py.NewList(4, 5))))
}

func TestReturnTypes_return_dict(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "config", func() *py.Dict { return py.DictOf("a", 1, "b", "two") })
	assertEq(t, call_(t, s.proxy_b, "config"), py.DictOf("a", 1, "b", "two"))
}

func TestReturnTypes_return_empty_dict(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "empty_dict", func() *py.Dict { return py.NewDict() })
	assertEq(t, call_(t, s.proxy_b, "empty_dict"), py.NewDict())
}

func TestReturnTypes_return_nested_dict(t *testing.T) {
	s := setUpRPC_(t)
	d := py.DictOf("level1", py.DictOf("level2", py.DictOf("level3", "deep")))
	attach_(s.B, "deep_dict", func() *py.Dict { return d })
	assertEq(t, call_(t, s.proxy_b, "deep_dict"), d)
}

func TestReturnTypes_return_mixed_collection(t *testing.T) {
	s := setUpRPC_(t)
	d := py.DictOf("nums", py.NewList(1, 2), "nested", py.DictOf("ok", true), "list_of_dicts", py.NewList(py.DictOf("x", 1)))
	attach_(s.B, "mixed", func() *py.Dict { return d })
	assertEq(t, call_(t, s.proxy_b, "mixed"), d)
}

// ────────────────────────────────────────────────────────────────────
// 2. Argument passing
// ────────────────────────────────────────────────────────────────────

func TestArgPassing_single_positional_arg(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "inc", func(x int) int { return x + 1 })
	assertEq(t, call_(t, s.proxy_b, "inc", 10), 11)
}

func TestArgPassing_multiple_positional_args(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "add", func(a, b int) int { return a + b })
	assertEq(t, call_(t, s.proxy_b, "add", 3, 7), 10)
}

func TestArgPassing_string_arg(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "upper", func(s string) string { return strings.ToUpper(s) })
	assertEq(t, call_(t, s.proxy_b, "upper", "hello"), "HELLO")
}

func TestArgPassing_list_arg(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "length", func(lst *py.List) int { return lst.Len() })
	assertEq(t, call_(t, s.proxy_b, "length", py.NewList(1, 2, 3, 4)), 4)
}

func TestArgPassing_dict_arg(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "keys", func(d *py.Dict) *py.List {
		keys := make([]string, 0, d.Len())
		for _, k := range d.Keys() {
			keys = append(keys, py.Str(k))
		}
		sort.Strings(keys)
		out := py.NewList()
		for _, k := range keys {
			out.Append(k)
		}
		return out
	})
	assertEq(t, call_(t, s.proxy_b, "keys", py.DictOf("b", 2, "a", 1)), py.NewList("a", "b"))
}

func TestArgPassing_kwarg_only(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "kw", func(args ...any) (any, error) {
		_, kw := py.SplitKw(args)
		if v, ok := kw["name"]; ok {
			return v, nil
		}
		return "anon", nil
	})
	assertEq(t, call_(t, s.proxy_b, "kw", py.Kw{"name": "Alice"}), "Alice")
}

func TestArgPassing_mixed_args_and_kwargs(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "format_msg", func(args ...any) (any, error) {
		pos, kw := py.SplitKw(args)
		suffix := "!"
		if v, ok := kw["suffix"]; ok {
			suffix = py.Str(v)
		}
		return py.Str(pos[0]) + ": " + py.Str(pos[1]) + suffix, nil
	})
	assertEq(t, call_(t, s.proxy_b, "format_msg", "INFO", "started", py.Kw{"suffix": "."}), "INFO: started.")
}

func TestArgPassing_no_args(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "constant", func() int { return 99 })
	assertEq(t, call_(t, s.proxy_b, "constant"), 99)
}

func TestArgPassing_none_arg(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "is_none", func(x any) bool { return py.IsNone(x) })
	assertEq(t, call_(t, s.proxy_b, "is_none", nil), true)
}

func TestArgPassing_bool_args(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "logic_and", func(a, b bool) bool { return a && b })
	assertEq(t, call_(t, s.proxy_b, "logic_and", true, true), true)
	assertEq(t, call_(t, s.proxy_b, "logic_and", true, false), false)
}

func TestArgPassing_float_precision(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "half", func(x float64) float64 { return x / 2.0 })
	assertAlmostEq_(t, call_(t, s.proxy_b, "half", 0.1), 0.05)
}

func TestArgPassing_large_string_arg(t *testing.T) {
	s := setUpRPC_(t)
	big := strings.Repeat("x", 50_000)
	attach_(s.B, "echo", func(s string) string { return s })
	assertEq(t, call_(t, s.proxy_b, "echo", big), big)
}

func TestArgPassing_nested_structure_arg(t *testing.T) {
	s := setUpRPC_(t)
	arg := py.DictOf("users", py.NewList(
		py.DictOf("name", "A", "scores", py.NewList(1, 2)),
		py.DictOf("name", "B", "scores", py.NewList(3)),
	))
	attach_(s.B, "count_users", func(d *py.Dict) int { return must(d.Getitem("users")).(*py.List).Len() })
	assertEq(t, call_(t, s.proxy_b, "count_users", arg), 2)
}

func TestArgPassing_many_positional_args(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "sum_all", func(args ...any) (any, error) {
		total := 0
		for _, a := range args {
			n, _ := py.Int(a)
			total += n
		}
		return total, nil
	})
	assertEq(t, call_(t, s.proxy_b, "sum_all", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 55)
}

func TestArgPassing_many_kwargs(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "kw_count", func(args ...any) (any, error) {
		_, kw := py.SplitKw(args)
		return len(kw), nil
	})
	assertEq(t, call_(t, s.proxy_b, "kw_count", py.Kw{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}), 5)
}

// ────────────────────────────────────────────────────────────────────
// 3. Error propagation
// ────────────────────────────────────────────────────────────────────

func TestErrorPropagation_value_error(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "fail_val", func() (any, error) { return nil, py.ValueError.New("bad value") })
	assertIn_(t, "bad value", call_err_(t, s.proxy_b.Attr("fail_val")))
}

func TestErrorPropagation_type_error(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "fail_type", func() (any, error) { return nil, py.TypeError.New("wrong type") })
	assertIn_(t, "TypeError", call_err_(t, s.proxy_b.Attr("fail_type")))
}

func TestErrorPropagation_key_error(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "fail_key", func() (any, error) { return nil, py.KeyError.New("missing_key") })
	assertIn_(t, "KeyError", call_err_(t, s.proxy_b.Attr("fail_key")))
}

func TestErrorPropagation_runtime_error(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "fail_rt", func() (any, error) { return nil, py.RuntimeError.New("custom runtime") })
	assertIn_(t, "custom runtime", call_err_(t, s.proxy_b.Attr("fail_rt")))
}

func TestErrorPropagation_attribute_error_for_missing_path(t *testing.T) {
	s := setUpRPC_(t)
	assertIn_(t, "AttributeError", call_err_(t, s.proxy_b.Attr("nonexistent_attribute_xyz")))
}

func TestErrorPropagation_deeply_nested_missing_path(t *testing.T) {
	s := setUpRPC_(t)
	chain := s.proxy_b.Attr("central").Attr("nonexistent").Attr("deeply").Attr("nested").Attr("method")
	assertIn_(t, "AttributeError", call_err_(t, chain))
}

func TestErrorPropagation_zero_division(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "divide", func(a, b float64) (any, error) { return truediv_(a, b) })
	assertIn_(t, "ZeroDivisionError", call_err_(t, s.proxy_b.Attr("divide"), 1, 0))
}

func TestErrorPropagation_index_error(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "fail_idx", func() (any, error) { return py.NewList().Get(99) })
	assertIn_(t, "IndexError", call_err_(t, s.proxy_b.Attr("fail_idx")))
}

func TestErrorPropagation_assertion_error(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "fail_assert", func() (any, error) { return nil, py.AssertionError.New("assertion failed") })
	assertIn_(t, "assertion failed", call_err_(t, s.proxy_b.Attr("fail_assert")))
}

// ────────────────────────────────────────────────────────────────────
// 4. Bidirectional RPC
// ────────────────────────────────────────────────────────────────────

func TestBidirectional_a_calls_b_and_b_calls_a(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.A, "name", func() string { return "A" })
	attach_(s.B, "name", func() string { return "B" })
	assertEq(t, call_(t, s.proxy_b, "name"), "B")
	assertEq(t, call_(t, s.proxy_a, "name"), "A")
}

func TestBidirectional_both_call_simultaneously(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.A, "slow_a", func() string { sleep(0.05); return "from-a" })
	attach_(s.B, "slow_b", func() string { sleep(0.05); return "from-b" })

	var mu sync.Mutex
	results := map[string]any{}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		v, _ := s.proxy_b.Attr("slow_b").Invoke()
		mu.Lock()
		results["b"] = v
		mu.Unlock()
	}()
	go func() {
		defer wg.Done()
		v, _ := s.proxy_a.Attr("slow_a").Invoke()
		mu.Lock()
		results["a"] = v
		mu.Unlock()
	}()
	wg.Wait()
	assertEq(t, results["b"], "from-b")
	assertEq(t, results["a"], "from-a")
}

func TestBidirectional_a_calls_b_which_reads_a_state(t *testing.T) {
	// B's handler reads local state, proving it runs on B's policy.
	s := setUpRPC_(t)
	attach_(s.B, "my_id", func() string { return s.B.Global_id() })
	assertEq(t, call_(t, s.proxy_b, "my_id"), s.B.Global_id())
}

func TestBidirectional_interleaved_calls(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.A, "echo_a", func(x any) string { return "a:" + py.Str(x) })
	attach_(s.B, "echo_b", func(x any) string { return "b:" + py.Str(x) })
	for i := 0; i < 10; i++ {
		assertEq(t, call_(t, s.proxy_b, "echo_b", i), "b:"+py.Str(i))
		assertEq(t, call_(t, s.proxy_a, "echo_a", i), "a:"+py.Str(i))
	}
}

// ────────────────────────────────────────────────────────────────────
// 5. Deep attribute chains
// ────────────────────────────────────────────────────────────────────

func TestDeepChains_single_level_chain(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "ping", func() string { return "pong" })
	assertEq(t, call_(t, s.proxy_b, "ping"), "pong")
}

func TestDeepChains_two_level_chain(t *testing.T) {
	// proxy_b.central.memory should resolve to B's central.memory.
	s := setUpRPC_(t)
	result := must(s.proxy_b.Attr("central").Attr("memory").Attr("pool_router").Attr("route").Invoke(
		py.Kw{"entries": py.NewList("any"), "pool_nickname": "_memory"}))
	if py.IsNone(result) {
		t.Fatal("route returned None")
	}
}

func TestDeepChains_three_level_chain_pool_router_pools_nicknames(t *testing.T) {
	// Verify we can read nested attributes via RPC chain.
	s := setUpRPC_(t)
	_ = s.proxy_b.Attr("central").Attr("memory").Attr("pool_router").Attr("route").Attr("__self__")
	pools_data := must(s.proxy_b.Attr("central").Attr("memory").Attr("pool_router").Attr("route").Invoke(
		py.Kw{"entries": py.NewList("x"), "pool_nickname": "_memory"}))
	if py.IsNone(pools_data) {
		t.Fatal("route returned None")
	}
}

func TestDeepChains_rpc_chain_is_lazy(t *testing.T) {
	// Attribute access doesn't trigger network I/O until __call__.
	s := setUpRPC_(t)
	chain := must(py.Getattr_path(s.proxy_b, []string{"central", "memory", "pool_router"}))
	if _, ok := chain.(*laila.RemoteAttrChain_); !ok {
		t.Fatalf("%T", chain)
	}
}

func TestDeepChains_chain_repr_includes_path(t *testing.T) {
	s := setUpRPC_(t)
	chain := s.proxy_b.Attr("central").Attr("memory").Attr("pool_router").Attr("route")
	r := py.Repr(chain)
	for _, seg := range []string{"central", "memory", "pool_router", "route"} {
		if !strings.Contains(r, seg) {
			t.Fatal(r)
		}
	}
}

// ────────────────────────────────────────────────────────────────────
// 6. Stateful RPC (modify remote state and read it back)
// ────────────────────────────────────────────────────────────────────

func TestStatefulRPC_mutate_and_read_counter(t *testing.T) {
	s := setUpRPC_(t)
	var mu sync.Mutex
	counter := 0
	attach_(s.B, "increment", func() int { mu.Lock(); defer mu.Unlock(); counter++; return counter })
	attach_(s.B, "get_counter", func() int { mu.Lock(); defer mu.Unlock(); return counter })

	call_(t, s.proxy_b, "increment")
	call_(t, s.proxy_b, "increment")
	call_(t, s.proxy_b, "increment")
	assertEq(t, call_(t, s.proxy_b, "get_counter"), 3)
}

func TestStatefulRPC_append_to_remote_list(t *testing.T) {
	s := setUpRPC_(t)
	var mu sync.Mutex
	items := py.NewList()
	attach_(s.B, "push", func(x any) int { mu.Lock(); defer mu.Unlock(); items.Append(x); return items.Len() })
	attach_(s.B, "get_items", func() *py.List { mu.Lock(); defer mu.Unlock(); return items.Copy() })

	call_(t, s.proxy_b, "push", "alpha")
	call_(t, s.proxy_b, "push", "beta")
	call_(t, s.proxy_b, "push", "gamma")
	assertEq(t, call_(t, s.proxy_b, "get_items"), py.NewList("alpha", "beta", "gamma"))
}

func TestStatefulRPC_set_and_get_dict_entry(t *testing.T) {
	s := setUpRPC_(t)
	var mu sync.Mutex
	store := py.NewDict()
	attach_(s.B, "set_val", func(k, v any) any { mu.Lock(); defer mu.Unlock(); store.Set(k, v); return nil })
	attach_(s.B, "get_val", func(k any) any { mu.Lock(); defer mu.Unlock(); return store.GetDefault(k, nil) })

	call_(t, s.proxy_b, "set_val", "name", "laila")
	call_(t, s.proxy_b, "set_val", "version", 2)
	assertEq(t, call_(t, s.proxy_b, "get_val", "name"), "laila")
	assertEq(t, call_(t, s.proxy_b, "get_val", "version"), 2)
	if v := call_(t, s.proxy_b, "get_val", "missing"); !py.IsNone(v) {
		t.Fatal(py.Repr(v))
	}
}

func TestStatefulRPC_remote_pool_add_and_query(t *testing.T) {
	// Add a named pool on B, then query it from A via RPC.
	s := setUpRPC_(t)
	extra_pool := must(laila.New_LAILA_IDENTIFIABLE_POOL_())
	must(0, laila.As_central_memory_(s.B.Central.Memory).Extend(extra_pool, py.Kw{"pool_nickname": "rpc-test-pool"}))
	result := must(s.proxy_b.Attr("central").Attr("memory").Attr("pool_router").Attr("route").Invoke(
		py.Kw{"entries": py.NewList("any"), "pool_nickname": "rpc-test-pool"}))
	if py.IsNone(result) {
		t.Fatal("route returned None")
	}
}

func TestStatefulRPC_memorize_on_remote_then_check(t *testing.T) {
	// Store an entry on B via direct access, verify it exists via RPC.
	s := setUpRPC_(t)
	pool := must(laila.New_LAILA_IDENTIFIABLE_POOL_())
	must(0, laila.As_central_memory_(s.B.Central.Memory).Extend(pool, py.Kw{"pool_nickname": "b-store"}))

	entry := must(laila.Constant(py.DictOf("msg", "from-a")))
	if err := pool.Setitem(entry.Global_id(), py.DictOf("data", "from-a")); err != nil {
		t.Fatal(err)
	}
	result := must(s.proxy_b.Attr("central").Attr("memory").Attr("pool_router").Attr("route").Invoke(
		py.Kw{"entries": py.NewList(entry.Global_id()), "pool_nickname": "b-store"}))
	if py.IsNone(result) {
		t.Fatal("route returned None")
	}
}

// ────────────────────────────────────────────────────────────────────
// 7. Concurrent RPC
// ────────────────────────────────────────────────────────────────────

func TestConcurrentRPC_parallel_calls_from_multiple_threads(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "square", func(x int) int { return x * x })

	var mu sync.Mutex
	results := map[int]any{}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, _ := s.proxy_b.Attr("square").Invoke(i)
			mu.Lock()
			results[i] = v
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	for i := 0; i < 20; i++ {
		assertEq(t, results[i], i*i)
	}
}

func TestConcurrentRPC_rapid_sequential_calls(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "identity", func(x any) any { return x })
	for i := 0; i < 50; i++ {
		assertEq(t, call_(t, s.proxy_b, "identity", i), i)
	}
}

func TestConcurrentRPC_threadpool_executor_calls(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "double", func(x int) int { return x * 2 })
	pool := py.New_ThreadPoolExecutor(8)
	futures := make([]*py.CFuture, 0, 30)
	for i := 0; i < 30; i++ {
		i := i
		futures = append(futures, must(pool.Submit(func() (any, error) { return s.proxy_b.Attr("double").Invoke(i) })))
	}
	results := py.NewList()
	for _, f := range futures {
		results.Append(must(f.Result(10)))
	}
	pool.Shutdown(true)
	want := py.NewList()
	for i := 0; i < 30; i++ {
		want.Append(i * 2)
	}
	assertEq(t, results, want)
}

func TestConcurrentRPC_concurrent_bidirectional(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.A, "from_a", func(x any) string { return "a:" + py.Str(x) })
	attach_(s.B, "from_b", func(x any) string { return "b:" + py.Str(x) })

	var mu sync.Mutex
	results_a := map[int]any{}
	results_b := map[int]any{}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			v, _ := s.proxy_b.Attr("from_b").Invoke(i)
			mu.Lock()
			results_b[i] = v
			mu.Unlock()
		}(i)
		go func(i int) {
			defer wg.Done()
			v, _ := s.proxy_a.Attr("from_a").Invoke(i)
			mu.Lock()
			results_a[i] = v
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	for i := 0; i < 10; i++ {
		assertEq(t, results_b[i], "b:"+py.Str(i))
		assertEq(t, results_a[i], "a:"+py.Str(i))
	}
}

// ────────────────────────────────────────────────────────────────────
// 8. Serialization edge cases
// ────────────────────────────────────────────────────────────────────

func echo_any_(x any) any { return x }

// truediv_ is ``a / b`` with Python's ZeroDivisionError.
func truediv_(a, b float64) (any, error) {
	if b == 0 {
		return nil, py.ZeroDivisionError.New("division by zero")
	}
	return a / b, nil
}

// sorted_list_ is ``sorted(lst)``.
func sorted_list_(lst *py.List) (*py.List, error) {
	items, err := py.Sorted(lst.Items())
	if err != nil {
		return nil, err
	}
	return py.NewList(items...), nil
}

func TestSerializationEdgeCases_special_json_chars_in_string(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "echo", echo_any_)
	special := "line1\nline2\ttab \"quotes\" \\backslash"
	assertEq(t, call_(t, s.proxy_b, "echo", special), special)
}

func TestSerializationEdgeCases_null_bytes_in_string(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "echo", echo_any_)
	assertEq(t, call_(t, s.proxy_b, "echo", "a\x00b"), "a\x00b")
}

func TestSerializationEdgeCases_very_nested_structure(t *testing.T) {
	s := setUpRPC_(t)
	d := py.DictOf("a", py.DictOf("b", py.DictOf("c", py.DictOf("d", py.DictOf("e", py.DictOf("f", "deep"))))))
	attach_(s.B, "echo", echo_any_)
	assertEq(t, call_(t, s.proxy_b, "echo", d), d)
}

func TestSerializationEdgeCases_list_of_mixed_types(t *testing.T) {
	s := setUpRPC_(t)
	lst := py.NewList(1, "two", 3.0, true, nil, py.NewList(4), py.DictOf("five", 5))
	attach_(s.B, "echo", echo_any_)
	assertEq(t, call_(t, s.proxy_b, "echo", lst), lst)
}

func TestSerializationEdgeCases_empty_args_and_kwargs(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "noop", func() string { return "ok" })
	assertEq(t, call_(t, s.proxy_b, "noop"), "ok")
}

func range_list_(n int) *py.List {
	out := py.NewList()
	for i := 0; i < n; i++ {
		out.Append(i)
	}
	return out
}

func TestSerializationEdgeCases_large_payload(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "length", func(x *py.List) int { return x.Len() })
	assertEq(t, call_(t, s.proxy_b, "length", range_list_(5000)), 5000)
}

func TestSerializationEdgeCases_return_large_payload(t *testing.T) {
	s := setUpRPC_(t)
	big_list := range_list_(5000)
	attach_(s.B, "big", func() *py.List { return big_list })
	assertEq(t, call_(t, s.proxy_b, "big"), big_list)
}

func TestSerializationEdgeCases_dict_with_numeric_string_keys(t *testing.T) {
	s := setUpRPC_(t)
	d := py.DictOf("0", "zero", "1", "one", "999", "many")
	attach_(s.B, "echo", echo_any_)
	assertEq(t, call_(t, s.proxy_b, "echo", d), d)
}

func TestSerializationEdgeCases_unicode_keys_and_values(t *testing.T) {
	s := setUpRPC_(t)
	d := py.DictOf("键", "值", "clé", "valeur", "キー", "バリュー")
	attach_(s.B, "echo", echo_any_)
	assertEq(t, call_(t, s.proxy_b, "echo", d), d)
}

func TestSerializationEdgeCases_empty_nested_structures(t *testing.T) {
	s := setUpRPC_(t)
	d := py.DictOf("a", py.NewDict(), "b", py.NewList(), "c", py.DictOf("d", py.NewList()))
	attach_(s.B, "echo", echo_any_)
	assertEq(t, call_(t, s.proxy_b, "echo", d), d)
}

// ────────────────────────────────────────────────────────────────────
// 9. Three-policy mesh
// ────────────────────────────────────────────────────────────────────

type mesh_ struct{ A, B, C *laila.LAILA_IDENTIFIABLE_POLICY_ }

func setUpMesh_(t *testing.T) *mesh_ {
	t.Helper()
	m := &mesh_{A: make_policy(t), B: make_policy(t), C: make_policy(t)}
	peer(t, m.A, m.B)
	peer(t, m.A, m.C)
	peer(t, m.B, m.C)
	sleep(0.5)
	t.Cleanup(func() { stopAll(m.A, m.B, m.C) })
	return m
}

func peers_(t *testing.T, from, to *laila.LAILA_IDENTIFIABLE_POLICY_) *laila.PeerProxy {
	t.Helper()
	return must(comm(from).Peers.Getitem(to.Global_id())).(*laila.PeerProxy)
}

func TestThreePolicyMesh_a_calls_b_and_c(t *testing.T) {
	m := setUpMesh_(t)
	attach_(m.B, "who", func() string { return "B" })
	attach_(m.C, "who", func() string { return "C" })
	assertEq(t, call_(t, peers_(t, m.A, m.B), "who"), "B")
	assertEq(t, call_(t, peers_(t, m.A, m.C), "who"), "C")
}

func TestThreePolicyMesh_b_calls_a_and_c(t *testing.T) {
	m := setUpMesh_(t)
	attach_(m.A, "who", func() string { return "A" })
	attach_(m.C, "who", func() string { return "C" })
	assertEq(t, call_(t, peers_(t, m.B, m.A), "who"), "A")
	assertEq(t, call_(t, peers_(t, m.B, m.C), "who"), "C")
}

func TestThreePolicyMesh_c_calls_a_and_b(t *testing.T) {
	m := setUpMesh_(t)
	attach_(m.A, "who", func() string { return "A" })
	attach_(m.B, "who", func() string { return "B" })
	assertEq(t, call_(t, peers_(t, m.C, m.A), "who"), "A")
	assertEq(t, call_(t, peers_(t, m.C, m.B), "who"), "B")
}

func TestThreePolicyMesh_aggregated_results_from_b_and_c(t *testing.T) {
	// A collects results from B and C and combines them locally.
	m := setUpMesh_(t)
	attach_(m.B, "value", func() int { return 10 })
	attach_(m.C, "value", func() int { return 20 })
	vb, _ := py.Int(call_(t, peers_(t, m.A, m.B), "value"))
	vc, _ := py.Int(call_(t, peers_(t, m.A, m.C), "value"))
	assertEq(t, vb+vc, 30)
}

func TestThreePolicyMesh_fan_out_from_a(t *testing.T) {
	// A calls B and C in parallel threads.
	m := setUpMesh_(t)
	attach_(m.B, "slow", func() string { sleep(0.05); return "B" })
	attach_(m.C, "slow", func() string { sleep(0.05); return "C" })
	proxy_b, proxy_c := peers_(t, m.A, m.B), peers_(t, m.A, m.C)

	var mu sync.Mutex
	results := map[string]any{}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		v, _ := proxy_b.Attr("slow").Invoke()
		mu.Lock()
		results["b"] = v
		mu.Unlock()
	}()
	go func() {
		defer wg.Done()
		v, _ := proxy_c.Attr("slow").Invoke()
		mu.Lock()
		results["c"] = v
		mu.Unlock()
	}()
	wg.Wait()
	assertEq(t, results["b"], "B")
	assertEq(t, results["c"], "C")
}

// ────────────────────────────────────────────────────────────────────
// 10. Connection error handling
// ────────────────────────────────────────────────────────────────────

func TestConnectionErrors_send_rpc_to_unknown_peer(t *testing.T) {
	p := make_policy(t)
	if err := comm(p).Start(); err != nil {
		t.Fatal(err)
	}
	defer stopAll(p)
	_, err := comm(p).Send_rpc_("FAKE_PEER_ID", []string{"foo"}, nil, nil, nil)
	assertRaises(t, py.ConnectionError, err)
}

func TestConnectionErrors_add_peer_bad_uri_raises(t *testing.T) {
	p := make_policy(t)
	if err := comm(p).Start(); err != nil {
		t.Fatal(err)
	}
	defer stopAll(p)
	if _, err := comm(p).Add_tcpip_peer("127.0.0.1", 1, "secret"); err == nil {
		t.Fatal("expected an exception")
	}
}

func TestConnectionErrors_proxy_after_disconnect(t *testing.T) {
	// After stopping B, calls from A's proxy should fail.
	A, B := make_policy(t), make_policy(t)
	peer(t, A, B)
	sleep(0.3)

	attach_(B, "ping", func() string { return "pong" })
	proxy_b := peers_(t, A, B)
	assertEq(t, call_(t, proxy_b, "ping"), "pong")

	stopAll(B)
	sleep(0.5)

	_, err := proxy_b.Attr("ping").Invoke()
	if err == nil || !(py.IsInstance(err, py.ConnectionError) || py.IsInstance(err, py.RuntimeError)) {
		t.Fatalf("expected ConnectionError/RuntimeError, got %v", err)
	}
	stopAll(A)
}

// ────────────────────────────────────────────────────────────────────
// 11. Real policy tree traversal via RPC
// ────────────────────────────────────────────────────────────────────

func TestRealPolicyTreeRPC_read_global_id_via_proxy(t *testing.T) {
	// Access the remote policy's global_id property.
	s := setUpRPC_(t)
	assertEq(t, must(py.Getattr(s.proxy_b, "global_id")), s.B.Global_id())
}

func TestRealPolicyTreeRPC_route_default_pool_via_proxy(t *testing.T) {
	s := setUpRPC_(t)
	result := must(s.proxy_b.Attr("central").Attr("memory").Attr("pool_router").Attr("route").Invoke(
		py.Kw{"entries": py.NewList("dummy"), "pool_nickname": "_memory"}))
	if py.IsNone(result) {
		t.Fatal("route returned None")
	}
}

func TestRealPolicyTreeRPC_extend_on_remote_via_rpc(t *testing.T) {
	// Add a pool on B locally, verify via proxy it's routeable.
	s := setUpRPC_(t)
	pool := must(laila.New_LAILA_IDENTIFIABLE_POOL_())
	must(0, laila.As_central_memory_(s.B.Central.Memory).Extend(pool, py.Kw{"pool_nickname": "rpc-added"}))
	result := must(s.proxy_b.Attr("central").Attr("memory").Attr("pool_router").Attr("route").Invoke(
		py.Kw{"entries": py.NewList("x"), "pool_nickname": "rpc-added"}))
	if py.IsNone(result) {
		t.Fatal("route returned None")
	}
}

func TestRealPolicyTreeRPC_read_remote_command_alpha_taskforce(t *testing.T) {
	// Reading a plain attribute requires a helper; the proxy only fires
	// RPC on __call__.
	s := setUpRPC_(t)
	attach_(s.B, "get_alpha_tf", func() any { return s.B.Central.Command.(*laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_).Alpha_taskforce })
	tf_id := call_(t, s.proxy_b, "get_alpha_tf")
	if _, ok := tf_id.(string); !ok || py.IsNone(tf_id) {
		t.Fatalf("expected str, got %s", py.Repr(tf_id))
	}
}

func TestRealPolicyTreeRPC_read_remote_pool_router_pools_nicknames(t *testing.T) {
	s := setUpRPC_(t)
	nicknames := s.proxy_b.Attr("central").Attr("memory").Attr("pool_router").Attr("pools_nicknames")
	if nicknames == nil {
		t.Fatal("nil chain")
	}
}

func TestRealPolicyTreeRPC_count_remote_taskforces(t *testing.T) {
	// B's command has at least one taskforce by default.
	s := setUpRPC_(t)
	attach_(s.B, "tf_count", func() int { return s.B.Central.Command.(*laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_).Taskforces.Len() })
	n, _ := py.Int(call_(t, s.proxy_b, "tf_count"))
	if n < 1 {
		t.Fatalf("tf_count = %d", n)
	}
}

// ────────────────────────────────────────────────────────────────────
// 12. Mathematical / computation RPC
// ────────────────────────────────────────────────────────────────────

func TestComputationRPC_fibonacci(t *testing.T) {
	s := setUpRPC_(t)
	fib := func(n int) int {
		a, b := 0, 1
		for i := 0; i < n; i++ {
			a, b = b, a+b
		}
		return a
	}
	attach_(s.B, "fib", fib)
	assertEq(t, call_(t, s.proxy_b, "fib", 10), 55)
	assertEq(t, call_(t, s.proxy_b, "fib", 0), 0)
	assertEq(t, call_(t, s.proxy_b, "fib", 1), 1)
}

func TestComputationRPC_matrix_multiply(t *testing.T) {
	s := setUpRPC_(t)
	matmul := func(a, b *py.List) *py.List {
		n := a.Len()
		get := func(m *py.List, i, j int) int {
			v, _ := py.Int(must(must(m.Get(i)).(*py.List).Get(j)))
			return v
		}
		result := py.NewList()
		for i := 0; i < n; i++ {
			row := py.NewList()
			for j := 0; j < n; j++ {
				sum := 0
				for k := 0; k < n; k++ {
					sum += get(a, i, k) * get(b, k, j)
				}
				row.Append(sum)
			}
			result.Append(row)
		}
		return result
	}
	attach_(s.B, "matmul", matmul)
	I := py.NewList(py.NewList(1, 0), py.NewList(0, 1))
	A := py.NewList(py.NewList(1, 2), py.NewList(3, 4))
	assertEq(t, call_(t, s.proxy_b, "matmul", A, I), A)
	assertEq(t, call_(t, s.proxy_b, "matmul", I, A), A)
}

func TestComputationRPC_sorting(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "sort", func(lst *py.List) (*py.List, error) { return sorted_list_(lst) })
	assertEq(t, call_(t, s.proxy_b, "sort", py.NewList(3, 1, 4, 1, 5, 9, 2, 6)), py.NewList(1, 1, 2, 3, 4, 5, 6, 9))
}

func TestComputationRPC_string_processing(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "process", func(str string) *py.Dict {
		words := py.NewList()
		for _, w := range strings.Fields(str) {
			words.Append(w)
		}
		runes := []rune(str)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return py.DictOf("length", len(runes), "words", words, "reversed", string(runes), "upper", strings.ToUpper(str))
	})
	result := call_(t, s.proxy_b, "process", "hello world").(*py.Dict)
	assertEq(t, must(result.Getitem("length")), 11)
	assertEq(t, must(result.Getitem("words")), py.NewList("hello", "world"))
	assertEq(t, must(result.Getitem("reversed")), "dlrow olleh")
	assertEq(t, must(result.Getitem("upper")), "HELLO WORLD")
}

func TestComputationRPC_reduce_list(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "product", func(lst *py.List) int {
		acc := 1
		for _, v := range lst.Items() {
			n, _ := py.Int(v)
			acc *= n
		}
		return acc
	})
	assertEq(t, call_(t, s.proxy_b, "product", py.NewList(1, 2, 3, 4, 5)), 120)
}

func TestComputationRPC_set_operations_via_lists(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "intersect", func(a, b *py.List) (*py.List, error) {
		in_b := map[int]bool{}
		for _, v := range b.Items() {
			n, _ := py.Int(v)
			in_b[n] = true
		}
		out := py.NewList()
		seen := map[int]bool{}
		for _, v := range a.Items() {
			n, _ := py.Int(v)
			if in_b[n] && !seen[n] {
				seen[n] = true
				out.Append(n)
			}
		}
		return sorted_list_(out)
	})
	assertEq(t, call_(t, s.proxy_b, "intersect", py.NewList(1, 2, 3, 4), py.NewList(3, 4, 5, 6)), py.NewList(3, 4))
}

// ────────────────────────────────────────────────────────────────────
// 13. Idempotency and repeated calls
// ────────────────────────────────────────────────────────────────────

func TestIdempotency_same_call_100_times(t *testing.T) {
	s := setUpRPC_(t)
	attach_(s.B, "echo", echo_any_)
	for i := 0; i < 100; i++ {
		assertEq(t, call_(t, s.proxy_b, "echo", i), i)
	}
}

func TestIdempotency_alternating_success_and_failure(t *testing.T) {
	s := setUpRPC_(t)
	var mu sync.Mutex
	n := 0
	attach_(s.B, "flaky", func() (any, error) {
		mu.Lock()
		defer mu.Unlock()
		n++
		if n%2 == 0 {
			return nil, py.ValueError.New("even call failed")
		}
		return "ok", nil
	})
	for i := 0; i < 10; i++ {
		if (i+1)%2 == 0 {
			call_err_(t, s.proxy_b.Attr("flaky"))
		} else {
			assertEq(t, call_(t, s.proxy_b, "flaky"), "ok")
		}
	}
}

func TestIdempotency_connection_survives_many_errors(t *testing.T) {
	// The RPC channel stays healthy even after repeated remote exceptions.
	s := setUpRPC_(t)
	attach_(s.B, "boom", func() (any, error) { return truediv_(1, 0) })
	attach_(s.B, "ok", func() string { return "healthy" })
	for i := 0; i < 20; i++ {
		call_err_(t, s.proxy_b.Attr("boom"))
	}
	assertEq(t, call_(t, s.proxy_b, "ok"), "healthy")
}
