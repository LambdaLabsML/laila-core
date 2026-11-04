// Port of tests/functional/compdata/edge_cases/unit_tests/test_compdata_edge_cases.py.
package unit_tests

import (
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

func cd(v any) laila.Computational_data { return must(laila.New_ComputationalData(v)) }

func data(c laila.Computational_data) any { return c.Base_().Data }

// buildSimple runs a SimpleConstitution chain and wraps the result in a
// ComputationalData.
func buildSimple(payload any, codes []string) (laila.Computational_data, error) {
	sc, err := laila.New_SimpleConstitution(py.Kw{"codes": codes})
	if err != nil {
		return nil, err
	}
	result, err := sc.Build(payload)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}
	if c, ok := result.(laila.Computational_data); ok {
		return c, nil
	}
	return laila.New_ComputationalData(result)
}

func serialize(c laila.Computational_data) (any, string) {
	payload, code, err := c.Serialize()
	if err != nil {
		panic(err)
	}
	return payload, code
}

func TestComputationalDataFactory(t *testing.T) {
	t.Run("string_data", func(t *testing.T) { assertEq(t, data(cd("hello")), "hello") })
	t.Run("int_data", func(t *testing.T) { assertEq(t, data(cd(42)), 42) })
	t.Run("float_data", func(t *testing.T) { assertEq(t, data(cd(3.14)), 3.14) })
	t.Run("dict_data", func(t *testing.T) {
		assertEq(t, data(cd(py.DictOf("key", "value"))), py.DictOf("key", "value"))
	})
	t.Run("list_data", func(t *testing.T) { assertEq(t, data(cd(py.NewList(1, 2, 3))), py.NewList(1, 2, 3)) })
	t.Run("bool_data", func(t *testing.T) {
		if data(cd(true)) != true {
			t.Fatal("bool identity")
		}
	})
	t.Run("bytes_data", func(t *testing.T) { assertEq(t, data(cd([]byte("binary"))), []byte("binary")) })
	t.Run("tuple_data", func(t *testing.T) { assertEq(t, data(cd(py.Tuple{1, 2, 3})), py.Tuple{1, 2, 3}) })
	t.Run("nested_dict", func(t *testing.T) {
		d := py.DictOf("a", py.DictOf("b", py.DictOf("c", py.NewList(1, 2))))
		assertEq(t, data(cd(d)), d)
	})
	t.Run("none_data_raises", func(t *testing.T) {
		_, err := laila.New_ComputationalData(nil)
		assertRaises(t, py.TypeError, err)
	})
	t.Run("no_args_raises", func(t *testing.T) {
		_, err := laila.New_ComputationalData()
		assertRaises(t, py.TypeError, err)
	})
	t.Run("two_positional_args_raises", func(t *testing.T) {
		_, err := laila.New_ComputationalData(1, 2)
		assertRaises(t, py.TypeError, err)
	})
	t.Run("positional_and_keyword_raises", func(t *testing.T) {
		_, err := laila.New_ComputationalData(1, py.Kw{"data": 2})
		assertRaises(t, py.TypeError, err)
	})
	t.Run("keyword_data", func(t *testing.T) {
		assertEq(t, data(must(laila.New_ComputationalData(py.Kw{"data": "kw"}))), "kw")
	})
}

func TestComputationalDataSerialize(t *testing.T) {
	for name, v := range map[string]any{
		"string": "test", "dict": py.DictOf("a", 1), "int": 42, "list": py.NewList(1, 2, 3), "empty_string": "",
	} {
		t.Run("serialize_"+name, func(t *testing.T) {
			payload, _ := serialize(cd(v))
			if payload == nil {
				t.Fatal("payload is None")
			}
		})
	}
}

func TestComputationalDataRecover(t *testing.T) {
	rt := func(v any) any {
		payload, code := serialize(cd(v))
		return data(must(buildSimple(payload, []string{code})))
	}
	t.Run("round_trip_string", func(t *testing.T) { assertEq(t, rt("hello"), "hello") })
	t.Run("round_trip_dict", func(t *testing.T) {
		assertEq(t, rt(py.DictOf("k", py.NewList(1, 2))), py.DictOf("k", py.NewList(1, 2)))
	})
	t.Run("round_trip_int", func(t *testing.T) { assertEq(t, rt(42), 42) })
	t.Run("round_trip_list", func(t *testing.T) {
		assertEq(t, rt(py.NewList(1, "two", 3.0)), py.NewList(1, "two", 3.0))
	})
	t.Run("round_trip_bool", func(t *testing.T) {
		if rt(false) != false {
			t.Fatal("bool identity")
		}
	})
	t.Run("recover_empty_sequence", func(t *testing.T) {
		result := must(buildSimple("raw-data", []string{}))
		if result == nil {
			t.Fatal("expected a ComputationalData")
		}
		assertEq(t, data(result), "raw-data")
	})
	t.Run("recover_none_payload_empty_sequence", func(t *testing.T) {
		if result := must(buildSimple(nil, []string{})); result != nil {
			t.Fatal("expected None")
		}
	})
	t.Run("recover_bad_code_raises", func(t *testing.T) {
		_, err := buildSimple([]byte("data"), []string{"this is not valid python"})
		if err == nil {
			t.Fatal("expected an exception")
		}
	})
	t.Run("recover_code_with_zero_callables_raises", func(t *testing.T) {
		_, err := buildSimple([]byte("data"), []string{"x = 42"})
		assertRaises(t, py.ValueError, err)
	})
	t.Run("recover_code_with_two_callables_raises", func(t *testing.T) {
		_, err := buildSimple([]byte("data"), []string{"def a(x): return x\ndef b(x): return x"})
		assertRaises(t, py.ValueError, err)
	})
}

func TestComputationalDataMisc(t *testing.T) {
	t.Run("repr", func(t *testing.T) {
		if !strings.Contains(py.Repr(cd("test")), "test") {
			t.Fatal("repr")
		}
	})
	t.Run("str", func(t *testing.T) {
		if !strings.Contains(py.Str(cd(42)), "42") {
			t.Fatal("str")
		}
	})
	t.Run("getitem", func(t *testing.T) {
		c := cd(py.NewList(10, 20, 30))
		assertEq(t, must(c.Getitem(0)), 10)
		assertEq(t, must(c.Getitem(2)), 30)
	})
	t.Run("getitem_dict", func(t *testing.T) {
		assertEq(t, must(cd(py.DictOf("a", 1, "b", 2)).Getitem("a")), 1)
	})
	t.Run("getitem_out_of_range_raises", func(t *testing.T) {
		_, err := cd(py.NewList(1)).Getitem(10)
		assertRaises(t, py.IndexError, err)
	})
	t.Run("getitem_bad_key_raises", func(t *testing.T) {
		_, err := cd(py.DictOf("a", 1)).Getitem("z")
		assertRaises(t, py.KeyError, err)
	})
	t.Run("serializer_property", func(t *testing.T) {
		if cd("x").Serializer() == nil {
			t.Fatal("serializer is None")
		}
	})
	t.Run("serializer_setter_wrong_type_raises", func(t *testing.T) {
		assertRaises(t, py.TypeError, cd("x").Set_serializer("not-a-serializer"))
	})
	t.Run("large_data_round_trip", func(t *testing.T) {
		items := make([]any, 10_000)
		for i := range items {
			items[i] = i
		}
		big := py.NewList(items...)
		payload, code := serialize(cd(big))
		assertEq(t, data(must(buildSimple(payload, []string{code}))), big)
	})
}
