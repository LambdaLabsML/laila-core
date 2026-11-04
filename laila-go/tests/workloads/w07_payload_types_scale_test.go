//go:build workload

// Port of tests/workloads/test_w07_payload_types_scale.py: payload
// fidelity and size limits through every serialisation path: raw
// in-memory, base64, base64+zlib, encrypted and SQLite-backed pools, for
// scalars, containers, numpy arrays of every flavour, images, and
// multi-hundred-MB blobs.
//
// Payloads whose Python type has no twin in the port's value model
// (``datetime``, ``Decimal``, ``Fraction``, ``uuid.UUID``, ``deque``,
// ``range``, ``OrderedDict``, object-dtype arrays, PIL images) are skipped
// with that reason, the way ``pytest.importorskip`` drops the PIL cases.
package workloads

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// Custom is the test module's ``class Custom`` (pickled through
// ``__reduce__``; the Go twin registers its constructor the way the
// Python module's class is importable by the unpickler).
type Custom struct{ X any }

const custom_global_ = "test_w07_payload_types_scale.Custom"

func (c *Custom) Reduce() (string, py.Tuple) { return custom_global_, py.Tuple{c.X} }

func (c *Custom) Pytype() string { return "Custom" }

func init() {
	py.Global_constructor[custom_global_] = func(args py.Tuple) (any, error) {
		if len(args) != 1 {
			return nil, py.TypeError.New("Custom() takes exactly one argument")
		}
		return &Custom{X: args[0]}, nil
	}
}

// eq is the test module's ``eq(a, b)``: type-strict structural equality
// with NaN-aware floats and numpy ``array_equal`` semantics.
func eq(a, b any) bool {
	switch x := a.(type) {
	case *py.NDArray:
		y, ok := b.(*py.NDArray)
		return ok && x.Eq(y)
	case float64:
		y, ok := b.(float64)
		if !ok {
			return false
		}
		if math.IsNaN(x) {
			return math.IsNaN(y)
		}
		return x == y
	case *py.Dict:
		y, ok := b.(*py.Dict)
		if !ok || x.Len() != y.Len() {
			return false
		}
		for _, it := range x.Items() {
			v, ok := y.Get(it.Key)
			if !ok || !eq(it.Value, v) {
				return false
			}
		}
		return true
	case *py.List:
		y, ok := b.(*py.List)
		if !ok || x.Len() != y.Len() {
			return false
		}
		for i, v := range x.Items() {
			if !eq(v, must(y.Get(i))) {
				return false
			}
		}
		return true
	case py.Tuple:
		y, ok := b.(py.Tuple)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !eq(x[i], y[i]) {
				return false
			}
		}
		return true
	case py.FrozenSet:
		_, ok := b.(py.FrozenSet)
		return ok && py.Eq(a, b)
	case *py.Set:
		_, ok := b.(*py.Set)
		return ok && py.Eq(a, b)
	case *Custom:
		y, ok := b.(*Custom)
		return ok && eq(x.X, y.X)
	case bool:
		_, ok := b.(bool)
		return ok && py.Eq(a, b)
	case string:
		_, ok := b.(string)
		return ok && a == b
	case []byte:
		_, ok := b.([]byte)
		return ok && py.Eq(a, b)
	case nil:
		return b == nil
	}
	if _, ok := b.(bool); ok {
		return false
	}
	return py.Eq(a, b)
}

var POOL_CONFIGS = []string{"mem-raw", "mem-b64", "mem-b64-zlib", "mem-b64-zlib-enc", "sqlite"}

// pool_cfg is the ``pool_cfg`` fixture: a pool with the given
// transformation chain registered under nickname ``t``.
func pool_cfg(t *testing.T, cfg string) *Pool {
	t.Helper()
	policy := Fresh_policy(t)
	var raw any
	switch cfg {
	case "mem-raw":
		raw = must(laila.New_DefaultPool())
	case "mem-b64":
		raw = must(laila.New_DefaultPool(py.Kw{"transformations": laila.Transformation_base64}))
	case "mem-b64-zlib":
		raw = must(laila.New_DefaultPool(py.Kw{"transformations": laila.Transformation_base64_compression}))
	case "mem-b64-zlib-enc":
		laila.Set_encryption_key(py.Fernet_generate_key())
		raw = must(laila.New_DefaultPool(py.Kw{"transformations": must(laila.Transformation_base64_compression_encryption())}))
	default:
		raw = must(Make_pool("sqlite", Disk_tmp(t)))
	}
	t.Cleanup(func() { Close_pool(raw) })
	mem_extend(t, policy, raw, "t")
	return laila.As_pool_(raw)
}

func for_cfgs(t *testing.T, body func(t *testing.T, cfg string, pool *Pool)) {
	t.Helper()
	for _, cfg := range POOL_CONFIGS {
		cfg := cfg
		t.Run("cfg="+cfg, func(t *testing.T) { body(t, cfg, pool_cfg(t, cfg)) })
	}
}

// roundtrip is ``roundtrip(val, nickname="t")``.
func roundtrip(val any, nickname ...string) (any, error) {
	nick := "t"
	if len(nickname) > 0 {
		nick = nickname[0]
	}
	e, err := laila.Constant(val)
	if err != nil {
		return nil, err
	}
	f, err := laila.Memorize(e, py.Kw{"pool_nickname": nick})
	if err != nil {
		return nil, err
	}
	if _, err := Wait(f, 120); err != nil {
		return nil, err
	}
	g, err := laila.Remember(e.Global_id(), py.Kw{"pool_nickname": nick, "persist": false})
	if err != nil {
		return nil, err
	}
	got, err := Wait(g, 120)
	if err != nil {
		return nil, err
	}
	return Unwrap(got), nil
}

// roundtrip_alpha_ is ``_roundtrip_alpha(val)``.
func roundtrip_alpha_(val any) (any, error) {
	e, err := laila.Constant(val)
	if err != nil {
		return nil, err
	}
	if _, err := Wait(must(laila.Memorize(e)), 120); err != nil {
		return nil, err
	}
	got, err := Wait(must(laila.Remember(e.Global_id(), py.Kw{"persist": false})), 120)
	if err != nil {
		return nil, err
	}
	return Unwrap(got), nil
}

// roundtrip_id is ``roundtrip_id(gid, nickname="t")``.
func roundtrip_id(gid string) (any, error) {
	got, err := Wait(must(laila.Remember(gid, py.Kw{"pool_nickname": "t", "persist": false})), 300)
	if err != nil {
		return nil, err
	}
	return Unwrap(got), nil
}

// ---------------------------------------------------------------------------
// payload tables
// ---------------------------------------------------------------------------

// skipped marks a payload whose Python type has no Go twin.
type skipped string

func arange_f64(n int) *py.NDArray {
	d := make([]float64, n)
	for i := range d {
		d[i] = float64(i)
	}
	return py.NDArray_float64([]int{n}, d)
}

func arange_i64(n int) *py.NDArray {
	d := make([]int64, n)
	for i := range d {
		d[i] = int64(i)
	}
	return py.NDArray_int64([]int{n}, d)
}

func ones_f64(shape []int, fortran bool) *py.NDArray {
	n := 1
	for _, s := range shape {
		n *= s
	}
	d := make([]float64, n)
	for i := range d {
		d[i] = 1
	}
	a := py.NDArray_float64(shape, d)
	a.Fortran_order = fortran
	return a
}

func SCALARS() map[string]any {
	return map[string]any{
		"int":         1,
		"big_int":     new(big.Int).Lsh(big.NewInt(1), 70),
		"neg":         -5,
		"float":       2.5,
		"nan":         math.NaN(),
		"inf":         math.Inf(1),
		"str":         "héllo ✓",
		"bytes":       []byte{0x00, 0x01, 0xff},
		"none":        nil,
		"bool":        true,
		"empty_str":   "",
		"empty_bytes": []byte{},
	}
}

func CONTAINERS() map[string]any {
	ints := py.NewList()
	for i := 0; i < 1000; i++ {
		ints.Append(i)
	}
	return map[string]any{
		"list_ints":        ints,
		"list_mixed":       py.NewList(1, "a", nil, 2.5, []byte("x")),
		"nested_list":      py.NewList(py.NewList(1, py.NewList(2, py.NewList(3, py.NewList(4))))),
		"tuple":            py.Tuple{1, 2, 3},
		"tuple_nested":     py.Tuple{py.Tuple{1, 2}, py.Tuple{3, py.Tuple{4}}},
		"set":              py.NewSet(1, 2, 3),
		"frozenset":        py.FrozenSet{Set: py.NewSet(1)},
		"dict_str":         py.DictOf("a", 1, "b", py.NewList(1, 2)),
		"dict_int_keys":    py.DictOf(1, "a", 2, "b"),
		"dict_bytes_val":   py.DictOf("k", []byte{0x00}),
		"dict_with_numpy":  py.DictOf("arr", arange_i64(5)),
		"dict_nested_deep": py.DictOf("a", py.DictOf("b", py.DictOf("c", py.DictOf("d", py.DictOf("e", 1))))),
		"dict_mixed_keys":  py.DictOf(1, "a", "1", "b"),
		"dict_float_key":   py.DictOf(1.5, "x"),
		"empty_dict":       py.NewDict(),
		"empty_list":       py.NewList(),
		"list_of_arrays":   py.NewList(arange_i64(3), arange_f64(4)),
	}
}

func NUMPY() map[string]any {
	i8 := make([]byte, 10)
	for i := range i8 {
		i8[i] = byte(i)
	}
	u64 := make([]byte, 80)
	for i := 0; i < 10; i++ {
		binary.LittleEndian.PutUint64(u64[i*8:], uint64(i))
	}
	structured := make([]byte, 12)
	binary.LittleEndian.PutUint32(structured, 1)
	binary.LittleEndian.PutUint64(structured[4:], math.Float64bits(2.0))
	unicode := make([]byte, 16)
	binary.LittleEndian.PutUint32(unicode, 'a')
	binary.LittleEndian.PutUint32(unicode[8:], 'b')
	binary.LittleEndian.PutUint32(unicode[12:], 'b')
	noncontig := make([]int64, 10)
	for i := range noncontig {
		noncontig[i] = int64(2 * i)
	}
	cplx := make([]byte, 16)
	binary.LittleEndian.PutUint64(cplx, math.Float64bits(1))
	binary.LittleEndian.PutUint64(cplx[8:], math.Float64bits(2))
	f16 := make([]byte, 10)
	for i, h := range []uint16{0x0000, 0x3c00, 0x4000, 0x4200, 0x4400} { // 0,1,2,3,4
		binary.LittleEndian.PutUint16(f16[i*2:], h)
	}
	return map[string]any{
		"f64":        arange_f64(10),
		"i8":         &py.NDArray{Dtype: "|i1", Shape: []int{10}, Data: i8},
		"u64":        &py.NDArray{Dtype: "<u8", Shape: []int{10}, Data: u64},
		"bool":       py.NDArray_bool([]int{2}, []bool{true, false}),
		"2d":         ones_f64([]int{3, 4}, false),
		"0d":         py.NDArray_int64([]int{}, []int64{5}),
		"structured": &py.NDArray{Dtype: "[('a', '<i4'), ('b', '<f8')]", Shape: []int{1}, Data: structured},
		"object":     skipped("object-dtype arrays hold Python objects; no Go twin"),
		"unicode":    &py.NDArray{Dtype: "<U2", Shape: []int{2}, Data: unicode},
		"empty":      py.NDArray_float64([]int{0}, []float64{}),
		"fortran":    ones_f64([]int{3, 3}, true),
		"noncontig":  py.NDArray_int64([]int{10}, noncontig), // np.save materialises the view
		"complex":    &py.NDArray{Dtype: "<c16", Shape: []int{1}, Data: cplx},
		"f16":        &py.NDArray{Dtype: "<f2", Shape: []int{5}, Data: f16},
		"nan_inf":    py.NDArray_float64([]int{3}, []float64{math.NaN(), math.Inf(1), math.Inf(-1)}),
		"big_shape":  py.NDArray_float64([]int{1, 1, 1, 1, 1, 7}, make([]float64, 7)),
	}
}

func OBJECTS() map[string]any {
	return map[string]any{
		"datetime": skipped("datetime.datetime has no Go twin in the port's value model"),
		"decimal":  skipped("decimal.Decimal has no Go twin in the port's value model"),
		"fraction": skipped("fractions.Fraction has no Go twin in the port's value model"),
		"uuid":     skipped("uuid.UUID is not picklable in the port's value model"),
		"custom":   &Custom{X: 3},
		"deque":    skipped("collections.deque has no Go twin in the port's value model"),
		"range":    skipped("range has no Go twin in the port's value model"),
	}
}

func sorted_names(m map[string]any) []string {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

func fidelity_table(t *testing.T, table func() map[string]any) {
	t.Helper()
	for_cfgs(t, func(t *testing.T, cfg string, pool *Pool) {
		vals := table()
		for _, name := range sorted_names(vals) {
			name := name
			t.Run("name="+name, func(t *testing.T) {
				val := vals[name]
				if s, ok := val.(skipped); ok {
					t.Skip(string(s))
				}
				got := must(roundtrip(val))
				assertTrue(t, eq(val, got), fmt.Sprintf("%s: %s != %s", name, py.Repr(val), py.Repr(got)))
			})
		}
	})
}

// ---------------------------------------------------------------------------
// fidelity
// ---------------------------------------------------------------------------

func TestFidelity_scalars(t *testing.T)    { fidelity_table(t, SCALARS) }
func TestFidelity_containers(t *testing.T) { fidelity_table(t, CONTAINERS) }
func TestFidelity_numpy(t *testing.T)      { fidelity_table(t, NUMPY) }
func TestFidelity_objects(t *testing.T)    { fidelity_table(t, OBJECTS) }

func TestFidelity_many_types_in_one_batch(t *testing.T) {
	for_cfgs(t, func(t *testing.T, cfg string, pool *Pool) {
		vals := []any{}
		for _, table := range []map[string]any{SCALARS(), CONTAINERS(), NUMPY(), OBJECTS()} {
			for _, name := range sorted_names(table) {
				if _, ok := table[name].(skipped); ok {
					continue
				}
				vals = append(vals, table[name])
			}
		}
		reps := N_scaled(5, 2)
		all := make([]any, 0, len(vals)*reps)
		for r := 0; r < reps; r++ {
			all = append(all, vals...)
		}
		es := make([]*laila.Entry, len(all))
		ids := py.NewList()
		for i, v := range all {
			es[i] = must(laila.Constant(v))
			ids.Append(es[i].Global_id())
		}
		must(Wait(must(laila.Memorize(entries_list(es), py.Kw{"pool_nickname": "t"})), 300))
		got := must(Wait(must(laila.Remember(ids, py.Kw{"pool_nickname": "t", "persist": false})), 300)).(*py.List)
		bad := []int{}
		for i, v := range all {
			if !eq(v, Unwrap(must(got.Get(i)))) {
				bad = append(bad, i)
			}
		}
		assertEq(t, bad, []int{})
	})
}

// ---------------------------------------------------------------------------
// known gaps (fixed)
// ---------------------------------------------------------------------------

func for_transformed(t *testing.T, body func(t *testing.T, pool *Pool)) {
	t.Helper()
	for _, cfg := range []string{"mem-b64", "sqlite"} {
		cfg := cfg
		t.Run("transformed="+cfg, func(t *testing.T) { body(t, pool_cfg(t, cfg)) })
	}
}

func TestFidelityKnownGaps_dict_with_tuple_keys(t *testing.T) {
	// F-13/F-14 (fixed): strict msgpack -> pickle fallback keeps exact types.
	cases := map[string]any{
		"flat":    py.DictOf(py.Tuple{1, 2}, "a"),
		"in_list": py.NewList(py.DictOf(py.Tuple{1}, 1)),
		"nested":  py.DictOf("outer", py.DictOf(py.Tuple{0, 0}, 1)),
	}
	for_transformed(t, func(t *testing.T, pool *Pool) {
		for _, id := range []string{"flat", "in_list", "nested"} {
			val := cases[id]
			t.Run(id, func(t *testing.T) { assertTrue(t, eq(val, must(roundtrip(val)))) })
		}
	})
}

func TestFidelityKnownGaps_nested_tuples_keep_type(t *testing.T) {
	// F-13/F-14 (fixed): strict msgpack -> pickle fallback keeps exact types.
	cases := map[string]any{
		"in_dict":      py.DictOf("k", py.Tuple{1, 2}),
		"in_list_dict": py.NewList(py.DictOf("b", py.Tuple{1, 2})),
		"in_list_list": py.NewList(py.NewList(py.Tuple{1})),
	}
	for_transformed(t, func(t *testing.T, pool *Pool) {
		for _, id := range []string{"in_dict", "in_list_dict", "in_list_list"} {
			val := cases[id]
			t.Run(id, func(t *testing.T) { assertTrue(t, eq(val, must(roundtrip(val)))) })
		}
	})
}

func TestFidelityKnownGaps_ordered_dict_keeps_type(t *testing.T) {
	// F-13/F-14 (fixed): strict msgpack -> pickle fallback keeps exact types.
	for_transformed(t, func(t *testing.T, pool *Pool) {
		t.Skip("collections.OrderedDict has no Go twin in the port's value model (py.Dict is insertion-ordered)")
	})
}

func TestFidelityKnownGaps_top_level_tuple_survives_via_pickle(t *testing.T) {
	for_transformed(t, func(t *testing.T, pool *Pool) {
		val := py.Tuple{1, py.Tuple{2, 3}}
		assertTrue(t, eq(val, must(roundtrip(val))))
	})
}

// ---------------------------------------------------------------------------
// large payloads
// ---------------------------------------------------------------------------

func TestLargePayloads_numpy_blob_raw_mem(t *testing.T) {
	for _, mb := range []int{16, 64, 192} {
		mb := mb
		t.Run(fmt.Sprintf("mb=%d", mb), func(t *testing.T) {
			Fresh_policy(t)
			arr := Rand_array(mb<<20, mb)
			var got any
			sw := Timed(func() { got = must(roundtrip_alpha_(arr)) })
			assertTrue(t, got == any(arr) || arr.Eq(got))
			assertTrue(t, sw.Elapsed < 120)
		})
	}
}

func TestLargePayloads_numpy_blob_base64(t *testing.T) {
	for _, mb := range []int{8, 32, 96} {
		mb := mb
		t.Run(fmt.Sprintf("mb=%d", mb), func(t *testing.T) {
			pool_cfg(t, "mem-b64")
			arr := Rand_array(mb<<20, mb)
			var got any
			sw := Timed(func() { got = must(roundtrip(arr)) })
			assertTrue(t, eq(arr, got))
			assertTrue(t, sw.Elapsed < 240, fmt.Sprintf("%d MB through base64 took %.1fs", mb, sw.Elapsed))
		})
	}
}

func TestLargePayloads_numpy_blob_zlib_random_incompressible(t *testing.T) {
	for _, mb := range []int{8, 32} {
		mb := mb
		t.Run(fmt.Sprintf("mb=%d", mb), func(t *testing.T) {
			pool_cfg(t, "mem-b64-zlib")
			arr := Rand_array(mb<<20, mb)
			var got any
			sw := Timed(func() { got = must(roundtrip(arr)) })
			assertTrue(t, eq(arr, got))
			assertTrue(t, sw.Elapsed < 300)
		})
	}
}

func TestLargePayloads_compressible_text_zlib(t *testing.T) {
	for _, mb := range []int{32, 128} {
		mb := mb
		t.Run(fmt.Sprintf("mb=%d", mb), func(t *testing.T) {
			p := pool_cfg(t, "mem-b64-zlib")
			text := Compressible_text(mb << 20)
			e := must(laila.Constant(text))
			must(Wait(must(laila.Memorize(e, py.Kw{"pool_nickname": "t"})), 300))
			raw := must(p.Read_(e.Global_id()))
			stored := len(py.Str(raw))
			assertTrue(t, stored < len(text)/10, fmt.Sprintf("zlib did not compress: %d vs %d", stored, len(text)))
			assertEq(t, must(roundtrip_id(e.Global_id())), text)
		})
	}
}

func TestLargePayloads_encrypted_blob(t *testing.T) {
	for _, mb := range []int{8, 32} {
		mb := mb
		t.Run(fmt.Sprintf("mb=%d", mb), func(t *testing.T) {
			pool_cfg(t, "mem-b64-zlib-enc")
			blob := Rand_bytes(mb<<20, mb)
			assertEq(t, must(roundtrip(blob)), blob)
		})
	}
}

func TestLargePayloads_persistent_backend_32mb_array(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		policy := Fresh_policy(t)
		_, raw := with_pool(t, kind)
		mem_extend(t, policy, raw, "t")
		arr := Rand_array(32<<20, 1)
		var got any
		sw := Timed(func() { got = must(roundtrip(arr)) })
		assertTrue(t, eq(arr, got))
		assertTrue(t, sw.Elapsed < 240)
	})
}

func TestLargePayloads_dict_with_200k_keys_through_msgpack(t *testing.T) {
	pool_cfg(t, "mem-b64")
	d := py.NewDict()
	for i := 0; i < N_scaled(200_000, 20_000); i++ {
		d.Set(fmt.Sprintf("k%d", i), i)
	}
	assertTrue(t, py.Eq(must(roundtrip(d)), d))
}

func TestLargePayloads_list_with_1m_ints_through_msgpack(t *testing.T) {
	pool_cfg(t, "mem-b64")
	lst := py.NewList()
	for i := 0; i < N_scaled(1_000_000, 100_000); i++ {
		lst.Append(i)
	}
	assertTrue(t, py.Eq(must(roundtrip(lst)), lst))
}

func TestLargePayloads_deeply_nested_dict_through_msgpack(t *testing.T) {
	pool_cfg(t, "mem-b64")
	var obj any = py.DictOf("leaf", 1)
	for i := 0; i < N_scaled(400, 100); i++ {
		obj = py.DictOf("n", obj)
	}
	got, err := roundtrip(obj)
	if err != nil {
		if errors.Is(err, py.RecursionError) || errors.Is(err, py.ValueError) {
			t.Skipf("serializer refuses depth: %s", py.Typename(err))
		}
		t.Fatal(err)
	}
	assertTrue(t, py.Eq(got, obj))
}

func TestLargePayloads_many_medium_blobs_sequential(t *testing.T) {
	pool_cfg(t, "mem-b64")
	n := N_scaled(200, 30)
	es, ids := constants(t, n, func(i int) any { return Rand_bytes(256<<10, i) })
	var got *py.List
	sw := Timed(func() {
		must(Wait(must(laila.Memorize(entries_list(es), py.Kw{"pool_nickname": "t"})), 600))
		got = must(Wait(must(laila.Remember(ids, py.Kw{"pool_nickname": "t", "persist": false})), 600)).(*py.List)
	})
	for i, e := range es {
		assertTrue(t, py.Eq(Unwrap(must(got.Get(i))), must(e.Data())))
	}
	assertTrue(t, sw.Elapsed/float64(n) < 1.0)
}

// ---------------------------------------------------------------------------
// images (``pytest.importorskip("PIL")``: no Go twin, every case skips)
// ---------------------------------------------------------------------------

func TestImages_png_bytes_roundtrip_b64(t *testing.T) {
	t.Skip("PIL has no Go twin (pytest.importorskip(\"PIL\"))")
}

func TestImages_pil_image_object_roundtrip(t *testing.T) {
	t.Skip("PIL has no Go twin (pytest.importorskip(\"PIL\"))")
}

func TestImages_batch_of_images_as_arrays(t *testing.T) {
	t.Skip("PIL has no Go twin (pytest.importorskip(\"PIL\"))")
}

