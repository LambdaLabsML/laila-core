//go:build workload

// Port of tests/workloads/test_w08_transformation_pipelines.py.
//
// Transformation pipelines pushed hard: every ordering of the built-in
// transformations, encryption key mismatch / absence, corrupted stored
// records, tampered constitutions (arbitrary code at read time), direct
// ``TransformationSequence`` throughput on large buffers, and many pools
// with different pipelines sharing one policy.
//
// Deviations from the Python module are confined to the value model:
// ``memoryview`` / ``bytearray`` inputs collapse onto ``[]byte`` (the
// port's single bytes type), and the tampered-constitution case asserts
// the same observable (no side effect in the process environment) even
// though the port never evaluates recovery code at all.
package workloads

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// fernet_key is the ``fernet_key`` fixture: a fresh process-wide key,
// cleared at teardown.
func fernet_key(t *testing.T) []byte {
	t.Helper()
	key := py.Fernet_generate_key()
	laila.Set_encryption_key(key)
	t.Cleanup(func() { laila.Set_encryption_key(nil) })
	return key
}

// rt_ is ``_rt(val, nickname)``.
func rt_(val any, nickname string) (any, *laila.Entry, error) {
	e, err := laila.Constant(py.Kw{"data": val})
	if err != nil {
		return nil, nil, err
	}
	f, err := laila.Memorize(e, py.Kw{"pool_nickname": nickname})
	if err != nil {
		return nil, nil, err
	}
	if _, err := Wait(f, 120); err != nil {
		return nil, nil, err
	}
	g, err := laila.Remember(e.Global_id(), py.Kw{"pool_nickname": nickname, "persist": false})
	if err != nil {
		return nil, nil, err
	}
	got, err := Wait(g, 120)
	if err != nil {
		return nil, nil, err
	}
	return Unwrap(got), e, nil
}

var PIPELINES = map[string]func() []any{
	"b64":             func() []any { return []any{must(laila.New_Base64())} },
	"b64-zlib":        func() []any { return []any{must(laila.New_Base64()), must(laila.New_Zlib())} },
	"fernet":          func() []any { return []any{must(laila.New_FernetEncryption())} },
	"fernet-b64":      func() []any { return []any{must(laila.New_FernetEncryption()), must(laila.New_Base64())} },
	"b64-fernet":      func() []any { return []any{must(laila.New_Base64()), must(laila.New_FernetEncryption())} },
	"b64-zlib-fernet": func() []any { return []any{must(laila.New_Base64()), must(laila.New_Zlib()), must(laila.New_FernetEncryption())} },
	"pickle-b64":      func() []any { return []any{must(laila.New_PickleSerializer()), must(laila.New_Base64())} },
	"msgpack-b64":     func() []any { return []any{must(laila.New_MsgpackSerializer()), must(laila.New_Base64())} },
	"empty":           func() []any { return []any{} },
}

func sorted_pipelines() []string {
	names := make([]string, 0, len(PIPELINES))
	for k := range PIPELINES {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

// seq_of is ``TransformationSequence(transformations=PIPELINES[name]())``.
func seq_of(name string) *laila.TransformationSequence {
	return must(laila.New_TransformationSequence(py.Kw{"transformations": PIPELINES[name]()}))
}

// mem_pool_with is ``DefaultPool(transformations=seq)`` registered under
// ``nick``.
func mem_pool_with(t *testing.T, policy *laila.LAILA_IDENTIFIABLE_POLICY_, seq any, nick string) *Pool {
	t.Helper()
	raw := must(laila.New_DefaultPool(py.Kw{"transformations": seq}))
	mem_extend(t, policy, raw, nick)
	return laila.As_pool_(raw)
}

// rec_entry is ``rec["entry"]`` of a raw pool record.
func rec_entry(t *testing.T, rec any) *py.Dict {
	t.Helper()
	d, ok := rec.(*py.Dict)
	if !ok {
		t.Fatalf("record is %s, want dict", py.Pytypename(rec))
	}
	return must(d.Getitem("entry")).(*py.Dict)
}

// ---------------------------------------------------------------------------
// pipeline orderings
// ---------------------------------------------------------------------------

func TestPipelineOrderings_mem_pool_roundtrip(t *testing.T) {
	for _, name := range sorted_pipelines() {
		name := name
		t.Run("name="+name, func(t *testing.T) {
			policy := Fresh_policy(t)
			fernet_key(t)
			mem_pool_with(t, policy, seq_of(name), "p")
			s100 := ""
			for i := 0; i < 100; i++ {
				s100 += "x"
			}
			r50 := py.NewList()
			for i := 0; i < 50; i++ {
				r50.Append(i)
			}
			vals := []any{py.DictOf("i", 7, "s", s100), r50, make([]byte, 100), arange_i64(10)}
			if name == "fernet" || name == "fernet-b64" {
				// Fernet is text-in: a hand-built chain that starts with it fails
				// loudly at memorize (the shipped preset puts Base64 first, F-15).
				_, err := Wait(must(laila.Memorize(must(laila.Constant(py.Kw{"data": vals[0]})), py.Kw{"pool_nickname": "p"})), 60)
				if !errors.Is(err, py.TypeError) {
					t.Fatalf("want TypeError, got %v", err)
				}
				return
			}
			for _, v := range vals {
				got, _, err := rt_(v, "p")
				check(t, err)
				if arr, ok := v.(*py.NDArray); ok {
					g, ok := got.(*py.NDArray)
					assertTrue(t, ok && arr.Eq(g), name)
				} else {
					assertTrue(t, py.Eq(got, v), name)
				}
			}
		})
	}
}

func TestPipelineOrderings_sqlite_pool_roundtrip_or_clear_error(t *testing.T) {
	// Pipelines whose output is not JSON-serialisable must fail loudly
	// and promptly on the JSON backends, not hang or corrupt.
	for _, name := range sorted_pipelines() {
		name := name
		t.Run("name="+name, func(t *testing.T) {
			policy := Fresh_policy(t)
			fernet_key(t)
			raw := must(Make_pool("sqlite", Disk_tmp(t), py.Kw{"transformations": seq_of(name)}))
			p := laila.As_pool_(raw)
			mem_extend(t, policy, raw, "p")
			defer func() {
				Quiesce(policy)
				Close_pool(raw)
			}()
			e := must(laila.Constant(py.Kw{"data": py.DictOf("i", 1)}))
			f := must(laila.Memorize(e, py.Kw{"pool_nickname": "p"}))
			ok := true
			sw := Timed(func() {
				if _, err := Wait(f, 60); err != nil {
					if !errors.Is(err, py.TypeError) {
						t.Error(err)
					}
					ok = false
				}
			})
			if t.Failed() {
				t.FailNow()
			}
			assertTrue(t, sw.Elapsed < 30)
			if ok {
				got := Unwrap(must(Wait(must(laila.Remember(e.Global_id(), py.Kw{"pool_nickname": "p", "persist": false})), 60)))
				assertEq(t, got, py.DictOf("i", 1))
			} else {
				assertTrue(t, !must(p.Exists(e.Global_id())))
			}
		})
	}
}

// usable_pipelines is ``[n for n in PIPELINES if n not in ("fernet", "fernet-b64")]``.
func usable_pipelines() []string {
	out := []string{}
	for _, n := range sorted_pipelines() {
		if n != "fernet" && n != "fernet-b64" {
			out = append(out, n)
		}
	}
	return out
}

func TestPipelineOrderings_many_pools_many_pipelines_one_policy(t *testing.T) {
	policy := Fresh_policy(t)
	fernet_key(t)
	usable := usable_pipelines()
	for _, name := range usable {
		mem_pool_with(t, policy, seq_of(name), name)
	}
	n := N_scaled(100, 20)
	es, ids := constants(t, n, func(i int) any { return py.DictOf("i", i) })
	for _, name := range usable {
		must(Wait(must(laila.Memorize(entries_list(es), py.Kw{"pool_nickname": name})), 120))
	}
	want := py.NewList()
	for i := 0; i < n; i++ {
		want.Append(i)
	}
	for _, name := range usable {
		got := must(Wait(must(laila.Remember(ids, py.Kw{"pool_nickname": name, "persist": false})), 120)).(*py.List)
		is := py.NewList()
		for _, g := range got.Items() {
			is.Append(must(Unwrap(g).(*py.Dict).Getitem("i")))
		}
		assertTrue(t, py.Eq(is, want), name)
	}
}

func TestPipelineOrderings_pipeline_threads_concurrent_different_pools(t *testing.T) {
	policy := Fresh_policy(t)
	fernet_key(t)
	names := []string{"b64", "b64-zlib", "b64-zlib-fernet", "b64-fernet"}
	for _, name := range names {
		mem_pool_with(t, policy, seq_of(name), name)
	}
	var mu sync.Mutex
	errs := []string{}
	z1000 := ""
	for i := 0; i < 1000; i++ {
		z1000 += "z"
	}
	worker := func(i int) {
		name := names[i%len(names)]
		for j := 0; j < N_scaled(40, 10); j++ {
			got, _, err := rt_(py.DictOf("i", i, "j", j, "blob", z1000), name)
			if err == nil && !py.Eq(must(got.(*py.Dict).Getitem("j")), j) {
				err = py.AssertionError.New("")
			}
			if err != nil {
				mu.Lock()
				errs = append(errs, py.Repr(err))
				mu.Unlock()
				return
			}
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(i)
		}()
	}
	wg.Wait()
	assertTrue(t, len(errs) == 0, errs)
}

// ---------------------------------------------------------------------------
// encryption keys
// ---------------------------------------------------------------------------

// enc_pool is ``DefaultPool(transformations=transformation_base64_compression_encryption())``
// registered under ``nick``.
func enc_pool(t *testing.T, policy *laila.LAILA_IDENTIFIABLE_POLICY_, nick string) *Pool {
	t.Helper()
	return mem_pool_with(t, policy, must(laila.Transformation_base64_compression_encryption()), nick)
}

func TestEncryptionKeys_wrong_key_at_read_raises_valueerror(t *testing.T) {
	policy := Fresh_policy(t)
	key := fernet_key(t)
	enc_pool(t, policy, "enc")
	e := must(laila.Constant(py.Kw{"data": py.DictOf("secret", 1)}))
	must(Wait(must(laila.Memorize(e, py.Kw{"pool_nickname": "enc"})), 60))
	laila.Set_encryption_key(py.Fernet_generate_key())
	f := must(laila.Remember(e.Global_id(), py.Kw{"pool_nickname": "enc", "persist": false}))
	if _, err := Wait(f, 60); !errors.Is(err, py.ValueError) {
		t.Fatalf("want ValueError, got %v", err)
	}
	laila.Set_encryption_key(key)
	got := Unwrap(must(Wait(must(laila.Remember(e.Global_id(), py.Kw{"pool_nickname": "enc", "persist": false})), 60)))
	assertEq(t, got, py.DictOf("secret", 1))
}

func TestEncryptionKeys_no_key_at_read_raises_runtimeerror(t *testing.T) {
	policy := Fresh_policy(t)
	fernet_key(t)
	enc_pool(t, policy, "enc")
	e := must(laila.Constant(py.Kw{"data": py.DictOf("secret", 1)}))
	must(Wait(must(laila.Memorize(e, py.Kw{"pool_nickname": "enc"})), 60))
	laila.Set_encryption_key(nil)
	f := must(laila.Remember(e.Global_id(), py.Kw{"pool_nickname": "enc", "persist": false}))
	if _, err := Wait(f, 60); !errors.Is(err, py.RuntimeError) {
		t.Fatalf("want RuntimeError, got %v", err)
	}
}

func TestEncryptionKeys_explicit_key_pool_ignores_process_key_at_write(t *testing.T) {
	policy := Fresh_policy(t)
	fernet_key(t)
	other := py.Fernet_generate_key()
	seq := must(laila.New_TransformationSequence(py.Kw{"transformations": []any{
		must(laila.New_Base64()), must(laila.New_FernetEncryption(py.Kw{"key": other})),
	}}))
	mem_pool_with(t, policy, seq, "enc")
	e := must(laila.Constant(py.Kw{"data": py.DictOf("secret", 1)}))
	must(Wait(must(laila.Memorize(e, py.Kw{"pool_nickname": "enc"})), 60))
	// reader resolves from process key -> mismatch fingerprint
	f := must(laila.Remember(e.Global_id(), py.Kw{"pool_nickname": "enc", "persist": false}))
	if _, err := Wait(f, 60); !errors.Is(err, py.ValueError) {
		t.Fatalf("want ValueError, got %v", err)
	}
	laila.Set_encryption_key(other)
	got := Unwrap(must(Wait(must(laila.Remember(e.Global_id(), py.Kw{"pool_nickname": "enc", "persist": false})), 60)))
	assertEq(t, got, py.DictOf("secret", 1))
}

func TestEncryptionKeys_key_rotation_mid_stream(t *testing.T) {
	policy := Fresh_policy(t)
	k1 := fernet_key(t)
	enc_pool(t, policy, "enc")
	es1, _ := constants(t, N_scaled(100, 20), func(i int) any { return py.DictOf("k", 1, "i", i) })
	must(Wait(must(laila.Memorize(entries_list(es1), py.Kw{"pool_nickname": "enc"})), 120))
	k2 := py.Fernet_generate_key()
	laila.Set_encryption_key(k2)
	enc_pool(t, policy, "enc2")
	es2, _ := constants(t, N_scaled(100, 20), func(i int) any { return py.DictOf("k", 2, "i", i) })
	must(Wait(must(laila.Memorize(entries_list(es2), py.Kw{"pool_nickname": "enc2"})), 120))
	// k2 active: es2 readable, es1 not
	got := Unwrap(must(Wait(must(laila.Remember(es2[0].Global_id(), py.Kw{"pool_nickname": "enc2", "persist": false})), 60)))
	assertEq(t, must(got.(*py.Dict).Getitem("k")), 2)
	if _, err := Wait(must(laila.Remember(es1[0].Global_id(), py.Kw{"pool_nickname": "enc", "persist": false})), 60); !errors.Is(err, py.ValueError) {
		t.Fatalf("want ValueError, got %v", err)
	}
	laila.Set_encryption_key(k1)
	got = Unwrap(must(Wait(must(laila.Remember(es1[len(es1)-1].Global_id(), py.Kw{"pool_nickname": "enc", "persist": false})), 60)))
	assertEq(t, must(got.(*py.Dict).Getitem("k")), 1)
}

func TestEncryptionKeys_key_material_not_in_stored_record(t *testing.T) {
	policy := Fresh_policy(t)
	key := fernet_key(t)
	p := enc_pool(t, policy, "enc")
	e := must(laila.Constant(py.Kw{"data": py.DictOf("secret", "hunter2")}))
	must(Wait(must(laila.Memorize(e, py.Kw{"pool_nickname": "enc"})), 60))
	raw := must(py.Json_dumps_opts(must(p.Read_(e.Global_id())), py.JsonOpts{Default: func(v any) (any, error) { return py.Str(v), nil }}))
	assertTrue(t, !strings.Contains(raw, string(key)))
	assertTrue(t, !strings.Contains(raw, "hunter2"))
}

// ---------------------------------------------------------------------------
// corruption
// ---------------------------------------------------------------------------

// b64_pool is the ``b64_pool`` fixture: a sqlite pool under ``p`` whose
// teardown quiesces the policy first (siblings of a failed group keep
// running; F-06 crash otherwise).
//
// The concrete backend is returned (not the embedded base) so ``_read`` /
// ``_write`` dispatch to the sqlite overrides, as Python's virtual calls do.
func b64_pool(t *testing.T) raw_pool {
	t.Helper()
	policy := Fresh_policy(t)
	raw := must(Make_pool("sqlite", Disk_tmp(t)))
	mem_extend(t, policy, raw, "p")
	t.Cleanup(func() {
		Quiesce(policy)
		Close_pool(raw)
	})
	return raw.(raw_pool)
}

// raw_pool is the ``_read`` / ``_write`` surface of a concrete backend.
type raw_pool interface {
	Read_(key string) (any, error)
	Write_(key string, value any) error
}

// store_ is ``TestCorruption._store(nickname="p")``.
func store_(t *testing.T, nickname ...string) *laila.Entry {
	t.Helper()
	nick := "p"
	if len(nickname) > 0 {
		nick = nickname[0]
	}
	x500 := ""
	for i := 0; i < 500; i++ {
		x500 += "x"
	}
	e := must(laila.Constant(py.Kw{"data": py.DictOf("i", 1, "blob", x500)}))
	must(Wait(must(laila.Memorize(e, py.Kw{"pool_nickname": nick})), 60))
	return e
}

func TestCorruption_truncated_payload_fails_promptly(t *testing.T) {
	pool := b64_pool(t)
	e := store_(t)
	rec := must(pool.Read_(e.Global_id()))
	ent := rec_entry(t, rec)
	payload := py.Str(must(ent.Getitem("payload")))
	ent.Set("payload", payload[:len(payload)/2])
	check(t, pool.Write_(e.Global_id(), rec))
	f := must(laila.Remember(e.Global_id(), py.Kw{"pool_nickname": "p", "persist": false}))
	var err error
	sw := Timed(func() { _, err = Wait(f, 60) })
	assertTrue(t, err != nil, "want an exception")
	assertTrue(t, sw.Elapsed < 30)
}

func TestCorruption_garbage_payload_fails_promptly(t *testing.T) {
	pool := b64_pool(t)
	e := store_(t)
	rec := must(pool.Read_(e.Global_id()))
	rec_entry(t, rec).Set("payload", "!!!not base64!!!")
	check(t, pool.Write_(e.Global_id(), rec))
	_, err := Wait(must(laila.Remember(e.Global_id(), py.Kw{"pool_nickname": "p", "persist": false})), 60)
	assertTrue(t, err != nil, "want an exception")
}

func TestCorruption_missing_constitution_returns_raw_payload(t *testing.T) {
	pool := b64_pool(t)
	e := store_(t)
	rec := must(pool.Read_(e.Global_id()))
	rec_entry(t, rec).Set("constitution", py.DictOf("_kind", "simple", "codes", py.NewList()))
	check(t, pool.Write_(e.Global_id(), rec))
	got := Unwrap(must(Wait(must(laila.Remember(e.Global_id(), py.Kw{"pool_nickname": "p", "persist": false})), 60)))
	_, isStr := got.(string) // the still-encoded payload
	assertTrue(t, isStr, py.Pytypename(got))
}

// F-16 (fixed): identity fields are validated and must match the key.
func TestCorruption_bad_identity_fields_fail(t *testing.T) {
	pool := b64_pool(t)
	e := store_(t)
	rec := must(pool.Read_(e.Global_id()))
	rec_entry(t, rec).Set("_uuid", "not-a-uuid")
	check(t, pool.Write_(e.Global_id(), rec))
	_, err := Wait(must(laila.Remember(e.Global_id(), py.Kw{"pool_nickname": "p", "persist": false})), 60)
	if !errors.Is(err, py.ValueError) {
		t.Fatalf("want ValueError, got %v", err)
	}
}

func TestCorruption_record_under_wrong_key_fails(t *testing.T) {
	pool := b64_pool(t)
	e1, e2 := store_(t), store_(t)
	check(t, pool.Write_(e1.Global_id(), must(pool.Read_(e2.Global_id()))))
	_, err := Wait(must(laila.Remember(e1.Global_id(), py.Kw{"pool_nickname": "p", "persist": false})), 60)
	if !errors.Is(err, py.KeyError) {
		t.Fatalf("want KeyError, got %v", err)
	}
}

func TestCorruption_type_chain_mismatch_pipeline_fails_clearly(t *testing.T) {
	// Zlib emits str, Base64 wants bytes: an invalid chain must fail at memorize, not hang.
	policy := Fresh_policy(t)
	seq := must(laila.New_TransformationSequence(py.Kw{"transformations": []any{
		must(laila.New_Base64()), must(laila.New_Zlib()), must(laila.New_Base64()),
	}}))
	mem_pool_with(t, policy, seq, "bad")
	f := must(laila.Memorize(must(laila.Constant(py.Kw{"data": py.DictOf("i", 1)})), py.Kw{"pool_nickname": "bad"}))
	if _, err := Wait(f, 60); !errors.Is(err, py.TypeError) {
		t.Fatalf("want TypeError, got %v", err)
	}
}

func TestCorruption_non_record_json_in_pool(t *testing.T) {
	pool := b64_pool(t)
	const gid = "LAILA:ENTRY:00000000-0000-4000-8000-000000000001"
	check(t, pool.Write_(gid, py.DictOf("foo", "bar")))
	_, err := Wait(must(laila.Remember(gid, py.Kw{"pool_nickname": "p", "persist": false})), 60)
	assertTrue(t, err != nil, "want an exception")
}

func TestCorruption_many_corrupt_records_in_batch_do_not_hang(t *testing.T) {
	pool := b64_pool(t)
	n := N_scaled(30, 10)
	es := make([]*laila.Entry, n)
	ids := py.NewList()
	for i := range es {
		es[i] = store_(t)
		ids.Append(es[i].Global_id())
	}
	for i := 0; i < n; i += 3 {
		rec := must(pool.Read_(es[i].Global_id()))
		rec_entry(t, rec).Set("payload", "corrupt")
		check(t, pool.Write_(es[i].Global_id(), rec))
	}
	f := must(laila.Remember(ids, py.Kw{"pool_nickname": "p", "persist": false}))
	var err error
	sw := Timed(func() { _, err = Wait(f, 120) })
	assertTrue(t, err != nil, "want an exception")
	assertTrue(t, sw.Elapsed < 60)
	// documented: the group raised early; its siblings are still running
	Quiesce(must(laila.Get_active_policy()))
	results := must(f.(*laila.GroupFuture).What())
	assertTrue(t, results != nil, "want a dict")
}

// F-17 (fixed): recovery code must match a loaded transformation template.
func TestCorruption_tampered_constitution_does_not_execute_code(t *testing.T) {
	pool := b64_pool(t)
	e := store_(t)
	rec := must(pool.Read_(e.Global_id()))
	marker := fmt.Sprintf("LAILA_WORKLOAD_PWNED_%d", os.Getpid())
	codes := py.NewList()
	codes.Append("def backward(inp):\n    import os\n    os.environ[" + py.Repr(marker) + "] = '1'\n    return inp\n")
	rec_entry(t, rec).Set("constitution", py.DictOf("_kind", "simple", "codes", codes))
	check(t, pool.Write_(e.Global_id(), rec))
	_, _ = Wait(must(laila.Remember(e.Global_id(), py.Kw{"pool_nickname": "p", "persist": false})), 60)
	_, present := os.LookupEnv(marker)
	assertTrue(t, !present)
	_ = os.Unsetenv(marker)
}

// ---------------------------------------------------------------------------
// direct sequence throughput
// ---------------------------------------------------------------------------

func TestDirectSequenceThroughput_base64_forward_backward_large(t *testing.T) {
	for _, mb := range []int{16, 64} {
		mb := mb
		t.Run(fmt.Sprintf("mb=%d", mb), func(t *testing.T) {
			seq := laila.Transformation_base64
			data := Rand_bytes(mb<<20, mb)
			var out, back any
			var codes []string
			sw := Timed(func() {
				out, codes = must2(seq.Forward(data))
				back = must(seq.Iter()[0].Backward(out))
			})
			assertEq(t, back, data)
			assertEq(t, len(codes), 1)
			assertTrue(t, sw.Elapsed < 60)
		})
	}
}

func TestDirectSequenceThroughput_zlib_pipeline_large_compressible(t *testing.T) {
	for _, mb := range []int{16, 64} {
		mb := mb
		t.Run(fmt.Sprintf("mb=%d", mb), func(t *testing.T) {
			seq := laila.Transformation_base64_compression
			data := []byte(Compressible_text(mb << 20))
			var out any
			sw := Timed(func() { out, _ = must2(seq.Forward(data)) })
			assertTrue(t, must(py.Len(out)) < len(data)/5)
			assertTrue(t, sw.Elapsed < 120)
		})
	}
}

func TestDirectSequenceThroughput_sequence_append_chaining(t *testing.T) {
	seq := must(laila.New_TransformationSequence())
	must(must(seq.Append(must(laila.New_Base64()))).Append(must(laila.New_Zlib())))
	assertEq(t, len(seq.Iter()), 2)
	hello := make([]byte, 0, 5000)
	for i := 0; i < 1000; i++ {
		hello = append(hello, "hello"...)
	}
	_, codes := must2(seq.Forward(hello))
	assertEq(t, len(codes), 2)
}

func TestDirectSequenceThroughput_empty_sequence_is_identity(t *testing.T) {
	seq := must(laila.New_TransformationSequence())
	out, codes := must2(seq.Forward([]byte("x")))
	assertTrue(t, py.Eq(out, []byte("x")) && len(codes) == 0)
}

func TestDirectSequenceThroughput_fernet_token_grows_linearly(t *testing.T) {
	fernet_key(t)
	f := must(laila.New_FernetEncryption())
	b := must(laila.New_Base64())
	sizes := []int{1 << 10, 1 << 16, 1 << 20}
	ratios := []float64{}
	for _, s := range sizes {
		tok := must(f.Forward(must(b.Forward(Rand_bytes(s)))))
		ratios = append(ratios, float64(must(py.Len(tok)))/float64(s))
	}
	for _, r := range ratios {
		assertTrue(t, 1.3 < r && r < 2.0, ratios)
	}
}

// F-15 (fixed): the preset is Base64 -> Fernet.
func TestDirectSequenceThroughput_encryption_only_preset_accepts_serialised_bytes(t *testing.T) {
	fernet_key(t)
	seq := must(laila.Transformation_encryption())
	out, codes := must2(seq.Forward([]byte("serialised payload bytes")))
	assertTrue(t, must(py.Len(out)) > 0 && len(codes) > 0)
}

func TestDirectSequenceThroughput_memoryview_and_bytearray_inputs(t *testing.T) {
	// memoryview / bytearray collapse onto []byte in the port's value model;
	// the bytes-like acceptance contract is exercised through the pointer
	// form As_bytes_ also admits.
	b := must(laila.New_Base64())
	data := Rand_bytes(1 << 16)
	assertEq(t, must(b.Backward(must(b.Forward(data)))), data)
	ba := append([]byte(nil), data...)
	assertEq(t, must(b.Backward(must(b.Forward(&ba)))), data)
}

func TestDirectSequenceThroughput_type_errors_are_clear(t *testing.T) {
	z := must(laila.New_Zlib())
	if _, err := z.Forward([]byte("bytes-not-str")); !errors.Is(err, py.TypeError) {
		t.Fatalf("want TypeError, got %v", err)
	}
	if _, err := z.Backward([]byte("bytes-not-str")); !errors.Is(err, py.TypeError) {
		t.Fatalf("want TypeError, got %v", err)
	}
}

// must2 unwraps (a, b, err), panicking on error.
func must2[A, B any](a A, b B, err error) (A, B) {
	if err != nil {
		panic(err)
	}
	return a, b
}
