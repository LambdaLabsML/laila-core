//go:build workload

// Port of tests/workloads/test_w22_end_to_end_workflows.py.
//
// W22 -- realistic end-to-end workflows.
//
// Each test is a small "application": ETL into tiered persistent pools and
// reopening the files cold, model-checkpoint lineage over hundreds of
// evolutions, an image dataset + multi-threaded data loader over HDF5,
// encrypted hand-off between policies, derived-entry build pipelines over
// persistent manifests and a distributed compute loop against a child
// process.
package workloads

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

const T22_ = 300.0

// w22_reopen is ``_reopen(kind, path, **kw)``.
func w22_reopen(kind string, path string, kw ...py.Kw) (any, error) {
	o := py.MergeKw(kw...)
	o["file_path"] = path
	switch kind {
	case "sqlite":
		return laila.New_SQLitePool(o)
	case "duckdb":
		return laila.New_DuckDBPool(o)
	}
	return laila.New_HDF5Pool(o)
}

// w22_path_of is ``_path_of(pool)``: ``pool.file_path``.
func w22_path_of(pool any) string { return file_path(pool) }

// w22_wait_err runs a memory verb and waits on its future, returning the
// first error (``with pytest.raises(Exception): laila.xxx(...).wait(_T)``).
func w22_wait_err(fn func() (any, error), timeout float64) error {
	f, err := fn()
	if err != nil {
		return err
	}
	_, err = Wait(f, timeout)
	return err
}

// w22_remember is ``laila.remember(ref, pool_nickname=nick, persist=False).wait(_T)``.
func w22_remember(t *testing.T, ref any, nick string) any {
	t.Helper()
	return must(Wait(must(laila.Remember(ref, py.Kw{"pool_nickname": nick, "persist": false})), T22_))
}

// w22_memorize is ``laila.memorize(x, pool_nickname=nick).wait(_T)``
// (alpha when nick is "").
func w22_memorize(t *testing.T, x any, nick string) {
	t.Helper()
	if nick == "" {
		must(Wait(must(laila.Memorize(x)), T22_))
		return
	}
	must(Wait(must(laila.Memorize(x, py.Kw{"pool_nickname": nick})), T22_))
}

// w22_item is ``unwrap(v)[key]``.
func w22_item(v any, key string) any {
	return py.Must(Unwrap(v).(*py.Dict).Getitem(key))
}

// w22_sorted_ints is ``sorted(unwrap(g) for g in got)`` (or of a key of
// each) as ints.
func w22_sorted_ints(got any, key string) []int {
	out := []int{}
	for _, g := range py.Iter(got) {
		v := Unwrap(g)
		if key != "" {
			v = py.Must(v.(*py.Dict).Getitem(key))
		}
		out = append(out, py.Must(py.Int(v)))
	}
	sort.Ints(out)
	return out
}

// w22_f32 is ``np.ndarray`` (float32, 1-D) from a Go slice -- ``w.copy()``.
func w22_f32(w []float32) *py.NDArray { return py.NDArray_float32([]int{len(w)}, w) }

// w22_f32s decodes a float32 array back into a Go slice.
func w22_f32s(a *py.NDArray) []float32 {
	if a.Dtype != "<f4" {
		panic(py.TypeError.Newf("array dtype is %s, not <f4", a.Dtype))
	}
	out := make([]float32, len(a.Data)/4)
	for i := range out {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(a.Data[4*i:]))
	}
	return out
}

// w22_allclose is ``np.allclose(arr, value, atol=atol)``.
func w22_allclose(a *py.NDArray, value float64, atol float64) bool {
	for _, x := range w22_f32s(a) {
		if math.Abs(float64(x)-value) > atol+1e-5*math.Abs(value) {
			return false
		}
	}
	return true
}

// w22_add_scalar is ``arr + 0.001`` (a new float32 array).
func w22_add_scalar(a *py.NDArray, v float32) *py.NDArray {
	w := w22_f32s(a)
	for i := range w {
		w[i] += v
	}
	return w22_f32(w)
}

// w22_shape_is is ``a.shape == shape``.
func w22_shape_is(a *py.NDArray, shape ...int) bool {
	if len(a.Shape) != len(shape) {
		return false
	}
	for i := range shape {
		if a.Shape[i] != shape[i] {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// ETL
// ---------------------------------------------------------------------------

func TestEtlPipeline_ingest_transform_tier_persist_reopen(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		policy := Fresh_policy(t)
		disk_tmp := Disk_tmp(t)
		n := N_scaled(3000, 300)
		origin := must(Make_pool(kind, disk_tmp))
		cache := must(laila.New_DefaultPool())
		must(laila.As_pool_(cache).Lshift(origin)) // cache << origin
		mem_extend(t, policy, origin, "warehouse")
		mem_extend(t, policy, cache, "hot")
		path := w22_path_of(origin)
		var raw []*laila.Entry
		var summary *laila.Entry
		agg := py.NewDict()
		ids := py.NewList()
		var ingest, cold, warm *Stopwatch
		func() {
			defer Close_pool(origin)
			// 1. ingest raw records
			raw = make([]*laila.Entry, n)
			for i := range raw {
				raw[i] = must(laila.Entry_constant(py.DictOf("id", i, "amount", (i*37)%1000, "cat", fmt.Sprintf("c%d", i%7))))
			}
			ingest = Timed(func() { w22_memorize(t, entries_list(raw), "warehouse") })
			// 2. transform: per-category aggregates (in Python, as an app would)
			for _, e := range raw {
				d := must(e.Data()).(*py.Dict)
				cat := must(d.Getitem("cat"))
				agg.Set(cat, agg.GetDefault(cat, 0).(int)+must(d.Getitem("amount")).(int))
			}
			summary = must(laila.Entry_constant(agg, py.Kw{"nickname": "summary-" + kind}))
			w22_memorize(t, summary, "warehouse")
			// 3. read through the hot tier: first touch warms, second is served from cache
			for _, e := range raw[:n/3] {
				ids.Append(e.Global_id())
			}
			var got *py.List
			cold = Timed(func() { got = w22_remember(t, ids, "hot").(*py.List) })
			want := []int{}
			for _, g := range got.Items() {
				want = append(want, must(py.Int(w22_item(g, "id"))))
			}
			assertEq(t, want, Range(n/3))
			warm = Timed(func() { got = w22_remember(t, ids, "hot").(*py.List) })
			assertEq(t, got.Len(), n/3)
			for _, i := range ids.Items() {
				assertTrue(t, must(laila.As_pool_(cache).Contains(i)))
			}
			// 4. forget a slice from the warehouse and confirm the cache does not resurrect it
			head := must(ids.Getslice(py.NewSlice(0, 50)))
			must(Wait(must(laila.Forget(head, py.Kw{"pool_nickname": "warehouse"})), T22_))
			for _, i := range head.Items() {
				assertTrue(t, !must(laila.As_pool_(origin).Contains(i)))
			}
		}()
		// 5. cold reopen of the file in a brand new pool object
		reopened := must(w22_reopen(kind, path))
		func() {
			defer func() {
				Quiesce(policy)
				Close_pool(reopened)
			}()
			mem_extend(t, policy, reopened, "warehouse-2")
			back := w22_remember(t, summary.Global_id(), "warehouse-2")
			assertEq(t, Unwrap(back), agg)
			back = w22_remember(t, raw[len(raw)-1].Global_id(), "warehouse-2")
			assertEq(t, w22_item(back, "id"), n-1)
			err := w22_wait_err(func() (any, error) {
				return laila.Remember(must(ids.Get(0)), py.Kw{"pool_nickname": "warehouse-2", "persist": false})
			}, T22_)
			assertTrue(t, err != nil, "expected the forgotten id to raise")
		}()
		assertTrue(t, ingest.Elapsed < 120 && cold.Elapsed < 120 && warm.Elapsed < 120)
	})
}

func TestEtlPipeline_manifest_driven_etl_sqlite(t *testing.T) {
	policy := Fresh_policy(t)
	disk_tmp := Disk_tmp(t)
	n := N_scaled(2000, 200)
	pool := must(Make_pool("sqlite", disk_tmp))
	mem_extend(t, policy, pool, "wh")
	defer func() {
		Quiesce(policy)
		Close_pool(pool)
	}()
	batches := map[int]*laila.Manifest{}
	order := []int{}
	for b := 0; b < 10; b++ {
		items := py.NewDict()
		for i := 0; i < n/10; i++ {
			items.Set(fmt.Sprintf("r%d", i), must(laila.Entry_constant(py.DictOf("b", b, "i", i))))
		}
		m := must(laila.New_Manifest(py.Kw{"data": items, "nickname": fmt.Sprintf("batch-%d", b)}))
		batches[b] = m
		order = append(order, b)
		w22_memorize(t, m, "wh")
	}
	// a manifest of manifests = the full dataset
	full_data := py.NewDict()
	for _, b := range order {
		full_data.Set(fmt.Sprintf("batch%d", b), batches[b])
	}
	full := must(laila.New_Manifest(py.Kw{"data": full_data, "nickname": "dataset"}))
	w22_memorize(t, full, "wh")
	back := w22_remember(t, full.Global_id(), "wh")
	inner := w22_remember(t, back, "wh").(*py.List)
	assertEq(t, inner.Len(), 10)
	total := 0
	for _, m := range inner.Items() {
		rows := w22_remember(t, m, "wh").(*py.List)
		first := w22_item(must(rows.Get(0)), "b")
		for _, r := range rows.Items() {
			assertEq(t, w22_item(r, "b"), first)
		}
		total += rows.Len()
	}
	assertEq(t, total, 10*(n/10))
}

// F-W22-01 (fixed): children/manifests resolve from the source pool first.
func TestEtlPipeline_manifest_realized_from_named_pool(t *testing.T) {
	policy := Fresh_policy(t)
	disk_tmp := Disk_tmp(t)
	pool := must(Make_pool("sqlite", disk_tmp))
	mem_extend(t, policy, pool, "wh")
	defer func() {
		Quiesce(policy)
		Close_pool(pool)
	}()
	n := N_scaled(200, 20)
	items := py.NewDict()
	for i := 0; i < n; i++ {
		items.Set(fmt.Sprintf("r%d", i), must(laila.Entry_constant(i)))
	}
	m := must(laila.New_Manifest(py.Kw{"data": items}))
	w22_memorize(t, m, "wh")
	back := w22_remember(t, m.Global_id(), "wh")
	real := must(laila.As_manifest_(back).Realized()).(*py.Dict)
	assertEq(t, real.Len(), n)
}

// ---------------------------------------------------------------------------
// checkpoint lineage
// ---------------------------------------------------------------------------

func TestCheckpointLineage_hundreds_of_evolutions_recover_latest_and_specific(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		policy := Fresh_policy(t)
		disk_tmp := Disk_tmp(t)
		steps := N_scaled(300, 50)
		pool := must(Make_pool(kind, disk_tmp))
		mem_extend(t, policy, pool, "ckpt")
		path := w22_path_of(pool)
		w := make([]float32, 1024) // np.zeros(1024, dtype=np.float32)
		ck := must(laila.Variable(py.Kw{"data": py.DictOf("step", 0, "w", w22_f32(w)), "nickname": "model-" + kind}))
		base := strings.SplitN(ck.Global_id(), "@", 2)[0]
		func() {
			defer func() {
				Quiesce(policy)
				Close_pool(pool)
			}()
			w22_memorize(t, ck, "ckpt")
			sw := Timed(func() {
				for s := 1; s <= steps; s++ {
					for i := range w {
						w[i] += 0.001
					}
					check(t, ck.Set_data(py.DictOf("step", s, "w", w22_f32(w))))
					w22_memorize(t, ck, "ckpt")
				}
			})
			latest := w22_remember(t, fmt.Sprintf("%s@evolution=-1", base), "ckpt")
			assertEq(t, w22_item(latest, "step"), steps)
			mid := w22_remember(t, fmt.Sprintf("%s@evolution=%d", base, steps/2), "ckpt")
			assertEq(t, w22_item(mid, "step"), steps/2)
			assertTrue(t, w22_allclose(w22_item(mid, "w").(*py.NDArray), float64(steps/2)*0.001, 1e-4))
			assertTrue(t, sw.Elapsed/float64(steps) < 0.5)
		}()
		// recovery after a "process restart": a fresh policy and a reopened file
		p2 := must(laila.New_DefaultPolicy())
		laila.Activate_policy(p2)
		re := must(w22_reopen(kind, path))
		func() {
			defer func() {
				Quiesce(p2)
				Close_pool(re)
				laila.Activate_policy(policy)
				laila.Local_policies_.PopDefault(As_policy(p2).Global_id(), nil)
			}()
			mem_extend(t, As_policy(p2), re, "ckpt")
			latest := w22_remember(t, fmt.Sprintf("%s@evolution=-1", base), "ckpt")
			assertEq(t, w22_item(latest, "step"), steps)
			// resume training from the recovered checkpoint
			resumed := laila.As_entry_(latest)
			assertTrue(t, resumed != nil, py.Pytypename(latest))
			check(t, resumed.Set_data(py.DictOf("step", steps+1, "w", w22_add_scalar(w22_item(latest, "w").(*py.NDArray), 0.001))))
			w22_memorize(t, resumed, "ckpt")
			newest := w22_remember(t, fmt.Sprintf("%s@evolution=-1", base), "ckpt")
			assertEq(t, w22_item(newest, "step"), steps+1)
		}()
	})
}

// ---------------------------------------------------------------------------
// image dataset loader
// ---------------------------------------------------------------------------

// w22_dataset_fixture is the ``dataset`` fixture: a (manifest, imgs, labels,
// pool) tuple over an HDF5 (or SQLite) pool registered as "imgs".
type w22_dataset_fixture struct {
	manifest *laila.Manifest
	imgs     []*laila.Entry
	labels   []int
	pool     any
}

// w22_rand_img is ``rng.integers(0, 255, size=(32, 32, 3), dtype=np.uint8)``.
func w22_rand_img(rng *rand.Rand) *py.NDArray {
	buf := make([]byte, 32*32*3)
	for i := range buf {
		buf[i] = byte(rng.Intn(255))
	}
	return py.NDArray_uint8([]int{32, 32, 3}, buf)
}

func w22_dataset(t *testing.T) *w22_dataset_fixture {
	t.Helper()
	policy := Fresh_policy(t)
	disk_tmp := Disk_tmp(t)
	kind := "sqlite"
	if contains_str(PERSISTENT_KINDS, "hdf5") {
		kind = "hdf5"
	}
	pool := must(Make_pool(kind, disk_tmp))
	mem_extend(t, policy, pool, "imgs")
	n := N_scaled(600, 100)
	rng := rand.New(rand.NewSource(0))
	imgs := make([]*laila.Entry, n)
	labels := make([]int, n)
	for i := range imgs {
		imgs[i] = must(laila.Entry_constant(w22_rand_img(rng)))
		labels[i] = i % 10
	}
	w22_memorize(t, entries_list(imgs), "imgs")
	data := py.NewDict()
	for i, e := range imgs {
		data.Set(fmt.Sprintf("img%d", i), e)
	}
	manifest := must(laila.New_Manifest(py.Kw{"data": data, "nickname": "train-set"}))
	w22_memorize(t, manifest, "imgs")
	t.Cleanup(func() {
		Quiesce(policy)
		Close_pool(pool)
	})
	return &w22_dataset_fixture{manifest: manifest, imgs: imgs, labels: labels, pool: pool}
}

func TestImageDatasetLoader_multithreaded_random_batches(t *testing.T) {
	ds := w22_dataset(t)
	imgs := ds.imgs
	n := len(imgs)
	ids := make([]string, n)
	for i, e := range imgs {
		ids[i] = e.Global_id()
	}
	epochs, batch := 3, 32
	seen := make([]int, n)
	var seen_mu sync.Mutex // list item ``+=`` is GIL-atomic in Python

	loader := func(th int) (any, error) {
		rng := rand.New(rand.NewSource(int64(th)))
		total := 0
		for ep := 0; ep < epochs; ep++ {
			order := Range(n)
			rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
			for s := 0; s < n; s += batch {
				idx := order[s:min(s+batch, n)]
				want := py.NewList()
				for _, i := range idx {
					want.Append(ids[i])
				}
				f, err := laila.Remember(want, py.Kw{"pool_nickname": "imgs", "persist": false})
				if err != nil {
					return nil, err
				}
				got, err := Wait(f, T22_)
				if err != nil {
					return nil, err
				}
				arrs := []*py.NDArray{}
				for _, g := range py.Iter(got) {
					a, ok := Unwrap(g).(*py.NDArray)
					if !ok || !w22_shape_is(a, 32, 32, 3) || a.Dtype != "|u1" {
						return nil, py.AssertionError.Newf("bad image %s", py.Repr(Unwrap(g)))
					}
					arrs = append(arrs, a)
				}
				// x = np.stack(arrs).astype(np.float32) / 255.0
				x := make([]float32, 0, len(arrs)*32*32*3)
				for _, a := range arrs {
					for _, b := range a.Data {
						x = append(x, float32(b)/255.0)
					}
				}
				if len(x) != len(idx)*32*32*3 {
					return nil, py.AssertionError.Newf("stacked shape mismatch: %d", len(x))
				}
				seen_mu.Lock()
				for _, i := range idx {
					seen[i]++
				}
				seen_mu.Unlock()
				total += len(idx)
			}
		}
		return total, nil
	}

	var totals []any
	sw := Timed(func() { totals = must(Run_threads(loader, 4)) })
	sum := 0
	for _, tot := range totals {
		assertEq(t, tot, epochs*n)
		sum += tot.(int)
	}
	for _, c := range seen {
		assertEq(t, c, 4*epochs)
	}
	imgs_per_s := float64(sum) / sw.Elapsed
	assertTrue(t, imgs_per_s > 50, fmt.Sprintf("%.0f img/s", imgs_per_s))
}

func TestImageDatasetLoader_manifest_realized_matches_direct_reads(t *testing.T) {
	ds := w22_dataset(t)
	back := w22_remember(t, ds.manifest.Global_id(), "imgs")
	rows := w22_remember(t, back, "imgs").(*py.List)
	assertEq(t, rows.Len(), len(ds.imgs))
	order := laila.As_manifest_(back).Global_ids()
	by_gid := map[string]any{}
	for i, g := range order {
		by_gid[g] = Unwrap(must(rows.Get(i)))
	}
	// random.Random(1).sample(range(len(imgs)), 20)
	sample := rand.New(rand.NewSource(1)).Perm(len(ds.imgs))[:20]
	for _, i := range sample {
		assertTrue(t, py.Eq(by_gid[ds.imgs[i].Global_id()], must(ds.imgs[i].Data())), i)
	}
}

// ---------------------------------------------------------------------------
// encrypted hand-off
// ---------------------------------------------------------------------------

func TestEncryptedHandoff_encrypted_sqlite_shared_between_policies(t *testing.T) {
	policy := Fresh_policy(t)
	disk_tmp := Disk_tmp(t)
	key := py.Fernet_generate_key()
	laila.Set_encryption_key(key)
	seq := must(laila.New_TransformationSequence(py.Kw{"transformations": []any{
		must(laila.New_Base64()), must(laila.New_Zlib()), must(laila.New_FernetEncryption())}}))
	pool := must(Make_pool("sqlite", disk_tmp, py.Kw{"transformations": seq}))
	path := w22_path_of(pool)
	mem_extend(t, policy, pool, "vault")
	n := N_scaled(500, 100)
	es, ids := constants(t, n, func(i int) any { return py.DictOf("secret", i, "blob", strings.Repeat("x", 200)) })
	func() {
		defer func() {
			Quiesce(policy)
			Close_pool(pool)
		}()
		w22_memorize(t, entries_list(es), "vault")
		// raw rows must not contain plaintext
		conn := must(pool.(*laila.SQLitePool).Connection_())
		cur := must(conn.Execute("SELECT value FROM laila_pool_entries LIMIT 1"))
		row := py.Str(cur.Fetchone()[0])
		assertTrue(t, !strings.Contains(row, "secret") && !strings.Contains(row, "xxxx"))
	}()
	// consumer policy with the right key
	consumer := must(laila.New_DefaultPolicy())
	laila.Activate_policy(consumer)
	re := must(w22_reopen("sqlite", path, py.Kw{"transformations": must(laila.New_TransformationSequence(py.Kw{"transformations": []any{
		must(laila.New_Base64()), must(laila.New_Zlib()), must(laila.New_FernetEncryption())}}))}))
	func() {
		defer func() {
			laila.Set_encryption_key(nil)
			Quiesce(consumer)
			Close_pool(re)
			laila.Activate_policy(policy)
			laila.Local_policies_.PopDefault(As_policy(consumer).Global_id(), nil)
		}()
		mem_extend(t, As_policy(consumer), re, "vault")
		got := w22_remember(t, ids, "vault")
		assertEq(t, w22_sorted_ints(got, "secret"), Range(n))
		// wrong key -> every read fails, nothing crashes
		laila.Set_encryption_key(py.Fernet_generate_key())
		err := w22_wait_err(func() (any, error) {
			return laila.Remember(es[0].Global_id(), py.Kw{"pool_nickname": "vault", "persist": false})
		}, T22_)
		assertTrue(t, err != nil, "expected the wrong key to fail the read")
	}()
}

// ---------------------------------------------------------------------------
// build pipelines
// ---------------------------------------------------------------------------

const W22_FEATURIZE_ = "def featurize(manifest):\n" +
	"    d = manifest.realized\n" +
	"    return {k: (e.data['amount'] * 2 if isinstance(e.data, dict) else e.data) for k, e in d.items()}\n"

const W22_REDUCE_ = "def reduce(manifest):\n" +
	"    d = manifest.realized\n" +
	"    total = 0\n" +
	"    for e in d.values():\n" +
	"        v = e.data\n" +
	"        total += sum(v.values()) if isinstance(v, dict) else v\n" +
	"    return total\n"

// w22_featurize_ is the Go body of W22_FEATURIZE_.
func w22_featurize_(arg any) (any, error) {
	d, err := laila.As_manifest_(arg).Realized()
	if err != nil {
		return nil, err
	}
	out := py.NewDict()
	for _, it := range d.(*py.Dict).Items() {
		v, err := laila.As_entry_(it.Value).Data()
		if err != nil {
			return nil, err
		}
		if dd, ok := v.(*py.Dict); ok {
			amount, err := py.Int(py.Must(dd.Getitem("amount")))
			if err != nil {
				return nil, err
			}
			out.Set(it.Key, amount*2)
		} else {
			out.Set(it.Key, v)
		}
	}
	return out, nil
}

// w22_reduce_ is the Go body of W22_REDUCE_.
func w22_reduce_(arg any) (any, error) {
	d, err := laila.As_manifest_(arg).Realized()
	if err != nil {
		return nil, err
	}
	total := 0
	for _, e := range d.(*py.Dict).Values() {
		v, err := laila.As_entry_(e).Data()
		if err != nil {
			return nil, err
		}
		if dd, ok := v.(*py.Dict); ok {
			for _, x := range dd.Values() {
				n, err := py.Int(x)
				if err != nil {
					return nil, err
				}
				total += n
			}
		} else {
			n, err := py.Int(v)
			if err != nil {
				return nil, err
			}
			total += n
		}
	}
	return total, nil
}

func init() {
	laila.Register_constitution(W22_FEATURIZE_, w22_featurize_)
	laila.Register_constitution(W22_REDUCE_, w22_reduce_)
}

// w22_build_run is ``TestBuildPipelines._run(pool_nickname, shards, per)``.
func w22_build_run(t *testing.T, pool_nickname string, shards, per int) {
	t.Helper()
	mapped := []*laila.Entry{}
	for s := 0; s < shards; s++ {
		items := py.NewDict()
		for i := 0; i < per; i++ {
			items.Set(fmt.Sprintf("r%d", i), must(laila.Entry_constant(py.DictOf("amount", s*per+i))))
		}
		m := must(laila.New_Manifest(py.Kw{"data": items}))
		w22_memorize(t, m, "") // alpha: where builds resolve from
		if pool_nickname != "" {
			w22_memorize(t, m, pool_nickname)
		}
		mapped = append(mapped, must(laila.Variable(py.Kw{"constitution": W22_FEATURIZE_, "manifest": m})))
	}
	var futs []any
	sw := Timed(func() {
		for _, e := range mapped {
			futs = append(futs, must(laila.Build(e)))
		}
		for _, f := range futs {
			must(Wait(f, T22_))
		}
	})
	for _, e := range mapped {
		assertEq(t, e.State(), laila.EntryState_READY)
	}
	Release_all(futs...)
	w22_memorize(t, entries_list(mapped), "")
	red_data := py.NewDict()
	for s, e := range mapped {
		red_data.Set(fmt.Sprintf("m%d", s), e)
	}
	red := must(laila.New_Manifest(py.Kw{"data": red_data}))
	w22_memorize(t, red, "")
	final := must(laila.Variable(py.Kw{"constitution": W22_REDUCE_, "manifest": red}))
	must(Wait(must(laila.Build(final)), T22_))
	expected := 2 * sum_range(shards*per)
	assertEq(t, must(final.Data()), expected)
	if pool_nickname != "" {
		w22_memorize(t, final, pool_nickname)
		back := w22_remember(t, final.Global_id(), pool_nickname)
		assertEq(t, Unwrap(back), expected)
	}
	assertTrue(t, sw.Elapsed < 240)
}

func TestBuildPipelines_map_reduce_dual_written_to_sqlite(t *testing.T) {
	policy := Fresh_policy(t)
	disk_tmp := Disk_tmp(t)
	pool := must(Make_pool("sqlite", disk_tmp))
	mem_extend(t, policy, pool, "db")
	defer func() {
		Quiesce(policy)
		Close_pool(pool)
	}()
	w22_build_run(t, "db", N_scaled(20, 5), N_scaled(100, 20))
}

// F-W22-01 (fixed): children/manifests resolve from the source pool first.
func TestBuildPipelines_build_over_manifest_in_named_pool_only(t *testing.T) {
	policy := Fresh_policy(t)
	disk_tmp := Disk_tmp(t)
	pool := must(Make_pool("sqlite", disk_tmp))
	mem_extend(t, policy, pool, "db")
	defer func() {
		Quiesce(policy)
		Close_pool(pool)
	}()
	items := py.NewDict()
	for i := 0; i < N_scaled(100, 20); i++ {
		items.Set(fmt.Sprintf("r%d", i), must(laila.Entry_constant(py.DictOf("amount", i))))
	}
	m := must(laila.New_Manifest(py.Kw{"data": items}))
	w22_memorize(t, m, "db")
	e := must(laila.Variable(py.Kw{"constitution": W22_FEATURIZE_, "manifest": m}))
	must(Wait(must(laila.Build(e)), T22_))
	assertEq(t, e.State(), laila.EntryState_READY)
}

// ---------------------------------------------------------------------------
// distributed compute
// ---------------------------------------------------------------------------

// w22_square_sum_extra is the ``extra`` code of the worker ChildPeer::
//
//	async def _sq(gid):
//	    e = laila.remember(gid, pool_nickname='store', persist=False)
//	    e = await e
//	    v = e.data if hasattr(e, 'data') else e
//	    return sum(x * x for x in v)
//	def square_sum(gid):
//	    return laila.command.submit([lambda: _sq(gid)])
//	object.__setattr__(policy, 'square_sum', square_sum)
func w22_square_sum_extra(args map[string]string) error {
	policy, err := laila.Get_active_policy()
	if err != nil {
		return err
	}
	sq := func(gid string) py.CoroutineFunc {
		return func(_ ...any) (any, error) {
			f, err := laila.Remember(gid, py.Kw{"pool_nickname": "store", "persist": false})
			if err != nil {
				return nil, err
			}
			e, err := py.Await(f)
			if err != nil {
				return nil, err
			}
			v := Unwrap(e)
			total := 0
			for _, x := range py.Iter(v) {
				n, err := py.Int(x)
				if err != nil {
					return nil, err
				}
				total += n * n
			}
			return total, nil
		}
	}
	square_sum := func(gid string) (any, error) {
		cmd, err := laila.Command()
		if err != nil {
			return nil, err
		}
		coro := sq(gid)
		return cmd.(*laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_).Submit([]any{
			func() (any, error) { return py.Coro(coro), nil }, // lambda: _sq(gid)
		})
	}
	As_policy(policy).Private_set("square_sum", square_sum)
	return nil
}

func init() {
	Child_extras["w22_square_sum"] = w22_square_sum_extra
}

func TestDistributedCompute_parent_child_work_loop(t *testing.T) {
	policy := Fresh_policy(t)
	disk_tmp := Disk_tmp(t)
	proto := must(laila.New_DefaultTCPProtocol(py.Kw{"host": "127.0.0.1"}))
	Comm(policy).Liveness_interval = 60.0
	check(t, Comm(policy).Add_connection(proto))
	cp := must(New_ChildPeer("worker", py.Kw{"kind": "tcp", "home": disk_tmp, "liveness": 60.0, "extra": "w22_square_sum"}).Enter())
	defer cp.Exit()
	gid := must(laila.Add_peer(cp.Uri(), cp.Secret()))
	proxy := must(laila.Request(gid))
	n := N_scaled(150, 30)
	results := map[int]any{}
	var lock sync.Mutex

	producer := func(th int) (any, error) {
		for j := th; j < n; j += 4 {
			vec := py.NewList()
			for x := j; x < j+10; x++ {
				vec.Append(x)
			}
			e, err := laila.Entry_constant(vec)
			if err != nil {
				return nil, err
			}
			f, err := laila.Memorize(e, py.Kw{"dst_policy": gid, "dst_pool": "store"})
			if err != nil {
				return nil, err
			}
			if _, err := Wait(f, T22_); err != nil {
				return nil, err
			}
			rf, err := proxy.Attr("square_sum").Invoke(e.Global_id())
			if err != nil {
				return nil, err
			}
			v, err := Wait(rf, T22_)
			if err != nil {
				return nil, err
			}
			val := Unwrap(v)
			Release_all(rf)
			lock.Lock()
			results[j] = val
			lock.Unlock()
		}
		return true, nil
	}

	sw := Timed(func() {
		for _, ok := range must(Run_threads(producer, 4)) {
			assertTrue(t, ok == true)
		}
	})
	assertEq(t, len(results), n)
	for j, v := range results {
		want := 0
		for x := j; x < j+10; x++ {
			want += x * x
		}
		assertEq(t, v, want)
	}
	assertTrue(t, sw.Elapsed < 240)
	Quiesce(policy)
}
