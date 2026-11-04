// Port of tests/functional/pools/filesystem/unit_tests/test_filesystem_pool.py.
//
// The loopback mount steps (``_resolve_pool_dir``, ``_is_mounted``,
// ``_mount_image``, ``_create_image_file``) are patched for the duration of
// each test exactly as ``unittest.mock.patch.object`` does in Python, so no
// root privileges are needed.
package unit_tests

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func assertCountEqual(t *testing.T, got any, want ...any) {
	t.Helper()
	g := must(py.Sorted(py.Iter(got)))
	w := must(py.Sorted(want))
	if !py.Eq(g, w) {
		t.Fatalf("got %s, want %s", py.Repr(g), py.Repr(w))
	}
}

func assertRaises(t *testing.T, cls *py.Type, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s, got nil", cls.Name)
	}
	if !errors.Is(err, cls) {
		t.Fatalf("expected %s, got %v", cls.Name, err)
	}
}

func jsonDumps(v any) string { return must(py.Json_dumps(v)) }

// mocks mirrors the patched ``_mount_image`` / ``_create_image_file`` /
// ``_is_mounted`` / ``_resolve_pool_dir`` members with call counters and
// overridable return values.
type mocks struct {
	mount_root string

	mount_image_calls       atomic.Int64
	create_image_file_calls atomic.Int64

	is_mounted_return_value         bool
	resolve_pool_dir_return_value   string // "" -> side_effect (mount_root / pool_id)
	resolve_image_path_return_value string // "" -> original body
}

func (m *mocks) reset_mock() {
	m.mount_image_calls.Store(0)
	m.create_image_file_calls.Store(0)
}

// setUp installs the patchers and builds self.pool; restores on cleanup.
func setUp(t *testing.T) (*mocks, *laila.FilesystemPool) {
	t.Helper()
	m := &mocks{mount_root: must(os.MkdirTemp("", "laila_filesystem_mount_"))}

	original_resolve_pool_dir := laila.FilesystemPool_resolve_pool_dir_
	original_is_mounted := laila.FilesystemPool_is_mounted_
	original_mount_image := laila.FilesystemPool_mount_image_
	original_create_image_file := laila.FilesystemPool_create_image_file_
	original_resolve_image_path := laila.FilesystemPool_resolve_image_path_

	laila.FilesystemPool_resolve_pool_dir_ = func(pool *laila.FilesystemPool) string {
		if m.resolve_pool_dir_return_value != "" {
			return m.resolve_pool_dir_return_value
		}
		return filepath.Join(m.mount_root, pool.Pool_id())
	}
	laila.FilesystemPool_is_mounted_ = func(pool *laila.FilesystemPool, path string) bool {
		return m.is_mounted_return_value
	}
	laila.FilesystemPool_mount_image_ = func(pool *laila.FilesystemPool) error {
		m.mount_image_calls.Add(1)
		return py.Mkdir_p(pool.Mount_dir())
	}
	laila.FilesystemPool_create_image_file_ = func(pool *laila.FilesystemPool) error {
		m.create_image_file_calls.Add(1)
		if err := py.Mkdir_p(filepath.Dir(pool.Image_path())); err != nil {
			return err
		}
		handle, err := os.OpenFile(pool.Image_path(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return py.FromGo(err)
		}
		return handle.Close()
	}
	laila.FilesystemPool_resolve_image_path_ = func(pool *laila.FilesystemPool) (string, error) {
		if m.resolve_image_path_return_value != "" {
			return m.resolve_image_path_return_value, nil
		}
		return original_resolve_image_path(pool)
	}

	pool := must(laila.New_FilesystemPool())

	t.Cleanup(func() {
		_ = pool.Close()
		laila.FilesystemPool_resolve_image_path_ = original_resolve_image_path
		laila.FilesystemPool_create_image_file_ = original_create_image_file
		laila.FilesystemPool_mount_image_ = original_mount_image
		laila.FilesystemPool_is_mounted_ = original_is_mounted
		laila.FilesystemPool_resolve_pool_dir_ = original_resolve_pool_dir
		_ = os.RemoveAll(m.mount_root)
	})
	return m, pool
}

func listdir(t *testing.T, dir string) []string {
	t.Helper()
	entries := must(os.ReadDir(dir))
	out := []string{}
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

func TestPoolIdFormat(t *testing.T) {
	_, pool := setUp(t)
	if !strings.HasPrefix(pool.Pool_id(), "LAILA:POOL:") {
		t.Fatal(pool.Pool_id())
	}
}

func TestImageAndMountPathsAreCreated(t *testing.T) {
	m, pool := setUp(t)
	if !py.Exists(pool.Image_path()) {
		t.Fatal("image path missing")
	}
	if !py.Is_dir(pool.Mount_dir()) {
		t.Fatal("mount dir missing")
	}
	if !strings.HasSuffix(pool.Image_path(), ".img") {
		t.Fatal(pool.Image_path())
	}
	assertEq(t, pool.Pool_dir(), filepath.Join(m.mount_root, pool.Pool_id()))
	assertEq(t, pool.Image_path(), filepath.Join(pool.Pool_dir(), pool.Pool_id()+".img"))
	assertEq(t, pool.Mount_dir(), filepath.Join(pool.Pool_dir(), "mnt"))
}

func TestGetMissingReturnsNoneWithoutCreatingFile(t *testing.T) {
	_, pool := setUp(t)
	assertEq(t, must(pool.Getitem("missing")), nil)
	assertEq(t, must(pool.Exists("missing")), false)
	assertEq(t, listdir(t, pool.Mount_dir()), []string{})
}

func TestSetAndGetRoundtripString(t *testing.T) {
	_, pool := setUp(t)
	must(0, pool.Setitem("a", jsonDumps("123")))
	assertEq(t, must(pool.Getitem("a")), "123")
}

func TestSetNonStringRaisesTypeError(t *testing.T) {
	_, pool := setUp(t)
	assertRaises(t, py.TypeError, pool.Setitem("bad", 123))
}

func TestSetNoneRaisesTypeError(t *testing.T) {
	_, pool := setUp(t)
	assertRaises(t, py.TypeError, pool.Setitem("none", nil))
}

func TestDelitemSilentIfMissing(t *testing.T) {
	_, pool := setUp(t)
	must(0, pool.Delitem("ghost"))
	assertEq(t, must(pool.Exists("ghost")), false)
}

func TestDelitemRemovesExisting(t *testing.T) {
	_, pool := setUp(t)
	must(0, pool.Setitem("x", jsonDumps("val")))
	assertEq(t, must(pool.Exists("x")), true)
	must(0, pool.Delitem("x"))
	assertEq(t, must(pool.Exists("x")), false)
	assertEq(t, must(pool.Getitem("x")), nil)
}

func TestExistsTrueWhenKeyPresent(t *testing.T) {
	_, pool := setUp(t)
	must(0, pool.Setitem("present", jsonDumps(7)))
	assertEq(t, must(pool.Exists("present")), true)
	assertEq(t, must(pool.Contains("present")), true)
}

func TestKeysSnapshotReturnsList(t *testing.T) {
	_, pool := setUp(t)
	must(0, pool.Setitem("a", jsonDumps(1)))
	must(0, pool.Setitem("b", jsonDumps(2)))
	ks := must(pool.Keys(py.Kw{"as_generator": false}))
	if _, ok := ks.(*py.List); !ok {
		t.Fatalf("keys() returned %T", ks)
	}
	assertCountEqual(t, ks, "a", "b")
}

func TestKeysGeneratorYieldsAllCurrentKeys(t *testing.T) {
	_, pool := setUp(t)
	items := []any{}
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("k%d", i)
		must(0, pool.Setitem(k, jsonDumps(i)))
		items = append(items, k)
	}
	got := must(py.Collect(must(pool.Keys(py.Kw{"as_generator": true})).(py.Iterator)))
	assertCountEqual(t, got, items...)
}

func TestKeysSnapshotIsNotAffectedByLaterMutations(t *testing.T) {
	_, pool := setUp(t)
	must(0, pool.Setitem("a", jsonDumps(1)))
	snap := must(pool.Keys())
	must(0, pool.Setitem("b", jsonDumps(2)))
	assertCountEqual(t, snap, "a")
}

func TestOverwriteValue(t *testing.T) {
	_, pool := setUp(t)
	must(0, pool.Setitem("k", jsonDumps("v1")))
	must(0, pool.Setitem("k", jsonDumps("v2")))
	assertEq(t, must(pool.Getitem("k")), "v2")
}

func TestMultipleInstancesHaveIndependentMounts(t *testing.T) {
	setUp(t)
	p1 := must(laila.New_FilesystemPool(py.Kw{"nickname": "filesystem-pool-one"}))
	p2 := must(laila.New_FilesystemPool(py.Kw{"nickname": "filesystem-pool-two"}))
	defer p1.Close()
	defer p2.Close()
	must(0, p1.Setitem("x", jsonDumps(1)))
	must(0, p2.Setitem("x", jsonDumps(2)))
	assertEq(t, must(p1.Getitem("x")), 1)
	assertEq(t, must(p2.Getitem("x")), 2)
	if p1.Mount_dir() == p2.Mount_dir() {
		t.Fatal("mount dirs must differ")
	}
}

func TestConcurrentWritesNoLoss(t *testing.T) {
	_, pool := setUp(t)
	n := 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			must(0, pool.Setitem(fmt.Sprint(i), jsonDumps(i)))
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		assertEq(t, must(pool.Getitem(fmt.Sprint(i))), i)
	}
}

func TestConcurrentWritesAndReads(t *testing.T) {
	_, pool := setUp(t)
	stop := py.NewEvent()
	var mu sync.Mutex
	read_errors := []string{}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		i := 0
		for !stop.Is_set() {
			must(0, pool.Setitem(fmt.Sprint(i), jsonDumps(i)))
			i = (i + 1) % 10
		}
	}()
	go func() {
		defer wg.Done()
		for !stop.Is_set() {
			for _, k := range py.Iter(must(pool.Keys())) {
				v := must(pool.Getitem(k))
				if v != nil {
					// isinstance(v, int): json.loads yields int64 in the port.
					if _, ok := v.(int64); !ok {
						mu.Lock()
						read_errors = append(read_errors, fmt.Sprintf("bad value %s", py.Repr(v)))
						mu.Unlock()
					}
				}
			}
		}
	}()
	time.Sleep(200 * time.Millisecond)
	stop.Set()
	wg.Wait()

	assertEq(t, read_errors, []string{})
}

func TestStoreEntryPayloadAsObject(t *testing.T) {
	_, pool := setUp(t)
	e := must(laila.Constant(py.DictOf("x", 1)))
	payload := py.DictOf("global_id", e.Global_id(), "data", must(e.Data()))
	must(0, pool.Setitem(e.Global_id(), payload))
	assertEq(t, must(pool.Getitem(e.Global_id())), payload)
}

func TestCloseIsIdempotent(t *testing.T) {
	_, pool := setUp(t)
	must(0, pool.Close())
	must(0, pool.Close())
}

func TestDeleteThenRecreate(t *testing.T) {
	_, pool := setUp(t)
	must(0, pool.Setitem("z", jsonDumps(9)))
	must(0, pool.Delitem("z"))
	assertEq(t, must(pool.Exists("z")), false)
	must(0, pool.Setitem("z", jsonDumps(10)))
	assertEq(t, must(pool.Getitem("z")), 10)
}

func TestPathsFollowFixedPoolLayout(t *testing.T) {
	m, _ := setUp(t)
	pool := must(laila.New_FilesystemPool(py.Kw{"nickname": "filesystem-fixed-paths"}))
	defer pool.Close()
	assertEq(t, pool.Pool_dir(), filepath.Join(m.mount_root, pool.Pool_id()))
	assertEq(t, pool.Image_path(), filepath.Join(pool.Pool_dir(), pool.Pool_id()+".img"))
	assertEq(t, pool.Mount_dir(), filepath.Join(pool.Pool_dir(), "mnt"))
	if !py.Exists(pool.Image_path()) {
		t.Fatal("image path missing")
	}
}

func TestMountDirArgumentIsRejected(t *testing.T) {
	setUp(t)
	_, err := laila.New_FilesystemPool(py.Kw{"mount_dir": "/tmp/not-allowed"})
	assertRaises(t, py.ValueError, err)
}

func TestImageDirArgumentIsRejected(t *testing.T) {
	setUp(t)
	_, err := laila.New_FilesystemPool(py.Kw{"image_dir": "/tmp/not-allowed"})
	assertRaises(t, py.ValueError, err)
}

func TestImagePathArgumentIsRejected(t *testing.T) {
	setUp(t)
	_, err := laila.New_FilesystemPool(py.Kw{"image_path": filepath.Join("/tmp", "bad.img")})
	assertRaises(t, py.ValueError, err)
}

func TestPersistenceAcrossPoolInstances(t *testing.T) {
	m, _ := setUp(t)
	pool_dir := filepath.Join(m.mount_root, "persistent-pool")
	image_path := filepath.Join(pool_dir, "persistent.img")
	mount_dir := filepath.Join(pool_dir, "mnt")
	must(0, py.Mkdir_p(mount_dir))

	original_pool_dir_return := m.resolve_pool_dir_return_value
	m.resolve_pool_dir_return_value = pool_dir
	m.resolve_image_path_return_value = image_path
	var pool2 *laila.FilesystemPool
	func() {
		defer func() {
			m.resolve_image_path_return_value = ""
			m.resolve_pool_dir_return_value = original_pool_dir_return
		}()
		pool1 := must(laila.New_FilesystemPool())
		must(0, pool1.Setitem("persistent", jsonDumps(py.DictFromKw(py.Kw{"a": 1}))))
		must(0, pool1.Close())
		pool2 = must(laila.New_FilesystemPool())
	}()
	defer pool2.Close()
	assertEq(t, must(pool2.Getitem("persistent")), py.DictFromKw(py.Kw{"a": 1}))
}

func TestKeyWithSpecialCharsRoundtrip(t *testing.T) {
	_, pool := setUp(t)
	key := "key/with:special=chars"
	must(0, pool.Setitem(key, jsonDumps("value")))
	assertEq(t, must(pool.Exists(key)), true)
	assertEq(t, must(pool.Getitem(key)), "value")
	must(0, pool.Delitem(key))
	assertEq(t, must(pool.Exists(key)), false)
}

func TestReusesExistingMountWithoutMountingImage(t *testing.T) {
	m, _ := setUp(t)
	pool_dir := filepath.Join(m.mount_root, "reused-pool")
	image_path := filepath.Join(pool_dir, "reused.img")
	mount_dir := filepath.Join(pool_dir, "mnt")
	must(0, py.Mkdir_p(pool_dir))
	must(0, py.Mkdir_p(mount_dir))
	must(0, must(os.OpenFile(image_path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)).Close())

	m.reset_mock()

	original_pool_dir_return := m.resolve_pool_dir_return_value
	original_is_mounted := m.is_mounted_return_value
	m.resolve_pool_dir_return_value = pool_dir
	m.resolve_image_path_return_value = image_path
	var pool *laila.FilesystemPool
	func() {
		defer func() {
			m.resolve_image_path_return_value = ""
			m.resolve_pool_dir_return_value = original_pool_dir_return
			m.is_mounted_return_value = original_is_mounted
			if pool != nil {
				_ = pool.Close()
			}
		}()
		m.is_mounted_return_value = true
		pool = must(laila.New_FilesystemPool())
		assertEq(t, pool.Mount_dir(), mount_dir)
		assertEq(t, m.mount_image_calls.Load(), int64(0))
		assertEq(t, m.create_image_file_calls.Load(), int64(0))
	}()
	defer os.RemoveAll(pool_dir)
	if !py.Is_dir(mount_dir) {
		t.Fatal("mount dir missing")
	}
}

func TestExistingImageIsMountedWithoutRecreatingImage(t *testing.T) {
	m, _ := setUp(t)
	pool_dir := filepath.Join(m.mount_root, "existing-pool")
	image_path := filepath.Join(pool_dir, "existing.img")
	must(0, py.Mkdir_p(pool_dir))
	must(0, os.WriteFile(image_path, []byte("existing-image"), 0o644))

	m.reset_mock()

	original_pool_dir_return := m.resolve_pool_dir_return_value
	m.resolve_pool_dir_return_value = pool_dir
	m.resolve_image_path_return_value = image_path
	var pool *laila.FilesystemPool
	func() {
		defer func() {
			m.resolve_image_path_return_value = ""
			m.resolve_pool_dir_return_value = original_pool_dir_return
		}()
		pool = must(laila.New_FilesystemPool())
	}()
	defer os.RemoveAll(pool_dir)
	defer pool.Close()
	assertEq(t, m.mount_image_calls.Load(), int64(1))
	assertEq(t, m.create_image_file_calls.Load(), int64(0))
	assertEq(t, pool.Image_path(), image_path)
}
