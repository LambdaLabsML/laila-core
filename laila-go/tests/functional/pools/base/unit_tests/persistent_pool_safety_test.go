// Port of tests/functional/pools/base/unit_tests/test_persistent_pool_safety.py.
//
// Fast regression guards for persistent-pool safety (F-05, F-06).
//
//   - F-06: ``close()`` takes the pool lock on SQLite / DuckDB / HDF5, so a
//     writer racing with ``close()`` sees ``RuntimeError("... is closed")``
//     rather than a backend-specific error (or a SIGSEGV).
//
// The HDF5 / DuckDB variants (and F-05, HDF5 durability across os._exit)
// are ported with those backends in Phase 5; here the mixin runs against
// SQLitePool.
package unit_tests

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// closeRacePool is the ``_CloseRaceMixin`` surface.
type closeRacePool interface {
	Write_(key string, value any) error
	Close() error
}

func closeWhileWritingRaisesRuntimeErrorOnly(t *testing.T, pool closeRacePool) {
	t.Helper()
	var mu sync.Mutex
	errs := []string{}
	var start sync.WaitGroup // threading.Barrier(5)
	start.Add(1)
	var wg sync.WaitGroup
	for n := 0; n < 4; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			start.Wait()
			for i := 0; i < 200; i++ {
				err := pool.Write_(fmt.Sprintf("w%d-%d", n, i), py.DictOf("i", i))
				if err == nil {
					continue
				}
				mu.Lock()
				if errors.Is(err, py.RuntimeError) {
					errs = append(errs, "RuntimeError")
				} else {
					errs = append(errs, py.Typename(err)) // the defect
				}
				mu.Unlock()
				return
			}
		}(n)
	}
	start.Done()
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	for _, e := range errs {
		if e != "RuntimeError" {
			t.Fatalf("errors = %v", errs)
		}
	}
	if err := pool.Write_("after", py.DictOf("x", 1)); !errors.Is(err, py.RuntimeError) {
		t.Fatalf("want RuntimeError after close, got %v", err)
	}
}

func TestSQLiteCloseWhileWritingRaisesRuntimeErrorOnly(t *testing.T) {
	tmp := t.TempDir()
	pool := must(laila.New_SQLitePool(py.Kw{"file_path": filepath.Join(tmp, "p.db"), "index_enabled": false}))
	closeWhileWritingRaisesRuntimeErrorOnly(t, pool)
}
