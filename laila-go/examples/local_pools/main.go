// Go twin of tutorials/01_basics/02_local_pools.ipynb: register several
// local pools on the active policy, then memorize / remember / verify /
// forget the same numpy entry through each one.
//
// Python                                              Go
// ------                                              --
// from laila.data import SQLitePool                   data.New_SQLitePool(py.Kw{"file_path": ...})
// laila.memory.extend(pool, pool_nickname=nick)       laila.As_central_memory_(mem).Extend(pool, py.Kw{"pool_nickname": nick})
// with laila.guarantee: laila.memorize(e, dst_pool=n) laila.Guarantee.With(func() error { _, err := laila.Memorize(e, py.Kw{"dst_pool": n}); return err })
// laila.remember(gid, dst_pool=n).data                laila.Remember(gid, py.Kw{"dst_pool": n}) -> runtime.Wait -> .Data()
// np.array_equal(a, b)                                a.Eq(b)
// laila.forget(gid, pool=n)                           laila.Forget(gid, py.Kw{"pool": n})
//
// The tutorial's Filesystem / Redis / HDF5 pools need root (loop mounts),
// a Redis server and the cgo HDF5 driver respectively; this example uses
// the two backends that work anywhere: SQLite (a file under a temp dir)
// and the in-memory DefaultPool. Swap in any other ``*Pool`` from
// laila-go/data -- the verbs do not change.
//
// Run: go run ./examples/local_pools
package main

import (
	"fmt"
	"os"
	"path/filepath"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/data"
	"github.com/amirzadehlambda/laila/laila-go/py"
	"github.com/amirzadehlambda/laila/laila-go/runtime"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	defer laila.Terminate()

	policy, err := laila.New_DefaultPolicy()
	if err != nil {
		return err
	}
	laila.Activate_policy(policy)

	tmp, err := os.MkdirTemp("", "laila-local-pools-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)

	sqlite, err := data.New_SQLitePool(py.Kw{"nickname": "tutorial_sqlite", "file_path": filepath.Join(tmp, "tutorial.laila_sqlitedb")})
	if err != nil {
		return err
	}
	defer sqlite.Close()
	memory, err := laila.New_DefaultPool(py.Kw{"nickname": "tutorial_memory"})
	if err != nil {
		return err
	}
	pools := []struct {
		nick string
		pool any
	}{
		{"sqlite", sqlite},
		{"memory", memory},
	}

	// A 10x10 matrix of pseudo-random floats (np.random.randn(10, 10)).
	values := make([]float64, 100)
	seed := uint64(0x9E3779B97F4A7C15)
	for i := range values {
		seed ^= seed << 13
		seed ^= seed >> 7
		seed ^= seed << 17
		values[i] = float64(int64(seed>>11))/float64(1<<53)*2 - 1
	}
	matrix := py.NDArray_float64([]int{10, 10}, values)
	entry, err := laila.Constant(matrix, py.Kw{"nickname": "tutorial_matrix"})
	if err != nil {
		return err
	}
	fmt.Println("Entry global_id:", entry.Global_id())
	fmt.Println("Data shape:", matrix.Shape)

	mem, err := laila.Memory()
	if err != nil {
		return err
	}
	for _, p := range pools {
		if err := laila.As_central_memory_(mem).Extend(p.pool, py.Kw{"pool_nickname": p.nick}); err != nil {
			return err
		}

		if err := laila.Guarantee.With(func() error {
			_, err := laila.Memorize(entry, py.Kw{"dst_pool": p.nick})
			return err
		}); err != nil {
			return err
		}
		fmt.Printf("[%s] memorized\n", p.nick)

		var recalled any
		if err := laila.Guarantee.With(func() error {
			fut, err := laila.Remember(entry.Global_id(), py.Kw{"dst_pool": p.nick})
			if err != nil {
				return err
			}
			recalled, err = runtime.Wait(fut)
			return err
		}); err != nil {
			return err
		}
		recalled_data, err := recalled.(*laila.Entry).Data()
		if err != nil {
			return err
		}
		arr, ok := recalled_data.(*py.NDArray)
		if !ok {
			return fmt.Errorf("[%s] remembered a %s, not an ndarray", p.nick, py.Pytypename(recalled_data))
		}
		fmt.Printf("[%s] remembered - data shape: %v\n", p.nick, arr.Shape)

		if !matrix.Eq(arr) {
			return fmt.Errorf("data mismatch on %s", p.nick)
		}
		fmt.Printf("[%s] verified\n", p.nick)

		if err := laila.Guarantee.With(func() error {
			_, err := laila.Forget(entry.Global_id(), py.Kw{"pool": p.nick})
			return err
		}); err != nil {
			return err
		}
		fmt.Printf("[%s] cleaned up\n\n", p.nick)
	}
	return nil
}
