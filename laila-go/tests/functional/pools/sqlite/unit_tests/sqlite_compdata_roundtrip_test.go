// Port of tests/functional/pools/sqlite/unit_tests/test_sqlite_compdata_roundtrip.py.
//
// Dtype-travel round-trip tests: ComputationalData (numpy) via SQLitePool.
package unit_tests

import (
	"path/filepath"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
	"github.com/amirzadehlambda/laila/laila-go/tests/functional/pools/base/compdata_roundtrip"
)

func _make_sqlite_pool() compdata_roundtrip.Pool {
	tmp := compdata_roundtrip.Make_tmpdir("laila_sqlite_cd_rt_")
	return must(laila.New_SQLitePool(py.Kw{"file_path": filepath.Join(tmp, "pool.laila_sqlitedb")}))
}

// TestSQLiteCompDataRoundtrip is the matrix of numpy dtype round-trips
// through a real SQLitePool.
func TestSQLiteCompDataRoundtrip(t *testing.T) {
	_N_REGISTERED := compdata_roundtrip.Register_dtype_matrix(t, _make_sqlite_pool, true)
	if _N_REGISTERED == 0 {
		t.Fatal("no dtype cases registered")
	}
}
