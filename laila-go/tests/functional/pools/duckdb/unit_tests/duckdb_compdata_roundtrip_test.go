// Port of tests/functional/pools/duckdb/unit_tests/test_duckdb_compdata_roundtrip.py.
//
// Dtype-travel round-trip tests: ComputationalData (numpy / torch) via
// DuckDBPool.
//
// Auto-skips entirely if the ``duckdb`` package is not installed in the
// current environment (``DuckDBPool`` raises ImportError at instantiation
// time).
package unit_tests

import (
	"path/filepath"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	_ "github.com/amirzadehlambda/laila/laila-go/drivers/duckdb"
	"github.com/amirzadehlambda/laila/laila-go/py"
	"github.com/amirzadehlambda/laila/laila-go/tests/functional/pools/base/compdata_roundtrip"
)

// Probe that DuckDBPool is usable in this environment (the pool test
// in this package shares the flag: its class import never fails).
var HAVE_DUCKDBPOOL = func() bool {
	_probe_tmp := compdata_roundtrip.Make_tmpdir("laila_duckdb_probe_")
	_probe, err := laila.New_DuckDBPool(py.Kw{"file_path": filepath.Join(_probe_tmp, "probe.duckdb")})
	if err != nil {
		return false
	}
	_ = _probe.Close()
	return true
}()

func _make_duckdb_pool() compdata_roundtrip.Pool {
	tmp := compdata_roundtrip.Make_tmpdir("laila_duckdb_cd_rt_")
	return must(laila.New_DuckDBPool(py.Kw{"file_path": filepath.Join(tmp, "pool.duckdb")}))
}

// TestDuckDBCompDataRoundtrip is the matrix of numpy dtype round-trips
// through a real DuckDBPool.
func TestDuckDBCompDataRoundtrip(t *testing.T) {
	if !HAVE_DUCKDBPOOL {
		t.Skip("DuckDBPool unavailable (duckdb not installed)")
	}
	// test_backend_available: sentinel reporting 'skipped' when DuckDB is
	// unavailable.
	if !HAVE_DUCKDBPOOL {
		t.Fatal("HAVE_DUCKDBPOOL")
	}
	_N_REGISTERED := compdata_roundtrip.Register_dtype_matrix(t, _make_duckdb_pool, true)
	if _N_REGISTERED == 0 {
		t.Fatal("no dtype cases registered")
	}
}
