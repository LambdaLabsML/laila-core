// Port of tests/functional/pools/hdf5/unit_tests/test_hdf5_compdata_roundtrip.py.
//
// Dtype-travel round-trip tests: ComputationalData (numpy / torch) via
// HDF5Pool.
//
// Verifies that numpy and torch payloads survive a real HDF5 put/get with
// dtype, shape, and values exactly preserved.
package unit_tests

import (
	"path/filepath"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	_ "github.com/amirzadehlambda/laila/laila-go/drivers/hdf5"
	"github.com/amirzadehlambda/laila/laila-go/py"
	"github.com/amirzadehlambda/laila/laila-go/tests/functional/pools/base/compdata_roundtrip"
)


func _make_hdf5_pool() compdata_roundtrip.Pool {
	tmp := compdata_roundtrip.Make_tmpdir("laila_hdf5_cd_rt_")
	return must(laila.New_HDF5Pool(py.Kw{"file_path": filepath.Join(tmp, "pool.h5py")}))
}

// TestHDF5CompDataRoundtrip is the matrix of numpy dtype round-trips
// through a real HDF5Pool.
func TestHDF5CompDataRoundtrip(t *testing.T) {
	if !HAVE_HDF5POOL {
		t.Skip("HDF5Pool unavailable")
	}
	_N_REGISTERED := compdata_roundtrip.Register_dtype_matrix(t, _make_hdf5_pool, true)
	if _N_REGISTERED == 0 {
		t.Fatal("no dtype cases registered")
	}
}
