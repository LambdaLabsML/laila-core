// Port of tests/functional/pools/mempool/unit_tests/test_memory_compdata_roundtrip.py.
//
// Dtype-travel round-trip tests: ComputationalData (numpy) via MemoryPool.
//
// The in-memory base pool accepts any object, so the envelope dict is
// stored as-is (no JSON encoding).
package unit_tests

import (
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/tests/functional/pools/base/compdata_roundtrip"
)

func _make_memory_pool() compdata_roundtrip.Pool {
	return must(laila.New_LAILA_IDENTIFIABLE_POOL_())
}

// TestMemoryCompDataRoundtrip is the matrix of numpy dtype round-trips
// through the in-memory base pool.
func TestMemoryCompDataRoundtrip(t *testing.T) {
	_N_REGISTERED := compdata_roundtrip.Register_dtype_matrix(t, _make_memory_pool, false)
	if _N_REGISTERED == 0 {
		t.Fatal("no dtype cases registered")
	}
}
