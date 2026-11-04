// Port of tests/functional/pools/filesystem/unit_tests/test_filesystem_compdata_roundtrip.py.
//
// Dtype-travel round-trip tests: ComputationalData (numpy) via FilesystemPool.
//
// Auto-skips entirely when the loopback ext4 image cannot be mounted (typical
// in environments without root / loop-device privileges).
package unit_tests

import (
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/tests/functional/pools/base/compdata_roundtrip"
)

// Probe: FilesystemPool construction requires mounting a loopback image.
var HAVE_FILESYSTEMPOOL = func() bool {
	_probe, err := laila.New_FilesystemPool()
	if err != nil {
		return false
	}
	_ = _probe.Close()
	return true
}()

func _make_filesystem_pool() compdata_roundtrip.Pool {
	return must(laila.New_FilesystemPool())
}

// TestFilesystemCompDataRoundtrip is the matrix of numpy dtype round-trips
// through a real FilesystemPool.
func TestFilesystemCompDataRoundtrip(t *testing.T) {
	if !HAVE_FILESYSTEMPOOL {
		t.Skip("FilesystemPool unavailable (cannot mount loopback image)")
	}
	// Sentinel: reports as 'skipped' when FilesystemPool is unavailable.
	t.Run("test_backend_available", func(t *testing.T) {
		if !HAVE_FILESYSTEMPOOL {
			t.Fatal("HAVE_FILESYSTEMPOOL is false")
		}
	})
	_N_REGISTERED := compdata_roundtrip.Register_dtype_matrix(t, _make_filesystem_pool, true)
	if _N_REGISTERED == 0 {
		t.Fatal("no dtype cases registered")
	}
}
