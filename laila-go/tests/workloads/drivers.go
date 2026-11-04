//go:build workload && cgo

// Python's workload suite always has ``duckdb`` installed (the
// ``[duckdb]`` extra) and gates HDF5 on ``import h5py``; the Go twins are
// cgo driver modules, so link them whenever cgo is on. Without cgo
// ``PERSISTENT_KINDS`` collapses to ``["sqlite"]`` the same way a missing
// ``h5py`` drops ``"hdf5"`` in Python.
package workloads

import (
	_ "github.com/amirzadehlambda/laila/laila-go/drivers/duckdb"
	_ "github.com/amirzadehlambda/laila/laila-go/drivers/hdf5"
)
