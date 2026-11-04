package laila

import (
	"sort"
	"sync"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// Optional third-party drivers (port glue; no Python counterpart).
//
// Every storage backend in ``laila/data`` guards its SDK import:
//
//	try:
//	    import boto3
//	except ImportError:
//	    boto3 = None
//
// and raises ``ImportError("boto3 is required for S3Pool")`` from the
// first call that needs it. The Go port keeps that contract with a
// process-wide registry keyed by the Python *module* name: a driver
// module under ``laila-go/drivers/<extra>`` (one per ``pyproject``
// extra, exactly like ``pip install laila-core[<extra>]``) implements
// the small interface the pool needs and registers it from ``init()``:
//
//	import _ "github.com/amirzadehlambda/laila/laila-go/drivers/s3"
//
// Until that blank import happens the pool behaves as Python does
// without the extra installed: the class exists, constructing it works
// where Python's does, and the first SDK call returns ImportError with
// the same message. Backends whose Python module imports the SDK
// unguarded (``h5py``, ``redis``) hide the class on ImportError; their
// Go classes exist but Driver_available reports False so ported tests
// skip exactly as ``HAVE_HDF5POOL = False`` does.
var (
	drivers_mu_ sync.RWMutex
	drivers_    = map[string]any{}
)

// Register_driver binds an implementation to a Python module name
// (``"boto3"``, ``"duckdb"``, ``"psycopg"``, ``"pymongo"``, ``"redis"``,
// ``"h5py"``, ``"google.cloud.storage"``, ``"azure.storage.blob"``,
// ``"huggingface_hub"``). Re-registering replaces the previous driver.
func Register_driver(module string, impl any) {
	drivers_mu_.Lock()
	drivers_[module] = impl
	drivers_mu_.Unlock()
}

// Unregister_driver removes a driver; False when none was bound.
func Unregister_driver(module string) bool {
	drivers_mu_.Lock()
	defer drivers_mu_.Unlock()
	if _, ok := drivers_[module]; !ok {
		return false
	}
	delete(drivers_, module)
	return true
}

// Driver_ is the module global (``boto3``): the registered implementation
// or nil, exactly like the ``except ImportError: boto3 = None`` fallback.
func Driver_(module string) any {
	drivers_mu_.RLock()
	defer drivers_mu_.RUnlock()
	return drivers_[module]
}

// Driver_available is ``<module> is not None``.
func Driver_available(module string) bool { return Driver_(module) != nil }

// Available_drivers lists the registered module names (sorted).
func Available_drivers() []string {
	drivers_mu_.RLock()
	defer drivers_mu_.RUnlock()
	out := make([]string, 0, len(drivers_))
	for k := range drivers_ {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Import_error_ is the ``ImportError`` a pool raises when its driver is
// missing; the message is the Python one.
func Import_error_(message string) error { return py.ImportError.New(message) }

// ---------------------------------------------------------------------------
// Row-store cursor shape shared by the SQL drivers (duckdb, psycopg).
// ---------------------------------------------------------------------------

// Sql_rows_ is the DB-API cursor slice the SQL-backed pools consume:
// ``cursor.fetchone()`` / ``cursor.fetchall()``.
type Sql_rows_ interface {
	// Fetchone returns the next row or nil when exhausted.
	Fetchone() ([]any, error)
	// Fetchall returns every remaining row.
	Fetchall() ([][]any, error)
}
