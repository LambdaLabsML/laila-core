package laila

import "github.com/amirzadehlambda/laila/laila-go/py"

// policy/central/command/taskforce/process_pool_executor/future.py --
// concrete future wrapping ``concurrent.futures.Future`` for process-pool
// execution.

// ProcessPackageFuture is the wrapper around a ``ProcessPoolExecutor``
// native future.
type ProcessPackageFuture struct {
	ConcurrentPackageFuture
}

// ProcessPackageFuture_class is the class descriptor.
var ProcessPackageFuture_class = py.NewClass("ProcessPackageFuture", (*ProcessPackageFuture)(nil), ConcurrentPackageFuture_class)

// New_ProcessPackageFuture is ``ProcessPackageFuture(**data)``.
func New_ProcessPackageFuture(kw ...py.Kw) (*ProcessPackageFuture, error) {
	f := ProcessPackageFuture_class.New().(*ProcessPackageFuture)
	if err := f.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return f, nil
}
