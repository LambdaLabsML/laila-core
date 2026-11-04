package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// data/schema/pool_wrapper.py -- PoolWrapper: lightweight proxy binding a
// pool to a manifest for scoped copies.
//
// Returned by LAILA_IDENTIFIABLE_POOL_.Getitem when the key is a
// Manifest. The wrapper exists so that the ``<=`` operator can copy *only
// the entries listed by the manifest* between two pools, instead of
// copying everything::
//
//	my_future = pool_dest[manifest] <= pool_src[manifest]
//
// # How the copy works
//
// The right-hand side selects the source: ``pool_src[manifest]`` returns a
// PoolWrapper over ``pool_src``. The left-hand side selects the
// destination the same way. Le then walks the manifest's leaf entries,
// fetching each one from the source pool via ``memory.remember`` (which
// handles serialisation and proxy chains for us), then storing it in the
// destination pool via ``memory.memorize``. The whole copy runs
// concurrently with a 4-way semaphore on a daemon thread, returning a
// GroupFuture whose children correspond to per-entry copy operations.
//
// # Concurrency model
//
// PoolWrapper itself holds no lock -- all I/O is delegated to the wrapped
// pool's existing public methods, which already take care of their own
// atomic-lock coverage. The ``<=`` runner builds a GroupFuture of
// per-entry ConcurrentPackageFuture children that the active policy's
// future bank can observe like any other group future.

// PoolWrapper is a manifest-scoped view over a pool, used as the LHS /
// RHS of ``<=`` copies.
//
// A wrapper is conceptually "this pool, but only the entries listed by
// *manifest*". The type is intentionally minimal: just two slots (Pool
// and Manifest) and one operator (Le) that performs the actual cross-pool
// copy.
//
//   - Pool: the bound pool instance. Must be reachable from the active
//     local policy's central memory.
//   - Manifest: the manifest whose ``global_id`` leaves define the
//     entries in scope. Both the source and destination wrapper are
//     expected to use the same manifest in a copy operation.
type PoolWrapper struct {
	Pool     any
	Manifest any
}

// New_PoolWrapper is ``PoolWrapper(pool, manifest)``.
func New_PoolWrapper(pool any, manifest any) *PoolWrapper {
	return &PoolWrapper{Pool: pool, Manifest: manifest}
}

// Le is ``self <= other`` -- copy *other*'s manifest entries into this
// wrapper's pool.
//
// Spawns a daemon thread running an asyncio loop that, for each entry id
// in the manifest, ``remember``s the entry from the source pool and
// ``memorize``s it into the destination pool. A four-way semaphore caps
// the in-flight copy concurrency.
//
// Each per-entry copy populates a corresponding ConcurrentPackageFuture in
// a parent GroupFuture so callers can wait on the whole batch with the
// usual future API.
//
// *other* is the source pool + manifest binding; the manifest is read
// from *self* (both wrappers should carry the same manifest in a normal
// copy). TypeError when *other* is not a PoolWrapper (``NotImplemented``).
//
// Raises RuntimeError (inside the children) if the source pool's
// ``remember`` returns None (which happens when the active policy's
// *default* pool is used as the source -- manifest copies require a
// non-default source).
func (w *PoolWrapper) Le(other any) (any, error) {
	src, ok := other.(*PoolWrapper)
	if !ok {
		return nil, py.TypeError.Newf("'<=' not supported between instances of 'PoolWrapper' and '%s'",
			py.Pytypename(other))
	}

	active_policy, err := Get_active_policy()
	if err != nil {
		return nil, err
	}
	command := getattr(getattr(active_policy, "central"), "command")
	internal_taskforce := getattr(command, "internal_taskforce")
	policy_gid := global_id_of_(active_policy)

	entry_ids := []string{}
	for _, gid := range py.Iter(w.Manifest) {
		entry_ids = append(entry_ids, py.Str(gid))
	}

	children := []*ConcurrentPackageFuture{}
	future_ids := []any{}
	for _, entry_id := range entry_ids {
		child, err := New_ConcurrentPackageFuture(py.Kw{
			"taskforce_id": internal_taskforce,
			"policy_id":    policy_gid,
			"purpose":      "manifest_copy:" + entry_id,
		})
		if err != nil {
			return nil, err
		}
		children = append(children, child)
		future_ids = append(future_ids, child.Global_id())
	}

	group_future, err := New_GroupFuture(py.Kw{
		"taskforce_id": internal_taskforce,
		"policy_id":    policy_gid,
		"future_ids":   py.NewList(future_ids...),
	})
	if err != nil {
		return nil, err
	}
	for _, child := range children {
		child.Future_group_id = group_future.Global_id()
	}

	src_pool := As_pool_(src.Pool)
	dest_pool := As_pool_(w.Pool)
	memory := getattr(getattr(active_policy, "central"), "memory")
	future_bank := getattr(active_policy, "future_bank").(*py.Dict)

	semaphore := py.New_AsyncSemaphore(4)

	copy_one := func(entry_id string, child *ConcurrentPackageFuture) func() (any, error) {
		return func() (any, error) {
			err := attempt(func() error {
				if err := semaphore.Acquire(); err != nil {
					return err
				}
				defer semaphore.Release()
				child.Set_status(FutureStatus_RUNNING)

				remember_ref, err := callm(memory, "remember",
					py.Kw{"entry_ids": entry_id, "pool_id": src_pool.Global_id()})
				if err != nil {
					return err
				}
				if py.IsNone(remember_ref) {
					return py.RuntimeError.New("Manifest pool copy requires a non-default source pool.")
				}
				remember_fut := future_bank.GetDefault(global_id_of_(remember_ref), nil)
				if py.IsNone(remember_fut) {
					return py.KeyError.New(global_id_of_(remember_ref))
				}
				entry, err := await_future_(remember_fut)
				if err != nil {
					return err
				}

				memorize_ref, err := callm(memory, "memorize",
					py.Kw{"entries": entry, "pool_id": dest_pool.Global_id()})
				if err != nil {
					return err
				}
				if !py.IsNone(memorize_ref) {
					memorize_fut := future_bank.GetDefault(global_id_of_(memorize_ref), nil)
					if !py.IsNone(memorize_fut) {
						if _, err := await_future_(memorize_fut); err != nil {
							return err
						}
					}
				}

				child.Set_exception(nil)
				child.Set_result(entry_id)
				child.Set_status(FutureStatus_FINISHED)
				return nil
			})
			if err != nil {
				child.Set_exception(err)
				child.Set_result(nil)
				child.Set_status(FutureStatus_ERROR)
			}
			return nil, nil
		}
	}

	copy_all := func() (any, error) {
		fns := []func() (any, error){}
		for i, entry_id := range entry_ids {
			fns = append(fns, copy_one(entry_id, children[i]))
		}
		return py.Gather(fns, py.Kw{"return_exceptions": true})
	}

	run_copy_loop := func() {
		if _, err := py.Run(copy_all); err != nil {
			fail_pending_children_(children, err)
		}
	}

	if err := py.New_Thread(py.Kw{
		"target": run_copy_loop,
		"name":   "ManifestCopy-" + group_future.Global_id(),
		"daemon": true,
	}).Start(); err != nil {
		return nil, err
	}

	return group_future, nil
}

// fail_pending_children_ flips every child that has not reached a
// terminal state to ERROR with *err* (the ``except Exception`` branch of
// the detached event-loop runners).
func fail_pending_children_(children []*ConcurrentPackageFuture, err error) {
	for _, child := range children {
		switch child.Status() {
		case FutureStatus_FINISHED, FutureStatus_ERROR, FutureStatus_CANCELLED:
			continue
		}
		child.Set_exception(err)
		child.Set_result(nil)
		child.Set_status(FutureStatus_ERROR)
	}
}
