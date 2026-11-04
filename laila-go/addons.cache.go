package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// addons/cache.py -- Default caching process: ``RAM << HDD (HDF5) << CLOUD``.
//
// A single call wires the active policy's alpha pool (RAM) in front of a
// local HDF5 pool, which in turn fronts the cloud pool holding the
// dataset. Reads routed at the alpha pool (the default for
// ``laila.Remember`` / ``Manifest.Realized``) then walk the chain on a
// miss and write the blob back into every tier in front:
//
//	alpha misses -> hdd misses -> cloud hits -> hdd caches -> alpha caches
//
// Writes, deletes and ``exists`` stay local to the pool they are issued
// on, so ``laila.Forget(gids)`` evicts from RAM only and never touches the
// HDF5 cache or the cloud origin.

// Default_tiers registers *hdd* and *cloud* and wires ``laila.alpha_pool
// << hdd << cloud`` (``default_tiers(cloud, *, hdd=None,
// hdd_nickname="hdd", cloud_nickname="cloud", force=False)``).
//
//   - cloud: the origin pool holding the dataset: a live pool, or the gid
//     / nickname of a pool already registered with ``laila.Memory()``.
//   - hdd: the on-disk tier. Defaults to a fresh
//     ``HDF5Pool(nickname=hdd_nickname)`` whose file lives under
//     ``<laila root>/pools/<uuid>/pool.h5py`` (relocate with
//     Set_default_directory *before* calling).
//   - hdd_nickname, cloud_nickname: nicknames the two pools are registered
//     under when they are live objects (ignored when a string ref is
//     given).
//   - force (default false): when the alpha pool is already proxied to a
//     *different* pool, re-wire it instead of raising.
//
// Returns ``(hdd_pool, cloud_pool)``: the live pool objects, nearest tier
// first. The call is idempotent: wiring the same two pools twice is a
// no-op.
func Default_tiers(cloud any, kw ...py.Kw) (any, any, error) {
	opts := py.MergeKw(kw...)
	hdd := opts.Get("hdd", nil)
	hdd_nickname := py.Str(opts.Get("hdd_nickname", "hdd"))
	cloud_nickname := py.Str(opts.Get("cloud_nickname", "cloud"))
	force := py.Truthy(opts.Get("force", false))

	memory_any, err := Memory()
	if err != nil {
		return nil, nil, err
	}
	memory := As_central_memory_(memory_any)
	cloud_pool, err := Ensure_registered_(memory, cloud, cloud_nickname)
	if err != nil {
		return nil, nil, err
	}

	if py.IsNone(hdd) {
		fresh, err := New_HDF5Pool(py.Kw{"nickname": hdd_nickname})
		if err != nil {
			return nil, nil, err
		}
		hdd = fresh
	}
	hdd_pool, err := Ensure_registered_(memory, hdd, hdd_nickname)
	if err != nil {
		return nil, nil, err
	}

	if global_id_of_(hdd_pool) == global_id_of_(cloud_pool) {
		return nil, nil, py.ValueError.New("hdd and cloud must be different pools")
	}

	alpha_any, err := Alpha_pool()
	if err != nil {
		return nil, nil, err
	}
	alpha := As_pool_(alpha_any)
	current := alpha.Proxy_to()
	if !py.IsNone(current) && global_id_of_(current) != global_id_of_(hdd_pool) {
		if !force {
			return nil, nil, py.ValueError.Newf(
				"alpha pool is already proxied to %s; pass force=True to re-wire it",
				global_id_of_(current))
		}
		if err := alpha.Set_proxy_to(nil); err != nil {
			return nil, nil, err
		}
	}

	if py.IsNone(alpha.Proxy_to()) {
		if err := alpha.Set_proxy_to(hdd_pool); err != nil {
			return nil, nil, err
		}
	}
	hp := As_pool_(hdd_pool)
	if upstream := hp.Proxy_to(); py.IsNone(upstream) || global_id_of_(upstream) != global_id_of_(cloud_pool) {
		if err := hp.Set_proxy_to(cloud_pool); err != nil {
			return nil, nil, err
		}
	}
	return hdd_pool, cloud_pool, nil
}

// Ensure_registered_ returns the live pool for *pool_ref*, registering
// live objects under *nickname* (``_ensure_registered``).
func Ensure_registered_(memory *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_, pool_ref any, nickname string) (any, error) {
	if _, ok := pool_ref.(string); ok {
		return memory.Resolve_pool_ref_(pool_ref)
	}
	router := memory.router_()
	gid := global_id_of_(pool_ref)
	if !router.Pools.Contains(gid) {
		if err := memory.Extend(pool_ref, py.Kw{"pool_nickname": nickname}); err != nil {
			return nil, err
		}
	} else if !router.Pools_nicknames.Contains(nickname) {
		router.Pools_nicknames.Set(nickname, gid)
	}
	return router.Pools.Getitem(gid)
}
