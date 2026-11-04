package laila

import "github.com/amirzadehlambda/laila/laila-go/py"

// policy/central/memory/router/pool_router.py -- pool router: resolves a
// memory request to a concrete Pool.
//
// The router maintains three coupled data structures:
//
//   - ``pools`` -- ``{global_id: Pool}`` -- the authoritative pool registry.
//   - ``pools_pq`` -- a ``(-affinity, global_id)`` priority queue used as
//     the fallback ordering when nothing more specific is requested.
//   - ``pools_nicknames`` -- ``{nickname: global_id}`` -- a friendly-name
//     index so callers can pass ``pool_nickname="cache"`` instead of a
//     full UUID-bearing gid.
//
// Routing precedence used by Route:
//
//  1. Explicit ``pool_id`` -- always wins.
//  2. ``pool_nickname`` -- resolved through ``pools_nicknames``.
//  3. (Future) ``affinity`` -- not yet implemented; falls through to (4).
//  4. The ``DEFAULT`` nickname's pool, which is auto-registered if missing.

// LAILA_IDENTIFIABLE_POOL_ROUTER_ selects the destination pool for
// ``memorize`` / ``remember`` / ``forget`` calls.
//
// Constructed automatically by every LAILA_IDENTIFIABLE_CENTRAL_MEMORY_
// and exposed as ``policy.central.memory.pool_router``. Users typically
// interact with it indirectly through ``Policy.extend`` and the
// ``pool_id`` / ``pool_nickname`` kwargs on the high-level memory API,
// but it is also usable directly for advanced multi-pool setups.
type LAILA_IDENTIFIABLE_POOL_ROUTER_ struct {
	LAILA_CLI_CAPABLE_CLASS_
	LAILA_IDENTIFIABLE_OBJECT_

	Pools           *py.Dict          `py:"pools,cli_exempt,factory=New_pools_"`
	Pools_pq        *py.PriorityQueue `py:"pools_pq,cli_exempt,factory=New_pools_pq_"`
	Pools_nicknames *py.Dict          `py:"pools_nicknames,factory=New_pools_nicknames_"`
}

// LAILA_IDENTIFIABLE_POOL_ROUTER_class is the class descriptor;
// ``_scopes`` defaults to ``[_POOL_ROUTER_SCOPE]``.
var LAILA_IDENTIFIABLE_POOL_ROUTER_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_POOL_ROUTER", (*LAILA_IDENTIFIABLE_POOL_ROUTER_)(nil),
	LAILA_CLI_CAPABLE_CLASS_class, LAILA_IDENTIFIABLE_OBJECT_class,
).
	Set_private_default("_scopes", func() any { return []string{POOL_ROUTER_SCOPE_} })

// DefaultPoolRouter is the class descriptor behind ``DefaultPoolRouter``.
var DefaultPoolRouter = LAILA_IDENTIFIABLE_POOL_ROUTER_class

// New_LAILA_IDENTIFIABLE_POOL_ROUTER_ is ``_LAILA_IDENTIFIABLE_POOL_ROUTER(**data)``.
func New_LAILA_IDENTIFIABLE_POOL_ROUTER_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_POOL_ROUTER_, error) {
	r := LAILA_IDENTIFIABLE_POOL_ROUTER_class.New().(*LAILA_IDENTIFIABLE_POOL_ROUTER_)
	if err := r.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return r, nil
}

// New_DefaultPoolRouter is ``DefaultPoolRouter(**kw)``.
func New_DefaultPoolRouter(kw ...py.Kw) (*LAILA_IDENTIFIABLE_POOL_ROUTER_, error) {
	return New_LAILA_IDENTIFIABLE_POOL_ROUTER_(kw...)
}

// New_pools_ is the ``pools`` default factory (``dict``).
func (r *LAILA_IDENTIFIABLE_POOL_ROUTER_) New_pools_() *py.Dict { return py.NewDict() }

// New_pools_pq_ is the ``pools_pq`` default factory (``PriorityQueue``).
func (r *LAILA_IDENTIFIABLE_POOL_ROUTER_) New_pools_pq_() *py.PriorityQueue {
	return py.New_PriorityQueue()
}

// New_pools_nicknames_ is the ``pools_nicknames`` default factory (``dict``).
func (r *LAILA_IDENTIFIABLE_POOL_ROUTER_) New_pools_nicknames_() *py.Dict { return py.NewDict() }

// Pool_router_base_ exposes the embedded base to code holding a subclass.
func (r *LAILA_IDENTIFIABLE_POOL_ROUTER_) Pool_router_base_() *LAILA_IDENTIFIABLE_POOL_ROUTER_ {
	return r
}

// Model_post_init auto-registers a DefaultPool (in-memory) when no pools
// were supplied.
//
// This is what makes a fresh ``DefaultPolicy()`` immediately usable
// without any pool wiring -- the in-memory default is good enough for
// tests and quickstart, and users override it by calling
// ``Policy.extend`` with a "real" pool (filesystem, S3, postgres, ...).
func (r *LAILA_IDENTIFIABLE_POOL_ROUTER_) Model_post_init(ctx any) error {
	if err := r.LAILA_IDENTIFIABLE_OBJECT_.Model_post_init(ctx); err != nil {
		return err
	}
	if err := CLI_CAPABLE_model_post_init(r.Self()); err != nil {
		return err
	}
	if r.Pools.Len() == 0 {
		pool, err := New_DefaultPool()
		if err != nil {
			return err
		}
		return r.Extend(pool, py.Kw{"affinity": 1, "pool_nickname": DEFAULT_POOL_NICKNAME_})
	}
	return nil
}

// Extend registers a pool with optional affinity priority and nickname
// (``extend(pool, *, affinity=None, pool_nickname=None)``).
//
// Stores *pool* under its ``global_id`` in Pools, pushes a
// ``(-affinity, gid)`` entry into the affinity priority queue (negated so
// higher affinity sorts first), and -- if a nickname was given -- adds a
// ``nickname -> gid`` entry to Pools_nicknames.
//
// Re-registering an existing nickname overwrites the previous binding;
// re-registering an existing gid overwrites the pool instance. That makes
// hot-swapping a pool implementation possible at the cost of users
// needing to be careful about accidentally shadowing a gid.
//
//   - affinity: routing priority (higher = preferred). Defaults to 0,
//     which sorts last in the priority queue.
//   - pool_nickname: human-readable alias for this pool, usable as the
//     ``pool_nickname`` kwarg on memory operations.
func (r *LAILA_IDENTIFIABLE_POOL_ROUTER_) Extend(pool any, kw ...py.Kw) error {
	opts := py.MergeKw(kw...)
	affinity := opts.Get("affinity", nil)
	pool_nickname := opts.Get("pool_nickname", nil)
	if py.IsNone(affinity) {
		affinity = 0 // farthest away
	}
	gid := global_id_of_(pool)
	// ``-affinity``: higher affinity sorts first.
	neg, err := py.Neg(affinity)
	if err != nil {
		return err
	}
	r.Pools_pq.Put(py.Tuple{neg, gid})
	r.Pools.Set(gid, pool)
	if !py.IsNone(pool_nickname) {
		r.Pools_nicknames.Set(py.Str(pool_nickname), gid)
	}
	return nil
}

// Route resolves the destination pool for *entries*
// (``route(entries, *, pool_id=None, pool_nickname=None, affinity=None)``).
//
// Today the routing decision does not actually depend on *entries* -- it
// is purely based on the explicit ``pool_id`` / ``pool_nickname``
// arguments and the default-pool fallback. The *entries* parameter is
// retained for forward compatibility with future strategies (e.g.
// content-based or affinity-based routing that picks per-entry
// destinations).
//
// KeyError if ``pool_id`` is given but not registered, or if
// ``pool_nickname`` does not resolve.
func (r *LAILA_IDENTIFIABLE_POOL_ROUTER_) Route(entries any, kw ...py.Kw) (any, error) {
	opts := py.MergeKw(kw...)
	pool_id := opts.Get("pool_id", nil)
	if !py.IsNone(pool_id) {
		return r.pool_item_(pool_id)
	}
	return r.Route_by_nickname_(py.Kw{"pool_nickname": opts.Get("pool_nickname", nil)})
}

// pool_item_ is ``self.pools[key]`` (KeyError when missing).
func (r *LAILA_IDENTIFIABLE_POOL_ROUTER_) pool_item_(key any) (any, error) {
	v, ok := r.Pools.Get(py.Str(key))
	if !ok {
		return nil, py.KeyError.New(py.Repr(key))
	}
	return v, nil
}

// Route_by_nickname_ resolves a pool by nickname, falling back to the
// default pool.
//
// When *pool_nickname* is None the fallback is the pool registered under
// DEFAULT_POOL_NICKNAME_ (always present because Model_post_init
// auto-registers one if missing).
//
// KeyError if *pool_nickname* is set but not registered, or if the default
// nickname has been removed and no fallback exists.
func (r *LAILA_IDENTIFIABLE_POOL_ROUTER_) Route_by_nickname_(kw ...py.Kw) (any, error) {
	pool_nickname := py.MergeKw(kw...).Get("pool_nickname", nil)
	name := DEFAULT_POOL_NICKNAME_
	if !py.IsNone(pool_nickname) {
		name = py.Str(pool_nickname)
	}
	gid, ok := r.Pools_nicknames.Get(name)
	if !ok {
		return nil, py.KeyError.New(py.Repr(name))
	}
	return r.pool_item_(gid)
}
