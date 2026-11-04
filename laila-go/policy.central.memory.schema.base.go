package laila

import (
	"sort"
	"strings"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/memory/schema/base.py -- central memory system:
// memorize, remember, forget, and pool duplication.
//
// This module hosts LAILA_IDENTIFIABLE_CENTRAL_MEMORY_, the backbone of
// every policy's storage layer. It coordinates four moving parts:
//
//   - the PoolRouter (one per central memory) that picks destinations,
//   - the *alpha pool* -- a privileged pool that acts as the default
//     destination and the cache target for ``remember(..., persist=True)``,
//   - the per-pool TransformationSequence that defines the serialization
//     pipeline (e.g. ``base64 -> zlib -> msgpack``),
//   - the internal task-force (``command.internal_taskforce``), where every
//     per-entry coroutine is submitted for concurrent I/O, separate from
//     the alpha task-force that runs user-submitted work.
//
// Two operation flavors live side-by-side:
//
//   - *parallel-individual* paths (Parallel_individual_record_ / Fetch_ /
//     Delete_) -- one async coroutine per entry, suitable for any pool that
//     exposes per-key I/O. The default for everything.
//   - *batch-accelerated* paths (``Batch_accelerated_*``) -- placeholders
//     for pools that can multiplex many keys in a single round-trip (``COPY``
//     for postgres, ``mset`` for redis, etc.). Not yet implemented; the
//     placeholders raise.
//
// The Remember_with_persist_ / Duplicate_pool_ helpers run entire async
// pipelines on a daemon thread so the work proceeds concurrently without
// polluting the caller's event loop.

// LAILA_IDENTIFIABLE_CENTRAL_MEMORY_ is the central memory controller for
// storing, retrieving, and deleting entries.
//
// Owns a PoolRouter that decides which pool a given operation lands on,
// plus an *alpha pool* selection that doubles as the default destination
// and the cache target for cache-back reads.
//
// Three top-level public methods drive the system:
//
//   - Memorize -- write entries to the routed pool.
//   - Remember -- read entries (with optional cache-back into the alpha
//     pool).
//   - Forget   -- delete entries from the routed pool.
//
// All three are decorated with Ensure_list so callers can pass either a
// single entry / id or a list and get uniform list-shaped behavior
// internally.
type LAILA_IDENTIFIABLE_CENTRAL_MEMORY_ struct {
	LAILA_CLI_CAPABLE_CLASS_
	LAILA_IDENTIFIABLE_OBJECT_

	// Policy_ref_ is the weak back-reference to the owning policy, set by
	// LAILA_IDENTIFIABLE_POLICY_.Model_post_init. Memory operations submit
	// their coroutines to the *owner's* command (not the process-global
	// active policy), so ``laila.memorize(..., dst_policy=B)`` never has to
	// transiently activate ``B``.
	Policy_ref_ any `py:"_policy_ref"`
	Pool_router any `py:"pool_router"`
	Alpha_pool  any `py:"alpha_pool"`
}

// LAILA_IDENTIFIABLE_CENTRAL_MEMORY_class is the class descriptor;
// ``_scopes`` defaults to ``[_CENTRAL_MEMORY_SCOPE]``.
var LAILA_IDENTIFIABLE_CENTRAL_MEMORY_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_CENTRAL_MEMORY", (*LAILA_IDENTIFIABLE_CENTRAL_MEMORY_)(nil),
	LAILA_CLI_CAPABLE_CLASS_class, LAILA_IDENTIFIABLE_OBJECT_class,
).
	Set_private_default("_scopes", func() any { return []string{CENTRAL_MEMORY_SCOPE_} })

// New_LAILA_IDENTIFIABLE_CENTRAL_MEMORY_ is
// ``_LAILA_IDENTIFIABLE_CENTRAL_MEMORY(**data)``.
func New_LAILA_IDENTIFIABLE_CENTRAL_MEMORY_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_CENTRAL_MEMORY_, error) {
	m := LAILA_IDENTIFIABLE_CENTRAL_MEMORY_class.New().(*LAILA_IDENTIFIABLE_CENTRAL_MEMORY_)
	if err := m.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return m, nil
}

// New_DefaultCentralMemory is ``DefaultCentralMemory(**kw)``.
func New_DefaultCentralMemory(kw ...py.Kw) (*LAILA_IDENTIFIABLE_CENTRAL_MEMORY_, error) {
	return New_LAILA_IDENTIFIABLE_CENTRAL_MEMORY_(kw...)
}

// Central_memory_base_ exposes the embedded base to code holding a
// subclass.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Central_memory_base_() *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_ {
	return m
}

// Central_memory_hooks is the set of internal dispatch methods a subclass
// may override (Python resolves these as instance attributes, so tests
// stub them with ``Mock``; the Go port dispatches through Self()).
type Central_memory_hooks interface {
	Record_(entries *py.List, pool *LAILA_IDENTIFIABLE_POOL_) (any, error)
	Parallel_individual_record_(entries *py.List, pool *LAILA_IDENTIFIABLE_POOL_) (any, error)
	Batch_accelerated_record_(entries *py.List, pool *LAILA_IDENTIFIABLE_POOL_) (any, error)
	Fetch_(entry_ids []string, kw ...py.Kw) (any, error)
	Parallel_individual_fetch_(entry_ids []string, kw ...py.Kw) (any, error)
	Batch_accelerated_fetch_(keys []string, kw ...py.Kw) (any, error)
	Delete_(entry_ids []string, pool *LAILA_IDENTIFIABLE_POOL_) (any, error)
	Parallel_individual_delete_(entry_ids []string, pool *LAILA_IDENTIFIABLE_POOL_) (any, error)
	Batch_accelerated_delete_(entry_ids []string, pool *LAILA_IDENTIFIABLE_POOL_) (any, error)
}

// hooks is the most-derived dispatch implementation.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) hooks() Central_memory_hooks {
	if h, ok := m.Self().(Central_memory_hooks); ok {
		return h
	}
	return m
}

// As_central_memory_ returns the embedded central memory of any derived
// instance (nil when v is not one).
func As_central_memory_(v any) *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_ {
	if m, ok := v.(*LAILA_IDENTIFIABLE_CENTRAL_MEMORY_); ok {
		return m
	}
	if b, ok := v.(interface {
		Central_memory_base_() *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_
	}); ok {
		return b.Central_memory_base_()
	}
	return nil
}

// ------------------------------------------------------------------
// Owner resolution
// ------------------------------------------------------------------

// Policy_ returns the policy that owns this memory.
//
// Falls back to the process-global active policy for standalone memory
// objects (unit tests, ad-hoc construction) that were never attached to a
// policy.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Policy_() (any, error) {
	if ref, ok := m.Policy_ref_.(py.Weak); ok && ref != nil {
		if owner := ref.Value(); !py.IsNone(owner) {
			return owner, nil
		}
	}
	return Get_active_policy()
}

// Command_ returns the owning policy's central command.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Command_() (*LAILA_IDENTIFIABLE_CENTRAL_COMMAND_, error) {
	policy, err := m.Policy_()
	if err != nil {
		return nil, err
	}
	cmd, ok := getattr(getattr(policy, "central"), "command").(interface {
		Central_command_base_() *LAILA_IDENTIFIABLE_CENTRAL_COMMAND_
	})
	if !ok {
		return nil, py.AttributeError.New("'NoneType' object has no attribute 'command'")
	}
	return cmd.Central_command_base_(), nil
}

// Owner_gid_ returns the owning policy's global id (used as record
// provenance).
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Owner_gid_() (string, error) {
	policy, err := m.Policy_()
	if err != nil {
		return "", err
	}
	return global_id_of_(policy), nil
}

// Future_bank_ returns the bank futures created by this memory
// self-register in.
//
// Futures always register with the active *local* policy (the caller's
// scope, so ``laila.guarantee`` sees them) regardless of which policy's
// command runs them.
func Future_bank_() (*py.Dict, error) {
	policy, err := Get_active_local_policy_()
	if err != nil {
		return nil, err
	}
	bank, ok := getattr(policy, "future_bank").(*py.Dict)
	if !ok {
		return nil, py.AttributeError.New("policy has no future_bank")
	}
	return bank, nil
}

// Model_post_init wires a default PoolRouter and picks an alpha pool.
//
// Run order:
//
//  1. If no router was supplied, instantiate a DefaultPoolRouter (which
//     itself auto-registers an in-memory DefaultPool under the ``DEFAULT``
//     nickname).
//  2. If no ``alpha_pool`` was set, prefer the pool registered under
//     ``DEFAULT``. If for any reason that nickname is missing, create a
//     fresh DefaultPool and adopt it.
//
// After this hook returns, ``self.alpha_pool`` is guaranteed to resolve
// through ``self.pool_router.pools[self.alpha_pool]``.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Model_post_init(ctx any) error {
	if err := m.LAILA_IDENTIFIABLE_OBJECT_.Model_post_init(ctx); err != nil {
		return err
	}
	if err := CLI_CAPABLE_model_post_init(m.Self()); err != nil {
		return err
	}
	if py.IsNone(m.Pool_router) {
		router, err := New_DefaultPoolRouter()
		if err != nil {
			return err
		}
		m.Pool_router = router
	}
	if py.IsNone(m.Alpha_pool) {
		router := m.router_()
		if gid, ok := router.Pools_nicknames.Get(DEFAULT_POOL_NICKNAME_); ok {
			m.Alpha_pool = gid
		} else {
			alpha, err := New_DefaultPool()
			if err != nil {
				return err
			}
			if err := router.Extend(alpha, py.Kw{"affinity": 1, "pool_nickname": DEFAULT_POOL_NICKNAME_}); err != nil {
				return err
			}
			m.Alpha_pool = global_id_of_(alpha)
		}
	}
	return nil
}

// router_ is ``self.pool_router`` as the router base.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) router_() *LAILA_IDENTIFIABLE_POOL_ROUTER_ {
	if r, ok := m.Pool_router.(interface {
		Pool_router_base_() *LAILA_IDENTIFIABLE_POOL_ROUTER_
	}); ok {
		return r.Pool_router_base_()
	}
	return nil
}

// Extend forwards pool registration to PoolRouter.Extend
// (``extend(pool, *, affinity=None, pool_nickname=None)``).
//
// Convenience pass-through so user code can write
// ``policy.central.memory.extend(my_pool)`` without reaching into the
// router.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Extend(pool any, kw ...py.Kw) error {
	opts := py.MergeKw(kw...)
	_, err := callm(m.Pool_router, "extend", pool,
		py.Kw{"affinity": opts.Get("affinity", nil), "pool_nickname": opts.Get("pool_nickname", nil)})
	return err
}

// Resolve_pool_ref_ resolves a pool object, gid string, or nickname to a
// live pool instance.
//
// Lookup order:
//
//  1. *pool_ref* is already a pool -- returned as-is.
//  2. *pool_ref* is a string that matches a registered pool gid --
//     returned from PoolRouter.pools.
//  3. *pool_ref* is a string that matches a registered nickname --
//     resolved via PoolRouter.pools_nicknames and then looked up in
//     PoolRouter.pools.
//
// KeyError if the string matches neither a gid nor a nickname; TypeError
// if *pool_ref* is neither a pool nor a string.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Resolve_pool_ref_(pool_ref any) (any, error) {
	if As_pool_(pool_ref) != nil {
		return pool_ref, nil
	}
	if s, ok := pool_ref.(string); ok {
		router := m.router_()
		if v, ok := router.Pools.Get(s); ok {
			return v, nil
		}
		if gid, ok := router.Pools_nicknames.Get(s); ok {
			if v, ok := router.Pools.Get(gid); ok {
				return v, nil
			}
			return nil, py.KeyError.New(py.Repr(gid))
		}
		return nil, py.KeyError.Newf("Pool '%s' was not found.", s)
	}
	return nil, py.TypeError.New("pool_ref must be a pool object, pool id, or pool nickname.")
}

// Route_pool_ resolves the target pool for a memory op
// (``_route_pool(entries, *, pool=None, pool_id=None, pool_nickname=None,
// affinity=None)``).
//
// Unifies the three ways a caller can name a pool:
//
//   - ``pool`` -- a live pool *object*, a gid string, or a nickname,
//     resolved directly through Resolve_pool_ref_. This is the path that
//     supports *standalone* pools (e.g. an S3 / Redis pool instance the
//     caller configured itself and handed in).
//   - ``pool_id`` / ``pool_nickname`` -- the classic router inputs,
//     resolved through the PoolRouter (``pool_id`` > ``pool_nickname`` >
//     default alpha).
//
// ``pool`` wins when supplied; otherwise the router decides.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Route_pool_(entries any, kw ...py.Kw) (*LAILA_IDENTIFIABLE_POOL_, error) {
	opts := py.MergeKw(kw...)
	var resolved any
	var err error
	if pool := opts.Get("pool", nil); !py.IsNone(pool) {
		resolved, err = m.Resolve_pool_ref_(pool)
	} else {
		resolved, err = callm(m.Pool_router, "route", entries, py.Kw{
			"pool_id":       opts.Get("pool_id", nil),
			"pool_nickname": opts.Get("pool_nickname", nil),
			"affinity":      opts.Get("affinity", nil),
		})
	}
	if err != nil {
		return nil, err
	}
	p := As_pool_(resolved)
	if p == nil {
		return nil, py.TypeError.Newf("routed value is not a pool: %s", py.Pytypename(resolved))
	}
	return p, nil
}

// Borrow is the context manager that lends entries to the caller for the
// scope of a ``with`` block (``borrow(keys=None, global_borrow=False)``).
// Not yet implemented.
//
// Planned semantics: the listed *keys* are pinned in the alpha pool for
// the duration of the block and released on exit. With
// ``global_borrow=True`` the borrow is announced to peer policies so they
// avoid double-fetching the same artefacts during the same window.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Borrow(args ...any) (any, func(), error) {
	// TODO: need to make sure cross-borrowing does not lead to stall
	return nil, nil, py.NotImplementedError.New()
}

// Duplicate_pool_ copies every entry from *pool_src* into *pool_dest*
// asynchronously (``_duplicate_pool(pool_src, pool_dest, *,
// inflight_max_entries=4)``).
//
// Implements the ``cache <= origin`` operator on pools and is also the
// building block behind any "promote this snapshot to that storage"
// workflow. Each entry is read from the source pool, then written to the
// destination pool; both legs proceed concurrently up to
// *inflight_max_entries* at a time.
//
// The return value is a GroupFuture whose ``future_ids`` reference one
// ConcurrentPackageFuture per entry. Each per-entry future flips to
// ``FINISHED`` when *that* entry has landed in *pool_dest*. Use
// ``with laila.guarantee:`` around the call to block until the entire
// duplication is done.
//
// ValueError if *inflight_max_entries* is below 1; KeyError if either
// pool is not registered with central memory; RuntimeError (inside the
// children) if the source pool would have returned None from a
// ``remember``.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Duplicate_pool_(pool_src any, pool_dest any, kw ...py.Kw) (*GroupFuture, error) {
	opts := py.MergeKw(kw...)
	inflight_max_entries, err := py.Int(opts.Get("inflight_max_entries", 4))
	if err != nil {
		return nil, err
	}
	cmd, err := m.Command_()
	if err != nil {
		return nil, err
	}
	owner_gid, err := m.Owner_gid_()
	if err != nil {
		return nil, err
	}
	future_bank, err := Future_bank_()
	if err != nil {
		return nil, err
	}

	if inflight_max_entries < 1 {
		return nil, py.ValueError.New("duplicate_pool requires inflight_max_entries >= 1.")
	}

	src_any, err := m.Resolve_pool_ref_(pool_src)
	if err != nil {
		return nil, err
	}
	dest_any, err := m.Resolve_pool_ref_(pool_dest)
	if err != nil {
		return nil, err
	}
	src_pool, dest_pool := As_pool_(src_any), As_pool_(dest_any)
	// Both legs are routed through central memory by pool gid, so an
	// unregistered pool would otherwise fail per entry with an opaque
	// KeyError on the gid. Check up front and say what to do instead.
	for _, rp := range []struct {
		role string
		pool *LAILA_IDENTIFIABLE_POOL_
	}{{"source", src_pool}, {"destination", dest_pool}} {
		if !m.router_().Pools.Contains(rp.pool.Global_id()) {
			return nil, py.KeyError.Newf(
				"%s pool %s is not registered with central memory; "+
					"call laila.memory.extend(pool) first", rp.role, rp.pool.Global_id())
		}
	}
	raw_keys, err := src_pool.Self().(interface{ Keys(...py.Kw) (any, error) }).Keys()
	if err != nil {
		return nil, err
	}
	entry_ids := keys_to_strings_(raw_keys)

	children := []*ConcurrentPackageFuture{}
	future_ids := []any{}
	for _, entry_id := range entry_ids {
		child, err := New_ConcurrentPackageFuture(py.Kw{
			"taskforce_id": cmd.Internal_taskforce,
			"policy_id":    owner_gid,
			"purpose":      "duplicate_pool:" + entry_id,
		})
		if err != nil {
			return nil, err
		}
		children = append(children, child)
		future_ids = append(future_ids, child.Global_id())
	}

	group_future, err := New_GroupFuture(py.Kw{
		"taskforce_id": cmd.Internal_taskforce,
		"policy_id":    owner_gid,
		"future_ids":   py.NewList(future_ids...),
	})
	if err != nil {
		return nil, err
	}
	for _, child := range children {
		child.Future_group_id = group_future.Global_id()
	}

	semaphore := py.New_AsyncSemaphore(inflight_max_entries)
	self := m.Self()

	duplicate_one := func(entry_id string, child *ConcurrentPackageFuture) func() (any, error) {
		return func() (any, error) {
			err := attempt(func() error {
				if err := semaphore.Acquire(); err != nil {
					return err
				}
				defer semaphore.Release()
				child.Set_status(FutureStatus_RUNNING)

				remember_ref, err := callm(self, "remember",
					py.Kw{"entry_ids": entry_id, "pool_id": src_pool.Global_id()})
				if err != nil {
					return err
				}
				if py.IsNone(remember_ref) {
					return py.RuntimeError.New("duplicate_pool requires a non-default source pool.")
				}
				remember_fut, ok := future_bank.Get(global_id_of_(remember_ref))
				if !ok {
					return py.KeyError.New(global_id_of_(remember_ref))
				}
				entry, err := await_future_(remember_fut)
				if err != nil {
					return err
				}

				memorize_ref, err := callm(self, "memorize",
					py.Kw{"entries": entry, "pool_id": dest_pool.Global_id()})
				if err != nil {
					return err
				}
				if !py.IsNone(memorize_ref) {
					if memorize_fut, ok := future_bank.Get(global_id_of_(memorize_ref)); ok {
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

	duplicate_all := func() (any, error) {
		fns := []func() (any, error){}
		for i, entry_id := range entry_ids {
			fns = append(fns, duplicate_one(entry_id, children[i]))
		}
		return py.Gather(fns, py.Kw{"return_exceptions": true})
	}

	run_duplication_event_loop := func() {
		if _, err := py.Run(duplicate_all); err != nil {
			fail_pending_children_(children, err)
		}
	}

	if err := py.New_Thread(py.Kw{
		"target": run_duplication_event_loop,
		"name":   "DuplicatePool-" + group_future.Global_id(),
		"daemon": true,
	}).Start(); err != nil {
		return nil, err
	}

	return group_future, nil
}

// MEMORIZE_SIG_ is the Python signature of ``memorize`` (for Ensure_list).
var MEMORIZE_SIG_ = py.Sig("memorize", py.P("entries"), py.KWD("pool", nil), py.KWD("pool_id", nil),
	py.KWD("pool_nickname", nil), py.KWD("affinity", nil))

// Memorize persists *entries* to the routed pool
// (``memorize(entries, *, pool=None, pool_id=None, pool_nickname=None,
// affinity=None)``; ``@ensure_list("entries")``).
//
// Routes through the PoolRouter (``pool_id`` > ``pool_nickname`` >
// default), records a ``memorize`` log line, and dispatches to either the
// batch-accelerated path (if the target pool supports it) or the
// per-entry parallel path.
//
//   - entries: one or more entries to write. Ensure_list wraps a single
//     entry in a 1-list before this body runs.
//   - pool: direct pool selector (highest priority). Accepts a live,
//     *standalone* pool instance the caller configured, a gid, or a
//     nickname. See Route_pool_.
//   - pool_id: explicit pool gid, classic router input.
//   - pool_nickname: friendly name resolved through the router.
//   - affinity: reserved for future affinity routing.
//
// Returns a future (single entry), a group future (many entries), or nil
// if the pool path was synchronous and no future handle was needed.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Memorize(args ...any) (any, error) {
	return Ensure_list("entries")(MEMORIZE_SIG_, func(args ...any) (any, error) {
		pos, kw := py.SplitKw(args)
		entries := ensured_list_(pos[0])
		pool, err := m.Route_pool_(entries, py.Kw{
			"pool":          kw.Get("pool", nil),
			"pool_id":       kw.Get("pool_id", nil),
			"pool_nickname": kw.Get("pool_nickname", nil),
			"affinity":      kw.Get("affinity", nil),
		})
		if err != nil {
			return nil, err
		}

		_ = attempt(func() error {
			logger, err := Get_logger()
			if err != nil {
				return err
			}
			policy, err := m.Policy_()
			if err != nil {
				return err
			}
			logger.Record_memorize(py.Kw{"entries": entries, "pool": pool.Self(), "policy": policy})
			return nil
		})

		return m.hooks().Record_(entries, pool)
	})(args...)
}

// Record_ picks the right write path based on the pool's
// ``batch_accelerated`` flag.
//
// Pools that can multiplex many writes per round-trip
// (``batch_accelerated=True``) are routed to Batch_accelerated_record_;
// others use the per-entry parallel path.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Record_(entries *py.List, pool *LAILA_IDENTIFIABLE_POOL_) (any, error) {
	if pool.Batch_accelerated {
		return m.hooks().Batch_accelerated_record_(entries, pool)
	}
	return m.hooks().Parallel_individual_record_(entries, pool)
}

// Parallel_individual_record_ memorizes each entry as a single per-entry
// async coroutine.
//
// Each entry's coroutine:
//
//  1. wraps the entry in a Record and runs ``serialize`` inline (pure CPU,
//     no await needed),
//  2. awaits the pool's ``_write_async`` for the actual storage
//     round-trip.
//
// Submits one coroutine per entry to the internal taskforce. Returns the
// single future for one entry, or a GroupFuture for many.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Parallel_individual_record_(entries *py.List, pool *LAILA_IDENTIFIABLE_POOL_) (any, error) {
	cmd, err := m.Command_()
	if err != nil {
		return nil, err
	}
	internal_id := cmd.Internal_taskforce
	policy_gid, err := m.Owner_gid_()
	if err != nil {
		return nil, err
	}
	transformations := pool.Transformations

	var memorize_one py.CoroutineFunc = func(args ...any) (any, error) {
		e_any := args[0]
		e := As_entry_(e_any)
		if e == nil {
			return nil, py.AttributeError.Newf("'%s' object has no attribute 'global_id'", py.Pytypename(e_any))
		}
		// A variable entry whose payload was re-assigned since its last
		// memorize becomes a new evolution (in place); an untouched one is
		// re-written under the same key.
		e.Bump_evolution_if_locally_modified()
		record, err := New_Record(py.Kw{"entry": e_any, "recorder": policy_gid})
		if err != nil {
			return nil, err
		}
		var blob any
		blob, err = record.Serialize(transformations)
		if err != nil {
			return nil, err
		}
		if py.Hasattr(blob, "data") {
			if d, err := py.Getattr(blob, "data"); err == nil {
				blob = d
			}
		}
		if err := pool.Write_async(e.Global_id(), blob); err != nil {
			return nil, err
		}
		e.Mark_memorized()
		gid := pool.Global_id()
		e.Set_source_pool_gid(&gid)
		return e.Global_id(), nil
	}

	// ``partial`` of a coroutine function is recognised as one by
	// Ensure_coroutine_function and skips the sync-offload hop.
	factories := py.NewList()
	for _, entry := range entries.Items() {
		factories.Append(memorize_one.Partial(entry))
	}
	return cmd.Submit(factories, py.Kw{"taskforce_id": internal_id})
}

// Batch_accelerated_record_ batch-records entries (not yet implemented).
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Batch_accelerated_record_(entries *py.List, pool *LAILA_IDENTIFIABLE_POOL_) (any, error) {
	return nil, py.NotImplementedError.New()
}

// REMEMBER_SIG_ is the Python signature of ``remember`` (for Ensure_list).
var REMEMBER_SIG_ = py.Sig("remember", py.P("entry_ids"), py.KWD("pool", nil), py.KWD("pool_id", nil),
	py.KWD("pool_nickname", nil), py.KWD("affinity", nil), py.KWD("persist", true), py.KWD("resolve", true))

// Remember fetches entries from the routed pool, optionally caching them
// into the alpha pool on the way back
// (``remember(entry_ids, *, pool=None, pool_id=None, pool_nickname=None,
// affinity=None, persist=True, resolve=True)``; ``@ensure_list("entry_ids")``).
//
// When the routed source pool is *also* the alpha pool, or when
// ``persist=False`` is requested, the fetched entries are simply returned
// through the regular Fetch_ path. Otherwise, Remember_with_persist_ is
// used: each entry is fetched from the source pool *and* written to the
// alpha pool, with the per-entry future only flipping to ``FINISHED``
// once both legs are done.
//
// That cache-back semantics is what lets ``with laila.guarantee:`` block
// until the alpha pool has the entries -- subsequent reads from the alpha
// pool are then guaranteed to be hot.
//
//   - entry_ids: identifier(s) to fetch. Lists of live Entry instances
//     are accepted but the gid is what's actually used.
//   - pool_id: explicit source pool gid.
//   - pool_nickname: friendly name for the source pool.
//   - affinity: reserved for future affinity routing.
//   - persist (default True): whether to cache fetched entries into the
//     alpha pool.
//   - resolve (default True): run *recursive blueprint resolution* on
//     every ComplexManifest fetched: its manifest-scoped leaves are read
//     from the same pool and cast into typed members (see
//     ComplexManifest.Resolve). False returns the bare blueprint.
//
// Returns a future-like handle resolving to the entry or list of entries.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Remember(args ...any) (any, error) {
	return Ensure_list("entry_ids")(REMEMBER_SIG_, func(args ...any) (any, error) {
		pos, kw := py.SplitKw(args)
		entry_ids := ensured_list_(pos[0])
		pool, err := m.Route_pool_(entry_ids, py.Kw{
			"pool":          kw.Get("pool", nil),
			"pool_id":       kw.Get("pool_id", nil),
			"pool_nickname": kw.Get("pool_nickname", nil),
			"affinity":      kw.Get("affinity", nil),
		})
		if err != nil {
			return nil, err
		}

		_ = attempt(func() error {
			logger, err := Get_logger()
			if err != nil {
				return err
			}
			policy, err := m.Policy_()
			if err != nil {
				return err
			}
			logger.Record_remember(py.Kw{"entry_ids": entry_ids, "pool": pool.Self(), "policy": policy})
			return nil
		})

		persist := py.Truthy(kw.Get("persist", true))
		resolve := py.Truthy(kw.Get("resolve", true))
		if !persist || pool.Global_id() == py.Str(m.Alpha_pool) {
			return m.hooks().Fetch_(entry_ids_strings_(entry_ids), py.Kw{"pool": pool, "resolve": resolve})
		}
		return m.Remember_with_persist_(entry_ids_strings_(entry_ids), py.Kw{"pool": pool, "resolve": resolve})
	})(args...)
}

// entry_ids_strings_ is ``[str(x) for x in ids]`` -- entries contribute
// their ``global_id``.
func entry_ids_strings_(ids *py.List) []string {
	out := make([]string, 0, ids.Len())
	for _, x := range ids.Items() {
		if e := As_entry_(x); e != nil {
			out = append(out, e.Global_id())
			continue
		}
		out = append(out, py.Str(x))
	}
	return out
}

// Remember_with_persist_ fetches entries from *pool* and then memorizes
// them into the alpha pool.
//
// Returns a single future (for one id) or a GroupFuture (for many) whose
// children only reach ``FINISHED`` once both the fetch and the alpha-pool
// write have completed. Modeled after Duplicate_pool_.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Remember_with_persist_(entry_ids []string, kw ...py.Kw) (any, error) {
	opts := py.MergeKw(kw...)
	pool := As_pool_(opts.Get("pool", nil))
	resolve := py.Truthy(opts.Get("resolve", true))
	cmd, err := m.Command_()
	if err != nil {
		return nil, err
	}
	owner_gid, err := m.Owner_gid_()
	if err != nil {
		return nil, err
	}

	children := []*ConcurrentPackageFuture{}
	future_ids := []any{}
	for _, entry_id := range entry_ids {
		child, err := New_ConcurrentPackageFuture(py.Kw{
			"taskforce_id": cmd.Internal_taskforce,
			"policy_id":    owner_gid,
			"purpose":      "remember_with_persist:" + entry_id,
		})
		if err != nil {
			return nil, err
		}
		children = append(children, child)
		future_ids = append(future_ids, child.Global_id())
	}

	var group_future *GroupFuture
	if len(children) > 1 {
		group_future, err = New_GroupFuture(py.Kw{
			"taskforce_id": cmd.Internal_taskforce,
			"policy_id":    owner_gid,
			"future_ids":   py.NewList(future_ids...),
		})
		if err != nil {
			return nil, err
		}
		for _, child := range children {
			child.Future_group_id = group_future.Global_id()
		}
	}

	alpha_pool_id := m.Alpha_pool
	self := m.Self()

	run := func() (any, error) {
		err := attempt(func() error {
			for _, child := range children {
				child.Set_status(FutureStatus_RUNNING)
			}

			fetch_ref, err := m.hooks().Fetch_(entry_ids, py.Kw{"pool": pool, "resolve": resolve})
			if err != nil {
				return err
			}
			if py.IsNone(fetch_ref) {
				return py.RuntimeError.New(
					"remember with persist requires a non-default source pool " +
						"that returns a future.")
			}
			// The fetch / memorize futures below are consumed right here and
			// never handed out, so this coroutine owns their release.
			fetched, err := func() (any, error) {
				defer release_(fetch_ref)
				return await_future_(fetch_ref)
			}()
			if err != nil {
				return err
			}

			entries := []any{}
			if lst, ok := fetched.(*py.List); ok {
				entries = lst.Items()
			} else if lst, ok := fetched.([]any); ok {
				entries = lst
			} else {
				entries = []any{fetched}
			}

			// Nested manifests a complex manifest resolved to are cached
			// back too, so the next read from alpha resolves.
			ready_entries := py.NewList()
			for _, e := range m.With_resolved_members_(entries) {
				if ent := As_entry_(e); ent != nil && ent.State() == EntryState_READY {
					ready_entries.Append(e)
				}
			}
			if ready_entries.Len() > 0 {
				memorize_ref, err := callm(self, "memorize",
					py.Kw{"entries": ready_entries, "pool_id": alpha_pool_id})
				if err != nil {
					return err
				}
				if !py.IsNone(memorize_ref) {
					if _, err := func() (any, error) {
						defer release_(memorize_ref)
						return await_future_(memorize_ref)
					}(); err != nil {
						return err
					}
				}
			}

			for i, child := range children {
				if i >= len(entries) {
					break
				}
				child.Set_exception(nil)
				child.Set_result(entries[i])
				child.Set_status(FutureStatus_FINISHED)
			}
			return nil
		})
		if err != nil {
			fail_pending_children_(children, err)
		}
		return nil, nil
	}

	// Carry only the resolve chain (not the caller's slot context) onto
	// the helper thread so cycle detection keeps working across it.
	resolve_chain := RESOLVE_CHAIN_.Value()

	run_event_loop := func() {
		RESOLVE_CHAIN_.Set(resolve_chain)
		if _, err := py.Run(run); err != nil {
			fail_pending_children_(children, err)
		}
	}

	thread_name := "RememberPersist-" + children[0].Global_id()
	if group_future != nil {
		thread_name = "RememberPersist-" + group_future.Global_id()
	}
	if err := py.New_Thread(py.Kw{
		"target": run_event_loop,
		"name":   thread_name,
		"daemon": true,
	}).Start(); err != nil {
		return nil, err
	}

	if group_future != nil {
		return group_future, nil
	}
	return children[0], nil
}

// release_ is ``future.release()`` on any future-like value.
func release_(future any) {
	if r, ok := future.(interface{ Release() }); ok {
		r.Release()
		return
	}
	_, _ = callm(future, "release")
}

// ------------------------------------------------------------------
// Attribute-based key resolution (``remember("ENTRY:x@...")``)
// ------------------------------------------------------------------

// SEARCH_ATTRIBUTES_ are the attributes ``remember`` knows how to search
// on. Anything else in the ``@`` suffix of an entry reference is rejected
// up front.
var SEARCH_ATTRIBUTES_ = map[string]struct{}{
	EVOLUTION_ATTRIBUTE:          {},
	CREATION_TIMESTAMP_ATTRIBUTE: {},
}

// Key_evolution_ is the evolution encoded in a storage key; ``-1`` for a
// constant (no ``@``).
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Key_evolution_(key string) int {
	return memory_key_evolution_(key)
}

func memory_key_evolution_(key string) int {
	_, tail, found := strings.Cut(key, "@")
	if !found || tail == "" {
		return -1
	}
	attrs, err := Parse_global_id_attributes(tail)
	if err != nil {
		return -1
	}
	raw := attrs.GetDefault(EVOLUTION_ATTRIBUTE, nil)
	if py.IsNone(raw) || !isdigit(py.Str(raw)) {
		return -1
	}
	n, _ := py.Int(py.Str(raw))
	return n
}

// Record_creation_timestamp_ is kept as a static alias for callers that
// used to find it here.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Record_creation_timestamp_(raw any) any {
	return Record_creation_timestamp_(raw)
}

// Pool_chain_ is *pool* followed by its ``_proxy_to`` upstream tiers,
// front to back.
func Pool_chain_(pool *LAILA_IDENTIFIABLE_POOL_) []*LAILA_IDENTIFIABLE_POOL_ {
	chain := []*LAILA_IDENTIFIABLE_POOL_{}
	seen := map[uintptr]struct{}{}
	cur := pool
	for cur != nil {
		if _, ok := seen[py.Id(cur.Self())]; ok {
			break
		}
		chain = append(chain, cur)
		seen[py.Id(cur.Self())] = struct{}{}
		cur = As_pool_(cur.Proxy_to_)
	}
	return chain
}

// Candidate_ is one ``(tier, key, from_index)`` candidate of a lineage
// query.
type Candidate_ struct {
	Tier    *LAILA_IDENTIFIABLE_POOL_
	Key     string
	Indexed bool
}

// Resolve_entry_key_async_ turns an entry *reference* into the storage key
// to read.
//
// The reference is a full global id, optionally carrying search
// attributes after ``@``:
//
//   - ``LAILA:ENTRY:<uuid>@evolution=N`` (``N >= 0``) -- exact key, no
//     lookup.
//   - ``LAILA:ENTRY:<uuid>`` -- the exact key when it exists locally (a
//     constant); otherwise the stored evolution with the highest counter
//     across *pool* and its proxy chain.
//   - ``LAILA:ENTRY:<uuid>@evolution=-k`` -- the k-th evolution from the
//     end (``-1`` = latest); a constant key counts as the lowest.
//   - ``LAILA:ENTRY:<uuid>@creation_timestamp=<iso>`` (optionally with
//     ``evolution``) -- the stored evolution whose entry
//     creation_timestamp equals the given stamp exactly.
//
// Resolution over the proxy chain:
//
//  1. every tier contributes candidates -- its index shard (Search_keys_,
//     one read) or, without an index answer, a prefix scan
//     (Candidate_keys_);
//  2. the union is ranked once (so an origin holding a newer evolution
//     than its cache wins) and only the chosen key is **validated** with
//     ``_exists_async``; a stale indexed key is removed from its shard
//     (repairing the persisted index) and the next best is tried, and
//     when the indexes turn out to be entirely stale the chain is
//     re-scanned from its keys;
//  3. for timestamp queries the candidate records are read on the tier
//     that holds them (no write-back); the matching raw record is
//     returned so the caller does not read it twice.
//
// Returns ``(storage_key, raw_record_or_nil)``. ValueError on an
// unsupported search attribute; KeyError when no stored evolution
// satisfies the query.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Resolve_entry_key_async_(pool *LAILA_IDENTIFIABLE_POOL_, eid_in any) (string, any, error) {
	// Shorthands ("ENTRY:nick@evolution=3") are normally expanded by
	// ``laila.remember``; expand here too so direct callers of central
	// memory get the same behaviour.
	eid, err := Resolve_global_id(Entry_class, py.Str(eid_in))
	if err != nil {
		return "", nil, err
	}
	base, attrs, err := Split_global_id_attributes(eid)
	if err != nil {
		return "", nil, err
	}
	unknown := []string{}
	for _, k := range attrs.Keys() {
		if _, ok := SEARCH_ATTRIBUTES_[py.Str(k)]; !ok {
			unknown = append(unknown, py.Str(k))
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		supported := []string{}
		for k := range SEARCH_ATTRIBUTES_ {
			supported = append(supported, k)
		}
		sort.Strings(supported)
		return "", nil, py.ValueError.Newf(
			"Unsupported search attribute(s) %s in %s; supported: %s",
			py.Repr(stringsToAny(unknown)), py.Repr(eid), py.Repr(stringsToAny(supported)))
	}
	raw_evolution := attrs.GetDefault(EVOLUTION_ATTRIBUTE, nil)
	var evolution *int
	if !py.IsNone(raw_evolution) {
		n, err := py.Int(raw_evolution)
		if err != nil {
			return "", nil, err
		}
		evolution = &n
	}
	creation_timestamp := attrs.GetDefault(CREATION_TIMESTAMP_ATTRIBUTE, nil)
	chain := Pool_chain_(pool)

	// Fast paths: a non-negative explicit evolution is an exact key; a
	// bare id that exists as-is (a constant) needs no lookup.
	if py.IsNone(creation_timestamp) {
		if evolution != nil && *evolution >= 0 {
			return eid, nil, nil
		}
		if evolution == nil {
			exists, err := pool.hooks().Exists_async_(base)
			if err != nil {
				return "", nil, err
			}
			if exists {
				return base, nil, nil
			}
		}
	}

	// Lineage queries are answered over the *union* of every tier's
	// candidates (a cache may hold an older evolution than its origin),
	// ranking once and validating only the chosen key. A chosen key that
	// is gone (external delete, lost race) is un-indexed on its tier --
	// repairing the persisted shard -- and the next best is tried; when
	// an index turns out to be entirely stale the tier is re-scanned from
	// its keys.
	candidates, err := m.Gather_candidates_(chain, base, attrs, evolution, true)
	if err != nil {
		return "", nil, err
	}

	if py.IsNone(creation_timestamp) {
		rescanned := false
		for {
			picked := m.Pick_candidate_(candidates, base, evolution)
			if picked == nil {
				// Nothing (valid) left from the indexes: scan the keys once.
				if !rescanned {
					rescanned = true
					candidates, err = m.Gather_candidates_(chain, base, attrs, evolution, false)
					if err != nil {
						return "", nil, err
					}
					if len(candidates) > 0 {
						continue
					}
				}
				return "", nil, py.KeyError.Newf("Entry %s not found in pool %s", eid, pool.Global_id())
			}
			exists, err := picked.Tier.hooks().Exists_async_(picked.Key)
			if err != nil {
				return "", nil, err
			}
			if exists {
				return picked.Key, nil, nil
			}
			candidates = remove_candidate_(candidates, picked)
			if picked.Indexed {
				tier, key := picked.Tier, picked.Key
				if _, err := py.To_thread(func() (any, error) { tier.Index().Remove(key); return nil, nil }); err != nil {
					return "", nil, err
				}
			}
		}
	}

	// Time-based search: inspect each candidate record's creation_timestamp.
	if evolution != nil && *evolution < 0 {
		// The stamp must agree with the k-th evolution from the end.
		ranked := ranked_evolutions_(candidates)
		idx := len(ranked) + *evolution
		var wanted *int
		if idx >= 0 {
			w := ranked[idx]
			wanted = &w
		}
		filtered := []*Candidate_{}
		for _, c := range candidates {
			if wanted != nil && memory_key_evolution_(c.Key) == *wanted {
				filtered = append(filtered, c)
			}
		}
		candidates = filtered
	}
	type match_ struct {
		evo int
		key string
		raw any
	}
	matches := []match_{}
	stamp := py.Str(creation_timestamp)
	for _, c := range candidates {
		raw, err := c.Tier.hooks().Read_async_(c.Key)
		if err != nil {
			return "", nil, err
		}
		if py.IsNone(raw) {
			continue
		}
		if ts := Record_creation_timestamp_(raw); !py.IsNone(ts) && py.Str(ts) == stamp {
			matches = append(matches, match_{memory_key_evolution_(c.Key), c.Key, raw})
		}
	}
	if len(matches) == 0 {
		return "", nil, py.KeyError.Newf(
			"No evolution of %s with creation_timestamp=%s in pool %s",
			base, py.Repr(stamp), pool.Global_id())
	}
	best := matches[0]
	for _, mt := range matches[1:] {
		if mt.evo > best.evo {
			best = mt
		}
	}
	return best.key, best.raw, nil
}

// remove_candidate_ is ``candidates.remove(picked)``.
func remove_candidate_(candidates []*Candidate_, picked *Candidate_) []*Candidate_ {
	out := candidates[:0:0]
	removed := false
	for _, c := range candidates {
		if c == picked && !removed {
			removed = true
			continue
		}
		out = append(out, c)
	}
	return out
}

// ranked_evolutions_ is ``sorted({key_evolution(c.key) for c in candidates})``.
func ranked_evolutions_(candidates []*Candidate_) []int {
	seen := map[int]struct{}{}
	ranked := []int{}
	for _, c := range candidates {
		e := memory_key_evolution_(c.Key)
		if _, ok := seen[e]; !ok {
			seen[e] = struct{}{}
			ranked = append(ranked, e)
		}
	}
	sort.Ints(ranked)
	return ranked
}

// Gather_candidates_ collects ``(tier, key, from_index)`` candidates for
// *base* across *chain*.
//
// Each tier contributes its indexed keys (Search_keys_; one shard read)
// or, when it has no index answer or *use_index* is false, the keys from a
// prefix scan (Candidate_keys_). A key found on several tiers is
// attributed to the nearest one.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Gather_candidates_(chain []*LAILA_IDENTIFIABLE_POOL_, base string, attrs *py.Dict, evolution *int, use_index bool) ([]*Candidate_, error) {
	seen := map[string]struct{}{}
	out := []*Candidate_{}
	prefix := base + "@"
	for _, tier := range chain {
		tier := tier
		var keys any
		if use_index {
			v, err := py.To_thread(func() (any, error) { return tier.hooks().Search_keys_(base, attrs) })
			if err != nil {
				return nil, err
			}
			keys = v
		}
		from_index := !py.IsNone(keys)
		if py.IsNone(keys) {
			v, err := py.To_thread(func() (any, error) { return tier.hooks().Candidate_keys_(base) })
			if err != nil {
				return nil, err
			}
			keys = v
		}
		for _, key := range keys_to_strings_(keys) {
			if _, dup := seen[key]; dup {
				continue
			}
			if key != base && !strings.HasPrefix(key, prefix) {
				continue
			}
			if evolution != nil && *evolution >= 0 && memory_key_evolution_(key) != *evolution {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, &Candidate_{Tier: tier, Key: key, Indexed: from_index})
		}
	}
	return out, nil
}

// Pick_candidate_ chooses the candidate a lineage query refers to, or nil.
//
//   - ``evolution is None``: the constant key when present, else the
//     highest evolution across all tiers;
//   - ``evolution < 0``: the k-th from the end over the sorted,
//     de-duplicated evolutions (constants rank lowest);
//   - ``evolution >= 0``: the (already filtered) exact match.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Pick_candidate_(candidates []*Candidate_, base string, evolution *int) *Candidate_ {
	if len(candidates) == 0 {
		return nil
	}
	if evolution == nil {
		for _, cand := range candidates {
			if cand.Key == base {
				return cand
			}
		}
		best := candidates[0]
		for _, c := range candidates[1:] {
			if memory_key_evolution_(c.Key) > memory_key_evolution_(best.Key) {
				best = c
			}
		}
		return best
	}
	if *evolution < 0 {
		ranked := ranked_evolutions_(candidates)
		idx := len(ranked) + *evolution
		if idx < 0 {
			return nil
		}
		wanted := ranked[idx]
		for _, cand := range candidates {
			if memory_key_evolution_(cand.Key) == wanted {
				return cand
			}
		}
		return nil
	}
	return candidates[0]
}

// Fetch_ dispatches fetching to batch or non-batch path based on pool
// capability (``_fetch(entry_ids, *, pool=None, borrow=False,
// resolve=True)``).
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Fetch_(entry_ids []string, kw ...py.Kw) (any, error) {
	opts := py.MergeKw(kw...)
	if py.Truthy(opts.Get("borrow", false)) {
		return nil, py.NotImplementedError.New()
	}
	pool := As_pool_(opts.Get("pool", nil))
	if pool.Batch_accelerated {
		return m.hooks().Batch_accelerated_fetch_(entry_ids, py.Kw{"pool": pool})
	}
	return m.hooks().Parallel_individual_fetch_(entry_ids, py.Kw{"pool": pool, "resolve": opts.Get("resolve", true)})
}

// Parallel_individual_fetch_ fetches and deserializes each entry via a
// single per-entry coroutine.
//
// Each entry's coroutine:
//
//  1. awaits the pool's ``_read_async`` for the storage round-trip,
//  2. awaits Record_build_async_ to deserialize and recursively hydrate
//     any nested entries (which themselves may await further fetches on
//     the same loop).
//
// Submits one coroutine per entry to the internal taskforce and returns
// the single future (one entry) or a GroupFuture (many).
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Parallel_individual_fetch_(entry_ids []string, kw ...py.Kw) (any, error) {
	opts := py.MergeKw(kw...)
	pool := As_pool_(opts.Get("pool", nil))
	resolve := py.Truthy(opts.Get("resolve", true))
	cmd, err := m.Command_()
	if err != nil {
		return nil, err
	}
	internal_id := cmd.Internal_taskforce
	if err := Check_resolve_cycle(entry_ids...); err != nil {
		return nil, err
	}

	var remember_one py.CoroutineFunc = func(args ...any) (any, error) {
		eid := py.Str(args[0])
		key, raw, err := m.Resolve_entry_key_async_(pool, eid)
		if err != nil {
			return nil, err
		}
		// Extend the resolve chain with the *concrete* key so a floating
		// reference (``@evolution=-1``) cannot hide a cycle through a
		// nested manifest / constitution.
		token := RESOLVE_CHAIN_.Set(append(append([]string(nil), RESOLVE_CHAIN_.Value()...), key))
		defer RESOLVE_CHAIN_.Reset(token)
		if py.IsNone(raw) {
			raw, err = pool.Read_through_async_(key)
			if err != nil {
				return nil, err
			}
		}
		if py.IsNone(raw) {
			return nil, py.KeyError.Newf("Entry %s not found in pool %s", eid, pool.Global_id())
		}
		record, err := Record_build_async_(raw)
		if err != nil {
			return nil, err
		}
		entry, err := Check_record_identity_(key, record.GetDefault("entry", nil))
		if err != nil {
			return nil, err
		}
		return m.Resolve_on_arrival_(Stamp_source_pool_(entry, pool), pool, resolve)
	}

	factories := py.NewList()
	for _, entry_id := range entry_ids {
		factories.Append(remember_one.Partial(entry_id))
	}
	return cmd.Submit(factories, py.Kw{"taskforce_id": internal_id})
}

// ------------------------------------------------------------------
// Recursive blueprint resolution hook
// ------------------------------------------------------------------

// Resolve_on_arrival_ resolves a freshly hydrated ComplexManifest against
// *pool* (``_resolve_on_arrival(entry, pool, resolve)``; a coroutine body
// -- call it from inside a running task).
//
// Only complex manifests (scope ``MANIFEST:COMPLEX``) are touched: their
// manifest-scoped leaves are fetched from the same pool and cast into
// typed members, recursively. Any other entry -- a plain manifest
// included -- is returned as is, with no scan.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Resolve_on_arrival_(entry any, pool *LAILA_IDENTIFIABLE_POOL_, resolve bool) (any, error) {
	if !resolve {
		return entry, nil
	}
	if cm := As_complex_manifest_(entry); cm != nil && py.Isinstance(entry, ComplexManifest_class) {
		var pool_arg any
		if pool != nil {
			pool_arg = pool.Self()
		}
		if _, err := py.Await(cm.Async_resolve(py.Kw{"pool": pool_arg, "persist": false})); err != nil {
			return nil, err
		}
	}
	return entry, nil
}

// With_resolved_members_ is *entries* plus the manifests their complex
// manifests resolved to (for cache-back) (``_with_resolved_members``).
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) With_resolved_members_(entries []any) []any {
	out := append([]any(nil), entries...)
	seen := map[string]struct{}{}
	for _, e := range out {
		seen[global_id_of_(e)] = struct{}{}
	}
	for _, entry := range entries {
		cm := As_complex_manifest_(entry)
		if cm == nil || !py.Isinstance(entry, ComplexManifest_class) {
			continue
		}
		for _, member := range cm.Manifest_members_(py.Kw{"recursive": true}) {
			gid := global_id_of_(member)
			if _, dup := seen[gid]; !dup {
				seen[gid] = struct{}{}
				out = append(out, member)
			}
		}
	}
	return out
}

// Batch_accelerated_fetch_ is the batch-accelerated fetch path (not yet
// implemented).
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Batch_accelerated_fetch_(keys []string, kw ...py.Kw) (any, error) {
	return nil, py.NotImplementedError.New()
}

// ------------------------------------------------------------------
// Record identity validation
// ------------------------------------------------------------------

// Stamp_source_pool_ remembers which pool *entry* came from (see
// ``Entry._source_pool_gid``).
func Stamp_source_pool_(entry any, pool *LAILA_IDENTIFIABLE_POOL_) any {
	if e := As_entry_(entry); e != nil {
		gid := pool.Global_id()
		e.Set_source_pool_gid(&gid)
	}
	return entry
}

// Check_record_identity_ requires the rebuilt *entry* to be the one *key*
// names.
//
// The stored record carries its own identity fields; a record edited,
// corrupted or copied under the wrong key must fail loudly instead of
// being returned silently. Compares the ``<scopes>:<uuid>`` head, and the
// evolution when the key carries one (constant keys have none).
func Check_record_identity_(key string, entry any) (any, error) {
	key_head, key_attrs, err := Split_global_id_attributes(key)
	if err != nil {
		return entry, nil
	}
	e := As_entry_(entry)
	if e == nil {
		return entry, nil
	}
	head := To_global_id(py.Kw{"uuid": e.Uuid_, "scopes": e.Scopes_, "evolution": nil})
	mismatch := head != key_head
	key_evolution := key_attrs.GetDefault(EVOLUTION_ATTRIBUTE, nil)
	if !mismatch && !py.IsNone(key_evolution) && !strings.HasPrefix(py.Str(key_evolution), "-") {
		mismatch = py.Str(intPtrValue(e.Evolution_)) != py.Str(key_evolution)
	}
	if mismatch {
		return nil, py.KeyError.Newf("record identity mismatch: key %s holds entry %s",
			py.Repr(key), py.Repr(e.Global_id()))
	}
	return entry, nil
}

// Direct_read_concurrency_ is the concurrency bound for
// Read_entries_async_.
//
// Mirrors what the per-entry submit path could have in flight on the
// internal taskforce (``num_workers * max_async_per_thread``), so
// switching to the direct resolver does not change the pressure put on a
// pool backend.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Direct_read_concurrency_() int {
	cmd, err := m.Command_()
	if err != nil {
		return 1
	}
	tf := cmd.Taskforces.GetDefault(cmd.Internal_taskforce, nil)
	workers := 1
	if v := getattr(tf, "num_workers"); !py.IsNone(v) {
		if n, err := py.Int(v); err == nil && n != 0 {
			workers = n
		}
	}
	per_thread := 64
	if v := getattr(tf, "max_async_per_thread"); !py.IsNone(v) {
		if n, err := py.Int(v); err == nil && n != 0 {
			per_thread = n
		}
	}
	if workers*per_thread < 1 {
		return 1
	}
	return workers * per_thread
}

// Read_entries_async_ fetches *entry_ids* by awaiting the pool directly --
// no per-entry futures (``_read_entries_async(entry_ids, *, pool=None,
// pool_id=None, pool_nickname=None, persist=True, max_concurrency=None,
// resolve=True)``).
//
// This is the direct-await counterpart of Remember for callers that
// consume the entries themselves and never need a per-entry handle
// (``Manifest.realized`` / ``async_realized``). Semantics match Remember:
//
//   - the pool is routed the same way (``pool`` > ``pool_id`` /
//     ``pool_nickname`` > router default);
//   - every id is checked against the current resolve chain and each read
//     extends the chain, so cyclic constitutions still raise
//     CyclicDependencyError;
//   - with ``persist=True`` and a non-alpha source pool, the READY entries
//     are cached into the alpha pool before returning (awaiting -- and
//     releasing -- the single memorize group).
//
// Reads run concurrently under an asyncio.Semaphore (default: the internal
// taskforce's slot capacity). Nested fetches triggered while building an
// entry (nested manifests, constitutions) still go through
// ``command.submit`` and park the surrounding slot exactly as before.
//
// Returns the entries in the order of *entry_ids*. KeyError if any id is
// missing from the routed pool.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Read_entries_async_(entry_ids_in any, kw ...py.Kw) (*py.List, error) {
	opts := py.MergeKw(kw...)
	entry_ids := []string{}
	for _, x := range py.Iter(entry_ids_in) {
		entry_ids = append(entry_ids, py.Str(x))
	}
	if len(entry_ids) == 0 {
		return py.NewList(), nil
	}

	routed, err := m.Route_pool_(entry_ids, py.Kw{
		"pool":          opts.Get("pool", nil),
		"pool_id":       opts.Get("pool_id", nil),
		"pool_nickname": opts.Get("pool_nickname", nil),
	})
	if err != nil {
		return nil, err
	}
	if routed.Batch_accelerated {
		// Same limitation as Fetch_(): batch-accelerated pools have no
		// implementation yet.
		return nil, py.NotImplementedError.New()
	}

	_ = attempt(func() error {
		logger, err := Get_logger()
		if err != nil {
			return err
		}
		policy, err := m.Policy_()
		if err != nil {
			return err
		}
		logger.Record_remember(py.Kw{"entry_ids": py.NewList(stringsToAny(entry_ids)...), "pool": routed.Self(), "policy": policy})
		return nil
	})

	if err := Check_resolve_cycle(entry_ids...); err != nil {
		return nil, err
	}
	limit := m.Direct_read_concurrency_()
	if mc := opts.Get("max_concurrency", nil); !py.IsNone(mc) {
		if limit, err = py.Int(mc); err != nil {
			return nil, err
		}
	}
	if limit < 1 {
		limit = 1
	}
	sem := py.New_AsyncSemaphore(limit)
	resolve := py.Truthy(opts.Get("resolve", true))

	one := func(eid string) func() (any, error) {
		return func() (any, error) {
			if err := sem.Acquire(); err != nil {
				return nil, err
			}
			defer sem.Release()
			key, raw, err := m.Resolve_entry_key_async_(routed, eid)
			if err != nil {
				return nil, err
			}
			token := RESOLVE_CHAIN_.Set(append(append([]string(nil), RESOLVE_CHAIN_.Value()...), key))
			defer RESOLVE_CHAIN_.Reset(token)
			if py.IsNone(raw) {
				if raw, err = routed.Read_through_async_(key); err != nil {
					return nil, err
				}
			}
			if py.IsNone(raw) {
				return nil, py.KeyError.Newf("Entry %s not found in pool %s", eid, routed.Global_id())
			}
			record, err := Record_build_async_(raw)
			if err != nil {
				return nil, err
			}
			entry, err := Check_record_identity_(key, record.GetDefault("entry", nil))
			if err != nil {
				return nil, err
			}
			return m.Resolve_on_arrival_(Stamp_source_pool_(entry, routed), routed, resolve)
		}
	}

	fns := []func() (any, error){}
	for _, eid := range entry_ids {
		fns = append(fns, one(eid))
	}
	results, err := py.Gather(fns)
	if err != nil {
		return nil, err
	}
	entries := py.NewList(results...)

	if py.Truthy(opts.Get("persist", true)) && routed.Global_id() != py.Str(m.Alpha_pool) {
		ready := py.NewList()
		for _, e := range m.With_resolved_members_(entries.Items()) {
			if ent := As_entry_(e); ent != nil && ent.State() == EntryState_READY {
				ready.Append(e)
			}
		}
		if ready.Len() > 0 {
			ref, err := callm(m.Self(), "memorize", ready, py.Kw{"pool_id": m.Alpha_pool})
			if err != nil {
				return nil, err
			}
			if !py.IsNone(ref) {
				if _, err := func() (any, error) {
					defer release_(ref)
					return await_future_(ref)
				}(); err != nil {
					return nil, err
				}
			}
		}
	}

	return entries, nil
}

// Read_entries_direct_ runs Read_entries_async_ as *one* task on the
// internal taskforce (``_read_entries_direct(entry_ids, *, pool=None,
// pool_id=None, pool_nickname=None, persist=True)``).
//
// Returns a single Future whose ``.data`` is the list of entries (one
// future for the whole batch instead of one per entry). The caller owns
// the future: ``wait()`` / ``await`` it, read ``.data`` and ``release()``
// it.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Read_entries_direct_(entry_ids_in any, kw ...py.Kw) (any, error) {
	opts := py.MergeKw(kw...)
	cmd, err := m.Command_()
	if err != nil {
		return nil, err
	}
	entry_ids := py.NewList(py.Iter(entry_ids_in)...)
	var coro_factory py.CoroutineFunc = func(args ...any) (any, error) {
		return m.Read_entries_async_(entry_ids, py.Kw{
			"pool":          opts.Get("pool", nil),
			"pool_id":       opts.Get("pool_id", nil),
			"pool_nickname": opts.Get("pool_nickname", nil),
			"persist":       opts.Get("persist", true),
			"resolve":       opts.Get("resolve", true),
		})
	}
	return cmd.Submit(py.NewList(coro_factory), py.Kw{"taskforce_id": cmd.Internal_taskforce})
}

// ------------------------------------------------------------------
// Cross-peer (over-the-wire) memorize / remember
// ------------------------------------------------------------------

// Remote_memorize_ reconstructs wire-serialized entries and memorizes them
// locally (``_remote_memorize(serialized_entries, *, pool=None,
// pool_id=None, pool_nickname=None)``).
//
// Invoked over a transport by a peer's ``laila.memorize`` ``dst_policy=``
// call. Each item in *serialized_entries* is the JSON-safe form produced
// by ``Entry.serialize(transformation_base64)`` on the sender; here it is
// rebuilt into a live Entry, written through the normal Memorize path, and
// the call blocks until the write completes so the caller gets a definite
// list of stored gids back.
//
// ``pool`` (a gid or nickname *string* resolvable on this policy) wins
// over the classic ``pool_id`` / ``pool_nickname``. A standalone pool
// *object* can never cross the wire, so only string selectors are
// accepted here.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Remote_memorize_(serialized_entries any, kw ...py.Kw) ([]string, error) {
	opts := py.MergeKw(kw...)
	entries := py.NewList()
	for _, s := range py.Iter(serialized_entries) {
		e, err := Build_by_scope(s, py.Kw{"asynchronous": false})
		if err != nil {
			return nil, err
		}
		entries.Append(e)
	}
	future, err := callm(m.Self(), "memorize", entries, py.Kw{
		"pool": opts.Get("pool", nil), "pool_id": opts.Get("pool_id", nil), "pool_nickname": opts.Get("pool_nickname", nil),
	})
	if err != nil {
		return nil, err
	}
	if !py.IsNone(future) && py.Hasattr(future, "wait") {
		if _, err := callm(future, "wait", py.Kw{"timeout": 60}); err != nil {
			return nil, err
		}
	}
	out := []string{}
	for _, e := range entries.Items() {
		out = append(out, global_id_of_(e))
	}
	return out, nil
}

// Remote_remember_ fetches entries locally and returns them
// wire-serialized (``_remote_remember(entry_ids, *, pool=None,
// pool_id=None, pool_nickname=None, persist=False)``).
//
// The inverse of Remote_memorize_: invoked over a transport by a peer's
// ``laila.remember`` ``dst_policy=`` call. Resolves the entries from the
// local pool, then returns each as the JSON-safe
// ``Entry.serialize(transformation_base64)`` form so the caller can
// rebuild real Entry objects on its side.
//
// ``persist`` is the caller's ``laila.remember(..., persist=)`` flag: with
// True the entries are also cached back into *this* (the peer's) alpha
// pool, exactly as a local ``remember`` would.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Remote_remember_(entry_ids any, kw ...py.Kw) (*py.List, error) {
	opts := py.MergeKw(kw...)
	future, err := callm(m.Self(), "remember", entry_ids, py.Kw{
		"pool":          opts.Get("pool", nil),
		"pool_id":       opts.Get("pool_id", nil),
		"pool_nickname": opts.Get("pool_nickname", nil),
		"persist":       py.Truthy(opts.Get("persist", false)),
	})
	if err != nil {
		return nil, err
	}
	result := future
	if py.Hasattr(future, "wait") {
		if result, err = callm(future, "wait", py.Kw{"timeout": 60}); err != nil {
			return nil, err
		}
	}
	var entries []any
	if lst, ok := result.(*py.List); ok {
		entries = lst.Items()
	} else if lst, ok := result.([]any); ok {
		entries = lst
	} else {
		entries = []any{result}
	}
	out := py.NewList()
	for _, e := range entries {
		ent := As_entry_(e)
		if ent == nil {
			return nil, py.AttributeError.Newf("'%s' object has no attribute 'serialize'", py.Pytypename(e))
		}
		s, err := ent.Serialize(Transformation_base64)
		if err != nil {
			return nil, err
		}
		out.Append(s)
	}
	return out, nil
}

// Remote_forget_ deletes *entry_ids* from a local pool on behalf of a peer
// (``_remote_forget(entry_ids, *, pool=None, pool_id=None,
// pool_nickname=None)``).
//
// Invoked over a transport by a peer's ``laila.forget`` ``policy=`` call.
// Blocks until the delete completes so the caller gets a definite
// acknowledgement (the list of gids it asked to delete).
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Remote_forget_(entry_ids any, kw ...py.Kw) ([]string, error) {
	opts := py.MergeKw(kw...)
	ids := gids_of_(entry_ids)
	future, err := callm(m.Self(), "forget", py.NewList(stringsToAny(ids)...), py.Kw{
		"pool": opts.Get("pool", nil), "pool_id": opts.Get("pool_id", nil), "pool_nickname": opts.Get("pool_nickname", nil),
	})
	if err != nil {
		return nil, err
	}
	if !py.IsNone(future) && py.Hasattr(future, "wait") {
		if _, err := callm(future, "wait", py.Kw{"timeout": 60}); err != nil {
			return nil, err
		}
	}
	return ids, nil
}

// gids_of_ is ``[x.global_id if hasattr(x, "global_id") else str(x) for x in xs]``.
func gids_of_(xs any) []string {
	out := []string{}
	for _, x := range py.Iter(xs) {
		if py.Hasattr(x, "global_id") {
			out = append(out, global_id_of_(x))
		} else {
			out = append(out, py.Str(x))
		}
	}
	return out
}

// ------------------------------------------------------------------
// 3-party relays (source-side orchestration)
//
// These run on the *source* policy B when an orchestrator A issues a
// ``src_policy=B, dst_policy=C`` transfer. A reaches B over the A<->B
// link and asks B to move data over B's own B<->C link -- A never
// brokers the B<->C connection.
// ------------------------------------------------------------------

// Relay_memorize_ pushes entries this policy holds to *dst_policy* (push
// relay; ``_relay_memorize(entry_ids, *, src_pool=None, dst_policy=None,
// dst_pool=None, comm=None)``).
//
// Runs on the source policy B: read *entry_ids* from B's own ``src_pool``
// and memorize them into ``dst_policy`` C's ``dst_pool`` over B's existing
// peer link to C. Blocks until the push completes and returns the stored
// gids.
//
// Raises a clear error if B is not peered to ``dst_policy``.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Relay_memorize_(entry_ids any, kw ...py.Kw) ([]string, error) {
	opts := py.MergeKw(kw...)
	ids := gids_of_(entry_ids)
	// Local leg on *this* memory (owner-bound: no reliance on which policy
	// happens to be active); remote leg through the shim so the peer link
	// / ``comm`` resolution stays in one place.
	fetched, err := callm(m.Self(), "remember", py.NewList(stringsToAny(ids)...),
		py.Kw{"pool": opts.Get("src_pool", nil), "persist": false})
	if err != nil {
		return nil, err
	}
	result := fetched
	if py.Hasattr(fetched, "wait") {
		if result, err = callm(fetched, "wait", py.Kw{"timeout": 60}); err != nil {
			return nil, err
		}
	}
	var entries *py.List
	if lst, ok := result.(*py.List); ok {
		entries = lst
	} else {
		entries = py.NewList(result)
	}

	pushed, err := Memorize(entries, py.Kw{
		"dst_policy": opts.Get("dst_policy", nil),
		"dst_pool":   opts.Get("dst_pool", nil),
		"comm":       opts.Get("comm", nil),
	})
	if err != nil {
		return nil, err
	}
	if !py.IsNone(pushed) && py.Hasattr(pushed, "wait") {
		if _, err := callm(pushed, "wait", py.Kw{"timeout": 60}); err != nil {
			return nil, err
		}
	}
	out := []string{}
	for _, e := range entries.Items() {
		out = append(out, global_id_of_(e))
	}
	return out, nil
}

// Relay_remember_ pulls entries from *dst_policy* into this policy (pull
// relay; ``_relay_remember(entry_ids, *, dst_policy=None, dst_pool=None,
// src_pool=None, comm=None, persist=True)``).
//
// Runs on the source policy B: fetch *entry_ids* from ``dst_policy`` C's
// ``dst_pool`` over B's own B<->C link, store them into B's ``src_pool``
// (or B's alpha pool), and return the stored gids.
//
// Raises a clear error if B is not peered to ``dst_policy``.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Relay_remember_(entry_ids any, kw ...py.Kw) ([]string, error) {
	opts := py.MergeKw(kw...)
	ids := gids_of_(entry_ids)
	fetched, err := Remember(py.NewList(stringsToAny(ids)...), py.Kw{
		"dst_policy": opts.Get("dst_policy", nil),
		"dst_pool":   opts.Get("dst_pool", nil),
		"comm":       opts.Get("comm", nil),
		"persist":    false,
	})
	if err != nil {
		return nil, err
	}
	result := fetched
	if py.Hasattr(fetched, "wait") {
		if result, err = callm(fetched, "wait", py.Kw{"timeout": 60}); err != nil {
			return nil, err
		}
	}
	var entries *py.List
	if lst, ok := result.(*py.List); ok {
		entries = lst
	} else {
		entries = py.NewList(result)
	}

	if py.Truthy(opts.Get("persist", true)) {
		stored, err := callm(m.Self(), "memorize", entries, py.Kw{"pool": opts.Get("src_pool", nil)})
		if err != nil {
			return nil, err
		}
		if !py.IsNone(stored) && py.Hasattr(stored, "wait") {
			if _, err := callm(stored, "wait", py.Kw{"timeout": 60}); err != nil {
				return nil, err
			}
		}
	}
	out := []string{}
	for _, e := range entries.Items() {
		out = append(out, global_id_of_(e))
	}
	return out, nil
}

// FORGET_SIG_ is the Python signature of ``forget`` (for Ensure_list).
var FORGET_SIG_ = py.Sig("forget", py.P("entry_ids"), py.KWD("pool", nil), py.KWD("pool_id", nil),
	py.KWD("pool_nickname", nil), py.KWD("affinity", nil))

// Forget deletes *entry_ids* from the routed pool
// (``forget(entry_ids, *, pool=None, pool_id=None, pool_nickname=None,
// affinity=None)``; ``@ensure_list("entry_ids")``).
//
// Parameters mirror Memorize / Remember. The deletion is *pool-local*: it
// only affects the routed pool, not any other pool that may hold a copy.
// To remove an entry from every registered pool, iterate over
// ``self.pool_router.pools.values()`` and call Forget per pool with an
// explicit ``pool_id``.
//
// Returns a future-like handle resolving when deletion finishes.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Forget(args ...any) (any, error) {
	return Ensure_list("entry_ids")(FORGET_SIG_, func(args ...any) (any, error) {
		pos, kw := py.SplitKw(args)
		entry_ids := ensured_list_(pos[0])
		pool, err := m.Route_pool_(entry_ids, py.Kw{
			"pool":          kw.Get("pool", nil),
			"pool_id":       kw.Get("pool_id", nil),
			"pool_nickname": kw.Get("pool_nickname", nil),
			"affinity":      kw.Get("affinity", nil),
		})
		if err != nil {
			return nil, err
		}

		_ = attempt(func() error {
			logger, err := Get_logger()
			if err != nil {
				return err
			}
			policy, err := m.Policy_()
			if err != nil {
				return err
			}
			logger.Record_forget(py.Kw{"entry_ids": entry_ids, "pool": pool.Self(), "policy": policy})
			return nil
		})

		return m.hooks().Delete_(entry_ids_strings_(entry_ids), pool)
	})(args...)
}

// Delete_ dispatches deletion to batch or non-batch path based on pool
// capability.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Delete_(entry_ids []string, pool *LAILA_IDENTIFIABLE_POOL_) (any, error) {
	if pool.Batch_accelerated {
		return m.hooks().Batch_accelerated_delete_(entry_ids, pool)
	}
	return m.hooks().Parallel_individual_delete_(entry_ids, pool)
}

// Batch_accelerated_delete_ batch-deletes entries (not yet implemented).
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Batch_accelerated_delete_(entry_ids []string, pool *LAILA_IDENTIFIABLE_POOL_) (any, error) {
	return nil, py.NotImplementedError.New()
}

// Parallel_individual_delete_ deletes each entry via a single per-entry
// async coroutine.
func (m *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_) Parallel_individual_delete_(entry_ids []string, pool *LAILA_IDENTIFIABLE_POOL_) (any, error) {
	cmd, err := m.Command_()
	if err != nil {
		return nil, err
	}
	internal_id := cmd.Internal_taskforce

	var delete_one py.CoroutineFunc = func(args ...any) (any, error) {
		key := py.Str(args[0])
		// A negative evolution ("@evolution=-1" = latest) must be resolved
		// to the concrete key; an evolution-less reference stays exact.
		_, attrs, err := Split_global_id_attributes(key)
		if err != nil {
			return nil, err
		}
		raw_evolution := attrs.GetDefault(EVOLUTION_ATTRIBUTE, nil)
		if !py.IsNone(raw_evolution) && strings.HasPrefix(py.Str(raw_evolution), "-") {
			if key, _, err = m.Resolve_entry_key_async_(pool, key); err != nil {
				return nil, err
			}
		}
		if err := pool.Delete_async(key); err != nil {
			return nil, err
		}
		return key, nil
	}

	factories := py.NewList()
	for _, entry_id := range entry_ids {
		factories.Append(delete_one.Partial(entry_id))
	}
	return cmd.Submit(factories, py.Kw{"taskforce_id": internal_id})
}

func init() {
	DefaultCentralMemory = LAILA_IDENTIFIABLE_CENTRAL_MEMORY_class
}

// ensured_list_ views the value produced by Ensure_list (a list, set or
// frozenset) as a *py.List; sets keep their iteration order.
func ensured_list_(v any) *py.List {
	if l, ok := v.(*py.List); ok {
		return l
	}
	return py.NewList(py.Iter(v)...)
}
