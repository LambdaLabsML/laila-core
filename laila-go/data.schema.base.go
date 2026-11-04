package laila

import (
	"strings"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// data/schema/base.py -- base schema for all LAILA storage-pool
// implementations.
//
// A *pool* is laila's name for a key-value store that persists serialized
// Entry blobs. Pools are the leaves of the storage hierarchy that the
// central memory subsystem routes ``memorize`` / ``remember`` / ``forget``
// calls to. Concrete subclasses plug into different backends -- in-memory,
// filesystem, S3, Redis, Postgres, SQLite, DuckDB, HDF5, GCS, Azure,
// BackBlaze, Cloudflare, HuggingFace -- by overriding the small set of
// "internal storage hooks" described below.
//
// # Public surface (proxy-aware)
//
//   - ``pool[key]`` (Getitem) -> stored blob, transparently falling back
//     through the proxy chain on miss and caching the result on the way
//     back.
//   - ``pool[key] = entry`` (Setitem) -> local-only write.
//   - ``del pool[key]`` (Delitem) -> local-only delete.
//   - ``pool[manifest]`` -> PoolWrapper view scoped by a manifest.
//   - ``pool.exists(key)`` / ``key in pool`` -> local-only existence check.
//   - ``pool.keys(as_generator=...)`` -> local-only key enumeration.
//   - ``pool.empty()`` -> local-only wipe.
//   - ``pool.sync()`` -> flush in-memory cache (raises if cacheless).
//   - ``cache <= origin`` (Le) -> bulk duplicate via central memory.
//
// # Subclass contract (override these)
//
// Synchronous: Read_, Write_, Delete_, Exists_, Keys_, Empty_. Default
// implementations operate on the in-memory ``resource`` dict.
//
// Asynchronous: Read_async_, Write_async_, Delete_async_, Exists_async_.
// Default implementations just call the sync hook on the calling event
// loop -- subclasses with a real async client should override these.
//
// Port note: Go has no virtual methods, so every hook is reached through
// the Pool_hooks interface on ``Self()``; a subclass overrides a hook by
// declaring the method on its own struct (which shadows the embedded
// default).
//
// # Proxy chains
//
// Pools can be wired into proxy chains via ``_proxy_to``. A read that
// misses the local store falls through to the upstream pool, and a
// successful upstream read is cached locally before being returned. This
// lets users compose layered caches like ``in_memory << hdf5 << s3`` so
// that hot reads never leave the in-memory tier.

// Pool_hooks is the set of internal storage hooks a backend may override.
type Pool_hooks interface {
	Read_(key string) (any, error)
	Write_(key string, value any) error
	Delete_(key string) error
	Exists_(key string) (bool, error)
	Keys_(kw ...py.Kw) (any, error)
	Empty_() error
	Read_async_(key string) (any, error)
	Write_async_(key string, value any) error
	Delete_async_(key string) error
	Exists_async_(key string) (bool, error)
	Search_keys_(base_gid string, attributes *py.Dict) (any, error)
	Candidate_keys_(base_gid string) ([]string, error)
}

// LAILA_IDENTIFIABLE_POOL_ is the abstract base class for laila storage
// pools.
//
// A pool is the *map* flavour of LAILA_IDENTIFIABLE_DATA_CONTAINER_: keys
// are entry ``global_id`` strings.
//
// Implements the proxy-aware public read/write/delete/exists/keys API and
// provides default in-memory hook implementations so simple pools can be
// created by just inheriting and overriding nothing. Real backends
// override the Read_ / Write_ / Delete_ / Keys_ / Exists_ / Empty_ hooks
// (and, optionally, their ``_async`` counterparts).
//
// Attributes
//
//   - Resource: default in-memory backing store. Concrete subclasses can
//     repurpose this (e.g. an in-memory cache fronting a remote store) or
//     leave it unused.
//   - Batch_accelerated: whether the backend can handle batched writes
//     more efficiently than individual ones. Consulted by central memory
//     when choosing a write strategy.
//   - Transformations: optional pipeline of Transformation objects applied
//     to every blob before write and reversed on read. Lets a pool
//     opaquely add compression, encoding, or encryption on top of the
//     storage backend.
//   - Index_enabled: maintain the per-base evolution / creation-timestamp
//     index (see pool_index) on every write and delete. Turn off for
//     pools whose keys are never looked up by attribute (log sinks, ring
//     buffers) to save the extra shard write per store.
//   - Index_pool: where the index shards are stored. nil (default) means
//     this pool itself; point it at an in-memory pool when index writes
//     on the data backend are too expensive.
//
// Index bookkeeping keys (``LAILA:POOL_INDEX:...``) are hidden from Keys
// unless ``include_index=True`` is passed.
type LAILA_IDENTIFIABLE_POOL_ struct {
	LAILA_IDENTIFIABLE_DATA_CONTAINER_

	Resource          *py.Dict                `py:"resource,cli_exempt,factory=New_resource_"`
	Batch_accelerated bool                    `py:"batch_accelerated" default:"false"`
	Transformations   *TransformationSequence `py:"transformations,cli_exempt"`
	Index_enabled     bool                    `py:"index_enabled" default:"true"`
	Index_pool        any                     `py:"index_pool,cli_exempt"`

	Proxy_to_ any        `py:"_proxy_to"`
	Index_    *PoolIndex `py:"_index"`
}

// LAILA_IDENTIFIABLE_POOL_class is the class descriptor; ``_scopes``
// defaults to ``[_POOL_SCOPE]`` and ``_persistent_storage`` is False
// (in-process storage).
var LAILA_IDENTIFIABLE_POOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_POOL", (*LAILA_IDENTIFIABLE_POOL_)(nil),
	LAILA_IDENTIFIABLE_DATA_CONTAINER_class,
).
	Set_private_default("_scopes", func() any { return []string{POOL_SCOPE_} }).
	Set_classvar("_persistent_storage", false)

// New_LAILA_IDENTIFIABLE_POOL_ is ``_LAILA_IDENTIFIABLE_POOL(**data)``.
func New_LAILA_IDENTIFIABLE_POOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_POOL_, error) {
	p := LAILA_IDENTIFIABLE_POOL_class.New().(*LAILA_IDENTIFIABLE_POOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// New_resource_ is the ``resource`` default factory (``dict``).
func (p *LAILA_IDENTIFIABLE_POOL_) New_resource_() *py.Dict { return py.NewDict() }

// Pool_base_ exposes the embedded base to code holding a subclass.
func (p *LAILA_IDENTIFIABLE_POOL_) Pool_base_() *LAILA_IDENTIFIABLE_POOL_ { return p }

// As_pool_ returns the embedded *LAILA_IDENTIFIABLE_POOL_ of any
// pool-derived instance (nil when v is not one).
func As_pool_(v any) *LAILA_IDENTIFIABLE_POOL_ {
	if p, ok := v.(*LAILA_IDENTIFIABLE_POOL_); ok {
		return p
	}
	if b, ok := v.(interface{ Pool_base_() *LAILA_IDENTIFIABLE_POOL_ }); ok {
		return b.Pool_base_()
	}
	return nil
}

// hooks is the most-derived hook implementation.
func (p *LAILA_IDENTIFIABLE_POOL_) hooks() Pool_hooks { return p.Self().(Pool_hooks) }

// Model_post_init validates the configuration of pools backed by external
// storage.
//
// A persistent backend serialises records to JSON / bytes, so it needs a
// transformation sequence to turn an Entry into a storable blob;
// ``transformations=None`` there would make every ``memorize`` fail later
// with an opaque serialisation error. It is rejected up front instead.
// In-memory pools may use nil.
func (p *LAILA_IDENTIFIABLE_POOL_) Model_post_init(ctx any) error {
	if err := p.LAILA_IDENTIFIABLE_DATA_CONTAINER_.Model_post_init(ctx); err != nil {
		return err
	}
	if p.Persistent_storage_() && p.Transformations == nil {
		return py.ValueError.Newf(
			"%s is backed by persistent storage and requires a "+
				"transformation sequence (got transformations=None); use e.g. "+
				"laila.transformations.transformation_base64", py.Pytypename(p.Self()))
	}
	return nil
}

// Persistent_storage_ is ``type(self)._persistent_storage``: True for
// backends whose storage outlives the process (every built-in file /
// database / object-store pool sets it). It gates the
// ``transformations=None`` rejection above and the persistence of the
// pool-index identity. Custom backends opt in by setting it on their
// class; in-process pools leave it False.
func (p *LAILA_IDENTIFIABLE_POOL_) Persistent_storage_() bool {
	if v, ok := p.Class_().Classvar("_persistent_storage"); ok {
		return py.Truthy(v)
	}
	return false
}

// Persistent_index_identity is whether the PoolIndex persists its
// identity in this pool (see F-03).
func (p *LAILA_IDENTIFIABLE_POOL_) Persistent_index_identity_() bool { return p.Persistent_storage_() }

// Pool_id is the unique identifier for this pool. Alias for Global_id.
func (p *LAILA_IDENTIFIABLE_POOL_) Pool_id() string { return p.Global_id() }

// -------- Index --------

// Index is this pool's PoolIndex (created on first access).
func (p *LAILA_IDENTIFIABLE_POOL_) Index() *PoolIndex {
	// Lazy creation is GIL-atomic in Python; here the (reentrant) pool
	// lock makes the first-access publication race-free.
	p.Lock()
	defer p.Unlock()
	if p.Index_ == nil {
		p.Index_ = New_PoolIndex(p)
	}
	return p.Index_
}

// Index_record_ indexes *key* after a write (no-op for index keys or when
// the index is disabled).
func (p *LAILA_IDENTIFIABLE_POOL_) Index_record_(key string, value any) {
	if p.Index_enabled && !Is_index_key(key) {
		p.Index().Record(key, value)
	}
}

// Index_remove_ un-indexes *key* after a delete.
func (p *LAILA_IDENTIFIABLE_POOL_) Index_remove_(key string) {
	if p.Index_enabled && !Is_index_key(key) {
		p.Index().Remove(key)
	}
}

// Resolve_indexed_ answers an attribute lookup from the index alone, or
// nil.
//
//   - base_gid: ``LAILA:ENTRY:<uuid>`` (no ``@`` suffix).
//   - attributes: search arguments of the reference: ``evolution`` (may be
//     negative, ``-1`` = latest) and / or ``creation_timestamp``.
//
// Returns a storage key the index believes exists (central memory
// validates it), or nil when the index cannot answer and the caller
// should fall back to scanning.
func (p *LAILA_IDENTIFIABLE_POOL_) Resolve_indexed_(base_gid string, attributes *py.Dict) (*string, error) {
	if !p.Index_enabled {
		return nil, nil
	}
	raw_evolution := attributes.GetDefault("evolution", nil)
	var evolution *int
	if !py.IsNone(raw_evolution) {
		n, err := py.Int(raw_evolution)
		if err != nil {
			return nil, err
		}
		evolution = &n
	}
	timestamp := attributes.GetDefault("creation_timestamp", nil)
	if !py.IsNone(timestamp) {
		return p.Index().By_creation_timestamp(base_gid, py.Str(timestamp), evolution), nil
	}
	if evolution == nil {
		return p.Index().Nth(base_gid, -1), nil
	}
	return p.Index().Nth(base_gid, *evolution), nil
}

// -------- Index-maintaining write / delete wrappers --------
// Every writer (central memory, proxy write-back, ``pool[k] = v``) goes
// through these so the index sees all mutations. Backends keep overriding
// only the Write_ / Delete_ (``_async``) hooks.

// Write stores *value* under *key* locally and indexes it.
func (p *LAILA_IDENTIFIABLE_POOL_) Write(key string, value any) error {
	if err := p.hooks().Write_(key, value); err != nil {
		return err
	}
	p.Index_record_(key, value)
	return nil
}

// Delete deletes *key* locally and un-indexes it.
func (p *LAILA_IDENTIFIABLE_POOL_) Delete(key string) error {
	if err := p.hooks().Delete_(key); err != nil {
		return err
	}
	p.Index_remove_(key)
	return nil
}

// Write_async is the async Write (index maintenance runs on a worker
// thread).
func (p *LAILA_IDENTIFIABLE_POOL_) Write_async(key string, value any) error {
	if err := p.hooks().Write_async_(key, value); err != nil {
		return err
	}
	if p.Index_enabled && !Is_index_key(key) {
		_, err := py.To_thread(func() (any, error) { p.Index().Record(key, value); return nil, nil })
		return err
	}
	return nil
}

// Delete_async is the async Delete (index maintenance runs on a worker
// thread).
func (p *LAILA_IDENTIFIABLE_POOL_) Delete_async(key string) error {
	if err := p.hooks().Delete_async_(key); err != nil {
		return err
	}
	if p.Index_enabled && !Is_index_key(key) {
		_, err := py.To_thread(func() (any, error) { p.Index().Remove(key); return nil, nil })
		return err
	}
	return nil
}

// -------- Proxy properties --------

// Proxy is the write-only property used to wire proxy relationships.
//
// Always returns nil -- the property exists so that the natural-looking
// assignment ``origin.proxy = cache`` (Set_proxy) can be used to express
// "make ``cache`` a proxy for ``origin``" (i.e. set ``cache._proxy_to =
// origin``). Use Proxy_to if you want to read the relationship.
func (p *LAILA_IDENTIFIABLE_POOL_) Proxy() any { return nil }

// Set_proxy is ``origin.proxy = pool``.
func (p *LAILA_IDENTIFIABLE_POOL_) Set_proxy(pool any) error {
	if !py.IsNone(pool) {
		return Link_proxy_(pool, p.Self())
	}
	return nil
}

// Proxy_to is the origin pool this one is a cache/proxy for, or nil.
func (p *LAILA_IDENTIFIABLE_POOL_) Proxy_to() any { return p.Proxy_to_ }

// Set_proxy_to is ``self.proxy_to = pool``.
func (p *LAILA_IDENTIFIABLE_POOL_) Set_proxy_to(pool any) error {
	if py.IsNone(pool) {
		p.Proxy_to_ = nil
		return nil
	}
	return Link_proxy_(p.Self(), pool)
}

// Link_proxy_ sets ``cache._proxy_to = origin`` after checking the chain
// stays acyclic.
//
// ValueError when *cache* is *origin*, or *origin*'s upstream chain
// already leads back to *cache* (``a << b`` then ``b << a``).
func Link_proxy_(cache any, origin any) error {
	cache_pool := As_pool_(cache)
	if cache_pool == nil {
		return py.TypeError.New("proxy cache must be a pool")
	}
	if py.Is(cache, origin) {
		return py.ValueError.Newf("A pool cannot proxy itself (%s)", cache_pool.Global_id())
	}
	seen := map[uintptr]struct{}{}
	cur := origin
	for !py.IsNone(cur) {
		if _, ok := seen[py.Id(cur)]; ok {
			break
		}
		if py.Is(cur, cache) {
			return py.ValueError.Newf(
				"Proxy cycle: %s already reaches %s through its proxy chain",
				global_id_of_(origin), cache_pool.Global_id())
		}
		seen[py.Id(cur)] = struct{}{}
		cur = getattr(cur, "_proxy_to")
	}
	cache_pool.Proxy_to_ = origin
	return nil
}

// Upstream_tiers_ lists the upstream pools of this one, nearest first;
// stops at a repeated pool (defensive).
func (p *LAILA_IDENTIFIABLE_POOL_) Upstream_tiers_() []*LAILA_IDENTIFIABLE_POOL_ {
	seen := map[uintptr]struct{}{py.Id(p.Self()): {}}
	out := []*LAILA_IDENTIFIABLE_POOL_{}
	cur := p.Proxy_to_
	for !py.IsNone(cur) {
		if _, ok := seen[py.Id(cur)]; ok {
			break
		}
		seen[py.Id(cur)] = struct{}{}
		tier := As_pool_(cur)
		if tier == nil {
			break
		}
		out = append(out, tier)
		cur = tier.Proxy_to_
	}
	return out
}

// Lshift is ``cache << origin``: install ``cache`` as a proxy for
// ``origin``.
//
// Returns *other* (the origin) so the operator chains right-to-left,
// letting expressions like ``mem << hdf5 << s3`` build a multi-tier cache
// where ``mem`` fronts ``hdf5`` which fronts ``s3``.
func (p *LAILA_IDENTIFIABLE_POOL_) Lshift(other any) (any, error) {
	if err := As_pool_(other).Set_proxy(p.Self()); err != nil {
		return nil, err
	}
	return other, nil
}

// Rshift is ``origin >> cache``: install ``cache`` as a proxy for
// ``origin``.
//
// Returns *other* (the cache) so the operator chains left-to-right,
// letting expressions like ``s3 >> hdf5 >> mem`` build a multi-tier cache
// where ``mem`` fronts ``hdf5`` which fronts ``s3``.
func (p *LAILA_IDENTIFIABLE_POOL_) Rshift(other any) (any, error) {
	if err := p.Set_proxy(other); err != nil {
		return nil, err
	}
	return other, nil
}

// -------- Internal storage hooks (override in subclasses) --------

// Read_ reads *key* from this pool's own storage.
//
// The default implementation reads from the in-memory ``resource`` dict
// under the pool's atomic lock and returns nil for missing keys (so the
// proxy-aware Getitem can detect a miss and fall through to the upstream
// pool).
func (p *LAILA_IDENTIFIABLE_POOL_) Read_(key string) (any, error) {
	p.Lock()
	blob, ok := p.Resource.Get(key)
	p.Unlock()
	if !ok || py.IsNone(blob) {
		return nil, nil
	}
	return blob, nil
}

// Write_ writes *value* under *key* into this pool's own storage.
//
// Default implementation writes into ``resource`` under the atomic lock.
// Implementations should treat *value* as opaque bytes/dict (the central
// memory layer has already serialised the entry).
func (p *LAILA_IDENTIFIABLE_POOL_) Write_(key string, value any) error {
	p.Lock()
	p.Resource.Set(key, value)
	p.Unlock()
	return nil
}

// Delete_ deletes *key* from this pool's own storage. Missing keys are
// silently tolerated.
func (p *LAILA_IDENTIFIABLE_POOL_) Delete_(key string) error {
	p.Lock()
	if p.Resource.Contains(key) {
		_ = p.Resource.Del(key)
	}
	p.Unlock()
	return nil
}

// Exists_ returns true if *key* is present in this pool's own storage.
// Should not consult the proxy chain.
func (p *LAILA_IDENTIFIABLE_POOL_) Exists_(key string) (bool, error) {
	p.Lock()
	defer p.Unlock()
	return p.Resource.Contains(key), nil
}

// Keys_ enumerates this pool's own keys.
//
// ``as_generator=False`` (default) snapshots and returns a list under the
// atomic lock (cheap, O(N) memory). ``as_generator=True`` returns an
// Iterator that holds the lock while iterating -- useful when keys are
// expensive to materialize but the caller wants to stream.
func (p *LAILA_IDENTIFIABLE_POOL_) Keys_(kw ...py.Kw) (any, error) {
	as_generator := py.Truthy(py.MergeKw(kw...).Get("as_generator", false))
	if !as_generator {
		p.Lock()
		defer p.Unlock()
		return py.NewList(p.Resource.Keys()...), nil
	}
	var gen py.Iterator = func(yield func(any, error) bool) {
		p.Lock()
		defer p.Unlock()
		// ``yield from self.resource.keys()``: like a dict view iterator,
		// a size change by the consumer (which re-enters the RLock) is
		// reported as RuntimeError on the next step.
		size := p.Resource.Len()
		for _, k := range p.Resource.Keys() {
			if !yield(k, nil) {
				return
			}
			if p.Resource.Len() != size {
				yield(nil, py.RuntimeError.New("dictionary changed size during iteration"))
				return
			}
		}
	}
	return gen, nil
}

// keys_list_ is ``list(self._keys())`` as strings (through the hook).
func (p *LAILA_IDENTIFIABLE_POOL_) keys_list_() []string {
	raw, err := p.hooks().Keys_()
	if err != nil {
		return nil
	}
	return keys_to_strings_(raw)
}

// keys_to_strings_ materialises a keys result (list / iterator) into
// strings.
func keys_to_strings_(raw any) []string {
	out := []string{}
	if it, ok := raw.(py.Iterator); ok {
		for v, err := range it {
			if err != nil {
				break
			}
			out = append(out, py.Str(v))
		}
		return out
	}
	for _, v := range py.Iter(raw) {
		out = append(out, py.Str(v))
	}
	return out
}

// Search_keys_ lists candidate keys for an attribute lookup
// (``remember("ENTRY:x@...")``).
//
// Central memory calls this *before* falling back to a full Keys_ scan
// when it has to find "the evolutions of *base_gid*" (``LAILA:ENTRY:<uuid>``,
// no ``@`` suffix) that satisfy *attributes* (``{"evolution": "3"}``,
// ``{"creation_timestamp": "..."}`` ...).
//
// The default answers from this pool's Index when it has a shard for
// *base_gid* and returns nil otherwise ("no index, scan"). Returning a
// list -- even an empty one -- is authoritative and skips the scan;
// central memory still validates the key it picks against the store.
func (p *LAILA_IDENTIFIABLE_POOL_) Search_keys_(base_gid string, attributes *py.Dict) (any, error) {
	if !p.Index_enabled {
		return nil, nil
	}
	keys := p.Index().Candidates(base_gid)
	if keys == nil {
		return nil, nil
	}
	return keys, nil
}

// Candidate_keys_ is the local-only list of keys that are evolutions of
// *base_gid*.
//
// Matches the exact key (a constant) and every ``base_gid@...`` key
// (variables). Default implementation filters Keys_; listing-based
// backends can push the prefix down.
func (p *LAILA_IDENTIFIABLE_POOL_) Candidate_keys_(base_gid string) ([]string, error) {
	raw, err := p.hooks().Keys_()
	if err != nil {
		return nil, err
	}
	prefix := base_gid + "@"
	out := []string{}
	for _, k := range keys_to_strings_(raw) {
		if k == base_gid || strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out, nil
}

// Empty_ wipes this pool's own storage. Default implementation clears
// ``resource`` under the atomic lock. Should not propagate to the proxy
// chain.
func (p *LAILA_IDENTIFIABLE_POOL_) Empty_() error {
	p.Lock()
	p.Resource.Clear()
	p.Unlock()
	return nil
}

// -------- Default async hooks (override in async-capable pools) --------

// Read_async_ is the async read; default just delegates to the sync Read_
// inline.
//
// Subclasses backed by a native-async client should override this to
// ``await`` non-blocking I/O. The default implementation runs the sync
// call on the calling loop, which blocks every other coroutine on that
// loop for the read's duration -- correct but honest about the backend's
// true (sync) nature.
func (p *LAILA_IDENTIFIABLE_POOL_) Read_async_(key string) (any, error) { return p.hooks().Read_(key) }

// Write_async_ is the async write; default delegates to sync Write_.
func (p *LAILA_IDENTIFIABLE_POOL_) Write_async_(key string, value any) error {
	return p.hooks().Write_(key, value)
}

// Delete_async_ is the async delete; default delegates to sync Delete_.
func (p *LAILA_IDENTIFIABLE_POOL_) Delete_async_(key string) error { return p.hooks().Delete_(key) }

// Exists_async_ is the async exists; default delegates to sync Exists_.
func (p *LAILA_IDENTIFIABLE_POOL_) Exists_async_(key string) (bool, error) {
	return p.hooks().Exists_(key)
}

// Read_through_async_ is the async, proxy-aware read: the coroutine
// counterpart of Getitem.
//
// Reads from this pool's own async storage and, on a miss, falls through
// the ``_proxy_to`` chain. A successful upstream read is cached back into
// this pool (via Write_async) before being returned, so later reads bypass
// the upstream -- exactly the layered-cache semantics ``mem << hdf5 << s3``
// relies on.
func (p *LAILA_IDENTIFIABLE_POOL_) Read_through_async_(key string) (any, error) {
	value, err := p.hooks().Read_async_(key)
	if err != nil {
		return nil, err
	}
	if !py.IsNone(value) {
		return value, nil
	}
	// Walk the chain iteratively (cycle-safe) and write the hit back into
	// every tier in front of the one that held it.
	missed := []*LAILA_IDENTIFIABLE_POOL_{p}
	for _, tier := range p.Upstream_tiers_() {
		value, err = tier.hooks().Read_async_(key)
		if err != nil {
			return nil, err
		}
		if !py.IsNone(value) {
			for i := len(missed) - 1; i >= 0; i-- {
				if err := missed[i].Write_async(key, value); err != nil {
					return nil, err
				}
			}
			return value, nil
		}
		missed = append(missed, tier)
	}
	return nil, nil
}

// -------- Proxy-aware public API --------

// Getitem is ``pool[key]``: retrieve the blob for *key*, with proxy
// fall-through and write-back.
//
// Two special cases:
//
//   - If *key* is a Manifest, the call short-circuits and returns a
//     PoolWrapper view scoped by that manifest. That lets users write
//     ``pool[manifest]["nested_key"]`` to read keys *as resolved through*
//     the manifest.
//   - If the local read misses and ``_proxy_to`` is set, the request is
//     forwarded to the upstream pool. On a successful upstream read, the
//     value is *cached* into this pool before being returned, so
//     subsequent reads bypass the upstream.
func (p *LAILA_IDENTIFIABLE_POOL_) Getitem(key any) (any, error) {
	if manifestCls, ok := py.Class_by_name("Manifest"); ok && py.Isinstance(key, manifestCls) {
		return New_PoolWrapper(p.Self(), key), nil
	}
	skey := py.Str(key)
	value, err := p.hooks().Read_(skey)
	if err != nil {
		return nil, err
	}
	if !py.IsNone(value) {
		return value, nil
	}
	missed := []*LAILA_IDENTIFIABLE_POOL_{p}
	for _, tier := range p.Upstream_tiers_() {
		value, err = tier.hooks().Read_(skey)
		if err != nil {
			return nil, err
		}
		if !py.IsNone(value) {
			for i := len(missed) - 1; i >= 0; i-- {
				if err := missed[i].Write(skey, value); err != nil {
					return nil, err
				}
			}
			return value, nil
		}
		missed = append(missed, tier)
	}
	return nil, nil
}

// Setitem is ``pool[key] = entry``: store *entry* under *key*. Local-only;
// never propagates to a proxy origin.
func (p *LAILA_IDENTIFIABLE_POOL_) Setitem(key any, entry any) error {
	return p.Write(py.Str(key), entry)
}

// Delitem is ``del pool[key]``: delete the entry for *key*. Local-only;
// never propagates to a proxy origin.
func (p *LAILA_IDENTIFIABLE_POOL_) Delitem(key any) error { return p.Delete(py.Str(key)) }

// Empty removes every entry from this pool. Local-only; never propagates
// to a proxy origin.
//
// Also drops this pool's index shards, including those held by a separate
// Index_pool.
func (p *LAILA_IDENTIFIABLE_POOL_) Empty() error {
	if p.Index_enabled {
		p.Index().Clear()
	}
	return p.hooks().Empty_()
}

// Exists returns true if *key* is present in this pool. Local-only check.
func (p *LAILA_IDENTIFIABLE_POOL_) Exists(key string) (bool, error) { return p.hooks().Exists_(key) }

// Contains is ``key in pool`` -- thin alias for Exists.
func (p *LAILA_IDENTIFIABLE_POOL_) Contains(key any) (bool, error) {
	return p.Self().(interface{ Exists(string) (bool, error) }).Exists(py.Str(key))
}

// Keys returns the keys stored in this pool. Local-only enumeration.
//
//   - as_generator (default False): if False, return a snapshot list
//     (cheap-ish, O(N) memory). If True, return an Iterator that holds
//     the atomic lock for its full lifetime -- prefer this when keys are
//     expensive to materialize but the consumer wants to stream.
//   - include_index (default False): also yield the pool's own index
//     bookkeeping keys (``LAILA:POOL_INDEX:...``). Hidden by default so
//     consumers that enumerate a pool (``duplicate_pool``, manifests,
//     user code) only see entries.
func (p *LAILA_IDENTIFIABLE_POOL_) Keys(kw ...py.Kw) (any, error) {
	opts := py.MergeKw(kw...)
	as_generator := py.Truthy(opts.Get("as_generator", false))
	include_index := py.Truthy(opts.Get("include_index", false))
	raw, err := p.hooks().Keys_(py.Kw{"as_generator": as_generator})
	if err != nil {
		return nil, err
	}
	if include_index {
		return raw, nil
	}
	if as_generator {
		inner, ok := raw.(py.Iterator)
		if !ok {
			inner = py.Iterator_of(py.Iter(raw))
		}
		var gen py.Iterator = func(yield func(any, error) bool) {
			for k, err := range inner {
				if err != nil {
					yield(nil, err)
					return
				}
				if Is_index_key(k) {
					continue
				}
				if !yield(k, nil) {
					return
				}
			}
		}
		return gen, nil
	}
	out := py.NewList()
	for _, k := range keys_to_strings_(raw) {
		if !Is_index_key(k) {
			out.Append(k)
		}
	}
	return out, nil
}

// Sync flushes any in-memory write cache to the backing store.
//
// The base implementation always raises -- override in subclasses that
// maintain a write-back cache (e.g. some HDF5 / DuckDB configurations).
// Pools that write directly to storage on every set should leave the
// base implementation in place; callers can then suppress the
// NotImplementedError when treating ``sync`` as a no-op.
func (p *LAILA_IDENTIFIABLE_POOL_) Sync() error {
	return py.NotImplementedError.New(
		"Sync is not implemented for this pool, the pool is cacheless, i.e. operations are immediately executed on the underlying storage.")
}

// Le is ``self <= other`` -- bulk-copy ``other`` pool's contents into this
// one.
//
// Delegates to ``central.memory._duplicate_pool`` on the active local
// policy, so the copy goes through the same routing / manifest machinery
// as a normal ``memorize`` workflow. *other* may be a pool instance or a
// pool nickname/global-id string (TypeError for anything else --
// ``NotImplemented`` in Python, which the interpreter turns into the same
// TypeError).
func (p *LAILA_IDENTIFIABLE_POOL_) Le(other any) (any, error) {
	if _, isStr := other.(string); !isStr && As_pool_(other) == nil {
		return nil, py.TypeError.Newf("'<=' not supported between instances of '%s' and '%s'",
			py.Pytypename(p.Self()), py.Pytypename(other))
	}
	policy, err := Get_active_policy()
	if err != nil {
		return nil, err
	}
	memory := getattr(getattr(policy, "central"), "memory")
	return callm(memory, "_duplicate_pool", other, p.Self())
}

func init() {
	py.Register_property("pool_id", "index", "proxy", "proxy_to", "_persistent_index_identity")
	// ``DefaultPool = _LAILA_IDENTIFIABLE_POOL`` (macros/defaults.py).
	DefaultPool = LAILA_IDENTIFIABLE_POOL_class
}
