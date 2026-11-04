package laila

import (
	"sort"
	"strings"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// data/schema/pool_index.py -- per-pool evolution / creation-timestamp
// index.
//
// A PoolIndex answers, for one *base* global id (``LAILA:ENTRY:<uuid>``,
// no ``@`` suffix), the questions ``laila.remember`` asks when a reference
// carries search attributes:
//
//   - which evolutions are stored (and whether a constant key exists), so
//     ``@evolution=-1`` (latest) / ``-k`` resolve without listing keys;
//   - which evolution was created at a given ``creation_timestamp``, so
//     ``@creation_timestamp=<iso>`` resolves without reading every record.
//
// # Layout
//
// One **shard per base**. A shard is a *constant* Entry with scope
// ``POOL_INDEX`` and the deterministic, stable storage key
// ``LAILA:POOL_INDEX:<uuid5("pool_index:<index identity>:<base>")>`` whose
// payload is::
//
//	{"base": base, "evolutions": [0, 1, 2], "constant": False,
//	 "creation_timestamps": {"2026-...": 2, ...}}
//
// Shards live in ``owner.index_pool`` (default: the owner pool itself;
// point it at an in-memory pool for cheap index writes). Because the key
// is constant, a shard is rewritten in place: no listing, no straggler
// cleanup, so maintaining a shard is O(size of that base's history) and
// independent of the size of the pool. Shards are loaded on demand.
//
// # Index identity
//
// Shard ids are derived from the pool's **index identity**, not from the
// transient ``uuid`` of the object: the identity is persisted in the owner
// pool under the reserved key ``LAILA:POOL_INDEX:<uuid5("pool_index:identity")>``
// the first time the index is used, and adopted by every later pool
// instance that opens the same backing store. Reopening a file therefore
// finds its shards again (no orphaned shards, no silent fall back to key
// scans). In-memory pools never persist anything and keep using their
// ``uuid``.
//
// # Legacy layout
//
// Earlier releases stored shards as *evolvable* entries
// (``<shard id>@evolution=N``). The first time an index touches a store
// it migrates such shards to the constant layout (one key listing per
// pool instance, performed lazily and only when a legacy shard may exist).
//
// # Consistency model
//
// The index is a **validated cache, never an authority**. Every hit is
// confirmed by the read that follows it in central memory; a miss
// invalidates the base and falls back to the key scan. A failed shard
// write logs, invalidates the base, and never fails the user's
// ``memorize``. PoolIndex.Rebuild repopulates every shard from the owner's
// keys.
//
// Maintenance is **write-through**: the pool's ``write`` / ``delete``
// wrappers call Record / Remove, which update the shard in memory and
// persist it immediately, all under the owner's atomic lock. Shard keys
// are never indexed themselves and are hidden from the pool's public
// ``keys()``.

var POOL_INDEX_LOG_ = py.Get_logger("laila.data.schema.pool_index")

var INDEX_KEY_PREFIX_ = TOPMOST_SCOPE_ + ":" + POOL_INDEX_SCOPE_ + ":"
var LAILA_KEY_PREFIX_ = TOPMOST_SCOPE_ + ":"

// CREATION_TIMESTAMP_ATTRIBUTE is the name of the search attribute keyed
// by an entry's creation stamp.
const CREATION_TIMESTAMP_ATTRIBUTE = "creation_timestamp"

// INDEX_IDENTITY_KEY is the reserved (hidden) key under which a pool
// persists its index identity. Derived from a fixed namespace so it does
// not depend on the active laila namespace: the same file must find its
// identity whoever reopens it.
var INDEX_IDENTITY_KEY = INDEX_KEY_PREFIX_ + py.Uuid5(py.NAMESPACE_URL, "laila:pool_index:identity").String()

// Is_index_key is true for storage keys that belong to an index shard (or
// the identity record).
func Is_index_key(key any) bool {
	s, ok := key.(string)
	return ok && strings.HasPrefix(s, INDEX_KEY_PREFIX_)
}

// Is_laila_key is true for keys central memory manages (``LAILA:...``);
// raw user keys are not indexed.
func Is_laila_key(key any) bool {
	s, ok := key.(string)
	return ok && strings.HasPrefix(s, LAILA_KEY_PREFIX_)
}

// Record_creation_timestamp_ pulls the entry ``creation_timestamp`` out of
// a stored record without rebuilding it.
//
// *raw* is whatever the pool holds: a JSON string / bytes, a record dict
// whose ``entry`` is a serialized dict, or (in-memory pools with no
// transformations) a record dict whose ``entry`` is a live Entry.
func Record_creation_timestamp_(raw any) any {
	if b, ok := raw.([]byte); ok {
		raw = string(b)
	}
	if s, ok := raw.(string); ok {
		parsed, err := py.Json_loads(s)
		if err != nil {
			return nil
		}
		raw = parsed
	}
	d, ok := asDict(raw)
	if !ok {
		return getattr_default(raw, "creation_timestamp", nil)
	}
	entry := d.GetDefault("entry", d)
	if ed, ok := asDict(entry); ok {
		return ed.GetDefault("_creation_timestamp", nil)
	}
	return getattr_default(entry, "creation_timestamp", nil)
}

// getattr_default is ``getattr(obj, name, default)`` with pointer
// unwrapping (``*string`` -> ``str | None``).
func getattr_default(obj any, name string, def any) any {
	if py.IsNone(obj) {
		return def
	}
	v, err := py.Getattr(obj, name)
	if err != nil {
		return def
	}
	return py.Deref_any(v)
}

// Key_evolution_ is the evolution encoded in a storage key; nil for a
// constant (no ``@``).
func Key_evolution_(key string) *int {
	_, attrs, err := Split_global_id_attributes(key)
	if err != nil {
		return nil
	}
	raw := attrs.GetDefault(EVOLUTION_ATTRIBUTE, nil)
	if py.IsNone(raw) || !isdigit(py.Str(raw)) {
		return nil
	}
	n, err := py.Int(py.Str(raw))
	if err != nil {
		return nil
	}
	return &n
}

// Evolution_key_ is ``base`` or ``base@evolution=<n>``.
func Evolution_key_(base string, evolution *int) string {
	if evolution == nil {
		return base
	}
	return base + "@" + EVOLUTION_ATTRIBUTE + "=" + py.Str(*evolution)
}

// Rank_ is the sort rank: constants (no evolution) sort below every
// evolution.
func Rank_(evolution *int) int {
	if evolution == nil {
		return -1
	}
	return *evolution
}

// Pool_index_shard_ is the in-memory shard of one base: the Python dict
// ``{"base", "evolutions", "constant", "creation_timestamps"}``.
type Pool_index_shard_ struct {
	Base       string
	Evolutions []int
	Constant   bool
	// Creation_timestamps maps ``str -> int | None`` (insertion ordered,
	// as the Python dict is).
	Creation_timestamps *py.Dict
}

// PoolIndex is the shard-per-base evolution / creation-timestamp index of
// one pool.
//
// Created lazily by LAILA_IDENTIFIABLE_POOL_.Index. All methods take the
// owner's atomic lock (re-entrant), so callers may hold it already.
type PoolIndex struct {
	Owner_    *LAILA_IDENTIFIABLE_POOL_
	Shards_   map[string]*Pool_index_shard_
	Entries_  map[string]*Entry // base -> live shard Entry
	Missing_  map[string]struct{}
	Identity_ *string
	Migrated_ bool
}

// New_PoolIndex is ``PoolIndex(owner)``.
func New_PoolIndex(owner *LAILA_IDENTIFIABLE_POOL_) *PoolIndex {
	return &PoolIndex{
		Owner_:   owner,
		Shards_:  map[string]*Pool_index_shard_{},
		Entries_: map[string]*Entry{},
		Missing_: map[string]struct{}{},
	}
}

// ------------------------------------------------------------------
// Identity helpers
// ------------------------------------------------------------------

// Index_pool is the pool that stores the shards (``owner.index_pool`` or
// the owner).
func (x *PoolIndex) Index_pool() *LAILA_IDENTIFIABLE_POOL_ {
	if p := As_pool_(x.Owner_.Index_pool); p != nil {
		return p
	}
	return x.Owner_
}

// Identity is the stable identity shard ids are derived from.
//
// Persistent pools (``_persistent_storage = True``) store it in the owner
// under INDEX_IDENTITY_KEY on first use and adopt the stored value on
// reopen, so a fresh pool instance finds the shards an earlier instance
// wrote. In-process pools use their ``uuid`` and persist nothing.
func (x *PoolIndex) Identity() string {
	if x.Identity_ != nil {
		return *x.Identity_
	}
	x.Owner_.Lock()
	defer x.Owner_.Unlock()
	if x.Identity_ != nil {
		return *x.Identity_
	}
	identity := py.Str(x.Owner_.Uuid())
	if x.Owner_.Persistent_index_identity_() {
		err := attempt(func() error {
			stored, err := x.Owner_.hooks().Read_(INDEX_IDENTITY_KEY)
			if err != nil {
				return err
			}
			if b, ok := stored.([]byte); ok {
				stored = string(b)
			}
			if s, ok := stored.(string); ok {
				if stored, err = py.Json_loads(s); err != nil {
					return err
				}
			}
			if d, ok := asDict(stored); ok {
				if s, ok := d.GetDefault("identity", nil).(string); ok {
					identity = s
					return nil
				}
			}
			return x.Owner_.hooks().Write_(INDEX_IDENTITY_KEY, py.DictOf("identity", identity))
		})
		if err != nil { // the index is a cache: never break the caller
			POOL_INDEX_LOG_.Debug("pool index: could not persist identity: %s", py.Str(err))
		}
	}
	x.Identity_ = &identity
	return identity
}

// Shard_id is the deterministic, constant storage key of the shard for
// *base*.
func (x *PoolIndex) Shard_id(base string) string {
	return INDEX_KEY_PREFIX_ + x.Shard_uuid_(base)
}

// Shard_uuid_ is ``uuid5(identity, base)``: stable across processes and
// laila namespaces.
func (x *PoolIndex) Shard_uuid_(base string) string {
	namespace, err := py.Parse_uuid(x.Identity())
	if err != nil { // non-UUID identity (custom pools): hash it first
		namespace = py.Uuid5(py.NAMESPACE_URL, "laila:pool_index:"+x.Identity())
	}
	return py.Uuid5(namespace, base).String()
}

// ------------------------------------------------------------------
// Queries
// ------------------------------------------------------------------

// Candidates is every stored key of *base*, or nil when no shard exists.
func (x *PoolIndex) Candidates(base string) []string {
	shard := x.Shard_(base, false)
	if shard == nil {
		return nil
	}
	keys := []string{}
	if shard.Constant {
		keys = append(keys, base)
	}
	for _, e := range shard.Evolutions {
		e := e
		keys = append(keys, Evolution_key_(base, &e))
	}
	return keys
}

// Latest is the key of the highest evolution (or the constant key), or
// nil.
func (x *PoolIndex) Latest(base string) *string { return x.Nth(base, -1) }

// Nth is the key of the *n*-th evolution of *base*.
//
// ``n >= 0`` is an exact evolution; ``n < 0`` counts from the end of the
// sorted evolutions (``-1`` = latest). A constant key is preferred for
// ``n < 0`` when it is the only thing stored, and counts as the lowest
// rank otherwise. nil when out of range or unindexed.
func (x *PoolIndex) Nth(base string, n int) *string {
	shard := x.Shard_(base, false)
	if shard == nil {
		return nil
	}
	if n >= 0 {
		for _, e := range shard.Evolutions {
			if e == n {
				k := Evolution_key_(base, &n)
				return &k
			}
		}
		return nil
	}
	ranked := []*int{}
	if shard.Constant {
		ranked = append(ranked, nil)
	}
	for _, e := range shard.Evolutions {
		e := e
		ranked = append(ranked, &e)
	}
	idx := len(ranked) + n
	if idx < 0 || idx >= len(ranked) {
		return nil
	}
	k := Evolution_key_(base, ranked[idx])
	return &k
}

// By_creation_timestamp is the key of the evolution created at *timestamp*
// (exact match).
//
// With *evolution* given, the match must also be that evolution (negative
// values count from the end, as in Nth). nil when unindexed or no such
// stamp.
func (x *PoolIndex) By_creation_timestamp(base string, timestamp string, evolution *int) *string {
	shard := x.Shard_(base, false)
	if shard == nil {
		return nil
	}
	found, ok := shard.Creation_timestamps.Get(timestamp)
	if !ok {
		return nil
	}
	key := Evolution_key_(base, toIntPtrLoose(found))
	if evolution != nil {
		nth := x.Nth(base, *evolution)
		if nth == nil || *nth != key {
			return nil
		}
	}
	return &key
}

// toIntPtrLoose converts ``int | None`` (any numeric) to *int.
func toIntPtrLoose(v any) *int {
	if py.IsNone(v) {
		return nil
	}
	n, err := py.Int(v)
	if err != nil {
		return nil
	}
	return &n
}

// ------------------------------------------------------------------
// Maintenance (write-through)
// ------------------------------------------------------------------

// Record indexes a key that was just written with record *value*, then
// persists the shard.
//
// Only ``LAILA:`` keys are indexed: raw user keys (``pool["k0"]``) carry
// no lineage and skipping them keeps a plain write at one backend write
// instead of two.
func (x *PoolIndex) Record(key string, value any) {
	if Is_index_key(key) || !Is_laila_key(key) {
		return
	}
	base, _, err := Split_global_id_attributes(key)
	if err != nil {
		return
	}
	evolution := Key_evolution_(key)
	stamp := Record_creation_timestamp_(value)
	x.Owner_.Lock()
	defer x.Owner_.Unlock()
	shard := x.Shard_(base, true)
	if evolution == nil {
		shard.Constant = true
	} else if !containsInt(shard.Evolutions, *evolution) {
		shard.Evolutions = append(shard.Evolutions, *evolution)
		sort.Ints(shard.Evolutions)
	}
	if !py.IsNone(stamp) {
		stamp_s := py.Str(stamp)
		stamps := shard.Creation_timestamps
		// An evolution re-written with a different stamp: drop the stale one.
		for _, it := range stamps.Items() {
			if intPtrEqual(toIntPtrLoose(it.Value), evolution) && py.Str(it.Key) != stamp_s {
				_ = stamps.Del(it.Key)
			}
		}
		// Same-millisecond collision: keep the highest evolution.
		prev, present := stamps.Get(stamp_s)
		if !present || Rank_(evolution) > Rank_(toIntPtrLoose(prev)) {
			stamps.Set(stamp_s, intPtrValue(evolution))
		}
	}
	x.Flush_(base)
}

// Remove un-indexes a key that was just deleted, then persists (or
// drops) the shard.
func (x *PoolIndex) Remove(key string) {
	if Is_index_key(key) || !Is_laila_key(key) {
		return
	}
	base, _, err := Split_global_id_attributes(key)
	if err != nil {
		return
	}
	evolution := Key_evolution_(key)
	x.Owner_.Lock()
	defer x.Owner_.Unlock()
	shard := x.Shard_(base, false)
	if shard == nil {
		return
	}
	if evolution == nil {
		shard.Constant = false
	} else if containsInt(shard.Evolutions, *evolution) {
		out := shard.Evolutions[:0]
		removed := false
		for _, e := range shard.Evolutions {
			if e == *evolution && !removed {
				removed = true
				continue
			}
			out = append(out, e)
		}
		shard.Evolutions = out
	}
	kept := py.NewDict()
	for _, it := range shard.Creation_timestamps.Items() {
		if !intPtrEqual(toIntPtrLoose(it.Value), evolution) {
			kept.Set(it.Key, it.Value)
		}
	}
	shard.Creation_timestamps = kept
	if !shard.Constant && len(shard.Evolutions) == 0 {
		x.Drop_shard_(base)
	} else {
		x.Flush_(base)
	}
}

// Invalidate forgets the in-memory state of *base* (or of every base when
// nil).
//
// The next query reloads the shard from the index pool; central memory
// calls this when an index hit fails validation.
func (x *PoolIndex) Invalidate(base ...string) {
	x.Owner_.Lock()
	defer x.Owner_.Unlock()
	if len(base) == 0 || base[0] == "" {
		x.Shards_ = map[string]*Pool_index_shard_{}
		x.Entries_ = map[string]*Entry{}
		x.Missing_ = map[string]struct{}{}
		return
	}
	delete(x.Shards_, base[0])
	delete(x.Entries_, base[0])
	delete(x.Missing_, base[0])
}

// Clear deletes every shard of the owner's current keys from the index
// pool.
//
// Used by ``pool.empty()``: shards are addressed per base, so the owner's
// keys tell us which shards exist.
func (x *PoolIndex) Clear() {
	x.Owner_.Lock()
	defer x.Owner_.Unlock()
	bases := map[string]struct{}{}
	order := []string{}
	for _, k := range x.Owner_.keys_list_() {
		if Is_laila_key(k) && !Is_index_key(k) {
			base, _, err := Split_global_id_attributes(k)
			if err != nil {
				continue
			}
			if _, seen := bases[base]; !seen {
				bases[base] = struct{}{}
				order = append(order, base)
			}
		}
	}
	for _, base := range order {
		x.Drop_shard_(base)
	}
	x.Invalidate()
}

// Rebuild rebuilds every shard from the owner's keys and records.
//
// Reads each record once (for its creation stamp). Replaces the shard
// contents outright, so evolutions that no longer exist are dropped too.
func (x *PoolIndex) Rebuild() {
	x.Owner_.Lock()
	defer x.Owner_.Unlock()
	by_base := map[string][]string{}
	order := []string{}
	for _, key := range x.Owner_.keys_list_() {
		if Is_index_key(key) || !Is_laila_key(key) {
			continue
		}
		base, _, err := Split_global_id_attributes(key)
		if err != nil {
			continue
		}
		if _, ok := by_base[base]; !ok {
			order = append(order, base)
		}
		by_base[base] = append(by_base[base], key)
	}
	for _, base := range order {
		keys := by_base[base]
		x.Shard_(base, true) // load (to keep the shard entry's counter)
		shard := x.Shards_[base]
		shard.Evolutions = []int{}
		shard.Constant = false
		shard.Creation_timestamps = py.NewDict()
		for _, key := range keys {
			evolution := Key_evolution_(key)
			if evolution == nil {
				shard.Constant = true
			} else {
				shard.Evolutions = append(shard.Evolutions, *evolution)
			}
			raw, _ := x.Owner_.hooks().Read_(key)
			stamp := Record_creation_timestamp_(raw)
			if !py.IsNone(stamp) {
				stamp_s := py.Str(stamp)
				prev, present := shard.Creation_timestamps.Get(stamp_s)
				if !present || Rank_(evolution) > Rank_(toIntPtrLoose(prev)) {
					shard.Creation_timestamps.Set(stamp_s, intPtrValue(evolution))
				}
			}
		}
		sort.Ints(shard.Evolutions)
		x.Flush_(base)
	}
}

// ------------------------------------------------------------------
// Shard storage
// ------------------------------------------------------------------

// New_shard_ is an empty shard for *base*.
func (x *PoolIndex) New_shard_(base string) *Pool_index_shard_ {
	return &Pool_index_shard_{Base: base, Evolutions: []int{}, Creation_timestamps: py.NewDict()}
}

// Shard_ returns the in-memory shard for *base*, loading it from the
// index pool on first use (``create`` adds an empty one when absent).
func (x *PoolIndex) Shard_(base string, create bool) *Pool_index_shard_ {
	x.Owner_.Lock()
	defer x.Owner_.Unlock()
	if shard := x.Shards_[base]; shard != nil {
		return shard
	}
	if _, missing := x.Missing_[base]; !missing {
		if loaded := x.Load_(base); loaded != nil {
			return loaded
		}
		x.Missing_[base] = struct{}{}
	}
	if !create {
		return nil
	}
	delete(x.Missing_, base)
	shard := x.New_shard_(base)
	x.Shards_[base] = shard
	return shard
}

// Shard_from_raw_ rebuilds the in-memory shard (and caches its live
// Entry) from a stored record.
func (x *PoolIndex) Shard_from_raw_(base string, raw any) (*Pool_index_shard_, error) {
	built, err := Record_build_sync_(raw)
	if err != nil {
		return nil, err
	}
	entry := As_entry_(built.GetDefault("entry", nil))
	if entry == nil {
		return nil, nil
	}
	data, err := entry.Data()
	if err != nil {
		return nil, err
	}
	d, ok := asDict(data)
	if !ok {
		return nil, nil
	}
	shard := x.New_shard_(base)
	for _, e := range py.Iter(d.GetDefault("evolutions", py.NewList())) {
		n, err := py.Int(e)
		if err != nil {
			return nil, err
		}
		shard.Evolutions = append(shard.Evolutions, n)
	}
	sort.Ints(shard.Evolutions)
	shard.Constant = py.Truthy(d.GetDefault("constant", false))
	if ts, ok := asDict(d.GetDefault("creation_timestamps", py.NewDict())); ok {
		for _, it := range ts.Items() {
			shard.Creation_timestamps.Set(py.Str(it.Key), intPtrValue(toIntPtrLoose(it.Value)))
		}
	}
	x.Shards_[base] = shard
	x.Entries_[base] = entry
	return shard, nil
}

// Load_ loads the persisted shard of *base* from its constant key (one
// read, no listing).
func (x *PoolIndex) Load_(base string) *Pool_index_shard_ {
	sid := x.Shard_id(base)
	var out *Pool_index_shard_
	err := attempt(func() error {
		pool := x.Index_pool()
		raw, err := pool.hooks().Read_(sid)
		if err != nil {
			return err
		}
		if py.IsNone(raw) {
			// A persistent store written by an earlier release may still
			// hold legacy (evolvable) shards: clean them up once.
			x.Ensure_migrated_()
			return nil
		}
		out, err = x.Shard_from_raw_(base, raw)
		return err
	})
	if err != nil { // index is a cache: never let it break the caller
		POOL_INDEX_LOG_.Warning("pool index: could not load shard for %s: %s", base, py.Str(err))
		return nil
	}
	return out
}

// Ensure_migrated_ runs Migrate_legacy_shards_ once, and only for
// persistent index pools.
func (x *PoolIndex) Ensure_migrated_() {
	if x.Migrated_ {
		return
	}
	if !x.Index_pool().Persistent_storage_() {
		x.Migrated_ = true // in-process storage cannot hold legacy shards
		return
	}
	x.Migrate_legacy_shards_()
}

// Migrate_legacy_shards_ is the one-time purge of legacy
// ``<shard>@evolution=N`` shards.
//
// Earlier releases derived shard ids from the transient uuid of the pool
// *instance* that wrote them (and the active laila namespace), so shards
// found in a reopened store can never be attributed to the identity now in
// use: they are dead weight and are deleted. The index pool is listed once
// per PoolIndex instance and only when a shard read misses. Lineage
// information is rebuilt lazily by the write-through maintenance, or
// eagerly with Rebuild.
func (x *PoolIndex) Migrate_legacy_shards_() {
	x.Migrated_ = true
	pool := x.Index_pool()
	err := attempt(func() error {
		legacy := []string{}
		for _, k := range pool.keys_list_() {
			if Is_index_key(k) && strings.Contains(k, "@") {
				legacy = append(legacy, k)
			}
		}
		for _, key := range legacy {
			_ = attempt(func() error { return pool.hooks().Delete_(key) }) // best effort cleanup
		}
		if len(legacy) > 0 {
			POOL_INDEX_LOG_.Info("pool index: removed %d legacy index shard(s)", len(legacy))
		}
		return nil
	})
	if err != nil { // best effort
		POOL_INDEX_LOG_.Warning("pool index: legacy shard cleanup failed: %s", py.Str(err))
	}
}

// Shard_payload_ is the fresh container payload of a shard: the in-memory
// pool stores a snapshot, and the live shard must never alias what is
// stored.
func (s *Pool_index_shard_) Shard_payload_() *py.Dict {
	evolutions := py.NewList()
	for _, e := range s.Evolutions {
		evolutions.Append(e)
	}
	return py.DictOf(
		"base", s.Base,
		"evolutions", evolutions,
		"constant", s.Constant,
		"creation_timestamps", s.Creation_timestamps.Copy(),
	)
}

// Flush_ persists the in-memory shard of *base* in place under its
// constant key.
func (x *PoolIndex) Flush_(base string) {
	shard := x.Shards_[base]
	if shard == nil {
		return
	}
	pool := x.Index_pool()
	err := attempt(func() error {
		payload := shard.Shard_payload_()
		entry := x.Entries_[base]
		if entry == nil {
			var err error
			entry, err = Entry_contingent(py.Kw{
				"uuid":      x.Shard_uuid_(base),
				"scopes":    []string{POOL_INDEX_SCOPE_},
				"evolution": nil, // constant: one stable key per base
				"data":      payload,
				"state":     EntryState_READY,
			})
			if err != nil {
				return err
			}
			x.Entries_[base] = entry
		} else if err := entry.Set_data(payload); err != nil {
			return err
		}
		record, err := New_Record(py.Kw{"entry": entry})
		if err != nil {
			return err
		}
		blob, err := record.Serialize(pool.Transformations)
		if err != nil {
			return err
		}
		if err := pool.hooks().Write_(x.Shard_id(base), blob); err != nil {
			return err
		}
		entry.Mark_memorized()
		return nil
	})
	if err != nil {
		POOL_INDEX_LOG_.Warning("pool index: could not persist shard for %s: %s", base, py.Str(err))
		x.Invalidate(base)
	}
}

// Drop_shard_ deletes the persisted shard for *base* and forgets it.
func (x *PoolIndex) Drop_shard_(base string) {
	pool := x.Index_pool()
	err := attempt(func() error {
		x.Ensure_migrated_()
		return pool.hooks().Delete_(x.Shard_id(base))
	})
	if err != nil {
		POOL_INDEX_LOG_.Warning("pool index: could not drop shard for %s: %s", base, py.Str(err))
	}
	delete(x.Shards_, base)
	delete(x.Entries_, base)
	x.Missing_[base] = struct{}{}
}

func containsInt(xs []int, n int) bool {
	for _, x := range xs {
		if x == n {
			return true
		}
	}
	return false
}

func intPtrEqual(a, b *int) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
