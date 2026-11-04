package laila

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/memory/schema/manifest.py -- Manifest: a structured map
// of user-defined keys to ``global_id`` references.
//
// A Manifest is an Entry whose payload is a *blueprint*: a nested
// dictionary whose leaves are ``global_id`` strings (or lists thereof). A
// manifest is therefore a normal READY entry -- its payload is the
// blueprint dict itself. The ``Realized`` property batch-fetches every
// referenced entry through the active policy's central memory and returns
// a nested dict of Entry objects mirroring the blueprint. The fetch uses
// the memory's direct-await resolver (Read_entries_async_ /
// Read_entries_direct_): one task for the whole batch rather than one
// future per child, released as soon as the entries have been collected.

// ----------------------------------------------------------------------
// Non-memorizing algorithmic help: SQL index
// ----------------------------------------------------------------------
// The Manifest SQL index is a *non-memorizing algorithmic helper* -- a
// lightweight, temporary, query-side convenience that a manifest owns
// directly while it is being operated on. It deliberately bypasses
// ``central.memory`` (see ``agentic/internal/memory.md`` for the sanctioned
// exemption): it is never memorized, never registered with a pool router,
// and never travels with the manifest on the wire. It is a plain sqlite
// database file under ``<laila_root>/indices`` that can be cleared or
// invalidated at any time.

var FORBIDDEN_SQL_ = []string{"ORDER BY", "LIMIT", "GROUP BY", "HAVING", "JOIN", "UNION"}

const (
	SQL_INDEX_TABLE_ = "manifest"
	SQL_ROW_IDX_COL_ = "__row_idx__"
	SQL_META_TABLE_  = "__laila_index_meta__"
)

var SQL_IDENT_RE_ = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Is_primitive_sql_value_ is ``isinstance(val, _PRIMITIVE_TYPES)`` --
// ``(str, int, float, bool, bytes, type(None))``.
func Is_primitive_sql_value_(val any) bool {
	switch val.(type) {
	case nil, string, bool, []byte, float32, float64,
		int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	}
	return py.Pytypename(val) == "int"
}

// SqlResult_ is an eagerly fetched query result with the cursor methods
// the index uses.
type SqlResult_ struct {
	rows [][]any
}

// Fetchall returns (and drains) the rows.
func (r *SqlResult_) Fetchall() [][]any {
	rows := r.rows
	r.rows = nil
	return rows
}

// Fetchone pops the first row or returns nil.
func (r *SqlResult_) Fetchone() []any {
	if len(r.rows) == 0 {
		return nil
	}
	row := r.rows[0]
	r.rows = r.rows[1:]
	return row
}

// Iter is ``iter(self.fetchall())``.
func (r *SqlResult_) Iter() []any {
	rows := r.Fetchall()
	out := make([]any, len(rows))
	for i, row := range rows {
		out[i] = py.Tuple(row)
	}
	return out
}

// LockedConnection_ serializes every use of one sqlite connection behind
// an RLock.
//
// The index connection may be used from any goroutine (taskforce workers
// included), but cursors and the statement cache are not safe for
// concurrent use: two threads running the same statement text could reset
// each other's cursor and read truncated / duplicated rows. Every
// statement here runs *and* fetches under the lock, so callers get a
// complete, consistent result regardless of thread.
type LockedConnection_ struct {
	Conn_ *py.Sqlite3Connection
	Lock  *py.RLock
}

// New_LockedConnection_ is ``_LockedConnection(conn)``.
func New_LockedConnection_(conn *py.Sqlite3Connection) *LockedConnection_ {
	return &LockedConnection_{Conn_: conn, Lock: py.NewRLock()}
}

// Execute runs sql with params and fetches every row under the lock.
func (c *LockedConnection_) Execute(sql string, params ...any) (*SqlResult_, error) {
	c.Lock.Lock_()
	defer c.Lock.Release()
	cursor, err := c.Conn_.Execute(sql, params...)
	if err != nil {
		return nil, err
	}
	return &SqlResult_{rows: cursor.Fetchall()}, nil
}

// Executemany runs sql once per parameter row under the lock.
func (c *LockedConnection_) Executemany(sql string, seq_of_params [][]any) error {
	c.Lock.Lock_()
	defer c.Lock.Release()
	return c.Conn_.Executemany(sql, seq_of_params)
}

// Commit commits under the lock.
func (c *LockedConnection_) Commit() error {
	c.Lock.Lock_()
	defer c.Lock.Release()
	return c.Conn_.Commit()
}

// Close closes the connection under the lock.
func (c *LockedConnection_) Close() error {
	c.Lock.Lock_()
	defer c.Lock.Release()
	return c.Conn_.Close()
}

// SqlState_ is the in-memory handle for a manifest's non-memorizing SQL
// index.
//
// Holds the owned sqlite connection plus the data-side metadata needed to
// map query results back to blueprint keys. Stored on the manifest as a
// private attribute so it never affects the manifest's on-the-wire
// serialization.
type SqlState_ struct {
	Conn          *LockedConnection_
	Db_path       string
	Is_persistent bool
	Table_name    string
	Columns       []string
	Indexed       *py.Set
	Row_keys      []any
	Stale         bool
}

// Sql_row_ is one ``(row_key, row_dict)`` pair returned by Sql_rows_.
type Sql_row_ struct {
	Key any
	Row *py.Dict
}

// Manifest_hooks are the subclass customisation points of the SQL index
// (``_sql_rows`` / ``_sql_project``).
type Manifest_hooks interface {
	Sql_rows_() ([]Sql_row_, *py.Set, error)
	Sql_project_(matched_keys []any, select_items []string) (any, error)
}

// Manifest is the Entry subclass wrapping a nested dict of ``global_id``
// references.
//
// A manifest maps user-defined string keys to ``global_id`` strings,
// lists of ``global_id`` strings, or recursively nested dicts following
// the same rules. It can be constructed from raw ID strings or from Entry
// objects.
//
// The blueprint *is* the manifest's payload, so ``manifest.data`` returns
// the nested mapping of ``global_id`` strings. Realized synchronously
// fetches every referenced entry via the active policy's central memory
// and returns a nested dict of Entry objects mirroring the blueprint
// structure.
//
// SQL filtering: ``manifest.sql("SELECT ... FROM ... WHERE ...")`` filters
// the blueprint via a small sqlite-backed index built on demand. This
// index is a *non-memorizing algorithmic helper*: a lightweight,
// temporary, query-side convenience that the manifest owns directly and
// that deliberately bypasses ``central.memory`` (it is never memorized,
// never routed, and never travels with the manifest). Subclasses can
// customise it by overriding Sql_rows_ and Sql_project_.
//
// Constructor keywords: ``data`` (a nested dict whose leaves are
// ``global_id`` strings, lists of them, or Entry instances -- entries are
// converted to a blueprint of gids and stashed for a subsequent
// Memorize), ``blueprint`` (alias of ``data``), ``uuid``, ``nickname``,
// ``global_id``.
type Manifest struct {
	Entry
	Pending_entries_ *py.List      `py:"_pending_entries"`
	Nickname_        *string       `py:"_nickname"`
	Sql_state_       *SqlState_    `py:"_sql_state"`
	Sql_finalizer_   *py.Finalizer `py:"_sql_finalizer"`
}

// Manifest_class is the class descriptor (``_DEFAULT_SCOPES =
// [_MANIFEST_SCOPE]``).
//
// ``_OWNS_CHILDREN`` is True on manifests whose leaves belong to them (a
// checkpoint's tensors): ``ComplexManifest.forget`` recurses into those; a
// plain manifest's leaves (a dataset) are shared and only dropped as
// entries.
var Manifest_class = py.NewClass("Manifest", (*Manifest)(nil), Entry_class).
	Set_classvar("_DEFAULT_SCOPES", []string{MANIFEST_SCOPE_}).
	Set_classvar("_OWNS_CHILDREN", false)

// New_Manifest is ``Manifest(**data)``.
func New_Manifest(kw ...py.Kw) (*Manifest, error) {
	m := Manifest_class.New().(*Manifest)
	if err := m.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return m, nil
}

// Manifest_base_ exposes the embedded Manifest to code holding a subclass.
func (m *Manifest) Manifest_base_() *Manifest { return m }

// As_manifest_ returns the embedded *Manifest of any Manifest-derived
// instance (nil when v is not one).
func As_manifest_(v any) *Manifest {
	if m, ok := v.(*Manifest); ok {
		return m
	}
	if b, ok := v.(interface{ Manifest_base_() *Manifest }); ok {
		return b.Manifest_base_()
	}
	return nil
}

// hooks is the most-derived SQL customisation.
func (m *Manifest) hooks() Manifest_hooks { return m.Self().(Manifest_hooks) }

// Init_ is ``Manifest.__init__(**data)``: classify ``data`` / ``blueprint``
// (all entries or all strings, never a mix), derive the blueprint and the
// pending entries, then run ``Entry.__init__`` with ``evolution=None`` and
// the blueprint as a READY payload.
func (m *Manifest) Init_(data py.Kw) error {
	raw_data := data.Pop("data", nil)
	blueprint_kwarg := data.Pop("blueprint", nil)
	nickname := data.Get("nickname", nil)

	var blueprint any
	var pending *py.List

	source := raw_data
	if py.IsNone(source) {
		source = blueprint_kwarg
	}

	if !py.IsNone(source) {
		has_entries, has_strings, err := Manifest_classify_data_(source)
		if err != nil {
			return err
		}
		if has_entries && has_strings {
			return py.ValueError.New(
				"Manifest data must contain either all Entry objects or all " +
					"global_id strings, not a mix of both.")
		}
		if has_entries {
			bp, err := Manifest_extract_blueprint_(source)
			if err != nil {
				return err
			}
			blueprint = bp
			ents, err := Manifest_iter_entries_impl_(source)
			if err != nil {
				return err
			}
			pending = py.NewList()
			for _, e := range ents {
				pending.Append(e.Self())
			}
		} else {
			if err := Manifest_validate_blueprint_(source); err != nil {
				return err
			}
			blueprint = Manifest_deepcopy_blueprint_(source)
		}
	}

	entry_kwargs := py.Kw{}
	for k, v := range data {
		entry_kwargs[k] = v
	}
	entry_kwargs["evolution"] = nil
	if blueprint != nil {
		entry_kwargs["data"] = blueprint
		if _, ok := entry_kwargs["state"]; !ok {
			entry_kwargs["state"] = EntryState_READY
		}
	}

	if err := m.Entry.Init_(entry_kwargs); err != nil {
		return err
	}
	m.Pending_entries_ = pending
	m.Nickname_ = nil
	if s, ok := nickname.(string); ok {
		m.Nickname_ = &s
	}
	return nil
}

// Nickname is the nickname this manifest was created with (nil when read
// back from a pool).
func (m *Manifest) Nickname() any {
	if m.Nickname_ == nil {
		return nil
	}
	return *m.Nickname_
}

// Manifest_is_manifest_gid is ``Manifest.is_manifest_gid(gid)``: true when
// *gid* names a manifest of any kind (``LAILA:MANIFEST[:...]:<uuid>``).
//
// Decided from the string alone -- the scopes are part of the gid -- so
// classifying a blueprint leaf costs no I/O.
func Manifest_is_manifest_gid(gid any) bool {
	s, ok := gid.(string)
	if !ok {
		return false
	}
	// Search attributes (``@evolution=-1``) are not part of the identity
	// and would be rejected by Process_global_id.
	head, _, err := Split_global_id_attributes(s)
	if err != nil {
		return false
	}
	processed, err := Process_global_id(head)
	if err != nil {
		return false
	}
	scopes, _ := processed["scopes"].([]string)
	return len(scopes) > 0 && scopes[0] == MANIFEST_SCOPE_
}

// Manifest_deepcopy_blueprint_ is ``copy.deepcopy(source)`` normalised to
// the port's *py.Dict / *py.List shapes.
func Manifest_deepcopy_blueprint_(source any) *py.Dict {
	d, _ := asDict(source)
	out := py.NewDict()
	for _, it := range d.Items() {
		out.Set(it.Key, manifest_copy_value_(it.Value))
	}
	return out
}

func manifest_copy_value_(val any) any {
	switch {
	case is_str_(val):
		return val
	case is_list_(val):
		return py.NewList(py.Iter(val)...)
	case is_mapping_(val):
		return Manifest_deepcopy_blueprint_(val)
	}
	return py.Deepcopy(val)
}

func is_str_(v any) bool { _, ok := v.(string); return ok }

func is_list_(v any) bool {
	switch v.(type) {
	case *py.List, []any, []string:
		return true
	}
	return false
}

func is_mapping_(v any) bool {
	switch v.(type) {
	case *py.Dict, py.Kw, map[string]any:
		return true
	}
	return false
}

// ------------------------------------------------------------------
// Properties
// ------------------------------------------------------------------

// Set_data replaces the blueprint and marks any SQL index stale
// (``manifest.data = new_data``).
//
// Sql promises the index is reused *until the blueprint mutates*;
// re-assigning ``manifest.data`` is such a mutation, so the next query
// rebuilds from the new rows (same as Extend / ``+=``).
func (m *Manifest) Set_data(new_data any) error {
	if err := m.Entry.Set_data(new_data); err != nil {
		return err
	}
	// ``from_dict`` builds via ``cls.__new__`` and sets ``data`` before
	// private attributes exist.
	if m.Bound() {
		m.Invalidate_index()
	}
	return nil
}

// Blueprint is the nested dict with ``global_id`` strings as leaf values.
// Equivalent to ``manifest.data``.
func (m *Manifest) Blueprint() (any, error) { return m.Self().(interface{ Data() (any, error) }).Data() }

// blueprint_dict_ is ``self.data`` as a *py.Dict (nil when empty).
func (m *Manifest) blueprint_dict_() (*py.Dict, error) {
	bp, err := m.Self().(interface{ Data() (any, error) }).Data()
	if err != nil {
		return nil, err
	}
	if py.IsNone(bp) {
		return nil, nil
	}
	d, ok := asDict(bp)
	if !ok {
		return nil, py.TypeError.Newf("Manifest blueprint must be a dict, got %s", py.Pytypename(bp))
	}
	return d, nil
}

// Realized synchronously fetches all referenced entries and returns them.
//
// Blocks until every leaf ``global_id`` has been materialised via the
// active policy's central memory. Returns a nested dict of Entry objects
// mirroring the blueprint structure. No caching -- each access
// re-fetches.
//
// RuntimeError if the manifest has no blueprint; KeyError if any
// referenced entry is missing from the routed pool.
func (m *Manifest) Realized() (any, error) {
	// Virtual dispatch: a _RealizedManifest (process-pool worker) answers
	// from its pre-built entries even when reached through the embedded
	// Manifest.
	if rm, ok := m.Self().(*RealizedManifest_); ok && &rm.Manifest == m {
		return rm.Realized()
	}
	bp, err := m.blueprint_dict_()
	if err != nil {
		return nil, err
	}
	if bp == nil {
		return nil, py.RuntimeError.New("No blueprint to resolve — manifest is empty.")
	}

	known, err := m.preresolved_()
	if err != nil {
		return nil, err
	}
	all_gids := m.unresolved_gids_(known)
	if len(all_gids) == 0 {
		return Manifest_rebuild_with_entries_(bp, known)
	}

	policy, err := Get_active_policy()
	if err != nil {
		return nil, err
	}
	memory := getattr(getattr(policy, "central"), "memory")
	mem := As_central_memory_(memory)
	if mem == nil {
		return nil, py.AttributeError.New("'NoneType' object has no attribute '_read_entries_direct'")
	}

	read := func(pool any) ([]any, error) {
		// Direct-await resolver: one task on the internal taskforce reads
		// every child concurrently, instead of one future per child.
		ref, err := mem.Read_entries_direct_(all_gids, py.Kw{"pool": pool})
		if err != nil {
			return nil, err
		}
		// The manifest owns this future; release it from the bank once
		// consumed so a large realized() does not pin its children.
		defer func() { _, _ = callm(ref, "release") }()
		waited, err := callm(ref, "wait", nil)
		if err != nil {
			return nil, err
		}
		data, err := py.Getattr(waited, "data")
		if err != nil {
			return nil, err
		}
		return py.Iter(data), nil
	}

	var results []any
	var last_exc error
	found := false
	for _, pool := range m.Candidate_pools_(memory) {
		res, err := read(pool)
		if err != nil {
			if errors.Is(err, py.KeyError) {
				last_exc = err
				continue
			}
			return nil, err
		}
		results = res
		found = true
		break
	}
	if !found {
		return nil, py.KeyError.New(
			"Manifest.realized: one or more referenced entries failed to resolve.").From(last_exc)
	}

	if len(results) != len(all_gids) || any_none_(results) {
		return nil, py.KeyError.New("Manifest.realized: one or more referenced entries failed to resolve.")
	}

	resolved_map := known.Copy()
	for i, gid := range all_gids {
		resolved_map.Set(gid, results[i])
	}
	return Manifest_rebuild_with_entries_(bp, resolved_map)
}

func any_none_(xs []any) bool {
	for _, x := range xs {
		if py.IsNone(x) {
			return true
		}
	}
	return false
}

// Preresolved_ is ``{gid: object}`` for leaves that are already in hand
// (never re-fetched).
//
// A plain manifest holds nothing; a ComplexManifest contributes its
// resolved / assigned members.
func (m *Manifest) Preresolved_() (*py.Dict, error) { return py.NewDict(), nil }

// preresolved_ is the virtual ``self._preresolved()``.
func (m *Manifest) preresolved_() (*py.Dict, error) {
	return m.Self().(interface{ Preresolved_() (*py.Dict, error) }).Preresolved_()
}

// unresolved_gids_ is ``[gid for gid in self if gid not in known]``.
func (m *Manifest) unresolved_gids_(known *py.Dict) []any {
	out := []any{}
	for _, gid := range m.Iter() {
		if !known.Contains(gid) {
			out = append(out, gid)
		}
	}
	return out
}

// Candidate_pools_ lists the pools to look for referenced entries in,
// most likely first:
//
//  1. the pool this manifest was last read from / written to
//     (``_source_pool_gid``), when known;
//  2. the alpha pool (nil -- the historical behaviour);
//  3. every other registered pool, so a manifest whose children live in
//     a different named pool still resolves (bounded by the number of
//     pools).
func (m *Manifest) Candidate_pools_(memory any) []any {
	var hint any
	if gid := m.Source_pool_gid(); gid != nil {
		hint = *gid
	}
	router := getattr(memory, "pool_router")
	known := []string{}
	if !py.IsNone(router) {
		for _, it := range mappingItems(getattr(router, "pools")) {
			known = append(known, py.Str(it.Key))
		}
	}
	alpha := getattr(memory, "alpha_pool")
	order := []any{}
	in_known := func(h any) bool {
		for _, k := range known {
			if py.Eq(k, h) {
				return true
			}
		}
		return false
	}
	if !py.IsNone(hint) && in_known(hint) {
		order = append(order, hint)
	}
	if !py.Eq(hint, alpha) {
		order = append(order, nil)
	}
	for _, gid := range known {
		if !py.Eq(gid, hint) && !py.Eq(gid, alpha) {
			order = append(order, gid)
		}
	}
	return order
}

// Async_realized is the awaitable that asynchronously resolves all
// referenced entries.
//
// Returns a coroutine so callers can ``await manifest.async_realized``
// inside an async context (e.g. ``async with laila.guarantee_async``).
// Mirrors the semantics of Realized but awaits the underlying read
// instead of blocking.
//
// RuntimeError if the manifest has no blueprint; KeyError if any
// referenced entry is missing from the routed pool.
func (m *Manifest) Async_realized() py.Coroutine {
	if rm, ok := m.Self().(*RealizedManifest_); ok && &rm.Manifest == m {
		return rm.Async_realized()
	}
	return func() (any, error) {
		bp, err := m.blueprint_dict_()
		if err != nil {
			return nil, err
		}
		if bp == nil {
			return nil, py.RuntimeError.New("No blueprint to resolve — manifest is empty.")
		}

		known, err := m.preresolved_()
		if err != nil {
			return nil, err
		}
		all_gids := m.unresolved_gids_(known)
		if len(all_gids) == 0 {
			return Manifest_rebuild_with_entries_(bp, known)
		}

		policy, err := Get_active_policy()
		if err != nil {
			return nil, err
		}
		memory := getattr(getattr(policy, "central"), "memory")
		mem := As_central_memory_(memory)
		if mem == nil {
			return nil, py.AttributeError.New("'NoneType' object has no attribute '_read_entries_async'")
		}
		on_loop := CURRENT_SLOT_.Value() != nil

		read := func(pool any) ([]any, error) {
			if on_loop {
				// Already running on a taskforce loop: await the pool
				// directly -- zero futures for the whole batch.
				lst, err := mem.Read_entries_async_(all_gids, py.Kw{"pool": pool})
				if err != nil {
					return nil, err
				}
				return lst.Items(), nil
			}
			// Foreign loop (user's asyncio.run): keep pool I/O on the
			// taskforce loops via a single submitted task.
			ref, err := mem.Read_entries_direct_(all_gids, py.Kw{"pool": pool})
			if err != nil {
				return nil, err
			}
			defer func() { _, _ = callm(ref, "release") }()
			awaited, err := py.Await(ref)
			if err != nil {
				return nil, err
			}
			data, err := py.Getattr(awaited, "data")
			if err != nil {
				return nil, err
			}
			return py.Iter(data), nil
		}

		var results []any
		var last_exc error
		found := false
		for _, pool := range m.Candidate_pools_(memory) {
			res, err := read(pool)
			if err != nil {
				if errors.Is(err, py.KeyError) {
					last_exc = err
					continue
				}
				return nil, err
			}
			results = res
			found = true
			break
		}
		if !found {
			return nil, py.KeyError.New(
				"Manifest.async_realized: one or more referenced entries failed to resolve.").From(last_exc)
		}

		if len(results) != len(all_gids) || any_none_(results) {
			return nil, py.KeyError.New(
				"Manifest.async_realized: one or more referenced entries failed to resolve.")
		}

		resolved_map := known.Copy()
		for i, gid := range all_gids {
			resolved_map.Set(gid, results[i])
		}
		return Manifest_rebuild_with_entries_(bp, resolved_map)
	}
}

// ------------------------------------------------------------------
// Core operations (all return GroupFuture)
// ------------------------------------------------------------------

// Memorize uploads all referenced entries and stores the manifest itself
// (``memorize(*, pool=None, pool_nickname=None, pool_id=None,
// batch_size=128)``).
//
// Collects any pending Entry objects provided at construction time,
// uploads them in batches, then stores the manifest itself (whose payload
// is the blueprint dict).
//
// ``laila.memorize(manifest, dst_pool=...)`` dispatches here, so the two
// spellings are equivalent.
//
//   - pool: destination pool -- a live pool, its ``global_id`` or a
//     registered nickname (same forms as ``laila.memorize(dst_pool=)``).
//   - pool_nickname, pool_id: back-compat aliases for *pool*.
//   - batch_size (default 128): number of pending entries per underlying
//     ``laila.memorize`` call.
//   - context: the sibling members of the ComplexManifest this manifest
//     is being memorized from (``{key: live object}``). Ignored by a
//     plain manifest; typed manifests use it to reference their siblings
//     (an optimizer names the model whose parameters it drives).
func (m *Manifest) Memorize(kw ...py.Kw) (*GroupFuture, error) {
	opts := py.MergeKw(kw...)
	bp, err := m.blueprint_dict_()
	if err != nil {
		return nil, err
	}
	if bp == nil {
		return nil, py.RuntimeError.New("Nothing to memorize — manifest has no blueprint.")
	}
	batch_size, err := manifest_batch_size_(opts)
	if err != nil {
		return nil, err
	}

	routed := Manifest_resolve_pool_(opts.Get("pool", nil), opts.Get("pool_nickname", nil), opts.Get("pool_id", nil))
	all_future_ids := []string{}
	policy, err := Get_active_policy()
	if err != nil {
		return nil, err
	}
	_ = attempt(func() error {
		// Children and the manifest itself land in ``routed``: remember it
		// so Realized looks there first.
		if py.IsNone(routed) {
			m.Set_source_pool_gid(nil)
			return nil
		}
		mem := As_central_memory_(getattr(getattr(policy, "central"), "memory"))
		resolved, err := mem.Resolve_pool_ref_(routed)
		if err != nil {
			return err
		}
		if py.IsNone(resolved) {
			m.Set_source_pool_gid(nil)
		} else {
			gid := global_id_of_(resolved)
			m.Set_source_pool_gid(&gid)
		}
		return nil
	})

	if m.Pending_entries_ != nil && m.Pending_entries_.Len() > 0 {
		items := m.Pending_entries_.Items()
		for i := 0; i < len(items); i += batch_size {
			end := min(i+batch_size, len(items))
			batch := py.NewList(items[i:end]...)
			ref, err := Memorize(batch, py.Kw{"dst_pool": routed})
			if err != nil {
				return nil, err
			}
			ids, err := Manifest_collect_future_ids_(ref)
			if err != nil {
				return nil, err
			}
			all_future_ids = append(all_future_ids, ids...)
		}
		m.Pending_entries_ = nil
	}

	// Wrapped in a list on purpose: a *bare* Manifest handed to
	// ``laila.memorize`` dispatches back to this method, whereas a list
	// stores the manifest as a plain entry (blueprint payload).
	self_ref, err := Memorize(py.NewList(m.Self()), py.Kw{"dst_pool": routed})
	if err != nil {
		return nil, err
	}
	ids, err := Manifest_collect_future_ids_(self_ref)
	if err != nil {
		return nil, err
	}
	all_future_ids = append(all_future_ids, ids...)

	return New_GroupFuture(py.Kw{
		"taskforce_id": getattr(getattr(getattr(policy, "central"), "command"), "internal_taskforce"),
		"policy_id":    global_id_of_(policy),
		"future_ids":   all_future_ids,
	})
}

// manifest_batch_size_ reads ``batch_size`` (default 128) as a positive int.
func manifest_batch_size_(opts py.Kw) (int, error) {
	n, err := py.Int(opts.Get("batch_size", 128))
	if err != nil {
		return 0, err
	}
	if n <= 0 {
		return 0, py.ValueError.New("range() arg 3 must not be zero")
	}
	return n, nil
}

// Remember recalls all referenced entries from the pool
// (``remember(*, pool=None, pool_nickname=None, pool_id=None,
// batch_size=128, persist=True)``).
//
// ``laila.remember(manifest, dst_pool=...)`` dispatches here. The returned
// GroupFuture's ``.data`` is the flat list of entries in ``list(manifest)``
// order.
//
//   - pool: source pool (live pool, ``global_id`` or nickname).
//   - pool_nickname, pool_id: back-compat aliases for *pool*.
//   - batch_size (default 128): number of gids per underlying
//     ``laila.remember`` call.
//   - persist (default True): forwarded to ``laila.remember`` -- cache
//     fetched entries into the alpha pool.
func (m *Manifest) Remember(kw ...py.Kw) (*GroupFuture, error) {
	opts := py.MergeKw(kw...)
	bp, err := m.blueprint_dict_()
	if err != nil {
		return nil, err
	}
	if bp == nil {
		return nil, py.RuntimeError.New("No blueprint to resolve — manifest is empty.")
	}
	batch_size, err := manifest_batch_size_(opts)
	if err != nil {
		return nil, err
	}
	persist := py.Truthy(opts.Get("persist", true))

	routed := Manifest_resolve_pool_(opts.Get("pool", nil), opts.Get("pool_nickname", nil), opts.Get("pool_id", nil))
	all_gids := m.Iter()
	all_future_ids := []string{}
	policy, err := Get_active_policy()
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(all_gids); i += batch_size {
		end := min(i+batch_size, len(all_gids))
		batch := py.NewList(all_gids[i:end]...)
		ref, err := Remember(batch, py.Kw{"dst_pool": routed, "persist": persist})
		if err != nil {
			return nil, err
		}
		ids, err := Manifest_collect_future_ids_(ref)
		if err != nil {
			return nil, err
		}
		all_future_ids = append(all_future_ids, ids...)
	}

	return New_GroupFuture(py.Kw{
		"taskforce_id": getattr(getattr(getattr(policy, "central"), "command"), "internal_taskforce"),
		"policy_id":    global_id_of_(policy),
		"future_ids":   all_future_ids,
	})
}

// Forget deletes all referenced entries and the manifest itself from the
// pool (``forget(*, pool=None, pool_nickname=None, pool_id=None,
// batch_size=128)``).
//
// ``laila.forget(manifest, pool=...)`` dispatches here.
//
//   - pool: pool to delete from (live pool, ``global_id`` or nickname).
//   - pool_nickname, pool_id: back-compat aliases for *pool*.
//   - batch_size (default 128): number of gids per underlying
//     ``laila.forget`` call.
func (m *Manifest) Forget(kw ...py.Kw) (*GroupFuture, error) {
	opts := py.MergeKw(kw...)
	bp, err := m.blueprint_dict_()
	if err != nil {
		return nil, err
	}
	if bp == nil {
		return nil, py.RuntimeError.New("No blueprint — nothing to forget.")
	}
	batch_size, err := manifest_batch_size_(opts)
	if err != nil {
		return nil, err
	}

	routed := Manifest_resolve_pool_(opts.Get("pool", nil), opts.Get("pool_nickname", nil), opts.Get("pool_id", nil))
	all_gids := m.Iter()
	all_future_ids := []string{}
	policy, err := Get_active_policy()
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(all_gids); i += batch_size {
		end := min(i+batch_size, len(all_gids))
		batch := py.NewList(all_gids[i:end]...)
		ref, err := Forget(batch, py.Kw{"pool": routed})
		if err != nil {
			return nil, err
		}
		ids, err := Manifest_collect_future_ids_(ref)
		if err != nil {
			return nil, err
		}
		all_future_ids = append(all_future_ids, ids...)
	}

	self_ref, err := Forget(m.Global_id(), py.Kw{"pool": routed})
	if err != nil {
		return nil, err
	}
	ids, err := Manifest_collect_future_ids_(self_ref)
	if err != nil {
		return nil, err
	}
	all_future_ids = append(all_future_ids, ids...)

	return New_GroupFuture(py.Kw{
		"taskforce_id": getattr(getattr(getattr(policy, "central"), "command"), "internal_taskforce"),
		"policy_id":    global_id_of_(policy),
		"future_ids":   all_future_ids,
	})
}

// ------------------------------------------------------------------
// SQL index  (non-memorizing algorithmic help)
// ------------------------------------------------------------------

// Sql filters the manifest with ``SELECT ... FROM ... [WHERE ...]``.
//
// A small, regex-based SQL surface over a *non-memorizing* sqlite index
// that this manifest owns directly (it bypasses ``central.memory``
// entirely -- it is never memorized and never leaves the local machine).
// The index is built lazily on first use and reused until
// Invalidate_index / Clear_index is called or the blueprint mutates.
//
// Supported surface:
//
//   - ``SELECT <items> FROM <name> [WHERE <predicate>]`` only.
//   - ``ORDER BY`` / ``LIMIT`` / ``GROUP BY`` / ``HAVING`` / ``JOIN`` /
//     ``UNION`` are rejected up front with a ValueError.
//   - ``==`` is normalized to ``=`` so casual queries work.
//   - The table name in ``FROM`` is treated as an alias and ignored (any
//     identifier is accepted).
//   - ``SELECT`` items may be ``*``, ``<name>``, or ``<alias>.<name>``;
//     the alias prefix is stripped. Items beyond ``*`` are ignored by the
//     default projection.
//   - String literals in ``WHERE`` must be single-quoted
//     (``WHERE owner = 'alice'``). A bareword right-hand side is
//     interpreted by sqlite as a column name; if it can't be resolved the
//     resulting error is re-raised as a ValueError suggesting single
//     quotes.
//
// Returns a fresh, same-type manifest containing the matched rows. Sql
// never mutates the receiver in place.
func (m *Manifest) Sql(query any) (any, error) {
	select_items, _, where, err := Manifest_parse_sql_(query)
	if err != nil {
		return nil, err
	}

	// The manifest's own (re-entrant) lock serializes build / rebuild /
	// query / clear across threads; the connection adds its own
	// statement-level lock underneath.
	var matched_keys []any
	if err := m.with_atomic_(func() error {
		if m.Sql_state_ == nil {
			if err := m.Build_index(); err != nil {
				return err
			}
		} else if m.Sql_state_.Stale {
			if err := m.Sql_rebuild_in_place_(py.Kw{"widen": true}); err != nil {
				return err
			}
		}

		state := m.Sql_state_
		sql_text := fmt.Sprintf(`SELECT "%s" FROM "%s"`, SQL_ROW_IDX_COL_, state.Table_name)
		if where != nil {
			sql_text += " WHERE " + *where
		}

		cursor, err := state.Conn.Execute(sql_text)
		if err != nil {
			if errors.Is(err, py.OperationalError) {
				message := err.Error()
				if strings.Contains(message, "no such column") {
					return py.ValueError.Newf(
						"%s. If you meant a string literal, wrap it in single "+
							"quotes (e.g. WHERE owner = 'alice'); unquoted barewords are "+
							"treated as column names by SQL.", message).From(err)
				}
			}
			return err
		}
		matched_keys = []any{}
		for _, row := range cursor.Fetchall() {
			idx, err := py.Int(row[0])
			if err != nil {
				return err
			}
			matched_keys = append(matched_keys, state.Row_keys[idx])
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return m.hooks().Sql_project_(matched_keys, select_items)
}

// with_atomic_ runs fn under ``with self.atomic():``.
func (m *Manifest) with_atomic_(fn func() error) error {
	_, release, err := m.Atomic()
	if err != nil {
		return err
	}
	defer release()
	return fn()
}

// Build_index materializes the manifest into a sqlite table + indexes
// (``build_index(*, on=(), composite=(), persist=None, widen=True)``).
//
// Idempotent: a subsequent call is a no-op while the cached index is
// fresh, and triggers an in-place rebuild if the index was invalidated.
// Subsequent Sql calls reuse the same connection until Invalidate_index
// is called or the blueprint mutates.
//
// ``persist``, if provided, is the path to a file-backed sqlite db that
// survives process restarts (and is attached without a rebuild when it
// already holds a matching table); otherwise the index lives under
// ``<laila_root>/indices/<manifest-uuid>/``.
func (m *Manifest) Build_index(kw ...py.Kw) error {
	opts := py.MergeKw(kw...)
	return m.with_atomic_(func() error {
		if m.Sql_state_ != nil {
			if m.Sql_state_.Stale {
				return m.Sql_rebuild_in_place_(py.Kw{"widen": opts.Get("widen", true)})
			}
			return nil
		}
		return m.Sql_build_fresh_(py.Kw{
			"on":        opts.Get("on", nil),
			"composite": opts.Get("composite", nil),
			"persist":   opts.Get("persist", nil),
		})
	})
}

// Invalidate_index marks the cached index stale without closing the
// connection.
//
// Cheap: the next Sql does an in-place rebuild (``DELETE FROM ...`` then
// re-INSERT, indexes preserved). Called automatically by Extend / ``+=``;
// subclasses that mutate their own blueprint should call this after
// writing.
func (m *Manifest) Invalidate_index() {
	if m.Sql_state_ != nil {
		m.Sql_state_.Stale = true
	}
}

// Clear_index closes the sqlite connection and drops the cached index
// (``clear_index(*, remove_persisted=False)``).
//
// A non-persistent (temporary) index file is disposable and is always
// unlinked here. A user-chosen ``persist=`` file is left in place unless
// ``remove_persisted`` is true. After Clear_index the next Sql rebuilds
// from scratch.
func (m *Manifest) Clear_index(kw ...py.Kw) error {
	remove_persisted := py.Truthy(py.MergeKw(kw...).Get("remove_persisted", false))
	return m.with_atomic_(func() error {
		finalizer := m.Sql_finalizer_
		if finalizer != nil {
			finalizer.Detach()
			m.Sql_finalizer_ = nil
		}

		state := m.Sql_state_
		if state == nil {
			return nil
		}
		_ = state.Conn.Close()
		if !state.Is_persistent || remove_persisted {
			Manifest_sql_unlink_db_(state.Db_path)
		}
		m.Sql_state_ = nil
		return nil
	})
}

// ----- Subclass hooks --------------------------------------------

// Sql_rows_ returns ``(rows, columns)`` for the SQL index.
//
// Each row is ``(row_key, row_dict)``. ``row_key`` uniquely addresses the
// row inside the blueprint (any hashable; tuples allowed). ``row_dict`` is
// the flat column -> value map used for filtering. ``columns`` is the
// union of queryable keys.
//
// Default implementation: one row per top-level key, using the value as
// the row dict if it is a dict, otherwise wrapping it as ``{"value":
// <scalar>}``. Columns are the union of inner dict keys (or ``{"value"}``
// for scalar entries).
//
// IMPORTANT: ``row_dict`` values must be primitive scalars (str, int,
// float, bool, bytes, None). LAILA_IDENTIFIABLE_OBJECT_ instances and
// non-primitive containers are rejected at Build_index time. If your
// blueprint references Entries, pass ``.global_id`` (a str) instead; if it
// contains lists/dicts, flatten them in your override (e.g.
// ``",".join(tags)`` or ``json.dumps(meta)``).
//
// The row order must be deterministic for a given blueprint so a
// reattached on-disk index can remap rows back to keys without a
// rebuild.
func (m *Manifest) Sql_rows_() ([]Sql_row_, *py.Set, error) {
	blueprint, err := m.blueprint_dict_()
	if err != nil {
		return nil, nil, err
	}
	if blueprint == nil {
		blueprint = py.NewDict()
	}
	rows := []Sql_row_{}
	columns := py.NewSet()
	for _, it := range blueprint.Items() {
		var row *py.Dict
		if d, ok := asDict(it.Value); ok {
			row = d.Copy()
		} else {
			row = py.DictOf("value", it.Value)
		}
		rows = append(rows, Sql_row_{Key: it.Key, Row: row})
		columns.Update(row.Keys())
	}
	return rows, columns, nil
}

// Sql_project_ builds a new same-type manifest from matched row keys.
//
// Default implementation keeps the top-level entries whose key appears in
// ``matched_keys`` and ignores ``SELECT`` items beyond ``*``.
func (m *Manifest) Sql_project_(matched_keys []any, select_items []string) (any, error) {
	blueprint, err := m.blueprint_dict_()
	if err != nil {
		return nil, err
	}
	if blueprint == nil {
		blueprint = py.NewDict()
	}
	subset := py.NewDict()
	for _, k := range matched_keys {
		if v, ok := blueprint.Get(k); ok {
			subset.Set(k, manifest_copy_value_(v))
		}
	}
	return New_manifest_of_(m.Class_(), py.Kw{"blueprint": subset})
}

// New_manifest_of_ is ``type(self)(**kw)`` for Manifest and its subclasses.
func New_manifest_of_(cls *py.Class, kw py.Kw) (any, error) {
	inst := cls.New()
	if err := inst.(interface{ Init_(py.Kw) error }).Init_(kw); err != nil {
		return nil, err
	}
	return inst, nil
}

// ----- SQL parser helpers ----------------------------------------

var (
	sql_select_re_ = regexp.MustCompile(`(?is)^\s*SELECT\s+(.+?)\s+FROM\s+` +
		"([`\"']?[A-Za-z_][A-Za-z0-9_]*[`\"']?)\\s*" +
		`(?:WHERE\s+(.+))?$`)
)

// Manifest_parse_sql_ parses a tiny ``SELECT ... FROM ... [WHERE ...]``
// query.
//
// Strips a trailing ``;``, normalizes ``==`` to ``=``, rejects forbidden
// clauses, and returns ``(select_items, from_alias, where_clause_or_nil)``.
func Manifest_parse_sql_(query any) ([]string, string, *string, error) {
	q, ok := query.(string)
	if !ok {
		return nil, "", nil, py.ValueError.New("sql() query must be a string.")
	}

	normalized := strings.TrimSpace(strings.TrimRight(strings.TrimSpace(q), ";"))
	normalized = strings.ReplaceAll(normalized, "==", "=")
	upper := strings.ToUpper(normalized)
	for _, clause := range FORBIDDEN_SQL_ {
		pattern := regexp.MustCompile(`\b` + strings.ReplaceAll(clause, " ", `\s+`) + `\b`)
		if pattern.MatchString(upper) {
			return nil, "", nil, py.ValueError.Newf(
				"Unsupported SQL clause: %s. Only "+
					"'SELECT <items> FROM <name> [WHERE <predicate>]' is supported.", clause)
		}
	}

	match := sql_select_re_.FindStringSubmatch(normalized)
	if match == nil {
		return nil, "", nil, py.ValueError.New(
			"Only 'SELECT <items> FROM <name> [WHERE <predicate>]' is supported.")
	}

	select_items := []string{}
	for _, item := range strings.Split(match[1], ",") {
		select_items = append(select_items, Manifest_strip_table_prefix_(strings.TrimSpace(item)))
	}
	from_alias := strings.Trim(match[2], "`\"'")
	var where *string
	if match[3] != "" {
		w := strings.TrimSpace(match[3])
		where = &w
	}
	return select_items, from_alias, where, nil
}

// Manifest_strip_table_prefix_ drops an ``alias.`` prefix and surrounding
// quotes from a SELECT item.
func Manifest_strip_table_prefix_(item string) string {
	stripped := strings.Trim(strings.TrimSpace(item), "`\"")
	if stripped == "*" {
		return stripped
	}
	if strings.Contains(stripped, ".") {
		stripped = strings.SplitN(stripped, ".", 2)[1]
	}
	return strings.Trim(stripped, "`\"")
}

// Manifest_validate_primitive_rows_ rejects non-primitive row values
// before any sqlite state exists.
func Manifest_validate_primitive_rows_(rows []Sql_row_) error {
	for _, r := range rows {
		for _, it := range r.Row.Items() {
			if py.Isinstance(it.Value, LAILA_IDENTIFIABLE_OBJECT_class) {
				return py.TypeError.Newf(
					"Cannot index column %s: row %s has value "+
						"of type %s (Laila identifiable object). "+
						"Index columns must be primitive scalars "+
						"(str, int, float, bool, bytes, None). "+
						"Pass `.global_id` instead.",
					py.Repr(it.Key), py.Repr(r.Key), py.Pytypename(it.Value))
			}
			if !Is_primitive_sql_value_(it.Value) {
				return py.TypeError.Newf(
					"Cannot index column %s: row %s has value "+
						"of type %s. Index columns must be "+
						"primitive scalars. Flatten lists/dicts in your "+
						"_sql_rows() override (e.g. ','.join(tags) or "+
						"json.dumps(meta)).",
					py.Repr(it.Key), py.Repr(r.Key), py.Pytypename(it.Value))
			}
		}
	}
	return nil
}

// ----- SQL index internals ---------------------------------------

// Index_db_path_ is the default on-disk path for this manifest's index
// file.
//
// One file per *instance* (``<pid>-<random>.laila_sqlitedb``) inside the
// per-manifest ``indices/<uuid>/`` directory: two live manifests that
// share a uuid (same nickname, or the same gid loaded twice) must never
// attach to each other's temporary table.
func (m *Manifest) Index_db_path_() (string, error) {
	base := filepath.Join(LAILA_DEFAULT_DIRECTORIES["indices"], m.Uuid())
	if err := py.Mkdir_p(base); err != nil {
		return "", err
	}
	return filepath.Join(base, fmt.Sprintf("%d-%s.laila_sqlitedb", py.Getpid(), py.Uuid4().Hex()[:8])), nil
}

// Sql_build_fresh_ builds the index from scratch (or attaches a matching
// on-disk file) (``_sql_build_fresh(*, on, composite, persist)``).
func (m *Manifest) Sql_build_fresh_(kw ...py.Kw) error {
	opts := py.MergeKw(kw...)
	rows, column_set, err := m.hooks().Sql_rows_()
	if err != nil {
		return err
	}
	if err := Manifest_validate_primitive_rows_(rows); err != nil {
		return err
	}
	columns := sorted_str_set_(column_set)
	row_keys := make([]any, len(rows))
	for i, r := range rows {
		row_keys[i] = r.Key
	}

	persist := opts.Get("persist", nil)
	is_persistent := !py.IsNone(persist)
	var db_path string
	if is_persistent {
		db_path = py.Expanduser(py.Str(persist))
		if parent := filepath.Dir(db_path); parent != "" && parent != "." {
			if err := py.Mkdir_p(parent); err != nil {
				return err
			}
		}
	} else {
		db_path, err = m.Index_db_path_()
		if err != nil {
			return err
		}
	}

	raw_conn, err := py.Sqlite3_connect(db_path)
	if err != nil {
		return err
	}
	conn := New_LockedConnection_(raw_conn)
	fingerprint := Manifest_sql_fingerprint_(columns, rows)

	exists, err := Manifest_index_table_exists_(conn)
	if err != nil {
		return err
	}
	if exists {
		count, err := Manifest_index_row_count_(conn)
		if err != nil {
			return err
		}
		if count == len(rows) {
			existing_columns, err := Manifest_existing_columns_(conn)
			if err != nil {
				return err
			}
			stored_fp, err := Manifest_read_fingerprint_(conn)
			if err != nil {
				return err
			}
			if py.NewSet(stringsToAny(existing_columns)...).Eq(py.NewSet(stringsToAny(columns)...)) &&
				stored_fp != nil && *stored_fp == fingerprint {
				indexed, err := Manifest_existing_indexes_(conn)
				if err != nil {
					return err
				}
				m.Sql_state_ = &SqlState_{
					Conn:          conn,
					Db_path:       db_path,
					Is_persistent: is_persistent,
					Table_name:    SQL_INDEX_TABLE_,
					Columns:       existing_columns,
					Indexed:       indexed,
					Row_keys:      row_keys,
					Stale:         false,
				}
				m.Sql_register_gc_cleanup_()
				return nil
			}
		}
	}

	if err := Manifest_create_and_populate_(conn, columns, rows); err != nil {
		return err
	}
	if err := Manifest_write_fingerprint_(conn, fingerprint); err != nil {
		return err
	}
	indexed, err := Manifest_apply_indexes_(conn, opts.Get("on", nil), opts.Get("composite", nil))
	if err != nil {
		return err
	}
	m.Sql_state_ = &SqlState_{
		Conn:          conn,
		Db_path:       db_path,
		Is_persistent: is_persistent,
		Table_name:    SQL_INDEX_TABLE_,
		Columns:       columns,
		Indexed:       indexed,
		Row_keys:      row_keys,
		Stale:         false,
	}
	m.Sql_register_gc_cleanup_()
	return nil
}

// sorted_str_set_ is ``sorted(columns)`` for a set of column names.
func sorted_str_set_(s *py.Set) []string {
	out := make([]string, 0, s.Len())
	for _, c := range s.Items() {
		out = append(out, py.Str(c))
	}
	return py.Sorted_strings(out)
}

// Sql_rebuild_in_place_ re-materializes rows into the existing table,
// preserving indexes (``_sql_rebuild_in_place(*, widen=True)``).
func (m *Manifest) Sql_rebuild_in_place_(kw ...py.Kw) error {
	widen := py.Truthy(py.MergeKw(kw...).Get("widen", true))
	state := m.Sql_state_
	if state == nil {
		return nil
	}

	rows, column_set, err := m.hooks().Sql_rows_()
	if err != nil {
		return err
	}
	if err := Manifest_validate_primitive_rows_(rows); err != nil {
		return err
	}
	columns := sorted_str_set_(column_set)
	conn := state.Conn

	new_columns := []string{}
	for _, c := range columns {
		if !contains_str_(state.Columns, c) {
			new_columns = append(new_columns, c)
		}
	}
	if len(new_columns) > 0 {
		if !widen {
			return py.ValueError.Newf("Index rebuild needs new columns %s but widen=False.",
				py.Repr(py.NewList(stringsToAny(new_columns)...)))
		}
		for _, col := range new_columns {
			if _, err := conn.Execute(fmt.Sprintf(`ALTER TABLE "%s" ADD COLUMN "%s"`, state.Table_name, col)); err != nil {
				return err
			}
		}
	}

	all_columns := append(append([]string(nil), state.Columns...), new_columns...)
	if _, err := conn.Execute(fmt.Sprintf(`DELETE FROM "%s"`, state.Table_name)); err != nil {
		return err
	}
	if err := Manifest_insert_rows_(conn, state.Table_name, all_columns, rows); err != nil {
		return err
	}
	if err := Manifest_write_fingerprint_(conn, Manifest_sql_fingerprint_(columns, rows)); err != nil {
		return err
	}
	if err := conn.Commit(); err != nil {
		return err
	}

	state.Columns = all_columns
	state.Row_keys = make([]any, len(rows))
	for i, r := range rows {
		state.Row_keys[i] = r.Key
	}
	state.Stale = false
	return nil
}

func contains_str_(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// Manifest_sql_fingerprint_ is the content hash of what the index table
// holds (columns + rows).
//
// Rows are validated to be primitive scalars before this runs, so
// ``repr`` is a stable, order-preserving encoding.
func Manifest_sql_fingerprint_(columns []string, rows []Sql_row_) string {
	row_items := make([]any, len(rows))
	for i, r := range rows {
		row_items[i] = py.Tuple{r.Key, r.Row}
	}
	payload := py.Repr(py.Tuple{py.NewList(stringsToAny(columns)...), py.NewList(row_items...)})
	return py.Sha256_hex([]byte(payload))
}

// Manifest_write_fingerprint_ stores the fingerprint in the meta table.
func Manifest_write_fingerprint_(conn *LockedConnection_, fingerprint string) error {
	if _, err := conn.Execute(fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS "%s" (key TEXT PRIMARY KEY, value TEXT)`, SQL_META_TABLE_)); err != nil {
		return err
	}
	if _, err := conn.Execute(fmt.Sprintf(
		`INSERT OR REPLACE INTO "%s" (key, value) VALUES (?, ?)`, SQL_META_TABLE_),
		"fingerprint", fingerprint); err != nil {
		return err
	}
	return conn.Commit()
}

// Manifest_read_fingerprint_ reads the stored fingerprint (nil when the
// meta table or the row is missing).
func Manifest_read_fingerprint_(conn *LockedConnection_) (*string, error) {
	cursor, err := conn.Execute(
		"SELECT name FROM sqlite_master WHERE type='table' AND name=?", SQL_META_TABLE_)
	if err != nil {
		return nil, err
	}
	if cursor.Fetchone() == nil {
		return nil, nil
	}
	cursor, err = conn.Execute(fmt.Sprintf(`SELECT value FROM "%s" WHERE key = ?`, SQL_META_TABLE_), "fingerprint")
	if err != nil {
		return nil, err
	}
	row := cursor.Fetchone()
	if row == nil {
		return nil, nil
	}
	s := py.Str(row[0])
	return &s, nil
}

// Manifest_index_table_exists_ reports whether the index table exists.
func Manifest_index_table_exists_(conn *LockedConnection_) (bool, error) {
	cursor, err := conn.Execute(
		"SELECT name FROM sqlite_master WHERE type='table' AND name=?", SQL_INDEX_TABLE_)
	if err != nil {
		return false, err
	}
	return cursor.Fetchone() != nil, nil
}

// Manifest_index_row_count_ is ``SELECT COUNT(*)`` on the index table.
func Manifest_index_row_count_(conn *LockedConnection_) (int, error) {
	cursor, err := conn.Execute(fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, SQL_INDEX_TABLE_))
	if err != nil {
		return 0, err
	}
	row := cursor.Fetchone()
	if row == nil {
		return 0, py.TypeError.New("'NoneType' object is not subscriptable")
	}
	return py.Int(row[0])
}

// Manifest_existing_columns_ lists the data columns of the index table.
func Manifest_existing_columns_(conn *LockedConnection_) ([]string, error) {
	cursor, err := conn.Execute(fmt.Sprintf(`PRAGMA table_info("%s")`, SQL_INDEX_TABLE_))
	if err != nil {
		return nil, err
	}
	out := []string{}
	for _, r := range cursor.Fetchall() {
		name := py.Str(r[1])
		if name != SQL_ROW_IDX_COL_ {
			out = append(out, name)
		}
	}
	return out, nil
}

// Manifest_existing_indexes_ lists the index names on the index table.
func Manifest_existing_indexes_(conn *LockedConnection_) (*py.Set, error) {
	cursor, err := conn.Execute(fmt.Sprintf(`PRAGMA index_list("%s")`, SQL_INDEX_TABLE_))
	if err != nil {
		return nil, err
	}
	out := py.NewSet()
	for _, r := range cursor.Fetchall() {
		out.Add(r[1])
	}
	return out, nil
}

// Manifest_create_and_populate_ (re)creates the index table and inserts
// the rows.
func Manifest_create_and_populate_(conn *LockedConnection_, columns []string, rows []Sql_row_) error {
	for _, col := range columns {
		if err := Manifest_validate_ident_(col); err != nil {
			return err
		}
	}
	col_defs := ""
	for _, c := range columns {
		col_defs += fmt.Sprintf(`, "%s"`, c)
	}
	if _, err := conn.Execute(fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, SQL_INDEX_TABLE_)); err != nil {
		return err
	}
	if _, err := conn.Execute(fmt.Sprintf(
		`CREATE TABLE "%s" ("%s" INTEGER PRIMARY KEY%s)`, SQL_INDEX_TABLE_, SQL_ROW_IDX_COL_, col_defs)); err != nil {
		return err
	}
	if err := Manifest_insert_rows_(conn, SQL_INDEX_TABLE_, columns, rows); err != nil {
		return err
	}
	return conn.Commit()
}

// Manifest_insert_rows_ bulk-inserts rows (row index + column values).
func Manifest_insert_rows_(conn *LockedConnection_, table string, columns []string, rows []Sql_row_) error {
	var statement string
	data := [][]any{}
	if len(columns) > 0 {
		quoted := make([]string, len(columns))
		for i, c := range columns {
			quoted[i] = fmt.Sprintf(`"%s"`, c)
		}
		placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(columns)+1), ", ")
		statement = fmt.Sprintf(`INSERT INTO "%s" ("%s", %s) VALUES (%s)`,
			table, SQL_ROW_IDX_COL_, strings.Join(quoted, ", "), placeholders)
		for idx, r := range rows {
			params := []any{idx}
			for _, c := range columns {
				params = append(params, r.Row.GetDefault(c, nil))
			}
			data = append(data, params)
		}
	} else {
		statement = fmt.Sprintf(`INSERT INTO "%s" ("%s") VALUES (?)`, table, SQL_ROW_IDX_COL_)
		for idx := range rows {
			data = append(data, []any{idx})
		}
	}
	if len(data) > 0 {
		return conn.Executemany(statement, data)
	}
	return nil
}

// Manifest_apply_indexes_ creates single-column (``on``) and composite
// indexes; returns the set of indexed columns / column tuples.
func Manifest_apply_indexes_(conn *LockedConnection_, on any, composite any) (*py.Set, error) {
	indexed := py.NewSet()
	if !py.IsNone(on) {
		for _, c := range py.Iter(on) {
			col := py.Str(c)
			if err := Manifest_validate_ident_(c); err != nil {
				return nil, err
			}
			if _, err := conn.Execute(fmt.Sprintf(
				`CREATE INDEX IF NOT EXISTS "idx_%s_%s" ON "%s" ("%s")`,
				SQL_INDEX_TABLE_, col, SQL_INDEX_TABLE_, col)); err != nil {
				return nil, err
			}
			indexed.Add(col)
		}
	}
	if !py.IsNone(composite) {
		for _, tup := range py.Iter(composite) {
			cols := []string{}
			for _, c := range py.Iter(tup) {
				if err := Manifest_validate_ident_(c); err != nil {
					return nil, err
				}
				cols = append(cols, py.Str(c))
			}
			name := "idx_" + SQL_INDEX_TABLE_ + "_" + strings.Join(cols, "_")
			quoted := make([]string, len(cols))
			for i, c := range cols {
				quoted[i] = fmt.Sprintf(`"%s"`, c)
			}
			if _, err := conn.Execute(fmt.Sprintf(
				`CREATE INDEX IF NOT EXISTS "%s" ON "%s" (%s)`,
				name, SQL_INDEX_TABLE_, strings.Join(quoted, ", "))); err != nil {
				return nil, err
			}
			indexed.Add(py.Tuple(stringsToAny(cols)))
		}
	}
	if err := conn.Commit(); err != nil {
		return nil, err
	}
	return indexed, nil
}

// Manifest_validate_ident_ rejects column names that are not plain SQL
// identifiers.
func Manifest_validate_ident_(name any) error {
	s, ok := name.(string)
	if !ok || !SQL_IDENT_RE_.MatchString(s) {
		return py.ValueError.Newf("Invalid column identifier for indexing: %s", py.Repr(name))
	}
	return nil
}

// ----- GC cleanup of temporary index files -----------------------

// Sql_register_gc_cleanup_ ensures the manifest's temporary index file is
// nuked on GC.
//
// ``laila.forget`` cannot cover this -- the manifest object may outlive
// (or live elsewhere than) the entry that was forgotten -- so a
// per-instance finalizer removes the temporary index file when the
// manifest is garbage-collected. Only non-persistent (temporary) indexes
// are finalized; a user-chosen ``persist=`` file is left untouched.
func (m *Manifest) Sql_register_gc_cleanup_() {
	if finalizer := m.Sql_finalizer_; finalizer != nil {
		finalizer.Detach()
		m.Sql_finalizer_ = nil
	}

	state := m.Sql_state_
	if state == nil || state.Is_persistent {
		return
	}

	conn, db_path := state.Conn, state.Db_path
	m.Sql_finalizer_ = py.Finalize(m, func() { Manifest_sql_finalize_(conn, db_path) })
}

// Manifest_sql_finalize_ closes the connection and deletes a temporary
// index file.
func Manifest_sql_finalize_(conn *LockedConnection_, db_path string) {
	_ = attempt(func() error { return conn.Close() })
	Manifest_sql_unlink_db_(db_path)
}

// Manifest_sql_unlink_db_ removes an index db file and its (now-empty)
// per-manifest dir.
func Manifest_sql_unlink_db_(db_path string) {
	if db_path == "" {
		return
	}
	if py.Exists(db_path) {
		_ = os.Remove(db_path)
	}
	parent := filepath.Dir(db_path)
	if parent != "" {
		if info, err := os.Stat(parent); err == nil && info.IsDir() {
			if entries, err := os.ReadDir(parent); err == nil && len(entries) == 0 {
				_ = os.Remove(parent)
			}
		}
	}
}

// ------------------------------------------------------------------
// Mapping-like API  (top-level blueprint keys for dict(manifest))
// ------------------------------------------------------------------

// Keys returns the top-level blueprint keys.
func (m *Manifest) Keys() ([]any, error) {
	bp, err := m.blueprint_dict_()
	if err != nil {
		return nil, err
	}
	if bp == nil {
		return []any{}, nil
	}
	return bp.Keys(), nil
}

// Values returns the top-level blueprint values.
func (m *Manifest) Values() ([]any, error) {
	bp, err := m.blueprint_dict_()
	if err != nil {
		return nil, err
	}
	if bp == nil {
		return []any{}, nil
	}
	return bp.Values(), nil
}

// Items returns the top-level blueprint items.
func (m *Manifest) Items() ([]py.Item, error) {
	bp, err := m.blueprint_dict_()
	if err != nil {
		return nil, err
	}
	if bp == nil {
		return []py.Item{}, nil
	}
	return bp.Items(), nil
}

// Sub_manifest returns a new Manifest containing only the specified
// top-level keys.
func (m *Manifest) Sub_manifest(keys any) (*Manifest, error) {
	bp, err := m.blueprint_dict_()
	if err != nil {
		return nil, err
	}
	if bp == nil {
		return nil, py.RuntimeError.New("Cannot create sub-manifest — manifest is empty.")
	}
	wanted := py.Iter(keys)
	missing := py.NewList()
	for _, k := range wanted {
		if !bp.Contains(k) {
			missing.Append(k)
		}
	}
	if missing.Len() > 0 {
		return nil, py.KeyError.Newf("Keys not found in manifest: %s", py.Repr(missing))
	}
	subset := py.NewDict()
	for _, k := range wanted {
		subset.Set(k, manifest_copy_value_(bp.GetDefault(k, nil)))
	}
	return New_Manifest(py.Kw{"blueprint": subset})
}

// Extend merges another manifest's blueprint into this one in-place
// (``extend(other, *, overwrite=False)``).
func (m *Manifest) Extend(other any, kw ...py.Kw) error {
	overwrite := py.Truthy(py.MergeKw(kw...).Get("overwrite", false))
	o := As_manifest_(other)
	if o == nil || !py.Isinstance(other, Manifest_class) {
		return py.TypeError.Newf("extend() requires a Manifest, got %s", py.Pytypename(other))
	}
	other_bp, err := o.blueprint_dict_()
	if err != nil {
		return err
	}
	if other_bp == nil {
		return nil
	}

	bp, err := m.blueprint_dict_()
	if err != nil {
		return err
	}
	var new_bp *py.Dict
	if bp == nil {
		new_bp = Manifest_deepcopy_blueprint_(other_bp)
	} else {
		if !overwrite {
			overlap := py.NewSet(bp.Keys()...).Intersection(py.NewSet(other_bp.Keys()...))
			if overlap.Len() > 0 {
				sorted, err := py.Sorted(overlap.Items())
				if err != nil {
					return err
				}
				return py.KeyError.Newf("Duplicate top-level keys: %s", py.Repr(py.NewList(sorted...)))
			}
		}
		new_bp = bp.Copy()
		new_bp.Update(Manifest_deepcopy_blueprint_(other_bp))
	}

	payload, err := New_ComputationalData(new_bp)
	if err != nil {
		return err
	}
	m.Lock()
	m.Payload_ = payload
	// Same bookkeeping as the ``data`` setter: the blueprint changed in
	// this process, so a later memorize of a variable manifest bumps.
	m.Locally_modified_ = true
	m.Unlock()

	if o.Pending_entries_ != nil && o.Pending_entries_.Len() > 0 {
		if m.Pending_entries_ == nil {
			m.Pending_entries_ = o.Pending_entries_.Copy()
		} else {
			m.Pending_entries_.Extend(o.Pending_entries_)
		}
	}

	// The blueprint changed: mark any cached SQL index stale so the next
	// sql() rebuilds in place (non-memorizing algorithmic help).
	m.Invalidate_index()
	return nil
}

// Iadd is ``manifest += other`` -- merge *other* in-place and return the
// receiver. TypeError (Python's NotImplemented fallback) for non-manifests.
func (m *Manifest) Iadd(other any) (any, error) {
	if !py.Isinstance(other, Manifest_class) {
		return nil, py.TypeError.Newf("unsupported operand type(s) for +=: 'Manifest' and '%s'", py.Pytypename(other))
	}
	if err := m.Extend(other); err != nil {
		return nil, err
	}
	return m.Self(), nil
}

// Add is ``manifest + other`` -- return a new manifest with merged
// blueprints. TypeError (Python's NotImplemented fallback) for
// non-manifests.
func (m *Manifest) Add(other any) (any, error) {
	if !py.Isinstance(other, Manifest_class) {
		return nil, py.TypeError.Newf("unsupported operand type(s) for +: 'Manifest' and '%s'", py.Pytypename(other))
	}
	o := As_manifest_(other)

	bp, err := m.blueprint_dict_()
	if err != nil {
		return nil, err
	}
	other_bp, err := o.blueprint_dict_()
	if err != nil {
		return nil, err
	}
	if bp != nil && other_bp != nil {
		overlap := py.NewSet(bp.Keys()...).Intersection(py.NewSet(other_bp.Keys()...))
		if overlap.Len() > 0 {
			sorted, err := py.Sorted(overlap.Items())
			if err != nil {
				return nil, err
			}
			return nil, py.KeyError.Newf("Duplicate top-level keys: %s", py.Repr(py.NewList(sorted...)))
		}
	}

	merged := py.NewDict()
	if bp != nil {
		merged.Update(Manifest_deepcopy_blueprint_(bp))
	}
	if other_bp != nil {
		merged.Update(Manifest_deepcopy_blueprint_(other_bp))
	}

	if merged.Len() == 0 {
		return New_Manifest()
	}

	result, err := New_Manifest(py.Kw{"blueprint": merged})
	if err != nil {
		return nil, err
	}

	pending := py.NewList()
	if m.Pending_entries_ != nil && m.Pending_entries_.Len() > 0 {
		pending.Extend(m.Pending_entries_)
	}
	if o.Pending_entries_ != nil && o.Pending_entries_.Len() > 0 {
		pending.Extend(o.Pending_entries_)
	}
	if pending.Len() > 0 {
		result.Pending_entries_ = pending
	}
	return result, nil
}

// Getitem looks up a top-level key in the blueprint (``manifest[key]``).
func (m *Manifest) Getitem(key any) (any, error) {
	bp, err := m.blueprint_dict_()
	if err != nil {
		return nil, err
	}
	if bp == nil {
		return nil, py.KeyError.New(key)
	}
	return bp.Getitem(key)
}

// Setitem assigns a top-level blueprint key (``manifest["config"] =
// entry``).
//
// Accepted values are blueprint fragments -- a ``global_id`` string, a
// list of them, a nested dict of them -- or Entry objects (anywhere
// inside a list / dict too), which are converted to their gid and queued
// for the next Memorize. A manifest or a ManifestBacked object is
// rejected here: a manifest that holds other manifests is a
// ComplexManifest (``laila.complex_manifest(...)``), which is the only
// kind the read path resolves recursively.
//
// Re-assigning an existing key replaces it. The blueprint changed, so the
// manifest is marked locally modified and any SQL index is invalidated
// (same bookkeeping as Extend).
func (m *Manifest) Setitem(key any, value any) error {
	k, ok := key.(string)
	if !ok {
		return py.TypeError.Newf("Manifest keys must be strings, got %s", py.Pytypename(key))
	}
	leaf, pending, err := m.coerce_value_(k, value)
	if err != nil {
		return err
	}
	return m.with_atomic_(func() error {
		bp, err := m.blueprint_dict_()
		if err != nil {
			return err
		}
		new_bp := py.NewDict()
		if bp != nil {
			new_bp = bp.Copy()
		}
		new_bp.Set(k, leaf)
		if err := m.Set_blueprint_(new_bp); err != nil {
			return err
		}
		if len(pending) > 0 {
			if m.Pending_entries_ == nil {
				m.Pending_entries_ = py.NewList(pending...)
			} else {
				m.Pending_entries_.Extend(py.NewList(pending...))
			}
		}
		return nil
	})
}

// Delitem removes a top-level blueprint key (its entries are not
// forgotten).
func (m *Manifest) Delitem(key any) error {
	bp, err := m.blueprint_dict_()
	if err != nil {
		return err
	}
	if bp == nil || !bp.Contains(key) {
		return py.KeyError.New(key)
	}
	return m.with_atomic_(func() error {
		new_bp := bp.Copy()
		if err := new_bp.Del(key); err != nil {
			return err
		}
		return m.Set_blueprint_(new_bp)
	})
}

// Set_blueprint_ replaces the blueprint in place (Setitem / Delitem).
func (m *Manifest) Set_blueprint_(blueprint *py.Dict) error {
	if err := m.Set_data(blueprint); err != nil {
		return err
	}
	if m.State() != EntryState_READY {
		return m.Set_state(EntryState_READY)
	}
	return nil
}

// Coerce_value_ is ``(blueprint fragment, pending entries)`` for
// ``self[key] = value``.
func (m *Manifest) Coerce_value_(key string, value any) (any, []any, error) {
	if py.Isinstance(value, Manifest_class) || Is_manifest_backed_(value) {
		return nil, nil, py.TypeError.Newf(
			"Manifest[%s]: a %s cannot be a member of a plain "+
				"manifest; create the container with laila.complex_manifest(...) so the "+
				"nested manifest is memorized and resolved recursively",
			py.Repr(key), py.Pytypename(value))
	}
	if py.Isinstance(value, Entry_class) {
		return global_id_of_(value), []any{value}, nil
	}
	if s, ok := value.(string); ok {
		return s, nil, nil
	}
	if is_list_(value) || is_mapping_(value) {
		fragment := py.DictOf(key, value)
		has_entries, has_strings, err := Manifest_classify_data_(fragment)
		if err != nil {
			return nil, nil, err
		}
		if has_entries && has_strings {
			return nil, nil, py.ValueError.Newf(
				"Manifest[%s]: a fragment must hold either Entry objects or "+
					"global_id strings, not a mix of both", py.Repr(key))
		}
		if has_entries {
			ents, err := Manifest_iter_entries_impl_(fragment)
			if err != nil {
				return nil, nil, err
			}
			pending := make([]any, 0, len(ents))
			for _, e := range ents {
				if py.Isinstance(e.Self(), Manifest_class) {
					return nil, nil, py.TypeError.Newf(
						"Manifest[%s]: nested manifests need laila.complex_manifest(...)", py.Repr(key))
				}
				pending = append(pending, e.Self())
			}
			extracted, err := Manifest_extract_blueprint_(fragment)
			if err != nil {
				return nil, nil, err
			}
			return extracted.GetDefault(key, nil), pending, nil
		}
		if err := Manifest_validate_blueprint_(fragment); err != nil {
			return nil, nil, err
		}
		return manifest_copy_value_(value), nil, nil
	}
	return nil, nil, py.TypeError.Newf(
		"Manifest[%s]: expected a global_id, a list / dict of them or an Entry, got %s",
		py.Repr(key), py.Pytypename(value))
}

// coerce_value_ is the virtual ``self._coerce_value(key, value)``.
func (m *Manifest) coerce_value_(key string, value any) (any, []any, error) {
	return m.Self().(interface {
		Coerce_value_(key string, value any) (any, []any, error)
	}).Coerce_value_(key, value)
}

// Len is the number of top-level keys in the blueprint.
func (m *Manifest) Len() int {
	bp, err := m.blueprint_dict_()
	if err != nil || bp == nil {
		return 0
	}
	return bp.Len()
}

// ------------------------------------------------------------------
// Iteration / containment  (flattened global_id leaves)
// ------------------------------------------------------------------

// Iter yields every ``global_id`` string via a depth-first,
// insertion-order walk (``__iter__``).
func (m *Manifest) Iter() []any {
	bp, err := m.blueprint_dict_()
	if err != nil || bp == nil {
		return []any{}
	}
	return Manifest_iter_global_ids_(bp)
}

// Global_ids is Iter as a string slice (``list(manifest)``).
func (m *Manifest) Global_ids() []string {
	items := m.Iter()
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = py.Str(it)
	}
	return out
}

// Contains returns true if *global_id* appears anywhere in the blueprint
// leaves (``gid in manifest``).
func (m *Manifest) Contains(global_id any) bool {
	s, ok := global_id.(string)
	if !ok {
		return false
	}
	for _, gid := range m.Iter() {
		if py.Eq(gid, s) {
			return true
		}
	}
	return false
}

// Floating_leaves lists the leaves that resolve at *read* time rather
// than naming one stored record.
//
// A leaf is *floating* when its reference carries a negative
// ``evolution`` (``@evolution=-1`` = "whatever is latest") or a
// ``creation_timestamp`` search argument. Such leaves make the manifest's
// contents depend on the pool state at the moment of ``remember``; pinned
// manifests (reproducible datasets) should have none. Malformed leaves are
// ignored here.
func (m *Manifest) Floating_leaves() []string {
	floating := []string{}
	for _, raw := range m.Iter() {
		gid := py.Str(raw)
		_, attrs, err := Split_global_id_attributes(gid)
		if err != nil {
			continue
		}
		evolution := py.Str(attrs.GetDefault(EVOLUTION_ATTRIBUTE, ""))
		if strings.HasPrefix(evolution, "-") || attrs.Contains(CREATION_TIMESTAMP_ATTRIBUTE) {
			floating = append(floating, gid)
		}
	}
	return floating
}

// ------------------------------------------------------------------
// String representation
// ------------------------------------------------------------------

// String is ``str(manifest)`` -> global_id.
func (m *Manifest) String() string { return m.Global_id() }

// Repr is ``repr(manifest)`` -> ``<Class>(<gid>, entries=<n>)``.
func (m *Manifest) Repr() string {
	return fmt.Sprintf("%s(%s, entries=%d)", m.Class_().Name, m.Global_id(), len(m.Iter()))
}

// ------------------------------------------------------------------
// Static / class helpers
// ------------------------------------------------------------------

// Manifest_collect_future_ids_ extracts future IDs from a GroupFuture or
// a single future identity.
//
// The children are re-parented into the manifest-level GroupFuture
// returned to the caller, so an intermediate per-batch group shell is
// released here (children untouched); the caller releases the returned
// group, which releases the children.
func Manifest_collect_future_ids_(ref any) ([]string, error) {
	if py.IsNone(ref) {
		return []string{}, nil
	}
	if py.Hasattr(ref, "future_ids") {
		raw, err := py.Getattr(ref, "future_ids")
		if err != nil {
			return nil, err
		}
		ids := toStrings(raw)
		if g, ok := ref.(*GroupFuture); ok {
			g.Release_with(py.Kw{"children": false})
		}
		return ids, nil
	}
	return []string{global_id_of_(ref)}, nil
}

// Manifest_resolve_pool_ collapses ``pool`` / ``pool_id`` /
// ``pool_nickname`` into one routing value.
//
// Precedence mirrors the top-level shims: an explicit ``pool`` wins, then
// ``pool_id``, then ``pool_nickname``. nil means the alpha (default) pool.
// The result is accepted as-is by ``laila.memorize(dst_pool=)`` /
// ``laila.remember(dst_pool=)`` / ``laila.forget(pool=)``.
func Manifest_resolve_pool_(pool any, pool_nickname any, pool_id any) any {
	if !py.IsNone(pool) {
		return pool
	}
	if !py.IsNone(pool_id) {
		return pool_id
	}
	return pool_nickname
}

// Manifest_classify_data_ returns ``(has_entries, has_strings)`` for leaf
// values in *data*.
func Manifest_classify_data_(data any) (bool, bool, error) {
	d, ok := asDict(data)
	if !ok {
		return false, false, py.AttributeError.Newf("'%s' object has no attribute 'values'", py.Pytypename(data))
	}
	has_entries, has_strings := false, false
	var check func(val any)
	check = func(val any) {
		switch {
		case py.Isinstance(val, Entry_class):
			has_entries = true
		case is_str_(val):
			has_strings = true
		case is_list_(val):
			for _, item := range py.Iter(val) {
				check(item)
			}
		case is_mapping_(val):
			sub, _ := asDict(val)
			for _, v := range sub.Values() {
				check(v)
			}
		}
	}
	for _, v := range d.Values() {
		check(v)
	}
	return has_entries, has_strings, nil
}

// Manifest_iter_entries_impl_ yields every Entry object from a nested
// dict (``Manifest._iter_entries``).
func Manifest_iter_entries_impl_(data any) ([]*Entry, error) {
	d, ok := asDict(data)
	if !ok {
		return nil, py.AttributeError.Newf("'%s' object has no attribute 'values'", py.Pytypename(data))
	}
	out := []*Entry{}
	for _, val := range d.Values() {
		switch {
		case py.Isinstance(val, Entry_class):
			out = append(out, As_entry_(val))
		case is_list_(val):
			for _, item := range py.Iter(val) {
				if py.Isinstance(item, Entry_class) {
					out = append(out, As_entry_(item))
				}
			}
		case is_mapping_(val):
			nested, err := Manifest_iter_entries_impl_(val)
			if err != nil {
				return nil, err
			}
			out = append(out, nested...)
		}
	}
	return out, nil
}

// Manifest_validate_blueprint_ recursively validates that *data* follows
// the blueprint schema.
func Manifest_validate_blueprint_(data any) error {
	d, ok := asDict(data)
	if !ok {
		return py.ValueError.New("Blueprint must be a dict.")
	}
	for _, it := range d.Items() {
		if !is_str_(it.Key) {
			return py.ValueError.Newf("Blueprint keys must be strings, got %s", py.Repr_type(it.Key))
		}
		val := it.Value
		switch {
		case is_str_(val):
			continue
		case is_list_(val):
			for _, item := range py.Iter(val) {
				if !is_str_(item) {
					return py.ValueError.Newf("List values in blueprint must be strings, got %s", py.Repr_type(item))
				}
			}
		case is_mapping_(val):
			if err := Manifest_validate_blueprint_(val); err != nil {
				return err
			}
		default:
			return py.ValueError.Newf("Blueprint values must be str, list[str], or dict — got %s", py.Repr_type(val))
		}
	}
	return nil
}

// Manifest_extract_blueprint_ converts a dict of Entry objects to a
// blueprint of ``global_id`` strings.
func Manifest_extract_blueprint_(data any) (*py.Dict, error) {
	d, ok := asDict(data)
	if !ok {
		return nil, py.AttributeError.Newf("'%s' object has no attribute 'items'", py.Pytypename(data))
	}
	result := py.NewDict()
	for _, it := range d.Items() {
		val := it.Value
		switch {
		case py.Isinstance(val, Entry_class):
			result.Set(it.Key, global_id_of_(val))
		case is_str_(val):
			result.Set(it.Key, val)
		case is_list_(val):
			lst := py.NewList()
			for _, item := range py.Iter(val) {
				if py.Isinstance(item, Entry_class) {
					lst.Append(global_id_of_(item))
				} else {
					lst.Append(item)
				}
			}
			result.Set(it.Key, lst)
		case is_mapping_(val):
			sub, err := Manifest_extract_blueprint_(val)
			if err != nil {
				return nil, err
			}
			result.Set(it.Key, sub)
		default:
			return nil, py.ValueError.Newf("Unexpected value type in manifest data: %s", py.Repr_type(val))
		}
	}
	return result, nil
}

// Manifest_iter_global_ids_ is the depth-first, insertion-order walk
// yielding every leaf ``global_id``.
func Manifest_iter_global_ids_(blueprint *py.Dict) []any {
	out := []any{}
	for _, val := range blueprint.Values() {
		switch {
		case is_str_(val):
			out = append(out, val)
		case is_list_(val):
			out = append(out, py.Iter(val)...)
		case is_mapping_(val):
			sub, _ := asDict(val)
			out = append(out, Manifest_iter_global_ids_(sub)...)
		}
	}
	return out
}

// Manifest_rebuild_with_entries_ reconstructs the nested structure
// replacing ``global_id`` strings with entries.
func Manifest_rebuild_with_entries_(blueprint *py.Dict, resolved_map *py.Dict) (*py.Dict, error) {
	result := py.NewDict()
	for _, it := range blueprint.Items() {
		val := it.Value
		switch {
		case is_str_(val):
			v, err := resolved_map.Getitem(val)
			if err != nil {
				return nil, err
			}
			result.Set(it.Key, v)
		case is_list_(val):
			lst := py.NewList()
			for _, gid := range py.Iter(val) {
				v, err := resolved_map.Getitem(gid)
				if err != nil {
					return nil, err
				}
				lst.Append(v)
			}
			result.Set(it.Key, lst)
		case is_mapping_(val):
			sub, _ := asDict(val)
			nested, err := Manifest_rebuild_with_entries_(sub, resolved_map)
			if err != nil {
				return nil, err
			}
			result.Set(it.Key, nested)
		}
	}
	return result, nil
}

// ------------------------------------------------------------------
// ManifestBacked
// ------------------------------------------------------------------

// ManifestBacked marks objects that are *backed by* a Manifest.
//
// Some objects cannot embed Manifest (a pydantic model with a validating
// ``__setattr__`` in Python) but still want ``laila.Memorize(obj)`` /
// ``laila.Remember(obj)`` / ``laila.Forget(obj)`` to mean "turn me into
// my manifest and run its bulk operation" -- ``laila.addons.torch.Module``
// and ``Optimizer`` are the canonical Python examples. The top-level
// shims treat a bare ManifestBacked exactly like a bare Manifest: they
// call ``obj.Memorize(pool=...)``, ``obj.Remember(pool=..., persist=...)``
// or ``obj.Forget(pool=...)`` and return whatever that returns.
//
// Implementations provide Manifest (a fresh Manifest describing their
// current state), Global_id (the manifest's identity, derived from a
// nickname) and the three bulk operations, which normally delegate to a
// Manifest subclass that implements the type-specific Remember. A plain
// interface on purpose (Python: an ABC): it carries no state and composes
// with any struct.
type ManifestBacked interface {
	// Manifest is a fresh manifest snapshot of this object's current state.
	Manifest() (*Manifest, error)
	// Global_id is the ``global_id`` of the manifest this object is stored under.
	Global_id() string
	// Memorize stores the manifest (and its entries); returns a future-like.
	Memorize(kw ...py.Kw) (any, error)
	// Remember loads the stored manifest back into this object; returns a future-like.
	Remember(kw ...py.Kw) (any, error)
	// Forget deletes the manifest and its entries from *pool*; returns a future-like.
	Forget(kw ...py.Kw) (any, error)
}

// Is_manifest_backed_ is ``isinstance(v, ManifestBacked)``.
func Is_manifest_backed_(v any) bool {
	_, ok := v.(ManifestBacked)
	return ok
}

// manifest_or_backed_ is ``isinstance(v, (Manifest, ManifestBacked))``.
func manifest_or_backed_(v any) bool {
	return py.Isinstance(v, Manifest_class) || Is_manifest_backed_(v)
}

// ------------------------------------------------------------------
// ComplexManifest
// ------------------------------------------------------------------

// ComplexManifest is a manifest whose members may be manifests
// themselves.
//
// Scope ``MANIFEST:COMPLEX`` -- the gid itself announces that there are
// manifests underneath, so the read path only ever walks complex
// manifests; a plain Manifest (a dataset with a million leaves) is never
// scanned.
//
// Write side:
//
//	everything, _ := laila.New_ComplexManifest(py.Kw{"nickname": "everything"})
//	everything.Setitem("model", model)         // a ManifestBacked (or an adapted bare object)
//	everything.Setitem("dataset", dataset)     // a plain Manifest
//	everything.Setitem("config", config_entry) // an Entry
//	laila.Memorize(everything, py.Kw{"dst_pool": "ckpt"})
//
// Memorize is recursive: every member that is a Manifest or
// ManifestBacked runs its *own* Memorize into the same pool (receiving
// the sibling members as ``context``), plain entries are batched as
// usual, and the complex manifest's blueprint is ``{key: member gid}``.
//
// Read side -- *recursive blueprint resolution*:
//
//	everything := laila.Remember("MANIFEST:COMPLEX:everything", py.Kw{"dst_pool": "ckpt"}) ... Wait()
//	everything.Getitem("model")    // a typed manifest (through its scope builder)
//	everything.Getitem("dataset")  // the dataset Manifest (one record, never walked)
//	everything.Getitem("config")   // still a gid: plain entries are not fetched here
//	live, _ := everything.Rebuild() // {"model": ..., "config": Entry, ...}
//
// When a complex manifest arrives from a pool, every top-level leaf whose
// gid carries the ``MANIFEST`` scope is fetched from the same pool,
// hydrated into its typed class through the scope builder and attached
// as a member; a nested complex manifest is resolved recursively in
// turn. Leaves that are not manifests stay gids. Rebuild then asks each
// typed member to reconstruct its live object, fetches plain entries and
// returns ``{key: object}`` in blueprint order.
//
// Members are top-level keys only; nested dict / list fragments behave
// exactly as in a plain manifest (gids, realized to entries).
type ComplexManifest struct {
	Manifest
	// Members_ is ``{key: object}`` -- what ``self[key]`` currently is: the
	// assigned object (write side), the typed manifest (after resolution)
	// or the live object (after rebuild).
	Members_ *py.Dict `py:"_members"`
	// Adapted_ is ``{key: ManifestBacked}`` for members that were adapted
	// from a bare object: the wrapper that memorizes / forgets on the bare
	// object's behalf.
	Adapted_  *py.Dict `py:"_adapted"`
	Resolved_ bool     `py:"_resolved"`
}

// ComplexManifest_class is the class descriptor (``_DEFAULT_SCOPES =
// [_MANIFEST_SCOPE, _COMPLEX_MANIFEST_SCOPE]``, ``_OWNS_CHILDREN = True``).
var ComplexManifest_class = py.NewClass("ComplexManifest", (*ComplexManifest)(nil), Manifest_class).
	Set_classvar("_DEFAULT_SCOPES", []string{MANIFEST_SCOPE_, COMPLEX_MANIFEST_SCOPE_}).
	Set_classvar("_OWNS_CHILDREN", true)

// New_ComplexManifest is ``ComplexManifest(**data)``.
func New_ComplexManifest(kw ...py.Kw) (*ComplexManifest, error) {
	c := ComplexManifest_class.New().(*ComplexManifest)
	if err := c.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return c, nil
}

// As_complex_manifest_ returns the embedded *ComplexManifest of any
// ComplexManifest-derived instance (nil when v is not one).
func As_complex_manifest_(v any) *ComplexManifest {
	if c, ok := v.(*ComplexManifest); ok {
		return c
	}
	if b, ok := v.(interface{ ComplexManifest_base_() *ComplexManifest }); ok {
		return b.ComplexManifest_base_()
	}
	return nil
}

// ComplexManifest_base_ exposes the embedded ComplexManifest to code
// holding a subclass.
func (c *ComplexManifest) ComplexManifest_base_() *ComplexManifest { return c }

// Manifest_owns_children_ is ``obj._OWNS_CHILDREN`` through the MRO.
func Manifest_owns_children_(obj any) bool {
	cls := py.Classof(obj)
	if cls == nil {
		return false
	}
	v, ok := cls.Classvar("_OWNS_CHILDREN")
	return ok && py.Truthy(v)
}

// Init_ is ``ComplexManifest.__init__(**data)``: the manifest is built
// empty and every ``data`` / ``blueprint`` item goes through Setitem so
// manifests and manifest-backed objects become members.
func (c *ComplexManifest) Init_(data py.Kw) error {
	raw_data := data.Pop("data", nil)
	blueprint_kwarg := data.Pop("blueprint", nil)
	source := raw_data
	if py.IsNone(source) {
		source = blueprint_kwarg
	}
	c.Members_ = py.NewDict()
	c.Adapted_ = py.NewDict()
	c.Resolved_ = false
	if err := c.Manifest.Init_(data); err != nil {
		return err
	}
	if !py.IsNone(source) {
		d, ok := asDict(source)
		if !ok {
			return py.ValueError.New("ComplexManifest data must be a dict.")
		}
		if d.Len() == 0 {
			if err := c.Set_blueprint_(py.NewDict()); err != nil {
				return err
			}
		}
		for _, it := range d.Items() {
			if err := c.Setitem(it.Key, it.Value); err != nil {
				return err
			}
		}
	}
	return nil
}

// members_ / adapted_ are the private dicts, created lazily: an instance
// hydrated through ``from_dict`` bypasses Init_.
func (c *ComplexManifest) members_() *py.Dict {
	if c.Members_ == nil {
		c.Members_ = py.NewDict()
	}
	return c.Members_
}

func (c *ComplexManifest) adapted_() *py.Dict {
	if c.Adapted_ == nil {
		c.Adapted_ = py.NewDict()
	}
	return c.Adapted_
}

// ----- adapters ------------------------------------------------------

// Adapter_factory is ``factory(value, key, container_nickname)``: returns
// the ManifestBacked (or Manifest) standing in for *value*.
type Adapter_factory func(value any, key string, nickname any) (any, error)

// Adapter_entry_ is one ``(type, factory)`` pair of the adapter registry.
type Adapter_entry_ struct {
	Type    any
	Factory Adapter_factory
}

// ComplexManifest_ADAPTERS_ is ``ComplexManifest._ADAPTERS``: ``(type,
// factory)`` pairs, most recently registered first.
var ComplexManifest_ADAPTERS_ = []Adapter_entry_{}

var complex_manifest_adapters_mu_ sync.RWMutex

// ComplexManifest_register_adapter is ``ComplexManifest.register_adapter(type_,
// factory)``: teaches ``everything[key] = value`` how to wrap a bare
// *type_*.
//
// *type_* is a *py.Class (``py.Isinstance``), a reflect.Type (exact type
// or implemented interface) or a ``func(any) bool`` predicate.
// *factory(value, key, container_nickname)* must return a ManifestBacked
// (or Manifest) standing in for *value*. The most recently registered
// matching type wins.
func ComplexManifest_register_adapter(type_ any, factory Adapter_factory) {
	complex_manifest_adapters_mu_.Lock()
	defer complex_manifest_adapters_mu_.Unlock()
	ComplexManifest_ADAPTERS_ = append([]Adapter_entry_{{Type: type_, Factory: factory}}, ComplexManifest_ADAPTERS_...)
}

// ComplexManifest_unregister_adapter_ drops every adapter registered for
// *type_* (tests restore the registry with it; Python edits ``_ADAPTERS``
// in place).
func ComplexManifest_unregister_adapter_(type_ any) {
	complex_manifest_adapters_mu_.Lock()
	defer complex_manifest_adapters_mu_.Unlock()
	kept := ComplexManifest_ADAPTERS_[:0:0]
	for _, a := range ComplexManifest_ADAPTERS_ {
		if a.Type != type_ {
			kept = append(kept, a)
		}
	}
	ComplexManifest_ADAPTERS_ = kept
}

// adapter_matches_ is ``isinstance(value, type_)`` for the registry's
// type forms.
func adapter_matches_(type_ any, value any) bool {
	switch t := type_.(type) {
	case *py.Class:
		return py.Isinstance(value, t)
	case reflect.Type:
		vt := reflect.TypeOf(value)
		if vt == nil {
			return false
		}
		if t.Kind() == reflect.Interface {
			return vt.Implements(t)
		}
		return vt == t
	case func(any) bool:
		return t(value)
	}
	return false
}

// ComplexManifest_adapt_ is ``ComplexManifest._adapt(value, key, nickname)``:
// the adapted stand-in for *value*, or nil when no adapter matches.
func ComplexManifest_adapt_(value any, key string, nickname any) (any, error) {
	complex_manifest_adapters_mu_.RLock()
	adapters := append([]Adapter_entry_(nil), ComplexManifest_ADAPTERS_...)
	complex_manifest_adapters_mu_.RUnlock()
	for _, a := range adapters {
		if adapter_matches_(a.Type, value) {
			return a.Factory(value, key, nickname)
		}
	}
	return nil, nil
}

// ----- mapping surface -----------------------------------------------

// Coerce_value_ records manifests / manifest-backed objects (and adapted
// bare objects) as members and defers everything else to Manifest.
func (c *ComplexManifest) Coerce_value_(key string, value any) (any, []any, error) {
	_, _ = c.adapted_().Pop(key)
	_, _ = c.members_().Pop(key)
	if manifest_or_backed_(value) {
		c.members_().Set(key, value)
		return global_id_of_(value), nil, nil
	}
	adapted, err := ComplexManifest_adapt_(value, key, c.Nickname())
	if err != nil {
		return nil, nil, err
	}
	if !py.IsNone(adapted) {
		c.members_().Set(key, value)
		c.adapted_().Set(key, adapted)
		return global_id_of_(adapted), nil, nil
	}
	if py.Isinstance(value, Entry_class) {
		c.members_().Set(key, value)
	}
	return c.Manifest.Coerce_value_(key, value)
}

// Getitem is the member under *key* when there is one, else the
// blueprint value.
//
// After assignment this is the object that was assigned; after a read
// from a pool it is the typed manifest the leaf resolved to; after
// Rebuild it is the live object. ``Blueprint()[key]`` is always the raw
// gid / fragment.
func (c *ComplexManifest) Getitem(key any) (any, error) {
	if v, ok := c.members_().Get(key); ok {
		return v, nil
	}
	return c.Manifest.Getitem(key)
}

// Delitem removes a top-level key and its member.
func (c *ComplexManifest) Delitem(key any) error {
	if err := c.Manifest.Delitem(key); err != nil {
		return err
	}
	_, _ = c.members_().Pop(key)
	_, _ = c.adapted_().Pop(key)
	return nil
}

// Members is a read-only view of the current members (``{key: object}``).
func (c *ComplexManifest) Members() *py.Dict { return c.members_().Copy() }

// Snapshot_ is the stored copy (in-memory pools): it must not share the
// live members -- a later read resolves them from the pool like any other
// pool would.
func (c *ComplexManifest) Snapshot_() (any, error) {
	copied, err := c.Manifest.Snapshot_()
	if err != nil {
		return nil, err
	}
	if clone := As_complex_manifest_(copied); clone != nil {
		clone.Members_ = py.NewDict()
		clone.Adapted_ = py.NewDict()
		clone.Resolved_ = false
	}
	return copied, nil
}

// Preresolved_ is ``{gid: member}`` for the members that are entries
// (resolved manifests included).
func (c *ComplexManifest) Preresolved_() (*py.Dict, error) {
	bp, err := c.blueprint_dict_()
	if err != nil {
		return nil, err
	}
	out := py.NewDict()
	if bp == nil {
		return out, nil
	}
	for _, it := range c.members_().Items() {
		gid, ok := bp.GetDefault(it.Key, nil).(string)
		if ok && py.Isinstance(it.Value, Entry_class) {
			out.Set(gid, it.Value)
		}
	}
	return out, nil
}

// Handle_ is the memorizable stand-in for member *key* (the adapter
// wrapper when there is one).
func (c *ComplexManifest) Handle_(key any) any {
	if v, ok := c.adapted_().Get(key); ok {
		return v
	}
	return c.members_().GetDefault(key, nil)
}

// Manifest_members_ lists the members that are manifests (typed or
// plain), nested complex ones expanded (``_manifest_members(*,
// recursive=True)``).
func (c *ComplexManifest) Manifest_members_(kw ...py.Kw) []any {
	recursive := py.Truthy(py.MergeKw(kw...).Get("recursive", true))
	out := []any{}
	for _, key := range c.members_().Keys() {
		handle := c.Handle_(key)
		if py.Isinstance(handle, Manifest_class) {
			out = append(out, handle)
			if nested := As_complex_manifest_(handle); recursive && nested != nil {
				out = append(out, nested.Manifest_members_(py.Kw{"recursive": true})...)
			}
		}
	}
	return out
}

// ----- memorize (recursive) ------------------------------------------

// Memorize memorizes every member into *pool*, then the complex manifest
// itself.
//
// Members that are manifests / manifest-backed objects run their own
// ``Memorize(pool=..., context=<sibling handles>)`` first (so a typed
// manifest can reference a sibling), plain pending entries are batched,
// and finally the blueprint ``{key: member gid}`` is stored under this
// manifest's gid. One GroupFuture covering every write is returned.
func (c *ComplexManifest) Memorize(kw ...py.Kw) (*GroupFuture, error) {
	opts := py.MergeKw(kw...)
	bp, err := c.blueprint_dict_()
	if err != nil {
		return nil, err
	}
	if bp == nil {
		return nil, py.RuntimeError.New("Nothing to memorize — manifest has no blueprint.")
	}
	batch_size, err := manifest_batch_size_(opts)
	if err != nil {
		return nil, err
	}
	routed := Manifest_resolve_pool_(opts.Get("pool", nil), opts.Get("pool_nickname", nil), opts.Get("pool_id", nil))
	policy, err := Get_active_policy()
	if err != nil {
		return nil, err
	}
	all_future_ids := []string{}
	handles := py.NewDict()
	for _, key := range bp.Keys() {
		if c.members_().Contains(key) {
			handles.Set(key, c.Handle_(key))
		}
	}

	for _, key := range bp.Keys() {
		handle := handles.GetDefault(key, nil)
		if !manifest_or_backed_(handle) {
			continue
		}
		ref, err := manifest_bulk_op_(handle, "memorize", py.Kw{
			"pool": routed, "batch_size": batch_size, "context": handles,
		})
		if err != nil {
			return nil, err
		}
		ids, err := Manifest_collect_future_ids_(ref)
		if err != nil {
			return nil, err
		}
		all_future_ids = append(all_future_ids, ids...)
	}

	self_ref, err := c.Manifest.Memorize(py.Kw{"pool": routed, "batch_size": batch_size})
	if err != nil {
		return nil, err
	}
	ids, err := Manifest_collect_future_ids_(self_ref)
	if err != nil {
		return nil, err
	}
	all_future_ids = append(all_future_ids, ids...)
	return New_GroupFuture(py.Kw{
		"taskforce_id": getattr(getattr(getattr(policy, "central"), "command"), "internal_taskforce"),
		"policy_id":    global_id_of_(policy),
		"future_ids":   all_future_ids,
	})
}

// manifest_bulk_op_ runs ``obj.<op>(**kw)`` on a Manifest (any subclass)
// or a ManifestBacked -- the shared dispatch of the top-level shims and
// the recursive ComplexManifest operations.
func manifest_bulk_op_(obj any, op string, kw py.Kw) (any, error) {
	if mb, ok := obj.(ManifestBacked); ok {
		switch op {
		case "memorize":
			return mb.Memorize(kw)
		case "remember":
			return mb.Remember(kw)
		case "forget":
			return mb.Forget(kw)
		}
	}
	switch op {
	case "memorize":
		if x, ok := obj.(interface {
			Memorize(kw ...py.Kw) (*GroupFuture, error)
		}); ok {
			return x.Memorize(kw)
		}
	case "remember":
		if x, ok := obj.(interface {
			Remember(kw ...py.Kw) (*GroupFuture, error)
		}); ok {
			return x.Remember(kw)
		}
	case "forget":
		if x, ok := obj.(interface {
			Forget(kw ...py.Kw) (*GroupFuture, error)
		}); ok {
			return x.Forget(kw)
		}
	}
	return callm(obj, op, kw)
}

// ----- recursive blueprint resolution --------------------------------

// Manifest_leaf_ is one ``(key, gid)`` pair of a blueprint.
type Manifest_leaf_ struct {
	Key string
	Gid string
}

// Unresolved_manifest_leaves_ lists the ``(key, gid)`` of top-level
// manifest-scoped leaves without a member.
func (c *ComplexManifest) Unresolved_manifest_leaves_() []Manifest_leaf_ {
	bp, err := c.blueprint_dict_()
	if err != nil || bp == nil {
		return nil
	}
	out := []Manifest_leaf_{}
	for _, it := range bp.Items() {
		if c.members_().Contains(it.Key) {
			continue
		}
		gid, ok := it.Value.(string)
		if ok && Manifest_is_manifest_gid(gid) {
			out = append(out, Manifest_leaf_{Key: py.Str(it.Key), Gid: gid})
		}
	}
	return out
}

// has_unresolved_member_ is ``any(isinstance(m, ComplexManifest) and not
// m._resolved for m in self._members.values())``.
func (c *ComplexManifest) has_unresolved_member_() bool {
	for _, member := range c.members_().Values() {
		if nested := As_complex_manifest_(member); nested != nil && !nested.Resolved_ {
			return true
		}
	}
	return false
}

// Async_resolve is the awaitable form of Resolve (``async_resolve(*,
// pool=None, pool_nickname=None, pool_id=None, persist=False)``): the
// coroutine resolves to the receiver.
func (c *ComplexManifest) Async_resolve(kw ...py.Kw) py.Coroutine {
	opts := py.MergeKw(kw...)
	return func() (any, error) {
		bp, err := c.blueprint_dict_()
		if err != nil {
			return nil, err
		}
		if c.Resolved_ || bp == nil {
			return c.Self(), nil
		}
		policy, err := Get_active_policy()
		if err != nil {
			return nil, err
		}
		memory := As_central_memory_(getattr(getattr(policy, "central"), "memory"))
		if memory == nil {
			return nil, py.AttributeError.New("'NoneType' object has no attribute '_read_entries_async'")
		}
		persist := py.Truthy(opts.Get("persist", false))
		leaves := c.Unresolved_manifest_leaves_()
		if len(leaves) > 0 {
			gids := make([]string, len(leaves))
			for i, leaf := range leaves {
				gids[i] = leaf.Gid
			}
			results, err := c.Read_manifests_async_(memory, gids,
				Manifest_resolve_pool_(opts.Get("pool", nil), opts.Get("pool_nickname", nil), opts.Get("pool_id", nil)),
				persist)
			if err != nil {
				return nil, err
			}
			for i, leaf := range leaves {
				entry := results[i]
				if !py.Isinstance(entry, Manifest_class) {
					return nil, py.KeyError.Newf(
						"%s[%s]: %s did not resolve to a manifest (got %s)",
						c.Class_().Name, py.Repr(leaf.Key), leaf.Gid, py.Pytypename(entry))
				}
				c.members_().Set(leaf.Key, entry)
			}
		}
		for _, member := range c.members_().Values() {
			if nested := As_complex_manifest_(member); nested != nil && !nested.Resolved_ {
				if _, err := py.Await(nested.Async_resolve(py.Kw{
					"pool": opts.Get("pool", nil), "pool_nickname": opts.Get("pool_nickname", nil),
					"pool_id": opts.Get("pool_id", nil), "persist": persist,
				})); err != nil {
					return nil, err
				}
			}
		}
		c.Resolved_ = true
		return c.Self(), nil
	}
}

// Read_manifests_async_ fetches *gids* trying *pool* first, then this
// manifest's candidate pools (``_read_manifests_async``; a coroutine body
// -- call it from inside a running task).
func (c *ComplexManifest) Read_manifests_async_(memory *LAILA_IDENTIFIABLE_CENTRAL_MEMORY_, gids []string, pool any, persist bool) ([]any, error) {
	on_loop := CURRENT_SLOT_.Value() != nil
	candidates := []any{}
	if !py.IsNone(pool) {
		candidates = append(candidates, pool)
	}
	for _, candidate := range c.Candidate_pools_(memory.Self()) {
		seen := false
		for _, known := range candidates {
			if py.Eq(known, candidate) {
				seen = true
				break
			}
		}
		if !seen {
			candidates = append(candidates, candidate)
		}
	}

	ids := py.NewList(stringsToAny(gids)...)
	read := func(candidate any) ([]any, error) {
		if on_loop {
			lst, err := memory.Read_entries_async_(ids, py.Kw{"pool": candidate, "persist": persist})
			if err != nil {
				return nil, err
			}
			return lst.Items(), nil
		}
		ref, err := memory.Read_entries_direct_(ids, py.Kw{"pool": candidate, "persist": persist})
		if err != nil {
			return nil, err
		}
		defer release_(ref)
		awaited, err := py.Await(ref)
		if err != nil {
			return nil, err
		}
		data, err := py.Getattr(awaited, "data")
		if err != nil {
			return nil, err
		}
		return py.Iter(data), nil
	}

	var last_exc error
	for _, candidate := range candidates {
		res, err := read(candidate)
		if err != nil {
			if errors.Is(err, py.KeyError) {
				last_exc = err
				continue
			}
			return nil, err
		}
		return res, nil
	}
	return nil, py.KeyError.Newf("%s: nested manifest(s) %s not found in any pool",
		c.Class_().Name, py.Repr(ids)).From(last_exc)
}

// Resolve is the recursive blueprint resolution: cast manifest-scoped
// leaves into members (``resolve(*, pool=None, pool_nickname=None,
// pool_id=None, persist=False)``).
//
// Walks the top-level blueprint; every leaf whose gid carries the
// ``MANIFEST`` scope (decided from the string, no I/O) is fetched -- from
// *pool* first, then from the pool this manifest came from, the alpha
// pool and every other registered pool -- hydrated into its typed class
// (Manifest, ComplexManifest, a user subclass, ...) and stored as the
// member under that key. Nested complex manifests are resolved
// recursively. Leaves that are not manifests are left as gids.
// Idempotent; returns the receiver.
//
// This runs automatically when a complex manifest is read through
// ``laila.Remember`` (pass ``resolve=False`` there to opt out).
func (c *ComplexManifest) Resolve(kw ...py.Kw) (any, error) {
	opts := py.MergeKw(kw...)
	bp, err := c.blueprint_dict_()
	if err != nil {
		return nil, err
	}
	if c.Resolved_ || bp == nil ||
		(len(c.Unresolved_manifest_leaves_()) == 0 && !c.has_unresolved_member_()) {
		c.Resolved_ = true
		return c.Self(), nil
	}

	var task py.CoroutineFunc = func(args ...any) (any, error) {
		_, err := py.Await(c.Async_resolve(opts))
		return nil, err
	}
	command, err := Command()
	if err != nil {
		return nil, err
	}
	ref, err := callm(command, "submit", py.NewList(task))
	if err != nil {
		return nil, err
	}
	defer release_(ref)
	if _, err := callm(ref, "wait"); err != nil {
		return nil, err
	}
	// ``ref.result`` re-raises the task's exception, if any.
	if _, err := py.Getattr(ref, "result"); err != nil {
		return nil, err
	}
	return c.Self(), nil
}

// ----- rebuild ---------------------------------------------------------

// Rebuild turns every member back into the live object it was made from
// (``rebuild(*, pool=None, pool_nickname=None, pool_id=None, device=None,
// context=None, persist=False)``).
//
// Resolves first when needed, then walks the blueprint in insertion
// order: a typed manifest exposing ``rebuild`` (a model, an optimizer, a
// nested complex manifest) is rebuilt with the shared *context* (``{gid:
// rebuilt object}``, so an optimizer finds the model whose parameters it
// drives); a plain manifest (a dataset) is returned as is; a plain entry
// leaf is fetched; nested dict / list fragments are realized to entries.
// Objects assigned locally are returned untouched. Results replace the
// members, so ``self[key]`` is the live object afterwards and a later
// Memorize re-snapshots it.
//
//   - pool, pool_nickname, pool_id: where to read from; defaults to the
//     pool this manifest came from (then the alpha pool, then every
//     registered pool).
//   - device: forwarded to members that take one (nil = CPU).
//   - context: ``{gid: live object}`` seed (*py.Dict), extended as
//     members are rebuilt.
//   - persist (default False): cache fetched entries into the alpha pool.
func (c *ComplexManifest) Rebuild(kw ...py.Kw) (*py.Dict, error) {
	opts := py.MergeKw(kw...)
	bp, err := c.blueprint_dict_()
	if err != nil {
		return nil, err
	}
	if bp == nil {
		return nil, py.RuntimeError.New("No blueprint to rebuild — manifest is empty.")
	}
	routed := Manifest_resolve_pool_(opts.Get("pool", nil), opts.Get("pool_nickname", nil), opts.Get("pool_id", nil))
	persist := py.Truthy(opts.Get("persist", false))
	device := opts.Get("device", nil)
	if _, err := c.Resolve(py.Kw{"pool": routed, "persist": persist}); err != nil {
		return nil, err
	}
	var context *py.Dict
	if raw := opts.Get("context", nil); py.IsNone(raw) {
		context = py.NewDict()
	} else if d, ok := asDict(raw); ok {
		context = d
	} else {
		return nil, py.TypeError.Newf("rebuild() context must be a dict, got %s", py.Pytypename(raw))
	}

	realized_any, err := c.Realized()
	if err != nil {
		return nil, err
	}
	realized, _ := asDict(realized_any)
	member_kw := py.Kw{"pool": routed, "device": device, "context": context, "persist": persist}

	out := py.NewDict()
	for _, it := range bp.Items() {
		key, value := it.Key, it.Value
		member := c.members_().GetDefault(key, nil)
		var built any
		switch {
		case py.IsNone(member):
			if built, err = realized.Getitem(key); err != nil {
				return nil, err
			}
		case As_complex_manifest_(member) != nil:
			// Rebuilt in place: the nested complex manifest stays the
			// member (same type as assigned), its own members go live.
			if _, err := As_complex_manifest_(member).Rebuild(member_kw); err != nil {
				return nil, err
			}
			built = member
		case py.Isinstance(member, Manifest_class) && py.Hasattr(member, "rebuild"):
			if built, err = callm(member, "rebuild", member_kw); err != nil {
				return nil, err
			}
		default:
			built = member
		}
		if gid, ok := value.(string); ok {
			if !context.Contains(gid) {
				context.Set(gid, built)
			}
			if py.Isinstance(member, Manifest_class) && !manifest_or_backed_(built) {
				// A bare object came back: keep a manifest-backed handle on
				// it so memorize / forget still work.
				if handle := getattr(member, "handle"); !py.IsNone(handle) {
					c.adapted_().Set(key, handle)
				}
			}
			c.members_().Set(key, built)
		}
		out.Set(key, built)
	}
	return out, nil
}

// Rebuilt is ``rebuild()`` with defaults.
func (c *ComplexManifest) Rebuilt() (*py.Dict, error) { return c.Rebuild() }

// ----- forget (owner-aware) ------------------------------------------

// Forget deletes the complex manifest and the members it owns from *pool*
// (``forget(*, pool=None, pool_nickname=None, pool_id=None,
// batch_size=128)``).
//
// Members that own their leaves (typed checkpoints, nested complex
// manifests -- ``_OWNS_CHILDREN``) are forgotten through their own Forget
// (leaves included); a plain manifest member (a dataset) is only dropped
// as a record, its entries are left alone; plain entry leaves are
// forgotten. Typed members are resolved first so their kind is known.
func (c *ComplexManifest) Forget(kw ...py.Kw) (*GroupFuture, error) {
	opts := py.MergeKw(kw...)
	bp, err := c.blueprint_dict_()
	if err != nil {
		return nil, err
	}
	if bp == nil {
		return nil, py.RuntimeError.New("No blueprint — nothing to forget.")
	}
	batch_size, err := manifest_batch_size_(opts)
	if err != nil {
		return nil, err
	}
	routed := Manifest_resolve_pool_(opts.Get("pool", nil), opts.Get("pool_nickname", nil), opts.Get("pool_id", nil))
	if _, err := c.Resolve(py.Kw{"pool": routed}); err != nil && !errors.Is(err, py.KeyError) {
		return nil, err
	} // already gone: fall through to a plain delete of the leaves
	policy, err := Get_active_policy()
	if err != nil {
		return nil, err
	}
	all_future_ids := []string{}
	plain := []any{}

	for _, it := range bp.Items() {
		key, value := it.Key, it.Value
		handle := c.Handle_(key)
		switch {
		case Is_manifest_backed_(handle) || (py.Isinstance(handle, Manifest_class) && Manifest_owns_children_(handle)):
			ref, err := manifest_bulk_op_(handle, "forget", py.Kw{"pool": routed, "batch_size": batch_size})
			if err != nil {
				return nil, err
			}
			ids, err := Manifest_collect_future_ids_(ref)
			if err != nil {
				return nil, err
			}
			all_future_ids = append(all_future_ids, ids...)
		case is_str_(value):
			plain = append(plain, value)
		case is_list_(value) || is_mapping_(value):
			plain = append(plain, Manifest_iter_global_ids_(py.DictOf(key, value))...)
		}
	}

	for i := 0; i < len(plain); i += batch_size {
		end := min(i+batch_size, len(plain))
		ref, err := Forget(py.NewList(plain[i:end]...), py.Kw{"pool": routed})
		if err != nil {
			return nil, err
		}
		ids, err := Manifest_collect_future_ids_(ref)
		if err != nil {
			return nil, err
		}
		all_future_ids = append(all_future_ids, ids...)
	}
	self_ref, err := Forget(c.Global_id(), py.Kw{"pool": routed})
	if err != nil {
		return nil, err
	}
	ids, err := Manifest_collect_future_ids_(self_ref)
	if err != nil {
		return nil, err
	}
	all_future_ids = append(all_future_ids, ids...)
	return New_GroupFuture(py.Kw{
		"taskforce_id": getattr(getattr(getattr(policy, "central"), "command"), "internal_taskforce"),
		"policy_id":    global_id_of_(policy),
		"future_ids":   all_future_ids,
	})
}

// ComplexManifest_build_from_dict_sync_ is
// ``ComplexManifest._build_from_dict_sync``.
func ComplexManifest_build_from_dict_sync_(in_dict any) (any, error) {
	return Entry_build_from_dict_of_(ComplexManifest_class, in_dict)
}

// ComplexManifest_build_from_dict_async_ is
// ``ComplexManifest._build_from_dict_async``.
func ComplexManifest_build_from_dict_async_(in_dict any) (any, error) {
	return Entry_build_from_dict_of_(ComplexManifest_class, in_dict)
}

// RealizedManifest_ is a Manifest whose referenced entries are already in
// hand.
//
// Used inside process-pool workers running a constitution body
// (``laila.build(entry, taskforce_id=<process pool>)``): the parent
// realizes the manifest and ships the entries along, so Realized /
// Async_realized return the pre-built nested dict instead of reading
// pools through a policy (there is none in the worker).
type RealizedManifest_ struct {
	Manifest
	Prebuilt_ *py.Dict `py:"_prebuilt"`
}

// RealizedManifest_class is the class descriptor of ``_RealizedManifest``.
var RealizedManifest_class = py.NewClass("_RealizedManifest", (*RealizedManifest_)(nil), Manifest_class)

// New_RealizedManifest_ is ``_RealizedManifest(**data)``.
func New_RealizedManifest_(kw ...py.Kw) (*RealizedManifest_, error) {
	m := RealizedManifest_class.New().(*RealizedManifest_)
	if err := m.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	if m.Prebuilt_ == nil {
		m.Prebuilt_ = py.NewDict()
	}
	return m, nil
}

// Realized returns the pre-built nested dict.
func (m *RealizedManifest_) Realized() (any, error) {
	if m.Prebuilt_ == nil {
		m.Prebuilt_ = py.NewDict()
	}
	return m.Prebuilt_, nil
}

// Async_realized is a coroutine resolving to the pre-built nested dict.
func (m *RealizedManifest_) Async_realized() py.Coroutine {
	return func() (any, error) { return m.Realized() }
}

// Manifest_build_from_dict_sync_ is ``Manifest._build_from_dict_sync``
// (the inherited Entry classmethod bound to Manifest).
func Manifest_build_from_dict_sync_(in_dict any) (any, error) {
	return Entry_build_from_dict_of_(Manifest_class, in_dict)
}

// Manifest_build_from_dict_async_ is ``Manifest._build_from_dict_async``.
func Manifest_build_from_dict_async_(in_dict any) (any, error) {
	return Entry_build_from_dict_of_(Manifest_class, in_dict)
}

// Manifest_from_dict is ``Manifest.from_dict(in_dict)``.
func Manifest_from_dict(in_dict *py.Dict) (*Manifest, error) {
	e, err := Entry_from_dict_of_(Manifest_class, in_dict)
	if err != nil {
		return nil, err
	}
	return As_manifest_(e.Self()), nil
}

func init() {
	py.Register_property("blueprint", "realized", "async_realized", "nickname", "members", "rebuilt")
	Register_builder(MANIFEST_SCOPE_,
		func(d *py.Dict, kw py.Kw) (any, error) { return Manifest_build_from_dict_sync_(d) },
		func(d *py.Dict, kw py.Kw) (any, error) { return Manifest_build_from_dict_async_(d) })
	Register_builder(MANIFEST_SCOPE_+":"+COMPLEX_MANIFEST_SCOPE_,
		func(d *py.Dict, kw py.Kw) (any, error) { return ComplexManifest_build_from_dict_sync_(d) },
		func(d *py.Dict, kw py.Kw) (any, error) { return ComplexManifest_build_from_dict_async_(d) })

	// Hooks the entry module needs from the manifest module.
	Manifest_iter_entries_ = func(realized any) ([]*Entry, error) {
		return Manifest_iter_entries_impl_(realized)
	}
	Rebuild_realized_manifest_ = func(blueprint any, manifest_uuid string, resolved *py.Dict) (any, error) {
		m, err := New_RealizedManifest_(py.Kw{"data": blueprint, "uuid": manifest_uuid})
		if err != nil {
			return nil, err
		}
		bp, err := m.blueprint_dict_()
		if err != nil {
			return nil, err
		}
		if bp == nil {
			bp = py.NewDict()
		}
		prebuilt, err := Manifest_rebuild_with_entries_(bp, resolved)
		if err != nil {
			return nil, err
		}
		m.Prebuilt_ = prebuilt
		return m, nil
	}
}
