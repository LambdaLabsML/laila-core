package laila

import (
	"strings"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// entry/constitution/build_maps.py -- scope-based build dispatch for
// serialised entries.
//
// Different Entry subclasses (Entry, Manifest, user subclasses) need
// different deserialisation logic. Each registers a builder pair
// ``(sync_fn, async_fn)`` keyed by its scope string in BUILDER_MAP, and
// Build_by_scope reads the ``_scopes`` field of a serialised dict to look
// up the right builder.

// Entry_builder hydrates a serialised dict into an Entry.
type Entry_builder func(in_dict *py.Dict, kw py.Kw) (any, error)

// Builder_pair is ``(sync_builder, async_builder)``.
type Builder_pair struct {
	Sync  Entry_builder
	Async Entry_builder
}

// BUILDER_MAP is scope -> builder pair.
var BUILDER_MAP = map[string]Builder_pair{}

// Register_builder registers a (sync, async) builder pair for scope.
// Re-registering silently overwrites the previous pair.
//
// ``scope`` is the subclass's ``_scopes`` joined with ``":"`` -- usually
// just the first element (``"ENTRY"``, ``"MANIFEST"``), or a longer key
// for a typed subclass that refines a generic one (``"MANIFEST:COMPLEX"``,
// ``"MANIFEST:TORCH_MODULE"``). Build_by_scope tries the most specific
// key first and falls back to shorter prefixes, so readers that only know
// the generic scope still hydrate the refined records.
func Register_builder(scope string, sync_fn Entry_builder, async_fn Entry_builder) {
	BUILDER_MAP[scope] = Builder_pair{Sync: sync_fn, Async: async_fn}
}

// Lookup_builder is the builder pair for *scopes*: the longest registered
// prefix wins (``lookup_builder(scopes)``).
//
// ``["MANIFEST", "TORCH_MODULE"]`` resolves to the builder registered
// under ``"MANIFEST:TORCH_MODULE"`` when there is one and to the plain
// ``"MANIFEST"`` builder otherwise, so a refined scope never breaks a
// reader that predates it. Returns ``(pair, false)`` when no prefix is
// known. A nil / empty *scopes* means ``["ENTRY"]``.
func Lookup_builder(scopes []string) (Builder_pair, bool) {
	if len(scopes) == 0 {
		scopes = []string{ENTRY_SCOPE_}
	}
	for n := len(scopes); n > 0; n-- {
		if pair, ok := BUILDER_MAP[strings.Join(scopes[:n], ":")]; ok {
			return pair, true
		}
	}
	return Builder_pair{}, false
}

// Build_by_scope dispatches hydration based on the ``_scopes`` field of
// in_dict. Accepted inputs: a dict (scopes looked up through
// Lookup_builder -- most specific joined prefix first, then shorter ones;
// "ENTRY" when absent), a JSON string (parsed first), or a live Entry
// (returned as-is). Keyword ``asynchronous`` (default false) selects the
// async builder; in the port both builders run to completion on the
// calling goroutine. ValueError when the scope has no builder;
// RuntimeError for other inputs.
func Build_by_scope(in_dict any, kw ...py.Kw) (any, error) {
	opts := py.MergeKw(kw...)
	asynchronous := py.Truthy(opts.Pop("asynchronous", false))
	var d *py.Dict
	switch x := in_dict.(type) {
	case *py.Dict:
		d = x
	case py.Kw, map[string]any, *py.DotMap:
		d, _ = As_dict_(x)
	case string:
		parsed, err := py.Json_loads(x)
		if err != nil {
			return nil, err
		}
		pd, ok := parsed.(*py.Dict)
		if !ok {
			return nil, py.RuntimeError.New("Invalid input for entry build.")
		}
		d = pd
	default:
		if py.Isinstance(in_dict, Entry_class) {
			return in_dict, nil
		}
		return nil, py.RuntimeError.New("Invalid input for entry build.")
	}
	scopes := []string{ENTRY_SCOPE_}
	if raw, ok := d.Get("_scopes"); ok && !py.IsNone(raw) {
		if items := py.Iter(raw); len(items) > 0 {
			scopes = scopes[:0]
			for _, s := range items {
				scopes = append(scopes, py.Str(s))
			}
		}
	}
	pair, ok := Lookup_builder(scopes)
	if !ok {
		keys := make([]any, 0, len(BUILDER_MAP))
		for _, k := range sortedKeys(BUILDER_MAP) {
			keys = append(keys, k)
		}
		return nil, py.ValueError.Newf("No builder registered for scope '%s'. Registered scopes: %s",
			strings.Join(scopes, ":"), py.Repr(py.NewList(keys...)))
	}
	fn := pair.Sync
	if asynchronous {
		fn = pair.Async
	}
	return fn(d, opts)
}
