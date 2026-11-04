package laila

import "github.com/amirzadehlambda/laila/laila-go/py"

// macros/aliases.py -- convenience aliases re-exported at the laila
// package level.
//
// These short names are the public entry points users actually reach for
// in everyday code:
//
//   - Constant   -- alias for Entry.constant. Build an unversioned entry
//     whose identity does not change as the payload is rewritten.
//   - Variable   -- alias for Entry.variable. Build an evolution-tracking
//     entry whose identity bumps on each rewrite.
//   - Contingent -- alias for Entry.contingent. Escape hatch that forwards
//     raw keyword arguments straight to the Entry constructor, bypassing
//     the consistency checks done by Entry.constant / Entry.variable.
//   - Future     -- alias for the Future class (``laila.future``).

// Constant is ``laila.constant(data, *, global_id=None, uuid=None,
// nickname=None)`` (Entry.constant).
func Constant(data any, kw ...py.Kw) (*Entry, error) { return Entry_constant(data, kw...) }

// Variable is ``laila.variable(data=None, *, uuid=None, evolution=None,
// state=None, constitution=None, manifest=None, global_id=None,
// nickname=None)`` (Entry.variable). ``data`` is accepted both
// positionally and as the ``data`` keyword.
func Variable(args ...any) (*Entry, error) {
	pos, kw := py.SplitKw(args)
	var data any
	if len(pos) > 1 {
		return nil, py.TypeError.Newf("variable() takes at most 1 positional argument (%d given)", len(pos))
	}
	if len(pos) == 1 {
		data = pos[0]
	} else {
		data = kw.Pop("data", nil)
	}
	return Entry_variable(data, kw)
}

// Contingent is ``laila.contingent(**kwargs)`` (Entry.contingent).
func Contingent(kw ...py.Kw) (*Entry, error) { return Entry_contingent(kw...) }

// Future_alias is ``laila.future``: the Future class descriptor. (The
// Future type itself is declared in
// policy.central.command.schema.future.future.future.go; in Go the alias
// is the type, so this only exposes the descriptor for dynamic code.)
func Future_alias() *py.Class { return Future_class }
