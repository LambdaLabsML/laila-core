package laila

import (
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// entry/constitution/constitution.py -- abstract Constitution base class:
// the recipe for materialising an Entry's payload.
//
// A constitution tells the build pipeline how to recover (or compute) an
// entry's payload from upstream input. Constitutions are first-class,
// serialisable objects so an entry can be persisted with its build recipe
// intact and re-executed in another process.
//
//   - SimpleConstitution: an ordered list of source strings, each defining
//     one callable; Build threads payload_input through the chain. The
//     canonical use is the inverse transformation chain emitted by
//     Entry.Serialize.
//   - ComplexConstitution: a single source string ``f(manifest) ->
//     payload`` plus a Manifest (or its global_id).
//
// Port note -- executing source. Go cannot ``exec`` Python. Exec_one_fn_
// resolves a code string in two ways: (1) a Go builder registered for
// that exact source text (Register_constitution: the text stays the
// stable identity that is signed and stored, the Go function is the
// implementation); (2) a recovery-code snippet recognised by a loaded
// transformation's template (Compile_backward_code_). Anything else is a
// ValueError, so arbitrary Python from storage or the wire never runs.
// Cross-language *complex* constitutions are unsupported (as in the C/JS
// ports); they fail at the HMAC gate first.

// Constitution is the instance protocol shared by every constitution.
type Constitution interface {
	Build(payload_input any) (any, error)
	As_dict() (*py.Dict, error)
	Class_() *py.Class
	// Kind_ is the class's ``_kind`` tag.
	Kind_() string
}

// Constitution_REGISTRY_ is ``_REGISTRY``: ``_kind`` tag -> class.
var Constitution_REGISTRY_ = map[string]*py.Class{}

// Constitution_from_dict_hooks_ holds each kind's ``_from_dict``.
var Constitution_from_dict_hooks_ = map[string]func(in_dict *py.Dict) (Constitution, error){}

// Register_kind_ is ``@_register_kind(kind)``: registers cls under kind,
// stamps the tag on the class and records its ``_from_dict`` hook.
func Register_kind_(kind string, cls *py.Class, from_dict func(in_dict *py.Dict) (Constitution, error)) *py.Class {
	Constitution_REGISTRY_[kind] = cls
	Constitution_from_dict_hooks_[kind] = from_dict
	cls.Set_classvar("_kind", kind)
	return cls
}

// ---------------------------------------------------------------------------
// Builder registry (the port's ``exec`` replacement for user code)
// ---------------------------------------------------------------------------

// Constitution_builder is a Go implementation of a constitution callable:
// ``f(manifest) -> payload`` for complex constitutions, ``f(value) ->
// value`` for hand-written simple steps.
type Constitution_builder func(arg any) (any, error)

var (
	constitution_builders_lock_ sync.RWMutex
	constitution_builders_      = map[string]Constitution_builder{}
)

// Register_constitution binds a Go builder to a constitution source text.
// The text is what gets stored, signed and compared (so it must be the
// same string on every process that wants to build the entry); fn is what
// runs. Re-registering replaces the previous builder.
func Register_constitution(code string, fn Constitution_builder) {
	constitution_builders_lock_.Lock()
	constitution_builders_[code] = fn
	constitution_builders_lock_.Unlock()
}

// Unregister_constitution removes a builder; false when none was bound.
func Unregister_constitution(code string) bool {
	constitution_builders_lock_.Lock()
	defer constitution_builders_lock_.Unlock()
	if _, ok := constitution_builders_[code]; !ok {
		return false
	}
	delete(constitution_builders_, code)
	return true
}

// Registered_constitution_codes returns the bound source texts (sorted).
func Registered_constitution_codes() []string {
	constitution_builders_lock_.RLock()
	defer constitution_builders_lock_.RUnlock()
	out := make([]string, 0, len(constitution_builders_))
	for k := range constitution_builders_ {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

var def_count_re_ = regexp.MustCompile(`(?m)^(?:def|class)\s+[A-Za-z_]`)

// Exec_one_fn_ is ``_exec_one_fn(code)``: resolve a source string to its
// single callable. Resolution order: a registered Go builder for the exact
// text, then a recognised recovery-code template. ValueError otherwise --
// including the Python-side "must define exactly one callable" rule for
// texts that visibly define zero or several top-level callables.
func Exec_one_fn_(code string) (Constitution_builder, error) {
	constitution_builders_lock_.RLock()
	fn, ok := constitution_builders_[code]
	constitution_builders_lock_.RUnlock()
	if ok {
		return fn, nil
	}
	native, recognised, err := Compile_backward_code_(code)
	if err != nil {
		return nil, err
	}
	if recognised {
		return Constitution_builder(native), nil
	}
	if n := len(def_count_re_.FindAllStringIndex(code, -1)); n != 1 {
		return nil, py.ValueError.New("constitution code must define exactly one callable function")
	}
	head := strings.SplitN(strings.TrimSpace(code), "\n", 2)[0]
	return nil, py.ValueError.Newf(
		"constitution code is not a recognised laila recovery snippet and no Go builder is "+
			"registered for it (laila.Register_constitution); laila-go cannot execute arbitrary Python (%s)",
		py.Repr(head))
}

// Constitution_ is the abstract base for entry constitutions.
type Constitution_ struct {
	py.Object
}

// Constitution_class is the class descriptor of Constitution (abstract).
var Constitution_class = func() *py.Class {
	c := py.NewClass("Constitution", (*Constitution_)(nil)).Set_classvar("_kind", "")
	c.Abstract = true
	return c
}()

// Kind_ is ``cls._kind``.
func (c *Constitution_) Kind_() string {
	if v, ok := c.Class_().Classvar("_kind"); ok {
		return v.(string)
	}
	return ""
}

// Constitution_from_dict is ``Constitution.from_dict(in_dict)``: reads the
// ``_kind`` tag, looks the subclass up in the registry and delegates to its
// ``_from_dict``. nil in -> nil out. KeyError when the tag is missing;
// ValueError when the kind is not registered.
func Constitution_from_dict(in_dict any) (Constitution, error) {
	if py.IsNone(in_dict) {
		return nil, nil
	}
	d, err := As_dict_(in_dict)
	if err != nil {
		return nil, err
	}
	kind, ok := d.Get("_kind")
	if !ok {
		return nil, py.KeyError.New("serialized constitution is missing the '_kind' tag")
	}
	hook, ok := Constitution_from_dict_hooks_[py.Str(kind)]
	if !ok {
		keys := make([]any, 0, len(Constitution_REGISTRY_))
		for _, k := range sortedKeys(Constitution_REGISTRY_) {
			keys = append(keys, k)
		}
		return nil, py.ValueError.Newf(
			"no Constitution subclass registered for kind %s. Registered: %s",
			py.Repr(kind), py.Repr(py.NewList(keys...)))
	}
	return hook(d)
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// As_dict_ views a mapping value (``*py.Dict``, ``py.Kw``,
// ``map[string]any``, ``*py.DotMap``) as a *py.Dict; TypeError otherwise.
func As_dict_(v any) (*py.Dict, error) {
	switch x := v.(type) {
	case *py.Dict:
		return x, nil
	case py.Kw:
		return py.DictFromKw(x), nil
	case map[string]any:
		return py.DictFromMap(x), nil
	case *py.DotMap:
		return x.ToDict(), nil
	}
	return nil, py.TypeError.Newf("expected a dict, got %s", py.Pytypename(v))
}
