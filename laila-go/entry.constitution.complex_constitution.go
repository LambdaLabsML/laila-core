package laila

import (
	"errors"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// entry/constitution/complex_constitution.py -- complex constitution: a
// single source string driven by a Manifest.
//
// A ComplexConstitution carries one source string defining a single
// callable ``f(manifest) -> payload``. At build time it resolves its bound
// Manifest from the active policy's memory (using the stored global_id if
// the live object is not on hand), forces every entry the manifest
// references to materialise inside a ``laila.guarantee`` scope, and
// applies the callable to the manifest.
//
// The serialised form embeds only the manifest's global_id, never its
// contents, so a build always sees the *current* state of the referenced
// entries.
//
// Port note: the source string is resolved through Exec_one_fn_, i.e. a
// Go builder registered with Register_constitution for that exact text.

// ComplexConstitution is a one-shot constitution function bound to a
// Manifest. Neither the code nor the manifest can be reassigned once set.
type ComplexConstitution struct {
	Constitution_
	Code_               *string `py:"_code"`
	Manifest_           any     `py:"_manifest"`
	Manifest_global_id_ *string `py:"_manifest_global_id"`
}

// ComplexConstitution_class is the class descriptor (kind "complex").
var ComplexConstitution_class = py.NewClass("ComplexConstitution", (*ComplexConstitution)(nil), Constitution_class)

func init() {
	Register_kind_("complex", ComplexConstitution_class, ComplexConstitution_from_dict_)
	py.Register_property("code", "manifest", "manifest_global_id")
}

// New_ComplexConstitution is ``ComplexConstitution(code=..., manifest=...)``
// or ``(code=..., manifest_global_id=...)``. A live manifest takes
// precedence over a bare gid. Omitting both is allowed; the constitution
// cannot be built until a manifest is attached.
func New_ComplexConstitution(kw ...py.Kw) (*ComplexConstitution, error) {
	c := ComplexConstitution_class.New().(*ComplexConstitution)
	data := py.MergeKw(kw...)
	if err := py.Init(c, ComplexConstitution_class, py.Kw{}); err != nil {
		return nil, err
	}
	if code := data.Get("code", nil); !py.IsNone(code) {
		if err := c.Set_code(code); err != nil {
			return nil, err
		}
	}
	if manifest := data.Get("manifest", nil); !py.IsNone(manifest) {
		if err := c.Set_manifest(manifest); err != nil {
			return nil, err
		}
	} else if gid := data.Get("manifest_global_id", nil); !py.IsNone(gid) {
		s := py.Str(gid)
		c.Manifest_global_id_ = &s
	}
	return c, nil
}

// Code is the single source string defining the constitution callable
// (nil when unset).
func (c *ComplexConstitution) Code() *string { return c.Code_ }

// Set_code assigns the constitution code, one time only. AttributeError
// if already set; TypeError unless value is a string.
func (c *ComplexConstitution) Set_code(value any) error {
	if c.Code_ != nil {
		return py.AttributeError.New("ComplexConstitution code is already set and cannot be reassigned.")
	}
	s, ok := value.(string)
	if !ok {
		return py.TypeError.New("code must be a Python source string")
	}
	c.Code_ = &s
	return nil
}

// Manifest is the bound Manifest, if available (nil right after
// deserialisation until the first resolve).
func (c *ComplexConstitution) Manifest() any { return c.Manifest_ }

// Set_manifest binds a live Manifest, one time only; records its
// global_id. AttributeError if already bound; TypeError unless value is a
// Manifest.
func (c *ComplexConstitution) Set_manifest(value any) error {
	if c.Manifest_ != nil {
		return py.AttributeError.New("ComplexConstitution manifest is already set and cannot be reassigned.")
	}
	manifestCls, _ := py.Class_by_name("Manifest")
	if manifestCls == nil || !py.Isinstance(value, manifestCls) {
		return py.TypeError.New("manifest must be a Manifest instance")
	}
	c.Manifest_ = value
	gid := py.Str(getattr(value, "global_id"))
	c.Manifest_global_id_ = &gid
	return nil
}

// Manifest_global_id is the bound manifest's global_id, if known.
func (c *ComplexConstitution) Manifest_global_id() *string { return c.Manifest_global_id_ }

// Manifest_candidate_pools_ lists the pools to try for the manifest: the
// hinted pool, alpha (nil), then the rest. ``[nil]`` when no policy is
// available.
func Manifest_candidate_pools_(pool_hint *string) []any {
	var known []string
	var alpha any
	ok := func() (ok bool) {
		defer func() {
			if recover() != nil {
				ok = false
			}
		}()
		policy, err := Get_active_policy()
		if err != nil {
			return false
		}
		memory := getattr(getattr(policy, "central"), "memory")
		if py.IsNone(memory) {
			return false
		}
		for _, it := range mappingItems(getattr(getattr(memory, "pool_router"), "pools")) {
			known = append(known, py.Str(it.Key))
		}
		alpha = getattr(memory, "alpha_pool")
		return true
	}()
	if !ok {
		return []any{nil}
	}
	order := []any{}
	hint := ""
	if pool_hint != nil {
		hint = *pool_hint
	}
	in := func(s string) bool {
		for _, k := range known {
			if k == s {
				return true
			}
		}
		return false
	}
	if pool_hint != nil && in(hint) {
		order = append(order, hint)
	}
	if pool_hint == nil || !py.Eq(hint, alpha) {
		order = append(order, nil)
	}
	for _, g := range known {
		if g != hint && !py.Eq(g, alpha) {
			order = append(order, g)
		}
	}
	return order
}

// resolve runs the remember loop shared by the sync and async paths; the
// ``wait`` function awaits one future.
func (c *ComplexConstitution) resolve(pool_hint *string, wait func(ref any) (any, error)) (any, error) {
	if c.Manifest_ != nil {
		return c.Manifest_, nil
	}
	if c.Manifest_global_id_ == nil {
		return nil, nil
	}
	var last_exc error
	for _, pool := range Manifest_candidate_pools_(pool_hint) {
		ref, err := Remember(*c.Manifest_global_id_, py.Kw{"dst_pool": pool})
		if err != nil {
			return nil, err
		}
		resolved, err := wait(ref)
		// Internal, self-consumed future: release it from the bank.
		_, _ = callm(ref, "release")
		if err != nil {
			if errors.Is(err, py.KeyError) {
				last_exc = err
				continue
			}
			return nil, err
		}
		return c.Record_resolved_manifest_(resolved)
	}
	return nil, py.KeyError.Newf("Could not resolve manifest %s from any registered pool",
		py.Repr(*c.Manifest_global_id_)).From(last_exc)
}

// Resolve_manifest_sync_ synchronously resolves the manifest from its
// global_id (cache hit when already bound; nil when neither a manifest nor
// a gid is bound). Blocks on ``laila.remember(...).wait()``.
func (c *ComplexConstitution) Resolve_manifest_sync_(pool_hint *string) (any, error) {
	return c.resolve(pool_hint, func(ref any) (any, error) { return callm(ref, "wait", nil) })
}

// Resolve_manifest_async_ is the coroutine variant: awaits the remember
// future instead of blocking on wait().
func (c *ComplexConstitution) Resolve_manifest_async_(pool_hint *string) (any, error) {
	return c.resolve(pool_hint, func(ref any) (any, error) {
		if aw, ok := ref.(interface{ Await() (any, error) }); ok {
			return aw.Await()
		}
		return callm(ref, "wait", nil)
	})
}

// Resolve_manifest_ dispatches on ``asynchronous``.
func (c *ComplexConstitution) Resolve_manifest_(kw ...py.Kw) (any, error) {
	opts := py.MergeKw(kw...)
	var hint *string
	if h := opts.Get("pool_hint", nil); !py.IsNone(h) {
		s := py.Str(h)
		hint = &s
	}
	if py.Truthy(opts.Get("asynchronous", false)) {
		return c.Resolve_manifest_async_(hint)
	}
	return c.Resolve_manifest_sync_(hint)
}

// Record_resolved_manifest_ caches the resolved manifest and returns it.
// A single-element list collapses to its element. RuntimeError when the
// lookup returned nil.
func (c *ComplexConstitution) Record_resolved_manifest_(resolved any) (any, error) {
	switch x := resolved.(type) {
	case *py.List:
		if x.Len() > 0 {
			resolved, _ = x.Get(0)
		} else {
			resolved = nil
		}
	case []any:
		if len(x) > 0 {
			resolved = x[0]
		} else {
			resolved = nil
		}
	}
	if py.IsNone(resolved) {
		gid := ""
		if c.Manifest_global_id_ != nil {
			gid = *c.Manifest_global_id_
		}
		return nil, py.RuntimeError.Newf(
			"Could not resolve manifest from global_id %s: not found in active memory.", py.Repr(gid))
	}
	c.Manifest_ = resolved
	return c.Manifest_, nil
}

// Build resolves the manifest and applies the single constitution
// callable; payload_input is ignored. RuntimeError when no code is set or
// the manifest cannot be resolved.
func (c *ComplexConstitution) Build(payload_input any) (any, error) {
	if c.Code_ == nil {
		return nil, py.RuntimeError.New("no constitution code defined.")
	}
	target, err := c.Resolve_manifest_sync_(nil)
	if err != nil {
		return nil, err
	}
	if py.IsNone(target) {
		return nil, py.RuntimeError.New("no manifest available to run constitution.")
	}
	if err := Guarantee.With(func() error {
		_, err := py.Getattr(target, "realized")
		return err
	}); err != nil {
		return nil, err
	}
	fn, err := Exec_one_fn_(*c.Code_)
	if err != nil {
		return nil, err
	}
	return fn(target)
}

// As_dict serialises as ``{"_kind": "complex", "code", "manifest_global_id",
// "_hmac"}``; ``_hmac`` is the HMAC-SHA256 over the canonical code /
// manifest pair keyed by the per-user constitution secret.
func (c *ComplexConstitution) As_dict() (*py.Dict, error) {
	var code, gid any
	if c.Code_ != nil {
		code = *c.Code_
	}
	if c.Manifest_global_id_ != nil {
		gid = *c.Manifest_global_id_
	}
	return py.DictOf(
		"_kind", "complex",
		"code", code,
		"manifest_global_id", gid,
		"_hmac", Sign_complex(code, gid),
	), nil
}

// ComplexConstitution_from_dict_ rebuilds from the serialised dict.
// ValueError if the ``_hmac`` signature is missing or does not verify.
func ComplexConstitution_from_dict_(in_dict *py.Dict) (Constitution, error) {
	code := in_dict.GetDefault("code", nil)
	gid := in_dict.GetDefault("manifest_global_id", nil)
	if !Verify_complex(code, gid, in_dict.GetDefault("_hmac", nil)) {
		return nil, py.ValueError.New(
			"untrusted constitution: complex constitution signature missing or invalid " +
				"(written by a process without this user's ~/.laila/constitution.key)")
	}
	return New_ComplexConstitution(py.Kw{"code": code, "manifest_global_id": gid})
}