package laila

import (
	"errors"
	"reflect"
	"strings"
	"sync"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// atomic/definitions/globally_atomic_identifiable_object.py -- globally-
// atomic identifiable object with secret-gated locking.
//
// Extends _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT with a *global*
// (policy-scoped) lock layered on top of the local one. The global lock is
// bound to a single ``policy_id`` while it is held; a secret token returned
// from Lock_global is the only key that can release the lock or pass
// through the secret-gated attribute wrapper.
//
// Gate semantics
//
// Only *public* callables defined by a subclass (``bump``, ``write``, ...)
// are gated. Private / dunder names and everything defined on this class or
// its bases (``lock_global``, ``atomic``, ``model_dump``, ...) are never
// wrapped. While no global lock is held the gate is a cheap name check.
//
// While a global lock is held, a gated call passes when either the
// ``secret=`` keyword matches the holder token or the call happens inside
// the holder's own ``atomic(scope="global")`` block (the secret travels in
// a ContextVar). Otherwise PermissionError is raised.
//
// Port note: Python installs the gate through ``__getattribute__``. Go has
// no attribute interception for direct method calls, so the gate is
// reachable two ways that behave identically:
//
//   - dynamic dispatch (py.Getattr / RPC): Getattribute wraps public
//     subclass methods in a Gated_method exactly like Python;
//   - direct Go calls: a subclass's public method calls
//     ``if err := o.Gate_("bump", kw); err != nil { return ..., err }``
//     first, which is the body of Python's ``_gated`` wrapper.

// HELD_SECRET_ is the secret of the global lock held by the *current*
// context (set inside ``atomic(scope="global")`` so the holder's body
// passes the gate).
var HELD_SECRET_ = py.NewContextVar[*string]("laila_global_lock_secret", nil)

// LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_ is an identifiable object that
// coordinates access via a policy-scoped global lock.
type LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_ struct {
	LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_
	// Global_lock_ is ``_global_lock``: whether the lock is currently held.
	Global_lock_ bool `py:"_global_lock"`
	// Holder_policy_id_ is ``_holder_policy_id``: which policy holds it.
	Holder_policy_id_ *string `py:"_holder_policy_id"`
	// Holder_secret_ is ``_holder_secret``: the token proving you're the holder.
	Holder_secret_ *string `py:"_holder_secret"`
	// Hold_count_ is ``_hold_count``: nested acquisitions by the holder.
	Hold_count_ int `py:"_hold_count"`
}

// LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_class is the class descriptor.
var LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_class = py.NewClass(
	"_LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT",
	(*LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_)(nil),
	LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_class,
)

// New_LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_ is the constructor.
func New_LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_(kw ...py.Kw) (*LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_, error) {
	o := LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_class.New().(*LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_)
	if err := o.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return o, nil
}

// ungated_names_ is ``_ungated_names``: the names that are never gated --
// everything this class and its bases define (lock machinery, pydantic
// API, identity helpers). Built lazily; subclass additions *are* gated.
var (
	ungated_once_  sync.Once
	ungated_names_ map[string]bool
)

// Ungated_ is ``cls._ungated()``: the frozenset of never-gated names.
func Ungated_() map[string]bool {
	ungated_once_.Do(func() {
		collected := map[string]bool{}
		t := reflect.TypeOf((*LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_)(nil))
		for i := 0; i < t.NumMethod(); i++ {
			name := t.Method(i).Name
			collected[name] = true
			collected[py.Pyname(name)] = true
		}
		for _, f := range LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_class.Fields() {
			collected[f.Name] = true
			collected[f.GoName] = true
		}
		for _, name := range []string{
			"model_dump", "model_dump_json", "model_copy", "model_validate", "model_construct",
			"model_fields", "model_post_init", "model_config", "dict", "json", "copy",
		} {
			collected[name] = true
		}
		ungated_names_ = collected
	})
	return ungated_names_
}

// Lock_global acquires (or re-acquires) the global lock for a given
// policy (``lock_global(policy_id, *, timeout_s=None)``).
//
// Returns a secret token on success (the *same* token, with the hold count
// incremented, when the holder re-acquires), nil if another policy holds
// the lock. TimeoutError if the underlying local lock cannot be acquired
// within timeout_s.
func (o *LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_) Lock_global(policy_id string, kw ...py.Kw) (*string, error) {
	opts := py.MergeKw(kw...)
	_, release, err := o.Atomic_(o.Self(), py.Kw{"scope": "local", "timeout_s": opts.Get("timeout_s", nil)})
	if err != nil {
		return nil, err
	}
	defer release()
	if o.Global_lock_ {
		if o.Holder_policy_id_ != nil && *o.Holder_policy_id_ == policy_id && o.Holder_secret_ != nil {
			o.Hold_count_ += 1
			return o.Holder_secret_, nil
		}
		return nil, nil
	}
	o.Global_lock_ = true
	o.Holder_policy_id_ = py.Ptr(policy_id)
	o.Holder_secret_ = py.Ptr(py.Token_hex(8))
	o.Hold_count_ = 1
	return o.Holder_secret_, nil
}

// Unlock_global releases one hold on the global lock if *secret* matches
// the holder token. The lock is released for real when the hold count
// reaches zero; a wrong (or nil) secret is a silent no-op.
func (o *LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_) Unlock_global(secret *string) {
	_, release, err := o.Atomic_(o.Self(), py.Kw{"scope": "local"})
	if err != nil {
		return
	}
	defer release()
	if secret == nil || o.Holder_secret_ == nil || *secret != *o.Holder_secret_ {
		return
	}
	o.Hold_count_ -= 1
	if o.Hold_count_ > 0 {
		return
	}
	o.Global_lock_ = false
	o.Holder_policy_id_ = nil
	o.Holder_secret_ = nil
	o.Hold_count_ = 0
}

// Global_locked returns true if the global lock is held (or the local
// lock times out) (``global_locked(*, timeout_s=None)``).
func (o *LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_) Global_locked(kw ...py.Kw) bool {
	opts := py.MergeKw(kw...)
	_, release, err := o.Atomic_(o.Self(), py.Kw{"scope": "local", "timeout_s": opts.Get("timeout_s", nil)})
	if errors.Is(err, py.TimeoutError) {
		return true
	}
	defer release()
	return o.Global_lock_
}

// Verify_secret_ is ``_verify_secret(secret, *, timeout_s=None)``: true
// when *secret* (or the context's held secret) matches the holder token,
// or when no global lock is held; false on a local-lock timeout.
func (o *LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_) Verify_secret_(secret *string, kw ...py.Kw) bool {
	opts := py.MergeKw(kw...)
	_, release, err := o.Atomic_(o.Self(), py.Kw{"scope": "local", "timeout_s": opts.Get("timeout_s", nil)})
	if err != nil {
		return false
	}
	defer release()
	if !o.Global_lock_ {
		return true
	}
	if secret != nil && o.Holder_secret_ != nil && *secret == *o.Holder_secret_ {
		return true
	}
	held := HELD_SECRET_.GetOr(nil)
	return held != nil && o.Holder_secret_ != nil && *held == *o.Holder_secret_
}

// Atomic is the context manager for local or global atomic sections
// (``atomic(*, scope="local", timeout_s=None, policy_id=None)``).
//
// scope "local" takes the local lock; "global" takes the policy-scoped
// global lock for the block and, inside it, the holder's own calls pass
// the gate without ``secret=``.
//
// ValueError if scope is invalid or policy_id is missing for global scope;
// PermissionError if the global lock is already held by another policy.
func (o *LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_) Atomic(kw ...py.Kw) (any, func(), error) {
	opts := py.MergeKw(kw...)
	scope := py.Str(opts.Get("scope", "local"))
	timeout_s := opts.Get("timeout_s", nil)
	if scope == "local" {
		return o.Atomic_(o.Self(), py.Kw{"scope": "local", "timeout_s": timeout_s})
	}
	if scope != "global" {
		return nil, nil, py.ValueError.New("Invalid scope for _LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT.atomic() call.")
	}
	policy_id := opts.Get("policy_id", nil)
	if py.IsNone(policy_id) {
		return nil, nil, py.ValueError.New("policy_id is required for global atomic scope.")
	}
	secret, err := o.Lock_global(py.Str(policy_id), py.Kw{"timeout_s": timeout_s})
	if err != nil {
		return nil, nil, err
	}
	if secret == nil {
		return nil, nil, py.PermissionError.New("Global lock already held by another policy.")
	}
	token := HELD_SECRET_.Set(secret)
	release := func() {
		_ = HELD_SECRET_.Reset(token)
		o.Unlock_global(secret)
	}
	return o.Self(), release, nil
}

// Is_gated_ reports whether attribute *name* is subject to the secret gate
// right now: a public callable not defined by this class or its bases,
// while a global lock is held. Mirrors the cheap checks at the top of
// Python's ``__getattribute__``; the "benign unlocked read" of
// ``_global_lock`` is done with a non-blocking try-lock so a concurrent
// holder of the local lock makes the gate conservative (it defers to
// Verify_secret_, which decides under the lock) instead of racing.
func (o *LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_) Is_gated_(name string) bool {
	if strings.HasPrefix(name, "_") || Ungated_()[name] {
		return false
	}
	if !o.Lock(0) {
		return true
	}
	locked := o.Global_lock_
	o.Unlock()
	return locked
}

// Gate_ is the body of Python's ``_gated`` wrapper for direct Go calls:
// a public subclass method calls it first with its own name and keyword
// arguments. When the gate applies, ``secret`` is popped from kw and
// verified; PermissionError is returned on a failed check.
func (o *LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_) Gate_(name string, kw py.Kw) error {
	if !o.Is_gated_(name) {
		return nil
	}
	var secret *string
	if s := kw.Pop("secret", nil); !py.IsNone(s) {
		secret = py.Ptr(py.Str(s))
	}
	if !o.Verify_secret_(secret) {
		return o.permission_error_(name)
	}
	return nil
}

func (o *LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_) permission_error_(name string) error {
	var holder any
	if o.Holder_policy_id_ != nil {
		holder = *o.Holder_policy_id_
	}
	return py.PermissionError.Newf(
		"%s.%s() is globally locked by policy %s; pass secret=<token> or call it inside the holder's atomic(scope='global') block.",
		o.Class_().Name, name, py.Repr(holder),
	)
}

// Getattribute is ``__getattribute__``: gates public subclass callables
// behind the holder secret while globally locked (dynamic dispatch path).
func (o *LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_) Getattribute(name string, attr any) (any, error) {
	if strings.HasPrefix(name, "_") || !py.Callable(attr) {
		return attr, nil
	}
	if !o.Is_gated_(name) {
		return attr, nil
	}
	return &Gated_method{Owner: o, Name: name, Attr: attr}, nil
}

// Gated_method is the ``_gated`` closure: a callable that verifies the
// secret (``secret=`` kwarg or the holder's context) before delegating.
type Gated_method struct {
	Owner *LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_
	Name  string
	Attr  any
}

// Call verifies the secret then invokes the wrapped attribute.
func (g *Gated_method) Call(args []any, kw py.Kw) (any, error) {
	kw = kw.Copy()
	var secret *string
	if s := kw.Pop("secret", nil); !py.IsNone(s) {
		secret = py.Ptr(py.Str(s))
	}
	if !g.Owner.Verify_secret_(secret) {
		return nil, g.Owner.permission_error_(g.Name)
	}
	// Python calls the raw attribute here (the wrapper *is* the gate). The
	// Go method body runs its own Gate_ for direct calls, so publish the
	// verified holder secret in the context for the duration of the call:
	// the inner check then passes exactly like the Python body would.
	if secret != nil && HELD_SECRET_.GetOr(nil) == nil {
		token := HELD_SECRET_.Set(secret)
		defer func() { _ = HELD_SECRET_.Reset(token) }()
	}
	return py.Call(g.Attr, args, kw)
}
