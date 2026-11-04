package py

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// glsState is everything stored per goroutine. Only the owning goroutine
// ever reads or writes its state; vars is treated as immutable (Set
// replaces the map) so a child goroutine may safely inherit the pointer.
type glsState struct {
	vars   map[any]any            // contextvars (inherited by py.Go, like copy_context)
	locals map[*Local]map[string]any // threading.local (never inherited)
	thread *Thread
	task   *Task
	loop   *Loop
}

// gls is a sharded goid -> *glsState registry. Goroutine ids are assigned
// sequentially so a low-bit shard index spreads neighbours across shards;
// each shard is a plain map under an RWMutex (cheaper than sync.Map for
// the read-mostly, integer-keyed access pattern on the hot path).
const glsShards = 256

type glsShard struct {
	mu sync.RWMutex
	m  map[int64]*glsState
	_  [40]byte // pad to a cache line
}

var gls [glsShards]glsShard

func init() {
	for i := range gls {
		gls[i].m = map[int64]*glsState{}
	}
}

func glsGet() *glsState {
	id := Goid()
	sh := &gls[id&(glsShards-1)]
	sh.mu.RLock()
	s := sh.m[id]
	sh.mu.RUnlock()
	return s
}

func glsEnsure() *glsState {
	id := Goid()
	sh := &gls[id&(glsShards-1)]
	sh.mu.RLock()
	s := sh.m[id]
	sh.mu.RUnlock()
	if s != nil {
		return s
	}
	sh.mu.Lock()
	if s = sh.m[id]; s == nil {
		s = &glsState{vars: map[any]any{}}
		sh.m[id] = s
	}
	sh.mu.Unlock()
	return s
}

// glsClear drops the current goroutine's state (called on goroutine exit
// by py.Go; goroutine ids are never reused so the entry would leak).
func glsClear() {
	id := Goid()
	sh := &gls[id&(glsShards-1)]
	sh.mu.Lock()
	delete(sh.m, id)
	sh.mu.Unlock()
}

// ContextVar is contextvars.ContextVar.
type ContextVar[T any] struct {
	name   string
	def    T
	hasDef bool
}

// NewContextVar declares a variable; an optional default may be given.
func NewContextVar[T any](name string, def ...T) *ContextVar[T] {
	c := &ContextVar[T]{name: name}
	if len(def) > 0 {
		c.def, c.hasDef = def[0], true
	}
	return c
}

// Name returns the variable name.
func (c *ContextVar[T]) Name() string { return c.name }

// Get returns the value in the current context, the default, or raises
// LookupError exactly like ContextVar.get().
func (c *ContextVar[T]) Get() (T, error) {
	if s := glsGet(); s != nil {
		if v, ok := s.vars[c]; ok {
			return v.(T), nil
		}
	}
	if c.hasDef {
		return c.def, nil
	}
	var zero T
	return zero, LookupError.New(c)
}

// GetOr is ContextVar.get(default).
func (c *ContextVar[T]) GetOr(def T) T {
	if s := glsGet(); s != nil {
		if v, ok := s.vars[c]; ok {
			return v.(T)
		}
	}
	return def
}

// Value is Get() for variables that always have a default (panics otherwise).
func (c *ContextVar[T]) Value() T { return Must(c.Get()) }

// Token is the value returned by Set and consumed by Reset.
type Token struct {
	v    any
	old  any
	had  bool
	goid int64
	used atomic.Bool
}

// Set assigns the value in the current context and returns a token.
func (c *ContextVar[T]) Set(v T) *Token {
	s := glsEnsure()
	old, had := s.vars[c]
	nv := make(map[any]any, len(s.vars)+1)
	for k, x := range s.vars {
		nv[k] = x
	}
	nv[c] = v
	s.vars = nv
	return &Token{v: c, old: old, had: had, goid: Goid()}
}

// Reset restores the value the variable had before the token's Set.
func (c *ContextVar[T]) Reset(t *Token) error {
	if t.v != any(c) {
		return ValueError.Newf("%s was created by a different ContextVar", t.Repr())
	}
	if t.goid != Goid() {
		return ValueError.Newf("%s was created in a different Context", t.Repr())
	}
	if !t.used.CompareAndSwap(false, true) {
		return RuntimeError.Newf("%s has already been used once", t.Repr())
	}
	s := glsEnsure()
	nv := make(map[any]any, len(s.vars))
	for k, x := range s.vars {
		nv[k] = x
	}
	if t.had {
		nv[c] = t.old
	} else {
		delete(nv, c)
	}
	s.vars = nv
	return nil
}

// Repr is <ContextVar name='x' ...>.
func (c *ContextVar[T]) Repr() string {
	if c.hasDef {
		return fmt.Sprintf("<ContextVar name=%s default=%s at %p>", Repr_str(c.name), Repr(c.def), c)
	}
	return fmt.Sprintf("<ContextVar name=%s at %p>", Repr_str(c.name), c)
}

// Repr is <Token var=... at ...>.
func (t *Token) Repr() string { return fmt.Sprintf("<Token var=%s at %p>", Repr(t.v), t) }

// Context is a snapshot of contextvars (contextvars.Context).
type Context struct{ vars map[any]any }

// Copy_context snapshots the current goroutine's contextvars.
func Copy_context() *Context {
	if s := glsGet(); s != nil {
		return &Context{vars: s.vars}
	}
	return &Context{vars: map[any]any{}}
}

// Run executes fn on the current goroutine with ctx installed, restoring
// the previous context afterwards (Context.run).
func (c *Context) Run(fn func()) {
	s := glsEnsure()
	saved := s.vars
	s.vars = c.vars
	defer func() { s.vars = saved }()
	fn()
}

// Go starts a goroutine that inherits the caller's contextvars (like
// copy_context()) but neither its threading.local values nor its event-loop
// membership (a new thread is never "the loop thread"); the goroutine's
// storage is released when fn returns. Every goroutine spawned by the port
// goes through here.
func Go(fn func()) { goWithState(nil, fn) }

// GoWithState starts a goroutine with explicit initial state (used by the
// Thread and Loop implementations).
func goWithState(init func(s *glsState), fn func()) {
	parent := Copy_context()
	go func() {
		s := glsEnsure()
		s.vars = parent.vars
		if init != nil {
			init(s)
		}
		defer glsClear()
		fn()
	}()
}

// Local is threading.local: per-goroutine attribute storage.
type Local struct{ _ struct{} }

// NewLocal creates a threading.local() object.
func NewLocal() *Local { return &Local{} }

func (l *Local) attrs(create bool) map[string]any {
	var s *glsState
	if create {
		s = glsEnsure()
	} else {
		s = glsGet()
		if s == nil {
			return nil
		}
	}
	if s.locals == nil {
		if !create {
			return nil
		}
		s.locals = map[*Local]map[string]any{}
	}
	m := s.locals[l]
	if m == nil && create {
		m = map[string]any{}
		s.locals[l] = m
	}
	return m
}

// Get returns the attribute for this goroutine: (value, found).
func (l *Local) Get(name string) (any, bool) {
	m := l.attrs(false)
	if m == nil {
		return nil, false
	}
	v, ok := m[name]
	return v, ok
}

// GetOr returns the attribute or def (getattr(local, name, def)).
func (l *Local) GetOr(name string, def any) any {
	if v, ok := l.Get(name); ok {
		return v
	}
	return def
}

// Set assigns the attribute for this goroutine.
func (l *Local) Set(name string, v any) { l.attrs(true)[name] = v }

// Del removes the attribute for this goroutine.
func (l *Local) Del(name string) {
	if m := l.attrs(false); m != nil {
		delete(m, name)
	}
}
