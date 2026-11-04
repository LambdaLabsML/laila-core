package py

import (
	"runtime"
	"sync"
	"weak"
)

// Finalizer is a ``weakref.finalize(obj, func)`` handle: ``fn`` runs once
// when the referent is collected unless Detach was called first. Unlike
// Python the Go runtime gives no exit-time guarantee, so callers that need
// cleanup at shutdown must also call it explicitly.
type Finalizer struct {
	mu       sync.Mutex
	cleanup  runtime.Cleanup
	alive    bool
	detached bool
}

// Finalize registers fn to run when *p is garbage-collected. fn must not
// reference p (otherwise p is never collected).
func Finalize[T any](p *T, fn func()) *Finalizer {
	f := &Finalizer{alive: true}
	f.cleanup = runtime.AddCleanup(p, func(fin *Finalizer) {
		fin.mu.Lock()
		run := fin.alive && !fin.detached
		fin.alive = false
		fin.mu.Unlock()
		if run {
			fn()
		}
	}, f)
	return f
}

// Detach is ``finalizer.detach()``: the callback will never run.
func (f *Finalizer) Detach() {
	if f == nil {
		return
	}
	f.mu.Lock()
	if f.alive && !f.detached {
		f.detached = true
		f.alive = false
		f.cleanup.Stop()
	}
	f.mu.Unlock()
}

// Alive reports whether the callback is still pending.
func (f *Finalizer) Alive() bool {
	if f == nil {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.alive
}

// Weak is a weakref.ref: Value() returns the referent or nil once it has
// been collected.
type Weak interface{ Value() any }

type weakOf[T any] struct{ p weak.Pointer[T] }

func (w weakOf[T]) Value() any {
	if v := w.p.Value(); v != nil {
		return v
	}
	return nil
}

// WeakRef builds a weak reference to a pointer.
func WeakRef[T any](p *T) Weak { return weakOf[T]{weak.Make(p)} }

type weakSelfOf struct {
	p weak.Pointer[Object]
}

func (w weakSelfOf) Value() any {
	if o := w.p.Value(); o != nil && o.self != nil {
		return o.self
	}
	return nil
}

// WeakRef_self builds a weak reference to a class instance that resolves
// to the instance's Self() (the most-derived pointer), so the referent
// keeps its subclass methods even when the caller only holds the
// embedded base. The weak pointer targets the embedded py.Object, an
// interior pointer of the instance, so it goes nil exactly when the
// instance is collected.
func WeakRef_self(o interface{ Object_() *Object }) Weak {
	return weakSelfOf{weak.Make(o.Object_())}
}

// WeakValueDictionary is weakref.WeakValueDictionary keyed by string:
// entries whose referent has been collected read as absent.
type WeakValueDictionary struct {
	mu sync.Mutex
	m  map[string]Weak
}

// NewWeakValueDictionary creates an empty dictionary.
func NewWeakValueDictionary() *WeakValueDictionary {
	return &WeakValueDictionary{m: map[string]Weak{}}
}

// Set is d[key] = value (value must be a WeakRef).
func (d *WeakValueDictionary) Set(key string, ref Weak) {
	d.mu.Lock()
	d.m[key] = ref
	d.mu.Unlock()
}

// Get is d.get(key): (value, found) with collected referents reported absent.
func (d *WeakValueDictionary) Get(key string) (any, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	ref, ok := d.m[key]
	if !ok {
		return nil, false
	}
	v := ref.Value()
	if v == nil {
		delete(d.m, key)
		return nil, false
	}
	return v, true
}

// Delete is del d[key] (no error when missing).
func (d *WeakValueDictionary) Delete(key string) {
	d.mu.Lock()
	delete(d.m, key)
	d.mu.Unlock()
}

// Clear empties the dictionary.
func (d *WeakValueDictionary) Clear() {
	d.mu.Lock()
	d.m = map[string]Weak{}
	d.mu.Unlock()
}

// Values returns the live referents (insertion order is not preserved,
// like a Python dict mutated by collection).
func (d *WeakValueDictionary) Values() []any {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := []any{}
	for k, ref := range d.m {
		if v := ref.Value(); v != nil {
			out = append(out, v)
		} else {
			delete(d.m, k)
		}
	}
	return out
}

// Len counts live entries.
func (d *WeakValueDictionary) Len() int { return len(d.Values()) }
